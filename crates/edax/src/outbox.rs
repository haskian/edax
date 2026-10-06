//! The transactional outbox's read side, an [`Outbox`], and the [`Relay`] that drains one to a
//! publisher: the event outbox and a saga's command outbox alike.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Mutex;

use edax_core::lock;
use edax_event::{Drain, EventMessage, Message, RowFailed};

use crate::failure::{failure_of, HandlerError};
use crate::pacing::Pacing;
use crate::settle::{run_sink, DeadLetter};
use crate::Error;

/// The read side of a transactional outbox: committed rows not yet published, and the acknowledgement
/// that prunes them. Generic over the row, so one store can hold the event outbox (`R` =
/// [`EventMessage`]) and the saga command outbox (`R` = [`CommandMessage`](crate::CommandMessage)) side by side.
pub trait Outbox<R> {
    /// Committed rows not yet published, oldest commit first, at most `limit`. Running more than one
    /// [`Relay`] over one store needs this call to lease or lock the rows it returns, or two relays
    /// read the same batch and publish it twice.
    fn unpublished(&self, limit: usize) -> Result<Vec<R>, Error>;
    /// Acknowledge published rows by id. Idempotent; an unknown id is ignored.
    fn mark_published(&self, ids: &[String]) -> Result<(), Error>;
}

/// An [`Outbox`] that also counts its backlog: how many committed unmarked rows wait now, counted
/// without paging them (`SELECT count(*)` over the unmarked rows). A relay that stalled and one that is
/// idle publish alike, nothing; the depth tells them apart. Optional, as in Python's `CountedOutbox`:
/// every in-memory outbox and [`SagaOutbox`](crate::SagaOutbox) count.
pub trait CountedOutbox<R>: Outbox<R> {
    /// The committed rows not yet marked published, outside any unit of work.
    fn unpublished_count(&self) -> Result<usize, Error>;
}

/// A dead-letter sink for a [`Relay`]: takes the [`DeadLetter`] of the row that cannot be published, with
/// the attempt and the failure that killed it, and returns once the row is somewhere a human will look.
pub type RelaySink<R> = Box<dyn Fn(DeadLetter<&R, &HandlerError>) -> Result<(), HandlerError> + Send + Sync>;

/// Drains an [`Outbox`] to a publisher: the transactional-outbox publisher, for the event outbox
/// (`R` = [`EventMessage`], the default) and the saga command outbox (`R` = [`CommandMessage`](crate::CommandMessage)) alike.
///
/// `publish` must return only once the transport has durably accepted the row. Anything that merely
/// hands it on, such as an in-process queue or a dispatcher's `submit`, makes the outbox
/// at-most-once: the row is marked published while it is still only in memory.
///
/// A drain publishes rows in order and marks every row it settled, also when a row fails part way:
/// publishing is at-least-once, so downstream deduplicates by row id. Order is promised per key, the
/// row's tenant and [`ordering_key`](Message::ordering_key): a failing row holds back the rows behind it
/// of its own key, which may be its aggregate's later events, and the other keys go on. Which rows a
/// drain publishes, and what a failure does, is the kernel's drain rule ([`Drain`]), the same in every
/// SDK. Whether a failed row is handed back or dead-lettered is its retry policy, keyed by row id: a
/// terminal failure, or a transient one at the [`max_attempts`](Self::max_attempts)-th attempt, goes to
/// the [`on_dead`](Self::on_dead) sink and is marked, so one poison row cannot stall its key; a drain
/// that handed a row back waits the largest backoff step among those rows before it returns the first
/// one's error, so a host looping on `drain` does not spin. With no sink a poison row is handed back on
/// every drain, and its wait grows. A second transient failure with no successful publish between them
/// stops the drain: the broker is likely down, and the rest would only fail alike. A fatal failure
/// ([`is_fatal`](crate::is_fatal): a refused credential, topic or setting) hands the row back uncounted,
/// never to the sink, and returns at once: every row behind it would fail alike, so a host looping on
/// `drain` stops on it instead of moving the outbox into the dead-letter queue. A sink that fails fatally
/// stops the drain the same way, which returns the sink's error. A drain whose store call
/// fails (`unpublished` or `mark_published`) waits too, one step per consecutive store failure, so a
/// store that is down is not hammered either; a drain whose store calls succeed starts that count over.
///
/// Overlapping drains of one relay take turns, so a timer and a "drain now" never publish one batch
/// twice. Two relays over one store need [`Outbox::unpublished`] to lease the rows it returns.
pub struct Relay<S, R = EventMessage> {
    store: S,
    /// The kernel's drain state, built by the first drain from the settings below, so the builder holds
    /// each setting once. Taken for a whole drain, so drains take turns.
    draining: Mutex<Option<Drain>>,
    max_attempts: u32,
    on_dead: Option<RelaySink<R>>,
    pacing: Pacing,
    _row: PhantomData<fn() -> R>,
}

impl<S: Outbox<R>, R: Message> Relay<S, R> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            draining: Mutex::new(None),
            max_attempts: 0,
            on_dead: None,
            pacing: Pacing::default(),
            _row: PhantomData,
        }
    }

    /// Dead-letter a row whose publish failed transiently `n` times; `0` (the default) never does.
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n;
        self
    }

    /// Where a row that cannot be published goes, instead of stalling the rows behind it. A sink
    /// that returns `Err` or panics hands the row back, and the next drain tries the sink again.
    pub fn on_dead(
        mut self,
        sink: impl Fn(DeadLetter<&R, &HandlerError>) -> Result<(), HandlerError> + Send + Sync + 'static,
    ) -> Self {
        self.on_dead = Some(Box::new(sink));
        self
    }

    /// How a failed drain waits before it returns: the backoff policy, the sleep and the jitter.
    pub fn pacing(mut self, pacing: Pacing) -> Self {
        self.pacing = pacing;
        self
    }

    /// The outbox this relay drains.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Publish one batch; return how many rows it settled (published or dead-lettered). Call in a
    /// loop to keep draining. `publish` is synchronous; wrap the call in your async runtime for a
    /// network transport. Its error is a [`HandlerError`], so a saga command dispatched through a
    /// [`CommandBus`](crate::CommandBus) keeps its verdict.
    pub fn drain<F>(&self, batch: usize, mut publish: F) -> Result<usize, HandlerError>
    where
        F: FnMut(&R) -> Result<(), HandlerError>,
    {
        let mut draining = lock(&self.draining);
        let drain = draining.get_or_insert_with(|| Drain::new(self.max_attempts, self.on_dead.is_some()));
        let rows = match self.store.unpublished(batch) {
            Ok(rows) => rows,
            Err(error) => return Err(self.store_failed(drain, error)),
        };
        drain.begin(&rows);
        // The failures the drain may raise, by row id: the kernel names the row, the host keeps its error.
        let mut failures: HashMap<String, HandlerError> = HashMap::new();
        for row in &rows {
            if !drain.admits(row.id()) {
                continue;
            }
            let Err(error) = publish(row) else {
                drain.published(row.id());
                continue;
            };
            let mut raised = error;
            match drain.failed(row.id(), failure_of(&*raised)) {
                RowFailed::HandBack | RowFailed::Stop => {}
                // A sink that refused the row, or panicked, hands it back like any failure, and the rows
                // published before it are still marked below. A fatal refusal stops the drain on the
                // sink's own error, since that is the wiring to fix.
                RowFailed::DeadLetter { attempt } => {
                    let sink = self.on_dead.as_ref().map(|sink| {
                        || {
                            sink(DeadLetter {
                                message: row,
                                attempt,
                                failure: &raised,
                            })
                        }
                    });
                    let answer = run_sink(sink);
                    if let (Some(RowFailed::Stop), Some(refusal)) =
                        (drain.sunk(row.id(), attempt, answer.failure), answer.refusal)
                    {
                        raised = refusal;
                    }
                }
            }
            failures.insert(row.id().to_owned(), raised);
        }
        let drained = drain.finish();
        if !drained.settled.is_empty() {
            if let Err(error) = self.store.mark_published(&drained.settled) {
                return Err(self.store_failed(drain, error));
            }
        }
        drain.store_answered();
        match drained.raise.and_then(|id| failures.remove(&id)) {
            Some(error) => {
                if let Some(step) = drained.wait {
                    self.pacing.wait(step);
                }
                Err(error)
            }
            None => Ok(drained.settled.len()),
        }
    }

    /// The store failed, not a row: wait a step that grows while it stays down, so a host looping on
    /// `drain` neither spins nor hammers it.
    fn store_failed(&self, drain: &mut Drain, error: Error) -> HandlerError {
        self.pacing.wait(drain.store_failed());
        error.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::account::{deposited, repo, Account, AccountEvent};
    use crate::{Aggregate, ErrorCode, EventDraft, EventStore, MemoryEventStore, MemoryTx, SaveOptions};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    #[test]
    fn outbox_relay_drains_then_idempotent() {
        let store = MemoryEventStore::new();
        let r = repo(store.clone());
        let mut a = Account::open("acct-5");
        a.apply(AccountEvent::Deposited(1));
        a.apply(AccountEvent::Deposited(2));
        r.save(&mut a, SaveOptions::default()).unwrap();

        let relay = Relay::new(store);
        let mut published: Vec<String> = Vec::new();
        let n = relay
            .drain(100, |env| {
                published.push(env.id.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(published.len(), 2);

        let n2 = relay.drain(100, |_env| Ok(())).unwrap();
        assert_eq!(n2, 0); // nothing left to publish
    }

    // --- the relay ---

    fn step_wait(step: u64) -> std::time::Duration {
        Pacing::recording().0.delay(step)
    }

    /// The event outbox with two fault hooks: every `unpublished` read is reported on `reads`, and
    /// `mark_published` fails while `fail_mark` is set (the relay crashed after the send).
    #[derive(Clone)]
    struct Hooked {
        inner: MemoryEventStore,
        reads: Option<Arc<Mutex<std::sync::mpsc::Sender<()>>>>,
        fail_mark: Arc<AtomicBool>,
    }

    impl Hooked {
        fn new(inner: MemoryEventStore) -> Self {
            Self {
                inner,
                reads: None,
                fail_mark: Arc::default(),
            }
        }
    }

    impl Outbox<EventMessage> for Hooked {
        fn unpublished(&self, limit: usize) -> Result<Vec<EventMessage>, Error> {
            if let Some(reads) = &self.reads {
                let _ = reads.lock().expect("no panics").send(());
            }
            self.inner.unpublished(limit)
        }
        fn mark_published(&self, ids: &[String]) -> Result<(), Error> {
            if self.fail_mark.load(Ordering::SeqCst) {
                return Err(Error::store("connection lost"));
            }
            self.inner.mark_published(ids)
        }
    }

    /// A store holding `n` committed events of one account, all waiting in the outbox.
    fn outbox_of(n: u64) -> MemoryEventStore {
        let store = MemoryEventStore::new();
        let drafts: Vec<EventDraft> = (1..=n).map(|seq| deposited("acct-r", seq, b"1")).collect();
        store.append(&mut MemoryTx::auto_commit(), "acct-r", 0, drafts).unwrap();
        store
    }

    #[derive(Clone, Copy, Debug)]
    enum Publish {
        Ok,
        Transient,
        Terminal,
        Contended,
        Fatal,
    }

    #[test]
    fn relay_drain_branches() {
        #[derive(Clone, Copy)]
        enum Sink {
            None,
            Takes,
            Fails,
            Panics,
            /// Refuses with a fatal error: the sink's own wiring is wrong.
            Fatal,
        }
        struct Case {
            name: &'static str,
            /// How publishing each of the four rows goes, on every drain.
            rows: [Publish; 4],
            sink: Sink,
            max_attempts: u32,
            /// How many drains to run.
            drains: usize,
            want_results: &'static [Result<usize, &'static str>],
            want_left: &'static [&'static str],
            want_dead: &'static [&'static str],
            want_waits: &'static [u64],
        }
        use Publish::{Contended, Fatal, Ok as Pub, Terminal, Transient};
        let base = Case {
            name: "",
            rows: [Pub, Pub, Pub, Pub],
            sink: Sink::None,
            max_attempts: 0,
            drains: 1,
            want_results: &[Ok(4)],
            want_left: &[],
            want_dead: &[],
            want_waits: &[],
        };
        let cases = [
            Case {
                name: "every row published and marked",
                ..base
            },
            Case {
                name: "a transient failure marks the published prefix and waits before it returns",
                rows: [Pub, Transient, Pub, Pub],
                want_results: &[Err("store: broker unavailable")],
                want_left: &["acct-r-2", "acct-r-3", "acct-r-4"],
                want_waits: &[0],
                ..base
            },
            Case {
                name: "the same row failing again waits a longer step",
                rows: [Transient, Pub, Pub, Pub],
                drains: 3,
                want_results: &[
                    Err("store: broker unavailable"),
                    Err("store: broker unavailable"),
                    Err("store: broker unavailable"),
                ],
                want_left: &["acct-r-1", "acct-r-2", "acct-r-3", "acct-r-4"],
                want_waits: &[0, 1, 2],
                ..base
            },
            Case {
                name: "no sink: a poison row stays and its wait grows",
                rows: [Pub, Terminal, Pub, Pub],
                drains: 2,
                want_results: &[Err("codec: message too large"), Err("codec: message too large")],
                want_left: &["acct-r-2", "acct-r-3", "acct-r-4"],
                want_waits: &[0, 1],
                ..base
            },
            Case {
                name: "a sink takes a poison row and the rows behind it go out",
                rows: [Pub, Terminal, Pub, Pub],
                sink: Sink::Takes,
                want_results: &[Ok(4)],
                want_dead: &["acct-r-2"],
                ..base
            },
            Case {
                name: "a sink that fails hands the row back and gets it again next drain",
                rows: [Terminal, Pub, Pub, Pub],
                sink: Sink::Fails,
                drains: 2,
                want_results: &[Err("codec: message too large"), Err("codec: message too large")],
                want_left: &["acct-r-1", "acct-r-2", "acct-r-3", "acct-r-4"],
                want_dead: &["acct-r-1", "acct-r-1"],
                want_waits: &[0, 1],
                ..base
            },
            Case {
                // Caught like the dispatcher's sink: unwinding out of `drain` would skip marking the
                // row published before it (sent again on every drain) and skip the wait.
                name: "a sink that panics hands the row back and the published prefix is marked",
                rows: [Pub, Terminal, Pub, Pub],
                sink: Sink::Panics,
                drains: 2,
                want_results: &[Err("codec: message too large"), Err("codec: message too large")],
                want_left: &["acct-r-2", "acct-r-3", "acct-r-4"],
                want_dead: &["acct-r-2", "acct-r-2"],
                want_waits: &[0, 1],
                ..base
            },
            Case {
                name: "max_attempts dead-letters a row that keeps failing transiently",
                rows: [Transient, Pub, Pub, Pub],
                sink: Sink::Takes,
                max_attempts: 2,
                drains: 2,
                want_results: &[Err("store: broker unavailable"), Ok(4)],
                want_dead: &["acct-r-1"],
                want_waits: &[0],
                ..base
            },
            Case {
                // A wiring fault fails every row alike: the sink never sees it, the ceiling never counts
                // it, and the drain returns at once for the host to stop on.
                name: "a fatal failure hands the row back uncounted and returns without a wait",
                rows: [Pub, Fatal, Pub, Pub],
                sink: Sink::Takes,
                max_attempts: 1,
                drains: 2,
                want_results: &[
                    Err("bad_handle: topic authorization failed"),
                    Err("bad_handle: topic authorization failed"),
                ],
                want_left: &["acct-r-2", "acct-r-3", "acct-r-4"],
                ..base
            },
            Case {
                // The sink's wiring fails every dead row alike: the drain stops on the sink's error, not
                // the row's, with no wait, and the row stays for the drain after the fix.
                name: "a sink that fails fatally stops the drain on its own error",
                rows: [Pub, Terminal, Pub, Pub],
                sink: Sink::Fatal,
                drains: 1,
                want_results: &[Err("bad_handle: dead-letter topic authorization failed")],
                want_left: &["acct-r-2", "acct-r-3", "acct-r-4"],
                want_dead: &["acct-r-2"],
                ..base
            },
            Case {
                name: "contention is handed back without spending an attempt",
                rows: [Contended, Pub, Pub, Pub],
                sink: Sink::Takes,
                max_attempts: 1,
                drains: 2,
                want_results: &[Err("in_progress: fulfill-7"), Err("in_progress: fulfill-7")],
                want_left: &["acct-r-1", "acct-r-2", "acct-r-3", "acct-r-4"],
                want_waits: &[0, 1],
                ..base
            },
        ];
        for c in cases {
            let store = outbox_of(4);
            let (pacing, waits) = Pacing::recording();
            let dead: Arc<Mutex<Vec<String>>> = Arc::default();
            let sink = Arc::clone(&dead);
            let mode = c.sink;
            let mut relay = Relay::new(store.clone()).pacing(pacing).max_attempts(c.max_attempts);
            if !matches!(mode, Sink::None) {
                relay = relay.on_dead(move |dead: DeadLetter<&EventMessage, &HandlerError>| {
                    sink.lock().expect("no panics").push(dead.message.id.clone());
                    match mode {
                        Sink::Fails => Err("dead-letter table unavailable".into()),
                        Sink::Panics => panic!("dead-letter sink bug"),
                        Sink::Fatal => Err(Error::bad_handle("dead-letter topic authorization failed").into()),
                        Sink::None | Sink::Takes => Ok(()),
                    }
                });
            }
            let rows = c.rows;
            let results: Vec<Result<usize, String>> = (0..c.drains)
                .map(|_| {
                    relay
                        .drain(10, |env| {
                            let index = env.sequence.unwrap_or(1).saturating_sub(1) as usize;
                            match rows.get(index).copied().unwrap_or(Pub) {
                                Pub => Ok(()),
                                Transient => Err(Error::store("broker unavailable").into()),
                                Terminal => Err(Error::codec("message too large").into()),
                                Contended => Err(Error::new(ErrorCode::InProgress, "fulfill-7").into()),
                                Fatal => Err(Error::bad_handle("topic authorization failed").into()),
                            }
                        })
                        .map_err(|e| e.to_string())
                })
                .collect();
            let want: Vec<Result<usize, String>> = c.want_results.iter().map(|r| r.map_err(str::to_owned)).collect();
            assert_eq!(results, want, "{}: results", c.name);
            let left: Vec<String> = store.unpublished(10).unwrap().into_iter().map(|e| e.id).collect();
            assert_eq!(left, c.want_left, "{}: left in the outbox", c.name);
            assert_eq!(*dead.lock().unwrap(), c.want_dead, "{}: dead-lettered", c.name);
            let want_waits: Vec<_> = c.want_waits.iter().map(|&step| step_wait(step)).collect();
            assert_eq!(*waits.lock().unwrap(), want_waits, "{}: waits", c.name);
        }
    }

    #[test]
    fn a_failing_key_holds_back_only_its_own_rows() {
        // Two aggregates interleaved in one outbox; publishing fails for some rows on every drain. Order is
        // promised per key, so the other aggregate's rows go out, and two transient failures in a row read
        // as an outage and stop the drain.
        struct Case {
            name: &'static str,
            /// How publishing goes for `acct-A`'s rows and for `acct-B`'s.
            fails: [Option<fn() -> HandlerError>; 2],
            want: Result<usize, &'static str>,
            want_left: &'static [&'static str],
        }
        fn transient() -> HandlerError {
            Error::store("broker unavailable").into()
        }
        fn terminal() -> HandlerError {
            Error::codec("message too large").into()
        }
        let cases = [
            Case {
                name: "a poison row holds back its aggregate and the other one is published",
                fails: [Some(terminal), None],
                want: Err("codec: message too large"),
                want_left: &["acct-A-1", "acct-A-2"],
            },
            Case {
                name: "one transient failure holds back its aggregate only",
                fails: [Some(transient), None],
                want: Err("store: broker unavailable"),
                want_left: &["acct-A-1", "acct-A-2"],
            },
            Case {
                name: "a transient failure on both aggregates stops the drain at the second",
                fails: [Some(transient), Some(transient)],
                want: Err("store: broker unavailable"),
                want_left: &["acct-A-1", "acct-B-1", "acct-A-2", "acct-B-2"],
            },
        ];
        for c in cases {
            let store = MemoryEventStore::new();
            for seq in 1..=2 {
                for aggregate in ["acct-A", "acct-B"] {
                    let draft = EventDraft {
                        id: format!("{aggregate}-{seq}"),
                        ..deposited(aggregate, seq, b"1")
                    };
                    store
                        .append(&mut MemoryTx::auto_commit(), aggregate, seq - 1, vec![draft])
                        .unwrap();
                }
            }
            let relay = Relay::new(store.clone()).pacing(Pacing::recording().0);
            let mut sent: Vec<String> = Vec::new();
            let got = relay.drain(10, |row| {
                sent.push(row.id.clone());
                match c.fails[usize::from(row.aggregate_id == "acct-B")] {
                    Some(fail) => Err(fail()),
                    None => Ok(()),
                }
            });
            assert_eq!(
                got.map_err(|e| e.to_string()),
                c.want.map_err(str::to_owned),
                "{}",
                c.name
            );
            let left: Vec<String> = store.unpublished(10).unwrap().into_iter().map(|e| e.id).collect();
            assert_eq!(left, c.want_left, "{}: left in the outbox", c.name);
            assert!(
                !sent.contains(&"acct-A-2".to_owned()),
                "{}: a held-back row is never sent",
                c.name
            );
        }
    }

    /// The event outbox with a script per store call: each `unpublished` and each `mark_published`
    /// takes the next answer off its queue, `false` failing it, and succeeds once its queue is empty.
    struct Scripted {
        inner: MemoryEventStore,
        reads: Mutex<std::collections::VecDeque<bool>>,
        marks: Mutex<std::collections::VecDeque<bool>>,
    }

    impl Outbox<EventMessage> for Scripted {
        fn unpublished(&self, limit: usize) -> Result<Vec<EventMessage>, Error> {
            if !self.reads.lock().expect("no panics").pop_front().unwrap_or(true) {
                return Err(Error::store("database down"));
            }
            self.inner.unpublished(limit)
        }
        fn mark_published(&self, ids: &[String]) -> Result<(), Error> {
            if !self.marks.lock().expect("no panics").pop_front().unwrap_or(true) {
                return Err(Error::store("connection lost"));
            }
            self.inner.mark_published(ids)
        }
    }

    #[test]
    fn a_store_that_fails_backs_the_drain_off_and_a_store_that_recovers_starts_over() {
        // The relay's own store is down: a host looping on `drain` must wait a step that grows while
        // it stays down, never spin on it, and start over once a drain's store calls succeed. A row's
        // publish failure is the row's own step, kept apart from the store's.
        struct Drain {
            name: &'static str,
            /// Whether the first row's publish fails, transiently.
            publish_fails: bool,
            want: Result<usize, &'static str>,
            /// The backoff step this drain waits, if it waits.
            want_wait: Option<u64>,
        }
        let drains = [
            Drain {
                name: "the read fails: step 0",
                publish_fails: false,
                want: Err("store: database down"),
                want_wait: Some(0),
            },
            Drain {
                name: "it fails again: step 1",
                publish_fails: false,
                want: Err("store: database down"),
                want_wait: Some(1),
            },
            Drain {
                name: "the mark fails: the store's step 2",
                publish_fails: false,
                want: Err("store: connection lost"),
                want_wait: Some(2),
            },
            Drain {
                name: "the store answers, a row fails: the row's step 0",
                publish_fails: true,
                want: Err("store: broker unavailable"),
                want_wait: Some(0),
            },
            Drain {
                name: "the read fails after a recovery: step 0 again",
                publish_fails: false,
                want: Err("store: database down"),
                want_wait: Some(0),
            },
            Drain {
                name: "everything lands: no wait",
                publish_fails: false,
                want: Ok(4),
                want_wait: None,
            },
            Drain {
                name: "the next store failure starts at step 0",
                publish_fails: false,
                want: Err("store: database down"),
                want_wait: Some(0),
            },
        ];
        let store = Scripted {
            inner: outbox_of(4),
            reads: Mutex::new([false, false, true, true, false, true, false].into()),
            marks: Mutex::new([false].into()),
        };
        let (pacing, waits) = Pacing::recording();
        let relay = Relay::new(store).pacing(pacing);
        for drain in drains {
            let before = waits.lock().unwrap().len();
            let got = relay.drain(10, |env| match drain.publish_fails && env.sequence == Some(1) {
                true => Err(Error::store("broker unavailable").into()),
                false => Ok(()),
            });
            assert_eq!(
                got.map_err(|e| e.to_string()),
                drain.want.map_err(str::to_owned),
                "{}",
                drain.name
            );
            let waited: Vec<_> = waits.lock().unwrap()[before..].to_vec();
            assert_eq!(
                waited,
                drain.want_wait.map(step_wait).into_iter().collect::<Vec<_>>(),
                "{}: wait",
                drain.name
            );
        }
        assert!(
            relay.store().inner.unpublished(10).unwrap().is_empty(),
            "every row was marked in the end"
        );
    }

    #[test]
    fn overlapping_drains_of_one_relay_publish_each_row_once() {
        // A timer and a "drain now" on one relay. The first drain is held inside its publish; the
        // second is started meanwhile. It must not read the rows the first has not marked yet, so it
        // waits its turn and then finds nothing.
        let (read_tx, reads) = std::sync::mpsc::channel::<()>();
        let mut hooked = Hooked::new(outbox_of(2));
        hooked.reads = Some(Arc::new(Mutex::new(read_tx)));
        let relay = Arc::new(Relay::new(hooked).pacing(Pacing::recording().0));
        let published: Arc<Mutex<Vec<String>>> = Arc::default();
        let (entered_tx, entered) = std::sync::mpsc::channel::<()>();
        let (release_tx, release) = std::sync::mpsc::channel::<()>();
        let release = Arc::new(Mutex::new(release));

        let first = {
            let (relay, published, release) = (Arc::clone(&relay), Arc::clone(&published), Arc::clone(&release));
            std::thread::spawn(move || {
                relay.drain(10, |env| {
                    if published.lock().expect("no panics").is_empty() {
                        entered_tx.send(()).expect("the test waits");
                        release.lock().expect("no panics").recv().expect("the test releases");
                    }
                    published.lock().expect("no panics").push(env.id.clone());
                    Ok(())
                })
            })
        };
        reads.recv().expect("the first drain read its batch");
        entered.recv().expect("the first drain is publishing");
        let second = {
            let (relay, published) = (Arc::clone(&relay), Arc::clone(&published));
            std::thread::spawn(move || {
                relay.drain(10, |env| {
                    published.lock().expect("no panics").push(env.id.clone());
                    Ok(())
                })
            })
        };
        assert!(
            reads.recv_timeout(std::time::Duration::from_millis(200)).is_err(),
            "the second drain read the outbox while the first was still publishing"
        );
        release_tx.send(()).expect("the first drain waits");
        assert_eq!(first.join().expect("no panics").ok(), Some(2));
        assert_eq!(second.join().expect("no panics").ok(), Some(0));
        assert_eq!(
            *published.lock().unwrap(),
            ["acct-r-1", "acct-r-2"],
            "each row published once"
        );
    }

    #[test]
    fn a_crash_after_the_publish_republishes_and_the_consumer_deduplicates() {
        // Lost acknowledgement: the rows went out but `mark_published` never landed. The next drain
        // publishes them again (at-least-once); a consumer that deduplicates by event id applies
        // each once.
        let hooked = Hooked::new(outbox_of(2));
        hooked.fail_mark.store(true, Ordering::SeqCst);
        let relay = Relay::new(hooked.clone()).pacing(Pacing::recording().0);
        let mut delivered: Vec<String> = Vec::new();
        let crashed = relay.drain(10, |env| {
            delivered.push(env.id.clone());
            Ok(())
        });
        assert_eq!(
            crashed.err().map(|e| e.to_string()).as_deref(),
            Some("store: connection lost")
        );
        hooked.fail_mark.store(false, Ordering::SeqCst);
        let resent = relay.drain(10, |env| {
            delivered.push(env.id.clone());
            Ok(())
        });
        assert_eq!(resent.ok(), Some(2));
        assert_eq!(
            delivered,
            ["acct-r-1", "acct-r-2", "acct-r-1", "acct-r-2"],
            "at-least-once"
        );
        let applied: std::collections::BTreeSet<&String> = delivered.iter().collect();
        assert_eq!(applied.len(), 2, "the consumer's dedup by id applies each once");
        assert!(hooked.inner.unpublished(10).unwrap().is_empty());
    }
}

//! The write-side storage seam: an [`EventStore`] you implement against your own database, plus
//! the orchestration over it — a [`Repository`] that rehydrates and persists aggregates, and a
//! [`Relay`] that drains the outbox to a publisher.
//!
//! The outbox lives on [`EventStore`] rather than in a seam of its own: a transactional outbox is
//! only worth the name if the event rows and the to-publish rows commit together, and one store is
//! the only place that atomicity can be guaranteed.

use edax_event::{EventEnvelope, Metadata, Namespace};

use crate::{Aggregate, AggregateCodec, Error, EventCodec};

/// A marshaled aggregate at a given version, so a load can skip replaying the whole stream.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub aggregate_id: String,
    pub type_name: String,
    pub version: u64,
    pub data: Vec<u8>,
}

/// Owns the transaction boundary: begin a unit of work, commit it, or roll it back. The stores that
/// take a `&mut Tx` ([`EventStore`], [`ProjectionStore`](crate::ProjectionStore),
/// [`IdempotencyStore`](crate::IdempotencyStore)) are pure participants, so any number of them commit
/// together. An [`EventStore`] *is* a [`Transactor`]; a backend can also implement this on its own
/// and hand the same `Tx` to every store. Prefer [`Transactor::transaction`] over calling
/// begin/commit/rollback by hand.
pub trait Transactor {
    /// Backend transaction handle (a connection / transaction). Use `()` for a store that needs none.
    type Tx;

    fn begin(&self) -> Result<Self::Tx, Error>;
    fn commit(&self, tx: Self::Tx) -> Result<(), Error>;
    fn rollback(&self, tx: Self::Tx);

    /// Scoped unit of work: run `body`, commit on `Ok`, roll back on `Err`. Thread the `&mut Tx` to
    /// every store that should commit together (events, projection, idempotency record).
    fn transaction<R>(&self, body: impl FnOnce(&mut Self::Tx) -> Result<R, Error>) -> Result<R, Error>
    where
        Self: Sized,
    {
        with_tx(self, body)
    }
}

/// Holds the open handle until the unit of work settles it. Dropping it unsettled — the body
/// returned `Err`, or it unwound — rolls back. Without this, a panic would leak the handle: a
/// backend that holds the transaction's keys until it ends would hold them forever, and every
/// later write to those keys would fail with [`Error::Concurrency`].
struct Rollback<'a, T: Transactor> {
    transactor: &'a T,
    tx: Option<T::Tx>,
}

impl<T: Transactor> Rollback<'_, T> {
    /// Settle by committing. Consumes the guard, so its `Drop` finds no handle to roll back.
    fn commit(mut self) -> Result<(), Error> {
        match self.tx.take() {
            Some(tx) => self.transactor.commit(tx),
            None => Ok(()),
        }
    }
}

impl<T: Transactor> Drop for Rollback<'_, T> {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            self.transactor.rollback(tx);
        }
    }
}

/// The free-function form of [`Transactor::transaction`]. A panic in `body` rolls back and keeps
/// unwinding; the caller sees the panic, the backend sees a settled transaction.
pub fn with_tx<T, R>(tr: &T, body: impl FnOnce(&mut T::Tx) -> Result<R, Error>) -> Result<R, Error>
where
    T: Transactor,
{
    let mut guard = Rollback { transactor: tr, tx: Some(tr.begin()?) };
    // `None` is unreachable: only `commit` takes the handle, and it consumes the guard.
    let outcome = guard.tx.as_mut().map(body);
    match outcome {
        Some(Ok(r)) => {
            guard.commit()?;
            Ok(r)
        }
        Some(Err(e)) => Err(e),
        None => Err(Error::BadHandle("unit of work has no open transaction".to_string())),
    }
}

/// The durable log: per-aggregate events with optimistic concurrency, aggregate snapshots, and a
/// transactional outbox — all under one transaction so committed state and to-publish records land
/// together.
///
/// The boundary itself lives on [`Transactor`] (the supertrait): begin a unit of work, thread the
/// `&mut Tx` through these writes, then commit so they land atomically.
pub trait EventStore: Transactor {
    /// Events for the aggregate with `sequence > after`, in order (`after` skips a snapshot).
    fn load(&self, tx: &mut Self::Tx, aggregate_id: &str, after: u64) -> Result<Vec<EventEnvelope>, Error>;

    fn load_snapshot(&self, tx: &mut Self::Tx, aggregate_id: &str) -> Result<Option<Snapshot>, Error>;
    fn save_snapshot(&self, tx: &mut Self::Tx, snapshot: &Snapshot) -> Result<(), Error>;

    /// Append `events` and enqueue the same envelopes to the outbox, atomically. Return
    /// [`Error::Concurrency`] if the stored version differs from `expected_version`.
    fn append(
        &self,
        tx: &mut Self::Tx,
        aggregate_id: &str,
        expected_version: u64,
        events: &[EventEnvelope],
    ) -> Result<(), Error>;

    /// Outbox rows not yet published (relay read side). Running more than one [`Relay`] over one
    /// store needs this call to lease or lock the rows it returns, or two relays read the same
    /// batch and publish it twice.
    fn unpublished(&self, limit: usize) -> Result<Vec<EventEnvelope>, Error>;
    /// Acknowledge published outbox rows by envelope id (relay).
    fn mark_published(&self, ids: &[String]) -> Result<(), Error>;
}

fn default_now() -> i64 {
    // The kernel clock is u64 nanos; it fits i64 until 2262, so saturating is a no-op in practice.
    i64::try_from(edax_core::current_timestamp()).unwrap_or(i64::MAX)
}

/// Loads aggregates (snapshot then replay the rest) and saves them (append the pending events and a
/// fresh snapshot in one transaction; the store also enqueues the outbox).
pub struct Repository<S, EC, AC> {
    store: S,
    events: EC,
    aggregates: AC,
    new_id: Box<dyn Fn() -> String + Send + Sync>,
    now: Box<dyn Fn() -> i64 + Send + Sync>,
}

impl<S, EC, AC> Repository<S, EC, AC>
where
    S: EventStore,
    AC: AggregateCodec,
    EC: EventCodec<Domain = <AC::Aggregate as Aggregate>::Event>,
{
    pub fn new(store: S, events: EC, aggregates: AC) -> Self {
        Self { store, events, aggregates, new_id: Box::new(edax_core::guid), now: Box::new(default_now) }
    }

    /// Override id and clock — useful for deterministic tests. Closures, so a caller can capture a
    /// seeded counter instead of reaching for a global.
    pub fn with_clock(
        mut self,
        new_id: impl Fn() -> String + Send + Sync + 'static,
        now: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> Self {
        self.new_id = Box::new(new_id);
        self.now = Box::new(now);
        self
    }

    /// Rehydrate the aggregate (snapshot, then replay the events after it) in a unit of work of its
    /// own. Inside a transaction the caller already opened, use [`Repository::load_in`].
    pub fn load(&self, aggregate_id: &str) -> Result<AC::Aggregate, Error> {
        self.store.transaction(|tx| self.load_in(tx, aggregate_id))
    }

    /// [`Repository::load`] inside the caller's transaction: the read sees that transaction's own
    /// appends, so a save then a load in one unit of work agree (read-your-writes).
    pub fn load_in(&self, tx: &mut S::Tx, aggregate_id: &str) -> Result<AC::Aggregate, Error> {
        self.replay(tx, aggregate_id)
    }

    /// Persist the aggregate's pending events in a unit of work of its own; return the committed
    /// envelopes (publish eagerly, or leave them to the [`Relay`]). On a clean commit the aggregate
    /// is reset to its new version; on error it is left untouched, so a retry (worth it for
    /// [`Error::Store`], not for the terminal [`Error::Codec`]) re-sends the same pending events.
    /// Inside a transaction the caller already opened, use [`Repository::save_in`].
    pub fn save(&self, agg: &mut AC::Aggregate) -> Result<Vec<EventEnvelope>, Error> {
        self.save_with(agg, Namespace::new(), Metadata::new())
    }

    /// [`Repository::save`] inside the caller's transaction, so the events, the snapshot and the
    /// outbox rows commit or roll back with whatever else the caller writes under `tx` — a
    /// projection row, an idempotency claim. The aggregate is reset as soon as the store accepts the
    /// writes, before the caller commits: after a rollback it no longer holds the events, so reload
    /// it rather than save it again.
    pub fn save_in(&self, tx: &mut S::Tx, agg: &mut AC::Aggregate) -> Result<Vec<EventEnvelope>, Error> {
        self.save_with_in(tx, agg, Namespace::new(), Metadata::new())
    }

    /// Like [`Repository::save`], but stamps `namespace` and `metadata` onto every committed envelope, such as
    /// a trace id from the request context, so it lands on the outbox and stitches command to event to
    /// consumer. Keep per-delivery and per-request ids here, not in the command payload, so they never change
    /// an idempotency hash.
    pub fn save_with(
        &self,
        agg: &mut AC::Aggregate,
        namespace: Namespace,
        metadata: Metadata,
    ) -> Result<Vec<EventEnvelope>, Error> {
        // Stage inside the transaction, reset outside it. This method owns the commit, so it waits for the
        // commit to land before clearing the aggregate: a failed commit leaves the pending events in place and
        // the caller can save again. `save_with_in` cannot, because its caller commits after it returns.
        let (envelopes, current) =
            self.store.transaction(|tx| self.stage(tx, agg, namespace, metadata))?;
        Self::reset(agg, current);
        Ok(envelopes)
    }

    /// [`Repository::save_with`] inside the caller's transaction; see [`Repository::save_in`].
    pub fn save_with_in(
        &self,
        tx: &mut S::Tx,
        agg: &mut AC::Aggregate,
        namespace: Namespace,
        metadata: Metadata,
    ) -> Result<Vec<EventEnvelope>, Error> {
        let (envelopes, current) = self.stage(tx, agg, namespace, metadata)?;
        Self::reset(agg, current);
        Ok(envelopes)
    }

    /// Drop the events the store accepted and pin the aggregate to its new version.
    fn reset(agg: &mut AC::Aggregate, current: u64) {
        let root = agg.root_mut();
        root.take_pending();
        root.version = current;
    }

    /// Marshal the pending events, append them and write the snapshot under `tx`. Returns the
    /// envelopes and the aggregate's new version; resetting the aggregate is the caller's, because
    /// only the caller knows whether the commit has landed yet.
    fn stage(
        &self,
        tx: &mut S::Tx,
        agg: &AC::Aggregate,
        namespace: Namespace,
        metadata: Metadata,
    ) -> Result<(Vec<EventEnvelope>, u64), Error> {
        let root = agg.root();
        let initial = root.version;
        let current = root.current();
        let aggregate_id = root.id.clone();

        // Marshal the whole batch before opening the transaction: a payload that cannot be written
        // fails the save outright, so a half-marshaled batch can never reach the store.
        let envelopes: Vec<EventEnvelope> = root
            .pending()
            .iter()
            .enumerate()
            .map(|(i, event)| {
                Ok(EventEnvelope {
                    id: (self.new_id)(),
                    aggregate_id: aggregate_id.clone(),
                    event: self.events.marshal(event)?,
                    sequence: Some(initial + 1 + i as u64),
                    namespace: namespace.clone(),
                    metadata: metadata.clone(),
                    timestamp: (self.now)(),
                })
            })
            .collect::<Result<_, Error>>()?;

        let snapshot = Snapshot {
            aggregate_id: aggregate_id.clone(),
            type_name: self.aggregates.type_name().to_string(),
            version: current,
            // Snapshot every save; add an interval (every N events) if dump cost bites.
            data: self.aggregates.dump(agg)?,
        };

        self.store.append(tx, &aggregate_id, initial, &envelopes)?;
        self.store.save_snapshot(tx, &snapshot)?;
        Ok((envelopes, current))
    }

    fn replay(&self, tx: &mut S::Tx, id: &str) -> Result<AC::Aggregate, Error> {
        let snapshot = self.store.load_snapshot(tx, id)?;
        let base = snapshot.as_ref().map_or(0, |s| s.version);
        let mut agg = self.aggregates.load(id, base, snapshot.as_ref().map(|s| s.data.as_slice()))?;
        let mut last = base;
        for env in self.store.load(tx, id, base)? {
            // Replay drives state directly — not apply(), which would re-queue these as pending.
            agg.transition(&self.events.unmarshal(&env.event)?);
            if let Some(seq) = env.sequence {
                last = seq;
            }
        }
        agg.root_mut().version = last;
        Ok(agg)
    }
}

/// Drains the store's outbox to a publisher: the transactional-outbox publisher. Mark-published runs only
/// after every row in the batch is published, so a failure mid-batch redelivers rather than drops.
///
/// `publish` must return only once the transport has durably accepted the envelope. Anything that merely
/// hands the envelope on, such as an in-process queue or a dispatcher's `submit`, makes the outbox
/// at-most-once: the row is marked published while the event is still only in memory.
///
/// One relay per store, unless [`EventStore::unpublished`] leases the rows it returns.
pub struct Relay<S> {
    store: S,
}

impl<S: EventStore> Relay<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// Publish one batch of unpublished events; return how many. Call in a loop to keep draining.
    /// `publish` is synchronous; wrap the call in your async runtime for a network transport.
    pub fn drain<F>(&self, batch: usize, mut publish: F) -> Result<usize, Error>
    where
        F: FnMut(&EventEnvelope) -> Result<(), Error>,
    {
        let rows = self.store.unpublished(batch)?;
        for env in &rows {
            publish(env)?;
        }
        if !rows.is_empty() {
            let ids: Vec<String> = rows.iter().map(|e| e.id.clone()).collect();
            self.store.mark_published(&ids)?;
        }
        Ok(rows.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{JsonCodec, MemoryEventStore, MemoryTx, Root};
    use edax_event::Event;
    use serde::{Deserialize, Serialize};
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    // --- a tiny domain ---

    #[derive(Clone)]
    enum AccountEvent {
        Deposited(i64),
    }

    struct Account {
        root: Root<AccountEvent>,
        balance: i64,
    }

    impl Account {
        fn open(id: &str) -> Self {
            Self { root: Root::new(id, 0), balance: 0 }
        }
    }

    impl Aggregate for Account {
        type Event = AccountEvent;
        fn root(&self) -> &Root<Self::Event> {
            &self.root
        }
        fn root_mut(&mut self) -> &mut Root<Self::Event> {
            &mut self.root
        }
        fn transition(&mut self, event: &Self::Event) {
            match event {
                AccountEvent::Deposited(n) => self.balance += n,
            }
        }
    }

    struct AccountEvents;
    impl EventCodec for AccountEvents {
        type Domain = AccountEvent;
        fn marshal(&self, e: &AccountEvent) -> Result<Event, Error> {
            let AccountEvent::Deposited(n) = e;
            Ok(Event { name: "Deposited".into(), version: 1, data: n.to_string().into_bytes() })
        }
        fn unmarshal(&self, raw: &Event) -> Result<AccountEvent, Error> {
            let s = std::str::from_utf8(&raw.data).map_err(|e| Error::Codec(e.to_string()))?;
            let n = s.parse::<i64>().map_err(|e| Error::Codec(e.to_string()))?;
            Ok(AccountEvent::Deposited(n))
        }
    }

    struct AccountCodec;
    impl AggregateCodec for AccountCodec {
        type Aggregate = Account;
        fn type_name(&self) -> &str {
            "Account"
        }
        fn load(&self, id: &str, version: u64, data: Option<&[u8]>) -> Result<Account, Error> {
            let mut a = Account { root: Root::new(id, version), balance: 0 };
            if let Some(d) = data {
                let s = std::str::from_utf8(d).map_err(|e| Error::Codec(e.to_string()))?;
                a.balance = s.parse().map_err(|e: std::num::ParseIntError| Error::Codec(e.to_string()))?;
            }
            Ok(a)
        }
        fn dump(&self, agg: &Account) -> Result<Vec<u8>, Error> {
            Ok(agg.balance.to_string().into_bytes())
        }
    }

    fn next_id() -> String {
        static N: AtomicU64 = AtomicU64::new(1);
        format!("e{}", N.fetch_add(1, Ordering::Relaxed))
    }

    fn repo(store: MemoryEventStore) -> Repository<MemoryEventStore, AccountEvents, AccountCodec> {
        Repository::new(store, AccountEvents, AccountCodec).with_clock(next_id, || 0)
    }

    /// A `Deposited` envelope as another writer would append it behind the repository's back.
    fn deposited(aggregate_id: &str, sequence: u64, data: &[u8]) -> EventEnvelope {
        EventEnvelope {
            id: format!("{aggregate_id}-{sequence}"),
            aggregate_id: aggregate_id.into(),
            event: Event { name: "Deposited".into(), version: 1, data: data.to_vec() },
            sequence: Some(sequence),
            namespace: Namespace::new(),
            metadata: Metadata::new(),
            timestamp: 0,
        }
    }

    #[test]
    fn save_assigns_sequences_and_resets_aggregate() {
        let r = repo(MemoryEventStore::new());
        let mut a = Account::open("acct-1");
        a.apply(AccountEvent::Deposited(10));
        a.apply(AccountEvent::Deposited(5));
        let envs = r.save(&mut a).unwrap();
        assert_eq!(envs.iter().map(|e| e.sequence).collect::<Vec<_>>(), vec![Some(1), Some(2)]);
        assert!(a.root().pending().is_empty());
        assert_eq!((a.root().version, a.root().pending().len()), (2, 0));
    }

    #[test]
    fn reload_via_snapshot() {
        let r = repo(MemoryEventStore::new());
        let mut a = Account::open("acct-2");
        a.apply(AccountEvent::Deposited(10));
        a.apply(AccountEvent::Deposited(5));
        r.save(&mut a).unwrap(); // snapshots at version 2
        let b = r.load("acct-2").unwrap();
        assert_eq!(b.balance, 15);
        assert_eq!(b.root().version, 2);
    }

    #[test]
    fn load_pure_replay_no_snapshot() {
        let store = MemoryEventStore::new();
        // Seed events through the public append (no snapshot) so load() must pure-replay.
        store.append(&mut MemoryTx::auto_commit(), "acct-3", 0, &[deposited("acct-3", 1, b"7"), deposited("acct-3", 2, b"3")]).unwrap();
        let a = repo(store).load("acct-3").unwrap();
        assert_eq!(a.balance, 10);
        assert_eq!(a.root().version, 2);
    }

    #[test]
    fn empty_save_appends_nothing_and_still_checks_the_version() {
        let store = MemoryEventStore::new();
        let r = repo(store.clone());
        let mut a = Account::open("acct-6");
        a.apply(AccountEvent::Deposited(4));
        r.save(&mut a).unwrap();

        let envs = r.save(&mut a).unwrap(); // zero-iteration marshal loop
        assert!(envs.is_empty());
        assert_eq!((a.root().version, a.root().pending().len()), (1, 0));
        assert_eq!(store.load(&mut MemoryTx::auto_commit(), "acct-6", 0).unwrap().len(), 1, "nothing appended");
        assert_eq!(store.unpublished(10).unwrap().len(), 1, "nothing enqueued");

        // The empty append is still a compare-and-set, so a stale caller that applied nothing is told
        // so — and its snapshot write is short-circuited rather than rolling the stored one backwards.
        let mut stale = r.load("acct-6").unwrap(); // at version 1
        let mut winner = r.load("acct-6").unwrap();
        winner.apply(AccountEvent::Deposited(6));
        r.save(&mut winner).unwrap(); // store -> version 2, balance 10
        let got = r.save(&mut stale);
        assert!(matches!(got, Err(Error::Concurrency { expected: 1, stored: 2 })), "got {:?}", got.err());
        let snap = store.load_snapshot(&mut MemoryTx::auto_commit(), "acct-6").unwrap().expect("the winner's snapshot");
        assert_eq!((snap.version, snap.data), (2, b"10".to_vec()));
    }

    #[test]
    fn load_replays_events_after_the_snapshot() {
        // Snapshot at v1, then two events land behind it (another writer): load must start from the
        // snapshot and fold only the tail, ending at the tail's last sequence.
        let store = MemoryEventStore::new();
        let r = repo(store.clone());
        let mut a = Account::open("acct-7");
        a.apply(AccountEvent::Deposited(10));
        r.save(&mut a).unwrap(); // snapshot at v1, balance 10

        store.append(&mut MemoryTx::auto_commit(), "acct-7", 1, &[deposited("acct-7", 2, b"5"), deposited("acct-7", 3, b"7")]).unwrap();

        let b = r.load("acct-7").unwrap();
        assert_eq!(b.balance, 22);
        assert_eq!((b.root().version, b.root().pending().len()), (3, 0));
    }

    #[test]
    fn load_surfaces_a_codec_error_from_replay() {
        let store = MemoryEventStore::new();
        store.append(&mut MemoryTx::auto_commit(), "acct-8", 0, &[deposited("acct-8", 1, b"not a number")]).unwrap();
        let got = repo(store).load("acct-8");
        assert!(matches!(got, Err(Error::Codec(_))), "got {:?}", got.err());
    }

    #[test]
    fn with_tx_branches() {
        // The boundary's whole contract: commit on a clean exit, roll back on a raise, exactly one of
        // the two and only after `begin` succeeded. A spy pins *which* one ran and in what order;
        // what a commit or rollback does to the data is the memory backend's own test.
        /// `begin` / `commit` fail with the message given, if any; every call is logged in order.
        struct Spy {
            begin: Option<&'static str>,
            commit: Option<&'static str>,
            log: RefCell<Vec<&'static str>>,
        }
        impl Spy {
            fn record(&self, step: &'static str, failure: Option<&'static str>) -> Result<(), Error> {
                self.log.borrow_mut().push(step);
                failure.map_or(Ok(()), |m| Err(Error::Store(m.to_string())))
            }
        }
        impl Transactor for Spy {
            type Tx = ();
            fn begin(&self) -> Result<(), Error> {
                self.record("begin", self.begin)
            }
            fn commit(&self, _tx: ()) -> Result<(), Error> {
                self.record("commit", self.commit)
            }
            fn rollback(&self, _tx: ()) {
                let _ = self.record("rollback", None);
            }
        }

        struct Case {
            name: &'static str,
            begin: Option<&'static str>,
            commit: Option<&'static str>,
            body: fn() -> Result<u8, Error>,
            want_log: &'static [&'static str],
            /// The value a clean unit of work yields, or the message the error must carry.
            want: Result<u8, &'static str>,
        }
        let cases = [
            Case {
                name: "clean body commits",
                begin: None,
                commit: None,
                body: || Ok(7),
                want_log: &["begin", "commit"],
                want: Ok(7),
            },
            Case {
                name: "failing body rolls back and never commits",
                begin: None,
                commit: None,
                body: || Err(Error::Store("body gave up".into())),
                want_log: &["begin", "rollback"],
                want: Err("store error: body gave up"),
            },
            Case {
                name: "a refused commit surfaces and does not also roll back",
                begin: None,
                commit: Some("commit refused"),
                body: || Ok(7),
                want_log: &["begin", "commit"],
                want: Err("store error: commit refused"),
            },
            Case {
                name: "a refused begin runs neither the body nor a rollback",
                begin: Some("begin refused"),
                commit: None,
                body: || panic!("the body must not run without a transaction"),
                want_log: &["begin"],
                want: Err("store error: begin refused"),
            },
        ];
        for c in cases {
            let spy = Spy { begin: c.begin, commit: c.commit, log: RefCell::new(Vec::new()) };
            let got = with_tx(&spy, |_tx| (c.body)());
            assert_eq!(spy.log.borrow().as_slice(), c.want_log, "{}", c.name);
            match c.want {
                Ok(value) => assert_eq!(got.ok(), Some(value), "{}", c.name),
                Err(message) => assert_eq!(got.err().map(|e| e.to_string()).as_deref(), Some(message), "{}", c.name),
            }
        }
    }

    #[test]
    fn stale_expected_version_conflicts() {
        let store = MemoryEventStore::new();
        let r = repo(store.clone());
        let mut a = Account::open("acct-4");
        a.apply(AccountEvent::Deposited(1));
        r.save(&mut a).unwrap();

        let mut stale = r.load("acct-4").unwrap(); // initial 1
        let mut other = r.load("acct-4").unwrap();
        other.apply(AccountEvent::Deposited(2));
        r.save(&mut other).unwrap(); // store now at version 2

        stale.apply(AccountEvent::Deposited(3));
        match r.save(&mut stale) {
            Err(Error::Concurrency { expected: 1, stored: 2 }) => {}
            other => panic!("expected concurrency error, got {:?}", other.err()),
        }

        // The rejected append short-circuits the snapshot write: the stored snapshot is still the
        // winner's (version 2, balance 3), never the loser's state — a snapshot may lag, never lead.
        let snap = store.load_snapshot(&mut MemoryTx::auto_commit(), "acct-4").unwrap().expect("winner's snapshot");
        assert_eq!((snap.version, snap.data), (2, b"3".to_vec()));
        assert_eq!(store.load(&mut MemoryTx::auto_commit(), "acct-4", 0).unwrap().len(), 2, "nothing appended by the loser");
    }

    // --- a domain whose codecs genuinely fail ---

    /// A posting keyed by `(account, leg)`. `serde_json` cannot write a non-string map key, so an
    /// event carrying entries really fails to marshal and a memo posting (`entries` empty) really
    /// succeeds — no test double pretends to fail.
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
    struct Posted {
        reference: String,
        entries: BTreeMap<(String, u8), i64>,
    }

    struct Ledger {
        root: Root<Posted>,
        balances: BTreeMap<(String, u8), i64>,
    }

    impl Aggregate for Ledger {
        type Event = Posted;
        fn root(&self) -> &Root<Self::Event> {
            &self.root
        }
        fn root_mut(&mut self) -> &mut Root<Self::Event> {
            &mut self.root
        }
        fn transition(&mut self, event: &Self::Event) {
            for (account, minor_units) in &event.entries {
                *self.balances.entry(account.clone()).or_default() += minor_units;
            }
        }
    }

    /// Snapshots the balances as JSON, so the snapshot seam fails on exactly the keys the event
    /// seam fails on.
    struct LedgerCodec;
    impl AggregateCodec for LedgerCodec {
        type Aggregate = Ledger;
        fn type_name(&self) -> &str {
            "Ledger"
        }
        fn load(&self, id: &str, version: u64, data: Option<&[u8]>) -> Result<Ledger, Error> {
            let balances = match data {
                Some(d) => serde_json::from_slice(d).map_err(|e| Error::Codec(e.to_string()))?,
                None => BTreeMap::new(),
            };
            Ok(Ledger { root: Root::new(id, version), balances })
        }
        fn dump(&self, agg: &Ledger) -> Result<Vec<u8>, Error> {
            serde_json::to_vec(&agg.balances).map_err(|e| Error::Codec(e.to_string()))
        }
    }

    #[test]
    fn save_reports_codec_failures_and_appends_nothing() {
        /// One posting line: account, leg, minor units.
        type Entry = (&'static str, u8, i64);

        fn posted(reference: &str, entries: &[Entry]) -> Posted {
            let entries = entries.iter().map(|(a, leg, n)| ((a.to_string(), *leg), *n)).collect();
            Posted { reference: reference.to_string(), entries }
        }

        struct Case {
            name: &'static str,
            /// Opening balances, which drive the snapshot codec.
            opening: &'static [Entry],
            /// Entries per applied event, which drive the event codec.
            events: &'static [&'static [Entry]],
            /// The sequences committed on success, or the message the error must carry.
            want: Result<&'static [u64], &'static str>,
        }
        let cases = [
            Case {
                name: "every seam writes: the batch commits",
                opening: &[],
                events: &[&[], &[]],
                want: Ok(&[1, 2]),
            },
            Case {
                name: "the only event fails to marshal",
                opening: &[],
                events: &[&[("cash", 0, 1_250)]],
                want: Err("codec error: key must be a string"),
            },
            Case {
                name: "the third of three fails: no partial batch reaches the store",
                opening: &[],
                events: &[&[], &[], &[("cash", 0, 1_250)]],
                want: Err("codec error: key must be a string"),
            },
            Case {
                name: "every event marshals but the snapshot does not",
                opening: &[("cash", 0, 1_250)],
                events: &[&[]],
                want: Err("codec error: key must be a string"),
            },
        ];

        for (i, c) in cases.into_iter().enumerate() {
            let id = format!("ledger-{i}");
            let store = MemoryEventStore::new();
            let repo = Repository::new(store.clone(), JsonCodec::<Posted>::new("Posted", 1), LedgerCodec)
                .with_clock(next_id, || 0);
            let mut ledger = Ledger {
                root: Root::new(&id, 0),
                balances: posted("opening", c.opening).entries,
            };
            for (n, entries) in c.events.iter().enumerate() {
                ledger.apply(posted(&format!("inv-{n}"), entries));
            }

            let got = repo.save(&mut ledger);
            let appended = store.load(&mut MemoryTx::auto_commit(), &id, 0).unwrap();
            let snapshot = store.load_snapshot(&mut MemoryTx::auto_commit(), &id).unwrap();
            match c.want {
                Ok(sequences) => {
                    let envelopes = got.unwrap_or_else(|e| panic!("{}: want Ok, got {e:?}", c.name));
                    let committed: Vec<u64> = envelopes.iter().filter_map(|e| e.sequence).collect();
                    assert_eq!(committed, sequences, "{}", c.name);
                    assert_eq!(appended.len(), sequences.len(), "{}", c.name);
                    assert_eq!(store.unpublished(10).unwrap().len(), sequences.len(), "{}", c.name);
                    assert_eq!(snapshot.map(|s| s.data), Some(b"{}".to_vec()), "{}", c.name);
                    assert!(ledger.root().pending().is_empty(), "{}", c.name);
                    assert_eq!(ledger.root().version, sequences.len() as u64, "{}", c.name);
                }
                Err(message) => {
                    assert!(matches!(&got, Err(Error::Codec(_))), "{}: got {:?}", c.name, got.as_ref().err());
                    assert_eq!(got.err().map(|e| e.to_string()).as_deref(), Some(message), "{}", c.name);
                    assert!(appended.is_empty(), "{}: appended {appended:?}", c.name);
                    assert!(store.unpublished(10).unwrap().is_empty(), "{}: outbox not empty", c.name);
                    assert!(snapshot.is_none(), "{}: a snapshot was written", c.name);
                    // The aggregate is untouched, so the caller still owns the events it applied.
                    assert_eq!(ledger.root().pending().len(), c.events.len(), "{}", c.name);
                }
            }
        }
    }

    #[test]
    fn a_panic_inside_with_tx_rolls_back_and_frees_the_stream() {
        // Without the drop guard the token stays in the adapter's open map, which holds the
        // stream's keys forever: every later append on it would fail with `Concurrency`.
        let store = MemoryEventStore::new();
        let deposit = |seq: u64| EventEnvelope {
            id: format!("panic-{seq}"),
            aggregate_id: "acct-panic".to_string(),
            event: Event { name: "Deposited".into(), version: 1, data: b"1".to_vec() },
            sequence: Some(seq),
            namespace: Default::default(),
            metadata: Default::default(),
            timestamp: 0,
        };

        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<(), Error> = store.transaction(|tx| {
                store.append(tx, "acct-panic", 0, &[deposit(1)])?;
                panic!("the handler blew up mid-transaction");
            });
        }));
        assert!(unwound.is_err(), "the panic keeps unwinding past with_tx");

        // Rolled back, so the stream is still at version 0 and the same append lands.
        let second = store.append(&mut MemoryTx::auto_commit(), "acct-panic", 0, &[deposit(1)]);
        assert!(second.is_ok(), "append after the panic: {:?}", second.err());
        let stored = store.load(&mut MemoryTx::auto_commit(), "acct-panic", 0).unwrap();
        assert_eq!(stored.len(), 1, "only the second append is stored");
    }

    #[test]
    fn outbox_relay_drains_then_idempotent() {
        let store = MemoryEventStore::new();
        let r = repo(store.clone());
        let mut a = Account::open("acct-5");
        a.apply(AccountEvent::Deposited(1));
        a.apply(AccountEvent::Deposited(2));
        r.save(&mut a).unwrap();

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
}

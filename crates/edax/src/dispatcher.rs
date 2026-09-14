//! Ordered-parallel consumer.
//!
//! [`KeyOrderedDispatcher`] fans one input stream out to a local pool of worker threads, routing each
//! event by aggregate id so all events for an aggregate land on the same worker (handled in order
//! on the success path — see [`KeyOrderedDispatcher`] for what a nack does to that order) while
//! different aggregates progress in parallel. The read-side [`crate::router::EventRouter`]
//! plugs in as the per-event handler.

use std::any::Any;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{channel, sync_channel, Receiver, SendError, Sender, SyncSender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use edax_core::backoff_millis;
use edax_event::{partition, EventEnvelope};

use crate::command::is_terminal;
use crate::Error;

/// A broker delivery: the event plus the handles to settle it. Composition over the persisted
/// [`EventEnvelope`] — the ack token is per-delivery transport state (a Kafka offset, an SQS receipt,
/// a Rabbit delivery tag), not part of the event, and most are not stable across redelivery.
///
/// Build one in your broker adapter and hand it to [`KeyOrderedDispatcher::deliver`]; the worker acks on
/// success and nacks (for redelivery) when the handler errors. The settle closures run on a worker
/// thread, hence the `Send + 'static` bounds. Or hold one yourself and call [`Delivery::ack`] /
/// [`Delivery::nack`] for synchronous in-loop consumption.
pub struct Delivery {
    /// The event to handle; lent to the consumer's handler.
    pub envelope: EventEnvelope,
    /// The broker's own redelivery count when it keeps one, else `0`. It is what lets a
    /// dead-letter ceiling survive a consumer restart; see
    /// [`DispatcherBuilder::max_attempts`].
    pub attempt: u32,
    on_ack: Box<dyn FnOnce() + Send>,
    on_nack: Box<dyn FnOnce() + Send>,
}

impl Delivery {
    pub fn new(
        envelope: EventEnvelope,
        on_ack: impl FnOnce() + Send + 'static,
        on_nack: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self { envelope, attempt: 0, on_ack: Box::new(on_ack), on_nack: Box::new(on_nack) }
    }

    /// The handler succeeded; the broker can drop the message.
    pub fn ack(self) {
        (self.on_ack)()
    }

    /// The handler failed; the broker should redeliver.
    pub fn nack(self) {
        (self.on_nack)()
    }
}

/// How a handler run failed. A panic carries the unwind payload (a `&str` or `String` message from
/// `panic!`), so a dead-letter sink can record it.
#[derive(Debug)]
pub enum HandlerFailure {
    Error(Error),
    Panic(Box<dyn Any + Send>),
}

/// A delivery the dispatcher gave up on, handed to the `on_dead` hook (see
/// [`DispatcherBuilder::on_dead`]). Owns the envelope the worker held, so nothing is ever copied on
/// the success path and a sink can publish it without a clone.
#[derive(Debug)]
pub struct DeadLetter {
    pub envelope: EventEnvelope,
    /// The failed attempt that killed it: `1` for a terminal error on first delivery, the
    /// configured `max_attempts` for an exhausted transient one.
    pub attempt: u32,
    pub failure: HandlerFailure,
}

type Handler = dyn Fn(&EventEnvelope) -> Result<(), Error> + Send + Sync;
type DeadLetterHook = dyn Fn(DeadLetter) -> Result<(), Error> + Send + Sync;

/// Fans one input stream out to a local pool of worker threads; `workers` is purely local parallelism. The
/// worker for an event is `partition(aggregate_id, workers)`, so every event for an aggregate lands on the
/// same worker and is handled in submitted order, while different aggregates run concurrently.
///
/// Ordering holds for successful deliveries only: a nack requeues the delivery at the broker's tail, behind
/// same-key deliveries already on this worker, so a handler that must not see its key out of order has to
/// reject a stale event itself ([`should_apply`](crate::should_apply)). [`Error::InProgress`] never counts
/// toward [`DispatcherBuilder::max_attempts`] and blocks every key on this worker until the peer's claim expires.
pub struct KeyOrderedDispatcher {
    senders: Vec<Queue>,
    workers: Vec<JoinHandle<()>>,
}

/// One worker's inbox. Bounded by default; unbounded only when the host asks for it, which is why
/// this is not one `SyncSender` — `sync_channel(0)` is a rendezvous, not "no limit".
enum Queue {
    Bounded(SyncSender<Delivery>),
    Unbounded(Sender<Delivery>),
}

impl Queue {
    /// Blocks while a bounded queue is full. The error hands the delivery back so the caller can
    /// nack it, which is the whole point of returning it by value.
    #[allow(clippy::result_large_err)]
    fn send(&self, delivery: Delivery) -> Result<(), SendError<Delivery>> {
        match self {
            Queue::Bounded(sender) => sender.send(delivery),
            Queue::Unbounded(sender) => sender.send(delivery),
        }
    }
}

/// Deliveries a worker may hold before [`KeyOrderedDispatcher::deliver`] blocks.
const DEFAULT_QUEUE_DEPTH: usize = 1024;

/// Configures a [`KeyOrderedDispatcher`]; from [`KeyOrderedDispatcher::builder`].
pub struct DispatcherBuilder {
    workers: usize,
    handler: Arc<Handler>,
    max_attempts: u32,
    queue_depth: usize,
    on_dead: Option<Arc<DeadLetterHook>>,
}

impl DispatcherBuilder {
    /// Dead-letter a delivery whose handler fails transiently for the `n`-th time. `0` (the
    /// default) disables the ceiling. An *attempt* is one handler run of a delivery, counted as
    /// `max(Delivery::attempt, in-process count) + 1`. The in-process count resets on restart, so
    /// a crash mid-retry grants a fresh `n`; a broker-supplied [`Delivery::attempt`] makes the
    /// ceiling survive restarts.
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n;
        self
    }

    /// Deliveries one worker may hold before [`KeyOrderedDispatcher::deliver`] blocks. Default
    /// 1024; `0` means unbounded, which trades backpressure for memory the broker controls.
    pub fn queue_depth(mut self, n: usize) -> Self {
        self.queue_depth = n;
        self
    }

    /// Install the dead-letter sink. A delivery is dead-lettered (`on_dead` called, then the delivery acked,
    /// not nacked) when the handler error is terminal (see [`crate::Error`]), so it dies on the first failure
    /// whatever `max_attempts` says, or when the failure is transient (a transient error or a panic) and this
    /// is the `max_attempts`-th failed attempt.
    ///
    /// Without a sink a dead delivery still nacks: never a silent drop. A sink that returns `Err` or panics
    /// leaves the delivery neither acked nor nacked, so the broker redelivers on its session timeout and the
    /// sink gets another try. Runs on a worker thread, hence the bounds.
    pub fn on_dead<F>(mut self, on_dead: F) -> Self
    where
        F: Fn(DeadLetter) -> Result<(), Error> + Send + Sync + 'static,
    {
        self.on_dead = Some(Arc::new(on_dead));
        self
    }

    /// Spawn `max(1, workers)` worker threads, each draining its own channel through the handler.
    pub fn build(self) -> KeyOrderedDispatcher {
        let n = self.workers.max(1);
        let mut senders = Vec::with_capacity(n);
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let (sender, receiver) = match self.queue_depth {
                0 => {
                    let (sender, receiver) = channel::<Delivery>();
                    (Queue::Unbounded(sender), receiver)
                }
                depth => {
                    let (sender, receiver) = sync_channel::<Delivery>(depth);
                    (Queue::Bounded(sender), receiver)
                }
            };
            let worker = Worker {
                handler: Arc::clone(&self.handler),
                max_attempts: self.max_attempts,
                on_dead: self.on_dead.clone(),
                attempts: HashMap::new(),
                contended: HashMap::new(),
            };
            senders.push(sender);
            let name = format!("edax-worker-{i}");
            // A refused thread drops its receiver, so `deliver` nacks that partition rather than
            // stranding it — the same path a stopped dispatcher takes.
            if let Ok(handle) = std::thread::Builder::new().name(name).spawn(move || worker.run(receiver)) {
                handles.push(handle);
            }
        }
        KeyOrderedDispatcher { senders, workers: handles }
    }
}

impl KeyOrderedDispatcher {
    /// Spawn `max(1, workers)` worker threads with the default policy: a failing handler nacks and
    /// the broker redelivers forever. See [`KeyOrderedDispatcher::builder`] to bound that.
    pub fn new<H>(workers: usize, handler: H) -> Self
    where
        H: Fn(&EventEnvelope) -> Result<(), Error> + Send + Sync + 'static,
    {
        Self::builder(workers, handler).build()
    }

    /// Start configuring a dispatcher; [`DispatcherBuilder::build`] spawns it.
    pub fn builder<H>(workers: usize, handler: H) -> DispatcherBuilder
    where
        H: Fn(&EventEnvelope) -> Result<(), Error> + Send + Sync + 'static,
    {
        DispatcherBuilder {
            workers,
            handler: Arc::new(handler),
            max_attempts: 0,
            queue_depth: DEFAULT_QUEUE_DEPTH,
            on_dead: None,
        }
    }

    /// Submit a bare envelope, fire-and-forget (no acknowledgement). Routed to its worker by
    /// aggregate id; events for one aggregate keep FIFO order.
    pub fn submit(&self, env: EventEnvelope) {
        // No-op settlers; ZST closures don't allocate, so this stays as cheap as a bare send.
        self.deliver(Delivery::new(env, || {}, || {}));
    }

    /// Submit a [`Delivery`]: the worker acks it when the handler returns, nacks it (for the broker
    /// to redeliver) when the handler errors or panics, and dead-letters it under the builder's
    /// policy. Same aggregate-id routing and FIFO ordering. Blocks while that worker's queue is
    /// full (see [`DispatcherBuilder::queue_depth`]).
    pub fn deliver(&self, delivery: Delivery) {
        let workers = u32::try_from(self.senders.len()).unwrap_or(u32::MAX);
        let idx = partition(&delivery.envelope.aggregate_id, workers) as usize;
        // The worker loop never unwinds, so the receiver lives until stop() consumes self. If it is
        // gone anyway, nack rather than strand the delivery.
        if let Err(SendError(delivery)) = self.senders[idx].send(delivery) {
            delivery.nack();
        }
    }

    /// Drop the senders so each worker drains its queue and exits, then join all workers.
    pub fn stop(self) {
        drop(self.senders);
        for handle in self.workers {
            // The worker body catches every unwind, so join only fails on an aborted thread.
            let _ = handle.join();
        }
    }
}

/// One worker thread's state. The attempt map is per worker and unlocked: an envelope id always
/// carries the same aggregate id, and the aggregate routes to exactly one worker for the life of
/// the dispatcher, so no other thread ever reads or writes this entry. Entries for nacked messages
/// that never come back stay until the id is seen again; bound with a TTL or LRU if a broker-side
/// DLQ steals them under sustained failure.
struct Worker {
    handler: Arc<Handler>,
    max_attempts: u32,
    on_dead: Option<Arc<DeadLetterHook>>,
    attempts: HashMap<String, u32>,
    /// Backoff step per message currently losing a claim race. Separate from `attempts` because
    /// contention is not a failed attempt; same unbounded-entry caveat.
    contended: HashMap<String, u32>,
}

impl Worker {
    fn run(mut self, receiver: Receiver<Delivery>) {
        // Loop ends when all senders are dropped (channel disconnected) on stop().
        for delivery in receiver {
            self.settle(delivery);
        }
    }

    /// Run the handler, then ack, nack or dead-letter the delivery by outcome.
    fn settle(&mut self, delivery: Delivery) {
        let Delivery { envelope, attempt: broker_attempt, on_ack, on_nack } = delivery;
        let id = envelope.id.clone();
        let Some(failure) = self.run_handler(&envelope) else {
            self.contended.remove(&id);
            self.attempts.remove(&id);
            run_settler(on_ack);
            return;
        };
        if matches!(&failure, HandlerFailure::Error(Error::InProgress(_))) {
            self.on_contended(id, on_nack);
            return;
        }
        self.on_failure(envelope, id, broker_attempt, failure, on_ack, on_nack);
    }

    /// The handler's verdict: `None` on success, otherwise how it failed. A panic must not kill the
    /// worker — the deliveries queued behind it would be dropped unsettled and the partition would
    /// stall. The default panic hook has already printed it. Redelivery ordering against later
    /// same-aggregate messages already acked is the broker's contract, not ours.
    fn run_handler(&self, envelope: &EventEnvelope) -> Option<HandlerFailure> {
        match catch_unwind(AssertUnwindSafe(|| (self.handler)(envelope))) {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(HandlerFailure::Error(e)),
            Err(payload) => Some(HandlerFailure::Panic(payload)),
        }
    }

    /// Another consumer holds this key's idempotency claim. Contention, not a failed attempt:
    /// counting it would dead-letter a message whose holder is only slow, and nacking at once would
    /// spin the broker until the claim's TTL expires. Wait one step of the kernel's backoff
    /// schedule, then redeliver without touching the budget.
    fn on_contended(&mut self, id: String, on_nack: Box<dyn FnOnce() + Send>) {
        let step = self.contended.entry(id).or_insert(0);
        std::thread::sleep(Duration::from_millis(backoff_millis(*step)));
        *step += 1;
        run_settler(on_nack);
    }

    /// Dead-letter the delivery or nack it for redelivery, per the builder's policy.
    fn on_failure(
        &mut self,
        envelope: EventEnvelope,
        id: String,
        broker_attempt: u32,
        failure: HandlerFailure,
        on_ack: Box<dyn FnOnce() + Send>,
        on_nack: Box<dyn FnOnce() + Send>,
    ) {
        self.contended.remove(&id);
        let attempt = broker_attempt.max(self.attempts.get(&id).copied().unwrap_or(0)).saturating_add(1);
        // A panic is unclassified, so it is transient: a bug on one payload is exactly the poison
        // message the ceiling exists for, but a single panic cannot be told from a transient one.
        let terminal = match &failure {
            HandlerFailure::Error(e) => is_terminal(e),
            HandlerFailure::Panic(_) => false,
        };
        let exhausted = self.max_attempts > 0 && attempt >= self.max_attempts;
        match self.on_dead.clone() {
            Some(on_dead) if terminal || exhausted => {
                let dead = DeadLetter { envelope, attempt, failure };
                // The sink runs before any ack: if it errors or panics, nothing is settled and the
                // broker redelivers, so the message is never dropped. The attempt map keeps its old
                // count, so the redelivery dead-letters again and the sink gets another try.
                let Ok(Ok(())) = catch_unwind(AssertUnwindSafe(|| on_dead(dead))) else {
                    return;
                };
                run_settler(on_ack);
                self.attempts.remove(&id);
            }
            _ => {
                if self.max_attempts > 0 {
                    self.attempts.insert(id, attempt);
                }
                run_settler(on_nack);
            }
        }
    }
}

/// Run a settle closure. A panicking one is the adapter's bug and there is nothing left that could
/// settle the delivery, but it must not take the worker down with it.
fn run_settler(f: Box<dyn FnOnce() + Send>) {
    let _ = catch_unwind(AssertUnwindSafe(f));
}

#[cfg(test)]
mod tests {
    use super::*;
    use edax_event::Event;
    use std::sync::{Arc, Mutex};

    fn envelope(aggregate_id: &str, name: &str, sequence: u64) -> EventEnvelope {
        EventEnvelope {
            id: format!("{aggregate_id}-{sequence}"),
            aggregate_id: aggregate_id.to_string(),
            event: Event { name: name.to_string(), version: 1, data: Vec::new() },
            sequence: Some(sequence),
            namespace: Default::default(),
            metadata: Default::default(),
            timestamp: 0,
        }
    }

    /// A bounded queue is the whole point of `queue_depth`: once the worker is busy and the one
    /// buffered slot is taken, the next `deliver` waits for the handler instead of queueing without
    /// limit.
    #[test]
    fn deliver_blocks_while_the_workers_queue_is_full() {
        let (entered, has_entered) = std::sync::mpsc::channel::<()>();
        let (release, may_return) = std::sync::mpsc::channel::<()>();
        let may_return = Mutex::new(may_return);
        let dispatcher = KeyOrderedDispatcher::builder(1, move |_: &EventEnvelope| {
            let _ = entered.send(());
            // Each handler run consumes one token, so the test decides when a worker frees a slot.
            let _ = may_return.lock().expect("no handler panics while holding the gate").recv();
            Ok(())
        })
        .queue_depth(1)
        .build();

        let deliver = |seq: u64| Delivery::new(envelope("order-a", "Tick", seq), || {}, || {});
        dispatcher.deliver(deliver(1));
        has_entered.recv().expect("the worker picked up the first delivery");
        dispatcher.deliver(deliver(2)); // fills the one buffered slot

        // The third has nowhere to go until the first handler returns and the worker takes the second.
        let (delivered, third_returned) = std::sync::mpsc::channel::<()>();
        let sender = std::thread::spawn(move || {
            dispatcher.deliver(deliver(3));
            let _ = delivered.send(());
            dispatcher
        });
        assert!(
            third_returned.recv_timeout(Duration::from_millis(100)).is_err(),
            "deliver returned while the queue was full"
        );

        release.send(()).expect("the first handler is waiting");
        third_returned.recv_timeout(Duration::from_secs(5)).expect("deliver returns once a slot frees");

        // Let the two queued deliveries finish so the workers can be joined.
        for _ in 0..2 {
            release.send(()).expect("a handler is waiting");
        }
        sender.join().expect("the sending thread finished").stop();
    }

    #[test]
    fn key_ordered_dispatch_preserves_per_aggregate_order() {
        let collected: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&collected);
        let consumer = KeyOrderedDispatcher::new(2, move |env: &EventEnvelope| {
            sink.lock().unwrap().push((env.aggregate_id.clone(), env.sequence.unwrap()));
            Ok(())
        });

        // Interleave two aggregates' events; each aggregate's events are submitted in order.
        for seq in 1..=4u64 {
            consumer.submit(envelope("order-a", "Tick", seq));
            consumer.submit(envelope("order-b", "Tick", seq));
        }
        consumer.stop();

        let got = collected.lock().unwrap();
        for agg in ["order-a", "order-b"] {
            let seqs: Vec<u64> =
                got.iter().filter(|(a, _)| a == agg).map(|(_, s)| *s).collect();
            assert_eq!(seqs, vec![1, 2, 3, 4], "events for {agg} arrived out of order");
        }
    }

    #[test]
    fn worker_survives_a_panicking_handler_and_nacks_it() {
        // One worker so the panicking delivery and the ones behind it share a queue: the panic must
        // settle as a nack and the later deliveries must still be handled and acked.
        let acked: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let nacked: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let consumer = KeyOrderedDispatcher::new(0, |env: &EventEnvelope| {
            if env.event.name == "Boom" {
                panic!("handler bug");
            }
            Ok(())
        });

        for (seq, name) in [(1, "Boom"), (2, "Tick"), (3, "Tick")] {
            let (a, n) = (Arc::clone(&acked), Arc::clone(&nacked));
            let (ack_id, nack_id) = (format!("a-{seq}"), format!("a-{seq}"));
            consumer.deliver(Delivery::new(
                envelope("a", name, seq),
                move || a.lock().unwrap().push(ack_id),
                move || n.lock().unwrap().push(nack_id),
            ));
        }
        consumer.stop();

        assert_eq!(*nacked.lock().unwrap(), vec!["a-1".to_string()], "the panic nacks for redelivery");
        assert_eq!(*acked.lock().unwrap(), vec!["a-2".to_string(), "a-3".to_string()], "the worker kept draining");
    }

    #[test]
    fn settling_a_delivery_by_hand_runs_one_side_only() {
        // The synchronous in-loop path: hold a Delivery and settle it yourself. Settling consumes
        // it, so the other side can never also run.
        struct Case {
            name: &'static str,
            ack: bool,
            want: &'static [&'static str],
        }
        let cases = [
            Case { name: "ack runs only the ack closure", ack: true, want: &["ack"] },
            Case { name: "nack runs only the nack closure", ack: false, want: &["nack"] },
        ];
        for c in cases {
            let settled: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
            let (a, n) = (Arc::clone(&settled), Arc::clone(&settled));
            let delivery = Delivery::new(
                envelope("order-a", "Tick", 1),
                move || a.lock().unwrap().push("ack"),
                move || n.lock().unwrap().push("nack"),
            );
            if c.ack {
                delivery.ack();
            } else {
                delivery.nack();
            }
            assert_eq!(*settled.lock().unwrap(), c.want, "{}", c.name);
        }
    }

    #[test]
    fn a_panicking_settle_closure_does_not_kill_the_worker() {
        // An adapter's settle closure is the last thing that could unwind a worker. Nothing is left
        // that could settle *that* delivery, but the ones queued behind it must still be drained.
        let acked: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let consumer = KeyOrderedDispatcher::new(1, |_: &EventEnvelope| Ok(()));
        for seq in 1..=2u64 {
            let a = Arc::clone(&acked);
            consumer.deliver(Delivery::new(
                envelope("a", "Tick", seq),
                move || {
                    assert_ne!(seq, 1, "adapter bug");
                    a.lock().unwrap().push(seq);
                },
                || {},
            ));
        }
        consumer.stop();
        assert_eq!(*acked.lock().unwrap(), vec![2], "the worker kept draining past the panicking ack");
    }

    #[test]
    fn deliveries_ack_on_success_and_nack_on_error() {
        let acked: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let nacked: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));

        let consumer = KeyOrderedDispatcher::new(2, |env: &EventEnvelope| {
            // a transient downstream failure for one aggregate -> its delivery nacks for redelivery
            if env.aggregate_id == "order-b" {
                Err(Error::Store("projection unavailable".into()))
            } else {
                Ok(())
            }
        });

        for seq in 1..=3u64 {
            for agg in ["order-a", "order-b"] {
                let mid = format!("{agg}-{seq}").into_bytes();
                let (a, n) = (Arc::clone(&acked), Arc::clone(&nacked));
                let (ack_id, nack_id) = (mid.clone(), mid.clone());
                consumer.deliver(Delivery::new(
                    envelope(agg, "Tick", seq),
                    move || a.lock().unwrap().push(ack_id),
                    move || n.lock().unwrap().push(nack_id),
                ));
            }
        }
        consumer.stop();

        let mut got_ack = acked.lock().unwrap().clone();
        let mut got_nack = nacked.lock().unwrap().clone();
        got_ack.sort();
        got_nack.sort();
        let ids = |agg: &str| (1..=3).map(|s| format!("{agg}-{s}").into_bytes()).collect::<Vec<_>>();
        assert_eq!(got_ack, ids("order-a"), "successes should ack");
        assert_eq!(got_nack, ids("order-b"), "failures should nack for redelivery");
    }

    /// What the handler does on one run of the delivery under test.
    #[derive(Clone, Copy)]
    enum Outcome {
        Ok,
        Terminal,
        Transient,
        Panic,
    }

    /// What the `on_dead` hook does on each call, in order; the last entry repeats.
    #[derive(Clone, Copy)]
    enum Sink {
        Ok,
        Err,
        Panic,
    }

    #[test]
    fn dead_letter_branches() {
        // One worker, one envelope delivered once per `outcomes` entry, so the in-process attempt
        // count is exercised across redeliveries. `settled` records "<delivery>:<ack|nack>" in
        // order; a delivery missing from it was left unsettled. `dead` records the attempt each
        // `on_dead` call reported.
        struct Case {
            name: &'static str,
            outcomes: &'static [Outcome],
            broker_attempt: u32,
            max_attempts: u32,
            on_dead: Option<&'static [Sink]>,
            want_settled: &'static [&'static str],
            want_dead: &'static [u32],
        }
        let cases = [
            Case {
                name: "success acks",
                outcomes: &[Outcome::Ok],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:ack"],
                want_dead: &[],
            },
            Case {
                name: "a terminal error dead-letters on the first attempt",
                outcomes: &[Outcome::Terminal],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:ack"],
                want_dead: &[1],
            },
            Case {
                name: "a transient error nacks for redelivery",
                outcomes: &[Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack"],
                want_dead: &[],
            },
            Case {
                name: "the max_attempts ceiling dead-letters a transient error",
                outcomes: &[Outcome::Transient, Outcome::Transient, Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack", "2:nack", "3:ack"],
                want_dead: &[3],
            },
            Case {
                name: "max_attempts 0 disables the ceiling",
                outcomes: &[Outcome::Transient, Outcome::Transient, Outcome::Transient, Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 0,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack", "2:nack", "3:nack", "4:nack"],
                want_dead: &[],
            },
            Case {
                name: "max_attempts 0 still dead-letters a terminal error",
                outcomes: &[Outcome::Terminal],
                broker_attempt: 0,
                max_attempts: 0,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:ack"],
                want_dead: &[1],
            },
            Case {
                name: "a broker-supplied attempt raises the effective count",
                outcomes: &[Outcome::Transient],
                broker_attempt: 2,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:ack"],
                want_dead: &[3],
            },
            Case {
                name: "the in-process count wins over a lower broker count",
                outcomes: &[Outcome::Transient, Outcome::Transient],
                broker_attempt: 1,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack", "2:ack"],
                want_dead: &[3],
            },
            Case {
                name: "a success resets the in-process count",
                outcomes: &[Outcome::Transient, Outcome::Ok, Outcome::Transient, Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack", "2:ack", "3:nack", "4:nack"],
                want_dead: &[],
            },
            Case {
                name: "no on_dead nacks a terminal error rather than dropping it",
                outcomes: &[Outcome::Terminal],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: None,
                want_settled: &["1:nack"],
                want_dead: &[],
            },
            Case {
                name: "no on_dead nacks at the ceiling rather than dropping",
                outcomes: &[Outcome::Transient, Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 2,
                on_dead: None,
                want_settled: &["1:nack", "2:nack"],
                want_dead: &[],
            },
            Case {
                name: "on_dead returning Err leaves the delivery unsettled",
                outcomes: &[Outcome::Terminal],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Err]),
                want_settled: &[],
                want_dead: &[1],
            },
            Case {
                name: "on_dead panicking leaves the delivery unsettled",
                outcomes: &[Outcome::Terminal],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Panic]),
                want_settled: &[],
                want_dead: &[1],
            },
            Case {
                name: "a redelivery after a failed on_dead retries the sink at the same attempt",
                outcomes: &[Outcome::Transient, Outcome::Transient, Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 2,
                on_dead: Some(&[Sink::Panic, Sink::Ok]),
                want_settled: &["1:nack", "3:ack"],
                want_dead: &[2, 2],
            },
            Case {
                name: "a panicking handler is transient, not terminal",
                outcomes: &[Outcome::Panic],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack"],
                want_dead: &[],
            },
            Case {
                name: "a panicking handler counts toward the ceiling",
                outcomes: &[Outcome::Panic, Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 2,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack", "2:ack"],
                want_dead: &[2],
            },
        ];
        for c in cases {
            let settled: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let dead: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
            let runs = Arc::new(Mutex::new(0usize));
            let outcomes = c.outcomes;
            let handler = move |_: &EventEnvelope| {
                let mut runs = runs.lock().unwrap();
                let outcome = outcomes[*runs];
                *runs += 1;
                match outcome {
                    Outcome::Ok => Ok(()),
                    Outcome::Terminal => Err(Error::Codec("bad bytes".into())),
                    Outcome::Transient => Err(Error::Store("projection unavailable".into())),
                    Outcome::Panic => panic!("handler bug"),
                }
            };
            let mut builder = KeyOrderedDispatcher::builder(1, handler).max_attempts(c.max_attempts);
            if let Some(sinks) = c.on_dead {
                let dead = Arc::clone(&dead);
                builder = builder.on_dead(move |letter: DeadLetter| {
                    let mut dead = dead.lock().unwrap();
                    let sink = sinks[dead.len().min(sinks.len() - 1)];
                    dead.push(letter.attempt);
                    drop(dead);
                    assert_eq!(letter.envelope.id, "a-1", "the sink gets the envelope");
                    match sink {
                        Sink::Ok => Ok(()),
                        Sink::Err => Err(Error::Store("dead-letter sink unavailable".into())),
                        Sink::Panic => panic!("dead-letter sink bug"),
                    }
                });
            }
            let consumer = builder.build();
            for n in 1..=c.outcomes.len() {
                let (a, k) = (Arc::clone(&settled), Arc::clone(&settled));
                let mut delivery = Delivery::new(
                    envelope("a", "Tick", 1),
                    move || a.lock().unwrap().push(format!("{n}:ack")),
                    move || k.lock().unwrap().push(format!("{n}:nack")),
                );
                delivery.attempt = c.broker_attempt;
                consumer.deliver(delivery);
            }
            consumer.stop();
            assert_eq!(*settled.lock().unwrap(), c.want_settled, "{}: settlements", c.name);
            assert_eq!(*dead.lock().unwrap(), c.want_dead, "{}: on_dead attempts", c.name);
        }
    }

    #[test]
    fn dead_letters_flow_into_a_second_broker_and_per_aggregate_order_holds() {
        use crate::backend::InMemoryBroker;
        use std::time::Duration;

        // End to end through the in-memory broker: a terminal event dies into the DLQ, a transient
        // one is nacked, requeued and succeeds on redelivery, and every aggregate's deliveries are
        // handled in the order they were submitted, whatever the settlement path.
        let broker = InMemoryBroker::new();
        let dlq = InMemoryBroker::new();
        let handled: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&handled);
        let dead_sink = dlq.clone();
        let consumer = KeyOrderedDispatcher::builder(2, move |env: &EventEnvelope| {
            let mut seen = sink.lock().unwrap();
            let first_time = !seen.iter().any(|(a, s)| *a == env.aggregate_id && *s == env.sequence.unwrap());
            seen.push((env.aggregate_id.clone(), env.sequence.unwrap()));
            match (env.event.name.as_str(), first_time) {
                ("Poison", _) => Err(Error::Codec("bad bytes".into())),
                ("Flaky", true) => Err(Error::Store("projection unavailable".into())),
                _ => Ok(()),
            }
        })
        .max_attempts(3)
        .on_dead(move |letter: DeadLetter| {
            dead_sink.publish(letter.envelope);
            Ok(())
        })
        .build();

        for (agg, seq, name) in [("a", 1, "Poison"), ("b", 1, "Tick"), ("a", 2, "Tick"), ("a", 3, "Flaky"), ("b", 2, "Flaky"), ("a", 4, "Tick")] {
            broker.publish(envelope(agg, name, seq));
        }
        // Drain until the broker goes quiet. The wait bounds a lost redelivery: the loop ends when
        // no nack has landed within it.
        let mut submitted: Vec<(String, u64)> = Vec::new();
        while let Some(delivery) = broker.poll(Duration::from_millis(500)) {
            submitted.push((delivery.envelope.aggregate_id.clone(), delivery.envelope.sequence.unwrap()));
            consumer.deliver(delivery);
        }
        consumer.stop();

        let handled = handled.lock().unwrap();
        for agg in ["a", "b"] {
            let want: Vec<u64> = submitted.iter().filter(|(a, _)| a == agg).map(|(_, s)| *s).collect();
            let got: Vec<u64> = handled.iter().filter(|(a, _)| a == agg).map(|(_, s)| *s).collect();
            assert_eq!(got, want, "aggregate {agg} was handled in submission order");
        }
        assert_eq!(submitted.len(), 8, "the two flaky events were redelivered once each");
        assert!(broker.empty(), "every delivery was settled");
        let dead = dlq.poll(Duration::ZERO).expect("the poison event reached the DLQ");
        assert_eq!((dead.envelope.aggregate_id.as_str(), dead.envelope.sequence), ("a", Some(1)));
        dead.ack();
        assert!(dlq.empty(), "only the poison event died");
    }
}

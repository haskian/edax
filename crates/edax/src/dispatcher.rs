//! Ordered-parallel consumer.
//!
//! [`KeyOrderedDispatcher`] fans one input stream out to a local pool of worker threads, routing each
//! event by its ordering key, its aggregate, so all events for an aggregate land on the same worker (handled in order
//! on the success path — see [`KeyOrderedDispatcher`] for what a nack does to that order) while
//! different aggregates progress in parallel. The read-side [`crate::router::EventRouter`]
//! plugs in as the per-event handler.

use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{channel, sync_channel, Receiver, SendError, Sender, SyncSender};
use std::sync::Arc;
use std::thread::JoinHandle;

use edax_core::Error;
use edax_event::{partition, EventMessage, Failure, Message as _, Retries, Verdict};

use crate::command::{code_of, HandlerError};
use crate::pacing::Pacing;

/// A broker delivery: the event plus the handles to settle it. Composition over the stored
/// [`EventMessage`] — the ack token is per-delivery transport state (a Kafka offset, an SQS receipt,
/// a Rabbit delivery tag), not part of the event, and most are not stable across redelivery.
///
/// Build one in your broker adapter and hand it to [`KeyOrderedDispatcher::deliver`]; the worker acks on
/// success and nacks (for redelivery) when the handler errors. The settle closures run on a worker
/// thread, hence the `Send + 'static` bounds. Or hold one yourself and call [`Delivery::ack`] /
/// [`Delivery::nack`] for synchronous in-loop consumption.
pub struct Delivery {
    /// The event to handle; lent to the consumer's handler.
    pub event: EventMessage,
    /// The broker's own redelivery count when it keeps one, else `0`. It is what lets a
    /// dead-letter ceiling survive a consumer restart; see
    /// [`DispatcherBuilder::max_attempts`].
    pub attempt: u32,
    on_ack: Box<dyn FnOnce() + Send>,
    on_nack: Box<dyn FnOnce() + Send>,
    live: Box<dyn Fn() -> bool + Send>,
}

impl Delivery {
    /// A delivery that stays live until it is settled. See [`with_live`](Self::with_live) for a
    /// transport that retires deliveries it already handed out.
    pub fn new(
        event: EventMessage,
        on_ack: impl FnOnce() + Send + 'static,
        on_nack: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self { event, attempt: 0, on_ack: Box::new(on_ack), on_nack: Box::new(on_nack), live: Box::new(|| true) }
    }

    /// Ask `live` before the handler runs and before the delivery settles. A log-based transport
    /// retires every delivery it handed out from a partition it rewinds or loses — the kernel's
    /// `OffsetTracker::is_live` — and fetches those messages again. A worker skips a delivery that
    /// is no longer live: running it would apply a later event of its key ahead of the redelivered
    /// earlier one, and settling it would settle nothing.
    pub fn with_live(mut self, live: impl Fn() -> bool + Send + 'static) -> Self {
        self.live = Box::new(live);
        self
    }

    /// Whether the transport still counts on this delivery being settled.
    pub fn is_live(&self) -> bool {
        (self.live)()
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
    Error(HandlerError),
    Panic(Box<dyn Any + Send>),
}

/// A delivery the dispatcher gave up on, handed to the `on_dead` hook (see
/// [`DispatcherBuilder::on_dead`]). Owns the event the worker held, so nothing is ever copied on
/// the success path and a sink can publish it without a clone.
#[derive(Debug)]
pub struct DeadLetter {
    pub event: EventMessage,
    /// The failed attempt that killed it: `1` for a terminal error on first delivery, the
    /// configured `max_attempts` for an exhausted transient one.
    pub attempt: u32,
    pub failure: HandlerFailure,
}

type Handler = dyn Fn(&EventMessage) -> Result<(), HandlerError> + Send + Sync;
type DeadLetterHook = dyn Fn(DeadLetter) -> Result<(), HandlerError> + Send + Sync;

/// Fans one input stream out to a local pool of worker threads; `workers` is purely local parallelism. The
/// worker for an event is `partition(ordering_key, workers)`, and an event's ordering key is its aggregate, so
/// every event for an aggregate lands on the same worker and is handled in submitted order, while different aggregates run concurrently.
///
/// Ordering holds through a nack when the transport rewinds the way a log does: the nack seeks the
/// partition back, the transport retires every delivery of it already handed out, and a worker skips a
/// delivery that is no longer [`live`](Delivery::with_live), so the redelivered earlier event runs before
/// the later one. A transport that requeues a nacked delivery behind later ones instead reorders the key,
/// and a handler on it skips a stale event itself ([`is_stale`](crate::is_stale), then
/// [`advance`](crate::advance) after it applies). Every nack waits one step of the builder's [`Pacing`]
/// first, or a handler failing on a downed dependency spins the broker as fast as it redelivers. An
/// [`ErrorCode::InProgress`](crate::ErrorCode::InProgress) failure waits the same way but never counts
/// toward [`DispatcherBuilder::max_attempts`], and blocks every key on this worker until the peer's claim
/// expires.
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
    pacing: Arc<Pacing>,
    /// The worker threads' stack, `None` for the platform default. A test sets an impossible one to make
    /// a spawn fail.
    stack_size: Option<usize>,
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
    /// not nacked) when the handler error is terminal (see [`HandlerError`]), so it dies on the first failure
    /// whatever `max_attempts` says, or when the failure is transient (a transient error or a panic) and this
    /// is the `max_attempts`-th failed attempt.
    ///
    /// Without a sink a dead delivery still nacks: never a silent drop. A sink that returns `Err` or panics
    /// nacks the delivery after one backoff step, like a transient failure, so the redelivery gives the sink
    /// another try; leaving it unsettled would pin a log-based broker's commits, because nothing redelivers
    /// it while the consumer lives. Runs on a worker thread, hence the bounds.
    pub fn on_dead<F>(mut self, on_dead: F) -> Self
    where
        F: Fn(DeadLetter) -> Result<(), HandlerError> + Send + Sync + 'static,
    {
        self.on_dead = Some(Arc::new(on_dead));
        self
    }

    /// Give every worker thread after the first an impossible stack, so the platform refuses the second
    /// spawn and the first worker has to be stopped and joined.
    #[cfg(test)]
    fn stack_size_for_test(mut self, bytes: usize) -> Self {
        self.stack_size = Some(bytes);
        self
    }

    /// How a nack waits before it hands a delivery back: the backoff policy, the sleep and the jitter.
    /// Default [`Pacing::default`].
    pub fn pacing(mut self, pacing: Pacing) -> Self {
        self.pacing = Arc::new(pacing);
        self
    }

    /// Spawn `workers` worker threads, each draining its own channel through the handler.
    ///
    /// # Errors
    /// The terminal [`ErrorCode::BadHandle`](crate::ErrorCode::BadHandle) for zero workers, a wiring
    /// mistake; a store error when the platform refuses a worker thread. A dispatcher missing a worker
    /// would nack that worker's partition on every delivery with no backoff, a hot loop, so it is never
    /// handed out: the threads already spawned are stopped and joined first.
    pub fn build(self) -> Result<KeyOrderedDispatcher, Error> {
        let n = self.workers;
        if n == 0 {
            return Err(Error::bad_handle("a dispatcher needs at least one worker, got 0"));
        }
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
                on_dead: self.on_dead.clone(),
                retries: Retries::new(self.max_attempts, self.on_dead.is_some()),
                pacing: Arc::clone(&self.pacing),
            };
            senders.push(sender);
            let mut thread = std::thread::Builder::new().name(format!("edax-worker-{i}"));
            if let Some(bytes) = self.stack_size.filter(|_| i > 0) {
                thread = thread.stack_size(bytes);
            }
            match thread.spawn(move || worker.run(receiver)) {
                Ok(handle) => handles.push(handle),
                Err(cause) => {
                    // Closing every channel ends the workers already running; then wait for them.
                    drop(senders);
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(Error::store(format_args!("could not spawn dispatcher worker {i}: {cause}")));
                }
            }
        }
        Ok(KeyOrderedDispatcher { senders, workers: handles })
    }
}

impl KeyOrderedDispatcher {
    /// Spawn `workers` worker threads with the default policy: a failing handler nacks and the broker
    /// redelivers forever. See [`KeyOrderedDispatcher::builder`] to bound that.
    ///
    /// # Errors
    /// As [`DispatcherBuilder::build`].
    pub fn new<H>(workers: usize, handler: H) -> Result<Self, Error>
    where
        H: Fn(&EventMessage) -> Result<(), HandlerError> + Send + Sync + 'static,
    {
        Self::builder(workers, handler).build()
    }

    /// Start configuring a dispatcher; [`DispatcherBuilder::build`] spawns it.
    pub fn builder<H>(workers: usize, handler: H) -> DispatcherBuilder
    where
        H: Fn(&EventMessage) -> Result<(), HandlerError> + Send + Sync + 'static,
    {
        DispatcherBuilder {
            workers,
            handler: Arc::new(handler),
            max_attempts: 0,
            queue_depth: DEFAULT_QUEUE_DEPTH,
            on_dead: None,
            pacing: Arc::new(Pacing::default()),
            stack_size: None,
        }
    }

    /// Submit a bare event, fire-and-forget (no acknowledgement). Routed to its worker by its
    /// ordering key; events for one aggregate keep FIFO order.
    pub fn submit(&self, env: EventMessage) {
        // No-op settlers; ZST closures don't allocate, so this stays as cheap as a bare send.
        self.deliver(Delivery::new(env, || {}, || {}));
    }

    /// Submit a [`Delivery`]: the worker acks it when the handler returns, nacks it (for the broker
    /// to redeliver) when the handler errors or panics, and dead-letters it under the builder's
    /// policy. Same ordering-key routing and FIFO ordering. Blocks while that worker's queue is
    /// full (see [`DispatcherBuilder::queue_depth`]).
    pub fn deliver(&self, delivery: Delivery) {
        let workers = u32::try_from(self.senders.len()).unwrap_or(u32::MAX);
        let idx = usize::try_from(partition(delivery.event.ordering_key(), workers)).unwrap_or(usize::MAX);
        // The worker loop never unwinds, so the receiver lives until stop() consumes self. If it is
        // gone anyway — or the partition names no queue, which `partition`'s range rules out — nack
        // rather than strand the delivery.
        let Some(queue) = self.senders.get(idx) else {
            delivery.nack();
            return;
        };
        if let Err(SendError(delivery)) = queue.send(delivery) {
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

/// One worker thread's state. The retry policy is per worker and unlocked: an event id always
/// carries the same aggregate id, and the aggregate routes to exactly one worker for the life of
/// the dispatcher, so no other thread ever reads or writes its entry. Entries for nacked messages
/// that never come back stay until the id is seen again; bound with a TTL or LRU if a broker-side
/// DLQ steals them under sustained failure.
struct Worker {
    handler: Arc<Handler>,
    on_dead: Option<Arc<DeadLetterHook>>,
    /// The kernel's retry and dead-letter policy: when a failure is handed back, after which backoff
    /// step, and when it dies. The worker performs what it answers.
    retries: Retries,
    /// How a backoff step is waited out: the builder's policy, sleep and jitter.
    pacing: Arc<Pacing>,
}

impl Worker {
    fn run(mut self, receiver: Receiver<Delivery>) {
        // Loop ends when all senders are dropped (channel disconnected) on stop().
        for delivery in receiver {
            self.settle(delivery);
        }
    }

    /// Run the handler, then ack, nack or dead-letter the delivery as the retry policy answers. A
    /// delivery the transport retired is skipped: not run when it is dead already, not settled when it
    /// died while the handler ran. Its message comes back by a later fetch.
    fn settle(&mut self, delivery: Delivery) {
        let Delivery { event, attempt: broker_attempt, on_ack, on_nack, live } = delivery;
        if !live() {
            return;
        }
        let outcome = self.run_handler(&event);
        if !live() {
            return;
        }
        let Some(failure) = outcome else {
            self.retries.succeeded(&event.id);
            run_settler(on_ack);
            return;
        };
        // A panic carries no code, so it is unclassified and transient: a bug on one payload is
        // exactly the poison message the ceiling exists for, but a single panic cannot be told from a
        // transient one. The kernel reads the code: another consumer holding the key's claim is
        // contention, not a failed attempt, so it never dead-letters a message whose holder is slow.
        let code = match &failure {
            HandlerFailure::Error(e) => code_of(&**e),
            HandlerFailure::Panic(_) => None,
        };
        let id = event.id.clone();
        match self.retries.failed(&id, broker_attempt, Failure::of(code)) {
            Verdict::Nack { step } => self.nack_after_backoff(step, on_nack),
            Verdict::Dead { attempt } => {
                // The sink runs before any ack. If it errors or panics, the delivery is nacked after
                // one backoff step, the transient path: a log-based broker never redelivers a
                // delivery left unsettled while its consumer lives, so leaving it would pin the
                // partition's commits. The policy keeps the attempt, so the redelivery dead-letters
                // again and the sink gets another try.
                let dead = DeadLetter { event, attempt, failure };
                let sunk = self.on_dead.as_ref().is_some_and(|on_dead| matches!(catch_unwind(AssertUnwindSafe(|| on_dead(dead))), Ok(Ok(()))));
                if sunk {
                    run_settler(on_ack);
                    self.retries.dead_lettered(&id);
                } else {
                    let step = self.retries.sink_failed(&id, attempt);
                    self.nack_after_backoff(step, on_nack);
                }
            }
        }
    }

    /// The handler's verdict: `None` on success, otherwise how it failed. A panic must not kill the
    /// worker — the deliveries queued behind it would be dropped unsettled and the partition would
    /// stall. The default panic hook has already printed it. Redelivery ordering against later
    /// same-aggregate messages already acked is the broker's contract, not ours.
    fn run_handler(&self, event: &EventMessage) -> Option<HandlerFailure> {
        match catch_unwind(AssertUnwindSafe(|| (self.handler)(event))) {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(HandlerFailure::Error(e)),
            Err(payload) => Some(HandlerFailure::Panic(payload)),
        }
    }

    /// Wait one step of the backoff schedule, then nack. Every failure arm waits: nacking at once has
    /// a handler on a downed dependency spinning the broker as fast as it redelivers, and on a seeking
    /// transport every nack re-reads the partition tail. The wait blocks this worker, and therefore
    /// every key routed to it — per-key ordering forbids skipping ahead.
    fn nack_after_backoff(&self, step: u64, on_nack: Box<dyn FnOnce() + Send>) {
        self.pacing.wait(step);
        run_settler(on_nack);
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
    use crate::{Error, ErrorCode};
    use crate::testkit::event;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// The kernel's wait for backoff step `step` under [`Pacing::recording`]'s draw.
    fn kernel_wait(step: u64) -> Duration {
        Pacing::recording().0.delay(step)
    }

    /// A bounded queue is the whole point of `queue_depth`: once the worker is busy and the one
    /// buffered slot is taken, the next `deliver` waits for the handler instead of queueing without
    /// limit.
    #[test]
    fn deliver_blocks_while_the_workers_queue_is_full() {
        let (entered, has_entered) = std::sync::mpsc::channel::<()>();
        let (release, may_return) = std::sync::mpsc::channel::<()>();
        let may_return = Mutex::new(may_return);
        let dispatcher = KeyOrderedDispatcher::builder(1, move |_: &EventMessage| {
            let _ = entered.send(());
            // Each handler run consumes one token, so the test decides when a worker frees a slot.
            let _ = may_return.lock().expect("no handler panics while holding the gate").recv();
            Ok(())
        })
        .queue_depth(1)
        .build()
        .expect("the dispatcher's workers spawn");

        let deliver = |seq: u64| Delivery::new(event("order-a", "Tick", seq), || {}, || {});
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
    fn build_branches() {
        // Zero workers is a wiring mistake, and a worker the platform refuses would leave its partition
        // nacked on every delivery with no backoff: neither dispatcher is handed out.
        let zero = KeyOrderedDispatcher::new(0, |_: &EventMessage| Ok(()));
        assert!(matches!(&zero, Err(e) if e.code == ErrorCode::BadHandle && e.message.contains("got 0")), "zero workers");
        let refused = KeyOrderedDispatcher::builder(2, |_: &EventMessage| Ok(()))
            .stack_size_for_test(usize::MAX)
            .build();
        assert!(matches!(&refused, Err(e) if e.code == ErrorCode::Store && e.message.starts_with("could not spawn dispatcher worker 1")), "spawn refused");
        let built = KeyOrderedDispatcher::new(2, |_: &EventMessage| Ok(())).expect("two workers spawn");
        built.stop();
    }

    #[test]
    fn key_ordered_dispatch_preserves_per_aggregate_order() {
        let collected: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&collected);
        let consumer = KeyOrderedDispatcher::new(2, move |env: &EventMessage| {
            sink.lock().unwrap().push((env.aggregate_id.clone(), env.sequence.unwrap()));
            Ok(())
        }).expect("the dispatcher's workers spawn");

        // Interleave two aggregates' events; each aggregate's events are submitted in order.
        for seq in 1..=4u64 {
            consumer.submit(event("order-a", "Tick", seq));
            consumer.submit(event("order-b", "Tick", seq));
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
        let consumer = KeyOrderedDispatcher::new(1, |env: &EventMessage| {
            if env.name == "Boom" {
                panic!("handler bug");
            }
            Ok(())
        }).expect("the dispatcher's workers spawn");

        for (seq, name) in [(1, "Boom"), (2, "Tick"), (3, "Tick")] {
            let (a, n) = (Arc::clone(&acked), Arc::clone(&nacked));
            let (ack_id, nack_id) = (format!("a-{seq}"), format!("a-{seq}"));
            consumer.deliver(Delivery::new(
                event("a", name, seq),
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
                event("order-a", "Tick", 1),
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
        let consumer = KeyOrderedDispatcher::new(1, |_: &EventMessage| Ok(())).expect("the dispatcher's workers spawn");
        for seq in 1..=2u64 {
            let a = Arc::clone(&acked);
            consumer.deliver(Delivery::new(
                event("a", "Tick", seq),
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

        let consumer = KeyOrderedDispatcher::new(2, |env: &EventMessage| {
            // a transient downstream failure for one aggregate -> its delivery nacks for redelivery
            if env.aggregate_id == "order-b" {
                Err(Error::store("projection unavailable").into())
            } else {
                Ok(())
            }
        }).expect("the dispatcher's workers spawn");

        for seq in 1..=3u64 {
            for agg in ["order-a", "order-b"] {
                let mid = format!("{agg}-{seq}").into_bytes();
                let (a, n) = (Arc::clone(&acked), Arc::clone(&nacked));
                let (ack_id, nack_id) = (mid.clone(), mid.clone());
                consumer.deliver(Delivery::new(
                    event(agg, "Tick", seq),
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
        InProgress,
        CachedFailure,
        Rejected,
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
        // One worker, one event delivered once per `outcomes` entry, so the in-process attempt
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
            /// Backoff steps waited, in order. A nack costs one step of the kernel's schedule; an
            /// ack and a dead-letter cost none. An in-progress nack waits its own contention step.
            want_slept: &'static [u64],
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
                want_slept: &[],
            },
            Case {
                name: "a terminal error dead-letters on the first attempt",
                outcomes: &[Outcome::Terminal],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:ack"],
                want_dead: &[1],
                want_slept: &[],
            },
            Case {
                name: "a transient error nacks for redelivery",
                outcomes: &[Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack"],
                want_dead: &[],
                want_slept: &[0],
            },
            Case {
                name: "the max_attempts ceiling dead-letters a transient error",
                outcomes: &[Outcome::Transient, Outcome::Transient, Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack", "2:nack", "3:ack"],
                want_dead: &[3],
                want_slept: &[0, 1],
            },
            Case {
                name: "max_attempts 0 disables the ceiling",
                outcomes: &[Outcome::Transient, Outcome::Transient, Outcome::Transient, Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 0,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack", "2:nack", "3:nack", "4:nack"],
                want_dead: &[],
                want_slept: &[0, 1, 2, 3],
            },
            Case {
                name: "max_attempts 0 still dead-letters a terminal error",
                outcomes: &[Outcome::Terminal],
                broker_attempt: 0,
                max_attempts: 0,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:ack"],
                want_dead: &[1],
                want_slept: &[],
            },
            Case {
                name: "a broker-supplied attempt raises the effective count",
                outcomes: &[Outcome::Transient],
                broker_attempt: 2,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:ack"],
                want_dead: &[3],
                want_slept: &[],
            },
            Case {
                name: "the in-process count wins over a lower broker count",
                outcomes: &[Outcome::Transient, Outcome::Transient],
                broker_attempt: 1,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack", "2:ack"],
                want_dead: &[3],
                want_slept: &[1],
            },
            Case {
                name: "a success resets the in-process count",
                outcomes: &[Outcome::Transient, Outcome::Ok, Outcome::Transient, Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack", "2:ack", "3:nack", "4:nack"],
                want_dead: &[],
                want_slept: &[0, 0, 1],
            },
            Case {
                name: "no on_dead nacks a terminal error rather than dropping it",
                outcomes: &[Outcome::Terminal],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: None,
                want_settled: &["1:nack"],
                want_dead: &[],
                want_slept: &[0],
            },
            Case {
                name: "no on_dead nacks at the ceiling rather than dropping",
                outcomes: &[Outcome::Transient, Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 2,
                on_dead: None,
                want_settled: &["1:nack", "2:nack"],
                want_dead: &[],
                want_slept: &[0, 1],
            },
            Case {
                // A log-based broker never redelivers an unsettled delivery while its consumer
                // lives, so a failed sink nacks through the backoff path instead of pinning commits.
                name: "on_dead returning Err nacks after a backoff step",
                outcomes: &[Outcome::Terminal],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Err]),
                want_settled: &["1:nack"],
                want_dead: &[1],
                want_slept: &[0],
            },
            Case {
                name: "on_dead panicking nacks after a backoff step",
                outcomes: &[Outcome::Terminal],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Panic]),
                want_settled: &["1:nack"],
                want_dead: &[1],
                want_slept: &[0],
            },
            Case {
                // The failed sink's attempt is recorded like a failed handler's, so the redelivery
                // dead-letters again at once, the sink sees the true count of handler runs, and the
                // backoff grows with it.
                name: "a redelivery after a failed on_dead retries the sink at the next attempt",
                outcomes: &[Outcome::Transient, Outcome::Transient, Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 2,
                on_dead: Some(&[Sink::Panic, Sink::Ok]),
                want_settled: &["1:nack", "2:nack", "3:ack"],
                want_dead: &[2, 3],
                want_slept: &[0, 1],
            },
            Case {
                name: "a sink failing on a terminal error keeps the first attempt and dead-letters next time",
                outcomes: &[Outcome::Terminal, Outcome::Terminal],
                broker_attempt: 0,
                max_attempts: 0,
                on_dead: Some(&[Sink::Err, Sink::Ok]),
                want_settled: &["1:nack", "2:ack"],
                want_dead: &[1, 2],
                want_slept: &[0],
            },
            Case {
                name: "a panicking handler is transient, not terminal",
                outcomes: &[Outcome::Panic],
                broker_attempt: 0,
                max_attempts: 3,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack"],
                want_dead: &[],
                want_slept: &[0],
            },
            Case {
                name: "a panicking handler counts toward the ceiling",
                outcomes: &[Outcome::Panic, Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 2,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack", "2:ack"],
                want_dead: &[2],
                want_slept: &[0],
            },
            Case {
                // Contention, not a failed attempt: it waits its own step and never counts, so a slow
                // peer cannot push a message to the ceiling.
                name: "in progress nacks without spending the budget",
                outcomes: &[Outcome::InProgress, Outcome::InProgress, Outcome::Transient, Outcome::Transient],
                broker_attempt: 0,
                max_attempts: 2,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:nack", "2:nack", "3:nack", "4:ack"],
                want_dead: &[2],
                want_slept: &[0, 1, 0],
            },
            Case {
                // A saga or a command dispatched inside the handler fails with the command path's
                // error; a cached terminal failure keeps its verdict and dies at once.
                name: "a replayed cached failure dead-letters on the first attempt",
                outcomes: &[Outcome::CachedFailure],
                broker_attempt: 0,
                max_attempts: 5,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:ack"],
                want_dead: &[1],
                want_slept: &[],
            },
            Case {
                name: "a host rejection marked terminal dead-letters on the first attempt",
                outcomes: &[Outcome::Rejected],
                broker_attempt: 0,
                max_attempts: 5,
                on_dead: Some(&[Sink::Ok]),
                want_settled: &["1:ack"],
                want_dead: &[1],
                want_slept: &[],
            },
        ];
        for c in cases {
            let settled: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let dead: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
            let runs = Arc::new(Mutex::new(0usize));
            let outcomes = c.outcomes;
            let handler = move |_: &EventMessage| {
                let mut runs = runs.lock().unwrap();
                let outcome = outcomes[*runs];
                *runs += 1;
                match outcome {
                    Outcome::Ok => Ok(()),
                    Outcome::Terminal => Err(Error::codec("bad bytes").into()),
                    Outcome::Transient => Err(Error::store("projection unavailable").into()),
                    Outcome::Panic => panic!("handler bug"),
                    Outcome::InProgress => Err(Error::new(ErrorCode::InProgress, "a-1").into()),
                    Outcome::CachedFailure => Err(Error::new(ErrorCode::CachedFailure, "codec: bad bytes").into()),
                    Outcome::Rejected => Err(crate::Terminal::new("order already shipped").into()),
                }
            };
            let (pacing, slept) = Pacing::recording();
            let mut builder = KeyOrderedDispatcher::builder(1, handler).max_attempts(c.max_attempts).pacing(pacing);
            if let Some(sinks) = c.on_dead {
                let dead = Arc::clone(&dead);
                builder = builder.on_dead(move |letter: DeadLetter| {
                    let mut dead = dead.lock().unwrap();
                    let sink = sinks[dead.len().min(sinks.len() - 1)];
                    dead.push(letter.attempt);
                    drop(dead);
                    assert_eq!(letter.event.id, "a-1", "the sink gets the event");
                    match sink {
                        Sink::Ok => Ok(()),
                        Sink::Err => Err(Error::store("dead-letter sink unavailable").into()),
                        Sink::Panic => panic!("dead-letter sink bug"),
                    }
                });
            }
            let consumer = builder.build().expect("the dispatcher's workers spawn");
            for n in 1..=c.outcomes.len() {
                let (a, k) = (Arc::clone(&settled), Arc::clone(&settled));
                let mut delivery = Delivery::new(
                    event("a", "Tick", 1),
                    move || a.lock().unwrap().push(format!("{n}:ack")),
                    move || k.lock().unwrap().push(format!("{n}:nack")),
                );
                delivery.attempt = c.broker_attempt;
                consumer.deliver(delivery);
            }
            consumer.stop();
            assert_eq!(*settled.lock().unwrap(), c.want_settled, "{}: settlements", c.name);
            assert_eq!(*dead.lock().unwrap(), c.want_dead, "{}: on_dead attempts", c.name);
            let want_slept: Vec<Duration> = c.want_slept.iter().map(|&step| kernel_wait(step)).collect();
            assert_eq!(*slept.lock().unwrap(), want_slept, "{}: backoff steps", c.name);
        }
    }

    #[test]
    fn liveness_branches() {
        use std::sync::atomic::{AtomicBool, Ordering};
        // `dies` says when the transport retires the delivery: never, before the worker takes it,
        // or while the handler runs (a rewind from another key's nack, a rebalance).
        struct Case {
            name: &'static str,
            dies: Option<&'static str>,
            fails: bool,
            want_runs: usize,
            want_settled: &'static [&'static str],
        }
        let cases = [
            Case { name: "a live delivery runs and acks", dies: None, fails: false, want_runs: 1, want_settled: &["ack"] },
            Case { name: "a live failing delivery nacks", dies: None, fails: true, want_runs: 1, want_settled: &["nack"] },
            Case { name: "a dead delivery is skipped: no run, no settlement", dies: Some("before"), fails: false, want_runs: 0, want_settled: &[] },
            Case { name: "a delivery that died mid-handler is not acked", dies: Some("during"), fails: false, want_runs: 1, want_settled: &[] },
            Case { name: "a delivery that died mid-handler is not nacked either", dies: Some("during"), fails: true, want_runs: 1, want_settled: &[] },
        ];
        for c in cases {
            let live = Arc::new(AtomicBool::new(c.dies != Some("before")));
            let runs = Arc::new(Mutex::new(0usize));
            let settled: Arc<Mutex<Vec<&'static str>>> = Arc::default();
            let (h_live, h_runs, fails, dies) = (Arc::clone(&live), Arc::clone(&runs), c.fails, c.dies);
            let consumer = KeyOrderedDispatcher::builder(1, move |_: &EventMessage| {
                *h_runs.lock().unwrap() += 1;
                if dies == Some("during") {
                    h_live.store(false, Ordering::SeqCst);
                }
                if fails {
                    return Err(Error::store("projection unavailable").into());
                }
                Ok(())
            })
            .pacing(Pacing::recording().0)
            .build()
            .expect("the dispatcher's workers spawn");
            let (a, n, l) = (Arc::clone(&settled), Arc::clone(&settled), Arc::clone(&live));
            let delivery = Delivery::new(event("a", "Tick", 1), move || a.lock().unwrap().push("ack"), move || n.lock().unwrap().push("nack"))
                .with_live(move || l.load(Ordering::SeqCst));
            consumer.deliver(delivery);
            consumer.stop();
            assert_eq!(*runs.lock().unwrap(), c.want_runs, "{}: handler runs", c.name);
            assert_eq!(*settled.lock().unwrap(), c.want_settled, "{}: settlements", c.name);
        }
    }

    #[test]
    fn dead_letters_flow_into_a_second_broker_and_per_aggregate_order_holds() {
        use crate::backend::MemoryBroker;
        use std::time::Duration;

        // End to end through the in-memory broker: a terminal event dies into the DLQ, a transient
        // one is nacked, the partition rewinds and it succeeds on redelivery, and every aggregate's
        // events are handled in order, whatever the settlement path: a copy retired by the rewind
        // is skipped, never run ahead of the redelivered earlier event.
        let broker = MemoryBroker::new();
        let dlq = MemoryBroker::new();
        let handled: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&handled);
        let dead_sink = dlq.clone();
        let consumer = KeyOrderedDispatcher::builder(2, move |env: &EventMessage| {
            let mut seen = sink.lock().unwrap();
            let first_time = !seen.iter().any(|(a, s)| *a == env.aggregate_id && *s == env.sequence.unwrap());
            seen.push((env.aggregate_id.clone(), env.sequence.unwrap()));
            match (env.name.as_str(), first_time) {
                ("Poison", _) => Err(Error::codec("bad bytes").into()),
                ("Flaky", true) => Err(Error::store("projection unavailable").into()),
                _ => Ok(()),
            }
        })
        .max_attempts(3)
        .on_dead(move |letter: DeadLetter| {
            dead_sink.publish(letter.event);
            Ok(())
        })
        .pacing(Pacing::recording().0)
        .build()
        .expect("the dispatcher's workers spawn");

        for (agg, seq, name) in [("a", 1, "Poison"), ("b", 1, "Tick"), ("a", 2, "Tick"), ("a", 3, "Flaky"), ("b", 2, "Flaky"), ("a", 4, "Tick")] {
            broker.publish(event(agg, name, seq));
        }
        // Drain until the broker goes quiet. The wait bounds a lost redelivery: the loop ends when
        // no nack has landed within it.
        while let Some(delivery) = broker.poll(Duration::from_millis(500)).expect("poll") {
            consumer.deliver(delivery);
        }
        consumer.stop();

        let handled = handled.lock().unwrap();
        for (agg, last) in [("a", 4), ("b", 2)] {
            let got: Vec<u64> = handled.iter().filter(|(a, _)| a == agg).map(|(_, s)| *s).collect();
            // A rewind re-runs what was already handled, so a sequence may repeat, but a handler
            // never sees one before every lower one of its aggregate.
            for (i, seq) in got.iter().enumerate() {
                assert!((1..*seq).all(|lower| got[..i].contains(&lower)), "aggregate {agg} handled {seq} early: {got:?}");
            }
            assert_eq!(got.iter().max(), Some(&last), "aggregate {agg} reached its last event");
        }
        assert!(broker.empty(), "every delivery was settled");
        // Dead-lettering is at-least-once like every other settlement: a rewind of the partition (b-2's
        // nack) can retire the poison's delivery after its sink ran and before its ack landed, so the
        // redelivered copy dies again. The DLQ may hold the poison more than once, and nothing else.
        let mut dead = Vec::new();
        while let Some(letter) = dlq.poll(Duration::ZERO).expect("poll") {
            dead.push((letter.event.aggregate_id.clone(), letter.event.sequence));
            letter.ack();
        }
        assert!(!dead.is_empty(), "the poison event reached the DLQ");
        assert!(dead.iter().all(|d| d == &("a".to_owned(), Some(1))), "only the poison event died: {dead:?}");
        assert!(dlq.empty());
    }
}

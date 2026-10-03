//! Dev/test in-memory broker, single process, not durable. A thin wrapper over the shared
//! `edax_memory::MemoryBroker` state machine (a partitioned log, consumer groups, settlement through
//! the Kafka kernel's offset tracker). The machine's poll is non-blocking, so this module adds a
//! `Condvar` and [`MemoryBroker::poll`] can block up to a timeout.

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use edax_core::lock;
use edax_event::EventEnvelope;

use crate::dispatcher::Delivery;
use crate::Error;

/// The consumer group a broker built with [`MemoryBroker::new`] reads as.
const DEFAULT_GROUP: &str = "default";

/// Both producer and consumer over one in-memory log, so a [`Relay`](crate::Relay) publishing into it
/// and a [`KeyOrderedDispatcher`](crate::KeyOrderedDispatcher) consuming from it talk without a broker.
/// `Clone` shares the log and the group, so one instance can be the relay's sink, another the consumer,
/// a third a dead-letter sink. [`group`](Self::group) shares the log under another consumer group, which
/// reads every message on its own.
///
/// It behaves like a Kafka topic, so a handler that only survives a queue fails here first: a nack
/// rewinds the partition and redelivers the nacked message and every later one, acked or not; the
/// deliveries handed out before the rewind are retired, and the dispatcher skips them
/// ([`Delivery::with_live`]); [`crash`](Self::crash) redelivers everything above the committed
/// watermark. A delivery stays unsettled until it is acked or nacked, so [`empty`](Self::empty) reports
/// the same "nothing left to do" a durable broker does. Nothing survives the process.
#[derive(Clone)]
pub struct MemoryBroker {
    inner: Arc<Inner>,
    group: Arc<str>,
}

impl Default for MemoryBroker {
    fn default() -> Self {
        Self::over(edax_memory::MemoryBroker::new())
    }
}

struct Inner {
    machine: edax_memory::MemoryBroker,
    /// The lock the waiting side holds from its empty peek into `queued.wait`. A publisher takes it
    /// before notifying, so a message landing between a poller's peek and its wait cannot slip by
    /// unannounced: the publisher either sees the poller already waiting, or the poller's peek sees
    /// the message.
    wait: Mutex<()>,
    /// Signals a fetchable message to a blocked [`MemoryBroker::poll`].
    queued: Condvar,
}

impl Inner {
    /// Wake every blocked poller: a message for one group is a message for every group. Called after
    /// the machine changed, holding the wait lock across the notify (see `wait`).
    fn announce(&self) {
        let _guard = lock(&self.wait);
        self.queued.notify_all();
    }
}

impl MemoryBroker {
    /// A broker with one partition, read as the default consumer group.
    pub fn new() -> Self {
        Self::default()
    }

    /// A broker with `partitions` partitions, read as the default consumer group; a key lands on
    /// Kafka's partition for it. `0` is a `codec` error.
    pub fn with_partitions(partitions: u32) -> Result<Self, Error> {
        Ok(Self::over(edax_memory::MemoryBroker::with_partitions(partitions)?))
    }

    fn over(machine: edax_memory::MemoryBroker) -> Self {
        let inner = Inner { machine, wait: Mutex::new(()), queued: Condvar::new() };
        Self { inner: Arc::new(inner), group: Arc::from(DEFAULT_GROUP) }
    }

    /// The same log, read as consumer group `name`: it gets every message, settles on its own, and
    /// starts at the beginning of the log.
    pub fn group(&self, name: &str) -> Self {
        Self { inner: Arc::clone(&self.inner), group: Arc::from(name) }
    }

    /// Append an envelope to its key's partition.
    pub fn publish(&self, env: EventEnvelope) {
        self.inner.machine.publish(env);
        self.inner.announce();
    }

    /// Take the next message for this group as a [`Delivery`], waiting up to `timeout` for one.
    /// `Ok(None)` when the timeout elapses first; `Duration::ZERO` makes it a non-blocking peek. A
    /// `timeout` the clock cannot represent (`Duration::MAX`) waits without a deadline.
    /// `Delivery::attempt` is always `0`: this broker keeps no redelivery count.
    pub fn poll(&self, timeout: Duration) -> Result<Option<Delivery>, Error> {
        // `None` is "no deadline": adding the timeout would overflow the clock, which is further
        // out than any caller can wait for anyway.
        let deadline = Instant::now().checked_add(timeout);
        let mut guard = lock(&self.inner.wait);
        let polled = loop {
            if let Some(polled) = self.inner.machine.try_poll(&self.group)? {
                break polled;
            }
            guard = match deadline {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Ok(None);
                    }
                    self.inner
                        .queued
                        .wait_timeout(guard, remaining)
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .0
                }
                None => self.inner.queued.wait(guard).unwrap_or_else(|poisoned| poisoned.into_inner()),
            };
        };
        drop(guard);

        let token = polled.token;
        let (ack, nack, live) = (self.clone(), self.clone(), self.clone());
        let delivery = Delivery::new(
            polled.envelope,
            move || {
                // The settle result is not needed: `Delivery` is consumed on settle, so it cannot
                // settle twice, and a stale one settles nothing by the machine's own rule.
                ack.inner.machine.ack(&ack.group, token);
            },
            move || {
                // The rewind makes messages fetchable again, so a parked poller must hear of it.
                nack.inner.machine.nack(&nack.group, token);
                nack.inner.announce();
            },
        )
        .with_live(move || live.inner.machine.is_live(&live.group, token));
        Ok(Some(delivery))
    }

    /// Simulate the death of this group's consumer, or a rebalance: every delivery it holds is
    /// retired, and every message above the committed watermark is fetched again.
    pub fn crash(&self) {
        self.inner.machine.crash(&self.group);
        self.inner.announce();
    }

    /// True only when this group has committed past every message: nothing queued, nothing in flight.
    pub fn empty(&self) -> bool {
        self.inner.machine.is_empty(&self.group)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edax_event::Event;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use crate::{advance, is_stale, KeyOrderedDispatcher, Pacing};

    fn envelope(aggregate_id: &str, sequence: u64) -> EventEnvelope {
        EventEnvelope {
            id: format!("{aggregate_id}-{sequence}"),
            aggregate_id: aggregate_id.to_string(),
            event: Event { name: "Tick".to_string(), version: 1, data: Vec::new() },
            sequence: Some(sequence),
            namespace: Default::default(),
            metadata: Default::default(),
            timestamp: 0,
        }
    }

    const WAIT: Duration = Duration::from_millis(20);

    fn poll(broker: &MemoryBroker, timeout: Duration) -> Option<Delivery> {
        broker.poll(timeout).expect("a poll never fails here")
    }

    /// One step of a scenario. `Poll` pushes onto a list of held deliveries; the settle steps take
    /// the i-th one.
    enum Step {
        Publish(&'static str, u64),
        Poll,
        Ack(usize),
        Nack(usize),
        Drop(usize),
        Live(usize),
        Crash,
    }

    /// The wrapper end to end: every branch of the shared machine is reachable through a
    /// `Delivery`, and a settlement through the closures lands in the machine.
    #[test]
    fn settlement_branches() {
        use Step::*;
        struct Case {
            name: &'static str,
            steps: &'static [Step],
            /// Envelope ids that `Poll` produced, in order; `""` for a timed-out poll.
            want_polled: &'static [&'static str],
            want_live: &'static [bool],
            want_empty: bool,
        }
        let cases = [
            Case { name: "a new broker is empty and a poll times out", steps: &[Poll], want_polled: &[""], want_live: &[], want_empty: true },
            Case { name: "a published envelope makes it non-empty", steps: &[Publish("a", 1)], want_polled: &[], want_live: &[], want_empty: false },
            Case {
                name: "a polled delivery is unsettled, so still non-empty",
                steps: &[Publish("a", 1), Poll, Live(0)],
                want_polled: &["a-1"],
                want_live: &[true],
                want_empty: false,
            },
            Case { name: "ack settles it", steps: &[Publish("a", 1), Poll, Ack(0)], want_polled: &["a-1"], want_live: &[], want_empty: true },
            Case {
                name: "a nack rewinds: the nacked message and the later one come back, in order",
                steps: &[Publish("a", 1), Publish("a", 2), Poll, Poll, Ack(1), Nack(0), Poll, Poll],
                want_polled: &["a-1", "a-2", "a-1", "a-2"],
                want_live: &[],
                want_empty: false,
            },
            Case {
                name: "a nack retires the later delivery already handed out",
                steps: &[Publish("a", 1), Publish("a", 2), Poll, Poll, Nack(0), Live(1)],
                want_polled: &["a-1", "a-2"],
                want_live: &[false],
                want_empty: false,
            },
            Case {
                name: "a nacked then acked delivery is settled",
                steps: &[Publish("a", 1), Poll, Nack(0), Poll, Ack(1)],
                want_polled: &["a-1", "a-1"],
                want_live: &[],
                want_empty: true,
            },
            Case {
                name: "a dropped delivery is never settled",
                steps: &[Publish("a", 1), Poll, Drop(0)],
                want_polled: &["a-1"],
                want_live: &[],
                want_empty: false,
            },
            Case {
                name: "a crash redelivers what was not committed and retires what was held",
                steps: &[Publish("a", 1), Publish("a", 2), Poll, Poll, Ack(0), Crash, Live(1), Poll],
                want_polled: &["a-1", "a-2", "a-2"],
                want_live: &[false],
                want_empty: false,
            },
            Case {
                name: "the same id published twice needs two settlements",
                steps: &[Publish("a", 1), Publish("a", 1), Poll, Poll, Ack(0)],
                want_polled: &["a-1", "a-1"],
                want_live: &[],
                want_empty: false,
            },
            Case {
                name: "one partition keeps one order across keys",
                steps: &[Publish("a", 1), Publish("b", 1), Publish("a", 2), Poll, Poll, Poll],
                want_polled: &["a-1", "b-1", "a-2"],
                want_live: &[],
                want_empty: false,
            },
        ];
        for c in cases {
            let broker = MemoryBroker::new();
            let mut held: Vec<Option<Delivery>> = Vec::new();
            let mut polled: Vec<String> = Vec::new();
            let mut live: Vec<bool> = Vec::new();
            for step in c.steps {
                match step {
                    Publish(agg, seq) => broker.publish(envelope(agg, *seq)),
                    Poll => match poll(&broker, WAIT) {
                        Some(d) => {
                            polled.push(d.envelope.id.clone());
                            held.push(Some(d));
                        }
                        None => polled.push(String::new()),
                    },
                    Ack(i) => held[*i].take().expect("a held delivery").ack(),
                    Nack(i) => held[*i].take().expect("a held delivery").nack(),
                    Drop(i) => drop(held[*i].take()),
                    Live(i) => live.push(held[*i].as_ref().expect("a held delivery").is_live()),
                    Crash => broker.crash(),
                }
            }
            assert_eq!(polled, c.want_polled, "{}: polled ids", c.name);
            assert_eq!(live, c.want_live, "{}: liveness", c.name);
            assert_eq!(broker.empty(), c.want_empty, "{}: empty()", c.name);
        }
    }

    #[test]
    fn a_group_reads_every_message_on_its_own() {
        let projection = MemoryBroker::with_partitions(2).expect("two partitions");
        let saga = projection.group("saga");
        projection.publish(envelope("a", 1));
        let first = poll(&projection, WAIT).expect("the projection gets it");
        let second = poll(&saga, WAIT).expect("the saga gets it too");
        assert_eq!((first.envelope.id.as_str(), second.envelope.id.as_str()), ("a-1", "a-1"));
        first.ack();
        assert!(projection.empty(), "the projection settled its copy");
        assert!(!saga.empty(), "the saga's copy is its own");
        second.ack();
        assert!(saga.empty());
        assert!(MemoryBroker::with_partitions(0).is_err(), "zero partitions is refused");
    }

    #[test]
    fn a_clone_shares_the_log_and_a_blocked_poll_wakes_on_publish() {
        // The dead-letter use: the hook holds a clone on a worker thread and publishes into it,
        // while the test polls the original. The poll blocks until the publish lands.
        let broker = MemoryBroker::new();
        let sink = broker.clone();
        let publisher = std::thread::spawn(move || sink.publish(envelope("a", 1)));
        let delivery = poll(&broker, Duration::from_secs(2)).expect("the publish woke the poll");
        publisher.join().expect("publisher thread");
        assert_eq!(delivery.envelope.id, "a-1");
        assert_eq!(delivery.attempt, 0, "this broker keeps no redelivery count");
        delivery.ack();
        assert!(broker.empty(), "the clone and the original share one log");
    }

    #[test]
    fn a_blocked_poll_wakes_on_nack_and_on_crash() {
        for wake in ["nack", "crash"] {
            let broker = MemoryBroker::new();
            broker.publish(envelope("a", 1));
            let first = poll(&broker, WAIT).expect("queued");

            let consumer = broker.clone();
            let poller = std::thread::spawn(move || consumer.poll(Duration::from_secs(2)));
            std::thread::sleep(WAIT); // let the poller park before the rewind
            if wake == "nack" {
                first.nack();
            } else {
                broker.crash();
            }
            let again = poller.join().expect("poller thread").expect("poll").expect("the rewind woke the poll");
            assert_eq!(again.envelope.id, "a-1", "{wake}");
            again.ack();
            assert!(broker.empty(), "{wake}");
        }
    }

    #[test]
    fn an_unrepresentable_timeout_waits_without_a_deadline() {
        // `Instant::now() + Duration::MAX` overflows and panics, so the deadline must be absent
        // instead: the poll parks until a publish wakes it.
        let broker = MemoryBroker::new();
        let sink = broker.clone();
        let publisher = std::thread::spawn(move || {
            std::thread::sleep(WAIT); // let the poller park before the publish lands
            sink.publish(envelope("a", 1));
        });
        let delivery = poll(&broker, Duration::MAX).expect("the publish woke the unbounded wait");
        publisher.join().expect("publisher thread");
        assert_eq!(delivery.envelope.id, "a-1");
        delivery.ack();
        assert!(broker.empty());
    }

    #[test]
    fn a_zero_timeout_is_a_non_blocking_peek() {
        let broker = MemoryBroker::new();
        let started = Instant::now();
        assert!(poll(&broker, Duration::ZERO).is_none());
        assert!(started.elapsed() < Duration::from_millis(50), "returned at once");
        broker.publish(envelope("a", 1));
        assert!(poll(&broker, Duration::ZERO).is_some(), "a queued message is taken without waiting");
    }

    /// Seq 1 fails once, and the staleness guard must not lose it. A nack that requeued seq 1 behind
    /// seq 2 would apply seq 2 first, and the redelivered seq 1 would then be skipped as stale for
    /// good.
    #[test]
    fn a_nack_keeps_per_key_order_so_the_staleness_guard_loses_nothing() {
        let stored: Arc<Mutex<Option<u64>>> = Arc::default();
        let applied: Arc<Mutex<Vec<u64>>> = Arc::default();
        let failed_once = Arc::new(AtomicBool::new(false));
        let (st, ap, f) = (Arc::clone(&stored), Arc::clone(&applied), Arc::clone(&failed_once));
        let dispatcher = KeyOrderedDispatcher::builder(1, move |env: &EventEnvelope| {
            if env.sequence == Some(1) && !f.swap(true, Ordering::SeqCst) {
                return Err(crate::Error::store("projection db blip").into());
            }
            let mut s = st.lock().expect("lock");
            if !is_stale(*s, env.sequence) {
                ap.lock().expect("lock").push(env.sequence.expect("sequenced"));
                *s = advance(*s, env.sequence);
            }
            Ok(())
        })
        .pacing(Pacing::instant())
        .build()
        .expect("the dispatcher's workers spawn");
        let broker = MemoryBroker::new();
        broker.publish(envelope("acct-1", 1));
        broker.publish(envelope("acct-1", 2));
        while let Some(d) = poll(&broker, Duration::from_millis(200)) {
            dispatcher.deliver(d);
        }
        dispatcher.stop();
        assert_eq!(*applied.lock().expect("lock"), [1, 2], "seq 1 applied before seq 2, none lost");
        assert_eq!(*stored.lock().expect("lock"), Some(2));
        assert!(broker.empty());
    }

    /// A sink that fails once must get another try, and the broker must drain instead of holding the
    /// delivery unsettled forever.
    #[test]
    fn a_failing_sink_nacks_and_the_redelivery_reaches_it_again() {
        let sink_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&sink_calls);
        let dispatcher =
            KeyOrderedDispatcher::builder(1, |_: &EventEnvelope| Err(crate::Error::codec("poison").into()))
                .on_dead(move |_| match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Err(crate::Error::store("dlq down").into()),
                    _ => Ok(()),
                })
                .pacing(Pacing::instant())
                .build()
                .expect("the dispatcher's workers spawn");
        let broker = MemoryBroker::new();
        broker.publish(envelope("acct-1", 1));
        while let Some(d) = poll(&broker, Duration::from_millis(200)) {
            dispatcher.deliver(d);
        }
        dispatcher.stop();
        assert_eq!(sink_calls.load(Ordering::SeqCst), 2, "the sink got a second try");
        assert!(broker.empty(), "the dead letter settled the message");
    }
}

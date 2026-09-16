//! Dev/test in-memory broker, single process, not durable. A thin wrapper over the shared
//! `edax_memory::MemoryBroker` state machine (queue, FIFO order, per-token settlement, unsettled
//! accounting). The adapter's poll is non-blocking, so this module adds a `Condvar` and
//! [`MemoryBroker::poll`] can block up to a timeout.

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use edax_core::lock;
use edax_event::EventEnvelope;

use crate::dispatcher::Delivery;

/// Both producer and consumer over one FIFO queue, so a [`Relay`](crate::Relay) publishing into it and a
/// [`KeyOrderedDispatcher`](crate::KeyOrderedDispatcher) consuming from it talk without a broker. `Clone`
/// shares the queue, so one instance can be the relay's sink, another the consumer, a third a dead-letter sink.
///
/// [`publish`](Self::publish) returns once the envelope is queued; FIFO gives per-key order; `nack` requeues
/// at the tail, possibly behind later same-key messages, which the contract permits. A polled delivery stays
/// unsettled until it is acked or nacked, so [`empty`](Self::empty) reports the same "nothing left to do" a
/// durable broker does. Nothing survives the process.
#[derive(Clone, Default)]
pub struct MemoryBroker {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    machine: edax_memory::MemoryBroker,
    /// The lock the waiting side holds from its empty peek into `queued.wait`. A publisher takes it
    /// before notifying, so a message landing between a poller's peek and its wait cannot slip by
    /// unannounced: the publisher either sees the poller already waiting, or the poller's peek sees
    /// the message.
    wait: Mutex<()>,
    /// Signals a queued envelope to a blocked [`MemoryBroker::poll`].
    queued: Condvar,
}

impl Inner {
    /// Wake one blocked poller. Called after the machine has the envelope, holding the wait lock
    /// across the notify (see `wait`).
    fn announce(&self) {
        let _guard = lock(&self.wait);
        self.queued.notify_one();
    }
}

impl MemoryBroker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue an envelope at the tail.
    pub fn publish(&self, env: EventEnvelope) {
        self.inner.machine.publish(env);
        self.inner.announce();
    }

    /// Take the next envelope as a [`Delivery`], waiting up to `timeout` for one to be queued.
    /// `None` when the timeout elapses first; `Duration::ZERO` makes it a non-blocking peek. A
    /// `timeout` the clock cannot represent (`Duration::MAX`) waits without a deadline.
    /// `Delivery::attempt` is always `0`: this broker keeps no redelivery count.
    pub fn poll(&self, timeout: Duration) -> Option<Delivery> {
        // `None` is "no deadline": adding the timeout would overflow the clock, which is further
        // out than any caller can wait for anyway.
        let deadline = Instant::now().checked_add(timeout);
        let mut guard = lock(&self.inner.wait);
        let polled = loop {
            if let Some(polled) = self.inner.machine.try_poll() {
                break polled;
            }
            guard = match deadline {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return None;
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
        let ack_inner = Arc::clone(&self.inner);
        let nack_inner = Arc::clone(&self.inner);
        Some(Delivery::new(
            polled.envelope,
            move || {
                ack_inner.machine.ack(token);
            },
            move || {
                // The machine holds the envelope, so the requeue needs no copy here. The settle
                // result is not needed: `Delivery` is consumed on settle, so it cannot settle twice.
                nack_inner.machine.nack(token);
                nack_inner.announce();
            },
        ))
    }

    /// True only when nothing is queued *and* every polled delivery has been settled.
    pub fn empty(&self) -> bool {
        self.inner.machine.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edax_event::Event;

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

    /// One step of a scenario. `Poll` pushes onto a stack of held deliveries; the settle steps
    /// pop the most recent one.
    enum Step {
        Publish(&'static str, u64),
        Poll,
        Ack,
        Nack,
        Drop,
    }

    /// The wrapper end to end: every branch of the shared machine is reachable through a
    /// `Delivery`, and a settlement through the closures really lands in the machine.
    #[test]
    fn settlement_branches() {
        use Step::*;
        struct Case {
            name: &'static str,
            steps: &'static [Step],
            /// Envelope ids that `Poll` produced, in order; `""` for a timed-out poll.
            want_polled: &'static [&'static str],
            want_empty: bool,
        }
        let cases = [
            Case { name: "a new broker is empty and a poll times out", steps: &[Poll], want_polled: &[""], want_empty: true },
            Case { name: "a published envelope makes it non-empty", steps: &[Publish("a", 1)], want_polled: &[], want_empty: false },
            Case {
                name: "a polled delivery is unsettled, so still non-empty",
                steps: &[Publish("a", 1), Poll],
                want_polled: &["a-1"],
                want_empty: false,
            },
            Case { name: "ack removes it", steps: &[Publish("a", 1), Poll, Ack], want_polled: &["a-1"], want_empty: true },
            Case {
                name: "nack requeues it at the tail",
                steps: &[Publish("a", 1), Publish("a", 2), Poll, Nack, Poll, Poll],
                want_polled: &["a-1", "a-2", "a-1"],
                want_empty: false,
            },
            Case {
                name: "a nacked then acked delivery is settled",
                steps: &[Publish("a", 1), Poll, Nack, Poll, Ack],
                want_polled: &["a-1", "a-1"],
                want_empty: true,
            },
            Case {
                name: "a dropped delivery is never settled",
                steps: &[Publish("a", 1), Poll, Drop],
                want_polled: &["a-1"],
                want_empty: false,
            },
            Case {
                name: "the same id in flight twice needs two settlements",
                steps: &[Publish("a", 1), Publish("a", 1), Poll, Poll, Ack],
                want_polled: &["a-1", "a-1"],
                want_empty: false,
            },
            Case {
                name: "publishing keeps per-key FIFO order",
                steps: &[Publish("a", 1), Publish("b", 1), Publish("a", 2), Poll, Poll, Poll],
                want_polled: &["a-1", "b-1", "a-2"],
                want_empty: false,
            },
        ];
        for c in cases {
            let broker = MemoryBroker::new();
            let mut held: Vec<Delivery> = Vec::new();
            let mut polled: Vec<String> = Vec::new();
            for step in c.steps {
                match step {
                    Publish(agg, seq) => broker.publish(envelope(agg, *seq)),
                    Poll => match broker.poll(WAIT) {
                        Some(d) => {
                            polled.push(d.envelope.id.clone());
                            held.push(d);
                        }
                        None => polled.push(String::new()),
                    },
                    Ack => held.pop().expect("a held delivery").ack(),
                    Nack => held.pop().expect("a held delivery").nack(),
                    Drop => drop(held.pop().expect("a held delivery")),
                }
            }
            assert_eq!(polled, c.want_polled, "{}: polled ids", c.name);
            assert_eq!(broker.empty(), c.want_empty, "{}: empty()", c.name);
        }
    }

    #[test]
    fn a_clone_shares_the_queue_and_a_blocked_poll_wakes_on_publish() {
        // The dead-letter use: the hook holds a clone on a worker thread and publishes into it,
        // while the test polls the original. The poll blocks until the publish lands.
        let broker = MemoryBroker::new();
        let sink = broker.clone();
        let publisher = std::thread::spawn(move || sink.publish(envelope("a", 1)));
        let delivery = broker.poll(Duration::from_secs(2)).expect("the publish woke the poll");
        publisher.join().expect("publisher thread");
        assert_eq!(delivery.envelope.id, "a-1");
        assert_eq!(delivery.attempt, 0, "this broker keeps no redelivery count");
        delivery.ack();
        assert!(broker.empty(), "the clone and the original share one queue");
    }

    #[test]
    fn a_blocked_poll_wakes_on_nack() {
        // A nack requeues through the machine, so it must announce like a publish does or a poller
        // parked on the condvar sleeps through the redelivery until its timeout.
        let broker = MemoryBroker::new();
        broker.publish(envelope("a", 1));
        let first = broker.poll(WAIT).expect("queued");

        let consumer = broker.clone();
        let poller = std::thread::spawn(move || consumer.poll(Duration::from_secs(2)));
        std::thread::sleep(WAIT); // let the poller park before the nack
        first.nack();
        let again = poller.join().expect("poller thread").expect("the nack woke the poll");
        assert_eq!(again.envelope.id, "a-1");
        again.ack();
        assert!(broker.empty());
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
        let delivery = broker.poll(Duration::MAX).expect("the publish woke the unbounded wait");
        publisher.join().expect("publisher thread");
        assert_eq!(delivery.envelope.id, "a-1");
        delivery.ack();
        assert!(broker.empty());
    }

    #[test]
    fn a_zero_timeout_is_a_non_blocking_peek() {
        let broker = MemoryBroker::new();
        let started = Instant::now();
        assert!(broker.poll(Duration::ZERO).is_none());
        assert!(started.elapsed() < Duration::from_millis(50), "returned at once");
        broker.publish(envelope("a", 1));
        assert!(broker.poll(Duration::ZERO).is_some(), "a queued envelope is taken without waiting");
    }
}

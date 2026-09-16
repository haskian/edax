//! The in-memory broker: a queue with per-delivery settlement tokens. Not a store participant.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use edax_core::{lock, TokenMint};

/// A delivery handed out by [`MemoryBroker::try_poll`]: the envelope plus the token that settles it.
#[derive(Clone, Debug, PartialEq)]
pub struct PolledDelivery {
    pub token: i64,
    pub envelope: edax_event::EventEnvelope,
}

#[derive(Default)]
struct BrokerState {
    queue: VecDeque<edax_event::EventEnvelope>,
    /// The tokens polls hand out. Monotonic, so a token names exactly one delivery for the broker's
    /// whole life: a stale token (already settled) never aliases a live one.
    tokens: TokenMint,
    /// Polled deliveries not yet acked or nacked, by token. Keyed by token, not by envelope id:
    /// at-least-once puts the same id in flight twice (the outbox republishes after a crash between
    /// `publish` and `mark_published`), and each copy must settle on its own. Holding the envelope here
    /// is what lets `nack` requeue it without the host keeping a copy.
    unsettled: HashMap<i64, edax_event::EventEnvelope>,
}

/// In-memory broker state machine for development and tests: one FIFO queue plus the accounting of
/// polled-but-unsettled deliveries, behind a single lock. Non-blocking by design: [`try_poll`](Self::try_poll)
/// returns at once, and each SDK adds only its own waiting (a `Condvar`, an asyncio event) on top —
/// the one part a broker cannot share, because a blocking call across the binding would stall the
/// host's event loop. Settlement is data, not a callback: a poll hands out a token, and
/// [`ack`](Self::ack) / [`nack`](Self::nack) settle by it, so no closure has to cross the boundary.
///
/// [`is_empty`](Self::is_empty) is true only when nothing is queued *and* every polled delivery has
/// been settled, the same "nothing left to do" a durable broker reports. Without that a dispatcher that
/// dropped a delivery would pass here and lose the message against Kafka: the in-memory rig
/// would flatter exactly the bug it exists to catch.
#[derive(Default)]
pub struct MemoryBroker {
    state: Mutex<BrokerState>,
}

impl MemoryBroker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue `envelope` at the tail.
    pub fn publish(&self, envelope: edax_event::EventEnvelope) {
        lock(&self.state).queue.push_back(envelope);
    }

    /// Take the head of the queue as an unsettled delivery, or `None` at once when the queue is empty.
    pub fn try_poll(&self) -> Option<PolledDelivery> {
        let mut state = lock(&self.state);
        let envelope = state.queue.pop_front()?;
        let token = state.tokens.issue();
        state.unsettled.insert(token, envelope.clone());
        Some(PolledDelivery { token, envelope })
    }

    /// Settle the delivery `token` as handled: the broker drops it. Returns `false` and changes
    /// nothing if the token is not outstanding (already settled, or never issued), so the outstanding
    /// count only ever falls by deliveries really in flight.
    pub fn ack(&self, token: i64) -> bool {
        lock(&self.state).unsettled.remove(&token).is_some()
    }

    /// Settle the delivery `token` as failed: its envelope is requeued at the tail, and the next poll
    /// hands it out again under a fresh token. Returns `false` and requeues nothing if the token is not
    /// outstanding.
    pub fn nack(&self, token: i64) -> bool {
        let mut state = lock(&self.state);
        match state.unsettled.remove(&token) {
            Some(envelope) => {
                state.queue.push_back(envelope);
                true
            }
            None => false,
        }
    }

    /// True only when nothing is queued *and* every polled delivery has been settled.
    pub fn is_empty(&self) -> bool {
        let state = lock(&self.state);
        state.queue.is_empty() && state.unsettled.is_empty()
    }
}

#[cfg(test)]
// A test asserts on the value it just built; a panic there is the failure report.
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use edax_core::NO_DELIVERY;

    use crate::testkit::envelope;

    /// One step of a broker scenario. `Poll` records what `try_poll` returned; the settle steps name
    /// the i-th polled delivery, and `AckToken` settles a raw token that no poll issued.
    enum Step {
        Publish(&'static str, u64),
        Poll,
        Ack(usize),
        Nack(usize),
        AckToken(i64),
    }

    #[test]
    fn broker_settlement_branches() {
        use Step::*;
        struct Case {
            name: &'static str,
            steps: &'static [Step],
            /// Envelope ids `Poll` produced, in order; `None` for an empty poll.
            polled: &'static [Option<&'static str>],
            /// What each settle step returned, in order.
            settled: &'static [bool],
            empty: bool,
        }
        let cases = [
            Case { name: "a new broker is empty and try_poll returns nothing at once", steps: &[Poll], polled: &[None], settled: &[], empty: true },
            Case { name: "a published envelope makes it non-empty", steps: &[Publish("a", 1)], polled: &[], settled: &[], empty: false },
            Case {
                name: "a polled delivery is unsettled, so still non-empty",
                steps: &[Publish("a", 1), Poll],
                polled: &[Some("a-1")],
                settled: &[],
                empty: false,
            },
            Case {
                name: "ack settles it",
                steps: &[Publish("a", 1), Poll, Ack(0)],
                polled: &[Some("a-1")],
                settled: &[true],
                empty: true,
            },
            Case {
                name: "nack requeues it at the tail",
                steps: &[Publish("a", 1), Publish("a", 2), Poll, Nack(0), Poll, Poll],
                polled: &[Some("a-1"), Some("a-2"), Some("a-1")],
                settled: &[true],
                empty: false,
            },
            Case {
                name: "a nacked then acked delivery is settled",
                steps: &[Publish("a", 1), Poll, Nack(0), Poll, Ack(1)],
                polled: &[Some("a-1"), Some("a-1")],
                settled: &[true, true],
                empty: true,
            },
            Case {
                // Fails against a set of envelope ids: one ack would clear the single entry and the
                // broker would report drained with a delivery still outstanding.
                name: "the same id in flight twice needs two settlements",
                steps: &[Publish("a", 1), Publish("a", 1), Poll, Poll, Ack(0)],
                polled: &[Some("a-1"), Some("a-1")],
                settled: &[true],
                empty: false,
            },
            Case {
                name: "the same id in flight twice is drained by two settlements",
                steps: &[Publish("a", 1), Publish("a", 1), Poll, Poll, Ack(0), Ack(1)],
                polled: &[Some("a-1"), Some("a-1")],
                settled: &[true, true],
                empty: true,
            },
            Case {
                // Fails against an unguarded counter: the second ack would drive it to zero while
                // the second delivery is still in flight.
                name: "settling the same delivery twice is refused and loses nothing",
                steps: &[Publish("a", 1), Publish("a", 2), Poll, Poll, Ack(0), Ack(0)],
                polled: &[Some("a-1"), Some("a-2")],
                settled: &[true, false],
                empty: false,
            },
            Case {
                name: "after a refused double ack the real outstanding delivery still settles",
                steps: &[Publish("a", 1), Publish("a", 2), Poll, Poll, Ack(0), Ack(0), Ack(1)],
                polled: &[Some("a-1"), Some("a-2")],
                settled: &[true, false, true],
                empty: true,
            },
            Case {
                // At-least-once: a stale ack after a nack must not drop the requeued envelope.
                name: "ack after nack of the same token does not drop the requeued envelope",
                steps: &[Publish("a", 1), Poll, Nack(0), Ack(0), Poll],
                polled: &[Some("a-1"), Some("a-1")],
                settled: &[true, false],
                empty: false,
            },
            Case {
                name: "nack twice requeues once",
                steps: &[Publish("a", 1), Poll, Nack(0), Nack(0), Poll, Poll],
                polled: &[Some("a-1"), Some("a-1"), None],
                settled: &[true, false],
                empty: false,
            },
            Case {
                name: "a token no poll issued settles nothing",
                steps: &[Publish("a", 1), Poll, AckToken(999), AckToken(NO_DELIVERY)],
                polled: &[Some("a-1")],
                settled: &[false, false],
                empty: false,
            },
            Case {
                name: "polling keeps FIFO order across keys",
                steps: &[Publish("a", 1), Publish("b", 1), Publish("a", 2), Poll, Poll, Poll],
                polled: &[Some("a-1"), Some("b-1"), Some("a-2")],
                settled: &[],
                empty: false,
            },
        ];
        for c in cases {
            let broker = MemoryBroker::new();
            let mut held: Vec<i64> = Vec::new();
            let mut polled: Vec<Option<String>> = Vec::new();
            let mut settled: Vec<bool> = Vec::new();
            for step in c.steps {
                match step {
                    Publish(agg, seq) => broker.publish(envelope(&format!("{agg}-{seq}"), Some(*seq))),
                    Poll => match broker.try_poll() {
                        Some(d) => {
                            polled.push(Some(d.envelope.id.clone()));
                            held.push(d.token);
                        }
                        None => polled.push(None),
                    },
                    Ack(i) => settled.push(broker.ack(held[*i])),
                    Nack(i) => settled.push(broker.nack(held[*i])),
                    AckToken(token) => settled.push(broker.ack(*token)),
                }
            }
            let want: Vec<Option<String>> = c.polled.iter().map(|id| id.map(str::to_owned)).collect();
            assert_eq!(polled, want, "{}: polled ids", c.name);
            assert_eq!(settled, c.settled, "{}: settle results", c.name);
            assert_eq!(broker.is_empty(), c.empty, "{}: is_empty()", c.name);
        }
    }

    #[test]
    fn broker_tokens_are_unique_per_delivery_and_never_the_placeholder() {
        let broker = MemoryBroker::new();
        broker.publish(envelope("evt-1", Some(1)));
        let first = broker.try_poll().expect("queued");
        assert_ne!(first.token, NO_DELIVERY, "a live delivery never carries the placeholder token");
        assert_eq!(first.envelope, envelope("evt-1", Some(1)), "the envelope crosses field for field");

        // A redelivery is a new delivery: it settles by its own token, not the stale one.
        assert!(broker.nack(first.token));
        let again = broker.try_poll().expect("requeued");
        assert_ne!(again.token, first.token);
        assert!(!broker.ack(first.token), "the stale token is dead");
        assert!(broker.ack(again.token));
        assert!(broker.is_empty());
    }

    /// The queue and the accounting sit behind one lock, so contending pollers each get a distinct
    /// delivery and nothing is handed out twice or lost.
    #[test]
    fn concurrent_polls_hand_each_envelope_out_once() {
        let broker = MemoryBroker::new();
        for seq in 1..=4 {
            broker.publish(envelope(&format!("evt-{seq}"), Some(seq)));
        }
        let mut deliveries: Vec<PolledDelivery> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8).map(|_| s.spawn(|| broker.try_poll())).collect();
            handles.into_iter().filter_map(|h| h.join().expect("poller thread")).collect()
        });
        deliveries.sort_by_key(|d| d.token);
        assert_eq!(deliveries.iter().map(|d| d.envelope.id.as_str()).collect::<Vec<_>>(), ["evt-1", "evt-2", "evt-3", "evt-4"]);
        assert!(deliveries.windows(2).all(|w| w[0].token < w[1].token), "tokens are distinct and monotonic");
        assert!(!broker.is_empty(), "all four are unsettled");
        for d in &deliveries {
            assert!(broker.ack(d.token));
        }
        assert!(broker.is_empty());
    }
}

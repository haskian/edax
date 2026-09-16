//! The consumer's offset bookkeeping: one commit watermark per partition, settled by token.
//!
//! A dispatcher acks in worker order, not in fetch order, so a bare commit would skip past an
//! offset still in flight and lose it on restart. [`OffsetTracker::ack`] instead names a watermark:
//! the lowest offset still in flight, or one past the highest delivered when none remain.
//!
//! Settlement is by token, the [`MemoryBroker`](https://docs.rs/edax-memory) precedent, not by
//! `(partition, offset)`. That closes the stale-settle hole a key of coordinates leaves open:
//! revoke a partition, reset it, get it assigned back, re-book offset 5, and the pre-revoke ack for
//! 5 would otherwise discard the fresh booking. A token names one delivery for the tracker's whole
//! life, so the stale one settles nothing.
//!
//! Settlement is also data, not a callback: [`ack`](OffsetTracker::ack) and
//! [`nack`](OffsetTracker::nack) return the decision, and the IO shell performs it. The shell
//! reports the outcome back with [`committed`](OffsetTracker::committed), so a client commit that
//! failed is not recorded as one.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard};

/// The token value no booking ever carries, so a host's "no delivery" placeholder cannot alias a
/// live one.
pub const NO_DELIVERY: i64 = 0;

/// The offset that names no position: no partition state backs the decision, nothing has been
/// delivered yet, or nothing has been committed yet. Below every real Kafka offset, which starts at
/// 0, so a host comparing against it never mistakes it for progress.
const NO_OFFSET: i64 = -1;

/// What [`OffsetTracker::ack`] tells the shell to do: commit `offset` as the partition's watermark,
/// or nothing at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckDecision {
    /// True when the watermark is higher than the last one the shell reported committed. False for
    /// a stale or unknown token, and for a watermark already committed.
    pub commit: bool,
    /// The watermark to commit, or [`NO_OFFSET`] when `commit` is false because the token is stale
    /// or unknown.
    pub offset: i64,
}

/// What [`OffsetTracker::nack`] tells the shell to do: seek the partition back to `offset` for
/// redelivery, or nothing at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NackDecision {
    /// True when the partition should be sought back. False for a stale or unknown token.
    pub seek: bool,
    /// The offset to seek to — the lowest still in flight — or [`NO_OFFSET`] when `seek` is false.
    pub offset: i64,
}

/// One partition's bookkeeping, for a health check or a gauge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionStats {
    /// The topic this partition belongs to.
    pub topic: String,
    /// The partition number.
    pub partition: i32,
    /// Offsets delivered but not yet settled.
    pub unsettled: u64,
    /// The highest offset booked, or [`NO_OFFSET`] before the first booking.
    pub max_delivered: i64,
    /// The last watermark the shell reported committed, or [`NO_OFFSET`] before the first commit.
    pub committed: i64,
}

/// A partition of a topic. `String` and not `&str` because the state outlives every call.
type PartitionKey = (String, i32);

struct PartitionState {
    /// Booked but unsettled offsets, each with the token that settles it. Ordered, so the lowest —
    /// the watermark an ack may commit and the offset a nack seeks to — is the first key.
    inflight: BTreeMap<i64, i64>,
    max_delivered: i64,
    committed: i64,
}

impl Default for PartitionState {
    fn default() -> Self {
        Self { inflight: BTreeMap::new(), max_delivered: NO_OFFSET, committed: NO_OFFSET }
    }
}

impl PartitionState {
    /// The offset an ack would commit: the lowest still in flight, else one past the highest
    /// delivered. Saturating: an offset at the `i64` ceiling would otherwise wrap below every
    /// committed watermark and be refused forever.
    fn watermark(&self) -> i64 {
        match self.inflight.keys().next() {
            Some(&lowest) => lowest,
            None => self.max_delivered.saturating_add(1),
        }
    }
}

/// One booked delivery: where it sits, so a token alone settles it.
struct Booking {
    partition: PartitionKey,
    offset: i64,
}

struct Inner {
    /// Ordered by `(topic, partition)`, which is the order [`OffsetTracker::stats`] reports in.
    partitions: BTreeMap<PartitionKey, PartitionState>,
    /// Live tokens only. A token leaves on ack, on re-booking of its offset, and on reset of its
    /// partition; from then on it is stale and settles nothing.
    bookings: HashMap<i64, Booking>,
    next_token: i64,
}

impl Default for Inner {
    fn default() -> Self {
        Self { partitions: BTreeMap::new(), bookings: HashMap::new(), next_token: NO_DELIVERY + 1 }
    }
}

/// Lock the tracker state, recovering from poisoning instead of panicking.
///
/// This kernel runs inside a native binding, where an unwinding panic crosses the host language's
/// FFI boundary. Nothing inside a guarded section here can panic, so the state behind a poisoned
/// lock is still consistent — take it back rather than propagate a panic the host cannot catch.
fn lock<T>(state: &Mutex<T>) -> MutexGuard<'_, T> {
    state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The per-partition commit watermark, behind a single lock. One tracker serves every partition a
/// consumer is assigned: a commit is per partition, so one partition's progress never settles
/// another's.
///
/// The bookkeeping holds only while the partition is assigned. A revoked partition leaves its
/// in-flight offsets unsettled here and the new owner redelivers them from the last committed
/// watermark — at-least-once — so a rebalance listener calls [`reset`](Self::reset) for it.
#[derive(Default)]
pub struct OffsetTracker {
    state: Mutex<Inner>,
}

impl OffsetTracker {
    /// A tracker with nothing booked.
    pub fn new() -> Self {
        Self::default()
    }

    /// Book a fetched offset as in flight and return the token that settles it.
    ///
    /// Call this before decoding: a payload the host cannot decode still has to be settled, and an
    /// unbooked offset is either committed past by the next ack or replayed into the same failure
    /// forever. Booking an offset already in flight — the redelivery a [`nack`](Self::nack) asked
    /// for — takes a fresh token and retires the old one, so the redelivered copy settles once.
    pub fn book(&self, topic: &str, partition: i32, offset: i64) -> i64 {
        let mut inner = lock(&self.state);
        let token = inner.next_token;
        // Wrapping, not saturating: saturation would hand the same token out forever. A wrap needs
        // 2^63 bookings, and only matters if a token that old were still unsettled.
        inner.next_token = token.wrapping_add(1);
        let key: PartitionKey = (topic.to_owned(), partition);
        let state = inner.partitions.entry(key.clone()).or_default();
        let replaced = state.inflight.insert(offset, token);
        state.max_delivered = state.max_delivered.max(offset);
        if let Some(stale) = replaced {
            inner.bookings.remove(&stale);
        }
        inner.bookings.insert(token, Booking { partition: key, offset });
        token
    }

    /// Settle `token` as handled and report the watermark the shell should commit.
    ///
    /// A stale or unknown token commits nothing: it belongs to a delivery already settled, to a
    /// partition since reset, or to a booking a redelivery replaced. Report the commit back with
    /// [`committed`](Self::committed) once the client confirms it.
    pub fn ack(&self, token: i64) -> AckDecision {
        let mut inner = lock(&self.state);
        let Some(booking) = inner.bookings.remove(&token) else {
            return AckDecision { commit: false, offset: NO_OFFSET };
        };
        // A live booking always has its partition — `reset` drops both together — so this arm is
        // unreachable by construction. It is an `else` and not an index because a kernel inside a
        // binding answers, it never panics.
        let Some(state) = inner.partitions.get_mut(&booking.partition) else {
            return AckDecision { commit: false, offset: NO_OFFSET };
        };
        state.inflight.remove(&booking.offset);
        let watermark = state.watermark();
        // A commit reordered under async IO at worst re-commits a lower safe watermark; this guard
        // keeps the record monotonic so a later, higher ack is never skipped.
        AckDecision { commit: watermark > state.committed, offset: watermark }
    }

    /// Record that the client committed `offset` on this partition.
    ///
    /// Max-merge, so two commits that returned out of order leave the higher watermark recorded. A
    /// no-op for a partition never booked or since reset: recording a watermark for one nobody owns
    /// would refuse a lower, correct commit if it were assigned back.
    pub fn committed(&self, topic: &str, partition: i32, offset: i64) {
        let mut inner = lock(&self.state);
        if let Some(state) = inner.partitions.get_mut(&(topic.to_owned(), partition)) {
            state.committed = state.committed.max(offset);
        }
    }

    /// Settle `token` as failed and report the offset the shell should seek the partition to.
    ///
    /// The offset stays in flight until its redelivered copy settles, so a later ack cannot commit
    /// past it. The seek target is the lowest offset in flight, not this one: `seek` sets the
    /// partition's single fetch position, so nacking 5 then 7 would move it to 7 and 5 would never
    /// come back.
    pub fn nack(&self, token: i64) -> NackDecision {
        let inner = lock(&self.state);
        let Some(booking) = inner.bookings.get(&token) else {
            return NackDecision { seek: false, offset: NO_OFFSET };
        };
        let Some(state) = inner.partitions.get(&booking.partition) else {
            return NackDecision { seek: false, offset: NO_OFFSET };
        };
        // The nacked offset is still in flight, so the map is never empty here; its own offset is
        // the answer either way.
        let lowest = state.inflight.keys().next().copied().unwrap_or(booking.offset);
        NackDecision { seek: true, offset: lowest }
    }

    /// Drop a revoked partition's bookkeeping and retire its tokens.
    ///
    /// Its unsettled offsets are the new owner's to redeliver from the last committed watermark. A
    /// watermark left behind here would refuse a lower, correct commit if the partition came back,
    /// and a token left live would settle a delivery the tracker no longer owns.
    pub fn reset(&self, topic: &str, partition: i32) {
        let mut inner = lock(&self.state);
        let Some(state) = inner.partitions.remove(&(topic.to_owned(), partition)) else {
            return;
        };
        for token in state.inflight.values() {
            inner.bookings.remove(token);
        }
    }

    /// A snapshot of every assigned partition, ordered by topic then partition.
    pub fn stats(&self) -> Vec<PartitionStats> {
        lock(&self.state)
            .partitions
            .iter()
            .map(|((topic, partition), state)| PartitionStats {
                topic: topic.clone(),
                partition: *partition,
                // Saturating: `usize` is the machine's, `u64` is the contract's. They differ only
                // on a 128-bit target, and never with this many offsets in flight.
                unsettled: u64::try_from(state.inflight.len()).unwrap_or(u64::MAX),
                max_delivered: state.max_delivered,
                committed: state.committed,
            })
            .collect()
    }
}

#[cfg(test)]
// A test asserts on the value it just built; a panic there is the failure report.
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    /// One step of a tracker scenario. The settle steps name the i-th token `Book` handed out;
    /// `AckToken` / `NackToken` settle a raw token no booking issued.
    enum Step {
        Book(&'static str, i32, i64),
        Ack(usize),
        AckToken(i64),
        Nack(usize),
        NackToken(i64),
        Committed(&'static str, i32, i64),
        Reset(&'static str, i32),
    }

    /// `(topic, partition, unsettled, max_delivered, committed)` — `PartitionStats` without the
    /// owned `String`, so a case can be a constant.
    type StatsRow = (&'static str, i32, u64, i64, i64);

    #[test]
    fn offset_settlement_branches() {
        use Step::*;
        struct Case {
            name: &'static str,
            steps: &'static [Step],
            /// What each ack step returned, in order.
            acked: &'static [AckDecision],
            /// What each nack step returned, in order.
            nacked: &'static [NackDecision],
            /// The final `stats()`.
            stats: &'static [StatsRow],
        }
        let cases = [
            Case {
                name: "an ack that empties the inflight set commits one past the highest delivered",
                steps: &[Book("t", 0, 0), Ack(0)],
                acked: &[AckDecision { commit: true, offset: 1 }],
                nacked: &[],
                stats: &[("t", 0, 0, 0, -1)],
            },
            Case {
                name: "an out-of-order ack commits only up to the lowest offset still in flight",
                steps: &[Book("t", 0, 0), Book("t", 0, 1), Ack(1)],
                acked: &[AckDecision { commit: true, offset: 0 }],
                nacked: &[],
                stats: &[("t", 0, 1, 1, -1)],
            },
            Case {
                // The guard: without it this ack would re-commit a watermark already recorded.
                name: "an ack whose watermark is already committed refuses to step back",
                steps: &[Book("t", 0, 0), Book("t", 0, 1), Book("t", 0, 2), Ack(0), Committed("t", 0, 1), Ack(2)],
                acked: &[AckDecision { commit: true, offset: 1 }, AckDecision { commit: false, offset: 1 }],
                nacked: &[],
                stats: &[("t", 0, 1, 2, 1)],
            },
            Case {
                // Two commits that returned out of order: the higher one stands.
                name: "committed max-merges a late lower confirmation",
                steps: &[
                    Book("t", 0, 0),
                    Book("t", 0, 1),
                    Ack(0),
                    Ack(1),
                    Committed("t", 0, 2),
                    Committed("t", 0, 1),
                ],
                acked: &[AckDecision { commit: true, offset: 1 }, AckDecision { commit: true, offset: 2 }],
                nacked: &[],
                stats: &[("t", 0, 0, 1, 2)],
            },
            Case {
                name: "a nack seeks the lowest offset in flight, not the nacked one",
                steps: &[Book("t", 0, 0), Book("t", 0, 1), Book("t", 0, 2), Nack(2), Nack(0)],
                acked: &[],
                nacked: &[NackDecision { seek: true, offset: 0 }, NackDecision { seek: true, offset: 0 }],
                stats: &[("t", 0, 3, 2, -1)],
            },
            Case {
                name: "a token no booking issued settles nothing",
                steps: &[Book("t", 0, 0), AckToken(999), NackToken(NO_DELIVERY)],
                acked: &[AckDecision { commit: false, offset: -1 }],
                nacked: &[NackDecision { seek: false, offset: -1 }],
                stats: &[("t", 0, 1, 0, -1)],
            },
            Case {
                name: "reset drops the partition, so its tokens are stale",
                steps: &[Book("t", 0, 5), Book("t", 0, 6), Reset("t", 0), Ack(0), NackToken(2)],
                acked: &[AckDecision { commit: false, offset: -1 }],
                nacked: &[NackDecision { seek: false, offset: -1 }],
                stats: &[],
            },
            Case {
                // The hole a `(partition, offset)` key leaves: the pre-revoke ack for 5 must not
                // discard the booking the re-assignment made.
                name: "a partition assigned back after reset ignores the pre-reset ack",
                steps: &[Book("t", 0, 5), Reset("t", 0), Book("t", 0, 5), Ack(0), Ack(1)],
                acked: &[AckDecision { commit: false, offset: -1 }, AckDecision { commit: true, offset: 6 }],
                nacked: &[],
                stats: &[("t", 0, 0, 5, -1)],
            },
            Case {
                name: "re-booking an offset replaces its token, and the ack for the old one is stale",
                steps: &[Book("t", 0, 0), Nack(0), Book("t", 0, 0), Ack(0), Ack(1)],
                acked: &[AckDecision { commit: false, offset: -1 }, AckDecision { commit: true, offset: 1 }],
                nacked: &[NackDecision { seek: true, offset: 0 }],
                stats: &[("t", 0, 0, 0, -1)],
            },
            Case {
                name: "committed for a partition nobody booked is a no-op",
                steps: &[Book("t", 0, 0), Committed("t", 9, 100)],
                acked: &[],
                nacked: &[],
                stats: &[("t", 0, 1, 0, -1)],
            },
            Case {
                name: "each partition keeps its own watermark, and stats sorts by topic then partition",
                steps: &[Book("u", 0, 4), Book("t", 1, 0), Book("t", 0, 0), Ack(1)],
                acked: &[AckDecision { commit: true, offset: 1 }],
                nacked: &[],
                stats: &[("t", 0, 1, 0, -1), ("t", 1, 0, 0, -1), ("u", 0, 1, 4, -1)],
            },
        ];
        for case in cases {
            let tracker = OffsetTracker::new();
            let mut tokens: Vec<i64> = Vec::new();
            let mut acked: Vec<AckDecision> = Vec::new();
            let mut nacked: Vec<NackDecision> = Vec::new();
            for step in case.steps {
                match step {
                    Book(topic, partition, offset) => tokens.push(tracker.book(topic, *partition, *offset)),
                    Ack(i) => acked.push(tracker.ack(tokens[*i])),
                    AckToken(token) => acked.push(tracker.ack(*token)),
                    Nack(i) => nacked.push(tracker.nack(tokens[*i])),
                    NackToken(token) => nacked.push(tracker.nack(*token)),
                    Committed(topic, partition, offset) => tracker.committed(topic, *partition, *offset),
                    Reset(topic, partition) => tracker.reset(topic, *partition),
                }
            }
            assert_eq!(acked, case.acked, "{}: ack decisions", case.name);
            assert_eq!(nacked, case.nacked, "{}: nack decisions", case.name);
            let stats = tracker.stats();
            let got: Vec<(&str, i32, u64, i64, i64)> = stats
                .iter()
                .map(|s| (s.topic.as_str(), s.partition, s.unsettled, s.max_delivered, s.committed))
                .collect();
            assert_eq!(got, case.stats, "{}: stats()", case.name);
        }
    }

    /// Tokens are the settlement identity, so they must be handed out in one deterministic,
    /// never-repeating order — including for an offset booked twice.
    #[test]
    fn tokens_are_monotonic_and_never_the_placeholder() {
        let tracker = OffsetTracker::new();
        let booked: Vec<i64> = (0..4).map(|offset| tracker.book("t", (offset % 2) as i32, offset)).collect();
        assert_eq!(booked, [1, 2, 3, 4], "tokens start at NO_DELIVERY + 1 and step by one");
        assert!(booked.iter().all(|&token| token != NO_DELIVERY));
        let again = tracker.book("t", 0, 0);
        assert_eq!(again, 5, "a re-booked offset takes a fresh token, never the retired one");
        assert!(!tracker.ack(booked[0]).commit, "the retired token is dead");
    }
}

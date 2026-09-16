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
use std::sync::{Arc, Mutex};

use edax_core::{lock, TokenMint};

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

/// A partition of a topic. The topic is an `Arc<str>` and not a `String` because both the map and
/// the booking that names it hold one: shared, it is allocated once per [`OffsetTracker::book`].
type PartitionKey = (Arc<str>, i32);

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

#[derive(Default)]
struct Inner {
    /// Ordered by `(topic, partition)`, which is the order [`OffsetTracker::stats`] reports in.
    partitions: BTreeMap<PartitionKey, PartitionState>,
    /// Live tokens only. A token leaves on ack, on re-booking of its offset, and on reset of its
    /// partition; from then on it is stale and settles nothing.
    bookings: HashMap<i64, Booking>,
    /// The tokens bookings settle by, shared with the in-memory broker so both state machines
    /// number their deliveries the same way.
    tokens: TokenMint,
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
        let token = inner.tokens.issue();
        // One copy of the topic per call, not two: the map and the booking both hold the key, and
        // cloning an `Arc` is a refcount bump rather than a second allocation.
        let key: PartitionKey = (Arc::from(topic), partition);
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
        if let Some(state) = inner.partitions.get_mut(&(Arc::from(topic), partition)) {
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
        let Some(state) = inner.partitions.remove(&(Arc::from(topic), partition)) else {
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
                topic: topic.to_string(),
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

    /// The branches `testdata/golden/kafka_offsets.json` does not reach. That file is the scenario
    /// driver for this state machine — every settlement rule is a case there, replayed by every
    /// binding — so what is left here is only what a golden scenario cannot express.
    #[test]
    fn branches_the_golden_scenarios_do_not_reach() {
        // A rebalance listener resets every revoked partition blindly, including ones this tracker
        // never booked. That is a no-op and must leave the partitions it does own untouched.
        let tracker = OffsetTracker::new();
        let token = tracker.book("t", 0, 4);
        tracker.reset("t", 9);
        tracker.reset("u", 0);
        assert_eq!(tracker.ack(token), AckDecision { commit: true, offset: 5 }, "the booked partition still settles");
        assert_eq!(tracker.stats().len(), 1, "only the partition that was booked is reported");

        // The `u64` width of `unsettled` is the contract's, and `stats` reports a partition with
        // nothing in flight rather than dropping it.
        let stats = tracker.stats();
        assert_eq!(
            stats.first(),
            Some(&PartitionStats {
                topic: "t".to_owned(),
                partition: 0,
                unsettled: 0,
                max_delivered: 4,
                committed: NO_OFFSET
            })
        );
    }
}

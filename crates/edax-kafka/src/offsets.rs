//! The consumer's offset bookkeeping: one commit watermark per partition, settled by token.
//!
//! A dispatcher acks in worker order, not in fetch order, so a bare commit would skip past an
//! offset still in flight and lose it on restart. The tracker instead keeps a watermark per
//! partition: the lowest offset still in flight, or one past the highest delivered when none remain.
//! [`OffsetTracker::due`] lists the watermarks not yet committed, so a shell commits them in one
//! batch on its own schedule instead of one round trip per ack.
//!
//! Settlement is by token, not by `(partition, offset)`. That closes the stale-settle hole a key of
//! coordinates leaves open: revoke a partition, reset it, get it assigned back, re-book offset 5,
//! and the pre-revoke ack for 5 would otherwise discard the fresh booking. A token names one
//! delivery for the tracker's whole life, so the stale one settles nothing.
//!
//! A nack *rewinds the partition*. The shell seeks the partition's one fetch position back to the
//! lowest offset in flight, so every booking at or above it is fetched again. The copies already
//! fetched are dead weight: running one would apply a later event of a key before the redelivered
//! earlier one. So the nack retires every token of the partition, and
//! [`is_live`](OffsetTracker::is_live) tells the dispatcher to skip a copy whose token is retired.
//! This is what keeps per-key order through a nack. It assumes what a Kafka client does: the shell
//! books each fetched record before it fetches the next one, and a seek drops the records the client
//! had buffered for that partition.
//!
//! Settlement is also data, not a callback: [`ack`](OffsetTracker::ack) and
//! [`nack`](OffsetTracker::nack) return the decision, and the IO shell performs it. The shell
//! reports a commit back with [`committed`](OffsetTracker::committed), so a client commit that
//! failed is not recorded as one.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use edax_core::{lock, Error, TokenMint};

/// What [`OffsetTracker::ack`] tells the shell. Exhaustive, so a new outcome is a compile error in
/// every shell rather than a silent default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "a stale ack settled nothing, which a shell may want to report"]
pub enum AckDecision {
    /// The token settled its offset. Whether a commit is now due is [`OffsetTracker::due`]'s answer.
    Settled,
    /// The token is stale or unknown — already settled, retired by a rewind, replaced by a
    /// redelivery, or from a partition since reset. Nothing settled.
    Stale,
}

/// What [`OffsetTracker::nack`] tells the shell to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "a dropped decision is a redelivery that never happens"]
pub enum NackDecision {
    /// Seek `partition` back to `offset`, the lowest still in flight, for redelivery. Every token of
    /// the partition is retired. The partition rides along so a shell that holds only the token (the
    /// in-memory broker) knows which fetch position to rewind.
    Seek {
        /// The partition the nacked token was booked on.
        partition: i32,
        /// The offset to seek it back to.
        offset: i64,
    },
    /// The token is stale or unknown. Nothing to do.
    Stale,
}

/// One partition's watermark that is higher than the last one committed: what the shell commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Due {
    /// The topic this partition belongs to.
    pub topic: String,
    /// The partition number.
    pub partition: i32,
    /// The watermark to commit: the next offset the group reads after a restart.
    pub offset: i64,
}

/// One partition's bookkeeping, for a health check or a gauge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionStats {
    /// The topic this partition belongs to.
    pub topic: String,
    /// The partition number.
    pub partition: i32,
    /// Offsets delivered but not yet settled, retired ones included: the watermark waits for each.
    pub unsettled: u64,
    /// Unsettled offsets whose token is still live: work a handler may still settle. A retired
    /// offset waits for a fetch instead.
    pub live: u64,
    /// The highest offset booked. A partition is reported only once an offset was booked on it.
    pub max_delivered: i64,
    /// The last watermark the shell reported committed, or `None` before the first commit.
    pub committed: Option<i64>,
}

struct PartitionState {
    /// Booked but unsettled offsets, each with the token that settles it — or the retired token a
    /// rewind left behind, which no longer settles anything. Ordered, so the lowest — the watermark
    /// and the offset a nack seeks to — is the first key.
    inflight: BTreeMap<i64, i64>,
    /// The lowest offset booked since this bookkeeping began. A shell fetches from the group's last
    /// commit, so a watermark no higher than this one commits nothing the group does not have.
    first: i64,
    max_delivered: i64,
    committed: Option<i64>,
}

impl PartitionState {
    /// The bookkeeping of a partition whose first booking is `offset`.
    fn new(offset: i64) -> Self {
        Self { inflight: BTreeMap::new(), first: offset, max_delivered: offset, committed: None }
    }

    /// The offset a commit names: the lowest still in flight, else one past the highest delivered.
    /// Saturating: an offset at the `i64` ceiling would otherwise wrap below every committed
    /// watermark. Committing `i64::MAX` for it redelivers that one offset after a restart, which
    /// at-least-once allows.
    fn watermark(&self) -> i64 {
        self.inflight.keys().next().copied().unwrap_or_else(|| self.max_delivered.saturating_add(1))
    }

    /// The watermark when it is progress: higher than the last one committed, and higher than the
    /// first offset booked, where the fetch started. A fresh tracker — after a restart or a
    /// rebalance — does not know the group's commit, but it fetched from there, so this spares the
    /// round trip that would re-commit it.
    fn due(&self) -> Option<i64> {
        Some(self.watermark()).filter(|&watermark| Some(watermark) > self.committed && watermark > self.first)
    }
}

/// One booked delivery: where it sits, so a token alone settles it. The topic is shared with the
/// partition map's key, so booking allocates it once per topic rather than once per offset.
struct Booking {
    topic: Arc<str>,
    partition: i32,
    offset: i64,
}

#[derive(Default)]
struct Inner {
    /// Topic, then partition: ordered, which is the order [`OffsetTracker::stats`] and
    /// [`OffsetTracker::due`] report in, and keyed by `Arc<str>` so a `&str` looks a topic up
    /// without allocating.
    topics: BTreeMap<Arc<str>, BTreeMap<i32, PartitionState>>,
    /// Live tokens only. A token leaves on ack, on re-booking of its offset, on a nack of any offset
    /// in its partition, and on reset of its partition; from then on it is stale and settles nothing.
    bookings: HashMap<i64, Booking>,
    /// This tracker's own token mint: a token settles only on the tracker that issued it.
    tokens: TokenMint,
}

impl Inner {
    fn partition_mut(&mut self, topic: &str, partition: i32) -> Option<&mut PartitionState> {
        self.topics.get_mut(topic)?.get_mut(&partition)
    }
}

/// The per-partition commit watermark, behind a single lock. One tracker serves every partition a
/// consumer is assigned: a commit is per partition, so one partition's progress never settles
/// another's.
///
/// The bookkeeping holds only while the partition is assigned. A revoked partition's unsettled
/// offsets are the new owner's to redeliver from the last committed watermark (at-least-once). So on
/// revoke a rebalance listener first commits what is [`due`](Self::due), while the partition is still
/// this member's, then calls [`reset`](Self::reset) for it, which retires every delivery of it still
/// queued. It may then wait, under its own deadline, for the handlers still running, so the new
/// owner never runs a key this member is running. It does not drain the queue before the commit: a
/// deep queue could outlast the rebalance, and the new owner fetches that work again from the
/// commit. A handler that finishes during the wait settles nothing, so its work runs again there.
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
    ///
    /// A negative offset is a `codec` error: Kafka numbers a partition's records from 0, so a
    /// negative one is a client fault, and committing past it would be meaningless.
    pub fn book(&self, topic: &str, partition: i32, offset: i64) -> Result<i64, Error> {
        if offset < 0 {
            return Err(Error::codec(format!(
                "offset {offset} on {topic}/{partition} is negative: a partition's offsets start at 0"
            )));
        }
        let mut inner = lock(&self.state);
        let token = inner.tokens.issue();
        // The topic is allocated only the first time it is booked; every later booking shares it.
        let topic = match inner.topics.get_key_value(topic) {
            Some((shared, _)) => Arc::clone(shared),
            None => Arc::from(topic),
        };
        let state = inner
            .topics
            .entry(Arc::clone(&topic))
            .or_default()
            .entry(partition)
            .or_insert_with(|| PartitionState::new(offset));
        let replaced = state.inflight.insert(offset, token);
        state.first = state.first.min(offset);
        state.max_delivered = state.max_delivered.max(offset);
        if let Some(stale) = replaced {
            inner.bookings.remove(&stale);
        }
        inner.bookings.insert(token, Booking { topic, partition, offset });
        Ok(token)
    }

    /// Whether `token` still settles its delivery. A dispatcher skips a queued delivery whose token
    /// is not live — no handler run and no settlement — because the transport fetches that offset
    /// again, behind the earlier one a nack rewound to.
    pub fn is_live(&self, token: i64) -> bool {
        lock(&self.state).bookings.contains_key(&token)
    }

    /// Settle `token` as handled.
    ///
    /// A stale or unknown token settles nothing: it belongs to a delivery already settled, to a
    /// partition since reset or rewound, or to a booking a redelivery replaced. What to commit is
    /// [`due`](Self::due)'s answer, so acks never wait on a commit round trip.
    pub fn ack(&self, token: i64) -> AckDecision {
        let mut inner = lock(&self.state);
        let Some(booking) = inner.bookings.remove(&token) else {
            return AckDecision::Stale;
        };
        // A live booking always has its partition — `reset` drops both together — so this arm is
        // unreachable by construction. It is an `else` and not an index because a kernel inside a
        // binding answers, it never panics.
        let Some(state) = inner.partition_mut(&booking.topic, booking.partition) else {
            return AckDecision::Stale;
        };
        state.inflight.remove(&booking.offset);
        AckDecision::Settled
    }

    /// The watermark of every partition whose watermark is higher than the last one committed,
    /// ordered by topic then partition. The shell commits them in one call and reports each back
    /// with [`committed`](Self::committed); a commit that failed is due again next time.
    pub fn due(&self) -> Vec<Due> {
        lock(&self.state)
            .topics
            .iter()
            .flat_map(|(topic, partitions)| {
                partitions.iter().filter_map(move |(partition, state)| {
                    state.due().map(|offset| Due { topic: topic.to_string(), partition: *partition, offset })
                })
            })
            .collect()
    }

    /// Record that the client committed `offset` on this partition.
    ///
    /// Max-merge, so two commits that returned out of order leave the higher watermark recorded. A
    /// no-op for a partition never booked or since reset: recording a watermark for one nobody owns
    /// would refuse a lower, correct commit if it were assigned back.
    pub fn committed(&self, topic: &str, partition: i32, offset: i64) {
        if let Some(state) = lock(&self.state).partition_mut(topic, partition) {
            state.committed = state.committed.max(Some(offset));
        }
    }

    /// Settle `token` as failed: rewind its partition, and report the offset the shell should seek to.
    ///
    /// The seek target is the lowest offset in flight, not this one: `seek` sets the partition's
    /// single fetch position, so nacking 5 then 7 would move it to 7 and 5 would never come back.
    /// Every offset at or above the target is fetched again, so every token of the partition is
    /// retired: a copy already fetched would otherwise run ahead of the redelivered earlier offset of
    /// its key. The offsets stay in flight until their redelivered copies settle, so the watermark
    /// cannot pass them.
    ///
    /// A retired offset the log no longer holds when the fetch comes back (compaction removed it in
    /// between) pins the watermark until the partition is reset by a rebalance or a restart.
    pub fn nack(&self, token: i64) -> NackDecision {
        let mut inner = lock(&self.state);
        let Inner { topics, bookings, .. } = &mut *inner;
        let Some(booking) = bookings.get(&token) else {
            return NackDecision::Stale;
        };
        let (partition, offset) = (booking.partition, booking.offset);
        let Some(state) = topics.get_mut(&*booking.topic).and_then(|partitions| partitions.get(&partition)) else {
            return NackDecision::Stale;
        };
        // The nacked offset is still in flight, so the map is never empty here; its own offset is
        // the answer either way.
        let target = state.inflight.keys().next().copied().unwrap_or(offset);
        for stale in state.inflight.values() {
            bookings.remove(stale);
        }
        NackDecision::Seek { partition, offset: target }
    }

    /// Drop a revoked partition's bookkeeping and retire its tokens.
    ///
    /// Its unsettled offsets are the new owner's to redeliver from the last committed watermark. A
    /// watermark left behind here would refuse a lower, correct commit if the partition came back,
    /// and a token left live would settle a delivery the tracker no longer owns.
    pub fn reset(&self, topic: &str, partition: i32) {
        let mut inner = lock(&self.state);
        let Some(partitions) = inner.topics.get_mut(topic) else {
            return;
        };
        let Some(state) = partitions.remove(&partition) else {
            return;
        };
        if partitions.is_empty() {
            inner.topics.remove(topic);
        }
        for token in state.inflight.values() {
            inner.bookings.remove(token);
        }
    }

    /// A snapshot of every assigned partition, ordered by topic then partition.
    pub fn stats(&self) -> Vec<PartitionStats> {
        let inner = lock(&self.state);
        inner
            .topics
            .iter()
            .flat_map(|(topic, partitions)| {
                let bookings = &inner.bookings;
                partitions.iter().map(move |(partition, state)| PartitionStats {
                    topic: topic.to_string(),
                    partition: *partition,
                    unsettled: count(state.inflight.len()),
                    live: count(state.inflight.values().filter(|token| bookings.contains_key(token)).count()),
                    max_delivered: state.max_delivered,
                    committed: state.committed,
                })
            })
            .collect()
    }
}

/// Saturating: `usize` is the machine's, `u64` is the contract's. They differ only on a 128-bit
/// target, and never with this many offsets in flight.
fn count(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

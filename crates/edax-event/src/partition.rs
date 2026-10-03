//! Local fan-out: which of a consumer's `W` workers runs an aggregate's events.
//!
//! `partition = fnv1a32(aggregate_id) % W`. A key-ordered dispatcher (both SDKs'
//! `KeyOrderedDispatcher`) calls it so every event of one aggregate runs on one worker, in order. The
//! choice never leaves the process: the broker has already routed the aggregate to one consumer, so
//! a sibling consumer with a different `W` never sees the same id and cannot disagree about it.
//!
//! This is **not** broker placement. Which Kafka partition a record lands on must agree with every
//! producer on the topic, Java and Kafka Connect included, so it is Kafka's own murmur2 partitioner
//! and lives in `edax-kafka` (`partition_for`). Ids are hashed as their UTF-8 bytes with no Unicode
//! normalization: the NFC and NFD spellings of one name are two ids. `testdata/golden/partition.json`
//! pins the function.

use edax_core::fnv1a32;

/// Map an aggregate id to one of `n` local workers. `n` is clamped to at least 1.
pub fn partition(aggregate_id: &str, n: u32) -> u32 {
    fnv1a32(aggregate_id.as_bytes()) % n.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_is_stable_and_bounded() {
        assert_eq!(partition("agg-1", 256), fnv1a32(b"agg-1") % 256);
        for n in [1u32, 2, 7, 256, 1024] {
            assert!(partition("whatever", n) < n);
        }
    }

    #[test]
    fn n_zero_does_not_panic() {
        assert_eq!(partition("x", 0), 0);
    }
}

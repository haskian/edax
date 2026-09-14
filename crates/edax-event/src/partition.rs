//! Consistent-hash partitioning. The single function every service must agree on bit-for-bit,
//! so the same aggregate id always maps to the same logical partition regardless of language.
//!
//! `partition = fnv1a32(aggregate_id) % N`, where `N` is a deployment-wide constant.
//!
//! NB: keep `N` (global logical partition count) separate from a consumer's local worker count
//! `W`. Worker assignment is `partition % W`, done by the consumer. Hashing directly by `W` would
//! make two consumers with different worker counts disagree on routing — so the kernel never does.
//!
//! A local fan-out pool is the one place `W` may be passed as `n` directly, and both SDKs'
//! `KeyOrderedDispatcher` does exactly that. It is safe there because the choice never leaves the
//! process: the broker has already routed the aggregate to one consumer, so a sibling consumer
//! with a different `W` never sees the same id and cannot disagree about it.

use edax_core::{fnv1a32, fnv1a64};

/// Map an aggregate id to one of `n` logical partitions. `n` is clamped to at least 1.
pub fn partition(aggregate_id: &str, n: u32) -> u32 {
    fnv1a32(aggregate_id.as_bytes()) % n.max(1)
}

/// Resize-friendly routing via rendezvous (highest-random-weight) hashing.
///
/// Picks the node in `0..n` with the highest `fnv1a64(aggregate_id ++ node)` score (ties broken
/// toward the lowest node index). Unlike `partition`'s `% n`, growing or shrinking `n` only remaps
/// the `~1/n` of ids whose winner actually changed, so consumers can rescale partitions without
/// reshuffling almost everything. `n` is clamped to at least 1.
///
/// This is an additive alternative to `partition` — pick one routing function per deployment and
/// keep it fixed; the two are not interchangeable mid-flight.
pub fn rendezvous(aggregate_id: &str, n: u32) -> u32 {
    let aggregate_id = aggregate_id.as_bytes();
    let n = n.max(1);
    // Score buffer is `id ++ node_le`; only the trailing 4 bytes change per node.
    let mut buf = Vec::with_capacity(aggregate_id.len() + 4);
    buf.extend_from_slice(aggregate_id);
    buf.extend_from_slice(&[0u8; 4]);
    let tail = aggregate_id.len();

    let mut best_node = 0u32;
    let mut best_score = 0u64;
    for node in 0..n {
        buf[tail..].copy_from_slice(&node.to_le_bytes());
        let score = fnv1a64(&buf);
        if score > best_score {
            best_score = score;
            best_node = node;
        }
    }
    best_node
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
        assert_eq!(rendezvous("x", 0), 0);
    }

    #[test]
    fn rendezvous_is_stable_and_bounded() {
        assert_eq!(rendezvous("agg-1", 8), rendezvous("agg-1", 8));
        for n in [1u32, 2, 7, 256] {
            assert!(rendezvous("whatever", n) < n);
        }
    }

    #[test]
    fn rendezvous_remaps_minimally_on_growth() {
        // Growing N from 8 to 9 should move only a small fraction of ids (those whose winner is
        // the new node 8), unlike modulo which would move ~8/9 of them.
        let ids: Vec<String> = (0..2000).map(|i| format!("agg-{i}")).collect();
        let moved = ids.iter().filter(|id| rendezvous(id, 8) != rendezvous(id, 9)).count();
        // Expected ~1/9 (~222). Generous bound guards the minimal-remap property without flaking.
        assert!(moved < ids.len() / 3, "moved {moved} of {} on 8->9", ids.len());
    }
}

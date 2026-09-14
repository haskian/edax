//! Causation / correlation id propagation — the rule that links a chain of events into a traceable
//! graph, agreed by every binding so a chain spanning services (one emits, another consumes and
//! emits the next) stays connected.
//!
//! Two well-known [`Metadata`] keys carry the chain; this stays in metadata so the envelope wire
//! format is untouched:
//!
//! - `correlation_id` — constant for every event in one logical flow (a request, a saga run).
//! - `causation_id` — the id of the single event that directly caused this one.
//!
//! When a parent event causes a child, [`propagate`] computes the child's two keys: the child's
//! `causation_id` is the parent's event id, and the child inherits the parent's `correlation_id`
//! (or, if the parent had none, the parent's event id seeds a new chain — making the parent the
//! root). The host merges the returned pair into the child event's metadata.
//!
//! An event with no parent starts a chain instead of extending one; [`originate`] computes its two
//! keys. Both functions return just the pair for the host to merge, and both give `correlation_id`
//! sole authority over whether a metadata is already on a chain.

use crate::domain::Metadata;

/// Metadata key whose value is constant across one logical flow.
pub const CORRELATION_ID: &str = "correlation_id";
/// Metadata key whose value is the id of the event that directly caused this one.
pub const CAUSATION_ID: &str = "causation_id";

/// Causation/correlation metadata for a child event, given its parent's metadata and event id.
///
/// `causation_id` = `parent_event_id`; `correlation_id` = the parent's `correlation_id` if present,
/// else `parent_event_id` (the parent roots the chain). Returns just these two keys for the caller
/// to merge into the child's metadata; no other parent metadata is carried.
pub fn propagate(parent_metadata: &Metadata, parent_event_id: &str) -> Metadata {
    let correlation = parent_metadata
        .get(CORRELATION_ID)
        .cloned()
        .unwrap_or_else(|| parent_event_id.to_string());
    let mut out = Metadata::new();
    out.insert(CORRELATION_ID.to_string(), correlation);
    out.insert(CAUSATION_ID.to_string(), parent_event_id.to_string());
    out
}

/// Causation/correlation metadata for an originating (root) event, given the metadata it was built
/// with and one freshly minted id.
///
/// Fills only what is missing; a supplied id is never overwritten. `correlation_id` defaults to
/// `fresh_id` (this event roots a new chain) and `causation_id` defaults to the resulting
/// `correlation_id` (a root causes itself). Returns just these two keys for the caller to merge
/// into the event's metadata; no other metadata is carried.
///
/// `fresh_id` comes from the caller because minting an id is non-deterministic. Keeping that out
/// leaves this function pure, so one golden vector pins the rule for every binding. It is unused
/// when both keys are already present.
pub fn originate(metadata: &Metadata, fresh_id: &str) -> Metadata {
    // `correlation_id` alone decides whether this metadata is already on a chain — the same
    // authority `propagate` gives it when it discards a parent's `causation_id`. So a lone
    // `causation_id` does not suppress the fresh correlation id.
    let correlation = metadata
        .get(CORRELATION_ID)
        .cloned()
        .unwrap_or_else(|| fresh_id.to_string());
    // Defaults to the *effective* correlation, not to `fresh_id`: a root has no causing event, so
    // it points at the chain it roots. `fresh_id` is not an event id and must never be claimed as one.
    let causation = metadata
        .get(CAUSATION_ID)
        .cloned()
        .unwrap_or_else(|| correlation.clone());
    let mut out = Metadata::new();
    out.insert(CORRELATION_ID.to_string(), correlation);
    out.insert(CAUSATION_ID.to_string(), causation);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(pairs: &[(&str, &str)]) -> Metadata {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn root_seeds_its_own_chain() {
        let child = propagate(&meta(&[]), "evt-root");
        assert_eq!(child[CORRELATION_ID], "evt-root");
        assert_eq!(child[CAUSATION_ID], "evt-root");
    }

    #[test]
    fn child_inherits_correlation_and_points_causation_at_parent() {
        let parent = meta(&[(CORRELATION_ID, "corr-1"), (CAUSATION_ID, "evt-1")]);
        let child = propagate(&parent, "evt-2");
        assert_eq!(child[CORRELATION_ID], "corr-1"); // inherited
        assert_eq!(child[CAUSATION_ID], "evt-2"); // parent's id, not parent's causation
        assert_eq!(child.len(), 2); // unrelated parent metadata is not carried
    }

    /// One `originate` case: branch name, input metadata, expected correlation, expected causation.
    type OriginateCase = (
        &'static str,
        &'static [(&'static str, &'static str)],
        &'static str,
        &'static str,
    );

    #[test]
    fn originate_fills_only_the_missing_keys() {
        let cases: [OriginateCase; 5] = [
            ("empty_mints_both_from_fresh_id", &[], "fresh-1", "fresh-1"),
            (
                "correlation_given_causation_follows_it",
                &[(CORRELATION_ID, "corr-1")],
                "corr-1",
                "corr-1",
            ),
            (
                "causation_given_correlation_is_fresh",
                &[(CAUSATION_ID, "evt-0")],
                "fresh-1",
                "evt-0",
            ),
            (
                "both_given_ignores_fresh_id",
                &[(CORRELATION_ID, "corr-1"), (CAUSATION_ID, "evt-0")],
                "corr-1",
                "evt-0",
            ),
            (
                "unrelated_keys_do_not_count_as_a_chain",
                &[("trace", "t-9")],
                "fresh-1",
                "fresh-1",
            ),
        ];
        for (branch, metadata, correlation, causation) in cases {
            let out = originate(&meta(metadata), "fresh-1");
            assert_eq!(out[CORRELATION_ID], correlation, "{branch}");
            assert_eq!(out[CAUSATION_ID], causation, "{branch}");
            assert_eq!(out.len(), 2, "{branch}: only the two chain keys are returned");
        }
    }

    #[test]
    fn a_root_plus_one_hop_is_a_connected_chain() {
        // The root's event id is not its correlation id — hosts mint the correlation separately —
        // so the hop has to link two different ids and still keep one flow.
        let root = originate(&meta(&[]), "corr-1");
        let child = propagate(&root, "evt-root");
        assert_eq!(root[CAUSATION_ID], root[CORRELATION_ID]); // the root causes itself
        assert_eq!(child[CORRELATION_ID], root[CORRELATION_ID]); // one flow across the hop
        assert_eq!(child[CAUSATION_ID], "evt-root"); // points back at the event that caused it
    }
}

//! The ordered consumer's staleness rule: whether an event is older than what was already applied,
//! and what the per-aggregate high-water mark becomes once an event is applied.
//!
//! A consumer keeps the highest sequence it has applied per aggregate, its watermark. For each event
//! it asks [`is_stale`]: is this an older event, a redelivery or an out-of-order copy, that must not
//! run again? After it applies an event it asks [`advance`] for the new watermark. Both questions are
//! the kernel's, so two consumers of one stream skip the same events and keep the same mark.
//!
//! This is ordering, not deduplication. An event whose sequence equals the watermark is not stale:
//! an ordinal cannot tell a redelivery of the last applied event from its first delivery, so a
//! handler whose effect is not idempotent also claims the event id (`event_idempotency`).
//! `testdata/golden/staleness.json` pins both functions and the fold over a redelivered stream.

/// Whether an event with sequence `incoming` is stale against the watermark `stored`.
///
/// - no watermark yet               -> not stale (nothing to order against)
/// - no sequence on the event       -> not stale (an unsequenced event has no place in the order)
/// - `incoming < stored`            -> stale: skip it
/// - `incoming >= stored`           -> not stale: apply it
///
/// Equal is not stale: the handler stays responsible for its own effect on that boundary.
pub fn is_stale(stored: Option<u64>, incoming: Option<u64>) -> bool {
    matches!((stored, incoming), (Some(stored), Some(incoming)) if incoming < stored)
}

/// The watermark after an event with sequence `incoming` is applied over the watermark `stored`.
///
/// The higher of the two. An unsequenced event (`None`) never lowers or erases the mark, and a lower
/// sequence never moves it back, so the next stale redelivery is still recognized as stale.
pub fn advance(stored: Option<u64>, incoming: Option<u64>) -> Option<u64> {
    stored.max(incoming)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// One row per branch of each function: (name, stored, incoming, stale, advanced).
    #[test]
    fn staleness_branches() {
        type Case = (&'static str, Option<u64>, Option<u64>, bool, Option<u64>);
        let cases: [Case; 8] = [
            ("nothing_stored_nothing_incoming", None, None, false, None),
            ("nothing_stored_first_event", None, Some(0), false, Some(0)),
            ("unsequenced_keeps_the_mark", Some(5), None, false, Some(5)),
            ("newer_advances", Some(5), Some(6), false, Some(6)),
            ("equal_is_not_stale_and_keeps_the_mark", Some(5), Some(5), false, Some(5)),
            ("older_is_stale_and_never_lowers_the_mark", Some(5), Some(4), true, Some(5)),
            ("at_the_u64_ceiling", Some(u64::MAX - 1), Some(u64::MAX), false, Some(u64::MAX)),
            ("stale_at_the_u64_ceiling", Some(u64::MAX), Some(u64::MAX - 1), true, Some(u64::MAX)),
        ];
        for (name, stored, incoming, stale, advanced) in cases {
            assert_eq!(is_stale(stored, incoming), stale, "{name}: is_stale");
            assert_eq!(advance(stored, incoming), advanced, "{name}: advance");
        }
    }

    /// The fold the review reproduced: the documented update `watermark = sequence` let the
    /// unsequenced event erase the mark, so the stale 3 applied and the mark moved back to 3.
    #[test]
    fn an_unsequenced_event_never_lets_a_stale_one_through() {
        let mut watermark = None;
        let mut applied = Vec::new();
        for incoming in [Some(1), Some(2), Some(5), None, Some(3)] {
            if !is_stale(watermark, incoming) {
                applied.push(incoming);
                watermark = advance(watermark, incoming);
            }
        }
        assert_eq!(applied, vec![Some(1), Some(2), Some(5), None]);
        assert_eq!(watermark, Some(5));
    }

    /// One delivery of an at-least-once stream: a sequenced event or an unsequenced one.
    fn delivery() -> impl Strategy<Value = Option<u64>> {
        prop_oneof![4 => (0u64..12).prop_map(Some), 1 => Just(None)]
    }

    proptest! {
        /// Any interleaving of duplicates, out-of-order copies and unsequenced events. A consumer
        /// that applies what is not stale and advances after each apply keeps three invariants: the
        /// watermark never moves back, nothing older than the mark applies, and no event at or
        /// above everything applied so far is skipped.
        #[test]
        fn a_consumer_folding_any_redelivery_order_keeps_per_key_order(stream in prop::collection::vec(delivery(), 0..40)) {
            let mut watermark: Option<u64> = None;
            let mut highest_applied: Option<u64> = None;
            for incoming in stream {
                let before = watermark;
                let fresh = match (incoming, highest_applied) {
                    (None, _) | (Some(_), None) => true,
                    (Some(seq), Some(high)) => seq >= high,
                };
                if is_stale(watermark, incoming) {
                    prop_assert!(!fresh, "a fresh event {incoming:?} was skipped at mark {watermark:?}");
                } else {
                    prop_assert!(fresh, "a stale event {incoming:?} applied over {highest_applied:?}");
                    highest_applied = highest_applied.max(incoming);
                    watermark = advance(watermark, incoming);
                }
                prop_assert!(watermark >= before, "the mark moved back from {before:?} to {watermark:?}");
                prop_assert_eq!(watermark, highest_applied, "the mark is the highest applied sequence");
            }
        }
    }
}

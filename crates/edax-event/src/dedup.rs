//! Staleness (ignore-old) decision for ordered consumers.
//!
//! A consumer keeps the highest sequence it has processed per aggregate. Before handling an
//! event it asks: should I apply this, or is it stale (a redelivery / out-of-order older event)?
//!
//! This is distinct from command idempotency-by-key (`edax-cqrs`): here a stale event is dropped by
//! comparing per-aggregate sequence; there a duplicate command is replayed by comparing a key.

/// Returns whether an incoming event should be applied given the last-applied sequence.
///
/// - no sequence on the event  -> always apply (nothing to order against)
/// - nothing stored yet         -> apply
/// - stored strictly greater    -> drop (stale); else apply
///
/// The comparison is strict `>`: an event whose sequence equals the stored one is re-applied, so
/// an at-least-once handler stays responsible for its own effect-idempotency on the boundary case.
pub fn should_apply(stored: Option<u64>, incoming: Option<u64>) -> bool {
    match (stored, incoming) {
        (Some(stored), Some(incoming)) => stored <= incoming,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_rules() {
        assert!(should_apply(None, None));
        assert!(should_apply(Some(5), None));
        assert!(should_apply(None, Some(0)));
        assert!(should_apply(Some(5), Some(6)));
        assert!(should_apply(Some(5), Some(5))); // equal -> apply (strict >)
        assert!(!should_apply(Some(5), Some(4))); // stale -> drop
    }
}

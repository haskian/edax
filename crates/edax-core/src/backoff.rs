//! Retry backoff: capped exponential with full jitter.
//!
//! The schedule is a cross-SDK decision, so it is pure and pinned by `testdata/golden/backoff.json`.
//! The randomness is an argument, not a side effect: each SDK draws a random `u64` per wait and
//! passes it as `entropy`, and a test passes a fixed one. Without jitter, every consumer that hit
//! the same downed dependency would retry in lockstep, at the same delays, for as long as it lasts.

/// A backoff policy: the first wait's ceiling and the bound on any wait, in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    /// The ceiling of the first retry's wait; each later attempt doubles it.
    pub base_millis: u64,
    /// The bound on any single wait.
    pub cap_millis: u64,
}

impl Backoff {
    /// The toolkit's default: 100 ms, doubling, capped at 30 s.
    pub const DEFAULT: Backoff = Backoff { base_millis: 100, cap_millis: 30_000 };

    /// The longest wait before retry `attempt` (0-based): `min(base * 2^attempt, cap)`. Saturates
    /// instead of overflowing, so every `u64` attempt is defined and a port that doubles in a loop
    /// cannot spin at the top of the range.
    pub fn ceiling_millis(self, attempt: u64) -> u64 {
        let factor = u32::try_from(attempt).ok().and_then(|shift| 1u64.checked_shl(shift)).unwrap_or(u64::MAX);
        self.base_millis.saturating_mul(factor).min(self.cap_millis)
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff::DEFAULT
    }
}

/// The wait in milliseconds before retry `attempt` (0-based): full jitter, `entropy` reduced into
/// `0 ..= ceiling` (see [`Backoff::ceiling_millis`]). The reduction is a plain modulo; its bias is
/// at most `ceiling / 2^64`, far below anything a wait can observe.
pub fn backoff_millis(policy: Backoff, attempt: u64, entropy: u64) -> u64 {
    let ceiling = policy.ceiling_millis(attempt);
    match ceiling.checked_add(1) {
        Some(span) => entropy % span,
        // A ceiling of `u64::MAX` spans every value, so any entropy is already in range.
        None => entropy,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn ceiling_branches() {
        let default = Backoff::DEFAULT;
        for (name, policy, attempt, want) in [
            ("first_wait_is_the_base", default, 0, 100),
            ("doubles_per_attempt", default, 2, 400),
            ("last_step_below_the_cap", default, 8, 25_600),
            ("capped", default, 9, 30_000),
            ("shift_past_63_saturates", default, 64, 30_000),
            ("attempt_past_u32_saturates", default, u64::from(u32::MAX) + 1, 30_000),
            ("zero_base_never_waits", Backoff { base_millis: 0, cap_millis: 30_000 }, 5, 0),
            ("cap_below_base_wins", Backoff { base_millis: 500, cap_millis: 50 }, 0, 50),
        ] {
            assert_eq!(policy.ceiling_millis(attempt), want, "{name}");
        }
        assert_eq!(Backoff::default(), Backoff::DEFAULT);
    }

    #[test]
    fn jitter_branches() {
        let unbounded = Backoff { base_millis: u64::MAX, cap_millis: u64::MAX };
        for (name, policy, attempt, entropy, want) in [
            ("zero_entropy_waits_nothing", Backoff::DEFAULT, 3, 0, 0),
            ("entropy_equal_to_the_ceiling_waits_the_ceiling", Backoff::DEFAULT, 0, 100, 100),
            ("entropy_wraps_past_the_ceiling", Backoff::DEFAULT, 0, 101, 0),
            ("a_full_range_ceiling_keeps_the_entropy", unbounded, 0, u64::MAX, u64::MAX),
        ] {
            assert_eq!(backoff_millis(policy, attempt, entropy), want, "{name}");
        }
    }

    proptest! {
        /// Full jitter never waits longer than the ceiling, and the ceiling never passes the cap.
        #[test]
        fn a_wait_stays_within_its_ceiling(base in any::<u64>(), cap in any::<u64>(), attempt in any::<u64>(), entropy in any::<u64>()) {
            let policy = Backoff { base_millis: base, cap_millis: cap };
            let ceiling = policy.ceiling_millis(attempt);
            prop_assert!(ceiling <= cap);
            prop_assert!(backoff_millis(policy, attempt, entropy) <= ceiling);
        }

        /// The ceiling never shrinks as attempts grow, so a later retry can wait at least as long.
        #[test]
        fn the_ceiling_is_monotonic(base in any::<u64>(), cap in any::<u64>(), attempt in 0u64..200) {
            let policy = Backoff { base_millis: base, cap_millis: cap };
            prop_assert!(policy.ceiling_millis(attempt) <= policy.ceiling_millis(attempt + 1));
        }
    }
}

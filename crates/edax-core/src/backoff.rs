//! Retry backoff schedule. Deterministic (no jitter) so it's identical across languages and
//! testable with golden vectors. If thundering-herd on a shared broker becomes a real problem, add
//! jitter host-side; randomness does not belong in the frozen kernel.

/// First retry delay.
pub const BASE_MILLIS: u64 = 100;
/// Upper bound on any single delay.
pub const CAP_MILLIS: u64 = 30_000;

/// Exponential backoff: `min(BASE * 2^attempt, CAP)` milliseconds. `attempt` is 0-based.
pub fn backoff_millis(attempt: u32) -> u64 {
    let factor = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
    BASE_MILLIS.saturating_mul(factor).min(CAP_MILLIS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule() {
        assert_eq!(backoff_millis(0), 100);
        assert_eq!(backoff_millis(1), 200);
        assert_eq!(backoff_millis(2), 400);
        assert_eq!(backoff_millis(8), 25_600);
        assert_eq!(backoff_millis(9), 30_000); // capped
        assert_eq!(backoff_millis(1000), 30_000); // no overflow, still capped
    }
}

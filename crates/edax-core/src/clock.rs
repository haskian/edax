//! Monotonic wall-clock timestamps.
//!
//! [`current_timestamp`] returns Unix time in nanoseconds, but guarantees each call yields a value
//! strictly greater than every prior call in this process — even if the wall clock stalls within a
//! nanosecond or steps backwards (NTP slew, leap, suspend). It does this with a process-local
//! high-water mark, so like [`crate::guid`] it is non-deterministic and carries no golden vector:
//! the guarantee is monotonicity, not a fixed value. The mark is per-process — it does not span
//! restarts or machines.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static LAST: AtomicU64 = AtomicU64::new(0);

/// Current Unix time in nanoseconds, strictly greater than every prior return in this process.
///
/// Tracks the wall clock when it advances; when the clock would repeat or move back, advances by
/// one nanosecond instead so the result is always unique and ordered.
pub fn current_timestamp() -> u64 {
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64) // u64 nanos overflow ~year 2554
        // A pre-1970 clock has no Unix duration. Falling back to 0 keeps the guarantee this function
        // actually makes — strictly increasing — by deferring to the high-water mark below.
        .unwrap_or(0);
    let mut last = LAST.load(Ordering::Relaxed);
    loop {
        let next = wall.max(last + 1);
        match LAST.compare_exchange_weak(last, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(observed) => last = observed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strictly_increasing() {
        let mut prev = 0;
        for _ in 0..100_000 {
            let t = current_timestamp();
            assert!(t > prev, "{t} !> {prev}"); // never repeats or goes back
            prev = t;
        }
    }
}

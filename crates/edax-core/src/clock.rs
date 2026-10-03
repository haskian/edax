//! Monotonic wall-clock timestamps.
//!
//! [`current_timestamp`] returns Unix time in nanoseconds, but guarantees each call yields a value
//! strictly greater than every prior call in this process — even if the wall clock stalls within a
//! nanosecond or steps backwards (NTP slew, leap, suspend). It does this with a process-local
//! high-water mark, so like [`crate::guid`] it is non-deterministic and carries no golden vector:
//! the guarantee is monotonicity, not a fixed value. The mark is per-process — it does not span
//! restarts or machines.
//!
//! The price of strict monotonicity: after the wall clock jumps forward and is then corrected (a VM
//! resume, a bad NTP source), every later call returns the mark plus one nanosecond until the wall
//! clock catches up again. Timestamps stay ordered but sit at the bad future value for that long,
//! and a lag computed from them reads short. Nothing in-process can tell such a jump from real time.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static LAST: AtomicI64 = AtomicI64::new(0);

/// Current Unix time in nanoseconds, strictly greater than every prior return in this process.
///
/// `i64`, the envelope's timestamp type, so no caller narrows. Tracks the wall clock when it
/// advances; when the clock would repeat or move back, advances by one nanosecond instead so the
/// result is always unique and ordered. Saturates at `i64::MAX` (the year 2262).
pub fn current_timestamp() -> i64 {
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        // A pre-1970 clock has no Unix duration. Falling back to 0 keeps the guarantee this function
        // actually makes — strictly increasing — by deferring to the high-water mark below.
        .unwrap_or(0);
    let mut last = LAST.load(Ordering::Relaxed);
    loop {
        let next = wall.max(last.saturating_add(1));
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

    /// The cross-thread half of the guarantee: the compare-and-swap loop hands every caller a
    /// distinct value, and each thread still sees its own calls strictly increase.
    #[test]
    fn threads_never_share_a_timestamp() {
        let per_thread: Vec<Vec<i64>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| (0..20_000).map(|_| current_timestamp()).collect::<Vec<i64>>()))
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        for stamps in &per_thread {
            assert!(stamps.windows(2).all(|w| w[0] < w[1]), "a thread saw time stand still");
        }
        let mut all: Vec<i64> = per_thread.into_iter().flatten().collect();
        let total = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), total, "two calls returned the same timestamp");
    }
}

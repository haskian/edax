//! How a retry waits: the kernel's jittered [`Backoff`] policy, the clock it waits on, and the
//! randomness it draws. [`retry`](crate::retry) and the
//! [`KeyOrderedDispatcher`](crate::KeyOrderedDispatcher) take one, so a host can tune the schedule
//! and a test can wait in no wall time.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use edax_core::{backoff_millis, Backoff};

/// Blocks the calling thread for a duration.
pub type Sleeper = Arc<dyn Fn(Duration) + Send + Sync>;
/// Draws a random `u64` for one wait's jitter.
pub type Entropy = Arc<dyn Fn() -> u64 + Send + Sync>;

/// The schedule a retry waits on, the sleep that waits it out, and the source of its jitter.
/// `Default` is the kernel's policy, a real thread sleep and a fresh random draw per wait.
#[derive(Clone)]
pub struct Pacing {
    /// The schedule: capped exponential, full jitter (see [`edax_core::backoff_millis`]).
    pub backoff: Backoff,
    /// How a wait is spent. A test passes a recorder instead of paying the wait in wall time.
    pub sleep: Sleeper,
    /// The random draw behind each wait's jitter. Without it, every consumer that hit the same
    /// downed dependency would retry in lockstep.
    pub entropy: Entropy,
}

impl Default for Pacing {
    fn default() -> Self {
        Pacing { backoff: Backoff::DEFAULT, sleep: Arc::new(std::thread::sleep), entropy: Arc::new(random_u64) }
    }
}

impl Pacing {
    /// The wait before retry `attempt` (0-based), as the kernel computes it from a fresh draw.
    pub fn delay(&self, attempt: u64) -> Duration {
        Duration::from_millis(backoff_millis(self.backoff, attempt, (self.entropy)()))
    }

    /// Wait out [`delay`](Pacing::delay) on this pacing's sleeper.
    pub fn wait(&self, attempt: u64) {
        (self.sleep)(self.delay(attempt));
    }

    /// The kernel's schedule, waited out in no time: for a test, or a host that paces its own loop.
    pub fn instant() -> Self {
        Pacing { sleep: Arc::new(|_| {}), ..Pacing::default() }
    }

    /// The kernel's schedule, recorded instead of slept, for a test. The draw is fixed at `u64::MAX`,
    /// so each recorded wait is its step's [`delay`](Pacing::delay) and a test reads the step back
    /// off it.
    pub fn recording() -> (Self, Arc<Mutex<Vec<Duration>>>) {
        let waits: Arc<Mutex<Vec<Duration>>> = Arc::default();
        let sink = Arc::clone(&waits);
        let pacing = Pacing {
            sleep: Arc::new(move |d| edax_core::lock(&sink).push(d)),
            entropy: Arc::new(|| u64::MAX),
            ..Pacing::default()
        };
        (pacing, waits)
    }
}

/// A random `u64` from the standard library alone: every `RandomState` is seeded with fresh keys,
/// so hashing nothing under a new one is a fresh draw. Jitter needs spread, not cryptography.
fn random_u64() -> u64 {
    RandomState::new().build_hasher().finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_wait_is_the_kernels_delay_for_the_drawn_entropy() {
        let slept: Arc<Mutex<Vec<Duration>>> = Arc::default();
        let recorder = Arc::clone(&slept);
        let pacing = Pacing {
            backoff: Backoff { base_millis: 100, cap_millis: 1_000 },
            sleep: Arc::new(move |d| recorder.lock().unwrap().push(d)),
            entropy: Arc::new(|| 150),
        };
        for attempt in [0, 1, 4] {
            pacing.wait(attempt);
        }
        // 150 % 101, 150 % 201, and the cap: 150 % 1001.
        let want = [49, 150, 150].map(Duration::from_millis);
        assert_eq!(*slept.lock().unwrap(), want);
    }

    #[test]
    fn the_default_draws_fresh_entropy_within_the_kernels_ceiling() {
        let pacing = Pacing::default();
        assert_eq!(pacing.backoff, Backoff::DEFAULT);
        let draws: Vec<u64> = (0..64).map(|_| (pacing.entropy)()).collect();
        assert!(draws.windows(2).any(|w| w[0] != w[1]), "the draw is not constant");
        for attempt in 0..20 {
            assert!(pacing.delay(attempt) <= Duration::from_millis(Backoff::DEFAULT.ceiling_millis(attempt)));
        }
    }
}

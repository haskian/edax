//! Locking inside a native binding.

use std::sync::{Mutex, MutexGuard};

/// Lock a mutex, recovering from poisoning instead of panicking.
///
/// These kernels run inside a native binding, where an unwinding panic crosses the host language's
/// FFI boundary. Nothing inside a guarded section can panic, so the state behind a poisoned lock is
/// still consistent — take it back rather than propagate a panic the host cannot catch.
pub fn lock<T>(state: &Mutex<T>) -> MutexGuard<'_, T> {
    state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
// A test asserts on the value it just built; a panic there is the failure report.
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// The poisoning branch, which no other test reaches. The panic it stages prints its own
    /// message during the run; that output is the test working, not the test failing.
    #[test]
    fn a_poisoned_lock_still_hands_back_its_state() {
        let state = Mutex::new(7);
        let poisoner = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let mut guard = lock(&state);
                    *guard = 9;
                    panic!("poison the lock");
                })
                .join()
        });
        assert!(poisoner.is_err(), "the guarded thread panicked, so the lock is poisoned");
        assert!(state.is_poisoned());
        assert_eq!(*lock(&state), 9, "the state is taken back, panic-free, with the write that landed");
    }
}

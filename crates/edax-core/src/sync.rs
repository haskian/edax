//! Locking inside a native binding.

use std::sync::{Mutex, MutexGuard};

/// Lock a mutex, recovering from poisoning instead of panicking.
///
/// These kernels run inside a native binding. PyO3 catches a panic at the function boundary, but it
/// surfaces as `PanicException`, a `BaseException` the SDK's `except Exception` arms never see, so a
/// poisoned lock must not become a second panic on every later call. The state behind it is still
/// consistent only because every caller's guarded section is panic-free: the workspace lints deny
/// `unwrap`, `expect` and indexing in every crate that takes this lock.
pub fn lock<T>(state: &Mutex<T>) -> MutexGuard<'_, T> {
    state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
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
        assert!(
            poisoner.is_err(),
            "the guarded thread panicked, so the lock is poisoned"
        );
        assert!(state.is_poisoned());
        assert_eq!(
            *lock(&state),
            9,
            "the state is taken back, panic-free, with the write that landed"
        );
    }
}

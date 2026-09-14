//! Shared test support for the integration suites.

use std::sync::Mutex;

/// A thread-safe `Cell`: one slot the guard, the handler and the test all read. The command path is
/// `Send + Sync`, so a `Cell` no longer fits.
#[derive(Default)]
pub struct Shared<T>(Mutex<T>);

impl<T: Copy + Default> Shared<T> {
    #[allow(dead_code)] // not every suite seeds a non-default slot
    pub fn new(value: T) -> Self {
        Self(Mutex::new(value))
    }

    pub fn get(&self) -> T {
        self.0.lock().map(|v| *v).unwrap_or_default()
    }

    pub fn set(&self, value: T) {
        if let Ok(mut slot) = self.0.lock() {
            *slot = value;
        }
    }
}

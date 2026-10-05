//! Shared test support for the integration suites. Each suite compiles this module on its own and uses
//! only part of it.

#![allow(
    dead_code,
    unused_imports,
    reason = "each suite uses only part of the shared support"
)]

use std::sync::Mutex;

use edax::testkit;
use edax::EventDraft;

// The unit suites' fixtures, so both kinds of suite start from one base event.
pub(crate) use testkit::RECORDED_AT;

/// The scope the command guard claims under.
pub const SCOPE: &str = "deposits";

/// A bare draft of event `name` with id `id` at `sequence` of `aggregate_id`; callers override the
/// fields under test.
pub fn draft(id: impl Into<String>, aggregate_id: &str, name: &str, sequence: u64) -> EventDraft {
    EventDraft {
        id: id.into(),
        ..testkit::draft(aggregate_id, name, sequence, &[])
    }
}

/// A thread-safe `Cell`: one slot the guard, the handler and the test all read. A `Mutex`, because
/// the command path is `Send + Sync`.
#[derive(Default)]
pub struct Shared<T>(Mutex<T>);

impl<T: Copy + Default> Shared<T> {
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

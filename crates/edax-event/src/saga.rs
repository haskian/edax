//! The saga step's persistence decision, and the shape of the commands a saga sends through its
//! outbox.
//!
//! A saga runner loads one correlation's instance, folds an event into it, and must then persist
//! the new state *and* the commands the fold decided in one versioned write: the saga outbox. A
//! relay sends the commands only after that write commits, so a fold that loses the version race
//! sends nothing, and a crash between the write and the send re-sends from the outbox instead of
//! losing the command. Completion writes a tombstone rather than removing the row, so a stale
//! writer conflicts on the bumped version and a late event finds the instance completed instead of
//! starting it again. Two SDKs running one saga over one store must agree on all of it, so the
//! decision is here, pinned by `testdata/golden/saga.json`.

use serde::{Deserialize, Serialize};

use crate::domain::Event;

/// What the store holds for a correlation when the runner loads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Loaded {
    /// No instance: the event may start one.
    Absent,
    /// A running instance at `version`.
    Active {
        /// The version the fold is based on.
        version: u64,
    },
    /// A completed instance (a tombstone) at `version`.
    Completed {
        /// The tombstone's version.
        version: u64,
    },
}

/// The write a saga step makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum SagaWrite {
    /// The instance is completed: discard the event without folding it. A late or redelivered
    /// event must not start a finished workflow again.
    Ignore,
    /// Nothing was loaded and the fold neither keeps state nor sends a command: write nothing, so a
    /// concurrent creator of the same correlation is not made to conflict with a no-op.
    Nothing,
    /// Save the new state and the commands at `expected + 1`, if the store is still at `expected`.
    Save {
        /// The version the write is conditional on.
        expected: u64,
    },
    /// Write the tombstone and the commands at `expected + 1`, if the store is still at `expected`.
    Complete {
        /// The version the write is conditional on.
        expected: u64,
    },
}

/// The write for a step that loaded `loaded` and whose fold `completes` the instance (or keeps it
/// running) and emitted commands or not (`has_commands`).
pub fn saga_write(loaded: Loaded, completes: bool, has_commands: bool) -> SagaWrite {
    let expected = match loaded {
        Loaded::Completed { .. } => return SagaWrite::Ignore,
        Loaded::Absent if completes && !has_commands => return SagaWrite::Nothing,
        Loaded::Absent => 0,
        Loaded::Active { version } => version,
    };
    if completes {
        SagaWrite::Complete { expected }
    } else {
        SagaWrite::Save { expected }
    }
}

/// The id of the `index`-th command a step committed at `version` (the version the write
/// produced) of `correlation_id`: `"{correlation_id}:{version}:{index}"`. Deterministic, so a
/// durable store can key the outbox row on it and a consumer can deduplicate a re-sent command on
/// it. Unique per committed transition: the version is the instance's, and the last two fields
/// hold no `:`, so the id parses back from the right.
pub fn saga_command_id(correlation_id: &str, version: u64, index: u64) -> String {
    format!("{correlation_id}:{version}:{index}")
}

/// One command in a saga's outbox: committed with the step that decided it, sent by a relay after
/// the commit, marked sent by `id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaCommand {
    /// [`saga_command_id`] of the step that decided it.
    pub id: String,
    /// The saga instance that decided it.
    pub correlation_id: String,
    /// The command, marshaled by the host's codec: its name, schema version and payload.
    pub command: Event,
    /// When the step decided it, in the host clock's nanoseconds: the outbox lag a health check reads.
    pub timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saga_write_branches() {
        let cases = [
            ("completed ignores a fold that keeps state", Loaded::Completed { version: 4 }, false, true, SagaWrite::Ignore),
            ("completed ignores a fold that completes", Loaded::Completed { version: 4 }, true, false, SagaWrite::Ignore),
            ("absent, completes, no commands: nothing", Loaded::Absent, true, false, SagaWrite::Nothing),
            ("absent, completes with commands: tombstone at 0", Loaded::Absent, true, true, SagaWrite::Complete { expected: 0 }),
            ("absent, keeps state: save at 0", Loaded::Absent, false, false, SagaWrite::Save { expected: 0 }),
            ("active, keeps state: save at its version", Loaded::Active { version: 3 }, false, true, SagaWrite::Save { expected: 3 }),
            ("active, completes: tombstone at its version", Loaded::Active { version: 3 }, true, false, SagaWrite::Complete { expected: 3 }),
        ];
        for (name, loaded, completes, has_commands, want) in cases {
            assert_eq!(saga_write(loaded, completes, has_commands), want, "{name}");
        }
    }

    #[test]
    fn command_ids_are_distinct_per_transition_and_index() {
        assert_eq!(saga_command_id("order-7", 2, 0), "order-7:2:0");
        // A `:` inside the correlation id cannot alias another transition: the last two fields are numbers.
        assert_ne!(saga_command_id("a:1", 2, 0), saga_command_id("a", 1, 2));
    }
}

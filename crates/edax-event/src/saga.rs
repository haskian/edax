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
//!
//! A broker redelivers an event whose step committed but whose ack was lost, and a crash replays
//! every event above the committed offset. Folding one again would decide its commands a second
//! time, under new ids, so the instance remembers what it folded ([`saga_admit`]) and the runner
//! skips a repeat before it folds. A completed instance folds nothing at all, so the same answer
//! skips an event that finds a tombstone.

use std::collections::BTreeMap;

use edax_core::{scoped, Error};
use serde::{Deserialize, Serialize};

use crate::causation::propagate;
use crate::domain::{CommandMessage, Event, EventMessage, Extensions, Metadata};
use crate::staleness::{advance, is_stale};

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
}

/// The write a saga step makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum SagaWrite {
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
/// running) and emitted commands or not (`has_commands`). A completed instance never gets here:
/// [`saga_admit`] skips every event that finds one.
pub fn saga_write(loaded: Loaded, completes: bool, has_commands: bool) -> SagaWrite {
    let expected = match loaded {
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

/// The last event a saga instance folded from one source aggregate.
#[derive(Debug, Serialize, Deserialize)]
struct Mark {
    /// The highest sequence folded from the aggregate; absent while every one was unsequenced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sequence: Option<u64>,
    /// The id of the last event folded from the aggregate.
    id: String,
}

/// Whether a saga step folds an event, from what the instance has folded so far.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum SagaAdmit {
    /// The instance is completed: discard the event without folding it. A late or redelivered
    /// event must not start a finished workflow again.
    Completed,
    /// The instance folded this event already: a redelivery or a replay after a crash. Skip it and
    /// write nothing, so its commands are not decided a second time.
    Repeat,
    /// Fold it, and save `folded` with the step's write.
    Fold {
        /// What the instance has folded once this step commits: the store keeps it as opaque text.
        folded: String,
    },
}

/// Whether an instance that has folded `folded` folds `env`, or is `completed` (a tombstone) and
/// folds nothing. `folded` is the text the last write saved, empty for a new instance; a store keeps
/// it as it is and never reads it.
///
/// The instance keeps one mark per source aggregate: the highest sequence and the last event id
/// folded from it. An event is a repeat when its id is the mark's, or when its sequence is below
/// the mark's ([`is_stale`]): a dispatcher hands one aggregate's events over in order, so a lower
/// one was folded before. An unsequenced event is a repeat only of the last one folded from its
/// aggregate. The marks grow by one per source aggregate, not per event.
///
/// # Errors
/// A `codec` error when `folded` is not text this function wrote.
pub fn saga_admit(completed: bool, folded: &str, env: &EventMessage) -> Result<SagaAdmit, Error> {
    if completed {
        return Ok(SagaAdmit::Completed);
    }
    let mut marks: BTreeMap<String, Mark> = if folded.is_empty() {
        BTreeMap::new()
    } else {
        serde_json::from_str(folded).map_err(|e| Error::codec(format_args!("saga folded marks {folded:?}: {e}")))?
    };
    let stored = marks.get(&env.aggregate_id);
    if stored.is_some_and(|mark| mark.id == env.id || is_stale(mark.sequence, env.sequence)) {
        return Ok(SagaAdmit::Repeat);
    }
    let sequence = advance(stored.and_then(|mark| mark.sequence), env.sequence);
    marks.insert(env.aggregate_id.clone(), Mark { sequence, id: env.id.clone() });
    let folded = serde_json::to_string(&marks).map_err(Error::codec)?;
    Ok(SagaAdmit::Fold { folded })
}

/// The id of the `index`-th command a step committed at `version` (the version the write
/// produced) of `correlation_id`: `"{correlation_id}:{version}:{index}"`. Deterministic, so a
/// durable store can key the outbox row on it and a consumer can deduplicate a re-sent command on
/// it. Unique per committed transition: the version is the instance's, and the last two fields
/// hold no `:`, so the id parses back from the right.
pub fn saga_command_id(correlation_id: &str, version: u64, index: u64) -> String {
    format!("{correlation_id}:{version}:{index}")
}

/// The `index`-th command the instance `instance_id` of the saga named `saga` decided at the step that
/// committed `version`, folding the event `cause_id` of the flow `cause_correlation_id`: the row its
/// outbox holds and the message its relay sends.
///
/// Both SDKs build it here, so one saga run over one store writes the same row from either:
///
/// - `id` is [`saga_command_id`]: the outbox row's key, unique within the saga's store;
/// - `idempotency_key` is that id [`scoped`] by the saga's name, `"{len(saga)}:{saga}:{id}"`: a re-sent row
///   keeps it, so the consuming bus deduplicates it, and two sagas that share an instance id and a
///   bus never replay or refuse each other's commands. The name's byte length keeps it injective;
/// - `aggregate_id` is the instance, so one instance's commands keep their order on every transport
///   and a store can find its rows;
/// - the lineage is the event's child ([`propagate`]): causation is the folded event, correlation is
///   the event's flow, not the instance id.
///
/// Pinned by `testdata/golden/saga.json`.
// The cause is two strings, not the event, because only its id and correlation are read: a host that
// holds them apart (a binding, say) need not rebuild a whole event to pass them.
#[allow(clippy::too_many_arguments)]
pub fn saga_command(
    saga: &str,
    instance_id: &str,
    version: u64,
    index: u64,
    command: Event,
    cause_id: &str,
    cause_correlation_id: &str,
    issued_at: i64,
) -> CommandMessage {
    let id = saga_command_id(instance_id, version, index);
    let lineage = propagate(cause_id, cause_correlation_id);
    CommandMessage {
        idempotency_key: Some(scoped(saga, &id)),
        id,
        aggregate_id: Some(instance_id.to_owned()),
        name: command.name,
        version: command.version,
        data: command.data,
        correlation_id: lineage.correlation_id,
        causation_id: lineage.causation_id,
        issued_at,
        extensions: Extensions::new(),
        metadata: Metadata::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::base_event;

    #[test]
    fn saga_write_branches() {
        let cases = [
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

    fn event(aggregate_id: &str, sequence: Option<u64>, id: &str) -> EventMessage {
        EventMessage { id: id.to_owned(), aggregate_id: aggregate_id.to_owned(), sequence, ..base_event() }
    }

    #[test]
    fn saga_admit_branches() {
        let fold = |folded: &str| Ok(SagaAdmit::Fold { folded: folded.to_owned() });
        let one = r#"{"order-7":{"sequence":1,"id":"evt-1"}}"#;
        let two = r#"{"order-7":{"sequence":2,"id":"evt-2"}}"#;
        let unsequenced = r#"{"order-7":{"id":"evt-u"}}"#;
        let cases: [(&str, &str, EventMessage, Result<SagaAdmit, String>); 11] = [
            ("a new instance folds and marks", "", event("order-7", Some(1), "evt-1"), fold(one)),
            ("the last event again is a repeat", one, event("order-7", Some(1), "evt-1"), Ok(SagaAdmit::Repeat)),
            ("an older event of the aggregate is a repeat", two, event("order-7", Some(1), "evt-1"), Ok(SagaAdmit::Repeat)),
            ("the next event folds and moves the mark", one, event("order-7", Some(2), "evt-2"), fold(two)),
            (
                "another aggregate folds beside the mark",
                one,
                event("payment-7", Some(1), "evt-p"),
                fold(r#"{"order-7":{"sequence":1,"id":"evt-1"},"payment-7":{"sequence":1,"id":"evt-p"}}"#),
            ),
            ("an unsequenced event repeats only the last id", unsequenced, event("order-7", None, "evt-u"), Ok(SagaAdmit::Repeat)),
            ("another unsequenced event folds", unsequenced, event("order-7", None, "evt-v"), fold(r#"{"order-7":{"id":"evt-v"}}"#)),
            ("an unsequenced event keeps the sequence mark", two, event("order-7", None, "evt-v"), fold(r#"{"order-7":{"sequence":2,"id":"evt-v"}}"#)),
            ("an equal sequence under another id folds", one, event("order-7", Some(1), "evt-x"), fold(r#"{"order-7":{"sequence":1,"id":"evt-x"}}"#)),
            ("a sequence after unsequenced events folds", unsequenced, event("order-7", Some(3), "evt-3"), fold(r#"{"order-7":{"sequence":3,"id":"evt-3"}}"#)),
            (
                "marks this function did not write are a codec error",
                "[1]",
                event("order-7", Some(1), "evt-1"),
                Err(r#"codec: saga folded marks "[1]": invalid type: sequence, expected a map at line 1 column 0"#.to_owned()),
            ),
        ];
        for (name, folded, env, want) in cases {
            assert_eq!(saga_admit(false, folded, &env).map_err(|e| e.to_string()), want, "{name}");
        }
        // A tombstone folds nothing, whatever the marks and the event: unread marks are no error.
        assert_eq!(saga_admit(true, "[1]", &event("order-7", Some(1), "evt-1")).map_err(|e| e.to_string()), Ok(SagaAdmit::Completed));
    }

    #[test]
    fn command_ids_are_distinct_per_transition_and_index() {
        assert_eq!(saga_command_id("order-7", 2, 0), "order-7:2:0");
        // A `:` inside the correlation id cannot alias another transition: the last two fields are numbers.
        assert_ne!(saga_command_id("a:1", 2, 0), saga_command_id("a", 1, 2));
    }
}

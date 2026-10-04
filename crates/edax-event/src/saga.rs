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
//!
//! A command the saga sent can die: its handler rejects it, and the relay hands the row to its
//! dead-letter sink. Nothing then tells the instance, which waits for a reply that never comes.
//! [`saga_command_failed`] is the reply the sink folds back instead: an event for the instance
//! that names the dead command and its error.

use std::collections::{BTreeMap, BTreeSet};

use edax_core::{scoped, Error, ErrorCode};
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
    /// Save the new state and the commands at `version`, if the store is still at `expected`.
    Save {
        /// The version the write is conditional on.
        expected: u64,
        /// The version the write commits, `expected + 1`: the one the step's command ids carry.
        version: u64,
    },
    /// Write the tombstone and the commands at `version`, if the store is still at `expected`.
    Complete {
        /// The version the write is conditional on.
        expected: u64,
        /// The version the write commits, `expected + 1`: the one the step's command ids carry.
        version: u64,
    },
}

/// The write for a step that loaded `loaded` and whose fold `completes` the instance (or keeps it
/// running) and emitted commands or not (`has_commands`). A completed instance never gets here:
/// [`saga_admit`] skips every event that finds one.
///
/// An instance at the last `u64` version has no version to commit: a `codec` error, the same on
/// every retry.
pub fn saga_write(loaded: Loaded, completes: bool, has_commands: bool) -> Result<SagaWrite, Error> {
    let expected = match loaded {
        Loaded::Absent if completes && !has_commands => return Ok(SagaWrite::Nothing),
        Loaded::Absent => 0,
        Loaded::Active { version } => version,
    };
    let version = expected
        .checked_add(1)
        .ok_or_else(|| Error::codec(format_args!("saga instance at version {expected} has no next version")))?;
    Ok(if completes { SagaWrite::Complete { expected, version } } else { SagaWrite::Save { expected, version } })
}

/// What a saga instance folded from one source aggregate. The sequenced and the unsequenced events
/// keep apart: an unsequenced event that overwrote the id guarding the sequence would let a replay of
/// the sequenced one fold again. The failures of the instance's own commands keep apart too, every one
/// of them: a relay that lost its mark replays one dead row after another died, and a failure folded
/// twice compensates twice.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Mark {
    /// The highest sequence folded from the aggregate; absent while every one was unsequenced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sequence: Option<u64>,
    /// The id of the last sequenced event folded from the aggregate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    /// The id of the last unsequenced event folded from the aggregate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    unsequenced: Option<String>,
    /// The id of every [`SAGA_COMMAND_FAILED`] event folded into the instance: one per command it sent
    /// at most, so bounded by them.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    failed: BTreeSet<String>,
}

/// Whether a saga step folds an event, from what the instance has folded so far.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum SagaAdmit {
    /// The instance is completed: discard the event without folding it. A late or redelivered
    /// event must not start a finished workflow again.
    Completed,
    /// The instance folded this event already: its id is one the instance kept. A redelivery or a
    /// replay after a crash. Skip it and write nothing, so its commands are not decided a second time.
    Repeat,
    /// The event's sequence is below the highest one the instance folded from its aggregate. In order
    /// delivery folded it before, so skip it like a repeat. Order does not prove it: an event that was
    /// dead-lettered and then re-driven after a later one folded lands here too, and is never folded.
    /// A runner reports this answer apart from [`SagaAdmit::Repeat`], so that loss is visible.
    BelowMark,
    /// Fold it, and save `folded` with the step's write.
    Fold {
        /// What the instance has folded once this step commits: the store keeps it as opaque text.
        folded: String,
    },
}

/// Whether an instance that has folded `folded` folds `event`, or is `completed` (a tombstone) and
/// folds nothing. `folded` is the text the last write saved, empty for a new instance; a store keeps
/// it as it is and never reads it.
///
/// The instance keeps one mark per source aggregate: the highest sequence folded from it with that
/// event's id, and the id of the last unsequenced event folded from it, apart. An event is a
/// [`SagaAdmit::Repeat`] when its id is one of the two. It is [`SagaAdmit::BelowMark`] when its
/// sequence is below the mark's ([`is_stale`]): a dispatcher hands one aggregate's events over in
/// order, so a lower one was folded before. That holds only while no event of the aggregate was
/// dead-lettered: once a later one folded, a re-driven dead letter is below the mark and never
/// folds. An unsequenced event is a repeat only of the last unsequenced one folded from its
/// aggregate, except a [`SAGA_COMMAND_FAILED`] one: the instance keeps the id of every failure it
/// folded, so each dead command folds once. The marks grow by one per source aggregate and by one per
/// dead command, not per event.
///
/// # Errors
/// A `codec` error when `folded` is not text this function wrote.
pub fn saga_admit(completed: bool, folded: &str, event: &EventMessage) -> Result<SagaAdmit, Error> {
    if completed {
        return Ok(SagaAdmit::Completed);
    }
    let mut marks: BTreeMap<String, Mark> = if folded.is_empty() {
        BTreeMap::new()
    } else {
        serde_json::from_str(folded).map_err(|e| Error::codec(format_args!("saga folded marks {folded:?}: {e}")))?
    };
    let mark = marks.entry(event.aggregate_id.clone()).or_default();
    if [&mark.id, &mark.unsequenced].into_iter().any(|id| id.as_deref() == Some(event.id.as_str()))
        || mark.failed.contains(&event.id)
    {
        return Ok(SagaAdmit::Repeat);
    }
    if is_stale(mark.sequence, event.sequence) {
        return Ok(SagaAdmit::BelowMark);
    }
    if event.sequence.is_some() {
        mark.sequence = advance(mark.sequence, event.sequence);
        mark.id = Some(event.id.clone());
    } else if event.name == SAGA_COMMAND_FAILED {
        mark.failed.insert(event.id.clone());
    } else {
        mark.unsequenced = Some(event.id.clone());
    }
    let folded = serde_json::to_string(&marks).map_err(Error::codec)?;
    Ok(SagaAdmit::Fold { folded })
}

/// The id of the `index`-th command a step committed at `version` (the version the write
/// produced) of the instance `instance_id`: `"{instance_id}:{version}:{index}"`. Deterministic, so a
/// durable store can key the outbox row on it and a consumer can deduplicate a re-sent command on
/// it. Unique per committed transition: the version is the instance's, and the last two fields
/// hold no `:`, so the id parses back from the right.
pub fn saga_command_id(instance_id: &str, version: u64, index: u64) -> String {
    format!("{instance_id}:{version}:{index}")
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
///   and a store can find its rows. Where one store holds every tenant, that instance id is the host's
///   own composition (inside the saga's `instance_id`), so nothing here composes a second time;
/// - `tenant` is the folded event's, so a command inherits the tenancy of the fact that caused it, and
///   the bus that runs it claims inside that tenant;
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
    tenant: Option<&str>,
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
        tenant: tenant.map(ToOwned::to_owned),
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

/// The name of the event [`saga_command_failed`] builds.
pub const SAGA_COMMAND_FAILED: &str = "SagaCommandFailed";

/// The payload of a [`SAGA_COMMAND_FAILED`] event, as JSON: which command of the instance died, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaCommandFailed {
    /// The dead command's id: the [`saga_command_id`] of its outbox row.
    pub command_id: String,
    /// The dead command's name.
    pub command: String,
    /// The taxonomy code of the failure (`"rejected"` for a host's business rejection), `None` for a
    /// failure no code classifies.
    pub code: Option<String>,
    /// The failure's message, without the code.
    pub error: String,
}

/// The event a saga folds when the command `command`, one its outbox sent, died of a failure with
/// the taxonomy `code` and the message `error`: the reply a participant that rejected it never sends.
/// A relay's dead-letter sink hands it to the runner, which folds it into the instance the row
/// names, so the saga can compensate.
///
/// - `id` is `"{command.id}:failed"`: a sink that runs twice for one row builds the same event, and
///   the instance skips the second as a repeat, whatever folded between them ([`saga_admit`]);
/// - `aggregate_id` is the command's: the saga instance that sent it;
/// - `sequence` is `None`: the failure has no place in any aggregate's order;
/// - `name` is [`SAGA_COMMAND_FAILED`], `version` `1`, and `data` the JSON of [`SagaCommandFailed`];
/// - `tenant` is the command's, and the lineage is the command's child ([`propagate`]);
/// - `recorded_at` is the caller's clock, as a saga command's `issued_at` is.
///
/// Pinned by `testdata/golden/saga.json`.
///
/// # Errors
/// A `codec` error when `command` names no instance (`aggregate_id` is `None`): it is no saga's row.
pub fn saga_command_failed(
    command: &CommandMessage,
    code: Option<ErrorCode>,
    error: &str,
    recorded_at: i64,
) -> Result<EventMessage, Error> {
    let instance_id = command
        .aggregate_id
        .clone()
        .ok_or_else(|| Error::codec(format_args!("command {:?} names no saga instance", command.id)))?;
    let payload = SagaCommandFailed {
        command_id: command.id.clone(),
        command: command.name.clone(),
        code: code.map(|code| code.as_str().to_owned()),
        error: error.to_owned(),
    };
    let lineage = propagate(&command.id, &command.correlation_id);
    Ok(EventMessage {
        id: format!("{}:failed", command.id),
        aggregate_id: instance_id,
        tenant: command.tenant.clone(),
        sequence: None,
        name: SAGA_COMMAND_FAILED.to_owned(),
        version: 1,
        data: serde_json::to_vec(&payload).map_err(Error::codec)?,
        correlation_id: lineage.correlation_id,
        causation_id: lineage.causation_id,
        occurred_at: None,
        recorded_at,
        extensions: Extensions::new(),
        metadata: Metadata::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::base_event;

    #[test]
    fn saga_write_branches() {
        let save = |expected, version| Ok(SagaWrite::Save { expected, version });
        let complete = |expected, version| Ok(SagaWrite::Complete { expected, version });
        let last = Err("codec: saga instance at version 18446744073709551615 has no next version".to_owned());
        let cases = [
            ("absent, completes, no commands: nothing", Loaded::Absent, true, false, Ok(SagaWrite::Nothing)),
            ("absent, completes with commands: tombstone at 0", Loaded::Absent, true, true, complete(0, 1)),
            ("absent, keeps state: save at 0", Loaded::Absent, false, false, save(0, 1)),
            ("active, keeps state: save at its version", Loaded::Active { version: 3 }, false, true, save(3, 4)),
            ("active, completes: tombstone at its version", Loaded::Active { version: 3 }, true, false, complete(3, 4)),
            ("active at the last version: no next version", Loaded::Active { version: u64::MAX }, false, true, last),
        ];
        for (name, loaded, completes, has_commands, want) in cases {
            assert_eq!(saga_write(loaded, completes, has_commands).map_err(|e| e.to_string()), want, "{name}");
        }
    }

    fn event(aggregate_id: &str, sequence: Option<u64>, id: &str) -> EventMessage {
        EventMessage { id: id.to_owned(), aggregate_id: aggregate_id.to_owned(), sequence, ..base_event() }
    }

    /// The failure of the `index`-th command the instance `order-7` sent at version 2.
    fn failure(index: u64) -> EventMessage {
        let id = format!("{}:failed", saga_command_id("order-7", 2, index));
        EventMessage { name: SAGA_COMMAND_FAILED.to_owned(), ..event("order-7", None, &id) }
    }

    #[test]
    fn saga_admit_branches() {
        let fold = |folded: &str| Ok(SagaAdmit::Fold { folded: folded.to_owned() });
        let one = r#"{"order-7":{"sequence":1,"id":"evt-1"}}"#;
        let two = r#"{"order-7":{"sequence":2,"id":"evt-2"}}"#;
        let unsequenced = r#"{"order-7":{"unsequenced":"evt-u"}}"#;
        let mixed = r#"{"order-7":{"sequence":2,"id":"evt-2","unsequenced":"evt-v"}}"#;
        let first_failed = r#"{"order-7":{"unsequenced":"evt-u","failed":["order-7:2:0:failed"]}}"#;
        let both_failed = r#"{"order-7":{"unsequenced":"evt-u","failed":["order-7:2:0:failed","order-7:2:1:failed"]}}"#;
        let cases: [(&str, &str, EventMessage, Result<SagaAdmit, String>); 17] = [
            ("a new instance folds and marks", "", event("order-7", Some(1), "evt-1"), fold(one)),
            ("the last event again is a repeat", one, event("order-7", Some(1), "evt-1"), Ok(SagaAdmit::Repeat)),
            ("an older event of the aggregate is below the mark", two, event("order-7", Some(1), "evt-1"), Ok(SagaAdmit::BelowMark)),
            ("the next event folds and moves the mark", one, event("order-7", Some(2), "evt-2"), fold(two)),
            (
                "another aggregate folds beside the mark",
                one,
                event("payment-7", Some(1), "evt-p"),
                fold(r#"{"order-7":{"sequence":1,"id":"evt-1"},"payment-7":{"sequence":1,"id":"evt-p"}}"#),
            ),
            ("an unsequenced event repeats only the last id", unsequenced, event("order-7", None, "evt-u"), Ok(SagaAdmit::Repeat)),
            ("another unsequenced event folds", unsequenced, event("order-7", None, "evt-v"), fold(r#"{"order-7":{"unsequenced":"evt-v"}}"#)),
            ("an unsequenced event keeps the sequence mark and its id", two, event("order-7", None, "evt-v"), fold(mixed)),
            ("a replayed sequenced event after an unsequenced one is a repeat", mixed, event("order-7", Some(2), "evt-2"), Ok(SagaAdmit::Repeat)),
            ("a replayed unsequenced event after a sequenced one is a repeat", mixed, event("order-7", None, "evt-v"), Ok(SagaAdmit::Repeat)),
            ("an equal sequence under another id folds", one, event("order-7", Some(1), "evt-x"), fold(r#"{"order-7":{"sequence":1,"id":"evt-x"}}"#)),
            (
                "a sequence after unsequenced events folds and keeps the unsequenced id",
                unsequenced,
                event("order-7", Some(3), "evt-3"),
                fold(r#"{"order-7":{"sequence":3,"id":"evt-3","unsequenced":"evt-u"}}"#),
            ),
            ("a failure folds beside the unsequenced id", unsequenced, failure(0), fold(first_failed)),
            ("a second failure keeps the first", first_failed, failure(1), fold(both_failed)),
            ("the first failure replayed after the second is a repeat", both_failed, failure(0), Ok(SagaAdmit::Repeat)),
            (
                "a failure kept as the unsequenced id before the failed set is a repeat",
                r#"{"order-7":{"unsequenced":"order-7:2:0:failed"}}"#,
                failure(0),
                Ok(SagaAdmit::Repeat),
            ),
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
        // A `:` inside the instance id cannot alias another transition: the last two fields are numbers.
        assert_ne!(saga_command_id("a:1", 2, 0), saga_command_id("a", 1, 2));
    }

    #[test]
    fn saga_command_failed_branches() {
        let row = saga_command("order", "order-7", 3, 0, Some("acme"), Event { name: "Fulfill".into(), version: 1, data: b"{}".to_vec() }, "evt-1", "corr-1", 5);
        let failed = saga_command_failed(&row, Some(ErrorCode::Rejected), "out of stock", 9).map_err(|e| e.to_string());
        let want = EventMessage {
            id: "order-7:3:0:failed".into(),
            aggregate_id: "order-7".into(),
            tenant: Some("acme".into()),
            sequence: None,
            name: SAGA_COMMAND_FAILED.into(),
            version: 1,
            data: br#"{"command_id":"order-7:3:0","command":"Fulfill","code":"rejected","error":"out of stock"}"#.to_vec(),
            correlation_id: "corr-1".into(),
            causation_id: "order-7:3:0".into(),
            occurred_at: None,
            recorded_at: 9,
            extensions: Extensions::new(),
            metadata: Metadata::new(),
        };
        assert_eq!(failed, Ok(want), "a saga row: the instance folds its failure");
        let stray = CommandMessage { aggregate_id: None, ..row };
        assert_eq!(
            saga_command_failed(&stray, None, "warehouse API timed out", 9).map_err(|e| e.to_string()),
            Err(r#"codec: command "order-7:3:0" names no saga instance"#.to_owned()),
            "a row with no instance is no saga's"
        );
    }
}

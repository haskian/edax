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
use crate::message::{CommandMessage, EventMessage, Extensions, Metadata, Payload};
use crate::staleness::{advance, is_stale};

/// What the store holds for a correlation when the runner loads it. One value for both kernel calls of a
/// step, [`saga_admit`] and [`saga_write`], so a tombstone is never told apart from an absent instance by
/// a second argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Loaded {
    /// No instance: the event may start one.
    Absent,
    /// A running instance at `version`.
    Active {
        /// The version the fold is based on.
        version: u64,
    },
    /// A completed instance: the tombstone at `version`. It folds nothing and takes no write.
    Completed {
        /// The tombstone's version.
        version: u64,
    },
}

impl Loaded {
    /// What a store row holds, from its flat fields: whether the row is a `completed` tombstone, and its
    /// `version`, `None` for no row. A store that keeps the row as columns, and a binding that crosses it
    /// as fields, read it through this one rule.
    ///
    /// # Errors
    /// A `codec` error for a tombstone with no version: no store writes one, since completion bumps the
    /// version like any write.
    pub fn from_row(completed: bool, version: Option<u64>) -> Result<Loaded, Error> {
        match (completed, version) {
            (false, None) => Ok(Loaded::Absent),
            (false, Some(version)) => Ok(Loaded::Active { version }),
            (true, Some(version)) => Ok(Loaded::Completed { version }),
            (true, None) => Err(Error::codec("a completed saga instance carries its version")),
        }
    }
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

impl SagaWrite {
    /// The write's name, without its versions.
    pub fn kind(&self) -> SagaWriteKind {
        match self {
            SagaWrite::Nothing => SagaWriteKind::Nothing,
            SagaWrite::Save { .. } => SagaWriteKind::Save,
            SagaWrite::Complete { .. } => SagaWriteKind::Complete,
        }
    }
}

edax_core::closed_names! {
    /// The name of a [`SagaWrite`]: what a binding branches on. Exhaustive on purpose: a runner must decide
    /// about every write, so a new one is a compile error in every SDK rather than a wildcard's default.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum SagaWriteKind {
        /// [`SagaWrite::Nothing`].
        Nothing = "nothing",
        /// [`SagaWrite::Save`].
        Save = "save",
        /// [`SagaWrite::Complete`].
        Complete = "complete",
    }
}

/// The write for a step that loaded `loaded` and whose fold `completes` the instance (or keeps it
/// running) and emitted commands or not (`has_commands`).
///
/// # Errors
/// A `codec` error, the same on every retry, for an instance at the last `u64` version, which has no
/// version to commit, and for a [`Loaded::Completed`] tombstone, which takes no write. [`saga_admit`]
/// skips every event that finds a tombstone, so a runner never asks.
pub fn saga_write(loaded: Loaded, completes: bool, has_commands: bool) -> Result<SagaWrite, Error> {
    let expected = match loaded {
        Loaded::Absent if completes && !has_commands => return Ok(SagaWrite::Nothing),
        Loaded::Absent => 0,
        Loaded::Active { version } => version,
        Loaded::Completed { version } => {
            return Err(Error::codec(format_args!(
                "saga instance completed at version {version} takes no write"
            )))
        }
    };
    let version = edax_core::next_version("saga instance", expected)?;
    Ok(if completes {
        SagaWrite::Complete { expected, version }
    } else {
        SagaWrite::Save { expected, version }
    })
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
    /// The id of every failure of the instance's own commands folded into it ([`saga_command_failed`]):
    /// one per command it sent at most, so bounded by them.
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

impl SagaAdmit {
    /// The answer's name, without the marks a fold saves.
    pub fn kind(&self) -> SagaAdmitKind {
        match self {
            SagaAdmit::Completed => SagaAdmitKind::Completed,
            SagaAdmit::Repeat => SagaAdmitKind::Repeat,
            SagaAdmit::BelowMark => SagaAdmitKind::BelowMark,
            SagaAdmit::Fold { .. } => SagaAdmitKind::Fold,
        }
    }
}

edax_core::closed_names! {
    /// The name of a [`SagaAdmit`]: what a binding branches on, and what a runner reports a skip as.
    /// Exhaustive on purpose, as [`SagaWriteKind`] is.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum SagaAdmitKind {
        /// [`SagaAdmit::Completed`].
        Completed = "completed",
        /// [`SagaAdmit::Repeat`].
        Repeat = "repeat",
        /// [`SagaAdmit::BelowMark`].
        BelowMark = "below_mark",
        /// [`SagaAdmit::Fold`].
        Fold = "fold",
    }
}

/// Whether the instance `loaded`, which has folded `folded`, folds `event`. A [`Loaded::Completed`]
/// tombstone folds nothing, whatever `folded` holds. `folded` is the text the last write saved, empty for
/// a new instance; a store keeps it as it is and never reads it.
///
/// The instance keeps one mark per source aggregate: the highest sequence folded from it with that
/// event's id, and the id of the last unsequenced event folded from it, apart. An event is a
/// [`SagaAdmit::Repeat`] when its id is one of the two. It is [`SagaAdmit::BelowMark`] when its
/// sequence is below the mark's ([`is_stale`]): a dispatcher hands one aggregate's events over in
/// order, so a lower one was folded before. That holds only while no event of the aggregate was
/// dead-lettered: once a later one folded, a re-driven dead letter is below the mark and never
/// folds. An unsequenced event is a repeat only of the last unsequenced one folded from its
/// aggregate, except the failure of one of the instance's own commands ([`saga_command_failed`]): the
/// instance keeps the id of every failure it folded, so each dead command folds once. A host event that
/// only shares the [`SAGA_COMMAND_FAILED`] name is an ordinary unsequenced event. The marks grow by one
/// per source aggregate and by one per dead command, not per event.
///
/// `saga` names the saga whose instance this is: a failure folds only for a command this saga sent.
///
/// # Errors
/// A `codec` error when `folded` is not text this function wrote.
pub fn saga_admit(saga: &str, loaded: Loaded, folded: &str, event: &EventMessage) -> Result<SagaAdmit, Error> {
    if let Loaded::Completed { .. } = loaded {
        return Ok(SagaAdmit::Completed);
    }
    let mut marks: BTreeMap<String, Mark> = if folded.is_empty() {
        BTreeMap::new()
    } else {
        serde_json::from_str(folded).map_err(|e| Error::codec(format_args!("saga folded marks {folded:?}: {e}")))?
    };
    let mark = marks.entry(event.aggregate_id.clone()).or_default();
    if [&mark.id, &mark.unsequenced]
        .into_iter()
        .any(|id| id.as_deref() == Some(event.id.as_str()))
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
    } else if is_command_failure(saga, event) {
        mark.failed.insert(event.id.clone());
    } else {
        mark.unsequenced = Some(event.id.clone());
    }
    let folded = serde_json::to_string(&marks).map_err(Error::codec)?;
    Ok(SagaAdmit::Fold { folded })
}

/// Whether `event` is the failure [`saga_command_failed`] built for a command of its own instance of the
/// saga `saga`: named [`SAGA_COMMAND_FAILED`], its id `"{command_id}:failed"`, and its cause a
/// [`saga_command_id`] of the saga and the instance. The whole shape, not the name alone: the name is not
/// reserved, and a host event under it that grew the failed set would break the bound of one mark per
/// dead command.
fn is_command_failure(saga: &str, event: &EventMessage) -> bool {
    event.name == SAGA_COMMAND_FAILED
        && event.id.strip_suffix(":failed") == Some(event.causation_id.as_str())
        && is_command_id_of(&event.causation_id, saga, &event.aggregate_id)
}

/// Whether `id` is a [`saga_command_id`] of the saga `saga` and its instance `instance_id`. It parses
/// back from the right, and rebuilding it rejects any number the id function would not have written.
fn is_command_id_of(id: &str, saga: &str, instance_id: &str) -> bool {
    let Some((rest, index)) = id.rsplit_once(':') else {
        return false;
    };
    let Some((_, version)) = rest.rsplit_once(':') else {
        return false;
    };
    version
        .parse::<u64>()
        .ok()
        .zip(index.parse::<u64>().ok())
        .is_some_and(|(version, index)| saga_command_id(saga, instance_id, version, index) == id)
}

/// Refuse a command that does not belong to the write of instance `id` of `saga` at `version`: every id
/// must be the kernel's for its saga and index, every command must target the instance, and every command
/// must pass [`Message::check`](crate::Message::check). The instance is the command's `aggregate_id`, so a
/// command without one belongs to no instance. Every saga store runs it before its write. Deterministic,
/// so `codec`.
pub fn check_saga_commands(saga: &str, id: &str, version: u64, commands: &[CommandMessage]) -> Result<(), Error> {
    for (index, command) in (0u64..).zip(commands) {
        let want = saga_command_id(saga, id, version, index);
        let instance = command.aggregate_id.as_deref();
        if command.id != want || instance != Some(id) {
            return Err(Error::codec(format_args!(
                "saga command {index} of {id} at version {version} is {:?} for instance {instance:?}, expected {want:?}",
                command.id
            )));
        }
        crate::Message::check(command)?;
    }
    Ok(())
}

/// The refusal of a write to instance `id` of `saga`, completed at `version`: completion is final, so a
/// tombstone takes no write. A `concurrency` error, as for any write that loses to another: the runner
/// redelivers the event, which then finds the tombstone. Every saga store raises it the same way.
pub fn saga_completed(saga: &str, id: &str, version: u64) -> Error {
    Error::new(
        ErrorCode::Concurrency,
        format_args!("saga {saga} instance {id} completed at version {version}: a completed instance takes no write"),
    )
}

/// The id of the `index`-th command a step of the saga `saga` committed at `version` (the version the
/// write produced) of the instance `instance_id`: `"{instance_id}:{version}:{index}"` [`scoped`] by the
/// saga's name, `"{len(saga)}:{saga}:{instance_id}:{version}:{index}"`. Deterministic, so a durable store
/// can key the outbox row on it and a consumer can deduplicate a re-sent command on it. Unique per
/// committed transition of one saga, and across sagas: two sagas may share an instance id and a bus, and
/// a consumer that dedups or counts retries by message id must not merge their commands. The last two
/// fields hold no `:`, so the id parses back from the right.
pub fn saga_command_id(saga: &str, instance_id: &str, version: u64, index: u64) -> String {
    scoped(saga, &format!("{instance_id}:{version}:{index}"))
}

/// The event a saga step folded, as the commands it sends read it: its id, its flow and its tenant.
/// Three fields, not the event, so a host that holds them apart (a binding, say) need not rebuild a
/// whole event to pass them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cause<'a> {
    /// The folded event's id: the causation id of each command it causes.
    pub id: &'a str,
    /// The folded event's flow, which each command it causes stays in.
    pub correlation_id: &'a str,
    /// The folded event's tenant, which each command it causes inherits.
    pub tenant: Option<&'a str>,
    /// Immutable erasure owner inherited by child commands.
    pub subject: Option<&'a str>,
}

impl<'a> From<&'a EventMessage> for Cause<'a> {
    fn from(event: &'a EventMessage) -> Self {
        Cause {
            id: &event.id,
            correlation_id: &event.correlation_id,
            tenant: event.tenant.as_deref(),
            subject: event.subject.as_deref(),
        }
    }
}

/// The `index`-th command the instance `instance_id` of the saga named `saga` decided at the step that
/// committed `version`, folding the event `cause`: the row its outbox holds and the message its relay
/// sends, carrying `payload`.
///
/// Both SDKs build it here, so one saga run over one store writes the same row from either:
///
/// - `id` is [`saga_command_id`], scoped by the saga's name: the outbox row's key, and unique across
///   sagas, so two sagas that share an instance id and a bus never merge their commands;
/// - `idempotency_key` is that id: a re-sent row keeps it, so the consuming bus deduplicates it, and two
///   sagas never replay or refuse each other's commands;
/// - `aggregate_id` is the instance, so one instance's commands keep their order on every transport
///   and a store can find its rows. Where one store holds every tenant, that instance id is the host's
///   own composition (inside the saga's `instance_id`), so nothing here composes a second time;
/// - `tenant` is the folded event's, so a command inherits the tenancy of the fact that caused it, and
///   the bus that runs it claims inside that tenant;
/// - the lineage is the event's child ([`propagate`]): causation is the folded event, correlation is
///   the event's flow, not the instance id.
///
/// Pinned by `testdata/golden/saga.json`.
pub fn saga_command(
    saga: &str,
    instance_id: &str,
    version: u64,
    index: u64,
    cause: Cause<'_>,
    payload: Payload,
    issued_at: i64,
) -> CommandMessage {
    let id = saga_command_id(saga, instance_id, version, index);
    let lineage = propagate(cause.id, cause.correlation_id);
    CommandMessage {
        idempotency_key: Some(id.clone()),
        id,
        aggregate_id: Some(instance_id.to_owned()),
        tenant: cause.tenant.map(ToOwned::to_owned),
        subject: cause.subject.map(ToOwned::to_owned),
        encryption: None,
        name: payload.name,
        version: payload.version,
        data: payload.data,
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
        error: if command.encryption.is_some() {
            String::new()
        } else {
            error.to_owned()
        },
    };
    let lineage = propagate(&command.id, &command.correlation_id);
    Ok(EventMessage {
        id: format!("{}:failed", command.id),
        aggregate_id: instance_id,
        tenant: command.tenant.clone(),
        subject: command.subject.clone(),
        encryption: None,
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
            (
                "absent, completes, no commands: nothing",
                Loaded::Absent,
                true,
                false,
                Ok(SagaWrite::Nothing),
            ),
            (
                "absent, completes with commands: tombstone at 0",
                Loaded::Absent,
                true,
                true,
                complete(0, 1),
            ),
            (
                "absent, keeps state: save at 0",
                Loaded::Absent,
                false,
                false,
                save(0, 1),
            ),
            (
                "active, keeps state: save at its version",
                Loaded::Active { version: 3 },
                false,
                true,
                save(3, 4),
            ),
            (
                "active, completes: tombstone at its version",
                Loaded::Active { version: 3 },
                true,
                false,
                complete(3, 4),
            ),
            (
                "active at the last version: no next version",
                Loaded::Active { version: u64::MAX },
                false,
                true,
                last,
            ),
            (
                "a tombstone takes no write",
                Loaded::Completed { version: 3 },
                false,
                true,
                Err("codec: saga instance completed at version 3 takes no write".to_owned()),
            ),
        ];
        for (name, loaded, completes, has_commands, want) in cases {
            assert_eq!(
                saga_write(loaded, completes, has_commands).map_err(|e| e.to_string()),
                want,
                "{name}"
            );
        }
    }

    fn event(aggregate_id: &str, sequence: Option<u64>, id: &str) -> EventMessage {
        EventMessage {
            id: id.to_owned(),
            aggregate_id: aggregate_id.to_owned(),
            sequence,
            ..base_event()
        }
    }

    /// The saga every admit case runs under.
    const SAGA: &str = "shipping";

    /// The failure of the `index`-th command the instance `order-7` of [`SAGA`] sent at version 2.
    fn failure(index: u64) -> EventMessage {
        let command_id = saga_command_id(SAGA, "order-7", 2, index);
        named_failure(&format!("{command_id}:failed"), &command_id)
    }

    /// An unsequenced event of `order-7` named [`SAGA_COMMAND_FAILED`], with any id and cause.
    fn named_failure(id: &str, causation_id: &str) -> EventMessage {
        EventMessage {
            name: SAGA_COMMAND_FAILED.to_owned(),
            causation_id: causation_id.to_owned(),
            ..event("order-7", None, id)
        }
    }

    #[test]
    fn saga_admit_branches() {
        let fold = |folded: &str| {
            Ok(SagaAdmit::Fold {
                folded: folded.to_owned(),
            })
        };
        let one = r#"{"order-7":{"sequence":1,"id":"evt-1"}}"#;
        let two = r#"{"order-7":{"sequence":2,"id":"evt-2"}}"#;
        let unsequenced = r#"{"order-7":{"unsequenced":"evt-u"}}"#;
        let mixed = r#"{"order-7":{"sequence":2,"id":"evt-2","unsequenced":"evt-v"}}"#;
        let first_failed = r#"{"order-7":{"unsequenced":"evt-u","failed":["8:shipping:order-7:2:0:failed"]}}"#;
        let both_failed = r#"{"order-7":{"unsequenced":"evt-u","failed":["8:shipping:order-7:2:0:failed","8:shipping:order-7:2:1:failed"]}}"#;
        let cases: [(&str, &str, EventMessage, Result<SagaAdmit, String>); 24] = [
            (
                "a new instance folds and marks",
                "",
                event("order-7", Some(1), "evt-1"),
                fold(one),
            ),
            (
                "the last event again is a repeat",
                one,
                event("order-7", Some(1), "evt-1"),
                Ok(SagaAdmit::Repeat),
            ),
            (
                "an older event of the aggregate is below the mark",
                two,
                event("order-7", Some(1), "evt-1"),
                Ok(SagaAdmit::BelowMark),
            ),
            (
                "the next event folds and moves the mark",
                one,
                event("order-7", Some(2), "evt-2"),
                fold(two),
            ),
            (
                "another aggregate folds beside the mark",
                one,
                event("payment-7", Some(1), "evt-p"),
                fold(r#"{"order-7":{"sequence":1,"id":"evt-1"},"payment-7":{"sequence":1,"id":"evt-p"}}"#),
            ),
            (
                "an unsequenced event repeats only the last id",
                unsequenced,
                event("order-7", None, "evt-u"),
                Ok(SagaAdmit::Repeat),
            ),
            (
                "another unsequenced event folds",
                unsequenced,
                event("order-7", None, "evt-v"),
                fold(r#"{"order-7":{"unsequenced":"evt-v"}}"#),
            ),
            (
                "an unsequenced event keeps the sequence mark and its id",
                two,
                event("order-7", None, "evt-v"),
                fold(mixed),
            ),
            (
                "a replayed sequenced event after an unsequenced one is a repeat",
                mixed,
                event("order-7", Some(2), "evt-2"),
                Ok(SagaAdmit::Repeat),
            ),
            (
                "a replayed unsequenced event after a sequenced one is a repeat",
                mixed,
                event("order-7", None, "evt-v"),
                Ok(SagaAdmit::Repeat),
            ),
            (
                "an equal sequence under another id folds",
                one,
                event("order-7", Some(1), "evt-x"),
                fold(r#"{"order-7":{"sequence":1,"id":"evt-x"}}"#),
            ),
            (
                "a sequence after unsequenced events folds and keeps the unsequenced id",
                unsequenced,
                event("order-7", Some(3), "evt-3"),
                fold(r#"{"order-7":{"sequence":3,"id":"evt-3","unsequenced":"evt-u"}}"#),
            ),
            (
                "a failure folds beside the unsequenced id",
                unsequenced,
                failure(0),
                fold(first_failed),
            ),
            (
                "a second failure keeps the first",
                first_failed,
                failure(1),
                fold(both_failed),
            ),
            (
                "the first failure replayed after the second is a repeat",
                both_failed,
                failure(0),
                Ok(SagaAdmit::Repeat),
            ),
            (
                "a failure kept as the unsequenced id before the failed set is a repeat",
                r#"{"order-7":{"unsequenced":"8:shipping:order-7:2:0:failed"}}"#,
                failure(0),
                Ok(SagaAdmit::Repeat),
            ),
            (
                "a host event under the failure name folds as an unsequenced event",
                "",
                named_failure("evt-h1", "cmd-1"),
                fold(r#"{"order-7":{"unsequenced":"evt-h1"}}"#),
            ),
            (
                "a second host event under the name replaces the first, so the marks stay bounded",
                r#"{"order-7":{"unsequenced":"evt-h1"}}"#,
                named_failure("evt-h2", "cmd-1"),
                fold(r#"{"order-7":{"unsequenced":"evt-h2"}}"#),
            ),
            (
                "a failure id over a cause that is no command id is no failure",
                "",
                named_failure("cmd-1:failed", "cmd-1"),
                fold(r#"{"order-7":{"unsequenced":"cmd-1:failed"}}"#),
            ),
            (
                "the failure of another instance's command is no failure of this one",
                "",
                named_failure("8:shipping:payment-7:2:0:failed", "8:shipping:payment-7:2:0"),
                fold(r#"{"order-7":{"unsequenced":"8:shipping:payment-7:2:0:failed"}}"#),
            ),
            (
                "the failure of another saga's command is no failure of this one",
                "",
                named_failure("7:billing:order-7:2:0:failed", "7:billing:order-7:2:0"),
                fold(r#"{"order-7":{"unsequenced":"7:billing:order-7:2:0:failed"}}"#),
            ),
            (
                "a fresh id over a real command id is no failure, so host events cannot grow the failed set",
                "",
                named_failure("evt-x", "8:shipping:order-7:2:0"),
                fold(r#"{"order-7":{"unsequenced":"evt-x"}}"#),
            ),
            (
                "a command id without the saga's name is no command id",
                "",
                named_failure("order-7:2:0:failed", "order-7:2:0"),
                fold(r#"{"order-7":{"unsequenced":"order-7:2:0:failed"}}"#),
            ),
            (
                "marks this function did not write are a codec error",
                "[1]",
                event("order-7", Some(1), "evt-1"),
                Err(
                    r#"codec: saga folded marks "[1]": invalid type: sequence, expected a map at line 1 column 0"#
                        .to_owned(),
                ),
            ),
        ];
        for (name, folded, env, want) in cases {
            let loaded = if folded.is_empty() {
                Loaded::Absent
            } else {
                Loaded::Active { version: 1 }
            };
            assert_eq!(
                saga_admit(SAGA, loaded, folded, &env).map_err(|e| e.to_string()),
                want,
                "{name}"
            );
        }
        // A tombstone folds nothing, whatever the marks and the event: unread marks are no error.
        let tombstone = Loaded::Completed { version: 3 };
        assert_eq!(
            saga_admit(SAGA, tombstone, "[1]", &event("order-7", Some(1), "evt-1")).map_err(|e| e.to_string()),
            Ok(SagaAdmit::Completed)
        );
    }

    #[test]
    fn a_command_id_is_recognised_only_as_the_id_function_writes_it() {
        let cases = [
            (
                "the id of the instance's command",
                "8:shipping:order-7:2:0",
                "order-7",
                true,
            ),
            (
                "a pooled instance id holds a colon",
                "8:shipping:4:acme:order-7:2:0",
                "4:acme:order-7",
                true,
            ),
            (
                "another instance's command",
                "8:shipping:payment-7:2:0",
                "order-7",
                false,
            ),
            ("another saga's command", "7:billing:order-7:2:0", "order-7", false),
            ("no saga name", "order-7:2:0", "order-7", false),
            ("no colon", "cmd-1", "order-7", false),
            ("one colon", "8:shipping:order-7:2", "order-7", false),
            (
                "a version that is no number",
                "8:shipping:order-7:x:0",
                "order-7",
                false,
            ),
            ("an index that is no number", "8:shipping:order-7:2:x", "order-7", false),
            (
                "a number the id function never writes",
                "8:shipping:order-7:+2:0",
                "order-7",
                false,
            ),
        ];
        for (name, id, instance_id, want) in cases {
            assert_eq!(is_command_id_of(id, SAGA, instance_id), want, "{name}");
        }
    }

    #[test]
    fn command_ids_are_distinct_per_transition_and_index() {
        assert_eq!(saga_command_id(SAGA, "order-7", 2, 0), "8:shipping:order-7:2:0");
        // A `:` inside the instance id cannot alias another transition: the last two fields are numbers.
        assert_ne!(saga_command_id(SAGA, "a:1", 2, 0), saga_command_id(SAGA, "a", 1, 2));
        // Nor can a `:` inside the saga's name alias another saga: the name's length prefixes it.
        assert_ne!(saga_command_id("a:1", "x", 2, 0), saga_command_id("a", "1:x", 2, 0));
    }

    #[test]
    fn saga_command_failed_branches() {
        let cause = Cause {
            subject: None,
            id: "evt-1",
            correlation_id: "corr-1",
            tenant: Some("acme"),
        };
        let row = saga_command(
            SAGA,
            "order-7",
            3,
            0,
            cause,
            Payload {
                name: "Fulfill".into(),
                version: 1,
                data: b"{}".to_vec(),
            },
            5,
        );
        let failed = saga_command_failed(&row, Some(ErrorCode::Rejected), "out of stock", 9).map_err(|e| e.to_string());
        let want = EventMessage {
        subject: None,
        encryption: None,
            id: "8:shipping:order-7:3:0:failed".into(),
            aggregate_id: "order-7".into(),
            tenant: Some("acme".into()),
            sequence: None,
            name: SAGA_COMMAND_FAILED.into(),
            version: 1,
            data: br#"{"command_id":"8:shipping:order-7:3:0","command":"Fulfill","code":"rejected","error":"out of stock"}"#.to_vec(),
            correlation_id: "corr-1".into(),
            causation_id: "8:shipping:order-7:3:0".into(),
            occurred_at: None,
            recorded_at: 9,
            extensions: Extensions::new(),
            metadata: Metadata::new(),
        };
        assert_eq!(failed, Ok(want), "a saga row: the instance folds its failure");
        let stray = CommandMessage {
            aggregate_id: None,
            ..row
        };
        assert_eq!(
            saga_command_failed(&stray, None, "warehouse API timed out", 9).map_err(|e| e.to_string()),
            Err(r#"codec: command "8:shipping:order-7:3:0" names no saga instance"#.to_owned()),
            "a row with no instance is no saga's"
        );
    }
}

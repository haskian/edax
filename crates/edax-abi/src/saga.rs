//! The saga step of `edax-event`: what a step writes, whether it folds an event, and the commands
//! it sends.

use crate::{error_code, names, to_wire};
use crate::{CommandMessage, EventMessage, Payload};

/// The write a saga step makes: `action` is one of [`saga_write_actions`] (`"nothing"`, `"save"` or
/// `"complete"`), `expected` the version a save or complete is conditional on, and `version` the one it
/// commits, which the step's command ids carry (both `None` for nothing). See [`edax_event::SagaWrite`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SagaWrite {
    pub action: String,
    pub expected: Option<u64>,
    pub version: Option<u64>,
}

/// What the store held for the instance, as both saga calls read it. See [`edax_event::Loaded::from_row`].
fn saga_loaded(completed: bool, version: Option<u64>) -> Result<edax_event::Loaded, String> {
    edax_event::Loaded::from_row(completed, version).map_err(to_wire)
}

/// The write for a step whose fold `completes` the instance or not, with or without commands, over what
/// was loaded: nothing (`version` `None`), a running instance at `version`, or a `completed` tombstone,
/// which takes no write (a `codec:` error; [`saga_admit`] skips it first). See [`edax_event::saga_write`].
// `version` is optional, so it trails: alef moves optional parameters to the end of the signature.
pub fn saga_write(
    completes: bool,
    has_commands: bool,
    completed: bool,
    version: Option<u64>,
) -> Result<SagaWrite, String> {
    let write = edax_event::saga_write(saga_loaded(completed, version)?, completes, has_commands).map_err(to_wire)?;
    let (expected, version) = match write {
        edax_event::SagaWrite::Nothing => (None, None),
        edax_event::SagaWrite::Save { expected, version } | edax_event::SagaWrite::Complete { expected, version } => {
            (Some(expected), Some(version))
        }
    };
    Ok(SagaWrite {
        action: write.kind().as_str().to_owned(),
        expected,
        version,
    })
}

/// Every name [`SagaWrite::action`] takes, in the kernel's order. A host checks the writes it branches on
/// against this list. See [`edax_event::SagaWriteKind`].
pub fn saga_write_actions() -> Vec<String> {
    names(edax_event::SagaWriteKind::NAMES)
}

/// Whether a saga step folds an event: `action` is one of [`saga_admit_actions`]. `"fold"` carries the
/// marks to save with the step in `folded`; the others are a skip (write nothing, `folded` `None`):
/// `"completed"` (a tombstone), `"repeat"` (an event the instance folded), or `"below_mark"` (a
/// sequence below the one the instance folded from its aggregate, assumed folded). See
/// [`edax_event::SagaAdmit`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SagaAdmit {
    pub action: String,
    pub folded: Option<String>,
}

/// Whether the instance of the saga named `saga` loaded at `version` (`None` for none), which saved the
/// marks `folded` (empty for a new instance), or is a `completed` tombstone, folds `message`. Marks this
/// function did not write are a `codec:` error. See [`edax_event::saga_admit`].
pub fn saga_admit(
    saga: &str,
    completed: bool,
    folded: &str,
    message: EventMessage,
    version: Option<u64>,
) -> Result<SagaAdmit, String> {
    let msg = edax_event::EventMessage::try_from(message).map_err(to_wire)?;
    let admit = edax_event::saga_admit(saga, saga_loaded(completed, version)?, folded, &msg).map_err(to_wire)?;
    let action = admit.kind().as_str().to_owned();
    let folded = match admit {
        edax_event::SagaAdmit::Fold { folded } => Some(folded),
        edax_event::SagaAdmit::Completed | edax_event::SagaAdmit::Repeat | edax_event::SagaAdmit::BelowMark => None,
    };
    Ok(SagaAdmit { action, folded })
}

/// Every name [`SagaAdmit::action`] takes, in the kernel's order. A host checks the answers it branches
/// on against this list. See [`edax_event::SagaAdmitKind`].
pub fn saga_admit_actions() -> Vec<String> {
    names(edax_event::SagaAdmitKind::NAMES)
}

/// The id of the `index`-th command a step of the saga named `saga` committed at `version`:
/// `"{len(saga)}:{saga}:{instance_id}:{version}:{index}"`. See [`edax_event::saga_command_id`].
pub fn saga_command_id(saga: &str, instance_id: &str, version: u64, index: u64) -> String {
    edax_event::saga_command_id(saga, instance_id, version, index)
}

/// The name of the event [`saga_command_failed`] builds: `"SagaCommandFailed"`. See
/// [`edax_event::SAGA_COMMAND_FAILED`].
pub fn saga_command_failed_name() -> String {
    edax_event::SAGA_COMMAND_FAILED.to_owned()
}

/// The event a saga folds when `command`, a row its outbox sent, died of the failure `error` with the
/// taxonomy `code` (`None` when no code classifies it): named `"SagaCommandFailed"`, for the instance
/// the row names, with the JSON payload `command_id`, `command`, `code` and `error`. `recorded_at` is
/// the caller's clock. An unknown code, or a row that names no instance, is a `codec:` error. Pinned
/// by `testdata/golden/saga.json`. See [`edax_event::saga_command_failed`].
// `code` is optional, so it trails: alef moves optional parameters to the end of the signature.
pub fn saga_command_failed(
    command: CommandMessage,
    error: &str,
    recorded_at: i64,
    code: Option<String>,
) -> Result<EventMessage, String> {
    let code = error_code(code)?;
    edax_event::saga_command_failed(
        &edax_event::CommandMessage::try_from(command).map_err(to_wire)?,
        code,
        error,
        recorded_at,
    )
    .map(EventMessage::from)
    .map_err(to_wire)
}

/// The `index`-th command the instance `instance_id` of the saga named `saga` decided at the step that
/// committed `version`, folding the event `cause_id` of the flow `cause_correlation_id` and `tenant`: the
/// row its outbox holds and the message its relay sends. `payload` is the host codec's output. Its `id`
/// is [`saga_command_id`], which names the saga, its `idempotency_key` that id, its aggregate the
/// instance, its tenant the folded event's, and its lineage the folded event's child. Where one store
/// holds every tenant, the instance id is the host's own composition, so nothing here composes again.
/// Pinned by `testdata/golden/saga.json`. See [`edax_event::saga_command`].
// The cause crosses flat, as three arguments, so a host passes the fields it holds and builds no
// native object for it. `tenant` is optional, so it trails: alef moves optional parameters to the end.
#[allow(clippy::too_many_arguments)]
pub fn saga_command(
    saga: &str,
    instance_id: &str,
    version: u64,
    index: u64,
    payload: Payload,
    cause_id: &str,
    cause_correlation_id: &str,
    issued_at: i64,
    tenant: Option<String>,
    subject: Option<String>,
) -> CommandMessage {
    let cause = edax_event::Cause {
        id: cause_id,
        correlation_id: cause_correlation_id,
        tenant: tenant.as_deref(),
        subject: subject.as_deref(),
    };
    CommandMessage::from(edax_event::saga_command(
        saga,
        instance_id,
        version,
        index,
        cause,
        edax_event::Payload::from(payload),
        issued_at,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::*;
    use crate::*;

    #[test]
    fn saga_admit_crosses_the_marks_as_text() {
        let env = |id: &str, sequence: u64| event_message(id, Some(sequence));
        let first = saga_admit("shipping", false, "", env("evt-1", 2), None).unwrap();
        assert_eq!(first.action, "fold", "a new instance folds");
        let folded = first.folded.expect("a fold carries its marks");
        let answer = |completed: bool, event: EventMessage| {
            let admit = saga_admit("shipping", completed, &folded, event, Some(1)).unwrap();
            (admit.action, admit.folded.is_none())
        };
        assert_eq!(
            answer(false, env("evt-1", 2)),
            ("repeat".to_owned(), true),
            "a redelivery is skipped"
        );
        assert_eq!(
            answer(false, env("evt-0", 1)),
            ("below_mark".to_owned(), true),
            "an older sequence is skipped apart"
        );
        assert_eq!(
            answer(true, env("evt-2", 3)),
            ("completed".to_owned(), true),
            "a tombstone folds nothing"
        );
        assert_eq!(
            answer(false, env("evt-2", 3)),
            ("fold".to_owned(), false),
            "the next event folds"
        );
        assert!(saga_admit("shipping", false, "[1]", env("evt-1", 1), Some(1))
            .err()
            .is_some_and(|e| e.starts_with("codec: saga folded marks")));
        let versionless = saga_admit("shipping", true, "", env("evt-1", 1), None).err();
        assert_eq!(
            versionless.as_deref(),
            Some("codec: a completed saga instance carries its version"),
            "no row a store wrote"
        );
    }

    #[test]
    fn saga_write_crosses_every_action() {
        let got = |version, completes, cmds| {
            let write = saga_write(completes, cmds, false, version).unwrap();
            (write.action, write.expected, write.version)
        };
        assert_eq!(got(None, true, false), ("nothing".to_owned(), None, None));
        assert_eq!(got(None, false, false), ("save".to_owned(), Some(0), Some(1)));
        assert_eq!(got(Some(3), false, false), ("save".to_owned(), Some(3), Some(4)));
        assert_eq!(got(Some(3), true, true), ("complete".to_owned(), Some(3), Some(4)));
        assert!(saga_write(false, true, false, Some(u64::MAX))
            .is_err_and(|e| e.starts_with("codec: saga instance at version")));
        assert!(
            saga_write(false, true, true, Some(3)).is_err_and(|e| e.ends_with("completed at version 3 takes no write"))
        );
        assert_eq!(saga_command_id("shipping", "order-7", 2, 1), "8:shipping:order-7:2:1");
    }

    #[test]
    fn saga_command_crosses_the_outbox_row() {
        let command = Payload {
            name: "Fulfill".to_owned(),
            version: 3,
            data: br#"{"order_id":"order-7"}"#.to_vec(),
        };
        let cause = event_message("evt-1", Some(1));
        let row = saga_command(
            "shipping",
            "order-7",
            2,
            1,
            command,
            &cause.id,
            &cause.correlation_id,
            7,
            None,
            None,
        );
        let want = CommandMessage {
            subject: None,
            encryption: None,
            tenant: None,
            id: "8:shipping:order-7:2:1".to_owned(),
            idempotency_key: Some("8:shipping:order-7:2:1".to_owned()),
            aggregate_id: Some("order-7".to_owned()),
            name: "Fulfill".to_owned(),
            version: 3,
            data: br#"{"order_id":"order-7"}"#.to_vec(),
            correlation_id: "corr-1".to_owned(),
            causation_id: "evt-1".to_owned(),
            issued_at: 7,
            extensions: HashMap::new(),
            metadata: HashMap::new(),
        };
        assert_same_command(row, want, "the cause's child, keyed and ordered by the instance");
    }
}

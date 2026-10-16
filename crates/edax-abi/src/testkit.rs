//! The fixtures every module's tests share: the kernel's base event and command, and the record values.

pub(crate) use edax_event::testkit::RECORDED_AT;
use edax_event::testkit::{base_command, base_draft, map};

use crate::{CommandMessage, EventDraft, EventMessage};

/// The kernel's base draft under `id` and `sequence`. It also sets the one optional field the base leaves
/// empty, `occurred_at`, so a boundary that dropped it would fail the round trip.
pub(crate) fn draft(id: &str, sequence: Option<u64>) -> EventDraft {
    EventDraft::from(edax_event::EventDraft {
        id: id.to_owned(),
        sequence,
        occurred_at: Some(1_699_999_999_000_000_000),
        ..base_draft()
    })
}

/// The draft above as the store records it at [`RECORDED_AT`].
pub(crate) fn event_message(id: &str, sequence: Option<u64>) -> EventMessage {
    EventMessage::from(
        edax_event::EventDraft::try_from(draft(id, sequence))
            .unwrap()
            .record(RECORDED_AT),
    )
}

/// The kernel's base command aimed at `aggregate_id`. It also sets the one map the base leaves empty,
/// `extensions`, so a boundary that dropped it would fail the round trip.
pub(crate) fn command_message(aggregate_id: Option<&str>) -> CommandMessage {
    CommandMessage::from(edax_event::CommandMessage {
        aggregate_id: aggregate_id.map(str::to_owned),
        extensions: map(&[("http.origin_ip", "203.0.113.7")]),
        ..base_command()
    })
}

/// Whole-record event equality, through the kernel's message. The record is a binding type with no
/// `PartialEq` (alef owns the derives on the generated twin), and the `From` into the kernel's message is
/// exhaustive, so a field added to the record later is compared too: what proves nothing is dropped at the
/// boundary.
pub(crate) fn assert_same_event(got: EventMessage, want: EventMessage, case: &str) {
    assert_eq!(
        edax_event::EventMessage::try_from(got),
        edax_event::EventMessage::try_from(want),
        "{case}"
    );
}

/// Whole-record command equality, for the reason [`assert_same_event`] gives.
pub(crate) fn assert_same_command(got: CommandMessage, want: CommandMessage, case: &str) {
    assert_eq!(
        edax_event::CommandMessage::try_from(got),
        edax_event::CommandMessage::try_from(want),
        "{case}"
    );
}

/// A real 64-bit payload hash, past 2^53 so a lossy (float) reader would be caught.
pub(crate) const HASH: u64 = 11_234_062_831_410_934_249;
/// One cached handler result; every case overrides only the field under test.
pub(crate) const RESULT: &str = r#"{"invoice_id":"invoice-7","total":5000}"#;

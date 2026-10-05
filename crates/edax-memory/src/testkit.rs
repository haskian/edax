//! Test fixtures shared by the modules' own test suites. One draft, one command and one wire helper,
//! so a case in any module reads the same way.

use edax_core::Error;
pub(crate) use edax_event::testkit::RECORDED_AT;
use edax_event::testkit::{base_command, base_draft, map};
use edax_event::{CommandMessage, EventDraft, EventMessage, Message};

/// One realistic event draft on the kernel's base draft (`edax_event::testkit::base_draft`), with a JSON
/// body and the producer's own time; every case overrides only the field under test.
pub(crate) fn draft(id: &str, sequence: Option<u64>) -> EventDraft {
    EventDraft {
        id: id.to_owned(),
        aggregate_id: "invoice-7".to_owned(),
        sequence,
        data: br#"{"invoice_id":"invoice-7","total":5000,"currency":"EUR"}"#.to_vec(),
        occurred_at: Some(1_699_999_999_000_000_000),
        metadata: map(&[("tenant", "acme")]),
        ..base_draft()
    }
}

/// The message the store returns for [`draft`]: the same fields, stamped at [`RECORDED_AT`].
pub(crate) fn event(id: &str, sequence: Option<u64>) -> EventMessage {
    draft(id, sequence).record(RECORDED_AT)
}

/// One realistic command on the kernel's base command, targeting `aggregate_id`, or none.
pub(crate) fn command(id: &str, aggregate_id: Option<&str>) -> CommandMessage {
    CommandMessage {
        id: id.to_owned(),
        idempotency_key: Some(format!("req-{id}")),
        aggregate_id: aggregate_id.map(str::to_owned),
        data: br#"{"invoice_id":"invoice-7","total":5000}"#.to_vec(),
        metadata: map(&[("tenant", "acme")]),
        ..base_command()
    }
}

/// The ids of outbox rows of any kind, in order.
pub(crate) fn ids<R: Message>(rows: &[R]) -> Vec<String> {
    rows.iter().map(|row| row.id().to_owned()).collect()
}

/// The wire form of a result, so every exact-message assertion below pins what a binding sees.
pub(crate) fn wire<T>(result: Result<T, Error>) -> Result<T, String> {
    result.map_err(|e| e.to_string())
}

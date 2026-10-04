//! Fixtures shared by the unit suites: one event draft, and the event a store records from it. A case
//! overrides only the field under test.

use edax_event::{EventDraft, EventMessage, Extensions, Metadata};

/// The clock reading every test append is stamped with, in unix nanoseconds.
pub(crate) const RECORDED_AT: i64 = 1_714_564_800_000_000_000;

/// The draft of event `name` at `sequence` of `aggregate_id`, with id `"{aggregate_id}-{sequence}"`.
pub(crate) fn draft(aggregate_id: &str, name: &str, sequence: u64, data: &[u8]) -> EventDraft {
    EventDraft {
        tenant: None,
        id: format!("{aggregate_id}-{sequence}"),
        aggregate_id: aggregate_id.to_owned(),
        sequence: Some(sequence),
        name: name.to_owned(),
        version: 1,
        data: data.to_vec(),
        correlation_id: "corr-1".to_owned(),
        causation_id: "cmd-1".to_owned(),
        occurred_at: None,
        extensions: Extensions::new(),
        metadata: Metadata::new(),
    }
}

/// [`draft`] with an empty payload, as the store records it at [`RECORDED_AT`].
pub(crate) fn event(aggregate_id: &str, name: &str, sequence: u64) -> EventMessage {
    draft(aggregate_id, name, sequence, &[]).record(RECORDED_AT)
}

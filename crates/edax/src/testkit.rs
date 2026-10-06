//! Test fixtures and the store contract clauses, behind the `testkit` feature. An adopter's store test turns
//! the feature on in its dev-dependency and runs [`contract`] against its store; the SDK's own suites start
//! from the same fixtures.
//!
//! The fixtures start from the kernel's base event (`edax_event::testkit::base_draft`, the one
//! `envelopes.json` pins first), so a case overrides only the field under test.

pub mod account;
pub mod contract;

use edax_event::testkit::base_draft;
use edax_event::{EventDraft, EventMessage};

/// The clock reading every test append is stamped with, in unix nanoseconds.
pub const RECORDED_AT: i64 = 1_714_564_800_000_000_000;

/// The draft of event `name` at `sequence` of `aggregate_id`, with id `"{aggregate_id}-{sequence}"`,
/// carrying `data`, and the base draft's lineage and context.
pub fn draft(aggregate_id: &str, name: &str, sequence: u64, data: &[u8]) -> EventDraft {
    EventDraft {
        id: format!("{aggregate_id}-{sequence}"),
        aggregate_id: aggregate_id.to_owned(),
        sequence: Some(sequence),
        name: name.to_owned(),
        data: data.to_vec(),
        ..base_draft()
    }
}

/// [`draft`] with an empty payload, as the store records it at [`RECORDED_AT`].
pub fn event(aggregate_id: &str, name: &str, sequence: u64) -> EventMessage {
    draft(aggregate_id, name, sequence, &[]).record(RECORDED_AT)
}

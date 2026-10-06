//! Shared test fixtures: the base event and command `testdata/golden/envelopes.json` pins first, and the
//! event's draft. A test in any crate starts from one and overrides only the field it is about. Built for
//! tests only: under `cfg(test)` here, and behind the `testkit` feature for another crate.

use crate::message::{CommandMessage, EventDraft, EventMessage, Extensions, Payload};
use crate::saga::{saga_command as kernel_saga_command, Cause};
use std::collections::BTreeMap;

/// A string map from pairs, the shape of both [`Extensions`] and [`Metadata`](crate::Metadata).
pub fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
}

/// The clock reading every fixture is stamped at: the base event's `recorded_at`, the base command's
/// `issued_at`, and what a test store records at.
pub const RECORDED_AT: i64 = 1_700_000_000_000_000_000;

/// The base event: sequenced, a binary payload, one extension and one metadata key.
pub fn base_event() -> EventMessage {
    EventMessage {
        id: "evt-1".to_owned(),
        aggregate_id: "agg-1".to_owned(),
        tenant: None,
        sequence: Some(7),
        name: "InvoiceIssued".to_owned(),
        version: 1,
        data: vec![0xde, 0xad, 0xbe, 0xef],
        correlation_id: "corr-1".to_owned(),
        causation_id: "cmd-1".to_owned(),
        occurred_at: None,
        recorded_at: RECORDED_AT,
        extensions: map(&[("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")]),
        metadata: map(&[("trace", "abc-123")]),
    }
}

/// The draft [`base_event`] is recorded from: every field but `recorded_at`.
pub fn base_draft() -> EventDraft {
    let EventMessage {
        id,
        aggregate_id,
        tenant,
        sequence,
        name,
        version,
        data,
        correlation_id,
        causation_id,
        occurred_at,
        recorded_at: _,
        extensions,
        metadata,
    } = base_event();
    EventDraft {
        id,
        aggregate_id,
        tenant,
        sequence,
        name,
        version,
        data,
        correlation_id,
        causation_id,
        occurred_at,
        extensions,
        metadata,
    }
}

/// The base command: a client key, a target aggregate, a JSON payload and one metadata key.
pub fn base_command() -> CommandMessage {
    CommandMessage {
        id: "cmd-1".to_owned(),
        idempotency_key: Some("req-1".to_owned()),
        aggregate_id: Some("agg-1".to_owned()),
        tenant: None,
        name: "IssueInvoice".to_owned(),
        version: 1,
        data: br#"{"amount":10}"#.to_vec(),
        correlation_id: "corr-1".to_owned(),
        causation_id: "corr-1".to_owned(),
        issued_at: RECORDED_AT,
        extensions: Extensions::new(),
        metadata: map(&[("user", "u-7")]),
    }
}

/// The `index`-th command instance `id` of the saga `saga` sends at the write that commits `version`,
/// caused by its `Paid` event: the kernel's row, as a saga store stores it, with a real `Fulfill` payload.
pub fn saga_command(saga: &str, id: &str, version: u64, index: u64) -> CommandMessage {
    let payload = Payload {
        name: "Fulfill".into(),
        version: 1,
        data: format!(r#"{{"order_id":"{id}"}}"#).into_bytes(),
    };
    let cause_id = format!("{id}-paid");
    let cause = Cause {
        id: &cause_id,
        correlation_id: "corr-1",
        tenant: None,
    };
    kernel_saga_command(saga, id, version, index, cause, payload, RECORDED_AT)
}

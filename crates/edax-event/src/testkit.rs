//! Shared test fixtures: the base event and command `testdata/golden/envelopes.json` pins first. A
//! test in any crate starts from one and overrides only the field it is about. Built for tests only:
//! under `cfg(test)` here, and behind the `testkit` feature for another crate's dev-dependency.

use crate::domain::{CommandMessage, EventMessage, Extensions};
use std::collections::BTreeMap;

/// A string map from pairs, the shape of both [`Extensions`] and [`Metadata`](crate::Metadata).
pub fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
}

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
        recorded_at: 1_700_000_000_000_000_000,
        extensions: map(&[("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")]),
        metadata: map(&[("trace", "abc-123")]),
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
        issued_at: 1_700_000_000_000_000_000,
        extensions: Extensions::new(),
        metadata: map(&[("user", "u-7")]),
    }
}

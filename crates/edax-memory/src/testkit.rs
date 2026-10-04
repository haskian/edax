//! Test fixtures shared by the modules' own test suites. One draft, one command and one wire helper,
//! so a case in any module reads the same way.

use std::collections::BTreeMap;

use edax_core::Error;
use edax_event::{CommandMessage, EventDraft, EventMessage, Message};

/// The clock reading every test append is stamped with.
pub(crate) const RECORDED_AT: i64 = 1_700_000_000_000_000_000;

/// One realistic event draft; every case overrides only the field under test.
pub(crate) fn draft(id: &str, sequence: Option<u64>) -> EventDraft {
    EventDraft {
        tenant: None,
        id: id.to_owned(),
        aggregate_id: "invoice-7".to_owned(),
        sequence,
        name: "InvoiceIssued".to_owned(),
        version: 1,
        data: br#"{"invoice_id":"invoice-7","total":5000,"currency":"EUR"}"#.to_vec(),
        correlation_id: "corr-1".to_owned(),
        causation_id: "evt-0".to_owned(),
        occurred_at: Some(1_699_999_999_000_000_000),
        extensions: BTreeMap::from([("traceparent".to_owned(), "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_owned())]),
        metadata: BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]),
    }
}

/// The message the store returns for [`draft`]: the same fields, stamped at [`RECORDED_AT`].
pub(crate) fn event(id: &str, sequence: Option<u64>) -> EventMessage {
    draft(id, sequence).record(RECORDED_AT)
}

/// One realistic command targeting `aggregate_id`, or none.
pub(crate) fn command(id: &str, aggregate_id: Option<&str>) -> CommandMessage {
    CommandMessage {
        tenant: None,
        id: id.to_owned(),
        idempotency_key: Some(format!("req-{id}")),
        aggregate_id: aggregate_id.map(str::to_owned),
        name: "IssueInvoice".to_owned(),
        version: 1,
        data: br#"{"invoice_id":"invoice-7","total":5000}"#.to_vec(),
        correlation_id: "corr-1".to_owned(),
        causation_id: "corr-1".to_owned(),
        issued_at: 1_699_999_998_000_000_000,
        extensions: BTreeMap::new(),
        metadata: BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]),
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

//! Test fixtures shared by the modules' own test suites. One envelope shape and one wire helper, so
//! a case in any module reads the same way.

use std::collections::BTreeMap;

use edax_core::Error;

/// One realistic envelope; every case overrides only the field under test.
pub(crate) fn envelope(id: &str, sequence: Option<u64>) -> edax_event::EventEnvelope {
    edax_event::EventEnvelope {
        id: id.to_owned(),
        aggregate_id: "invoice-7".to_owned(),
        event: edax_event::Event {
            name: "InvoiceIssued".to_owned(),
            version: 1,
            data: br#"{"invoice_id":"invoice-7","total":5000,"currency":"EUR"}"#.to_vec(),
        },
        sequence,
        namespace: BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]),
        metadata: BTreeMap::from([
            ("correlation_id".to_owned(), "corr-1".to_owned()),
            ("causation_id".to_owned(), "evt-0".to_owned()),
        ]),
        timestamp: 1_700_000_000_000_000_000,
    }
}

pub(crate) fn ids(envs: &[edax_event::EventEnvelope]) -> Vec<String> {
    envs.iter().map(|e| e.id.clone()).collect()
}

/// The wire form of a result, so every exact-message assertion below pins what a binding sees.
pub(crate) fn wire<T>(result: Result<T, Error>) -> Result<T, String> {
    result.map_err(|e| e.to_string())
}

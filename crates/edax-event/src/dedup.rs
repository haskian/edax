//! The consumer's deduplication input: which key and which hash an event's idempotency guard
//! claims, pinned by `testdata/golden/event_idempotency.json`.
//!
//! Ordering is a different question with a different answer: [`crate::staleness`] skips an event
//! older than the per-aggregate watermark by comparing sequences; this module names the message so
//! a ledger can tell a redelivery of one event from a second event.

use crate::domain::EventMessage;

/// What an event's idempotency guard claims: the key and the payload hash bound to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventIdempotency<'a> {
    /// The message id. A message id, not the content: two events with equal bodies are still two
    /// events, and a redelivery of one carries the same id.
    pub key: &'a str,
    /// FNV-1a over the raw `data` bytes, the same function as `edax_cqrs::payload_hash`. Raw,
    /// not canonical: the producer encoded the bytes once and every redelivery carries them as is,
    /// so two bodies under one id are a different payload even when they mean the same.
    pub payload_hash: u64,
}

/// The key and hash an idempotency guard claims for `event`. One rule for every SDK, so two consumers
/// of one stream sharing a ledger bind the same key to the same hash.
pub fn event_idempotency(event: &EventMessage) -> EventIdempotency<'_> {
    EventIdempotency { key: &event.id, payload_hash: edax_core::fnv1a64(&event.data) }
}

//! The consumer's deduplication input: which key and which hash an event's idempotency guard
//! claims, pinned by `testdata/golden/event_idempotency.json`.
//!
//! Ordering is a different question with a different answer: [`crate::staleness`] skips an event
//! older than the per-aggregate watermark by comparing sequences; this module names the message so
//! a ledger can tell a redelivery of one event from a second event.

use edax_core::{Error, Tenancy};

use crate::domain::EventMessage;

/// What an event's idempotency guard claims: the key and the payload hash bound to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventIdempotency {
    /// The ledger key: the message id under the guard's scope, `"edax:evt:{len(scope)}:{scope}:{id}"`,
    /// composed with the message's tenant under [`Tenancy::Pool`]. The id, not the content: two events
    /// with equal bodies are still two events, and a redelivery of one carries the same id.
    pub key: String,
    /// FNV-1a over the raw `data` bytes, the same function as `edax_cqrs::payload_hash`. Raw,
    /// not canonical: the producer encoded the bytes once and every redelivery carries them as is,
    /// so two bodies under one id are a different payload even when they mean the same.
    pub payload_hash: u64,
}

/// The key and hash the idempotency guard named `scope` claims for `event`, in a deployment that
/// separates tenants by `mode`. One rule for every SDK, so a redelivery binds the same key to the
/// same hash whichever SDK consumes it.
///
/// The scope names the consumer (a read model, a notifier). Each consumer keeps its own record of
/// what it handled, so two consumers guarding over one ledger each run their handler once, rather
/// than the second finding the first's record and skipping. The key composes as a command claim does
/// ([`edax_core::guard_key`]). A mode that requires a tenant and an event that names none is a
/// `codec` error.
pub fn event_idempotency(event: &EventMessage, mode: Tenancy, scope: &str) -> Result<EventIdempotency, Error> {
    let key = edax_core::guard_key(mode, event.tenant.as_deref(), "evt", scope, &event.id)?;
    Ok(EventIdempotency { key, payload_hash: edax_core::fnv1a64(&event.data) })
}

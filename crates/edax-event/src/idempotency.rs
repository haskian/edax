//! The event guard's claim: which key and which hash an event's idempotency guard claims, pinned by
//! `testdata/golden/event_idempotency.json`. The record it claims, and the record's state machine, are
//! `edax-cqrs`'s, shared with the command guard.
//!
//! Ordering is a different question with a different answer: [`crate::staleness`] skips an event
//! older than the per-aggregate watermark by comparing sequences; this module names the message so
//! a ledger can tell a redelivery of one event from a second event.

use edax_core::{Error, GuardKind, Tenancy};

use crate::message::EventMessage;

/// What an event's idempotency guard claims: the key and the payload hash bound to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventIdempotency {
    /// The ledger key: the message id under the guard's scope, `"edax:evt:{len(scope)}:{scope}:{id}"`,
    /// composed with the message's tenant under [`Tenancy::Pool`]. It keys on the id because two events
    /// with equal bodies are still two events, and a redelivery of one carries the same id.
    pub key: String,
    /// [`edax_core::payload_hash`] over the raw `data` bytes, as the command guard hashes. Raw,
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
/// than the second finding the first's record and skipping. The key is [`event_claim_key`]. A mode
/// that requires a tenant and an event that names none is a `codec` error.
pub fn event_idempotency(event: &EventMessage, mode: Tenancy, scope: &str) -> Result<EventIdempotency, Error> {
    let key = event_claim_key(mode, event.tenant.as_deref(), scope, &event.id)?;
    Ok(EventIdempotency {
        key,
        payload_hash: edax_core::payload_hash(&event.data),
    })
}

/// The ledger key the event guard named `scope` claims for the event `event_id` of `tenant`:
/// `"edax:evt:{len(scope)}:{scope}:{event_id}"`, composed with the tenant under [`Tenancy::Pool`]. It
/// composes as a command claim does ([`edax_core::guard_key`]).
///
/// Apart from [`event_idempotency`] so an operator can forget the record of a dead-lettered event
/// from its id alone, and a re-drive runs its handler again. A mode that requires a tenant and a call
/// that names none is a `codec` error.
pub fn event_claim_key(mode: Tenancy, tenant: Option<&str>, scope: &str, event_id: &str) -> Result<String, Error> {
    edax_core::guard_key(mode, tenant, GuardKind::Event, scope, event_id)
}

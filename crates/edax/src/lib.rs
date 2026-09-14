//! Ergonomic Rust SDK over the kernels: aggregates, the storage seams, and the orchestration.
//!
//! The kernels own no I/O, so persistence is the SDK's job. Implement the seams against your own database:
//! [`EventStore`] (write side, with the transactional outbox), [`ProjectionStore`] (read side),
//! [`IdempotencyStore`] (command dedup) and [`SagaStore`]. Over them sit [`Repository`], [`Relay`],
//! [`CommandBus`], [`KeyOrderedDispatcher`], [`EventRouter`] and [`SagaRunner`]. [`backend`] ships dev-only
//! in-memory stores; production needs a store backed by a real database transaction.

pub mod aggregate;
pub mod backend;
mod chain;
pub mod codec;
pub mod command;
pub mod dispatcher;
pub mod projection;
pub mod router;
pub mod saga;
pub mod store;

pub use aggregate::{Aggregate, AggregateCodec, EventCodec, Root};
pub use backend::{
    InMemoryBroker, MemoryEventStore, MemoryIdempotencyStore, MemoryProjectionStore, MemorySagaStore, MemoryTransactor,
    MemoryTx,
};
pub use codec::{Format, Json, JsonCodec, SerdeCodec};
#[cfg(feature = "msgpack")]
pub use codec::{Msgpack, MsgpackCodec};
pub use command::{
    idempotent, idempotent_in, retry, Claim, Command, CommandBus, CommandBusError, Handler, HandlerError,
    IdempotencyStore, Middleware, Record, State,
};
pub use dispatcher::{DeadLetter, Delivery, DispatcherBuilder, HandlerFailure, KeyOrderedDispatcher};
pub use projection::{Entity, ProjectionStore, Query};
pub use router::{idempotent_by_id, idempotent_by_id_in, EventHandler, EventIdempotencyStore, EventMiddleware, EventRouter};
pub use edax_core::{guid, ErrorCode};
// Kernel decisions a host calls directly, re-exported so a host need not reach past `edax` into
// `edax_event`. No SDK code path calls `should_apply`: the host owns where the last-applied sequence lives.
pub use edax_event::{originate, propagate, should_apply, CAUSATION_ID, CORRELATION_ID};
pub use saga::{Saga, SagaInstance, SagaRunner, SagaStore};
pub use store::{with_tx, EventStore, Relay, Repository, Snapshot, Transactor};

/// What can go wrong saving, loading or handling an event or a command. The taxonomy is part of the store
/// contract: `Concurrency` only for a version conflict, `Store` for a backend failure (retried), `Codec` for a
/// marshaling failure (not retried).
///
/// This is the SDK's one taxonomy, so a verdict has one variant however it was reached. Both idempotency
/// guards, [`idempotent`] and [`idempotent_by_id`], report the same variants, and one rule classifies each
/// terminal or transient. [`CommandBusError`] holds only the two ways a dispatch itself fails.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// An append's expected version did not match the stored one: a concurrent writer got there first.
    /// Reload the aggregate and retry.
    Concurrency { expected: u64, stored: u64 },
    /// A codec could not marshal or unmarshal.
    Codec(String),
    /// The backing store failed.
    Store(String),
    /// A handler was already registered for this command type or event name: a wiring mistake at startup.
    /// Deterministic, so it is never retried.
    AlreadyRegistered(String),
    /// An idempotency key (an event id, or a command's key) was reused with a different payload.
    /// Terminal: an integrity error, not a duplicate.
    PayloadMismatch(String),
    /// The idempotency key is claimed by another handler run that has not completed. Transient: nack or
    /// retry, so the winner's outcome can replay once it settles.
    InProgress(String),
    /// Replay of a terminal handler failure cached under the idempotency key. Terminal in its own
    /// right; carries the message only, since the record schema stores no error type.
    CachedFailure(String),
    /// A transaction handle names no open transaction: never begun, already settled, or belonging to another
    /// store. Terminal, and the same handle fails the same way every time. Build every store that must commit
    /// together over one transactor.
    BadHandle(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Concurrency { expected, stored } => {
                write!(f, "concurrency conflict: expected version {expected}, stored {stored}")
            }
            Error::Codec(m) => write!(f, "codec error: {m}"),
            Error::Store(m) => write!(f, "store error: {m}"),
            Error::AlreadyRegistered(n) => write!(f, "handler already registered for: {n}"),
            Error::PayloadMismatch(k) => write!(f, "idempotency key reused with a different payload: {k}"),
            Error::InProgress(k) => write!(f, "idempotency key claimed by an in-flight handler, not yet complete: {k}"),
            Error::CachedFailure(m) => write!(f, "replayed cached handler failure: {m}"),
            Error::BadHandle(m) => write!(f, "transaction handle names no open transaction: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// The kernel's code for this variant. Exhaustive: a new variant must name its code, and the code carries
    /// the terminal or transient verdict, which the SDK never restates itself.
    pub fn code(&self) -> ErrorCode {
        match self {
            Error::Concurrency { .. } => ErrorCode::Concurrency,
            Error::Codec(_) => ErrorCode::Codec,
            Error::Store(_) => ErrorCode::Store,
            Error::AlreadyRegistered(_) => ErrorCode::AlreadyRegistered,
            Error::PayloadMismatch(_) => ErrorCode::PayloadMismatch,
            Error::InProgress(_) => ErrorCode::InProgress,
            Error::CachedFailure(_) => ErrorCode::CachedFailure,
            Error::BadHandle(_) => ErrorCode::BadHandle,
        }
    }
}

#[cfg(test)]
mod tests {
    /// The kernel decisions a host calls directly are reachable from `edax` itself, so a Rust host
    /// names them exactly as a Python host does (`edax.should_apply`). A dropped re-export would
    /// otherwise only surface in someone else's build.
    #[test]
    fn host_facing_kernel_decisions_are_reachable_from_the_sdk_root() {
        assert!(crate::should_apply(Some(5), Some(6)), "newer than stored: apply");
        assert!(!crate::should_apply(Some(5), Some(4)), "stale: drop");
        // The boundary the kernel pins: equal sequences re-apply, so the handler stays responsible
        // for its own effect-idempotency there.
        assert!(crate::should_apply(Some(5), Some(5)));
        assert!(!crate::guid().is_empty());
    }
}

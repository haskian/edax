//! The reference Rust SDK over the kernels: aggregates, the storage seams, and the orchestration.
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
mod pacing;
pub mod projection;
pub mod router;
pub mod saga;
pub mod store;
#[cfg(test)]
mod testkit;

pub use aggregate::{Aggregate, AggregateCodec, EventCodec, Root};
pub use backend::{
    MemoryBroker, MemoryEventStore, MemoryIdempotencyStore, MemoryLedger, MemoryProjectionStore, MemorySagaStore,
    MemoryTransactor, MemoryTx,
};
pub use codec::{PayloadFormat, Json, JsonCodec, JsonRegistryCodec, RegistryCodec, SerdeCodec, UpcastStep, Upcaster};
#[cfg(feature = "msgpack")]
pub use codec::{Msgpack, MsgpackCodec, MsgpackRegistryCodec};
pub use command::{
    idempotent, idempotent_in, retry, wrap, Claim, ClaimHandle, Command, CommandBus, Handler, HandlerError, IdempotencyStore,
    Middleware, Record, State, Terminal,
};
pub use dispatcher::{DeadLetter, Delivery, DispatcherBuilder, HandlerFailure, KeyOrderedDispatcher};
pub use pacing::{Entropy, Pacing, Sleeper};
pub use projection::{Entity, Page, ProjectionStore, Query};
pub use router::{idempotent_by_id, idempotent_by_id_in, EventHandler, EventIdempotencyStore, EventMiddleware, EventRouter};
/// The SDK's one failure type is the kernel's: a [`ErrorCode`] and a message, whose `Display` is the
/// wire form `"<code>: <message>"`. Every SDK renders and caches a failure the same way, and the code
/// carries the terminal / transient verdict ([`ErrorCode::is_terminal`]).
pub use edax_core::{backoff_millis, current_timestamp, guid, Backoff, Error, ErrorCode};
// Kernel decisions a host calls directly, re-exported so a host need not reach past `edax` into
// `edax_event`. No SDK code path calls `is_stale` or `advance`: the host owns where the watermark lives.
pub use edax_event::{advance, is_stale, originate, propagate};
// The message shapes the SDK's seams take and return, and the lineage a save is handed.
pub use edax_event::{CommandMessage, EventDraft, EventMessage, Extensions, Lineage, Metadata};
pub use saga::{Saga, SagaCommandFailed, SagaInstance, SagaRunner, SagaStore, SAGA_COMMAND_FAILED};
pub use store::{retry_transaction, transaction, EventStore, Outbox, OutboxRow, Relay, RelaySink, Repository, Snapshot, Transactor};

#[cfg(test)]
mod tests {
    /// The kernel decisions a host calls directly are reachable from `edax` itself, so a Rust host
    /// names them exactly as a Python host does (`edax.is_stale`, `edax.advance`). A dropped
    /// re-export would otherwise only surface in someone else's build.
    #[test]
    fn host_facing_kernel_decisions_are_reachable_from_the_sdk_root() {
        assert!(!crate::is_stale(Some(5), Some(6)), "newer than stored: apply");
        assert!(crate::is_stale(Some(5), Some(4)), "stale: skip");
        // The boundary the kernel pins: equal sequences re-apply, so the handler stays responsible
        // for its own effect-idempotency there.
        assert!(!crate::is_stale(Some(5), Some(5)));
        assert_eq!(crate::advance(Some(5), None), Some(5), "an unsequenced event keeps the mark");
        assert!(!crate::guid().is_empty());
        assert!(crate::current_timestamp() < crate::current_timestamp());
        assert!(crate::backoff_millis(crate::Backoff::DEFAULT, 0, u64::MAX) <= 100);
    }
}

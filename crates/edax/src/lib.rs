//! The reference Rust SDK over the kernels: aggregates, the storage seams, and the orchestration.
//!
//! The kernels own no I/O, so persistence is the SDK's job. Implement the seams against your own database:
//! [`EventStore`] (write side, with the transactional outbox), [`ProjectionStore`] and [`ProjectionQuery`] (read side),
//! [`IdempotencyStore`] (command dedup) and [`SagaStore`]. Over them sit [`Repository`], [`Relay`],
//! [`CommandBus`], [`KeyOrderedDispatcher`], [`EventRouter`] and [`SagaRunner`]. [`backend`] ships dev-only
//! in-memory stores; production needs a store backed by a real database transaction.

pub mod aggregate;
pub mod backend;
mod chain;
pub mod codec;
pub mod command;
pub mod dispatcher;
mod failure;
pub mod idempotency;
pub mod outbox;
mod pacing;
pub mod projection;
pub mod repository;
pub mod router;
pub mod saga;
mod settle;
pub mod store;
#[cfg(any(test, feature = "testkit"))]
pub mod testkit;
pub mod unit_of_work;

pub use aggregate::{Aggregate, AggregateCodec, EventCodec, PayloadCodec, Root};
pub use backend::{
    MemoryBroker, MemoryEventStore, MemoryIdempotencyStore, MemoryLedger, MemoryProjectionStore, MemorySagaStore,
    MemoryTransactor, MemoryTx,
};
pub use codec::{
    Json, JsonCodec, JsonRegistryCodec, Msgpack, MsgpackCodec, MsgpackRegistryCodec, PayloadFormat, RegistryCodec,
    SerdeCodec, UpcastStep, Upcaster,
};
pub use command::{
    command_message, idempotent, idempotent_in, retry, Command, CommandBus, DispatchOptions, Handler, Middleware, Next,
};
pub use dispatcher::{Delivery, DispatcherBuilder, HandlerFailure, KeyOrderedDispatcher};
/// The SDK's one failure type is the kernel's: a [`ErrorCode`] and a message, whose `Display` is the
/// wire form `"<code>: <message>"`. Every SDK renders and caches a failure the same way, and the code
/// carries the terminal / transient verdict ([`ErrorCode::is_terminal`]).
pub use edax_core::{backoff_millis, current_timestamp, guid, Backoff, Error, ErrorCode};
pub use failure::{is_fatal, HandlerError, Terminal};
pub use idempotency::{forget, forget_event, Claim, ClaimHandle, IdempotencyStore, Record, State};
pub use outbox::{CountedOutbox, Outbox, Relay, RelaySink};
pub use pacing::{Entropy, Pacing, Sleeper};
pub use projection::{Entity, Page, ProjectionQuery, ProjectionStore, Query};
pub use repository::{Repository, SaveOptions};
pub use router::{idempotent_by_id, idempotent_by_id_in, EventHandler, EventMiddleware, EventRouter};
pub use settle::DeadLetter;
// The tenancy rule a host chooses a mode by ([`Repository::tenancy`]) and composes its own keys with
// (a saga instance, a read-model id), so a Rust host names it from `edax` as a Python host does.
pub use edax_core::{check_tenant, tenant_key, Tenancy, TENANT_MAX};
// Kernel decisions a host calls directly, re-exported so a host need not reach past `edax` into
// `edax_event`. No SDK code path calls `is_stale` or `advance`: the host owns where the watermark lives.
pub use edax_event::{advance, is_stale, originate, propagate};
// The ledger keys the two guards claim, which `forget` and `forget_event` compose to re-drive a dead letter.
pub use edax_cqrs::claim_key;
// The document rules a projection store runs, so a Rust backend refuses and keys rows as the memory store
// and a Python backend do: `document` on save, `keyed_document` on every read. A query spec is checked by
// `Query::validate`.
pub use edax_cqrs::{document, keyed_document};
// The host-value types a decimal or an instant field is declared with, so a command hashes, and an event
// payload reads and writes, as a Python host's `Decimal` and aware `datetime` do.
pub use edax_cqrs::{Decimal, Instant};
pub use edax_event::event_claim_key;
// The message shapes the SDK's seams take and return, the payload a codec marshals, and the lineage a
// save is handed.
pub use edax_event::{CommandMessage, EventDraft, EventMessage, Extensions, Lineage, Metadata, Payload};
pub use saga::{Saga, SagaCommandFailed, SagaInstance, SagaOutbox, SagaRunner, SagaStore, SAGA_COMMAND_FAILED};
pub use store::{EventStore, Snapshot};
pub use unit_of_work::{retry_transaction, transaction, Transactor};

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
        assert_eq!(
            crate::advance(Some(5), None),
            Some(5),
            "an unsequenced event keeps the mark"
        );
        assert!(!crate::guid().is_empty());
        assert!(crate::current_timestamp() < crate::current_timestamp());
        assert!(crate::backoff_millis(crate::Backoff::DEFAULT, 0, u64::MAX) <= 100);
        assert_eq!(
            crate::tenant_key(crate::Tenancy::Pool, Some("acme"), "order-1"),
            Ok("4:acme:order-1".to_owned())
        );
        assert!(crate::check_tenant(&"a".repeat(crate::TENANT_MAX + 1)).is_err());
        assert_eq!(
            crate::claim_key(crate::Tenancy::Silo, None, "api", "req-1"),
            Ok("edax:cmd:3:api:req-1".to_owned())
        );
        assert_eq!(
            crate::event_claim_key(crate::Tenancy::Silo, None, "api", "e-1"),
            Ok("edax:evt:3:api:e-1".to_owned())
        );
        let row = crate::document("order-1", serde_json::json!({"total": 5})).expect("an object");
        assert_eq!(crate::keyed_document("order-1", row)["id"], "order-1");
        assert!(
            crate::document("order-1", serde_json::json!([1])).is_err(),
            "an array is no row"
        );
    }
}

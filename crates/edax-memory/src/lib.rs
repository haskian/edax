//! In-memory backends for the edax seams, written once and shared by the Rust SDK and the
//! Python binding so both get the same ordering, the same optimistic-concurrency checks and the same
//! transaction boundary.
//!
//! [`MemoryTransactor`] owns the unit of work: [`begin`](MemoryTransactor::begin) hands out a token,
//! every store method takes that token, and [`commit`](MemoryTransactor::commit) applies the
//! transaction's staged writes atomically while [`rollback`](MemoryTransactor::rollback) drops them.
//! [`MemoryEventStore`], [`MemoryProjectionStore`], [`MemoryIdempotencyStore`], [`MemorySagaStore`]
//! and [`MemoryCommandOutbox`] are views over one transactor, so a claim, a projection row, a saga
//! instance and an outbox command commit or roll back with the events they belong to. [`MemoryBroker`] is separate: a broker is not a store
//! participant. A failure is the kernel's [`edax_core::Error`], carrying one of the codes these
//! stores emit: `concurrency`, `codec`, `store`, `in_progress` and `bad_handle` of the
//! [`ErrorCode`](edax_core::ErrorCode) taxonomy. Its `Display` is the `"<code>: <message>"` wire form
//! a binding crosses and every SDK maps to its typed errors.
//!
//! The API is typed: versions are the kernel's `u64`, documents and saga state are JSON values, and a
//! query is the kernel's [`Query`](edax_cqrs::Query). JSON text is a binding's boundary shape, so
//! the binding converts to them, not this crate. The idempotency store's claims are fenced on a handle,
//! as Redis fences on the claim value, and [`MemoryIdempotencyStore::expire`] lapses one, so a test can
//! rehearse a takeover. Bring a durable store to production; the cross-process ledger is `edax-redis`.


mod broker;
mod event;
mod idempotency;
mod outbox;
mod projection;
mod saga;
mod tx;
#[cfg(test)]
mod testkit;

pub use broker::{MemoryBroker, Message, PolledDelivery};
pub use event::{MemoryEventStore, Snapshot};
pub use idempotency::{Claimed, MemoryIdempotencyStore};
pub use outbox::MemoryCommandOutbox;
pub use projection::{MemoryProjectionStore, Versioned};
pub use saga::{MemorySagaStore, SagaRow};
pub use tx::{Document, MemoryTransactor, NO_TX};

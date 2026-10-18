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

// Shared by both SDKs, so every public item states what it promises.
#![warn(missing_docs)]

mod broker;
mod event;
mod idempotency;
mod ingest;
mod outbox;
mod outbox_rows;
mod projection;
mod saga;
#[cfg(test)]
mod testkit;
mod tx;
mod versioned;

pub use broker::{LogMessage, MemoryBroker, PolledDelivery};
pub use event::{MemoryEventStore, Snapshot};
pub use idempotency::MemoryIdempotencyStore;
pub use ingest::MemoryIngest;
pub use outbox::MemoryCommandOutbox;
pub use projection::{Document, MemoryProjectionStore};
pub use saga::{MemorySagaStore, SagaRow};
pub use tx::{MemoryTransactor, Tx};
pub use versioned::Versioned;

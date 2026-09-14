//! In-memory backend adapters for the edax seams, written once and shared by the Rust SDK and the
//! Python binding so both get the same ordering, the same optimistic-concurrency checks and the same
//! transaction boundary.
//!
//! [`MemoryTransactor`] owns the unit of work: [`begin`](MemoryTransactor::begin) hands out a token,
//! every store method takes that token, and [`commit`](MemoryTransactor::commit) applies the
//! transaction's staged writes atomically while [`rollback`](MemoryTransactor::rollback) drops them.
//! [`MemoryEventStore`], [`MemoryProjectionStore`], [`MemoryIdempotencyStore`] and [`MemorySagaStore`]
//! are views over one transactor, so a claim, a projection row and a saga instance commit or roll
//! back with the events they belong to. [`MemoryBroker`] is separate: a broker is not a store
//! participant. A failure is a typed [`AdapterError`], one variant per code these adapters emit:
//! `concurrency`, `codec`, `store` and `bad_handle` of the kernel's [`ErrorCode`] taxonomy. Its
//! `Display` is the
//! `"<code>: <message>"` wire form a binding crosses and every SDK maps to its typed errors. Bring a
//! durable store to production; a cross-process store like Redis is each SDK's own concern.

// These adapters run inside a native binding: an unwinding panic crosses the host language's FFI
// boundary, where it cannot be caught. Every fallible step returns `AdapterError` instead.
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

mod broker;
mod error;
mod event;
mod idempotency;
mod projection;
mod saga;
mod tx;
#[cfg(test)]
mod testkit;

pub use broker::{MemoryBroker, PolledDelivery, NO_DELIVERY};
pub use error::AdapterError;
pub use event::{MemoryEventStore, Snapshot};
pub use idempotency::MemoryIdempotencyStore;
pub use projection::MemoryProjectionStore;
pub use saga::MemorySagaStore;
pub use tx::{MemoryTransactor, NO_TX};

//! In-process dev/test backends ([`memory`], [`broker`]); each seam's contract is the trait doc that defines it.

pub mod broker;
pub mod memory;

pub use broker::MemoryBroker;
pub use memory::{
    MemoryEventStore, MemoryIdempotencyStore, MemoryProjectionStore, MemorySagaStore, MemoryTransactor, MemoryTx,
};

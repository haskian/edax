//! edax-core — primitives shared across the edax toolkit.
//!
//! These are the pieces both the event kernel (edax-event) and the command kernel (edax-cqrs) depend
//! on: deterministic hashing and a retry backoff schedule (pure and consistency-critical — all
//! bindings produce identical results), the backend-neutral [`filter`] query AST + evaluator (whose
//! match decision must be identical in every language), the [`error`] taxonomy (whose terminal /
//! transient verdict decides what an idempotency guard persists, so every SDK must agree), plus `guid`
//! and `current_timestamp` for minting new event ids and timestamps (non-deterministic by nature, so
//! the exceptions to that rule). [`sync::lock`] and [`token`] are the runtime primitives the
//! adapters share: one poison-tolerant lock and one settlement-token mint, so two state machines
//! cannot drift on either.

// Every public item is a cross-language contract, so it states what it promises.
#![warn(missing_docs)]

pub mod backoff;
pub mod clock;
pub mod error;
pub mod filter;
pub mod guid;
pub mod hash;
pub mod sync;
pub mod token;

pub use backoff::backoff_millis;
pub use clock::current_timestamp;
pub use error::{AdapterError, ErrorCode};
pub use filter::{eval, eval_json, Filter, Operator};
pub use guid::{guid, guid_simple};
pub use hash::{fnv1a32, fnv1a64};
pub use sync::lock;
pub use token::{TokenMint, NO_DELIVERY};

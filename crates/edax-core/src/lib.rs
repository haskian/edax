//! edax-core — primitives shared across the edax toolkit.
//!
//! These are the pieces both the event-driven kernel (edax-event) and the command-and-query kernel
//! (edax-cqrs) depend on: deterministic hashing and a jittered retry [`Backoff`] (pure and
//! consistency-critical — all bindings produce identical results), the one [`Error`] type and its
//! [`ErrorCode`] taxonomy (whose terminal / transient verdict decides what an idempotency guard
//! persists, so every SDK must agree), plus [`guid()`] and [`current_timestamp`] for minting new event
//! ids and timestamps (non-deterministic by nature, so the exceptions to that rule). [`scoped`]
//! namespaces a key by a scope injectively. [`lock`] and [`TokenMint`] are the runtime primitives
//! the adapters share: one poison-tolerant lock and one settlement-token mint, so two state
//! machines cannot drift on either.

// Every public item is a cross-language contract, so it states what it promises.
#![warn(missing_docs)]

// Private modules, re-exported flat: one path per item (`edax_core::guid`, not `edax_core::guid::guid`).
mod backoff;
mod clock;
mod error;
mod guid;
mod hash;
mod key;
mod sync;
mod token;

pub use backoff::{backoff_millis, Backoff};
pub use clock::current_timestamp;
pub use error::{verdict, verdict_of_wire, Error, ErrorCode};
pub use guid::{guid, guid_simple};
pub use hash::{fnv1a32, fnv1a64};
pub use key::{check_tenancy, check_tenant, scoped, tenant_key, Tenancy, TENANT_MAX};
pub use sync::lock;
pub use token::{TokenMint, NO_TOKEN};

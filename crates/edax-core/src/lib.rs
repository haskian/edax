//! edax-core — primitives shared across the edax toolkit.
//!
//! These are the pieces both the event-driven kernel (edax-event) and the command-and-query kernel
//! (edax-cqrs) depend on: deterministic hashing and a jittered retry [`Backoff`] (pure and
//! consistency-critical — all bindings produce identical results), the one [`Error`] type and its
//! [`ErrorCode`] taxonomy (whose terminal / transient verdict decides what an idempotency guard
//! persists, so every SDK must agree), plus [`guid()`] and [`current_timestamp`] for minting new event
//! ids and timestamps (non-deterministic by nature, so the exceptions to that rule). The `key` module
//! decides how a key is composed: [`scoped`] namespaces a key by a scope injectively, [`Tenancy`] and
//! [`tenant_key`] put the tenant in it, and [`guard_key`] is the ledger key an idempotency guard claims.
//! [`next_version`] is the one step every versioned row takes. The `names` module's [`closed_names!`] is
//! the one shape of a closed set of names a decision crosses a binding as, and [`is_open_name`] the one
//! rule a name a host chooses follows.
//!
//! [`lock`] and [`TokenMint`] are Rust-only runtime helpers, not cross-language contracts: one
//! poison-tolerant lock and one settlement-token mint the Rust adapters share, so two state machines
//! cannot drift on either. No binding crosses them. `docs/kernels/core.md` lists the whole scope.

// Every public item states what it promises: a cross-language contract, or, for `lock` and
// `TokenMint`, the runtime guarantee the Rust adapters rely on.
#![warn(missing_docs)]

// Private modules, re-exported flat: one path per item (`edax_core::guid`, not `edax_core::guid::guid`).
mod backoff;
mod clock;
mod error;
mod guid;
mod hash;
mod key;
mod names;
mod sync;
mod token;
mod version;

pub use backoff::{backoff_millis, Backoff};
pub use clock::current_timestamp;
pub use error::{is_terminal, Error, ErrorCode};
pub use guid::{guid, guid_simple, uuid7_micros};
pub use hash::{fnv1a32, fnv1a64, payload_hash};
pub use key::{
    check_subject, check_tenancy, check_tenant, guard_key, scoped, tenant_key, GuardKind, Tenancy, SUBJECT_MAX,
    TENANT_MAX,
};
pub use names::{distinct_names, is_open_name, OPEN_NAME_MAX};
pub use sync::lock;
pub use token::{TokenMint, NO_TOKEN};
pub use version::next_version;

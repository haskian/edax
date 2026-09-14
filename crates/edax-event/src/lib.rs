//! edax-event — the event-sourcing consistency kernel for the edax polyglot toolkit.
//!
//! Pure, synchronous, allocation-light, free of I/O, threads, and callbacks. The bindings own
//! concurrency, ergonomics, and transports. The kernel owns only the consistency-critical
//! decisions that all services must agree on bit-for-bit:
//!
//! - [`partition`] — consistent-hash routing (same aggregate id -> same logical partition), plus
//!   resize-friendly [`partition::rendezvous`] routing
//! - [`codec`] — selectable, deterministic envelope serializations ([`codec::Format`])
//! - [`query`] — sort/paginate semantics over filtered documents (read-side ordering + cursors);
//!   the filter AST it evaluates lives in `edax-core`
//! - [`upcast`] — versioned-event upcast planning (ordering + gap detection)
//! - [`causation`] — causation/correlation id propagation rule
//! - [`dedup`] — ignore-old decision for ordered, at-least-once delivery
//! - [`sequence`] — the dense-batch rule an append must satisfy before a store writes it
//!
//! Shared primitives (hashing, backoff) live in `edax-core`.

// Every public item is a cross-language contract, so it states what it promises.
#![warn(missing_docs)]

pub mod causation;
pub mod codec;
pub mod dedup;
pub mod domain;
pub mod partition;
pub mod query;
pub mod sequence;
pub mod upcast;

pub use causation::{originate, propagate, CAUSATION_ID, CORRELATION_ID};
pub use codec::{decode_to_json, encode_from_json, CodecError, Format};
pub use dedup::should_apply;
pub use domain::{Event, EventEnvelope, Metadata, Namespace};
pub use partition::{partition, rendezvous};
pub use query::{encode_cursor, encode_cursor_json, query_json, Query, SortKey};
pub use sequence::check_batch;
pub use upcast::{upcast_plan, upcast_plan_json};

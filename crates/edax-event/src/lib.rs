//! edax-event — the event-sourcing consistency kernel for the edax polyglot toolkit.
//!
//! Pure, synchronous, allocation-light, free of I/O, threads, and callbacks. The bindings own
//! concurrency, ergonomics, and transports. The kernel owns only the consistency-critical
//! decisions that all services must agree on bit-for-bit:
//!
//! - [`partition`](mod@partition) — local fan-out: the worker a key-ordered dispatcher runs an
//!   aggregate on. Broker placement is the transport's, so Kafka's lives in `edax-kafka`.
//! - [`domain`] — the two messages, [`EventMessage`] and [`CommandMessage`], their ordering key, the
//!   key a received command is claimed under, and the extension key rule
//! - [`codec`] — selectable, deterministic message serializations ([`codec::Format`]), one media
//!   type per format and kind, and their evolution rule
//! - [`upcast`] — versioned-event upcast planning (ordering + gap detection) and each name's target version
//! - [`causation`] — the [`Lineage`] a message inherits or starts
//! - [`staleness`] — whether an event is older than the per-aggregate watermark, and the watermark
//!   after it applies
//! - [`dedup`] — the key and hash an event's idempotency guard claims
//! - [`sequence`] — the dense-batch rule an append must satisfy before a store writes it, and when a
//!   save writes a snapshot
//! - [`retry`] — when a failed message is handed back, after how long, and when it dies
//! - [`saga`] — whether a saga step folds an event (a redelivery is skipped), the write it makes
//!   (save, complete, nothing), the command message its outbox holds, and the event that reports a
//!   dead command back to its instance
//!
//! Shared primitives (hashing, backoff) live in `edax-core`. The read side — the filter AST, its
//! evaluator, and the sort/cursor/paging rules — is `edax-cqrs`, which owns both halves of the
//! separation it is named for.

// Every public item is a cross-language contract, so it states what it promises.
#![warn(missing_docs)]

pub mod causation;
pub mod codec;
pub mod dedup;
pub mod domain;
pub mod partition;
pub mod retry;
pub mod saga;
pub mod sequence;
pub mod staleness;
#[cfg(any(test, feature = "testkit"))]
pub mod testkit;
pub mod upcast;

pub use causation::{originate, propagate, Lineage};
pub use codec::Format;
pub use dedup::{event_idempotency, EventIdempotency};
pub use domain::{
    check_extensions, ordering_key, received_key, CommandMessage, Event, EventDraft, EventMessage, Extensions, Kind,
    Message, Metadata, EXTENSION_KEY_MAX,
};
pub use partition::partition;
pub use retry::{Failure, Retries, Tracked, Verdict};
pub use saga::{
    saga_admit, saga_command, saga_command_failed, saga_command_id, saga_write, Loaded, SagaAdmit, SagaCommandFailed,
    SagaWrite, SAGA_COMMAND_FAILED,
};
pub use sequence::{check_batch, snapshot_due};
pub use staleness::{advance, is_stale};
pub use upcast::{upcast_plan, upcast_target, FIRST_VERSION};

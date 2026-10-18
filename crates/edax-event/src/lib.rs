//! edax-event — the event-sourcing consistency kernel for the edax polyglot toolkit.
//!
//! Sans-IO: no I/O, threads or callbacks. [`Retries`] and [`Drain`] keep state, which the host owns and
//! holds per consumer or relay. The bindings own concurrency, ergonomics, and transports. The kernel owns only the consistency-critical
//! decisions that all services must agree on bit-for-bit:
//!
//! - [`partition`](mod@partition) — local fan-out: the worker a key-ordered dispatcher runs an
//!   aggregate on. Broker placement is the transport's, so Kafka's lives in `edax-kafka`.
//! - [`message`] — the two messages, [`EventMessage`] and [`CommandMessage`], the [`Payload`] each
//!   carries, their ordering key, the tenant a saved command takes, and the extension key rule
//! - [`codec`] — selectable, deterministic message serializations ([`codec::Format`]), one media
//!   type per format and kind, and their evolution rule
//! - [`upcast`] — versioned-event upcast planning (ordering + gap detection) and each name's target version
//! - [`causation`] — the [`Lineage`] a message inherits or starts
//! - [`staleness`] — whether an event is older than the per-aggregate watermark, and the watermark
//!   after it applies
//! - [`idempotency`] — the key and hash an event's idempotency guard claims. The record it claims
//!   and its state machine are `edax-cqrs`'s, shared with the command guard, as is the key a
//!   received command is claimed under
//! - [`sequence`] — the dense-batch rule an append must satisfy before a store writes it, the drafts a
//!   save appends, and when it writes a snapshot
//! - [`retry`] — when a failed message is handed back, after how long, and when it dies; when an
//!   in-process retry runs its work again; and how long a loop waits on a source that keeps failing
//! - [`drain`] — which rows an outbox relay's drain publishes, and what a failed row does to the rows
//!   behind it
//! - [`saga`] — whether a saga step folds an event (a redelivery is skipped), the write it makes
//!   (save, complete, nothing), the command message its outbox holds, and the event that reports a
//!   dead command back to its instance
//!
//! Shared primitives (hashing, backoff) live in `edax-core`. The read side — the filter AST, its
//! evaluator, and the sort/cursor/paging rules — is `edax-cqrs`, which owns both halves of the
//! separation it is named for.

// Every public item is a cross-language contract, so it states what it promises.
#![warn(missing_docs)]

pub mod batch;

pub mod causation;
pub mod codec;
pub mod drain;
pub mod erasure;
pub mod idempotency;
pub mod message;
pub mod partition;
pub mod retry;
pub mod saga;
pub mod sealing;
pub mod sequence;
pub mod staleness;
#[cfg(any(test, feature = "testkit"))]
pub mod testkit;
pub mod upcast;

pub use causation::{originate, propagate, Lineage};
pub use codec::Format;
pub use drain::{Drain, Drained, RowFailed, RowFailedKind};
pub use idempotency::{event_claim_key, event_idempotency, EventIdempotency};
pub use message::{
    check_extensions, ordering_key, own_command, CommandMessage, EventDraft, EventMessage, Extensions, Kind, Message,
    Metadata, Payload, EXTENSION_KEY_MAX,
};
pub use partition::partition;
pub use retry::{retry_attempts, retry_step, Failure, Retries, Streak, Tracked, Verdict, VerdictKind};
pub use saga::{
    check_saga_commands, saga_admit, saga_command, saga_command_failed, saga_command_id, saga_completed, saga_write,
    Cause, Loaded, SagaAdmit, SagaAdmitKind, SagaCommandFailed, SagaWrite, SagaWriteKind, SAGA_COMMAND_FAILED,
};
pub use sequence::{check_batch, check_snapshot, draft_batch, snapshot_due, DraftBatch, SaveContext};
pub use staleness::{advance, is_stale};
pub use upcast::{upcast_plan, upcast_target, FIRST_VERSION};

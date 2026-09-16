//! edax-kafka — the Kafka protocol kernel for the edax polyglot toolkit.
//!
//! Pure, synchronous, free of I/O and of any Kafka client library: every SDK ships its own IO shell
//! (`aiokafka`, `rdkafka`, ...) over these decisions, so a topic reads the same whatever language
//! wrote it and a consumer commits the same offsets whatever language runs it. The kernel owns only
//! what the two sides must agree on bit-for-bit:
//!
//! - [`offsets`] — the token-settled per-partition commit watermark an [`OffsetTracker`] hands a
//!   consumer as [`AckDecision`] / [`NackDecision`] data, never as a callback
//! - [`record`] — the record a published envelope becomes: the key rule, the four contract headers,
//!   the media type that names the value's wire format, and the poison-message id
//!
//! Placement is not here: the client's partitioner still picks the partition from the key.

// Every public item is a cross-language contract, so it states what it promises.
#![warn(missing_docs)]
// This kernel runs inside a native binding: an unwinding panic crosses the host language's FFI
// boundary, where it cannot be caught. Every fallible step returns a value instead.
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

pub mod offsets;
pub mod record;

pub use offsets::{AckDecision, NackDecision, OffsetTracker, PartitionStats, NO_DELIVERY};
pub use record::{
    content_type, decode_record, encode_record, media_type, poison_id, record, KafkaHeader, KafkaRecord,
    CONTENT_TYPE, HEADER_EVENT, HEADER_EVENT_VERSION, HEADER_ID,
};

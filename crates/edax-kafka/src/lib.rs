//! edax-kafka — the Kafka protocol kernel for the edax polyglot toolkit.
//!
//! Pure, synchronous, free of I/O and of any Kafka client library: every SDK ships its own IO shell
//! (`aiokafka`, `rdkafka`, ...) over these decisions, so a topic reads the same whatever language
//! wrote it and a consumer commits the same offsets whatever language runs it. The kernel owns only
//! what the two sides must agree on bit-for-bit:
//!
//! - [`offsets`] — the token-settled per-partition commit watermark an [`OffsetTracker`] hands a
//!   consumer as [`AckDecision`] / [`NackDecision`] / [`Due`] data, never as a callback, and the
//!   rewind a nack makes so the consumer keeps per-key order
//! - [`record`](mod@record) — the record a published envelope becomes: the key rule, the `content-type` header
//!   that names the value's wire format (the media type itself is `edax_event::Format::media_type`,
//!   since it names the codec and not the transport), the [`route`] a fetched record takes by it,
//!   the poison-message id, and the record a dead-letter sink publishes
//! - [`partition`] — broker placement: the partition a keyed record lands on, Java's murmur2 rule,
//!   which every shell passes explicitly so mixed-language producers agree

// Every public item is a cross-language contract, so it states what it promises.
#![warn(missing_docs)]

pub mod offsets;
pub mod partition;
pub mod record;

pub use offsets::{AckDecision, Due, NackDecision, OffsetTracker, PartitionStats};
pub use partition::partition_for;
pub use record::{
    check_media_types, content_type, dead_letter_record, dead_letter_source, decode_record, encode_record, poison_id, record, route,
    DeadLetterSource, KafkaHeader, KafkaRecord, Route, CONTENT_TYPE, DLQ_ERROR, DLQ_OFFSET, DLQ_PARTITION, DLQ_TOPIC,
};

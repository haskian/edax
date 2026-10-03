//! The Kafka record a published envelope becomes, and the envelope a fetched record decodes back to.
//!
//! Two layers. The primitives — [`record`] and [`content_type`] — own the key rule and the header
//! set while the caller owns the value bytes, so a host serializer keeps the contract header. The
//! composed pair — [`encode_record`] and [`decode_record`] — runs the kernel codec end to end, and
//! is what the golden vectors pin.
//!
//! One header, [`CONTENT_TYPE`], because it is the one a consumer reads: [`route`] picks the
//! serializer by it. No aggregate id (it is the key), no sequence, id or event name (they are in the
//! value), no metadata copies (trace propagation is the host's own `traceparent`). Add a header only
//! with a reader for it: every header costs bytes on every record, and a header nobody reads is a
//! promise nobody keeps. The four `edax-dlq-*` headers ride only on a dead-letter record, and
//! [`dead_letter_source`] is their reader.
//!
//! The `content-type` header names the codec and not the transport, so the string itself is
//! `edax_event::Format::media_type`'s and any transport that carries a type uses the same one.
//!
//! Placement is [`crate::partition_for`]: the shell passes the partition it names with the record.

use edax_core::Error;
use edax_event::{EventEnvelope, Format};

/// Header naming the wire format of the value bytes; the value is a media type (the vocabulary note
/// on [`Format::media_type`] separates the two words). Lower-case, the Kafka convention and the
/// CloudEvents Kafka binding's spelling; the lookup in [`content_type`] is case-sensitive, so a
/// producer must write it exactly like this.
pub const CONTENT_TYPE: &str = "content-type";
/// One Kafka record header. The value is bytes, not text: a foreign header on the same topic may
/// carry anything, and the kernel must hand it back unchanged rather than lose it.
///
/// Kafka allows a null header value; this type has none, and a shell crosses a null value as empty
/// bytes. So a null `content-type` routes like an empty one: [`Route::Unknown`], poison, never the
/// default. One rule for both, because a shell that kept them apart would need a third.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaHeader {
    /// The header name, as the client hands it over.
    pub key: String,
    /// The header value, uninterpreted.
    pub value: Vec<u8>,
}

impl KafkaHeader {
    /// One header, copied out of borrowed parts.
    pub fn new(key: &str, value: &[u8]) -> Self {
        KafkaHeader { key: key.to_owned(), value: value.to_vec() }
    }
}

/// One Kafka record: what a producer sends and what a consumer fetched, minus the client's own
/// coordinates (topic, partition, offset), which the IO shell holds. A null key or value (a keyless
/// record, a tombstone) crosses as empty bytes, the same rule as a null header value: an edax record
/// always has both, and a foreign one without them is poison that dead-letters with them empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaRecord {
    /// The partitioning key: the aggregate id, or the host's override. UTF-8 bytes.
    pub key: Vec<u8>,
    /// The encoded envelope.
    pub value: Vec<u8>,
    /// The headers: [`record`] writes [`CONTENT_TYPE`] only; a fetched record carries whatever its
    /// producer and the middlewares wrote.
    pub headers: Vec<KafkaHeader>,
}

/// Build the record for `env` from value bytes the caller already encoded.
///
/// `key` overrides the partitioning key; `None` uses the aggregate id, which is what keeps one
/// aggregate's events on one partition and therefore in order. `content_type` names the format of
/// `value`, so a host serializer must carry a media type of its own and register it in every SDK
/// that reads the topic.
pub fn record(env: &EventEnvelope, key: Option<&str>, value: Vec<u8>, content_type: &str) -> KafkaRecord {
    KafkaRecord {
        key: key.unwrap_or(&env.aggregate_id).as_bytes().to_vec(),
        value,
        headers: vec![KafkaHeader::new(CONTENT_TYPE, content_type.as_bytes())],
    }
}

/// The value's media type, or `None` when no readable one is set.
///
/// The first [`CONTENT_TYPE`] header wins, matched case-sensitively: a record carrying two is
/// ambiguous, and picking the first is the rule both sides can implement identically. A first
/// header whose value is not UTF-8 is not a media type any lookup can match, so it reads as absent
/// rather than reaching past it to a second one. The value is returned whole: parameters such as
/// `; charset=utf-8` are not stripped, so a registry keyed by media type will not match a header
/// that carries them. No edax producer writes parameters; strip them here, pinned by a golden, the
/// day a foreign producer does.
pub fn content_type(headers: &[KafkaHeader]) -> Option<&str> {
    let header = headers.iter().find(|header| header.key == CONTENT_TYPE)?;
    std::str::from_utf8(&header.value).ok()
}

/// Where a fetched record's value goes, by its [`CONTENT_TYPE`]: the one routing rule every shell
/// runs, so two SDKs reading one topic send the same record to the same serializer or to the same
/// dead-letter sink. Exhaustive: a shell decides about every arm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// No readable content type: the shell's default serializer decodes it. A record written
    /// before this kernel, or by a client that drops headers, still decodes.
    Default,
    /// The content type is the media type at this index of the registered list.
    Registered(usize),
    /// A readable content type nobody registered, the empty string included. Poison: never a
    /// silent fallback to the default, which is how a wrong envelope reaches a handler.
    Unknown(String),
}

/// Route a fetched record by its headers against the media types the shell registered.
///
/// [`content_type`] reads the header: the first [`CONTENT_TYPE`] wins, matched case-sensitively,
/// and a first value that is not UTF-8 reads as absent. The match against `registered` is exact,
/// so a media type with parameters (`; charset=utf-8`) or an empty value is [`Route::Unknown`]. The
/// first registration of a media type wins; a shell refuses duplicates before it gets here.
pub fn route(headers: &[KafkaHeader], registered: &[&str]) -> Route {
    match content_type(headers) {
        None => Route::Default,
        Some(media_type) => match registered.iter().position(|known| *known == media_type) {
            Some(index) => Route::Registered(index),
            None => Route::Unknown(media_type.to_owned()),
        },
    }
}

/// The formats this kernel mints, in the order [`decode_record`] registers their media types.
const FORMATS: [Format; 2] = [Format::Json, Format::Msgpack];

/// Build the record for `env` with the kernel codec: [`Format::encode`] for the value and
/// [`Format::media_type`] for the header. Infallible, like both of those.
pub fn encode_record(format: Format, env: &EventEnvelope, key: Option<&str>) -> KafkaRecord {
    record(env, key, format.encode(env), format.media_type())
}

/// Decode a fetched record back to its envelope.
///
/// [`route`] over every format this kernel mints decides the format; a media type it does not
/// mint is a `codec` error (`unknown format: ...`), never a silent fallback, so a producer rollout
/// the consumer cannot read becomes one poison message instead of a stream of decode failures. No
/// readable content type falls back to `default`.
pub fn decode_record(default: Format, value: &[u8], headers: &[KafkaHeader]) -> Result<EventEnvelope, Error> {
    let media_types = FORMATS.map(|format| format.media_type());
    let format = match route(headers, &media_types) {
        Route::Default => default,
        // Every index `route` returns is one of `media_types`, so the `None` arm is unreachable; it
        // answers with the same refusal an unknown media type gets rather than panicking.
        Route::Registered(index) => FORMATS.get(index).copied().ok_or_else(|| Error::codec("unknown format"))?,
        Route::Unknown(media_type) => Format::from_media_type(&media_type)?,
    };
    format.decode(value)
}

/// Header naming the topic a dead-lettered record was fetched from.
pub const DLQ_TOPIC: &str = "edax-dlq-topic";
/// Header naming the partition a dead-lettered record was fetched from, in decimal.
pub const DLQ_PARTITION: &str = "edax-dlq-partition";
/// Header naming the offset a dead-lettered record was fetched from, in decimal.
pub const DLQ_OFFSET: &str = "edax-dlq-offset";
/// Header carrying why the record was dead-lettered, as text.
pub const DLQ_ERROR: &str = "edax-dlq-error";

/// The record a dead-letter sink publishes for a record it could not handle.
///
/// The key, the value and every header are kept verbatim, so a republish to the source topic routes
/// and decodes exactly as the original did once the fault is fixed: the content type, a middleware's
/// key id and a `traceparent` all survive. Four headers are appended: [`DLQ_TOPIC`],
/// [`DLQ_PARTITION`], [`DLQ_OFFSET`] and [`DLQ_ERROR`]. A record dead-lettered twice carries two sets;
/// [`dead_letter_source`] reads the last. The coordinates are the record's identity, so a sink that
/// sees the same offset twice after a redelivery dedups on them.
pub fn dead_letter_record(record: &KafkaRecord, topic: &str, partition: i32, offset: i64, error: &str) -> KafkaRecord {
    let mut headers = record.headers.clone();
    headers.extend([
        KafkaHeader::new(DLQ_TOPIC, topic.as_bytes()),
        KafkaHeader::new(DLQ_PARTITION, partition.to_string().as_bytes()),
        KafkaHeader::new(DLQ_OFFSET, offset.to_string().as_bytes()),
        KafkaHeader::new(DLQ_ERROR, error.as_bytes()),
    ]);
    KafkaRecord { key: record.key.clone(), value: record.value.clone(), headers }
}

/// Where a dead-lettered record came from and why, as [`dead_letter_record`] wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterSource {
    /// The topic the record was fetched from.
    pub topic: String,
    /// The partition it was fetched from.
    pub partition: i32,
    /// The offset it was fetched from.
    pub offset: i64,
    /// Why it was dead-lettered.
    pub error: String,
}

/// Read the source a dead-letter record names, from the last set of `edax-dlq-*` headers: the most
/// recent dead-lettering. `None` when any of the four is missing or unreadable, so a republish job
/// never guesses a coordinate.
pub fn dead_letter_source(headers: &[KafkaHeader]) -> Option<DeadLetterSource> {
    let last = |name: &str| -> Option<&str> {
        let header = headers.iter().rev().find(|header| header.key == name)?;
        std::str::from_utf8(&header.value).ok()
    };
    Some(DeadLetterSource {
        topic: last(DLQ_TOPIC)?.to_owned(),
        partition: last(DLQ_PARTITION)?.parse().ok()?,
        offset: last(DLQ_OFFSET)?.parse().ok()?,
        error: last(DLQ_ERROR)?.to_owned(),
    })
}

/// The identity of a message that did not decode: `"{topic}/{partition}/{offset}"`.
///
/// Topic, partition and offset together are the only identity a poison message has — it has no
/// envelope and so no id. The offset alone repeats on every partition, so two poison messages would
/// share one id and a dead-letter queue keyed on it would keep the first and reject the second.
pub fn poison_id(topic: &str, partition: i32, offset: i64) -> String {
    format!("{topic}/{partition}/{offset}")
}

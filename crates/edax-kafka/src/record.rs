//! The Kafka record a published message becomes, and the message a fetched record decodes back to.
//!
//! A message is one of two kinds, an event or a command, and one generic set of functions serves
//! both. [`record`] owns the key rule and the header set while the caller owns the value bytes, so a
//! host serializer keeps the contract header. On the way back [`route`] picks the serializer a fetched
//! record's value goes to; the serializer decodes it. The golden vectors pin both.
//!
//! The key is the message's ordering key unless the host overrides it: an event's aggregate id, a
//! command's target aggregate or else its own id, composed with the message's tenant where the
//! deployment's [`Tenancy`] composes keys ([`record_key`]). One aggregate's messages then share one
//! partition and keep their order, and two tenants' same-named aggregates do not share a key.
//!
//! One header, [`CONTENT_TYPE`], because it is the one a consumer reads: [`route`] picks the
//! serializer by it, and its media type names both the format and the kind. No aggregate id (it is
//! the key), no id, name or version (they are in the value), no extension or metadata copies (trace
//! propagation rides in `extensions`). Add a header only with a reader for it: every header costs
//! bytes on every record, and a header nobody reads is a promise nobody keeps. The four `edax-dlq-*`
//! headers ride only on a dead-letter record, and [`dead_letter_source`] is their reader.
//!
//! The `content-type` header names the codec and not the transport, so the string itself is
//! `edax_event::Format::media_type`'s and any transport that carries a type uses the same one.
//!
//! A record without a readable `content-type` goes to the consumer's default serializer when that one
//! reads events: records written before the header existed are all events. A command always carries
//! the header, so a header-less record under a command default is poison ([`Route::Headerless`]).
//!
//! Placement is [`crate::partition_for`]: the shell passes the partition it names with the record.

use edax_core::{tenant_key, Error, ErrorCode, Tenancy};
use edax_event::{Kind, Message};

/// Header naming the wire format of the value bytes; the value is a media type (the vocabulary note
/// on [`edax_event::Format::media_type`] separates the two words). Lower-case, the Kafka convention and the
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
        KafkaHeader {
            key: key.to_owned(),
            value: value.to_vec(),
        }
    }
}

/// One Kafka record: what a producer sends and what a consumer fetched, minus the client's own
/// coordinates (topic, partition, offset), which the IO shell holds. A null key or value (a keyless
/// record, a tombstone) crosses as empty bytes, the same rule as a null header value: an edax record
/// always has both, and a foreign one without them is poison that dead-letters with them empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaRecord {
    /// The partitioning key: the message's ordering key, or the host's override. UTF-8 bytes.
    pub key: Vec<u8>,
    /// The encoded message.
    pub value: Vec<u8>,
    /// The headers: [`record`] writes [`CONTENT_TYPE`] only; a fetched record carries whatever its
    /// producer and the middlewares wrote.
    pub headers: Vec<KafkaHeader>,
}

/// The partitioning key of `msg` under `tenancy`: its [`Message::ordering_key`] (an event's aggregate,
/// a command's target aggregate or else its own id), composed with its tenant where the mode composes
/// keys ([`edax_core::tenant_key`]). Under [`Tenancy::Pool`] two tenants' same-named aggregates then
/// take two keys, and every producer of one topic places an aggregate on one partition. A mode that
/// requires a tenant and a message that names none is a `codec` error.
pub fn record_key<M: Message>(tenancy: Tenancy, msg: &M) -> Result<String, Error> {
    tenant_key(tenancy, msg.tenant(), msg.ordering_key())
}

/// Build the record for a message of either kind from value bytes the caller already encoded.
///
/// `key` overrides the partitioning key outright, and composing a tenant into it is then the host's
/// job; `None` uses [`record_key`] under `tenancy`. One aggregate's messages, and a command with the
/// events it causes, then share one partition and keep their order. `content_type` names the format
/// and kind of `value`, so a host serializer must carry a media type of its own and register it in
/// every SDK that reads the topic. Fails only as [`record_key`] does.
pub fn record<M: Message>(
    msg: &M,
    tenancy: Tenancy,
    key: Option<&str>,
    value: Vec<u8>,
    content_type: &str,
) -> Result<KafkaRecord, Error> {
    let key = match key {
        Some(key) => key.to_owned(),
        None => record_key(tenancy, msg)?,
    };
    Ok(KafkaRecord {
        key: key.into_bytes(),
        value,
        headers: vec![KafkaHeader::new(CONTENT_TYPE, content_type.as_bytes())],
    })
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
    /// No readable content type, and the shell's default serializer reads events: it decodes the
    /// record. A record written before this kernel, or by a client that drops headers, still decodes.
    Default,
    /// No readable content type, and the shell's default serializer reads commands. Poison: a command
    /// always names its media type, so a header-less record on a command topic is not one.
    Headerless,
    /// The content type is the media type at this index of the registered list. The serializer
    /// registered there decides the kind.
    Registered(usize),
    /// A readable content type nobody registered, the empty string included. Poison: never a
    /// silent fallback to the default, which is how a wrong message reaches a handler.
    Unknown(String),
}

/// Route a fetched record by its headers against the media types the shell registered.
///
/// [`content_type`] reads the header: the first [`CONTENT_TYPE`] wins, matched case-sensitively,
/// and a first value that is not UTF-8 reads as absent. The match against `registered` is exact,
/// so a media type with parameters (`; charset=utf-8`) or an empty value is [`Route::Unknown`]. A
/// shell builds `registered` once and holds it to [`check_media_types`] first. A media type names a
/// format and a kind, so one format registers once per kind it reads. `default` is the kind the
/// shell's default serializer reads, which decides a header-less record.
pub fn route(headers: &[KafkaHeader], registered: &[&str], default: Kind) -> Route {
    match content_type(headers) {
        None => match default {
            Kind::Event => Route::Default,
            Kind::Command => Route::Headerless,
        },
        Some(media_type) => match registered.iter().position(|known| *known == media_type) {
            Some(index) => Route::Registered(index),
            None => Route::Unknown(media_type.to_owned()),
        },
    }
}

/// Refuse a registry that names one media type twice, as an `already_registered` error naming it.
/// Two serializers under one media type would decode one topic two ways: a header-less record with
/// the default, a typed record with whichever came first.
pub fn check_media_types(registered: &[&str]) -> Result<(), Error> {
    let mut seen = std::collections::HashSet::new();
    match registered.iter().find(|media_type| !seen.insert(**media_type)) {
        Some(media_type) => Err(Error::new(
            ErrorCode::AlreadyRegistered,
            format_args!("two serializers claim the media type {media_type:?}"),
        )),
        None => Ok(()),
    }
}

/// Header naming the topic a dead-lettered record was fetched from.
pub const DLQ_TOPIC: &str = "edax-dlq-topic";
/// Header naming the partition a dead-lettered record was fetched from, in decimal.
pub const DLQ_PARTITION: &str = "edax-dlq-partition";
/// Header naming the offset a dead-lettered record was fetched from, in decimal.
pub const DLQ_OFFSET: &str = "edax-dlq-offset";
/// Header carrying why the record was dead-lettered, as text: the failure's wire form, `"<code>: <message>"`,
/// or [`DLQ_UNCLASSIFIED`] in place of the code for a failure that carries none.
pub const DLQ_ERROR: &str = "edax-dlq-error";
/// What [`DLQ_ERROR`] names in place of a code for a failure no code classifies (a host's own error), so
/// a republish job groups dead letters by one vocabulary whichever SDK wrote them.
pub const DLQ_UNCLASSIFIED: &str = "unclassified";
/// Header naming how many bytes of value a dead-letter record dropped, in decimal. Only a
/// [`dead_letter_stub`] carries it.
pub const DLQ_DROPPED: &str = "edax-dlq-dropped";

/// The record a dead-letter sink publishes for a record it could not handle.
///
/// The key, the value and every header are kept verbatim, so a republish to the source topic routes
/// and decodes exactly as the original did once the fault is fixed: the content type, a middleware's
/// key id and a `traceparent` all survive. Four headers are appended: [`DLQ_TOPIC`],
/// [`DLQ_PARTITION`], [`DLQ_OFFSET`] and [`DLQ_ERROR`]. A record dead-lettered twice carries two sets;
/// [`dead_letter_source`] reads the last. The coordinates are the record's identity, so a sink that
/// sees the same offset twice after a redelivery dedups on them.
///
/// The failure is `code` and `message`, rendered here so every SDK writes one form:
/// [`ErrorCode::tag`]'s wire form for a coded failure, [`DLQ_UNCLASSIFIED`] in the code's place for
/// one with no code.
pub fn dead_letter_record(
    record: &KafkaRecord,
    topic: &str,
    partition: i32,
    offset: i64,
    code: Option<ErrorCode>,
    message: &str,
) -> KafkaRecord {
    let error = match code {
        Some(code) => code.tag(message),
        None => format!("{DLQ_UNCLASSIFIED}: {message}"),
    };
    let mut headers = record.headers.clone();
    headers.extend([
        KafkaHeader::new(DLQ_TOPIC, topic.as_bytes()),
        KafkaHeader::new(DLQ_PARTITION, partition.to_string().as_bytes()),
        KafkaHeader::new(DLQ_OFFSET, offset.to_string().as_bytes()),
        KafkaHeader::new(DLQ_ERROR, error.as_bytes()),
    ]);
    KafkaRecord {
        key: record.key.clone(),
        value: record.value.clone(),
        headers,
    }
}

/// The dead-letter record `dead` with its value dropped, for a dead-letter topic that refused `dead`
/// itself as one record it cannot take (too large). The key and every header stay, and [`DLQ_DROPPED`]
/// names how many bytes went. The coordinates in the `edax-dlq-*` headers are the pointer: the source
/// topic still holds the value at that offset until its retention passes. A shell sends the stub once
/// the broker refused the full record as a `codec` error ([`crate::broker_error`]), so the dead-letter
/// topic still learns where and why, and the source partition moves on rather than hand the record back
/// forever.
pub fn dead_letter_stub(dead: &KafkaRecord) -> KafkaRecord {
    let mut headers = dead.headers.clone();
    headers.push(KafkaHeader::new(DLQ_DROPPED, dead.value.len().to_string().as_bytes()));
    KafkaRecord {
        key: dead.key.clone(),
        value: Vec::new(),
        headers,
    }
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
    /// How many bytes of value a [`dead_letter_stub`] dropped, `None` for a record that kept its value.
    pub dropped: Option<u64>,
}

/// Read the source a dead-letter record names, from the last set of `edax-dlq-*` headers: the most
/// recent dead-lettering. `None` when any of the four is missing or unreadable, or a [`DLQ_DROPPED`] is
/// unreadable, so a republish job never guesses a coordinate. A stub republished and dead-lettered again
/// still reads as dropped: its value is empty either way.
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
        dropped: match last(DLQ_DROPPED) {
            None => None,
            Some(dropped) => Some(dropped.parse().ok()?),
        },
    })
}

/// The identity of a message that did not decode: `"{topic}/{partition}/{offset}"`.
///
/// Topic, partition and offset together are the only identity a poison message has: it did not
/// decode, so it has no id. The offset alone repeats on every partition, so two poison messages would
/// share one id and a dead-letter queue keyed on it would keep the first and reject the second.
pub fn poison_id(topic: &str, partition: i32, offset: i64) -> String {
    format!("{topic}/{partition}/{offset}")
}

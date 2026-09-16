//! The Kafka record a published envelope becomes, and the envelope a fetched record decodes back to.
//!
//! Two layers. The primitives — [`record`] and [`content_type`] — own the key rule and the header
//! set while the caller owns the value bytes, so a host serializer keeps the contract headers. The
//! composed pair — [`encode_record`] and [`decode_record`] — runs the kernel codec end to end, and
//! is what the golden vectors pin.
//!
//! Four headers, all contract, in this order: [`CONTENT_TYPE`], [`HEADER_ID`], [`HEADER_EVENT`],
//! [`HEADER_EVENT_VERSION`]. No aggregate id (it is the key), no sequence (it is in the value), no
//! metadata copies (trace propagation is the host's own `traceparent`). Add a header only with a
//! reader for it: every header costs bytes on every record.
//!
//! Reserved for later: `partition: Option<u32>` on [`KafkaRecord`], filled from
//! `edax_event::partition(aggregate_id, n)` when a shell passes a partition count. It is **not
//! built** — today the client's partitioner places the record by key, which already keeps one
//! aggregate's events on one partition and in order.

use edax_event::{CodecError, EventEnvelope, Format};

// Vocabulary, because the two words below are not synonyms (RFC 9110 §8.3, RFC 6838):
//
// * `content-type` is a *header*. Its value is a media type, optionally followed by parameters
//   such as `; charset=utf-8`. Functions here that read or write the header say `content_type`.
// * A *media type* is the bare `type/subtype` string the header carries. Functions here that
//   produce that string say `media_type`.
//
// Anatomy of `application/vnd.edax.envelope+json`:
//
// * `application/` — the top-level type for opaque, program-consumed data.
// * `vnd.` — the vendor tree (RFC 6838 §3.2). Subtypes without a prefix are IANA standards such as
//   `application/json`; `vnd.` marks a product-specific format. Registration with IANA is optional
//   there and has not been done, which is fine for a private protocol. `x-` is the deprecated
//   unregistered prefix and is not used.
// * `edax.envelope` — the vendor and the format: an event envelope as `edax-event` encodes it.
// * `+json` / `+msgpack` — the structured-syntax suffix (RFC 6839). It tells tooling that has
//   never heard of edax how to parse the bytes, and it is why plain `application/json` is not
//   used: that name could not tell an edax envelope from any other JSON on a shared topic, and the
//   consumer's registry needs one exact string per serializer.

/// The media type of an envelope encoded with [`Format::Json`]. Private: [`media_type`] is the one
/// way in, so the mapping cannot be read here and written differently there.
const MEDIA_TYPE_JSON: &str = "application/vnd.edax.envelope+json";
/// The media type of an envelope encoded with [`Format::Msgpack`].
const MEDIA_TYPE_MSGPACK: &str = "application/vnd.edax.envelope+msgpack";

/// Header naming the wire format of the value bytes; the value is a media type (see the vocabulary
/// note above). Lower-case, the Kafka convention and the CloudEvents Kafka binding's spelling; the
/// lookup in [`content_type`] is case-sensitive, so a producer must write it exactly like this.
pub const CONTENT_TYPE: &str = "content-type";
/// Header carrying the envelope id, so a consumer can dedup or trace without decoding the value.
pub const HEADER_ID: &str = "edax-id";
/// Header carrying the event name, so a router can filter without decoding the value.
pub const HEADER_EVENT: &str = "edax-event";
/// Header carrying the event version as ASCII decimal, so an upcaster can plan without decoding the
/// value. Text, not a binary int: header values have no shared integer encoding across clients.
pub const HEADER_EVENT_VERSION: &str = "edax-event-version";

/// One Kafka record header. The value is bytes, not text: a foreign header on the same topic may
/// carry anything, and the kernel must hand it back unchanged rather than lose it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaHeader {
    /// The header name, as the client hands it over.
    pub key: String,
    /// The header value, uninterpreted.
    pub value: Vec<u8>,
}

/// One Kafka record: what a producer sends and what a consumer fetched, minus the client's own
/// coordinates (topic, partition, offset), which the IO shell holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaRecord {
    /// The partitioning key: the aggregate id, or the host's override. UTF-8 bytes.
    pub key: Vec<u8>,
    /// The encoded envelope.
    pub value: Vec<u8>,
    /// The four contract headers, in the order [`record`] writes them.
    pub headers: Vec<KafkaHeader>,
}

/// The media type that names `format` on the wire, `application/vnd.edax.envelope+<suffix>` (the
/// vocabulary note at the top of this file explains each part). `Format` is `#[non_exhaustive]`: a variant this
/// kernel does not know is a [`CodecError::UnknownFormat`], never a fallback, so a record can never
/// carry a header that names the wrong codec. The change that adds a variant mints its media type here.
pub fn media_type(format: Format) -> Result<&'static str, CodecError> {
    match format {
        Format::Json => Ok(MEDIA_TYPE_JSON),
        Format::Msgpack => Ok(MEDIA_TYPE_MSGPACK),
        other => Err(CodecError::UnknownFormat(format!("{other:?}"))),
    }
}

/// Build the record for `env` from value bytes the caller already encoded.
///
/// `key` overrides the partitioning key; `None` uses the aggregate id, which is what keeps one
/// aggregate's events on one partition and therefore in order. `content_type` names the format of
/// `value`, so a host serializer must carry a media type of its own and register it in every SDK
/// that reads the topic.
pub fn record(env: &EventEnvelope, key: Option<&str>, value: Vec<u8>, content_type: &str) -> KafkaRecord {
    let header = |key: &str, value: &[u8]| KafkaHeader { key: key.to_owned(), value: value.to_vec() };
    KafkaRecord {
        key: key.unwrap_or(&env.aggregate_id).as_bytes().to_vec(),
        value,
        headers: vec![
            header(CONTENT_TYPE, content_type.as_bytes()),
            header(HEADER_ID, env.id.as_bytes()),
            header(HEADER_EVENT, env.event.name.as_bytes()),
            header(HEADER_EVENT_VERSION, env.event.version.to_string().as_bytes()),
        ],
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

/// Build the record for `env` with the kernel codec: [`Format::encode`] for the value and
/// [`media_type`] for the header.
pub fn encode_record(format: Format, env: &EventEnvelope, key: Option<&str>) -> Result<KafkaRecord, CodecError> {
    Ok(record(env, key, format.encode(env), media_type(format)?))
}

/// Decode a fetched record back to its envelope.
///
/// The record's own [`CONTENT_TYPE`] decides the format; a media type this kernel does not mint is
/// a [`CodecError::UnknownFormat`], never a silent fallback, so a producer rollout the consumer
/// cannot read becomes one poison message instead of a stream of decode failures. No readable
/// content type falls back to `default`, so a header-less record — anything written before this
/// kernel, or by a client that drops headers — still decodes.
pub fn decode_record(default: Format, value: &[u8], headers: &[KafkaHeader]) -> Result<EventEnvelope, CodecError> {
    let format = match content_type(headers) {
        Some(media_type) => format_of(media_type)?,
        None => default,
    };
    format.decode(value)
}

/// The inverse of [`media_type`]: the only two media types a record may name.
fn format_of(media_type: &str) -> Result<Format, CodecError> {
    match media_type {
        MEDIA_TYPE_JSON => Ok(Format::Json),
        MEDIA_TYPE_MSGPACK => Ok(Format::Msgpack),
        other => Err(CodecError::UnknownFormat(other.to_owned())),
    }
}

/// The identity of a message that did not decode: `"{topic}/{partition}/{offset}"`.
///
/// Topic, partition and offset together are the only identity a poison message has — it has no
/// envelope and so no id. The offset alone repeats on every partition, so two poison messages would
/// share one id and a dead-letter queue keyed on it would keep the first and reject the second.
pub fn poison_id(topic: &str, partition: i32, offset: i64) -> String {
    format!("{topic}/{partition}/{offset}")
}

#[cfg(test)]
// A test asserts on the value it just built; a panic there is the failure report.
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    /// The header lookup, whose branches the golden vectors cannot reach: a golden case carries a
    /// decodable record, and three of these four carry nothing to decode.
    #[test]
    fn content_type_reads_the_first_readable_header_only() {
        struct Case {
            name: &'static str,
            headers: Vec<KafkaHeader>,
            expected: Option<&'static str>,
        }
        let header = |key: &str, value: &[u8]| KafkaHeader { key: key.to_owned(), value: value.to_vec() };
        let cases = [
            Case { name: "absent", headers: vec![header(HEADER_ID, b"evt-1")], expected: None },
            Case {
                name: "present",
                headers: vec![header(CONTENT_TYPE, MEDIA_TYPE_MSGPACK.as_bytes())],
                expected: Some(MEDIA_TYPE_MSGPACK),
            },
            Case {
                name: "the first of two wins",
                headers: vec![
                    header(CONTENT_TYPE, MEDIA_TYPE_MSGPACK.as_bytes()),
                    header(CONTENT_TYPE, MEDIA_TYPE_JSON.as_bytes()),
                ],
                expected: Some(MEDIA_TYPE_MSGPACK),
            },
            Case {
                // A client that upper-cased the name wrote a different header, not this one.
                name: "the name is matched case-sensitively",
                headers: vec![header("Content-Type", MEDIA_TYPE_JSON.as_bytes())],
                expected: None,
            },
            Case {
                // Reads as absent, and does not reach past itself to the second header.
                name: "a non-UTF-8 first value is no media type",
                headers: vec![
                    header(CONTENT_TYPE, &[0xff, 0xfe]),
                    header(CONTENT_TYPE, MEDIA_TYPE_JSON.as_bytes()),
                ],
                expected: None,
            },
        ];
        for case in cases {
            assert_eq!(content_type(&case.headers), case.expected, "{}", case.name);
        }
    }
}

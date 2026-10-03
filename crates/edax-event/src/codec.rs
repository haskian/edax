//! Envelope codecs. The lib provides deterministic, built-in serializations so that any two
//! services agree on the bytes for a given logical envelope. Each format sorts map keys (via
//! `BTreeMap`) and fixes field order, so encoding is reproducible across languages.
//!
//! Built-in formats: [`Format::Json`] (human-readable) and [`Format::Msgpack`] (compact binary).
//!
//! One evolution rule for both, pinned by `testdata/golden/envelope_decode.json`:
//!
//! - The envelope and its nested `event` are **maps** keyed by field name in both formats, never
//!   positional arrays. A reader refuses an array where a map belongs.
//! - A reader **ignores a field it does not know**, so a writer may add one and an older reader keeps
//!   decoding (forward compatible). A missing `sequence` reads as `None`; every other field is
//!   required.
//! - **Trailing bytes** after the envelope are refused in both formats: a frame holds one envelope.
//! - A repeated field is refused; a repeated key inside `namespace` or `metadata` keeps its last value.
//!
//! Custom formats are NOT handled here. The consistency primitives ([`crate::partition()`],
//! [`crate::is_stale`]) operate on fields, not encoded bytes, so a service that serializes its
//! own envelopes with a format the lib doesn't know still routes and orders correctly. It carries
//! its own bytes and skips this module.

use crate::domain::EventEnvelope;
use edax_core::Error;

/// The media type of an envelope encoded with [`Format::Json`]. Private, so the mapping cannot be
/// read through [`Format::media_type`] here and written differently there.
const MEDIA_TYPE_JSON: &str = "application/vnd.edax.envelope+json";
/// The media type of an envelope encoded with [`Format::Msgpack`].
const MEDIA_TYPE_MSGPACK: &str = "application/vnd.edax.envelope+msgpack";

/// A built-in envelope serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Format {
    /// Human-readable JSON; `data` is base64.
    Json,
    /// Compact MessagePack; `data` is native bytes.
    Msgpack,
}

impl Format {
    /// Resolve a format by name (`"json"` / `"msgpack"`). Used by the bindings. An unknown name is
    /// a `codec` error.
    pub fn from_name(name: &str) -> Result<Format, Error> {
        match name {
            "json" => Ok(Format::Json),
            "msgpack" => Ok(Format::Msgpack),
            other => Err(unknown_format(other)),
        }
    }

    /// The name a binding selects this format by, the inverse of [`from_name`](Format::from_name).
    pub fn name(&self) -> &'static str {
        match self {
            Format::Json => "json",
            Format::Msgpack => "msgpack",
        }
    }

    // Vocabulary, because the two words below are not synonyms (RFC 9110 §8.3, RFC 6838):
    //
    // * `content-type` is a *header*. Its value is a media type, optionally followed by parameters
    //   such as `; charset=utf-8`. Functions that read or write the header say `content_type`.
    // * A *media type* is the bare `type/subtype` string the header carries. Functions that produce
    //   that string say `media_type`.
    //
    // Anatomy of `application/vnd.edax.envelope+json`:
    //
    // * `application/` — the top-level type for opaque, program-consumed data.
    // * `vnd.` — the vendor tree (RFC 6838 §3.2). Subtypes without a prefix are IANA standards such
    //   as `application/json`; `vnd.` marks a product-specific format. Registration with IANA is
    //   optional there and has not been done, which is fine for a private protocol. `x-` is the
    //   deprecated unregistered prefix and is not used.
    // * `edax.envelope` — the vendor and the format: an event envelope as `edax-event` encodes it.
    // * `+json` / `+msgpack` — the structured-syntax suffix (RFC 6839). It tells tooling that has
    //   never heard of edax how to parse the bytes, and it is why plain `application/json` is not
    //   used: that name could not tell an edax envelope from any other JSON on a shared topic, and
    //   the consumer's registry needs one exact string per serializer.

    /// The media type that names this format on the wire, `application/vnd.edax.envelope+<suffix>`
    /// (the vocabulary note above explains each part). It names the *codec*, not the transport, so
    /// any transport that carries a type — a Kafka `content-type` header, an HTTP request — uses
    /// this one string. Infallible: the match is exhaustive here, in the crate that defines
    /// `Format`, so the change that adds a variant mints its media type in the same edit.
    pub fn media_type(&self) -> &'static str {
        match self {
            Format::Json => MEDIA_TYPE_JSON,
            Format::Msgpack => MEDIA_TYPE_MSGPACK,
        }
    }

    /// The inverse of [`media_type`](Format::media_type): the only media types edax mints.
    ///
    /// Anything else is a `codec` error (`unknown format: ...`) and never a fallback, so a consumer
    /// reading a producer rollout it does not know refuses one message instead of decoding it as the
    /// wrong format. The string must match whole: parameters such as `; charset=utf-8` are not
    /// stripped.
    pub fn from_media_type(media_type: &str) -> Result<Format, Error> {
        match media_type {
            MEDIA_TYPE_JSON => Ok(Format::Json),
            MEDIA_TYPE_MSGPACK => Ok(Format::Msgpack),
            other => Err(unknown_format(other)),
        }
    }

    /// Encode an envelope to this format's canonical wire bytes: a map keyed by field name in both
    /// formats (MessagePack through `to_vec_named`, never the positional array). Infallible: the
    /// envelope only holds strings, ints, byte vecs, and string-keyed maps, none of which can fail to
    /// serialize.
    #[allow(clippy::expect_used, reason = "the envelope's field types cannot fail to serialize")]
    pub fn encode(self, env: &EventEnvelope) -> Vec<u8> {
        match self {
            Format::Json => serde_json::to_vec(env).expect("envelope is always serializable"),
            Format::Msgpack => rmp_serde::to_vec_named(env).expect("envelope is always serializable"),
        }
    }

    /// Decode this format's wire bytes back to an envelope, under the evolution rule in the module
    /// docs. Bytes that are not a valid envelope in this format are a `codec` error: the same bytes
    /// fail the same way every time.
    pub fn decode(self, bytes: &[u8]) -> Result<EventEnvelope, Error> {
        match self {
            Format::Json => {
                let mut de = serde_json::Deserializer::from_slice(bytes);
                let env = map_only(&mut de).map_err(decode_error)?;
                // `end` refuses anything but trailing whitespace.
                de.end().map_err(decode_error)?;
                Ok(env)
            }
            Format::Msgpack => {
                let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(bytes));
                let env = map_only(&mut de).map_err(decode_error)?;
                if usize::try_from(de.position()).ok() != Some(bytes.len()) {
                    return Err(Error::codec("decode: trailing bytes after the envelope"));
                }
                Ok(env)
            }
        }
    }
}

fn decode_error(error: impl std::fmt::Display) -> Error {
    Error::codec(format_args!("decode: {error}"))
}

/// Deserialize `T` from a map only. serde's derive also reads a struct from a positional array, so
/// without this a reader would accept two shapes of one envelope, and a writer that added a field
/// would make a fixed-length array poison for every older reader.
pub(crate) fn map_only<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    struct MapOnly<T>(std::marker::PhantomData<T>);
    impl<'de, T: serde::Deserialize<'de>> serde::de::Visitor<'de> for MapOnly<T> {
        type Value = T;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a map keyed by field name")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<T, A::Error> {
            T::deserialize(serde::de::value::MapAccessDeserializer::new(map))
        }
    }
    deserializer.deserialize_map(MapOnly(std::marker::PhantomData))
}

fn unknown_format(name: &str) -> Error {
    Error::codec(format_args!("unknown format: {name}"))
}

/// serde helper for the `data` byte field: base64 string in human-readable formats, native bytes
/// in binary ones. Keeps one struct correct across both JSON and MessagePack.
pub(crate) mod bytes_field {
    use base64::{engine::general_purpose::STANDARD as B64, Engine};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.serialize_str(&B64.encode(bytes))
        } else {
            s.serialize_bytes(bytes)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        if d.is_human_readable() {
            let s = String::deserialize(d)?;
            B64.decode(s.as_bytes()).map_err(serde::de::Error::custom)
        } else {
            Ok(serde_bytes::ByteBuf::deserialize(d)?.into_vec())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Event, Metadata, Namespace};

    fn sample() -> EventEnvelope {
        let mut namespace = Namespace::new();
        namespace.insert("tenant".into(), "acme".into());
        let mut metadata = Metadata::new();
        metadata.insert("trace".into(), "abc-123".into());
        EventEnvelope {
            id: "evt-1".into(),
            aggregate_id: "agg-1".into(),
            event: Event {
                name: "InvoiceIssued".into(),
                version: 1,
                data: vec![0xde, 0xad, 0xbe, 0xef],
            },
            sequence: Some(7),
            namespace,
            metadata,
            timestamp: 1_700_000_000_000_000_000,
        }
    }

    #[test]
    fn each_format_round_trips() {
        for fmt in [Format::Json, Format::Msgpack] {
            let env = sample();
            let wire = fmt.encode(&env);
            assert_eq!(fmt.decode(&wire).unwrap(), env, "{fmt:?}");
        }
    }

    #[test]
    fn json_is_human_readable_msgpack_is_compact() {
        let env = sample();
        let json = Format::Json.encode(&env);
        let msgpack = Format::Msgpack.encode(&env);
        // JSON is valid UTF-8 text containing the base64 payload; msgpack is smaller and binary.
        assert!(std::str::from_utf8(&json).unwrap().contains("3q2+7w==")); // base64 of deadbeef
        assert!(msgpack.len() < json.len());
    }

    #[test]
    fn map_keys_encode_in_sorted_order() {
        // Reproducible bytes across languages rest on this: whatever order a host filled the maps in,
        // the encoding sorts the keys.
        let mut env = sample();
        env.metadata = Metadata::new();
        for key in ["trace", "actor"] {
            env.metadata.insert(key.into(), "x".into());
        }
        let json = String::from_utf8(Format::Json.encode(&env)).unwrap();
        assert!(json.contains(r#""metadata":{"actor":"x","trace":"x"}"#), "{json}");
    }

    /// The next envelope version as a writer would ship it: today's fields plus one more, in the
    /// event and in the envelope. A reader of either version must read the other's bytes.
    mod next_version {
        use crate::domain::{Metadata, Namespace};
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Serialize, Deserialize)]
        pub struct Event {
            pub name: String,
            pub version: i32,
            #[serde(with = "crate::codec::bytes_field")]
            pub data: Vec<u8>,
            #[serde(default)]
            pub schema: Option<String>,
        }

        #[derive(Debug, Serialize, Deserialize)]
        pub struct EventEnvelope {
            pub id: String,
            pub aggregate_id: String,
            pub event: Event,
            pub sequence: Option<u64>,
            pub namespace: Namespace,
            pub metadata: Metadata,
            pub timestamp: i64,
            #[serde(default)]
            pub priority: Option<String>,
        }
    }

    fn next_version_of(env: &EventEnvelope) -> next_version::EventEnvelope {
        next_version::EventEnvelope {
            id: env.id.clone(),
            aggregate_id: env.aggregate_id.clone(),
            event: next_version::Event {
                name: env.event.name.clone(),
                version: env.event.version,
                data: env.event.data.clone(),
                schema: Some("invoice.v2".into()),
            },
            sequence: env.sequence,
            namespace: env.namespace.clone(),
            metadata: env.metadata.clone(),
            timestamp: env.timestamp,
            priority: Some("high".into()),
        }
    }

    /// A newer writer adds a field: this reader ignores it, in both formats, so a producer rollout
    /// does not turn every message into poison for the consumers that have not upgraded yet.
    #[test]
    fn a_newer_writers_envelope_reads_here() {
        let env = sample();
        let newer = next_version_of(&env);
        let wires = [
            (Format::Json, serde_json::to_vec(&newer).unwrap()),
            (Format::Msgpack, rmp_serde::to_vec_named(&newer).unwrap()),
        ];
        for (format, wire) in wires {
            assert_eq!(format.decode(&wire).unwrap(), env, "{format:?}");
        }
    }

    /// The reverse: this writer's bytes read in a newer reader whose added fields default, so a
    /// consumer can upgrade before the producers do.
    #[test]
    fn this_writers_envelope_reads_in_a_newer_reader() {
        let env = sample();
        for format in [Format::Json, Format::Msgpack] {
            let wire = format.encode(&env);
            let newer: next_version::EventEnvelope = match format {
                Format::Json => serde_json::from_slice(&wire).unwrap(),
                _ => rmp_serde::from_slice(&wire).unwrap(),
            };
            assert_eq!((newer.priority, newer.event.schema), (None, None), "{format:?}");
            assert_eq!((newer.id, newer.event.data, newer.sequence), (env.id.clone(), env.event.data.clone(), env.sequence));
        }
    }

    /// The trailing-bytes rule in both formats: whitespace after a JSON document is not data, any
    /// byte after a MessagePack envelope is.
    #[test]
    fn a_frame_holds_one_envelope() {
        let env = sample();
        let cases: [(&str, Format, &[u8], bool); 4] = [
            ("json_trailing_whitespace_is_not_data", Format::Json, b" \n", true),
            ("json_trailing_document", Format::Json, b"{}", false),
            ("msgpack_trailing_byte", Format::Msgpack, b"\xc0", false),
            ("msgpack_second_envelope", Format::Msgpack, b"\x80", false),
        ];
        for (name, format, trailing, decodes) in cases {
            let mut wire = format.encode(&env);
            wire.extend_from_slice(trailing);
            assert_eq!(format.decode(&wire).is_ok(), decodes, "{name}");
        }
    }

    /// Both vocabularies round-trip, so the name a binding selects by and the media type a record
    /// carries cannot drift from the format they resolve to.
    #[test]
    fn every_format_round_trips_through_its_name_and_its_media_type() {
        for fmt in [Format::Json, Format::Msgpack] {
            assert_eq!(Format::from_name(fmt.name()).unwrap(), fmt);
            assert_eq!(Format::from_media_type(fmt.media_type()).unwrap(), fmt);
            // The suffix is the structured-syntax one (RFC 6839), so tooling that never heard of
            // edax still knows how to parse the bytes.
            assert_eq!(fmt.media_type(), format!("application/vnd.edax.envelope+{}", fmt.name()));
        }
    }

    #[test]
    fn malformed_input_is_rejected() {
        assert!(Format::from_name("protobuf").is_err(), "unknown format name");
        assert!(Format::from_media_type("application/json").is_err(), "a media type edax never mints");
        assert!(
            Format::from_media_type("application/vnd.edax.envelope+json; charset=utf-8").is_err(),
            "parameters are not stripped, so the string must match whole"
        );
        assert!(
            Format::Json.decode(b"not json").is_err(),
            "json wire bytes are not json"
        );
        assert!(
            Format::Msgpack.decode(b"not msgpack").is_err(),
            "msgpack wire bytes are not msgpack"
        );
        assert!(
            Format::Json.decode(br#"{"id": "evt-1"}"#).is_err(),
            "json is missing envelope fields"
        );
        // `data` is base64 in the human-readable format, so an unpaddable string is a decode error.
        let bad_base64 = br#"{"id":"e","aggregate_id":"a","event":{"name":"N","version":1,"data":"!!"},
            "sequence":null,"namespace":{},"metadata":{},"timestamp":0}"#;
        assert!(Format::Json.decode(bad_base64).is_err(), "data is not base64");
    }
}

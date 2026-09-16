//! Envelope codecs. The lib provides deterministic, built-in serializations so that any two
//! services agree on the bytes for a given logical envelope. Each format sorts map keys (via
//! `BTreeMap`) and fixes field order, so encoding is reproducible across languages.
//!
//! Built-in formats: [`Format::Json`] (human-readable) and [`Format::Msgpack`] (compact binary).
//!
//! Custom formats are NOT handled here. The consistency primitives ([`crate::partition`],
//! [`crate::should_apply`]) operate on fields, not encoded bytes — so a service that serializes its
//! own envelopes with a format the lib doesn't know still routes and dedups correctly. It simply
//! carries its own bytes and skips this module.

use crate::domain::EventEnvelope;
use std::fmt;

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
    /// Resolve a format by name (`"json"` / `"msgpack"`). Used by the bindings.
    pub fn from_name(name: &str) -> Result<Format, CodecError> {
        match name {
            "json" => Ok(Format::Json),
            "msgpack" => Ok(Format::Msgpack),
            other => Err(CodecError::UnknownFormat(other.to_string())),
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
    /// Anything else is a [`CodecError::UnknownFormat`] and never a fallback, so a consumer reading
    /// a producer rollout it does not know refuses one message instead of decoding it as the wrong
    /// format. The string must match whole: parameters such as `; charset=utf-8` are not stripped.
    pub fn from_media_type(media_type: &str) -> Result<Format, CodecError> {
        match media_type {
            MEDIA_TYPE_JSON => Ok(Format::Json),
            MEDIA_TYPE_MSGPACK => Ok(Format::Msgpack),
            other => Err(CodecError::UnknownFormat(other.to_owned())),
        }
    }

    /// Encode an envelope to this format's canonical wire bytes. Infallible: the envelope only
    /// holds strings, ints, byte vecs, and string-keyed maps, none of which can fail to serialize.
    pub fn encode(self, env: &EventEnvelope) -> Vec<u8> {
        match self {
            Format::Json => serde_json::to_vec(env).expect("envelope is always serializable"),
            Format::Msgpack => rmp_serde::to_vec(env).expect("envelope is always serializable"),
        }
    }

    /// Decode this format's wire bytes back to an envelope.
    pub fn decode(self, bytes: &[u8]) -> Result<EventEnvelope, CodecError> {
        match self {
            Format::Json => serde_json::from_slice(bytes).map_err(|e| CodecError::Decode(e.to_string())),
            Format::Msgpack => rmp_serde::from_slice(bytes).map_err(|e| CodecError::Decode(e.to_string())),
        }
    }
}

/// A marshaling failure. Deterministic: the same bytes fail the same way every time.
#[derive(Debug)]
#[non_exhaustive]
pub enum CodecError {
    /// The wire-format name is not one this kernel knows.
    UnknownFormat(String),
    /// The bytes are not a valid envelope in the named format.
    Decode(String),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::UnknownFormat(s) => write!(f, "unknown format: {s}"),
            CodecError::Decode(s) => write!(f, "decode: {s}"),
        }
    }
}

impl std::error::Error for CodecError {}

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

// --- JSON boundary transcoders used by the bindings ---
//
// The bindings always hand the envelope to the lib as JSON (the universal, easy-to-build host
// interchange) and select the wire `format` separately. These transcode JSON <-> wire so the host
// API stays uniform while the on-the-wire format is JSON, MessagePack, or (host-side) custom.
//
// Errors are `String` here, not `CodecError`: these helpers serve the Python binding, which carries a
// flat error message across the boundary. The typed `CodecError` is for native Rust callers.

/// Parse boundary JSON and encode to `format`'s wire bytes.
pub fn encode_from_json(format: Format, json: &[u8]) -> Result<Vec<u8>, String> {
    let env: EventEnvelope = serde_json::from_slice(json).map_err(|e| e.to_string())?;
    Ok(format.encode(&env))
}

/// Decode `format`'s wire bytes and emit boundary JSON.
pub fn decode_to_json(format: Format, wire: &[u8]) -> Result<Vec<u8>, String> {
    let env = format.decode(wire).map_err(|e| e.to_string())?;
    serde_json::to_vec(&env).map_err(|e| e.to_string())
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

    #[test]
    fn json_boundary_transcodes_to_each_format() {
        let json = serde_json::to_vec(&sample()).unwrap();
        for fmt in [Format::Json, Format::Msgpack] {
            let wire = encode_from_json(fmt, &json).unwrap();
            let back = decode_to_json(fmt, &wire).unwrap();
            // Round-tripping through the wire preserves the logical envelope.
            assert_eq!(encode_from_json(fmt, &back).unwrap(), wire, "{fmt:?}");
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
        assert!(
            encode_from_json(Format::Msgpack, bad_base64).is_err(),
            "boundary json rejects bad data"
        );
        assert!(
            decode_to_json(Format::Msgpack, b"not msgpack").is_err(),
            "boundary rejects bad wire bytes"
        );
    }
}

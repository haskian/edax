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

    #[test]
    fn malformed_input_is_rejected() {
        assert!(Format::from_name("protobuf").is_err(), "unknown format name");
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

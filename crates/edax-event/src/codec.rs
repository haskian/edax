//! Message codecs. The lib provides deterministic, built-in serializations so that any two services
//! agree on the bytes for a given logical message. Each format sorts map keys (via `BTreeMap`) and
//! fixes field order, so encoding is reproducible across languages.
//!
//! Built-in formats: [`Format::Json`] (human-readable) and [`Format::Msgpack`] (compact binary). Each
//! encodes both kinds of [`Message`], [`EventMessage`](crate::EventMessage) and
//! [`CommandMessage`](crate::CommandMessage), and a media type names the
//! pair of format and kind.
//!
//! One evolution rule for both formats and both kinds, pinned by `testdata/golden/envelope_decode.json`:
//!
//! - A message is a **map** keyed by field name in both formats, never a positional array, and so is
//!   every struct inside it. A reader refuses an array where a map belongs ([`StructsFromMaps`]).
//! - A reader **ignores a field it does not know**, so a writer may add one and an older reader keeps
//!   decoding (forward compatible). An optional field (`tenant`, `sequence`, `occurred_at`,
//!   `idempotency_key`, a command's `aggregate_id`) is written as `null` and reads as `None` when
//!   missing; every other field is required.
//! - **Trailing bytes** after the message are refused in both formats: a frame holds one message.
//! - A repeated field is refused; a repeated key inside `extensions` or `metadata` keeps its last value.
//! - An [`Extensions`](crate::message::Extensions) key outside [`check_extensions`] is refused on
//!   encode and on decode, so a producer never writes what every consumer would refuse.
//! - A `tenant` outside [`check_tenant`] (empty, or over [`edax_core::TENANT_MAX`] bytes) is refused
//!   on encode and on decode, so a tenant another service minted is checked before any key is composed
//!   from it.
//!
//! The bytes do not say which kind they hold: the media type does. A transport that carries one, such
//! as a Kafka `content-type` header, decodes by it ([`Format::from_media_type`]), and a decode of the
//! wrong kind fails on that, not on whichever field happens to be missing.
//!
//! A domain event's or command's **payload** meets the same format rules through
//! [`Format::encode_payload`] and [`Format::decode_payload`], which every SDK codec calls: a struct at any
//! depth reads from a map only, a plain string or number reads as itself, and trailing bytes are refused.
//! Pinned, through the Rust SDK's registry codec, by `testdata/golden/event_codec.json`.
//!
//! Custom formats are NOT handled here. The consistency primitives ([`crate::partition()`],
//! [`crate::is_stale`]) operate on fields, not encoded bytes, so a service that serializes its own
//! messages with a format the lib doesn't know still routes and orders correctly.

mod maps;

pub use maps::StructsFromMaps;

use crate::message::{check_extensions, Kind, Message};
use edax_core::check_tenant;
use edax_core::Error;
use serde::de::{DeserializeOwned, DeserializeSeed};
use serde::Serialize;
use std::marker::PhantomData;

/// The media types edax mints, one per format and kind. Private, so the mapping cannot be read
/// through [`Format::media_type`] here and written differently there.
const MEDIA_TYPE_EVENT_JSON: &str = "application/vnd.edax.event+json";
const MEDIA_TYPE_EVENT_MSGPACK: &str = "application/vnd.edax.event+msgpack";
const MEDIA_TYPE_COMMAND_JSON: &str = "application/vnd.edax.command+json";
const MEDIA_TYPE_COMMAND_MSGPACK: &str = "application/vnd.edax.command+msgpack";

edax_core::closed_names! {
    /// A built-in message serialization, named `"json"` or `"msgpack"`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[non_exhaustive]
    pub enum Format {
        /// Human-readable JSON; `data` is base64.
        Json = "json",
        /// Compact MessagePack; `data` is native bytes.
        Msgpack = "msgpack",
    }
}

/// The field rules the codec holds every message to, in both directions: the extension key rule, and
/// the tenant rule when the message names a tenant. Checking on decode as well as on encode makes the
/// wire a trust boundary, so a tenant minted by another service is checked before a key is composed
/// from it.
fn check_message<M: Message>(msg: &M) -> Result<(), Error> {
    check_extensions(msg.extensions())?;
    msg.tenant().map_or(Ok(()), check_tenant)
}

impl Format {
    /// Resolve a format by name (`"json"` / `"msgpack"`). Used by the bindings. An unknown name is
    /// a `codec` error.
    pub fn from_name(name: &str) -> Result<Format, Error> {
        Format::by_name(name).ok_or_else(|| unknown_format(name))
    }

    // Vocabulary, because the two words below are not synonyms (RFC 9110 §8.3, RFC 6838):
    //
    // * `content-type` is a *header*. Its value is a media type, optionally followed by parameters
    //   such as `; charset=utf-8`. Functions that read or write the header say `content_type`.
    // * A *media type* is the bare `type/subtype` string the header carries. Functions that produce
    //   that string say `media_type`.
    //
    // Anatomy of `application/vnd.edax.event+json`:
    //
    // * `application/` — the top-level type for opaque, program-consumed data.
    // * `vnd.` — the vendor tree (RFC 6838 §3.2). Subtypes without a prefix are IANA standards such
    //   as `application/json`; `vnd.` marks a product-specific format. Registration with IANA is
    //   optional there and has not been done, which is fine for a private protocol. `x-` is the
    //   deprecated unregistered prefix and is not used.
    // * `edax.event` / `edax.command` — the vendor and the kind of message the bytes hold.
    // * `+json` / `+msgpack` — the structured-syntax suffix (RFC 6839). It tells tooling that has
    //   never heard of edax how to parse the bytes, and it is why plain `application/json` is not
    //   used: that name could not tell an edax message from any other JSON on a shared topic, and
    //   the consumer's registry needs one exact string per serializer.

    /// The media type that names this format and `kind` on the wire,
    /// `application/vnd.edax.<kind>+<suffix>` (the vocabulary note above explains each part). It names
    /// the *codec*, not the transport, so any transport that carries a type uses this one string.
    /// Infallible: both matches are exhaustive here, so the change that adds a variant mints its media
    /// type in the same edit.
    pub fn media_type(&self, kind: Kind) -> &'static str {
        match (self, kind) {
            (Format::Json, Kind::Event) => MEDIA_TYPE_EVENT_JSON,
            (Format::Msgpack, Kind::Event) => MEDIA_TYPE_EVENT_MSGPACK,
            (Format::Json, Kind::Command) => MEDIA_TYPE_COMMAND_JSON,
            (Format::Msgpack, Kind::Command) => MEDIA_TYPE_COMMAND_MSGPACK,
        }
    }

    /// The inverse of [`media_type`](Format::media_type): the format and kind of the only media types
    /// edax mints.
    ///
    /// Anything else is a `codec` error (`unknown format: ...`) and never a fallback, so a consumer
    /// reading a producer rollout it does not know refuses one message instead of decoding it as the
    /// wrong format. The string must match whole: parameters such as `; charset=utf-8` are not
    /// stripped.
    pub fn from_media_type(media_type: &str) -> Result<(Format, Kind), Error> {
        match media_type {
            MEDIA_TYPE_EVENT_JSON => Ok((Format::Json, Kind::Event)),
            MEDIA_TYPE_EVENT_MSGPACK => Ok((Format::Msgpack, Kind::Event)),
            MEDIA_TYPE_COMMAND_JSON => Ok((Format::Json, Kind::Command)),
            MEDIA_TYPE_COMMAND_MSGPACK => Ok((Format::Msgpack, Kind::Command)),
            other => Err(unknown_format(other)),
        }
    }

    /// Encode a message of either kind to this format's canonical wire bytes: a map keyed by field
    /// name in both formats (MessagePack through `to_vec_named`, never the positional array). A bad
    /// extension key or tenant id is a `codec` error. A message holds only strings, integers, bytes and
    /// string maps, so the serializer itself has nothing to refuse.
    pub fn encode<M: Message + Serialize>(self, msg: &M) -> Result<Vec<u8>, Error> {
        check_message(msg)?;
        self.encode_payload(msg)
    }

    /// Decode this format's wire bytes to a message of kind `M`, under the evolution rule in the module
    /// docs. Bytes that are not a valid `M` in this format are a `codec` error: the same bytes fail the
    /// same way every time.
    pub fn decode<M: Message + DeserializeOwned>(self, bytes: &[u8]) -> Result<M, Error> {
        let msg: M = self.decode_payload(bytes, PhantomData)?;
        check_message(&msg)?;
        Ok(msg)
    }

    /// Encode any serde value to this format's bytes: JSON through `serde_json`, MessagePack with every
    /// struct as a map keyed by field name (`to_vec_named`), as Python's msgspec writes it. A value the
    /// format cannot write (a non-string JSON map key, a refusing `Serialize`) is a `codec` error.
    pub fn encode_payload<T: Serialize + ?Sized>(self, value: &T) -> Result<Vec<u8>, Error> {
        match self {
            Format::Json => serde_json::to_vec(value).map_err(Error::codec),
            Format::Msgpack => rmp_serde::to_vec_named(value).map_err(Error::codec),
        }
    }

    /// Decode exactly one value from this format's bytes through `seed`, under the payload rules: every
    /// struct at any depth reads from a map only ([`StructsFromMaps`]), and trailing bytes are refused
    /// (whitespace after a JSON value is not data). Bytes that are not such a value are a `codec` error.
    pub fn decode_payload<'de, S: DeserializeSeed<'de>>(self, bytes: &'de [u8], seed: S) -> Result<S::Value, Error> {
        match self {
            Format::Json => {
                let mut de = serde_json::Deserializer::from_slice(bytes);
                let value = seed.deserialize(StructsFromMaps(&mut de)).map_err(decode_error)?;
                de.end().map_err(decode_error)?;
                Ok(value)
            }
            Format::Msgpack => {
                let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(bytes));
                let value = seed.deserialize(StructsFromMaps(&mut de)).map_err(decode_error)?;
                if usize::try_from(de.position()).ok() != Some(bytes.len()) {
                    return Err(Error::codec("decode: trailing bytes after the value"));
                }
                Ok(value)
            }
        }
    }
}

fn decode_error(error: impl std::fmt::Display) -> Error {
    Error::codec(format_args!("decode: {error}"))
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
    use crate::message::{CommandMessage, EventMessage, Metadata};
    use crate::testkit::{base_command, base_event};

    #[test]
    fn each_format_round_trips_both_kinds() {
        for fmt in [Format::Json, Format::Msgpack] {
            let event = base_event();
            assert_eq!(
                fmt.decode::<EventMessage>(&fmt.encode(&event).unwrap()).unwrap(),
                event,
                "{fmt:?}"
            );
            let cmd = base_command();
            assert_eq!(
                fmt.decode::<CommandMessage>(&fmt.encode(&cmd).unwrap()).unwrap(),
                cmd,
                "{fmt:?}"
            );
        }
    }

    #[test]
    fn a_value_the_serializer_refuses_is_a_codec_error_not_a_panic() {
        // No message reaches this today, but the kernel never panics on a host's input.
        struct Refuses;
        impl Serialize for Refuses {
            fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("no wire form"))
            }
        }
        for fmt in [Format::Json, Format::Msgpack] {
            assert_eq!(
                fmt.encode_payload(&Refuses),
                Err(Error::codec("no wire form")),
                "{fmt:?}"
            );
        }
    }

    /// The payload rule at every depth and through every serde shape the adapter wraps: a struct reads
    /// from a map only, wherever it sits, and every other shape reads as it did. Each input is one JSON
    /// value, written in both formats.
    #[test]
    fn a_struct_reads_from_a_map_only_at_every_depth() {
        use serde::Deserialize;
        use serde_json::json;
        use std::collections::BTreeMap;

        #[derive(Debug, Deserialize, PartialEq)]
        struct Inner {
            city: String,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Wrapper(i64);
        #[derive(Debug, Deserialize, PartialEq)]
        enum Shape {
            Unit,
            Newtype(Inner),
            Tuple(i64, String),
            Struct { city: String },
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Outer {
            inner: Inner,
            listed: Vec<Inner>,
            keyed: BTreeMap<String, Inner>,
            optional: Option<Inner>,
            shape: Shape,
            wrapped: Wrapper,
            pair: (i64, String),
        }

        fn decode<T: for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug>(
            format: Format,
            value: &serde_json::Value,
        ) -> Result<T, String> {
            let bytes = format.encode_payload(value).map_err(|e| e.to_string())?;
            format
                .decode_payload(&bytes, PhantomData::<T>)
                .map_err(|e| e.to_string())
        }
        let nyc = || Inner { city: "NYC".into() };
        let outer = |overrides: serde_json::Value| {
            let mut value = json!({
                "inner": {"city": "NYC"},
                "listed": [{"city": "NYC"}],
                "keyed": {"home": {"city": "NYC"}},
                "optional": {"city": "NYC"},
                "shape": {"Newtype": {"city": "NYC"}},
                "wrapped": 7,
                "pair": [1, "x"],
                "unknown": [1, 2],
            });
            for (field, item) in overrides.as_object().unwrap() {
                value[field] = item.clone();
            }
            value
        };
        let want = |shape: Shape| Outer {
            inner: nyc(),
            listed: vec![nyc()],
            keyed: BTreeMap::from([("home".to_owned(), nyc())]),
            optional: Some(nyc()),
            shape,
            wrapped: Wrapper(7),
            pair: (1, "x".into()),
        };
        let refused = "as a map keyed by field name";
        let cases: [(&str, serde_json::Value, Result<Outer, &str>); 11] = [
            (
                "every_struct_a_map_reads_and_an_unknown_field_is_ignored",
                outer(json!({})),
                Ok(want(Shape::Newtype(nyc()))),
            ),
            (
                "a_unit_variant_reads",
                outer(json!({"shape": "Unit"})),
                Ok(want(Shape::Unit)),
            ),
            (
                "a_tuple_variant_reads_from_its_array",
                outer(json!({"shape": {"Tuple": [1, "x"]}})),
                Ok(want(Shape::Tuple(1, "x".into()))),
            ),
            (
                "a_struct_variant_reads_from_a_map",
                outer(json!({"shape": {"Struct": {"city": "NYC"}}})),
                Ok(want(Shape::Struct { city: "NYC".into() })),
            ),
            (
                "an_absent_option_reads_as_none",
                outer(json!({"optional": null})),
                Ok(Outer {
                    optional: None,
                    ..want(Shape::Newtype(nyc()))
                }),
            ),
            (
                "a_nested_struct_as_an_array",
                outer(json!({"inner": ["NYC"]})),
                Err(refused),
            ),
            (
                "a_struct_in_a_list_as_an_array",
                outer(json!({"listed": [["NYC"]]})),
                Err(refused),
            ),
            (
                "a_struct_in_a_map_as_an_array",
                outer(json!({"keyed": {"home": ["NYC"]}})),
                Err(refused),
            ),
            (
                "a_struct_in_an_option_as_an_array",
                outer(json!({"optional": ["NYC"]})),
                Err(refused),
            ),
            (
                "a_struct_in_a_newtype_variant_as_an_array",
                outer(json!({"shape": {"Newtype": ["NYC"]}})),
                Err(refused),
            ),
            (
                "a_struct_variant_as_an_array",
                outer(json!({"shape": {"Struct": ["NYC"]}})),
                Err(refused),
            ),
        ];
        for format in [Format::Json, Format::Msgpack] {
            for (name, value, want) in &cases {
                match (decode::<Outer>(format, value), want) {
                    (Ok(got), Ok(want)) => assert_eq!(&got, want, "{format:?} {name}"),
                    (Err(error), Err(needle)) => {
                        assert!(
                            error.starts_with("codec: decode: invalid type") && error.contains(needle),
                            "{format:?} {name}: {error}"
                        );
                    }
                    (got, want) => panic!("{format:?} {name}: got {got:?}, want {want:?}"),
                }
            }
            // The top level, and the shapes that are no struct, which read as themselves.
            let top = decode::<Inner>(format, &json!(["NYC"])).expect_err("top level");
            assert!(top.contains(refused), "{format:?}: {top}");
            assert_eq!(
                decode::<String>(format, &json!("new-name")),
                Ok("new-name".to_owned()),
                "{format:?}: a string"
            );
            assert_eq!(
                decode::<Wrapper>(format, &json!(7)),
                Ok(Wrapper(7)),
                "{format:?}: a newtype"
            );
            assert_eq!(
                decode::<Vec<i64>>(format, &json!([1, 2])),
                Ok(vec![1, 2]),
                "{format:?}: a list"
            );
        }
    }

    #[test]
    fn json_is_human_readable_msgpack_is_compact() {
        let env = base_event();
        let json = Format::Json.encode(&env).unwrap();
        let msgpack = Format::Msgpack.encode(&env).unwrap();
        // JSON is valid UTF-8 text containing the base64 payload; msgpack is smaller and binary.
        assert!(std::str::from_utf8(&json).unwrap().contains("3q2+7w==")); // base64 of deadbeef
        assert!(msgpack.len() < json.len());
    }

    #[test]
    fn map_keys_encode_in_sorted_order() {
        // Reproducible bytes across languages rest on this: whatever order a host filled the maps in,
        // the encoding sorts the keys.
        let mut env = base_event();
        env.metadata = Metadata::new();
        for key in ["trace", "actor"] {
            env.metadata.insert(key.into(), "x".into());
        }
        let json = String::from_utf8(Format::Json.encode(&env).unwrap()).unwrap();
        assert!(json.contains(r#""metadata":{"actor":"x","trace":"x"}"#), "{json}");
    }

    /// The next event version as a writer would ship it: today's fields plus one more. A reader of
    /// either version must read the other's bytes.
    mod next_version {
        use crate::message::{Extensions, Metadata};
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Serialize, Deserialize)]
        pub struct EventMessage {
            pub id: String,
            pub aggregate_id: String,
            pub sequence: Option<u64>,
            pub name: String,
            pub version: u32,
            #[serde(with = "crate::codec::bytes_field")]
            pub data: Vec<u8>,
            pub correlation_id: String,
            pub causation_id: String,
            pub occurred_at: Option<i64>,
            pub recorded_at: i64,
            pub extensions: Extensions,
            pub metadata: Metadata,
            #[serde(default)]
            pub priority: Option<String>,
        }
    }

    fn next_version_of(env: &EventMessage) -> next_version::EventMessage {
        next_version::EventMessage {
            id: env.id.clone(),
            aggregate_id: env.aggregate_id.clone(),
            sequence: env.sequence,
            name: env.name.clone(),
            version: env.version,
            data: env.data.clone(),
            correlation_id: env.correlation_id.clone(),
            causation_id: env.causation_id.clone(),
            occurred_at: env.occurred_at,
            recorded_at: env.recorded_at,
            extensions: env.extensions.clone(),
            metadata: env.metadata.clone(),
            priority: Some("high".into()),
        }
    }

    /// A newer writer adds a field: this reader ignores it, in both formats, so a producer rollout
    /// does not turn every message into poison for the consumers that have not upgraded yet.
    #[test]
    fn a_newer_writers_message_reads_here() {
        let env = base_event();
        let newer = next_version_of(&env);
        let wires = [
            (Format::Json, serde_json::to_vec(&newer).unwrap()),
            (Format::Msgpack, rmp_serde::to_vec_named(&newer).unwrap()),
        ];
        for (format, wire) in wires {
            assert_eq!(format.decode::<EventMessage>(&wire).unwrap(), env, "{format:?}");
        }
    }

    /// The reverse: this writer's bytes read in a newer reader whose added fields default, so a
    /// consumer can upgrade before the producers do.
    #[test]
    fn this_writers_message_reads_in_a_newer_reader() {
        let env = base_event();
        for format in [Format::Json, Format::Msgpack] {
            let wire = format.encode(&env).unwrap();
            let newer: next_version::EventMessage = match format {
                Format::Json => serde_json::from_slice(&wire).unwrap(),
                _ => rmp_serde::from_slice(&wire).unwrap(),
            };
            assert_eq!(newer.priority, None, "{format:?}");
            assert_eq!(
                (newer.id, newer.data, newer.sequence),
                (env.id.clone(), env.data.clone(), env.sequence)
            );
        }
    }

    /// The trailing-bytes rule in both formats: whitespace after a JSON document is not data, any
    /// byte after a MessagePack message is.
    #[test]
    fn a_frame_holds_one_message() {
        let env = base_event();
        let cases: [(&str, Format, &[u8], bool); 4] = [
            ("json_trailing_whitespace_is_not_data", Format::Json, b" \n", true),
            ("json_trailing_document", Format::Json, b"{}", false),
            ("msgpack_trailing_byte", Format::Msgpack, b"\xc0", false),
            ("msgpack_second_message", Format::Msgpack, b"\x80", false),
        ];
        for (name, format, trailing, decodes) in cases {
            let mut wire = format.encode(&env).unwrap();
            wire.extend_from_slice(trailing);
            assert_eq!(format.decode::<EventMessage>(&wire).is_ok(), decodes, "{name}");
        }
    }

    /// Both vocabularies round-trip, so the name a binding selects by and the media type a record
    /// carries cannot drift from the format and kind they resolve to.
    #[test]
    fn every_format_and_kind_round_trips_through_its_media_type() {
        for fmt in [Format::Json, Format::Msgpack] {
            assert_eq!(Format::from_name(fmt.as_str()).unwrap(), fmt);
            for kind in [Kind::Event, Kind::Command] {
                assert_eq!(Format::from_media_type(fmt.media_type(kind)).unwrap(), (fmt, kind));
                // The suffix is the structured-syntax one (RFC 6839), so tooling that never heard of
                // edax still knows how to parse the bytes.
                assert_eq!(
                    fmt.media_type(kind),
                    format!("application/vnd.edax.{}+{}", kind.as_str(), fmt.as_str())
                );
            }
        }
    }

    #[test]
    fn malformed_input_is_rejected() {
        assert!(Format::from_name("protobuf").is_err(), "unknown format name");
        assert!(
            Format::from_media_type("application/json").is_err(),
            "a media type edax never mints"
        );
        assert!(
            Format::from_media_type("application/vnd.edax.event+json; charset=utf-8").is_err(),
            "parameters are not stripped, so the string must match whole"
        );
        assert!(
            Format::Json.decode::<EventMessage>(b"not json").is_err(),
            "json wire bytes are not json"
        );
        assert!(
            Format::Msgpack.decode::<EventMessage>(b"not msgpack").is_err(),
            "msgpack wire bytes are not msgpack"
        );
        assert!(
            Format::Json.decode::<EventMessage>(br#"{"id": "evt-1"}"#).is_err(),
            "json is missing message fields"
        );
        // `data` is base64 in the human-readable format, so an unpaddable string is a decode error.
        let bad_base64 = br#"{"id":"e","aggregate_id":"a","sequence":null,"name":"N","version":1,"data":"!!",
            "correlation_id":"c","causation_id":"c","occurred_at":null,"recorded_at":0,"extensions":{},"metadata":{}}"#;
        assert!(
            Format::Json.decode::<EventMessage>(bad_base64).is_err(),
            "data is not base64"
        );
    }
}

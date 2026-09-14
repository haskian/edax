//! Ready-made event codecs: serialize a domain event to bytes under a fixed name, and back.
//!
//! An [`EventCodec`] is the boundary between your domain event type and the `name`/`version`/`bytes` the
//! store persists. [`SerdeCodec`] covers the common case, one event type serialized whole, so you do not
//! hand-write `marshal` and `unmarshal`; see `docs/sdk/aggregates.md`. A snapshot
//! ([`AggregateCodec`](crate::AggregateCodec)) stays hand-written: only you know which fields are state and
//! which are derived. Every failure here is [`Error::Codec`], which is terminal: the same bytes fail the same
//! way on every attempt, so the caller dead-letters instead of retrying.

use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::Serialize;

use edax_event::Event;

use crate::{Error, EventCodec};

/// A serde wire format a [`SerdeCodec`] can encode to. Zero-sized: it names the format and holds no state.
pub trait Format {
    fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, Error>;
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error>;
}

/// JSON: text, human-readable, cannot write a non-string map key or a NaN.
pub struct Json;

impl Format for Json {
    fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(value).map_err(|e| Error::Codec(e.to_string()))
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
        serde_json::from_slice(bytes).map_err(|e| Error::Codec(e.to_string()))
    }
}

/// MessagePack: binary, smaller, writes serde shapes JSON cannot.
#[cfg(feature = "msgpack")]
pub struct Msgpack;

#[cfg(feature = "msgpack")]
impl Format for Msgpack {
    fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
        rmp_serde::to_vec(value).map_err(|e| Error::Codec(e.to_string()))
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
        rmp_serde::from_slice(bytes).map_err(|e| Error::Codec(e.to_string()))
    }
}

/// Serializes one event type in format `F`. The codec carries the event's schema identity, its `name` and
/// `version`, since serde alone does not name the type. One name per codec, so the whole `T` shares it; for a
/// distinct name per enum variant, hand-write the codec and match on the variant.
pub struct SerdeCodec<T, F> {
    name: String,
    version: i32,
    _marker: PhantomData<fn() -> (T, F)>,
}

impl<T, F> SerdeCodec<T, F> {
    pub fn new(name: impl Into<String>, version: i32) -> Self {
        Self { name: name.into(), version, _marker: PhantomData }
    }
}

impl<T: Serialize + DeserializeOwned, F: Format> EventCodec for SerdeCodec<T, F> {
    type Domain = T;

    fn marshal(&self, event: &T) -> Result<Event, Error> {
        // Serialization is a real error channel, not a formality: a format cannot write every serde
        // shape, and a hand-written `Serialize` may refuse outright.
        Ok(Event { name: self.name.clone(), version: self.version, data: F::encode(event)? })
    }

    fn unmarshal(&self, raw: &Event) -> Result<T, Error> {
        F::decode(&raw.data)
    }
}

/// [`SerdeCodec`] over [`Json`].
pub type JsonCodec<T> = SerdeCodec<T, Json>;
/// [`SerdeCodec`] over [`Msgpack`].
#[cfg(feature = "msgpack")]
pub type MsgpackCodec<T> = SerdeCodec<T, Msgpack>;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Serializer};

    use super::*;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    enum AccountEvent {
        Deposited { amount: i64 },
    }

    /// Postings keyed by `(account, leg)`. JSON has no way to write a non-string map key, so this
    /// event marshals under MessagePack and genuinely fails under `serde_json`.
    #[derive(Serialize, Deserialize, PartialEq, Debug, Default)]
    struct Posted {
        entries: BTreeMap<(String, u8), i64>,
    }

    fn posted() -> Posted {
        Posted { entries: BTreeMap::from([(("cash".to_string(), 0u8), 1_250i64)]) }
    }

    /// An amount that validates on the way out: an unknown currency has no wire representation, so
    /// the hand-written `Serialize` refuses. Fails under every format, unlike [`Posted`].
    #[derive(Deserialize, PartialEq, Debug)]
    struct Withdrawn {
        minor_units: i64,
        currency: String,
    }

    impl Serialize for Withdrawn {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            if self.currency.len() != 3 {
                return Err(serde::ser::Error::custom(format!("unknown currency {:?}", self.currency)));
            }
            serializer.serialize_i64(self.minor_units)
        }
    }

    #[test]
    fn json_roundtrip() {
        let want = AccountEvent::Deposited { amount: 42 };

        let json = JsonCodec::<AccountEvent>::new("AccountEvent", 1);
        let raw = json.marshal(&want).unwrap();
        assert_eq!(raw.name, "AccountEvent");
        assert_eq!(raw.version, 1);
        assert_eq!(raw.data, br#"{"Deposited":{"amount":42}}"#.to_vec());
        assert_eq!(json.unmarshal(&raw).unwrap(), want);
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn msgpack_roundtrip() {
        let want = AccountEvent::Deposited { amount: 42 };

        let mp = MsgpackCodec::<AccountEvent>::new("AccountEvent", 1);
        let raw = mp.marshal(&want).unwrap();
        assert_eq!(raw.name, "AccountEvent");
        assert!(!raw.data.is_empty()); // binary; the real check is the round-trip
        assert_eq!(mp.unmarshal(&raw).unwrap(), want);

        // The payload JSON refuses still round-trips here, so the failure below is the format's
        // limit, not a broken codec.
        let mp = MsgpackCodec::<Posted>::new("Posted", 1);
        let raw = mp.marshal(&posted()).unwrap();
        assert_eq!(mp.unmarshal(&raw).unwrap(), posted());
    }

    #[test]
    fn marshal_reports_a_serializer_failure_instead_of_panicking() {
        struct Case {
            name: &'static str,
            marshal: fn() -> Result<Event, Error>,
            /// The marshaled name on success, or the serializer's own message on failure.
            want: Result<&'static str, &'static str>,
        }
        #[cfg_attr(not(feature = "msgpack"), allow(unused_mut))]
        let mut cases = vec![
            Case {
                name: "json writes a plain event",
                marshal: || JsonCodec::<Posted>::new("Posted", 1).marshal(&Posted::default()),
                want: Ok("Posted"),
            },
            Case {
                name: "json cannot write a non-string map key",
                marshal: || JsonCodec::<Posted>::new("Posted", 1).marshal(&posted()),
                want: Err("codec error: key must be a string"),
            },
            Case {
                name: "json surfaces a refusing Serialize impl",
                marshal: || {
                    let event = Withdrawn { minor_units: 500, currency: "bitcoin".into() };
                    JsonCodec::<Withdrawn>::new("Withdrawn", 1).marshal(&event)
                },
                want: Err(r#"codec error: unknown currency "bitcoin""#),
            },
        ];
        #[cfg(feature = "msgpack")]
        cases.extend([
            Case {
                name: "msgpack writes what json cannot",
                marshal: || MsgpackCodec::<Posted>::new("Posted", 1).marshal(&posted()),
                want: Ok("Posted"),
            },
            Case {
                name: "msgpack surfaces a refusing Serialize impl",
                marshal: || {
                    let event = Withdrawn { minor_units: 500, currency: "bitcoin".into() };
                    MsgpackCodec::<Withdrawn>::new("Withdrawn", 1).marshal(&event)
                },
                want: Err(r#"codec error: unknown currency "bitcoin""#),
            },
        ]);
        for c in cases {
            let got = (c.marshal)();
            match c.want {
                Ok(name) => {
                    let raw = got.unwrap_or_else(|e| panic!("{}: want Ok, got {e:?}", c.name));
                    assert_eq!(raw.name, name, "{}", c.name);
                    assert_eq!(raw.version, 1, "{}", c.name);
                    assert!(!raw.data.is_empty(), "{}", c.name);
                }
                Err(message) => {
                    assert!(matches!(&got, Err(Error::Codec(_))), "{}: got {:?}", c.name, got.as_ref().err());
                    assert_eq!(got.err().map(|e| e.to_string()).as_deref(), Some(message), "{}", c.name);
                }
            }
        }
    }
}

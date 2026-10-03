//! Ready-made event codecs: serialize a domain event to bytes under its name, and back.
//!
//! An [`EventCodec`] is the boundary between your domain event type and the `name`/`version`/`bytes` the
//! store persists. [`RegistryCodec`] is the common case and Python's `json_codec` twin: one event name per
//! variant of your event enum, the variant's own value as the payload, so a Python service reading the
//! stream sees the same names and bytes it writes itself. [`SerdeCodec`] covers one event type under one
//! name. A snapshot ([`AggregateCodec`](crate::AggregateCodec)) stays hand-written: only you know which
//! fields are state and which are derived.
//!
//! Every unmarshal asks the kernel's [`upcast_plan`](edax_event::upcast_plan), so a payload from a newer
//! writer than this codec is refused, never decoded silently under the older schema; an older payload is
//! raised through the name's [`Upcaster`], or read as it is when the name has none. Every failure here is a
//! `codec` error, which is terminal: the same bytes fail the same way on every attempt, so the caller
//! dead-letters instead of retrying.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::marker::PhantomData;

use serde::de::{self, DeserializeOwned, DeserializeSeed, IntoDeserializer, Visitor};
use serde::ser::{self, Impossible, SerializeMap, Serializer};
use serde::Serialize;

use edax_event::Event;

use crate::{Error, ErrorCode, EventCodec};

/// A serde wire format a codec can encode to. Zero-sized: it names the format and holds no state.
pub trait PayloadFormat {
    fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, Error>;
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error>;
    /// Decode through a seed: what a [`RegistryCodec`] needs to read one variant's payload.
    fn decode_seed<'de, S: DeserializeSeed<'de>>(bytes: &'de [u8], seed: S) -> Result<S::Value, Error>;
}

/// JSON: text, human-readable, cannot write a non-string map key or a NaN.
pub struct Json;

impl PayloadFormat for Json {
    fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(value).map_err(Error::codec)
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
        serde_json::from_slice(bytes).map_err(Error::codec)
    }

    fn decode_seed<'de, S: DeserializeSeed<'de>>(bytes: &'de [u8], seed: S) -> Result<S::Value, Error> {
        let mut de = serde_json::Deserializer::from_slice(bytes);
        let value = seed.deserialize(&mut de).map_err(Error::codec)?;
        de.end().map_err(Error::codec)?;
        Ok(value)
    }
}

/// MessagePack: binary, smaller, writes serde shapes JSON cannot. A struct is written as a map keyed by
/// field name, as Python's msgspec writes it, so the two SDKs write the same bytes for the same event and
/// a reader with an added field still reads an older payload.
#[cfg(feature = "msgpack")]
pub struct Msgpack;

#[cfg(feature = "msgpack")]
impl PayloadFormat for Msgpack {
    fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, Error> {
        rmp_serde::to_vec_named(value).map_err(Error::codec)
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
        Self::decode_seed(bytes, PhantomData::<T>)
    }

    fn decode_seed<'de, S: DeserializeSeed<'de>>(bytes: &'de [u8], seed: S) -> Result<S::Value, Error> {
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(bytes));
        let value = seed.deserialize(&mut de).map_err(Error::codec)?;
        // The JSON reader refuses trailing data, so this one does too: one payload is one value.
        if usize::try_from(de.position()).ok() != Some(bytes.len()) {
            return Err(Error::codec("trailing bytes after the payload"));
        }
        Ok(value)
    }
}

/// One single-step transform: upgrade a payload at version `v` to `v + 1`.
pub type UpcastStep = Box<dyn Fn(Vec<u8>) -> Result<Vec<u8>, Error> + Send + Sync>;

/// The `version -> version + 1` transforms of one event name. The transforms are domain code; which of
/// them run, and in what order, is the kernel's plan, so every SDK raises a stored payload the same way.
#[derive(Default)]
pub struct Upcaster {
    steps: BTreeMap<i32, UpcastStep>,
}

impl Upcaster {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register the transform that upgrades `version` to `version + 1`. A second transform for one
    /// version is a terminal [`ErrorCode::AlreadyRegistered`]: keeping either one silently would run a
    /// migration nobody chose.
    pub fn step(
        mut self,
        version: i32,
        transform: impl Fn(Vec<u8>) -> Result<Vec<u8>, Error> + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        if self.steps.contains_key(&version) {
            return Err(Error::new(ErrorCode::AlreadyRegistered, format_args!("upcast step {version} is registered twice")));
        }
        self.steps.insert(version, Box::new(transform));
        Ok(self)
    }

    /// Raise `data` from version `from` to `to`, in the kernel's planned order. A missing step, or
    /// `from > to`, is a `codec` error.
    pub fn to(&self, data: Vec<u8>, from: i32, to: i32) -> Result<Vec<u8>, Error> {
        raise(Some(self), data, from, to)
    }
}

/// Raise `data` from `from` to `to` through `upcaster`'s steps, or as the identity when the name has no
/// upcaster. Always asks the kernel, so a payload from a newer writer is refused either way.
fn raise(upcaster: Option<&Upcaster>, mut data: Vec<u8>, from: i32, to: i32) -> Result<Vec<u8>, Error> {
    let available: Option<Vec<i32>> = upcaster.map(|u| u.steps.keys().copied().collect());
    for version in edax_event::upcast_plan(from, to, available.as_deref())? {
        let step = upcaster
            .and_then(|u| u.steps.get(&version))
            .ok_or_else(|| Error::codec(format_args!("the plan names step {version}, which has no transform")))?;
        data = step(data)?;
    }
    Ok(data)
}

/// Serializes one event type in format `F` under one name. The codec carries the event's schema identity,
/// its `name` and `version`, since serde alone does not name the type. For several event types use
/// [`RegistryCodec`], which names each variant of an enum the way Python's registry names each type.
pub struct SerdeCodec<T, F> {
    name: String,
    version: i32,
    upcaster: Option<Upcaster>,
    _marker: PhantomData<fn() -> (T, F)>,
}

impl<T, F> SerdeCodec<T, F> {
    pub fn new(name: impl Into<String>, version: i32) -> Self {
        Self { name: name.into(), version, upcaster: None, _marker: PhantomData }
    }

    /// Raise payloads stored at an older version through `upcaster` on unmarshal.
    pub fn with_upcaster(mut self, upcaster: Upcaster) -> Self {
        self.upcaster = Some(upcaster);
        self
    }
}

impl<T: Serialize + DeserializeOwned, F: PayloadFormat> EventCodec for SerdeCodec<T, F> {
    type Domain = T;

    fn marshal(&self, event: &T) -> Result<Event, Error> {
        // Serialization is a real error channel, not a formality: a format cannot write every serde
        // shape, and a hand-written `Serialize` may refuse outright.
        Ok(Event { name: self.name.clone(), version: self.version, data: F::encode(event)? })
    }

    fn unmarshal(&self, raw: &Event) -> Result<T, Error> {
        if raw.name != self.name {
            return Err(Error::codec(format_args!("event {:?} is not {:?}, the one name this codec reads", raw.name, self.name)));
        }
        let data = raise(self.upcaster.as_ref(), raw.data.clone(), raw.version, self.version)?;
        F::decode(&data)
    }
}

/// [`SerdeCodec`] over [`Json`].
pub type JsonCodec<T> = SerdeCodec<T, Json>;
/// [`SerdeCodec`] over [`Msgpack`].
#[cfg(feature = "msgpack")]
pub type MsgpackCodec<T> = SerdeCodec<T, Msgpack>;

/// Maps the variants of an event enum to event names and serializes each variant's own value whole: the
/// Rust twin of Python's `json_codec({"Deposited": Deposited, ...})`. `T` is an externally tagged enum (serde's
/// default) whose variants are newtype variants, `Deposited(Deposited)`, or unit variants, `Frozen`: the
/// variant name is the event name, and the wrapped value is the payload (a unit variant writes an empty
/// map, as an empty Python struct does). Struct and tuple variants are refused at marshal, because their
/// payload would have no type a second SDK could name.
///
/// One version for the whole registry, as in Python: bump it when any payload changes, and give the names
/// whose shape changed an [`Upcaster`]. An unknown name on unmarshal is a `codec` error.
pub struct RegistryCodec<T, F> {
    version: i32,
    names: &'static [&'static str],
    upcasters: HashMap<String, Upcaster>,
    _marker: PhantomData<fn() -> (T, F)>,
}

impl<T: DeserializeOwned, F> RegistryCodec<T, F> {
    /// A registry writing `version`. A `T` that is not an externally tagged enum is a `codec` error.
    pub fn new(version: i32) -> Result<Self, Error> {
        Ok(Self { version, names: variant_names::<T>()?, upcasters: HashMap::new(), _marker: PhantomData })
    }

    /// Raise older payloads of `name` through `upcaster`. A name that is not a variant of `T` is a
    /// `codec` error, and a second upcaster for one name is [`ErrorCode::AlreadyRegistered`]: either
    /// would leave a stale payload decoding as it is.
    pub fn upcaster(mut self, name: &str, upcaster: Upcaster) -> Result<Self, Error> {
        if !self.names.contains(&name) {
            return Err(Error::codec(format_args!("an upcaster is registered for {name:?}, which is no event name of this registry")));
        }
        if self.upcasters.insert(name.to_owned(), upcaster).is_some() {
            return Err(Error::new(ErrorCode::AlreadyRegistered, format_args!("{name:?} has two upcasters")));
        }
        Ok(self)
    }

    /// The event names this registry reads and writes: the variants of `T`.
    pub fn names(&self) -> &'static [&'static str] {
        self.names
    }
}

impl<T: Serialize + DeserializeOwned, F: PayloadFormat> EventCodec for RegistryCodec<T, F> {
    type Domain = T;

    fn marshal(&self, event: &T) -> Result<Event, Error> {
        let (name, data) = event.serialize(Split::<F>(PhantomData)).map_err(|e| e.0)?;
        Ok(Event { name: name.to_owned(), version: self.version, data })
    }

    fn unmarshal(&self, raw: &Event) -> Result<T, Error> {
        if !self.names.contains(&raw.name.as_str()) {
            return Err(Error::codec(format_args!("no event type registered for {:?}", raw.name)));
        }
        let data = raise(self.upcasters.get(&raw.name), raw.data.clone(), raw.version, self.version)?;
        T::deserialize(Named::<F> { name: &raw.name, data: &data, _format: PhantomData }).map_err(|e| e.0)
    }
}

/// [`RegistryCodec`] over [`Json`].
pub type JsonRegistryCodec<T> = RegistryCodec<T, Json>;
/// [`RegistryCodec`] over [`Msgpack`].
#[cfg(feature = "msgpack")]
pub type MsgpackRegistryCodec<T> = RegistryCodec<T, Msgpack>;

/// The error type serde's traits need around the SDK's own: the orphan rule forbids implementing
/// serde's error traits on [`Error`] here.
#[derive(Debug)]
struct Refusal(Error);

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Refusal {}

impl ser::Error for Refusal {
    fn custom<M: fmt::Display>(msg: M) -> Self {
        Refusal(Error::codec(msg))
    }
}

impl de::Error for Refusal {
    fn custom<M: fmt::Display>(msg: M) -> Self {
        Refusal(Error::codec(msg))
    }
}

const NEEDS_VARIANTS: &str = "a registry codec writes an enum of newtype variants (`Deposited(Deposited)`) or unit variants";

/// The empty map a unit variant writes: the payload an empty Python struct has.
struct EmptyMap;

impl Serialize for EmptyMap {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_map(Some(0))?.end()
    }
}

/// A serializer that takes one enum value apart into its variant name and its payload in `F`.
struct Split<F>(PhantomData<F>);

macro_rules! refuse {
    ($($method:ident($($arg:ty),*);)*) => {
        $(fn $method(self, $(_: $arg),*) -> Result<Self::Ok, Self::Error> {
            Err(Refusal(Error::codec(NEEDS_VARIANTS)))
        })*
    };
}

impl<F: PayloadFormat> Serializer for Split<F> {
    type Ok = (&'static str, Vec<u8>);
    type Error = Refusal;
    type SerializeSeq = Impossible<Self::Ok, Refusal>;
    type SerializeTuple = Impossible<Self::Ok, Refusal>;
    type SerializeTupleStruct = Impossible<Self::Ok, Refusal>;
    type SerializeTupleVariant = Impossible<Self::Ok, Refusal>;
    type SerializeMap = Impossible<Self::Ok, Refusal>;
    type SerializeStruct = Impossible<Self::Ok, Refusal>;
    type SerializeStructVariant = Impossible<Self::Ok, Refusal>;

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Refusal> {
        Ok((variant, F::encode(value).map_err(Refusal)?))
    }

    fn serialize_unit_variant(self, _name: &'static str, _index: u32, variant: &'static str) -> Result<Self::Ok, Refusal> {
        Ok((variant, F::encode(&EmptyMap).map_err(Refusal)?))
    }

    refuse! {
        serialize_bool(bool); serialize_i8(i8); serialize_i16(i16); serialize_i32(i32); serialize_i64(i64);
        serialize_u8(u8); serialize_u16(u16); serialize_u32(u32); serialize_u64(u64); serialize_f32(f32);
        serialize_f64(f64); serialize_char(char); serialize_str(&str); serialize_bytes(&[u8]); serialize_none();
        serialize_unit(); serialize_unit_struct(&'static str);
    }

    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<Self::Ok, Refusal> {
        Err(Refusal(Error::codec(NEEDS_VARIANTS)))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, _: &T) -> Result<Self::Ok, Refusal> {
        Err(Refusal(Error::codec(NEEDS_VARIANTS)))
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Refusal> {
        Err(Refusal(Error::codec(NEEDS_VARIANTS)))
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Refusal> {
        Err(Refusal(Error::codec(NEEDS_VARIANTS)))
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeTupleStruct, Refusal> {
        Err(Refusal(Error::codec(NEEDS_VARIANTS)))
    }
    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeTupleVariant, Refusal> {
        Err(Refusal(Error::codec(NEEDS_VARIANTS)))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Refusal> {
        Err(Refusal(Error::codec(NEEDS_VARIANTS)))
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Refusal> {
        Err(Refusal(Error::codec(NEEDS_VARIANTS)))
    }
    fn serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeStructVariant, Refusal> {
        Err(Refusal(Error::codec(NEEDS_VARIANTS)))
    }
}

/// A deserializer that hands an enum's `Deserialize` the variant `name` with `data`, in `F`, as its
/// content: the inverse of [`Split`].
struct Named<'a, F> {
    name: &'a str,
    data: &'a [u8],
    _format: PhantomData<F>,
}

impl<'de, F: PayloadFormat> de::Deserializer<'de> for Named<'de, F> {
    type Error = Refusal;

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Refusal> {
        Err(Refusal(Error::codec(NEEDS_VARIANTS)))
    }

    fn deserialize_enum<V: Visitor<'de>>(self, _: &'static str, _: &'static [&'static str], visitor: V) -> Result<V::Value, Refusal> {
        visitor.visit_enum(self)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf option unit
        unit_struct newtype_struct seq tuple tuple_struct map struct identifier ignored_any
    }
}

impl<'de, F: PayloadFormat> de::EnumAccess<'de> for Named<'de, F> {
    type Error = Refusal;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self), Refusal> {
        let variant = seed.deserialize(IntoDeserializer::<Refusal>::into_deserializer(self.name))?;
        Ok((variant, self))
    }
}

impl<'de, F: PayloadFormat> de::VariantAccess<'de> for Named<'de, F> {
    type Error = Refusal;

    fn unit_variant(self) -> Result<(), Refusal> {
        F::decode::<de::IgnoredAny>(self.data).map(|_| ()).map_err(Refusal)
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, Refusal> {
        F::decode_seed(self.data, seed).map_err(Refusal)
    }

    fn tuple_variant<V: Visitor<'de>>(self, _: usize, _: V) -> Result<V::Value, Refusal> {
        Err(Refusal(Error::codec(NEEDS_VARIANTS)))
    }

    fn struct_variant<V: Visitor<'de>>(self, _: &'static [&'static str], _: V) -> Result<V::Value, Refusal> {
        Err(Refusal(Error::codec(NEEDS_VARIANTS)))
    }
}

/// The variant names of `T`, read off its `Deserialize` by a deserializer that records the list the
/// derive hands `deserialize_enum` and then stops.
fn variant_names<T: DeserializeOwned>() -> Result<&'static [&'static str], Error> {
    struct Probe<'a>(&'a Cell<Option<&'static [&'static str]>>);

    impl<'de> de::Deserializer<'de> for Probe<'_> {
        type Error = Refusal;

        fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Refusal> {
            Err(Refusal(Error::codec(NEEDS_VARIANTS)))
        }

        fn deserialize_enum<V: Visitor<'de>>(self, _: &'static str, variants: &'static [&'static str], _: V) -> Result<V::Value, Refusal> {
            self.0.set(Some(variants));
            Err(Refusal(Error::codec("probe")))
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf option unit
            unit_struct newtype_struct seq tuple tuple_struct map struct identifier ignored_any
        }
    }

    let found = Cell::new(None);
    let _ = T::deserialize(Probe(&found));
    found.get().ok_or_else(|| Error::codec(NEEDS_VARIANTS))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::Deserialize;

    use super::*;

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    struct Deposited {
        account_id: String,
        amount_cents: i64,
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    struct Withdrawn {
        account_id: String,
        amount_cents: i64,
        memo: Option<String>,
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    enum AccountEvent {
        Deposited(Deposited),
        Withdrawn(Withdrawn),
        Frozen,
    }

    fn deposited() -> AccountEvent {
        AccountEvent::Deposited(Deposited { account_id: "acct-1".into(), amount_cents: 4200 })
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
    struct Refused {
        minor_units: i64,
        currency: String,
    }

    impl Serialize for Refused {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            if self.currency.len() != 3 {
                return Err(serde::ser::Error::custom(format!("unknown currency {:?}", self.currency)));
            }
            serializer.serialize_i64(self.minor_units)
        }
    }

    #[test]
    fn a_registry_writes_one_name_per_variant_with_the_variant_as_the_payload() {
        let codec = JsonRegistryCodec::<AccountEvent>::new(1).unwrap();
        assert_eq!(codec.names(), ["Deposited", "Withdrawn", "Frozen"]);
        for (event, name, data) in [
            (deposited(), "Deposited", r#"{"account_id":"acct-1","amount_cents":4200}"#),
            (
                AccountEvent::Withdrawn(Withdrawn { account_id: "acct-1".into(), amount_cents: 5, memo: None }),
                "Withdrawn",
                r#"{"account_id":"acct-1","amount_cents":5,"memo":null}"#,
            ),
            (AccountEvent::Frozen, "Frozen", "{}"),
        ] {
            let raw = codec.marshal(&event).unwrap();
            assert_eq!((raw.name.as_str(), raw.version, raw.data.as_slice()), (name, 1, data.as_bytes()), "{name}");
            assert_eq!(codec.unmarshal(&raw).unwrap(), event, "{name} round trip");
        }
    }

    #[test]
    fn unmarshal_branches() {
        let upcaster = Upcaster::new()
            .step(1, |data| {
                // v1 had `amount` in whole units; v2 is cents.
                let mut v: serde_json::Value = serde_json::from_slice(&data).map_err(Error::codec)?;
                let amount = v["amount"].as_i64().unwrap_or(0);
                v["amount_cents"] = (amount * 100).into();
                serde_json::to_vec(&v).map_err(Error::codec)
            })
            .unwrap();
        let codec = JsonRegistryCodec::<AccountEvent>::new(2).unwrap().upcaster("Deposited", upcaster).unwrap();
        let event = |name: &str, version: i32, data: &str| Event { name: name.into(), version, data: data.as_bytes().to_vec() };
        let cases: [(&str, Event, Result<AccountEvent, &str>); 9] = [
            ("current payload", event("Deposited", 2, r#"{"account_id":"acct-1","amount_cents":4200}"#), Ok(deposited())),
            ("older payload raised by its upcaster", event("Deposited", 1, r#"{"account_id":"acct-1","amount":42}"#), Ok(deposited())),
            (
                "older payload of a name with no upcaster reads as it is",
                event("Withdrawn", 1, r#"{"account_id":"acct-1","amount_cents":5,"memo":"atm"}"#),
                Ok(AccountEvent::Withdrawn(Withdrawn { account_id: "acct-1".into(), amount_cents: 5, memo: Some("atm".into()) })),
            ),
            (
                "a newer writer's payload is refused, with an upcaster",
                event("Deposited", 3, r#"{"account_id":"acct-1","amount_cents":4200}"#),
                Err("codec: cannot upcast downward: from 3 > to 2 (the payload comes from a newer writer)"),
            ),
            (
                "a newer writer's payload is refused, without one",
                event("Withdrawn", 3, r#"{"account_id":"acct-1","amount_cents":5,"memo":null,"fee":1}"#),
                Err("codec: cannot upcast downward: from 3 > to 2 (the payload comes from a newer writer)"),
            ),
            (
                "a gap in the upcaster is refused",
                event("Deposited", 0, r#"{"account_id":"acct-1","amount":42}"#),
                Err("codec: missing upcast step 0 (no transform for v0 -> v1)"),
            ),
            ("an unknown name is refused", event("Closed", 2, "{}"), Err(r#"codec: no event type registered for "Closed""#)),
            ("a unit variant reads from its empty map", event("Frozen", 2, "{}"), Ok(AccountEvent::Frozen)),
            (
                "a payload that is not the variant's shape is refused",
                event("Deposited", 2, r#"{"account_id":7}"#),
                Err("codec: invalid type: integer `7`, expected a string at line 1 column 15"),
            ),
        ];
        for (name, raw, want) in cases {
            let got = codec.unmarshal(&raw).map_err(|e| e.to_string());
            assert_eq!(got, want.map_err(str::to_owned), "{name}");
        }
    }

    #[test]
    fn construction_refusals() {
        #[derive(Serialize, Deserialize)]
        struct NotAnEnum {
            n: i64,
        }
        assert_eq!(
            JsonRegistryCodec::<NotAnEnum>::new(1).err().map(|e| e.to_string()),
            Some(format!("codec: {NEEDS_VARIANTS}"))
        );
        let dangling = JsonRegistryCodec::<AccountEvent>::new(1).unwrap().upcaster("Closed", Upcaster::new());
        assert_eq!(
            dangling.err().map(|e| e.to_string()).as_deref(),
            Some(r#"codec: an upcaster is registered for "Closed", which is no event name of this registry"#)
        );
        let twice = JsonRegistryCodec::<AccountEvent>::new(1)
            .unwrap()
            .upcaster("Deposited", Upcaster::new())
            .unwrap()
            .upcaster("Deposited", Upcaster::new());
        assert_eq!(twice.err().map(|e| e.code), Some(ErrorCode::AlreadyRegistered));
        let step_twice = Upcaster::new().step(1, Ok).unwrap().step(1, Ok);
        assert_eq!(step_twice.err().map(|e| e.to_string()).as_deref(), Some("already_registered: upcast step 1 is registered twice"));
    }

    #[test]
    fn a_struct_or_tuple_variant_is_refused_at_marshal() {
        #[derive(Serialize, Deserialize, Debug)]
        enum Shapes {
            Point { x: i64 },
            Pair(i64, i64),
        }
        let codec = JsonRegistryCodec::<Shapes>::new(1).unwrap();
        for shape in [Shapes::Point { x: 1 }, Shapes::Pair(1, 2)] {
            assert_eq!(codec.marshal(&shape).err().map(|e| e.to_string()), Some(format!("codec: {NEEDS_VARIANTS}")));
        }
        for (name, data) in [("Point", r#"{"x":1}"#), ("Pair", "[1,2]")] {
            let raw = Event { name: name.into(), version: 1, data: data.as_bytes().to_vec() };
            assert_eq!(codec.unmarshal(&raw).err().map(|e| e.to_string()), Some(format!("codec: {NEEDS_VARIANTS}")), "{name}");
        }
    }

    /// Every serde shape the registry cannot name is refused on both sides, with the same terminal
    /// `codec` error: the serializer that splits a value into name and payload, and the deserializer
    /// that hands a variant its payload back.
    #[test]
    fn every_shape_that_is_not_a_variant_is_refused() {
        #[derive(Serialize)]
        struct Unit;
        #[derive(Serialize)]
        struct Wrapper(i64);
        #[derive(Serialize)]
        struct Pair(i64, i64);
        #[derive(Serialize)]
        struct Fields {
            n: i64,
        }
        /// Raw bytes: a shape serde has, which no derive produces for a plain field.
        struct Bytes;
        impl Serialize for Bytes {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_bytes(b"x")
            }
        }
        /// A `Serialize` that refuses before choosing a shape, through the serializer's own error.
        struct Refuses;
        impl Serialize for Refuses {
            fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("no wire form"))
            }
        }
        fn split<T: Serialize>(value: T) -> String {
            match value.serialize(Split::<Json>(PhantomData)) {
                Ok((name, _)) => format!("split as {name}"),
                Err(refusal) => refusal.to_string(),
            }
        }
        let refused = format!("codec: {NEEDS_VARIANTS}");
        let shapes: [(&str, String); 22] = [
            ("bool", split(true)),
            ("i8", split(1i8)),
            ("i16", split(1i16)),
            ("i32", split(1i32)),
            ("i64", split(1i64)),
            ("u8", split(1u8)),
            ("u16", split(1u16)),
            ("u32", split(1u32)),
            ("u64", split(1u64)),
            ("f32", split(1.5f32)),
            ("f64", split(1.5f64)),
            ("char", split('x')),
            ("str", split("x")),
            ("bytes", split(Bytes)),
            ("none", split(Option::<i64>::None)),
            ("some", split(Some(1i64))),
            ("unit", split(())),
            ("unit struct", split(Unit)),
            ("newtype struct", split(Wrapper(1))),
            ("seq", split(vec![1i64])),
            ("tuple", split((1i64, 2i64))),
            ("tuple struct", split(Pair(1, 2))),
        ];
        for (shape, got) in shapes {
            assert_eq!(got, refused, "{shape}");
        }
        assert_eq!(split(BTreeMap::from([("k", 1i64)])), refused, "map");
        assert_eq!(split(Fields { n: 1 }), refused, "struct");
        assert_eq!(split(Refuses), "codec: no wire form", "a Serialize that refuses keeps its own message");

        // The other direction: a variant's payload read as anything but an enum is refused, and a
        // variant name the type does not know is the deserializer's own codec error.
        let named = || Named::<Json> { name: "Deposited", data: b"{}", _format: PhantomData };
        assert_eq!(<i64 as serde::Deserialize>::deserialize(named()).map_err(|r| r.to_string()), Err(refused));
        let unknown = Named::<Json> { name: "Closed", data: b"{}", _format: PhantomData };
        let got = AccountEvent::deserialize(unknown).map_err(|r| r.to_string());
        assert!(got.is_err_and(|e| e.starts_with("codec: unknown variant `Closed`")), "unknown variant");
    }

    #[test]
    fn serde_codec_unmarshal_branches() {
        let codec = JsonCodec::<Deposited>::new("Deposited", 2);
        let event = |name: &str, version: i32| Event { name: name.into(), version, data: br#"{"account_id":"acct-1","amount_cents":4200}"#.to_vec() };
        let want = Deposited { account_id: "acct-1".into(), amount_cents: 4200 };
        assert_eq!(codec.unmarshal(&event("Deposited", 2)).unwrap(), want, "current");
        assert_eq!(codec.unmarshal(&event("Deposited", 1)).unwrap(), want, "older, no upcaster: identity");
        assert_eq!(
            codec.unmarshal(&event("Deposited", 3)).err().map(|e| e.to_string()).as_deref(),
            Some("codec: cannot upcast downward: from 3 > to 2 (the payload comes from a newer writer)"),
            "newer writer"
        );
        assert_eq!(
            codec.unmarshal(&event("Withdrawn", 2)).err().map(|e| e.to_string()).as_deref(),
            Some(r#"codec: event "Withdrawn" is not "Deposited", the one name this codec reads"#),
            "another name"
        );
        let raised = JsonCodec::<Deposited>::new("Deposited", 2)
            .with_upcaster(Upcaster::new().step(1, |_| Ok(br#"{"account_id":"acct-9","amount_cents":1}"#.to_vec())).unwrap());
        assert_eq!(raised.unmarshal(&event("Deposited", 1)).unwrap().account_id, "acct-9", "an upcaster raises v1");
        assert_eq!(
            Upcaster::new().to(b"x".to_vec(), 1, 2).err().map(|e| e.to_string()).as_deref(),
            Some("codec: missing upcast step 1 (no transform for v1 -> v2)")
        );
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn msgpack_writes_structs_as_maps_and_round_trips() {
        let codec = MsgpackRegistryCodec::<AccountEvent>::new(1).unwrap();
        let raw = codec.marshal(&deposited()).unwrap();
        assert_eq!(raw.data.first(), Some(&0x82), "a two-field map, as msgspec writes it");
        assert_eq!(codec.unmarshal(&raw).unwrap(), deposited());
        let frozen = codec.marshal(&AccountEvent::Frozen).unwrap();
        assert_eq!(frozen.data, [0x80], "a unit variant is an empty map");
        let mut trailing = raw.clone();
        trailing.data.push(0xc0);
        assert_eq!(codec.unmarshal(&trailing).err().map(|e| e.to_string()).as_deref(), Some("codec: trailing bytes after the payload"));

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
                want: Err("codec: key must be a string"),
            },
            Case {
                name: "json surfaces a refusing Serialize impl",
                marshal: || {
                    let event = Refused { minor_units: 500, currency: "bitcoin".into() };
                    JsonCodec::<Refused>::new("Refused", 1).marshal(&event)
                },
                want: Err(r#"codec: unknown currency "bitcoin""#),
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
                    let event = Refused { minor_units: 500, currency: "bitcoin".into() };
                    MsgpackCodec::<Refused>::new("Refused", 1).marshal(&event)
                },
                want: Err(r#"codec: unknown currency "bitcoin""#),
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
                    assert!(matches!(&got, Err(Error { code: ErrorCode::Codec, .. })), "{}: got {:?}", c.name, got.as_ref().err());
                    assert_eq!(got.err().map(|e| e.to_string()).as_deref(), Some(message), "{}", c.name);
                }
            }
        }
    }
}

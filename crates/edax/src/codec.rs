//! Ready-made event codecs: serialize a domain event to bytes under its name, and back.
//!
//! An [`EventCodec`] is the boundary between your domain event type and the `name`/`version`/`bytes` the
//! store persists. [`RegistryCodec`] is the common case and Python's `json_codec` twin: one event name per
//! variant of your event enum, the variant's own value as the payload, so a Python service reading the
//! stream sees the same names and bytes it writes itself. [`SerdeCodec`] covers one event type under one
//! name. A snapshot ([`AggregateCodec`](crate::AggregateCodec)) stays hand-written: only you know which
//! fields are state and which are derived.
//!
//! Each event name has its own schema version, and its [`Upcaster`] is that version's only source: the
//! kernel's [`upcast_target`](edax_event::upcast_target) makes it one past the highest step, or 1 for a
//! name with no upcaster. So raising one name's version never moves another's, and a chain with a hole
//! fails when the codec is built. Every unmarshal asks the kernel's [`upcast_plan`](edax_event::upcast_plan),
//! so a payload from a newer writer than this codec is refused, never decoded silently under the older
//! schema; an older payload is raised through the name's [`Upcaster`], or read as it is when the name has
//! none. Every failure here is a
//! `codec` error, which is terminal: the same bytes fail the same way on every attempt, so the caller
//! dead-letters instead of retrying.

use std::borrow::Cow;
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
/// The name's schema version is one past its highest step: to bump a name whose shape did not change,
/// register an identity step.
#[derive(Default)]
pub struct Upcaster {
    steps: BTreeMap<u32, UpcastStep>,
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
        version: u32,
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
    pub fn to(&self, data: Vec<u8>, from: u32, to: u32) -> Result<Vec<u8>, Error> {
        raise(Some(self), &data, from, to).map(Cow::into_owned)
    }
}

/// The schema version a name with `upcaster` writes, checked by the kernel: one past the highest step, or
/// the first version with no upcaster. A hole in the steps is a `codec` error.
fn target(upcaster: Option<&Upcaster>) -> Result<u32, Error> {
    let available: Option<Vec<u32>> = upcaster.map(|u| u.steps.keys().copied().collect());
    edax_event::upcast_target(available.as_deref())
}

/// Raise `data` from `from` to `to` through `upcaster`'s steps, or as the identity when the name has no
/// upcaster. Always asks the kernel, so a payload from a newer writer is refused either way. Borrowed
/// until a step runs, so the common empty plan copies nothing.
fn raise<'a>(upcaster: Option<&Upcaster>, data: &'a [u8], from: u32, to: u32) -> Result<Cow<'a, [u8]>, Error> {
    let available: Option<Vec<u32>> = upcaster.map(|u| u.steps.keys().copied().collect());
    let mut data = Cow::Borrowed(data);
    for version in edax_event::upcast_plan(from, to, available.as_deref())? {
        let step = upcaster
            .and_then(|u| u.steps.get(&version))
            .ok_or_else(|| Error::codec(format_args!("the plan names step {version}, which has no transform")))?;
        data = Cow::Owned(step(data.into_owned())?);
    }
    Ok(data)
}

/// Serializes one event type in format `F` under one name. The codec carries the event's schema identity,
/// its `name` and `version`, since serde alone does not name the type. The version is 1, or the one its
/// [`Upcaster`] reaches. For several event types use [`RegistryCodec`], which names each variant of an
/// enum the way Python's registry names each type.
pub struct SerdeCodec<T, F> {
    name: String,
    version: u32,
    upcaster: Option<Upcaster>,
    _marker: PhantomData<fn() -> (T, F)>,
}

impl<T, F> SerdeCodec<T, F> {
    /// A codec writing `name` at the first schema version.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), version: edax_event::FIRST_VERSION, upcaster: None, _marker: PhantomData }
    }

    /// Write one past `upcaster`'s highest step, and raise payloads stored at an older version through it
    /// on unmarshal. A hole in its steps is a `codec` error here, not on the first old payload.
    pub fn with_upcaster(mut self, upcaster: Upcaster) -> Result<Self, Error> {
        self.version = target(Some(&upcaster))?;
        self.upcaster = Some(upcaster);
        Ok(self)
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
        let data = raise(self.upcaster.as_ref(), &raw.data, raw.version, self.version)?;
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
/// Each name has its own schema version, as in Python: 1, or one past the highest step of the name's
/// [`Upcaster`]. When one payload changes, give that name an upcaster (or one more step); every other name
/// keeps its version. An unknown name on unmarshal is a `codec` error.
pub struct RegistryCodec<T, F> {
    names: &'static [&'static str],
    /// The names with an upcaster, each with the version it reaches. Every other name writes the first.
    upcasters: HashMap<&'static str, (u32, Upcaster)>,
    _marker: PhantomData<fn() -> (T, F)>,
}

impl<T: DeserializeOwned, F> RegistryCodec<T, F> {
    /// A registry writing every name at the first schema version. A `T` that is not an externally tagged
    /// enum is a `codec` error.
    pub fn new() -> Result<Self, Error> {
        Ok(Self { names: variant_names::<T>()?, upcasters: HashMap::new(), _marker: PhantomData })
    }

    /// Write `name` one past `upcaster`'s highest step, and raise its older payloads through it. A name
    /// that is not a variant of `T`, or a hole in the steps, is a `codec` error, and a second upcaster for
    /// one name is [`ErrorCode::AlreadyRegistered`]: each would leave a stale payload unreadable or
    /// decoding as it is.
    pub fn upcaster(mut self, name: &str, upcaster: Upcaster) -> Result<Self, Error> {
        let Some(&name) = self.names.iter().find(|n| **n == name) else {
            return Err(Error::codec(format_args!("an upcaster is registered for {name:?}, which is no event name of this registry")));
        };
        let version = target(Some(&upcaster)).map_err(|e| Error::codec(format_args!("the upcaster for {name:?}: {}", e.message)))?;
        if self.upcasters.insert(name, (version, upcaster)).is_some() {
            return Err(Error::new(ErrorCode::AlreadyRegistered, format_args!("{name:?} has two upcasters")));
        }
        Ok(self)
    }

    /// The schema version this registry writes `name` at, and raises its older payloads to.
    fn version(&self, name: &str) -> u32 {
        self.upcasters.get(name).map_or(edax_event::FIRST_VERSION, |(version, _)| *version)
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
        Ok(Event { name: name.to_owned(), version: self.version(name), data })
    }

    fn unmarshal(&self, raw: &Event) -> Result<T, Error> {
        if !self.names.contains(&raw.name.as_str()) {
            return Err(Error::codec(format_args!("no event type registered for {:?}", raw.name)));
        }
        let upcaster = self.upcasters.get(raw.name.as_str()).map(|(_, upcaster)| upcaster);
        let data = raise(upcaster, &raw.data, raw.version, self.version(&raw.name))?;
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

/// The refusal of every serde shape that is not an enum of newtype or unit variants.
fn refused<T>() -> Result<T, Refusal> {
    Err(Refusal(Error::codec(NEEDS_VARIANTS)))
}

macro_rules! refuse {
    ($($method:ident($($arg:ty),*);)*) => {
        $(fn $method(self, $(_: $arg),*) -> Result<Self::Ok, Self::Error> {
            refused()
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
        refused()
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, _: &T) -> Result<Self::Ok, Refusal> {
        refused()
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Refusal> {
        refused()
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Refusal> {
        refused()
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeTupleStruct, Refusal> {
        refused()
    }
    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeTupleVariant, Refusal> {
        refused()
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Refusal> {
        refused()
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Refusal> {
        refused()
    }
    fn serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeStructVariant, Refusal> {
        refused()
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
        refused()
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
        refused()
    }

    fn struct_variant<V: Visitor<'de>>(self, _: &'static [&'static str], _: V) -> Result<V::Value, Refusal> {
        refused()
    }
}

/// The variant names of `T`, read off its `Deserialize` by a deserializer that records the list the
/// derive hands `deserialize_enum` and then stops.
fn variant_names<T: DeserializeOwned>() -> Result<&'static [&'static str], Error> {
    struct Probe<'a>(&'a Cell<Option<&'static [&'static str]>>);

    impl<'de> de::Deserializer<'de> for Probe<'_> {
        type Error = Refusal;

        fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Refusal> {
            refused()
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
        let codec = JsonRegistryCodec::<AccountEvent>::new().unwrap();
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
        // Deposited is at v2 through its upcaster; Withdrawn and Frozen have none, so they stay at v1.
        let codec = JsonRegistryCodec::<AccountEvent>::new().unwrap().upcaster("Deposited", upcaster).unwrap();
        let event = |name: &str, version: u32, data: &str| Event { name: name.into(), version, data: data.as_bytes().to_vec() };
        let cases: [(&str, Event, Result<AccountEvent, &str>); 9] = [
            ("current payload", event("Deposited", 2, r#"{"account_id":"acct-1","amount_cents":4200}"#), Ok(deposited())),
            ("older payload raised by its upcaster", event("Deposited", 1, r#"{"account_id":"acct-1","amount":42}"#), Ok(deposited())),
            (
                "older payload of a name with no upcaster reads as it is",
                event("Withdrawn", 0, r#"{"account_id":"acct-1","amount_cents":5,"memo":"atm"}"#),
                Ok(AccountEvent::Withdrawn(Withdrawn { account_id: "acct-1".into(), amount_cents: 5, memo: Some("atm".into()) })),
            ),
            (
                "a newer writer's payload is refused, with an upcaster",
                event("Deposited", 3, r#"{"account_id":"acct-1","amount_cents":4200}"#),
                Err("codec: cannot upcast downward: from 3 > to 2 (the payload comes from a newer writer)"),
            ),
            (
                "a newer writer's payload is refused, without one",
                event("Withdrawn", 2, r#"{"account_id":"acct-1","amount_cents":5,"memo":null,"fee":1}"#),
                Err("codec: cannot upcast downward: from 2 > to 1 (the payload comes from a newer writer)"),
            ),
            (
                "a gap in the upcaster is refused",
                event("Deposited", 0, r#"{"account_id":"acct-1","amount":42}"#),
                Err("codec: missing upcast step 0 (no transform for v0 -> v1)"),
            ),
            ("an unknown name is refused", event("Closed", 2, "{}"), Err(r#"codec: no event type registered for "Closed""#)),
            ("a unit variant reads from its empty map", event("Frozen", 1, "{}"), Ok(AccountEvent::Frozen)),
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

    /// The review's probe: Deposited changed shape at v2, then only Withdrawn changes. Withdrawn gets its
    /// own step, and Deposited keeps reading its v1 and v2 rows; under one shared registry version both
    /// failed with a missing step 2.
    #[test]
    fn bumping_one_name_moves_no_other() {
        let cents = |data: Vec<u8>| {
            let mut v: serde_json::Value = serde_json::from_slice(&data).map_err(Error::codec)?;
            v["amount_cents"] = (v["amount"].as_i64().unwrap_or(0) * 100).into();
            serde_json::to_vec(&v).map_err(Error::codec)
        };
        let memo = |data: Vec<u8>| {
            let mut v: serde_json::Value = serde_json::from_slice(&data).map_err(Error::codec)?;
            v["memo"] = "migrated".into();
            serde_json::to_vec(&v).map_err(Error::codec)
        };
        let codec = JsonRegistryCodec::<AccountEvent>::new()
            .unwrap()
            .upcaster("Deposited", Upcaster::new().step(1, cents).unwrap())
            .unwrap()
            .upcaster("Withdrawn", Upcaster::new().step(1, memo).unwrap())
            .unwrap();
        let withdrawn = |memo: &str| AccountEvent::Withdrawn(Withdrawn { account_id: "acct-1".into(), amount_cents: 5, memo: Some(memo.into()) });
        for (name, event, version) in [("Deposited", deposited(), 2), ("Withdrawn", withdrawn("atm"), 2), ("Frozen", AccountEvent::Frozen, 1)] {
            assert_eq!(codec.marshal(&event).unwrap().version, version, "{name} writes its own version");
        }
        let event = |name: &str, version: u32, data: &str| Event { name: name.into(), version, data: data.as_bytes().to_vec() };
        let cases: [(&str, Event, AccountEvent); 4] = [
            ("the unchanged name's v2 row reads", event("Deposited", 2, r#"{"account_id":"acct-1","amount_cents":4200}"#), deposited()),
            ("the unchanged name's v1 row is still raised", event("Deposited", 1, r#"{"account_id":"acct-1","amount":42}"#), deposited()),
            ("the bumped name's v1 row is raised", event("Withdrawn", 1, r#"{"account_id":"acct-1","amount_cents":5}"#), withdrawn("migrated")),
            ("the bumped name's v2 row reads", event("Withdrawn", 2, r#"{"account_id":"acct-1","amount_cents":5,"memo":"atm"}"#), withdrawn("atm")),
        ];
        for (name, raw, want) in cases {
            assert_eq!(codec.unmarshal(&raw).map_err(|e| e.to_string()), Ok(want), "{name}");
        }
    }

    #[test]
    fn construction_refusals() {
        #[derive(Serialize, Deserialize)]
        struct NotAnEnum {
            n: i64,
        }
        assert_eq!(
            JsonRegistryCodec::<NotAnEnum>::new().err().map(|e| e.to_string()),
            Some(format!("codec: {NEEDS_VARIANTS}"))
        );
        let dangling = JsonRegistryCodec::<AccountEvent>::new().unwrap().upcaster("Closed", Upcaster::new());
        assert_eq!(
            dangling.err().map(|e| e.to_string()).as_deref(),
            Some(r#"codec: an upcaster is registered for "Closed", which is no event name of this registry"#)
        );
        let twice = JsonRegistryCodec::<AccountEvent>::new()
            .unwrap()
            .upcaster("Deposited", Upcaster::new())
            .unwrap()
            .upcaster("Deposited", Upcaster::new());
        assert_eq!(twice.err().map(|e| e.code), Some(ErrorCode::AlreadyRegistered));
        // A hole in the chain leaves the rows at the missing version unreadable, so it fails here.
        let holed = || Upcaster::new().step(1, Ok).unwrap().step(3, Ok).unwrap();
        let skip = "upcast steps skip v2: a payload stored at v2 could never be raised";
        let registry = JsonRegistryCodec::<AccountEvent>::new().unwrap().upcaster("Deposited", holed());
        assert_eq!(registry.err().map(|e| e.to_string()), Some(format!(r#"codec: the upcaster for "Deposited": {skip}"#)));
        let single = JsonCodec::<Deposited>::new("Deposited").with_upcaster(holed());
        assert_eq!(single.err().map(|e| e.to_string()), Some(format!("codec: {skip}")));
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
        let codec = JsonRegistryCodec::<Shapes>::new().unwrap();
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
        let codec = JsonCodec::<Deposited>::new("Deposited");
        let event = |name: &str, version: u32| Event { name: name.into(), version, data: br#"{"account_id":"acct-1","amount_cents":4200}"#.to_vec() };
        let want = Deposited { account_id: "acct-1".into(), amount_cents: 4200 };
        assert_eq!(codec.marshal(&want).unwrap().version, 1, "no upcaster: the first version");
        assert_eq!(codec.unmarshal(&event("Deposited", 1)).unwrap(), want, "current");
        assert_eq!(codec.unmarshal(&event("Deposited", 0)).unwrap(), want, "older, no upcaster: identity");
        assert_eq!(
            codec.unmarshal(&event("Deposited", 2)).err().map(|e| e.to_string()).as_deref(),
            Some("codec: cannot upcast downward: from 2 > to 1 (the payload comes from a newer writer)"),
            "newer writer"
        );
        assert_eq!(
            codec.unmarshal(&event("Withdrawn", 1)).err().map(|e| e.to_string()).as_deref(),
            Some(r#"codec: event "Withdrawn" is not "Deposited", the one name this codec reads"#),
            "another name"
        );
        let raised = JsonCodec::<Deposited>::new("Deposited")
            .with_upcaster(Upcaster::new().step(1, |_| Ok(br#"{"account_id":"acct-9","amount_cents":1}"#.to_vec())).unwrap())
            .unwrap();
        assert_eq!(raised.marshal(&want).unwrap().version, 2, "an upcaster's last step sets the version");
        assert_eq!(raised.unmarshal(&event("Deposited", 1)).unwrap().account_id, "acct-9", "an upcaster raises v1");
        assert_eq!(
            Upcaster::new().to(b"x".to_vec(), 1, 2).err().map(|e| e.to_string()).as_deref(),
            Some("codec: missing upcast step 1 (no transform for v1 -> v2)")
        );
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn msgpack_writes_structs_as_maps_and_round_trips() {
        let codec = MsgpackRegistryCodec::<AccountEvent>::new().unwrap();
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
        let mp = MsgpackCodec::<Posted>::new("Posted");
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
                marshal: || JsonCodec::<Posted>::new("Posted").marshal(&Posted::default()),
                want: Ok("Posted"),
            },
            Case {
                name: "json cannot write a non-string map key",
                marshal: || JsonCodec::<Posted>::new("Posted").marshal(&posted()),
                want: Err("codec: key must be a string"),
            },
            Case {
                name: "json surfaces a refusing Serialize impl",
                marshal: || {
                    let event = Refused { minor_units: 500, currency: "bitcoin".into() };
                    JsonCodec::<Refused>::new("Refused").marshal(&event)
                },
                want: Err(r#"codec: unknown currency "bitcoin""#),
            },
        ];
        #[cfg(feature = "msgpack")]
        cases.extend([
            Case {
                name: "msgpack writes what json cannot",
                marshal: || MsgpackCodec::<Posted>::new("Posted").marshal(&posted()),
                want: Ok("Posted"),
            },
            Case {
                name: "msgpack surfaces a refusing Serialize impl",
                marshal: || {
                    let event = Refused { minor_units: 500, currency: "bitcoin".into() };
                    MsgpackCodec::<Refused>::new("Refused").marshal(&event)
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

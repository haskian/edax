//! [`StructsFromMaps`]: a deserializer adapter that reads a struct only from a map, at every depth.
//!
//! serde's derive reads a struct from a map keyed by field name *or* from a positional array, so a
//! reader would accept two shapes of one value. Python's msgspec reads a struct from a map only, at
//! every depth, so a Rust reader that took the array would decode bytes a Python reader dead-letters.
//! And a writer that added a field would turn a fixed-length array into poison for every older reader.
//! The adapter holds every serde type to the map form: it forwards each `deserialize_*` call unchanged,
//! except that a struct (and a struct variant) is read through `deserialize_map` with a visitor that
//! takes nothing but a map. It wraps every nested deserializer, seed and access the same way, so a
//! struct inside a list, a map, an `Option` or an enum variant meets the rule too.
//!
//! A plain string or number, a list, a map, a newtype and a tuple read as before. The one limit: a
//! value that serde buffers before it picks the type (an untagged or internally tagged enum, a
//! `#[serde(flatten)]` field) reads its inner structs from the buffer, which the adapter cannot see.

use std::fmt;

use serde::de::{self, DeserializeSeed, Deserializer, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};

/// Wraps a format's deserializer so that every struct it reads, at any depth, reads from a map only.
/// Every other shape passes through unchanged. See the module docs.
pub struct StructsFromMaps<D>(pub D);

/// A seed whose value deserializes through the adapter.
struct Seed<S>(S);

/// A visitor whose nested values deserialize through the adapter.
struct Nested<V>(V);

/// A struct's visitor: it takes a map only, and its fields deserialize through the adapter.
struct MapOnly<V>(V);

struct Seq<A>(A);
struct Map<A>(A);
struct Enum<A>(A);
struct Variant<A>(A);

impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for Seed<S> {
    type Value = S::Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<S::Value, D::Error> {
        self.0.deserialize(StructsFromMaps(deserializer))
    }
}

macro_rules! forward_deserialize {
    ($($method:ident),* $(,)?) => {$(
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
            self.0.$method(Nested(visitor))
        }
    )*};
}

impl<'de, D: Deserializer<'de>> Deserializer<'de> for StructsFromMaps<D> {
    type Error = D::Error;

    forward_deserialize!(
        deserialize_any,
        deserialize_bool,
        deserialize_i8,
        deserialize_i16,
        deserialize_i32,
        deserialize_i64,
        deserialize_i128,
        deserialize_u8,
        deserialize_u16,
        deserialize_u32,
        deserialize_u64,
        deserialize_u128,
        deserialize_f32,
        deserialize_f64,
        deserialize_char,
        deserialize_str,
        deserialize_string,
        deserialize_bytes,
        deserialize_byte_buf,
        deserialize_option,
        deserialize_unit,
        deserialize_seq,
        deserialize_map,
        deserialize_identifier,
        deserialize_ignored_any,
    );

    fn deserialize_unit_struct<V: Visitor<'de>>(self, name: &'static str, visitor: V) -> Result<V::Value, D::Error> {
        self.0.deserialize_unit_struct(name, Nested(visitor))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(self, name: &'static str, visitor: V) -> Result<V::Value, D::Error> {
        self.0.deserialize_newtype_struct(name, Nested(visitor))
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, D::Error> {
        self.0.deserialize_tuple(len, Nested(visitor))
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_tuple_struct(name, len, Nested(visitor))
    }

    /// The one call the adapter changes: a struct is asked for as a map, and its visitor refuses the rest.
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_map(MapOnly(visitor))
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_enum(name, variants, Nested(visitor))
    }

    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }
}

macro_rules! forward_visit {
    ($($method:ident($ty:ty)),* $(,)?) => {$(
        fn $method<E: de::Error>(self, value: $ty) -> Result<V::Value, E> {
            self.0.$method(value)
        }
    )*};
}

impl<'de, V: Visitor<'de>> Visitor<'de> for Nested<V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.expecting(f)
    }

    forward_visit!(
        visit_bool(bool),
        visit_i8(i8),
        visit_i16(i16),
        visit_i32(i32),
        visit_i64(i64),
        visit_i128(i128),
        visit_u8(u8),
        visit_u16(u16),
        visit_u32(u32),
        visit_u64(u64),
        visit_u128(u128),
        visit_f32(f32),
        visit_f64(f64),
        visit_char(char),
        visit_str(&str),
        visit_borrowed_str(&'de str),
        visit_string(String),
        visit_bytes(&[u8]),
        visit_borrowed_bytes(&'de [u8]),
        visit_byte_buf(Vec<u8>),
    );

    fn visit_none<E: de::Error>(self) -> Result<V::Value, E> {
        self.0.visit_none()
    }

    fn visit_unit<E: de::Error>(self) -> Result<V::Value, E> {
        self.0.visit_unit()
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<V::Value, D::Error> {
        self.0.visit_some(StructsFromMaps(deserializer))
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, deserializer: D) -> Result<V::Value, D::Error> {
        self.0.visit_newtype_struct(StructsFromMaps(deserializer))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<V::Value, A::Error> {
        self.0.visit_seq(Seq(seq))
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<V::Value, A::Error> {
        self.0.visit_map(Map(map))
    }

    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<V::Value, A::Error> {
        self.0.visit_enum(Enum(data))
    }
}

impl<'de, V: Visitor<'de>> Visitor<'de> for MapOnly<V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.expecting(f)?;
        f.write_str(" as a map keyed by field name")
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<V::Value, A::Error> {
        self.0.visit_map(Map(map))
    }
}

impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for Seq<A> {
    type Error = A::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>, A::Error> {
        self.0.next_element_seed(Seed(seed))
    }

    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}

impl<'de, A: MapAccess<'de>> MapAccess<'de> for Map<A> {
    type Error = A::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, A::Error> {
        self.0.next_key_seed(Seed(seed))
    }

    fn next_value_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value, A::Error> {
        self.0.next_value_seed(Seed(seed))
    }

    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}

impl<'de, A: EnumAccess<'de>> EnumAccess<'de> for Enum<A> {
    type Error = A::Error;
    type Variant = Variant<A::Variant>;

    fn variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<(T::Value, Self::Variant), A::Error> {
        let (value, variant) = self.0.variant_seed(Seed(seed))?;
        Ok((value, Variant(variant)))
    }
}

impl<'de, A: VariantAccess<'de>> VariantAccess<'de> for Variant<A> {
    type Error = A::Error;

    fn unit_variant(self) -> Result<(), A::Error> {
        self.0.unit_variant()
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, A::Error> {
        self.0.newtype_variant_seed(Seed(seed))
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, A::Error> {
        self.0.tuple_variant(len, Nested(visitor))
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, A::Error> {
        self.0.struct_variant(fields, MapOnly(visitor))
    }
}

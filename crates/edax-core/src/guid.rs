//! Globally-unique event ids.
//!
//! [`guid`] returns a UUID version 7: a 48-bit Unix-millis timestamp, a 42-bit counter and 32
//! random bits. Within one process the counter is shared, so ids are strictly increasing in mint
//! order, which makes them a safe index key; across processes they are ordered only by their
//! millisecond. [`guid_simple`] mints the same kind of id in the hyphen-free form a fixed-width
//! token needs. Unlike the rest of the toolkit this is *non-deterministic*: it reads the clock and a
//! random source. Minting carries no golden vector, only the structural guarantees a v7 id must always
//! hold. [`uuid7_micros`] deterministically extracts an existing UUIDv7's timestamp.

use uuid::Uuid;

use crate::Error;

/// The UUIDv7's 48-bit Unix-millisecond timestamp, expressed exactly in microseconds.
///
/// Refuses malformed UUIDs, other versions and variants other than RFC 4122 with a `codec` error.
/// The producer's timestamp describes placement, not a trusted clock or a global ordering.
pub fn uuid7_micros(value: &str) -> Result<i64, Error> {
    let id = Uuid::parse_str(value).map_err(|error| Error::codec(format_args!("invalid UUIDv7: {error}")))?;
    if id.get_version_num() != 7 || id.get_variant() != uuid::Variant::RFC4122 {
        return Err(Error::codec("expected an RFC 4122 UUIDv7"));
    }
    // Even the largest 48-bit millisecond value fits i64 after multiplication by 1000.
    Ok((id.as_u128() >> 80) as i64 * 1_000)
}

/// A fresh time-ordered globally-unique id (UUID v7), lowercase hyphenated.
pub fn guid() -> String {
    Uuid::now_v7().to_string()
}

/// A fresh UUID v7 without the hyphens: 32 lowercase hexadecimal characters.
///
/// The fixed-width form a ledger value needs, where the token sits at a fixed offset and every
/// claimant's must be the same number of bytes.
pub fn guid_simple() -> String {
    Uuid::now_v7().simple().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid7_timestamp_is_exact() {
        for (name, value, micros) in [
            ("zero", "00000000-0000-7000-8000-000000000000", 0),
            ("one_millisecond", "00000000-0001-7000-8000-000000000000", 1_000),
            (
                "conventional",
                "017f22e2-79b0-7cc3-98c4-dc0c0c07398f",
                1_645_557_742_000_000,
            ),
            (
                "simple_uppercase",
                "017F22E279B07CC398C4DC0C0C07398F",
                1_645_557_742_000_000,
            ),
            (
                "maximum",
                "ffffffff-ffff-7fff-bfff-ffffffffffff",
                281_474_976_710_655_000,
            ),
        ] {
            assert_eq!(uuid7_micros(value).unwrap(), micros, "{name}");
        }
    }

    #[test]
    fn uuid7_timestamp_rejects_invalid_ids() {
        for (name, value) in [
            ("empty", ""),
            ("malformed", "not-a-uuid"),
            ("wrong_version", "017f22e2-79b0-4cc3-98c4-dc0c0c07398f"),
            ("ncs_variant", "017f22e2-79b0-7cc3-18c4-dc0c0c07398f"),
            ("microsoft_variant", "017f22e2-79b0-7cc3-d8c4-dc0c0c07398f"),
            ("future_variant", "017f22e2-79b0-7cc3-f8c4-dc0c0c07398f"),
        ] {
            let error = uuid7_micros(value).unwrap_err();
            assert_eq!(error.code, crate::ErrorCode::Codec, "{name}");
            assert!(error.message.contains("UUIDv7"), "{name}: {error}");
        }
    }

    #[test]
    fn well_formed_v7_and_unique() {
        let a = guid();
        assert_ne!(a, guid()); // the shared counter -> distinct ids

        let lengths: Vec<usize> = a.split('-').map(str::len).collect();
        assert_eq!(lengths, [8, 4, 4, 4, 12]); // 8-4-4-4-12 hyphenated form

        let u = Uuid::parse_str(&a).unwrap();
        assert_eq!(u.get_version_num(), 7);
        assert_eq!(u.get_variant(), uuid::Variant::RFC4122);
    }

    #[test]
    fn the_simple_form_is_32_lowercase_hex_digits() {
        let a = guid_simple();
        assert_eq!(a.len(), 32); // the width a fence compares
        assert!(
            a.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "{a} is not lowercase hex"
        );
        assert_ne!(a, guid_simple()); // the shared counter -> distinct ids
        assert_eq!(Uuid::parse_str(&a).unwrap().get_version_num(), 7);
    }

    /// Adopters index on these ids, so the in-process order is pinned: both forms come from one
    /// counter, and a later mint always sorts after an earlier one.
    #[test]
    fn ids_minted_in_one_process_are_strictly_increasing() {
        let mut previous = guid_simple();
        for i in 0..50_000 {
            let next = if i % 2 == 0 {
                guid().replace('-', "")
            } else {
                guid_simple()
            };
            assert!(next > previous, "{next} did not sort after {previous}");
            previous = next;
        }
    }
}

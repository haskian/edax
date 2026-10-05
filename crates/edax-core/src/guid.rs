//! Globally-unique event ids.
//!
//! [`guid`] returns a UUID version 7: a 48-bit Unix-millis timestamp, a 42-bit counter and 32
//! random bits. Within one process the counter is shared, so ids are strictly increasing in mint
//! order, which makes them a safe index key; across processes they are ordered only by their
//! millisecond. [`guid_simple`] mints the same kind of id in the hyphen-free form a fixed-width
//! token needs. Unlike the rest of the toolkit this is *non-deterministic*: it reads the clock and a
//! random source. So it carries no golden vector, only the structural guarantees a v7 id must always
//! hold.

use uuid::Uuid;

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

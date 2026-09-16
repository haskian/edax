//! Globally-unique event ids.
//!
//! [`guid`] returns a UUID version 7: a 48-bit Unix-millis timestamp prefix followed by random
//! bits, so ids are unique, roughly time-ordered, and index-friendly. [`guid_simple`] is the same
//! id in the hyphen-free form a fixed-width token needs. Unlike the rest of the
//! toolkit this is *non-deterministic* — it reads the clock and a random source — so it carries no
//! golden vector, only the structural guarantees a v7 id must always hold.

use uuid::Uuid;

/// A fresh time-ordered globally-unique id (UUID v7), lowercase hyphenated.
pub fn guid() -> String {
    Uuid::now_v7().to_string()
}

/// The same id without the hyphens: 32 lowercase hexadecimal characters.
///
/// The fixed-width form a compare-and-write fence needs, where the token is a prefix of a stored
/// value and every claimant's must be the same number of bytes.
pub fn guid_simple() -> String {
    Uuid::now_v7().simple().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn well_formed_v7_and_unique() {
        let a = guid();
        assert_ne!(a, guid()); // 74 random bits -> distinct ids

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
        assert!(a.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)), "{a} is not lowercase hex");
        assert_ne!(a, guid_simple()); // 74 random bits -> distinct ids
        assert_eq!(Uuid::parse_str(&a).unwrap().get_version_num(), 7);
    }
}

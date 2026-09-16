//! The Redis idempotency ledger: the stored value's layout, and the Lua a fenced write runs.
//!
//! # The value
//!
//! One key, `{prefix}{key}`, holds `<owner><record>`: a fixed-width hex fence token followed by the
//! kernel's idempotency-record JSON. One key means one TTL governs both halves. A sibling key
//! holding the token would expire on a clock of its own, so a claimant could win the record, lose
//! the owner, and hold a claim it can never settle.
//!
//! The record half is never written here. [`claim_value`] asks `edax-cqrs` for the pending record's
//! bytes and [`record_value`] takes a settled record's bytes from the caller, so the record's wire
//! shape has exactly one author and `testdata/golden/idempotency_record.json` keeps pinning it.
//!
//! # The fence
//!
//! A claim can lapse and be taken over. The lapsed worker's `complete` must not overwrite the new
//! holder's claim and its `release` must not delete it, so both settle through a compare-and-write:
//! [`FENCED_SET_SCRIPT`] and [`FENCED_DEL_SCRIPT`] read the token's width with `GETRANGE` and write
//! only on a match. A `GET` then a `SET` would leave exactly the gap the fence exists to close.
//! The bodies are constants, byte for byte, so two SDKs registering the script share one `EVALSHA`
//! digest instead of each caching its own reformatting.

use edax_core::{guid_simple, AdapterError};
use edax_cqrs::{encode_record, Record};

/// Width of the owner token in the stored value, in ASCII hex characters.
pub const OWNER_LEN: usize = 32;

/// A fresh fence token: the kernel's id in its 32-character lowercase hex form, no hyphens.
///
/// v7 rather than random bytes so tokens sort by mint time, which makes a ledger dump readable; the
/// 74 random bits are what make two claimants distinguishable.
pub fn new_owner() -> String {
    guid_simple()
}

/// The stored value for a fresh claim: `owner` followed by the kernel's **pending** record for
/// `payload_hash`.
///
/// This is the value a claimant writes with `SET NX PX`, so whoever loses the race reads the
/// winner's pending record and learns the key is in flight rather than "unseen".
pub fn claim_value(owner: &str, payload_hash: u64) -> Result<String, AdapterError> {
    // The record's bytes are `edax-cqrs`'s, never hand-formatted here: a ledger written by one SDK
    // is read back through the same decoder in every other.
    let bytes = encode_record(&Record::<()>::pending(payload_hash)).map_err(AdapterError::codec)?;
    // `serde_json` emits UTF-8, so this cannot fail; it is checked rather than unwrapped because a
    // panic inside a native binding crosses an FFI boundary that cannot catch it.
    let record = String::from_utf8(bytes).map_err(AdapterError::codec)?;
    record_value(owner, &record)
}

/// The stored value for a settled claim: `owner` followed by `record_json` as the caller encoded it.
///
/// The record is passed through untouched. A host that already holds the record's bytes (an SDK
/// encoding its own result type) must not re-encode them here, or a result would round-trip through
/// a second serializer on the way to the ledger.
pub fn record_value(owner: &str, record_json: &str) -> Result<String, AdapterError> {
    check_owner(owner)?;
    Ok(format!("{owner}{record_json}"))
}

/// Take a stored value apart into `(owner, record)`.
///
/// Strict about the frame — a value that is not UTF-8, or too short to hold a token, cannot be
/// settled by anyone — and liberal about the token itself: a token this kernel would not mint still
/// reads back, so a ledger written by an older store stays readable. What the record half says is
/// the record decoder's business, not this function's.
pub fn split_value(raw: &[u8]) -> Result<(String, String), AdapterError> {
    let text = std::str::from_utf8(raw).map_err(|e| AdapterError::codec(format!("stored value is not UTF-8: {e}")))?;
    if text.len() < OWNER_LEN {
        return Err(AdapterError::codec(format!(
            "stored value is {} bytes, shorter than the {OWNER_LEN}-character owner token",
            text.len()
        )));
    }
    // Only a multibyte character straddling the token's width can fail here, and that value was
    // never written by this kernel: the token is ASCII.
    let (owner, record) = text.split_at_checked(OWNER_LEN).ok_or_else(|| {
        AdapterError::codec(format!(
            "stored value does not split into an owner token at {OWNER_LEN} bytes"
        ))
    })?;
    Ok((owner.to_owned(), record.to_owned()))
}

/// Set the value under a key **only while it still carries the caller's owner token**, refreshing
/// the TTL. `KEYS[1]` is the key; `ARGV` is the owner token, the new value, and the TTL in
/// milliseconds. Returns `1` when the write landed, `0` when the key is absent or another claimant
/// holds it.
pub const FENCED_SET_SCRIPT: &str = "
if redis.call('GETRANGE', KEYS[1], 0, #ARGV[1] - 1) == ARGV[1] then
  return redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3]) and 1 or 0
end
return 0
";

/// Delete a key **only while it still carries the caller's owner token**. `KEYS[1]` is the key;
/// `ARGV[1]` is the owner token. Returns `1` when the delete landed, `0` when the key is absent or
/// another claimant holds it.
pub const FENCED_DEL_SCRIPT: &str = "
if redis.call('GETRANGE', KEYS[1], 0, #ARGV[1] - 1) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
";

/// Reject a token that could never fence anything: the fence compares the first [`OWNER_LEN`] bytes,
/// so a token of another width would match a prefix of someone else's, and a non-hex byte is a token
/// this kernel did not mint. The token itself is kept out of the message: it is the capability that
/// authorizes overwriting a claim.
fn check_owner(owner: &str) -> Result<(), AdapterError> {
    if owner.len() != OWNER_LEN {
        return Err(AdapterError::codec(format!(
            "owner token is {} bytes, expected {OWNER_LEN}",
            owner.len()
        )));
    }
    if !owner.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(AdapterError::codec("owner token is not ASCII hexadecimal"));
    }
    Ok(())
}

#[cfg(test)]
// A test asserts on the value it just built; a panic there is the failure report.
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    /// A real dispatch's payload hash, past 2^53 so a lossy reader is caught. Shared with
    /// `crates/edax-py/tests/test_redis.py`, which pins the same claim value from the Python side.
    const HASH: u64 = 11_258_580_006_047_770_801;
    /// The pending record those bytes carry, as the ledger holds it.
    const PENDING: &str = r#"{"payload_hash":11258580006047770801,"result":null,"error":null,"pending":true}"#;
    /// A well-formed token, in the shape `new_owner` mints.
    const OWNER: &str = "0190b5c8e1f37c4b9a2d5e6f7a8b9c0d";

    /// One owner-validation branch, shared by the two writers because they share the check.
    struct OwnerCase {
        name: &'static str,
        owner: &'static str,
        /// The error's wire form, or `None` when the token is accepted.
        error: Option<&'static str>,
    }

    fn owner_cases() -> [OwnerCase; 6] {
        [
            OwnerCase {
                name: "minted_token_is_accepted",
                owner: OWNER,
                error: None,
            },
            OwnerCase {
                name: "uppercase_hex_is_accepted",
                owner: "0190B5C8E1F37C4B9A2D5E6F7A8B9C0D",
                error: None,
            },
            OwnerCase {
                name: "one_character_short",
                owner: "0190b5c8e1f37c4b9a2d5e6f7a8b9c0",
                error: Some("codec: owner token is 31 bytes, expected 32"),
            },
            OwnerCase {
                name: "one_character_long",
                owner: "0190b5c8e1f37c4b9a2d5e6f7a8b9c0de",
                error: Some("codec: owner token is 33 bytes, expected 32"),
            },
            OwnerCase {
                name: "empty",
                owner: "",
                error: Some("codec: owner token is 0 bytes, expected 32"),
            },
            OwnerCase {
                // Right width, wrong alphabet: `z` is not a hex digit.
                name: "non_hex_at_full_width",
                owner: "0190b5c8e1f37c4b9a2d5e6f7a8b9c0z",
                error: Some("codec: owner token is not ASCII hexadecimal"),
            },
        ]
    }

    /// The token's *shape* is this kernel's business — the fence compares a fixed width — while
    /// the id behind it is `edax_core::guid_simple`'s, tested there.
    #[test]
    fn a_minted_token_is_one_the_fence_can_compare() {
        let owner = new_owner();
        assert_eq!(owner.len(), OWNER_LEN);
        assert_ne!(owner, new_owner(), "74 random bits -> distinct tokens");
        // A minted token must pass the check the writers apply, or no claim could ever be written.
        assert_eq!(record_value(&owner, PENDING), Ok(format!("{owner}{PENDING}")));
    }

    #[test]
    fn claim_value_branches() {
        for case in owner_cases() {
            let got = claim_value(case.owner, HASH);
            match case.error {
                None => assert_eq!(got, Ok(format!("{}{PENDING}", case.owner)), "{}", case.name),
                Some(wire) => assert_eq!(got.unwrap_err().to_string(), wire, "{}", case.name),
            }
        }
        // The hash is carried whole: a reader that rounded it through a double would lose the low bits.
        assert_eq!(
            claim_value(OWNER, u64::MAX),
            Ok(format!(
                r#"{OWNER}{{"payload_hash":18446744073709551615,"result":null,"error":null,"pending":true}}"#
            ))
        );
        assert_eq!(
            claim_value(OWNER, 0),
            Ok(format!(
                r#"{OWNER}{{"payload_hash":0,"result":null,"error":null,"pending":true}}"#
            ))
        );
    }

    #[test]
    fn record_value_branches() {
        const DONE: &str =
            r#"{"payload_hash":11258580006047770801,"result":{"balance_cents":4200},"error":null,"pending":false}"#;
        for case in owner_cases() {
            let got = record_value(case.owner, DONE);
            match case.error {
                None => assert_eq!(got, Ok(format!("{}{DONE}", case.owner)), "{}", case.name),
                Some(wire) => assert_eq!(got.unwrap_err().to_string(), wire, "{}", case.name),
            }
        }
        // The record half is passed through untouched, whatever it holds.
        for (name, record) in [
            ("empty_record", ""),
            ("non_ascii_record", r#"{"payload_hash":1,"result":"café ☕","error":null,"pending":false}"#),
            ("not_json_at_all", "not json"),
        ] {
            assert_eq!(record_value(OWNER, record), Ok(format!("{OWNER}{record}")), "{name}");
        }
        // A claim value is a record value over the kernel's pending record; the two agree by construction.
        assert_eq!(claim_value(OWNER, HASH), record_value(OWNER, PENDING));
    }
}

//! The Redis idempotency ledger: the key, the stored value's layout, the settings a store is built
//! with, and what a claim's reply means.
//!
//! # The key
//!
//! [`ledger_key`] is `{prefix}{key}` as UTF-8 bytes. Two SDKs that compose it differently never meet
//! on one ledger, and nothing fails: both run every command. So the composition is here.
//!
//! # The value
//!
//! One key holds `1<owner><record>`: the layout version `1`, a fixed-width hex owner token, then the
//! kernel's idempotency-record JSON. One key means one TTL governs both halves. A sibling key holding
//! the token would expire on a clock of its own, so a claimant could win the record, lose the owner,
//! and hold a claim it can never settle. The version byte lets a later layout be refused by name
//! instead of being split at the wrong place.
//!
//! The record half is never written here. [`claim_value`] asks `edax-cqrs` for the pending record's
//! bytes and [`settled_value`] takes a settled record's bytes from the caller, so the record's wire
//! shape has exactly one author and `testdata/golden/idempotency_record.json` keeps pinning it.
//!
//! # The claim handle
//!
//! The value a claimant writes is its handle: [`claim_value`] mints it, the claim script stores it,
//! and every later write of that claimant compares the stored value with it **exactly**. A claim
//! that lapsed and was taken over holds another value, so its old owner settles nothing. A settled
//! record holds another value too, so a late release never deletes the outcome of committed work.
//! See [`crate::scripts`].

use edax_core::{guid_simple, Error};
use edax_cqrs::{decode_record, encode_record, Claimed, Record, State};

/// The layout version, the first byte of every stored value.
pub const LAYOUT_VERSION: u8 = b'1';

/// Width of the owner token in the stored value, in lowercase ASCII hex characters.
pub const OWNER_LEN: usize = 32;

/// The largest TTL a store accepts, in milliseconds: 2^53 - 1, the largest integer every SDK
/// language's number type holds exactly. Redis adds the TTL to its clock and refuses a sum that
/// overflows, so a bound far below `i64::MAX` keeps that refusal out of reach.
pub const MAX_TTL_MS: i64 = (1 << 53) - 1;

/// A fresh claim's value for `payload_hash`, under a new owner token: [`claim_value`] over the kernel's
/// id in its 32-character lowercase hex form, no hyphens. The value is the claim's handle.
///
/// v7 rather than random bytes so tokens sort by mint time, which makes a ledger dump readable. The
/// 32 random bits and the shared 42-bit counter (`edax_core::guid`) are what make two claimants
/// distinguishable.
pub fn new_claim(payload_hash: u64) -> Result<Vec<u8>, Error> {
    claim_value(&guid_simple(), payload_hash)
}

/// The ledger defaults every SDK's store is built with unless its host overrides them, in
/// milliseconds: the record TTL (a day, the dedup window a settled outcome is replayed in), the claim
/// TTL (five minutes, how long a dead worker's claim blocks its key) and the deadline on each Redis
/// call. One set, so two SDKs on one ledger left at their defaults agree on its windows.
pub const DEFAULT_TTL_MS: i64 = 86_400_000;
/// See [`DEFAULT_TTL_MS`].
pub const DEFAULT_CLAIM_TTL_MS: i64 = 300_000;
/// See [`DEFAULT_TTL_MS`].
pub const DEFAULT_OP_TIMEOUT_MS: i64 = 5_000;

/// Refuse a prefix that cannot name a ledger. The prefix is the ledger's identity: an empty one puts
/// every service sharing a Redis into one key space, where a request id two services both use is one
/// claim.
fn check_prefix(prefix: &str) -> Result<(), Error> {
    if prefix.is_empty() {
        return Err(Error::bad_handle(
            "the ledger prefix is empty; name the service and the environment, as in \"billing:prod:idem:\"",
        ));
    }
    Ok(())
}

/// The Redis key for an idempotency key: `{prefix}{key}`, as UTF-8 bytes.
///
/// No separator is added, so the prefix carries its own. Concatenation is not injective: prefix
/// `billing:` with key `prod:x` is the key prefix `billing:prod:` gives `x`. Distinct services take
/// prefixes neither of which starts with the other.
pub fn ledger_key(prefix: &str, key: &str) -> Result<Vec<u8>, Error> {
    check_prefix(prefix)?;
    Ok([prefix.as_bytes(), key.as_bytes()].concat())
}

/// Refuse a TTL Redis would refuse on every write. `name` is the setting's name, for the message.
fn check_ttl(name: &str, ttl_ms: i64) -> Result<(), Error> {
    if !(1..=MAX_TTL_MS).contains(&ttl_ms) {
        return Err(Error::bad_handle(format_args!(
            "{name} is {ttl_ms} ms; a Redis TTL is a whole number of milliseconds from 1 to {MAX_TTL_MS}"
        )));
    }
    Ok(())
}

/// How many times per claim TTL a heartbeat renews a held claim: it sends a renewal every third of the
/// TTL, timed from the claim's send. [`check_config`] refuses a deadline of a third of the TTL or more.
pub const HEARTBEATS_PER_TTL: i64 = 3;

/// When beat `beat` of a heartbeat is due, in the clock's milliseconds: `sent_at_ms + beat *
/// claim_ttl_ms / HEARTBEATS_PER_TTL`, where `sent_at_ms` is when the store sent the claim. Rounded
/// down, so a beat is never late, and saturating at the ends of `i64`. The schedule runs from the
/// claim's send and never from a reply: the bound [`check_config`] proves holds only on this schedule,
/// so every SDK times its beats with this function rather than restating it.
pub fn heartbeat_due_ms(sent_at_ms: i64, claim_ttl_ms: i64, beat: u32) -> i64 {
    // In i128 the product cannot overflow: |beat * claim_ttl_ms| < 2^32 * 2^63.
    let offset = (i128::from(beat) * i128::from(claim_ttl_ms)).div_euclid(i128::from(HEARTBEATS_PER_TTL));
    let due = i128::from(sent_at_ms) + offset;
    i64::try_from(due).unwrap_or(if due < 0 { i64::MIN } else { i64::MAX })
}

/// Refuse the settings a ledger store is built with, as a `bad_handle` error naming the first one
/// refused, so a misconfigured store fails when it is built rather than on every dispatch: an empty
/// `prefix`, a `ttl_ms` or `claim_ttl_ms` Redis would refuse, an `op_timeout_ms` deadline that is not
/// positive, or a `heartbeat` whose `op_timeout_ms` is not under `claim_ttl_ms / HEARTBEATS_PER_TTL`.
///
/// The heartbeat bound: the store sends beat `k` at `start + k * claim_ttl_ms / 3`, where `start` is
/// when it sent the claim, on a monotonic clock. A beat's reply time never moves the next beat. A beat
/// sent at `S` that lands sets the lease to at least `S + claim_ttl_ms`, since it lands no earlier than
/// it is sent. If the next beat stalls, the one after is sent at `S + 2 * claim_ttl_ms / 3` and lands
/// within `op_timeout_ms`, so before `S + claim_ttl_ms` whenever `op_timeout_ms` is under a third of the
/// TTL. Otherwise one stalled renewal lets a second worker win the key while the first still runs its
/// handler.
pub fn check_config(
    prefix: &str,
    ttl_ms: i64,
    claim_ttl_ms: i64,
    op_timeout_ms: i64,
    heartbeat: bool,
) -> Result<(), Error> {
    check_prefix(prefix)?;
    check_ttl("ttl_ms", ttl_ms)?;
    check_ttl("claim_ttl_ms", claim_ttl_ms)?;
    if op_timeout_ms <= 0 {
        return Err(Error::bad_handle(format_args!(
            "op_timeout_ms is {op_timeout_ms} ms; a deadline must be positive"
        )));
    }
    // Saturating: a deadline near `i64::MAX` is refused, never wrapped below the TTL.
    if heartbeat && op_timeout_ms.saturating_mul(HEARTBEATS_PER_TTL) >= claim_ttl_ms {
        return Err(Error::bad_handle(format_args!(
            "op_timeout_ms is {op_timeout_ms} ms and claim_ttl_ms is {claim_ttl_ms} ms; a heartbeat renews every 1/{HEARTBEATS_PER_TTL} of the claim TTL, so one renewal stalled for the whole deadline lets the claim lapse unless op_timeout_ms is under 1/{HEARTBEATS_PER_TTL} of claim_ttl_ms"
        )));
    }
    Ok(())
}

/// The value a fresh claim writes, which is also the claim's handle: the layout version, `owner`,
/// then the kernel's **pending** record for `payload_hash`.
///
/// Whoever loses the race reads the winner's pending record and learns the key is in flight rather
/// than "unseen".
pub fn claim_value(owner: &str, payload_hash: u64) -> Result<Vec<u8>, Error> {
    check_owner(owner)?;
    // The record's bytes are `edax-cqrs`'s, never hand-formatted here: a ledger written by one SDK
    // is read back through the same decoder in every other.
    let record = encode_record(&Record::<()>::pending(payload_hash))?;
    Ok(frame(owner, &record))
}

/// The value that settles the claim `claim`: the claim's own owner token, then `record_json` as the
/// caller encoded it.
///
/// The record is passed through untouched. A host that already holds the record's bytes (an SDK
/// encoding its own result type) must not re-encode them here, or a result would round-trip through
/// a second serializer on the way to the ledger.
///
/// A record that reads as pending is refused as `bad_handle` ([`edax_cqrs::check_settled`]): it would
/// hold the key as a claim nobody runs for the whole record TTL.
pub fn settled_value(claim: &[u8], record_json: &str) -> Result<Vec<u8>, Error> {
    let (owner, _) = parse_claim(claim)?;
    edax_cqrs::check_settled(record_json.as_bytes())?;
    Ok(frame(owner, record_json.as_bytes()))
}

/// What the complete script's reply means for `key`: `written` is whether it wrote the record (`1`)
/// rather than finding another value under the key (`0`). Not written means the claim lapsed and was
/// taken over, or the key settled with another record: [`edax_cqrs::lapsed_claim`], a `store` error.
/// The outcome was not recorded, so the caller must not report success.
pub fn complete_outcome(key: &str, written: bool) -> Result<(), Error> {
    if written {
        Ok(())
    } else {
        Err(edax_cqrs::lapsed_claim(key))
    }
}

/// Refuse a claim handle this kernel never minted: not a whole value, an owner token of the wrong
/// shape, or a record that is not pending. The scripts compare the handle exactly, so a malformed
/// one could only ever fail to match. Refusing it names the wiring mistake instead.
pub fn check_claim(claim: &[u8]) -> Result<(), Error> {
    parse_claim(claim).map(|_| ())
}

/// Take a stored value apart into `(owner, record)`.
///
/// Strict about the frame: a value that is not UTF-8, carries another layout version, or is too
/// short to hold a token cannot be settled by anyone. Liberal about the token itself: a token this
/// kernel would not mint still reads back, so the record behind it is never stranded. What the
/// record half says is the record decoder's business, not this function's.
pub fn split_value(raw: &[u8]) -> Result<(String, String), Error> {
    let (owner, record) = unframe(raw).map_err(|fault| match fault {
        FrameFault::NotUtf8(e) => Error::codec(format_args!("stored value is not UTF-8: {e}")),
        FrameFault::Empty => Error::codec("stored value is empty"),
        FrameFault::Version(version) => Error::codec(format_args!(
            "stored value has layout version {version:?}; this kernel reads version {:?}",
            char::from(LAYOUT_VERSION)
        )),
        FrameFault::Short => Error::codec(format_args!("stored value has no {OWNER_LEN}-character owner token")),
    })?;
    Ok((owner.to_owned(), record.to_owned()))
}

/// Why a value has no frame to take apart. Each reader words it for its own input: a stored value
/// read back, or a claim handle the caller passed.
enum FrameFault {
    NotUtf8(std::str::Utf8Error),
    Empty,
    /// The first character, which is not [`LAYOUT_VERSION`].
    Version(char),
    /// Too short for a token, or a multibyte character straddles the token's width. Neither was
    /// written by this kernel: the token is ASCII.
    Short,
}

/// The one frame reader: `raw` as `(owner, record)` under the layout version, the token unchecked.
fn unframe(raw: &[u8]) -> Result<(&str, &str), FrameFault> {
    let text = std::str::from_utf8(raw).map_err(FrameFault::NotUtf8)?;
    let Some(rest) = text.strip_prefix(char::from(LAYOUT_VERSION)) else {
        return Err(text.chars().next().map_or(FrameFault::Empty, FrameFault::Version));
    };
    rest.split_at_checked(OWNER_LEN).ok_or(FrameFault::Short)
}

/// Read the claim script's reply. `reply` is the value the key held before the script ran, `None`
/// when it was free; `claim` is the value this caller asked the script to write, and the handle a
/// [`Claimed::Won`] carries.
///
/// A reply equal to `claim` is this caller's own claim: the first send landed, its reply was lost,
/// and the client sent the command again (redis-py does by default). Reading that as someone else's
/// claim would block the key until the claim lapsed, with no handler running.
///
/// A value [`split_value`] refuses holds the key all the same, and nobody can settle it. It reads as
/// `Prior` of the kernel's unreadable record ([`Record::unreadable`]) under the claimant's hash, as an
/// unreadable record half does, so every SDK replays it as a cached `codec` failure. Only a `claim`
/// this kernel never minted is an error.
pub fn claim_outcome(reply: Option<&[u8]>, claim: &[u8]) -> Result<Claimed, Error> {
    match reply {
        None => Ok(Claimed::Won(claim.to_vec())),
        Some(held) if held == claim => Ok(Claimed::Won(claim.to_vec())),
        Some(held) => match split_value(held) {
            Ok((_, record)) => Ok(Claimed::Prior(record.into_bytes())),
            Err(cause) => {
                let (_, claimant_hash) = parse_claim(claim)?;
                Ok(Claimed::Prior(encode_record(&Record::<()>::unreadable(
                    claimant_hash,
                    &cause,
                ))?))
            }
        },
    }
}

/// How many times a forget reads a key that changed under it before it gives up as transient. Each
/// round is one read and one [`FORGET_SCRIPT`](crate::FORGET_SCRIPT), and the key changes only when a
/// record lapses and a new claim takes it, so a few rounds suffice.
pub const FORGET_READS: u32 = 3;

/// The error a forget of `key` gives up with once the key changed under it on each of its
/// [`FORGET_READS`] reads: `store`, so transient. Each change is a record lapsing and a new claim taking
/// the key, which settles, so a later forget succeeds.
pub fn forget_exhausted(key: &str) -> Error {
    Error::store(format_args!(
        "idempotency key {key} changed on every one of {FORGET_READS} forget reads"
    ))
}

/// What forgetting a key does, from the value it holds. `#[must_use]` and exhaustive: every variant
/// needs a decision.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgetOutcome {
    /// The key is free: nothing to forget, send nothing.
    Nothing,
    /// Run [`FORGET_SCRIPT`](crate::FORGET_SCRIPT) with the value read as `ARGV[1]`. It deletes the key
    /// only if the key still holds that value, so a claim that took a lapsed key over in between survives.
    Delete,
}

/// Whether a forget deletes `key`, which held `held` when it was read (`None`: the key was free).
///
/// The record decides, in the kernel's reading order. A pending claim is refused as `in_progress`:
/// its run may still complete, and its record would land after the delete. A completed or failed
/// record is deleted, so a re-driven message runs its handler again. So is a value no claimant can
/// settle, a frame [`split_value`] refuses or a record half no reader decodes, because
/// [`claim_outcome`] replays it as a cached failure: forgetting it is the way out of a poisoned key.
pub fn forget_outcome(key: &str, held: Option<&[u8]>) -> Result<ForgetOutcome, Error> {
    let Some(held) = held else {
        return Ok(ForgetOutcome::Nothing);
    };
    // A frame no claimant can split is no claim: it may go, like a record no reader decodes.
    if let Ok((_, record)) = split_value(held) {
        edax_cqrs::forget(key, record.as_bytes())?;
    }
    Ok(ForgetOutcome::Delete)
}

fn frame(owner: &str, record: &[u8]) -> Vec<u8> {
    [&[LAYOUT_VERSION][..], owner.as_bytes(), record].concat()
}

/// A well-formed claim handle's owner token and the payload hash its pending record carries. See
/// [`check_claim`].
fn parse_claim(claim: &[u8]) -> Result<(&str, u64), Error> {
    let refused = |why: &str| Error::codec(format_args!("claim handle is not a claim this kernel minted: {why}"));
    let (owner, record) = unframe(claim).map_err(|fault| match fault {
        FrameFault::NotUtf8(_) => refused("not UTF-8"),
        FrameFault::Empty | FrameFault::Version(_) => refused("no layout version"),
        FrameFault::Short => refused("too short"),
    })?;
    check_owner(owner)?;
    match decode_record::<()>(record.as_bytes()) {
        Ok(Record {
            payload_hash,
            state: State::Pending,
        }) => Ok((owner, payload_hash)),
        _ => Err(refused("its record is not a pending claim")),
    }
}

/// Reject a token that is not one [`new_claim`] mints: 32 lowercase hex characters. Upper case is
/// refused rather than folded, because the scripts compare bytes and `ABC` is not `abc`. The token
/// itself is kept out of the message: inside a claim value it authorizes settling that claim.
fn check_owner(owner: &str) -> Result<(), Error> {
    if owner.len() != OWNER_LEN {
        return Err(Error::codec(format_args!(
            "owner token is {} bytes, expected {OWNER_LEN}",
            owner.len()
        )));
    }
    if !owner.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(Error::codec("owner token is not lowercase hexadecimal"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real dispatch's payload hash, past 2^53 so a lossy reader is caught.
    /// `crates/edax-py/tests/test_redis.py` uses the same hash on the Python side.
    const HASH: u64 = 11_258_580_006_047_770_801;
    /// The pending record those bytes carry, as the ledger holds it.
    const PENDING: &str =
        r#"{"payload_hash":11258580006047770801,"result":null,"error":null,"error_code":null,"pending":true}"#;
    /// A settled record, as `edax-cqrs` encodes a success.
    const DONE: &str = r#"{"payload_hash":11258580006047770801,"result":{"balance_cents":4200},"error":null,"error_code":null,"pending":false}"#;
    /// A well-formed token, in the shape `new_claim` mints.
    const OWNER: &str = "0190b5c8e1f37c4b9a2d5e6f7a8b9c0d";

    fn claim() -> Vec<u8> {
        format!("1{OWNER}{PENDING}").into_bytes()
    }

    #[test]
    fn a_minted_claim_is_one_the_scripts_can_compare() {
        let claim = new_claim(HASH).unwrap();
        let (owner, record) = split_value(&claim).unwrap();
        assert_eq!(owner.len(), OWNER_LEN);
        assert_eq!(record, PENDING);
        // A minted token must pass the check the writers apply, or no claim could ever be written.
        assert_eq!(claim_value(&owner, HASH), Ok(claim.clone()));
        assert_eq!(check_claim(&claim), Ok(()));
        // The shared counter and the random bits make every claim's owner distinct.
        assert_ne!(owner, split_value(&new_claim(HASH).unwrap()).unwrap().0);
    }

    #[test]
    fn claim_value_branches() {
        let cases: [(&str, &str, Option<&str>); 6] = [
            ("minted_token_is_accepted", OWNER, None),
            (
                "uppercase_hex_is_refused",
                "0190B5C8E1F37C4B9A2D5E6F7A8B9C0D",
                Some("codec: owner token is not lowercase hexadecimal"),
            ),
            (
                "one_character_short",
                "0190b5c8e1f37c4b9a2d5e6f7a8b9c0",
                Some("codec: owner token is 31 bytes, expected 32"),
            ),
            (
                "one_character_long",
                "0190b5c8e1f37c4b9a2d5e6f7a8b9c0de",
                Some("codec: owner token is 33 bytes, expected 32"),
            ),
            ("empty", "", Some("codec: owner token is 0 bytes, expected 32")),
            (
                "non_hex_at_full_width",
                "0190b5c8e1f37c4b9a2d5e6f7a8b9c0z",
                Some("codec: owner token is not lowercase hexadecimal"),
            ),
        ];
        for (name, owner, error) in cases {
            let got = claim_value(owner, HASH);
            match error {
                None => assert_eq!(got, Ok(format!("1{owner}{PENDING}").into_bytes()), "{name}"),
                Some(wire) => assert_eq!(got.unwrap_err().to_string(), wire, "{name}"),
            }
        }
        // The hash is carried whole: a reader that rounded it through a double would lose the low bits.
        let max = String::from_utf8(claim_value(OWNER, u64::MAX).unwrap()).unwrap();
        assert!(max.contains(r#""payload_hash":18446744073709551615"#), "{max}");
    }

    #[test]
    fn settled_value_branches() {
        assert_eq!(
            settled_value(&claim(), DONE),
            Ok(format!("1{OWNER}{DONE}").into_bytes())
        );
        // The record half is passed through untouched, whatever it holds.
        assert_eq!(
            settled_value(&claim(), "not json"),
            Ok(format!("1{OWNER}not json").into_bytes())
        );
        let refused: [(&str, Vec<u8>, &str); 7] = [
            ("empty_handle", Vec::new(), "no layout version"),
            ("not_utf8", vec![b'1', 0xff], "not UTF-8"),
            (
                "unversioned_token",
                format!("{OWNER}{PENDING}").into_bytes(),
                "no layout version",
            ),
            ("truncated_token", b"1019".to_vec(), "too short"),
            (
                "uppercase_token",
                format!("1{}{PENDING}", OWNER.to_uppercase()).into_bytes(),
                "lowercase",
            ),
            (
                "a_settled_value",
                format!("1{OWNER}{DONE}").into_bytes(),
                "not a pending claim",
            ),
            (
                "not_a_record",
                format!("1{OWNER}{{").into_bytes(),
                "not a pending claim",
            ),
        ];
        for (name, handle, why) in refused {
            let err = settled_value(&handle, DONE).unwrap_err();
            assert_eq!(err.code, edax_core::ErrorCode::Codec, "{name}");
            assert!(err.message.contains(why), "{name}: {err}");
            assert!(
                !err.message.contains(OWNER),
                "{name}: the token leaked into the message"
            );
            assert_eq!(check_claim(&handle).unwrap_err(), err, "{name}: one check");
        }
        assert_eq!(check_claim(&claim()), Ok(()));
    }

    #[test]
    fn settled_value_refuses_a_pending_record() {
        let refused = settled_value(&claim(), PENDING).unwrap_err();
        assert_eq!(
            refused,
            edax_cqrs::check_settled(PENDING.as_bytes()).unwrap_err(),
            "the record kernel's refusal"
        );
        assert_eq!(refused.code, edax_core::ErrorCode::BadHandle);
    }

    #[test]
    fn complete_outcome_branches() {
        assert_eq!(complete_outcome("order-7", true), Ok(()), "written");
        assert_eq!(
            complete_outcome("order-7", false),
            Err(edax_cqrs::lapsed_claim("order-7")),
            "the fence refused it"
        );
    }

    #[test]
    fn heartbeat_due_branches() {
        for (name, sent_at, claim_ttl, beat, want) in [
            ("the_claim_itself", 1_000, 6_000, 0, 1_000),
            ("the_first_beat_is_a_third_on", 1_000, 6_000, 1, 3_000),
            ("the_third_beat_is_a_whole_ttl_on", 1_000, 6_000, 3, 7_000),
            ("a_ttl_not_divisible_by_three_rounds_down", 0, 10, 1, 3),
            ("the_second_beat_of_it_rounds_down_too", 0, 10, 2, 6),
            ("saturates_at_the_top", i64::MAX - 1, MAX_TTL_MS, u32::MAX, i64::MAX),
            ("saturates_at_the_bottom", i64::MIN, -MAX_TTL_MS, u32::MAX, i64::MIN),
        ] {
            assert_eq!(heartbeat_due_ms(sent_at, claim_ttl, beat), want, "{name}");
        }
    }

    #[test]
    fn split_value_branches() {
        assert_eq!(split_value(&claim()), Ok((OWNER.to_owned(), PENDING.to_owned())));
        let refused: [(&str, Vec<u8>, &str); 5] = [
            ("empty", Vec::new(), "codec: stored value is empty"),
            (
                "unversioned",
                format!("{OWNER}{PENDING}").into_bytes(),
                "codec: stored value has layout version '0'; this kernel reads version '1'",
            ),
            (
                "short",
                b"1abc".to_vec(),
                "codec: stored value has no 32-character owner token",
            ),
            ("not_utf8", vec![b'1', 0xff], "codec: stored value is not UTF-8"),
            (
                "straddling",
                format!("1{}é", "0".repeat(31)).into_bytes(),
                "codec: stored value has no 32-character owner token",
            ),
        ];
        for (name, raw, wire) in refused {
            let got = split_value(&raw).unwrap_err().to_string();
            assert!(got.starts_with(wire), "{name}: {got}");
        }
    }

    #[test]
    fn claim_outcome_branches() {
        let other = format!("1{}{PENDING}", "f".repeat(32)).into_bytes();
        let settled = format!("1{OWNER}{DONE}").into_bytes();
        assert_eq!(claim_outcome(None, &claim()), Ok(Claimed::Won(claim())), "free key");
        assert_eq!(
            claim_outcome(Some(&claim()), &claim()),
            Ok(Claimed::Won(claim())),
            "own claim, reply lost"
        );
        assert_eq!(
            claim_outcome(Some(&other), &claim()),
            Ok(Claimed::Prior(PENDING.as_bytes().to_vec()))
        );
        assert_eq!(
            claim_outcome(Some(&settled), &claim()),
            Ok(Claimed::Prior(DONE.as_bytes().to_vec()))
        );
        // A value no layout reads still holds the key: the claimant reads the kernel's unreadable record
        // under its own hash, a cached codec failure.
        let unreadable = |cause: &str| {
            let record = Record::<()>::unreadable(HASH, &Error::codec(cause));
            Ok(Claimed::Prior(encode_record(&record).unwrap()))
        };
        assert_eq!(
            claim_outcome(Some(b"garbage"), &claim()),
            unreadable("stored value has layout version 'g'; this kernel reads version '1'")
        );
        assert_eq!(
            claim_outcome(Some(&[b'1', 0xff]), &claim()),
            unreadable("stored value is not UTF-8: invalid utf-8 sequence of 1 bytes from index 1")
        );
        assert_eq!(claim_outcome(Some(b""), &claim()), unreadable("stored value is empty"));
        // The claimant's own handle is the one thing that must be well formed.
        let refused = claim_outcome(Some(b"garbage"), b"not a claim").unwrap_err();
        assert_eq!(
            refused.to_string(),
            "codec: claim handle is not a claim this kernel minted: no layout version"
        );
    }

    #[test]
    fn key_and_settings_branches() {
        assert_eq!(ledger_key("idem:", "order-7"), Ok(b"idem:order-7".to_vec()));
        assert_eq!(ledger_key("idem:", ""), Ok(b"idem:".to_vec()));
        assert_eq!(
            ledger_key("", "order-7").unwrap_err().code,
            edax_core::ErrorCode::BadHandle
        );
        // One row per refusal of `check_config`, and the edges of the TTL range.
        let refused = |prefix, ttl, claim_ttl, deadline, heartbeat| {
            check_config(prefix, ttl, claim_ttl, deadline, heartbeat)
                .err()
                .map(|e| (e.code, e.message))
        };
        let bad = |message: &str| Some((edax_core::ErrorCode::BadHandle, message.to_owned()));
        assert_eq!(refused("x", 1, MAX_TTL_MS, 1, false), None);
        assert_eq!(
            refused("x", DEFAULT_TTL_MS, DEFAULT_CLAIM_TTL_MS, DEFAULT_OP_TIMEOUT_MS, true),
            None,
            "a heartbeat at the defaults"
        );
        assert_eq!(
            refused("x", 1, 4, 1, true),
            None,
            "a deadline just under a third of the claim TTL"
        );
        assert_eq!(
            refused("", 1, 1, 1, false),
            bad("the ledger prefix is empty; name the service and the environment, as in \"billing:prod:idem:\"")
        );
        for ttl in [0, -5, MAX_TTL_MS + 1, i64::MIN] {
            let want =
                format!("ttl_ms is {ttl} ms; a Redis TTL is a whole number of milliseconds from 1 to {MAX_TTL_MS}");
            assert_eq!(refused("x", ttl, 1, 1, false), bad(&want), "{ttl}");
        }
        assert_eq!(
            refused("x", 1, 0, 1, false),
            bad(&format!(
                "claim_ttl_ms is 0 ms; a Redis TTL is a whole number of milliseconds from 1 to {MAX_TTL_MS}"
            ))
        );
        assert_eq!(
            refused("x", 1, 1, 0, false),
            bad("op_timeout_ms is 0 ms; a deadline must be positive")
        );
        let stalled = |deadline: i64, claim_ttl: i64| {
            bad(&format!("op_timeout_ms is {deadline} ms and claim_ttl_ms is {claim_ttl} ms; a heartbeat renews every 1/3 of the claim TTL, so one renewal stalled for the whole deadline lets the claim lapse unless op_timeout_ms is under 1/3 of claim_ttl_ms"))
        };
        assert_eq!(
            refused("x", 1, 15_000, 5_000, true),
            stalled(5_000, 15_000),
            "a deadline of exactly a third"
        );
        assert_eq!(
            refused("x", 1, 2, 1, true),
            stalled(1, 2),
            "a claim TTL too short to divide"
        );
        assert_eq!(
            refused("x", 1, MAX_TTL_MS, i64::MAX, true),
            stalled(i64::MAX, MAX_TTL_MS),
            "saturates"
        );
        assert_eq!(refused("x", 1, 15_000, 5_000, false), None, "no heartbeat, no bound");
    }
}

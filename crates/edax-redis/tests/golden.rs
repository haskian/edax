//! Golden vectors for the Redis ledger kernel: the cross-language consistency contract.
//!
//! These cases are derived from edax-redis and written to `testdata/golden/*.json`. Every binding's
//! test suite reads the same files and must produce byte-identical results. One drift in any
//! binding fails its test.
//!
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax-redis --test golden`.
//! Without the env var this test ASSERTS the committed files still match the code.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a golden generator runs only as a test, and its panic is the failure report"
)]

use edax_core::ErrorCode;
use edax_cqrs::{encode_record, Record};
use edax_golden::{hex, write_or_check};
use edax_redis::{
    check_claim, check_config, claim_outcome, claim_value, ledger_key, reply_error, settled_value, split_value,
    ClaimOutcome, CLAIM_SCRIPT, COMPLETE_SCRIPT, EXTEND_SCRIPT, MAX_TTL_MS, RELEASE_SCRIPT,
};
use serde_json::{json, Value};

/// A real dispatch's payload hash, past 2^53 so a reader that rounds through a double is caught.
/// `crates/edax-py/tests/test_redis.py` pins the same one.
const HASH: u64 = 11_258_580_006_047_770_801;
/// The owner token of that same Python fixture: a value a reader can eyeball against the record.
const ZEROS: &str = "00000000000000000000000000000000";
/// A token in the shape `new_owner` mints, so the vector shows a realistic value too.
const MINTED: &str = "0190b5c8e1f37c4b9a2d5e6f7a8b9c0d";

/// A settled record's JSON, encoded by the kernel that owns the shape.
fn encoded(record: &Record<Value>) -> String {
    String::from_utf8(encode_record(record).expect("a JSON value always encodes")).expect("serde_json emits UTF-8")
}

fn text(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).expect("the kernel writes UTF-8 values")
}

fn claim_of(owner: &str) -> String {
    text(claim_value(owner, HASH).expect("a well-formed owner"))
}

fn settled_of(claim: &str) -> String {
    let done = encoded(&Record::done(
        HASH,
        json!({"account_id": "acct-1", "balance_cents": 4200}),
    ));
    text(settled_value(claim.as_bytes(), &done).expect("a well-formed claim"))
}

/// `claim_value`: the value a winning claimant writes, which is also its handle.
fn claim_cases() -> Vec<Value> {
    [
        // The value `crates/edax-py/tests/test_redis.py` holds as `CLAIM`.
        ("pending_claim", ZEROS, HASH),
        ("minted_owner_token", MINTED, HASH),
        // u64::MAX and 0 are the ends of the hash range; a binding that reads the hash as a signed
        // or floating-point number fails one of them.
        ("hash_at_u64_max", MINTED, u64::MAX),
        ("hash_zero", MINTED, 0),
    ]
    .into_iter()
    .map(|(name, owner, payload_hash)| {
        json!({
            "name": name,
            "op": "claim",
            "owner": owner,
            "payload_hash": payload_hash,
            "value": text(claim_value(owner, payload_hash).expect("a well-formed owner")),
        })
    })
    .collect()
}

/// `settled_value`: the value a claimant writes over its own claim. The record half is the caller's
/// bytes, passed through, so these cases pin the framing and not the record's shape.
fn settled_cases() -> Vec<Value> {
    let cases: [(&str, String); 5] = [
        (
            "cached_success",
            encoded(&Record::done(
                HASH,
                json!({"account_id": "acct-1", "balance_cents": 4200}),
            )),
        ),
        // A stored `null` result is a cached success, not "no result": the record kernel's rule,
        // carried through the ledger unchanged.
        (
            "cached_success_with_null_result",
            encoded(&Record::done(HASH, Value::Null)),
        ),
        (
            "cached_failure",
            encoded(&Record::failed(HASH, ErrorCode::Codec, "unknown field `amount`")),
        ),
        // Multibyte UTF-8 in the record half: the value is a string, and the byte count is not the
        // character count.
        (
            "non_ascii_result",
            encoded(&Record::done(HASH, json!({"note": "café ☕ 🦀"}))),
        ),
        // The degenerate frame: a version, a token and nothing after it. It must still be a legal value.
        ("empty_record", String::new()),
    ];
    let claim = claim_of(MINTED);
    cases
        .into_iter()
        .map(|(name, record_json)| {
            json!({
                "name": name,
                "op": "settled",
                "claim": claim,
                "record_json": record_json,
                "value": text(settled_value(claim.as_bytes(), &record_json).expect("a well-formed claim")),
            })
        })
        .collect()
}

/// `split_value`: the same values read back, plus the frames no caller could settle. `raw_hex` is
/// the stored bytes, because two of these cases are not text at all.
fn split_cases() -> Vec<Value> {
    let cases: [(&str, Vec<u8>); 10] = [
        ("claim_value_round_trips", claim_of(ZEROS).into_bytes()),
        ("settled_value_round_trips", settled_of(&claim_of(MINTED)).into_bytes()),
        ("owner_only_leaves_an_empty_record", format!("1{ZEROS}").into_bytes()),
        // Liberal on read: a token this kernel would not mint still reads back, so the record behind
        // it is never stranded.
        (
            "non_hex_owner_is_accepted_on_read",
            format!("1{}{{}}", "z".repeat(32)).into_bytes(),
        ),
        // The unversioned layout of edax 0.1: refused by name rather than split one byte off.
        ("unversioned_value_is_refused", format!("{MINTED}{{}}").into_bytes()),
        (
            "another_layout_version_is_refused",
            format!("2{MINTED}{{}}").into_bytes(),
        ),
        ("one_byte_short_of_a_token", format!("1{}", &ZEROS[..31]).into_bytes()),
        ("empty", Vec::new()),
        ("not_utf8", [&b"1"[..], &[0xff], &b"0".repeat(31)[..]].concat()),
        // Long enough in bytes, but a two-byte character straddles the token's width, so there is
        // no owner token to take: byte length alone does not make a value splittable.
        (
            "multibyte_straddles_the_token_width",
            format!("1{}é{{}}", "0".repeat(31)).into_bytes(),
        ),
    ];
    cases
        .into_iter()
        .map(|(name, raw)| {
            let want = match split_value(&raw) {
                Ok((owner, record)) => json!({ "owner": owner, "record": record }),
                Err(error) => json!({ "error": error.to_string() }),
            };
            json!({ "name": name, "op": "split", "raw_hex": hex(&raw), "want": want })
        })
        .collect()
}

fn outcome_json(got: Result<impl Into<Value>, edax_core::Error>) -> Value {
    match got {
        Ok(value) => json!({ "ok": value.into() }),
        Err(error) => json!({ "error": error.to_string() }),
    }
}

/// `ledger_key`: the key every SDK must compose identically, or two SDKs never meet on one ledger.
fn key_cases() -> Vec<Value> {
    [
        ("service_and_environment", "billing:prod:idem:", "order-7"),
        ("empty_key", "idem:", ""),
        ("non_ascii_key", "idem:", "naročilo-7 ☕"),
        // No separator is added: the prefix carries its own, so `idem` + `order-7` is `idemorder-7`.
        ("prefix_without_a_separator", "idem", "order-7"),
        // A hash tag in the prefix is composed as given, and pins every key to one cluster slot.
        ("hash_tag_prefix", "{idem}:", "order-7"),
        ("empty_prefix_is_refused", "", "order-7"),
    ]
    .into_iter()
    .map(|(name, prefix, key)| {
        let want = outcome_json(ledger_key(prefix, key).map(|bytes| hex(&bytes)));
        json!({ "name": name, "op": "key", "prefix": prefix, "key": key, "want": want })
    })
    .collect()
}

/// `check_config`: the settings a store refuses when it is built, one row per refusal and the edges
/// of the TTL range.
fn config_cases() -> Vec<Value> {
    [
        ("accepted", "idem:", 86_400_000, 300_000, 5_000, false),
        ("the_largest_exact_integer", "idem:", MAX_TTL_MS, MAX_TTL_MS, 1, false),
        ("a_heartbeat_over_three_milliseconds", "idem:", 1, 3, 1, true),
        ("an_empty_prefix_is_refused", "", 86_400_000, 300_000, 5_000, false),
        ("a_zero_ttl_is_refused", "idem:", 0, 300_000, 5_000, false),
        ("a_negative_claim_ttl_is_refused", "idem:", 86_400_000, -5, 5_000, false),
        ("past_the_largest_exact_integer_is_refused", "idem:", MAX_TTL_MS + 1, 300_000, 5_000, false),
        ("a_zero_deadline_is_refused", "idem:", 86_400_000, 300_000, 0, false),
        ("a_heartbeat_over_two_milliseconds_is_refused", "idem:", 86_400_000, 2, 5_000, true),
    ]
    .into_iter()
    .map(|(name, prefix, ttl_ms, claim_ttl_ms, op_timeout_ms, heartbeat)| {
        let want = outcome_json(check_config(prefix, ttl_ms, claim_ttl_ms, op_timeout_ms, heartbeat).map(|()| Value::Null));
        json!({
            "name": name, "op": "config", "prefix": prefix, "ttl_ms": ttl_ms, "claim_ttl_ms": claim_ttl_ms,
            "op_timeout_ms": op_timeout_ms, "heartbeat": heartbeat, "want": want,
        })
    })
    .collect()
}

/// `check_claim`: the claim handles `settled_value` and the release and extend paths accept.
fn handle_cases() -> Vec<Value> {
    let claim = claim_of(MINTED);
    let cases: [(&str, Vec<u8>); 6] = [
        ("a_minted_claim", claim.clone().into_bytes()),
        ("empty", Vec::new()),
        ("a_bare_owner_token", MINTED.as_bytes().to_vec()),
        ("a_truncated_claim", claim.as_bytes()[..3].to_vec()),
        (
            "an_uppercase_owner_token",
            claim_of(MINTED)
                .replacen(MINTED, &MINTED.to_uppercase(), 1)
                .into_bytes(),
        ),
        ("a_settled_value", settled_of(&claim).into_bytes()),
    ];
    cases
        .into_iter()
        .map(|(name, handle)| {
            let want = outcome_json(check_claim(&handle).map(|()| Value::Null));
            json!({ "name": name, "op": "handle", "handle_hex": hex(&handle), "want": want })
        })
        .collect()
}

/// `claim_outcome`: what the claim script's reply means.
fn outcome_cases() -> Vec<Value> {
    let claim = claim_of(MINTED);
    let other = claim_of(ZEROS);
    let cases: [(&str, Option<Vec<u8>>); 5] = [
        ("free_key_is_won", None),
        // The first send landed and its reply was lost; the client sent the script again.
        ("own_claim_resent_is_won", Some(claim.clone().into_bytes())),
        ("another_claim_is_prior", Some(other.into_bytes())),
        (
            "a_settled_record_is_prior",
            Some(settled_of(&claim_of(ZEROS)).into_bytes()),
        ),
        ("an_unreadable_value_replays_a_cached_codec_failure", Some(b"{not a ledger value".to_vec())),
    ];
    cases
        .into_iter()
        .map(|(name, reply)| {
            let want = outcome_json(
                claim_outcome(reply.as_deref(), claim.as_bytes()).map(|outcome| match outcome {
                    ClaimOutcome::Won => json!({ "won": true }),
                    ClaimOutcome::Prior(record) => json!({ "prior": record }),
                }),
            );
            json!({
                "name": name,
                "op": "outcome",
                "reply_hex": reply.as_deref().map(hex),
                "claim": claim,
                "want": want,
            })
        })
        .collect()
}

/// `reply_error`: the verdict on a Redis error reply.
fn reply_cases() -> Vec<Value> {
    [
        (
            "wrong_type",
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
        ("invalid_expire_time", "ERR invalid expire time in 'set' command"),
        (
            "no_permission",
            "NOPERM User idem has no permissions to run the 'evalsha' command",
        ),
        (
            "wrong_password",
            "WRONGPASS invalid username-password pair or user is disabled.",
        ),
        (
            "read_only_replica_during_failover",
            "READONLY You can't write against a read only replica.",
        ),
        ("loading", "LOADING Redis is loading the dataset in memory"),
        ("busy_script", "BUSY Redis is busy running a script."),
        ("unknown_prefix", "SOMEDAY a reply this kernel has never seen"),
    ]
    .into_iter()
    .map(
        |(name, reply)| json!({ "name": name, "op": "reply", "reply": reply, "error": reply_error(reply).to_string() }),
    )
    .collect()
}

/// The Lua bodies, byte for byte. A client registers the source once and sends `EVALSHA` after, so
/// two SDKs that reformat the script differently cache two digests for one fence.
fn script_cases() -> Vec<Value> {
    [
        ("claim", CLAIM_SCRIPT),
        ("complete", COMPLETE_SCRIPT),
        ("release", RELEASE_SCRIPT),
        ("extend", EXTEND_SCRIPT),
    ]
    .into_iter()
    .map(|(name, body)| json!({ "name": name, "body": body }))
    .collect()
}

/// The TTL a fence case's key holds before the script runs.
const INITIAL_TTL: u64 = 50_000;
/// The claim TTL a fence case passes.
const CLAIM_TTL: u64 = 30_000;
/// The record TTL a fence case passes.
const RECORD_TTL: u64 = 90_000;

/// What each script does, as a table a test replays against a real server: the key's value before,
/// the script and its `ARGV`, then the reply and the key's value and TTL after. The expectations
/// are written out by hand, not computed: they are the specification the Lua must meet, and no
/// Rust code here re-implements the scripts. Each row runs on one key, `KEYS[1]`.
fn fence_cases() -> Vec<Value> {
    let claim = claim_of(MINTED);
    let other = claim_of(ZEROS);
    let settled = settled_of(&claim);
    let other_settled = settled_of(&other);
    let stub = claim[..3].to_owned(); // a prefix of the claim: the old fence matched it
    let complete = |handle: &str| json!([handle, settled, RECORD_TTL]);
    let none = Value::Null;

    // (name, initial value, script, args, reply, value after, TTL after)
    let rows: Vec<(&str, Value, &str, Value, Value, Value, Value)> = vec![
        (
            "claim_a_free_key",
            none.clone(),
            "claim",
            json!([claim, CLAIM_TTL]),
            none.clone(),
            json!(claim),
            json!(CLAIM_TTL),
        ),
        // The first send landed and its reply was lost: the re-send reads the caller's own claim.
        (
            "claim_resent_after_a_lost_reply",
            json!(claim),
            "claim",
            json!([claim, CLAIM_TTL]),
            json!(claim),
            json!(claim),
            json!(INITIAL_TTL),
        ),
        (
            "claim_a_key_another_holds",
            json!(other),
            "claim",
            json!([claim, CLAIM_TTL]),
            json!(other),
            json!(other),
            json!(INITIAL_TTL),
        ),
        (
            "claim_a_settled_key",
            json!(settled),
            "claim",
            json!([claim, CLAIM_TTL]),
            json!(settled),
            json!(settled),
            json!(INITIAL_TTL),
        ),
        (
            "complete_the_held_claim",
            json!(claim),
            "complete",
            complete(&claim),
            json!(1),
            json!(settled),
            json!(RECORD_TTL),
        ),
        // The first complete landed and its reply was lost: the re-send settles on the same value.
        (
            "complete_resent_after_it_landed",
            json!(settled),
            "complete",
            complete(&claim),
            json!(1),
            json!(settled),
            json!(RECORD_TTL),
        ),
        (
            "complete_after_the_claim_lapsed",
            none.clone(),
            "complete",
            complete(&claim),
            json!(0),
            none.clone(),
            none.clone(),
        ),
        (
            "complete_after_a_takeover",
            json!(other),
            "complete",
            complete(&claim),
            json!(0),
            json!(other),
            json!(INITIAL_TTL),
        ),
        (
            "complete_over_another_settled_record",
            json!(other_settled),
            "complete",
            complete(&claim),
            json!(0),
            json!(other_settled),
            json!(INITIAL_TTL),
        ),
        // An empty handle on a free key: the old prefix fence wrote a ghost key here.
        (
            "complete_with_an_empty_handle",
            none.clone(),
            "complete",
            complete(""),
            json!(0),
            none.clone(),
            none.clone(),
        ),
        (
            "complete_with_a_prefix_of_the_claim",
            json!(claim),
            "complete",
            complete(&stub),
            json!(0),
            json!(claim),
            json!(INITIAL_TTL),
        ),
        (
            "release_the_held_claim",
            json!(claim),
            "release",
            json!([claim]),
            json!(1),
            none.clone(),
            none.clone(),
        ),
        // The complete landed and its reply was lost, so the guard releases: the record of committed
        // work must stay.
        (
            "release_after_complete_landed",
            json!(settled),
            "release",
            json!([claim]),
            json!(0),
            json!(settled),
            json!(INITIAL_TTL),
        ),
        (
            "release_after_a_takeover",
            json!(other),
            "release",
            json!([claim]),
            json!(0),
            json!(other),
            json!(INITIAL_TTL),
        ),
        (
            "release_a_free_key",
            none.clone(),
            "release",
            json!([claim]),
            json!(0),
            none.clone(),
            none.clone(),
        ),
        (
            "release_with_an_empty_handle",
            json!(claim),
            "release",
            json!([""]),
            json!(0),
            json!(claim),
            json!(INITIAL_TTL),
        ),
        (
            "release_with_a_prefix_of_the_claim",
            json!(claim),
            "release",
            json!([stub]),
            json!(0),
            json!(claim),
            json!(INITIAL_TTL),
        ),
        (
            "extend_the_held_claim",
            json!(claim),
            "extend",
            json!([claim, CLAIM_TTL]),
            json!(1),
            json!(claim),
            json!(CLAIM_TTL),
        ),
        (
            "extend_by_a_stale_owner",
            json!(other),
            "extend",
            json!([claim, CLAIM_TTL]),
            json!(0),
            json!(other),
            json!(INITIAL_TTL),
        ),
        (
            "extend_a_settled_record",
            json!(settled),
            "extend",
            json!([claim, CLAIM_TTL]),
            json!(0),
            json!(settled),
            json!(INITIAL_TTL),
        ),
        (
            "extend_a_free_key",
            none.clone(),
            "extend",
            json!([claim, CLAIM_TTL]),
            json!(0),
            none.clone(),
            none,
        ),
    ];
    rows.into_iter()
        .map(|(name, initial, script, args, reply, value, ttl)| {
            json!({
                "name": name,
                "initial": initial,
                "initial_ttl_ms": INITIAL_TTL,
                "script": script,
                "args": args,
                "want_reply": reply,
                "want_value": value,
                "want_ttl_ms": ttl,
            })
        })
        .collect()
}

#[test]
fn golden() {
    let mut values = claim_cases();
    values.extend(settled_cases());
    values.extend(split_cases());
    write_or_check("redis_value.json", values);

    let mut ledger = key_cases();
    ledger.extend(config_cases());
    ledger.extend(handle_cases());
    ledger.extend(outcome_cases());
    ledger.extend(reply_cases());
    write_or_check("redis_ledger.json", ledger);

    write_or_check("redis_scripts.json", script_cases());
    write_or_check("redis_fence.json", fence_cases());
}

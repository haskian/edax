//! Golden vectors for the Redis ledger kernel: the cross-language consistency contract.
//!
//! These cases are derived from edax-redis and written to `testdata/golden/*.json`. Every binding's
//! test suite reads the same files and must produce byte-identical results. One drift in any
//! binding fails its test.
//!
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax-redis --test golden`.
//! Without the env var this test ASSERTS the committed files still match the code.

use edax_cqrs::{encode_record, Record};
use edax_golden::{hex, write_or_check};
use edax_redis::{claim_value, record_value, split_value, FENCED_DEL_SCRIPT, FENCED_SET_SCRIPT};
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

/// `claim_value`: the value a winning claimant writes with `SET NX PX`.
fn claim_cases() -> Vec<Value> {
    [
        // The value `crates/edax-py/tests/test_redis.py` holds as `RECORD_BYTES`.
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
            "value": claim_value(owner, payload_hash).expect("a well-formed owner"),
        })
    })
    .collect()
}

/// `record_value`: the value a settling claimant writes over its own claim. The record half is the
/// caller's bytes, passed through, so these cases pin the framing and not the record's shape.
fn record_cases() -> Vec<Value> {
    let cases: [(&str, &str, String); 5] = [
        (
            "cached_success",
            MINTED,
            encoded(&Record::done(HASH, json!({"account_id": "acct-1", "balance_cents": 4200}))),
        ),
        // A stored `null` result is a cached success, not "no result": the record kernel's rule,
        // carried through the ledger unchanged.
        ("cached_success_with_null_result", MINTED, encoded(&Record::done(HASH, Value::Null))),
        ("cached_failure", MINTED, encoded(&Record::failed(HASH, "codec: bad payload"))),
        // Multibyte UTF-8 in the record half: the value is a string, and the byte count is not the
        // character count.
        ("non_ascii_result", MINTED, encoded(&Record::done(HASH, json!({"note": "café ☕ 🦀"})))),
        // The degenerate frame: a token and nothing after it. It must still be a legal value.
        ("empty_record", ZEROS, String::new()),
    ];
    cases
        .into_iter()
        .map(|(name, owner, record_json)| {
            json!({
                "name": name,
                "op": "record",
                "owner": owner,
                "record_json": record_json,
                "value": record_value(owner, &record_json).expect("a well-formed owner"),
            })
        })
        .collect()
}

/// `split_value`: the same values read back, plus the frames no caller could settle. `raw_hex` is
/// the stored bytes, because two of these cases are not text at all.
fn split_cases() -> Vec<Value> {
    let claim = claim_value(ZEROS, HASH).expect("a well-formed owner");
    let settled = record_value(MINTED, &encoded(&Record::done(HASH, json!({"balance_cents": 4200}))))
        .expect("a well-formed owner");
    let cases: [(&str, Vec<u8>); 8] = [
        ("claim_value_round_trips", claim.into_bytes()),
        ("record_value_round_trips", settled.into_bytes()),
        ("owner_only_leaves_an_empty_record", ZEROS.as_bytes().to_vec()),
        // Liberal on read: a token this kernel would not mint still reads back, so a ledger written
        // by an older store is never stranded.
        ("non_hex_owner_is_accepted_on_read", b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz{}".to_vec()),
        ("one_byte_short_of_a_token", ZEROS.as_bytes()[..31].to_vec()),
        ("empty", Vec::new()),
        ("not_utf8", [&b"0"[..], &[0xff], &b"0".repeat(30)[..]].concat()),
        // Long enough in bytes, but a two-byte character straddles the token's width, so there is
        // no owner token to take: byte length alone does not make a value splittable.
        ("multibyte_straddles_the_token_width", format!("{}é{{}}", "0".repeat(31)).into_bytes()),
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

/// The Lua bodies, byte for byte. A client registers the source once and sends `EVALSHA` after, so
/// two SDKs that reformat the script differently cache two digests for one fence.
fn script_cases() -> Vec<Value> {
    [("fenced_set", FENCED_SET_SCRIPT), ("fenced_del", FENCED_DEL_SCRIPT)]
        .into_iter()
        .map(|(name, body)| json!({ "name": name, "body": body }))
        .collect()
}

#[test]
fn golden() {
    let mut values = claim_cases();
    values.extend(record_cases());
    values.extend(split_cases());
    write_or_check("redis_value.json", values);
    write_or_check("redis_scripts.json", script_cases());
}

//! Golden vectors for the command-side kernel. Every binding's test suite reads the same files.
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax-cqrs --test golden`.

use edax_cqrs::{
    canonical_json, command_hash, decide, decode_record, encode_record, outcome, payload_hash, Record, State,
};
use edax_golden::write_or_check;
use serde_json::{json, Value};

fn payload_hash_cases() -> Vec<Value> {
    // Last case hashes multibyte UTF-8 bytes, pinning agreement on non-ASCII input.
    ["", "a", "foobar", r#"{"email":"a@b.c"}"#, "user.create", "café ☕ 🦀"]
        .iter()
        .map(|s| json!({ "input": s, "expected": payload_hash(s.as_bytes()) }))
        .collect()
}

fn canonical_payload_hash_cases() -> Vec<Value> {
    // payload_hash_json canonicalizes (in the kernel) before hashing, so every binding agrees on the
    // hash regardless of key order, whitespace, or non-ASCII encoding. The "café" case is exactly the
    // one a host-side json.dumps(ensure_ascii=True) would escape to é and hash differently.
    // `{"n":1}` vs `{"n":1.0}` hash *differently* — int and float are distinct canonical forms, so a
    // binding must not coerce one to the other. The nested-object case pins recursive key sorting.
    [
        r#"{"a":1,"b":2}"#,
        r#"{ "b": 2, "a": 1 }"#,
        r#"{"name":"café"}"#,
        r#"{"n":1.0}"#,
        r#"{"n":1}"#,
        r#"{"k":"v","items":[3,1,2]}"#,
        r#"{"outer":{"z":1,"a":2}}"#,
        r#"{}"#,
    ]
    .iter()
    .map(|s| {
        let canon = canonical_json(s.as_bytes()).unwrap();
        json!({ "input": s, "expected": payload_hash(&canon) })
    })
    .collect()
}

fn command_hash_cases() -> Vec<Value> {
    // The hash *input* rule: the whole command value, canonicalized. Each case is a command as its
    // field map, and both SDK suites rebuild it as their own command type (a serde struct, a
    // dataclass), dispatch it through their real `idempotent` middleware, and compare the hash the
    // store received. `type` is routing metadata: it names the handler and is not a field.
    //
    // The base fixture carries `request_id`, the field the idempotency key is read from. Under the
    // old per-SDK rule a Rust `payload()` that returned the business body without it hashed
    // differently from Python's whole-object encoding; the first case is that command.
    let base = json!({
        "order_id": "ord-7f3c",
        "customer": "Ada Lovelace",
        "total_cents": 4200,
        "currency": "USD",
        "weight_kg": 2.5,
        "reference": 42,
        "note": "leave at the door",
        "tags": ["eu", "paid"],
        "shipping": { "city": "NYC", "zip": "10001" },
        "request_id": "req-7f3c-1",
    });
    let with = |overrides: Value| {
        let mut command = base.clone();
        for (field, value) in overrides.as_object().expect("overrides are an object") {
            command[field] = value.clone();
        }
        command
    };
    let cases: [(&str, Value); 7] = [
        ("whole_command_including_the_idempotency_key_field", base.clone()),
        // Array order is data, so swapping two tags is a different command.
        ("array_order_is_data", with(json!({ "tags": ["paid", "eu"] }))),
        // A whole-number float is still a float: `1.0` must not collapse to `1` in either SDK.
        ("whole_number_float_stays_a_float", with(json!({ "weight_kg": 1.0 }))),
        // Above i64::MAX and past 2^53: an SDK that rounds through a float or a signed int fails.
        ("u64_above_i64_max_stays_an_integer", with(json!({ "reference": 9_223_372_036_854_775_809u64 }))),
        // An optional field that is unset is serialized as null, not omitted.
        ("unset_optional_is_null_not_omitted", with(json!({ "note": null }))),
        // Hashed as UTF-8 bytes: an encoder that escapes to \u00e9 would diverge.
        ("non_ascii_hashes_as_utf8", with(json!({ "customer": "café ☕ 🦀" }))),
        ("empty_array_and_null_nested_field", with(json!({ "tags": [], "shipping": { "city": "NYC", "zip": null } }))),
    ];
    cases
        .into_iter()
        .map(|(name, command)| {
            let expected = command_hash(&command).expect("a field map always hashes");
            json!({ "name": name, "type": "order.place", "command": command, "expected": expected })
        })
        .collect()
}

fn idempotency_cases() -> Vec<Value> {
    let a = payload_hash(b"cmd-a");
    let b = payload_hash(b"cmd-b");
    // (Some(0), a): a stored hash of 0 is a real prior, not "absent" — still a mismatch against a.
    let cases: [(Option<u64>, u64); 6] =
        [(None, a), (Some(a), a), (Some(b), a), (None, 0), (Some(0), 0), (Some(0), a)];
    cases
        .into_iter()
        .map(|(prior, current)| {
            json!({ "prior": prior, "current": current, "expected": decide(prior, current).as_str() })
        })
        .collect()
}

fn idempotency_record_cases() -> Vec<Value> {
    // The record's wire shape and reading order are the cross-SDK contract, and the case this vector
    // exists to settle: a stored `null` result is a cached *success* (Python `None`, Rust `()`), not
    // "no result". `hash` is past 2^53, so a reader that rounds through a float fails the vector.
    let hash = payload_hash(b"cmd-a");
    let other = payload_hash(b"cmd-b");
    let stored = |record: &Record<Value>| String::from_utf8(encode_record(record).unwrap()).unwrap();
    struct Case {
        name: &'static str,
        stored: String,
        current: u64,
    }
    let cases = [
        Case { name: "pending_claim", stored: stored(&Record::pending(hash)), current: hash },
        Case {
            name: "cached_success_with_value",
            stored: stored(&Record::done(hash, json!({"invoice_id": "invoice-7", "total": 5000}))),
            current: hash,
        },
        Case {
            name: "cached_success_with_null_result",
            stored: stored(&Record::done(hash, Value::Null)),
            current: hash,
        },
        Case { name: "cached_failure", stored: stored(&Record::failed(hash, "codec: bad payload")), current: hash },
        // Records from a writer that predates `pending` (or omits `result`): still readable.
        Case {
            name: "older_writer_without_pending_field_success",
            stored: format!(r#"{{"payload_hash":{hash},"result":42,"error":null}}"#),
            current: hash,
        },
        Case {
            name: "older_writer_without_pending_field_failure",
            stored: format!(r#"{{"payload_hash":{hash},"result":null,"error":"boom"}}"#),
            current: hash,
        },
        Case {
            name: "absent_result_reads_as_null",
            stored: format!(r#"{{"payload_hash":{hash},"pending":false}}"#),
            current: hash,
        },
        // Precedence when a foreign writer over-specifies: pending, then error, then result.
        Case {
            name: "pending_wins_over_a_stale_error",
            stored: format!(r#"{{"payload_hash":{hash},"result":null,"error":"boom","pending":true}}"#),
            current: hash,
        },
        Case {
            name: "error_wins_over_a_result",
            stored: format!(r#"{{"payload_hash":{hash},"result":42,"error":"boom","pending":false}}"#),
            current: hash,
        },
        // The hash is checked before the state: a mismatch is rejected whatever the winner did —
        // in flight, already failed, or already succeeded.
        Case { name: "payload_mismatch_while_pending", stored: stored(&Record::pending(hash)), current: other },
        Case {
            name: "payload_mismatch_on_cached_failure",
            stored: stored(&Record::failed(hash, "codec: bad payload")),
            current: other,
        },
        Case {
            name: "payload_mismatch_on_cached_success",
            stored: stored(&Record::done(hash, json!(42))),
            current: other,
        },
    ];
    cases
        .into_iter()
        .map(|c| {
            let record = decode_record::<Value>(c.stored.as_bytes()).unwrap();
            let mut expected = json!({
                "name": c.name,
                "stored": c.stored,
                "current": c.current,
                "payload_hash": record.payload_hash,
                "state": record.state.as_str(),
                "outcome": outcome(&record, c.current).as_str(),
                "encoded": stored(&record),
            });
            match &record.state {
                State::Pending => {}
                State::Failed(error) => expected["error"] = json!(error),
                State::Done(result) => expected["result"] = result.clone(),
            }
            expected
        })
        .collect()
}

#[test]
fn golden() {
    write_or_check("payload_hash.json", payload_hash_cases());
    write_or_check("canonical_payload_hash.json", canonical_payload_hash_cases());
    write_or_check("command_hash.json", command_hash_cases());
    write_or_check("idempotency.json", idempotency_cases());
    write_or_check("idempotency_record.json", idempotency_record_cases());
}

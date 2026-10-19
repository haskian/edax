//! Golden vectors for the command-and-query kernel — both the command side (canonical bytes,
//! hashes, the idempotency record and its outcome) and the query side (filter, sort, cursors). Every
//! binding's test suite reads the same files.
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax-cqrs --test golden`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a golden generator runs only as a test, and its panic is the failure report"
)]

use edax_core::{ErrorCode, Tenancy};
use edax_cqrs::{
    canonical_decimal, canonical_json, canonical_timestamp, check_settled, command_hash, complete_fence, decode_cursor,
    decode_prior, decode_record, document_json, effective_sort, encode_cursor, encode_record, eval_json,
    keyed_document, outcome, payload_hash, query_json, received_key, release_fence, replay, settle_failure,
    timestamp_from_unix_nanos, Fence, Holder, Record, Settle, SortKey, State,
};
use edax_golden::{refusal, write_or_check};
use serde_json::{json, Value};

fn canonical_payload_hash_cases() -> Vec<Value> {
    // payload_hash_json canonicalizes (in the kernel, RFC 8785) before hashing, so every binding
    // agrees on the hash regardless of key order, whitespace, number spelling or non-ASCII encoding.
    // The "café" case is exactly the one a host-side json.dumps(ensure_ascii=True) would escape to
    // \u00e9. `{"n":1}` and `{"n":1.0}` hash the *same*: numbers are doubles, so an SDK whose encoder
    // writes a whole float as `1` (JavaScript, Go) agrees with one that writes `1.0`. Each pair the
    // generator asserts equal is one command spelled two ways.
    let rows = [
        r#"{"a":1,"b":2}"#,
        r#"{ "b": 2, "a": 1 }"#,
        r#"{"name":"café"}"#,
        r#"{"name":"caf\u00e9"}"#,
        r#"{"n":1.0}"#,
        r#"{"n":1}"#,
        r#"{"n":1e2}"#,
        r#"{"n":100}"#,
        r#"{"n":-0}"#,
        r#"{"n":0}"#,
        r#"{"crab":"\ud83e\udd80"}"#,
        r#"{"crab":"🦀"}"#,
        r#"{"a":1,"a":2}"#,
        r#"{"a":2}"#,
        "{\"ls\":\"\u{2028}\"}",
        r#"{"ls":"\u2028"}"#,
        r#"{"n":1e20}"#,
        r#"{"n":1.0E+20}"#,
        r#"{"n":9007199254740991}"#,
        r#"{"n":9007199254740991.0}"#,
        r#"{"k":"v","items":[3,1,2]}"#,
        r#"{"outer":{"z":1,"a":2}}"#,
        r#"{}"#,
        r#"{"n":9007199254740993}"#,
        r#"{"n":-9007199254740993}"#,
        r#"{"n":1e400}"#,
        r#"{"s":"\ud800"}"#,
    ];
    let hash = |input: &str| canonical_json(input.as_bytes()).map(|canon| payload_hash(&canon));
    for (a, b) in [
        (2, 3),
        (4, 5),
        (6, 7),
        (8, 9),
        (10, 11),
        (12, 13),
        (14, 15),
        (16, 17),
        (18, 19),
    ] {
        assert_eq!(
            hash(rows[a]),
            hash(rows[b]),
            "{} and {} are one payload",
            rows[a],
            rows[b]
        );
    }
    rows.iter()
        .map(|input| match hash(input) {
            Ok(expected) => json!({ "input": input, "expected": expected, "error": null }),
            Err(error) => json!({ "input": input, "expected": null, "error": error.to_string() }),
        })
        .collect()
}

/// The canonical form itself (RFC 8785), so a second SDK (or a store that re-canonicalizes) can check
/// bytes and not only hashes. Each row: the input text, its canonical text or the error, and the
/// hash. The rows pin what `serde_json` features or a host encoder could otherwise change — key
/// order, number spelling, escaping — and the kernel's own rule: an integer literal outside the
/// I-JSON safe range `-(2^53-1)..=2^53-1`, which a double would round or could not tell from its
/// neighbour, is refused.
fn canonical_json_cases() -> Vec<Value> {
    [
        (
            "keys_inserted_out_of_order",
            r#"{"b":2,"a":1,"c":{"z":0,"y":[{"q":1,"p":2}]}}"#,
        ),
        (
            "keys_sort_by_utf16_code_units",
            r#"{"ｱ":1,"🦀":2,"é":3,"z":4,"Z":5,"a":6,"aa":7}"#,
        ),
        ("whitespace_is_dropped", " { \"a\" : [ 1 , 2 ] }\n"),
        ("repeated_key_keeps_the_last_value", r#"{"a":1,"a":2}"#),
        ("int_and_whole_float_are_one_number", r#"{"n":[2,2.0,2e0,20e-1,2E+0]}"#),
        ("negative_zero_is_zero", r#"{"n":[-0,-0.0]}"#),
        (
            "fractions_are_shortest_round_trip",
            r#"{"n":[1.10,0.1,1.5e-7,0.000001,1e2,123456.789]}"#,
        ),
        (
            "safe_integers_stay_exact",
            r#"{"n":[9007199254740991,-9007199254740991]}"#,
        ),
        ("integer_at_2_pow_53_is_refused", r#"{"n":9007199254740992}"#),
        ("negative_integer_at_2_pow_53_is_refused", r#"{"n":-9007199254740992}"#),
        ("integer_past_2_pow_53_is_refused", r#"{"n":9007199254740993}"#),
        (
            "negative_integer_past_2_pow_53_is_refused",
            r#"{"n":-9007199254740993}"#,
        ),
        ("u64_max_is_refused", r#"{"n":18446744073709551615}"#),
        (
            "nested_wide_integer_is_refused",
            r#"{"a":[{"b":[1,9007199254740993]}]}"#,
        ),
        ("float_past_2_pow_53_rounds_as_a_double", r#"{"n":9007199254740993.0}"#),
        ("whole_float_below_1e21_is_written_out", r#"{"n":[1e20,-1e20]}"#),
        ("float_from_1e21_takes_an_exponent", r#"{"n":[1e21,1.5e300,-1e21]}"#),
        ("escapes_are_decoded", r#"{"s":"\u00e9\/\ud83e\udd80"}"#),
        (
            "control_characters_are_escaped",
            r#"{"s":"\b\f\n\r\t\u0001\u001f\"\\"}"#,
        ),
        ("non_ascii_stays_raw", r#"{"s":"café ☕ 🦀"}"#),
        ("digits_in_a_string_are_text", r#"{"id":"18446744073709551616"}"#),
        ("literals_and_nesting", r#"[true,false,null,[],{}]"#),
        ("number_out_of_range", r#"{"n":1e400}"#),
        ("lone_surrogate", r#"{"s":"\ud800"}"#),
        ("trailing_characters", r#"{} {}"#),
    ]
    .into_iter()
    .map(|(name, input)| match canonical_json(input.as_bytes()) {
        Ok(canonical) => json!({
            "name": name,
            "input": input,
            "canonical": String::from_utf8(canonical.clone()).unwrap(),
            "hash": payload_hash(&canonical),
            "error": null,
        }),
        Err(error) => {
            json!({ "name": name, "input": input, "canonical": null, "hash": null, "error": error.to_string() })
        }
    })
    .collect()
}

/// The key a received command is claimed under: the client's key, else the message's own id.
fn received_key_cases() -> Vec<Value> {
    let cases: [(Option<&str>, &str); 3] = [
        (Some("req-7"), "cmd-1"), // the client's key wins
        (None, "cmd-1"),          // no key: the id deduplicates a redelivery
        (Some(""), "cmd-1"),      // an empty key is a key
    ];
    cases
        .into_iter()
        .map(|(idempotency_key, id)| json!({ "idempotency_key": idempotency_key, "id": id, "expected": received_key(idempotency_key, id) }))
        .collect()
}

/// The command guard's ledger key: scope and key, then the tenant under a pool, injective whatever any
/// of them holds. A mode that wants a tenant and a command that names none is the `codec` error.
fn claim_key_cases() -> Vec<Value> {
    let cases: [(Tenancy, Option<&str>, &str, &str); 13] = [
        (Tenancy::Silo, None, "billing", "req-1"),
        (Tenancy::Silo, None, "", "req-1"), // an unscoped guard still gets the prefix and the length
        (Tenancy::Silo, None, "a:b", "c"),  // a colon in the scope
        (Tenancy::Silo, None, "a", "b:c"),  // the same text split the other way: a different key
        (Tenancy::Silo, None, "café", "clé-1"), // the length counts bytes, not characters
        (Tenancy::Silo, None, "ingress", ""), // an empty key is still a key
        (Tenancy::Silo, Some("acme"), "billing", "req-1"), // a silo never keys by the tenant it carries
        (Tenancy::Pool, Some("acme"), "billing", "req-1"), // the tenant goes outermost
        (Tenancy::Pool, Some("a:b"), "c", "req-1"), // a colon in the tenant
        (Tenancy::Pool, Some("a"), "b:c", "req-1"), // the same text split the other way
        (Tenancy::Pool, None, "billing", "req-1"), // a pool refuses a shared row
        (Tenancy::Bridge, Some("acme"), "billing", "req-1"), // the store is the tenant's: the key stays bare
        (Tenancy::Bridge, None, "billing", "req-1"), // a bridge cannot route without a tenant
    ];
    cases
        .into_iter()
        .map(|(mode, tenant, scope, key)| {
            let outcome = edax_cqrs::claim_key(mode, tenant, scope, key);
            json!({
                "tenancy": mode.as_str(),
                "tenant": tenant,
                "scope": scope,
                "idempotency_key": key,
                "expected": outcome.as_ref().ok(),
                "error": refusal(&outcome),
            })
        })
        .collect()
}

fn command_hash_cases() -> Vec<Value> {
    // The hash *input* rule: the command's name and its whole value, canonicalized. Each case is a
    // command as its field map, and both SDK suites rebuild it as their own command type (a serde
    // struct, a dataclass), dispatch it through their real `idempotent` middleware, and compare the
    // hash the store received. The idempotency key travels beside the command, so it is no field.
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
    });
    let with = |overrides: Value| {
        let mut command = base.clone();
        for (field, value) in overrides.as_object().expect("overrides are an object") {
            command[field] = value.clone();
        }
        command
    };
    let cases: [(&str, &str, Value); 11] = [
        ("the_name_and_the_whole_command", "order.place", base.clone()),
        // The name is hashed: one key reused for another command with an equal payload is a mismatch.
        ("another_name_is_another_command", "order.quote", base.clone()),
        // Array order is data, so swapping two tags is a different command.
        (
            "array_order_is_data",
            "order.place",
            with(json!({ "tags": ["paid", "eu"] })),
        ),
        // Numbers hash by value: a whole-number float is the integer, so `1.0` here and `1` from an
        // encoder that drops the fraction bind to one hash.
        (
            "whole_number_float_hashes_as_the_integer",
            "order.place",
            with(json!({ "weight_kg": 1.0 })),
        ),
        // The same float field as an encoder that drops the fraction writes it: the same hash.
        (
            "whole_number_float_given_as_an_int",
            "order.place",
            with(json!({ "weight_kg": 1 })),
        ),
        // 2^53 - 1, the widest safe integer (I-JSON, RFC 7493): it hashes as itself.
        (
            "safe_integer_stays_exact",
            "order.place",
            with(json!({ "reference": 9_007_199_254_740_991u64 })),
        ),
        // From 2^53 a double cannot tell two ids one apart, so they would hash alike: refused as
        // `codec`, in every SDK, before any handler runs. Above `i64::MAX` too, where a signed reader
        // also fails.
        (
            "integer_at_2_pow_53_is_refused",
            "order.place",
            with(json!({ "reference": 9_007_199_254_740_992u64 })),
        ),
        (
            "u64_above_i64_max_is_refused",
            "order.place",
            with(json!({ "reference": 9_223_372_036_854_775_809u64 })),
        ),
        // An optional field that is unset is serialized as null, not omitted.
        (
            "unset_optional_is_null_not_omitted",
            "order.place",
            with(json!({ "note": null })),
        ),
        // Hashed as UTF-8 bytes: an encoder that escapes to \u00e9 would diverge.
        (
            "non_ascii_hashes_as_utf8",
            "order.place",
            with(json!({ "customer": "café ☕ 🦀" })),
        ),
        (
            "empty_array_and_null_nested_field",
            "order.place",
            with(json!({ "tags": [], "shipping": { "city": "NYC", "zip": null } })),
        ),
    ];
    let hashes: std::collections::BTreeMap<&str, Result<u64, edax_core::Error>> = cases
        .iter()
        .map(|(name, command_name, command)| (*name, command_hash(command_name, command)))
        .collect();
    assert_eq!(
        hashes["whole_number_float_hashes_as_the_integer"], hashes["whole_number_float_given_as_an_int"],
        "a whole float and its integer spelling are one command"
    );
    cases
        .into_iter()
        .map(|(name, command_name, command)| {
            // The JSON-text entry point must agree with the value one on every case.
            let text = serde_json::to_vec(&command).expect("a case is JSON");
            assert_eq!(command_hash(command_name, &command), edax_cqrs::command_hash_json(command_name, &text), "{name}");
            match command_hash(command_name, &command) {
                Ok(expected) => json!({ "name": name, "type": command_name, "command": command, "expected": expected, "error": null }),
                Err(error) => json!({ "name": name, "type": command_name, "command": command, "expected": null, "error": error.to_string() }),
            }
        })
        .chain(host_value_command_hash_cases())
        .collect()
}

/// A command with a decimal and an instant field, whose value each SDK builds with its own host type
/// (`edax::Decimal` / `edax::Instant`, Python's `Decimal` / aware `datetime`) from the vector's markers
/// before it dispatches. Another spelling of one value is one command, and an instant without an offset
/// cannot be built at all.
#[derive(serde::Serialize, serde::Deserialize)]
struct Withdraw {
    account: String,
    amount: edax_cqrs::Decimal,
    at: edax_cqrs::Instant,
}

fn host_value_command_hash_cases() -> Vec<Value> {
    let command = |amount: &str, at: &str| json!({ "account": "acct-1", "amount": { "$decimal": amount }, "at": { "$instant": at } });
    let cases: [(&str, Value); 4] = [
        (
            "host_values_in_canonical_form",
            command("9.5", "2024-01-01T00:30:00.000000Z"),
        ),
        (
            "decimal_trailing_zeros_are_one_value",
            command("9.50", "2024-01-01T00:30:00.000000Z"),
        ),
        (
            "instant_offset_hashes_as_utc",
            command("9.5", "2024-01-01T01:30:00+01:00"),
        ),
        (
            "instant_without_offset_is_refused",
            command("9.5", "2024-01-01T00:30:00"),
        ),
    ];
    let rows: Vec<(&str, Value, Result<u64, String>)> = cases
        .into_iter()
        .map(|(name, command)| {
            let hashed = serde_json::from_value::<Withdraw>(edax_golden::host_text(&command))
                .map_err(|e| e.to_string())
                .and_then(|built| command_hash("Withdraw", &built).map_err(|e| e.to_string()));
            (name, command, hashed)
        })
        .collect();
    let hashes: std::collections::BTreeSet<&u64> =
        rows.iter().filter_map(|(_, _, hashed)| hashed.as_ref().ok()).collect();
    assert_eq!(
        hashes.len(),
        1,
        "every spelling of one decimal and one instant is one command: {hashes:?}"
    );
    rows.into_iter()
        .map(|(name, command, hashed)| match hashed {
            Ok(expected) => {
                json!({ "name": name, "type": "Withdraw", "command": command, "expected": expected, "error": null })
            }
            Err(error) => {
                assert!(
                    error.starts_with("codec: "),
                    "{name}: the kernel's codec error: {error}"
                );
                json!({ "name": name, "type": "Withdraw", "command": command, "expected": null, "error": error })
            }
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
        Case {
            name: "pending_claim",
            stored: stored(&Record::pending(hash)),
            current: hash,
        },
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
        Case {
            name: "cached_failure",
            stored: stored(&Record::failed(hash, ErrorCode::Codec, "unknown field `amount`")),
            current: hash,
        },
        // A host's business rejection, cached with its own code. This is what the Rust SDK's guard
        // writes for a `Terminal` failure, and the Python suite reads its code back.
        Case {
            name: "cached_rejection_written_by_rust",
            stored: stored(&Record::failed(
                hash,
                ErrorCode::Rejected,
                "insufficient funds: balance 300, withdrawal 500",
            )),
            current: hash,
        },
        // An empty message is still a failure: only `null` means "not failed".
        Case {
            name: "cached_failure_with_empty_message",
            stored: stored(&Record::failed(hash, ErrorCode::Codec, "")),
            current: hash,
        },
        // A writer that knows a newer code: the failure still reads, with no code.
        Case {
            name: "unknown_error_code_reads_as_no_code",
            stored: format!(
                r#"{{"payload_hash":{hash},"result":null,"error":"boom","error_code":"quota_exceeded","pending":false}}"#
            ),
            current: hash,
        },
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
        Case {
            name: "payload_mismatch_while_pending",
            stored: stored(&Record::pending(hash)),
            current: other,
        },
        Case {
            name: "payload_mismatch_on_cached_failure",
            stored: stored(&Record::failed(hash, ErrorCode::Codec, "unknown field `amount`")),
            current: other,
        },
        Case {
            name: "payload_mismatch_on_cached_success",
            stored: stored(&Record::done(hash, json!(42))),
            current: other,
        },
        // A stored hash of 0 is a real hash, not "absent": it mismatches any other command and
        // replays against itself.
        Case {
            name: "stored_zero_hash_mismatches",
            stored: stored(&Record::done(0, json!(42))),
            current: hash,
        },
        Case {
            name: "stored_zero_hash_replays_against_zero",
            stored: stored(&Record::done(0, json!(42))),
            current: 0,
        },
    ];
    // Bytes no reader can decode. Each still holds its key: `decode_prior` reads it as a terminal
    // `codec` failure, and `decode_error` pins the strict reader's refusal. The message is the JSON
    // parser's own text, so it is pinned too. A row whose hash does not read is claimed with `other`,
    // so the failure visibly carries the claimant's hash; a row whose hash reads keeps the stored one.
    let unreadable = [
        Case {
            name: "not_an_object",
            stored: "[1,null,null]".to_owned(),
            current: other,
        },
        Case {
            name: "missing_payload_hash",
            stored: r#"{"result":null,"error":null,"pending":false}"#.to_owned(),
            current: other,
        },
        Case {
            name: "float_payload_hash",
            stored: format!(r#"{{"payload_hash":{hash}.0,"pending":false}}"#),
            current: other,
        },
        Case {
            name: "negative_payload_hash",
            stored: r#"{"payload_hash":-1,"pending":false}"#.to_owned(),
            current: other,
        },
        Case {
            name: "string_payload_hash",
            stored: format!(r#"{{"payload_hash":"{hash}","pending":false}}"#),
            current: other,
        },
        Case {
            name: "payload_hash_past_u64",
            stored: r#"{"payload_hash":18446744073709551616,"pending":false}"#.to_owned(),
            current: other,
        },
        Case {
            name: "non_string_error",
            stored: format!(r#"{{"payload_hash":{hash},"error":42,"pending":false}}"#),
            current: hash,
        },
        Case {
            name: "non_string_error_code",
            stored: format!(r#"{{"payload_hash":{hash},"error":"boom","error_code":7,"pending":false}}"#),
            current: hash,
        },
        Case {
            name: "null_pending",
            stored: format!(r#"{{"payload_hash":{hash},"pending":null}}"#),
            current: hash,
        },
        // A result no writer may store: read as the nearest double, it would replay a value the winner
        // never returned, and a typed reader would refuse it instead.
        Case {
            name: "result_past_64_bits",
            stored: format!(r#"{{"payload_hash":{hash},"result":18446744073709551616,"error":null,"pending":false}}"#),
            current: hash,
        },
        Case {
            name: "result_below_i64_min",
            stored: format!(
                r#"{{"payload_hash":{hash},"result":{{"total":-9223372036854775809}},"error":null,"pending":false}}"#
            ),
            current: hash,
        },
        // A state the wire does not know: `pending` is a bool, never a word.
        Case {
            name: "unknown_pending_state",
            stored: format!(r#"{{"payload_hash":{hash},"pending":"settling"}}"#),
            current: hash,
        },
        Case {
            name: "not_json",
            stored: "{".to_owned(),
            current: other,
        },
        Case {
            name: "garbage",
            stored: "\u{1}\u{7f}edax".to_owned(),
            current: other,
        },
        // A record the Rust SDK was writing, cut off mid-write: the shared ledger's torn row.
        Case {
            name: "torn_write_from_rust",
            stored: stored(&Record::done(hash, json!({"invoice_id": "invoice-7", "total": 5000})))[..48].to_owned(),
            current: other,
        },
        // The hash still reads beside a broken field, so another command reusing the key is told so.
        Case {
            name: "unreadable_record_reused_by_another_command",
            stored: format!(r#"{{"payload_hash":{hash},"error":42,"pending":false}}"#),
            current: other,
        },
    ];
    let cases = cases.into_iter().chain([
        // A field the kernel does not know is ignored, like a newer writer's addition to an envelope.
        Case {
            name: "an_unknown_field_is_ignored",
            stored: format!(r#"{{"payload_hash":{hash},"result":7,"error":null,"pending":false,"ttl_ms":1000}}"#),
            current: hash,
        },
        // A host's rejection keeps why it refused, so a replay tells `not_found` from `conflict`.
        Case {
            name: "a_rejection_keeps_its_reason",
            stored: stored(&Record {
                payload_hash: hash,
                state: State::Failed {
                    code: Some(ErrorCode::Rejected),
                    message: "no such order".to_owned(),
                    reason: Some("not_found".to_owned()),
                },
            }),
            current: hash,
        },
        // A reason a reader cannot use reads as none: the failure, and its code, still replay.
        Case {
            name: "a_reason_beside_another_code_reads_as_none",
            stored: format!(
                r#"{{"payload_hash":{hash},"result":null,"error":"bad","error_code":"codec","error_reason":"not_found","pending":false}}"#
            ),
            current: hash,
        },
        Case {
            name: "a_reason_that_is_not_a_name_reads_as_none",
            stored: format!(
                r#"{{"payload_hash":{hash},"result":null,"error":"no such order","error_code":"rejected","error_reason":"Not Found","pending":false}}"#
            ),
            current: hash,
        },
        Case {
            name: "a_reason_that_is_not_a_string_reads_as_none",
            stored: format!(
                r#"{{"payload_hash":{hash},"result":null,"error":"no such order","error_code":"rejected","error_reason":7,"pending":false}}"#
            ),
            current: hash,
        },
    ]);
    cases
        .chain(unreadable)
        .map(|c| {
            let record = decode_prior::<Value>(c.stored.as_bytes(), c.current);
            let mut expected = json!({
                "name": c.name,
                "stored": c.stored,
                "current": c.current,
                "payload_hash": record.payload_hash,
                "state": record.state.kind().as_str(),
                "outcome": outcome(&record, c.current).kind().as_str(),
                "encoded": stored(&record),
            });
            match decode_record::<Value>(c.stored.as_bytes()) {
                Ok(strict) => assert_eq!(strict, record, "{}: a readable record reads alike both ways", c.name),
                Err(error) => expected["decode_error"] = json!(error.to_string()),
            }
            match &record.state {
                State::Pending => {}
                State::Failed { code, message, reason } => {
                    expected["error"] = json!(message);
                    expected["error_code"] = json!(code.map(ErrorCode::as_str));
                    expected["error_reason"] = json!(reason);
                }
                State::Done(result) => expected["result"] = result.clone(),
            }
            // Whether a store's `complete` takes these bytes as the record that settles a claim.
            expected["check_settled"] = refusal(&check_settled(c.stored.as_bytes()));
            // What a losing claimant raises on replay under the key `req-1`: the kernel's text, so every
            // SDK shows the same.
            if let Err(error) = replay(&record, c.current, "req-1") {
                expected["replay_error"] = json!(error.to_string());
            }
            expected
        })
        .collect()
}

/// `document_json`: what a projection store's save accepts as a read-model document, and
/// `keyed_document`: what its `get` and `get_many` read back, the row key under `id`. Only an object can
/// carry that key; an integer past 64 bits would come back rounded.
fn projection_document_cases() -> Vec<Value> {
    [
        (
            "an_object_is_kept",
            r#"{"email":"ada@example.com","age":36,"tags":["eu","paid"]}"#,
        ),
        ("an_empty_object_is_kept", "{}"),
        ("the_row_key_overwrites_an_id_field", r#"{"id":"stale","age":36}"#),
        ("an_array_is_refused", r#"["ada@example.com"]"#),
        ("a_scalar_is_refused", "36"),
        ("null_is_refused", "null"),
        (
            "an_integer_past_64_bits_is_refused",
            r#"{"balance":18446744073709551616}"#,
        ),
        ("text_that_is_not_json_is_refused", r#"{"email":"#),
    ]
    .into_iter()
    .map(|(name, json)| {
        let want = match document_json("user-1", json.as_bytes()) {
            Ok(document) => json!({ "ok": document, "keyed": keyed_document("user-1", document.clone()) }),
            Err(error) => json!({ "error": error.to_string() }),
        };
        json!({ "name": name, "id": "user-1", "json": json, "want": want })
    })
    .collect()
}

fn filter_cases() -> Vec<Value> {
    use edax_cqrs::filter::{and, contains, decimal, eq, exists, gt, gte, is_in, lt, lte, ne, not, or, starts_with};
    let to = |f: edax_cqrs::Filter| serde_json::to_value(f).unwrap();
    let cases: Vec<(&str, Value, Value)> = vec![
        ("eq_matches", to(eq("status", json!("open"))), json!({"status": "open"})),
        (
            "eq_differs",
            to(eq("status", json!("open"))),
            json!({"status": "closed"}),
        ),
        (
            "ne_differs",
            to(ne("status", json!("closed"))),
            json!({"status": "open"}),
        ),
        ("ne_on_a_missing_path_is_false", to(ne("status", json!("x"))), json!({})),
        (
            "not_eq_on_a_missing_path_is_true",
            to(not(eq("status", json!("x")))),
            json!({}),
        ),
        ("gt_greater", to(gt("total", json!(4200))), json!({"total": 5000})),
        ("gt_less", to(gt("total", json!(4200))), json!({"total": 100})),
        (
            "gt_across_types_is_false",
            to(gt("total", json!(4200))),
            json!({"total": "5000"}),
        ),
        ("eq_is_numeric_1_equals_1_0", to(eq("n", json!(1.0))), json!({"n": 1})),
        // Past 2^53 an f64 comparison would call these equal.
        (
            "eq_wide_integers_are_exact",
            to(eq("n", json!(9_007_199_254_740_993u64))),
            json!({"n": 9_007_199_254_740_992u64}),
        ),
        (
            "gt_wide_integers_are_exact",
            to(gt("n", json!(9_007_199_254_740_992u64))),
            json!({"n": 9_007_199_254_740_993u64}),
        ),
        (
            "in_hits",
            to(is_in("status", vec![json!("open"), json!("closed")])),
            json!({"status": "open"}),
        ),
        (
            "in_misses",
            to(is_in("status", vec![json!("a")])),
            json!({"status": "open"}),
        ),
        (
            "in_on_a_missing_path_is_false",
            to(is_in("status", vec![json!("a")])),
            json!({}),
        ),
        (
            "in_empty_matches_nothing",
            to(is_in("status", vec![])),
            json!({"status": "open"}),
        ),
        ("exists_present", to(exists("p", true)), json!({"p": 1})),
        ("exists_null_is_not_present", to(exists("p", true)), json!({"p": null})),
        ("exists_false_on_missing", to(exists("p", false)), json!({})),
        ("not_exists_on_missing_is_false", to(not(exists("p", false))), json!({})),
        (
            "nested_path",
            to(eq("address.city", json!("NYC"))),
            json!({"address": {"city": "NYC"}}),
        ),
        (
            "nested_missing_key",
            to(eq("address.zip", json!("x"))),
            json!({"address": {"city": "NYC"}}),
        ),
        ("path_through_a_non_object", to(eq("a.b", json!("x"))), json!({"a": 5})),
        (
            "a_numeric_segment_is_a_key_not_an_index",
            to(eq("tags.0", json!("eu"))),
            json!({"tags": ["eu"]}),
        ),
        (
            "a_numeric_segment_reads_an_object_key",
            to(eq("tags.0", json!("eu"))),
            json!({"tags": {"0": "eu"}}),
        ),
        (
            "and_all",
            to(and(vec![eq("status", json!("open")), gt("total", json!(4200))])),
            json!({"status": "open", "total": 5000}),
        ),
        (
            "and_one_false",
            to(and(vec![eq("status", json!("open")), gt("total", json!(9999))])),
            json!({"status": "open", "total": 5000}),
        ),
        (
            "or_one_true",
            to(or(vec![eq("status", json!("closed")), gt("total", json!(4200))])),
            json!({"status": "open", "total": 5000}),
        ),
        (
            "gte_at_the_boundary",
            to(gte("total", json!(5000))),
            json!({"total": 5000}),
        ),
        (
            "lte_at_the_boundary",
            to(lte("total", json!(5000))),
            json!({"total": 5000}),
        ),
        ("lt_strings", to(lt("name", json!("z"))), json!({"name": "m"})),
        // Strings order by code point, which is UTF-8 byte order: "Z" < "a", and U+FF71 < U+1F980
        // although UTF-16 code units (JavaScript, Java, C#) put the crab first.
        (
            "strings_order_upper_before_lower",
            to(lt("s", json!("a"))),
            json!({"s": "Z"}),
        ),
        (
            "strings_order_by_code_point_not_utf16",
            to(lt("s", json!("🦀"))),
            json!({"s": "\u{ff71}"}),
        ),
        // No Unicode normalization: NFC and NFD spellings of one name differ.
        (
            "strings_are_not_normalized",
            to(eq("s", json!("caf\u{e9}"))),
            json!({"s": "cafe\u{301}"}),
        ),
        // RFC3339 orders as strings only in one fixed-width form: these two read wrong on purpose.
        (
            "timestamp_without_a_fraction_sorts_after_one_with",
            to(lt("t", json!("2024-01-01T00:00:00Z"))),
            json!({"t": "2024-01-01T00:00:00.500000Z"}),
        ),
        (
            "timestamp_with_an_offset_sorts_by_wall_time",
            to(gt("t", json!("2024-01-01T00:00:00Z"))),
            json!({"t": "2024-01-01T00:30:00+01:00"}),
        ),
        // The form every SDK writes a host timestamp in (`host_values.json`) orders by instant.
        (
            "timestamp_in_the_fixed_width_utc_form_orders_by_instant",
            to(lt("t", json!("2024-01-01T00:00:00.500000Z"))),
            json!({"t": "2024-01-01T00:00:00.000000Z"}),
        ),
        // The decimal collation compares decimal text by exact value; code point order does not.
        (
            "decimal_text_orders_by_code_point_without_the_collation",
            to(lt("p", json!("10"))),
            json!({"p": "9.5"}),
        ),
        ("decimal_lt_by_value", to(decimal::lt("p", "10")), json!({"p": "9.5"})),
        ("decimal_gt_by_value", to(decimal::gt("p", "9.5")), json!({"p": "10"})),
        (
            "decimal_eq_ignores_trailing_zeros",
            to(decimal::eq("p", "9.5")),
            json!({"p": "9.50"}),
        ),
        (
            "decimal_eq_reads_an_exponent",
            to(decimal::eq("p", "1E+2")),
            json!({"p": "100"}),
        ),
        (
            "decimal_ne_on_an_equal_value",
            to(decimal::ne("p", "9.5")),
            json!({"p": "9.50"}),
        ),
        (
            "decimal_gte_at_the_boundary",
            to(decimal::gte("p", "9.50")),
            json!({"p": "9.5"}),
        ),
        (
            "decimal_lte_at_the_boundary",
            to(decimal::lte("p", "-0")),
            json!({"p": "0.000"}),
        ),
        (
            "decimal_negatives_order_by_value",
            to(decimal::lt("p", "-9.5")),
            json!({"p": "-10"}),
        ),
        (
            "decimal_past_17_digits_is_exact",
            to(decimal::gt("p", "12345678901234567890.1")),
            json!({"p": "12345678901234567890.10000000001"}),
        ),
        (
            "decimal_in_by_value",
            to(decimal::is_in("p", ["1", "9.5"])),
            json!({"p": "9.50"}),
        ),
        (
            "decimal_in_misses",
            to(decimal::is_in("p", ["9.51"])),
            json!({"p": "9.50"}),
        ),
        // Under the collation a value that is not decimal text compares as a mismatched type does.
        (
            "decimal_against_other_text_is_unordered",
            to(decimal::lt("p", "10")),
            json!({"p": "n/a"}),
        ),
        (
            "decimal_eq_against_other_text_is_false",
            to(decimal::eq("p", "10")),
            json!({"p": "n/a"}),
        ),
        (
            "decimal_ne_against_other_text_is_true",
            to(decimal::ne("p", "10")),
            json!({"p": "n/a"}),
        ),
        (
            "decimal_against_a_number_is_unordered",
            to(decimal::gt("p", "1")),
            json!({"p": 5}),
        ),
        (
            "decimal_against_nan_text_is_unordered",
            to(decimal::lt("p", "1")),
            json!({"p": "NaN"}),
        ),
        (
            "starts_with_prefix",
            to(starts_with("sku", json!("INV-"))),
            json!({"sku": "INV-2024-01"}),
        ),
        (
            "starts_with_other_prefix",
            to(starts_with("sku", json!("ORD-"))),
            json!({"sku": "INV-2024-01"}),
        ),
        (
            "starts_with_non_string",
            to(starts_with("n", json!("5"))),
            json!({"n": 5}),
        ),
        (
            "contains_member",
            to(contains("tags", json!("eu"))),
            json!({"tags": ["paid", "eu"]}),
        ),
        (
            "contains_non_member",
            to(contains("tags", json!("usd"))),
            json!({"tags": ["paid", "eu"]}),
        ),
        (
            "contains_needs_an_array",
            to(contains("tags", json!("eu"))),
            json!({"tags": "eu"}),
        ),
        ("contains_is_numeric", to(contains("v", json!(1.0))), json!({"v": [1]})),
        (
            "contains_null_element",
            to(contains("v", json!(null))),
            json!({"v": [1, null]}),
        ),
        // Equality is numeric at every depth, the same as at the top level.
        (
            "eq_nested_array_is_numeric",
            to(eq("a", json!([1.0]))),
            json!({"a": [1]}),
        ),
        (
            "eq_nested_object_is_numeric",
            to(eq("o", json!({"x": [2.0]}))),
            json!({"o": {"x": [2]}}),
        ),
        ("eq_array_length_differs", to(eq("a", json!([1, 1]))), json!({"a": [1]})),
        // Bools order false < true, as the sort ranks them.
        ("gt_true_over_false", to(gt("b", json!(false))), json!({"b": true})),
        ("lte_false_under_true", to(lte("b", json!(true))), json!({"b": false})),
        (
            "bool_against_number_is_unordered",
            to(gt("b", json!(0))),
            json!({"b": true}),
        ),
        ("and_empty_is_true", to(and(vec![])), json!({})),
        ("or_empty_is_false", to(or(vec![])), json!({})),
        // Refused: what the grammar admits but no document could mean, and typos.
        ("refused_null_comparand", to(eq("p", json!(null))), json!({"p": null})),
        (
            "refused_null_in_member",
            to(is_in("p", vec![json!(1), json!(null)])),
            json!({"p": 1}),
        ),
        ("refused_empty_path", to(exists("", true)), json!({})),
        (
            "refused_dotted_empty_segment",
            to(eq("a..b", json!(1))),
            json!({"a": {"": {"b": 1}}}),
        ),
        (
            "refused_unknown_field",
            json!({"compare": {"path": "p", "op": "eq", "vlaue": 1}}),
            json!({"p": 1}),
        ),
        ("refused_unknown_node", json!({"near": {"path": "p"}}), json!({"p": 1})),
        (
            "refused_decimal_comparand_that_is_not_decimal_text",
            to(decimal::gt("p", "ten")),
            json!({"p": "11"}),
        ),
        (
            "refused_decimal_comparand_that_is_a_number",
            json!({"compare": {"path": "p", "op": "gt", "value": 10, "collation": "decimal"}}),
            json!({"p": "11"}),
        ),
        (
            "refused_decimal_member_that_is_not_decimal_text",
            to(decimal::is_in("p", ["1", "Infinity"])),
            json!({"p": "1"}),
        ),
        (
            "refused_starts_with_a_number_prefix",
            to(starts_with("sku", json!(5))),
            json!({"sku": "5"}),
        ),
        (
            "refused_starts_with_under_a_collation",
            json!({"compare": {"path": "p", "op": "starts_with", "value": "1", "collation": "decimal"}}),
            json!({"p": "11"}),
        ),
        (
            "refused_unknown_collation",
            json!({"compare": {"path": "p", "op": "lt", "value": "1", "collation": "natural"}}),
            json!({"p": "0"}),
        ),
    ];
    cases
        .into_iter()
        .map(|(name, filter, doc)| {
            let result = eval_json(
                &serde_json::to_vec(&filter).unwrap(),
                &serde_json::to_vec(&doc).unwrap(),
            );
            assert_eq!(name.starts_with("refused_"), result.is_err(), "{name}");
            json!({
                "name": name,
                "filter": filter,
                "document": doc,
                "expected": result.as_ref().ok(),
                "error": refusal(&result),
            })
        })
        .collect()
}

fn query_cases() -> Vec<Value> {
    // Each case pins the page and its `next` cursor, so bindings verify ordering, pagination and
    // byte-identical cursor encoding together. A cursor in a spec is minted by the kernel from the
    // row named beside it, so the spec stays readable.
    let mint = |row: Value, sort: Value| -> String {
        encode_cursor(&row, &serde_json::from_value::<Vec<SortKey>>(sort).unwrap()).unwrap()
    };
    let by_n = || json!([{"path": "n"}]);
    let by_p = || json!([{"path": "p", "collation": "decimal"}]);
    // Prices as decimal text: one tie spelled two ways, a negative, a value past 17 digits, a number,
    // text that is not decimal, and a row without a price.
    let decimals = || {
        json!([
            {"id": "a", "p": "10"},
            {"id": "b", "p": "9.50"},
            {"id": "c", "p": "9.5"},
            {"id": "d", "p": "-2.25"},
            {"id": "e", "p": "12345678901234567890.1"},
            {"id": "f", "p": 3},
            {"id": "g", "p": "n/a"},
            {"id": "h"},
            {"id": "i", "p": "1E+1"},
        ])
    };
    let ns = |values: &[i64]| -> Value {
        values
            .iter()
            .enumerate()
            .map(|(i, n)| json!({"id": format!("r{i}"), "n": n}))
            .collect()
    };
    let cases: Vec<(&str, Value, Value)> = vec![
        ("sort_ascending", json!({"sort": by_n()}), ns(&[3, 1, 2])),
        (
            "offset_and_limit_with_more_rows",
            json!({"sort": by_n(), "offset": 1, "limit": 2}),
            ns(&[3, 1, 2, 5, 4]),
        ),
        (
            "descending_missing_path_first",
            json!({"sort": [{"path": "n", "desc": true}]}),
            json!([{"id": "a", "n": 1}, {"id": "b"}, {"id": "c", "n": 3}]),
        ),
        // Mixed types exercise the total type order: number < string < bool < array < object < null.
        (
            "mixed_types_total_order",
            json!({"sort": [{"path": "v"}]}),
            json!([{"id": "a", "v": "a"}, {"id": "b", "v": 2}, {"id": "c", "v": true}, {"id": "d", "v": null}, {"id": "e", "v": [1]}, {"id": "f", "v": {"k": 1}}, {"id": "g", "v": false}]),
        ),
        (
            "filter_then_sort",
            json!({"filter": {"compare": {"path": "s", "op": "eq", "value": "open"}}, "sort": by_n()}),
            json!([{"id": "a", "s": "open", "n": 2}, {"id": "b", "s": "closed", "n": 1}, {"id": "c", "s": "open", "n": 1}]),
        ),
        (
            "resume_after_a_cursor",
            json!({"sort": by_n(), "limit": 1, "after": mint(json!({"id": "r1", "n": 1}), by_n())}),
            ns(&[1, 1, 2, 3]),
        ),
        (
            "resume_descending",
            json!({"sort": [{"path": "n", "desc": true}], "after": mint(json!({"id": "r2", "n": 3}), json!([{"path": "n", "desc": true}]))}),
            ns(&[1, 2, 3]),
        ),
        // The cursor row sits inside a tie on `bal`; the tiebreak resumes inside the tie.
        (
            "resume_mid_tie",
            json!({"sort": [{"path": "bal"}], "limit": 1, "after": mint(json!({"id": "b", "bal": 100}), json!([{"path": "bal"}]))}),
            json!([{"id": "a", "bal": 100}, {"id": "b", "bal": 100}, {"id": "c", "bal": 100}, {"id": "d", "bal": 200}]),
        ),
        // Resuming from a row whose sort value is null: only rows after it in the null tail remain.
        (
            "resume_from_a_null_row",
            json!({"sort": by_n(), "after": mint(json!({"id": "b"}), by_n())}),
            json!([{"id": "a"}, {"id": "b"}, {"id": "c"}, {"id": "d", "n": 1}]),
        ),
        (
            "offset_after_a_cursor",
            json!({"sort": by_n(), "offset": 1, "after": mint(json!({"id": "r0", "n": 1}), by_n())}),
            ns(&[1, 2, 3, 4]),
        ),
        // A whole-number double past 2^53 sits in the cursor as integer digits that are not its exact
        // value; read back as the double, the walk resumes inside the tie and not before its own row.
        (
            "resume_after_a_whole_number_double_past_2_pow_53",
            json!({"sort": by_n(), "limit": 1, "after": mint(json!({"id": "a", "n": 5.808820077685227e16}), by_n())}),
            json!([{"id": "a", "n": 5.808820077685227e16}, {"id": "b", "n": 5.808820077685227e16}, {"id": "c"}]),
        ),
        // Ints, floats and integers past 2^53 in one key: by value, exactly.
        (
            "numeric_sort_by_value",
            json!({"sort": [{"path": "v"}]}),
            json!([{"id": "a", "v": 9_007_199_254_740_993u64}, {"id": "b", "v": 2.5}, {"id": "c", "v": 9_007_199_254_740_992u64}, {"id": "d", "v": 2}, {"id": "e", "v": -1.5}]),
        ),
        (
            "string_sort_by_code_point",
            json!({"sort": [{"path": "s"}]}),
            json!([{"id": "a", "s": "a"}, {"id": "b", "s": "Z"}, {"id": "c", "s": "🦀"}, {"id": "d", "s": "\u{ff71}"}, {"id": "e", "s": ""}]),
        ),
        (
            "multi_key_tie_broken_by_the_second",
            json!({"sort": [{"path": "bal", "desc": true}, {"path": "id"}], "limit": 2}),
            json!([{"id": "a", "bal": 150}, {"id": "b", "bal": 300}, {"id": "c", "bal": 150}]),
        ),
        (
            "no_sort_keeps_input_order_and_mints_no_cursor",
            json!({"limit": 2}),
            ns(&[3, 1, 2]),
        ),
        // Decimal text sorts by code point without the collation and by exact value under it, ties
        // broken by id. Decimal text sorts before other text, and numbers keep their type rank.
        (
            "decimal_text_without_the_collation_sorts_by_code_point",
            json!({"sort": [{"path": "p"}]}),
            decimals(),
        ),
        ("decimal_sort_by_value", json!({"sort": by_p()}), decimals()),
        (
            "decimal_sort_descending",
            json!({"sort": [{"path": "p", "desc": true, "collation": "decimal"}]}),
            decimals(),
        ),
        (
            "decimal_sort_pages_and_mints_next",
            json!({"sort": by_p(), "limit": 3}),
            decimals(),
        ),
        // The cursor carries the row's text unchanged, so the walk resumes inside the `9.5` tie.
        (
            "decimal_sort_resumes_after_a_cursor",
            json!({"sort": by_p(), "limit": 2, "after": mint(json!({"id": "b", "p": "9.50"}), by_p())}),
            decimals(),
        ),
        (
            "decimal_filter_then_decimal_sort",
            json!({"filter": {"compare": {"path": "p", "op": "gte", "value": "9.5", "collation": "decimal"}}, "sort": by_p()}),
            decimals(),
        ),
        // "7" and "07" tie under a decimal id key, so the plain id still breaks the tie: the walk from
        // "07" resumes at "7" instead of skipping it.
        (
            "decimal_id_sort_keeps_the_plain_tiebreak",
            json!({"sort": [{"path": "id", "collation": "decimal"}], "limit": 1, "after": mint(json!({"id": "07"}), json!([{"path": "id", "collation": "decimal"}]))}),
            json!([{"id": "8"}, {"id": "7"}, {"id": "07"}]),
        ),
        ("offset_past_the_end", json!({"sort": by_n(), "offset": 5}), ns(&[1, 2])),
        // Refused: a cursor under another sort, a cursor without one, and a page whose `next` would
        // be minted from a row without the tiebreak id.
        (
            "refused_cursor_under_another_sort",
            json!({"sort": [{"path": "s"}], "after": mint(json!({"id": "r0", "n": 1}), by_n())}),
            ns(&[1, 2]),
        ),
        (
            "refused_cursor_under_another_direction",
            json!({"sort": [{"path": "n", "desc": true}], "after": mint(json!({"id": "r0", "n": 1}), by_n())}),
            ns(&[1, 2]),
        ),
        (
            "refused_cursor_without_a_sort",
            json!({"after": mint(json!({"id": "r0", "n": 1}), by_n())}),
            ns(&[1, 2]),
        ),
        (
            "refused_next_from_a_row_without_an_id",
            json!({"sort": [{"path": "k"}], "limit": 1}),
            json!([{"k": 1}, {"k": 1}]),
        ),
        (
            "refused_next_from_a_row_with_an_object_id",
            json!({"sort": [{"path": "k"}], "limit": 1}),
            json!([{"id": {"n": 0}, "k": 1}, {"id": {"n": 1}, "k": 1}]),
        ),
        ("refused_unknown_field", json!({"sort": by_n(), "limt": 1}), ns(&[1])),
        (
            "refused_null_comparand_in_the_filter",
            json!({"filter": {"compare": {"path": "n", "op": "eq", "value": null}}, "sort": by_n()}),
            ns(&[1]),
        ),
        (
            "refused_sort_path_with_an_empty_segment",
            json!({"sort": [{"path": "a..b"}]}),
            ns(&[1]),
        ),
        // The collation is part of the sort a cursor is bound to.
        (
            "refused_cursor_under_another_collation",
            json!({"sort": [{"path": "p"}], "after": mint(json!({"id": "b", "p": "9.50"}), by_p())}),
            decimals(),
        ),
    ];
    cases
        .into_iter()
        .map(|(name, q, docs)| {
            let result = query_json(&serde_json::to_vec(&q).unwrap(), &serde_json::to_vec(&docs).unwrap());
            assert_eq!(name.starts_with("refused_"), result.is_err(), "{name}");
            let page: Option<Value> = result.as_ref().ok().map(|text| serde_json::from_str(text).unwrap());
            json!({
                "name": name,
                "query": q,
                "documents": docs,
                "expected_page": page,
                "error": refusal(&result),
            })
        })
        .collect()
}

fn cursor_cases() -> Vec<Value> {
    // The round trip a backend depends on: a cursor minted from a row decodes back to the tuple its
    // `WHERE` clause binds, under the sort the page is actually ordered by. Each case pins both
    // halves, so a binding that decodes differently fails here rather than silently paging wrong.
    let minted: Vec<(Value, Value)> = vec![
        // The ordinary case: one declared key, `id` appended as the tiebreak.
        (json!([{"path": "age"}]), json!({"id": "user-1", "age": 36})),
        // Two declared keys; the tiebreak still lands last.
        (
            json!([{"path": "s"}, {"path": "n", "desc": true}]),
            json!({"id": "a", "s": "open", "n": 4}),
        ),
        // `id` already terminal: no second tiebreak, and the same sort as the tiebreak left implicit.
        (json!([{"path": "n"}, {"path": "id"}]), json!({"id": "z", "n": 1})),
        (json!([{"path": "id"}]), json!({"id": "only"})),
        (json!([{"path": "id", "desc": true}]), json!({"id": "only"})),
        // A missing sort path encodes as null and must decode back as null, not as an absent slot.
        (json!([{"path": "missing"}]), json!({"id": "user-2"})),
        // Every type the total order spans, so the tuple's JSON round trip is pinned for each, and
        // non-ASCII in the tuple, which is written raw before base64url.
        (
            json!([{"path": "v"}]),
            json!({"id": "t1", "v": {"k": [1, "two", true, null, "café ☕"]}}),
        ),
        // Bytes whose standard base64 would carry `+` and `/`: base64url carries `-` and `_`.
        (json!([{"path": "q"}]), json!({"id": "~~~", "q": "x?>"})),
        // 2^53 - 1 and its negative: the widest integers the RFC 8785 cursor text keeps exact.
        (
            json!([{"path": "n"}]),
            json!({"id": 9_007_199_254_740_991u64, "n": -9_007_199_254_740_991i64}),
        ),
        // Whole-number doubles past 2^53, which RFC 8785 writes as integer digits that are not their
        // exact value (`58088200776852270`, `1110828756322661400`): they decode as the doubles.
        (json!([{"path": "n"}]), json!({"id": "f1", "n": 5.808820077685227e16})),
        (
            json!([{"path": "n"}, {"path": "m", "desc": true}]),
            json!({"id": "f2", "n": -1.1108287563226614e18, "m": [1e20]}),
        ),
        // A decimal key: the collation is part of the effective sort, and the tuple carries the
        // row's decimal text exactly, past what a double holds.
        (
            json!([{"path": "p", "collation": "decimal"}]),
            json!({"id": "d1", "p": "12345678901234567890.123456789"}),
        ),
    ];
    let minted_rows = minted.into_iter().map(|(sort, doc)| {
        let keys: Vec<SortKey> = serde_json::from_value(sort.clone()).unwrap();
        let cursor = encode_cursor(&doc, &keys).unwrap();
        json!({
            "name": null,
            "sort": sort,
            "document": doc,
            "expected_cursor": cursor,
            "expected_effective_sort": effective_sort(&keys),
            "expected_tuple": decode_cursor(&cursor, &keys).unwrap(),
            "error": null,
        })
    });
    // Refusals, each with the kernel's own text: a cursor read under a sort it was not minted under,
    // and a row a cursor cannot be minted from.
    let replayed = encode_cursor(&json!({"id": "a", "n": 1}), &[SortKey::new("n", false)]).unwrap();
    let refused: Vec<(&str, Value, Value, Option<&str>)> = vec![
        (
            "decode_under_another_key",
            json!([{"path": "s"}]),
            Value::Null,
            Some(&replayed),
        ),
        (
            "decode_under_another_direction",
            json!([{"path": "n", "desc": true}]),
            Value::Null,
            Some(&replayed),
        ),
        (
            "decode_under_one_key_more",
            json!([{"path": "n"}, {"path": "s"}]),
            Value::Null,
            Some(&replayed),
        ),
        ("decode_without_a_sort", json!([]), Value::Null, Some(&replayed)),
        (
            "decode_standard_base64",
            json!([{"path": "n"}]),
            Value::Null,
            Some("W1tdXQ=="),
        ),
        ("encode_without_a_sort", json!([]), json!({"id": "user-3"}), None),
        (
            "encode_from_a_row_without_an_id",
            json!([{"path": "n"}]),
            json!({"n": 1}),
            None,
        ),
        (
            "encode_from_a_row_with_a_null_id",
            json!([{"path": "n"}]),
            json!({"n": 1, "id": null}),
            None,
        ),
        // Arrays and objects compare by rank only, so such ids tie the way a null id does.
        (
            "encode_from_a_row_with_an_array_id",
            json!([{"path": "n"}]),
            json!({"n": 1, "id": ["t1", 1]}),
            None,
        ),
        (
            "encode_from_a_row_with_an_object_id",
            json!([{"path": "n"}]),
            json!({"n": 1, "id": {"tenant": "t1", "n": 1}}),
            None,
        ),
        // The cursor is RFC 8785 text, whose doubles could not tell this value from its neighbour.
        (
            "encode_a_sort_value_at_2_pow_53",
            json!([{"path": "n"}]),
            json!({"n": 9_007_199_254_740_992u64, "id": "a"}),
            None,
        ),
    ];
    let refused_rows = refused.into_iter().map(|(name, sort, doc, cursor)| {
        let keys: Vec<SortKey> = serde_json::from_value(sort.clone()).unwrap();
        let error = match cursor {
            Some(cursor) => decode_cursor(cursor, &keys).unwrap_err(),
            None => encode_cursor(&doc, &keys).unwrap_err(),
        };
        json!({
            "name": name,
            "sort": sort,
            "document": doc,
            "expected_cursor": cursor,
            "expected_effective_sort": effective_sort(&keys),
            "expected_tuple": null,
            "error": error.to_string(),
        })
    });
    minted_rows.chain(refused_rows).collect()
}

/// The guard's failure settle table: one row per taxonomy code plus the uncoded failure, an empty
/// message, and a rejection's reason kept, dropped beside another code and dropped when it is no name.
/// `want` is `{"cache": <record>}`, the failed record a won claim completes with, or `"release"`.
fn idempotency_settle_cases() -> Vec<Value> {
    let hash = payload_hash(b"cmd-a");
    let codes = std::iter::once(None).chain(ErrorCode::ALL.iter().copied().map(Some));
    let rows = codes.map(|code| (code, "unknown field `amount`", None)).chain([
        (Some(ErrorCode::Codec), "", None),
        (Some(ErrorCode::Rejected), "no such order", Some("not_found")),
        (Some(ErrorCode::Codec), "unknown field `amount`", Some("not_found")),
        (Some(ErrorCode::Rejected), "no such order", Some("Not Found")),
    ]);
    rows.map(|(code, message, reason)| {
        let want = match settle_failure::<Value>(hash, code, message, reason) {
            Settle::Cache(record) => json!({ "cache": String::from_utf8(encode_record(&record).unwrap()).unwrap() }),
            Settle::Release => json!("release"),
        };
        json!({ "code": code.map(ErrorCode::as_str), "message": message, "reason": reason, "payload_hash": hash, "want": want })
    })
    .collect()
}

/// What a guard does with one dispatch, as a scenario both SDKs replay through their own guard over their
/// memory store. `prior` is what the key holds before (`null`: nothing, so this dispatch wins and runs);
/// `run` is how the handler ends (`{"result": <json>}` or `{"error": {"code", "message"}}`), `null` when
/// it must not run. `want` is what the dispatch returns (`{"result": <json>}`) or raises
/// (`{"error": <wire>}`), and `want_record` what the key holds after (`null`: released).
fn idempotency_guard_cases() -> Vec<Value> {
    let key = "req-1";
    let hash = payload_hash(b"cmd-a");
    let other = payload_hash(b"cmd-b");
    let text = |record: &Record<Value>| String::from_utf8(encode_record(record).unwrap()).unwrap();
    let result = json!({ "invoice_id": "invoice-7", "total": 5000 });
    enum Run {
        Returns(Value),
        Fails(Option<ErrorCode>, &'static str),
        Rejects(&'static str, &'static str),
    }
    let won: [(&str, Run); 8] = [
        ("a_success_is_cached", Run::Returns(result.clone())),
        ("a_null_success_is_cached_as_null", Run::Returns(Value::Null)),
        (
            "a_terminal_failure_is_cached_with_its_code",
            Run::Fails(Some(ErrorCode::Codec), "unknown field `amount`"),
        ),
        (
            "a_host_rejection_is_cached_as_rejected",
            Run::Fails(Some(ErrorCode::Rejected), "insufficient funds"),
        ),
        (
            "a_host_rejection_keeps_its_reason",
            Run::Rejects("no such order", "not_found"),
        ),
        (
            "a_transient_failure_releases_the_claim",
            Run::Fails(Some(ErrorCode::Store), "database unavailable"),
        ),
        (
            "an_unclassified_failure_releases_the_claim",
            Run::Fails(None, "connection reset"),
        ),
        // The wiring failed, not the work: a cached record would replay it after the host fixed the wiring.
        (
            "a_fatal_failure_releases_the_claim",
            Run::Fails(
                Some(ErrorCode::BadHandle),
                "kafka broker error 29: topic authorization failed",
            ),
        ),
    ];
    let won = won.into_iter().map(|(name, run)| {
        let (run, want, record) = match run {
            Run::Returns(value) => {
                let record = text(&Record::done(hash, value.clone()));
                (json!({ "result": value }), json!({ "result": value }), Some(record))
            }
            Run::Fails(code, message) => {
                let error = match code {
                    Some(code) => edax_core::Error::new(code, message).to_string(),
                    None => message.to_owned(),
                };
                let record = match settle_failure::<Value>(hash, code, message, None) {
                    Settle::Cache(record) => Some(text(&record)),
                    Settle::Release => None,
                };
                (json!({ "error": { "code": code.map(ErrorCode::as_str), "message": message, "reason": null } }), json!({ "error": error }), record)
            }
            Run::Rejects(message, reason) => {
                let error = edax_core::Error::new(ErrorCode::Rejected, message).to_string();
                let record = match settle_failure::<Value>(hash, Some(ErrorCode::Rejected), message, Some(reason)) {
                    Settle::Cache(record) => Some(text(&record)),
                    Settle::Release => None,
                };
                (json!({ "error": { "code": "rejected", "message": message, "reason": reason } }), json!({ "error": error }), record)
            }
        };
        json!({ "name": name, "key": key, "payload_hash": hash, "prior": null, "run": run, "want": want, "want_reason": null, "want_record": record })
    });
    let lost: [(&str, String, u64); 8] = [
        (
            "a_loser_replays_the_cached_result",
            text(&Record::done(hash, result.clone())),
            hash,
        ),
        (
            "a_loser_replays_the_cached_failure_with_its_code",
            text(&Record::failed(hash, ErrorCode::Codec, "unknown field `amount`")),
            hash,
        ),
        ("a_claim_in_flight_is_in_progress", text(&Record::pending(hash)), hash),
        (
            "a_reused_key_is_a_payload_mismatch",
            text(&Record::done(hash, result.clone())),
            other,
        ),
        (
            "an_unreadable_record_replays_as_a_cached_codec_failure",
            "{".to_owned(),
            hash,
        ),
        (
            "a_failure_cached_without_a_code_replays_its_message",
            format!(r#"{{"payload_hash":{hash},"error":"boom","pending":false}}"#),
            hash,
        ),
        // The replayed error keeps the code in its text and the reason beside it, as `want_reason`.
        (
            "a_loser_replays_a_rejection_with_its_reason",
            text(&Record {
                payload_hash: hash,
                state: State::Failed {
                    code: Some(ErrorCode::Rejected),
                    message: "no such order".to_owned(),
                    reason: Some("not_found".to_owned()),
                },
            }),
            hash,
        ),
        // A reused key is another command's: the stored reason is not this one's to replay.
        (
            "a_reused_key_carries_no_reason",
            text(&Record {
                payload_hash: hash,
                state: State::Failed {
                    code: Some(ErrorCode::Rejected),
                    message: "no such order".to_owned(),
                    reason: Some("not_found".to_owned()),
                },
            }),
            other,
        ),
    ];
    let lost = lost.into_iter().map(|(name, prior, current)| {
        let record = decode_prior::<Value>(prior.as_bytes(), current);
        let (want, reason) = match replay(&record, current, key) {
            Ok(value) => (json!({ "result": value }), None),
            Err(error) => {
                // Only a replayed failure carries the stored reason: never a mismatch or a claim in flight.
                let reason = match (outcome(&record, current), &record.state) {
                    (edax_cqrs::Outcome::Failed { .. }, State::Failed { reason, .. }) => reason.clone(),
                    _ => None,
                };
                (json!({ "error": error.to_string() }), reason)
            }
        };
        json!({ "name": name, "key": key, "payload_hash": current, "prior": prior, "run": null, "want": want, "want_reason": reason, "want_record": prior })
    });
    won.chain(lost).collect()
}

/// The JSON text a host value becomes in every SDK, so a document, a filter and a command hash agree
/// across languages. Each row names the value by its `kind` and its `input` text, and pins the
/// `json` an SDK's encoder writes for it, or the `error` code it refuses it with.
///
/// - `decimal`: the kernel's canonical decimal text as a JSON string.
/// - `datetime`: the kernel's fixed-width UTC text as a JSON string, so string order is instant
///   order. The input is RFC 3339 text, and a time without an offset is refused: its instant is
///   unknown. `epoch_micros` pins the instant, which this generator checks the text against before it
///   checks that the texts sort as the instants do.
/// - `date`: ISO `YYYY-MM-DD`, already one width.
/// - `unix_nanos`: nanoseconds since the Unix epoch, as a stored event's time or a clock reading, written
///   as the `datetime` form floored to the microsecond. The input is decimal text.
/// - `float`: a double as JSON writes it. JSON has no form for `NaN` or an infinity, and every SDK's
///   JSON encoder writes them as `null` (`serde_json`, Python's msgspec), so a hash, a filter and a
///   document agree on them. A field typed as a plain double cannot read that `null` back.
fn host_values_cases() -> Vec<Value> {
    let decimals: [(&str, &str); 16] = [
        ("decimal_integer", "42"),
        ("decimal_fraction", "9.5"),
        ("decimal_trailing_zeros_are_one_value", "9.50"),
        ("decimal_a_tenth_stays_exact", "0.1"),
        ("decimal_leading_zeros_go", "007.250"),
        ("decimal_negative", "-12.30"),
        ("decimal_negative_zero_is_zero", "-0.0"),
        ("decimal_zero_with_an_exponent", "0E-10"),
        ("decimal_exponent", "1E+2"),
        ("decimal_negative_exponent", "1.5E-7"),
        ("decimal_past_17_digits_stays_exact", "12345678901234567890.123456789"),
        ("decimal_past_2_pow_53_stays_exact", "9007199254740993"),
        ("decimal_refused_nan", "NaN"),
        ("decimal_refused_signalling_nan", "sNaN"),
        ("decimal_refused_infinity", "-Infinity"),
        ("decimal_refused_past_the_digit_bound", "1E+1000"),
    ];
    let decimal_rows = decimals.into_iter().map(|(name, input)| {
        let result = canonical_decimal(input);
        assert_eq!(name.contains("_refused_"), result.is_err(), "{name}");
        json!({
            "name": name,
            "kind": "decimal",
            "input": input,
            "json": result.as_ref().ok().map(|text| serde_json::to_string(text).unwrap()),
            "epoch_micros": null,
            "error": refusal(&result),
        })
    });
    // (name, input, the instant in microseconds since the epoch); a refused row has none.
    let datetimes: [(&str, &str, Option<i64>); 10] = [
        (
            "datetime_utc_whole_second_gets_a_zero_fraction",
            "2024-01-01T00:00:00+00:00",
            Some(1_704_067_200_000_000),
        ),
        (
            "datetime_z_suffix",
            "2024-01-01T00:00:00.250000Z",
            Some(1_704_067_200_250_000),
        ),
        (
            "datetime_utc_with_a_fraction",
            "2024-01-01T00:00:00.500000+00:00",
            Some(1_704_067_200_500_000),
        ),
        (
            "datetime_milliseconds_widen_to_six_digits",
            "2024-01-01T00:00:00.123+00:00",
            Some(1_704_067_200_123_000),
        ),
        (
            "datetime_microsecond_precision",
            "2024-01-01T00:00:00.000001+00:00",
            Some(1_704_067_200_000_001),
        ),
        (
            "datetime_positive_offset_converts_to_utc",
            "2024-01-01T01:30:00+01:00",
            Some(1_704_069_000_000_000),
        ),
        (
            "datetime_negative_offset_crosses_midnight",
            "2023-12-31T22:00:00-05:00",
            Some(1_704_078_000_000_000),
        ),
        (
            "datetime_offset_with_minutes",
            "2024-01-01T05:45:00.000001+05:45",
            Some(1_704_067_200_000_001),
        ),
        (
            "datetime_year_below_1000_keeps_four_digits",
            "0999-12-31T23:59:59.999999+00:00",
            Some(-30_610_224_000_000_001),
        ),
        ("datetime_refused_naive", "2024-01-01T00:00:00", None),
    ];
    let mut by_text: Vec<(String, i64)> = Vec::new();
    let datetime_rows: Vec<Value> = datetimes
        .into_iter()
        .map(|(name, input, micros)| {
            let result = canonical_timestamp(input);
            assert_eq!(micros.is_none(), result.is_err(), "{name}");
            if let (Ok(text), Some(micros)) = (&result, micros) {
                assert_eq!(
                    fixed_width_utc_micros(text),
                    micros,
                    "{name}: the text is not the pinned instant"
                );
                by_text.push((text.clone(), micros));
            }
            json!({
                "name": name,
                "kind": "datetime",
                "input": input,
                "json": result.as_ref().ok().map(|text| serde_json::to_string(text).unwrap()),
                "epoch_micros": micros,
                "error": refusal(&result),
            })
        })
        .collect();
    by_text.sort();
    assert!(
        by_text.windows(2).all(|pair| pair[0].1 <= pair[1].1),
        "string order is not instant order: {by_text:?}"
    );
    let date_row = json!({
        "name": "date_keeps_iso",
        "kind": "date",
        "input": "0999-03-07",
        "json": "\"0999-03-07\"",
        "epoch_micros": null,
        "error": null,
    });
    let floats: [(&str, &str); 4] = [
        ("float_finite", "1.5"),
        ("float_nan_is_written_as_null", "NaN"),
        ("float_infinity_is_written_as_null", "inf"),
        ("float_negative_infinity_is_written_as_null", "-inf"),
    ];
    let float_rows = floats.into_iter().map(|(name, input)| {
        // What the Rust SDK writes: its payloads, documents and command hashes go through serde_json.
        let value: f64 = input.parse().unwrap();
        json!({
            "name": name,
            "kind": "float",
            "input": input,
            "json": serde_json::to_string(&value).unwrap(),
            "epoch_micros": null,
            "error": null,
        })
    });
    // `input` is decimal text, not a JSON number: a reader that decodes numbers as doubles would round
    // one past 2^53.
    let nanos: [(&str, i64); 6] = [
        ("unix_nanos_whole_microsecond", 1_704_067_200_000_001_000),
        ("unix_nanos_floor_to_the_microsecond", 1_704_067_200_000_001_999),
        ("unix_nanos_epoch", 0),
        ("unix_nanos_before_the_epoch_floors_back", -1),
        ("unix_nanos_min", i64::MIN),
        ("unix_nanos_max", i64::MAX),
    ];
    let nanos_rows = nanos.into_iter().map(|(name, nanos)| {
        let text = timestamp_from_unix_nanos(nanos).unwrap();
        assert_eq!(canonical_timestamp(&text).unwrap(), text, "{name}: not canonical");
        let micros = nanos.div_euclid(1_000);
        assert_eq!(fixed_width_utc_micros(&text), micros, "{name}: not floored");
        json!({
            "name": name,
            "kind": "unix_nanos",
            "input": nanos.to_string(),
            "json": serde_json::to_string(&text).unwrap(),
            "epoch_micros": micros,
            "error": null,
        })
    });
    decimal_rows
        .chain(datetime_rows)
        .chain(std::iter::once(date_row))
        .chain(float_rows)
        .chain(nanos_rows)
        .collect()
}

/// Microseconds since the Unix epoch of `YYYY-MM-DDTHH:MM:SS.ffffffZ`, checking its exact shape.
/// Days from the civil date use Howard Hinnant's algorithm, valid for the proleptic Gregorian years.
fn fixed_width_utc_micros(text: &str) -> i64 {
    let bytes = text.as_bytes();
    assert_eq!(bytes.len(), 27, "{text}: not the fixed width");
    for (at, separator) in [
        (4, b'-'),
        (7, b'-'),
        (10, b'T'),
        (13, b':'),
        (16, b':'),
        (19, b'.'),
        (26, b'Z'),
    ] {
        assert_eq!(bytes[at], separator, "{text}: byte {at}");
    }
    let field = |from: usize, to: usize| -> i64 { text[from..to].parse().unwrap() };
    let (year, month, day) = (field(0, 4), field(5, 7), field(8, 10));
    let shifted = if month <= 2 { year - 1 } else { year };
    let era = shifted.div_euclid(400);
    let year_of_era = shifted - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    let seconds = days * 86_400 + field(11, 13) * 3_600 + field(14, 16) * 60 + field(17, 19);
    seconds * 1_000_000 + field(20, 26)
}

/// The fence every ledger store applies to a settling write, as a table a store that cannot call the
/// kernel (Redis's Lua) replays: one row per write and per class of what the key holds. `want` is
/// `{"ok": "apply" | "leave"}` or `{"error": <wire>}`, and `holds_after` what the key holds once the
/// write is done: a store checks its own value against it, so a fence that answered right but wrote
/// wrong still fails.
fn idempotency_fence_cases() -> Vec<Value> {
    const KEY: &str = "order-7";
    let fence_name = |fence: Fence| match fence {
        Fence::Apply => "apply",
        Fence::Leave => "leave",
    };
    let mut rows = Vec::new();
    for &holder in Holder::ALL {
        let complete = complete_fence(KEY, holder);
        // An applied complete leaves the settled record; an applied release leaves the key free.
        let after = match complete {
            Ok(Fence::Apply) => Holder::SameRecord,
            _ => holder,
        };
        let want = match complete {
            Ok(fence) => json!({ "ok": fence_name(fence) }),
            Err(error) => json!({ "error": error.to_string() }),
        };
        rows.push((
            format!("complete_over_{}", holder.as_str()),
            "complete",
            holder,
            want,
            after,
        ));
    }
    for &holder in Holder::ALL {
        let release = release_fence(holder);
        let after = if release == Fence::Apply { Holder::Free } else { holder };
        rows.push((
            format!("release_over_{}", holder.as_str()),
            "release",
            holder,
            json!({ "ok": fence_name(release) }),
            after,
        ));
    }
    rows.into_iter()
        .map(|(name, op, holds, want, after)| {
            json!({ "name": name, "op": op, "key": KEY, "holds": holds.as_str(), "want": want, "holds_after": after.as_str() })
        })
        .collect()
}

fn ingest_cases() -> Vec<Value> {
    use edax_cqrs::ingest::{self, Contract, Input, Rejection, Stored};
    use edax_cqrs::namespace;
    use std::collections::BTreeMap;

    let target = "orders-v1";
    let input = |offset: i64, sequence: Option<u64>, status: &str| {
        let raw = json!({"offset":offset,"sequence":sequence,"status":status}).to_string();
        Input {
            source: ingest::source_key(target, "cluster-a/orders-v1", "orders", 0, offset).unwrap(),
            key: namespace::key(target, &json!(["upsert", "acme", null, "order-42"]).to_string()).unwrap(),
            stream: "order-42".into(),
            event_id: format!("order-42-event-{offset}"),
            sequence,
            content: json!({"aggregate_id":"order-42", "sequence":sequence, "name":"OrderState", "version":1, "status":status}),
            private: Some(json!({"order_id":"order-42", "status":status})),
            public: None,
            erased: false,
            fingerprint: None,
            raw_fingerprint: ingest::raw_fingerprint(raw.as_bytes()),
            raw,
            failure: None,
        }
    };
    let empty = BTreeMap::new();
    let no_rejections = BTreeMap::new();
    let mut rows = Vec::new();
    let mut add = |name: &str,
                   mode: &str,
                   inputs: Vec<Input>,
                   stored: &BTreeMap<String, Stored>,
                   rejected: &BTreeMap<String, Rejection>| {
        let result = ingest::plan(mode, &inputs, stored, rejected);
        rows.push(json!({
            "name":name, "op":"plan", "mode":mode, "inputs":inputs, "stored":stored,
            "rejected":rejected, "want":result.as_ref().ok(), "error":refusal(&result),
        }));
    };
    let created = input(0, Some(1), "created");
    let paid = input(1, Some(2), "paid");
    let shipped = input(2, Some(3), "shipped");
    let other_stream = Input {
        stream: "order-99".into(),
        ..input(3, Some(4), "paid")
    };
    let paid_state = ingest::plan("upsert", std::slice::from_ref(&paid), &empty, &no_rejections).unwrap();
    let mut conflicting_paid = input(4, Some(2), "cancelled");
    // Keep raw identity different while using the same committed sequence and target.
    conflicting_paid.event_id = "order-42-conflicting-payment".into();
    let mut append = created.clone();
    append.sequence = None;
    append.key = namespace::key(target, &json!(["append", "acme", null, append.event_id]).to_string()).unwrap();
    let append_state = ingest::plan("append", std::slice::from_ref(&append), &empty, &no_rejections).unwrap();
    let append_conflict = Input {
        private: Some(json!({"order_id":"order-42","status":"cancelled"})),
        ..append.clone()
    };
    for (name, nul) in [
        (
            "nul_in_value_rejected_as_record",
            Input {
                private: Some(json!({"order_id":"order-42","note":"a\u{0}b"})),
                ..paid.clone()
            },
        ),
        (
            "nul_in_stream_rejected_as_record",
            Input {
                stream: "order\u{0}42".into(),
                ..paid.clone()
            },
        ),
    ] {
        add(name, "upsert", vec![nul], &empty, &no_rejections);
    }
    add(
        "append_new_event_without_sequence",
        "append",
        vec![append.clone()],
        &empty,
        &no_rejections,
    );
    add(
        "append_exact_replay",
        "append",
        vec![append.clone()],
        &append_state.writes,
        &no_rejections,
    );
    add(
        "append_conflicting_replay",
        "append",
        vec![append_conflict],
        &append_state.writes,
        &no_rejections,
    );
    add(
        "append_duplicate_in_batch",
        "append",
        vec![append.clone(), append],
        &empty,
        &no_rejections,
    );
    add(
        "upsert_absent_key_first_owner_wins",
        "upsert",
        vec![created.clone(), other_stream.clone()],
        &empty,
        &no_rejections,
    );
    add(
        "upsert_lower_sequence_ignored",
        "upsert",
        vec![created.clone()],
        &paid_state.writes,
        &no_rejections,
    );
    add(
        "upsert_equal_sequence_exact_replay",
        "upsert",
        vec![paid.clone()],
        &paid_state.writes,
        &no_rejections,
    );
    add(
        "upsert_equal_sequence_conflict",
        "upsert",
        vec![conflicting_paid.clone()],
        &paid_state.writes,
        &no_rejections,
    );
    add(
        "upsert_higher_sequence",
        "upsert",
        vec![shipped.clone()],
        &paid_state.writes,
        &no_rejections,
    );
    add(
        "equal_new_sequence_first_value_wins",
        "upsert",
        vec![paid.clone(), conflicting_paid.clone()],
        &empty,
        &no_rejections,
    );
    add(
        "compaction_preserves_original_outcomes",
        "upsert",
        vec![shipped.clone(), created.clone(), paid.clone()],
        &empty,
        &no_rejections,
    );
    add(
        "stale_conflicts_are_ignored",
        "upsert",
        vec![paid.clone(), conflicting_paid.clone()],
        &ingest::plan("upsert", std::slice::from_ref(&shipped), &empty, &no_rejections)
            .unwrap()
            .writes,
        &no_rejections,
    );
    let erased = Input {
        erased: true,
        private: None,
        ..input(5, Some(4), "erased")
    };
    add(
        "erased_input_accepted_without_write",
        "upsert",
        vec![erased.clone()],
        &paid_state.writes,
        &no_rejections,
    );
    add(
        "erased_input_never_creates_a_row",
        "upsert",
        vec![erased],
        &empty,
        &no_rejections,
    );
    let sealed_key = [9u8; 32];
    let keyed = |input: Input| {
        let input = Input {
            public: Some(json!({"status":"paid"})),
            ..input
        };
        let fingerprint = ingest::keyed_fingerprint(
            &sealed_key,
            &input.content,
            input.private.as_ref(),
            input.public.as_ref(),
        )
        .unwrap();
        Input {
            fingerprint: Some(fingerprint),
            ..input
        }
    };
    let sealed_paid = keyed(paid.clone());
    let sealed_state = ingest::plan("upsert", std::slice::from_ref(&sealed_paid), &empty, &no_rejections).unwrap();
    add(
        "keyed_fingerprint_is_stored",
        "upsert",
        vec![sealed_paid.clone()],
        &empty,
        &no_rejections,
    );
    add(
        "keyed_fingerprint_exact_replay",
        "upsert",
        vec![sealed_paid],
        &sealed_state.writes,
        &no_rejections,
    );
    add(
        "unsealed_replay_of_a_sealed_sequence_conflicts",
        "upsert",
        vec![paid.clone()],
        &sealed_state.writes,
        &no_rejections,
    );
    let tombstone = Input {
        private: None,
        ..input(5, Some(4), "deleted")
    };
    let deleted = ingest::plan(
        "upsert",
        std::slice::from_ref(&tombstone),
        &paid_state.writes,
        &no_rejections,
    )
    .unwrap();
    add(
        "tombstone_retains_owner_and_sequence",
        "upsert",
        vec![tombstone.clone()],
        &paid_state.writes,
        &no_rejections,
    );
    add(
        "tombstone_exact_replay",
        "upsert",
        vec![tombstone],
        &deleted.writes,
        &no_rejections,
    );
    add(
        "tombstone_rejects_new_owner",
        "upsert",
        vec![other_stream.clone()],
        &deleted.writes,
        &no_rejections,
    );
    add(
        "tombstone_ignores_stale_resurrection",
        "upsert",
        vec![created.clone()],
        &deleted.writes,
        &no_rejections,
    );
    add(
        "missing_sequence_rejected",
        "upsert",
        vec![input(6, None, "created")],
        &empty,
        &no_rejections,
    );
    add(
        "postgres_sequence_overflow_rejected",
        "upsert",
        vec![input(7, Some(i64::MAX as u64 + 1), "created")],
        &empty,
        &no_rejections,
    );
    add(
        "zero_sequence_is_valid",
        "upsert",
        vec![input(8, Some(0), "created")],
        &empty,
        &no_rejections,
    );
    let invalid = Input {
        failure: Some("codec: order status is malformed".into()),
        ..created.clone()
    };
    let rejected = ingest::plan("upsert", std::slice::from_ref(&invalid), &empty, &no_rejections).unwrap();
    add(
        "invalid_original_does_not_propose_owner",
        "upsert",
        vec![invalid, other_stream.clone()],
        &empty,
        &no_rejections,
    );
    add(
        "durable_rejection_excluded_before_owner_selection",
        "upsert",
        vec![created.clone(), other_stream.clone()],
        &empty,
        &rejected.rejections,
    );
    let mut published = rejected.rejections.clone();
    for rejection in published.values_mut() {
        rejection.published = true;
    }
    add(
        "published_rejection_still_excludes_original",
        "upsert",
        vec![created.clone(), other_stream],
        &empty,
        &published,
    );
    let first_batch = vec![paid.clone(), conflicting_paid.clone(), shipped.clone()];
    let first_result = ingest::plan("upsert", &first_batch, &empty, &no_rejections).unwrap();
    add(
        "commit_before_crash",
        "upsert",
        first_batch.clone(),
        &empty,
        &no_rejections,
    );
    add(
        "replay_after_commit_keeps_rejection_despite_compaction",
        "upsert",
        first_batch,
        &first_result.writes,
        &first_result.rejections,
    );
    let reused_source = Input {
        raw_fingerprint: ingest::raw_fingerprint(b"changed-source-record"),
        raw: "changed-source-record".into(),
        ..created.clone()
    };
    add(
        "durable_rejection_detects_source_reuse",
        "upsert",
        vec![reused_source.clone()],
        &empty,
        &rejected.rejections,
    );
    add(
        "source_reuse_in_batch_refuses_entire_call",
        "upsert",
        vec![created.clone(), reused_source],
        &empty,
        &no_rejections,
    );
    let original_digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let full_record = Input {
        raw_fingerprint: original_digest.into(),
        failure: Some("codec: order status is malformed".into()),
        ..created.clone()
    };
    let stub_record = Input {
        raw: json!({"topic":"orders","partition":0,"offset":0,"oversized":true,"sha256":original_digest}).to_string(),
        ..full_record.clone()
    };
    let full_rejection = ingest::plan("upsert", std::slice::from_ref(&full_record), &empty, &no_rejections).unwrap();
    let stub_rejection = ingest::plan("upsert", std::slice::from_ref(&stub_record), &empty, &no_rejections).unwrap();
    add(
        "source_digest_survives_full_to_stub_replay",
        "upsert",
        vec![stub_record.clone()],
        &empty,
        &full_rejection.rejections,
    );
    add(
        "source_digest_survives_stub_to_full_replay",
        "upsert",
        vec![full_record.clone()],
        &empty,
        &stub_rejection.rejections,
    );
    add(
        "source_digest_survives_full_and_stub_in_batch",
        "upsert",
        vec![full_record.clone(), stub_record.clone()],
        &empty,
        &no_rejections,
    );
    let changed_digest = Input {
        raw_fingerprint: "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210".into(),
        ..stub_record
    };
    add(
        "source_digest_change_refused_on_replay",
        "upsert",
        vec![changed_digest.clone()],
        &empty,
        &full_rejection.rejections,
    );
    add(
        "source_digest_change_refuses_entire_batch",
        "upsert",
        vec![full_record, changed_digest],
        &empty,
        &no_rejections,
    );
    add(
        "unknown_mode_refused",
        "fold",
        vec![created.clone()],
        &empty,
        &no_rejections,
    );
    add("empty_batch_no_changes", "upsert", vec![], &empty, &no_rejections);

    let contract = Contract {
        mode: "upsert".into(),
        generation: target.into(),
        source_incarnation: "cluster-a/orders-v1".into(),
        mapper_id: "order-state-v1".into(),
        at_rest: "sealed".into(),
    };
    for (name, previous, expected) in [
        ("bind_new_target", None, contract.clone()),
        ("bind_same_target", Some(contract.clone()), contract.clone()),
        (
            "changed_mapper_refused",
            Some(contract.clone()),
            Contract {
                mapper_id: "order-state-v2".into(),
                ..contract.clone()
            },
        ),
        (
            "changed_source_refused",
            Some(contract.clone()),
            Contract {
                source_incarnation: "cluster-b/orders-v1".into(),
                ..contract.clone()
            },
        ),
        (
            "changed_at_rest_refused",
            Some(contract.clone()),
            Contract {
                at_rest: "plaintext".into(),
                ..contract.clone()
            },
        ),
        (
            "unsupported_at_rest_refused",
            None,
            Contract {
                at_rest: "hashed".into(),
                ..contract.clone()
            },
        ),
        (
            "unsupported_contract_mode",
            None,
            Contract {
                mode: "fold".into(),
                ..contract.clone()
            },
        ),
        (
            "empty_generation_refused",
            None,
            Contract {
                generation: String::new(),
                ..contract.clone()
            },
        ),
    ] {
        let result = ingest::bind_contract(previous.as_ref(), &expected).map(|()| true);
        rows.push(json!({"name":name,"op":"bind","previous":previous,"expected":expected,"want":result.as_ref().ok(),"error":refusal(&result)}));
    }
    for (name, inputs) in [
        ("valid_scoped_input", vec![created.clone()]),
        (
            "source_offset_at_json_safe_integer_boundary",
            vec![Input {
                source: ingest::source_key(target, "cluster-a/orders-v1", "orders", 0, 9_007_199_254_740_992).unwrap(),
                ..created.clone()
            }],
        ),
        (
            "source_offset_at_postgres_bigint_maximum",
            vec![Input {
                source: ingest::source_key(target, "cluster-a/orders-v1", "orders", 0, i64::MAX).unwrap(),
                ..created.clone()
            }],
        ),
        (
            "foreign_target_refused",
            vec![Input {
                key: namespace::key("orders-v2", r#"["upsert","acme",null,"order-42"]"#).unwrap(),
                ..created.clone()
            }],
        ),
        (
            "foreign_source_incarnation_refused",
            vec![Input {
                source: ingest::source_key(target, "other-cluster", "orders", 0, 0).unwrap(),
                ..created.clone()
            }],
        ),
        (
            "noncanonical_identity_refused",
            vec![Input {
                key: format!(
                    "{}[\"upsert\", \"acme\", \"order-42\"]",
                    namespace::prefix(target).unwrap()
                ),
                ..created.clone()
            }],
        ),
        (
            "empty_stream_refused",
            vec![Input {
                stream: String::new(),
                ..created.clone()
            }],
        ),
        (
            "row_mode_must_match_contract",
            vec![Input {
                key: ingest::row_key(target, "append", Some("acme"), None, &created.event_id).unwrap(),
                ..created.clone()
            }],
        ),
        (
            "source_partition_above_i32_maximum_refused",
            vec![Input {
                source: namespace::key(
                    target,
                    &json!(["source", "cluster-a/orders-v1", "orders", i64::from(i32::MAX) + 1, "0"]).to_string(),
                )
                .unwrap(),
                ..created.clone()
            }],
        ),
        (
            "non_string_tenant_refused",
            vec![Input {
                key: namespace::key(target, &json!(["upsert", 42, null, "order-42"]).to_string()).unwrap(),
                ..created.clone()
            }],
        ),
        (
            "empty_row_id_refused",
            vec![Input {
                key: namespace::key(target, &json!(["upsert", "acme", null, ""]).to_string()).unwrap(),
                ..created.clone()
            }],
        ),
        (
            "empty_event_id_refused",
            vec![Input {
                event_id: String::new(),
                ..created.clone()
            }],
        ),
        (
            "uppercase_raw_fingerprint_refused",
            vec![Input {
                raw_fingerprint: created.raw_fingerprint.to_uppercase(),
                ..created.clone()
            }],
        ),
        (
            "short_raw_fingerprint_refused",
            vec![Input {
                raw_fingerprint: "0123456789abcdef".into(),
                ..created.clone()
            }],
        ),
        (
            "short_keyed_fingerprint_refused",
            vec![Input {
                fingerprint: Some("0123456789abcdef".into()),
                ..created.clone()
            }],
        ),
        (
            "uppercase_keyed_fingerprint_refused",
            vec![Input {
                fingerprint: Some(created.raw_fingerprint.to_uppercase()),
                ..created.clone()
            }],
        ),
        (
            "empty_subject_refused",
            vec![Input {
                key: namespace::key(target, &json!(["upsert", "acme", "", "order-42"]).to_string()).unwrap(),
                ..created.clone()
            }],
        ),
        (
            "non_string_subject_refused",
            vec![Input {
                key: namespace::key(target, &json!(["upsert", "acme", 42, "order-42"]).to_string()).unwrap(),
                ..created.clone()
            }],
        ),
        (
            "pre_subject_row_key_refused",
            vec![Input {
                key: namespace::key(target, &json!(["upsert", "acme", "order-42"]).to_string()).unwrap(),
                ..created.clone()
            }],
        ),
    ] {
        let result = ingest::validate_inputs(&contract, &inputs).map(|()| true);
        rows.push(json!({"name":name,"op":"validate","contract":contract,"inputs":inputs,"want":result.as_ref().ok(),"error":refusal(&result)}));
    }
    // A sealed target stores a subject's private part only as its protected-state envelope.
    let envelope = |tenant: &str, subject: &str| -> Value {
        let encryption = edax_seal::EncryptionInfo {
            format_version: 1,
            key_namespace: "people".into(),
            key_id: "generation-7".into(),
            cipher: edax_seal::Cipher::XChaCha20Poly1305,
            nonce: vec![0x24; edax_seal::Cipher::XChaCha20Poly1305.nonce_len()],
        };
        let state = edax_event::sealing::ProtectedState::new(
            Some(tenant.into()),
            Some(subject.into()),
            false,
            encryption,
            vec![0x5a; 40],
        );
        serde_json::from_str(&edax_event::sealing::encode_protected_state(&state).unwrap()).unwrap()
    };
    let personal = Input {
        key: ingest::row_key(target, "upsert", Some("acme"), Some("person-7"), "order-42").unwrap(),
        fingerprint: Some("ab".repeat(32)),
        private: Some(envelope("acme", "person-7")),
        ..created.clone()
    };
    let plaintext_contract = Contract {
        at_rest: "plaintext".into(),
        ..contract.clone()
    };
    for (name, contract, input) in [
        ("sealed_subject_row_accepted", &contract, personal.clone()),
        (
            "sealed_subject_plaintext_private_refused",
            &contract,
            Input {
                private: Some(json!({"email":"person-7@example.com"})),
                ..personal.clone()
            },
        ),
        (
            "sealed_subject_without_keyed_fingerprint_refused",
            &contract,
            Input {
                fingerprint: None,
                ..personal.clone()
            },
        ),
        (
            "envelope_of_another_subject_refused",
            &contract,
            Input {
                private: Some(envelope("acme", "person-8")),
                ..personal.clone()
            },
        ),
        (
            "envelope_of_another_tenant_refused",
            &contract,
            Input {
                private: Some(envelope("globex", "person-7")),
                ..personal.clone()
            },
        ),
        (
            "sealed_deletion_marker_accepted",
            &contract,
            Input {
                private: None,
                public: None,
                ..personal.clone()
            },
        ),
        (
            "erased_subject_row_accepted",
            &contract,
            Input {
                private: None,
                fingerprint: None,
                erased: true,
                ..personal.clone()
            },
        ),
        (
            "refused_subject_row_accepted",
            &contract,
            Input {
                private: None,
                fingerprint: None,
                failure: Some("codec: sealed record refused".into()),
                ..personal.clone()
            },
        ),
        ("row_without_subject_stays_plaintext", &contract, created.clone()),
        (
            "plaintext_target_keeps_subject_private",
            &plaintext_contract,
            Input {
                private: Some(json!({"email":"person-7@example.com"})),
                fingerprint: None,
                ..personal.clone()
            },
        ),
    ] {
        let inputs = vec![input];
        let result = ingest::check_sealed(contract, &inputs).map(|()| true);
        rows.push(json!({"name":name,"op":"check_sealed","contract":contract,"inputs":inputs,"want":result.as_ref().ok(),"error":refusal(&result)}));
    }
    let append_contract = Contract {
        mode: "append".into(),
        ..contract.clone()
    };
    for (name, row_id) in [
        ("valid_append_contract_input", created.event_id.as_str()),
        ("append_row_id_must_equal_event_id", "different-event"),
    ] {
        let inputs = vec![Input {
            key: ingest::row_key(target, "append", Some("acme"), None, row_id).unwrap(),
            ..created.clone()
        }];
        let result = ingest::validate_inputs(&append_contract, &inputs).map(|()| true);
        rows.push(json!({"name":name,"op":"validate","contract":append_contract,"inputs":inputs,"want":result.as_ref().ok(),"error":refusal(&result)}));
    }
    for (name, offset) in [
        ("numeric_source_offset_refused", json!(0)),
        ("leading_zero_source_offset_refused", json!("00")),
        ("positive_sign_source_offset_refused", json!("+1")),
        ("negative_source_offset_refused", json!("-1")),
        ("negative_zero_source_offset_refused", json!("-0")),
        ("whitespace_source_offset_refused", json!(" 1")),
        ("source_offset_beyond_bigint_refused", json!("9223372036854775808")),
    ] {
        let inputs = vec![Input {
            source: namespace::key(
                target,
                &json!(["source", "cluster-a/orders-v1", "orders", 0, offset]).to_string(),
            )
            .unwrap(),
            ..created.clone()
        }];
        let result = ingest::validate_inputs(&contract, &inputs).map(|()| true);
        rows.push(json!({"name":name,"op":"validate","contract":contract,"inputs":inputs,"want":result.as_ref().ok(),"error":refusal(&result)}));
    }
    for (name, partition, offset) in [
        ("source_offset_zero", 0, 0),
        ("source_offset_last_json_safe_integer", 0, 9_007_199_254_740_991),
        ("source_offset_two_to_53", 0, 9_007_199_254_740_992),
        ("source_offset_two_to_53_plus_one", 0, 9_007_199_254_740_993),
        ("source_offset_bigint_maximum", i32::MAX, i64::MAX),
        ("source_negative_offset_refused", 0, -1),
        ("source_bigint_minimum_refused", 0, i64::MIN),
        ("source_negative_partition_refused", -1, 0),
    ] {
        let incarnation = "cluster-a/orders-v1";
        let topic = "orders";
        let result = ingest::source_key(target, incarnation, topic, partition, offset);
        rows.push(json!({"name":name,"op":"source","target":target,"incarnation":incarnation,"topic":topic,"partition":partition,"offset":offset,"want":result.as_ref().ok(),"error":refusal(&result)}));
    }
    for (name, target, mode, tenant, subject, row_id) in [
        ("row_key_upsert", "orders-v1", "upsert", Some("acme"), None, "order-42"),
        ("row_key_append", "orders-v1", "append", Some("acme"), None, "event-42"),
        ("row_key_absent_tenant", "orders-v1", "upsert", None, None, "order-42"),
        (
            "row_key_empty_tenant",
            "orders-v1",
            "upsert",
            Some(""),
            None,
            "order-42",
        ),
        (
            "row_key_nul_tenant",
            "orders-v1",
            "upsert",
            Some("acme\0store"),
            None,
            "order-42",
        ),
        ("row_key_nul_row", "orders-v1", "upsert", None, None, "order\0-42"),
        (
            "row_key_unicode",
            "naročila-v1",
            "upsert",
            Some("trgovina-ž"),
            None,
            "naročilo-42",
        ),
        ("row_key_empty_row_refused", "orders-v1", "upsert", None, None, ""),
        ("row_key_empty_event_refused", "orders-v1", "append", None, None, ""),
        (
            "row_key_invalid_mode_refused",
            "orders-v1",
            "fold",
            None,
            None,
            "order-42",
        ),
        ("row_key_empty_target_refused", "", "upsert", None, None, "order-42"),
        (
            "row_key_nul_target_refused",
            "orders\0v1",
            "upsert",
            None,
            None,
            "order-42",
        ),
        (
            "row_key_with_subject",
            "orders-v1",
            "upsert",
            Some("acme"),
            Some("person-42"),
            "order-42",
        ),
        (
            "row_key_subject_without_tenant",
            "orders-v1",
            "upsert",
            None,
            Some("person-42"),
            "order-42",
        ),
        (
            "row_key_empty_subject_refused",
            "orders-v1",
            "upsert",
            Some("acme"),
            Some(""),
            "order-42",
        ),
        (
            "row_key_nul_subject_refused",
            "orders-v1",
            "upsert",
            Some("acme"),
            Some("person\0-42"),
            "order-42",
        ),
    ] {
        let result = ingest::row_key(target, mode, tenant, subject, row_id);
        rows.push(json!({"name":name,"op":"row_key","target":target,"mode":mode,"tenant":tenant,"subject":subject,"row_id":row_id,"want":result.as_ref().ok(),"error":refusal(&result)}));
    }
    for (name, mode, tenant, subject) in [
        ("scope_range_of_a_subject", "upsert", Some("acme"), "person-42"),
        ("scope_range_without_tenant", "append", None, "person-42"),
        ("scope_range_empty_subject_refused", "upsert", Some("acme"), ""),
        ("scope_range_invalid_mode_refused", "fold", Some("acme"), "person-42"),
    ] {
        let result = ingest::scope_range("orders-v1", mode, tenant, subject);
        rows.push(json!({"name":name,"op":"scope_range","target":"orders-v1","mode":mode,"tenant":tenant,"subject":subject,"want":result.as_ref().ok(),"error":refusal(&result)}));
    }
    let key = [7u8; 32];
    for (name, key, private) in [
        ("keyed_fingerprint", key.as_slice(), json!({"email":"ana@example.com"})),
        (
            "keyed_fingerprint_other_value",
            key.as_slice(),
            json!({"email":"bob@example.com"}),
        ),
        (
            "keyed_fingerprint_short_key_refused",
            &key[..16],
            json!({"email":"ana@example.com"}),
        ),
    ] {
        let content = json!({"name":"OrderPlaced","aggregate_id":"order-42"});
        let public = json!({"status":"paid"});
        let result = ingest::keyed_fingerprint(key, &content, Some(&private), Some(&public));
        rows.push(json!({"name":name,"op":"keyed_fingerprint","key":key,"state":{"content":content,"private":private,"public":public},"want":result.as_ref().ok(),"error":refusal(&result)}));
    }
    for (set, names) in [
        ("ingest_mode", ingest::Mode::NAMES),
        ("ingest_outcome", ingest::Outcome::NAMES),
    ] {
        rows.push(json!({"name":format!("{set}_names"),"op":"names","set":set,"want":names,"error":null}));
    }
    rows
}

#[test]
fn golden_ingest() {
    write_or_check("ingest.json", ingest_cases());
}

#[test]
fn golden() {
    write_or_check("idempotency_fence.json", idempotency_fence_cases());
    write_or_check("host_values.json", host_values_cases());
    write_or_check("filter.json", filter_cases());
    write_or_check("query.json", query_cases());
    write_or_check("cursor.json", cursor_cases());
    write_or_check("canonical_payload_hash.json", canonical_payload_hash_cases());
    write_or_check("canonical_json.json", canonical_json_cases());
    write_or_check("command_hash.json", command_hash_cases());
    write_or_check("command_received_key.json", received_key_cases());
    write_or_check("claim_key.json", claim_key_cases());
    write_or_check("idempotency_record.json", idempotency_record_cases());
    write_or_check("idempotency_settle.json", idempotency_settle_cases());
    write_or_check("idempotency_guard.json", idempotency_guard_cases());
    write_or_check("projection_document.json", projection_document_cases());
}

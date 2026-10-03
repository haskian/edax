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

use edax_cqrs::query::{decode_cursor, effective_sort, encode_cursor, query_json, SortKey};
use edax_cqrs::eval_json;
use edax_core::ErrorCode;
use edax_cqrs::{
    cached_failure, canonical_json, command_hash, decode_prior, decode_record, encode_record, outcome, payload_hash, Outcome,
    Record, State,
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
        r#"{"n":9007199254740992}"#,
        r#"{"n":9007199254740992.0}"#,
        r#"{"k":"v","items":[3,1,2]}"#,
        r#"{"outer":{"z":1,"a":2}}"#,
        r#"{}"#,
        r#"{"n":9007199254740993}"#,
        r#"{"n":-9007199254740993}"#,
        r#"{"n":1e400}"#,
        r#"{"s":"\ud800"}"#,
    ];
    let hash = |input: &str| canonical_json(input.as_bytes()).map(|canon| payload_hash(&canon));
    for (a, b) in [(2, 3), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13), (14, 15), (16, 17), (18, 19)] {
        assert_eq!(hash(rows[a]), hash(rows[b]), "{} and {} are one payload", rows[a], rows[b]);
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
/// order, number spelling, escaping — and the kernel's own rule: an integer literal past 2^53, which
/// a double would round, is refused.
fn canonical_json_cases() -> Vec<Value> {
    [
        ("keys_inserted_out_of_order", r#"{"b":2,"a":1,"c":{"z":0,"y":[{"q":1,"p":2}]}}"#),
        ("keys_sort_by_utf16_code_units", r#"{"ｱ":1,"🦀":2,"é":3,"z":4,"Z":5,"a":6,"aa":7}"#),
        ("whitespace_is_dropped", " { \"a\" : [ 1 , 2 ] }\n"),
        ("repeated_key_keeps_the_last_value", r#"{"a":1,"a":2}"#),
        ("int_and_whole_float_are_one_number", r#"{"n":[2,2.0,2e0,20e-1,2E+0]}"#),
        ("negative_zero_is_zero", r#"{"n":[-0,-0.0]}"#),
        ("fractions_are_shortest_round_trip", r#"{"n":[1.10,0.1,1.5e-7,0.000001,1e2,123456.789]}"#),
        ("integers_at_2_pow_53_stay_exact", r#"{"n":[9007199254740992,-9007199254740992]}"#),
        ("integer_past_2_pow_53_is_refused", r#"{"n":9007199254740993}"#),
        ("negative_integer_past_2_pow_53_is_refused", r#"{"n":-9007199254740993}"#),
        ("u64_max_is_refused", r#"{"n":18446744073709551615}"#),
        ("nested_wide_integer_is_refused", r#"{"a":[{"b":[1,9007199254740993]}]}"#),
        ("float_past_2_pow_53_rounds_as_a_double", r#"{"n":9007199254740993.0}"#),
        ("whole_float_below_1e21_is_written_out", r#"{"n":[1e20,-1e20]}"#),
        ("float_from_1e21_takes_an_exponent", r#"{"n":[1e21,1.5e300,-1e21]}"#),
        ("escapes_are_decoded", r#"{"s":"\u00e9\/\ud83e\udd80"}"#),
        ("control_characters_are_escaped", r#"{"s":"\b\f\n\r\t\u0001\u001f\"\\"}"#),
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
        Err(error) => json!({ "name": name, "input": input, "canonical": null, "hash": null, "error": error.to_string() }),
    })
    .collect()
}

fn command_hash_cases() -> Vec<Value> {
    // The hash *input* rule: the whole command value, canonicalized. Each case is a command as its
    // field map, and both SDK suites rebuild it as their own command type (a serde struct, a
    // dataclass), dispatch it through their real `idempotent` middleware, and compare the hash the
    // store received. `type` is routing metadata: it names the handler and is not a field.
    //
    // The base fixture carries `request_id`, the field the idempotency key is read from. A hash over
    // the business body alone would leave it out; the first case pins that it is hashed.
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
    let cases: [(&str, Value); 10] = [
        ("whole_command_including_the_idempotency_key_field", base.clone()),
        // Array order is data, so swapping two tags is a different command.
        ("array_order_is_data", with(json!({ "tags": ["paid", "eu"] }))),
        // Numbers hash by value: a whole-number float is the integer, so `1.0` here and `1` from an
        // encoder that drops the fraction bind to one hash.
        ("whole_number_float_hashes_as_the_integer", with(json!({ "weight_kg": 1.0 }))),
        // The same float field as an encoder that drops the fraction writes it: the same hash.
        ("whole_number_float_given_as_an_int", with(json!({ "weight_kg": 1 }))),
        // 2^53, the widest integer a double (and so RFC 8785) keeps exact: it hashes as itself.
        ("integer_at_2_pow_53_stays_exact", with(json!({ "reference": 9_007_199_254_740_992u64 }))),
        // One past it a double rounds, so two ids one apart would hash alike: refused as `codec`, in
        // every SDK, before any handler runs. Above `i64::MAX` too, where a signed reader also fails.
        ("integer_past_2_pow_53_is_refused", with(json!({ "reference": 9_007_199_254_740_993u64 }))),
        ("u64_above_i64_max_is_refused", with(json!({ "reference": 9_223_372_036_854_775_809u64 }))),
        // An optional field that is unset is serialized as null, not omitted.
        ("unset_optional_is_null_not_omitted", with(json!({ "note": null }))),
        // Hashed as UTF-8 bytes: an encoder that escapes to \u00e9 would diverge.
        ("non_ascii_hashes_as_utf8", with(json!({ "customer": "café ☕ 🦀" }))),
        ("empty_array_and_null_nested_field", with(json!({ "tags": [], "shipping": { "city": "NYC", "zip": null } }))),
    ];
    let hashes: std::collections::BTreeMap<&str, Result<u64, edax_core::Error>> =
        cases.iter().map(|(name, command)| (*name, command_hash(command))).collect();
    assert_eq!(
        hashes["whole_number_float_hashes_as_the_integer"], hashes["whole_number_float_given_as_an_int"],
        "a whole float and its integer spelling are one command"
    );
    cases
        .into_iter()
        .map(|(name, command)| match command_hash(&command) {
            Ok(expected) => json!({ "name": name, "type": "order.place", "command": command, "expected": expected, "error": null }),
            Err(error) => json!({ "name": name, "type": "order.place", "command": command, "expected": null, "error": error.to_string() }),
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
        Case {
            name: "cached_failure",
            stored: stored(&Record::failed(hash, ErrorCode::Codec, "unknown field `amount`")),
            current: hash,
        },
        // A host's business rejection, cached with its own code. This is what the Rust SDK's guard
        // writes for a `Terminal` failure, and the Python suite reads its code back.
        Case {
            name: "cached_rejection_written_by_rust",
            stored: stored(&Record::failed(hash, ErrorCode::Rejected, "insufficient funds: balance 300, withdrawal 500")),
            current: hash,
        },
        // An empty message is still a failure: only `null` means "not failed".
        Case { name: "cached_failure_with_empty_message", stored: stored(&Record::failed(hash, ErrorCode::Codec, "")), current: hash },
        // A writer that knows a newer code: the failure still reads, with no code.
        Case {
            name: "unknown_error_code_reads_as_no_code",
            stored: format!(r#"{{"payload_hash":{hash},"result":null,"error":"boom","error_code":"quota_exceeded","pending":false}}"#),
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
        Case { name: "payload_mismatch_while_pending", stored: stored(&Record::pending(hash)), current: other },
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
        Case { name: "stored_zero_hash_mismatches", stored: stored(&Record::done(0, json!(42))), current: hash },
        Case { name: "stored_zero_hash_replays_against_zero", stored: stored(&Record::done(0, json!(42))), current: 0 },
    ];
    // Bytes no reader can decode. Each still holds its key: `decode_prior` reads it as a terminal
    // `codec` failure, and `decode_error` pins the strict reader's refusal. The message is the JSON
    // parser's own text, so it is pinned too. A row whose hash does not read is claimed with `other`,
    // so the failure visibly carries the claimant's hash; a row whose hash reads keeps the stored one.
    let unreadable = [
        Case { name: "not_an_object", stored: "[1,null,null]".to_owned(), current: other },
        Case { name: "missing_payload_hash", stored: r#"{"result":null,"error":null,"pending":false}"#.to_owned(), current: other },
        Case { name: "float_payload_hash", stored: format!(r#"{{"payload_hash":{hash}.0,"pending":false}}"#), current: other },
        Case { name: "negative_payload_hash", stored: r#"{"payload_hash":-1,"pending":false}"#.to_owned(), current: other },
        Case { name: "string_payload_hash", stored: format!(r#"{{"payload_hash":"{hash}","pending":false}}"#), current: other },
        Case { name: "payload_hash_past_u64", stored: r#"{"payload_hash":18446744073709551616,"pending":false}"#.to_owned(), current: other },
        Case { name: "non_string_error", stored: format!(r#"{{"payload_hash":{hash},"error":42,"pending":false}}"#), current: hash },
        Case { name: "non_string_error_code", stored: format!(r#"{{"payload_hash":{hash},"error":"boom","error_code":7,"pending":false}}"#), current: hash },
        Case { name: "null_pending", stored: format!(r#"{{"payload_hash":{hash},"pending":null}}"#), current: hash },
        // A state the wire does not know: `pending` is a bool, never a word.
        Case { name: "unknown_pending_state", stored: format!(r#"{{"payload_hash":{hash},"pending":"settling"}}"#), current: hash },
        Case { name: "not_json", stored: "{".to_owned(), current: other },
        Case { name: "garbage", stored: "\u{1}\u{7f}edax".to_owned(), current: other },
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
                "state": record.state.as_str(),
                "outcome": outcome(&record, c.current).as_str(),
                "encoded": stored(&record),
            });
            match decode_record::<Value>(c.stored.as_bytes()) {
                Ok(strict) => assert_eq!(strict, record, "{}: a readable record reads alike both ways", c.name),
                Err(error) => expected["decode_error"] = json!(error.to_string()),
            }
            match &record.state {
                State::Pending => {}
                State::Failed { code, message } => {
                    expected["error"] = json!(message);
                    expected["error_code"] = json!(code.map(ErrorCode::as_str));
                }
                State::Done(result) => expected["result"] = result.clone(),
            }
            // What a losing claimant raises on replay: the kernel's text, so every SDK shows the same.
            if let Outcome::Failed { code, message } = outcome(&record, c.current) {
                expected["replay"] = json!(cached_failure(code, message).to_string());
            }
            expected
        })
        .collect()
}

fn filter_cases() -> Vec<Value> {
    use edax_cqrs::filter::{and, contains, eq, exists, gt, gte, is_in, lt, lte, ne, not, or, starts_with};
    let to = |f: edax_cqrs::Filter| serde_json::to_value(f).unwrap();
    let cases: Vec<(&str, Value, Value)> = vec![
        ("eq_matches", to(eq("status", json!("open"))), json!({"status": "open"})),
        ("eq_differs", to(eq("status", json!("open"))), json!({"status": "closed"})),
        ("ne_differs", to(ne("status", json!("closed"))), json!({"status": "open"})),
        ("ne_on_a_missing_path_is_false", to(ne("status", json!("x"))), json!({})),
        ("not_eq_on_a_missing_path_is_true", to(not(eq("status", json!("x")))), json!({})),
        ("gt_greater", to(gt("total", json!(4200))), json!({"total": 5000})),
        ("gt_less", to(gt("total", json!(4200))), json!({"total": 100})),
        ("gt_across_types_is_false", to(gt("total", json!(4200))), json!({"total": "5000"})),
        ("eq_is_numeric_1_equals_1_0", to(eq("n", json!(1.0))), json!({"n": 1})),
        // Past 2^53 an f64 comparison would call these equal.
        ("eq_wide_integers_are_exact", to(eq("n", json!(9_007_199_254_740_993u64))), json!({"n": 9_007_199_254_740_992u64})),
        ("gt_wide_integers_are_exact", to(gt("n", json!(9_007_199_254_740_992u64))), json!({"n": 9_007_199_254_740_993u64})),
        ("in_hits", to(is_in("status", vec![json!("open"), json!("closed")])), json!({"status": "open"})),
        ("in_misses", to(is_in("status", vec![json!("a")])), json!({"status": "open"})),
        ("in_on_a_missing_path_is_false", to(is_in("status", vec![json!("a")])), json!({})),
        ("in_empty_matches_nothing", to(is_in("status", vec![])), json!({"status": "open"})),
        ("exists_present", to(exists("p", true)), json!({"p": 1})),
        ("exists_null_is_not_present", to(exists("p", true)), json!({"p": null})),
        ("exists_false_on_missing", to(exists("p", false)), json!({})),
        ("not_exists_on_missing_is_false", to(not(exists("p", false))), json!({})),
        ("nested_path", to(eq("address.city", json!("NYC"))), json!({"address": {"city": "NYC"}})),
        ("nested_missing_key", to(eq("address.zip", json!("x"))), json!({"address": {"city": "NYC"}})),
        ("path_through_a_non_object", to(eq("a.b", json!("x"))), json!({"a": 5})),
        ("a_numeric_segment_is_a_key_not_an_index", to(eq("tags.0", json!("eu"))), json!({"tags": ["eu"]})),
        ("a_numeric_segment_reads_an_object_key", to(eq("tags.0", json!("eu"))), json!({"tags": {"0": "eu"}})),
        ("and_all", to(and(vec![eq("status", json!("open")), gt("total", json!(4200))])), json!({"status": "open", "total": 5000})),
        ("and_one_false", to(and(vec![eq("status", json!("open")), gt("total", json!(9999))])), json!({"status": "open", "total": 5000})),
        ("or_one_true", to(or(vec![eq("status", json!("closed")), gt("total", json!(4200))])), json!({"status": "open", "total": 5000})),
        ("gte_at_the_boundary", to(gte("total", json!(5000))), json!({"total": 5000})),
        ("lte_at_the_boundary", to(lte("total", json!(5000))), json!({"total": 5000})),
        ("lt_strings", to(lt("name", json!("z"))), json!({"name": "m"})),
        // Strings order by code point, which is UTF-8 byte order: "Z" < "a", and U+FF71 < U+1F980
        // although UTF-16 code units (JavaScript, Java, C#) put the crab first.
        ("strings_order_upper_before_lower", to(lt("s", json!("a"))), json!({"s": "Z"})),
        ("strings_order_by_code_point_not_utf16", to(lt("s", json!("🦀"))), json!({"s": "\u{ff71}"})),
        // No Unicode normalization: NFC and NFD spellings of one name differ.
        ("strings_are_not_normalized", to(eq("s", json!("caf\u{e9}"))), json!({"s": "cafe\u{301}"})),
        // RFC3339 orders as strings only in one fixed-width form: these two read wrong on purpose.
        ("timestamp_without_a_fraction_sorts_after_one_with", to(lt("t", json!("2024-01-01T00:00:00Z"))), json!({"t": "2024-01-01T00:00:00.500000Z"})),
        ("timestamp_with_an_offset_sorts_by_wall_time", to(gt("t", json!("2024-01-01T00:00:00Z"))), json!({"t": "2024-01-01T00:30:00+01:00"})),
        ("starts_with_prefix", to(starts_with("sku", json!("INV-"))), json!({"sku": "INV-2024-01"})),
        ("starts_with_other_prefix", to(starts_with("sku", json!("ORD-"))), json!({"sku": "INV-2024-01"})),
        ("starts_with_non_string", to(starts_with("n", json!("5"))), json!({"n": 5})),
        ("contains_member", to(contains("tags", json!("eu"))), json!({"tags": ["paid", "eu"]})),
        ("contains_non_member", to(contains("tags", json!("usd"))), json!({"tags": ["paid", "eu"]})),
        ("contains_needs_an_array", to(contains("tags", json!("eu"))), json!({"tags": "eu"})),
        ("contains_is_numeric", to(contains("v", json!(1.0))), json!({"v": [1]})),
        ("contains_null_element", to(contains("v", json!(null))), json!({"v": [1, null]})),
        // Equality is numeric at every depth, the same as at the top level.
        ("eq_nested_array_is_numeric", to(eq("a", json!([1.0]))), json!({"a": [1]})),
        ("eq_nested_object_is_numeric", to(eq("o", json!({"x": [2.0]}))), json!({"o": {"x": [2]}})),
        ("eq_array_length_differs", to(eq("a", json!([1, 1]))), json!({"a": [1]})),
        // Bools order false < true, as the sort ranks them.
        ("gt_true_over_false", to(gt("b", json!(false))), json!({"b": true})),
        ("lte_false_under_true", to(lte("b", json!(true))), json!({"b": false})),
        ("bool_against_number_is_unordered", to(gt("b", json!(0))), json!({"b": true})),
        ("and_empty_is_true", to(and(vec![])), json!({})),
        ("or_empty_is_false", to(or(vec![])), json!({})),
        // Refused: what the grammar admits but no document could mean, and typos.
        ("refused_null_comparand", to(eq("p", json!(null))), json!({"p": null})),
        ("refused_null_in_member", to(is_in("p", vec![json!(1), json!(null)])), json!({"p": 1})),
        ("refused_empty_path", to(exists("", true)), json!({})),
        ("refused_dotted_empty_segment", to(eq("a..b", json!(1))), json!({"a": {"": {"b": 1}}})),
        ("refused_unknown_field", json!({"compare": {"path": "p", "op": "eq", "vlaue": 1}}), json!({"p": 1})),
        ("refused_unknown_node", json!({"near": {"path": "p"}}), json!({"p": 1})),
    ];
    cases
        .into_iter()
        .map(|(name, filter, doc)| {
            let result = eval_json(&serde_json::to_vec(&filter).unwrap(), &serde_json::to_vec(&doc).unwrap());
            assert_eq!(name.starts_with("refused_"), result.is_err(), "{name}");
            json!({
                "name": name,
                "filter": filter,
                "document": doc,
                "expected": result.as_ref().ok(),
                "error_code": result.as_ref().err().map(|e| e.code.as_str()),
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
    let ns = |values: &[i64]| -> Value {
        values.iter().enumerate().map(|(i, n)| json!({"id": format!("r{i}"), "n": n})).collect()
    };
    let cases: Vec<(&str, Value, Value)> = vec![
        ("sort_ascending", json!({"sort": by_n()}), ns(&[3, 1, 2])),
        ("offset_and_limit_with_more_rows", json!({"sort": by_n(), "offset": 1, "limit": 2}), ns(&[3, 1, 2, 5, 4])),
        ("descending_missing_path_first", json!({"sort": [{"path": "n", "desc": true}]}), json!([{"id": "a", "n": 1}, {"id": "b"}, {"id": "c", "n": 3}])),
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
        ("resume_after_a_cursor", json!({"sort": by_n(), "limit": 1, "after": mint(json!({"id": "r1", "n": 1}), by_n())}), ns(&[1, 1, 2, 3])),
        ("resume_descending", json!({"sort": [{"path": "n", "desc": true}], "after": mint(json!({"id": "r2", "n": 3}), json!([{"path": "n", "desc": true}]))}), ns(&[1, 2, 3])),
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
        ("offset_after_a_cursor", json!({"sort": by_n(), "offset": 1, "after": mint(json!({"id": "r0", "n": 1}), by_n())}), ns(&[1, 2, 3, 4])),
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
        ("multi_key_tie_broken_by_the_second", json!({"sort": [{"path": "bal", "desc": true}, {"path": "id"}], "limit": 2}),
            json!([{"id": "a", "bal": 150}, {"id": "b", "bal": 300}, {"id": "c", "bal": 150}])),
        ("no_sort_keeps_input_order_and_mints_no_cursor", json!({"limit": 2}), ns(&[3, 1, 2])),
        ("offset_past_the_end", json!({"sort": by_n(), "offset": 5}), ns(&[1, 2])),
        // Refused: a cursor under another sort, a cursor without one, and a page whose `next` would
        // be minted from a row without the tiebreak id.
        ("refused_cursor_under_another_sort", json!({"sort": [{"path": "s"}], "after": mint(json!({"id": "r0", "n": 1}), by_n())}), ns(&[1, 2])),
        ("refused_cursor_under_another_direction", json!({"sort": [{"path": "n", "desc": true}], "after": mint(json!({"id": "r0", "n": 1}), by_n())}), ns(&[1, 2])),
        ("refused_cursor_without_a_sort", json!({"after": mint(json!({"id": "r0", "n": 1}), by_n())}), ns(&[1, 2])),
        ("refused_next_from_a_row_without_an_id", json!({"sort": [{"path": "k"}], "limit": 1}), json!([{"k": 1}, {"k": 1}])),
        ("refused_unknown_field", json!({"sort": by_n(), "limt": 1}), ns(&[1])),
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
                "error_code": result.as_ref().err().map(|e| e.code.as_str()),
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
        (json!([{"path": "s"}, {"path": "n", "desc": true}]), json!({"id": "a", "s": "open", "n": 4})),
        // `id` already terminal: no second tiebreak, and the same sort as the tiebreak left implicit.
        (json!([{"path": "n"}, {"path": "id"}]), json!({"id": "z", "n": 1})),
        (json!([{"path": "id"}]), json!({"id": "only"})),
        (json!([{"path": "id", "desc": true}]), json!({"id": "only"})),
        // A missing sort path encodes as null and must decode back as null, not as an absent slot.
        (json!([{"path": "missing"}]), json!({"id": "user-2"})),
        // Every type the total order spans, so the tuple's JSON round trip is pinned for each, and
        // non-ASCII in the tuple, which is written raw before base64url.
        (json!([{"path": "v"}]), json!({"id": "t1", "v": {"k": [1, "two", true, null, "café ☕"]}})),
        // Bytes whose standard base64 would carry `+` and `/`: base64url carries `-` and `_`.
        (json!([{"path": "q"}]), json!({"id": "~~~", "q": "x?>"})),
        // 2^53 and its negative: the widest integers the RFC 8785 cursor text keeps exact.
        (json!([{"path": "n"}]), json!({"id": 9_007_199_254_740_992u64, "n": -9_007_199_254_740_992i64})),
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
    let replayed = encode_cursor(&json!({"id": "a", "n": 1}), &[SortKey { path: "n".into(), desc: false }]).unwrap();
    let refused: Vec<(&str, Value, Value, Option<&str>)> = vec![
        ("decode_under_another_key", json!([{"path": "s"}]), Value::Null, Some(&replayed)),
        ("decode_under_another_direction", json!([{"path": "n", "desc": true}]), Value::Null, Some(&replayed)),
        ("decode_under_one_key_more", json!([{"path": "n"}, {"path": "s"}]), Value::Null, Some(&replayed)),
        ("decode_without_a_sort", json!([]), Value::Null, Some(&replayed)),
        ("decode_standard_base64", json!([{"path": "n"}]), Value::Null, Some("W1tdXQ==")),
        ("encode_without_a_sort", json!([]), json!({"id": "user-3"}), None),
        ("encode_from_a_row_without_an_id", json!([{"path": "n"}]), json!({"n": 1}), None),
        ("encode_from_a_row_with_a_null_id", json!([{"path": "n"}]), json!({"n": 1, "id": null}), None),
        // The cursor is RFC 8785 text, whose doubles would round this value onto its neighbour.
        ("encode_a_sort_value_past_2_pow_53", json!([{"path": "n"}]), json!({"n": 9_007_199_254_740_993u64, "id": "a"}), None),
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

/// The guard's failure settle table: one row per taxonomy code plus the uncoded failure. `cache` is the
/// code a won claim caches the failure under, or `null` when it releases the claim instead.
fn idempotency_settle_cases() -> Vec<Value> {
    let codes = std::iter::once(None).chain(edax_core::ErrorCode::ALL.iter().copied().map(Some));
    codes
        .map(|code| {
            let cache = match edax_cqrs::settle(code) {
                edax_cqrs::Settle::Cache(code) => Some(code.as_str()),
                edax_cqrs::Settle::Release => None,
            };
            json!({ "code": code.map(|c| c.as_str()), "cache": cache })
        })
        .collect()
}

#[test]
fn golden() {
    write_or_check("filter.json", filter_cases());
    write_or_check("query.json", query_cases());
    write_or_check("cursor.json", cursor_cases());
    write_or_check("payload_hash.json", payload_hash_cases());
    write_or_check("canonical_payload_hash.json", canonical_payload_hash_cases());
    write_or_check("canonical_json.json", canonical_json_cases());
    write_or_check("command_hash.json", command_hash_cases());
    write_or_check("idempotency_record.json", idempotency_record_cases());
    write_or_check("idempotency_settle.json", idempotency_settle_cases());
}

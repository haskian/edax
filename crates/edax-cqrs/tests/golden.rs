//! Golden vectors for the command-and-query kernel — both the command side (canonical bytes,
//! hashes, the idempotency decision and record) and the query side (filter, sort, cursors). Every
//! binding's test suite reads the same files.
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax-cqrs --test golden`.

use edax_cqrs::query::{decode_cursor, effective_sort, encode_cursor, query_json, SortKey};
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

fn filter_cases() -> Vec<Value> {
    use edax_cqrs::filter::{and, contains, eq, exists, gt, gte, is_in, lt, lte, ne, not, or, starts_with};
    let cases: Vec<(edax_cqrs::Filter, Value)> = vec![
        (eq("status", json!("open")), json!({"status": "open"})),
        (eq("status", json!("open")), json!({"status": "closed"})),
        (ne("status", json!("closed")), json!({"status": "open"})),
        (ne("status", json!("x")), json!({})), // missing path -> false
        (not(eq("status", json!("x"))), json!({})), // missing path -> true
        (gt("total", json!(4200)), json!({"total": 5000})),
        (gt("total", json!(4200)), json!({"total": 100})),
        (gt("total", json!(4200)), json!({"total": "5000"})), // cross-type -> false
        (eq("n", json!(1.0)), json!({"n": 1})), // numeric equality (1 == 1.0)
        (is_in("status", vec![json!("open"), json!("closed")]), json!({"status": "open"})),
        (is_in("status", vec![json!("a")]), json!({"status": "open"})),
        (is_in("status", vec![json!("a")]), json!({})), // missing path -> false
        (exists("p", true), json!({"p": 1})),
        (exists("p", true), json!({"p": null})), // null is not present
        (exists("p", false), json!({})), // missing -> complement true
        (eq("address.city", json!("NYC")), json!({"address": {"city": "NYC"}})),
        (eq("address.zip", json!("x")), json!({"address": {"city": "NYC"}})),
        (
            and(vec![eq("status", json!("open")), gt("total", json!(4200))]),
            json!({"status": "open", "total": 5000}),
        ),
        (
            and(vec![eq("status", json!("open")), gt("total", json!(9999))]),
            json!({"status": "open", "total": 5000}),
        ),
        (
            or(vec![eq("status", json!("closed")), gt("total", json!(4200))]),
            json!({"status": "open", "total": 5000}),
        ),
        (gte("total", json!(5000)), json!({"total": 5000})),
        (lt("name", json!("z")), json!({"name": "m"})),
        (starts_with("sku", json!("INV-")), json!({"sku": "INV-2024-01"})),
        (starts_with("sku", json!("ORD-")), json!({"sku": "INV-2024-01"})),
        (starts_with("n", json!("5")), json!({"n": 5})), // non-string -> false
        (contains("tags", json!("eu")), json!({"tags": ["paid", "eu"]})),
        (contains("tags", json!("usd")), json!({"tags": ["paid", "eu"]})),
        (contains("tags", json!("eu")), json!({"tags": "eu"})), // not an array -> false
        (eq("a.b", json!("x")), json!({"a": 5})), // path descends through a non-object segment -> false
        (lte("total", json!(5000)), json!({"total": 5000})), // <= at the equality boundary -> true
        (and(vec![]), json!({})), // vacuous true
        (or(vec![]), json!({})),  // vacuous false
    ];
    cases
        .into_iter()
        .map(|(f, doc)| {
            json!({
                "filter": serde_json::to_value(&f).unwrap(),
                "document": doc,
                "expected": edax_cqrs::eval(&f, &doc),
            })
        })
        .collect()
}

fn query_cases() -> Vec<Value> {
    // Each case pins the page and the cursor minted from its last row, so bindings verify both
    // ordering/pagination and byte-identical cursor encoding.
    let cases: Vec<(Value, Value)> = vec![
        (json!({"sort": [{"path": "n"}]}), json!([{"n": 3}, {"n": 1}, {"n": 2}])),
        (json!({"sort": [{"path": "n"}], "offset": 1, "limit": 2}), json!([{"n": 3}, {"n": 1}, {"n": 2}, {"n": 5}, {"n": 4}])),
        (json!({"sort": [{"path": "n", "desc": true}]}), json!([{"n": 1}, {}, {"n": 3}])),
        // Mixed types exercise the total type order: number < string < bool < array < object < null.
        (json!({"sort": [{"path": "v"}]}), json!([{"v": "a"}, {"v": 2}, {"v": true}, {"v": null}, {"v": [1]}, {"v": {"k": 1}}])),
        (
            json!({"filter": {"compare": {"path": "s", "op": "eq", "value": "open"}}, "sort": [{"path": "n"}]}),
            json!([{"s": "open", "n": 2}, {"s": "closed", "n": 1}, {"s": "open", "n": 1}]),
        ),
        (json!({"sort": [{"path": "n"}], "limit": 1, "after": "WzFd"}), json!([{"n": 1}, {"n": 2}, {"n": 3}])), // WzFd = base64("[1]")
        // No sort: input order is preserved and the cursor is base64("[]") = "W10=".
        (json!({}), json!([{"n": 3}, {"n": 1}, {"n": 2}])),
        // Multi-key sort with a tie on the first key broken by the second; the cursor is a 2-tuple.
        (json!({"sort": [{"path": "bal", "desc": true}, {"path": "id"}], "limit": 2}),
            json!([{"id": "a", "bal": 150}, {"id": "b", "bal": 300}, {"id": "c", "bal": 150}])),
        // Offset past the end: empty page, null cursor.
        (json!({"sort": [{"path": "n"}], "offset": 5}), json!([{"n": 1}, {"n": 2}])),
    ];
    cases
        .into_iter()
        .map(|(q, docs)| {
            let qb = serde_json::to_vec(&q).unwrap();
            let db = serde_json::to_vec(&docs).unwrap();
            let page: Vec<Value> = serde_json::from_slice(&query_json(&qb, &db).unwrap()).unwrap();
            let sort: Vec<SortKey> = serde_json::from_value(q.get("sort").cloned().unwrap_or(json!([]))).unwrap();
            let cursor = page.last().map(|row| encode_cursor(row, &sort));
            json!({ "query": q, "documents": docs, "expected_page": page, "expected_cursor": cursor })
        })
        .collect()
}

fn cursor_cases() -> Vec<Value> {
    // The round trip a backend depends on: a cursor minted from a row decodes back to the tuple its
    // `WHERE` clause binds, under the sort the page is actually ordered by. Each case pins both
    // halves, so a binding that decodes differently fails here rather than silently paging wrong.
    let cases: Vec<(Value, Value)> = vec![
        // The ordinary case: one declared key, `id` appended as the tiebreak.
        (json!([{"path": "age"}]), json!({"id": "user-1", "age": 36})),
        // Two declared keys; the tiebreak still lands last.
        (json!([{"path": "s"}, {"path": "n", "desc": true}]), json!({"id": "a", "s": "open", "n": 4})),
        // `id` already terminal: no second tiebreak is appended.
        (json!([{"path": "n"}, {"path": "id"}]), json!({"id": "z", "n": 1})),
        // Sorting on `id` alone is already tie-free.
        (json!([{"path": "id"}]), json!({"id": "only"})),
        // A missing sort path encodes as null and must decode back as null, not as an absent slot.
        (json!([{"path": "missing"}]), json!({"id": "user-2"})),
        // Every type the total order spans, so the tuple's JSON round trip is pinned for each.
        (json!([{"path": "v"}]), json!({"id": "t1", "v": {"k": [1, "two", true, null]}})),
        // An unsorted query has no order to resume from; the tiebreak is a no-op on an empty sort.
        (json!([]), json!({"id": "user-3", "age": 1})),
    ];
    cases
        .into_iter()
        .map(|(sort, doc)| {
            let keys: Vec<SortKey> = serde_json::from_value(sort.clone()).unwrap();
            let cursor = encode_cursor(&doc, &keys);
            let decoded = decode_cursor(&cursor, &keys).unwrap();
            json!({
                "sort": sort,
                "document": doc,
                "expected_cursor": cursor,
                "expected_effective_sort": effective_sort(&keys),
                "expected_tuple": decoded,
            })
        })
        .chain(
            // Cursors that did not come from this sort. The applier pads a short tuple with null and
            // ignores a long one, so the decoder must report exactly what the applier would compare.
            [
                (json!([{"path": "a"}, {"path": "b"}]), "W10=", json!([null, null, null])),
                (json!([{"path": "a"}]), "WzEsMiwzLDRd", json!([1, 2])),
            ]
            .into_iter()
            .map(|(sort, cursor, tuple)| {
                let keys: Vec<SortKey> = serde_json::from_value(sort.clone()).unwrap();
                assert_eq!(json!(decode_cursor(cursor, &keys).unwrap()), tuple, "mismatched-arity case");
                json!({
                    "sort": sort,
                    "document": Value::Null,
                    "expected_cursor": cursor,
                    "expected_effective_sort": effective_sort(&keys),
                    "expected_tuple": tuple,
                })
            }),
        )
        .collect()
}

#[test]
fn golden() {
    write_or_check("filter.json", filter_cases());
    write_or_check("query.json", query_cases());
    write_or_check("cursor.json", cursor_cases());
    write_or_check("payload_hash.json", payload_hash_cases());
    write_or_check("canonical_payload_hash.json", canonical_payload_hash_cases());
    write_or_check("command_hash.json", command_hash_cases());
    write_or_check("idempotency.json", idempotency_cases());
    write_or_check("idempotency_record.json", idempotency_record_cases());
}

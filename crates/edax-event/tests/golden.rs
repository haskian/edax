//! Golden vectors: the cross-language consistency contract.
//!
//! These cases are derived from edax-event and written to `testdata/golden/*.json`. Every binding's
//! test suite reads the same files and must produce byte-identical results. One drift in any
//! binding fails its test.
//!
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax-event --test golden`.
//! Without the env var this test ASSERTS the committed files still match the code.

use edax_event::{encode_from_json, Format};
use edax_golden::{hex, write_or_check};
use serde_json::{json, Value};

/// Every id against every `n` for one routing function. `n = 0` clamps to 1 (every id -> 0).
fn routing_cases(ns: &[u32], route: fn(&str, u32) -> u32) -> Vec<Value> {
    let ids = ["agg-1", "agg-2", "order-42", "user:7", "", "🦀-aggregate"];
    ids.iter()
        .flat_map(|id| ns.iter().map(move |&n| json!({ "id": id, "n": n, "expected": route(id, n) })))
        .collect()
}

fn partition_cases() -> Vec<Value> {
    routing_cases(&[0, 1, 7, 256, 1024], edax_event::partition)
}

fn rendezvous_cases() -> Vec<Value> {
    routing_cases(&[0, 1, 7, 8, 9, 256], edax_event::rendezvous)
}

fn upcast_cases() -> Vec<Value> {
    let cases: [(i32, i32, &[i32]); 7] = [
        (1, 1, &[]),          // no-op
        (1, 3, &[1, 2]),      // contiguous
        (1, 3, &[1]),         // gap at 2 (missing middle step) -> error
        (1, 3, &[2]),         // gap at 1 (missing first step) -> error names step 1, not 2
        (2, 5, &[2, 3, 4]),   // contiguous from 2
        (3, 2, &[2]),         // from > to -> error
        (0, 2, &[0, 1, 9]),   // extra unrelated step is fine
    ];
    cases
        .into_iter()
        .map(|(from, to, available)| {
            let result = edax_event::upcast_plan(from, to, available);
            json!({
                "from": from,
                "to": to,
                "available": available,
                "plan": result.as_ref().ok(),
                "error": result.as_ref().err(),
            })
        })
        .collect()
}

fn causation_cases() -> Vec<Value> {
    use edax_event::Metadata;
    let mk = |pairs: &[(&str, &str)]| -> Metadata {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    };
    let cases: Vec<(Metadata, &str)> = vec![
        (mk(&[]), "evt-root"),                                  // root seeds its own chain
        (mk(&[("correlation_id", "corr-1")]), "evt-2"),        // inherit existing correlation
        (mk(&[("correlation_id", "corr-1"), ("causation_id", "evt-1")]), "evt-2"),
        (mk(&[("causation_id", "evt-1")]), "evt-2"),           // causation but no correlation -> correlation seeds from parent id
        (mk(&[("trace", "t-9")]), "evt-3"),                     // unrelated metadata ignored
    ];
    cases
        .into_iter()
        .map(|(parent, parent_id)| {
            json!({
                "parent_metadata": parent,
                "parent_event_id": parent_id,
                "expected": edax_event::propagate(&parent, parent_id),
            })
        })
        .collect()
}

fn originate_cases() -> Vec<Value> {
    use edax_event::Metadata;
    let mk = |pairs: &[(&str, &str)]| -> Metadata {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    };
    // One `fresh_id` throughout, so every case shows whether the kernel reached for it or not.
    let cases: Vec<Metadata> = vec![
        mk(&[]),                                                        // nothing given -> both minted
        mk(&[("correlation_id", "corr-1")]),                            // causation follows the given correlation, not fresh_id
        mk(&[("causation_id", "evt-0")]),                               // a lone causation is not a chain -> fresh correlation
        mk(&[("correlation_id", "corr-1"), ("causation_id", "evt-0")]), // both given -> fresh_id unused
        mk(&[("trace", "t-9")]),                                        // unrelated metadata is neither a chain nor carried out
        mk(&[("correlation_id", "corr-1"), ("trace", "t-9")]),          // unrelated metadata dropped even beside a kept key
    ];
    cases
        .into_iter()
        .map(|metadata| {
            json!({
                "metadata": metadata,
                "fresh_id": "fresh-1",
                "expected": edax_event::originate(&metadata, "fresh-1"),
            })
        })
        .collect()
}

fn envelope_cases() -> Vec<Value> {
    // Boundary-JSON inputs (data is base64). Fixed ids + timestamps => reproducible wire bytes.
    let envelopes = vec![
        json!({
            "id": "evt-1", "aggregate_id": "agg-1",
            "event": {"name": "InvoiceIssued", "version": 1, "data": "3q2+7w=="},
            "sequence": 7,
            "namespace": {"tenant": "acme"}, "metadata": {"trace": "abc-123"},
            "timestamp": 1_700_000_000_000_000_000i64
        }),
        json!({
            "id": "evt-2", "aggregate_id": "agg-2",
            "event": {"name": "OrderPlaced", "version": 2, "data": ""},
            "sequence": Value::Null,
            "namespace": {}, "metadata": {},
            "timestamp": 0
        }),
        json!({
            "id": "evt-3", "aggregate_id": "user:7",
            "event": {"name": "UserRegistered", "version": 1, "data": "AAECAwQF"},
            "sequence": 1,
            "namespace": {"region": "eu", "tenant": "acme"},
            "metadata": {"actor": "sys", "trace": "z"},
            "timestamp": 1_234_567_890_000_000_000i64
        }),
        // Unicode in name/aggregate/namespace/metadata (JSON \u escapes vs msgpack raw UTF-8) plus a
        // 2^53+1 sequence that exceeds f64 integer precision — pins exact wire bytes for both formats.
        json!({
            "id": "evt-4", "aggregate_id": "café-7",
            "event": {"name": "Paiement effectué", "version": 3, "data": "8J+mgA=="},
            "sequence": 9_007_199_254_740_993i64,
            "namespace": {"région": "eu"}, "metadata": {"acteur": "système"},
            "timestamp": 1_700_000_000_000_000_000i64
        }),
    ];
    envelopes
        .into_iter()
        .map(|env| {
            let json_bytes = serde_json::to_vec(&env).unwrap();
            json!({
                "envelope": env,
                "json_hex": hex(&encode_from_json(Format::Json, &json_bytes).unwrap()),
                "msgpack_hex": hex(&encode_from_json(Format::Msgpack, &json_bytes).unwrap()),
            })
        })
        .collect()
}

fn dedup_cases() -> Vec<Value> {
    let pairs: [(Option<u64>, Option<u64>); 9] = [
        (None, None),
        (Some(5), None),
        (None, Some(0)),
        (Some(5), Some(6)),
        (Some(5), Some(5)),
        (Some(5), Some(4)),
        (Some(0), Some(0)),
        (Some(u64::MAX - 1), Some(u64::MAX)), // apply at the u64 ceiling (no overflow)
        (Some(u64::MAX), Some(u64::MAX - 1)), // stale at the u64 ceiling -> drop
    ];
    pairs
        .into_iter()
        .map(|(s, i)| json!({ "stored": s, "incoming": i, "expected": edax_event::should_apply(s, i) }))
        .collect()
}

/// The dense-batch rule, one case per branch: accepted runs and every way a batch is refused.
/// `error` is null when the batch is accepted, else the kernel's message naming the batch index.
fn sequence_batch_cases() -> Vec<Value> {
    let batches: Vec<(i64, Vec<Option<u64>>)> = vec![
        (7, vec![]),                                   // empty batch: a version check only
        (0, vec![Some(1)]),                            // first append
        (2, vec![Some(3), Some(4), Some(5)]),          // dense run
        (0, vec![Some(1), None]),                      // an envelope with no sequence
        (1, vec![Some(2), Some(4)]),                   // gap
        (0, vec![Some(2), Some(1)]),                   // reordered
        (0, vec![Some(1), Some(1)]),                   // repeat
        (1, vec![Some(u64::MAX)]),                     // above the i64 ceiling
    ];
    batches
        .into_iter()
        .map(|(expected, sequences)| {
            let error = edax_event::sequence::check_batch(expected, &sequences).err();
            json!({ "expected": expected, "sequences": sequences, "error": error })
        })
        .collect()
}

#[test]
fn golden() {
    write_or_check("partition.json", partition_cases());
    write_or_check("rendezvous.json", rendezvous_cases());
    write_or_check("envelopes.json", envelope_cases());
    write_or_check("upcast.json", upcast_cases());
    write_or_check("causation.json", causation_cases());
    write_or_check("causation_originate.json", originate_cases());
    write_or_check("dedup.json", dedup_cases());
    write_or_check("sequence_batch.json", sequence_batch_cases());
}

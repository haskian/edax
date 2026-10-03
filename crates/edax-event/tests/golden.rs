//! Golden vectors: the cross-language consistency contract.
//!
//! These cases are derived from edax-event and written to `testdata/golden/*.json`. Every binding's
//! test suite reads the same files and must produce byte-identical results. One drift in any
//! binding fails its test.
//!
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax-event --test golden`.
//! Without the env var this test ASSERTS the committed files still match the code.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a golden generator runs only as a test, and its panic is the failure report"
)]

use edax_core::ErrorCode;
use edax_event::{Event, EventEnvelope, Format, Metadata, Namespace};
use edax_golden::{hex, write_or_check};
use serde_json::{json, Value};

/// Every id against every `n` for one routing function. `n = 0` clamps to 1 (every id -> 0).
fn routing_cases(ns: &[u32], route: fn(&str, u32) -> u32) -> Vec<Value> {
    // The two spellings of "café" are NFC (one code point) and NFD (`e` + a combining acute): ids are
    // hashed as their bytes, never normalized, so the two land apart.
    let ids = ["agg-1", "agg-2", "order-42", "user:7", "", "🦀-aggregate", "caf\u{e9}", "cafe\u{301}"];
    ids.iter()
        .flat_map(|id| ns.iter().map(move |&n| json!({ "id": id, "n": n, "expected": route(id, n) })))
        .collect()
}

fn partition_cases() -> Vec<Value> {
    routing_cases(&[0, 1, 7, 256, 1024, u32::MAX], edax_event::partition)
}

fn upcast_cases() -> Vec<Value> {
    // `available: None` is a name with no transforms registered: every step is the identity, so only
    // the downgrade rule (a payload from a newer writer) can refuse it.
    let cases: [(i32, i32, Option<&[i32]>); 18] = [
        (1, 1, Some(&[])),          // no-op
        (1, 3, Some(&[1, 2])),      // contiguous
        (1, 3, Some(&[1])),         // gap at 2 (missing middle step) -> error
        (1, 3, Some(&[2])),         // gap at 1 (missing first step) -> error names step 1, not 2
        (2, 5, Some(&[2, 3, 4])),   // contiguous from 2
        (3, 2, Some(&[2])),         // from > to -> error
        (0, 2, Some(&[0, 1, 9])),   // extra unrelated step is fine
        (1, 3, Some(&[])),          // nothing available but a plan is needed -> error names the first step
        (1, 4, Some(&[3, 1, 2])),   // `available` is a set: its order is not the plan's
        (1, 3, Some(&[2, 1, 2, 1])), // duplicates in `available` collapse
        (-2, 1, Some(&[-2, -1, 0])), // negative versions are versions like any other
        (i32::MAX, i32::MAX, Some(&[])), // the i32 ceiling, no-op
        (i32::MAX - 1, i32::MAX, Some(&[i32::MAX - 1])), // the last step names v(MAX-1) -> v(MAX)
        (i32::MIN, i32::MAX, Some(&[i32::MIN])), // a span of 2^32 fails at its second step, without walking it
        (1, 1, None),               // no transforms, current: nothing to run
        (1, 3, None),               // no transforms, older payload: identity, nothing to run
        (5, 1, None),               // no transforms, newer writer: refused, never decoded silently
        (i32::MIN, i32::MAX, None), // identity over a 2^32 span is still an empty plan, not a walk
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
                "error": result.as_ref().err().map(ToString::to_string),
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
        // An empty id is an id: present means on a chain, whatever its truthiness in a host language.
        (mk(&[("correlation_id", "")]), "evt-4"),
        (mk(&[]), ""), // an empty parent event id still seeds the chain with itself
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
        mk(&[("correlation_id", "")]),                                  // an empty correlation is present: fresh_id unused for it
        mk(&[("causation_id", "")]),                                    // an empty causation is kept, not defaulted
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

fn map(pairs: &[(&str, &str)]) -> Namespace {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
}

/// The first envelope every vector shares; each case overrides only the fields it is about.
fn base_envelope() -> EventEnvelope {
    EventEnvelope {
        id: "evt-1".into(),
        aggregate_id: "agg-1".into(),
        event: Event { name: "InvoiceIssued".into(), version: 1, data: vec![0xde, 0xad, 0xbe, 0xef] },
        sequence: Some(7),
        namespace: map(&[("tenant", "acme")]),
        metadata: map(&[("trace", "abc-123")]),
        timestamp: 1_700_000_000_000_000_000,
    }
}

fn envelope_cases() -> Vec<Value> {
    let with = |edit: &dyn Fn(&mut EventEnvelope)| {
        let mut env = base_envelope();
        edit(&mut env);
        env
    };
    let envelopes: Vec<(&str, EventEnvelope)> = vec![
        ("base", base_envelope()),
        (
            "unsequenced_empty_payload_empty_maps",
            EventEnvelope {
                id: "evt-2".into(),
                aggregate_id: "agg-2".into(),
                event: Event { name: "OrderPlaced".into(), version: 2, data: Vec::new() },
                sequence: None,
                namespace: Namespace::new(),
                metadata: Metadata::new(),
                timestamp: 0,
            },
        ),
        ("two_keys_per_map", with(&|e| {
            e.namespace = map(&[("region", "eu"), ("tenant", "acme")]);
            e.metadata = map(&[("actor", "sys"), ("trace", "z")]);
        })),
        // Non-ASCII text is written raw in both formats (serde_json does not \u-escape it), and the
        // 2^53+1 sequence is past every float-typed reader.
        ("non_ascii_and_a_sequence_past_2_53", with(&|e| {
            e.aggregate_id = "café-7".into();
            e.event.name = "Paiement effectué".into();
            e.event.data = "🦀".as_bytes().to_vec();
            e.sequence = Some(9_007_199_254_740_993);
            e.namespace = map(&[("région", "eu")]);
            e.metadata = map(&[("acteur", "système")]);
        })),
        // Map keys sort by UTF-8 bytes: "Z" < "a", "" first, and U+FF71 after U+1F980 by bytes
        // (EF.. < F0..) although JavaScript, Java and C# sort it first by UTF-16 code units.
        ("keys_sort_by_utf8_bytes_not_utf16", with(&|e| {
            e.metadata = map(&[("a", "1"), ("Z", "2"), ("", "3"), ("\u{ff71}", "4"), ("🦀", "5")]);
        })),
        // `"` and `\` escaped; U+001F written as lowercase \u001f; DEL and U+2028 written raw.
        ("escaping", with(&|e| {
            e.metadata = map(&[("quote", "a\"b"), ("backslash", "a\\b"), ("unit_separator", "\u{1f}"), ("del", "\u{7f}"), ("line_separator", "\u{2028}")]);
        })),
        ("integer_extremes", with(&|e| {
            e.timestamp = i64::MIN;
            e.event.version = i32::MIN;
            e.sequence = Some(u64::MAX);
        })),
        ("integer_ceilings", with(&|e| {
            e.timestamp = i64::MAX;
            e.event.version = i32::MAX;
            e.sequence = Some(0);
        })),
        ("negative_timestamp_before_the_epoch", with(&|e| e.timestamp = -1_000_000_000)),
        // MessagePack picks the narrowest integer form; each pair straddles one width boundary.
        ("msgpack_fixint_to_uint8", with(&|e| { e.sequence = Some(127); e.timestamp = 128; })),
        ("msgpack_uint8_to_uint16", with(&|e| { e.sequence = Some(255); e.timestamp = 256; })),
        ("msgpack_uint16_to_uint32", with(&|e| { e.sequence = Some(65_535); e.timestamp = 65_536; })),
        ("msgpack_uint32_to_uint64", with(&|e| { e.sequence = Some(4_294_967_295); e.timestamp = 4_294_967_296; })),
        ("msgpack_negative_fixint_to_int8", with(&|e| { e.event.version = -32; e.timestamp = -33; })),
        // bin8 holds up to 255 bytes, bin16 from 256; fixstr up to 31 bytes, str8 from 32.
        ("msgpack_bin8_ceiling_and_fixstr_ceiling", with(&|e| {
            e.event.data = vec![0xab; 255];
            e.id = "e".repeat(31);
        })),
        ("msgpack_bin16_floor_and_str8_floor", with(&|e| {
            e.event.data = vec![0xab; 256];
            e.id = "e".repeat(32);
        })),
    ];
    envelopes
        .into_iter()
        .map(|(name, env)| {
            json!({
                "name": name,
                "envelope": serde_json::to_value(&env).unwrap(),
                "json_hex": hex(&Format::Json.encode(&env)),
                "msgpack_hex": hex(&Format::Msgpack.encode(&env)),
            })
        })
        .collect()
}

/// A wire value a decode case is built from. Both formats serialize it the same way, so one case
/// says the same thing in JSON and MessagePack; a map is a list of pairs so a case can repeat a key.
enum W {
    Str(&'static str),
    Int(i64),
    Bytes(&'static [u8]),
    Null,
    Map(Vec<(&'static str, W)>),
    Arr(Vec<W>),
}

impl serde::Serialize for W {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeMap, SerializeSeq};
        match self {
            W::Str(v) => s.serialize_str(v),
            W::Int(v) => s.serialize_i64(*v),
            W::Bytes(v) if s.is_human_readable() => {
                use base64::Engine;
                s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(v))
            }
            W::Bytes(v) => s.serialize_bytes(v),
            W::Null => s.serialize_none(),
            W::Map(pairs) => {
                let mut m = s.serialize_map(Some(pairs.len()))?;
                for (k, v) in pairs {
                    m.serialize_entry(k, v)?;
                }
                m.end()
            }
            W::Arr(items) => {
                let mut a = s.serialize_seq(Some(items.len()))?;
                for v in items {
                    a.serialize_element(v)?;
                }
                a.end()
            }
        }
    }
}

/// The base envelope as wire fields, in the order a case edits them.
fn wire_fields() -> Vec<(&'static str, W)> {
    vec![
        ("id", W::Str("evt-1")),
        ("aggregate_id", W::Str("agg-1")),
        (
            "event",
            W::Map(vec![("name", W::Str("InvoiceIssued")), ("version", W::Int(1)), ("data", W::Bytes(&[0xde, 0xad, 0xbe, 0xef]))]),
        ),
        ("sequence", W::Int(7)),
        ("namespace", W::Map(vec![("tenant", W::Str("acme"))])),
        ("metadata", W::Map(vec![("trace", W::Str("abc-123"))])),
        ("timestamp", W::Int(1_700_000_000_000_000_000)),
    ]
}

fn without(field: &str) -> Vec<(&'static str, W)> {
    wire_fields().into_iter().filter(|(k, _)| *k != field).collect()
}

fn replacing(field: &str, value: W) -> Vec<(&'static str, W)> {
    let mut value = Some(value);
    wire_fields().into_iter().map(|(k, v)| if k == field { (k, value.take().unwrap()) } else { (k, v) }).collect()
}

/// The evolution rule: what a reader of each format accepts, ignores and refuses. Each case is
/// encoded in both formats from one wire value, so the two formats are held to the same rule.
fn envelope_decode_cases() -> Vec<Value> {
    let positional = || W::Arr(wire_fields().into_iter().map(|(_, v)| v).collect());
    let cases: Vec<(&str, W, &[u8])> = vec![
        ("the_base_envelope", W::Map(wire_fields()), b""),
        // A newer writer's field: ignored, so adding a field is forward compatible.
        ("an_unknown_field_is_ignored", W::Map(wire_fields().into_iter().chain([("priority", W::Str("high"))]).collect()), b""),
        ("an_unknown_field_in_the_event_is_ignored", W::Map(replacing(
            "event",
            W::Map(vec![("name", W::Str("InvoiceIssued")), ("version", W::Int(1)), ("data", W::Bytes(&[0xde, 0xad, 0xbe, 0xef])), ("schema", W::Str("v1"))]),
        )), b""),
        ("a_missing_sequence_reads_as_none", W::Map(without("sequence")), b""),
        ("a_null_sequence_reads_as_none", W::Map(replacing("sequence", W::Null)), b""),
        ("a_missing_required_field_is_refused", W::Map(without("timestamp")), b""),
        ("a_repeated_field_is_refused", W::Map(wire_fields().into_iter().chain([("id", W::Str("evt-2"))]).collect()), b""),
        ("a_repeated_metadata_key_keeps_the_last", W::Map(replacing(
            "metadata",
            W::Map(vec![("trace", W::Str("first")), ("trace", W::Str("last"))]),
        )), b""),
        // The positional form, of the old length and with one element more, is refused either way.
        ("a_positional_envelope_is_refused", positional(), b""),
        ("a_positional_envelope_with_an_extra_element_is_refused", W::Arr(
            wire_fields().into_iter().map(|(_, v)| v).chain([W::Str("extra")]).collect(),
        ), b""),
        ("a_positional_event_is_refused", W::Map(replacing(
            "event",
            W::Arr(vec![W::Str("InvoiceIssued"), W::Int(1), W::Bytes(&[0xde, 0xad, 0xbe, 0xef])]),
        )), b""),
        ("trailing_bytes_are_refused", W::Map(wire_fields()), b"\x00"),
        ("not_a_map_at_all", W::Str("evt-1"), b""),
    ];
    cases
        .into_iter()
        .map(|(name, wire, trailing)| {
            let encode = |format: Format| -> Vec<u8> {
                let mut bytes = match format {
                    Format::Json => serde_json::to_vec(&wire).unwrap(),
                    _ => rmp_serde::to_vec_named(&wire).unwrap(),
                };
                bytes.extend_from_slice(trailing);
                bytes
            };
            let decoded = |format: Format| -> Value {
                match format.decode(&encode(format)) {
                    Ok(env) => json!({ "envelope": serde_json::to_value(&env).unwrap() }),
                    // The code is the contract; the message is the parser's own text, which a second
                    // SDK's parser words differently.
                    Err(error) => json!({ "error_code": error.code.as_str() }),
                }
            };
            let (json, msgpack) = (decoded(Format::Json), decoded(Format::Msgpack));
            assert_eq!(json, msgpack, "{name}: the two formats must follow one rule");
            json!({
                "name": name,
                "json_hex": hex(&encode(Format::Json)),
                "msgpack_hex": hex(&encode(Format::Msgpack)),
                "want": json,
            })
        })
        .collect()
}

/// `is_stale` and `advance` per pair, plus the fold an ordered consumer runs over one aggregate's
/// redelivered stream: skip what is stale, advance after each apply.
fn staleness_cases() -> Vec<Value> {
    let pairs: [(Option<u64>, Option<u64>); 9] = [
        (None, None),
        (Some(5), None), // unsequenced: not stale, and the mark stays
        (None, Some(0)),
        (Some(5), Some(6)),
        (Some(5), Some(5)), // equal: not stale (ordering is not dedup)
        (Some(5), Some(4)),
        (Some(0), Some(0)),
        (Some(u64::MAX - 1), Some(u64::MAX)),
        (Some(u64::MAX), Some(u64::MAX - 1)),
    ];
    let rows = pairs.into_iter().map(|(stored, incoming)| {
        json!({
            "op": "pair",
            "stored": stored,
            "incoming": incoming,
            "stale": edax_event::is_stale(stored, incoming),
            "advanced": edax_event::advance(stored, incoming),
        })
    });
    let streams: [(&str, &[Option<u64>]); 3] = [
        // The naive `watermark = sequence` lets the unsequenced event erase the mark here, so the
        // stale 3 applies and the mark moves back to 3.
        ("an_unsequenced_event_keeps_the_mark", &[Some(1), Some(2), Some(5), None, Some(3)]),
        // A seek back redelivers 1 and 2 after 2 already applied: 1 is skipped, 2 re-applies.
        ("a_redelivery_after_a_seek_back", &[Some(1), Some(2), Some(1), Some(2), Some(3)]),
        ("out_of_order_copies", &[Some(3), Some(1), Some(2), Some(4), Some(4)]),
    ];
    let folds = streams.into_iter().map(|(name, stream)| {
        let mut watermark = None;
        let mut applied = Vec::new();
        for &incoming in stream {
            if !edax_event::is_stale(watermark, incoming) {
                applied.push(incoming);
                watermark = edax_event::advance(watermark, incoming);
            }
        }
        json!({ "op": "fold", "name": name, "stream": stream, "applied": applied, "watermark": watermark })
    });
    rows.chain(folds).collect()
}

/// The dense-batch rule, one case per branch: accepted runs and every way a batch is refused.
/// `error` is null when the batch is accepted, else the kernel's message naming the batch index.
fn sequence_batch_cases() -> Vec<Value> {
    let batches: Vec<(u64, Vec<Option<u64>>)> = vec![
        (7, vec![]),                                   // empty batch: a version check only
        (0, vec![Some(1)]),                            // first append
        (2, vec![Some(3), Some(4), Some(5)]),          // dense run
        (0, vec![Some(1), None]),                      // an envelope with no sequence
        (1, vec![Some(2), Some(4)]),                   // gap
        (0, vec![Some(2), Some(1)]),                   // reordered
        (0, vec![Some(1), Some(1)]),                   // repeat
        (1, vec![Some(u64::MAX)]),                     // far ahead
        (i64::MAX as u64, vec![Some(i64::MAX as u64 + 1)]), // past the i64 range: exact, not clamped
        (u64::MAX - 1, vec![Some(u64::MAX)]),          // the last sequence there is
        (u64::MAX - 1, vec![Some(u64::MAX), Some(u64::MAX)]), // a repeat at the ceiling is refused
        (u64::MAX, vec![Some(u64::MAX)]),              // a stream at the ceiling takes nothing
    ];
    batches
        .into_iter()
        .map(|(expected, sequences)| {
            let error = edax_event::sequence::check_batch(expected, &sequences).err().map(|e| e.to_string());
            json!({ "expected": expected, "sequences": sequences, "error": error })
        })
        .collect()
}

/// The snapshot interval, one row per branch: no interval, a save with nothing pending, a save inside
/// one interval, a save crossing one or several multiples, and the `u64` ceiling.
fn snapshot_due_cases() -> Vec<Value> {
    [(0, 0, 5), (1, 3, 3), (1, 3, 4), (5, 1, 4), (5, 4, 5), (5, 4, 12), (5, 5, 9), (u64::MAX, 0, u64::MAX - 1), (u64::MAX, 1, u64::MAX)]
        .into_iter()
        .map(|(every, initial, current)| {
            json!({ "every": every, "initial": initial, "current": current, "due": edax_event::snapshot_due(every, initial, current) })
        })
        .collect()
}

/// The event guard's input rule: the envelope id is the key, the raw `event.data` bytes are hashed.
/// The envelope's other fields take no part, so two deliveries of one event differing only in
/// metadata or timestamp claim the same key with the same hash.
fn event_idempotency_cases() -> Vec<Value> {
    let base = edax_event::EventEnvelope {
        id: "acct-1-3".to_owned(),
        aggregate_id: "acct-1".to_owned(),
        event: edax_event::Event {
            name: "Deposited".to_owned(),
            version: 1,
            data: br#"{"account_id":"acct-1","amount_cents":4200,"currency":"USD"}"#.to_vec(),
        },
        sequence: Some(3),
        namespace: Default::default(),
        metadata: [("trace".to_owned(), "abc".to_owned())].into_iter().collect(),
        timestamp: 1_714_564_800_000_000_000,
    };
    let with = |edit: fn(&mut edax_event::EventEnvelope)| {
        let mut env = base.clone();
        edit(&mut env);
        env
    };
    let cases = [
        ("deposit", base.clone()),
        // Same bytes reordered: raw, not canonical, so a different hash under the same key.
        ("raw_bytes_not_canonical", with(|e| e.event.data = br#"{"currency":"USD","account_id":"acct-1","amount_cents":4200}"#.to_vec())),
        ("metadata_and_timestamp_take_no_part", with(|e| {
            e.metadata.insert("retry".to_owned(), "2".to_owned());
            e.timestamp += 1;
        })),
        ("empty_body", with(|e| e.event.data = Vec::new())),
        ("non_utf8_body", with(|e| e.event.data = vec![0xde, 0xad, 0xbe, 0xef])),
    ];
    cases
        .into_iter()
        .map(|(name, env)| {
            let rule = edax_event::event_idempotency(&env);
            json!({
                "name": name,
                "id": env.id,
                "data_hex": hex(&env.event.data),
                "metadata": env.metadata,
                "timestamp": env.timestamp,
                "key": rule.key,
                "payload_hash": rule.payload_hash,
            })
        })
        .collect()
}

/// One step of a [`Retries`](edax_event::Retries) scenario: an operation and the answer it gave.
#[derive(Clone, Copy)]
enum RetryOp {
    Attempt(&'static str, u32),
    Failed(&'static str, u32, Option<ErrorCode>),
    Succeeded(&'static str),
    DeadLettered(&'static str),
    SinkFailed(&'static str, u32),
    Tracked,
}

fn failure_name(failure: edax_event::Failure) -> &'static str {
    match failure {
        edax_event::Failure::Terminal => "terminal",
        edax_event::Failure::Transient => "transient",
        edax_event::Failure::Contended => "contended",
    }
}

/// The failure kind of every error code, and of a failure with no code: what decides whether a
/// failed message is contention, a transient failure or a terminal one.
fn retry_failure_cases() -> Vec<Value> {
    std::iter::once(None)
        .chain(ErrorCode::ALL.iter().copied().map(Some))
        .map(|code| json!({ "code": code.map(ErrorCode::as_str), "failure": failure_name(edax_event::Failure::of(code)) }))
        .collect()
}

fn verdict_json(verdict: edax_event::Verdict) -> Value {
    match verdict {
        edax_event::Verdict::Nack { step } => json!({ "nack": step }),
        edax_event::Verdict::Dead { attempt } => json!({ "dead": attempt }),
    }
}

/// The retry and dead-letter policy as operation scripts: each scenario builds
/// `Retries::new(max_attempts, sink)` and replays `ops`, each with the answer the kernel gave, so a second
/// SDK replays the same script and must get the same answers. A failed op carries the failure's error
/// code, `null` for none, as a host passes it. One scenario per branch of the rule.
fn retries_cases() -> Vec<Value> {
    use RetryOp::*;
    const TRANSIENT: Option<ErrorCode> = Some(ErrorCode::Store);
    const TERMINAL: Option<ErrorCode> = Some(ErrorCode::Codec);
    const CONTENDED: Option<ErrorCode> = Some(ErrorCode::InProgress);
    let scenarios: Vec<(&str, u32, bool, Vec<RetryOp>)> = vec![
        (
            "transient_failures_back_off_then_die_at_the_ceiling",
            3,
            true,
            vec![Attempt("evt-1", 0), Failed("evt-1", 0, TRANSIENT), Attempt("evt-1", 0), Failed("evt-1", 0, TRANSIENT), Failed("evt-1", 0, TRANSIENT), DeadLettered("evt-1"), Tracked],
        ),
        ("a_terminal_failure_dies_on_its_first_attempt", 3, true, vec![Failed("evt-1", 0, TERMINAL), DeadLettered("evt-1"), Tracked]),
        (
            "with_no_sink_a_dead_message_nacks_with_a_growing_step",
            3,
            false,
            vec![Failed("evt-1", 0, TERMINAL), Failed("evt-1", 0, TERMINAL), Failed("evt-1", 0, TRANSIENT), Failed("evt-1", 0, TRANSIENT), Tracked],
        ),
        (
            "max_attempts_zero_has_no_ceiling",
            0,
            true,
            // The second failure carries no code: unclassified, so transient like the others.
            vec![Failed("evt-1", 0, TRANSIENT), Failed("evt-1", 0, None), Failed("evt-1", 0, TRANSIENT), Failed("evt-1", 0, TERMINAL)],
        ),
        ("a_broker_attempt_raises_the_count", 3, true, vec![Attempt("evt-1", 2), Failed("evt-1", 2, TRANSIENT)]),
        ("a_lower_broker_attempt_never_lowers_it", 5, true, vec![Failed("evt-1", 3, TRANSIENT), Attempt("evt-1", 0), Failed("evt-1", 0, TRANSIENT)]),
        (
            "contention_waits_its_own_step_and_never_counts",
            2,
            true,
            vec![
                Failed("evt-1", 0, CONTENDED),
                Failed("evt-1", 0, CONTENDED),
                Failed("evt-1", 0, CONTENDED),
                Attempt("evt-1", 0),
                Tracked,
                Failed("evt-1", 0, TRANSIENT),
                Tracked,
                Failed("evt-1", 0, CONTENDED),
            ],
        ),
        ("success_forgets_both_counts", 3, true, vec![Failed("evt-1", 0, TRANSIENT), Failed("evt-1", 0, CONTENDED), Tracked, Succeeded("evt-1"), Tracked, Attempt("evt-1", 0)]),
        (
            "a_failed_sink_keeps_the_attempt_and_the_redelivery_dies_again",
            2,
            true,
            vec![Failed("evt-1", 0, TRANSIENT), Failed("evt-1", 0, TRANSIENT), SinkFailed("evt-1", 2), Attempt("evt-1", 0), Failed("evt-1", 0, TRANSIENT), DeadLettered("evt-1"), Tracked],
        ),
        ("ids_are_counted_apart", 2, true, vec![Failed("evt-1", 0, TRANSIENT), Failed("evt-2", 0, TRANSIENT), Failed("evt-1", 0, TRANSIENT), Attempt("evt-2", 0), Tracked]),
        ("the_count_saturates_at_the_ceiling_of_u32", 0, true, vec![Attempt("evt-1", u32::MAX), Failed("evt-1", u32::MAX, TRANSIENT), Attempt("evt-1", 0)]),
    ];
    scenarios
        .into_iter()
        .map(|(name, max_attempts, sink, ops)| {
            let mut retries = edax_event::Retries::new(max_attempts, sink);
            let ops: Vec<Value> = ops
                .into_iter()
                .map(|op| match op {
                    Attempt(id, broker) => json!({ "op": "attempt", "id": id, "broker_attempt": broker, "want": retries.attempt(id, broker) }),
                    Failed(id, broker, code) => json!({
                        "op": "failed", "id": id, "broker_attempt": broker, "code": code.map(ErrorCode::as_str),
                        "want": verdict_json(retries.failed(id, broker, edax_event::Failure::of(code))),
                    }),
                    Succeeded(id) => {
                        retries.succeeded(id);
                        json!({ "op": "succeeded", "id": id })
                    }
                    DeadLettered(id) => {
                        retries.dead_lettered(id);
                        json!({ "op": "dead_lettered", "id": id })
                    }
                    SinkFailed(id, attempt) => {
                        json!({ "op": "sink_failed", "id": id, "attempt": attempt, "want": retries.sink_failed(id, attempt) })
                    }
                    Tracked => {
                        let t = retries.tracked();
                        json!({ "op": "tracked", "want": { "attempts": t.attempts, "contended": t.contended } })
                    }
                })
                .collect();
            json!({ "name": name, "max_attempts": max_attempts, "sink": sink, "ops": ops })
        })
        .collect()
}

/// The saga step's write for every loaded state and fold, whether a step folds an event, and the outbox
/// command id.
fn saga_cases() -> Vec<Value> {
    use edax_event::Loaded;
    let loaded_json = |loaded: Loaded| match loaded {
        Loaded::Absent => json!({ "state": "absent" }),
        Loaded::Active { version } => json!({ "state": "active", "version": version }),
    };
    let write_json = |write: edax_event::SagaWrite| match write {
        edax_event::SagaWrite::Nothing => json!({ "write": "nothing" }),
        edax_event::SagaWrite::Save { expected } => json!({ "write": "save", "expected": expected }),
        edax_event::SagaWrite::Complete { expected } => json!({ "write": "complete", "expected": expected }),
    };
    let mut cases = Vec::new();
    for loaded in [Loaded::Absent, Loaded::Active { version: 3 }, Loaded::Active { version: u64::MAX }] {
        for completes in [false, true] {
            for has_commands in [false, true] {
                let want = write_json(edax_event::saga_write(loaded, completes, has_commands));
                cases.push(json!({ "loaded": loaded_json(loaded), "completes": completes, "has_commands": has_commands, "want": want }));
            }
        }
    }
    // Whether a step folds an event, from whether the instance completed and the marks it saved: one row
    // per branch of `saga_admit`. A tombstone folds nothing, even under marks no runner wrote.
    let one = r#"{"order-7":{"sequence":1,"id":"evt-1"}}"#;
    let two = r#"{"order-7":{"sequence":2,"id":"evt-2"}}"#;
    let unsequenced = r#"{"order-7":{"id":"evt-u"}}"#;
    let admits: [(bool, &str, &str, Option<u64>, &str); 14] = [
        (true, "", "order-7", Some(1), "evt-1"),
        (true, "[1]", "order-7", Some(1), "evt-1"),
        (false, "", "order-7", Some(1), "evt-1"),
        (false, one, "order-7", Some(1), "evt-1"),
        (false, two, "order-7", Some(1), "evt-1"),
        (false, one, "order-7", Some(2), "evt-2"),
        (false, one, "payment-7", Some(1), "evt-p"),
        (false, unsequenced, "order-7", None, "evt-u"),
        (false, unsequenced, "order-7", None, "evt-v"),
        (false, two, "order-7", None, "evt-v"),
        (false, one, "order-7", Some(1), "evt-x"),
        (false, unsequenced, "order-7", Some(3), "evt-3"),
        (false, r#"{"café":{"sequence":18446744073709551615,"id":"évt"}}"#, "café", Some(u64::MAX), "évt"),
        (false, "[1]", "order-7", Some(1), "evt-1"),
    ];
    for (completed, folded, aggregate_id, sequence, id) in admits {
        let env = EventEnvelope { id: id.to_owned(), aggregate_id: aggregate_id.to_owned(), sequence, ..base_envelope() };
        let want = match edax_event::saga_admit(completed, folded, &env) {
            Ok(edax_event::SagaAdmit::Completed) => json!({ "completed": true }),
            Ok(edax_event::SagaAdmit::Repeat) => json!({ "repeat": true }),
            Ok(edax_event::SagaAdmit::Fold { folded }) => json!({ "folded": folded }),
            Err(e) => json!({ "error": e.to_string() }),
        };
        cases.push(json!({ "admit": { "completed": completed, "folded": folded, "aggregate_id": aggregate_id, "sequence": sequence, "id": id }, "want": want }));
    }
    for (correlation_id, version, index) in [("order-7", 1u64, 0u64), ("order-7", 2, 1), ("a:1", 2, 0), ("", 1, 0), ("café", u64::MAX, u64::MAX)] {
        cases.push(json!({
            "correlation_id": correlation_id, "version": version, "index": index,
            "command_id": edax_event::saga_command_id(correlation_id, version, index),
        }));
    }
    cases
}

#[test]
fn golden() {
    write_or_check("partition.json", partition_cases());
    write_or_check("envelopes.json", envelope_cases());
    write_or_check("upcast.json", upcast_cases());
    write_or_check("event_idempotency.json", event_idempotency_cases());
    write_or_check("causation.json", causation_cases());
    write_or_check("causation_originate.json", originate_cases());
    write_or_check("staleness.json", staleness_cases());
    write_or_check("envelope_decode.json", envelope_decode_cases());
    write_or_check("sequence_batch.json", sequence_batch_cases());
    write_or_check("snapshot_due.json", snapshot_due_cases());
    write_or_check("retries.json", retries_cases());
    write_or_check("retry_failure.json", retry_failure_cases());
    write_or_check("saga.json", saga_cases());
}

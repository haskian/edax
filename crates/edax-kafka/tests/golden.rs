//! Golden vectors: the cross-language Kafka contract.
//!
//! These cases are derived from edax-kafka and written to `testdata/golden/*.json`. Every binding's
//! test suite reads the same files and must produce byte-identical records, the same decode
//! failures and the same offset decisions. One drift in any binding fails its test.
//!
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax-kafka --test golden`.
//! Without the env var this test ASSERTS the committed files still match the code.

use edax_event::{Event, EventEnvelope, Format, Metadata, Namespace};
use edax_golden::write_or_check;
use edax_kafka::{
    content_type, decode_record, encode_record, record, KafkaHeader, KafkaRecord, OffsetTracker, CONTENT_TYPE,
};
use serde_json::{json, Value};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The format name a binding selects by, the one `Format::from_name` reads.
fn format_name(format: Format) -> &'static str {
    match format {
        Format::Msgpack => "msgpack",
        _ => "json",
    }
}

fn map(pairs: &[(&str, &str)]) -> Namespace {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
}

/// The base envelope, the one `envelopes.json` pins first; a case overrides only what it is about.
fn base() -> EventEnvelope {
    EventEnvelope {
        id: "evt-1".to_owned(),
        aggregate_id: "agg-1".to_owned(),
        event: Event { name: "InvoiceIssued".to_owned(), version: 1, data: vec![0xde, 0xad, 0xbe, 0xef] },
        sequence: Some(7),
        namespace: map(&[("tenant", "acme")]),
        metadata: map(&[("trace", "abc-123")]),
        timestamp: 1_700_000_000_000_000_000,
    }
}

/// No sequence, an empty payload and empty maps: the header set must be identical anyway.
fn unsequenced() -> EventEnvelope {
    EventEnvelope {
        id: "evt-2".to_owned(),
        aggregate_id: "agg-2".to_owned(),
        event: Event { name: "OrderPlaced".to_owned(), version: 2, data: Vec::new() },
        sequence: None,
        namespace: Namespace::new(),
        metadata: Metadata::new(),
        timestamp: 0,
    }
}

/// Non-ASCII in the key, the event name and the maps: the key and the `edax-event` header are raw
/// UTF-8 bytes, which a host that latin-1s them would fail here.
fn unicode() -> EventEnvelope {
    EventEnvelope {
        id: "evt-4".to_owned(),
        aggregate_id: "café-7".to_owned(),
        event: Event { name: "Paiement effectué".to_owned(), version: 3, data: "🦀".as_bytes().to_vec() },
        sequence: Some(9_007_199_254_740_993),
        namespace: map(&[("région", "eu")]),
        metadata: map(&[("acteur", "système")]),
        timestamp: 1_700_000_000_000_000_000,
    }
}

fn header(key: &str, value: &[u8]) -> KafkaHeader {
    KafkaHeader { key: key.to_owned(), value: value.to_vec() }
}

fn headers_json(headers: &[KafkaHeader]) -> Value {
    headers.iter().map(|h| json!({ "key": h.key, "value_hex": hex(&h.value) })).collect()
}

fn envelope_json(env: &EventEnvelope) -> Value {
    serde_json::to_value(env).unwrap()
}

/// What a producer puts on the wire: the key rule, the four headers, the value bytes.
fn kafka_record_cases() -> Vec<Value> {
    let cases: Vec<(&str, Format, Option<&str>, EventEnvelope)> = vec![
        ("json_keyed_by_aggregate_id", Format::Json, None, base()),
        ("msgpack_keyed_by_aggregate_id", Format::Msgpack, None, base()),
        // The override: a host keying by tenant to co-locate a tenant's aggregates.
        ("json_key_override", Format::Json, Some("tenant/acme"), base()),
        ("msgpack_key_override", Format::Msgpack, Some("tenant/acme"), base()),
        ("json_no_sequence_empty_payload", Format::Json, None, unsequenced()),
        ("msgpack_no_sequence_empty_payload", Format::Msgpack, None, unsequenced()),
        ("json_unicode", Format::Json, None, unicode()),
        ("msgpack_unicode", Format::Msgpack, None, unicode()),
    ];
    cases
        .into_iter()
        .map(|(name, format, key, env)| {
            let rec = encode_record(format, &env, key).expect("a built-in format has a media type");
            // The primitive path with the kernel's own bytes must build the identical record, or a
            // host serializer would get a different key or a different header set.
            assert_eq!(
                rec,
                record(&env, key, format.encode(&env), content_type(&rec.headers).unwrap()),
                "{name}"
            );
            json!({
                "name": name,
                "format": format_name(format),
                "key": key,
                "envelope": envelope_json(&env),
                "key_hex": hex(&rec.key),
                "value_hex": hex(&rec.value),
                "headers": headers_json(&rec.headers),
            })
        })
        .collect()
}

/// What a consumer makes of a fetched record: which format the headers select, and how a record it
/// cannot read fails.
fn kafka_decode_cases() -> Vec<Value> {
    let json_record = encode_record(Format::Json, &base(), None).expect("json has a media type");
    let msgpack_record = encode_record(Format::Msgpack, &base(), None).expect("msgpack has a media type");
    let without_content_type = |rec: &KafkaRecord| -> Vec<KafkaHeader> {
        rec.headers.iter().filter(|h| h.key != CONTENT_TYPE).cloned().collect()
    };
    let cases: Vec<(&str, Format, Vec<u8>, Vec<KafkaHeader>)> = vec![
        (
            // Anything written before this kernel, or by a client that drops headers.
            "no_content_type_falls_back_to_the_default",
            Format::Json,
            json_record.value.clone(),
            without_content_type(&json_record),
        ),
        (
            "content_type_overrides_the_default",
            Format::Json,
            msgpack_record.value.clone(),
            msgpack_record.headers.clone(),
        ),
        (
            "an_unknown_content_type_is_refused",
            Format::Json,
            json_record.value.clone(),
            [
                vec![header(CONTENT_TYPE, b"application/x-protobuf")],
                without_content_type(&json_record),
            ]
            .concat(),
        ),
        (
            // Ambiguous by construction: both sides must pick the first, not the last.
            "the_first_content_type_wins",
            Format::Json,
            msgpack_record.value.clone(),
            [msgpack_record.headers.clone(), vec![header(CONTENT_TYPE, b"application/vnd.edax.envelope+json")]]
                .concat(),
        ),
        (
            // The fallback is not a guess: bytes that are not the default format still fail.
            "header_less_bytes_in_another_format_do_not_decode",
            Format::Json,
            msgpack_record.value.clone(),
            Vec::new(),
        ),
    ];
    cases
        .into_iter()
        .map(|(name, default, value, headers)| {
            let want = match decode_record(default, &value, &headers) {
                Ok(env) => json!({ "envelope": envelope_json(&env) }),
                Err(error) => json!({ "error": error.to_string() }),
            };
            json!({
                "name": name,
                "default_format": format_name(default),
                "value_hex": hex(&value),
                "headers": headers_json(&headers),
                "want": want,
            })
        })
        .collect()
}

/// One step of an offset scenario. The settle steps name the i-th token `Book` handed out;
/// `AckToken` / `NackToken` settle a raw token no booking issued.
enum Step {
    Book(&'static str, i32, i64),
    Ack(usize),
    AckToken(i64),
    Nack(usize),
    NackToken(i64),
    Committed(&'static str, i32, i64),
    Reset(&'static str, i32),
    Stats,
}

fn ack_step(tracker: &OffsetTracker, token: i64) -> Value {
    let decision = tracker.ack(token);
    json!({ "op": "ack", "token": token, "want": { "commit": decision.commit, "offset": decision.offset } })
}

fn nack_step(tracker: &OffsetTracker, token: i64) -> Value {
    let decision = tracker.nack(token);
    json!({ "op": "nack", "token": token, "want": { "seek": decision.seek, "offset": decision.offset } })
}

/// Replay one scenario against a fresh tracker, recording what each step asked for and what the
/// kernel answered. A binding replays the same steps and must get the same answers.
fn replay(steps: &[Step]) -> Value {
    let tracker = OffsetTracker::new();
    let mut tokens: Vec<i64> = Vec::new();
    let mut out: Vec<Value> = Vec::new();
    for step in steps {
        out.push(match step {
            Step::Book(topic, partition, offset) => {
                let token = tracker.book(topic, *partition, *offset);
                tokens.push(token);
                json!({ "op": "book", "topic": topic, "partition": partition, "offset": offset, "want": { "token": token } })
            }
            Step::Ack(i) => ack_step(&tracker, tokens[*i]),
            Step::AckToken(token) => ack_step(&tracker, *token),
            Step::Nack(i) => nack_step(&tracker, tokens[*i]),
            Step::NackToken(token) => nack_step(&tracker, *token),
            Step::Committed(topic, partition, offset) => {
                tracker.committed(topic, *partition, *offset);
                json!({ "op": "committed", "topic": topic, "partition": partition, "offset": offset, "want": {} })
            }
            Step::Reset(topic, partition) => {
                tracker.reset(topic, *partition);
                json!({ "op": "reset", "topic": topic, "partition": partition, "want": {} })
            }
            Step::Stats => {
                let rows: Vec<Value> = tracker
                    .stats()
                    .iter()
                    .map(|s| {
                        json!({
                            "topic": s.topic,
                            "partition": s.partition,
                            "unsettled": s.unsettled,
                            "max_delivered": s.max_delivered,
                            "committed": s.committed,
                        })
                    })
                    .collect();
                json!({ "op": "stats", "want": { "rows": rows } })
            }
        });
    }
    Value::Array(out)
}

/// The watermark state machine, scenario by scenario. Every one ends in `stats`, so a binding that
/// answered each step right but kept the wrong state still fails.
fn kafka_offsets_cases() -> Vec<Value> {
    use Step::*;
    let scenarios: Vec<(&str, Vec<Step>)> = vec![
        (
            // The dispatcher acks in worker order: 2 and 1 settle while 0 is still in flight, so
            // the watermark stays at 0 until 0 settles.
            "out_of_order_acks_commit_the_watermark_only",
            vec![
                Book("t", 0, 0),
                Book("t", 0, 1),
                Book("t", 0, 2),
                Ack(2),
                Committed("t", 0, 0),
                Ack(1),
                Ack(0),
                Committed("t", 0, 3),
                Stats,
            ],
        ),
        (
            "a_nack_keeps_its_offset_in_flight_until_the_redelivery_settles",
            vec![
                Book("t", 0, 0),
                Book("t", 0, 1),
                Book("t", 0, 2),
                Ack(1),
                Committed("t", 0, 0),
                Nack(0),
                Ack(2),
                Book("t", 0, 0),
                Ack(0),
                Ack(3),
                Committed("t", 0, 3),
                Stats,
            ],
        ),
        (
            // Seeking to the later offset would move the single fetch position past the earlier
            // one, which would then never come back and would pin the watermark forever.
            "a_second_nack_rewinds_to_the_lowest_unsettled_offset",
            vec![Book("t", 0, 0), Book("t", 0, 1), Book("t", 0, 2), Nack(0), Nack(2), Stats],
        ),
        (
            "two_partitions_settle_independently",
            vec![
                Book("t", 0, 0),
                Book("t", 1, 0),
                Book("t", 0, 1),
                Book("t", 1, 1),
                Ack(3),
                Committed("t", 1, 0),
                Ack(0),
                Committed("t", 0, 1),
                Ack(1),
                Committed("t", 1, 2),
                Nack(2),
                Stats,
            ],
        ),
        (
            // Two commits in flight at once, the higher returning first: the guard records the
            // highest watermark committed, not the last one written.
            "the_guard_never_commits_a_watermark_already_recorded",
            vec![
                Book("t", 0, 0),
                Book("t", 0, 1),
                Book("t", 0, 2),
                Book("t", 0, 3),
                Ack(0),
                Ack(1),
                Committed("t", 0, 2),
                Committed("t", 0, 1),
                Ack(3),
                Stats,
            ],
        ),
        (
            // Revoke, re-assign, re-book: the pre-revoke ack must not discard the fresh booking.
            "a_partition_assigned_back_after_reset_ignores_its_stale_tokens",
            vec![
                Book("t", 0, 5),
                Book("t", 0, 6),
                Reset("t", 0),
                Book("t", 0, 5),
                Ack(0),
                NackToken(2),
                Ack(2),
                Committed("t", 0, 6),
                Stats,
            ],
        ),
        (
            "a_token_no_booking_issued_settles_nothing",
            vec![Book("t", 0, 0), AckToken(999), NackToken(0), Stats],
        ),
        (
            // Nothing booked it, so nothing records it: a watermark for an unowned partition would
            // refuse a lower, correct commit if it were ever assigned.
            "committed_for_an_unbooked_partition_is_a_no_op",
            vec![Book("t", 0, 0), Committed("t", 9, 100), Ack(0), Committed("t", 0, 1), Stats],
        ),
    ];
    scenarios
        .into_iter()
        .map(|(name, steps)| {
            assert!(matches!(steps.last(), Some(Step::Stats)), "{name} must end in stats");
            json!({ "name": name, "steps": replay(&steps) })
        })
        .collect()
}

#[test]
fn golden() {
    write_or_check("kafka_record.json", kafka_record_cases());
    write_or_check("kafka_decode.json", kafka_decode_cases());
    write_or_check("kafka_offsets.json", kafka_offsets_cases());
}

//! Golden vectors: the cross-language Kafka contract.
//!
//! These cases are derived from edax-kafka and written to `testdata/golden/*.json`. Every binding's
//! test suite reads the same files and must produce byte-identical records, the same decode
//! failures and the same offset decisions. One drift in any binding fails its test.
//!
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax-kafka --test golden`.
//! Without the env var this test ASSERTS the committed files still match the code.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a golden generator runs only as a test, and its panic is the failure report"
)]

use edax_event::testkit::{base_command, base_event, map};
use edax_event::{CommandMessage, EventMessage, Extensions, Format, Kind, Message, Metadata};
use edax_golden::{hex, write_or_check};
use edax_kafka::{
    check_media_types, content_type, dead_letter_record, dead_letter_source, decode_record, encode_record,
    partition_for, record, route, AckDecision, KafkaHeader, KafkaRecord, NackDecision, OffsetTracker, Route,
    CONTENT_TYPE,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};

fn event_with(edit: impl Fn(&mut EventMessage)) -> EventMessage {
    let mut msg = base_event();
    edit(&mut msg);
    msg
}

fn command_with(edit: impl Fn(&mut CommandMessage)) -> CommandMessage {
    let mut msg = base_command();
    edit(&mut msg);
    msg
}

/// No sequence, an empty payload and empty maps: the header set must be identical anyway.
fn unsequenced() -> EventMessage {
    event_with(|e| {
        e.id = "evt-2".to_owned();
        e.aggregate_id = "agg-2".to_owned();
        e.sequence = None;
        e.name = "OrderPlaced".to_owned();
        e.version = 2;
        e.data = Vec::new();
        e.occurred_at = Some(1_699_999_999_000_000_000);
        e.recorded_at = 0;
        e.extensions = Extensions::new();
        e.metadata = Metadata::new();
    })
}

/// Non-ASCII in the key, the name and the maps: the key and the value carry it as raw UTF-8 bytes,
/// which a host that latin-1s them would fail here.
fn unicode() -> EventMessage {
    event_with(|e| {
        e.id = "evt-4".to_owned();
        e.aggregate_id = "café-7".to_owned();
        e.sequence = Some(9_007_199_254_740_993);
        e.name = "Paiement effectué".to_owned();
        e.version = 3;
        e.data = "🦀".as_bytes().to_vec();
        e.metadata = map(&[("acteur", "système")]);
    })
}

/// A command with no target aggregate: its key falls back to its own id.
fn untargeted() -> CommandMessage {
    command_with(|c| {
        c.id = "cmd-2".to_owned();
        c.idempotency_key = None;
        c.aggregate_id = None;
        c.metadata = Metadata::new();
    })
}

fn headers_json(headers: &[KafkaHeader]) -> Value {
    headers.iter().map(|h| json!({ "key": h.key, "value_hex": hex(&h.value) })).collect()
}

/// One message of either kind encoded as a record, as a row: the record on success, the error
/// otherwise.
fn record_row<M: Message + Serialize>(name: &str, format: Format, key: Option<&str>, msg: &M) -> Value {
    let rec = encode_record(format, msg, key);
    if let Ok(rec) = &rec {
        // The primitive path with the kernel's own bytes must build the identical record, or a host
        // serializer would get a different key or a different header set.
        let value = format.encode(msg).unwrap();
        assert_eq!(*rec, record(msg, key, value, content_type(&rec.headers).unwrap()), "{name}");
    }
    let (key_hex, value_hex, headers, error) = match rec {
        Ok(rec) => (Some(hex(&rec.key)), Some(hex(&rec.value)), headers_json(&rec.headers), None),
        Err(error) => (None, None, Value::Null, Some(error.to_string())),
    };
    json!({
        "name": name,
        "kind": M::KIND.name(),
        "format": format.name(),
        "key": key,
        "message": serde_json::to_value(msg).unwrap(),
        "key_hex": key_hex,
        "value_hex": value_hex,
        "headers": headers,
        "error": error,
    })
}

/// The base event under a tenant: what a pool publishes, with the tenant as a typed field of the
/// value and composed into the key the producer passes.
fn tenanted() -> EventMessage {
    EventMessage { tenant: Some("acme".to_owned()), ..base_event() }
}

/// What a producer puts on the wire: the key rule, the `content-type` header, the value bytes.
fn kafka_record_cases() -> Vec<Value> {
    let bad_extension = |msg: &mut Extensions| {
        msg.insert("Traceparent".to_owned(), "00-a-b-01".to_owned());
    };
    let events: Vec<(&str, Format, Option<&str>, EventMessage)> = vec![
        ("event_json_keyed_by_aggregate_id", Format::Json, None, base_event()),
        ("event_msgpack_keyed_by_aggregate_id", Format::Msgpack, None, base_event()),
        // The override: a host keying by tenant to co-locate a tenant's aggregates.
        ("event_json_key_override", Format::Json, Some("tenant/acme"), base_event()),
        ("event_msgpack_key_override", Format::Msgpack, Some("tenant/acme"), base_event()),
        // What a pool producer passes: the ordering key composed with the tenant, the kernel's own
        // `tenant_key`, so two tenants' same-named aggregates never share a key or a partition.
        ("event_json_tenanted_keyed_by_the_composed_key", Format::Json, Some("4:acme:agg-1"), tenanted()),
        ("event_msgpack_tenanted_keyed_by_the_composed_key", Format::Msgpack, Some("4:acme:agg-1"), tenanted()),
        ("event_json_no_sequence_empty_payload", Format::Json, None, unsequenced()),
        ("event_msgpack_no_sequence_empty_payload", Format::Msgpack, None, unsequenced()),
        ("event_json_unicode", Format::Json, None, unicode()),
        ("event_msgpack_unicode", Format::Msgpack, None, unicode()),
        // A producer never mints what every consumer would refuse.
        ("event_bad_extension_key_is_refused", Format::Json, None, event_with(|e| bad_extension(&mut e.extensions))),
    ];
    let commands: Vec<(&str, Format, Option<&str>, CommandMessage)> = vec![
        ("command_json_keyed_by_aggregate_id", Format::Json, None, base_command()),
        ("command_msgpack_keyed_by_aggregate_id", Format::Msgpack, None, base_command()),
        ("command_json_without_aggregate_keyed_by_id", Format::Json, None, untargeted()),
        ("command_msgpack_without_aggregate_keyed_by_id", Format::Msgpack, None, untargeted()),
        ("command_json_key_override", Format::Json, Some("tenant/acme"), untargeted()),
        ("command_bad_extension_key_is_refused", Format::Json, None, command_with(|c| bad_extension(&mut c.extensions))),
    ];
    let events = events.iter().map(|(name, format, key, msg)| record_row(name, *format, *key, msg));
    let commands = commands.iter().map(|(name, format, key, msg)| record_row(name, *format, *key, msg));
    events.chain(commands).collect()
}

/// Broker placement: Java's default partitioner, which every shell passes as `partition=`. The first
/// six keys are Kafka's own `UtilsTest.testMurmur2` vectors, so a port is checked against Java and
/// not only against this kernel; `murmur2` carries the Java `int` those tests assert.
fn kafka_partition_cases() -> Vec<Value> {
    // `java_murmur2` is the Java `int` `Utils.murmur2` returns, from Kafka's `UtilsTest.testMurmur2`;
    // `None` where Kafka publishes no vector.
    let keys: [(&str, &[u8], Option<i32>); 11] = [
        ("kafka_utils_test_21", b"21", Some(-973_932_308)),
        ("kafka_utils_test_foobar", b"foobar", Some(-790_332_482)),
        ("kafka_utils_test_long", b"a-little-bit-long-string", Some(-985_981_536)),
        ("kafka_utils_test_longer", b"a-little-bit-longer-string", Some(-1_486_304_829)),
        ("kafka_utils_test_48_bytes", b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", Some(-58_897_971)),
        ("kafka_utils_test_abc", b"abc", Some(479_470_107)),
        // An empty aggregate id or key override still names one partition.
        ("empty_key", b"", None),
        ("aggregate_id", b"agg-1", None),
        // The key is UTF-8 bytes, never normalized.
        ("non_ascii_nfc", "caf\u{e9}-7".as_bytes(), None),
        ("non_ascii_nfd", "cafe\u{301}-7".as_bytes(), None),
        ("high_bytes", &[0xff, 0xfe, 0x80, 0x00, 0x7f], None),
    ];
    let mut cases: Vec<Value> = Vec::new();
    for (name, key, java_murmur2) in keys {
        for partitions in [1u32, 3, 12, 100, 2_147_483_647] {
            let expected = partition_for(key, partitions).unwrap();
            if let Some(java) = java_murmur2 {
                // `Utils.toPositive(murmur2(key)) % numPartitions`, computed from Java's own number.
                assert_eq!(expected, (java.cast_unsigned() & 0x7fff_ffff) % partitions, "{name} / {partitions}");
            }
            cases.push(json!({
                "name": name,
                "key_hex": hex(key),
                "java_murmur2": java_murmur2,
                "partitions": partitions,
                "expected": expected,
                "error": null,
            }));
        }
    }
    let refused = partition_for(b"agg-1", 0).unwrap_err();
    cases.push(json!({
        "name": "no_partitions",
        "key_hex": hex(b"agg-1"),
        "java_murmur2": null,
        "partitions": 0,
        "expected": null,
        "error": refused.to_string(),
    }));
    cases
}

/// What a consumer makes of a fetched record: which format and kind the headers select, and how a
/// record it cannot read fails.
fn kafka_decode_cases() -> Vec<Value> {
    let json_event = encode_record(Format::Json, &base_event(), None).unwrap();
    let msgpack_event = encode_record(Format::Msgpack, &base_event(), None).unwrap();
    let json_command = encode_record(Format::Json, &base_command(), None).unwrap();
    let msgpack_command = encode_record(Format::Msgpack, &base_command(), None).unwrap();
    let event_json = Format::Json.media_type(Kind::Event);
    let event_msgpack = Format::Msgpack.media_type(Kind::Event);
    let command_json = Format::Json.media_type(Kind::Command);
    let header = |media_type: &str| KafkaHeader::new(CONTENT_TYPE, media_type.as_bytes());
    let without_content_type = |rec: &KafkaRecord| -> Vec<KafkaHeader> {
        rec.headers.iter().filter(|h| h.key != CONTENT_TYPE).cloned().collect()
    };
    let events: Vec<(&str, Vec<u8>, Vec<KafkaHeader>)> = vec![
        (
            // Anything written before this kernel, or by a client that drops headers.
            "no_content_type_falls_back_to_the_default",
            json_event.value.clone(),
            without_content_type(&json_event),
        ),
        ("content_type_overrides_the_default", msgpack_event.value.clone(), msgpack_event.headers.clone()),
        (
            "an_unknown_content_type_is_refused",
            json_event.value.clone(),
            [vec![header("application/x-protobuf")], without_content_type(&json_event)].concat(),
        ),
        (
            // Ambiguous by construction: both sides must pick the first, not the last.
            "the_first_content_type_wins",
            msgpack_event.value.clone(),
            [msgpack_event.headers.clone(), vec![header(event_json)]].concat(),
        ),
        (
            // The fallback is not a guess: bytes that are not the default format still fail.
            "header_less_bytes_in_another_format_do_not_decode",
            msgpack_event.value.clone(),
            Vec::new(),
        ),
        (
            // Present and readable, so it names a format: the empty one, which nobody mints. A null
            // header value crosses as empty bytes, so this row is the null rule too.
            "an_empty_content_type_is_refused",
            json_event.value.clone(),
            vec![header("")],
        ),
        (
            // A client that upper-cased the name wrote a different header, so the default decodes.
            "an_upper_case_header_name_is_not_the_content_type",
            json_event.value.clone(),
            vec![KafkaHeader::new("Content-Type", event_msgpack.as_bytes())],
        ),
        (
            // Reads as absent, and does not reach past itself to the second header.
            "a_non_utf8_first_content_type_reads_as_absent",
            json_event.value.clone(),
            vec![KafkaHeader::new(CONTENT_TYPE, &[0xff, 0xfe]), header(event_msgpack)],
        ),
        (
            // Parameters are not stripped: the match is exact, so this is no kernel format.
            "a_parameterized_media_type_is_refused",
            json_event.value.clone(),
            vec![header(&format!("{event_json}; charset=utf-8"))],
        ),
        (
            // The 0.1 media type, minted before kinds: no longer a kernel format.
            "the_envelope_media_type_is_refused",
            json_event.value.clone(),
            vec![header("application/vnd.edax.envelope+json")],
        ),
        ("a_command_record_is_refused_by_kind", json_command.value.clone(), json_command.headers.clone()),
        (
            // The header decides the kind, so the bytes are never read: event bytes do not help.
            "event_bytes_under_a_command_media_type_are_refused_by_kind",
            json_event.value.clone(),
            vec![header(command_json)],
        ),
    ];
    let commands: Vec<(&str, Vec<u8>, Vec<KafkaHeader>)> = vec![
        ("a_json_command_decodes_by_its_content_type", json_command.value.clone(), json_command.headers.clone()),
        ("a_msgpack_command_decodes_by_its_content_type", msgpack_command.value.clone(), msgpack_command.headers.clone()),
        // No default for a command: it always carries the header, so a record without one is not a
        // command record.
        ("a_header_less_command_is_refused", json_command.value.clone(), Vec::new()),
        ("an_event_record_is_refused_by_kind", json_event.value.clone(), json_event.headers.clone()),
        (
            "command_bytes_under_an_event_media_type_are_refused_by_kind",
            json_command.value.clone(),
            vec![header(event_json)],
        ),
    ];
    let events = events.iter().map(|(name, value, headers)| decode_row::<EventMessage>(name, value, headers));
    let commands = commands.iter().map(|(name, value, headers)| decode_row::<CommandMessage>(name, value, headers));
    events.chain(commands).collect()
}

/// One fetched record decoded as kind `M`, as a row. Only an event reads the JSON default: a
/// header-less command is refused whatever the default, so its row names none.
fn decode_row<M: Message + Serialize + DeserializeOwned>(name: &str, value: &[u8], headers: &[KafkaHeader]) -> Value {
    let default = (M::KIND == Kind::Event).then_some(Format::Json);
    let want = match decode_record::<M>(Format::Json, value, headers) {
        Ok(msg) => json!({ "message": serde_json::to_value(&msg).unwrap() }),
        Err(error) => json!({ "error": error.to_string() }),
    };
    json!({
        "name": name,
        "kind": M::KIND.name(),
        "default_format": default.map(|format| format.name()),
        "value_hex": hex(value),
        "headers": headers_json(headers),
        "want": want,
    })
}

/// The routing rule every shell runs: which registered serializer a fetched record's headers pick,
/// the default, or none (poison). The registry is the shell's, so each row carries its own, and a
/// registry that names a media type twice is refused before any record routes against it.
fn kafka_route_cases() -> Vec<Value> {
    let json = Format::Json.media_type(Kind::Event);
    let msgpack = Format::Msgpack.media_type(Kind::Event);
    let command_json = Format::Json.media_type(Kind::Command);
    let header = KafkaHeader::new;
    let cases: Vec<(&str, Vec<KafkaHeader>, Vec<&str>)> = vec![
        ("no_headers_routes_to_the_default", Vec::new(), vec![json, msgpack]),
        // A command always names its media type, so a header-less record under a command default is poison.
        ("no_headers_under_a_command_default_is_headerless", Vec::new(), vec![command_json]),
        ("a_foreign_header_only_routes_to_the_default", vec![header("traceparent", b"00-0af7651916cd43dd-01")], vec![json]),
        ("a_registered_media_type_names_its_index", vec![header(CONTENT_TYPE, msgpack.as_bytes())], vec![json, msgpack]),
        // One format is two media types, one per kind, and each routes to its own serializer.
        (
            "a_command_media_type_names_its_own_index",
            vec![header(CONTENT_TYPE, command_json.as_bytes())],
            vec![json, command_json],
        ),
        (
            "a_command_media_type_nobody_registered_is_unknown",
            vec![header(CONTENT_TYPE, command_json.as_bytes())],
            vec![json, msgpack],
        ),
        ("a_host_media_type_is_as_good_as_a_kernel_one", vec![header(CONTENT_TYPE, b"application/x-test-id")], vec![json, "application/x-test-id"]),
        ("an_unregistered_media_type_is_unknown", vec![header(CONTENT_TYPE, b"application/x-protobuf")], vec![json, msgpack]),
        ("nothing_registered_makes_every_media_type_unknown", vec![header(CONTENT_TYPE, json.as_bytes())], Vec::new()),
        // A null value crosses as empty bytes, so this row is the null rule too: poison, not default.
        ("an_empty_value_is_unknown_not_the_default", vec![header(CONTENT_TYPE, b"")], vec![json, msgpack]),
        ("the_name_is_matched_case_sensitively", vec![header("Content-Type", msgpack.as_bytes())], vec![json, msgpack]),
        (
            "the_first_of_two_wins",
            vec![header(CONTENT_TYPE, msgpack.as_bytes()), header(CONTENT_TYPE, json.as_bytes())],
            vec![json, msgpack],
        ),
        (
            "a_non_utf8_first_value_reads_as_absent",
            vec![header(CONTENT_TYPE, &[0xff, 0xfe]), header(CONTENT_TYPE, json.as_bytes())],
            vec![json, msgpack],
        ),
        (
            "parameters_are_not_stripped",
            vec![header(CONTENT_TYPE, format!("{json}; charset=utf-8").as_bytes())],
            vec![json, msgpack],
        ),
        ("a_media_type_registered_twice_is_refused", Vec::new(), vec![json, "application/x-test-id", json]),
        ("a_host_media_type_registered_twice_is_refused", Vec::new(), vec!["application/x-test-id", "application/x-test-id"]),
    ];
    cases
        .into_iter()
        .map(|(name, headers, registered)| {
            if let Err(refused) = check_media_types(&registered) {
                let want = json!({ "error": refused.to_string() });
                return json!({ "name": name, "headers": headers_json(&headers), "registered": registered, "want": want });
            }
            let default = if name.contains("command_default") { Kind::Command } else { Kind::Event };
            let want = match route(&headers, &registered, default) {
                Route::Default => json!({ "route": "default" }),
                Route::Headerless => json!({ "route": "headerless" }),
                Route::Registered(index) => json!({ "route": "registered", "index": index }),
                Route::Unknown(media_type) => json!({ "route": "unknown", "media_type": media_type }),
            };
            // `content_type` is what `route` reads; pinning it here keeps the two from drifting.
            assert_eq!(
                content_type(&headers).is_none(),
                matches!(want["route"].as_str(), Some("default" | "headerless")),
                "{name}"
            );
            json!({
                "name": name,
                "headers": headers_json(&headers),
                "registered": registered,
                "default_kind": default.name(),
                "want": want,
            })
        })
        .collect()
}

/// The record a dead-letter sink publishes, and the source its reader recovers from it.
fn kafka_dead_letter_cases() -> Vec<Value> {
    let fetched = KafkaRecord {
        key: b"agg-1".to_vec(),
        value: Format::Json.encode(&base_event()).unwrap(),
        headers: vec![
            KafkaHeader::new(CONTENT_TYPE, Format::Json.media_type(Kind::Event).as_bytes()),
            KafkaHeader::new("traceparent", b"00-0af7651916cd43dd-01"),
            KafkaHeader::new("x-binary", &[0xff, 0x00]),
        ],
    };
    let keyless = KafkaRecord { key: Vec::new(), value: Vec::new(), headers: Vec::new() };
    let once = dead_letter_record(&fetched, "orders", 3, 42, "codec: unknown format: application/x-protobuf");
    let cases: Vec<(&str, KafkaRecord, &str, i32, i64, &str)> = vec![
        ("keeps_key_value_and_headers_verbatim", fetched.clone(), "orders", 3, 42, "codec: bad payload"),
        // A keyless tombstone from a foreign producer: both cross as empty bytes, and stay empty.
        ("a_keyless_tombstone_stays_empty", keyless, "orders", 0, 0, "codec: no message"),
        ("non_ascii_topic_and_error", fetched.clone(), "commandes-é", 0, 7, "rejected: montant négatif"),
        ("extreme_coordinates", fetched, "t", i32::MAX, i64::MAX, ""),
        // Republished and dead-lettered again: two sets of headers, and the reader takes the last.
        ("dead_lettered_twice_reads_the_last_source", once, "orders-retry", 0, 5, "store: still failing"),
    ];
    let mut rows: Vec<Value> = cases
        .into_iter()
        .map(|(name, record, topic, partition, offset, error)| {
            let dead = dead_letter_record(&record, topic, partition, offset, error);
            let source = dead_letter_source(&dead.headers).unwrap();
            assert_eq!((source.topic.as_str(), source.partition, source.offset), (topic, partition, offset), "{name}");
            json!({
                "name": name,
                "record": { "key_hex": hex(&record.key), "value_hex": hex(&record.value), "headers": headers_json(&record.headers) },
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "error": error,
                "want": {
                    "key_hex": hex(&dead.key),
                    "value_hex": hex(&dead.value),
                    "headers": headers_json(&dead.headers),
                    "source": { "topic": source.topic, "partition": source.partition, "offset": source.offset, "error": source.error },
                },
            })
        })
        .collect();
    // The reader refuses to guess: a missing or unreadable coordinate is no source at all.
    let refusals: Vec<(&str, Vec<KafkaHeader>)> = vec![
        ("no_dead_letter_headers", vec![KafkaHeader::new(CONTENT_TYPE, b"application/json")]),
        (
            "a_missing_offset",
            vec![
                KafkaHeader::new("edax-dlq-topic", b"t"),
                KafkaHeader::new("edax-dlq-partition", b"0"),
                KafkaHeader::new("edax-dlq-error", b"x"),
            ],
        ),
        (
            "a_partition_that_is_not_a_number",
            vec![
                KafkaHeader::new("edax-dlq-topic", b"t"),
                KafkaHeader::new("edax-dlq-partition", b"zero"),
                KafkaHeader::new("edax-dlq-offset", b"0"),
                KafkaHeader::new("edax-dlq-error", b"x"),
            ],
        ),
        (
            "a_missing_partition",
            vec![
                KafkaHeader::new("edax-dlq-topic", b"t"),
                KafkaHeader::new("edax-dlq-offset", b"0"),
                KafkaHeader::new("edax-dlq-error", b"x"),
            ],
        ),
        (
            "an_offset_that_is_not_a_number",
            vec![
                KafkaHeader::new("edax-dlq-topic", b"t"),
                KafkaHeader::new("edax-dlq-partition", b"0"),
                KafkaHeader::new("edax-dlq-offset", b"-"),
                KafkaHeader::new("edax-dlq-error", b"x"),
            ],
        ),
        (
            "a_missing_error",
            vec![
                KafkaHeader::new("edax-dlq-topic", b"t"),
                KafkaHeader::new("edax-dlq-partition", b"0"),
                KafkaHeader::new("edax-dlq-offset", b"0"),
            ],
        ),
        (
            "a_topic_that_is_not_utf8",
            vec![
                KafkaHeader::new("edax-dlq-topic", &[0xff]),
                KafkaHeader::new("edax-dlq-partition", b"0"),
                KafkaHeader::new("edax-dlq-offset", b"0"),
                KafkaHeader::new("edax-dlq-error", b"x"),
            ],
        ),
    ];
    for (name, headers) in refusals {
        assert!(dead_letter_source(&headers).is_none(), "{name}");
        rows.push(json!({ "name": name, "headers": headers_json(&headers), "want": { "source": null } }));
    }
    rows
}

/// One step of an offset scenario. The settle steps name the i-th token `Book` handed out;
/// `AckToken` / `NackToken` settle a raw token no booking issued.
enum Step {
    Book(&'static str, i32, i64),
    Ack(usize),
    AckToken(i64),
    Nack(usize),
    NackToken(i64),
    IsLive(usize),
    IsLiveToken(i64),
    Due,
    Committed(&'static str, i32, i64),
    Reset(&'static str, i32),
    Stats,
}

/// A decision as the vector carries it: its name, and the offset only where the decision has one.
fn ack_step(tracker: &OffsetTracker, token: i64) -> Value {
    let want = match tracker.ack(token) {
        AckDecision::Settled => json!({ "decision": "settled" }),
        AckDecision::Stale => json!({ "decision": "stale" }),
    };
    json!({ "op": "ack", "token": token, "want": want })
}

fn nack_step(tracker: &OffsetTracker, token: i64) -> Value {
    let want = match tracker.nack(token) {
        NackDecision::Seek { offset, .. } => json!({ "decision": "seek", "offset": offset }),
        NackDecision::Stale => json!({ "decision": "stale" }),
    };
    json!({ "op": "nack", "token": token, "want": want })
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
                let want = match tracker.book(topic, *partition, *offset) {
                    Ok(token) => {
                        tokens.push(token);
                        json!({ "token": token })
                    }
                    // A refused booking hands out no token, so it takes no index in `tokens`.
                    Err(error) => json!({ "error": error.to_string() }),
                };
                json!({ "op": "book", "topic": topic, "partition": partition, "offset": offset, "want": want })
            }
            Step::Ack(i) => ack_step(&tracker, tokens[*i]),
            Step::AckToken(token) => ack_step(&tracker, *token),
            Step::Nack(i) => nack_step(&tracker, tokens[*i]),
            Step::NackToken(token) => nack_step(&tracker, *token),
            Step::IsLive(i) => json!({ "op": "is_live", "token": tokens[*i], "want": { "live": tracker.is_live(tokens[*i]) } }),
            Step::IsLiveToken(token) => json!({ "op": "is_live", "token": token, "want": { "live": tracker.is_live(*token) } }),
            Step::Due => {
                let rows: Vec<Value> = tracker
                    .due()
                    .iter()
                    .map(|d| json!({ "topic": d.topic, "partition": d.partition, "offset": d.offset }))
                    .collect();
                json!({ "op": "due", "want": { "rows": rows } })
            }
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
                            "live": s.live,
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
            // the watermark stays at 0 until 0 settles. Nothing is due until it moves past the
            // last commit.
            "out_of_order_acks_commit_the_watermark_only",
            vec![
                Book("t", 0, 0),
                Book("t", 0, 1),
                Book("t", 0, 2),
                Due,
                Ack(2),
                Due,
                Committed("t", 0, 0),
                Due,
                Ack(1),
                Due,
                Ack(0),
                Due,
                Committed("t", 0, 3),
                Due,
                Stats,
            ],
        ),
        (
            // A nack rewinds the partition: every token in it is retired, the queued copies are
            // dead (`is_live` false, their acks stale), and the offsets stay in flight until their
            // redelivered copies settle. This is what keeps per-key order through a nack.
            "a_nack_rewinds_the_partition_and_retires_every_token",
            vec![
                Book("t", 0, 0),
                Book("t", 0, 1),
                Book("t", 0, 2),
                Nack(0),
                IsLive(0),
                IsLive(1),
                IsLive(2),
                Ack(1),
                Nack(2),
                Due,
                Stats,
                Book("t", 0, 0),
                Book("t", 0, 1),
                Book("t", 0, 2),
                IsLive(3),
                Ack(3),
                Ack(4),
                Ack(5),
                Due,
                Committed("t", 0, 3),
                Stats,
            ],
        ),
        (
            // The redelivery after a seek re-fetches offsets already acked: a re-booking of an
            // offset that is no longer in flight is a fresh delivery, and settles like one.
            "a_seek_rebooks_offsets_already_acked",
            vec![
                Book("t", 0, 0),
                Book("t", 0, 1),
                Book("t", 0, 2),
                Ack(1),
                Nack(0),
                Stats,
                Book("t", 0, 0),
                Book("t", 0, 1),
                Book("t", 0, 2),
                Ack(4),
                Due,
                Ack(3),
                Ack(5),
                Due,
                Stats,
            ],
        ),
        (
            // Seeking to the later offset would move the single fetch position past the earlier
            // one, which would then never come back and would pin the watermark forever.
            "a_second_nack_rewinds_to_the_lowest_unsettled_offset",
            vec![Book("t", 0, 0), Book("t", 0, 1), Book("t", 0, 2), Nack(2), Book("t", 0, 2), Nack(3), Stats],
        ),
        (
            // A rewind is per partition: the other partition's tokens stay live and settle.
            "two_partitions_settle_and_rewind_independently",
            vec![
                Book("t", 0, 0),
                Book("t", 1, 0),
                Book("t", 0, 1),
                Book("t", 1, 1),
                Ack(3),
                Committed("t", 1, 0),
                Ack(0),
                Nack(2),
                IsLive(1),
                Ack(1),
                Due,
                Committed("t", 1, 2),
                Committed("t", 0, 1),
                Due,
                Stats,
            ],
        ),
        (
            // Two commits in flight at once, the higher returning first: the tracker records the
            // highest watermark committed, not the last one written, so nothing lower is due again.
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
                Due,
                Ack(3),
                Due,
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
                IsLive(0),
                Due,
                Book("t", 0, 5),
                Ack(0),
                NackToken(2),
                Ack(2),
                Due,
                Committed("t", 0, 6),
                Stats,
            ],
        ),
        (
            "a_token_no_booking_issued_settles_nothing",
            vec![Book("t", 0, 0), AckToken(999), NackToken(0), IsLiveToken(999), IsLiveToken(0), Stats],
        ),
        (
            // Nothing booked it, so nothing records it: a watermark for an unowned partition would
            // refuse a lower, correct commit if it were ever assigned. An unknown topic likewise.
            "committed_for_an_unbooked_partition_is_a_no_op",
            vec![Book("t", 0, 0), Committed("t", 9, 100), Committed("u", 0, 7), Ack(0), Due, Committed("t", 0, 1), Stats],
        ),
        (
            // A rebalance listener resets every revoked partition blindly, including ones this
            // tracker never booked: a no-op that leaves the partitions it owns untouched. The last
            // partition of a topic takes the topic with it.
            "reset_of_an_unknown_partition_or_topic_is_a_no_op",
            vec![
                Book("t", 0, 4),
                Book("v", 1, 0),
                Book("v", 2, 0),
                Reset("t", 9),
                Reset("u", 0),
                Reset("v", 2),
                Ack(0),
                Ack(1),
                Due,
                Reset("v", 1),
                Due,
                Stats,
            ],
        ),
        (
            // Compaction or a transaction marker leaves holes: the watermark is one past the
            // highest delivered, not the count of deliveries.
            "an_offset_gap_commits_past_the_gap",
            vec![Book("t", 0, 0), Book("t", 0, 5), Ack(1), Due, Ack(0), Due, Stats],
        ),
        (
            // `i64::MAX + 1` does not exist, so the watermark saturates: the restart redelivers that
            // one offset, which at-least-once allows, rather than wrapping below every commit.
            "the_highest_offset_saturates_the_watermark",
            vec![Book("t", 0, 9_223_372_036_854_775_807), Ack(0), Due, Stats],
        ),
        (
            // Kafka numbers a partition from 0: a negative offset is a client fault, refused
            // before it takes a token or creates the partition.
            "a_negative_offset_is_refused",
            vec![Book("t", 0, -7), Book("t", 0, 0), Ack(0), Due, Stats],
        ),
        (
            // Topics report in byte order: upper case before lower case before non-ASCII.
            "topics_report_in_byte_order",
            vec![
                Book("\u{e9}t\u{e9}", 0, 0),
                Book("b", 1, 0),
                Book("b", 0, 0),
                Book("B", 0, 0),
                Book("a", 0, 0),
                Ack(0),
                Ack(1),
                Ack(2),
                Ack(3),
                Ack(4),
                Due,
                Stats,
            ],
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
    write_or_check("kafka_partition.json", kafka_partition_cases());
    write_or_check("kafka_decode.json", kafka_decode_cases());
    write_or_check("kafka_route.json", kafka_route_cases());
    write_or_check("kafka_dead_letter.json", kafka_dead_letter_cases());
    write_or_check("kafka_offsets.json", kafka_offsets_cases());
}

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

use edax_core::{ErrorCode, Tenancy};
use edax_event::testkit::{base_command, base_event, map};
use edax_event::{CommandMessage, EventMessage, Extensions, Format, Kind, Message, Metadata};
use edax_golden::{hex, refusal, write_or_check};
use edax_kafka::{
    acks_warning, broker_error, check_config, check_consumer, check_dead_sink, check_media_types, content_type,
    dead_letter_record, dead_letter_source, dead_letter_stub, partition_for, record, route, AckDecision,
    DeadLetterSource, KafkaHeader, KafkaRecord, NackDecision, OffsetTracker, Route, ACKS_ALL, CONTENT_TYPE,
};
use serde::Serialize;
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
    headers
        .iter()
        .map(|h| json!({ "key": h.key, "value_hex": hex(&h.value) }))
        .collect()
}

/// `msg` as the record a producer with the kernel codec in `format` sends: the value first, so a value
/// the codec refuses fails before the key rule, as in every shell.
fn encoded<M: Message + Serialize>(
    format: Format,
    msg: &M,
    tenancy: Tenancy,
    key: Option<&str>,
) -> Result<KafkaRecord, edax_core::Error> {
    record(msg, tenancy, key, format.encode(msg)?, format.media_type(M::KIND))
}

/// One message of either kind encoded as a record under `tenancy`, as a row: the record on success,
/// the error otherwise.
fn record_row<M: Message + Serialize>(
    name: &str,
    format: Format,
    tenancy: Tenancy,
    key: Option<&str>,
    msg: &M,
) -> Value {
    let rec = encoded(format, msg, tenancy, key);
    let (key_hex, value_hex, headers, error) = match rec {
        Ok(rec) => (
            Some(hex(&rec.key)),
            Some(hex(&rec.value)),
            headers_json(&rec.headers),
            None,
        ),
        Err(error) => (None, None, Value::Null, Some(error.to_string())),
    };
    json!({
        "name": name,
        "kind": M::KIND.as_str(),
        "format": format.as_str(),
        "tenancy": tenancy.as_str(),
        "key": key,
        "message": serde_json::to_value(msg).unwrap(),
        "key_hex": key_hex,
        "value_hex": value_hex,
        "headers": headers,
        "error": error,
    })
}

/// The base event under a tenant: what a pool publishes, with the tenant as a typed field of the
/// value and, under a pool, composed into the key.
fn tenanted() -> EventMessage {
    EventMessage {
        tenant: Some("acme".to_owned()),
        ..base_event()
    }
}

/// The base command under a tenant, `edit`ed.
fn tenanted_command(edit: impl Fn(&mut CommandMessage)) -> CommandMessage {
    command_with(|c| {
        edit(c);
        c.tenant = Some("acme".to_owned());
    })
}

/// What a producer puts on the wire: the key rule under each tenancy, the `content-type` header, the
/// value bytes.
fn kafka_record_cases() -> Vec<Value> {
    use Tenancy::{Bridge, Pool, Silo};
    let bad_extension = |msg: &mut Extensions| {
        msg.insert("Traceparent".to_owned(), "00-a-b-01".to_owned());
    };
    let events: Vec<(&str, Format, Tenancy, Option<&str>, EventMessage)> = vec![
        (
            "event_json_keyed_by_aggregate_id",
            Format::Json,
            Silo,
            None,
            base_event(),
        ),
        (
            "event_msgpack_keyed_by_aggregate_id",
            Format::Msgpack,
            Silo,
            None,
            base_event(),
        ),
        // The override: a host keying by tenant to co-locate a tenant's aggregates.
        (
            "event_json_key_override",
            Format::Json,
            Silo,
            Some("tenant/acme"),
            base_event(),
        ),
        (
            "event_msgpack_key_override",
            Format::Msgpack,
            Silo,
            Some("tenant/acme"),
            base_event(),
        ),
        // A pool composes the tenant into the ordering key, so two tenants' same-named aggregates
        // never share a key or a partition, whichever SDK produced the record.
        (
            "event_json_pool_keyed_by_the_composed_key",
            Format::Json,
            Pool,
            None,
            tenanted(),
        ),
        (
            "event_msgpack_pool_keyed_by_the_composed_key",
            Format::Msgpack,
            Pool,
            None,
            tenanted(),
        ),
        // A silo and a bridge carry the tenant in the value only: the key stays the bare aggregate.
        (
            "event_json_silo_keeps_a_tenanted_key_bare",
            Format::Json,
            Silo,
            None,
            tenanted(),
        ),
        (
            "event_json_bridge_keeps_a_tenanted_key_bare",
            Format::Json,
            Bridge,
            None,
            tenanted(),
        ),
        // An override replaces the rule outright, under a pool too.
        (
            "event_json_pool_key_override_is_kept_verbatim",
            Format::Json,
            Pool,
            Some("tenant/acme"),
            tenanted(),
        ),
        (
            "event_pool_without_a_tenant_is_refused",
            Format::Json,
            Pool,
            None,
            base_event(),
        ),
        (
            "event_bridge_without_a_tenant_is_refused",
            Format::Json,
            Bridge,
            None,
            base_event(),
        ),
        (
            "event_json_no_sequence_empty_payload",
            Format::Json,
            Silo,
            None,
            unsequenced(),
        ),
        (
            "event_msgpack_no_sequence_empty_payload",
            Format::Msgpack,
            Silo,
            None,
            unsequenced(),
        ),
        ("event_json_unicode", Format::Json, Silo, None, unicode()),
        ("event_msgpack_unicode", Format::Msgpack, Silo, None, unicode()),
        // A producer never mints what every consumer would refuse.
        (
            "event_bad_extension_key_is_refused",
            Format::Json,
            Silo,
            None,
            event_with(|e| bad_extension(&mut e.extensions)),
        ),
    ];
    let commands: Vec<(&str, Format, Tenancy, Option<&str>, CommandMessage)> = vec![
        (
            "command_json_keyed_by_aggregate_id",
            Format::Json,
            Silo,
            None,
            base_command(),
        ),
        (
            "command_msgpack_keyed_by_aggregate_id",
            Format::Msgpack,
            Silo,
            None,
            base_command(),
        ),
        (
            "command_json_without_aggregate_keyed_by_id",
            Format::Json,
            Silo,
            None,
            untargeted(),
        ),
        (
            "command_msgpack_without_aggregate_keyed_by_id",
            Format::Msgpack,
            Silo,
            None,
            untargeted(),
        ),
        (
            "command_json_key_override",
            Format::Json,
            Silo,
            Some("tenant/acme"),
            untargeted(),
        ),
        (
            "command_json_pool_keyed_by_the_composed_aggregate",
            Format::Json,
            Pool,
            None,
            tenanted_command(|_| ()),
        ),
        (
            "command_json_pool_without_aggregate_keyed_by_the_composed_id",
            Format::Json,
            Pool,
            None,
            tenanted_command(|c| c.aggregate_id = None),
        ),
        (
            "command_pool_without_a_tenant_is_refused",
            Format::Json,
            Pool,
            None,
            base_command(),
        ),
        (
            "command_bad_extension_key_is_refused",
            Format::Json,
            Silo,
            None,
            command_with(|c| bad_extension(&mut c.extensions)),
        ),
    ];
    let events = events
        .iter()
        .map(|(name, format, tenancy, key, msg)| record_row(name, *format, *tenancy, *key, msg));
    let commands = commands
        .iter()
        .map(|(name, format, tenancy, key, msg)| record_row(name, *format, *tenancy, *key, msg));
    events.chain(commands).collect()
}

/// Broker placement: Java's default partitioner, which every shell passes as `partition=`. The first
/// six keys are Kafka's own `UtilsTest.testMurmur2` vectors, so a port is checked against Java and
/// not only against this kernel; `murmur2` carries the Java `int` those tests assert.
fn kafka_partition_cases() -> Vec<Value> {
    // `java_murmur2` is the Java `int` `Utils.murmur2` returns, from Kafka's `UtilsTest.testMurmur2`;
    // `None` where Kafka publishes no vector.
    let keys: [(&str, &[u8], Option<i32>); 13] = [
        ("kafka_utils_test_21", b"21", Some(-973_932_308)),
        ("kafka_utils_test_foobar", b"foobar", Some(-790_332_482)),
        ("kafka_utils_test_long", b"a-little-bit-long-string", Some(-985_981_536)),
        (
            "kafka_utils_test_longer",
            b"a-little-bit-longer-string",
            Some(-1_486_304_829),
        ),
        (
            "kafka_utils_test_48_bytes",
            b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8",
            Some(-58_897_971),
        ),
        ("kafka_utils_test_abc", b"abc", Some(479_470_107)),
        // An empty aggregate id or key override still names one partition.
        ("empty_key", b"", None),
        ("aggregate_id", b"agg-1", None),
        // The key is UTF-8 bytes, never normalized.
        ("non_ascii_nfc", "caf\u{e9}-7".as_bytes(), None),
        ("non_ascii_nfd", "cafe\u{301}-7".as_bytes(), None),
        ("high_bytes", &[0xff, 0xfe, 0x80, 0x00, 0x7f], None),
        // Two tenants' `order-1` under a pool: the composed keys, which the memory broker must place
        // where Kafka does.
        ("pooled_acme_order", b"4:acme:order-1", None),
        ("pooled_other_order", b"5:other:order-1", None),
    ];
    let mut cases: Vec<Value> = Vec::new();
    for (name, key, java_murmur2) in keys {
        for partitions in [1u32, 3, 12, 100, 2_147_483_647] {
            let expected = partition_for(key, partitions).unwrap();
            if let Some(java) = java_murmur2 {
                // `Utils.toPositive(murmur2(key)) % numPartitions`, computed from Java's own number.
                assert_eq!(
                    expected,
                    (java.cast_unsigned() & 0x7fff_ffff) % partitions,
                    "{name} / {partitions}"
                );
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

/// What a consumer makes of a fetched record: the serializer [`route`] picks by its headers out of the
/// consumer's registry, and what that serializer decodes, or why the record is poison. Each row carries
/// the registry as the consumer builds it, kernel serializers only, the default first: a replayer builds
/// that consumer and does not guess one.
fn kafka_decode_cases() -> Vec<Value> {
    let json_event = encoded(Format::Json, &base_event(), Tenancy::Silo, None).unwrap();
    let msgpack_event = encoded(Format::Msgpack, &base_event(), Tenancy::Silo, None).unwrap();
    let json_command = encoded(Format::Json, &base_command(), Tenancy::Silo, None).unwrap();
    let msgpack_command = encoded(Format::Msgpack, &base_command(), Tenancy::Silo, None).unwrap();
    let event_json = Format::Json.media_type(Kind::Event);
    let event_msgpack = Format::Msgpack.media_type(Kind::Event);
    let command_json = Format::Json.media_type(Kind::Command);
    let header = |media_type: &str| KafkaHeader::new(CONTENT_TYPE, media_type.as_bytes());
    let without_content_type = |rec: &KafkaRecord| -> Vec<KafkaHeader> {
        rec.headers.iter().filter(|h| h.key != CONTENT_TYPE).cloned().collect()
    };
    let events = vec![(Format::Json, Kind::Event), (Format::Msgpack, Kind::Event)];
    let commands = vec![(Format::Json, Kind::Command), (Format::Msgpack, Kind::Command)];
    let mixed = vec![(Format::Json, Kind::Event), (Format::Json, Kind::Command)];
    type Row = (&'static str, Vec<(Format, Kind)>, Vec<u8>, Vec<KafkaHeader>);
    let rows: Vec<Row> = vec![
        (
            // Anything written before this kernel, or by a client that drops headers.
            "no_content_type_falls_back_to_the_default",
            events.clone(),
            json_event.value.clone(),
            without_content_type(&json_event),
        ),
        (
            "content_type_overrides_the_default",
            events.clone(),
            msgpack_event.value.clone(),
            msgpack_event.headers.clone(),
        ),
        (
            "an_unknown_content_type_is_poison",
            events.clone(),
            json_event.value.clone(),
            [
                vec![header("application/x-protobuf")],
                without_content_type(&json_event),
            ]
            .concat(),
        ),
        (
            // Ambiguous by construction: both sides must pick the first, not the last.
            "the_first_content_type_wins",
            events.clone(),
            msgpack_event.value.clone(),
            [msgpack_event.headers.clone(), vec![header(event_json)]].concat(),
        ),
        (
            // The fallback is not a guess: bytes that are not the default format still fail.
            "header_less_bytes_in_another_format_do_not_decode",
            events.clone(),
            msgpack_event.value.clone(),
            Vec::new(),
        ),
        (
            // Present and readable, so it names a media type: the empty one, which nobody registers. A
            // null header value crosses as empty bytes, so this row is the null rule too.
            "an_empty_content_type_is_poison",
            events.clone(),
            json_event.value.clone(),
            vec![header("")],
        ),
        (
            // A client that upper-cased the name wrote a different header, so the default decodes.
            "an_upper_case_header_name_is_not_the_content_type",
            events.clone(),
            json_event.value.clone(),
            vec![KafkaHeader::new("Content-Type", event_msgpack.as_bytes())],
        ),
        (
            // Reads as absent, and does not reach past itself to the second header.
            "a_non_utf8_first_content_type_reads_as_absent",
            events.clone(),
            json_event.value.clone(),
            vec![KafkaHeader::new(CONTENT_TYPE, &[0xff, 0xfe]), header(event_msgpack)],
        ),
        (
            // Parameters are not stripped: the match is exact, so nobody registered this one.
            "a_parameterized_media_type_is_poison",
            events.clone(),
            json_event.value.clone(),
            vec![header(&format!("{event_json}; charset=utf-8"))],
        ),
        (
            // The 0.1 media type, minted before kinds: no kernel serializer claims it.
            "the_envelope_media_type_is_poison",
            events.clone(),
            json_event.value.clone(),
            vec![header("application/vnd.edax.envelope+json")],
        ),
        // A consumer of one kind registers that kind's media types only, so the other kind is poison.
        (
            "a_command_record_under_an_event_registry_is_poison",
            events.clone(),
            json_command.value.clone(),
            json_command.headers.clone(),
        ),
        (
            "a_json_command_decodes_by_its_content_type",
            commands.clone(),
            json_command.value.clone(),
            json_command.headers.clone(),
        ),
        (
            "a_msgpack_command_decodes_by_its_content_type",
            commands.clone(),
            msgpack_command.value.clone(),
            msgpack_command.headers.clone(),
        ),
        // No default for a command: it always carries the header, so a record without one is not a
        // command record.
        (
            "a_header_less_record_under_a_command_default_is_poison",
            commands.clone(),
            json_command.value.clone(),
            Vec::new(),
        ),
        (
            "an_event_record_under_a_command_registry_is_poison",
            commands,
            json_event.value.clone(),
            json_event.headers.clone(),
        ),
        // A registry may hold both kinds: the media type names the kind, and the bytes decode as it.
        (
            "a_mixed_registry_decodes_an_event",
            mixed.clone(),
            json_event.value.clone(),
            json_event.headers.clone(),
        ),
        (
            "a_mixed_registry_decodes_a_command",
            mixed.clone(),
            json_command.value.clone(),
            json_command.headers.clone(),
        ),
        (
            // The header picks the serializer, and the serializer reads the bytes: event bytes under a
            // command media type fail to decode as a command.
            "event_bytes_under_a_command_media_type_do_not_decode",
            mixed,
            json_event.value.clone(),
            vec![header(command_json)],
        ),
    ];
    rows.into_iter()
        .map(|(name, registry, value, headers)| decode_row(name, &registry, &value, &headers))
        .collect()
}

/// One fetched record routed against `registry` and decoded by the serializer the route picks, as a
/// row. The default is the registry's first serializer, and its kind decides a header-less record.
fn decode_row(name: &str, registry: &[(Format, Kind)], value: &[u8], headers: &[KafkaHeader]) -> Value {
    let media_types: Vec<&str> = registry.iter().map(|(format, kind)| format.media_type(*kind)).collect();
    check_media_types(&media_types).unwrap();
    let (default_format, default_kind) = registry[0];
    let decoded = |(format, kind): (Format, Kind)| {
        let message = match kind {
            Kind::Event => format
                .decode::<EventMessage>(value)
                .map(|msg| serde_json::to_value(msg).unwrap()),
            Kind::Command => format
                .decode::<CommandMessage>(value)
                .map(|msg| serde_json::to_value(msg).unwrap()),
        };
        match message {
            Ok(message) => json!({ "kind": kind.as_str(), "message": message }),
            Err(error) => json!({ "error": error.to_string() }),
        }
    };
    let want = match route(headers, &media_types, default_kind) {
        Route::Default => decoded((default_format, Kind::Event)),
        Route::Registered(index) => decoded(registry[index]),
        Route::Headerless => json!({ "route": "headerless" }),
        Route::Unknown(media_type) => json!({ "route": "unknown", "media_type": media_type }),
    };
    let registry: Vec<Value> = registry
        .iter()
        .map(|(format, kind)| json!({ "format": format.as_str(), "kind": kind.as_str(), "media_type": format.media_type(*kind) }))
        .collect();
    json!({
        "name": name,
        "registry": registry,
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
        (
            "no_headers_under_a_command_default_is_headerless",
            Vec::new(),
            vec![command_json],
        ),
        (
            "a_foreign_header_only_routes_to_the_default",
            vec![header("traceparent", b"00-0af7651916cd43dd-01")],
            vec![json],
        ),
        (
            "a_registered_media_type_names_its_index",
            vec![header(CONTENT_TYPE, msgpack.as_bytes())],
            vec![json, msgpack],
        ),
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
        (
            "a_host_media_type_is_as_good_as_a_kernel_one",
            vec![header(CONTENT_TYPE, b"application/x-test-id")],
            vec![json, "application/x-test-id"],
        ),
        (
            "an_unregistered_media_type_is_unknown",
            vec![header(CONTENT_TYPE, b"application/x-protobuf")],
            vec![json, msgpack],
        ),
        (
            "nothing_registered_makes_every_media_type_unknown",
            vec![header(CONTENT_TYPE, json.as_bytes())],
            Vec::new(),
        ),
        // A null value crosses as empty bytes, so this row is the null rule too: poison, not default.
        (
            "an_empty_value_is_unknown_not_the_default",
            vec![header(CONTENT_TYPE, b"")],
            vec![json, msgpack],
        ),
        (
            "the_name_is_matched_case_sensitively",
            vec![header("Content-Type", msgpack.as_bytes())],
            vec![json, msgpack],
        ),
        (
            "the_first_of_two_wins",
            vec![
                header(CONTENT_TYPE, msgpack.as_bytes()),
                header(CONTENT_TYPE, json.as_bytes()),
            ],
            vec![json, msgpack],
        ),
        (
            "a_non_utf8_first_value_reads_as_absent",
            vec![
                header(CONTENT_TYPE, &[0xff, 0xfe]),
                header(CONTENT_TYPE, json.as_bytes()),
            ],
            vec![json, msgpack],
        ),
        (
            "parameters_are_not_stripped",
            vec![header(CONTENT_TYPE, format!("{json}; charset=utf-8").as_bytes())],
            vec![json, msgpack],
        ),
        (
            "a_media_type_registered_twice_is_refused",
            Vec::new(),
            vec![json, "application/x-test-id", json],
        ),
        (
            "a_host_media_type_registered_twice_is_refused",
            Vec::new(),
            vec!["application/x-test-id", "application/x-test-id"],
        ),
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
                "default_kind": default.as_str(),
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
    let keyless = KafkaRecord {
        key: Vec::new(),
        value: Vec::new(),
        headers: Vec::new(),
    };
    let once = dead_letter_record(
        &fetched,
        "orders",
        3,
        42,
        Some(ErrorCode::Codec),
        "unknown format: application/x-protobuf",
    );
    type Case<'a> = (&'a str, KafkaRecord, &'a str, i32, i64, Option<ErrorCode>, &'a str);
    let cases: Vec<Case> = vec![
        (
            "keeps_key_value_and_headers_verbatim",
            fetched.clone(),
            "orders",
            3,
            42,
            Some(ErrorCode::Codec),
            "bad payload",
        ),
        // A keyless tombstone from a foreign producer: both cross as empty bytes, and stay empty.
        (
            "a_keyless_tombstone_stays_empty",
            keyless,
            "orders",
            0,
            0,
            Some(ErrorCode::Codec),
            "no message",
        ),
        (
            "non_ascii_topic_and_error",
            fetched.clone(),
            "commandes-é",
            0,
            7,
            Some(ErrorCode::Rejected),
            "montant négatif",
        ),
        // A host's own error carries no code: the header names it unclassified, in every SDK alike.
        (
            "an_unclassified_failure",
            fetched.clone(),
            "orders",
            1,
            9,
            None,
            "ValueError: the PDF renderer crashed",
        ),
        (
            "extreme_coordinates",
            fetched,
            "t",
            i32::MAX,
            i64::MAX,
            Some(ErrorCode::Store),
            "",
        ),
        // Republished and dead-lettered again: two sets of headers, and the reader takes the last.
        (
            "dead_lettered_twice_reads_the_last_source",
            once,
            "orders-retry",
            0,
            5,
            Some(ErrorCode::Store),
            "still failing",
        ),
    ];
    let mut rows: Vec<Value> = cases
        .into_iter()
        .map(|(name, record, topic, partition, offset, code, message)| {
            let dead = dead_letter_record(&record, topic, partition, offset, code, message);
            let source = dead_letter_source(&dead.headers).unwrap();
            assert_eq!((source.topic.as_str(), source.partition, source.offset), (topic, partition, offset), "{name}");
            // The stub a dead-letter topic that refused `dead` gets: the same source, the value gone.
            let stub = dead_letter_stub(&dead);
            let stub_source = dead_letter_source(&stub.headers).unwrap();
            assert_eq!(stub_source, DeadLetterSource { dropped: Some(dead.value.len() as u64), ..source.clone() }, "{name}");
            let source_json = |source: &DeadLetterSource| {
                json!({ "topic": source.topic, "partition": source.partition, "offset": source.offset, "error": source.error, "dropped": source.dropped })
            };
            json!({
                "name": name,
                "record": { "key_hex": hex(&record.key), "value_hex": hex(&record.value), "headers": headers_json(&record.headers) },
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "code": code.map(ErrorCode::as_str),
                "message": message,
                "want": {
                    "key_hex": hex(&dead.key),
                    "value_hex": hex(&dead.value),
                    "headers": headers_json(&dead.headers),
                    "source": source_json(&source),
                    "stub": {
                        "key_hex": hex(&stub.key),
                        "value_hex": hex(&stub.value),
                        "headers": headers_json(&stub.headers),
                        "source": source_json(&stub_source),
                    },
                },
            })
        })
        .collect();
    // The reader refuses to guess: a missing or unreadable coordinate is no source at all.
    let refusals: Vec<(&str, Vec<KafkaHeader>)> = vec![
        (
            "no_dead_letter_headers",
            vec![KafkaHeader::new(CONTENT_TYPE, b"application/json")],
        ),
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
            // Optional, but never guessed either: an unreadable count is no source.
            "a_dropped_count_that_is_not_a_number",
            vec![
                KafkaHeader::new("edax-dlq-topic", b"t"),
                KafkaHeader::new("edax-dlq-partition", b"0"),
                KafkaHeader::new("edax-dlq-offset", b"0"),
                KafkaHeader::new("edax-dlq-error", b"x"),
                KafkaHeader::new("edax-dlq-dropped", b"many"),
            ],
        ),
        (
            // A count that is there but not text is unreadable, not absent.
            "a_dropped_count_that_is_not_utf8",
            vec![
                KafkaHeader::new("edax-dlq-topic", b"t"),
                KafkaHeader::new("edax-dlq-partition", b"0"),
                KafkaHeader::new("edax-dlq-offset", b"0"),
                KafkaHeader::new("edax-dlq-error", b"x"),
                KafkaHeader::new("edax-dlq-dropped", &[0xff]),
            ],
        ),
        (
            // The last count decides: an earlier readable one does not stand in for it.
            "a_last_dropped_count_that_is_not_utf8",
            vec![
                KafkaHeader::new("edax-dlq-topic", b"t"),
                KafkaHeader::new("edax-dlq-partition", b"0"),
                KafkaHeader::new("edax-dlq-offset", b"0"),
                KafkaHeader::new("edax-dlq-error", b"x"),
                KafkaHeader::new("edax-dlq-dropped", b"337"),
                KafkaHeader::new("edax-dlq-dropped", &[0xff]),
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
    Revoke(&'static str, i32),
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
            Step::IsLive(i) => {
                json!({ "op": "is_live", "token": tokens[*i], "want": { "live": tracker.is_live(tokens[*i]) } })
            }
            Step::IsLiveToken(token) => {
                json!({ "op": "is_live", "token": token, "want": { "live": tracker.is_live(*token) } })
            }
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
            Step::Revoke(topic, partition) => {
                let due = tracker
                    .revoke(topic, *partition)
                    .map(|d| json!({ "topic": d.topic, "partition": d.partition, "offset": d.offset }));
                json!({ "op": "revoke", "topic": topic, "partition": partition, "want": { "due": due } })
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
            vec![
                Book("t", 0, 0),
                Book("t", 0, 1),
                Book("t", 0, 2),
                Nack(2),
                Book("t", 0, 2),
                Nack(3),
                Stats,
            ],
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
            // A revoke hands back the partition's last due watermark and drops the partition in one
            // step: its tokens retire, its acks settle nothing, and a second revoke has nothing left.
            // A partition whose watermark is already committed, or never moved, has nothing due, and
            // is dropped all the same.
            "a_revoke_hands_back_the_last_watermark_and_drops_the_partition",
            vec![
                Book("t", 0, 0),
                Book("t", 0, 1),
                Book("t", 0, 2),
                Book("t", 1, 4),
                Book("t", 2, 0),
                Ack(0),
                Ack(1),
                Ack(4),
                Committed("t", 2, 1),
                Revoke("t", 0),
                IsLive(2),
                Ack(2),
                Revoke("t", 0),
                Revoke("t", 1),
                Revoke("t", 2),
                Revoke("u", 0),
                Due,
                Stats,
            ],
        ),
        (
            "a_token_no_booking_issued_settles_nothing",
            vec![
                Book("t", 0, 0),
                AckToken(999),
                NackToken(0),
                IsLiveToken(999),
                IsLiveToken(0),
                Stats,
            ],
        ),
        (
            // Nothing booked it, so nothing records it: a watermark for an unowned partition would
            // refuse a lower, correct commit if it were ever assigned. An unknown topic likewise.
            "committed_for_an_unbooked_partition_is_a_no_op",
            vec![
                Book("t", 0, 0),
                Committed("t", 9, 100),
                Committed("u", 0, 7),
                Ack(0),
                Due,
                Committed("t", 0, 1),
                Stats,
            ],
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

/// What a shell checks when it is built, by `op`: the `durations`, each refused at zero and below with
/// the setting named and accepted from one millisecond (a row sets one duration, or all four); the
/// `consumer` configurations it refuses; the `dead_sink` a running consumer needs; and the producer
/// `acks` it warns about.
fn kafka_config_cases() -> Vec<Value> {
    let names = [
        "publish_timeout_ms",
        "commit_interval_ms",
        "commit_timeout_ms",
        "drain_timeout_ms",
    ];
    let mut rows: Vec<(String, [Option<i64>; 4])> = vec![
        ("nothing_set".to_owned(), [None; 4]),
        ("all_at_the_floor".to_owned(), [Some(1); 4]),
    ];
    for (index, name) in names.iter().enumerate() {
        for (label, ms) in [
            ("one_millisecond", 1),
            ("zero", 0),
            ("negative", -1),
            ("most_negative", i64::MIN),
        ] {
            let mut settings = [None; 4];
            settings[index] = Some(ms);
            rows.push((format!("{name}_{label}"), settings));
        }
    }
    let durations = rows.into_iter().map(|(name, settings)| {
        let [publish, interval, commit, drain] = settings;
        let error = refusal(&check_config(publish, interval, commit, drain));
        json!({
            "name": name,
            "op": "durations",
            "publish_timeout_ms": publish,
            "commit_interval_ms": interval,
            "commit_timeout_ms": commit,
            "drain_timeout_ms": drain,
            "error": error,
        })
    });
    // The consumer configurations a shell refuses, one row per branch.
    let consumers = [
        ("consumer_in_a_group_without_auto_commit", false, true),
        ("consumer_that_auto_commits", true, true),
        ("consumer_with_no_group", false, false),
        ("consumer_that_auto_commits_with_no_group", true, false),
    ]
    .into_iter()
    .map(|(name, auto_commit, has_group)| {
        let error = refusal(&check_consumer(auto_commit, has_group));
        json!({ "name": name, "op": "consumer", "auto_commit": auto_commit, "has_group": has_group, "error": error })
    });
    // A consumer rewinds on a nack, so it refuses to run with no dead-letter sink.
    let dead_sinks = [
        ("consumer_with_a_dead_letter_sink", true),
        ("consumer_with_no_dead_letter_sink", false),
    ]
    .into_iter()
    .map(|(name, has_dead_sink)| {
        let error = refusal(&check_dead_sink(has_dead_sink));
        json!({ "name": name, "op": "dead_sink", "has_dead_sink": has_dead_sink, "error": error })
    });
    // The producer `acks` a shell warns about: every value but all.
    let acks = [("acks_all", ACKS_ALL), ("acks_leader", 1), ("acks_none", 0)]
        .into_iter()
        .map(|(name, acks)| json!({ "name": name, "op": "acks", "acks": acks, "warning": acks_warning(acks) }));
    durations.chain(consumers).chain(dead_sinks).chain(acks).collect()
}

/// The verdict on a broker's protocol error code: one row per code the kernel classifies, and the
/// codes it leaves transient, a retriable one and an unknown one included.
fn kafka_error_cases() -> Vec<Value> {
    let codes: [(&str, i64); 21] = [
        ("message_too_large", 10),
        ("record_list_too_large", 18),
        ("invalid_record", 87),
        ("invalid_topic_exception", 17),
        ("invalid_required_acks", 21),
        ("topic_authorization_failed", 29),
        ("group_authorization_failed", 30),
        ("cluster_authorization_failed", 31),
        // No edax producer sets a timestamp, so only the client's skewed clock earns this, on every record.
        ("invalid_timestamp", 32),
        ("unsupported_sasl_mechanism", 33),
        ("illegal_sasl_state", 34),
        ("unsupported_version", 35),
        ("unsupported_for_message_format", 43),
        ("invalid_producer_epoch", 47),
        ("transactional_id_authorization_failed", 53),
        ("sasl_authentication_failed", 58),
        // Retriable in Kafka: a checksum failure in transit, and a partition move for a moment.
        ("corrupt_message", 2),
        ("unknown_topic_or_partition", 3),
        ("not_leader_or_follower", 6),
        // aiokafka reports a code it has no class for (87, 90) as -1, so this row is what Python sees for one.
        ("unknown_server_error", -1),
        ("an_unknown_code", 999),
    ];
    codes
        .into_iter()
        .map(|(name, code)| {
            let message = format!("{name} from the broker");
            json!({ "name": name, "error_code": code, "message": message, "error": broker_error(code, &message).to_string() })
        })
        .collect()
}

#[test]
fn golden() {
    write_or_check("kafka_config.json", kafka_config_cases());
    write_or_check("kafka_error.json", kafka_error_cases());
    write_or_check("kafka_record.json", kafka_record_cases());
    write_or_check("kafka_partition.json", kafka_partition_cases());
    write_or_check("kafka_decode.json", kafka_decode_cases());
    write_or_check("kafka_route.json", kafka_route_cases());
    write_or_check("kafka_dead_letter.json", kafka_dead_letter_cases());
    write_or_check("kafka_offsets.json", kafka_offsets_cases());
}

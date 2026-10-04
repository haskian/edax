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

use edax_core::{ErrorCode, Tenancy};
use edax_event::testkit::{base_command, base_event, map};
use edax_event::{CommandMessage, EventMessage, Extensions, Format, Kind, Message, Metadata};
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
    // the downgrade rule (a payload from a newer writer) can refuse it. Every row also pins the version a
    // codec built with `available` writes, `upcast_target`, so a third SDK derives the same per-name version.
    let cases: [(u32, u32, Option<&[u32]>); 19] = [
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
        (u32::MAX, u32::MAX, Some(&[])), // the u32 ceiling, no-op
        (u32::MAX - 1, u32::MAX, Some(&[u32::MAX - 1])), // the last step names v(MAX-1) -> v(MAX)
        (0, u32::MAX, Some(&[0])), // a span of 2^32 - 1 fails at its second step, without walking it
        (1, 1, None),               // no transforms, current: nothing to run
        (1, 3, None),               // no transforms, older payload: identity, nothing to run
        (5, 1, None),               // no transforms, newer writer: refused, never decoded silently
        (0, u32::MAX, None),        // identity over a 2^32 - 1 span is still an empty plan, not a walk
        (1, 2, Some(&[1, 3])),      // the plan needs only step 1, but the hole at 2 has no target
        (u32::MAX, u32::MAX, Some(&[u32::MAX])), // a step at the ceiling raises past every schema version
    ];
    cases
        .into_iter()
        .map(|(from, to, available)| {
            let result = edax_event::upcast_plan(from, to, available);
            let target = edax_event::upcast_target(available);
            json!({
                "from": from,
                "to": to,
                "available": available,
                "plan": result.as_ref().ok(),
                "error": result.as_ref().err().map(ToString::to_string),
                "target": target.as_ref().ok(),
                "target_error": target.as_ref().err().map(ToString::to_string),
            })
        })
        .collect()
}

fn causation_cases() -> Vec<Value> {
    let cases: [(&str, &str); 4] = [
        ("evt-2", "corr-1"),     // the child inherits the flow and points at its parent
        ("evt-root", "evt-root"), // a parent that roots its own flow
        ("cmd-1", ""),           // an empty correlation is a correlation: carried as it is
        ("", "corr-1"),          // an empty parent id is still the parent's id
    ];
    cases
        .into_iter()
        .map(|(parent_id, parent_correlation_id)| {
            json!({
                "parent_id": parent_id,
                "parent_correlation_id": parent_correlation_id,
                "expected": edax_event::propagate(parent_id, parent_correlation_id),
            })
        })
        .collect()
}

fn originate_cases() -> Vec<Value> {
    // One `fresh_id` throughout, so every case shows whether the kernel reached for it or not.
    let cases: [(Option<&str>, Option<&str>); 6] = [
        (None, None),                       // nothing given -> both from fresh_id
        (Some("corr-1"), None),             // causation follows the given correlation, not fresh_id
        (None, Some("evt-0")),              // a lone causation is not a chain -> fresh correlation
        (Some("corr-1"), Some("evt-0")),    // both given -> fresh_id unused
        (Some(""), None),                   // an empty correlation is given: fresh_id unused for it
        (None, Some("")),                   // an empty causation is kept, not defaulted
    ];
    cases
        .into_iter()
        .map(|(correlation_id, causation_id)| {
            json!({
                "correlation_id": correlation_id,
                "causation_id": causation_id,
                "fresh_id": "fresh-1",
                "expected": edax_event::originate(correlation_id, causation_id, "fresh-1"),
            })
        })
        .collect()
}

fn envelope_cases() -> Vec<Value> {
    let with = |edit: &dyn Fn(&mut EventMessage)| {
        let mut env = base_event();
        edit(&mut env);
        env
    };
    let events: Vec<(&str, EventMessage)> = vec![
        ("base", base_event()),
        (
            "unsequenced_empty_payload_empty_maps",
            EventMessage {
                id: "evt-2".into(),
                aggregate_id: "agg-2".into(),
                tenant: None,
                sequence: None,
                name: "OrderPlaced".into(),
                version: 2,
                data: Vec::new(),
                correlation_id: "evt-2".into(),
                causation_id: "evt-2".into(),
                occurred_at: None,
                recorded_at: 0,
                extensions: Extensions::new(),
                metadata: Metadata::new(),
            },
        ),
        ("occurred_before_recorded", with(&|e| e.occurred_at = Some(1_699_999_999_000_000_000))),
        ("two_keys_per_map", with(&|e| {
            e.extensions = map(&[("http.origin_ip", "203.0.113.45"), ("traceparent", "00-a-b-01")]);
            e.metadata = map(&[("actor", "sys"), ("trace", "z")]);
        })),
        // Non-ASCII text is written raw in both formats (serde_json does not \u-escape it), and the
        // 2^53+1 sequence is past every float-typed reader.
        ("non_ascii_and_a_sequence_past_2_53", with(&|e| {
            e.aggregate_id = "café-7".into();
            e.name = "Paiement effectué".into();
            e.data = "🦀".as_bytes().to_vec();
            e.sequence = Some(9_007_199_254_740_993);
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
            e.recorded_at = i64::MIN;
            e.occurred_at = Some(i64::MIN);
            e.version = 0;
            e.sequence = Some(u64::MAX);
        })),
        ("integer_ceilings", with(&|e| {
            e.recorded_at = i64::MAX;
            e.occurred_at = Some(i64::MAX);
            e.version = u32::MAX;
            e.sequence = Some(0);
        })),
        ("negative_time_before_the_epoch", with(&|e| e.recorded_at = -1_000_000_000)),
        // MessagePack picks the narrowest integer form; each pair straddles one width boundary.
        ("msgpack_fixint_to_uint8", with(&|e| { e.sequence = Some(127); e.recorded_at = 128; })),
        ("msgpack_uint8_to_uint16", with(&|e| { e.sequence = Some(255); e.recorded_at = 256; })),
        ("msgpack_uint16_to_uint32", with(&|e| { e.sequence = Some(65_535); e.recorded_at = 65_536; })),
        ("msgpack_uint32_to_uint64", with(&|e| { e.sequence = Some(4_294_967_295); e.recorded_at = 4_294_967_296; })),
        ("msgpack_negative_fixint_to_int8", with(&|e| { e.recorded_at = -32; e.occurred_at = Some(-33); })),
        // bin8 holds up to 255 bytes, bin16 from 256; fixstr up to 31 bytes, str8 from 32.
        ("msgpack_bin8_ceiling_and_fixstr_ceiling", with(&|e| {
            e.data = vec![0xab; 255];
            e.id = "e".repeat(31);
        })),
        ("msgpack_bin16_floor_and_str8_floor", with(&|e| {
            e.data = vec![0xab; 256];
            e.id = "e".repeat(32);
        })),
        // A tenant is a typed field, so it is on the wire as itself and not as a metadata key. The
        // separator and the non-ASCII cases are what `edax_core::scoped` must keep injective.
        ("tenanted", with(&|e| e.tenant = Some("acme".into()))),
        ("tenant_holding_the_key_separator", with(&|e| e.tenant = Some("a:b".into()))),
        ("tenant_non_ascii_at_the_length_limit", with(&|e| e.tenant = Some("é".repeat(32)))),
    ];
    let with_command = |edit: &dyn Fn(&mut CommandMessage)| {
        let mut cmd = base_command();
        edit(&mut cmd);
        cmd
    };
    let commands: Vec<(&str, CommandMessage)> = vec![
        ("command_base", base_command()),
        ("command_tenanted", with_command(&|c| c.tenant = Some("acme".into()))),
        ("command_without_key_or_target", with_command(&|c| {
            c.idempotency_key = None;
            c.aggregate_id = None;
            c.metadata = Metadata::new();
        })),
        ("command_from_a_saga", edax_event::saga_command(
            "shipping",
            "order-7",
            4,
            1,
            None,
            edax_event::Event { name: "Fulfill".into(), version: 2, data: b"{}".to_vec() },
            &base_event().id,
            &base_event().correlation_id,
            1_700_000_000_000_000_001,
        )),
        ("command_non_ascii_and_extensions", with_command(&|c| {
            c.name = "Payer la facture".into();
            c.idempotency_key = Some("clé-1".into());
            c.extensions = map(&[("http.user_agent", "curl/8"), ("traceparent", "00-a-b-01")]);
        })),
    ];
    let events = events.iter().map(|(name, env)| envelope_row(name, env));
    let commands = commands.iter().map(|(name, cmd)| envelope_row(name, cmd));
    events.chain(commands).collect()
}

/// One message of either kind in both formats.
fn envelope_row<M: Message + serde::Serialize>(name: &str, msg: &M) -> Value {
    json!({
        "name": name,
        "kind": M::KIND.name(),
        "message": serde_json::to_value(msg).unwrap(),
        "json_hex": hex(&Format::Json.encode(msg).unwrap()),
        "msgpack_hex": hex(&Format::Msgpack.encode(msg).unwrap()),
    })
}

/// The ordering key of each kind of message: the target aggregate, else the message's own id.
fn ordering_key_cases() -> Vec<Value> {
    let cases: [(Option<&str>, &str); 4] = [
        (Some("order-7"), "cmd-1"), // a target orders the message
        (None, "cmd-1"),            // no target: the id spreads it
        (Some(""), "cmd-1"),        // an empty target is a target
        (Some("café"), "évt"),      // bytes, never normalized
    ];
    cases
        .into_iter()
        .map(|(aggregate_id, id)| {
            json!({ "aggregate_id": aggregate_id, "id": id, "expected": edax_event::ordering_key(aggregate_id, id) })
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
        .map(|(idempotency_key, id)| {
            json!({ "idempotency_key": idempotency_key, "id": id, "expected": edax_event::received_key(idempotency_key, id) })
        })
        .collect()
}

/// The extension key rule, one row per branch of `check_extensions`.
fn extensions_cases() -> Vec<Value> {
    let long_ok = "a".repeat(edax_event::EXTENSION_KEY_MAX);
    let too_long = "a".repeat(edax_event::EXTENSION_KEY_MAX + 1);
    let keys: Vec<&str> = vec![
        "traceparent",
        "http.origin_ip",
        "x-amzn-trace-id",
        "0day",
        &long_ok,
        "",
        "Traceparent",
        ".hidden",
        "has space",
        "ünïcode",
        &too_long,
    ];
    keys.into_iter()
        .map(|key| {
            let extensions = Extensions::from([(key.to_owned(), "v".to_owned())]);
            let result = edax_event::check_extensions(&extensions);
            json!({ "key": key, "valid": result.is_ok(), "error_code": result.err().map(|e| e.code.as_str()) })
        })
        .collect()
}

/// A wire value a decode case is built from. Both formats serialize it the same way, so one case
/// says the same thing in JSON and MessagePack; a map is a list of pairs so a case can repeat a key.
enum W {
    Str(&'static str),
    /// A string built at run time, for a case whose length is the point.
    Text(String),
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
            W::Text(v) => s.serialize_str(v),
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

/// The base event as wire fields, in the order a case edits them. It names no tenant, the single-tenant
/// default.
fn wire_fields() -> Vec<(&'static str, W)> {
    vec![
        ("id", W::Str("evt-1")),
        ("aggregate_id", W::Str("agg-1")),
        ("sequence", W::Int(7)),
        ("name", W::Str("InvoiceIssued")),
        ("version", W::Int(1)),
        ("data", W::Bytes(&[0xde, 0xad, 0xbe, 0xef])),
        ("correlation_id", W::Str("corr-1")),
        ("causation_id", W::Str("cmd-1")),
        ("occurred_at", W::Null),
        ("recorded_at", W::Int(1_700_000_000_000_000_000)),
        ("extensions", W::Map(vec![("traceparent", W::Str("00-a-b-01"))])),
        ("metadata", W::Map(vec![("trace", W::Str("abc-123"))])),
    ]
}

/// The base command as wire fields: every optional field set, so a case that drops one is a change.
fn command_wire_fields() -> Vec<(&'static str, W)> {
    vec![
        ("id", W::Str("cmd-1")),
        ("idempotency_key", W::Str("req-1")),
        ("aggregate_id", W::Str("agg-1")),
        ("tenant", W::Str("acme")),
        ("name", W::Str("IssueInvoice")),
        ("version", W::Int(1)),
        ("data", W::Bytes(br#"{"total":5000}"#)),
        ("correlation_id", W::Str("corr-1")),
        ("causation_id", W::Str("corr-1")),
        ("issued_at", W::Int(1_700_000_000_000_000_001)),
        ("extensions", W::Map(vec![("traceparent", W::Str("00-a-b-01"))])),
        ("metadata", W::Map(vec![("trace", W::Str("abc-123"))])),
    ]
}

fn without(field: &str) -> Vec<(&'static str, W)> {
    wire_fields().into_iter().filter(|(k, _)| *k != field).collect()
}

fn replacing(field: &str, value: W) -> Vec<(&'static str, W)> {
    let mut value = Some(value);
    wire_fields().into_iter().map(|(k, v)| if k == field { (k, value.take().unwrap()) } else { (k, v) }).collect()
}

/// The base event with one field the base leaves out.
fn with(field: &'static str, value: W) -> Vec<(&'static str, W)> {
    wire_fields().into_iter().chain([(field, value)]).collect()
}

fn command_without(field: &str) -> Vec<(&'static str, W)> {
    command_wire_fields().into_iter().filter(|(k, _)| *k != field).collect()
}

fn command_replacing(field: &str, value: W) -> Vec<(&'static str, W)> {
    let mut value = Some(value);
    command_wire_fields().into_iter().map(|(k, v)| if k == field { (k, value.take().unwrap()) } else { (k, v) }).collect()
}

/// `bytes` decoded as a message of `kind` in `format`: the message, or the error's code. The code is the
/// contract; the message is the parser's own text, which a second SDK's parser words differently.
fn decoded_as(kind: Kind, format: Format, bytes: &[u8]) -> Value {
    let message = match kind {
        Kind::Event => format.decode::<EventMessage>(bytes).map(|msg| serde_json::to_value(&msg).unwrap()),
        Kind::Command => format.decode::<CommandMessage>(bytes).map(|msg| serde_json::to_value(&msg).unwrap()),
    };
    match message {
        Ok(message) => json!({ "message": message }),
        Err(error) => json!({ "error_code": error.code.as_str() }),
    }
}

/// The evolution rule: what a reader of each format and kind accepts, ignores and refuses. Each case is
/// encoded in both formats from one wire value, so the two formats are held to the same rule.
fn envelope_decode_cases() -> Vec<Value> {
    use Kind::{Command, Event};
    let positional = || W::Arr(wire_fields().into_iter().map(|(_, v)| v).collect());
    // The tenant rule counts bytes: 32 two-byte characters are at the limit, 33 are past it.
    let tenant = |text: &str, times: usize| W::Text(text.repeat(times));
    let cases: Vec<(&str, Kind, W, &[u8])> = vec![
        ("the_base_message", Event, W::Map(wire_fields()), b""),
        // A newer writer's field: ignored, so adding a field is forward compatible.
        ("an_unknown_field_is_ignored", Event, W::Map(wire_fields().into_iter().chain([("priority", W::Str("high"))]).collect()), b""),
        ("a_missing_sequence_reads_as_none", Event, W::Map(without("sequence")), b""),
        ("a_null_sequence_reads_as_none", Event, W::Map(replacing("sequence", W::Null)), b""),
        ("a_missing_occurred_at_reads_as_none", Event, W::Map(without("occurred_at")), b""),
        ("a_set_occurred_at_reads_back", Event, W::Map(replacing("occurred_at", W::Int(1_600_000_000_000_000_000))), b""),
        ("a_missing_required_field_is_refused", Event, W::Map(without("recorded_at")), b""),
        ("a_missing_extensions_map_is_refused", Event, W::Map(without("extensions")), b""),
        ("a_negative_version_is_refused", Event, W::Map(replacing("version", W::Int(-1))), b""),
        ("a_bad_extension_key_is_refused", Event, W::Map(replacing("extensions", W::Map(vec![("Traceparent", W::Str("x"))]))), b""),
        // The wire is a trust boundary: a tenant another service minted is held to the key rule
        // before any key is composed from it.
        ("a_tenant_reads_back", Event, W::Map(with("tenant", W::Str("acme"))), b""),
        ("a_null_tenant_reads_as_none", Event, W::Map(with("tenant", W::Null)), b""),
        ("an_empty_tenant_is_refused", Event, W::Map(with("tenant", W::Str(""))), b""),
        ("a_tenant_of_64_bytes_reads_back", Event, W::Map(with("tenant", tenant("é", 32))), b""),
        ("an_over_long_tenant_is_refused", Event, W::Map(with("tenant", tenant("a", 65))), b""),
        ("a_tenant_of_33_characters_and_66_bytes_is_refused", Event, W::Map(with("tenant", tenant("é", 33))), b""),
        ("a_repeated_field_is_refused", Event, W::Map(wire_fields().into_iter().chain([("id", W::Str("evt-2"))]).collect()), b""),
        ("a_repeated_metadata_key_keeps_the_last", Event, W::Map(replacing(
            "metadata",
            W::Map(vec![("trace", W::Str("first")), ("trace", W::Str("last"))]),
        )), b""),
        // The positional form, of the base length and with one element more, is refused either way.
        ("a_positional_message_is_refused", Event, positional(), b""),
        ("a_positional_message_with_an_extra_element_is_refused", Event, W::Arr(
            wire_fields().into_iter().map(|(_, v)| v).chain([W::Str("extra")]).collect(),
        ), b""),
        ("trailing_bytes_are_refused", Event, W::Map(wire_fields()), b"\x00"),
        ("not_a_map_at_all", Event, W::Str("evt-1"), b""),
        // A command's optional fields read as `None` when missing, so an older or a single-tenant
        // producer's command still decodes; its required fields and its tenant are held as an event's.
        ("the_base_command", Command, W::Map(command_wire_fields()), b""),
        ("a_command_missing_its_tenant_reads_as_none", Command, W::Map(command_without("tenant")), b""),
        ("a_command_missing_its_idempotency_key_reads_as_none", Command, W::Map(command_without("idempotency_key")), b""),
        ("a_command_missing_its_aggregate_id_reads_as_none", Command, W::Map(command_without("aggregate_id")), b""),
        ("a_command_with_an_empty_tenant_is_refused", Command, W::Map(command_replacing("tenant", W::Str(""))), b""),
        ("a_command_with_an_over_long_tenant_is_refused", Command, W::Map(command_replacing("tenant", tenant("a", 65))), b""),
        ("a_command_missing_issued_at_is_refused", Command, W::Map(command_without("issued_at")), b""),
    ];
    cases
        .into_iter()
        .map(|(name, kind, wire, trailing)| {
            let encode = |format: Format| -> Vec<u8> {
                let mut bytes = match format {
                    Format::Json => serde_json::to_vec(&wire).unwrap(),
                    _ => rmp_serde::to_vec_named(&wire).unwrap(),
                };
                bytes.extend_from_slice(trailing);
                bytes
            };
            let json = decoded_as(kind, Format::Json, &encode(Format::Json));
            let msgpack = decoded_as(kind, Format::Msgpack, &encode(Format::Msgpack));
            assert_eq!(json, msgpack, "{name}: the two formats must follow one rule");
            json!({
                "name": name,
                "kind": kind.name(),
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

/// The event guard's input rule: the message id under the guard's scope is the key, composed with the
/// tenant under a pool, and the raw `data` bytes are hashed. The message's other fields take no part,
/// so two deliveries of one event differing only in metadata, extensions or time claim the same key
/// with the same hash. Two scopes are two keys for one event: that is what lets two consumers share a
/// ledger. A mode that wants a tenant and an event that names none is the `codec` error.
fn event_idempotency_cases() -> Vec<Value> {
    let base = EventMessage {
        id: "acct-1-3".to_owned(),
        aggregate_id: "acct-1".to_owned(),
        tenant: None,
        sequence: Some(3),
        name: "Deposited".to_owned(),
        version: 1,
        data: br#"{"account_id":"acct-1","amount_cents":4200,"currency":"USD"}"#.to_vec(),
        correlation_id: "corr-1".to_owned(),
        causation_id: "cmd-1".to_owned(),
        occurred_at: None,
        recorded_at: 1_714_564_800_000_000_000,
        extensions: Extensions::new(),
        metadata: [("trace".to_owned(), "abc".to_owned())].into_iter().collect(),
    };
    let with = |edit: fn(&mut EventMessage)| {
        let mut env = base.clone();
        edit(&mut env);
        env
    };
    let tenant = |tenant: &str| {
        let mut env = base.clone();
        env.tenant = Some(tenant.to_owned());
        env
    };
    let (silo, pool, bridge) = (Tenancy::Silo, Tenancy::Pool, Tenancy::Bridge);
    let cases = [
        ("deposit", silo, "balances", base.clone()),
        // The same event under another consumer's scope: its own key, so neither skips the other.
        ("another_scope_is_another_key", silo, "receipts", base.clone()),
        // A separator in the scope spells no other scope's key.
        ("separator_in_the_scope", silo, "a:b", base.clone()),
        // Same bytes reordered: raw, not canonical, so a different hash under the same key.
        ("raw_bytes_not_canonical", silo, "balances", with(|e| e.data = br#"{"currency":"USD","account_id":"acct-1","amount_cents":4200}"#.to_vec())),
        ("metadata_and_time_take_no_part", silo, "balances", with(|e| {
            e.metadata.insert("retry".to_owned(), "2".to_owned());
            e.extensions.insert("traceparent".to_owned(), "00-a-b-01".to_owned());
            e.recorded_at += 1;
        })),
        ("empty_body", silo, "balances", with(|e| e.data = Vec::new())),
        ("non_utf8_body", silo, "balances", with(|e| e.data = vec![0xde, 0xad, 0xbe, 0xef])),
        ("a_silo_never_keys_by_the_tenant", silo, "balances", tenant("acme")),
        ("a_pool_composes_the_tenant_outermost", pool, "balances", tenant("acme")),
        ("separator_in_the_tenant", pool, "balances", tenant("a:b")),
        ("a_pool_refuses_an_event_without_a_tenant", pool, "balances", base.clone()),
        ("a_bridge_keeps_the_key_bare", bridge, "balances", tenant("acme")),
        ("a_bridge_refuses_an_event_without_a_tenant", bridge, "balances", base.clone()),
    ];
    cases
        .into_iter()
        .map(|(name, mode, scope, env)| {
            let rule = edax_event::event_idempotency(&env, mode, scope);
            json!({
                "name": name,
                "tenancy": mode.name(),
                "scope": scope,
                "id": env.id,
                "tenant": env.tenant,
                "data_hex": hex(&env.data),
                "metadata": env.metadata,
                "extensions": env.extensions,
                "recorded_at": env.recorded_at,
                "key": rule.as_ref().ok().map(|rule| &rule.key),
                "payload_hash": rule.as_ref().ok().map(|rule| rule.payload_hash),
                "error": rule.as_ref().err().map(ToString::to_string),
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

/// The saga step's write for every loaded state and fold, whether a step folds an event, the outbox
/// command id and the command message itself.
fn saga_cases() -> Vec<Value> {
    use edax_event::Loaded;
    let loaded_json = |loaded: Loaded| match loaded {
        Loaded::Absent => json!({ "state": "absent" }),
        Loaded::Active { version } => json!({ "state": "active", "version": version }),
    };
    let write_json = |write: Result<edax_event::SagaWrite, edax_core::Error>| match write {
        Ok(edax_event::SagaWrite::Nothing) => json!({ "write": "nothing" }),
        Ok(edax_event::SagaWrite::Save { expected, version }) => json!({ "write": "save", "expected": expected, "version": version }),
        Ok(edax_event::SagaWrite::Complete { expected, version }) => {
            json!({ "write": "complete", "expected": expected, "version": version })
        }
        Err(e) => json!({ "error": e.to_string() }),
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
    let unsequenced = r#"{"order-7":{"unsequenced":"evt-u"}}"#;
    // One aggregate's sequenced and unsequenced events folded, then replayed after a crash: both repeat.
    let mixed = r#"{"order-7":{"sequence":2,"id":"evt-2","unsequenced":"evt-v"}}"#;
    let admits: [(bool, &str, &str, Option<u64>, &str); 16] = [
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
        (false, mixed, "order-7", Some(2), "evt-2"),
        (false, mixed, "order-7", None, "evt-v"),
        (false, one, "order-7", Some(1), "evt-x"),
        (false, unsequenced, "order-7", Some(3), "evt-3"),
        (false, r#"{"café":{"sequence":18446744073709551615,"id":"évt"}}"#, "café", Some(u64::MAX), "évt"),
        (false, "[1]", "order-7", Some(1), "evt-1"),
    ];
    let admit_json = |completed: bool, folded: &str, env: &EventMessage| match edax_event::saga_admit(completed, folded, env) {
        Ok(edax_event::SagaAdmit::Completed) => json!({ "completed": true }),
        Ok(edax_event::SagaAdmit::Repeat) => json!({ "repeat": true }),
        Ok(edax_event::SagaAdmit::BelowMark) => json!({ "below_mark": true }),
        Ok(edax_event::SagaAdmit::Fold { folded }) => json!({ "folded": folded }),
        Err(e) => json!({ "error": e.to_string() }),
    };
    for (completed, folded, aggregate_id, sequence, id) in admits {
        let env = EventMessage { id: id.to_owned(), aggregate_id: aggregate_id.to_owned(), sequence, ..base_event() };
        let want = admit_json(completed, folded, &env);
        cases.push(json!({ "admit": { "completed": completed, "folded": folded, "aggregate_id": aggregate_id, "sequence": sequence, "id": id }, "want": want }));
    }
    // Two of the instance's commands die and the relay, its mark lost, replays the first: each failure
    // folds once. A failure kept as the last unsequenced id, before the failed set, is a repeat still.
    let first = r#"{"order-7":{"failed":["order-7:2:0:failed"]}}"#;
    let both = r#"{"order-7":{"failed":["order-7:2:0:failed","order-7:2:1:failed"]}}"#;
    let failures: [(&str, &str); 4] = [
        ("", "order-7:2:0:failed"),
        (first, "order-7:2:1:failed"),
        (both, "order-7:2:0:failed"),
        (r#"{"order-7":{"unsequenced":"order-7:2:0:failed"}}"#, "order-7:2:0:failed"),
    ];
    for (folded, id) in failures {
        let name = edax_event::SAGA_COMMAND_FAILED;
        let env = EventMessage { id: id.to_owned(), aggregate_id: "order-7".to_owned(), sequence: None, name: name.to_owned(), ..base_event() };
        let want = admit_json(false, folded, &env);
        cases.push(json!({ "admit": { "completed": false, "folded": folded, "aggregate_id": "order-7", "sequence": null, "id": id, "name": name }, "want": want }));
    }
    for (instance_id, version, index) in [("order-7", 1u64, 0u64), ("order-7", 2, 1), ("a:1", 2, 0), ("", 1, 0), ("café", u64::MAX, u64::MAX)] {
        cases.push(json!({
            "instance_id": instance_id, "version": version, "index": index,
            "command_id": edax_event::saga_command_id(instance_id, version, index),
        }));
    }
    // The command message a step's outbox holds: keyed and ordered by the instance, the folded event's
    // child in lineage.
    // A pooled saga: the instance id is the host's own composition, and the row inherits its tenant.
    let cause = EventMessage { tenant: Some("acme".into()), ..base_event() };
    let instance = edax_core::scoped("acme", "order-7");
    let payload = edax_event::Event { name: "Fulfill".into(), version: 2, data: br#"{"order":"order-7"}"#.to_vec() };
    let command = edax_event::saga_command("shipping", &instance, 4, 1, cause.tenant.as_deref(), payload.clone(), &cause.id, &cause.correlation_id, 1_700_000_000_000_000_001);
    cases.push(json!({
        "saga_command": {
            "saga": "shipping",
            "instance_id": instance, "version": 4, "index": 1, "issued_at": 1_700_000_000_000_000_001_i64,
            "command": serde_json::to_value(&payload).unwrap(),
            "cause": serde_json::to_value(&cause).unwrap(),
        },
        "want": serde_json::to_value(&command).unwrap(),
    }));
    // The event a dead saga command folds back into its instance, and a row that names no instance.
    let stray = edax_event::CommandMessage { aggregate_id: None, ..command.clone() };
    let failures = [
        (command.clone(), Some(edax_core::ErrorCode::Rejected), "out of stock"),
        (command, None, "warehouse API timed out"),
        (stray, Some(edax_core::ErrorCode::Rejected), "out of stock"),
    ];
    for (row, code, error) in failures {
        let want = match edax_event::saga_command_failed(&row, code, error, 1_700_000_000_000_000_002) {
            Ok(event) => serde_json::to_value(&event).unwrap(),
            Err(e) => json!({ "error": e.to_string() }),
        };
        cases.push(json!({
            "saga_command_failed": {
                "command": serde_json::to_value(&row).unwrap(),
                "code": code.map(edax_core::ErrorCode::as_str),
                "error": error,
                "recorded_at": 1_700_000_000_000_000_002_i64,
            },
            "want": want,
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
    write_or_check("ordering_key.json", ordering_key_cases());
    write_or_check("command_received_key.json", received_key_cases());
    write_or_check("extensions.json", extensions_cases());
}

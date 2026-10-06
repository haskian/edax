//! Golden vector for the event codecs: the bytes the Rust registry codec writes for one event of each
//! variant shape, in both formats, and the foreign bytes it refuses. Python's `json_codec` /
//! `msgpack_codec` must write the same name and the same bytes for the same event, read them back, and
//! refuse the same bytes, so one stream reads the same from either SDK.
//!
//! Each written row carries `fields`, the event as a host holds it, with a decimal or an instant wrapped
//! in its marker (`{"$decimal": "9.50"}`): each SDK builds the event from them with its own host types,
//! so the row pins that both write the kernel's canonical text. A refused row carries the foreign bytes
//! and the `error` code instead.
//!
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax --test golden`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "a golden generator runs only as a test, and its panic is the failure report"
)]

use edax::{Decimal, ErrorCode, EventCodec, Instant, JsonRegistryCodec, MsgpackRegistryCodec};
use edax_event::{Format, Payload};
use edax_golden::{hex, host_text, refusal, write_or_check};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Deposited {
    account_id: String,
    amount_cents: i64,
    currency: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Withdrawn {
    account_id: String,
    amount_cents: i64,
    memo: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Tagged {
    tags: Vec<String>,
    flags: Vec<bool>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Address {
    city: String,
    zip: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Relocated {
    account_id: String,
    address: Address,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Priced {
    account_id: String,
    amount: Decimal,
    at: Instant,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
enum AccountEvent {
    Deposited(Deposited),
    Withdrawn(Withdrawn),
    Tagged(Tagged),
    Relocated(Relocated),
    Priced(Priced),
    Renamed(String),
    Frozen,
}

/// (case, event name, the event's fields as a host holds them).
fn written() -> Vec<(&'static str, &'static str, Value)> {
    vec![
        (
            "deposit",
            "Deposited",
            json!({"account_id": "acct-1", "amount_cents": 4200, "currency": "EUR"}),
        ),
        (
            "negative_and_wide_amount",
            "Deposited",
            json!({"account_id": "acct-1", "amount_cents": -9_007_199_254_740_993_i64, "currency": "USD"}),
        ),
        (
            "optional_field_absent",
            "Withdrawn",
            json!({"account_id": "acct-1", "amount_cents": 500, "memo": null}),
        ),
        (
            "non_ascii_text",
            "Withdrawn",
            json!({"account_id": "acct-é", "amount_cents": 500, "memo": "café ☕ 🦀"}),
        ),
        ("lists", "Tagged", json!({"tags": ["vip", ""], "flags": [true, false]})),
        (
            "nested_struct",
            "Relocated",
            json!({"account_id": "acct-1", "address": {"city": "NYC", "zip": "10001"}}),
        ),
        // A decimal and an instant are written in the kernel's canonical text, whatever spelling the host
        // built them from: `9.50` is `"9.5"`, and `+01:00` is fixed-width UTC.
        (
            "host_values_are_written_canonical",
            "Priced",
            json!({"account_id": "acct-1", "amount": {"$decimal": "9.50"}, "at": {"$instant": "2024-01-01T01:30:00+01:00"}}),
        ),
        // A payload that is no struct reads as itself: only a struct is held to the map form.
        ("string_payload_for_a_string_type_is_read", "Renamed", json!("new-name")),
        ("unit_variant", "Frozen", json!({})),
    ]
}

/// (case, event name, the foreign payload): bytes a writer outside edax could produce, which every SDK
/// refuses: a struct reads from a map only, at every depth, and a host value only from its canonical text.
fn refused() -> Vec<(&'static str, &'static str, Value)> {
    vec![
        (
            "array_for_a_struct_is_refused",
            "Deposited",
            json!(["acct-1", 4200, "EUR"]),
        ),
        (
            "nested_array_for_a_struct_is_refused",
            "Relocated",
            json!({"account_id": "acct-1", "address": ["NYC", "10001"]}),
        ),
        // A host value is read as strictly as it is written: an instant with no offset, and a decimal that
        // is no number, have no canonical text.
        (
            "naive_instant_is_refused",
            "Priced",
            json!({"account_id": "acct-1", "amount": "9.5", "at": "2024-01-01T00:30:00"}),
        ),
        (
            "nan_decimal_is_refused",
            "Priced",
            json!({"account_id": "acct-1", "amount": "NaN", "at": "2024-01-01T00:30:00.000000Z"}),
        ),
        (
            "infinite_decimal_is_refused",
            "Priced",
            json!({"account_id": "acct-1", "amount": "-Infinity", "at": "2024-01-01T00:30:00.000000Z"}),
        ),
    ]
}

fn event_codec_cases() -> Vec<Value> {
    let json_codec = JsonRegistryCodec::<AccountEvent>::new().unwrap();
    let msgpack_codec = MsgpackRegistryCodec::<AccountEvent>::new().unwrap();
    let written_rows = written().into_iter().map(|(case, name, fields)| {
        // The host's fields, read as the Rust host types: `Decimal` and `Instant` canonicalize here.
        let held = Payload {
            name: name.into(),
            version: 1,
            data: serde_json::to_vec(&host_text(&fields)).unwrap(),
        };
        let event = json_codec
            .unmarshal(&held)
            .unwrap_or_else(|e| panic!("{case}: the fields build the event: {e}"));
        let as_json = json_codec.marshal(&event).unwrap();
        let as_msgpack = msgpack_codec.marshal(&event).unwrap();
        assert_eq!(
            json_codec.unmarshal(&as_json).unwrap(),
            event,
            "{case}: json round trip"
        );
        assert_eq!(
            msgpack_codec.unmarshal(&as_msgpack).unwrap(),
            event,
            "{case}: msgpack round trip"
        );
        assert_eq!(
            (as_json.name.as_str(), as_msgpack.name.as_str()),
            (name, name),
            "{case}: one name in both formats"
        );
        json!({
            "case": case,
            "name": name,
            "version": as_json.version,
            "fields": fields,
            "json": String::from_utf8(as_json.data).unwrap(),
            "msgpack_hex": hex(&as_msgpack.data),
            "error": null,
        })
    });
    let refused_rows = refused().into_iter().map(|(case, name, payload)| {
        let as_json = Payload {
            name: name.into(),
            version: 1,
            data: Format::Json.encode_payload(&payload).unwrap(),
        };
        let as_msgpack = Payload {
            name: name.into(),
            version: 1,
            data: Format::Msgpack.encode_payload(&payload).unwrap(),
        };
        let (from_json, from_msgpack) = (json_codec.unmarshal(&as_json), msgpack_codec.unmarshal(&as_msgpack));
        let codes = (
            from_json.as_ref().err().map(|e| e.code),
            from_msgpack.as_ref().err().map(|e| e.code),
        );
        assert_eq!(
            codes,
            (Some(ErrorCode::Codec), Some(ErrorCode::Codec)),
            "{case}: refused in both formats"
        );
        json!({
            "case": case,
            "name": name,
            "version": 1,
            "fields": null,
            "json": String::from_utf8(as_json.data).unwrap(),
            "msgpack_hex": hex(&as_msgpack.data),
            // The decoder's own text: a second SDK's decoder words it differently, so only the code matches.
            "error": refusal(&from_json),
        })
    });
    written_rows.chain(refused_rows).collect()
}

#[test]
fn golden() {
    write_or_check("event_codec.json", event_codec_cases());
}

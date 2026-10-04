//! Golden vector for the event codecs: the bytes the Rust registry codec writes for one event of each
//! variant shape, in both formats. Python's `json_codec` / `msgpack_codec` must write the same name and
//! the same bytes for the same events, and read these back, so one stream reads the same from either SDK.
//!
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax --test golden`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "a golden generator runs only as a test, and its panic is the failure report"
)]

use edax::{EventCodec, JsonRegistryCodec, MsgpackRegistryCodec};
use edax_golden::{hex, write_or_check};
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
enum AccountEvent {
    Deposited(Deposited),
    Withdrawn(Withdrawn),
    Tagged(Tagged),
    Frozen,
}

fn events() -> Vec<(&'static str, AccountEvent)> {
    vec![
        (
            "deposit",
            AccountEvent::Deposited(Deposited { account_id: "acct-1".into(), amount_cents: 4200, currency: "EUR".into() }),
        ),
        (
            "negative_and_wide_amount",
            AccountEvent::Deposited(Deposited { account_id: "acct-1".into(), amount_cents: -9_007_199_254_740_993, currency: "USD".into() }),
        ),
        (
            "optional_field_absent",
            AccountEvent::Withdrawn(Withdrawn { account_id: "acct-1".into(), amount_cents: 500, memo: None }),
        ),
        (
            "non_ascii_text",
            AccountEvent::Withdrawn(Withdrawn { account_id: "acct-é".into(), amount_cents: 500, memo: Some("café ☕ 🦀".into()) }),
        ),
        ("lists", AccountEvent::Tagged(Tagged { tags: vec!["vip".into(), String::new()], flags: vec![true, false] })),
        ("unit_variant", AccountEvent::Frozen),
    ]
}

fn event_codec_cases() -> Vec<Value> {
    let json_codec = JsonRegistryCodec::<AccountEvent>::new().unwrap();
    let msgpack_codec = MsgpackRegistryCodec::<AccountEvent>::new().unwrap();
    events()
        .into_iter()
        .map(|(case, event)| {
            let as_json = json_codec.marshal(&event).unwrap();
            let as_msgpack = msgpack_codec.marshal(&event).unwrap();
            assert_eq!(json_codec.unmarshal(&as_json).unwrap(), event, "{case}: json round trip");
            assert_eq!(msgpack_codec.unmarshal(&as_msgpack).unwrap(), event, "{case}: msgpack round trip");
            assert_eq!(as_json.name, as_msgpack.name, "{case}: one name in both formats");
            json!({
                "case": case,
                "name": as_json.name,
                "version": as_json.version,
                "json": String::from_utf8(as_json.data).unwrap(),
                "msgpack_hex": hex(&as_msgpack.data),
            })
        })
        .collect()
}

#[test]
fn golden() {
    write_or_check("event_codec.json", event_codec_cases());
}

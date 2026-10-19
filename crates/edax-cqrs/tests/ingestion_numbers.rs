//! Raw JSON tokens must cross each SDK without a fixture parser rounding them first.
#![allow(clippy::unwrap_used, clippy::indexing_slicing, reason = "golden test assertions")]

use edax_core::Error;
use edax_cqrs::{ingest, projection};
use edax_golden::write_or_check;
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub fn replay(case: &Value) -> Result<Value, Error> {
    match case["op"].as_str().unwrap() {
        "plan" => {
            let inputs = ingest::inputs_json(case["inputs_json"].as_str().unwrap())?;
            ingest::plan(
                case["mode"].as_str().unwrap(),
                &inputs,
                &BTreeMap::new(),
                &BTreeMap::new(),
            )
            .map(|plan| json!(plan))
        }
        "projection_keys" => projection::request_json(case["request_json"].as_str().unwrap())
            .and_then(|request| projection::keys(&request))
            .map(|keys| json!(keys)),
        _ => panic!("unknown raw numeric golden operation"),
    }
}

fn inputs(mode: &str, field: &str, token: &str) -> String {
    let source = ingest::source_key("orders-v1", "source-v1", "orders", 0, i64::MAX).unwrap();
    let key = ingest::row_key("orders-v1", mode, Some("tenant-7"), None, "order-paid").unwrap();
    let content = if field == "content" {
        format!(r#"{{"total":{token}}}"#)
    } else {
        r#""OrderPaid""#.into()
    };
    let value = if field == "value" {
        format!(r#"{{"total":{token}}}"#)
    } else {
        r#"{"status":"paid"}"#.into()
    };
    format!(
        r#"[{{"source":{},"key":{},"stream":"order-42","event_id":"order-paid","sequence":9223372036854775807,"content":{content},"private":{value},"raw":"orders:0:9223372036854775807","raw_fingerprint":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","failure":null}}]"#,
        json!(source),
        json!(key)
    )
}

fn request(field: &str, token: &str) -> String {
    let payload = if field == "payload" { token } else { "null" };
    let metadata = if field == "metadata" { token } else { "null" };
    let reduced = match field {
        "state" => format!(r#", "reduced":[{{"private":null,"public":{token},"intents":[]}}]"#),
        "intent" => format!(
            r#", "reduced":[{{"private":null,"public":null,"intents":[{{"key":"receipt","destination":"billing","payload":{token}}}]}}]"#
        ),
        _ => String::new(),
    };
    let op = if reduced.is_empty() {
        "prepare_batch"
    } else {
        "apply_batch"
    };
    format!(
        r#"{{"contract":{{"generation":"orders-v1","source_identity":"source-v1","source_history":"history-v1","topic":"orders","reducer_id":"order-v1","envelope_id":"event-v1","keying_id":"key-v1","effect_namespace":"effects-v1","starts":{{"0":9223372036854775807}},"at_rest":"sealed"}},"operation":{{"op":"{op}","lease":{{"checkpoint_key":"checkpoint","partition":0,"owner":"worker","epoch":9223372036854775807,"next_offset":9223372036854775807,"expires_ms":9223372036854775807}},"inputs":[{{"offset":9223372036854775807,"event":{{"tenant":"tenant-7","aggregate":"order-42","event_id":"order-paid","sequence":9223372036854775807,"event_type":"OrderPaid","schema_version":"1","payload":{payload},"metadata":{metadata}}}}}],"proof":{{"identity":"source-v1","history":"history-v1","beginning":0,"end":9223372036854775807}}{reduced}}}}}"#
    )
}

#[test]
fn ingestion_numbers_golden() {
    let mut cases = Vec::new();
    for (name, token, accepted) in [
        ("safe_integer", "9007199254740991", true),
        ("unsafe_integer", "9007199254740992", false),
        ("beyond_u64", "18446744073709551617", false),
        ("negative_beyond_i64", "-9223372036854775809", false),
        ("wide_scientific_integer", "1000000000000000000001", false),
        ("negative_scientific_integer", "-1000000000000000000001", false),
        ("exponent_float", "1e+21", true),
    ] {
        for mode in ["append", "upsert"] {
            for field in ["content", "value"] {
                let mut case = json!({"name":format!("{mode}_{field}_{name}"),"op":"plan","mode":mode,"inputs_json":inputs(mode,field,token)});
                let result = replay(&case).unwrap();
                assert_eq!(
                    result["outcomes"],
                    json!([if accepted { "accepted" } else { "rejected" }]),
                    "{}",
                    case["name"]
                );
                assert_eq!(result["writes"].as_object().unwrap().len(), usize::from(accepted));
                assert_eq!(result["rejections"].as_object().unwrap().len(), usize::from(!accepted));
                case["want"] = result;
                case["error"] = Value::Null;
                cases.push(case);
            }
        }
        for field in ["payload", "metadata", "state", "intent"] {
            let mut case = json!({"name":format!("projection_{field}_{name}"),"op":"projection_keys","request_json":request(field,token)});
            let result = replay(&case);
            assert_eq!(result.is_ok(), accepted, "{}: {result:?}", case["name"]);
            match result {
                Ok(value) => {
                    case["want"] = value;
                    case["error"] = Value::Null;
                }
                Err(error) => {
                    case["want"] = Value::Null;
                    case["error"] = json!(error.to_string());
                }
            }
            cases.push(case);
        }
    }
    write_or_check("ingestion_numbers.json", cases);
}

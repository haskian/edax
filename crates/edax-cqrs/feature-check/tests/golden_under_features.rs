//! The canonical form (RFC 8785), the hashes and the cursors under `preserve_order`,
//! `arbitrary_precision` and `float_roundtrip`. Every row of every vector that covers them must still
//! match, refusals included: the 2^53 check reads the input text, so `arbitrary_precision` cannot
//! hide a wide literal from it.

use edax_cqrs::{canonical_json, command_hash, encode_cursor, payload_hash, Decimal, Instant, SortKey};
use edax_golden::{map_markers, read as golden, DECIMAL_MARKER};
use serde_json::Value;

/// `value` with each host-value marker built as the kernel's host type and written back as its text,
/// or the kernel's error in its wire form.
fn host_values(value: &Value) -> Result<Value, String> {
    map_markers(value, &|marker, text| {
        let canonical = if marker == DECIMAL_MARKER {
            text.parse::<Decimal>().map(|decimal| decimal.to_string())
        } else {
            text.parse::<Instant>().map(|instant| instant.to_string())
        };
        canonical.map(Value::String).map_err(|e| e.to_string())
    })
}

#[test]
fn the_features_are_on() {
    // Without them this crate proves nothing: insertion order survives and number text is kept.
    let value: Value = serde_json::from_str(r#"{"b":1,"a":1.10}"#).unwrap();
    assert_eq!(serde_json::to_string(&value).unwrap(), r#"{"b":1,"a":1.10}"#);
}

#[test]
fn canonical_json_vector_holds() {
    for case in golden("canonical_json.json") {
        let name = case["name"].as_str().unwrap();
        let input = case["input"].as_str().unwrap();
        match canonical_json(input.as_bytes()) {
            Ok(canonical) => {
                assert_eq!(
                    Some(std::str::from_utf8(&canonical).unwrap()),
                    case["canonical"].as_str(),
                    "{name}"
                );
                assert_eq!(Some(payload_hash(&canonical)), case["hash"].as_u64(), "{name}: hash");
            }
            Err(error) => assert_eq!(Some(error.to_string().as_str()), case["error"].as_str(), "{name}"),
        }
    }
}

#[test]
fn canonical_payload_hash_vector_holds() {
    for case in golden("canonical_payload_hash.json") {
        let input = case["input"].as_str().unwrap();
        match canonical_json(input.as_bytes()) {
            Ok(canonical) => assert_eq!(Some(payload_hash(&canonical)), case["expected"].as_u64(), "{input}"),
            Err(error) => assert_eq!(Some(error.to_string().as_str()), case["error"].as_str(), "{input}"),
        }
    }
}

#[test]
fn command_hash_vector_holds() {
    // The command is a `Value` here, which under `preserve_order` keeps the vector's key order and
    // under `arbitrary_precision` keeps the number text: exactly the two things the hash must ignore.
    // `arbitrary_precision` also keeps an integer past 2^53 as its text, so the refusal rows prove the
    // kernel reads the literal, not a `Value` that may already have rounded it.
    //
    // A host-value row's markers are built as `Decimal` / `Instant` first, in a `Value`, so the canonical
    // text those types write is hashed under the features too.
    for case in golden("command_hash.json") {
        let name = case["name"].as_str().unwrap();
        let hashed = host_values(&case["command"])
            .and_then(|command| command_hash(case["type"].as_str().unwrap(), &command).map_err(|e| e.to_string()));
        match hashed {
            Ok(hash) => assert_eq!(Some(hash), case["expected"].as_u64(), "{name}"),
            Err(error) => assert_eq!(Some(error.as_str()), case["error"].as_str(), "{name}"),
        }
    }
}

#[test]
fn cursor_vector_holds() {
    for case in golden("cursor.json") {
        if case["document"].is_null() || !case["error"].is_null() {
            continue; // decode-only and refusal rows: no cursor is minted
        }
        let keys: Vec<SortKey> = serde_json::from_value(case["sort"].clone()).unwrap();
        let cursor = encode_cursor(&case["document"], &keys).unwrap();
        assert_eq!(
            Some(cursor.as_str()),
            case["expected_cursor"].as_str(),
            "{}",
            case["sort"]
        );
    }
}

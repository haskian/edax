#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use edax_core::ErrorCode;
use edax_cqrs::protected::{
    command_fingerprint, decode_record, encode_record, ProtectedRecord, ProtectedReplay, ProtectedState,
};
use edax_golden::{hex, write_or_check};
use edax_seal::{Cipher, EncryptionInfo, KeyScope};
use serde_json::json;

const FINGERPRINT_KEY: [u8; 32] = [0x31; 32];
const DATA_KEY: [u8; 32] = [0x47; 32];
const COMMAND: &[u8] = br#"{"customer_id":"customer-42","address":"17 Example Lane"}"#;
const RESULT: &[u8] = br#"{"customer_id":"customer-42","address":"17 Example Lane","updated":true}"#;

#[test]
fn golden_protected_ledger() {
    let claim = edax_cqrs::claim_key(edax_core::Tenancy::Silo, None, "addresses", "request-17").unwrap();
    let mut cases = Vec::new();
    for (name, tenant, subject) in [
        ("namespace", None, None),
        ("tenant", Some("tenant-7"), None),
        ("subject", None, Some("customer-42")),
        ("tenant_subject", Some("tenant-7"), Some("customer-42")),
    ] {
        let scope = KeyScope {
            key_namespace: "production-eu".into(),
            tenant: tenant.map(str::to_owned),
            subject: subject.map(str::to_owned),
        };
        let storage_key = edax_cqrs::protected::claim_key(&scope, &claim).unwrap();
        assert_eq!(storage_key, claim);
        let other_scope = KeyScope {
            key_namespace: "production-us".into(),
            ..scope.clone()
        };
        assert_eq!(edax_cqrs::protected::claim_key(&other_scope, &claim).unwrap(), claim);
        let fingerprint = command_fingerprint("UpdateAddress", 1, COMMAND, "fingerprint-1", &FINGERPRINT_KEY).unwrap();
        let pending =
            ProtectedRecord::pending(scope.clone(), claim.clone(), "claim-owner-1".into(), fingerprint).unwrap();
        let other_scope_error = pending.replay(&other_scope, &claim, &pending.fingerprint).unwrap_err();
        assert_eq!(other_scope_error.code, ErrorCode::Codec);
        for cipher in Cipher::ALL {
            let info = EncryptionInfo {
                cipher: *cipher,
                nonce: vec![0x24; cipher.nonce_len()],
                ..info("data-key-1")
            };
            let done = pending
                .seal_result("claim-owner-1", info.clone(), &DATA_KEY, RESULT)
                .unwrap();
            let failed = pending.fail("claim-owner-1", ErrorCode::Rejected).unwrap();
            let done_record = encode_record(&done).unwrap();
            assert_eq!(
                decode_record(&done_record).unwrap().open_result(&DATA_KEY).unwrap(),
                RESULT
            );
            cases.push(json!({
                "kind": "record", "name": format!("{name}_{}", cipher.as_str()),
                "scope": scope, "scope_hex": scope.encode_hex().unwrap(),
                "claim_key": claim, "storage_key": storage_key, "owner": "claim-owner-1",
                "command_name": "UpdateAddress", "command_version": 1, "command_hex": hex(COMMAND),
                "fingerprint_key_id": "fingerprint-1", "fingerprint_key_hex": hex(&FINGERPRINT_KEY),
                "fingerprint_json": serde_json::to_string(&pending.fingerprint).unwrap(),
                "fingerprint_hex": hex(&pending.fingerprint.value),
                "data_key_hex": hex(&DATA_KEY), "encryption": info, "result_hex": hex(RESULT),
                "result_aad_hex": hex(&pending.result_context(&info).unwrap()),
                "pending_record": encode_record(&pending).unwrap(), "done_record": done_record,
                "failed_record": encode_record(&failed).unwrap(),
                "other_scope": other_scope, "other_scope_error": other_scope_error.to_string(),
            }));
        }
    }
    for (name, record) in [
        ("pending", edax_cqrs::Record::pending(42)),
        (
            "done",
            edax_cqrs::Record::done(42, json!({"customer_id":"customer-42","updated":true})),
        ),
        (
            "failed",
            edax_cqrs::Record::failed(42, ErrorCode::Rejected, "address refused"),
        ),
    ] {
        let record = String::from_utf8(edax_cqrs::encode_record(&record).unwrap()).unwrap();
        let error = decode_record(&record).unwrap_err();
        assert_eq!(error.code, ErrorCode::Codec);
        cases.push(json!({
            "kind":"legacy", "name":name, "scope":scope(), "claim_key":claim,
            "storage_key":edax_cqrs::protected::claim_key(&scope(), &claim).unwrap(),
            "record":record, "error":error.to_string(),
        }));
    }
    write_or_check("protected_ledger.json", cases);
}

fn scope() -> KeyScope {
    KeyScope {
        key_namespace: "production-eu".into(),
        tenant: Some("tenant-7".into()),
        subject: Some("customer-42".into()),
    }
}

fn info(generation: &str) -> EncryptionInfo {
    EncryptionInfo {
        format_version: 1,
        key_namespace: "production-eu".into(),
        key_id: generation.into(),
        cipher: Cipher::default(),
        nonce: vec![0x24; 24],
    }
}

fn pending() -> Result<ProtectedRecord, edax_core::Error> {
    let fingerprint = command_fingerprint("UpdateAddress", 1, COMMAND, "fingerprint-1", &FINGERPRINT_KEY)?;
    ProtectedRecord::pending(scope(), "request-17".into(), "claim-owner-1".into(), fingerprint)
}

#[test]
fn full_keyed_fingerprint_canonicalizes_content_and_binds_type_version_and_key() {
    let fingerprint = pending().unwrap().fingerprint;
    assert_eq!(fingerprint.algorithm, "hmac-sha256");
    assert_eq!(fingerprint.value.len(), 32);
    let reordered = br#"{ "address": "17 Example Lane", "customer_id": "customer-42" }"#;
    assert_eq!(
        command_fingerprint("UpdateAddress", 1, reordered, "fingerprint-1", &FINGERPRINT_KEY).unwrap(),
        fingerprint
    );
    for (name, version, data, key_id, key) in [
        ("ChangeAddress", 1, COMMAND, "fingerprint-1", FINGERPRINT_KEY),
        ("UpdateAddress", 2, COMMAND, "fingerprint-1", FINGERPRINT_KEY),
        (
            "UpdateAddress",
            1,
            br#"{"customer_id":"customer-42","address":"99 Changed Lane"}"#.as_slice(),
            "fingerprint-1",
            FINGERPRINT_KEY,
        ),
        ("UpdateAddress", 1, COMMAND, "fingerprint-2", FINGERPRINT_KEY),
        ("UpdateAddress", 1, COMMAND, "fingerprint-1", [0x32; 32]),
    ] {
        assert_ne!(
            command_fingerprint(name, version, data, key_id, &key).unwrap(),
            fingerprint
        );
    }
    assert!(command_fingerprint("UpdateAddress", 1, b"{", "fingerprint-1", &FINGERPRINT_KEY).is_err());
    assert!(command_fingerprint(
        "UpdateAddress",
        1,
        br#"{"id":9007199254740993}"#,
        "fingerprint-1",
        &FINGERPRINT_KEY
    )
    .is_err());
}

#[test]
fn retry_replays_across_fresh_ciphertext_and_data_key_rotation() {
    let original = pending().unwrap();
    assert!(matches!(
        original.replay(&scope(), "request-17", &original.fingerprint).unwrap(),
        ProtectedReplay::Pending
    ));
    let first = original
        .seal_result("claim-owner-1", info("data-key-1"), &DATA_KEY, RESULT)
        .unwrap();
    let second = original
        .seal_result("claim-owner-1", info("data-key-2"), &[0x48; 32], RESULT)
        .unwrap();
    assert_eq!(first.fingerprint, second.fingerprint);
    assert_ne!(first, second);
    for record in [&first, &second] {
        assert!(matches!(
            record.replay(&scope(), "request-17", &original.fingerprint).unwrap(),
            ProtectedReplay::Done(_)
        ));
    }
    assert_eq!(first.open_result(&DATA_KEY).unwrap(), RESULT);
    assert_eq!(second.open_result(&[0x48; 32]).unwrap(), RESULT);
    let mut fresh_nonce = info("data-key-1");
    fresh_nonce.nonce.fill(0x25);
    let fresh = original
        .seal_result("claim-owner-1", fresh_nonce, &DATA_KEY, RESULT)
        .unwrap();
    assert_ne!(first, fresh);
    assert_eq!(fresh.fingerprint, first.fingerprint);
}

#[test]
fn result_authentication_binds_claim_owner_scope_fingerprint_and_encryption_info() {
    let original = pending()
        .unwrap()
        .seal_result("claim-owner-1", info("data-key-1"), &DATA_KEY, RESULT)
        .unwrap();
    for field in [
        "claim",
        "owner",
        "namespace",
        "tenant",
        "subject",
        "fingerprint",
        "fingerprint_key",
        "encryption_key",
    ] {
        let mut changed = original.clone();
        match field {
            "claim" => changed.claim_id = "request-18".into(),
            "owner" => changed.owner = "claim-owner-2".into(),
            "namespace" => changed.scope.key_namespace = "production-us".into(),
            "tenant" => changed.scope.tenant = Some("tenant-8".into()),
            "subject" => changed.scope.subject = Some("customer-43".into()),
            "fingerprint" => changed.fingerprint.value[0] ^= 1,
            "fingerprint_key" => changed.fingerprint.key_id = "fingerprint-2".into(),
            "encryption_key" => {
                if let ProtectedState::Done { result } = &mut changed.state {
                    result.encryption.key_id = "data-key-2".into();
                }
            }
            _ => unreachable!(),
        }
        assert!(changed.open_result(&DATA_KEY).is_err(), "{field}");
    }
    assert!(original.replay(&scope(), "request-18", &original.fingerprint).is_err());
    let other_scope = KeyScope {
        tenant: Some("tenant-8".into()),
        ..scope()
    };
    assert!(original
        .replay(&other_scope, "request-17", &original.fingerprint)
        .is_err());
    let changed_payload = command_fingerprint(
        "UpdateAddress",
        1,
        br#"{"customer_id":"customer-43"}"#,
        "fingerprint-1",
        &FINGERPRINT_KEY,
    )
    .unwrap();
    assert_eq!(
        original
            .replay(&scope(), "request-17", &changed_payload)
            .unwrap_err()
            .code,
        ErrorCode::PayloadMismatch
    );
}

#[test]
fn malformed_fingerprints_and_records_are_refused() {
    let original = pending().unwrap();
    for field in ["algorithm", "key_id", "length"] {
        let mut changed = original.clone();
        match field {
            "algorithm" => changed.fingerprint.algorithm = "fnv1a".into(),
            "key_id" => changed.fingerprint.key_id.clear(),
            "length" => changed.fingerprint.value.truncate(31),
            _ => unreachable!(),
        }
        assert!(changed.check().is_err(), "{field}");
        assert!(encode_record(&changed).is_err(), "{field}");
        let bytes = serde_json::to_string(&changed).unwrap();
        assert!(decode_record(&bytes).is_err(), "{field}");
    }
    let mut invalid = original.clone();
    invalid.format_version = 2;
    assert!(invalid.check().is_err());
    let json = encode_record(&original).unwrap();
    let mut with_unkeyed_hash: serde_json::Value = serde_json::from_str(&json).unwrap();
    with_unkeyed_hash["payload_hash"] = serde_json::json!(42);
    assert!(decode_record(&with_unkeyed_hash.to_string()).is_err());
    let error = decode_record(r#"{"secret":"17 Example Lane","format_version":"bad"}"#).unwrap_err();
    assert!(!error.to_string().contains("17 Example Lane"));
}

#[test]
fn failures_persist_only_stable_codes_and_claim_fencing_is_enforced() {
    let original = pending().unwrap();
    let failed = original.fail("claim-owner-1", ErrorCode::Rejected).unwrap();
    let encoded = encode_record(&failed).unwrap();
    assert!(encoded.contains("rejected"));
    assert!(!encoded.contains("message"));
    assert!(!encoded.contains("payload_hash"));
    assert!(!encoded.contains("17 Example Lane"));
    assert_eq!(decode_record(&encoded).unwrap(), failed);
    assert!(matches!(
        failed.replay(&scope(), "request-17", &original.fingerprint).unwrap(),
        ProtectedReplay::Failed(ErrorCode::Rejected)
    ));
    assert_eq!(failed.open_result(&DATA_KEY).unwrap_err().code, ErrorCode::Rejected);
    for code in [
        ErrorCode::Store,
        ErrorCode::InProgress,
        ErrorCode::BadHandle,
        ErrorCode::KeyUnavailable,
        ErrorCode::KeyUnauthorized,
    ] {
        assert!(original.fail("claim-owner-1", code).is_err());
    }
    assert!(original.fail("wrong-owner", ErrorCode::Rejected).is_err());
    assert!(original
        .seal_result("wrong-owner", info("data-key-1"), &DATA_KEY, RESULT)
        .is_err());
    assert!(failed.fail("claim-owner-1", ErrorCode::Rejected).is_err());
    assert!(failed
        .seal_result("claim-owner-1", info("data-key-1"), &DATA_KEY, RESULT)
        .is_err());
    let done = original
        .seal_result("claim-owner-1", info("data-key-1"), &DATA_KEY, RESULT)
        .unwrap();
    let json = encode_record(&done).unwrap();
    assert!(!json.contains("17 Example Lane"));
    assert!(!json.contains("payload_hash"));
    assert_eq!(decode_record(&json).unwrap().open_result(&DATA_KEY).unwrap(), RESULT);
}

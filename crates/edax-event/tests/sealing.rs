//! Sealing contract and adversarial context tests. All keys/nonces below are fixed test material.
//! Regenerate only this vector: EDAX_REGEN=1 cargo test -p edax-event --test sealing golden_sealing.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions identify contract failures"
)]

use edax_event::sealing::{
    check_owner, command_context, event_context, message_scope, open_command, open_event, seal_command, seal_event,
    state_context,
};
use edax_event::testkit::{base_command, base_draft, RECORDED_AT};
use edax_event::{CommandMessage, EventDraft, EventMessage, Format};
use edax_golden::{hex, write_or_check};
use edax_seal::{Cipher, EncryptionInfo};
use serde_json::{json, Value};

const KEY: [u8; 32] = [0x47; 32];
const PLAINTEXT: &[u8] = br#"{"address":"17 Example Lane","customer":"customer-42"}"#;

fn info(cipher: Cipher) -> EncryptionInfo {
    EncryptionInfo {
        format_version: 1,
        key_namespace: "production-eu".into(),
        key_id: "generation-1".into(),
        cipher,
        nonce: vec![0x24; cipher.nonce_len()],
    }
}

fn draft() -> EventDraft {
    EventDraft {
        tenant: Some("tenant-7".into()),
        subject: Some("customer-42".into()),
        occurred_at: Some(RECORDED_AT - 1),
        data: PLAINTEXT.to_vec(),
        ..base_draft()
    }
}

fn command() -> CommandMessage {
    CommandMessage {
        tenant: Some("tenant-7".into()),
        subject: Some("customer-42".into()),
        data: PLAINTEXT.to_vec(),
        ..base_command()
    }
}

fn sealed_event(cipher: Cipher) -> EventMessage {
    seal_event(draft(), info(cipher), &KEY).unwrap().record(RECORDED_AT)
}

fn sealed_command(cipher: Cipher) -> CommandMessage {
    seal_command(command(), info(cipher), &KEY).unwrap()
}

fn common_changes() -> Vec<(&'static str, Value)> {
    vec![
        ("id", json!("different-message")),
        ("aggregate_id", json!("different-aggregate")),
        ("tenant", json!("tenant-8")),
        ("subject", json!("customer-43")),
        ("name", json!("DifferentPayload")),
        ("version", json!(2)),
        ("correlation_id", json!("different-flow")),
        ("causation_id", json!("different-parent")),
        ("extensions", json!({"traceparent": "altered"})),
        ("metadata", json!({"trace": "altered"})),
    ]
}

#[test]
fn every_event_producer_field_is_authenticated() {
    for cipher in Cipher::ALL {
        let original = sealed_event(*cipher);
        let mut changes = common_changes();
        changes.extend([
            ("sequence", json!(8)),
            ("sequence", Value::Null),
            ("occurred_at", json!(RECORDED_AT)),
            ("occurred_at", Value::Null),
            ("tenant", Value::Null),
            ("subject", Value::Null),
            ("encryption", Value::Null),
        ]);
        for (field, value) in changes {
            let mut wire = serde_json::to_value(&original).unwrap();
            wire[field] = value;
            let altered: EventMessage = serde_json::from_value(wire).unwrap();
            assert!(open_event(&altered, &KEY).is_err(), "{cipher:?}: {field}");
        }
        let mut recorded_later = original.clone();
        recorded_later.recorded_at = i64::MAX;
        assert_eq!(
            event_context(&original).unwrap(),
            event_context(&recorded_later).unwrap()
        );
        assert_eq!(open_event(&recorded_later, &KEY).unwrap(), PLAINTEXT);
    }
}

#[test]
fn every_command_producer_field_is_authenticated() {
    for cipher in Cipher::ALL {
        let original = sealed_command(*cipher);
        let mut changes = common_changes();
        changes.extend([
            ("idempotency_key", json!("different-intent")),
            ("idempotency_key", Value::Null),
            ("issued_at", json!(RECORDED_AT + 1)),
            ("aggregate_id", Value::Null),
            ("tenant", Value::Null),
            ("subject", Value::Null),
            ("encryption", Value::Null),
        ]);
        for (field, value) in changes {
            let mut wire = serde_json::to_value(&original).unwrap();
            wire[field] = value;
            let altered: CommandMessage = serde_json::from_value(wire).unwrap();
            assert!(open_command(&altered, &KEY).is_err(), "{cipher:?}: {field}");
        }
    }
}

#[test]
fn every_encryption_field_and_ciphertext_is_authenticated() {
    for cipher in Cipher::ALL {
        let event = sealed_event(*cipher);
        let command = sealed_command(*cipher);
        for field in ["format_version", "key_namespace", "key_id", "cipher", "nonce"] {
            let mut altered_event = event.clone();
            let encryption = altered_event.encryption.as_mut().unwrap();
            match field {
                "format_version" => encryption.format_version = 2,
                "key_namespace" => encryption.key_namespace.push_str("-other"),
                "key_id" => encryption.key_id.push_str("-other"),
                "cipher" => {
                    encryption.cipher = if *cipher == Cipher::Aes256Gcm {
                        Cipher::XChaCha20Poly1305
                    } else {
                        Cipher::Aes256Gcm
                    };
                    encryption.nonce.resize(encryption.cipher.nonce_len(), 0x24);
                }
                "nonce" => encryption.nonce[0] ^= 1,
                _ => unreachable!(),
            }
            let mut altered_command = command.clone();
            altered_command.encryption = altered_event.encryption.clone();
            assert!(open_event(&altered_event, &KEY).is_err(), "event {cipher:?}: {field}");
            assert!(
                open_command(&altered_command, &KEY).is_err(),
                "command {cipher:?}: {field}"
            );
        }
        for offset in [0, event.data.len() - 1] {
            let mut altered = event.clone();
            altered.data[offset] ^= 1;
            assert!(open_event(&altered, &KEY).is_err());
        }
        for offset in [0, command.data.len() - 1] {
            let mut altered = command.clone();
            altered.data[offset] ^= 1;
            assert!(open_command(&altered, &KEY).is_err());
        }
        assert!(open_event(&event, &[0; 32]).is_err());
        assert!(open_command(&command, &[0; 32]).is_err());
        assert_ne!(event_context(&event).unwrap(), command_context(&command).unwrap());
        let swapped = CommandMessage {
            data: event.data,
            ..command
        };
        assert!(
            open_command(&swapped, &KEY).is_err(),
            "event ciphertext cannot become a command"
        );
    }
}

#[test]
fn both_codecs_preserve_protection_without_payload_plaintext() {
    for cipher in Cipher::ALL {
        let event = sealed_event(*cipher);
        let command = sealed_command(*cipher);
        for format in Format::ALL {
            let event_wire = format.encode(&event).unwrap();
            let command_wire = format.encode(&command).unwrap();
            let decoded_event: EventMessage = format.decode(&event_wire).unwrap();
            let decoded_command: CommandMessage = format.decode(&command_wire).unwrap();
            assert_eq!(decoded_event, event);
            assert_eq!(decoded_command, command);
            assert_eq!(open_event(&decoded_event, &KEY).unwrap(), PLAINTEXT);
            assert_eq!(open_command(&decoded_command, &KEY).unwrap(), PLAINTEXT);
            let plaintext_base64 = serde_json::to_value(draft().record(RECORDED_AT)).unwrap()["data"]
                .as_str()
                .unwrap()
                .to_owned();
            for wire in [&event_wire, &command_wire] {
                assert!(!wire.windows(PLAINTEXT.len()).any(|bytes| bytes == PLAINTEXT));
                assert!(!wire
                    .windows(plaintext_base64.len())
                    .any(|bytes| bytes == plaintext_base64.as_bytes()));
            }
        }
    }
}

#[test]
fn malformed_encryption_is_never_plaintext_fallback() {
    for cipher in Cipher::ALL {
        for original in [
            serde_json::to_value(sealed_event(*cipher)).unwrap(),
            serde_json::to_value(sealed_command(*cipher)).unwrap(),
        ] {
            let event_kind = original.get("recorded_at").is_some();
            let mut malformed = Vec::new();
            for field in ["format_version", "key_namespace", "key_id", "cipher", "nonce"] {
                let mut altered = original.clone();
                altered["encryption"].as_object_mut().unwrap().remove(field);
                malformed.push((format!("missing {field}"), altered));
            }
            for (field, value) in [
                ("format_version", json!(0)),
                ("key_namespace", json!("")),
                ("key_id", json!("")),
                ("cipher", json!("unknown")),
                ("nonce", json!("")),
                ("nonce", json!("not base64!")),
            ] {
                let mut altered = original.clone();
                altered["encryption"][field] = value;
                malformed.push((format!("invalid {field}"), altered));
            }
            let mut truncated = original.clone();
            truncated["data"] = json!("");
            malformed.push(("missing authentication tag".into(), truncated));
            for (case, value) in malformed {
                // JSON object values have base64 bytes. MessagePack needs native bytes, tested
                // separately below so the failure cannot merely be its byte representation.
                let wire = serde_json::to_vec(&value).unwrap();
                let refused = if event_kind {
                    Format::Json.decode::<EventMessage>(&wire).is_err()
                } else {
                    Format::Json.decode::<CommandMessage>(&wire).is_err()
                };
                assert!(refused, "{cipher:?}: {case}");
            }
        }
        let mut malformed = sealed_event(*cipher);
        malformed.encryption.as_mut().unwrap().nonce.clear();
        let wire = rmp_serde::to_vec_named(&malformed).unwrap();
        assert!(Format::Msgpack.decode::<EventMessage>(&wire).is_err());
        assert!(Format::Msgpack.encode(&malformed).is_err());
        let mut malformed = sealed_command(*cipher);
        malformed.data.truncate(15);
        let wire = rmp_serde::to_vec_named(&malformed).unwrap();
        assert!(Format::Msgpack.decode::<CommandMessage>(&wire).is_err());
        assert!(Format::Msgpack.encode(&malformed).is_err());
    }
}

#[test]
fn historical_envelopes_default_to_unprotected_and_cannot_be_opened() {
    for is_event in [true, false] {
        let mut value = if is_event {
            serde_json::to_value(base_draft().record(RECORDED_AT)).unwrap()
        } else {
            serde_json::to_value(base_command()).unwrap()
        };
        value.as_object_mut().unwrap().remove("subject");
        value.as_object_mut().unwrap().remove("encryption");
        let wire = serde_json::to_vec(&value).unwrap();
        if is_event {
            let historical = Format::Json.decode::<EventMessage>(&wire).unwrap();
            assert!(historical.subject.is_none());
            assert!(historical.encryption.is_none());
            assert!(open_event(&historical, &KEY).is_err());
        } else {
            let historical = Format::Json.decode::<CommandMessage>(&wire).unwrap();
            assert!(historical.subject.is_none());
            assert!(historical.encryption.is_none());
            assert!(open_command(&historical, &KEY).is_err());
        }
    }
}

#[test]
fn ownership_scope_and_double_sealing_are_checked() {
    for (committed, owner, proposed, accepted) in [
        (false, None, Some("customer-42"), true),
        (true, Some("customer-42"), Some("customer-42"), true),
        (true, Some("customer-42"), Some("customer-43"), false),
        (true, Some("customer-42"), None, false),
        (true, None, Some("customer-42"), false),
        (true, None, None, true),
    ] {
        assert_eq!(check_owner(committed, owner, proposed).is_ok(), accepted);
    }
    let encryption = info(Cipher::default());
    let scope = message_scope("production-eu", Some("tenant-7"), Some("customer-42"), &encryption).unwrap();
    assert_eq!(scope.tenant.as_deref(), Some("tenant-7"));
    assert!(message_scope("staging", Some("tenant-7"), Some("customer-42"), &encryption).is_err());
    assert!(message_scope("production-eu", Some(""), Some("customer-42"), &encryption).is_err());
    assert!(message_scope("production-eu", Some("tenant-7"), None, &encryption).is_err());
    let sealed = seal_event(draft(), encryption.clone(), &KEY).unwrap();
    assert!(seal_event(sealed, encryption.clone(), &KEY).is_err());
    assert!(seal_command(sealed_command(Cipher::default()), encryption, &KEY).is_err());
}

#[derive(Clone)]
struct State<'a> {
    kind: &'a str,
    identity: &'a str,
    type_name: &'a str,
    version: u64,
    tenant: Option<&'a str>,
    subject: Option<&'a str>,
    marks: &'a str,
    completed: bool,
}

impl State<'_> {
    fn context(&self, info: &EncryptionInfo) -> Result<Vec<u8>, edax_core::Error> {
        state_context(
            self.kind,
            self.identity,
            self.type_name,
            self.version,
            self.tenant,
            self.subject,
            self.marks,
            self.completed,
            info,
        )
    }
}

fn state(kind: &str) -> State<'_> {
    State {
        kind,
        identity: "customer-42",
        type_name: "Customer",
        version: 7,
        tenant: Some("tenant-7"),
        subject: Some("customer-42"),
        marks: r#"{"last_event":"evt-1","sequence":7}"#,
        completed: false,
    }
}

#[test]
fn stored_state_cannot_move_between_records_or_versions() {
    for cipher in Cipher::ALL {
        for kind in ["snapshot", "saga"] {
            let original = state(kind);
            let info = info(*cipher);
            let context = original.context(&info).unwrap();
            let ciphertext = edax_seal::seal(&info, &KEY, &context, PLAINTEXT).unwrap();
            assert_eq!(edax_seal::open(&info, &KEY, &context, &ciphertext).unwrap(), PLAINTEXT);
            for field in [
                "kind",
                "identity",
                "type",
                "version",
                "tenant",
                "subject",
                "marks",
                "completed",
            ] {
                let mut altered = original.clone();
                match field {
                    "kind" => altered.kind = if kind == "snapshot" { "saga" } else { "snapshot" },
                    "identity" => altered.identity = "customer-43",
                    "type" => altered.type_name = "DifferentState",
                    "version" => altered.version += 1,
                    "tenant" => altered.tenant = Some("tenant-8"),
                    "subject" => altered.subject = Some("customer-43"),
                    "marks" => altered.marks = "{}",
                    "completed" => altered.completed = true,
                    _ => unreachable!(),
                }
                let changed_context = altered.context(&info).unwrap();
                assert!(
                    edax_seal::open(&info, &KEY, &changed_context, &ciphertext).is_err(),
                    "{cipher:?}/{kind}/{field}"
                );
            }
        }
    }
    assert!(state("unknown").context(&info(Cipher::default())).is_err());
}

#[test]
fn golden_sealing() {
    let mut cases = Vec::new();
    for cipher in Cipher::ALL {
        let encryption = info(*cipher);
        let event = sealed_event(*cipher);
        let command = sealed_command(*cipher);
        cases.push(json!({"kind":"event", "cipher":cipher.as_str(), "key_hex":hex(&KEY), "plaintext_hex":hex(PLAINTEXT), "encryption":encryption, "aad_hex":hex(&event_context(&event).unwrap()), "ciphertext_hex":hex(&event.data), "json_hex":hex(&Format::Json.encode(&event).unwrap()), "msgpack_hex":hex(&Format::Msgpack.encode(&event).unwrap())}));
        cases.push(json!({"kind":"command", "cipher":cipher.as_str(), "key_hex":hex(&KEY), "plaintext_hex":hex(PLAINTEXT), "encryption":encryption, "aad_hex":hex(&command_context(&command).unwrap()), "ciphertext_hex":hex(&command.data), "json_hex":hex(&Format::Json.encode(&command).unwrap()), "msgpack_hex":hex(&Format::Msgpack.encode(&command).unwrap())}));
        for kind in ["snapshot", "saga"] {
            let state = state(kind);
            let context = state.context(&encryption).unwrap();
            let ciphertext = edax_seal::seal(&encryption, &KEY, &context, PLAINTEXT).unwrap();
            cases.push(json!({"kind":kind,"cipher":cipher.as_str(),"key_hex":hex(&KEY),"plaintext_hex":hex(PLAINTEXT),"encryption":encryption,"identity":state.identity,"type_name":state.type_name,"version":state.version,"tenant":state.tenant,"subject":state.subject,"marks":state.marks,"completed":state.completed,"aad_hex":hex(&context),"ciphertext_hex":hex(&ciphertext)}));
        }
    }
    write_or_check("sealing.json", cases);
}

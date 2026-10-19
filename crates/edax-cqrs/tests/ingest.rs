use std::collections::BTreeMap;

use base64::{engine::general_purpose::STANDARD, Engine};
use edax_core::ErrorCode;
use edax_cqrs::ingest::{
    plan, raw_fingerprint, row_key, source_key, validate_inputs, Contract, Input, Rejection, Stored,
};
use edax_cqrs::namespace::{key, prefix};
use serde_json::json;

#[allow(clippy::unwrap_used, reason = "test fixture must be valid")]
fn event(offset: u64, sequence: u64, status: &str) -> Input {
    let content = json!({"type": "order.state", "status": status});
    let raw = json!({
        "topic": "orders", "partition": 0, "offset": offset,
        "key": STANDARD.encode(b"order-42"),
        "value": STANDARD.encode(content.to_string()),
        "headers": [["content-type", STANDARD.encode(b"application/json")]],
        "timestamp": 1_700_000_000_000_i64, "timestamp_type": 0, "oversized": false,
    })
    .to_string();
    Input {
        source: source_key("projection-v1", "orders-v1", "orders", 0, offset.try_into().unwrap()).unwrap(),
        key: row_key("projection-v1", "upsert", Some("tenant-7"), "order-42").unwrap(),
        stream: "order-42".into(),
        event_id: format!("order-42:{sequence}"),
        sequence: Some(sequence),
        content: content.clone(),
        value: Some(json!({"order_id": "order-42", "status": status})),
        raw_fingerprint: raw_fingerprint(raw.as_bytes()),
        raw,
        failure: None,
    }
}

#[allow(clippy::unwrap_used, reason = "test fixture must be valid")]
fn append_event(offset: u64, sequence: u64, status: &str) -> Input {
    let mut input = event(offset, sequence, status);
    input.key = row_key("projection-v1", "append", Some("tenant-7"), &input.event_id).unwrap();
    input
}

#[allow(clippy::unwrap_used, reason = "test fixture must be valid")]
fn current(input: &Input) -> BTreeMap<String, Stored> {
    plan(
        "upsert",
        std::slice::from_ref(input),
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
    .unwrap()
    .writes
}

#[test]
fn target_namespaces_and_key_parts_are_unambiguous() {
    assert_eq!(prefix("projection-v1").unwrap(), "13:projection-v1:");
    assert_eq!(prefix("é").unwrap(), "2:é:");
    assert_eq!(
        key("v1", r#"[ "tenant-7", {"b": 2, "a": 1} ]"#).unwrap(),
        r#"2:v1:["tenant-7",{"a":1,"b":2}]"#
    );
    for target in ["", "target\0v1"] {
        assert_eq!(prefix(target).unwrap_err().code, ErrorCode::BadHandle);
    }
    for parts in ["{}", "null", "[", "[9007199254740993]"] {
        assert_eq!(key("v1", parts).unwrap_err().code, ErrorCode::Codec);
    }
    assert_ne!(key("a:b", r#"["c"]"#).unwrap(), key("a", r#"["b:c"]"#).unwrap());
}

#[test]
fn row_keys_match_validated_input_identities() {
    for (mode, tenant) in [("upsert", Some("tenant-7")), ("append", None), ("append", Some(""))] {
        let contract = Contract {
            mode: mode.into(),
            generation: "projection-v1".into(),
            source_incarnation: "orders-v1".into(),
            mapper_id: "orders-mapper-v1".into(),
        };
        let mut input = event(0, 1, "open");
        let row_id = if mode == "append" { &input.event_id } else { "order-42" };
        input.key = row_key(&contract.generation, mode, tenant, row_id).unwrap();
        assert_eq!(
            input.key,
            key(&contract.generation, &json!([mode, tenant, row_id]).to_string()).unwrap()
        );
        assert_eq!(validate_inputs(&contract, &[input]), Ok(()));
    }
    for (name, target, mode, row_id) in [
        ("unknown mode", "v1", "fold", "order-42"),
        ("empty row", "v1", "upsert", ""),
        ("empty event", "v1", "append", ""),
        ("empty target", "", "upsert", "order-42"),
        ("NUL target", "v1\0", "upsert", "order-42"),
    ] {
        assert_eq!(
            row_key(target, mode, None, row_id).unwrap_err().code,
            ErrorCode::BadHandle,
            "{name}"
        );
    }
    assert_ne!(
        row_key("v1", "upsert", None, "order-42").unwrap(),
        row_key("v1", "upsert", Some(""), "order-42").unwrap()
    );
}

#[test]
fn current_state_comparison_branches() {
    let initial = event(10, 3, "paid");
    let stored = current(&initial);
    let cases = [
        ("stale", event(11, 2, "other"), "accepted", None),
        ("exact", event(11, 3, "paid"), "accepted", None),
        ("conflict", event(11, 3, "cancelled"), "rejected", None),
        ("newer", event(11, 4, "shipped"), "accepted", Some(4)),
    ];
    for (name, input, outcome, write_sequence) in cases {
        let result = plan("upsert", &[input], &stored, &BTreeMap::new()).unwrap();
        assert_eq!(result.outcomes, [outcome], "{name}");
        assert_eq!(
            result.writes.values().next().and_then(|row| row.sequence),
            write_sequence,
            "{name}"
        );
        assert_eq!(result.rejections.len(), usize::from(outcome == "rejected"), "{name}");
    }
}

#[test]
fn validates_originals_before_compaction_and_retains_first_owner() {
    let first = event(0, 4, "paid");
    let mut other = event(1, 6, "shipped");
    other.stream = "another-order".into();
    let conflicting = event(2, 4, "cancelled");
    let newer = event(3, 5, "shipped");
    let lower = event(4, 2, "open");
    let result = plan(
        "upsert",
        &[first.clone(), other, conflicting, newer.clone(), lower],
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(
        result.outcomes,
        ["accepted", "rejected", "rejected", "accepted", "accepted"]
    );
    assert_eq!(result.writes[&first.key].sequence, Some(5));
    assert_eq!(result.writes[&first.key].value, newer.value);
    assert_eq!(result.writes[&first.key].stream, first.stream);
}

#[test]
fn committed_reject_cannot_claim_an_absent_key_on_replay() {
    let mut rejected = event(0, 1, "invalid");
    rejected.failure = Some("codec: mapper refused this event".into());
    let initial = plan("upsert", &[rejected.clone()], &BTreeMap::new(), &BTreeMap::new()).unwrap();
    assert!(initial.writes.is_empty());
    rejected.failure = None;
    let mut valid = event(1, 2, "paid");
    valid.stream = "new-owner".into();
    let result = plan(
        "upsert",
        &[rejected.clone(), valid.clone()],
        &BTreeMap::new(),
        &initial.rejections,
    )
    .unwrap();
    assert_eq!(result.outcomes, ["rejected", "accepted"]);
    assert_eq!(result.writes[&valid.key].stream, "new-owner");
    assert_eq!(result.rejections, initial.rejections);
}

#[test]
fn invalid_records_never_propose_owners() {
    let cases = [
        ("absent", None, json!({"status": "open"})),
        ("overflow", Some(i64::MAX as u64 + 1), json!({"status": "open"})),
        ("unsafe_json", Some(1), json!({"total": 9_007_199_254_740_993_u64})),
    ];
    for (name, sequence, content) in cases {
        let mut invalid = event(0, 1, "open");
        invalid.sequence = sequence;
        invalid.content = content;
        let mut valid = event(1, 0, "paid");
        valid.stream = "new-owner".into();
        let result = plan("upsert", &[invalid, valid.clone()], &BTreeMap::new(), &BTreeMap::new()).unwrap();
        assert_eq!(result.outcomes, ["rejected", "accepted"], "{name}");
        assert_eq!(result.writes[&valid.key].stream, "new-owner", "{name}");
        assert!(
            result.rejections.values().all(|r| r.error.starts_with("codec: ")),
            "{name}"
        );
    }
}

#[test]
fn tombstones_keep_the_owner_and_sequence() {
    let mut deleted = event(1, 3, "deleted");
    deleted.value = None;
    let stored = current(&deleted);
    let mut different_owner = event(4, 5, "new");
    different_owner.stream = "different-order".into();
    let result = plan(
        "upsert",
        &[event(2, 2, "open"), deleted.clone(), different_owner],
        &stored,
        &BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(result.outcomes, ["accepted", "accepted", "rejected"]);
    assert!(result.writes.is_empty());
    assert_eq!(result.rejections.len(), 1);
    assert!(result
        .rejections
        .values()
        .all(|rejection| rejection.error.contains("another aggregate stream")));
}

#[test]
fn append_exact_replays_and_conflicts() {
    let first = append_event(0, 1, "open");
    let duplicate = append_event(1, 1, "open");
    let different = append_event(2, 1, "cancelled");
    let result = plan(
        "append",
        &[first.clone(), duplicate, different.clone()],
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(result.outcomes, ["accepted", "accepted", "rejected"]);
    assert_eq!(result.writes.len(), 1);
    assert_eq!(result.writes[&first.key].value, first.value);
    let replay = plan("append", &[first, different], &result.writes, &result.rejections).unwrap();
    assert_eq!(replay.outcomes, ["accepted", "rejected"]);
    assert!(replay.writes.is_empty());
}

#[test]
fn source_coordinate_reuse_is_fatal_even_before_rejection() {
    let first = event(0, 1, "open");
    let other = event(0, 1, "paid");
    let err = plan(
        "upsert",
        &[first.clone(), other.clone()],
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadHandle);
    let rejected = BTreeMap::from([(
        first.source.clone(),
        Rejection {
            fingerprint: first.raw_fingerprint.clone(),
            error: "rejected: invalid state".into(),
            raw: first.raw,
            published: true,
        },
    )]);
    let err = plan("upsert", &[other], &BTreeMap::new(), &rejected).unwrap_err();
    assert_eq!(err.code, ErrorCode::BadHandle);
}

#[test]
fn source_digest_survives_full_record_to_stub_changes() {
    let mut original = event(0, 1, "open");
    original.raw_fingerprint = raw_fingerprint(b"stable original source record");
    original.failure = Some("codec: mapper refused this event".into());
    let initial = plan("upsert", &[original.clone()], &BTreeMap::new(), &BTreeMap::new()).unwrap();
    let mut stub = original.clone();
    stub.raw = json!({"topic": "orders", "partition": 0, "offset": 0, "oversized": true}).to_string();
    for (name, inputs, rejected) in [
        ("persisted rejection", vec![stub.clone()], initial.rejections.clone()),
        ("same batch", vec![original.clone(), stub.clone()], BTreeMap::new()),
    ] {
        let result = plan("upsert", &inputs, &BTreeMap::new(), &rejected).unwrap();
        assert_eq!(result.outcomes, vec!["rejected"; inputs.len()], "{name}");
        let mut expected = initial.rejections.clone();
        if name == "persisted rejection" {
            expected.get_mut(&original.source).unwrap().raw = stub.raw.clone();
        }
        assert_eq!(result.rejections, expected, "{name}");
        assert!(result.writes.is_empty(), "{name}");
    }
    stub.raw_fingerprint = raw_fingerprint(b"different original source record");
    for (name, inputs, rejected) in [
        ("persisted changed source", vec![stub.clone()], initial.rejections),
        ("same batch changed source", vec![original, stub], BTreeMap::new()),
    ] {
        assert_eq!(
            plan("upsert", &inputs, &BTreeMap::new(), &rejected).unwrap_err().code,
            ErrorCode::BadHandle,
            "{name}"
        );
    }
}

#[test]
fn preserves_published_rejections_and_handles_empty_and_invalid_modes() {
    let input = event(0, 1, "open");
    let rejected = BTreeMap::from([(
        input.source.clone(),
        Rejection {
            fingerprint: input.raw_fingerprint.clone(),
            error: "rejected: invalid state".into(),
            raw: input.raw.clone(),
            published: true,
        },
    )]);
    let replay = plan("upsert", &[input], &BTreeMap::new(), &rejected).unwrap();
    assert_eq!(replay.rejections, rejected);
    assert!(plan("append", &[], &BTreeMap::new(), &BTreeMap::new())
        .unwrap()
        .outcomes
        .is_empty());
    assert_eq!(
        plan("fold", &[], &BTreeMap::new(), &BTreeMap::new()).unwrap_err().code,
        ErrorCode::BadHandle
    );
}

#[test]
fn valid_updates_survive_every_permutation_batch_cut_and_replay() {
    let mut deletion = event(2, 3, "deleted");
    deletion.value = None;
    let originals = [event(0, 1, "open"), event(1, 2, "paid"), deletion.clone()];
    for permutation in [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
        for cut in 0..4 {
            let inputs: Vec<Input> = permutation.iter().map(|&i| originals[i].clone()).collect();
            let mut stored = BTreeMap::new();
            let mut start = 0;
            for end in 1..=inputs.len() {
                if end != inputs.len() && cut & (1 << (end - 1)) == 0 {
                    continue;
                }
                for _ in 0..2 {
                    let result = plan("upsert", &inputs[start..end], &stored, &BTreeMap::new()).unwrap();
                    assert!(result.rejections.is_empty());
                    stored.extend(result.writes);
                }
                start = end;
            }
            assert_eq!(stored[&deletion.key].sequence, Some(3));
            assert_eq!(stored[&deletion.key].value, None);
        }
    }
}

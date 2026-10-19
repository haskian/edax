#![allow(clippy::unwrap_used, clippy::indexing_slicing, reason = "test assertions")]
use edax_cqrs::projection::{self, Contract, Request};
use serde_json::{json, Value};
use std::collections::BTreeMap;

fn contract() -> Contract {
    serde_json::from_value(json!({"generation":"account-balances-v1","source_identity":"cluster-42/topic-81","source_history":"no-rollback-policy-1","topic":"account-events","reducer_id":"balance-v1","envelope_id":"event-v1","keying_id":"account-v1","effect_namespace":"account-effects-v1","starts":{"0":0,"1":0},"at_rest":"sealed"})).unwrap()
}
fn event(sequence: i64, id: &str) -> Value {
    json!({"tenant":"bank-eu","aggregate":"account-42","event_id":id,"sequence":sequence,"event_type":"CreditPosted","schema_version":"1","payload":{"currency":"EUR","amount":"12.50"},"metadata":{"correlation_id":"payment-99"}})
}
fn input(offset: i64, event: Value) -> Value {
    json!({"offset":offset,"event":event})
}
fn proof() -> Value {
    json!({"identity":"cluster-42/topic-81","history":"no-rollback-policy-1","beginning":0,"end":100})
}
fn reduce(balance: &str) -> Value {
    json!({"private":null,"public":{"balance":balance,"currency":"EUR"},"intents":[{"key":"receipt","destination":"email","payload":{"account":"account-42"}}]})
}
/// A protected-state envelope of `subject` in tenant `bank-eu`, as a host seals a private part.
fn envelope(subject: &str) -> Value {
    let encryption = edax_seal::EncryptionInfo {
        format_version: 1,
        key_namespace: "accounts".into(),
        key_id: "generation-7".into(),
        cipher: edax_seal::Cipher::XChaCha20Poly1305,
        nonce: vec![0x24; edax_seal::Cipher::XChaCha20Poly1305.nonce_len()],
    };
    let state = edax_event::sealing::ProtectedState::new(
        Some("bank-eu".into()),
        Some(subject.into()),
        false,
        encryption,
        vec![0x5a; 40],
    );
    serde_json::from_str(&edax_event::sealing::encode_protected_state(&state).unwrap()).unwrap()
}
/// A subject's reducer result: its private part and its receipt's payload sealed.
fn reduce_sealed(subject: &str) -> Value {
    json!({"private":envelope(subject),"public":{"currency":"EUR"},"intents":[{"key":"receipt","destination":"email","payload":envelope(subject)}]})
}
fn prepare(lease: &Value, inputs: Vec<Value>) -> Value {
    json!({"op":"prepare_batch","lease":lease,"inputs":inputs,"proof":proof()})
}
fn apply(lease: &Value, inputs: Vec<Value>, reduced: Vec<Value>) -> Value {
    json!({"op":"apply_batch","lease":lease,"inputs":inputs,"proof":proof(),"reduced":reduced})
}
struct Model {
    records: BTreeMap<String, Value>,
    intents: BTreeMap<String, Value>,
    cases: Vec<Value>,
    c: Contract,
}
impl Model {
    fn new() -> Self {
        Self {
            records: BTreeMap::new(),
            intents: BTreeMap::new(),
            cases: vec![],
            c: contract(),
        }
    }
    fn call(&mut self, name: &str, operation: Value, now: i64, ok: bool) -> Value {
        let request: Request = serde_json::from_value(json!({"contract":self.c,"operation":operation})).unwrap();
        // A request whose keys the kernel refuses reads no records; `transition` refuses it alike.
        let keys = projection::keys(&request).unwrap_or_default();
        let lock = projection::lock(&request).ok();
        let records: BTreeMap<_, _> = keys
            .iter()
            .filter_map(|k| self.records.get(k).map(|v| (k.clone(), v.clone())))
            .collect();
        let outcome = projection::transition(&request, &records, now);
        assert_eq!(outcome.is_ok(), ok, "{name}: {outcome:?}");
        let (want, error, result) = match outcome {
            Ok(plan) => {
                for key in plan.inserts.keys() {
                    assert!(!self.records.contains_key(key), "{name}: insert of a present key");
                }
                self.records.extend(plan.inserts.clone());
                self.records.extend(plan.updates.clone());
                self.intents.extend(plan.intents.clone());
                (json!(plan), Value::Null, plan.result)
            }
            Err(error) => (Value::Null, json!(error.to_string()), Value::Null),
        };
        self.cases.push(json!({"name":name,"request":request,"keys":keys,"lock":lock,"records":records,"now":now,"want":want,"error":error}));
        result
    }
    fn expect_error(&self, code: &str) {
        let error = self.cases.last().unwrap()["error"].as_str().unwrap();
        assert!(error.starts_with(&format!("{code}: ")), "{error}");
    }
}

#[test]
fn ordered_projection_fences_and_cutovers() {
    let mut m = Model::new();
    m.call("initialize", json!({"op":"initialize"}), 0, true);
    let manifest = m.c.manifest_key().unwrap();
    let saved = m.records.remove(&manifest).unwrap();
    m.call("partial_restore_refused", json!({"op":"initialize"}), 0, false);
    m.expect_error("bad_handle");
    m.records.insert(manifest, saved);
    let l = m.call(
        "acquire",
        json!({"op":"acquire","partition":0,"owner":"worker-a","lease_ms":1000,"proof":proof()}),
        10,
        true,
    );
    m.call(
        "live_owner_blocks_takeover",
        json!({"op":"acquire","partition":0,"owner":"worker-b","lease_ms":1000,"proof":proof()}),
        11,
        false,
    );
    m.expect_error("in_progress");
    let stale = json!({"identity":"cluster-42/topic-81","history":"no-rollback-policy-1","beginning":0,"end":0});
    m.call(
        "owned_partition_with_stale_proof_waits",
        json!({"op":"acquire","partition":0,"owner":"worker-b","lease_ms":1000,"proof":stale}),
        11,
        false,
    );
    m.expect_error("in_progress");
    let mut oversized = event(1, "credit-1");
    oversized["payload"] = json!({"amount":1e18});
    m.call(
        "normalized_envelope_must_remain_canonical_safe",
        prepare(&l, vec![input(0, oversized)]),
        12,
        false,
    );
    m.call(
        "normalized_state_must_remain_canonical_safe",
        apply(
            &l,
            vec![input(0, event(1, "credit-1"))],
            vec![json!({"private":null,"public":{"amount":1e18},"intents":[]})],
        ),
        12,
        false,
    );
    let prepared = m.call(
        "prepare_first_delta",
        prepare(&l, vec![input(3, event(1, "credit-1"))]),
        12,
        true,
    );
    assert_eq!(prepared["items"][0]["novel"], true);
    assert_eq!(prepared["items"][0]["state"], Value::Null);
    m.call(
        "reducer_results_must_match_inputs",
        apply(&l, vec![input(3, event(1, "credit-1"))], vec![]),
        13,
        false,
    );
    m.expect_error("bad_handle");
    m.call(
        "novel_event_needs_a_reducer_result",
        apply(&l, vec![input(3, event(1, "credit-1"))], vec![Value::Null]),
        13,
        false,
    );
    m.expect_error("bad_handle");
    let l2 = m.call(
        "legitimate_broker_gap",
        apply(&l, vec![input(3, event(1, "credit-1"))], vec![reduce("12.50")]),
        13,
        true,
    );
    assert_eq!(l2["next_offset"], 4);
    assert_eq!(m.intents.len(), 1);
    let recovered = m.call(
        "uncertain_commit_recovers_database_cursor",
        json!({"op":"load","lease":l}),
        14,
        true,
    );
    assert_eq!(recovered, l2);
    m.call(
        "replay_takes_no_reducer_result",
        apply(&l2, vec![input(5, event(1, "credit-1"))], vec![reduce("999")]),
        15,
        false,
    );
    m.expect_error("bad_handle");
    let count = m.records.len();
    let l3 = m.call(
        "logical_duplicate_new_offset",
        apply(&l2, vec![input(5, event(1, "credit-1"))], vec![Value::Null]),
        15,
        true,
    );
    assert_eq!(m.records.len(), count);
    for (name, ev) in [
        ("event_identity_reused", event(2, "credit-1")),
        ("sequence_identity_reused", event(1, "other")),
        ("missing_delta", event(3, "credit-3")),
    ] {
        m.call(name, prepare(&l3, vec![input(6, ev)]), 16, false);
        m.expect_error("codec");
    }
    let mut changed = event(1, "credit-1");
    changed["metadata"]["correlation_id"] = json!("changed");
    m.call(
        "reducer_metadata_conflict",
        prepare(&l3, vec![input(6, changed)]),
        16,
        false,
    );
    m.expect_error("codec");
    // One batch: two deltas of one aggregate and an in-batch copy. The copy reads as a replay, and both
    // deltas see the state the batch read; the host carries the reducer's state between them.
    let batch = vec![
        input(6, event(2, "credit-2")),
        input(7, event(2, "credit-2")),
        input(8, event(3, "credit-3")),
    ];
    let prepared = m.call("batch_prepares_in_source_order", prepare(&l3, batch.clone()), 17, true);
    let novel: Vec<_> = prepared["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["novel"].clone())
        .collect();
    assert_eq!(novel, [json!(true), json!(false), json!(true)]);
    // The aggregate's stored state is sent once; the host carries the reducer's state to the next delta.
    assert_eq!(prepared["items"][0]["state"]["public"]["balance"], "12.50");
    assert_eq!(prepared["items"][2]["state"], Value::Null);
    let l4 = m.call(
        "batch_applies_every_delta_in_one_plan",
        apply(&l3, batch, vec![reduce("25.00"), Value::Null, reduce("37.50")]),
        17,
        true,
    );
    assert_eq!(l4["next_offset"], 9);
    assert_eq!(m.intents.len(), 3);
    let mut overlong = event(4, "credit-4");
    overlong["aggregate"] = json!("a".repeat(3000));
    m.call(
        "overlong_storage_key_refused",
        prepare(&l4, vec![input(9, overlong)]),
        18,
        false,
    );
    m.expect_error("codec");
    let mut nul_tenant = event(4, "credit-4");
    nul_tenant["tenant"] = json!("bank\u{0}eu");
    m.call(
        "nul_tenant_refused",
        prepare(&l4, vec![input(9, nul_tenant)]),
        18,
        false,
    );
    m.expect_error("codec");
    let receipt = json!({"key":"receipt","destination":"email","payload":{"account":"account-42"}});
    m.call(
        "duplicate_intent_key_refused",
        apply(
            &l4,
            vec![input(9, event(4, "credit-4"))],
            vec![json!({"private":null,"public":{"balance":"50.00","currency":"EUR"},"intents":[receipt, receipt]})],
        ),
        18,
        false,
    );
    m.expect_error("codec");
    m.call(
        "offset_below_cursor_refused",
        prepare(&l4, vec![input(8, event(4, "credit-4"))]),
        18,
        false,
    );
    m.expect_error("bad_handle");
    m.call(
        "offset_at_proof_end_refused",
        prepare(&l4, vec![input(100, event(4, "credit-4"))]),
        18,
        false,
    );
    m.expect_error("bad_handle");
    m.call(
        "nul_in_reducer_output_refused",
        apply(
            &l4,
            vec![input(9, event(4, "credit-4"))],
            vec![json!({"private":null,"public":{"note":"x\u{0}y"},"intents":[]})],
        ),
        18,
        false,
    );
    m.expect_error("codec");
    // Store faults blame the store, not the message: drop receipts the way a partial restore would.
    let replay = prepare(&l4, vec![input(9, event(1, "credit-1"))]);
    let request: Request = serde_json::from_value(json!({"contract":m.c,"operation":replay})).unwrap();
    let keys = projection::keys(&request).unwrap();
    let receipt = |kind: &str| keys.iter().find(|k| k.contains(kind)).unwrap().clone();
    let (event_key, sequence_key) = (receipt("\"event\""), receipt("\"sequence\""));
    let saved = m.records.clone();
    m.records.remove(&sequence_key);
    m.call("incomplete_receipts_blame_the_store", replay.clone(), 18, false);
    m.expect_error("bad_handle");
    m.records.remove(&event_key);
    m.call("state_ahead_of_receipts_blames_the_store", replay.clone(), 18, false);
    m.expect_error("bad_handle");
    m.records = saved.clone();
    let state_key = keys.iter().find(|k| k.contains("\"state\"")).unwrap().clone();
    m.records.remove(&state_key);
    m.call("receipts_without_state_blame_the_store", replay, 18, false);
    m.expect_error("bad_handle");
    m.records = saved;
    for (name, patch) in [
        ("retention_overtook_cursor", json!({"beginning":10})),
        ("topic_recreated", json!({"identity":"cluster-42/new-topic"})),
        ("history_changed", json!({"history":"rollback-2"})),
        ("truncated_end", json!({"end":8})),
    ] {
        let mut p = proof();
        for (k, v) in patch.as_object().unwrap() {
            p[k] = v.clone();
        }
        m.call(
            name,
            json!({"op":"prepare_batch","lease":l4,"inputs":[input(9, event(4, "credit-4"))],"proof":p}),
            19,
            false,
        );
        m.expect_error("bad_handle");
    }
    m.call(
        "refresh_needs_an_owner",
        json!({"op":"refresh","lease":{"checkpoint_key":l4["checkpoint_key"],"partition":0,"owner":"","epoch":1,"next_offset":9,"expires_ms":0},"lease_ms":1000,"proof":proof()}),
        19,
        false,
    );
    m.expect_error("bad_handle");
    let refreshed = m.call(
        "refresh_extends_a_live_lease",
        json!({"op":"refresh","lease":l4,"lease_ms":1000,"proof":proof()}),
        20,
        true,
    );
    assert_eq!(refreshed["epoch"], l4["epoch"]);
    let refenced = m.call(
        "refresh_refences_an_expired_current_lease",
        json!({"op":"refresh","lease":refreshed,"lease_ms":1000,"proof":proof()}),
        1020,
        true,
    );
    assert_eq!(refenced["epoch"], 2);
    let fresh = m.call(
        "expired_owner_takeover",
        json!({"op":"acquire","partition":0,"owner":"worker-b","lease_ms":1000,"proof":proof()}),
        2021,
        true,
    );
    m.call(
        "obsolete_writer_fenced",
        apply(&refenced, vec![input(9, event(4, "credit-4"))], vec![reduce("50.00")]),
        2022,
        false,
    );
    m.expect_error("bad_handle");
    let other = m.call(
        "acquire_second_partition",
        json!({"op":"acquire","partition":1,"owner":"worker-c","lease_ms":1000,"proof":proof()}),
        2022,
        true,
    );
    m.call(
        "aggregate_moved_partition_refused",
        prepare(&other, vec![input(0, event(4, "credit-4"))]),
        2022,
        false,
    );
    m.expect_error("codec");
    m.call(
        "stale_release_cannot_release_new_owner",
        json!({"op":"release","lease":refenced}),
        2023,
        true,
    );
    assert_eq!(
        m.call("fresh_owner_still_live", json!({"op":"load","lease":fresh}), 2024, true),
        fresh
    );
    let state = m.call(
        "get_reads_one_state",
        json!({"op":"get","tenant":"bank-eu","aggregate":"account-42"}),
        2025,
        true,
    );
    assert_eq!(state["sequence"], 3);

    // A destroyed key: the receipts move past the event, the reducer never runs, the state stays.
    let erased = |offset: i64, event: Value| json!({"offset":offset,"event":event,"erased":true});
    let mut erased_credit = event(4, "credit-4");
    erased_credit["payload"] = Value::Null;
    m.call(
        "erased_event_refuses_a_reducer_result",
        apply(&fresh, vec![erased(9, erased_credit.clone())], vec![reduce("50.00")]),
        2025,
        false,
    );
    m.expect_error("bad_handle");
    let prepared = m.call(
        "prepare_marks_an_erased_event_not_novel",
        prepare(&fresh, vec![erased(9, erased_credit.clone())]),
        2025,
        true,
    );
    assert_eq!(prepared["items"][0]["novel"], false);
    let fresh = m.call(
        "erased_event_writes_receipts_only",
        apply(&fresh, vec![erased(9, erased_credit.clone())], vec![Value::Null]),
        2025,
        true,
    );
    assert_eq!(fresh["next_offset"], 10);
    let fresh = m.call(
        "erased_replay_is_a_duplicate",
        apply(&fresh, vec![erased(10, erased_credit)], vec![Value::Null]),
        2025,
        true,
    );
    let state = m.call(
        "erased_event_leaves_state_unchanged",
        json!({"op":"get","tenant":"bank-eu","aggregate":"account-42"}),
        2025,
        true,
    );
    assert_eq!(state["sequence"], 3);

    // A sealed event: its receipts bind the keyed fingerprint, and its state sits under its subject.
    let mut sealed = event(1, "opened-77");
    sealed["aggregate"] = json!("account-77");
    sealed["subject"] = json!("person-7");
    let parsed: edax_cqrs::projection::Event = serde_json::from_value(sealed.clone()).unwrap();
    let keyed = projection::keyed_fingerprint(&[7u8; 32], &parsed).unwrap();
    let sealed_input = json!({"offset":11,"event":sealed,"fingerprint":keyed});
    let fresh = m.call(
        "keyed_fingerprint_binds_the_receipts",
        apply(&fresh, vec![sealed_input.clone()], vec![reduce_sealed("person-7")]),
        2025,
        true,
    );
    let stored = m.call(
        "get_reads_the_state_of_a_subject",
        json!({"op":"get","tenant":"bank-eu","subject":"person-7","aggregate":"account-77"}),
        2025,
        true,
    );
    assert_eq!(stored["sequence"], 1);
    m.call(
        "unkeyed_replay_of_a_keyed_event_conflicts",
        apply(
            &fresh,
            vec![json!({"offset":12,"event":sealed_input["event"]})],
            vec![Value::Null],
        ),
        2025,
        false,
    );
    m.expect_error("codec");
    let mut moved = event(2, "credit-77");
    moved["aggregate"] = json!("account-77");
    moved["subject"] = json!("person-8");
    m.call(
        "aggregate_cannot_change_its_subject",
        apply(&fresh, vec![input(12, moved)], vec![reduce_sealed("person-8")]),
        2025,
        false,
    );
    m.expect_error("codec");
    let mut short_mac = event(2, "credit-77b");
    short_mac["aggregate"] = json!("account-77");
    short_mac["subject"] = json!("person-7");
    m.call(
        "keyed_fingerprint_must_be_hex_digest",
        apply(
            &fresh,
            vec![json!({"offset":12,"event":short_mac,"fingerprint":"abc"})],
            vec![reduce_sealed("person-7")],
        ),
        2025,
        false,
    );
    m.expect_error("bad_handle");
    // A sealed generation never stores a subject's private part or intent payload as plaintext.
    let mut next = event(2, "credit-77d");
    next["aggregate"] = json!("account-77");
    next["subject"] = json!("person-7");
    let next_parsed: edax_cqrs::projection::Event = serde_json::from_value(next.clone()).unwrap();
    let next_input = json!({"offset":12,"event":next,"fingerprint":projection::keyed_fingerprint(&[7u8; 32], &next_parsed).unwrap()});
    let mut plaintext_payload = reduce_sealed("person-7");
    plaintext_payload["intents"][0]["payload"] = json!({"account":"account-77"});
    let mut foreign_private = reduce_sealed("person-7");
    foreign_private["private"] = envelope("person-8");
    for (name, reduced) in [
        ("plaintext_private_of_a_subject_refused", reduce("25.00")),
        ("plaintext_intent_payload_of_a_subject_refused", plaintext_payload),
        ("private_sealed_for_another_subject_refused", foreign_private),
    ] {
        m.call(name, apply(&fresh, vec![next_input.clone()], vec![reduced]), 2025, false);
        m.expect_error("bad_handle");
    }
    for (name, subject) in [
        ("empty_subject_refused", json!("")),
        ("nul_subject_refused", json!("person\u{0}-7")),
    ] {
        let mut refused = event(2, "credit-77c");
        refused["aggregate"] = json!("account-77");
        refused["subject"] = subject;
        m.call(
            name,
            apply(&fresh, vec![input(12, refused)], vec![reduce("25.00")]),
            2025,
            false,
        );
        m.expect_error("codec");
    }
    // A rewind after erasure replays an event applied before it: its old receipts stand.
    let mut applied = event(1, "credit-1");
    applied["payload"] = Value::Null;
    let fresh = m.call(
        "erased_replay_of_an_applied_event_is_a_duplicate",
        apply(&fresh, vec![erased(12, applied)], vec![Value::Null]),
        2025,
        true,
    );
    assert_eq!(fresh["next_offset"], 13);

    let cutover = json!({"source_identity":m.c.source_identity,"source_history":m.c.source_history,"topic":m.c.topic,"cutoffs":{"0":0,"1":0}});
    m.call(
        "activate_initial_generation",
        json!({"op":"activate","cutover":cutover}),
        2026,
        true,
    );
    m.call(
        "activation_replay_is_idempotent",
        json!({"op":"activate","cutover":cutover}),
        2026,
        true,
    );
    let frozen = m.call("freeze_captures_database_cutoffs", json!({"op":"freeze"}), 2027, true);
    assert_eq!(frozen["cutoffs"], json!({"0":13,"1":0}));
    m.call(
        "frozen_generation_rejects_new_write",
        prepare(&fresh, vec![input(9, event(4, "credit-4"))]),
        2028,
        false,
    );
    m.call(
        "cannot_reactivate_frozen_generation",
        json!({"op":"activate","cutover":cutover}),
        2029,
        false,
    );
    edax_golden::write_or_check("projection_state.json", m.cases);
}

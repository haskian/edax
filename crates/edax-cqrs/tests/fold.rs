#![allow(clippy::unwrap_used, clippy::indexing_slicing, reason = "test assertions")]
use edax_cqrs::fold::{self, Contract, Request};
use serde_json::{json, Value};
use std::collections::BTreeMap;

fn contract() -> Contract {
    serde_json::from_value(json!({"generation":"account-balances-v1","source_identity":"cluster-42/topic-81","source_history":"no-rollback-policy-1","topic":"account-events","reducer_id":"balance-v1","envelope_id":"event-v1","keying_id":"account-v1","effect_namespace":"account-effects-v1","starts":{"0":0,"1":0}})).unwrap()
}
fn event(sequence: i64, id: &str) -> Value {
    json!({"tenant":"bank-eu","aggregate":"account-42","event_id":id,"sequence":sequence,"event_type":"CreditPosted","schema_version":"1","payload":{"currency":"EUR","amount":"12.50"},"metadata":{"correlation_id":"payment-99"}})
}
fn proof() -> Value {
    json!({"identity":"cluster-42/topic-81","history":"no-rollback-policy-1","beginning":0,"end":100})
}
fn reduce(balance: &str) -> Value {
    json!({"state":{"balance":balance,"currency":"EUR"},"intents":[{"key":"receipt","destination":"email","payload":{"account":"account-42"}}]})
}
struct Model {
    records: BTreeMap<String, Value>,
    cases: Vec<Value>,
    c: Contract,
}
impl Model {
    fn new() -> Self {
        Self {
            records: BTreeMap::new(),
            cases: vec![],
            c: contract(),
        }
    }
    fn call(&mut self, name: &str, operation: Value, now: i64, ok: bool) -> Value {
        let request: Request = serde_json::from_value(json!({"contract":self.c,"operation":operation})).unwrap();
        let keys = fold::keys(&request).unwrap();
        let records: BTreeMap<_, _> = keys
            .into_iter()
            .filter_map(|k| self.records.get(&k).map(|v| (k, v.clone())))
            .collect();
        let outcome = fold::transition(&request, &records, now);
        assert_eq!(outcome.is_ok(), ok, "{name}: {outcome:?}");
        let (want, error, result) = match outcome {
            Ok(plan) => {
                self.records.extend(plan.writes.clone());
                (json!(plan), Value::Null, plan.result)
            }
            Err(error) => (Value::Null, json!(error.to_string()), Value::Null),
        };
        self.cases
            .push(json!({"name":name,"request":request,"records":records,"now":now,"want":want,"error":error}));
        result
    }
}

#[test]
fn ordered_folds_fences_and_cutovers() {
    let mut m = Model::new();
    m.call("initialize", json!({"op":"initialize"}), 0, true);
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
    let mut oversized = event(1, "credit-1");
    oversized["payload"] = json!({"amount":1e18});
    m.call(
        "normalized_envelope_must_remain_canonical_safe",
        json!({"op":"prepare","lease":l,"offset":0,"event":oversized,"proof":proof()}),
        12,
        false,
    );
    m.call("normalized_state_must_remain_canonical_safe", json!({"op":"apply","lease":l,"offset":0,"event":event(1,"credit-1"),"proof":proof(),"reduced":{"state":{"amount":1e18},"intents":[]}}), 12, false);
    let prepared = m.call(
        "prepare_first_delta",
        json!({"op":"prepare","lease":l,"offset":3,"event":event(1,"credit-1"),"proof":proof()}),
        12,
        true,
    );
    assert_eq!(prepared["novel"], true);
    assert_eq!(prepared["state"], Value::Null);
    let l2=m.call("legitimate_broker_gap",json!({"op":"apply","lease":l,"offset":3,"event":event(1,"credit-1"),"proof":proof(),"reduced":reduce("12.50")}),13,true);
    assert_eq!(l2["next_offset"], 4);
    let recovered = m.call(
        "uncertain_commit_recovers_database_cursor",
        json!({"op":"load","lease":l}),
        14,
        true,
    );
    assert_eq!(recovered, l2);
    let count = m.records.len();
    let l3 = m.call(
        "logical_duplicate_new_offset",
        json!({"op":"apply","lease":l2,"offset":5,"event":event(1,"credit-1"),"proof":proof(),"reduced":reduce("999")}),
        15,
        true,
    );
    assert_eq!(m.records.len(), count);
    for (name, ev) in [
        ("event_identity_reused", event(2, "credit-1")),
        ("sequence_identity_reused", event(1, "other")),
        ("missing_delta", event(3, "credit-3")),
    ] {
        m.call(
            name,
            json!({"op":"prepare","lease":l3,"offset":6,"event":ev,"proof":proof()}),
            16,
            false,
        );
    }
    let mut changed = event(1, "credit-1");
    changed["metadata"]["correlation_id"] = json!("changed");
    m.call(
        "reducer_metadata_conflict",
        json!({"op":"prepare","lease":l3,"offset":6,"event":changed,"proof":proof()}),
        16,
        false,
    );
    let l4=m.call("noop_advances_sequence",json!({"op":"apply","lease":l3,"offset":6,"event":event(2,"noop-2"),"proof":proof(),"reduced":{"state":{"balance":"12.50","currency":"EUR"},"intents":[]}}),17,true);
    for (name, patch) in [
        ("retention_overtook_cursor", json!({"beginning":8})),
        ("topic_recreated", json!({"identity":"cluster-42/new-topic"})),
        ("history_changed", json!({"history":"rollback-2"})),
        ("truncated_end", json!({"end":6})),
    ] {
        let mut p = proof();
        for (k, v) in patch.as_object().unwrap() {
            p[k] = v.clone();
        }
        m.call(
            name,
            json!({"op":"prepare","lease":l4,"offset":8,"event":event(3,"credit-3"),"proof":p}),
            18,
            false,
        );
    }
    let fresh = m.call(
        "expired_owner_takeover",
        json!({"op":"acquire","partition":0,"owner":"worker-b","lease_ms":1000,"proof":proof()}),
        1010,
        true,
    );
    m.call("obsolete_writer_fenced",json!({"op":"apply","lease":l4,"offset":8,"event":event(3,"credit-3"),"proof":proof(),"reduced":reduce("25.00")}),1011,false);
    m.call(
        "stale_release_cannot_release_new_owner",
        json!({"op":"release","lease":l4}),
        1012,
        true,
    );
    assert_eq!(
        m.call("fresh_owner_still_live", json!({"op":"load","lease":fresh}), 1013, true),
        fresh
    );
    let cutover = json!({"source_identity":m.c.source_identity,"source_history":m.c.source_history,"topic":m.c.topic,"cutoffs":{"0":0,"1":0}});
    m.call(
        "activate_initial_generation",
        json!({"op":"activate","cutover":cutover}),
        1014,
        true,
    );
    let frozen = m.call("freeze_captures_database_cutoffs", json!({"op":"freeze"}), 1015, true);
    assert_eq!(frozen["cutoffs"], json!({"0":7,"1":0}));
    m.call(
        "frozen_generation_rejects_new_write",
        json!({"op":"prepare","lease":fresh,"offset":8,"event":event(3,"credit-3"),"proof":proof()}),
        1016,
        false,
    );
    m.call(
        "cannot_reactivate_frozen_generation",
        json!({"op":"activate","cutover":cutover}),
        1017,
        false,
    );
    edax_golden::write_or_check("fold.json", m.cases);
}

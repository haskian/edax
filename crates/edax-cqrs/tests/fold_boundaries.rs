#![allow(clippy::unwrap_used, clippy::indexing_slicing, reason = "test assertions")]
use edax_core::Error;
use edax_cqrs::fold::{self, Contract, Proof, Request};
use serde_json::{json, Value};

fn contract() -> Value {
    json!({"generation":"balances-v1","source_identity":"cluster-42/topic-81","source_history":"no-rollback-v1","topic":"account-events","reducer_id":"balance-v1","envelope_id":"event-v1","keying_id":"account-v1","effect_namespace":"receipt-v1","starts":{"0":0,"1":10}})
}

fn case(mut input: Value, result: Result<Value, Error>) -> Value {
    match result {
        Ok(value) => {
            input["want"] = value;
            input["error"] = Value::Null;
        }
        Err(error) => {
            input["want"] = Value::Null;
            input["error"] = json!(error.to_string());
        }
    }
    input
}

#[test]
fn fold_boundary_golden() {
    let mut cases = Vec::new();
    for (name, operation) in [
        ("initialize_complete_partition_vector", json!({"op":"initialize"})),
        (
            "state_identity_escapes_nul_and_unicode",
            json!({"op":"get","tenant":"bank\0eu","aggregate":"račun-42"}),
        ),
        (
            "outbox_key_is_generation_scoped",
            json!({"op":"published","intent_id":"receipt\0credit-42"}),
        ),
        (
            "acquire_unknown_partition",
            json!({"op":"acquire","partition":2,"owner":"worker-a","lease_ms":1000,"proof":{"identity":"cluster-42/topic-81","history":"no-rollback-v1","beginning":0,"end":100}}),
        ),
        ("unknown_operation", json!({"op":"skip_delta"})),
    ] {
        let request = json!({"contract":contract(),"operation":operation});
        let result = serde_json::from_value::<Request>(request.clone())
            .map_err(Error::codec)
            .and_then(|r| fold::keys(&r))
            .map(|keys| json!(keys));
        cases.push(case(json!({"name":name,"op":"keys","request":request}), result));
    }
    for (name, identity, history, beginning, end, cursor, expected) in [
        (
            "cursor_at_beginning",
            "cluster-42/topic-81",
            "no-rollback-v1",
            10,
            20,
            10,
            true,
        ),
        (
            "cursor_at_readable_end",
            "cluster-42/topic-81",
            "no-rollback-v1",
            10,
            20,
            20,
            true,
        ),
        ("empty_source", "cluster-42/topic-81", "no-rollback-v1", 0, 0, 0, true),
        (
            "retention_lost_prefix",
            "cluster-42/topic-81",
            "no-rollback-v1",
            10,
            20,
            9,
            false,
        ),
        (
            "cursor_beyond_end",
            "cluster-42/topic-81",
            "no-rollback-v1",
            10,
            20,
            21,
            false,
        ),
        (
            "recreated_topic",
            "cluster-42/topic-99",
            "no-rollback-v1",
            10,
            20,
            10,
            false,
        ),
        (
            "changed_history",
            "cluster-42/topic-81",
            "rollback-v2",
            10,
            20,
            10,
            false,
        ),
        (
            "negative_beginning",
            "cluster-42/topic-81",
            "no-rollback-v1",
            -1,
            20,
            10,
            false,
        ),
        (
            "reversed_boundaries",
            "cluster-42/topic-81",
            "no-rollback-v1",
            20,
            10,
            10,
            false,
        ),
    ] {
        let c: Contract = serde_json::from_value(contract()).unwrap();
        let proof = json!({"identity":identity,"history":history,"beginning":beginning,"end":end});
        let p: Proof = serde_json::from_value(proof.clone()).unwrap();
        let result = c.validate().and_then(|()| c.continuity(&p, cursor));
        assert_eq!(result.is_ok(), expected, "{name}");
        cases.push(case(
            json!({"name":name,"op":"source","contract":c,"proof":proof,"cursor":cursor}),
            result.map(|()| Value::Null),
        ));
    }
    for (name, active, frozen, partition, offset, expected) in [
        (
            "building_suppresses_effects",
            false,
            false,
            json!(0),
            json!(5),
            Some(false),
        ),
        ("below_cutoff", true, false, json!(0), json!(4), Some(false)),
        ("at_cutoff", true, false, json!(0), json!(5), Some(true)),
        (
            "active_has_no_upper_limit",
            true,
            false,
            json!(0),
            json!(10),
            Some(true),
        ),
        ("frozen_below_upper", true, true, json!(0), json!(9), Some(true)),
        ("frozen_at_upper", true, true, json!(0), json!(10), Some(false)),
        ("unregistered_partition", true, false, json!(2), json!(5), Some(false)),
        ("invalid_partition", true, false, json!(2147483648i64), json!(5), None),
        ("invalid_offset", true, false, json!(0), json!("5"), None),
    ] {
        let manifest = json!({"contract":contract(),"active":active,"frozen":frozen,"cutoffs":{"0":5,"1":10},"upper":{"0":10,"1":20}});
        let intent = json!({"partition":partition,"offset":offset});
        let result = fold::eligible(&manifest, &intent);
        assert_eq!(result.as_ref().ok().copied(), expected, "{name}");
        cases.push(case(
            json!({"name":name,"op":"eligible","manifest":manifest,"intent":intent}),
            result.map(Value::Bool),
        ));
    }
    edax_golden::write_or_check("fold_boundaries.json", cases);
}

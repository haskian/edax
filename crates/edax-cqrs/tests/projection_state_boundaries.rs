#![allow(clippy::unwrap_used, clippy::indexing_slicing, reason = "test assertions")]
use edax_core::Error;
use edax_cqrs::projection::{self, Contract, Proof, Request};
use serde_json::{json, Value};

fn contract() -> Value {
    json!({"generation":"balances-v1","source_identity":"cluster-42/topic-81","source_history":"no-rollback-v1","topic":"account-events","reducer_id":"balance-v1","envelope_id":"event-v1","keying_id":"account-v1","effect_namespace":"receipt-v1","starts":{"0":0,"1":10},"at_rest":"sealed"})
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
fn projection_state_boundary_golden() {
    let mut cases = Vec::new();
    for (name, operation) in [
        ("initialize_complete_partition_vector", json!({"op":"initialize"})),
        (
            "state_identity_escapes_nul_and_unicode",
            json!({"op":"get","tenant":"bank\0eu","aggregate":"račun-42"}),
        ),
        (
            "batch_keys_are_unique_manifest_first",
            json!({"op":"prepare_batch","lease":{"checkpoint_key":"k","partition":1,"owner":"w","epoch":1,"next_offset":10,"expires_ms":5},"inputs":[{"offset":10,"event":{"tenant":null,"aggregate":"a-1","event_id":"e-1","sequence":1,"event_type":"T","schema_version":"1","payload":{},"metadata":{}}},{"offset":11,"event":{"tenant":null,"aggregate":"a-1","event_id":"e-2","sequence":2,"event_type":"T","schema_version":"1","payload":{},"metadata":{}}}],"proof":{"identity":"cluster-42/topic-81","history":"no-rollback-v1","beginning":0,"end":100}}),
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
            .and_then(|r| projection::keys(&r))
            .map(|keys| json!(keys));
        cases.push(case(json!({"name":name,"op":"keys","request":request}), result));
    }
    for (name, operation) in [
        ("initialize_is_exclusive", json!({"op":"initialize"})),
        ("freeze_is_exclusive", json!({"op":"freeze"})),
        (
            "get_shares_the_generation",
            json!({"op":"get","tenant":null,"aggregate":"a-1"}),
        ),
        (
            "release_locks_its_partition",
            json!({"op":"release","lease":{"checkpoint_key":"k","partition":1,"owner":"w","epoch":1,"next_offset":10,"expires_ms":5}}),
        ),
        (
            "unknown_partition_refused",
            json!({"op":"release","lease":{"checkpoint_key":"k","partition":7,"owner":"w","epoch":1,"next_offset":10,"expires_ms":5}}),
        ),
    ] {
        let request = json!({"contract":contract(),"operation":operation});
        let result = serde_json::from_value::<Request>(request.clone())
            .map_err(Error::codec)
            .and_then(|r| projection::lock(&r))
            .map(|lock| json!(lock));
        cases.push(case(json!({"name":name,"op":"lock","request":request}), result));
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
        let manifest = json!({"contract":contract(),"active":active,"frozen":frozen,"cutoffs":{"0":5,"1":10},"upper":{"0":10,"1":20},"position":0});
        let intent = json!({"partition":partition,"offset":offset});
        let result = projection::eligible(&manifest, &intent);
        assert_eq!(result.as_ref().ok().copied(), expected, "{name}");
        cases.push(case(
            json!({"name":name,"op":"eligible","manifest":manifest,"intent":intent}),
            result.map(Value::Bool),
        ));
    }
    let envelope = json!({"tenant":"bank-eu","subject":"person-7","aggregate":"account-7","event_id":"credit-1","sequence":1,"event_type":"CreditPosted","schema_version":"1"});
    let opened = json!({"tenant":"bank-eu","aggregate":"account-7","event_id":"credit-1","sequence":1,"event_type":"CreditPosted","schema_version":"1","payload":{"amount_minor":1250},"metadata":{}});
    let mut moved = opened.clone();
    moved["sequence"] = json!(2);
    for (name, event) in [
        ("sealed_input_takes_the_envelope_subject", Some(opened)),
        ("sealed_input_of_a_destroyed_key_is_erased", None),
        ("sealed_input_must_keep_the_envelope_identity", Some(moved)),
    ] {
        let parsed = event.clone().map(|e| serde_json::from_value(e).unwrap());
        let result = projection::sealed_input(9, &serde_json::from_value(envelope.clone()).unwrap(), parsed);
        cases.push(case(
            json!({"name":name,"op":"sealed_input","offset":9,"envelope":envelope,"event":event}),
            result.map(|input| json!(input)),
        ));
    }
    let c: Contract = serde_json::from_value(contract()).unwrap();
    for (name, tenant, subject) in [
        ("state_scope_range_of_a_subject", Some("bank-eu"), Some("person-7")),
        ("state_scope_range_without_tenant", None, Some("person-7")),
        ("state_scope_range_of_a_whole_tenant", Some("bank-eu"), None),
        ("state_scope_range_empty_subject_refused", Some("bank-eu"), Some("")),
    ] {
        let result = c.state_scope_range(&tenant.map(str::to_owned), subject);
        cases.push(case(
            json!({"name":name,"op":"state_scope_range","contract":contract(),"tenant":tenant,"subject":subject}),
            result.map(|range| json!(range)),
        ));
    }
    // What a host seals in reducer results and opens after reading, and the contract's at-rest rule.
    let event = |subject: Option<&str>, aggregate: &str| json!({"tenant":"bank-eu","subject":subject,"aggregate":aggregate,"event_id":format!("{aggregate}-credit-2"),"sequence":2,"event_type":"CreditPosted","schema_version":"1","payload":{"amount_minor":1250},"metadata":{}});
    let reduced = json!({"private":{"email":"ana@example.com"},"public":{"balance_minor":2500},"intents":[
        {"key":"receipt","destination":"email","payload":{"email":"ana@example.com"}},
        {"key":"ping","destination":"audit","payload":null}
    ]});
    let reductions = json!([
        {"event":event(Some("person-7"), "account-7"),"reduced":reduced},
        {"event":event(None, "account-8"),"reduced":reduced},
        {"event":event(Some("person-9"), "account-9"),"reduced":{"private":null,"public":{"balance_minor":0},"intents":[]}}
    ]);
    let mut plaintext = contract();
    plaintext["at_rest"] = json!("plaintext");
    for (name, contract) in [
        ("seal_plan_of_a_sealed_generation", contract()),
        (
            "seal_plan_of_a_plaintext_generation_seals_intents_only",
            plaintext.clone(),
        ),
    ] {
        let c: Contract = serde_json::from_value(contract.clone()).unwrap();
        let result = projection::seal_plan(&c, &serde_json::from_value::<Vec<_>>(reductions.clone()).unwrap());
        cases.push(case(
            json!({"name":name,"op":"seal_plan","contract":contract,"reductions":reductions}),
            result.map(|placed| json!(placed)),
        ));
    }
    let state = |private: Value| json!({"sequence":3,"partition":0,"private":private,"public":{"balance_minor":2500}});
    let sealed = json!({"edax_protected_state":1});
    for (name, contract, subject, record) in [
        (
            "open_state_of_a_subject",
            contract(),
            Some("person-7"),
            state(sealed.clone()),
        ),
        (
            "open_state_without_subject",
            contract(),
            None,
            state(json!({"email":"x"})),
        ),
        (
            "open_state_without_private",
            contract(),
            Some("person-7"),
            state(Value::Null),
        ),
        (
            "open_state_of_a_plaintext_generation",
            plaintext.clone(),
            Some("person-7"),
            state(json!({"email":"x"})),
        ),
    ] {
        let c: Contract = serde_json::from_value(contract.clone()).unwrap();
        let tenant = Some("bank-eu".to_owned());
        let result = projection::open_state(&c, &tenant, &subject.map(str::to_owned), "account-7", &record);
        cases.push(case(
            json!({"name":name,"op":"open_state","contract":contract,"tenant":tenant,"subject":subject,"aggregate":"account-7","record":record}),
            result.map(|part| json!(part)),
        ));
    }
    let intent = |subject: Option<&str>, payload: Value| json!({"id":"10:receipt-v1:[\"bank-eu\",\"account-7\",\"account-7-credit-2\",\"receipt\"]","generation":"balances-v1","tenant":"bank-eu","subject":subject,"aggregate":"account-7","destination":"email","payload":payload,"partition":0,"offset":4,"ordinal":0});
    for (name, record) in [
        ("open_intent_of_a_subject", intent(Some("person-7"), sealed.clone())),
        ("open_intent_without_subject", intent(None, json!({"email":"x"}))),
        ("open_intent_without_payload", intent(Some("person-7"), Value::Null)),
    ] {
        let result = projection::open_intent(&c, &record);
        cases.push(case(
            json!({"name":name,"op":"open_intent","contract":contract(),"record":record}),
            result.map(|part| json!(part)),
        ));
    }
    let mut unknown = contract();
    unknown["at_rest"] = json!("hashed");
    let request = json!({"contract":unknown,"operation":{"op":"initialize"}});
    let result = serde_json::from_value::<Request>(request.clone())
        .map_err(Error::codec)
        .and_then(|r| projection::keys(&r))
        .map(|keys| json!(keys));
    cases.push(case(
        json!({"name":"unsupported_at_rest_refused","op":"keys","request":request}),
        result,
    ));
    edax_golden::write_or_check("projection_state_boundaries.json", cases);
}

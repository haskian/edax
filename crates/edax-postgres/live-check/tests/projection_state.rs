//! The projection state SQL `edax-postgres` emits and the memory store in `edax-memory`, held to one
//! oracle: the same kernel transitions committed to both leave the same records, and both page, mark
//! and prune the same effect intents.

#![allow(clippy::unwrap_used, clippy::indexing_slicing, reason = "a test")]

#[allow(dead_code, reason = "each test binary uses part of the shared helpers")]
mod common;

use std::collections::BTreeMap;

use edax_cqrs::projection::{self, Contract, Request};
use edax_memory::{MemoryProjectionState, Writes};
use postgres::Client;
use serde_json::{json, Value};

fn contract() -> Contract {
    serde_json::from_value(json!({"generation":"account-balances-v1","source_identity":"cluster-42/topic-81","source_history":"no-rollback-policy-1","topic":"account-events","reducer_id":"balance-v1","envelope_id":"event-v1","keying_id":"account-v1","effect_namespace":"account-effects-v1","starts":{"0":0,"1":0}})).unwrap()
}

fn proof() -> Value {
    json!({"identity":"cluster-42/topic-81","history":"no-rollback-policy-1","beginning":0,"end":100})
}

struct Stores {
    client: Client,
    sql: BTreeMap<String, String>,
    memory: MemoryProjectionState,
    manifest: String,
    generation: String,
}

impl Stores {
    /// Run `operation` as one transaction on each store and return its result, the same from both.
    fn run(&mut self, operation: Value, now: i64) -> Value {
        let request: Request = serde_json::from_value(json!({"contract": contract(), "operation": operation})).unwrap();
        let keys = projection::keys(&request).unwrap();

        let read = self.memory.read(&keys);
        let plan = projection::transition(&request, &read.records, now).unwrap();
        let writes = Writes {
            inserts: plan.inserts.clone(),
            updates: plan.updates.clone(),
            intents: plan.intents.clone(),
        };
        self.memory.commit(&read.versions, &writes).unwrap();

        let mut tx = self.client.transaction().unwrap();
        let rows = tx.query(&self.sql["read"], &[&json!(keys)]).unwrap();
        let records: BTreeMap<String, Value> = rows
            .iter()
            .map(|row| (row.get(0), serde_json::from_str(row.get::<_, &str>(1)).unwrap()))
            .collect();
        assert_eq!(records, read.records, "both stores read the same records");
        let again = projection::transition(&request, &records, now).unwrap();
        assert_eq!(again.result, plan.result);
        for (statement, changes) in [
            ("insert", &again.inserts),
            ("update", &again.updates),
            ("insert_intents", &again.intents),
        ] {
            if !changes.is_empty() {
                tx.execute(&self.sql[statement], &[&json!(changes)]).unwrap();
            }
        }
        tx.commit().unwrap();
        plan.result
    }

    /// One credit per offset on `aggregate`, each with a receipt intent.
    fn credit(&mut self, lease: &Value, aggregate: &str, offsets: &[i64]) -> Value {
        self.credit_of(lease, None, aggregate, offsets)
    }

    /// [`Stores::credit`] for an aggregate an erasure subject owns.
    fn credit_of(&mut self, lease: &Value, subject: Option<&str>, aggregate: &str, offsets: &[i64]) -> Value {
        let inputs: Vec<Value> = offsets
            .iter()
            .zip(1..)
            .map(|(offset, sequence)| {
                json!({"offset": offset, "event": {"tenant":"bank-eu","subject":subject,"aggregate":aggregate,"event_id":format!("{aggregate}-credit-{sequence}"),"sequence":sequence,"event_type":"CreditPosted","schema_version":"1","payload":{"currency":"EUR","amount":"12.50"},"metadata":{"correlation_id":"payment-99"}}})
            })
            .collect();
        let reduced: Vec<Value> = inputs
            .iter()
            .map(|_| json!({"state":{"balance":"12.50","currency":"EUR"},"intents":[{"key":"receipt","destination":"email","payload":{"account":aggregate}}]}))
            .collect();
        self.run(
            json!({"op":"apply_batch","lease":lease,"inputs":inputs,"proof":proof(),"reduced":reduced}),
            20,
        )
    }

    /// The page each store delivers: the same intents in the same order.
    fn pending(&mut self, limit: usize) -> Vec<Value> {
        let memory = self
            .memory
            .unpublished(&self.manifest, &self.generation, limit)
            .unwrap();
        let rows = self
            .client
            .query(
                &self.sql["pending"],
                &[&self.manifest, &self.generation, &limit.to_string()],
            )
            .unwrap();
        let postgres: Vec<Value> = rows
            .iter()
            .map(|row| serde_json::from_str(row.get::<_, &str>(1)).unwrap())
            .collect();
        assert_eq!(postgres, memory, "both stores page the same intents");
        memory
    }

    fn mark(&mut self, keys: &[String]) -> u64 {
        let memory = self
            .memory
            .mark_published(&self.manifest, &self.generation, keys)
            .unwrap();
        let postgres = self
            .client
            .execute(
                &self.sql["mark_published"],
                &[&json!(keys), &self.manifest, &self.generation],
            )
            .unwrap();
        assert_eq!(postgres, memory as u64, "both stores mark the same intents");
        postgres
    }

    fn prune(&mut self, limit: usize) -> i64 {
        let memory = self.memory.prune(&self.manifest, &self.generation, limit).unwrap();
        let postgres: i64 = self
            .client
            .query_one(
                &self.sql["prune"],
                &[&self.manifest, &self.generation, &limit.to_string()],
            )
            .unwrap()
            .get(0);
        assert_eq!(postgres, memory as i64, "both stores prune the same intents");
        postgres
    }
}

/// Delete one subject's state records on both stores; both must delete the same number.
fn delete_scope(stores: &mut Stores, subject: &str) -> i64 {
    let (start, end) = contract().state_scope_range(&Some("bank-eu".into()), subject).unwrap();
    let memory = stores.memory.delete_scope(&start, &end);
    let postgres: i64 = stores
        .client
        .query_one(&stores.sql["delete_scope"], &[&start, &end])
        .unwrap()
        .get(0);
    assert_eq!(postgres, memory as i64, "both stores delete the same records");
    postgres
}

fn provision(schema: &str) -> Option<Stores> {
    let mut client = common::connect()?;
    let sql = edax_postgres::projection::statements(
        &json!({"schema": schema, "table": "edax_projection_state", "intents": "edax_projection_intents"}).to_string(),
    )
    .unwrap();
    client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS \"{schema}\" CASCADE; CREATE SCHEMA \"{schema}\"; {}",
            sql["ddl"]
        ))
        .unwrap();
    assert!(client.query_one(&sql["check"], &[]).unwrap().get::<_, bool>(0));
    let c = contract();
    Some(Stores {
        client,
        sql,
        memory: MemoryProjectionState::default(),
        manifest: c.manifest_key().unwrap(),
        generation: c.generation,
    })
}

#[test]
fn postgres_and_memory_delete_the_same_subject_scope() {
    let Some(mut stores) = provision("edax_live_projection_scope") else {
        return;
    };
    stores.run(json!({"op":"initialize"}), 0);
    let lease = stores.run(
        json!({"op":"acquire","partition":0,"owner":"worker-a","lease_ms":100_000,"proof":proof()}),
        10,
    );
    let lease = stores.credit_of(&lease, Some("person-7"), "account-1", &[0]);
    let lease = stores.credit_of(&lease, Some("person-7"), "account-2", &[1]);
    stores.credit_of(&lease, Some("person-70"), "account-3", &[2]);
    assert_eq!(delete_scope(&mut stores, "person-7"), 2);
    assert_eq!(
        delete_scope(&mut stores, "person-7"),
        0,
        "a repeated cleanup deletes nothing"
    );
    let get =
        |subject: &str, aggregate: &str| json!({"op":"get","tenant":"bank-eu","subject":subject,"aggregate":aggregate});
    assert_eq!(stores.run(get("person-7", "account-1"), 30), Value::Null);
    assert_eq!(stores.run(get("person-70", "account-3"), 30)["sequence"], 1);
}

fn positions(intents: &[Value]) -> Vec<(i64, i64)> {
    intents
        .iter()
        .map(|i| (i["partition"].as_i64().unwrap(), i["offset"].as_i64().unwrap()))
        .collect()
}

#[test]
fn postgres_and_memory_store_the_same_state_and_deliver_the_same_intents() {
    let Some(mut client) = common::connect() else {
        return;
    };
    let schema = "edax_live_projection";
    let sql = edax_postgres::projection::statements(
        &json!({"schema": schema, "table": "edax_projection_state", "intents": "edax_projection_intents"}).to_string(),
    )
    .unwrap();
    client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS \"{schema}\" CASCADE; CREATE SCHEMA \"{schema}\"; {}",
            sql["ddl"]
        ))
        .unwrap();
    assert!(client.query_one(&sql["check"], &[]).unwrap().get::<_, bool>(0));
    let c = contract();
    let mut stores = Stores {
        client,
        sql,
        memory: MemoryProjectionState::default(),
        manifest: c.manifest_key().unwrap(),
        generation: c.generation.clone(),
    };

    stores.run(json!({"op":"initialize"}), 0);
    let acquire = |partition: i64| json!({"op":"acquire","partition":partition,"owner":"worker-a","lease_ms":100_000,"proof":proof()});
    let zero = stores.run(acquire(0), 10);
    let one = stores.run(acquire(1), 10);
    stores.credit(&zero, "account-42", &[3, 5, 7]);
    stores.credit(&one, "account-77", &[2]);
    assert!(stores.pending(10).is_empty(), "an inactive generation delivers nothing");

    let cutover = json!({"source_identity":c.source_identity,"source_history":c.source_history,"topic":c.topic,"cutoffs":{"0":4,"1":0}});
    stores.run(json!({"op":"activate","cutover":cutover}), 30);
    assert_eq!(positions(&stores.pending(10)), [(0, 5), (1, 2), (0, 7)]);
    assert_eq!(positions(&stores.pending(1)), [(1, 2)]);

    let key = |id: &Value| c.intent_key(id.as_str().unwrap()).unwrap();
    let all = stores.pending(10);
    let delivered = key(&all[1]["id"]);
    let below: String = stores
        .client
        .query_one(
            &format!("SELECT key FROM \"{schema}\".edax_projection_intents WHERE source_offset = 3"),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(stores.mark(&[below, delivered, "unknown".into()]), 1);
    assert_eq!(positions(&stores.pending(10)), [(0, 5), (0, 7)]);
    assert_eq!(stores.prune(10), 1, "the intent below the cutoff goes");
    assert_eq!(stores.prune(10), 0);

    // A freeze bounds delivery by its upper offsets on both.
    stores.run(json!({"op":"freeze"}), 40);
    assert_eq!(positions(&stores.pending(10)), [(0, 5), (0, 7)]);
}

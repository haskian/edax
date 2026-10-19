//! Atomic reference storage for projection state and effect intents, the memory twin of
//! `edax_postgres::projection`.
//!
//! A transaction reads its records, runs the kernel's transitions and the host reducer without a lock,
//! then commits. The commit checks that every record it read still has the version it read, absent
//! records included, so two racing transactions cannot both win: the loser fails with `concurrency` and
//! retries, as a Postgres insert race or lock wait does.

use std::collections::BTreeMap;
use std::sync::Mutex;

use edax_core::{lock, Error, ErrorCode};
use edax_cqrs::projection::eligible;
use serde_json::Value;

#[derive(Default)]
struct State {
    /// Each record with the version of its last write; an absent record has version zero.
    records: BTreeMap<String, (u64, Value)>,
    intents: BTreeMap<String, Value>,
    version: u64,
    /// Last partition selected by each generation's delivery page.
    delivery: BTreeMap<(String, String), i64>,
}

/// What a transaction read: each present record, and the version of every key it asked for.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Read {
    /// The present records.
    pub records: BTreeMap<String, Value>,
    /// The version of every key read, zero for an absent one. Pass them back to [`MemoryProjectionState::commit`].
    pub versions: BTreeMap<String, u64>,
}

/// The writes one transaction commits: the kernel plan's `inserts`, `updates` and `intents`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Writes {
    /// Records the transaction read as absent.
    pub inserts: BTreeMap<String, Value>,
    /// Records the transaction read as present.
    pub updates: BTreeMap<String, Value>,
    /// New effect intents.
    pub intents: BTreeMap<String, Value>,
}

/// Independent reference database for one or more projection generations.
#[derive(Default)]
pub struct MemoryProjectionState {
    state: Mutex<State>,
}

impl MemoryProjectionState {
    /// Read `keys` and their versions.
    pub fn read(&self, keys: &[String]) -> Read {
        let state = lock(&self.state);
        let mut read = Read::default();
        for key in keys {
            let found = state.records.get(key);
            read.versions
                .insert(key.clone(), found.map_or(0, |(version, _)| *version));
            if let Some((_, value)) = found {
                read.records.insert(key.clone(), value.clone());
            }
        }
        read
    }

    /// Apply `writes` atomically if every record in `versions` is unchanged since it was read; otherwise a
    /// `concurrency` error and no change. Every insert and update must name a key the transaction read.
    pub fn commit(&self, versions: &BTreeMap<String, u64>, writes: &Writes) -> Result<(), Error> {
        let mut state = lock(&self.state);
        let current = |key: &str| state.records.get(key).map_or(0, |(version, _)| *version);
        if let Some(key) = versions
            .iter()
            .find_map(|(key, version)| (current(key) != *version).then_some(key))
        {
            return Err(Error::new(
                ErrorCode::Concurrency,
                format_args!("projection record {key} changed since this transaction read it"),
            ));
        }
        let unread = writes
            .inserts
            .keys()
            .chain(writes.updates.keys())
            .find(|key| !versions.contains_key(*key));
        if let Some(key) = unread {
            return Err(Error::bad_handle(format_args!(
                "projection write of unread record {key}"
            )));
        }
        if writes.inserts.keys().any(|key| current(key) != 0) || writes.updates.keys().any(|key| current(key) == 0) {
            return Err(Error::bad_handle(
                "projection insert of a present record or update of an absent one",
            ));
        }
        state.version += 1;
        let version = state.version;
        for (key, value) in writes.inserts.iter().chain(&writes.updates) {
            state.records.insert(key.clone(), (version, value.clone()));
        }
        state
            .intents
            .extend(writes.intents.iter().map(|(key, value)| (key.clone(), value.clone())));
        Ok(())
    }

    /// Up to `limit` intents of `generation` its manifest at `manifest_key` lets deliver: each
    /// partition's oldest `(offset, ordinal)` first, partitions taking turns across pages, as Postgres pages them.
    pub fn unpublished(&self, manifest_key: &str, generation: &str, limit: usize) -> Result<Vec<Value>, Error> {
        let mut state = lock(&self.state);
        let delivery_key = (manifest_key.to_owned(), generation.to_owned());
        let after = state.delivery.get(&delivery_key).copied().unwrap_or(-1);
        let Some((_, manifest)) = state.records.get(manifest_key) else {
            return Ok(Vec::new());
        };
        let mut ready = Vec::new();
        for intent in state.intents.values() {
            if of(intent, generation) && eligible(manifest, intent)? {
                ready.push((position(intent, "offset"), position(intent, "ordinal"), intent));
            }
        }
        ready.sort_by_key(|(offset, ordinal, _)| (*offset, *ordinal));
        let mut ranks = BTreeMap::<i64, usize>::new();
        let mut ranked: Vec<(usize, i64, &Value)> = ready
            .into_iter()
            .map(|(_, _, intent)| {
                let partition = position(intent, "partition");
                let rank = ranks.entry(partition).or_default();
                *rank += 1;
                (*rank, partition, intent)
            })
            .collect();
        ranked.sort_by_key(|(rank, partition, _)| (*rank, *partition <= after, *partition));
        let page: Vec<Value> = ranked
            .into_iter()
            .take(limit)
            .map(|(_, _, intent)| intent.clone())
            .collect();
        if let Some(last) = page.last() {
            state.delivery.insert(delivery_key, position(last, "partition"));
        }
        Ok(page)
    }

    /// Delete each intent at `keys` that is still eligible; return how many. An unknown or ineligible one
    /// stays, as a no-op.
    pub fn mark_published(&self, manifest_key: &str, generation: &str, keys: &[String]) -> Result<usize, Error> {
        let mut state = lock(&self.state);
        let Some((_, manifest)) = state.records.get(manifest_key).cloned() else {
            return Ok(0);
        };
        let mut gone = Vec::new();
        for key in keys {
            if let Some(intent) = state.intents.get(key) {
                if of(intent, generation) && eligible(&manifest, intent)? {
                    gone.push(key);
                }
            }
        }
        for key in &gone {
            state.intents.remove(*key);
        }
        Ok(gone.len())
    }

    /// Delete up to `limit` intents of an active generation that a cutover made permanently ineligible;
    /// return how many.
    pub fn prune(&self, manifest_key: &str, generation: &str, limit: usize) -> Result<usize, Error> {
        let mut state = lock(&self.state);
        let Some((_, manifest)) = state.records.get(manifest_key).cloned() else {
            return Ok(0);
        };
        if manifest.get("active").and_then(Value::as_bool) != Some(true) {
            return Ok(0);
        }
        let mut gone = Vec::new();
        for (key, intent) in &state.intents {
            if gone.len() == limit {
                break;
            }
            if of(intent, generation) && !eligible(&manifest, intent)? {
                gone.push(key.clone());
            }
        }
        for key in &gone {
            state.intents.remove(key);
        }
        Ok(gone.len())
    }
}

fn of(intent: &Value, generation: &str) -> bool {
    intent.get("generation").and_then(Value::as_str) == Some(generation)
}

/// An intent's integer `field`; `eligible` has already refused an intent without its partition and offset.
fn position(intent: &Value, field: &str) -> i64 {
    intent.get(field).and_then(Value::as_i64).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing, reason = "test assertions")]
    use super::*;
    use edax_cqrs::projection::{self, Contract, Request};
    use serde_json::json;

    fn contract() -> Contract {
        serde_json::from_value(json!({"generation":"account-balances-v1","source_identity":"cluster-42/topic-81","source_history":"no-rollback-policy-1","topic":"account-events","reducer_id":"balance-v1","envelope_id":"event-v1","keying_id":"account-v1","effect_namespace":"account-effects-v1","starts":{"0":0,"1":0}})).unwrap()
    }

    fn proof() -> Value {
        json!({"identity":"cluster-42/topic-81","history":"no-rollback-policy-1","beginning":0,"end":100})
    }

    fn request(operation: Value) -> Request {
        serde_json::from_value(json!({"contract": contract(), "operation": operation})).unwrap()
    }

    /// One transaction, as a host session runs it: read, let the kernel decide, commit.
    fn run(store: &MemoryProjectionState, operation: Value, now: i64) -> Result<Value, Error> {
        let request = request(operation);
        let read = store.read(&projection::keys(&request)?);
        let plan = projection::transition(&request, &read.records, now)?;
        let writes = Writes {
            inserts: plan.inserts,
            updates: plan.updates,
            intents: plan.intents,
        };
        store.commit(&read.versions, &writes)?;
        Ok(plan.result)
    }

    /// Commit one credit per offset on `aggregate`, each with a receipt intent, and return the lease.
    fn credit(store: &MemoryProjectionState, lease: Value, aggregate: &str, offsets: &[i64]) -> Value {
        let inputs: Vec<Value> = offsets
            .iter()
            .zip(1..)
            .map(|(offset, sequence)| {
                json!({"offset": offset, "event": {"tenant":"bank-eu","aggregate":aggregate,"event_id":format!("{aggregate}-credit-{sequence}"),"sequence":sequence,"event_type":"CreditPosted","schema_version":"1","payload":{"currency":"EUR","amount":"12.50"},"metadata":{"correlation_id":"payment-99"}}})
            })
            .collect();
        let reduced: Vec<Value> = inputs
            .iter()
            .map(|_| json!({"state":{"balance":"12.50","currency":"EUR"},"intents":[{"key":"receipt","destination":"email","payload":{"account":aggregate}}]}))
            .collect();
        run(
            store,
            json!({"op":"apply_batch","lease":lease,"inputs":inputs,"proof":proof(),"reduced":reduced}),
            20,
        )
        .unwrap()
    }

    fn offsets(intents: &[Value]) -> Vec<(i64, i64)> {
        intents
            .iter()
            .map(|i| (position(i, "partition"), position(i, "offset")))
            .collect()
    }

    #[test]
    fn racing_transactions_leave_one_winner() {
        let store = MemoryProjectionState::default();
        run(&store, json!({"op":"initialize"}), 0).unwrap();
        let acquire =
            |owner: &str| request(json!({"op":"acquire","partition":0,"owner":owner,"lease_ms":1000,"proof":proof()}));
        // Both read the partition unowned before either commits.
        let (a, b) = (acquire("worker-a"), acquire("worker-b"));
        let read_a = store.read(&projection::keys(&a).unwrap());
        let read_b = store.read(&projection::keys(&b).unwrap());
        let plan = |request: &Request, read: &Read| {
            let plan = projection::transition(request, &read.records, 10).unwrap();
            Writes {
                inserts: plan.inserts,
                updates: plan.updates,
                intents: plan.intents,
            }
        };
        let (writes_a, writes_b) = (plan(&a, &read_a), plan(&b, &read_b));
        store.commit(&read_a.versions, &writes_a).unwrap();
        let lost = store.commit(&read_b.versions, &writes_b).unwrap_err();
        assert_eq!(lost.code, ErrorCode::Concurrency);
        // The loser's retry reads the winner's lease and waits for it.
        let retried = run(
            &store,
            json!({"op":"acquire","partition":0,"owner":"worker-b","lease_ms":1000,"proof":proof()}),
            11,
        );
        assert_eq!(retried.unwrap_err().code, ErrorCode::InProgress);
    }

    #[test]
    fn writes_must_match_what_the_transaction_read() {
        let store = MemoryProjectionState::default();
        let read = store.read(&["a".to_owned()]);
        let update = Writes {
            updates: BTreeMap::from([("a".to_owned(), json!(1))]),
            ..Writes::default()
        };
        assert_eq!(
            store.commit(&read.versions, &update).unwrap_err().code,
            ErrorCode::BadHandle
        );
        let unread = Writes {
            inserts: BTreeMap::from([("b".to_owned(), json!(1))]),
            ..Writes::default()
        };
        assert_eq!(
            store.commit(&read.versions, &unread).unwrap_err().code,
            ErrorCode::BadHandle
        );
        let insert = Writes {
            inserts: BTreeMap::from([("a".to_owned(), json!(1))]),
            ..Writes::default()
        };
        store.commit(&read.versions, &insert).unwrap();
        assert_eq!(store.read(&["a".to_owned()]).records["a"], json!(1));
    }

    #[test]
    fn delivery_pages_partitions_in_turn_and_only_eligible_intents() {
        let store = MemoryProjectionState::default();
        let c = contract();
        let (manifest, generation) = (c.manifest_key().unwrap(), c.generation.clone());
        run(&store, json!({"op":"initialize"}), 0).unwrap();
        let lease = |partition: i64| {
            run(
                &store,
                json!({"op":"acquire","partition":partition,"owner":"worker-a","lease_ms":100_000,"proof":proof()}),
                10,
            )
            .unwrap()
        };
        let (zero, one) = (lease(0), lease(1));
        credit(&store, zero, "account-42", &[3, 5, 7]);
        credit(&store, one, "account-77", &[2]);
        // An inactive generation delivers nothing.
        assert!(store.unpublished(&manifest, &generation, 10).unwrap().is_empty());
        let cutover = json!({"source_identity":c.source_identity,"source_history":c.source_history,"topic":c.topic,"cutoffs":{"0":4,"1":0}});
        run(&store, json!({"op":"activate","cutover":cutover}), 30).unwrap();

        let page = store.unpublished(&manifest, &generation, 10).unwrap();
        assert_eq!(offsets(&page), [(0, 5), (1, 2), (0, 7)]);
        assert_eq!(
            offsets(&store.unpublished(&manifest, &generation, 1).unwrap()),
            [(1, 2)]
        );
        assert!(store
            .unpublished(&manifest, "another-generation", 10)
            .unwrap()
            .is_empty());

        let key = |offset: i64| {
            let intent = store
                .state
                .lock()
                .unwrap()
                .intents
                .iter()
                .find(|(_, i)| position(i, "offset") == offset)
                .map(|(k, _)| k.clone());
            intent.unwrap()
        };
        // Offset 3 sits below the cutoff: marking it is a no-op, and pruning removes it.
        let (below, delivered) = (key(3), key(2));
        assert_eq!(
            store
                .mark_published(&manifest, &generation, &[below, delivered, "unknown".into()])
                .unwrap(),
            1
        );
        assert_eq!(
            offsets(&store.unpublished(&manifest, &generation, 10).unwrap()),
            [(0, 5), (0, 7)]
        );
        assert_eq!(store.prune(&manifest, &generation, 0).unwrap(), 0);
        assert_eq!(store.prune(&manifest, &generation, 10).unwrap(), 1);
        assert_eq!(store.prune(&manifest, &generation, 10).unwrap(), 0);
        assert_eq!(store.state.lock().unwrap().intents.len(), 2);
    }

    #[test]
    fn short_delivery_pages_keep_their_turn_while_low_partitions_refill() {
        let store = MemoryProjectionState::default();
        let c = contract();
        let manifest = c.manifest_key().unwrap();
        run(&store, json!({"op":"initialize"}), 0).unwrap();
        run(&store, json!({"op":"activate","cutover":{"source_identity":c.source_identity,"source_history":c.source_history,"topic":c.topic,"cutoffs":{"0":0,"1":0}}}), 1).unwrap();
        let acquire = |partition| {
            run(
                &store,
                json!({"op":"acquire","partition":partition,"owner":"worker","lease_ms":100_000,"proof":proof()}),
                10,
            )
            .unwrap()
        };
        let mut zero = acquire(0);
        let one = acquire(1);
        credit(&store, one, "old-high-partition", &[0]);
        for offset in 0..4 {
            zero = credit(&store, zero, &format!("low-{offset}"), &[offset]);
            let page = store.unpublished(&manifest, &c.generation, 1).unwrap();
            assert_eq!(position(&page[0], "partition"), offset % 2);
            // Leave partition 1 unacknowledged: it remains retryable without blocking partition 0.
            if offset % 2 == 0 {
                store
                    .mark_published(
                        &manifest,
                        &c.generation,
                        &[c.intent_key(page[0]["id"].as_str().unwrap()).unwrap()],
                    )
                    .unwrap();
            }
        }
        assert!(store.unpublished(&manifest, &c.generation, 0).unwrap().is_empty());
        assert_eq!(
            position(&store.unpublished(&manifest, &c.generation, 1).unwrap()[0], "partition"),
            0
        );
    }

    #[test]
    fn absent_manifest_delivers_nothing() {
        let store = MemoryProjectionState::default();
        assert!(store.unpublished("missing", "g", 10).unwrap().is_empty());
        assert_eq!(store.mark_published("missing", "g", &["k".into()]).unwrap(), 0);
        assert_eq!(store.prune("missing", "g", 10).unwrap(), 0);
    }
}

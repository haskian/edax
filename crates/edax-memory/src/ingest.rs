//! Atomic reference storage for append and full-state ingestion.

use std::collections::BTreeMap;
use std::sync::Mutex;

use edax_core::{lock, Error};
use edax_cqrs::ingest::{self, Contract, Input, Plan, Rejection, Stored};

#[derive(Default)]
struct State {
    contracts: BTreeMap<String, Contract>,
    rows: BTreeMap<String, Stored>,
    rejected: BTreeMap<String, Rejection>,
}

/// Independent reference database. Keep this handle to preserve outcomes across runner restarts.
#[derive(Default)]
pub struct MemoryIngest {
    state: Mutex<State>,
}

impl MemoryIngest {
    /// Decide and apply a whole batch under one lock.
    pub fn commit(&self, contract: &Contract, inputs: &[Input]) -> Result<Plan, Error> {
        let mut state = lock(&self.state);
        ingest::validate_inputs(contract, inputs)?;
        ingest::bind_contract(state.contracts.get(&contract.generation), contract)?;
        let result = ingest::plan(&contract.mode, inputs, &state.rows, &state.rejected)?;
        state.contracts.insert(contract.generation.clone(), contract.clone());
        state.rows.extend(result.writes.clone());
        for (key, rejection) in &result.rejections {
            state.rejected.entry(key.clone()).or_insert_with(|| rejection.clone());
        }
        Ok(result)
    }

    /// A committed row, including its deletion marker and permanent stream owner.
    pub fn get(&self, key: &str) -> Option<Stored> {
        lock(&self.state).rows.get(key).cloned()
    }

    /// Unpublished rejections for one target prefix, in source-key order.
    pub fn pending(&self, prefix: &str, limit: usize) -> BTreeMap<String, Rejection> {
        lock(&self.state)
            .rejected
            .iter()
            .filter(|(key, row)| key.starts_with(prefix) && !row.published)
            .take(limit)
            .map(|(key, row)| (key.clone(), row.clone()))
            .collect()
    }

    /// Record a confirmed publication without accepting another record at the source coordinate.
    pub fn published(&self, source: &str, fingerprint: &str) -> Result<(), Error> {
        let mut state = lock(&self.state);
        let row = state
            .rejected
            .get_mut(source)
            .ok_or_else(|| Error::bad_handle("unknown ingestion rejection"))?;
        if row.fingerprint != fingerprint {
            return Err(Error::bad_handle("ingestion rejection fingerprint changed"));
        }
        row.published = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn contract_and_inputs_bind_atomically_inside_the_kernel() {
        let database = MemoryIngest::default();
        let contract = Contract {
            mode: "append".into(),
            generation: "orders-v1".into(),
            source_incarnation: "cluster-a".into(),
            mapper_id: "orders-v1".into(),
        };
        let original = Input {
            source: ingest::source_key("orders-v1", "cluster-a", "orders", 0, 1).unwrap(),
            key: edax_cqrs::namespace::key("orders-v1", r#"["append",null,"order-opened-1"]"#).unwrap(),
            stream: "order-42".into(),
            event_id: "order-opened-1".into(),
            sequence: Some(1),
            content: json!({"status":"open"}),
            value: Some(json!({"id":"order-42","status":"open"})),
            raw_fingerprint: None,
            raw: "order-opened-1".into(),
            failure: None,
        };
        assert_eq!(
            database
                .commit(&contract, std::slice::from_ref(&original))
                .unwrap()
                .outcomes,
            ["accepted"]
        );
        assert!(database
            .commit(
                &Contract {
                    mapper_id: "changed".into(),
                    ..contract.clone()
                },
                &[]
            )
            .is_err());
        assert!(database
            .commit(
                &Contract {
                    generation: "other".into(),
                    ..contract.clone()
                },
                std::slice::from_ref(&original)
            )
            .is_err());
        assert_eq!(database.get(&original.key).unwrap().value, original.value);
        assert!(database.commit(&contract, &[original]).unwrap().writes.is_empty());
    }
}

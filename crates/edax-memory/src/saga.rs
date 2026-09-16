//! The saga store view: per-correlation state with optimistic concurrency.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use edax_core::{lock, AdapterError};
use serde_json::Value;

use crate::tx::{held_by_other, store_view, versioned_row, Committed, MemoryState, MemoryTransactor, WriteSet};

/// In-memory saga instance store for development and tests: per-correlation state with optimistic
/// concurrency on the version. State crosses as JSON text and may be any JSON value (a saga that keeps
/// `null` is legitimate). The rule is written once here so every SDK's saga runner meets the same
/// referee: a `save` or `delete` at a stale version is a conflict, and a `delete` of an absent
/// instance is a no-op — the runner's redelivery of a completed instance must not fail. A write under
/// an open token is staged until the transactor commits. Not for production: a durable store enlists
/// the write in the events' transaction.
#[derive(Clone)]
pub struct MemorySagaStore {
    pub(crate) state: Arc<Mutex<MemoryState>>,
}

store_view!(MemorySagaStore, saga_store);

/// Refuse a write to an instance another open transaction has written: the holder's commit would
/// overwrite it. A staged delete reports version 0, the version an absent instance has.
fn refuse_held_saga(others: &HashMap<i64, WriteSet>, id: &str, expected: i64) -> Result<(), AdapterError> {
    match held_by_other(others, |set| set.sagas.get(id)) {
        Some(theirs) => Err(AdapterError::Concurrency { expected, stored: theirs.as_ref().map_or(0, |(v, _)| *v) }),
        None => Ok(()),
    }
}

/// The saga row as `tx` sees it: staged in the transaction (`Some(None)` is a staged delete), else
/// committed.
fn saga_row<'a>(committed: &'a Committed, own: Option<&'a WriteSet>, id: &str) -> Option<&'a (i64, Value)> {
    match own.and_then(|set| set.sagas.get(id)) {
        Some(staged) => staged.as_ref(),
        None => committed.sagas.get(id),
    }
}

impl MemorySagaStore {
    /// The stored instance for `id` as `tx` sees it, as JSON `{"id","version","state"}`, or `None`.
    pub fn load(&self, tx: i64, id: &str) -> Result<Option<String>, AdapterError> {
        lock(&self.state).read(tx, |committed, own| {
            let Some((version, state)) = saga_row(committed, own, id) else { return Ok(None) };
            versioned_row(id, *version, "state", state.clone()).map(Some)
        })?
    }

    /// Persist `state` at `version + 1` if the version `tx` sees still equals `version` (an absent
    /// instance is at version 0); returns the new version. A version mismatch is a conflict error, as
    /// is an id another open transaction has written and not yet committed; malformed state JSON is a
    /// codec error and writes nothing.
    pub fn save(&self, tx: i64, id: &str, version: i64, state_json: &str) -> Result<i64, AdapterError> {
        let state: Value = serde_json::from_str(state_json).map_err(AdapterError::codec)?;
        lock(&self.state).write(tx, |committed, others, own| {
            refuse_held_saga(others, id, version)?;
            let stored = saga_row(committed, Some(own), id).map_or(0, |(v, _)| *v);
            if stored != version {
                return Err(AdapterError::Concurrency { expected: version, stored });
            }
            let next = version.saturating_add(1); // saturating so no arithmetic under the lock can panic
            own.sagas.insert(id.to_owned(), Some((next, state)));
            Ok(next)
        })
    }

    /// Remove the instance at `id` if the version `tx` sees still equals `version`; a version
    /// mismatch is a conflict error. An absent instance is a no-op at any version: the delete is
    /// idempotent, so a redelivered completion after a lost response converges instead of failing.
    pub fn delete(&self, tx: i64, id: &str, version: i64) -> Result<(), AdapterError> {
        lock(&self.state).write(tx, |committed, others, own| {
            refuse_held_saga(others, id, version)?;
            let Some((stored, _)) = saga_row(committed, Some(own), id) else { return Ok(()) };
            if *stored != version {
                return Err(AdapterError::Concurrency { expected: version, stored: *stored });
            }
            own.sagas.insert(id.to_owned(), None);
            Ok(())
        })
    }
}

#[cfg(test)]
// A test asserts on the value it just built; a panic there is the failure report.
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::testkit::wire;
    use crate::tx::NO_TX;
    use serde_json::json;
    /// A saga store holding one open instance at version 1.
    fn seeded_saga_store() -> MemorySagaStore {
        let store = MemorySagaStore::new();
        store.save(NO_TX, "order-1", 0, r#"{"status":"awaiting_payment","total":1200}"#).expect("seed saves");
        store
    }

    #[test]
    fn saga_save_branches() {
        struct Case {
            name: &'static str,
            id: &'static str,
            version: i64,
            state: &'static str,
            expected: Result<i64, &'static str>,
            /// The stored `(version, status)` afterwards, or `None` for no row.
            stored: Option<(i64, &'static str)>,
        }
        let cases = [
            Case {
                name: "matching_version_bumps_to_next",
                id: "order-1",
                version: 1,
                state: r#"{"status":"shipped","total":1200}"#,
                expected: Ok(2),
                stored: Some((2, "shipped")),
            },
            Case {
                name: "stale_version_conflicts_and_keeps_the_stored_state",
                id: "order-1",
                version: 0,
                state: r#"{"status":"shipped","total":1200}"#,
                expected: Err("concurrency: expected 0, stored 1"),
                stored: Some((1, "awaiting_payment")),
            },
            Case {
                // An absent instance reads as version 0, so a first save at any other version
                // conflicts instead of creating the row.
                name: "absent_instance_at_a_nonzero_version_conflicts",
                id: "order-404",
                version: 1,
                state: r#"{"status":"shipped","total":1200}"#,
                expected: Err("concurrency: expected 1, stored 0"),
                stored: None,
            },
            Case {
                name: "absent_instance_at_version_zero_is_created",
                id: "order-404",
                version: 0,
                state: r#"{"status":"awaiting_payment","total":900}"#,
                expected: Ok(1),
                stored: Some((1, "awaiting_payment")),
            },
            Case {
                name: "malformed_json_is_a_codec_error_and_writes_nothing",
                id: "order-1",
                version: 1,
                state: r#"{"status":"shipped","#,
                expected: Err("codec: EOF while parsing a value at line 1 column 20"),
                stored: Some((1, "awaiting_payment")),
            },
        ];
        for case in cases {
            let store = seeded_saga_store();
            let got = store.save(NO_TX, case.id, case.version, case.state);
            assert_eq!(wire(got), case.expected.map_err(str::to_owned), "{}", case.name);
            let row = store.load(NO_TX, case.id).unwrap().map(|raw| {
                let v: Value = serde_json::from_str(&raw).unwrap();
                (v["version"].as_i64().unwrap(), v["state"]["status"].as_str().unwrap().to_owned())
            });
            assert_eq!(row, case.stored.map(|(v, s)| (v, s.to_owned())), "{} stored row", case.name);
        }
    }

    #[test]
    fn saga_delete_branches() {
        struct Case {
            name: &'static str,
            id: &'static str,
            version: i64,
            expected: Result<(), &'static str>,
            /// Whether `order-1` still exists afterwards.
            seed_remains: bool,
        }
        let cases = [
            Case { name: "matching_version_removes", id: "order-1", version: 1, expected: Ok(()), seed_remains: false },
            Case {
                name: "stale_version_conflicts_and_keeps_the_instance",
                id: "order-1",
                version: 0,
                expected: Err("concurrency: expected 0, stored 1"),
                seed_remains: true,
            },
            Case {
                // Idempotent: a redelivered completion after a lost response finds nothing and
                // must converge, not fail, whatever version it carries.
                name: "absent_instance_is_a_noop_at_version_zero",
                id: "order-404",
                version: 0,
                expected: Ok(()),
                seed_remains: true,
            },
            Case {
                name: "absent_instance_is_a_noop_at_any_version",
                id: "order-404",
                version: 7,
                expected: Ok(()),
                seed_remains: true,
            },
        ];
        for case in cases {
            let store = seeded_saga_store();
            let got = store.delete(NO_TX, case.id, case.version);
            assert_eq!(wire(got), case.expected.map_err(str::to_owned), "{}", case.name);
            assert_eq!(store.load(NO_TX, "order-1").unwrap().is_some(), case.seed_remains, "{} seed row", case.name);
            assert_eq!(store.load(NO_TX, "order-404").unwrap(), None, "{} never creates a row", case.name);
        }
    }

    #[test]
    fn saga_load_returns_the_versioned_instance_or_none() {
        let store = seeded_saga_store();
        let instance: Value = serde_json::from_str(&store.load(NO_TX, "order-1").unwrap().unwrap()).unwrap();
        assert_eq!(
            instance,
            json!({"id": "order-1", "version": 1, "state": {"status": "awaiting_payment", "total": 1200}})
        );
        assert_eq!(store.load(NO_TX, "order-404").unwrap(), None);

        // Any JSON value is a state: a saga that keeps `null` is distinct from a completed one.
        store.save(NO_TX, "order-2", 0, "null").unwrap();
        let kept: Value = serde_json::from_str(&store.load(NO_TX, "order-2").unwrap().unwrap()).unwrap();
        assert_eq!(kept, json!({"id": "order-2", "version": 1, "state": null}));
    }
}

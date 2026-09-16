//! The projection store view: read-model documents by id, with the query deferred to the kernel.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use edax_core::{lock, AdapterError};
use serde_json::Value;

use crate::tx::{held_by_other, store_view, versioned_row, Document, MemoryState, MemoryTransactor};

/// In-memory `ProjectionStore` implementation for development and tests: read-model documents keyed
/// by id with optimistic concurrency, querying deferred to the kernel. Documents cross as JSON text.
/// Iteration is id-ordered so an un-sorted query is deterministic across runs and languages; a
/// sorted query is pinned by the kernel either way. A save under an open token is staged until the
/// transactor commits. Not for production.
#[derive(Clone)]
pub struct MemoryProjectionStore {
    pub(crate) state: Arc<Mutex<MemoryState>>,
}

store_view!(MemoryProjectionStore, projection_store);

impl MemoryProjectionStore {
    /// The stored entity for `id` as `tx` sees it, as JSON `{"id","version","document"}`, or `None`.
    pub fn get(&self, tx: i64, id: &str) -> Result<Option<String>, AdapterError> {
        lock(&self.state).read(tx, |committed, own| {
            let row = own.and_then(|set| set.projections.get(id)).or_else(|| committed.projections.get(id));
            row.map(|(version, doc)| versioned_row(id, *version, "document", Value::Object(doc.clone()))).transpose()
        })?
    }

    /// Persist `document` at `version + 1` if the version `tx` sees still equals `version`; returns the
    /// new version. A version mismatch is a conflict error, as is an id another open transaction has
    /// saved and not yet committed.
    pub fn save(&self, tx: i64, id: &str, version: i64, document_json: &str) -> Result<i64, AdapterError> {
        // Parsed as an object, not any JSON value: [`get_many`](Self::get_many) injects the row key as
        // an `"id"` field, so a scalar or array document could be saved and then never queried. Reject
        // it at the write, where the caller can still fix it.
        let doc: Document = serde_json::from_str(document_json).map_err(AdapterError::codec)?;
        lock(&self.state).write(tx, |committed, others, own| {
            if let Some((stored, _)) = held_by_other(others, |set| set.projections.get(id)) {
                return Err(AdapterError::Concurrency { expected: version, stored: *stored });
            }
            let stored = own
                .projections
                .get(id)
                .or_else(|| committed.projections.get(id))
                .map_or(0, |(v, _)| *v);
            if stored != version {
                return Err(AdapterError::Concurrency { expected: version, stored });
            }
            let next = version.saturating_add(1); // saturating so no arithmetic under the lock can panic
            own.projections.insert(id.to_owned(), (next, doc));
            Ok(next)
        })
    }

    /// Run a read query (a JSON spec) over all documents as `tx` sees them; returns the page as a JSON
    /// array. The entity id is the map key, not a document field, so it is injected as `"id"` on each
    /// document before querying — the query kernel's keyset tiebreak sorts on it, keeping paging
    /// stable under a non-unique sort key (see `query::query_json`). The id is authoritative
    /// (overwrites any `"id"`).
    pub fn get_many(&self, tx: i64, query_json: &str) -> Result<String, AdapterError> {
        let docs: Vec<Value> = lock(&self.state).read(tx, |committed, own| {
            // Overlaid into an id-ordered map, so a staged row sorts where its committed self would.
            let mut rows: BTreeMap<&str, &Document> =
                committed.projections.iter().map(|(id, (_, doc))| (id.as_str(), doc)).collect();
            for (id, (_, doc)) in own.map(|set| &set.projections).into_iter().flatten() {
                rows.insert(id.as_str(), doc);
            }
            rows.into_iter()
                .map(|(id, doc)| {
                    let mut doc = doc.clone();
                    doc.insert("id".to_owned(), Value::String(id.to_owned()));
                    Value::Object(doc)
                })
                .collect()
        })?;
        let docs_bytes = serde_json::to_vec(&docs).map_err(AdapterError::codec)?;
        // A malformed query spec (or cursor) is a marshaling failure, so it carries the `codec` code:
        // unprefixed it would fall back to the host's transient store error and be retried forever.
        let out = edax_cqrs::query_json(query_json.as_bytes(), &docs_bytes).map_err(AdapterError::codec)?;
        String::from_utf8(out).map_err(AdapterError::codec)
    }
}

#[cfg(test)]
// A test asserts on the value it just built; a panic there is the failure report.
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::testkit::wire;
    use crate::tx::NO_TX;
    use edax_core::ErrorCode;
    use serde_json::json;
    /// A projection store holding one document at version 1.
    fn seeded_projection_store() -> MemoryProjectionStore {
        let store = MemoryProjectionStore::new();
        store
            .save(NO_TX, "user-1", 0, r#"{"email":"ada@example.com","age":36,"tags":["eu","paid"]}"#)
            .expect("seed saves");
        store
    }

    #[test]
    fn save_branches() {
        struct Case {
            name: &'static str,
            version: i64,
            document: &'static str,
            expected: Result<i64, &'static str>,
        }
        let cases = [
            Case {
                name: "matching_version_bumps_to_next",
                version: 1,
                document: r#"{"email":"ada@example.com","age":37,"tags":["eu","paid"]}"#,
                expected: Ok(2),
            },
            Case {
                name: "stale_version_conflicts",
                version: 0,
                document: r#"{"email":"ada@example.com","age":37,"tags":["eu","paid"]}"#,
                expected: Err("concurrency: expected 0, stored 1"),
            },
            Case {
                name: "malformed_json_is_a_codec_error",
                version: 1,
                document: r#"{"email":"ada@example.com","#,
                expected: Err("codec: EOF while parsing a value at line 1 column 27"),
            },
            Case {
                // Rejected at the write: get_many injects the row key as "id", which needs an object.
                name: "non_object_document_is_a_codec_error",
                version: 1,
                document: r#"["ada@example.com"]"#,
                expected: Err("codec: invalid type: sequence, expected a map at line 1 column 0"),
            },
        ];

        for case in cases {
            let store = seeded_projection_store();
            let got = store.save(NO_TX, "user-1", case.version, case.document);
            assert_eq!(wire(got), case.expected.map_err(str::to_owned), "{}", case.name);

            // A rejected save leaves the stored version and document untouched.
            let stored: Value = serde_json::from_str(&store.get(NO_TX, "user-1").unwrap().unwrap()).unwrap();
            let expected_age = if case.expected.is_ok() { 37 } else { 36 };
            assert_eq!(stored["document"]["age"], expected_age, "{} document", case.name);
        }

        // An unknown id reads as version 0, so a first save at any other version conflicts instead
        // of creating the row.
        let store = seeded_projection_store();
        let got = store.save(NO_TX, "user-404", 1, r#"{"email":"grace@example.com","age":29,"tags":[]}"#);
        assert_eq!(wire(got), Err("concurrency: expected 1, stored 0".to_owned()));
        assert_eq!(store.get(NO_TX, "user-404").unwrap(), None, "a conflicting first save creates no row");
    }

    #[test]
    fn get_returns_the_versioned_entity_or_none() {
        let store = seeded_projection_store();
        let entity: Value = serde_json::from_str(&store.get(NO_TX, "user-1").unwrap().unwrap()).unwrap();
        assert_eq!(
            entity,
            json!({
                "id": "user-1",
                "version": 1,
                "document": {"email": "ada@example.com", "age": 36, "tags": ["eu", "paid"]}
            })
        );
        assert_eq!(store.get(NO_TX, "user-404").unwrap(), None);
    }

    #[test]
    fn get_many_branches() {
        let store = MemoryProjectionStore::new();
        assert_eq!(store.get_many(NO_TX, "{}").unwrap(), "[]", "empty store pages empty");

        // The row key is authoritative: it overwrites whatever "id" the document carries.
        store.save(NO_TX, "user-1", 0, r#"{"id":"stale","email":"ada@example.com","age":36}"#).unwrap();
        store.save(NO_TX, "user-2", 0, r#"{"id":"stale","email":"grace@example.com","age":29}"#).unwrap();

        let page: Vec<Value> =
            serde_json::from_str(&store.get_many(NO_TX, r#"{"sort":[{"path":"age","desc":true}]}"#).unwrap()).unwrap();
        assert_eq!(page.iter().map(|d| d["id"].as_str().unwrap()).collect::<Vec<_>>(), ["user-1", "user-2"]);

        // Selection and paging are the kernel's, not the store's.
        let spec = r#"{"filter":{"compare":{"path":"age","op":"lt","value":30}},"limit":5}"#;
        let filtered: Vec<Value> = serde_json::from_str(&store.get_many(NO_TX, spec).unwrap()).unwrap();
        assert_eq!(filtered.iter().map(|d| d["id"].as_str().unwrap()).collect::<Vec<_>>(), ["user-2"]);

        // A malformed spec is terminal, so it carries the `codec` code rather than the transient default.
        let err = store.get_many(NO_TX, "{not a query}").unwrap_err();
        assert_eq!(err.code(), ErrorCode::Codec, "{err}");
        assert!(err.to_string().starts_with("codec: "), "{err}");
    }

    /// Why `get_many` injects the row key: every row here ties on the sort key, so only the `id`
    /// the store injects breaks the tie, and keyset paging drops and repeats nothing. Without the
    /// injection the tiebreak resolves to null on every row and a cursor page would be arbitrary.
    #[test]
    fn get_many_pages_by_cursor_over_a_tied_sort_key() {
        let store = MemoryProjectionStore::new();
        let ids = ["user-1", "user-2", "user-3"];
        for id in ids {
            store.save(NO_TX, id, 0, r#"{"email":"ada@example.com","age":36,"tags":["eu","paid"]}"#).expect("seed saves");
        }

        let mut seen: Vec<String> = Vec::new();
        let mut after: Option<String> = None;
        for _ in ids {
            let spec = match &after {
                Some(cursor) => json!({"sort": [{"path": "age"}], "limit": 1, "after": cursor}),
                None => json!({"sort": [{"path": "age"}], "limit": 1}),
            };
            let page: Vec<Value> = serde_json::from_str(&store.get_many(NO_TX, &spec.to_string()).unwrap()).unwrap();
            let row = page.first().expect("one row per page").clone();
            seen.push(row["id"].as_str().expect("the injected row key").to_owned());
            let cursor = edax_cqrs::encode_cursor_json(row.to_string().as_bytes(), br#"[{"path":"age"}]"#).unwrap();
            after = Some(String::from_utf8(cursor).expect("a cursor is base64 text"));
        }
        assert_eq!(seen, ids, "each page advances by exactly one row");
    }
}

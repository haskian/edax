//! The read-side seam: read-model documents keyed by id, served through the filter and query kernels.

use serde_json::Value;

use crate::Error;

/// The read query shape, its ordering key and the page it answers with, re-exported from the kernel;
/// see [`edax_cqrs::Query`] for wire rules.
pub use edax_cqrs::{Page, Query, SortKey};

/// A read model at a point in time: the queryable `document` and the `version` it was loaded at (`0` if never
/// saved). A save persists at `version + 1` only if the store is still at `version`, then advances it.
#[derive(Clone, Debug)]
pub struct Entity {
    pub id: String,
    pub version: u64,
    pub document: Value,
}

/// Stores read-model documents keyed by id, with optimistic concurrency and kernel-defined querying: the read
/// side's analogue of [`EventStore`](crate::EventStore). A backend serves [`ProjectionStore::get_many`] by
/// pushing filter, sort and pagination down to its native query dialect. No durable store ships here.
pub trait ProjectionStore {
    /// Backend transaction handle, the same one [`Transactor`](crate::Transactor) hands out, so a projection
    /// write enlists in the events' transaction and the two commit together. Use `()` if the store needs none.
    type Tx;

    /// The current entity, or `None` if absent.
    fn get(&self, tx: &mut Self::Tx, id: &str) -> Result<Option<Entity>, Error>;

    /// Persist `entity.document` at `entity.version + 1` if the stored version still equals `entity.version`;
    /// return [`Error::Concurrency`] otherwise. On success `entity.version` advances to the stored version.
    fn save(&self, tx: &mut Self::Tx, entity: &mut Entity) -> Result<(), Error>;

    /// The page of documents `query` selects (see [`Query`]), with the cursor that resumes after it:
    /// pass [`Page::next`] back as [`Query::after`] under the same sort. A backend that pages natively
    /// mints `next` with [`edax_cqrs::encode_cursor`] and reads `after` with
    /// [`edax_cqrs::decode_cursor`], so its cursors are every other backend's.
    fn get_many(&self, tx: &mut Self::Tx, query: &Query) -> Result<Page, Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryProjectionStore, MemoryTx};
    use crate::ErrorCode;
    use serde_json::json;

    fn entity(id: &str, doc: Value) -> Entity {
        Entity { id: id.into(), version: 0, document: doc }
    }

    /// The SDK does not own a query shape: [`Query`] *is* the kernel's, so the spec the SDK
    /// serializes is by construction the one the kernel parses. Driven by
    /// `testdata/golden/query.json`, the cross-language contract: every golden spec parses into this
    /// shape, survives a serialize/deserialize round-trip field-for-field, and its re-serialized
    /// bytes page exactly as the golden says. Re-declaring the shape here would fail this test.
    #[test]
    fn every_golden_query_round_trips_through_the_kernel_shape() {
        let cases = edax_golden::read("query.json");
        // The round-trip only proves anything for fields the golden actually sets.
        for field in ["filter", "sort", "offset", "limit", "after"] {
            assert!(cases.iter().any(|c| c["query"].get(field).is_some()), "no golden case sets {field}");
        }
        for (i, case) in cases.iter().enumerate() {
            if case["expected_page"].is_null() {
                continue; // a refused spec: the kernel's error rows, replayed by the binding suites
            }
            let spec: Query = serde_json::from_value(case["query"].clone())
                .unwrap_or_else(|e| panic!("case {i}: the golden spec does not parse: {e}"));
            let bytes = serde_json::to_vec(&spec).expect("a Query is always serializable");
            let reparsed: Query = serde_json::from_slice(&bytes)
                .unwrap_or_else(|e| panic!("case {i}: the serialized spec does not parse back: {e}"));
            assert_eq!(reparsed, spec, "case {i}: a field was lost between serialize and deserialize");

            let documents = serde_json::to_vec(&case["documents"]).expect("golden documents are JSON");
            let page = edax_cqrs::query_json(&bytes, &documents)
                .unwrap_or_else(|e| panic!("case {i}: the kernel rejected the spec: {e}"));
            let page: Value = serde_json::from_str(&page).expect("a page is JSON");
            assert_eq!(page, case["expected_page"], "case {i}");
        }
    }

    #[test]
    fn save_versions_then_get() {
        let store = MemoryProjectionStore::new();
        let mut tx = MemoryTx::auto_commit();
        let mut e = entity("u1", json!({"id": "u1", "name": "ada", "age": 36}));
        store.save(&mut tx, &mut e).unwrap();
        assert_eq!(e.version, 1); // advanced in place
        let got = store.get(&mut tx, "u1").unwrap().unwrap();
        assert_eq!(got.version, 1);
        assert_eq!(got.document["name"], json!("ada"));
    }

    #[test]
    fn stale_version_conflicts() {
        let store = MemoryProjectionStore::new();
        let mut tx = MemoryTx::auto_commit();
        let mut e = entity("u1", json!({"id": "u1", "n": 1}));
        store.save(&mut tx, &mut e).unwrap(); // version 1

        let mut other = store.get(&mut tx, "u1").unwrap().unwrap();
        other.document = json!({"id": "u1", "n": 2});
        store.save(&mut tx, &mut other).unwrap(); // store -> version 2

        e.document = json!({"id": "u1", "n": 3});
        let err = store.save(&mut tx, &mut e).unwrap_err(); // e.version is still 1, store at 2
        assert_eq!(err, Error::concurrency(1, 2));
    }

    /// Four read models, two of which tie on `age` — so a page boundary can land inside the tie.
    fn seeded_store() -> MemoryProjectionStore {
        let store = MemoryProjectionStore::new();
        for (i, &(name, age)) in [("ada", 36), ("bob", 41), ("cal", 36), ("dot", 29)].iter().enumerate() {
            let mut e = entity(&format!("u{i}"), json!({"name": name, "age": age}));
            store.save(&mut MemoryTx::auto_commit(), &mut e).unwrap();
        }
        store
    }

    fn field<'a>(page: &'a Page, path: &str) -> Vec<&'a str> {
        page.rows.iter().map(|d| d[path].as_str().expect("a string field")).collect()
    }

    #[test]
    fn list_filters_sorts_paginates() {
        let store = seeded_store();
        let mut tx = MemoryTx::auto_commit();

        let filter = || Some(edax_cqrs::filter::gte("age", json!(36)));
        let sort = || vec![SortKey { path: "name".into(), desc: false }];

        let page = store
            .get_many(&mut tx, &Query { filter: filter(), sort: sort(), limit: Some(2), ..Default::default() })
            .unwrap();
        assert_eq!(field(&page, "name"), ["ada", "bob"]);

        let page2 = store
            .get_many(&mut tx, &Query { filter: filter(), sort: sort(), offset: 2, ..Default::default() })
            .unwrap();
        assert_eq!(field(&page2, "name"), ["cal"]);
    }

    #[test]
    fn keyset_cursor_resumes_strictly_after_the_last_row() {
        // Paging by cursor rather than offset, across a boundary that falls *inside* a tie on the sort
        // key: the kernel's automatic `id` tiebreak is what keeps the tied rows from being dropped or
        // repeated. `model.rs` pages only by offset on the unique `id`, so it cannot see this.
        let store = seeded_store();
        let sort = vec![SortKey { path: "age".into(), desc: false }];
        let page = |after: Option<String>| {
            let q = Query { sort: sort.clone(), limit: Some(2), after, ..Default::default() };
            store.get_many(&mut MemoryTx::auto_commit(), &q).unwrap()
        };

        let first = page(None);
        assert_eq!(field(&first, "id"), ["u3", "u0"]); // dot 29, then the first age-36 row by id
        let second = page(first.next);
        assert_eq!(field(&second, "id"), ["u2", "u1"]); // the other age-36 row, then bob
        assert_eq!(second.next, None, "nothing follows the last page");

        // The cursor binds the sort it was minted under: replayed under another, it is refused.
        let minted = page(None).next;
        let other = Query { sort: vec![SortKey { path: "name".into(), desc: false }], after: minted, ..Default::default() };
        let got = store.get_many(&mut MemoryTx::auto_commit(), &other);
        assert!(matches!(&got, Err(Error { code: ErrorCode::Codec, .. })), "got {got:?}");
    }

    #[test]
    fn marshaling_failures_are_codec_not_store() {
        // The taxonomy is the contract: a marshaling failure is `Codec` (terminal, dead-lettered on the
        // first attempt), never `Store` (transient, retried until the redelivery budget runs out).
        let store = seeded_store();
        let mut tx = MemoryTx::auto_commit();

        // A scalar document has nowhere for the store to inject the row key as `"id"`, so it could be
        // saved and then never paged. Rejected at the write, while the caller can still fix it.
        let mut scalar = Entity { id: "u9".into(), version: 0, document: json!(7) };
        let got = store.save(&mut tx, &mut scalar);
        assert!(matches!(&got, Err(Error { code: ErrorCode::Codec, .. })), "got {got:?}");
        assert_eq!(scalar.version, 0, "a rejected save leaves the version alone");
        assert!(store.get(&mut tx, "u9").unwrap().is_none(), "nothing written");

        // A cursor that is not base64url of a kernel cursor.
        let q = Query {
            sort: vec![SortKey { path: "age".into(), desc: false }],
            after: Some("!! not base64 !!".into()),
            ..Default::default()
        };
        let got = store.get_many(&mut tx, &q);
        assert!(matches!(&got, Err(Error { code: ErrorCode::Codec, .. })), "got {got:?}");
    }
}

//! The projection store view: read-model documents by read model and id, with the query deferred to the
//! kernel.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use edax_core::{lock, Error};
use serde_json::{Map, Value};

use crate::tx::{store_view, MemoryState, MemoryTransactor, Tx};
use crate::versioned::Versioned;

/// A read-model document: always a JSON object, so the store can read it back with its row key as `"id"`.
pub type Document = Map<String, Value>;

/// The key of a read-model row: the read model's name, then the row id. Two read models share one
/// database, as they must for their writes to join one unit of work, and one id names two rows, as the
/// Postgres projections table keys `(projection, id)`.
pub(crate) type ProjectionKey = (String, String);

/// The key of row `id` of read model `projection`.
fn key(projection: &str, id: &str) -> ProjectionKey {
    (projection.to_owned(), id.to_owned())
}

/// In-memory `ProjectionStore` implementation for development and tests: read-model documents keyed
/// by read model and id with optimistic concurrency, querying deferred to the kernel. Iteration is id-ordered so an
/// un-sorted query is deterministic across runs and languages; a sorted query is pinned by the kernel
/// either way. A save under an open token is staged until the transactor commits. Not for production.
#[derive(Clone)]
pub struct MemoryProjectionStore {
    pub(crate) state: Arc<Mutex<MemoryState>>,
}

store_view!(MemoryProjectionStore, projection_store);

impl MemoryProjectionStore {
    /// The stored document for `id` in read model `projection` and its version, as `tx` sees it, or
    /// `None`. The document carries its row key as `"id"` ([`edax_cqrs::keyed_document`]), as every row
    /// [`get_many`](Self::get_many) pages.
    pub fn get(&self, tx: Tx, projection: &str, id: &str) -> Result<Option<Versioned<Document>>, Error> {
        lock(&self.state).read(tx, |committed, own| {
            let row = committed
                .projections
                .get(own.map(|set| &set.projections), &key(projection, id));
            row.map(|row| Versioned {
                version: row.version,
                value: edax_cqrs::keyed_document(id, row.value.clone()),
            })
        })
    }

    /// Persist `document` as `id` in read model `projection` at `version + 1` if the version `tx` sees still
    /// equals `version`; returns the
    /// new version. A version mismatch is a conflict error, as is an id another open transaction has
    /// saved and not yet committed; that conflict names the committed version, since the holder's own
    /// may yet roll back. The document is an object, never a scalar or an array, which the typed
    /// [`Document`] guarantees: build it with [`edax_cqrs::document`], the kernel's rule, since a read
    /// injects the row key as its `"id"` field. A read-model name or id holding NUL is a codec error
    /// ([`edax_core::check_text`]), as in every store.
    pub fn save(&self, tx: Tx, projection: &str, id: &str, version: u64, document: Document) -> Result<u64, Error> {
        edax_core::check_text("projection", projection)?;
        edax_core::check_text("projection id", id)?;
        lock(&self.state).write(tx, |committed, others, own| {
            let key = key(projection, id);
            let others = others.values().map(|set| &set.projections);
            let next = committed.projections.next(
                others,
                &own.projections,
                &key,
                version,
                &format!("projection {id}"),
                |_, _| Ok(()),
            )?;
            own.projections.insert(
                key,
                Versioned {
                    version: next,
                    value: document,
                },
            );
            Ok(next)
        })
    }

    /// Run `query` over the documents of read model `projection` as `tx` sees them; returns the kernel's page. Each row is read as
    /// [`get`](Self::get) reads it, its row key as `"id"` ([`edax_cqrs::keyed_document`]): the keyset
    /// tiebreak sorts on it, keeping paging stable under a non-unique sort key, and every row can mint a
    /// cursor. A cursor the query cannot read is the kernel's `codec` error.
    pub fn get_many(&self, tx: Tx, projection: &str, query: &edax_cqrs::Query) -> Result<edax_cqrs::Page, Error> {
        let docs: Vec<Value> = lock(&self.state).read(tx, |committed, own| {
            // Overlaid into an id-ordered map, so a staged row sorts where its committed self would.
            let mut rows: BTreeMap<&str, &Document> = rows_of(projection, committed.projections.iter()).collect();
            rows.extend(rows_of(
                projection,
                own.map(|set| &set.projections).into_iter().flatten(),
            ));
            rows.into_iter()
                .map(|(id, doc)| Value::Object(edax_cqrs::keyed_document(id, doc.clone())))
                .collect()
        })?;
        edax_cqrs::query(query, docs)
    }
}

/// The id and document of each of `rows` that belongs to read model `projection`.
fn rows_of<'a>(
    projection: &'a str,
    rows: impl Iterator<Item = (&'a ProjectionKey, &'a Versioned<Document>)>,
) -> impl Iterator<Item = (&'a str, &'a Document)> {
    rows.filter(move |(key, _)| key.0 == projection)
        .map(|(key, row)| (key.1.as_str(), &row.value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::wire;
    use crate::tx::Tx;
    use edax_cqrs::Query;
    use serde_json::json;

    /// A document from a JSON object literal.
    fn doc(value: Value) -> Document {
        match value {
            Value::Object(map) => map,
            other => panic!("a test document is an object, got {other}"),
        }
    }

    fn query(spec: Value) -> Query {
        serde_json::from_value(spec).expect("a test query is the kernel's spec")
    }

    /// A projection store holding one document at version 1.
    fn seeded_projection_store() -> MemoryProjectionStore {
        let store = MemoryProjectionStore::new();
        store
            .save(
                Tx::auto_commit(),
                "p",
                "user-1",
                0,
                doc(json!({"email": "ada@example.com", "age": 36, "tags": ["eu", "paid"]})),
            )
            .expect("seed saves");
        store
    }

    #[test]
    fn save_branches() {
        struct Case {
            name: &'static str,
            version: u64,
            expected: Result<u64, &'static str>,
        }
        let cases = [
            Case {
                name: "matching_version_bumps_to_next",
                version: 1,
                expected: Ok(2),
            },
            Case {
                name: "stale_version_conflicts",
                version: 0,
                expected: Err("concurrency: expected 0, stored 1"),
            },
            Case {
                name: "ahead_version_conflicts",
                version: 7,
                expected: Err("concurrency: expected 7, stored 1"),
            },
        ];

        for case in cases {
            let store = seeded_projection_store();
            let got = store.save(
                Tx::auto_commit(),
                "p",
                "user-1",
                case.version,
                doc(json!({"email": "ada@example.com", "age": 37})),
            );
            assert_eq!(wire(got), case.expected.map_err(str::to_owned), "{}", case.name);

            // A rejected save leaves the stored version and document untouched.
            let stored = store.get(Tx::auto_commit(), "p", "user-1").unwrap().expect("seeded");
            let expected_age = if case.expected.is_ok() { 37 } else { 36 };
            assert_eq!(stored.value["age"], expected_age, "{} document", case.name);
        }

        // An unknown id reads as version 0, so a first save at any other version conflicts instead
        // of creating the row.
        let store = seeded_projection_store();
        let got = store.save(
            Tx::auto_commit(),
            "p",
            "user-404",
            1,
            doc(json!({"email": "grace@example.com", "age": 29})),
        );
        assert_eq!(wire(got), Err("concurrency: expected 1, stored 0".to_owned()));
        assert_eq!(
            store.get(Tx::auto_commit(), "p", "user-404").unwrap(),
            None,
            "a conflicting first save creates no row"
        );

        // The version space is u64 end to end: the last version takes no further save.
        let store = MemoryProjectionStore::new();
        lock(&store.state)
            .committed
            .projections
            .seed(key("p", "user-9"), u64::MAX, Document::new());
        let got = store.save(Tx::auto_commit(), "p", "user-9", u64::MAX, Document::new());
        assert_eq!(
            wire(got),
            Err("codec: projection user-9 at version 18446744073709551615 has no next version".to_owned())
        );
    }

    #[test]
    fn a_held_id_conflicts_naming_the_committed_version() {
        // The holder's staged version may roll back, so the conflict reports what is committed.
        let store = seeded_projection_store();
        let tr = store.transactor();
        let holder = tr.begin();
        store
            .save(holder, "p", "user-1", 1, doc(json!({"age": 37})))
            .expect("the holder saves");
        let got = store.save(Tx::auto_commit(), "p", "user-1", 2, doc(json!({"age": 38})));
        assert_eq!(wire(got), Err("concurrency: expected 2, stored 1".to_owned()));
        assert!(tr.rollback(holder));
        assert_eq!(
            store.get(Tx::auto_commit(), "p", "user-1").unwrap().map(|v| v.version),
            Some(1)
        );
    }

    #[test]
    fn get_returns_the_keyed_versioned_document_or_none() {
        let store = seeded_projection_store();
        let keyed = doc(json!({"id": "user-1", "email": "ada@example.com", "age": 36, "tags": ["eu", "paid"]}));
        assert_eq!(
            store.get(Tx::auto_commit(), "p", "user-1").unwrap(),
            Some(Versioned {
                version: 1,
                value: keyed
            })
        );
        assert_eq!(store.get(Tx::auto_commit(), "p", "user-404").unwrap(), None);

        // The row key is authoritative in `get` as in `get_many`: it overwrites an `"id"` the document carries.
        store
            .save(
                Tx::auto_commit(),
                "p",
                "user-2",
                0,
                doc(json!({"id": "stale", "age": 29})),
            )
            .unwrap();
        let read = store.get(Tx::auto_commit(), "p", "user-2").unwrap().expect("saved");
        assert_eq!(read.value, doc(json!({"id": "user-2", "age": 29})));
    }

    #[test]
    fn get_many_branches() {
        let store = MemoryProjectionStore::new();
        let empty = store.get_many(Tx::auto_commit(), "p", &query(json!({}))).unwrap();
        assert!(empty.rows.is_empty() && empty.next.is_none(), "empty store pages empty");

        // The row key is authoritative: it overwrites whatever "id" the document carries.
        store
            .save(
                Tx::auto_commit(),
                "p",
                "user-1",
                0,
                doc(json!({"id": "stale", "email": "ada@example.com", "age": 36})),
            )
            .unwrap();
        store
            .save(
                Tx::auto_commit(),
                "p",
                "user-2",
                0,
                doc(json!({"id": "stale", "email": "grace@example.com", "age": 29})),
            )
            .unwrap();

        let page = store
            .get_many(
                Tx::auto_commit(),
                "p",
                &query(json!({"sort": [{"path": "age", "desc": true}]})),
            )
            .unwrap();
        assert_eq!(
            page.rows.iter().map(|d| d["id"].as_str().unwrap()).collect::<Vec<_>>(),
            ["user-1", "user-2"]
        );

        // Selection and paging are the kernel's, not the store's.
        let spec = query(json!({"filter": {"compare": {"path": "age", "op": "lt", "value": 30}}, "limit": 5}));
        let filtered = store.get_many(Tx::auto_commit(), "p", &spec).unwrap();
        assert_eq!(
            filtered
                .rows
                .iter()
                .map(|d| d["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["user-2"]
        );

        // A transaction pages over the committed rows plus its own staged ones, in id order.
        let tr = store.transactor();
        let tx = tr.begin();
        store
            .save(
                tx,
                "p",
                "user-0",
                0,
                doc(json!({"email": "linus@example.com", "age": 20})),
            )
            .unwrap();
        store
            .save(
                tx,
                "p",
                "user-2",
                1,
                doc(json!({"email": "grace@example.com", "age": 30})),
            )
            .unwrap();
        let own = store.get_many(tx, "p", &query(json!({}))).unwrap();
        let rows: Vec<(&str, i64)> = own
            .rows
            .iter()
            .map(|d| (d["id"].as_str().unwrap(), d["age"].as_i64().unwrap()))
            .collect();
        assert_eq!(
            rows,
            [("user-0", 20), ("user-1", 36), ("user-2", 30)],
            "staged rows overlay the committed ones"
        );
        assert_eq!(
            store
                .get_many(Tx::auto_commit(), "p", &query(json!({})))
                .unwrap()
                .rows
                .len(),
            2,
            "and nobody else sees them"
        );
        assert!(tr.rollback(tx));

        // A cursor the kernel cannot read is terminal, so it carries the `codec` code.
        let err = store
            .get_many(
                Tx::auto_commit(),
                "p",
                &query(json!({"sort": [{"path": "age"}], "after": "not-a-cursor"})),
            )
            .unwrap_err();
        assert_eq!(err.code, edax_core::ErrorCode::Codec, "{err}");
    }

    /// Why `get_many` injects the row key: every row here ties on the sort key, so only the `id`
    /// the store injects breaks the tie, and keyset paging drops and repeats nothing. Without the
    /// injection no row could mint a cursor, and the kernel would refuse the page's `next`.
    #[test]
    fn get_many_pages_by_cursor_over_a_tied_sort_key() {
        let store = MemoryProjectionStore::new();
        let ids = ["user-1", "user-2", "user-3"];
        for id in ids {
            store
                .save(
                    Tx::auto_commit(),
                    "p",
                    id,
                    0,
                    doc(json!({"email": "ada@example.com", "age": 36, "tags": ["eu", "paid"]})),
                )
                .expect("seed saves");
        }

        let mut seen: Vec<String> = Vec::new();
        let mut after: Option<String> = None;
        // One row per page: a walk longer than the rows plus one repeated a row.
        for _ in 0..=ids.len() {
            let spec = match &after {
                Some(cursor) => json!({"sort": [{"path": "age"}], "limit": 1, "after": cursor}),
                None => json!({"sort": [{"path": "age"}], "limit": 1}),
            };
            let page = store.get_many(Tx::auto_commit(), "p", &query(spec)).unwrap();
            seen.extend(
                page.rows
                    .iter()
                    .map(|row| row["id"].as_str().expect("the injected row key").to_owned()),
            );
            match page.next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        assert_eq!(
            seen, ids,
            "each page advances by exactly one row, and the last mints no cursor"
        );
    }

    /// Two read models over one database share no row: an id one has saved is absent from the other,
    /// whose first save at version 0 makes its own row, and each pages only its own. The names and ids
    /// would collide if the key joined them with a separator.
    #[test]
    fn read_models_share_no_rows() {
        let store = MemoryProjectionStore::new();
        let auto = Tx::auto_commit();
        store.save(auto, "p", "q:r", 0, doc(json!({"from": "p"}))).unwrap();
        assert_eq!(store.get(auto, "p:q", "q:r").unwrap(), None);
        assert_eq!(store.get(auto, "p:q", "r").unwrap(), None);
        assert_eq!(
            store.save(auto, "p:q", "q:r", 0, doc(json!({"from": "p:q"}))).unwrap(),
            1
        );
        let ids = |projection| {
            store
                .get_many(auto, projection, &query(json!({})))
                .unwrap()
                .rows
                .iter()
                .map(|row| {
                    (
                        row["id"].as_str().unwrap().to_owned(),
                        row["from"].as_str().unwrap().to_owned(),
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(ids("p"), [("q:r".to_owned(), "p".to_owned())]);
        assert_eq!(ids("p:q"), [("q:r".to_owned(), "p:q".to_owned())]);

        // Nor do they conflict: one read model's open save leaves the other's id free.
        let tr = store.transactor();
        let holder = tr.begin();
        store.save(holder, "p", "q:r", 1, doc(json!({"from": "p"}))).unwrap();
        assert_eq!(
            store.save(auto, "p:q", "q:r", 1, doc(json!({"from": "p:q"}))).unwrap(),
            2
        );
        assert!(tr.rollback(holder));
    }
}

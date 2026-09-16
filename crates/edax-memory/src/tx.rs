//! The transaction boundary: the token, the staged write sets, the committed database, and the
//! transactor that hands out both tokens and store views.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use edax_core::{lock, AdapterError};
use serde_json::{Map, Value};

use crate::event::{MemoryEventStore, Snapshot};
use crate::idempotency::MemoryIdempotencyStore;
use crate::projection::MemoryProjectionStore;
use crate::saga::MemorySagaStore;

/// A read-model document: always a JSON object, so the store can inject the row key as its `"id"`.
pub(crate) type Document = Map<String, Value>;

/// The JSON row a versioned read returns: the row key, the version it was read at, and the seam's
/// own payload under `payload_key` (`"document"` for a projection, `"state"` for a saga). One
/// builder, so the two rows cannot drift in shape.
pub(crate) fn versioned_row(id: &str, version: i64, payload_key: &str, payload: Value) -> Result<String, AdapterError> {
    let row = Value::Object(Map::from_iter([
        ("id".to_owned(), Value::String(id.to_owned())),
        ("version".to_owned(), Value::Number(version.into())),
        (payload_key.to_owned(), payload),
    ]));
    serde_json::to_string(&row).map_err(AdapterError::codec)
}

/// The token that names no transaction. A write made under it lands at once (auto-commit) and a
/// read under it sees only committed state. Every store method accepts it, so a caller that opens no
/// unit of work keeps today's write-then-read behaviour.
pub const NO_TX: i64 = 0;

/// Process-wide token source. A token names one transaction of one transactor for the whole process
/// lifetime, so a handle from another transactor is refused as unknown instead of aliasing a live
/// transaction there. Wrapping needs 2^63 begins, and only matters if a token that old were still open.
static NEXT_TOKEN: AtomicI64 = AtomicI64::new(NO_TX + 1);

/// A transaction token that names no open transaction: never begun, or already committed or rolled
/// back. Its own terminal code rather than `codec:`, which the kernel reserves for a value that
/// would not marshal: the same token fails identically on every retry, and a transient tag would
/// have a host retry a wiring mistake forever.
pub(crate) fn unknown_tx(tx: i64) -> AdapterError {
    AdapterError::BadHandle(format!("unknown transaction {tx}: not begun, or already committed or rolled back"))
}

/// An envelope's sequence as the `i64` the store's version arithmetic uses. Saturating, not wrapping:
/// a `u64` past `i64::MAX` would otherwise come back negative and sort before every real sequence.
pub(crate) fn sequence_of(env: &edax_event::EventEnvelope) -> i64 {
    env.sequence.map_or(0, |s| i64::try_from(s).unwrap_or(i64::MAX))
}



/// Everything committed, for every seam. One struct so one lock guards it all: a commit applies a
/// transaction's writes to every seam while holding that lock, so no reader can see the events
/// without the claim that guards them.
#[derive(Default)]
pub(crate) struct Committed {
    pub(crate) events: HashMap<String, Vec<edax_event::EventEnvelope>>, // aggregate_id -> ordered events
    pub(crate) snapshots: HashMap<String, Snapshot>,                   // aggregate_id -> snapshot
    pub(crate) outbox: Vec<edax_event::EventEnvelope>,                  // unpublished rows only (pruned on publish)
    pub(crate) projections: BTreeMap<String, (i64, Document)>,          // id -> (version, document); id-ordered
    pub(crate) claims: HashMap<String, Vec<u8>>,                        // idempotency key -> record bytes
    pub(crate) sagas: HashMap<String, (i64, Value)>,                    // correlation id -> (version, state)
}

/// One open transaction's staged writes: the overlay a read inside it sees over [`Committed`], and
/// the set [`MemoryTransactor::commit`] applies. A `None` in `claims` / `sagas` is a staged delete.
#[derive(Default)]
pub(crate) struct WriteSet {
    pub(crate) streams: HashMap<String, Vec<edax_event::EventEnvelope>>, // aggregate_id -> appended tail
    pub(crate) outbox: Vec<edax_event::EventEnvelope>,                   // the same envelopes, in append order
    pub(crate) snapshots: HashMap<String, Snapshot>,
    pub(crate) projections: HashMap<String, (i64, Document)>,
    pub(crate) claims: HashMap<String, Option<Vec<u8>>>,
    pub(crate) sagas: HashMap<String, Option<(i64, Value)>>,
}

impl WriteSet {
    /// The version another writer of `aggregate_id` would commit, if this set stages a tail for it.
    pub(crate) fn stream_version(&self, aggregate_id: &str) -> Option<i64> {
        self.streams.get(aggregate_id).and_then(|tail| tail.last()).map(sequence_of)
    }
}

/// The whole memory database: the committed state plus every open transaction, behind one lock.
#[derive(Default)]
pub(crate) struct MemoryState {
    pub(crate) committed: Committed,
    pub(crate) open: HashMap<i64, WriteSet>,
}

impl MemoryState {
    /// Run a read under `tx`: against the committed state alone for [`NO_TX`], or overlaid with the
    /// transaction's own staged writes (read-your-writes) for an open token.
    pub(crate) fn read<R>(&self, tx: i64, op: impl FnOnce(&Committed, Option<&WriteSet>) -> R) -> Result<R, AdapterError> {
        if tx == NO_TX {
            return Ok(op(&self.committed, None));
        }
        match self.open.get(&tx) {
            Some(own) => Ok(op(&self.committed, Some(own))),
            None => Err(unknown_tx(tx)),
        }
    }

    /// Run a write under `tx`. The operation sees the committed state, the *other* open transactions
    /// (so a key one of them holds can be refused) and its own write set to stage into. Under
    /// [`NO_TX`] the write set is a fresh one applied as soon as the operation succeeds, which is
    /// what makes a call outside any unit of work an auto-commit of exactly that call.
    pub(crate) fn write<R>(
        &mut self,
        tx: i64,
        op: impl FnOnce(&Committed, &HashMap<i64, WriteSet>, &mut WriteSet) -> Result<R, AdapterError>,
    ) -> Result<R, AdapterError> {
        if tx == NO_TX {
            let mut own = WriteSet::default();
            let result = op(&self.committed, &self.open, &mut own)?;
            self.committed.apply(own);
            return Ok(result);
        }
        // Taken out of the map for the call so `op` can borrow the other write sets immutably while
        // staging into this one; put back whatever the outcome, since a refused write must not
        // discard what the transaction staged before it.
        let Some(mut own) = self.open.remove(&tx) else { return Err(unknown_tx(tx)) };
        let result = op(&self.committed, &self.open, &mut own);
        self.open.insert(tx, own);
        result
    }
}

/// What another open transaction has staged for a key, if any: the whole reason a write to that key
/// is refused until the holder commits or rolls back. `accessor` names the seam's own map.
///
/// The first holder found decides the conflict. There can be more than one only across different
/// keys, since a held key is held against every other transaction.
pub(crate) fn held_by_other<'a, T>(
    others: &'a HashMap<i64, WriteSet>,
    accessor: impl Fn(&'a WriteSet) -> Option<T>,
) -> Option<T> {
    others.values().find_map(accessor)
}

impl Committed {
    /// Apply a write set. Infallible by construction: every check ran when the write was staged, and
    /// a staged key is held until its transaction ends, so nothing can have changed underneath it.
    pub(crate) fn apply(&mut self, set: WriteSet) {
        for (aggregate_id, tail) in set.streams {
            self.events.entry(aggregate_id).or_default().extend(tail);
        }
        self.outbox.extend(set.outbox);
        self.snapshots.extend(set.snapshots);
        self.projections.extend(set.projections);
        for (key, record) in set.claims {
            match record {
                Some(bytes) => self.claims.insert(key, bytes),
                None => self.claims.remove(&key),
            };
        }
        for (id, row) in set.sagas {
            match row {
                Some(instance) => self.sagas.insert(id, instance),
                None => self.sagas.remove(&id),
            };
        }
    }
}

/// The three impls every store view shares: a view is a handle on one transactor's state, so its
/// `Default`, `new` and `transactor` cannot differ between views without one of them meaning
/// something else. `Default` opens a database of its own — a view built that way shares nothing with
/// any other view, which is why a store normally comes from a transactor.
macro_rules! store_view {
    ($view:ident, $from_transactor:ident) => {
        impl Default for $view {
            fn default() -> Self {
                MemoryTransactor::new().$from_transactor()
            }
        }

        impl $view {
            /// A store over a database of its own.
            pub fn new() -> Self {
                Self::default()
            }

            /// The transactor this store enlists with.
            pub fn transactor(&self) -> MemoryTransactor {
                MemoryTransactor { state: Arc::clone(&self.state) }
            }
        }
    };
}
pub(crate) use store_view;

/// The transaction boundary of the in-memory backend, for development and tests. One transactor is
/// one database: the four stores it hands out share its state, so their writes under one token
/// commit together. A token from [`begin`](Self::begin) stages every write made under it; reads
/// under it see the committed state plus those staged writes; other transactions and [`NO_TX`]
/// readers see nothing of it until [`commit`](Self::commit). `Clone` shares the same database.
///
/// Isolation is by exclusion, not by blocking: a key (an aggregate's stream, a projection id, an
/// idempotency key, a correlation id) staged by one open transaction is held until that transaction
/// ends, and a second writer conflicts at once instead of waiting. A durable store would park the
/// second writer on a row lock; an in-memory store driven from one asyncio loop cannot park anyone
/// without deadlocking the loop, so the conflict is reported as the reload-and-retry signal the
/// writer would have received once the holder committed. An idempotency claim on a held key loses
/// and reads the holder's record as *pending* whatever the holder has staged since, because the
/// holder's outcome is not final until it commits. The whole database sits behind one lock, taken
/// once per call and never re-entered, so there is nothing to deadlock on; a commit applies every
/// seam's writes under that one hold, so no reader can observe half of one.
///
/// A transaction that is neither committed nor rolled back is not merely a held key: it is a
/// permanent conflict on every key it staged, for the process lifetime, because nothing can settle
/// the token that holds them. Every SDK's scoped unit of work guarantees one of commit or rollback
/// runs; [`open_count`](Self::open_count) is the diagnostic for a host that suspects one does not.
#[derive(Clone, Default)]
pub struct MemoryTransactor {
    pub(crate) state: Arc<Mutex<MemoryState>>,
}

impl MemoryTransactor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a transaction and return its token. Never [`NO_TX`].
    pub fn begin(&self) -> i64 {
        let token = NEXT_TOKEN.fetch_add(1, Ordering::Relaxed);
        lock(&self.state).open.insert(token, WriteSet::default());
        token
    }

    /// Apply the transaction's staged writes to every seam atomically and close it. A token that
    /// names no open transaction is refused: the caller believes it committed something.
    pub fn commit(&self, tx: i64) -> Result<(), AdapterError> {
        let mut state = lock(&self.state);
        let Some(set) = state.open.remove(&tx) else { return Err(unknown_tx(tx)) };
        state.committed.apply(set);
        Ok(())
    }

    /// Drop the transaction's staged writes and close it. Returns `false` and changes nothing if the
    /// token names no open transaction: there was nothing to roll back.
    pub fn rollback(&self, tx: i64) -> bool {
        lock(&self.state).open.remove(&tx).is_some()
    }

    /// How many transactions are open right now. A leak diagnostic: a count that does not return to
    /// its idle level names an abandoned token, and every key that token staged is conflicted for
    /// the process lifetime.
    pub fn open_count(&self) -> usize {
        lock(&self.state).open.len()
    }

    /// The event store over this database.
    pub fn event_store(&self) -> MemoryEventStore {
        MemoryEventStore { state: Arc::clone(&self.state) }
    }

    /// The projection store over this database.
    pub fn projection_store(&self) -> MemoryProjectionStore {
        MemoryProjectionStore { state: Arc::clone(&self.state) }
    }

    /// The idempotency store over this database.
    pub fn idempotency_store(&self) -> MemoryIdempotencyStore {
        MemoryIdempotencyStore { state: Arc::clone(&self.state) }
    }

    /// The saga store over this database.
    pub fn saga_store(&self) -> MemorySagaStore {
        MemorySagaStore { state: Arc::clone(&self.state) }
    }
}

#[cfg(test)]
// A test asserts on the value it just built; a panic there is the failure report.
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::testkit::{envelope, ids};
    /// One database with every seam, and a handle on each view.
    struct Db {
        tr: MemoryTransactor,
        events: MemoryEventStore,
        projections: MemoryProjectionStore,
        claims: MemoryIdempotencyStore,
        sagas: MemorySagaStore,
    }

    fn db() -> Db {
        let tr = MemoryTransactor::new();
        Db {
            events: tr.event_store(),
            projections: tr.projection_store(),
            claims: tr.idempotency_store(),
            sagas: tr.saga_store(),
            tr,
        }
    }

    const HASH: u64 = 11_234_062_831_410_934_249;
    const DOC: &str = r#"{"email":"ada@example.com","age":36}"#;
    const STATE: &str = r#"{"status":"awaiting_payment"}"#;

    /// Stage one write on every seam under `tx`.
    fn write_everything(db: &Db, tx: i64) {
        db.events.append(tx, "invoice-7", 0, vec![envelope("evt-1", Some(1))]).expect("append");
        db.events.save_snapshot(tx, "invoice-7", "Invoice", 1, b"snap".to_vec()).expect("snapshot");
        db.projections.save(tx, "user-1", 0, DOC).expect("projection");
        assert_eq!(db.claims.claim(tx, "req-A", HASH), Ok(None), "the claim is won");
        db.sagas.save(tx, "order-1", 0, STATE).expect("saga");
    }

    /// What a reader under `tx` sees of every seam: `(events, outbox, snapshot, projection, claim, saga)`.
    fn visible(db: &Db, tx: i64) -> (usize, usize, bool, bool, bool, bool) {
        (
            db.events.load(tx, "invoice-7", 0).unwrap().len(),
            db.events.unpublished(10).len(),
            db.events.load_snapshot(tx, "invoice-7").unwrap().is_some(),
            db.projections.get(tx, "user-1").unwrap().is_some(),
            db.claims.claim(tx, "req-A", HASH).unwrap().is_some(),
            db.sagas.load(tx, "order-1").unwrap().is_some(),
        )
    }

    const NOTHING: (usize, usize, bool, bool, bool, bool) = (0, 0, false, false, false, false);
    const EVERYTHING: (usize, usize, bool, bool, bool, bool) = (1, 1, true, true, true, true);

    #[test]
    fn transaction_outcome_branches() {
        struct Case {
            name: &'static str,
            /// Commit the transaction (`true`) or roll it back.
            commit: bool,
            /// What a reader outside any transaction sees afterwards.
            want: (usize, usize, bool, bool, bool, bool),
        }
        let cases = [
            Case { name: "commit_lands_every_seam_together", commit: true, want: EVERYTHING },
            Case { name: "rollback_leaves_nothing_behind_on_any_seam", commit: false, want: NOTHING },
        ];
        for c in cases {
            let db = db();
            let tx = db.tr.begin();
            write_everything(&db, tx);
            // Read-your-writes: the transaction sees its own staged writes, the outbox excepted (a
            // staged append is not a row to publish). Nobody else sees any of it yet.
            assert_eq!(visible(&db, tx), (1, 0, true, true, true, true), "{}: own view", c.name);
            // `visible` claims the key under NO_TX and must lose: the open transaction holds it.
            assert_eq!(visible(&db, NO_TX), (0, 0, false, false, true, false), "{}: outside view", c.name);
            let other = db.tr.begin();
            assert_eq!(visible(&db, other), (0, 0, false, false, true, false), "{}: another transaction", c.name);
            assert!(db.tr.rollback(other));

            if c.commit {
                db.tr.commit(tx).expect(c.name);
            } else {
                assert!(db.tr.rollback(tx), "{}", c.name);
            }
            assert_eq!(visible(&db, NO_TX), c.want, "{}: after", c.name);
            // The token is spent either way.
            assert_eq!(db.tr.commit(tx), Err(unknown_tx(tx)), "{}: a closed token cannot commit", c.name);
            assert!(!db.tr.rollback(tx), "{}: a closed token has nothing to roll back", c.name);
        }
    }

    #[test]
    fn a_held_key_conflicts_until_the_holder_ends() {
        // Every seam refuses a second writer of a key an open transaction staged, reporting the
        // version the holder would commit; once the holder rolls back the key is free again.
        struct Case {
            name: &'static str,
            write: fn(&Db, i64) -> Result<(), AdapterError>,
        }
        let cases = [
            Case {
                name: "event stream",
                write: |db, tx| db.events.append(tx, "invoice-7", 0, vec![envelope("evt-1", Some(1))]),
            },
            Case { name: "projection row", write: |db, tx| db.projections.save(tx, "user-1", 0, DOC).map(|_| ()) },
            Case { name: "saga instance", write: |db, tx| db.sagas.save(tx, "order-1", 0, STATE).map(|_| ()) },
            Case { name: "saga delete", write: |db, tx| db.sagas.delete(tx, "order-1", 0) },
        ];
        for c in cases {
            let db = db();
            let holder = db.tr.begin();
            write_everything(&db, holder);
            let want = Err(AdapterError::Concurrency { expected: 0, stored: 1 });
            assert_eq!((c.write)(&db, NO_TX), want, "{}: auto-commit writer", c.name);
            let other = db.tr.begin();
            assert_eq!((c.write)(&db, other), want, "{}: another transaction", c.name);
            assert!(db.tr.rollback(other));
            assert!(db.tr.rollback(holder));
            assert_eq!((c.write)(&db, NO_TX), Ok(()), "{}: free once the holder rolled back", c.name);
        }
    }

    #[test]
    fn a_claim_on_a_held_key_reads_pending_whatever_the_holder_staged() {
        // The holder completed its claim but has not committed: its outcome is not final, so a
        // second claimant must wait (InProgress), not replay a result that may yet roll back.
        let db = db();
        let holder = db.tr.begin();
        assert_eq!(db.claims.claim(holder, "req-A", HASH), Ok(None));
        let done = br#"{"payload_hash":11234062831410934249,"result":42,"error":null,"pending":false}"#;
        db.claims.complete(holder, "req-A", done).unwrap();

        let pending = format!(r#"{{"payload_hash":{HASH},"result":null,"error":null,"pending":true}}"#).into_bytes();
        assert_eq!(db.claims.claim(NO_TX, "req-A", HASH).unwrap().as_deref(), Some(&pending[..]));
        let other = db.tr.begin();
        assert_eq!(db.claims.claim(other, "req-A", HASH).unwrap().as_deref(), Some(&pending[..]));
        assert!(db.tr.rollback(other));

        // Once committed the real outcome is readable; a later claimant replays it.
        db.tr.commit(holder).unwrap();
        assert_eq!(db.claims.claim(NO_TX, "req-A", HASH).unwrap().as_deref(), Some(&done[..]));
    }

    #[test]
    fn complete_and_release_refuse_a_key_another_transaction_holds() {
        // The holder's commit would overwrite a `complete` written elsewhere, and a losing
        // claimant's `release` would free the winner's claim. A staged release holds nothing, so
        // the same two writes are allowed once the holder has given the key up.
        const DONE: &[u8] = br#"{"payload_hash":11234062831410934249,"result":42,"error":null,"pending":false}"#;
        struct Case {
            name: &'static str,
            /// What the other open transaction stages for `req-A` before the write under test.
            stage: fn(&Db, i64),
            write: fn(&Db, i64) -> Result<(), AdapterError>,
            expected: Result<(), AdapterError>,
            /// Whether `req-A` is claimed once both transactions have committed.
            claimed_after: bool,
        }
        let held = Err(AdapterError::Concurrency { expected: 0, stored: 0 });
        let cases = [
            Case {
                name: "complete_on_a_held_key_conflicts",
                stage: |db, tx| assert_eq!(db.claims.claim(tx, "req-A", HASH), Ok(None)),
                write: |db, tx| db.claims.complete(tx, "req-A", DONE),
                expected: held.clone(),
                claimed_after: true,
            },
            Case {
                name: "release_on_a_held_key_conflicts",
                stage: |db, tx| assert_eq!(db.claims.claim(tx, "req-A", HASH), Ok(None)),
                write: |db, tx| db.claims.release(tx, "req-A"),
                expected: held,
                claimed_after: true,
            },
            Case {
                name: "complete_over_a_staged_release_is_allowed",
                stage: |db, tx| {
                    assert_eq!(db.claims.claim(tx, "req-A", HASH), Ok(None));
                    db.claims.release(tx, "req-A").expect("the holder releases its own claim");
                },
                write: |db, tx| db.claims.complete(tx, "req-A", DONE),
                expected: Ok(()),
                claimed_after: true,
            },
            Case {
                name: "release_over_a_staged_release_is_allowed",
                stage: |db, tx| {
                    assert_eq!(db.claims.claim(tx, "req-A", HASH), Ok(None));
                    db.claims.release(tx, "req-A").expect("the holder releases its own claim");
                },
                write: |db, tx| db.claims.release(tx, "req-A"),
                expected: Ok(()),
                claimed_after: false,
            },
        ];
        for case in cases {
            let db = db();
            let holder = db.tr.begin();
            (case.stage)(&db, holder);
            let writer = db.tr.begin();
            assert_eq!((case.write)(&db, writer), case.expected, "{}", case.name);
            // The holder first: its staged release must not undo the write the writer was allowed.
            db.tr.commit(holder).expect("the holder's transaction settles");
            db.tr.commit(writer).expect("the writer's transaction settles");
            assert_eq!(
                db.claims.claim(NO_TX, "req-A", HASH).unwrap().is_some(),
                case.claimed_after,
                "{} committed claim",
                case.name
            );
        }
    }

    #[test]
    fn open_count_reports_unsettled_transactions() {
        let db = db();
        assert_eq!(db.tr.open_count(), 0, "a fresh transactor holds nothing open");
        let first = db.tr.begin();
        let second = db.tr.begin();
        assert_eq!(db.tr.open_count(), 2, "each begin opens one");
        db.tr.commit(first).expect("commit settles");
        assert_eq!(db.tr.open_count(), 1, "a commit closes its transaction");
        assert!(db.tr.rollback(second));
        assert_eq!(db.tr.open_count(), 0, "a rollback closes its transaction");
        assert!(!db.tr.rollback(second), "a settled token is not open");
        assert_eq!(db.tr.open_count(), 0, "a second rollback of a settled token changes nothing");
    }

    #[test]
    fn writes_within_one_transaction_compose() {
        // Read-your-writes on the version arithmetic: a second append continues the staged tail, a
        // release frees the key for the same transaction, and a save-then-delete leaves no row.
        let db = db();
        let tx = db.tr.begin();
        db.events.append(tx, "invoice-7", 0, vec![envelope("evt-1", Some(1))]).unwrap();
        db.events.append(tx, "invoice-7", 1, vec![envelope("evt-2", Some(2))]).unwrap();
        assert_eq!(
            db.events.append(tx, "invoice-7", 1, vec![envelope("evt-3", Some(2))]),
            Err(AdapterError::Concurrency { expected: 1, stored: 2 }),
            "the staged tail is the version the transaction sees"
        );
        assert_eq!(db.claims.claim(tx, "req-A", HASH), Ok(None));
        db.claims.release(tx, "req-A").unwrap();
        assert_eq!(db.claims.claim(tx, "req-A", HASH), Ok(None), "released, so won again");
        assert_eq!(db.sagas.save(tx, "order-1", 0, STATE), Ok(1));
        db.sagas.delete(tx, "order-1", 1).unwrap();
        assert_eq!(db.sagas.load(tx, "order-1"), Ok(None), "deleted in the same transaction");
        assert_eq!(db.sagas.save(tx, "order-1", 0, STATE), Ok(1), "and creatable again at version 0");
        // An empty batch is a version check only: it stages no tail and holds nothing.
        db.events.append(tx, "invoice-8", 0, vec![]).unwrap();
        db.events.append(NO_TX, "invoice-8", 0, vec![envelope("evt-9", Some(1))]).expect("not held");

        db.tr.commit(tx).unwrap();
        assert_eq!(ids(&db.events.load(NO_TX, "invoice-7", 0).unwrap()), ["evt-1", "evt-2"]);
        assert_eq!(ids(&db.events.unpublished(10)), ["evt-9", "evt-1", "evt-2"], "outbox: commit order, then append order");
        assert!(db.sagas.load(NO_TX, "order-1").unwrap().is_some());
    }

    #[test]
    fn an_unknown_token_is_refused_by_every_method() {
        // A token that was never begun, one from another transactor, and one already closed all read
        // as unknown: tokens are process-unique, so none can alias a live transaction here.
        let db = db();
        let foreign = MemoryTransactor::new().begin();
        let closed = db.tr.begin();
        db.tr.commit(closed).unwrap();
        for (name, tx) in [("never begun", 12_345), ("another transactor", foreign), ("already closed", closed)] {
            let want: Result<(), AdapterError> = Err(unknown_tx(tx));
            assert_eq!(db.events.load(tx, "invoice-7", 0).map(|_| ()), want, "{name}: load");
            assert_eq!(db.events.append(tx, "invoice-7", 0, vec![]).map(|_| ()), want, "{name}: append");
            assert_eq!(db.events.load_snapshot(tx, "invoice-7").map(|_| ()), want, "{name}: load_snapshot");
            assert_eq!(db.events.save_snapshot(tx, "invoice-7", "Invoice", 1, vec![]).map(|_| ()), want, "{name}: save_snapshot");
            assert_eq!(db.projections.get(tx, "user-1").map(|_| ()), want, "{name}: get");
            assert_eq!(db.projections.save(tx, "user-1", 0, DOC).map(|_| ()), want, "{name}: save");
            assert_eq!(db.projections.get_many(tx, "{}").map(|_| ()), want, "{name}: get_many");
            assert_eq!(db.claims.claim(tx, "req-A", HASH).map(|_| ()), want, "{name}: claim");
            assert_eq!(db.claims.complete(tx, "req-A", b"{}").map(|_| ()), want, "{name}: complete");
            assert_eq!(db.claims.release(tx, "req-A").map(|_| ()), want, "{name}: release");
            assert_eq!(db.sagas.load(tx, "order-1").map(|_| ()), want, "{name}: saga load");
            assert_eq!(db.sagas.save(tx, "order-1", 0, STATE).map(|_| ()), want, "{name}: saga save");
            assert_eq!(db.sagas.delete(tx, "order-1", 0).map(|_| ()), want, "{name}: saga delete");
            assert_eq!(db.tr.commit(tx).map(|_| ()), want, "{name}: commit");
            assert!(!db.tr.rollback(tx), "{name}: rollback");
            assert!(unknown_tx(tx).code().is_terminal(), "{name}: a wiring mistake is not retried");
        }
        assert_eq!(visible(&db, NO_TX), (0, 0, false, false, false, false), "nothing landed");
    }

    #[test]
    fn a_standalone_store_and_its_transactor_share_one_database() {
        let store = MemoryEventStore::new();
        let tx = store.transactor().begin();
        store.append(tx, "invoice-7", 0, vec![envelope("evt-1", Some(1))]).unwrap();
        assert!(store.load(NO_TX, "invoice-7", 0).unwrap().is_empty());
        store.transactor().commit(tx).unwrap();
        assert_eq!(ids(&store.load(NO_TX, "invoice-7", 0).unwrap()), ["evt-1"]);
        // Two standalone stores are two databases: a token of one is unknown to the other.
        let other = MemoryProjectionStore::new();
        let tx = store.transactor().begin();
        assert_eq!(other.get(tx, "user-1"), Err(unknown_tx(tx)));
    }

    /// Concurrent transactions on disjoint keys commit independently; on one key exactly one wins
    /// and the losers read the winner's version. All of it under the single lock, taken per call.
    #[test]
    fn concurrent_transactions_serialize_on_keys_not_on_the_database() {
        let db = db();
        let results: Vec<Result<(), AdapterError>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8)
                .map(|i| {
                    let db = &db;
                    s.spawn(move || {
                        let tx = db.tr.begin();
                        // Every thread writes its own projection row and races on one stream.
                        db.projections.save(tx, &format!("user-{i}"), 0, DOC)?;
                        let raced = db.events.append(tx, "invoice-7", 0, vec![envelope(&format!("evt-{i}"), Some(1))]);
                        match raced {
                            Ok(()) => db.tr.commit(tx),
                            Err(e) => {
                                assert!(db.tr.rollback(tx));
                                Err(e)
                            }
                        }
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().expect("writer thread")).collect()
        });
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1, "one writer of the stream wins: {results:?}");
        for e in results.iter().filter_map(|r| r.as_ref().err()) {
            assert_eq!(e, &AdapterError::Concurrency { expected: 0, stored: 1 });
        }
        assert_eq!(db.events.load(NO_TX, "invoice-7", 0).unwrap().len(), 1);
        let rows: Vec<Value> = serde_json::from_str(&db.projections.get_many(NO_TX, "{}").unwrap()).unwrap();
        assert_eq!(rows.len(), 1, "the losers rolled their rows back with the lost race");
    }
}

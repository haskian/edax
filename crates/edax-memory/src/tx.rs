//! The transaction boundary: the token, the staged write sets, the committed database, and the
//! transactor that hands out both tokens and store views. Each seam's tables and its merge rule live
//! in the seam's own module; this one composes them and applies a commit to all of them at once.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use edax_core::{lock, Error, ErrorCode, TokenMint, NO_TOKEN};
use edax_event::{CommandMessage, EventMessage};

use crate::event::{MemoryEventStore, Snapshot};
use crate::idempotency::{Held, MemoryIdempotencyStore};
use crate::outbox::MemoryCommandOutbox;
use crate::outbox_rows::OutboxRows;
use crate::projection::{Document, MemoryProjectionStore, ProjectionKey};
use crate::saga::{MemorySagaStore, SagaKey, SagaRow};
use crate::versioned::{Staged, VersionedTable};

/// A transaction token: names one transaction [`MemoryTransactor::begin`] opened, or
/// [`Tx::auto_commit`]. `Copy`, so a host keeps the open transaction's token wherever it needs it. A
/// token used after its transaction ended, or on another transactor's stores, is refused as unknown.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Tx(i64);

impl Tx {
    /// The token that names no transaction. A write made under it lands at once (auto-commit) and a
    /// read under it sees only committed state. Every store method accepts it, so a caller that opens
    /// no unit of work gets plain write-then-read behaviour. Also the `Default` token.
    pub const fn auto_commit() -> Tx {
        Tx(NO_TOKEN)
    }

    /// The token a binding's host handed back: the number [`raw`](Self::raw) gave it. Only a binding
    /// needs it, since a host that holds a number cannot hold a Rust value; any other number is a
    /// token no transaction holds, refused as unknown by every store method.
    pub const fn from_raw(raw: i64) -> Tx {
        Tx(raw)
    }

    /// The token as the number a binding hands its host.
    pub const fn raw(self) -> i64 {
        self.0
    }
}

impl Default for Tx {
    fn default() -> Self {
        Tx::auto_commit()
    }
}

impl std::fmt::Display for Tx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Process-wide token source, unlike the offset tracker's per-instance one: a token names one transaction of
/// one transactor for the whole process lifetime, so a handle from another transactor is refused as
/// unknown instead of aliasing a live transaction there.
static TX_TOKENS: TokenMint = TokenMint::new();

/// A transaction token that names no open transaction: never begun, or already committed or rolled
/// back. Its own terminal code, `bad_handle`, rather than `codec`, which the kernel reserves for a
/// value that would not marshal: the same token fails identically on every retry, and a transient
/// code would have a host retry a wiring mistake forever.
pub(crate) fn unknown_tx(tx: Tx) -> Error {
    Error::bad_handle(format_args!(
        "unknown transaction {tx}: not begun, or already committed or rolled back"
    ))
}

/// Everything committed, for every seam. One struct so one lock guards it all: a commit applies a
/// transaction's writes to every seam while holding that lock, so no reader can see the events
/// without the claim that guards them.
#[derive(Default)]
pub(crate) struct Committed {
    pub(crate) events: HashMap<String, Vec<EventMessage>>, // aggregate_id -> ordered events
    pub(crate) event_ids: HashSet<String>,                 // every stored event id
    pub(crate) snapshots: HashMap<String, Snapshot>,       // aggregate_id -> snapshot
    pub(crate) event_outbox: OutboxRows<EventMessage>,     // unpublished events only
    pub(crate) projections: VersionedTable<ProjectionKey, Document>, // (projection, id) -> document
    pub(crate) claims: HashMap<String, Held>,              // idempotency key -> record (and claim)
    pub(crate) sagas: VersionedTable<SagaKey, SagaRow>,    // (saga, instance id) -> row
    pub(crate) saga_outbox: HashMap<String, OutboxRows<CommandMessage>>, // saga -> its unsent commands only
    pub(crate) command_outbox: OutboxRows<CommandMessage>, // unsent outbox commands only
    pub(crate) command_after: HashMap<String, String>,     // command id -> the event it waits behind
}

/// One open transaction's staged writes: the overlay a read inside it sees over [`Committed`], and
/// the set [`MemoryTransactor::commit`] applies. A `None` in `claims` is a staged delete.
#[derive(Default)]
pub(crate) struct WriteSet {
    pub(crate) streams: HashMap<String, Vec<EventMessage>>, // aggregate_id -> appended tail
    pub(crate) event_outbox: Vec<EventMessage>,             // the same events, in append order
    pub(crate) snapshots: HashMap<String, Snapshot>,
    pub(crate) projections: Staged<ProjectionKey, Document>,
    pub(crate) claims: HashMap<String, Option<Held>>,
    pub(crate) sagas: Staged<SagaKey, SagaRow>,
    pub(crate) saga_outbox: HashMap<String, Vec<CommandMessage>>, // saga -> its commands, in write order
    pub(crate) command_outbox: Vec<CommandMessage>,               // outbox commands, in enqueue order
    pub(crate) command_after: HashMap<String, String>,            // command id -> the event it waits behind
    pub(crate) held_streams: HashSet<String>,                     // streams this transaction locked or appended to
}

/// The whole memory database: the committed state plus every open transaction, behind one lock.
#[derive(Default)]
pub(crate) struct MemoryState {
    pub(crate) committed: Committed,
    pub(crate) open: HashMap<Tx, WriteSet>,
}

impl MemoryState {
    /// Run a read under `tx`: against the committed state alone for [`Tx::auto_commit`], or overlaid with the
    /// transaction's own staged writes (read-your-writes) for an open token.
    pub(crate) fn read<R>(&self, tx: Tx, op: impl FnOnce(&Committed, Option<&WriteSet>) -> R) -> Result<R, Error> {
        if tx == Tx::auto_commit() {
            return Ok(op(&self.committed, None));
        }
        match self.open.get(&tx) {
            Some(own) => Ok(op(&self.committed, Some(own))),
            None => Err(unknown_tx(tx)),
        }
    }

    /// Run a write under `tx`. The operation sees the committed state, the *other* open transactions
    /// (so a key one of them holds can be refused) and its own write set to stage into. Under
    /// [`Tx::auto_commit`] the write set is a fresh one applied as soon as the operation succeeds, which is
    /// what makes a call outside any unit of work an auto-commit of exactly that call.
    pub(crate) fn write<R>(
        &mut self,
        tx: Tx,
        op: impl FnOnce(&Committed, &HashMap<Tx, WriteSet>, &mut WriteSet) -> Result<R, Error>,
    ) -> Result<R, Error> {
        if tx == Tx::auto_commit() {
            let mut own = WriteSet::default();
            let result = op(&self.committed, &self.open, &mut own)?;
            self.committed.apply(own);
            return Ok(result);
        }
        // Taken out of the map for the call so `op` can borrow the other write sets immutably while
        // staging into this one; put back whatever the outcome, since a refused write must not
        // discard what the transaction staged before it.
        let Some(mut own) = self.open.remove(&tx) else {
            return Err(unknown_tx(tx));
        };
        let result = op(&self.committed, &self.open, &mut own);
        self.open.insert(tx, own);
        result
    }
}

/// The conflict a write to `what` `key`, which another open transaction holds, gets.
pub(crate) fn held(what: &str, key: &str) -> Error {
    Error::new(
        ErrorCode::Concurrency,
        format_args!("{what} {key} is held by another open transaction"),
    )
}

/// What another open transaction has staged for a key, if any: the whole reason a write to that key
/// is refused until the holder commits or rolls back. `accessor` names the seam's own map.
///
/// The first holder found decides the conflict. There can be more than one only across different
/// keys, since a held key is held against every other transaction.
pub(crate) fn held_by_other<'a, T>(
    others: &'a HashMap<Tx, WriteSet>,
    accessor: impl Fn(&'a WriteSet) -> Option<T>,
) -> Option<T> {
    others.values().find_map(accessor)
}

impl Committed {
    /// Apply a write set. Infallible by construction: every check ran when the write was staged, and
    /// a staged key is held until its transaction ends, so nothing can have changed underneath it.
    /// Each seam merges its own writes by its own rule.
    pub(crate) fn apply(&mut self, set: WriteSet) {
        let WriteSet {
            streams,
            event_outbox,
            snapshots,
            projections,
            claims,
            sagas,
            saga_outbox,
            command_outbox,
            command_after,
            held_streams: _,
        } = set;
        self.apply_events(streams, event_outbox, snapshots);
        self.projections.apply(projections);
        self.apply_claims(claims);
        self.apply_sagas(sagas, saga_outbox);
        self.command_outbox.apply(command_outbox);
        self.command_after.extend(command_after);
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
                MemoryTransactor {
                    state: Arc::clone(&self.state),
                }
            }
        }
    };
}
pub(crate) use store_view;

/// The transaction boundary of the in-memory backend, for development and tests. One transactor is
/// one database: the five stores it hands out share its state, so their writes under one token
/// commit together. A token from [`begin`](Self::begin) stages every write made under it; reads
/// under it see the committed state plus those staged writes; other transactions and [`Tx::auto_commit`]
/// readers see nothing of it until [`commit`](Self::commit). `Clone` shares the same database.
///
/// Isolation is by exclusion, not by blocking: a key (an aggregate's stream, a projection id, an
/// idempotency key, a saga's instance) staged by one open transaction is held until that transaction
/// ends, and a second writer conflicts at once instead of waiting. A durable store would park the
/// second writer on a row lock; an in-memory store driven from one asyncio loop cannot park anyone
/// without deadlocking the loop, so the conflict is reported as the reload-and-retry signal the
/// writer would have received once the holder committed. An idempotency claim on a held key loses
/// and reads the holder's record as *pending* whatever the holder has staged since, because the
/// holder's outcome is not final until it commits. The whole database sits behind one lock, taken
/// once per call and never re-entered, so there is nothing to deadlock on; a commit applies every
/// seam's writes under that one hold, so no reader can observe half of one.
///
/// A transaction that is neither committed nor rolled back is a permanent conflict on every key it
/// staged, for the process lifetime, because nothing can settle the token that holds them. Every
/// SDK's scoped unit of work guarantees one of commit or rollback runs;
/// [`open_count`](Self::open_count) is the diagnostic for a host that suspects one does not.
#[derive(Clone, Default)]
pub struct MemoryTransactor {
    pub(crate) state: Arc<Mutex<MemoryState>>,
}

impl MemoryTransactor {
    /// An empty in-memory database with no open transaction.
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a transaction and return its token. Never [`Tx::auto_commit`].
    pub fn begin(&self) -> Tx {
        let token = Tx(TX_TOKENS.issue());
        lock(&self.state).open.insert(token, WriteSet::default());
        token
    }

    /// Apply the transaction's staged writes to every seam atomically and close it. A token that
    /// names no open transaction is refused: the caller believes it committed something.
    pub fn commit(&self, tx: Tx) -> Result<(), Error> {
        let mut state = lock(&self.state);
        let Some(set) = state.open.remove(&tx) else {
            return Err(unknown_tx(tx));
        };
        state.committed.apply(set);
        Ok(())
    }

    /// Drop the transaction's staged writes and close it. Returns `false` and changes nothing if the
    /// token names no open transaction: there was nothing to roll back.
    pub fn rollback(&self, tx: Tx) -> bool {
        lock(&self.state).open.remove(&tx).is_some()
    }

    /// This database's identity: equal for every transactor and store view over it, different from any
    /// other database's, for as long as one of them is alive. An SDK keys its open units of work by it,
    /// so a second unit of work on one database is caught however the caller reached it.
    pub fn database(&self) -> usize {
        Arc::as_ptr(&self.state).addr()
    }

    /// How many transactions are open right now. A leak diagnostic: a count that does not return to
    /// its idle level names an abandoned token, and every key that token staged is conflicted for
    /// the process lifetime.
    pub fn open_count(&self) -> usize {
        lock(&self.state).open.len()
    }

    /// The event store over this database.
    pub fn event_store(&self) -> MemoryEventStore {
        MemoryEventStore {
            state: Arc::clone(&self.state),
        }
    }

    /// The projection store over this database.
    pub fn projection_store(&self) -> MemoryProjectionStore {
        MemoryProjectionStore {
            state: Arc::clone(&self.state),
        }
    }

    /// The idempotency store over this database.
    pub fn idempotency_store(&self) -> MemoryIdempotencyStore {
        MemoryIdempotencyStore {
            state: Arc::clone(&self.state),
        }
    }

    /// The saga store over this database.
    pub fn saga_store(&self) -> MemorySagaStore {
        MemorySagaStore {
            state: Arc::clone(&self.state),
        }
    }

    /// The command outbox over this database.
    pub fn command_outbox(&self) -> MemoryCommandOutbox {
        MemoryCommandOutbox {
            state: Arc::clone(&self.state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::idempotency::Owner;
    use crate::saga::SagaRow;
    use crate::testkit::{draft, draft_on, ids, RECORDED_AT};
    use edax_cqrs::Claimed;
    use serde_json::{json, Value};
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

    fn doc() -> Document {
        let Value::Object(doc) = json!({"email": "ada@example.com", "age": 36}) else {
            unreachable!()
        };
        doc
    }

    fn state() -> Value {
        json!({"status": "awaiting_payment"})
    }

    /// The handle of a claim that must be won.
    fn won(claimed: Result<Claimed, Error>) -> Vec<u8> {
        match claimed {
            Ok(Claimed::Won(handle)) => handle,
            other => panic!("the claim must be won, got {other:?}"),
        }
    }

    /// The record of a claim that must be lost.
    fn prior(claimed: Result<Claimed, Error>) -> Vec<u8> {
        match claimed {
            Ok(Claimed::Prior(record)) => record,
            other => panic!("the claim must be lost, got {other:?}"),
        }
    }

    /// Stage one write on every seam under `tx`.
    fn write_everything(db: &Db, tx: Tx) {
        db.events
            .append(tx, "invoice-7", 0, vec![draft("evt-1", Some(1))], RECORDED_AT)
            .expect("append");
        db.events
            .save_snapshot(tx, "invoice-7", "Invoice", 1, b"snap".to_vec())
            .expect("snapshot");
        db.projections.save(tx, "p", "user-1", 0, doc()).expect("projection");
        won(db.claims.claim(tx, "req-A", HASH));
        db.sagas
            .save(tx, "shipping", "order-1", 0, state(), String::new(), vec![])
            .expect("saga");
    }

    /// What a reader under `tx` sees of every seam: `(events, outbox, snapshot, projection, claim, saga)`.
    fn visible(db: &Db, tx: Tx) -> (usize, usize, bool, bool, bool, bool) {
        (
            db.events.load(tx, "invoice-7", 0).unwrap().len(),
            db.events.unpublished(10).len(),
            db.events.load_snapshot(tx, "invoice-7").unwrap().is_some(),
            db.projections.get(tx, "p", "user-1").unwrap().is_some(),
            matches!(db.claims.claim(tx, "req-A", HASH), Ok(Claimed::Prior(_))),
            db.sagas.load(tx, "shipping", "order-1").unwrap().is_some(),
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
            Case {
                name: "commit_lands_every_seam_together",
                commit: true,
                want: EVERYTHING,
            },
            Case {
                name: "rollback_leaves_nothing_behind_on_any_seam",
                commit: false,
                want: NOTHING,
            },
        ];
        for c in cases {
            let db = db();
            let tx = db.tr.begin();
            write_everything(&db, tx);
            // Read-your-writes: the transaction sees its own staged writes, the outbox excepted (a
            // staged append is not a row to publish). Nobody else sees any of it yet.
            assert_eq!(visible(&db, tx), (1, 0, true, true, true, true), "{}: own view", c.name);
            // `visible` claims the key under Tx::auto_commit() and must lose: the open transaction holds it.
            assert_eq!(
                visible(&db, Tx::auto_commit()),
                (0, 0, false, false, true, false),
                "{}: outside view",
                c.name
            );
            let other = db.tr.begin();
            assert_eq!(
                visible(&db, other),
                (0, 0, false, false, true, false),
                "{}: another transaction",
                c.name
            );
            assert!(db.tr.rollback(other));

            if c.commit {
                db.tr.commit(tx).expect(c.name);
            } else {
                assert!(db.tr.rollback(tx), "{}", c.name);
            }
            assert_eq!(visible(&db, Tx::auto_commit()), c.want, "{}: after", c.name);
            // The token is spent either way.
            assert_eq!(
                db.tr.commit(tx),
                Err(unknown_tx(tx)),
                "{}: a closed token cannot commit",
                c.name
            );
            assert!(
                !db.tr.rollback(tx),
                "{}: a closed token has nothing to roll back",
                c.name
            );
        }
    }

    #[test]
    fn a_held_key_conflicts_until_the_holder_ends() {
        // Every seam refuses a second writer of a key an open transaction staged, reporting the
        // committed version, since the holder's may yet roll back; once the holder rolls back the
        // key is free again.
        struct Case {
            name: &'static str,
            write: fn(&Db, Tx) -> Result<(), Error>,
        }
        let cases = [
            Case {
                name: "event stream",
                write: |db, tx| {
                    db.events
                        .append(tx, "invoice-7", 0, vec![draft("evt-1", Some(1))], RECORDED_AT)
                        .map(|_| ())
                },
            },
            Case {
                name: "projection row",
                write: |db, tx| db.projections.save(tx, "p", "user-1", 0, doc()).map(|_| ()),
            },
            Case {
                name: "saga instance",
                write: |db, tx| {
                    db.sagas
                        .save(tx, "shipping", "order-1", 0, state(), String::new(), vec![])
                        .map(|_| ())
                },
            },
            Case {
                name: "saga completion",
                write: |db, tx| db.sagas.complete(tx, "shipping", "order-1", 0, vec![]).map(|_| ()),
            },
        ];
        for c in cases {
            let db = db();
            let holder = db.tr.begin();
            write_everything(&db, holder);
            let want = Err(Error::concurrency(0, 0));
            assert_eq!(
                (c.write)(&db, Tx::auto_commit()),
                want,
                "{}: auto-commit writer",
                c.name
            );
            let other = db.tr.begin();
            assert_eq!((c.write)(&db, other), want, "{}: another transaction", c.name);
            assert!(db.tr.rollback(other));
            assert!(db.tr.rollback(holder));
            assert_eq!(
                (c.write)(&db, Tx::auto_commit()),
                Ok(()),
                "{}: free once the holder rolled back",
                c.name
            );
        }
    }

    const DONE: &[u8] = br#"{"payload_hash":11234062831410934249,"result":42,"error":null,"pending":false}"#;

    fn pending() -> Vec<u8> {
        format!(r#"{{"payload_hash":{HASH},"result":null,"error":null,"error_code":null,"pending":true}}"#).into_bytes()
    }

    #[test]
    fn a_claim_on_a_held_key_reads_pending_whatever_the_holder_staged() {
        // The holder completed its claim but has not committed: its outcome is not final, so a
        // second claimant must wait (InProgress), not replay a result that may yet roll back.
        let db = db();
        let holder = db.tr.begin();
        let handle = won(db.claims.claim(holder, "req-A", HASH));
        db.claims.complete(holder, "req-A", &handle, DONE).unwrap();

        assert_eq!(prior(db.claims.claim(Tx::auto_commit(), "req-A", HASH)), pending());
        let other = db.tr.begin();
        assert_eq!(prior(db.claims.claim(other, "req-A", HASH)), pending());
        assert!(db.tr.rollback(other));

        // Once committed the real outcome is readable; a later claimant replays it.
        db.tr.commit(holder).unwrap();
        assert_eq!(prior(db.claims.claim(Tx::auto_commit(), "req-A", HASH)), DONE);
    }

    #[test]
    fn complete_and_release_refuse_a_key_another_transaction_holds() {
        // The holder's commit would overwrite a `complete` written elsewhere, and a losing
        // claimant's `release` would free the winner's claim. A release of a claim nothing committed
        // holds nothing, so the same two writes are allowed once the holder has given it up. A
        // release of a committed claim is a staged delete, and holds the key like a row lock.
        struct Case {
            name: &'static str,
            /// Commit a pending claim on `req-A` before either transaction opens; its handle is the
            /// one both transactions settle with.
            seed: bool,
            /// What the holder stages for `req-A` before the write under test; returns the handle.
            stage: fn(&Db, Tx, Vec<u8>) -> Vec<u8>,
            write: fn(&Db, Tx, &[u8]) -> Result<(), Error>,
            expected: Result<(), Error>,
            /// Whether `req-A` is claimed once both transactions have committed, in either order.
            claimed_after: bool,
        }
        let held = Err(held("idempotency key", "req-A"));
        let claim = |db: &Db, tx: Tx, _: Vec<u8>| won(db.claims.claim(tx, "req-A", HASH));
        let claim_then_release = |db: &Db, tx: Tx, _: Vec<u8>| {
            let handle = won(db.claims.claim(tx, "req-A", HASH));
            db.claims
                .release(tx, "req-A", &handle)
                .expect("the holder releases its own claim");
            handle
        };
        let release_seed = |db: &Db, tx: Tx, seed: Vec<u8>| {
            db.claims
                .release(tx, "req-A", &seed)
                .expect("the holder deletes the committed claim");
            seed
        };
        let cases = [
            Case {
                name: "complete_on_a_held_key_conflicts",
                seed: false,
                stage: claim,
                write: |db, tx, handle| db.claims.complete(tx, "req-A", handle, DONE),
                expected: held.clone(),
                claimed_after: true,
            },
            Case {
                name: "release_on_a_held_key_conflicts",
                seed: false,
                stage: claim,
                write: |db, tx, handle| db.claims.release(tx, "req-A", handle),
                expected: held.clone(),
                claimed_after: true,
            },
            Case {
                // The released claim's handle names nothing any more: the writer's complete is
                // refused by the fence, not by the hold, and nothing lands.
                name: "complete_after_an_uncommitted_claim_was_released_is_fenced",
                seed: false,
                stage: claim_then_release,
                write: |db, tx, handle| db.claims.complete(tx, "req-A", handle, DONE),
                expected: Err(edax_cqrs::lapsed_claim("req-A")),
                claimed_after: false,
            },
            Case {
                name: "release_after_an_uncommitted_claim_was_released_is_a_no_op",
                seed: false,
                stage: claim_then_release,
                write: |db, tx, handle| db.claims.release(tx, "req-A", handle),
                expected: Ok(()),
                claimed_after: false,
            },
            Case {
                // A staged delete of a committed claim holds the key: a second writer's record
                // would otherwise be deleted by the holder's commit.
                name: "complete_over_a_staged_delete_of_a_committed_claim_conflicts",
                seed: true,
                stage: release_seed,
                write: |db, tx, handle| db.claims.complete(tx, "req-A", handle, DONE),
                expected: held.clone(),
                claimed_after: false,
            },
            Case {
                name: "release_over_a_staged_delete_of_a_committed_claim_conflicts",
                seed: true,
                stage: release_seed,
                write: |db, tx, handle| db.claims.release(tx, "req-A", handle),
                expected: held,
                claimed_after: false,
            },
        ];
        for case in cases {
            for holder_commits_first in [true, false] {
                let name = format!("{} (holder commits first: {holder_commits_first})", case.name);
                let db = db();
                let seed = if case.seed {
                    won(db.claims.claim(Tx::auto_commit(), "req-A", HASH))
                } else {
                    Vec::new()
                };
                let holder = db.tr.begin();
                let handle = (case.stage)(&db, holder, seed);
                let writer = db.tr.begin();
                assert_eq!((case.write)(&db, writer, &handle), case.expected, "{name}");
                let order = if holder_commits_first {
                    [holder, writer]
                } else {
                    [writer, holder]
                };
                for tx in order {
                    db.tr.commit(tx).expect("each transaction settles");
                }
                let claimed = matches!(db.claims.claim(Tx::auto_commit(), "req-A", HASH), Ok(Claimed::Prior(_)));
                assert_eq!(claimed, case.claimed_after, "{name}: committed claim");
            }
        }
    }

    #[test]
    fn a_claim_after_releasing_an_uncommitted_claim_reads_what_another_winner_committed() {
        // tx1 claims and releases; tx2 wins the key and commits a result. tx1's re-claim must
        // replay tx2's record: its own release holds nothing and must not shadow it.
        let db = db();
        let first = db.tr.begin();
        let handle = won(db.claims.claim(first, "req-A", HASH));
        db.claims.release(first, "req-A", &handle).unwrap();
        let second = db.tr.begin();
        let winner = won(db.claims.claim(second, "req-A", HASH));
        db.claims.complete(second, "req-A", &winner, DONE).unwrap();
        db.tr.commit(second).unwrap();
        assert_eq!(prior(db.claims.claim(first, "req-A", HASH)), DONE, "replays the winner");
        db.tr.commit(first).unwrap();
        assert_eq!(
            prior(db.claims.claim(Tx::auto_commit(), "req-A", HASH)),
            DONE,
            "the winner's record stands"
        );
    }

    #[test]
    fn a_claim_over_a_staged_delete_of_a_committed_claim_reads_pending() {
        // The deleter may still roll back, so a claimant waits rather than winning a key the
        // committed claim still guards.
        let db = db();
        let handle = won(db.claims.claim(Tx::auto_commit(), "req-A", HASH));
        let deleter = db.tr.begin();
        db.claims.release(deleter, "req-A", &handle).unwrap();
        assert_eq!(prior(db.claims.claim(Tx::auto_commit(), "req-A", HASH)), pending());
        // The deleter itself freed the key, so its own re-claim wins it.
        let again = won(db.claims.claim(deleter, "req-A", HASH));
        assert_ne!(again, handle, "a new claim gets a new handle");
        assert!(db.tr.rollback(deleter));
        assert_eq!(
            prior(db.claims.claim(Tx::auto_commit(), "req-A", HASH)),
            pending(),
            "the rollback kept the claim"
        );
        db.claims
            .complete(Tx::auto_commit(), "req-A", &handle, DONE)
            .expect("and its handle still settles it");
    }

    #[test]
    fn a_staged_delete_does_not_outlive_the_claim_it_deletes() {
        // tx1 releases the committed claim, the claim lapses, and tx2 takes the key over and commits.
        // tx1's commit must leave tx2's claim alone, or a third claimant wins while tx2's handler runs.
        let db = db();
        let lapsed = won(db.claims.claim(Tx::auto_commit(), "req-A", HASH));
        let first = db.tr.begin();
        db.claims.release(first, "req-A", &lapsed).unwrap();
        assert!(db.claims.expire("req-A"));
        let second = db.tr.begin();
        let taker = won(db.claims.claim(second, "req-A", HASH));
        db.tr.commit(second).unwrap();
        db.tr.commit(first).unwrap();
        assert_eq!(
            prior(db.claims.claim(Tx::auto_commit(), "req-A", HASH)),
            pending(),
            "the taker's claim stands"
        );
        db.claims
            .complete(Tx::auto_commit(), "req-A", &taker, DONE)
            .expect("and its handle still settles it");
    }

    #[test]
    fn the_fence_settles_only_the_claim_a_handle_names() {
        // The fence, case by case, as the Redis complete and release scripts decide it. `key`
        // holds what `seed` left; the write carries the handle of the first claim.
        const OTHER: &[u8] = br#"{"payload_hash":11234062831410934249,"result":7,"error":null,"pending":false}"#;
        #[derive(Clone, Copy)]
        enum Seed {
            /// The first claim, still pending.
            Claimed,
            /// The first claim settled with `DONE`.
            Settled,
            /// The first claim expired: nothing is held.
            Expired,
            /// The first claim expired and a second claim took the key over.
            TakenOver,
            /// Never claimed: the handle names no claim at all.
            Absent,
        }
        struct Case {
            name: &'static str,
            seed: Seed,
            /// `Some(record)`: complete with it. `None`: release.
            complete: Option<&'static [u8]>,
            expected: Result<(), Error>,
            /// What the key reads afterwards: `None` free, else the record a claimant gets.
            after: Option<Vec<u8>>,
        }
        let lapsed = Err(edax_cqrs::lapsed_claim("req-A"));
        let cases = [
            Case {
                name: "complete_of_the_live_claim_settles_it",
                seed: Seed::Claimed,
                complete: Some(DONE),
                expected: Ok(()),
                after: Some(DONE.to_vec()),
            },
            Case {
                name: "release_of_the_live_claim_frees_the_key",
                seed: Seed::Claimed,
                complete: None,
                expected: Ok(()),
                after: None,
            },
            Case {
                name: "a_repeated_complete_is_idempotent",
                seed: Seed::Settled,
                complete: Some(DONE),
                expected: Ok(()),
                after: Some(DONE.to_vec()),
            },
            Case {
                name: "complete_with_another_record_over_a_settled_key_is_refused",
                seed: Seed::Settled,
                complete: Some(OTHER),
                expected: lapsed.clone(),
                after: Some(DONE.to_vec()),
            },
            Case {
                name: "release_after_complete_keeps_the_record",
                seed: Seed::Settled,
                complete: None,
                expected: Ok(()),
                after: Some(DONE.to_vec()),
            },
            Case {
                name: "complete_of_an_expired_claim_is_refused",
                seed: Seed::Expired,
                complete: Some(DONE),
                expected: lapsed.clone(),
                after: None,
            },
            Case {
                name: "release_of_an_expired_claim_is_a_no_op",
                seed: Seed::Expired,
                complete: None,
                expected: Ok(()),
                after: None,
            },
            Case {
                name: "a_stale_owners_complete_is_refused_after_a_takeover",
                seed: Seed::TakenOver,
                complete: Some(DONE),
                expected: lapsed.clone(),
                after: Some(pending()),
            },
            Case {
                name: "a_stale_owners_release_keeps_the_new_claim",
                seed: Seed::TakenOver,
                complete: None,
                expected: Ok(()),
                after: Some(pending()),
            },
            Case {
                name: "complete_of_a_key_never_claimed_is_refused",
                seed: Seed::Absent,
                complete: Some(DONE),
                expected: lapsed,
                after: None,
            },
            Case {
                name: "release_of_a_key_never_claimed_is_a_no_op",
                seed: Seed::Absent,
                complete: None,
                expected: Ok(()),
                after: None,
            },
        ];
        for case in cases {
            let db = db();
            let first = match case.seed {
                Seed::Absent => 1i64.to_be_bytes().to_vec(),
                _ => won(db.claims.claim(Tx::auto_commit(), "req-A", HASH)),
            };
            match case.seed {
                Seed::Settled => db
                    .claims
                    .complete(Tx::auto_commit(), "req-A", &first, DONE)
                    .expect("settles"),
                Seed::Expired => assert!(db.claims.expire("req-A"), "{}: the claim lapses", case.name),
                Seed::TakenOver => {
                    assert!(db.claims.expire("req-A"));
                    assert_ne!(
                        won(db.claims.claim(Tx::auto_commit(), "req-A", HASH)),
                        first,
                        "{}: a new handle",
                        case.name
                    );
                }
                Seed::Claimed | Seed::Absent => {}
            }
            let got = match case.complete {
                Some(record) => db.claims.complete(Tx::auto_commit(), "req-A", &first, record),
                None => db.claims.release(Tx::auto_commit(), "req-A", &first),
            };
            assert_eq!(got, case.expected, "{}", case.name);
            let after = match db.claims.claim(Tx::auto_commit(), "req-A", HASH) {
                Ok(Claimed::Won(_)) => None,
                Ok(Claimed::Prior(record)) => Some(record),
                Err(e) => panic!("{}: {e}", case.name),
            };
            assert_eq!(after, case.after, "{}: what the key holds", case.name);
        }
    }

    #[test]
    fn expire_lapses_only_committed_state() {
        // A staged claim is not committed, so it does not lapse; an absent key has nothing to lapse.
        let db = db();
        assert!(!db.claims.expire("req-A"), "nothing to lapse");
        let tx = db.tr.begin();
        let handle = won(db.claims.claim(tx, "req-A", HASH));
        assert!(
            !db.claims.expire("req-A"),
            "the open transaction's claim is not committed"
        );
        db.claims
            .complete(tx, "req-A", &handle, DONE)
            .expect("its handle still settles it");
        db.tr.commit(tx).unwrap();
        assert!(
            db.claims.expire("req-A"),
            "a settled record lapses too, as retention would"
        );
        won(db.claims.claim(Tx::auto_commit(), "req-A", HASH));
    }

    /// One step of an interleaved history over one idempotency key: which of three transactions
    /// acts, and what it does. `Commit` and `Rollback` end the transaction and open a fresh one in
    /// its slot, so a history keeps three writers live.
    #[derive(Debug, Clone, Copy)]
    enum Step {
        Claim(usize),
        Complete(usize),
        Release(usize),
        Commit(usize),
        Rollback(usize),
        /// The committed claim or record lapses, as a lease or a retention TTL would.
        Expire,
    }

    fn step() -> impl proptest::strategy::Strategy<Value = Step> {
        use proptest::prelude::*;
        (0..6u8, 0..3usize).prop_map(|(kind, tx)| match kind {
            0 => Step::Claim(tx),
            1 => Step::Complete(tx),
            2 => Step::Release(tx),
            3 => Step::Commit(tx),
            4 => Step::Rollback(tx),
            _ => Step::Expire,
        })
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig { cases: 512, ..Default::default() })]

        /// Isolation and fencing of the claim, over random interleavings of three transactions on one
        /// key, with the committed claim lapsing at random:
        /// - a claim is won only over a key with no committed record, or one this transaction
        ///   deleted itself (a stolen win runs the handler twice);
        /// - a commit removes a committed record only if that transaction released it while it was
        ///   committed, and it has not lapsed since (an erased winner's record lets the next dispatch
        ///   run the handler again);
        /// - a complete lands only with the handle of the claim the key holds as the transaction sees
        ///   it (a stale owner's record over a newer claim lets two outcomes land).
        #[test]
        fn claims_stay_isolated_across_interleaved_transactions(steps in proptest::collection::vec(step(), 1..40)) {
            const KEY: &str = "req-A";
            const DONE: &[u8] = br#"{"payload_hash":11234062831410934249,"result":42,"error":null,"pending":false}"#;
            let db = db();
            let committed = |db: &Db| lock(&db.tr.state).committed.claims.get(KEY).cloned();
            let seen = |db: &Db, tx: Tx| {
                let state = lock(&db.tr.state);
                match state.open.get(&tx).and_then(|own| own.claims.get(KEY)) {
                    Some(staged) => staged.clone(),
                    None => state.committed.claims.get(KEY).cloned(),
                }
            };
            let mut txs = [db.tr.begin(), db.tr.begin(), db.tr.begin()];
            // Per slot: the handle of the last claim it won, which it settles with; empty before any.
            let mut handles: [Vec<u8>; 3] = Default::default();
            // Per slot: whether its open transaction released the key while a record was committed.
            let mut deleted_committed = [false; 3];
            for step in steps {
                match step {
                    Step::Claim(i) => {
                        let before = committed(&db);
                        if let Ok(Claimed::Won(handle)) = db.claims.claim(txs[i], KEY, HASH) {
                            proptest::prop_assert!(
                                before.is_none() || deleted_committed[i],
                                "won a claim over a committed record it never released"
                            );
                            handles[i] = handle;
                        }
                    }
                    Step::Complete(i) => {
                        let held = seen(&db, txs[i]);
                        if db.claims.complete(txs[i], KEY, &handles[i], DONE).is_ok() {
                            let named = held.as_ref().is_some_and(|h| matches!(h.owner, Owner::Claim(id) if id.to_be_bytes() == *handles[i]));
                            let same = held.as_ref().is_some_and(|h| {
                                matches!(h.owner, Owner::SettledBy(Some(id)) if id.to_be_bytes() == *handles[i]) && h.record == DONE
                            });
                            proptest::prop_assert!(named || same, "a complete landed over a claim its handle does not name");
                        }
                    }
                    Step::Release(i) => {
                        let held = committed(&db).is_some_and(|h| matches!(h.owner, Owner::Claim(id) if id.to_be_bytes() == *handles[i]));
                        if db.claims.release(txs[i], KEY, &handles[i]).is_ok() && held {
                            deleted_committed[i] = true;
                        }
                    }
                    Step::Expire => {
                        // What every staged delete would delete lapsed, so none of them deletes anything.
                        db.claims.expire(KEY);
                        deleted_committed = [false; 3];
                    }
                    Step::Commit(i) => {
                        let before = committed(&db);
                        db.tr.commit(txs[i]).expect("an open token commits");
                        if before.is_some() && committed(&db).is_none() {
                            proptest::prop_assert!(deleted_committed[i], "a commit erased a record it never released");
                        }
                        txs[i] = db.tr.begin();
                        deleted_committed[i] = false;
                    }
                    Step::Rollback(i) => {
                        proptest::prop_assert!(db.tr.rollback(txs[i]));
                        txs[i] = db.tr.begin();
                        deleted_committed[i] = false;
                    }
                }
            }
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
        assert_eq!(
            db.tr.open_count(),
            0,
            "a second rollback of a settled token changes nothing"
        );
    }

    #[test]
    fn writes_within_one_transaction_compose() {
        // Read-your-writes on the version arithmetic: a second append continues the staged tail, a
        // release frees the key for the same transaction, and a save-then-delete leaves no row.
        let db = db();
        let tx = db.tr.begin();
        db.events
            .append(tx, "invoice-7", 0, vec![draft("evt-1", Some(1))], RECORDED_AT)
            .unwrap();
        db.events
            .append(tx, "invoice-7", 1, vec![draft("evt-2", Some(2))], RECORDED_AT)
            .unwrap();
        assert_eq!(
            db.events
                .append(tx, "invoice-7", 1, vec![draft("evt-3", Some(2))], RECORDED_AT),
            Err(Error::concurrency(1, 2)),
            "the staged tail is the version the transaction sees"
        );
        let handle = won(db.claims.claim(tx, "req-A", HASH));
        db.claims.release(tx, "req-A", &handle).unwrap();
        won(db.claims.claim(tx, "req-A", HASH));
        assert_eq!(
            db.sagas
                .save(tx, "shipping", "order-1", 0, state(), String::new(), vec![]),
            Ok(1)
        );
        assert_eq!(db.sagas.complete(tx, "shipping", "order-1", 1, vec![]), Ok(2));
        assert!(
            db.sagas
                .load(tx, "shipping", "order-1")
                .unwrap()
                .is_some_and(|row| row.value == SagaRow::Completed),
            "completed in the same transaction"
        );
        assert!(
            db.sagas
                .save(tx, "shipping", "order-1", 0, state(), String::new(), vec![])
                .is_err(),
            "and never started again"
        );
        // An empty batch is a version check only: it stages no tail and holds nothing.
        db.events.append(tx, "invoice-8", 0, vec![], RECORDED_AT).unwrap();
        db.events
            .append(
                Tx::auto_commit(),
                "invoice-8",
                0,
                vec![draft_on("invoice-8", "evt-9", Some(1))],
                RECORDED_AT,
            )
            .expect("not held");

        db.tr.commit(tx).unwrap();
        assert_eq!(
            ids(&db.events.load(Tx::auto_commit(), "invoice-7", 0).unwrap()),
            ["evt-1", "evt-2"]
        );
        assert_eq!(
            ids(&db.events.unpublished(10)),
            ["evt-9", "evt-1", "evt-2"],
            "outbox: commit order, then append order"
        );
        assert!(db
            .sagas
            .load(Tx::auto_commit(), "shipping", "order-1")
            .unwrap()
            .is_some());
    }

    #[test]
    fn an_unknown_token_is_refused_by_every_method() {
        // A token that was never begun, one from another transactor, and one already closed all read
        // as unknown: tokens are process-unique, so none can alias a live transaction here.
        let db = db();
        let foreign = MemoryTransactor::new().begin();
        let closed = db.tr.begin();
        db.tr.commit(closed).unwrap();
        for (name, tx) in [
            ("never begun", Tx::from_raw(12_345)),
            ("another transactor", foreign),
            ("already closed", closed),
        ] {
            let want: Result<(), Error> = Err(unknown_tx(tx));
            assert_eq!(db.events.load(tx, "invoice-7", 0).map(|_| ()), want, "{name}: load");
            assert_eq!(
                db.events.append(tx, "invoice-7", 0, vec![], RECORDED_AT).map(|_| ()),
                want,
                "{name}: append"
            );
            assert_eq!(
                db.events.load_snapshot(tx, "invoice-7").map(|_| ()),
                want,
                "{name}: load_snapshot"
            );
            assert_eq!(
                db.events
                    .save_snapshot(tx, "invoice-7", "Invoice", 1, vec![])
                    .map(|_| ()),
                want,
                "{name}: save_snapshot"
            );
            assert_eq!(db.projections.get(tx, "p", "user-1").map(|_| ()), want, "{name}: get");
            assert_eq!(
                db.projections.save(tx, "p", "user-1", 0, doc()).map(|_| ()),
                want,
                "{name}: save"
            );
            assert_eq!(
                db.projections
                    .get_many(tx, "p", &edax_cqrs::Query::default())
                    .map(|_| ()),
                want,
                "{name}: get_many"
            );
            assert_eq!(db.claims.claim(tx, "req-A", HASH).map(|_| ()), want, "{name}: claim");
            assert_eq!(
                db.claims.complete(tx, "req-A", b"", b"{}").map(|_| ()),
                want,
                "{name}: complete"
            );
            assert_eq!(db.claims.release(tx, "req-A", b"").map(|_| ()), want, "{name}: release");
            assert_eq!(db.claims.forget(tx, "req-A"), want, "{name}: forget");
            assert_eq!(
                db.sagas.load(tx, "shipping", "order-1").map(|_| ()),
                want,
                "{name}: saga load"
            );
            assert_eq!(
                db.sagas
                    .save(tx, "shipping", "order-1", 0, state(), String::new(), vec![])
                    .map(|_| ()),
                want,
                "{name}: saga save"
            );
            assert_eq!(
                db.sagas.complete(tx, "shipping", "order-1", 0, vec![]).map(|_| ()),
                want,
                "{name}: saga complete"
            );
            assert_eq!(db.tr.commit(tx).map(|_| ()), want, "{name}: commit");
            assert!(!db.tr.rollback(tx), "{name}: rollback");
            assert!(unknown_tx(tx).is_terminal(), "{name}: a wiring mistake is not retried");
        }
        assert_eq!(
            visible(&db, Tx::auto_commit()),
            (0, 0, false, false, false, false),
            "nothing landed"
        );
    }

    #[test]
    fn a_standalone_store_and_its_transactor_share_one_database() {
        let store = MemoryEventStore::new();
        let tx = store.transactor().begin();
        store
            .append(tx, "invoice-7", 0, vec![draft("evt-1", Some(1))], RECORDED_AT)
            .unwrap();
        assert!(store.load(Tx::auto_commit(), "invoice-7", 0).unwrap().is_empty());
        store.transactor().commit(tx).unwrap();
        assert_eq!(ids(&store.load(Tx::auto_commit(), "invoice-7", 0).unwrap()), ["evt-1"]);
        // Two standalone stores are two databases: a token of one is unknown to the other.
        let other = MemoryProjectionStore::new();
        let tx = store.transactor().begin();
        assert_eq!(other.get(tx, "p", "user-1"), Err(unknown_tx(tx)));
    }

    /// Concurrent transactions on disjoint keys commit independently; on one key exactly one wins
    /// and the losers read the winner's version. All of it under the single lock, taken per call.
    #[test]
    fn concurrent_transactions_serialize_on_keys_not_on_the_database() {
        let db = db();
        let results: Vec<Result<(), Error>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8)
                .map(|i| {
                    let db = &db;
                    s.spawn(move || {
                        let tx = db.tr.begin();
                        // Every thread writes its own projection row and races on one stream.
                        db.projections.save(tx, "p", &format!("user-{i}"), 0, doc())?;
                        let raced = db.events.append(
                            tx,
                            "invoice-7",
                            0,
                            vec![draft(&format!("evt-{i}"), Some(1))],
                            RECORDED_AT,
                        );
                        match raced {
                            Ok(_) => db.tr.commit(tx),
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
        assert_eq!(
            results.iter().filter(|r| r.is_ok()).count(),
            1,
            "one writer of the stream wins: {results:?}"
        );
        for e in results.iter().filter_map(|r| r.as_ref().err()) {
            // `stored 0` while the winner is still open, `stored 1` once it committed.
            assert_eq!(e.code, edax_core::ErrorCode::Concurrency, "{e}");
        }
        assert_eq!(db.events.load(Tx::auto_commit(), "invoice-7", 0).unwrap().len(), 1);
        let page = db
            .projections
            .get_many(Tx::auto_commit(), "p", &edax_cqrs::Query::default())
            .unwrap();
        assert_eq!(
            page.rows.len(),
            1,
            "the losers rolled their rows back with the lost race"
        );
    }
}

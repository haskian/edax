//! The transaction contract, executed: what `README.md` promises of one unit of work every store
//! participates in, and what the `IdempotencyStore` and `Transactor` docs promise of it. Four
//! tests, one per clause, over the public API — a `Repository`, a `CommandBus` behind
//! `idempotent_in`, a projection store, a saga store, all over one `MemoryTransactor`. The Python
//! suite runs the same four (`tests/test_transactor.py`), because the contract is cross-language.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "an integration test's helpers panic as its failure report, like the tests that call them"
)]

mod support;

use support::Shared;

use std::sync::Arc;

use edax::{
    idempotent_in, Aggregate, AggregateCodec, Command, CommandBus, Entity, Error, ErrorCode, EventCodec, EventStore, HandlerError,
    IdempotencyStore, MemoryEventStore, MemoryProjectionStore, MemorySagaStore, MemoryTransactor, MemoryTx,
    Outbox, ProjectionStore, Repository, Root, SagaStore, Transactor,
};
use edax_event::Event;
use serde::Serialize;
use serde_json::json;

// ---- a tiny domain ---------------------------------------------------------

#[derive(Clone)]
struct Deposited(i64);

struct Account {
    root: Root<Deposited>,
    balance: i64,
}

impl Aggregate for Account {
    type Event = Deposited;
    fn root(&self) -> &Root<Deposited> {
        &self.root
    }
    fn root_mut(&mut self) -> &mut Root<Deposited> {
        &mut self.root
    }
    fn transition(&mut self, event: &Deposited) {
        self.balance += event.0;
    }
}

struct AccountEvents;
impl EventCodec for AccountEvents {
    type Domain = Deposited;
    fn marshal(&self, e: &Deposited) -> Result<Event, Error> {
        Ok(Event { name: "Deposited".into(), version: 1, data: e.0.to_string().into_bytes() })
    }
    fn unmarshal(&self, raw: &Event) -> Result<Deposited, Error> {
        let text = std::str::from_utf8(&raw.data).map_err(Error::codec)?;
        text.parse().map(Deposited).map_err(Error::codec)
    }
}

struct AccountCodec;
impl AggregateCodec for AccountCodec {
    type Aggregate = Account;
    fn type_name(&self) -> &str {
        "Account"
    }
    fn load(&self, id: &str, version: u64, data: Option<&[u8]>) -> Result<Account, Error> {
        let balance = match data {
            Some(bytes) => std::str::from_utf8(bytes)
                .map_err(Error::codec)?
                .parse()
                .map_err(Error::codec)?,
            None => 0,
        };
        Ok(Account { root: Root::new(id, version), balance })
    }
    fn dump(&self, agg: &Account) -> Result<Vec<u8>, Error> {
        Ok(agg.balance.to_string().into_bytes())
    }
}

#[derive(Serialize)]
struct Deposit {
    account_id: String,
    amount: i64,
    request_id: String,
}

impl Command for Deposit {
    fn command_type(&self) -> &str {
        "Deposit"
    }
}

fn deposit(request_id: &str, amount: i64) -> Deposit {
    Deposit { account_id: "acct-1".into(), amount, request_id: request_id.into() }
}

// ---- the rig ---------------------------------------------------------------

type AccountRepo = Repository<MemoryEventStore, AccountEvents, AccountCodec>;

/// Every participant over one transactor, plus the bus whose guard reads the host's open handle.
struct Rig {
    db: MemoryTransactor,
    events: MemoryEventStore,
    projections: MemoryProjectionStore,
    sagas: MemorySagaStore<String>,
    claims: Arc<MemoryIdempotencyStore>,
    repo: Arc<AccountRepo>,
    /// The handle the guard and the handler enlist in: the host sets it for the dispatch.
    current: Arc<Shared<MemoryTx>>,
    calls: Arc<Shared<usize>>,
    bus: CommandBus,
}

type MemoryIdempotencyStore = edax::MemoryIdempotencyStore<i64>;

fn rig() -> Rig {
    let db = MemoryTransactor::new();
    let events = db.event_store();
    let repo = Arc::new(Repository::new(events.clone(), AccountEvents, AccountCodec).with_clock(next_id, || 0));
    let claims: Arc<MemoryIdempotencyStore> = Arc::new(db.idempotency_store());
    let current = Arc::new(Shared::new(MemoryTx::auto_commit()));
    let calls = Arc::new(Shared::default());

    let provider = Arc::clone(&current);
    let guard = idempotent_in(
        Arc::clone(&claims) as Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync>,
        |c: &Deposit| Some(c.request_id.clone()),
        move |_: &Deposit| provider.get(),
    );
    let (handler_repo, handler_tx, handler_calls) = (Arc::clone(&repo), Arc::clone(&current), Arc::clone(&calls));
    let handler: edax::Handler<Deposit, i64> = Box::new(move |cmd: &Deposit| {
        handler_calls.set(handler_calls.get() + 1);
        let mut tx = handler_tx.get();
        let mut account = handler_repo.load_in(&mut tx, &cmd.account_id)?;
        account.apply(Deposited(cmd.amount));
        handler_repo.save_in(&mut tx, &mut account)?;
        Ok(account.balance)
    });
    let mut bus = CommandBus::new();
    bus.register("Deposit", edax::wrap(handler, vec![guard])).expect("one handler");

    Rig { projections: db.projection_store(), sagas: db.saga_store(), db, events, claims, repo, current, calls, bus }
}

fn next_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(1);
    format!("e{}", N.fetch_add(1, Ordering::Relaxed))
}

impl Rig {
    /// Dispatch `cmd` with the guard and the handler enlisted in `tx`.
    fn dispatch_in(&self, tx: MemoryTx, cmd: &Deposit) -> Result<i64, HandlerError> {
        self.current.set(tx);
        let out = self.bus.dispatch(cmd);
        self.current.set(MemoryTx::auto_commit());
        out
    }

    /// Stage one write on every participant under `tx`, through the SDK's own orchestration.
    fn write_everything(&self, tx: &mut MemoryTx) -> Result<(), HandlerError> {
        self.dispatch_in(*tx, &deposit("req-A", 100))?;
        self.projections.save(tx, &mut Entity { id: "acct-1".into(), version: 0, document: json!({"balance": 100}) })?;
        self.sagas.save(tx, "order-1", &"open".to_string(), 0, "", &[])?;
        Ok(())
    }

    /// What a reader outside any transaction sees of every participant:
    /// `(events, outbox rows, snapshot, projection row, claim taken, saga instance)`.
    fn visible(&self) -> (usize, usize, bool, bool, bool, bool) {
        let mut tx = MemoryTx::auto_commit();
        let claim_taken = {
            // A losing claim proves the key is taken; a winning one must be undone, or the probe
            // itself would take the key.
            let probe = self.db.begin().expect("a probe transaction");
            let taken = !matches!(self.claims.claim(&mut { probe }, "req-A", 0), Ok(edax::Claim::Won(_)));
            self.db.rollback(probe);
            taken
        };
        (
            self.events.load(&mut tx, "acct-1", 0).expect("load").len(),
            self.events.unpublished(10).expect("outbox").len(),
            self.events.load_snapshot(&mut tx, "acct-1").expect("snapshot").is_some(),
            self.projections.get(&mut tx, "acct-1").expect("projection").is_some(),
            claim_taken,
            self.sagas.load(&mut tx, "order-1").expect("saga").is_some(),
        )
    }
}

const NOTHING: (usize, usize, bool, bool, bool, bool) = (0, 0, false, false, false, false);
const EVERYTHING: (usize, usize, bool, bool, bool, bool) = (1, 1, true, true, true, true);
/// Mid-transaction: no data is visible outside it, but the claim key is *held*, so a probe
/// claimant loses and reads pending. That is the one footprint a transaction shows before it
/// commits, and it is the point of an atomic claim: a concurrent dispatch of the same request must
/// wait for the winner's outcome, never run the handler a second time.
const HELD: (usize, usize, bool, bool, bool, bool) = (0, 0, false, false, true, false);

// ---- the four clauses --------------------------------------------------------

/// An error inside a unit of work leaves nothing behind: no events, no outbox rows, no snapshot, no
/// projection row, no idempotency claim, no saga instance.
#[test]
fn an_error_inside_a_transaction_leaves_nothing_behind() {
    let rig = rig();
    let out = rig.db.transaction(|tx| {
        rig.write_everything(tx)?;
        Err::<(), HandlerError>(Error::store("a later write in the same unit of work failed").into())
    });
    assert!(matches!(&out, Err(e) if e.downcast_ref::<Error>().is_some_and(|e| e.code == ErrorCode::Store)), "got {out:?}");
    assert_eq!(rig.calls.get(), 1, "the handler ran once");
    assert_eq!(rig.visible(), NOTHING);
}

/// A claim rolled back with its events releases the key: the next dispatch of the same command
/// runs the handler again, rather than replaying an outcome that never landed.
#[test]
fn a_rolled_back_claim_releases_the_key_so_the_next_dispatch_reruns_the_handler() {
    let rig = rig();
    let out = rig.db.transaction(|tx| {
        rig.dispatch_in(*tx, &deposit("req-A", 100))?;
        Err::<(), HandlerError>(Error::store("a later write in the same unit of work failed").into())
    });
    assert!(out.is_err());
    assert_eq!(rig.calls.get(), 1);

    let balance = rig.db.transaction(|tx| rig.dispatch_in(*tx, &deposit("req-A", 100))).expect("second dispatch");
    assert_eq!(rig.calls.get(), 2, "the handler ran again: no phantom cached result");
    assert_eq!(balance, 100, "and deposited once, not twice");
    assert_eq!(rig.events.load(&mut MemoryTx::auto_commit(), "acct-1", 0).expect("load").len(), 1);

    // Now the claim is committed: a third dispatch replays without running the handler.
    let replayed = rig.db.transaction(|tx| rig.dispatch_in(*tx, &deposit("req-A", 100))).expect("replay");
    assert_eq!((replayed, rig.calls.get()), (100, 2));
}

/// Inside a transaction a read sees that transaction's own uncommitted write; a reader outside it
/// sees nothing of it.
#[test]
fn a_transaction_reads_its_own_uncommitted_writes_which_nobody_else_sees() {
    let rig = rig();
    rig.db
        .transaction(|tx| {
            let mut account = rig.repo.load_in(tx, "acct-1")?;
            account.apply(Deposited(40));
            rig.repo.save_in(tx, &mut account)?;
            assert_eq!(rig.repo.load_in(tx, "acct-1")?.balance, 40, "read-your-writes through the repository");
            assert_eq!(rig.events.load(tx, "acct-1", 0)?.len(), 1, "read-your-writes through the store");
            let outside = rig.repo.load_in(&mut MemoryTx::auto_commit(), "acct-1")?;
            assert_eq!(outside.balance, 0, "an auto-commit read sees committed state");
            // `load` opens a unit of work of its own, which this open one on the same database refuses.
            assert!(rig.repo.load("acct-1").is_err_and(|e| e.code == ErrorCode::BadHandle), "a nested unit of work");
            assert_eq!(rig.visible(), NOTHING, "nothing is visible outside before the commit");
            Ok::<(), Error>(())
        })
        .expect("clean unit of work");
    assert_eq!(rig.repo.load("acct-1").expect("load").balance, 40, "visible once committed");
}

/// A commit makes every participant's write visible together: none before, all after.
#[test]
fn a_commit_makes_every_participants_write_visible_together() {
    let rig = rig();
    rig.db
        .transaction(|tx| {
            rig.write_everything(tx)?;
            assert_eq!(rig.visible(), HELD, "nothing landed before the commit; the claim key is held");
            Ok::<(), HandlerError>(())
        })
        .expect("clean unit of work");
    assert_eq!(rig.visible(), EVERYTHING);
    assert_eq!(rig.calls.get(), 1);
}

// ---- where the aggregate reset sits relative to the commit ------------------

/// The shipped store with one change: its commit lands nothing and reports a failure. Stands in for
/// a database that accepted every statement and then lost the transaction.
struct CommitFails(MemoryEventStore);

impl Transactor for CommitFails {
    type Tx = MemoryTx;

    fn begin(&self) -> Result<MemoryTx, Error> {
        self.0.begin()
    }

    fn commit(&self, tx: MemoryTx) -> Result<(), Error> {
        self.0.rollback(tx); // nothing lands
        Err(Error::store("commit failed; nothing landed"))
    }

    fn rollback(&self, tx: MemoryTx) {
        self.0.rollback(tx)
    }
}

impl EventStore for CommitFails {
    fn load(&self, tx: &mut MemoryTx, aggregate_id: &str, after: u64) -> Result<Vec<edax_event::EventEnvelope>, Error> {
        self.0.load(tx, aggregate_id, after)
    }

    fn load_snapshot(&self, tx: &mut MemoryTx, aggregate_id: &str) -> Result<Option<edax::Snapshot>, Error> {
        self.0.load_snapshot(tx, aggregate_id)
    }

    fn save_snapshot(&self, tx: &mut MemoryTx, snapshot: &edax::Snapshot) -> Result<(), Error> {
        self.0.save_snapshot(tx, snapshot)
    }

    fn append(
        &self,
        tx: &mut MemoryTx,
        aggregate_id: &str,
        expected_version: u64,
        events: &[edax_event::EventEnvelope],
    ) -> Result<(), Error> {
        self.0.append(tx, aggregate_id, expected_version, events)
    }
}

impl Outbox<edax_event::EventEnvelope> for CommitFails {
    fn unpublished(&self, limit: usize) -> Result<Vec<edax_event::EventEnvelope>, Error> {
        self.0.unpublished(limit)
    }

    fn mark_published(&self, ids: &[String]) -> Result<(), Error> {
        self.0.mark_published(ids)
    }
}

/// `Repository::save` owns the commit, so it waits for the commit before resetting: a commit that
/// lands nothing leaves the aggregate exactly as it was and the caller can save again. The Python
/// SDK draws the line in the same place (`Repository.save` resets after its `_enlist` block exits),
/// and `tests/test_store.py` runs this case.
#[test]
fn a_failed_commit_leaves_the_aggregate_holding_its_pending_events() {
    let db = MemoryTransactor::new();
    let inner = db.event_store(); // a second view on the same database, for reading back
    let repo = Repository::new(CommitFails(inner.clone()), AccountEvents, AccountCodec).with_clock(next_id, || 0);
    let mut account = Account { root: Root::new("acct-commit-fails", 0), balance: 0 };
    account.apply(Deposited(10));
    account.apply(Deposited(5));

    let err = repo.save(&mut account).expect_err("the commit fails");
    assert!(matches!(err, Error { code: ErrorCode::Store, .. }), "got {err:?}");
    assert_eq!(account.root().pending().len(), 2, "a failed commit must not consume the pending events");
    assert_eq!(account.root().version, 0);
    // Read through the honest view: the repository's own `load` would fail on the same commit.
    let stored = inner.load(&mut MemoryTx::auto_commit(), "acct-commit-fails", 0).expect("load");
    assert!(stored.is_empty(), "nothing landed");
}

/// The mirror case. With a caller-supplied handle the repository cannot wait for the commit — the
/// caller performs it after `save_in` returns — so the reset happens first and a rollback leaves an
/// aggregate that no longer holds its events. Reload it rather than saving it again.
#[test]
fn save_in_a_callers_transaction_resets_before_the_caller_commits() {
    let rig = rig();
    let mut account = Account { root: Root::new("acct-caller-tx", 0), balance: 0 };
    account.apply(Deposited(7));

    let out: Result<(), Error> = rig.db.transaction(|tx| {
        rig.repo.save_in(tx, &mut account)?;
        assert!(account.root().pending().is_empty(), "reset as soon as the store accepts the writes");
        Err(Error::store("a later write in the same unit of work failed"))
    });

    assert!(matches!(out, Err(Error { code: ErrorCode::Store, .. })), "got {out:?}");
    assert!(account.root().pending().is_empty(), "the rollback cannot put them back");
    assert_eq!(rig.repo.load("acct-caller-tx").expect("load").balance, 0, "nothing landed");
}

/// A repository call that opens its own unit of work, made inside a host's body on the same database,
/// would commit on its own clean exit, and the host's rollback could not take it back. It is
/// refused before anything begins, whichever way the host's body then ends, so nothing it wrote
/// commits apart from the host. The `_in` form joins the open unit of work instead.
#[test]
fn a_unit_of_work_opened_inside_a_host_body_on_the_same_database_is_refused() {
    type Nested = fn(&Rig) -> Result<(), Error>;
    struct Case {
        name: &'static str,
        nested: Nested,
    }
    let cases: [Case; 4] = [
        Case {
            name: "repository_save",
            nested: |rig| {
                let mut account = rig.repo.load_in(&mut MemoryTx::auto_commit(), "acct-1")?;
                account.apply(Deposited(40));
                rig.repo.save(&mut account).map(|_| ())
            },
        },
        Case { name: "repository_load", nested: |rig| rig.repo.load("acct-1").map(|_| ()) },
        Case { name: "the_event_store_as_transactor", nested: |rig| rig.events.transaction(|_| Ok::<_, Error>(())) },
        Case { name: "a_clone_of_the_transactor", nested: |rig| rig.db.clone().transaction(|_| Ok::<_, Error>(())) },
    ];
    for case in cases {
        for host_commits in [true, false] {
            let name = format!("{} (host commits: {host_commits})", case.name);
            let rig = rig();
            let out = rig.db.transaction(|tx| {
                let mut row = Entity { id: "acct-1".into(), version: 0, document: json!({"balance": 40}) };
                rig.projections.save(tx, &mut row)?;
                let refused = (case.nested)(&rig);
                assert!(refused.as_ref().is_err_and(|e| e.code == ErrorCode::BadHandle), "{name}: {refused:?}");
                if host_commits { Ok(()) } else { Err(Error::store("the host's later write failed")) }
            });
            assert_eq!(out.is_ok(), host_commits, "{name}");
            assert!(rig.events.load(&mut MemoryTx::auto_commit(), "acct-1", 0).unwrap().is_empty(), "{name}: no event committed");
            let row = rig.projections.get(&mut MemoryTx::auto_commit(), "acct-1").unwrap();
            assert_eq!(row.is_some(), host_commits, "{name}: the host's own write followed the host");
        }
    }

    // Two databases nest: a unit of work on another one commits on its own.
    let rig = rig();
    let other = MemoryTransactor::new();
    let other_events = other.event_store();
    let other_repo = Repository::new(other_events.clone(), AccountEvents, AccountCodec).with_clock(next_id, || 0);
    let out: Result<(), Error> = rig.db.transaction(|_| {
        let mut account = other_repo.load("acct-9")?;
        account.apply(Deposited(5));
        other_repo.save(&mut account)?;
        Err(Error::store("the outer database rolls back"))
    });
    assert!(out.is_err());
    assert_eq!(other_events.load(&mut MemoryTx::auto_commit(), "acct-9", 0).unwrap().len(), 1, "the other database committed");
}

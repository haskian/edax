//! Retry placement under a host transaction: a retry must re-run a whole unit of work.
//!
//! A second attempt inside one open transaction reads the first attempt's staged writes and applies
//! them again, and the one commit lands both. `retry` therefore refuses to run inside a
//! `transaction` body, and `retry_transaction` is the safe form: each attempt commits or rolls back
//! whole. Every case drives a real memory database and reads what committed back out of it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "an integration test's helpers panic as its failure report, like the tests that call them"
)]

mod support;

use support::{Shared, SCOPE};

use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use edax::{
    idempotent, idempotent_in, retry, retry_transaction, transaction, wrap, Command, CommandBus, Entity, Error,
    ErrorCode, HandlerError, IdempotencyStore, MemoryProjectionStore, MemoryTransactor, MemoryTx, Pacing,
    ProjectionStore,
};
use serde::Serialize;
use serde_json::json;

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

fn deposit() -> Deposit {
    Deposit { account_id: "acct-1".into(), amount: 100, request_id: "req-1".into() }
}

/// The committed balance of `acct-1`, read outside any unit of work.
fn balance(projections: &MemoryProjectionStore) -> i64 {
    let mut tx = MemoryTx::auto_commit();
    projections
        .get(&mut tx, "acct-1")
        .expect("the read lands")
        .map_or(0, |entity| entity.document["balance"].as_i64().unwrap_or(0))
}

/// Add `amount` to `acct-1` under `tx`: a load, then a versioned save, as a repository would.
fn add(projections: &MemoryProjectionStore, tx: &mut MemoryTx, amount: i64) -> Result<i64, Error> {
    let prior = projections.get(tx, "acct-1")?;
    let (version, current) = prior.map_or((0, 0), |e| (e.version, e.document["balance"].as_i64().unwrap_or(0)));
    let mut entity = Entity { id: "acct-1".into(), version, document: json!({ "balance": current + amount }) };
    projections.save(tx, &mut entity)?;
    Ok(current + amount)
}

/// One database, a bus over it and the handle slot a host publishes its open transaction through.
/// The handler deposits under the published handle, then fails transiently `blips` times after the
/// write, as a downstream call that times out would.
struct Rig {
    db: MemoryTransactor,
    projections: MemoryProjectionStore,
    handle: Arc<Shared<MemoryTx>>,
    runs: Arc<AtomicUsize>,
}

impl Rig {
    fn new() -> Self {
        let db = MemoryTransactor::new();
        Self {
            projections: db.projection_store(),
            db,
            handle: Arc::new(Shared::new(MemoryTx::auto_commit())),
            runs: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn bus(&self, middleware: Vec<edax::Middleware<Deposit, i64>>, blips: usize) -> CommandBus {
        let mut bus = CommandBus::new();
        let projections = self.projections.clone();
        let handle = Arc::clone(&self.handle);
        let runs = Arc::clone(&self.runs);
        let handler: edax::Handler<Deposit, i64> = Box::new(move |cmd: &Deposit| -> Result<i64, HandlerError> {
            let run = runs.fetch_add(1, Ordering::SeqCst);
            let mut tx = handle.get();
            let balance = add(&projections, &mut tx, cmd.amount)?;
            if run < blips {
                return Err(Box::new(Error::store("downstream call timed out after the save")));
            }
            Ok(balance)
        });
        bus.register("Deposit", wrap(handler, middleware)).expect("one handler per type");
        bus
    }

    fn guard(&self) -> edax::Middleware<Deposit, i64> {
        let handle = Arc::clone(&self.handle);
        let ledger = Arc::new(self.db.idempotency_store::<i64>());
        idempotent_in(
            ledger as Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync>,
            SCOPE,
            |cmd: &Deposit| Some(cmd.request_id.clone()),
            move |_: &Deposit| handle.get(),
        )
    }

    /// Dispatch inside a host unit of work that publishes its handle for the guard and the handler.
    /// The transaction body fails with the dispatch's own error, so its verdict reaches the caller.
    fn dispatch_in_host_transaction(&self, bus: &CommandBus) -> Result<i64, HandlerError> {
        transaction(&self.db, |tx| {
            self.handle.set(*tx);
            let result = bus.dispatch(&deposit());
            self.handle.set(MemoryTx::auto_commit());
            result
        })
    }
}

fn is_bad_handle(error: &HandlerError) -> bool {
    error.downcast_ref::<Error>().is_some_and(|e| e.code == ErrorCode::BadHandle)
}

#[test]
fn retry_refuses_to_run_inside_a_host_transaction() {
    // Without the refusal, both wirings would commit a balance of 200 for one deposit of 100: attempt
    // 2 re-reads attempt 1's staged write and applies the deposit on top of it.
    struct Case {
        name: &'static str,
        retry_outside_the_guard: bool,
    }
    for case in [
        Case { name: "retry_inside_idempotent_in", retry_outside_the_guard: false },
        Case { name: "retry_outside_idempotent_in", retry_outside_the_guard: true },
    ] {
        let rig = Rig::new();
        let chain = if case.retry_outside_the_guard { vec![retry(3, Pacing::instant()), rig.guard()] } else { vec![rig.guard(), retry(3, Pacing::instant())] };
        let bus = rig.bus(chain, 1);
        let refused = transaction(&rig.db, |tx| {
            rig.handle.set(*tx);
            let result = bus.dispatch::<_, i64>(&deposit());
            rig.handle.set(MemoryTx::auto_commit());
            Ok::<_, Error>(result)
        })
        .expect("the host's unit of work commits whatever the dispatch returned");
        assert!(refused.as_ref().is_err_and(is_bad_handle), "{}: {:?}", case.name, refused.err());
        assert_eq!(rig.runs.load(Ordering::SeqCst), 0, "{}: the handler never ran", case.name);
        assert_eq!(balance(&rig.projections), 0, "{}: nothing was deposited", case.name);
    }
}

#[test]
fn retry_outside_any_transaction_still_retries() {
    // No unit of work is open, so retry runs: at-least-once, every attempt's write lands on its own.
    let rig = Rig::new();
    let bus = rig.bus(vec![retry(2, Pacing::instant())], 1);
    assert_eq!(bus.dispatch::<_, i64>(&deposit()).ok(), Some(200), "attempt 2 sees attempt 1's committed write");
    assert_eq!(rig.runs.load(Ordering::SeqCst), 2);
}

#[test]
fn the_refusal_ends_with_the_transaction_body() {
    // The marker is scoped to the body: on return, on an error, and on a panic, a later retry runs.
    let rig = Rig::new();
    let bus = rig.bus(vec![retry(1, Pacing::instant())], 0);
    transaction(&rig.db, |_| Ok::<_, Error>(())).expect("a clean body commits");
    transaction(&rig.db, |_| Err::<(), _>(Error::store("body gave up"))).expect_err("the body's error");
    let panicked = catch_unwind(AssertUnwindSafe(|| transaction(&rig.db, |_| -> Result<(), Error> { panic!("boom") })));
    assert!(panicked.is_err(), "the panic keeps unwinding");
    assert_eq!(bus.dispatch::<_, i64>(&deposit()).ok(), Some(100), "outside every body, retry runs");
}

#[test]
fn a_nested_transaction_keeps_the_outer_one_marked() {
    // Two transactors over two databases nest; leaving the inner body must not unmark the outer one.
    let rig = Rig::new();
    let other = MemoryTransactor::new();
    let bus = rig.bus(vec![retry(1, Pacing::instant())], 0);
    let refused = transaction(&rig.db, |_| {
        transaction(&other, |_| Ok::<_, Error>(())).expect("the inner unit of work commits");
        Ok::<_, Error>(bus.dispatch::<_, i64>(&deposit()))
    })
    .expect("the outer unit of work commits");
    assert!(refused.as_ref().is_err_and(is_bad_handle), "{:?}", refused.err());
}

#[test]
fn retry_transaction_branches() {
    struct Case {
        name: &'static str,
        attempts: usize,
        /// Transient failures before the body succeeds (`usize::MAX`: never succeeds).
        blips: usize,
        /// The body fails terminally on its first run instead.
        terminal: bool,
        want_runs: usize,
        want_balance: i64,
        want_ok: bool,
    }
    let cases = [
        Case { name: "clean_first_attempt", attempts: 3, blips: 0, terminal: false, want_runs: 1, want_balance: 100, want_ok: true },
        // The failed attempt's deposit rolled back with it: one deposit lands, not two.
        Case { name: "transient_then_success", attempts: 3, blips: 1, terminal: false, want_runs: 2, want_balance: 100, want_ok: true },
        Case { name: "terminal_returns_at_once", attempts: 3, blips: 0, terminal: true, want_runs: 1, want_balance: 0, want_ok: false },
        Case { name: "attempts_exhausted", attempts: 2, blips: usize::MAX, terminal: false, want_runs: 2, want_balance: 0, want_ok: false },
        Case { name: "zero_attempts_clamps_to_one", attempts: 0, blips: usize::MAX, terminal: false, want_runs: 1, want_balance: 0, want_ok: false },
    ];
    for case in cases {
        let rig = Rig::new();
        let runs = Cell::new(0usize);
        let result = retry_transaction(&rig.db, case.attempts, &Pacing::instant(), |tx| {
            let run = runs.get();
            runs.set(run + 1);
            add(&rig.projections, tx, 100)?;
            if case.terminal {
                return Err(Error::codec("the deposit is malformed").into());
            }
            if run < case.blips {
                return Err(Error::store("downstream call timed out after the save").into());
            }
            Ok(())
        });
        assert_eq!(result.is_ok(), case.want_ok, "{}: {result:?}", case.name);
        assert_eq!(runs.get(), case.want_runs, "{}: runs", case.name);
        assert_eq!(balance(&rig.projections), case.want_balance, "{}: committed balance", case.name);
    }
}

#[test]
fn retry_transaction_retries_a_guarded_dispatch_once_per_outcome() {
    // The wiring `retry` refuses, done right: the guard and the handler share each attempt's handle,
    // the failed attempt rolls back its claim and its deposit together, and a redelivery replays.
    let rig = Rig::new();
    let bus = rig.bus(vec![rig.guard()], 1);
    let first = retry_transaction(&rig.db, 3, &Pacing::instant(), |tx| {
        rig.handle.set(*tx);
        let result = bus.dispatch::<_, i64>(&deposit());
        rig.handle.set(MemoryTx::auto_commit());
        result
    });
    assert_eq!(first.ok(), Some(100));
    assert_eq!(rig.runs.load(Ordering::SeqCst), 2, "the failed attempt and the one that landed");
    assert_eq!(balance(&rig.projections), 100, "one deposit committed");
    assert_eq!(rig.dispatch_in_host_transaction(&bus).ok(), Some(100), "a redelivery replays the record");
    assert_eq!(rig.runs.load(Ordering::SeqCst), 2, "and does not run the handler");
}

#[test]
fn retry_inside_idempotent_without_a_transaction_is_still_allowed() {
    // The auto-commit guard opens no unit of work, so the refusal does not apply.
    let rig = Rig::new();
    let ledger = Arc::new(rig.db.idempotency_store::<i64>());
    let guard = idempotent(
        ledger as Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync>,
        SCOPE,
        |cmd: &Deposit| Some(cmd.request_id.clone()),
    );
    let bus = rig.bus(vec![guard, retry(2, Pacing::instant())], 0);
    assert_eq!(bus.dispatch::<_, i64>(&deposit()).ok(), Some(100));
}

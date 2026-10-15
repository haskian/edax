//! The guard's unit-of-work guarantee, as a property over random histories.
//!
//! The invariant both SDKs owe, and the one whose absence silently drops work: **a key that reads
//! back as a completed success must have had its handler's write land**. Its converse separates
//! the two modes. Enlisted in the host's transaction, the claim and the write are one commit, so
//! the count is exactly one and a rollback leaves neither behind. Under the auto-commit handle the
//! two are independent writes, so a failure between them can leave work applied with no record —
//! at-least-once, which is why that mode may never be called effectively-once.
//!
//! The Python twin is `tests/test_unit_of_work.py`; both must agree, because the guarantee is a
//! property of the seam and not of one binding. `PROPTEST_CASES=4000` for a longer sweep.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "an integration test's helpers panic as its failure report, like the tests that call them"
)]

mod support;

use support::{Shared, SCOPE};

use std::sync::Arc;

use edax::Tenancy;
use edax::{
    idempotent_in, Claim, Command, CommandBus, Entity, Error, IdempotencyStore, MemoryProjectionStore,
    MemoryTransactor, MemoryTx, ProjectionStore, State, Transactor,
};
use edax_cqrs::claim_key;
use proptest::prelude::*;
use serde::Serialize;
use serde_json::json;

/// One guarded dispatch: which key, and how its handler ends.
#[derive(Debug, Clone, Copy)]
struct Op {
    key: u8,
    /// 0 succeeds, 1 fails transiently (the claim is released), 2 fails terminally (it is cached).
    outcome: u8,
    /// Whether the unit of work this dispatch runs in commits, or rolls back after the handler.
    commits: bool,
}

#[derive(Serialize, Clone)]
struct Deposit {
    key: String,
    outcome: u8,
}

impl Command for Deposit {
    const TYPE: &'static str = "Deposit";
    type Output = i64;
}

const KEYS: u8 = 3;

fn doc_id(key: u8) -> String {
    format!("acct-{key}")
}

/// Replays a history and reports, per key, what the ledger holds and how many times the handler's
/// write actually landed. "Landed" is read back out of the projection store, not inferred from
/// whether the dispatch returned an error.
struct History {
    db: MemoryTransactor,
    projections: MemoryProjectionStore,
    ledger: Arc<edax::MemoryIdempotencyStore<i64>>,
    bus: CommandBus,
}

impl History {
    /// `enlisted` decides the mode: the host publishes its open handle for the guard and the
    /// handler to share, or both run under the auto-commit handle and commit independently.
    fn new(enlisted: bool) -> (Self, Arc<Shared<MemoryTx>>) {
        let db = MemoryTransactor::new();
        let projections = db.projection_store("read_model");
        let ledger = Arc::new(db.idempotency_store::<i64>());
        let handle = Arc::new(Shared::new(MemoryTx::auto_commit()));

        let published = Arc::clone(&handle);
        let store = projections.clone();
        let mut bus = CommandBus::new();
        let current: Arc<dyn Fn() -> MemoryTx + Send + Sync> = if enlisted {
            let h = Arc::clone(&handle);
            Arc::new(move || h.get())
        } else {
            Arc::new(MemoryTx::auto_commit)
        };
        let guard = idempotent_in(
            Arc::clone(&ledger) as Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync>,
            SCOPE,
            Tenancy::Silo,
            move |_: &edax::CommandMessage| current(),
        );
        let handler: edax::Handler<Deposit> = Box::new(move |cmd: &Deposit| {
            let mut tx = published.get();
            let id = doc_id(cmd.key.parse::<u8>().unwrap());
            let prior = store.get(&mut tx, &id)?;
            let applied = prior
                .as_ref()
                .map_or(0, |e| e.document["applied"].as_i64().unwrap_or(0));
            let version = prior.as_ref().map_or(0, |e| e.version);
            store.save(
                &mut tx,
                &Entity {
                    id: id.clone(),
                    version,
                    document: json!({"applied": applied + 1}),
                },
            )?;
            match cmd.outcome {
                1 => Err(Box::new(Error::store("transient")) as edax::HandlerError),
                2 => Err(Box::new(Error::codec("terminal")) as edax::HandlerError),
                _ => Ok(applied + 1),
            }
        });
        bus.register(handler, vec![guard]).unwrap();

        (
            Self {
                db,
                projections,
                ledger,
                bus,
            },
            handle,
        )
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    /// `done => landed at least once`, in both modes; and enlisted, `done <=> landed exactly once`.
    #[test]
    fn a_done_record_never_covers_work_that_did_not_land(
        enlisted in any::<bool>(),
        ops in prop::collection::vec(
            (0u8..KEYS, 0u8..3, any::<bool>()).prop_map(|(key, outcome, commits)| Op { key, outcome, commits }),
            0..8usize,
        ),
    ) {
        let (h, handle) = History::new(enlisted);
        for op in &ops {
            let cmd = Deposit { key: op.key.to_string(), outcome: op.outcome };
            if enlisted {
                // The host owns the unit of work and publishes its handle; `commits == false`
                // stands for a later write in the same unit of work failing.
                let _: Result<(), edax::HandlerError> = h.db.transaction(|tx| {
                    handle.set(*tx);
                    // A failed dispatch aborts the unit of work, as a host that wraps
                    // `bus.dispatch` in one must: committing over it would keep writes the
                    // handler made before failing. `commits == false` then stands for a *later*
                    // write in the same unit of work failing after a clean dispatch.
                    h.bus.dispatch(&cmd, edax::DispatchOptions::keyed(cmd.key.clone()))?;
                    if op.commits { Ok(()) } else { Err(Error::store("a later write failed").into()) }
                });
                handle.set(MemoryTx::auto_commit());
            } else {
                let _ = h.bus.dispatch(&cmd, edax::DispatchOptions::keyed(cmd.key.clone()));
            }
        }

        for key in 0..KEYS {
            let mut tx = MemoryTx::auto_commit();
            let landed = h
                .projections
                .get(&mut tx, &doc_id(key))
                .unwrap()
                .map_or(0, |e| e.document["applied"].as_i64().unwrap_or(0));
            // Reading the record means claiming it, which is safe: the history is over.
            let prior = match h.ledger.claim(&mut tx, &claim_key(Tenancy::Silo, None, SCOPE, &key.to_string()).unwrap(), 0).unwrap() {
                Claim::Won(_) => None,
                Claim::Prior(record) => Some(record),
            };
            let done = matches!(prior.as_ref().map(|r| &r.state), Some(State::Done(_)));
            if done {
                prop_assert!(landed >= 1, "key {}: a done record over work that never landed", key);
                if enlisted {
                    prop_assert_eq!(landed, 1, "key {}: enlisted mode applied the work twice", key);
                }
            } else if enlisted && prior.is_none() {
                prop_assert_eq!(landed, 0, "key {}: work landed with no record to show for it", key);
            }
        }
    }
}

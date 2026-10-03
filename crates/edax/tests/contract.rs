//! The Rust twin of Python's `edax.testing.contracts` clauses for the saga store, the saga outbox and the
//! event store's snapshot and id rules: generic functions over the seam traits, so a backend an adopter
//! writes runs the same clauses, and every in-tree backend runs them here. Each clause is one invariant a
//! backend must hold; the in-memory store is the reference.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a contract clause's panic is its failure report"
)]

use edax::{Error, ErrorCode, EventStore, MemoryEventStore, MemorySagaStore, MemoryTx, Outbox, SagaCommand, SagaInstance, SagaStore, Snapshot};
use edax_event::{saga_command_id, Event, EventEnvelope};

fn command(id: &str, version: u64, index: u64) -> SagaCommand {
    SagaCommand {
        id: saga_command_id(id, version, index),
        correlation_id: id.to_owned(),
        command: Event { name: "Fulfill".into(), version: 1, data: format!(r#"{{"order_id":"{id}"}}"#).into_bytes() },
        timestamp: 1_714_564_800_000_000_000,
    }
}

fn is_conflict<T: std::fmt::Debug>(got: Result<T, Error>) -> bool {
    matches!(got, Err(Error { code: ErrorCode::Concurrency, .. }))
}

fn unsent<S: Outbox<SagaCommand>>(store: &S, id: &str) -> Vec<String> {
    store.unpublished(1000).unwrap().into_iter().filter(|c| c.correlation_id == id).map(|c| c.id).collect()
}

/// Every saga store clause, against a fresh store from `new`.
fn saga_store_contract<S, T>(new: impl Fn() -> S, mut tx: impl FnMut() -> T)
where
    S: SagaStore<State = String, Tx = T>,
{
    // Completion is a versioned tombstone: a stale completion conflicts, and the instance then reads as
    // completed one version on, not as absent.
    let store = new();
    assert_eq!(store.save(&mut tx(), "order-1", &"open".to_owned(), 0, &[]).unwrap(), 1);
    assert!(is_conflict(store.complete(&mut tx(), "order-1", 6, &[])), "a stale completion conflicts");
    assert_eq!(store.complete(&mut tx(), "order-1", 1, &[]).unwrap(), 2);
    assert_eq!(store.load(&mut tx(), "order-1").unwrap(), Some(SagaInstance::Completed { version: 2 }));

    // A completed instance takes no write: the stale writer (ABA), the fresh start (resurrection), a
    // write at the tombstone's own version, and a second completion all conflict.
    for (name, got) in [
        ("stale writer", store.save(&mut tx(), "order-1", &"two".to_owned(), 1, &[])),
        ("fresh start", store.save(&mut tx(), "order-1", &"open".to_owned(), 0, &[])),
        ("at the tombstone", store.save(&mut tx(), "order-1", &"two".to_owned(), 2, &[])),
        ("second completion", store.complete(&mut tx(), "order-1", 2, &[])),
    ] {
        assert!(is_conflict(got), "{name}");
        assert_eq!(store.load(&mut tx(), "order-1").unwrap(), Some(SagaInstance::Completed { version: 2 }), "{name}");
    }

    // The step's commands commit with its state, oldest first; a refused write enqueues none.
    let store = new();
    store.save(&mut tx(), "order-2", &"open".to_owned(), 0, &[command("order-2", 1, 0)]).unwrap();
    assert!(is_conflict(store.save(&mut tx(), "order-2", &"lost".to_owned(), 0, &[command("order-2", 1, 0)])));
    store.complete(&mut tx(), "order-2", 1, &[command("order-2", 2, 0), command("order-2", 2, 1)]).unwrap();
    assert_eq!(unsent(&store, "order-2"), ["order-2:1:0", "order-2:2:0", "order-2:2:1"]);
    let row = store.unpublished(1000).unwrap().into_iter().find(|c| c.id == "order-2:2:1").unwrap();
    assert_eq!(row, command("order-2", 2, 1), "every field survives");

    // mark_published prunes by id, idempotently; an unknown id is ignored.
    store.mark_published(&["order-2:1:0".to_owned()]).unwrap();
    store.mark_published(&["order-2:1:0".to_owned(), "never-existed".to_owned()]).unwrap();
    assert_eq!(unsent(&store, "order-2"), ["order-2:2:0", "order-2:2:1"]);
    assert!(store.unpublished(1).unwrap().len() <= 1, "the limit holds");
}

/// The snapshot and id rules of the event store, against a fresh store from `new`.
fn event_store_contract<S>(new: impl Fn() -> S)
where
    S: EventStore,
    S::Tx: Default,
{
    let event = |aggregate_id: &str, id: &str, sequence: u64| EventEnvelope {
        id: id.to_owned(),
        aggregate_id: aggregate_id.to_owned(),
        event: Event { name: "Deposited".into(), version: 1, data: br#"{"amount_cents":4200}"#.to_vec() },
        sequence: Some(sequence),
        namespace: Default::default(),
        metadata: Default::default(),
        timestamp: 0,
    };
    let snapshot = |version: u64, data: &[u8]| Snapshot { aggregate_id: "acct-1".into(), type_name: "Account".into(), version, data: data.to_vec() };

    // A snapshot may lag the stream but never lead it, and never moves back.
    let store = new();
    store.append(&mut S::Tx::default(), "acct-1", 0, &[event("acct-1", "evt-1", 1)]).unwrap();
    let leading = store.save_snapshot(&mut S::Tx::default(), &snapshot(2, b"ahead"));
    assert!(matches!(leading, Err(Error { code: ErrorCode::Codec, .. })), "a leading snapshot is refused: {leading:?}");
    store.append(&mut S::Tx::default(), "acct-1", 1, &[event("acct-1", "evt-2", 2)]).unwrap();
    store.save_snapshot(&mut S::Tx::default(), &snapshot(2, b"newer")).unwrap();
    store.save_snapshot(&mut S::Tx::default(), &snapshot(1, b"older")).unwrap();
    let kept = store.load_snapshot(&mut S::Tx::default(), "acct-1").unwrap().unwrap();
    assert_eq!((kept.version, kept.data), (2, b"newer".to_vec()), "an older snapshot never replaces a newer one");

    // An envelope id is unique store-wide.
    let duplicate = store.append(&mut S::Tx::default(), "acct-2", 0, &[event("acct-2", "evt-1", 1)]);
    assert!(matches!(duplicate, Err(Error { code: ErrorCode::Codec, .. })), "a taken id is refused: {duplicate:?}");
    assert!(store.load(&mut S::Tx::default(), "acct-2", 0).unwrap().is_empty(), "and writes nothing");
}

#[test]
fn the_memory_saga_store_meets_the_saga_store_contract() {
    saga_store_contract(MemorySagaStore::<String>::new, MemoryTx::auto_commit);
}

#[test]
fn the_memory_event_store_meets_the_snapshot_and_id_clauses() {
    event_store_contract(MemoryEventStore::new);
}

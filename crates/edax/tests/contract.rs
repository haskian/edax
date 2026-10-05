//! Every in-tree backend held to the store contract clauses in `edax::testkit::contract`: the same public
//! functions an adopter's store test calls with the `testkit` feature on. Each test here supplies a store
//! and, for an outbox, the closure that writes its rows.

use edax::testkit::contract::{self, fresh, saga_command, SAGA};
use edax::testkit::draft;
use edax::{
    CommandMessage, EventDraft, EventMessage, EventStore, MemoryEventStore, MemoryIdempotencyStore, MemoryLedger,
    MemorySagaStore, MemoryTx, SagaOutbox, SagaStore, Transactor,
};

#[test]
fn the_memory_saga_store_meets_the_saga_store_contract() {
    contract::saga_store(MemorySagaStore::<String>::new, MemoryTx::auto_commit);
}

#[test]
fn the_memory_idempotency_store_meets_the_held_key_clause() {
    let store = MemoryIdempotencyStore::<String>::new();
    contract::idempotency_held_key(&store, &store.transactor(), MemoryTx::auto_commit);
}

#[test]
fn the_memory_stores_meet_the_unreadable_record_clause() {
    let store = MemoryIdempotencyStore::<u64>::new();
    contract::idempotency_unreadable_record(
        &store,
        |key, bytes| store.put_raw(key, bytes).unwrap(),
        MemoryTx::auto_commit,
    );
    let ledger = MemoryLedger::<u64>::new();
    contract::idempotency_unreadable_record(&ledger, |key, bytes| ledger.put_raw(key, bytes).unwrap(), || ());
}

#[test]
fn the_memory_stores_meet_the_forget_clause() {
    contract::idempotency_forget(&MemoryIdempotencyStore::<String>::new(), MemoryTx::auto_commit);
    contract::idempotency_forget(&MemoryLedger::<String>::new(), || ());
}

#[test]
fn the_memory_event_store_meets_the_append_snapshot_and_id_clauses() {
    contract::event_store(MemoryEventStore::new);
}

#[test]
fn the_memory_event_store_meets_the_outbox_contract() {
    let store = MemoryEventStore::new();
    let write = |n: usize, commit: bool| {
        let aggregate_id = fresh("acct");
        let drafts = (1..=n as u64)
            .map(|sequence| EventDraft {
                id: fresh("evt"),
                ..draft(&aggregate_id, "Deposited", sequence, &[])
            })
            .collect();
        let mut tx = store.begin().unwrap();
        let stored = store.append(&mut tx, &aggregate_id, 0, drafts).unwrap();
        if commit {
            store.commit(tx).unwrap();
        } else {
            store.rollback(tx);
        }
        stored
    };
    contract::outbox::<EventMessage, _>(&store, write);
    contract::counted_outbox::<EventMessage, _>(&store, write);
}

#[test]
fn the_memory_saga_outbox_meets_the_outbox_contract() {
    let store = MemorySagaStore::<String>::new();
    let transactor = store.transactor();
    let outbox = SagaOutbox::new(store.clone(), SAGA);
    let write = |n: usize, commit: bool| {
        let id = fresh("order");
        let rows: Vec<CommandMessage> = (0..n as u64).map(|index| saga_command(SAGA, &id, 1, index)).collect();
        let mut tx = transactor.begin().unwrap();
        store
            .save(&mut tx, SAGA, &id, &"open".to_owned(), 0, "", &rows)
            .unwrap();
        if commit {
            transactor.commit(tx).unwrap();
        } else {
            transactor.rollback(tx);
        }
        rows
    };
    contract::outbox(&outbox, write);
    contract::counted_outbox(&outbox, write);
}

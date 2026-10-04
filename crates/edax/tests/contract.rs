//! The Rust twin of Python's `edax.testing.contracts` clauses for the saga store, the saga outbox, the
//! event store's append, snapshot and id rules, and the idempotency store's held-key and unreadable-record rules:
//! generic functions over the seam traits, so a backend an adopter writes runs the same clauses, and
//! every in-tree backend runs them here. Each clause is one invariant a
//! backend must hold; the in-memory store is the reference.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a contract clause's panic is its failure report"
)]

mod support;

use support::draft;

use edax::{
    Claim, CommandMessage, Error, ErrorCode, EventDraft, EventStore, Extensions, IdempotencyStore, MemoryEventStore,
    MemoryIdempotencyStore, MemoryLedger, MemorySagaStore, MemoryTx, Metadata, Outbox, Record, SagaInstance, SagaStore, Snapshot, State,
    Transactor,
};
use edax_event::{saga_command, Event};

/// The `index`-th command instance `id` sends at the write that commits `version`, caused by its `Paid`
/// event: the kernel's row.
fn command(id: &str, version: u64, index: u64) -> CommandMessage {
    let payload = Event { name: "Fulfill".into(), version: 1, data: format!(r#"{{"order_id":"{id}"}}"#).into_bytes() };
    saga_command("order", id, version, index, None, payload, &format!("{id}-paid"), "corr-1", 1_714_564_800_000_000_000)
}

fn is_conflict<T: std::fmt::Debug>(got: Result<T, Error>) -> bool {
    matches!(got, Err(Error { code: ErrorCode::Concurrency, .. }))
}

fn unsent<S: Outbox<CommandMessage>>(store: &S, id: &str) -> Vec<String> {
    store.unpublished(1000).unwrap().into_iter().filter(|c| c.aggregate_id.as_deref() == Some(id)).map(|c| c.id).collect()
}

/// Every saga store clause, against a fresh store from `new`.
fn saga_store_contract<S, T>(new: impl Fn() -> S, mut tx: impl FnMut() -> T)
where
    S: SagaStore<State = String, Tx = T>,
{
    // Completion is a versioned tombstone: a stale completion conflicts, and the instance then reads as
    // completed one version on, not as absent.
    let store = new();
    assert_eq!(store.save(&mut tx(), "order-1", &"open".to_owned(), 0, "", &[]).unwrap(), 1);
    assert!(is_conflict(store.complete(&mut tx(), "order-1", 6, &[])), "a stale completion conflicts");
    assert_eq!(store.complete(&mut tx(), "order-1", 1, &[]).unwrap(), 2);
    assert_eq!(store.load(&mut tx(), "order-1").unwrap(), Some(SagaInstance::Completed { version: 2 }));

    // A completed instance takes no write: the stale writer (ABA), the fresh start (resurrection), a
    // write at the tombstone's own version, and a second completion all conflict.
    for (name, got) in [
        ("stale writer", store.save(&mut tx(), "order-1", &"two".to_owned(), 1, "", &[])),
        ("fresh start", store.save(&mut tx(), "order-1", &"open".to_owned(), 0, "", &[])),
        ("at the tombstone", store.save(&mut tx(), "order-1", &"two".to_owned(), 2, "", &[])),
        ("second completion", store.complete(&mut tx(), "order-1", 2, &[])),
    ] {
        assert!(is_conflict(got), "{name}");
        assert_eq!(store.load(&mut tx(), "order-1").unwrap(), Some(SagaInstance::Completed { version: 2 }), "{name}");
    }

    // The step's commands commit with its state, oldest first; a refused write enqueues none.
    let store = new();
    store.save(&mut tx(), "order-2", &"open".to_owned(), 0, "", &[command("order-2", 1, 0)]).unwrap();
    assert!(is_conflict(store.save(&mut tx(), "order-2", &"lost".to_owned(), 0, "", &[command("order-2", 1, 0)])));
    store.complete(&mut tx(), "order-2", 1, &[command("order-2", 2, 0), command("order-2", 2, 1)]).unwrap();
    assert_eq!(unsent(&store, "order-2"), ["order-2:1:0", "order-2:2:0", "order-2:2:1"]);
    let row = store.unpublished(1000).unwrap().into_iter().find(|c| c.id == "order-2:2:1").unwrap();
    assert_eq!(row, command("order-2", 2, 1), "every field survives");

    // mark_published prunes by id, idempotently; an unknown id is ignored.
    store.mark_published(&["order-2:1:0".to_owned()]).unwrap();
    store.mark_published(&["order-2:1:0".to_owned(), "never-existed".to_owned()]).unwrap();
    assert_eq!(unsent(&store, "order-2"), ["order-2:2:0", "order-2:2:1"]);
    assert!(store.unpublished(1).unwrap().len() <= 1, "the limit holds");

    // `save` keeps the folded marks as written and `load` hands them back: the runner skips an event
    // redelivered after its step committed by them, so a store that drops them repeats the step.
    let store = new();
    let folded = r#"{"order-3":{"sequence":1,"id":"evt-1"}}"#;
    store.save(&mut tx(), "order-3", &"open".to_owned(), 0, folded, &[]).unwrap();
    let running = SagaInstance::Running { state: "open".to_owned(), version: 1, folded: folded.to_owned() };
    assert_eq!(store.load(&mut tx(), "order-3").unwrap(), Some(running), "the marks round-trip");
}

/// The held-key rule of an idempotency store whose writes join the caller's transaction: `complete` and
/// `release` of a key another open transaction holds never land over it, because that transaction's
/// commit would overwrite the record or the release would free its claim. This clause is for a store that
/// refuses with a transient conflict and writes nothing; one that blocks on a row lock instead would wait
/// here forever, on one thread. `auto` is a handle that joins no transaction.
fn idempotency_held_key_contract<S, T>(store: &S, transactor: &T, auto: impl Fn() -> T::Tx)
where
    S: IdempotencyStore<String, Tx = T::Tx>,
    T: Transactor,
{
    let Claim::Won(handle) = store.claim(&mut auto(), "req-1", 1234).unwrap() else { panic!("a free key is won") };
    // The holder stages the release of the committed claim: it holds the key until it ends.
    let mut holder = transactor.begin().unwrap();
    store.release(&mut holder, "req-1", &handle).unwrap();
    let mut other = transactor.begin().unwrap();
    let completed = store.complete(&mut other, "req-1", &handle, Record::done(1234, "late".to_owned()));
    assert!(is_conflict(completed), "a complete of a held key is a conflict");
    assert!(is_conflict(store.release(&mut other, "req-1", &handle)), "a release of a held key is a conflict");
    transactor.commit(other).unwrap();
    transactor.rollback(holder);
    // Nothing the refused calls staged landed: the claim is still pending, and settles as usual.
    let Claim::Prior(pending) = store.claim(&mut auto(), "req-1", 1234).unwrap() else { panic!("the key is held") };
    assert!(matches!(pending.state, edax::State::Pending), "the refused calls wrote nothing");
    store.complete(&mut auto(), "req-1", &handle, Record::done(1234, "done".to_owned())).unwrap();
    let Claim::Prior(settled) = store.claim(&mut auto(), "req-1", 1234).unwrap() else { panic!("the key is settled") };
    assert!(matches!(settled.state, edax::State::Done(ref result) if result == "done"));
}

/// A record no reader can decode still holds its key: a claim reads it as the kernel's terminal `codec`
/// failure (`edax_cqrs::decode_prior`), so the guard replays a cached failure and the handler never runs
/// again, as in every other SDK sharing the ledger. The failure keeps the stored hash when it reads, else
/// the claimant's. `put_raw` stores bytes under a key as the backend persists a record, unchecked.
fn idempotency_unreadable_record_contract<S>(store: &S, put_raw: impl Fn(&str, &[u8]), auto: impl Fn() -> S::Tx)
where
    S: IdempotencyStore<u64>,
{
    const STORED: u64 = 11_234_062_831_410_934_249;
    const CLAIMANT: u64 = 18_446_744_073_709_551_615;
    for (key, bytes, want_hash, cause) in [
        ("garbage", b"\x01\x7fedax".as_slice(), CLAIMANT, "expected value at line 1 column 1"),
        ("torn-write", br#"{"payload_hash":11234062831410934249,"result":{""#.as_slice(), CLAIMANT, "EOF while parsing a string"),
        ("not-an-object", b"[11234062831410934249]".as_slice(), CLAIMANT, "not a JSON object"),
        ("wrong-type", br#"{"payload_hash":11234062831410934249,"pending":"settling"}"#.as_slice(), STORED, "expected a boolean"),
        ("result-not-an-o", br#"{"payload_hash":11234062831410934249,"result":"x","pending":false}"#.as_slice(), STORED, "cached result is not a valid replay value"),
    ] {
        put_raw(key, bytes);
        for attempt in ["first claim", "redelivery"] {
            let Claim::Prior(record) = store.claim(&mut auto(), key, CLAIMANT).unwrap() else { panic!("{key}: {attempt} must not win") };
            assert_eq!(record.payload_hash, want_hash, "{key}: {attempt}");
            let State::Failed { code, message } = &record.state else { panic!("{key}: {attempt}: want Failed, got {:?}", record.state) };
            assert_eq!(*code, Some(ErrorCode::Codec), "{key}: {attempt}");
            assert!(message.starts_with("unreadable idempotency record: ") && message.contains(cause), "{key}: {attempt}: {message}");
        }
    }
}

/// The append, snapshot and id rules of the event store, against a fresh store from `new`.
fn event_store_contract<S>(new: impl Fn() -> S)
where
    S: EventStore,
    S::Tx: Default,
{
    let deposited = |aggregate_id: &str, id: &str, sequence: u64| EventDraft {
        data: br#"{"amount_cents":4200}"#.to_vec(),
        occurred_at: Some(1_714_564_799_000_000_000),
        extensions: Extensions::from([("traceparent".into(), "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into())]),
        metadata: Metadata::from([("tenant".into(), "acme".into())]),
        ..draft(id, aggregate_id, "Deposited", sequence)
    };
    let snapshot = |version: u64, data: &[u8]| Snapshot { aggregate_id: "acct-1".into(), type_name: "Account".into(), version, data: data.to_vec() };

    // An append returns each draft as the store recorded it, every field kept, and a load reads the same.
    let store = new();
    let first = deposited("acct-1", "evt-1", 1);
    let appended = store.append(&mut S::Tx::default(), "acct-1", 0, vec![first.clone()]).unwrap();
    let [stored] = appended.as_slice() else { panic!("one draft appends one event: {appended:?}") };
    assert_eq!(stored, &first.record(stored.recorded_at), "every draft field survives; the store adds only recorded_at");
    assert_eq!(store.load(&mut S::Tx::default(), "acct-1", 0).unwrap(), appended, "a load reads what the append returned");

    // A snapshot may lag the stream but never lead it, and never moves back.
    let leading = store.save_snapshot(&mut S::Tx::default(), &snapshot(2, b"ahead"));
    assert!(matches!(leading, Err(Error { code: ErrorCode::Codec, .. })), "a leading snapshot is refused: {leading:?}");
    store.append(&mut S::Tx::default(), "acct-1", 1, vec![deposited("acct-1", "evt-2", 2)]).unwrap();
    store.save_snapshot(&mut S::Tx::default(), &snapshot(2, b"newer")).unwrap();
    store.save_snapshot(&mut S::Tx::default(), &snapshot(1, b"older")).unwrap();
    let kept = store.load_snapshot(&mut S::Tx::default(), "acct-1").unwrap().unwrap();
    assert_eq!((kept.version, kept.data), (2, b"newer".to_vec()), "an older snapshot never replaces a newer one");

    // An event id is unique store-wide.
    let duplicate = store.append(&mut S::Tx::default(), "acct-2", 0, vec![deposited("acct-2", "evt-1", 1)]);
    assert!(matches!(duplicate, Err(Error { code: ErrorCode::Codec, .. })), "a taken id is refused: {duplicate:?}");
    assert!(store.load(&mut S::Tx::default(), "acct-2", 0).unwrap().is_empty(), "and writes nothing");
}

#[test]
fn the_memory_saga_store_meets_the_saga_store_contract() {
    saga_store_contract(MemorySagaStore::<String>::new, MemoryTx::auto_commit);
}

#[test]
fn the_memory_idempotency_store_meets_the_held_key_clause() {
    let store = MemoryIdempotencyStore::<String>::new();
    idempotency_held_key_contract(&store, &store.transactor(), MemoryTx::auto_commit);
}

#[test]
fn the_memory_stores_meet_the_unreadable_record_clause() {
    let store = MemoryIdempotencyStore::<u64>::new();
    idempotency_unreadable_record_contract(&store, |key, bytes| store.put_raw(key, bytes).unwrap(), MemoryTx::auto_commit);
    let ledger = MemoryLedger::<u64>::new();
    idempotency_unreadable_record_contract(&ledger, |key, bytes| ledger.put_raw(key, bytes).unwrap(), || ());
}

#[test]
fn the_memory_event_store_meets_the_append_snapshot_and_id_clauses() {
    event_store_contract(MemoryEventStore::new);
}

//! The store contract clauses, the Rust twin of Python's `edax.testing.contracts`: the saga store, every
//! outbox's read side (the event outbox and a saga's command outbox) and its backlog count, the event
//! store's append, snapshot and id rules, and the idempotency store's held-key, unreadable-record and forget
//! rules. Each is a generic function over a seam trait, so a backend an adopter writes runs the same clauses
//! the in-tree backends run in `crates/edax/tests/contract.rs`. Each clause is one invariant a backend must
//! hold; the in-memory store is the reference.
//!
//! Call them from a `#[test]` with the `testkit` feature on:
//!
//! ```ignore
//! #[test]
//! fn my_saga_store_meets_the_saga_store_contract() {
//!     edax::testkit::contract::saga_store(|| MySagaStore::connect(DATABASE_URL), || MyTx::auto_commit());
//! }
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a contract clause's panic is its failure report"
)]

use std::collections::HashSet;
use std::fmt::Debug;

use edax_event::Message;

use super::draft;
use crate::{
    Claim, CountedOutbox, Error, ErrorCode, EventDraft, EventStore, IdempotencyStore, Metadata, Outbox, Record,
    SagaInstance, SagaStore, Snapshot, State, Transactor,
};

/// The saga every saga store clause writes as.
pub const SAGA: &str = "order";

pub use edax_event::testkit::saga_command;

/// A fresh id per call, so each write of a clause names rows of its own, and a durable store shared across
/// test runs needs no truncation between them.
pub fn fresh(prefix: &str) -> String {
    format!("{prefix}-{}", crate::guid())
}

fn is_conflict<T: Debug>(got: Result<T, Error>) -> bool {
    matches!(
        got,
        Err(Error {
            code: ErrorCode::Concurrency,
            ..
        })
    )
}

fn unsent<S: SagaStore>(store: &S, saga: &str, id: &str) -> Vec<String> {
    store
        .unpublished(saga, 1000)
        .unwrap()
        .into_iter()
        .filter(|c| c.aggregate_id.as_deref() == Some(id))
        .map(|c| c.id)
        .collect()
}

/// Every saga store clause, against a fresh store from `new`. `tx` opens the handle each call writes under.
pub fn saga_store<S, T>(new: impl Fn() -> S, mut tx: impl FnMut() -> T)
where
    S: SagaStore<State = String, Tx = T>,
{
    // Completion is a versioned tombstone: a stale completion conflicts, and the instance then reads as
    // completed one version on, not as absent.
    let store = new();
    assert_eq!(
        store
            .save(&mut tx(), SAGA, "order-1", &"open".to_owned(), 0, "", &[])
            .unwrap(),
        1
    );
    assert!(
        is_conflict(store.complete(&mut tx(), SAGA, "order-1", 6, &[])),
        "a stale completion conflicts"
    );
    assert_eq!(store.complete(&mut tx(), SAGA, "order-1", 1, &[]).unwrap(), 2);
    assert_eq!(
        store.load(&mut tx(), SAGA, "order-1").unwrap(),
        Some(SagaInstance::Completed { version: 2 })
    );

    // A completed instance takes no write: the stale writer (ABA), the fresh start (resurrection), a
    // write at the tombstone's own version, and a second completion all conflict.
    for (name, got) in [
        (
            "stale writer",
            store.save(&mut tx(), SAGA, "order-1", &"two".to_owned(), 1, "", &[]),
        ),
        (
            "fresh start",
            store.save(&mut tx(), SAGA, "order-1", &"open".to_owned(), 0, "", &[]),
        ),
        (
            "at the tombstone",
            store.save(&mut tx(), SAGA, "order-1", &"two".to_owned(), 2, "", &[]),
        ),
        ("second completion", store.complete(&mut tx(), SAGA, "order-1", 2, &[])),
    ] {
        assert!(is_conflict(got), "{name}");
        assert_eq!(
            store.load(&mut tx(), SAGA, "order-1").unwrap(),
            Some(SagaInstance::Completed { version: 2 }),
            "{name}"
        );
    }

    // The step's commands commit with its state, oldest first; a refused write enqueues none.
    let store = new();
    store
        .save(
            &mut tx(),
            SAGA,
            "order-2",
            &"open".to_owned(),
            0,
            "",
            &[saga_command(SAGA, "order-2", 1, 0)],
        )
        .unwrap();
    assert!(is_conflict(store.save(
        &mut tx(),
        SAGA,
        "order-2",
        &"lost".to_owned(),
        0,
        "",
        &[saga_command(SAGA, "order-2", 1, 0)]
    )));
    store
        .complete(
            &mut tx(),
            SAGA,
            "order-2",
            1,
            &[saga_command(SAGA, "order-2", 2, 0), saga_command(SAGA, "order-2", 2, 1)],
        )
        .unwrap();
    assert_eq!(
        unsent(&store, SAGA, "order-2"),
        ["5:order:order-2:1:0", "5:order:order-2:2:0", "5:order:order-2:2:1"]
    );
    let row = store
        .unpublished(SAGA, 1000)
        .unwrap()
        .into_iter()
        .find(|c| c.id == "5:order:order-2:2:1")
        .unwrap();
    assert_eq!(row, saga_command(SAGA, "order-2", 2, 1), "every field survives");

    // `save` keeps the folded marks as written and `load` hands them back: the runner skips an event
    // redelivered after its step committed by them, so a store that drops them repeats the step.
    let store = new();
    let folded = r#"{"order-3":{"sequence":1,"id":"evt-1","unsequenced":"evt-u"}}"#;
    store
        .save(&mut tx(), SAGA, "order-3", &"open".to_owned(), 0, folded, &[])
        .unwrap();
    let running = SagaInstance::Running {
        state: "open".to_owned(),
        version: 1,
        folded: folded.to_owned(),
    };
    assert_eq!(
        store.load(&mut tx(), SAGA, "order-3").unwrap(),
        Some(running),
        "the marks round-trip"
    );

    // Two sagas over one store share neither an instance nor a command, even under one instance id: each
    // writes version 1 of its own row and drains only its own outbox.
    let store = new();
    let sagas = [(SAGA, "open"), ("billing", "invoiced")];
    for (saga, state) in sagas {
        let written = store.save(
            &mut tx(),
            saga,
            "order-4",
            &state.to_owned(),
            0,
            "",
            &[saga_command(saga, "order-4", 1, 0)],
        );
        assert_eq!(written.unwrap(), 1, "{saga}");
    }
    for (saga, state) in sagas {
        let running = SagaInstance::Running {
            state: state.to_owned(),
            version: 1,
            folded: String::new(),
        };
        assert_eq!(
            store.load(&mut tx(), saga, "order-4").unwrap(),
            Some(running),
            "{saga}: its own instance"
        );
        assert_eq!(
            unsent(&store, saga, "order-4"),
            [saga_command(saga, "order-4", 1, 0).id],
            "{saga}: its own outbox"
        );
    }
    // The mark names the shipped row by its id, which another saga's row never shares.
    store
        .mark_published(
            SAGA,
            &[
                saga_command(SAGA, "order-4", 1, 0).id,
                saga_command("billing", "order-4", 1, 0).id,
            ],
        )
        .unwrap();
    assert_eq!(
        unsent(&store, "billing", "order-4"),
        ["7:billing:order-4:1:0"],
        "a mark prunes one saga's row only"
    );
}

/// Every clause of an outbox's read side, the same for every outbox. `write(n, commit)` writes `n` rows
/// through the write port in one unit of work, commits it or rolls it back, and returns the rows as
/// written: what `unpublished` hands back for them once committed.
pub fn outbox<R, O>(outbox: &O, mut write: impl FnMut(usize, bool) -> Vec<R>)
where
    R: Message + PartialEq + Debug,
    O: Outbox<R>,
{
    // The rows of `rows` the outbox shows, in its order: other clauses leave rows of their own.
    let shown = |rows: &[R]| -> Vec<R> {
        let ids: HashSet<&str> = rows.iter().map(Message::id).collect();
        outbox
            .unpublished(1000)
            .unwrap()
            .into_iter()
            .filter(|row| ids.contains(row.id()))
            .collect()
    };
    let rows = write(2, true);
    assert_eq!(shown(&rows), rows, "a committed row comes back whole, in order");
    let rolled_back = write(1, false);
    assert!(shown(&rolled_back).is_empty(), "a rolled-back row never shows");
    let mut oldest_first = write(2, true);
    oldest_first.extend(write(1, true));
    assert_eq!(shown(&oldest_first), oldest_first, "oldest commit first");
    write(3, true);
    assert_eq!(outbox.unpublished(2).unwrap().len(), 2, "the limit decides the page");
    let rows = write(2, true);
    let first = rows.first().map(|row| row.id().to_owned()).unwrap();
    outbox.mark_published(&[]).unwrap();
    outbox.mark_published(std::slice::from_ref(&first)).unwrap();
    outbox.mark_published(&[first, "never-existed".to_owned()]).unwrap();
    assert_eq!(
        shown(&rows),
        rows[1..],
        "mark_published is idempotent and prunes only the marked row"
    );
}

/// The count clause of an outbox that counts its backlog: one per committed row, none for a rolled-back
/// one, and a marked row stops counting. A count that ignored `mark_published`, or counted uncommitted
/// rows, would report a backlog that never drains. Deltas, not absolutes: other clauses leave rows.
pub fn counted_outbox<R, O>(outbox: &O, mut write: impl FnMut(usize, bool) -> Vec<R>)
where
    R: Message,
    O: CountedOutbox<R>,
{
    let before = outbox.unpublished_count().unwrap();
    write(1, false);
    let rows = write(2, true);
    assert_eq!(outbox.unpublished_count().unwrap() - before, 2, "one per committed row");
    outbox.mark_published(&[rows[0].id().to_owned()]).unwrap();
    assert_eq!(
        outbox.unpublished_count().unwrap() - before,
        1,
        "a marked row stops counting"
    );
}

/// The held-key rule of an idempotency store whose writes join the caller's transaction: `complete` and
/// `release` of a key another open transaction holds never land over it, because that transaction's
/// commit would overwrite the record or the release would free its claim. This clause is for a store that
/// refuses with a transient conflict and writes nothing; one that blocks on a row lock instead would wait
/// here forever, on one thread. `auto` is a handle that joins no transaction.
pub fn idempotency_held_key<S, T>(store: &S, transactor: &T, auto: impl Fn() -> T::Tx)
where
    S: IdempotencyStore<String, Tx = T::Tx>,
    T: Transactor,
{
    let key = fresh("req");
    let Claim::Won(handle) = store.claim(&mut auto(), &key, 1234).unwrap() else {
        panic!("a free key is won")
    };
    // The holder stages the release of the committed claim: it holds the key until it ends.
    let mut holder = transactor.begin().unwrap();
    store.release(&mut holder, &key, &handle).unwrap();
    let mut other = transactor.begin().unwrap();
    let completed = store.complete(&mut other, &key, &handle, Record::done(1234, "late".to_owned()));
    assert!(is_conflict(completed), "a complete of a held key is a conflict");
    assert!(
        is_conflict(store.release(&mut other, &key, &handle)),
        "a release of a held key is a conflict"
    );
    transactor.commit(other).unwrap();
    transactor.rollback(holder);
    // Nothing the refused calls staged landed: the claim is still pending, and settles as usual.
    let Claim::Prior(pending) = store.claim(&mut auto(), &key, 1234).unwrap() else {
        panic!("the key is held")
    };
    assert_eq!(pending.state, State::Pending, "the refused calls wrote nothing");
    store
        .complete(&mut auto(), &key, &handle, Record::done(1234, "done".to_owned()))
        .unwrap();
    let Claim::Prior(settled) = store.claim(&mut auto(), &key, 1234).unwrap() else {
        panic!("the key is settled")
    };
    assert_eq!(settled.state, State::Done("done".to_owned()));
}

/// A record no reader can decode still holds its key: a claim reads it as the kernel's terminal `codec`
/// failure, `edax_cqrs::decode_prior`, so the guard replays a cached failure and the handler never runs
/// again, as in every other SDK sharing the ledger. The failure keeps the stored hash when it reads, else
/// the claimant's: the clause holds the store to the kernel's own reading, not to a copy of its text.
/// `put_raw` stores bytes under a key as the backend persists a record, unchecked.
pub fn idempotency_unreadable_record<S>(store: &S, put_raw: impl Fn(&str, &[u8]), auto: impl Fn() -> S::Tx)
where
    S: IdempotencyStore<u64>,
{
    const CLAIMANT: u64 = u64::MAX;
    for (name, bytes) in [
        ("garbage", b"\x01\x7fedax".as_slice()),
        (
            "torn write",
            br#"{"payload_hash":11234062831410934249,"result":{""#.as_slice(),
        ),
        ("not an object", b"[11234062831410934249]".as_slice()),
        (
            "wrong type",
            br#"{"payload_hash":11234062831410934249,"pending":"settling"}"#.as_slice(),
        ),
        // A result the store's typed `R` cannot read: Python has no such row, as any JSON value reads there.
        (
            "result not an R",
            br#"{"payload_hash":11234062831410934249,"result":"x","pending":false}"#.as_slice(),
        ),
    ] {
        let key = fresh("req");
        put_raw(&key, bytes);
        let want = edax_cqrs::decode_prior::<u64>(bytes, CLAIMANT);
        assert!(
            matches!(
                want.state,
                State::Failed {
                    code: Some(ErrorCode::Codec),
                    ..
                }
            ),
            "{name}: the kernel reads a codec failure"
        );
        for attempt in ["first claim", "redelivery"] {
            let Claim::Prior(record) = store.claim(&mut auto(), &key, CLAIMANT).unwrap() else {
                panic!("{name}: {attempt} must not win")
            };
            assert_eq!(record, want, "{name}: {attempt}: the kernel's cached codec failure");
        }
    }
}

/// The forget rule: a completed or failed record is deleted, so the next claim wins and a dead-lettered
/// message is re-driven; a pending claim is refused as `in_progress` and stays, because its run may still
/// complete; an absent key is a no-op. The keys come from [`crate::forget`] and [`crate::forget_event`], so
/// a forget deletes the row the guard claimed.
pub fn idempotency_forget<S>(store: &S, auto: impl Fn() -> S::Tx)
where
    S: IdempotencyStore<String>,
{
    const HASH: u64 = 1234;
    let silo = crate::Tenancy::Silo;
    let settled = [
        ("completed", Record::done(HASH, "receipt-1".to_owned())),
        (
            "failed",
            Record::failed(HASH, ErrorCode::Rejected, "insufficient funds"),
        ),
    ];
    for (name, record) in settled {
        let request = fresh(name);
        for (via, key) in [
            ("forget", crate::claim_key(silo, None, "payments", &request).unwrap()),
            (
                "forget_event",
                crate::event_claim_key(silo, None, "payments", &request).unwrap(),
            ),
        ] {
            let Claim::Won(handle) = store.claim(&mut auto(), &key, HASH).unwrap() else {
                panic!("{name}: a free key is won")
            };
            store.complete(&mut auto(), &key, &handle, record.clone()).unwrap();
            match via {
                "forget" => crate::forget(store, &mut auto(), "payments", &request, silo, None).unwrap(),
                _ => crate::forget_event(store, &mut auto(), "payments", &request, silo, None).unwrap(),
            }
            assert!(
                matches!(store.claim(&mut auto(), &key, HASH).unwrap(), Claim::Won(_)),
                "{name} via {via}: forgotten, so free"
            );
        }
    }

    let pending = fresh("pending");
    assert!(matches!(
        store.claim(&mut auto(), &pending, HASH).unwrap(),
        Claim::Won(_)
    ));
    let refused = store.forget(&mut auto(), &pending);
    assert!(
        matches!(
            &refused,
            Err(Error {
                code: ErrorCode::InProgress,
                ..
            })
        ),
        "a pending claim is refused: {refused:?}"
    );
    let Claim::Prior(held) = store.claim(&mut auto(), &pending, HASH).unwrap() else {
        panic!("the claim stays")
    };
    assert_eq!(held.state, State::Pending, "the refused forget kept the claim");

    store
        .forget(&mut auto(), &fresh("absent"))
        .expect("an absent key is a no-op");
    let refused = crate::forget(store, &mut auto(), "payments", "req-1", crate::Tenancy::Pool, None);
    assert!(
        matches!(
            &refused,
            Err(Error {
                code: ErrorCode::Codec,
                ..
            })
        ),
        "a pool names a tenant: {refused:?}"
    );
}

/// The append, snapshot and id rules of the event store, against a fresh store from `new`.
pub fn event_store<S>(new: impl Fn() -> S)
where
    S: EventStore,
    S::Tx: Default,
{
    // The base draft carries the lineage and a trace parent; these add what a store may drop.
    let deposited = |aggregate_id: &str, id: &str, sequence: u64| EventDraft {
        id: id.to_owned(),
        occurred_at: Some(1_714_564_799_000_000_000),
        metadata: Metadata::from([("tenant".into(), "acme".into()), ("note".into(), "déjà-vu".into())]),
        ..draft(
            aggregate_id,
            "Deposited",
            sequence,
            r#"{"amount_cents":4200,"memo":"déjà-vu"}"#.as_bytes(),
        )
    };
    let snapshot = |version: u64, data: &[u8]| Snapshot {
        subject: None,
        encryption: None,
        tenant: None,
        aggregate_id: "acct-1".into(),
        type_name: "Account".into(),
        version,
        data: data.to_vec(),
    };

    // An append returns each draft as the store recorded it, every field kept, and a load reads the same.
    let store = new();
    let first = deposited("acct-1", "evt-1", 1);
    let appended = store
        .append(&mut S::Tx::default(), "acct-1", 0, vec![first.clone()])
        .unwrap();
    let [stored] = appended.as_slice() else {
        panic!("one draft appends one event: {appended:?}")
    };
    assert_eq!(
        stored,
        &first.record(stored.recorded_at),
        "every draft field survives; the store adds only recorded_at"
    );
    assert_eq!(
        store.load(&mut S::Tx::default(), "acct-1", 0).unwrap(),
        appended,
        "a load reads what the append returned"
    );

    // A snapshot may lag the stream but never lead it, and never moves back.
    let leading = store.save_snapshot(&mut S::Tx::default(), &snapshot(2, b"ahead"));
    assert!(
        matches!(
            leading,
            Err(Error {
                code: ErrorCode::Codec,
                ..
            })
        ),
        "a leading snapshot is refused: {leading:?}"
    );
    store
        .append(
            &mut S::Tx::default(),
            "acct-1",
            1,
            vec![deposited("acct-1", "evt-2", 2)],
        )
        .unwrap();
    store
        .save_snapshot(&mut S::Tx::default(), &snapshot(2, b"newer"))
        .unwrap();
    store
        .save_snapshot(&mut S::Tx::default(), &snapshot(1, b"older"))
        .unwrap();
    let kept = store.load_snapshot(&mut S::Tx::default(), "acct-1").unwrap().unwrap();
    assert_eq!(
        (kept.version, kept.data),
        (2, b"newer".to_vec()),
        "an older snapshot never replaces a newer one"
    );

    // An event id is unique store-wide.
    let duplicate = store.append(
        &mut S::Tx::default(),
        "acct-2",
        0,
        vec![deposited("acct-2", "evt-1", 1)],
    );
    assert!(
        matches!(
            duplicate,
            Err(Error {
                code: ErrorCode::Codec,
                ..
            })
        ),
        "a taken id is refused: {duplicate:?}"
    );
    assert!(
        store.load(&mut S::Tx::default(), "acct-2", 0).unwrap().is_empty(),
        "and writes nothing"
    );
}

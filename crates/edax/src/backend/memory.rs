//! Dev/test in-memory backends, single process, not durable. [`MemoryTransactor`] is the transaction
//! boundary; [`MemoryEventStore`], [`MemoryProjectionStore`], [`MemoryIdempotencyStore`] and
//! [`MemorySagaStore`] wrap the shared `edax-memory` stores it hands out, converting only the host's typed
//! values (a result `O`, a saga state `S`) to the adapters' JSON values or record bytes, and [`MemoryTx`] to
//! the adapters' token. [`MemoryLedger`] is the idempotency store on a database of its own, the ordering a
//! Redis ledger has. Failures need no conversion: the adapters fail with the kernel's [`Error`], which is
//! the SDK's own. For production, bring stores backed by a real database.

use std::marker::PhantomData;
use std::sync::Arc;

use edax_cqrs::{decode_prior, encode_record, Claimed};
use edax_event::{CommandMessage, EventDraft, EventMessage};
use edax_memory::SagaRow;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::idempotency::{Claim, ClaimHandle, IdempotencyStore, Record};
use crate::outbox::{CountedOutbox, Outbox};
use crate::saga::{SagaInstance, SagaStore};
use crate::{Entity, Error, EventStore, Page, ProjectionQuery, ProjectionStore, Query, Snapshot, Transactor};

/// The in-memory backend's transaction handle: the `edax-memory` token of a transaction
/// [`MemoryTransactor::begin`] opened, or `MemoryTx::auto_commit()` for a call outside any unit of work,
/// which is also its `Default` and what [`idempotent`](crate::idempotent) uses when no tx is provided. A
/// plain token, `Copy`, so a host can keep the open transaction's handle wherever it needs it (a `Cell` a
/// middleware's tx provider reads, for instance). A handle used after its transaction ended, or against
/// another transactor's stores, is refused as unknown by every store method.
pub use edax_memory::Tx as MemoryTx;

/// What every view over an `edax-memory` store shares: `new`, and the [`MemoryTransactor`] it enlists
/// with. A generic view also gets `Default` (a database of its own) and `Clone` (sharing it) by hand,
/// since a derive would demand the same of its type parameter, which the store never holds.
macro_rules! memory_view {
    ($view:ident<$param:ident> { $marker:ident }) => {
        impl<$param> Default for $view<$param> {
            fn default() -> Self {
                Self { inner: Default::default(), $marker: PhantomData }
            }
        }

        impl<$param> Clone for $view<$param> {
            fn clone(&self) -> Self {
                Self { inner: self.inner.clone(), $marker: PhantomData }
            }
        }

        memory_view!(impl $view<$param>);
    };
    ($view:ident) => {
        memory_view!(impl $view<>);
    };
    (impl $view:ident<$($param:ident)?>) => {
        impl<$($param)?> $view<$($param)?> {
            /// A store over a database of its own.
            pub fn new() -> Self {
                Self::default()
            }

            /// The transactor this store enlists with.
            pub fn transactor(&self) -> MemoryTransactor {
                MemoryTransactor { inner: self.inner.transactor() }
            }
        }
    };
}

/// The transaction boundary of the in-memory backend over `edax_memory::MemoryTransactor`: one database
/// whose four stores commit and roll back together. Open a unit of work with [`Transactor::transaction`] and
/// thread the `&mut MemoryTx` to every store; appends, snapshots, projection rows, idempotency records and
/// saga instances then land atomically on a clean exit or vanish on an error. Reads under the handle see its
/// own staged writes; nobody else sees them before the commit. `Clone` shares the database.
///
/// Isolation is by exclusion: a key an open transaction wrote is held until it ends, and a second writer gets
/// a [`ErrorCode::Concurrency`](crate::ErrorCode::Concurrency) error at once, not a wait. Every transactor
/// and every standalone `new()` store owns a database.
#[derive(Clone, Default)]
pub struct MemoryTransactor {
    inner: edax_memory::MemoryTransactor,
}

impl MemoryTransactor {
    pub fn new() -> Self {
        Self::default()
    }

    /// The event store over this database.
    pub fn event_store(&self) -> MemoryEventStore {
        MemoryEventStore {
            inner: self.inner.event_store(),
            now: default_clock(),
        }
    }

    /// The projection store over this database.
    pub fn projection_store(&self) -> MemoryProjectionStore {
        MemoryProjectionStore {
            inner: self.inner.projection_store(),
        }
    }

    /// The idempotency store over this database, typed by the handler result it caches.
    pub fn idempotency_store<O>(&self) -> MemoryIdempotencyStore<O> {
        MemoryIdempotencyStore {
            inner: self.inner.idempotency_store(),
            result: PhantomData,
        }
    }

    /// The saga store over this database, typed by the saga state it holds.
    pub fn saga_store<S>(&self) -> MemorySagaStore<S> {
        MemorySagaStore {
            inner: self.inner.saga_store(),
            state: PhantomData,
        }
    }

    /// How many transactions are open on this database: a leak diagnostic. A count that does not return
    /// to its idle level names a unit of work that was neither committed nor rolled back, and every key
    /// it staged stays conflicted for the process lifetime.
    pub fn open_count(&self) -> usize {
        self.inner.open_count()
    }
}

impl Transactor for MemoryTransactor {
    type Tx = MemoryTx;

    fn identity(&self) -> usize {
        self.inner.database()
    }

    fn begin(&self) -> Result<MemoryTx, Error> {
        Ok(self.inner.begin())
    }

    fn commit(&self, tx: MemoryTx) -> Result<(), Error> {
        self.inner.commit(tx)
    }

    fn rollback(&self, tx: MemoryTx) {
        // `false` means the token was already closed: there was nothing to roll back, and the seam
        // has no failure to report for a rollback.
        self.inner.rollback(tx);
    }
}

/// In-memory [`EventStore`] for dev and tests over `edax_memory::MemoryEventStore`. `Clone` shares one
/// backing store (the inner is shared), so the same instance can hand to a
/// [`Repository`](crate::Repository) and a [`Relay`](crate::Relay).
///
/// An `EventStore` is a [`Transactor`]: this one delegates to the [`MemoryTransactor`] it belongs
/// to, so `store.transaction(|tx| ...)` opens the database's unit of work. `new()` builds a store
/// over a database of its own; [`MemoryTransactor::event_store`] builds one that shares a database
/// with the other stores.
///
/// It records each appended event at its own clock, the kernel's
/// [`current_timestamp`](edax_core::current_timestamp), unless [`with_clock`](Self::with_clock)
/// replaces it. A clone shares the clock.
#[derive(Clone)]
pub struct MemoryEventStore {
    inner: edax_memory::MemoryEventStore,
    now: Clock,
}

/// A clock reading in unix nanoseconds. An `Arc`, so a clone of the store shares it.
type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

fn default_clock() -> Clock {
    Arc::new(edax_core::current_timestamp)
}

impl Default for MemoryEventStore {
    fn default() -> Self {
        Self {
            inner: edax_memory::MemoryEventStore::default(),
            now: default_clock(),
        }
    }
}

memory_view!(MemoryEventStore);

impl MemoryEventStore {
    /// Record appended events at `now` instead of the kernel's clock, for deterministic tests. A
    /// closure, so a caller can capture a seeded reading instead of reaching for a global.
    pub fn with_clock(mut self, now: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.now = Arc::new(now);
        self
    }
}

impl Transactor for MemoryEventStore {
    type Tx = MemoryTx;

    fn identity(&self) -> usize {
        self.transactor().identity()
    }

    fn begin(&self) -> Result<MemoryTx, Error> {
        self.transactor().begin()
    }

    fn commit(&self, tx: MemoryTx) -> Result<(), Error> {
        self.transactor().commit(tx)
    }

    fn rollback(&self, tx: MemoryTx) {
        self.transactor().rollback(tx);
    }
}

impl Outbox<EventMessage> for MemoryEventStore {
    fn unpublished(&self, limit: usize) -> Result<Vec<EventMessage>, Error> {
        Ok(self.inner.unpublished(limit))
    }

    fn mark_published(&self, ids: &[String]) -> Result<(), Error> {
        self.inner.mark_published(ids);
        Ok(())
    }
}

impl CountedOutbox<EventMessage> for MemoryEventStore {
    fn unpublished_count(&self) -> Result<usize, Error> {
        Ok(self.inner.unpublished_count())
    }
}

impl EventStore for MemoryEventStore {
    fn load(&self, tx: &mut MemoryTx, aggregate_id: &str, after: u64) -> Result<Vec<EventMessage>, Error> {
        self.inner.load(*tx, aggregate_id, after)
    }

    fn load_snapshot(&self, tx: &mut MemoryTx, aggregate_id: &str) -> Result<Option<Snapshot>, Error> {
        Ok(self.inner.load_snapshot(*tx, aggregate_id)?.map(|s| Snapshot {
            aggregate_id: aggregate_id.to_string(),
            type_name: s.type_name,
            version: s.version,
            data: s.data,
        }))
    }

    fn save_snapshot(&self, tx: &mut MemoryTx, snapshot: &Snapshot) -> Result<(), Error> {
        self.inner.save_snapshot(
            *tx,
            &snapshot.aggregate_id,
            &snapshot.type_name,
            snapshot.version,
            snapshot.data.clone(),
        )
    }

    /// Stamps `recorded_at` from the store's clock: see [`with_clock`](MemoryEventStore::with_clock).
    fn append(
        &self,
        tx: &mut MemoryTx,
        aggregate_id: &str,
        expected: u64,
        drafts: Vec<EventDraft>,
    ) -> Result<Vec<EventMessage>, Error> {
        self.inner.append(*tx, aggregate_id, expected, drafts, (self.now)())
    }
}

/// Dev/test in-memory [`ProjectionStore`] over `edax_memory::MemoryProjectionStore`; `get_many` defers
/// to the query kernel. `Clone` shares one backing store. Not for production — bring a store whose
/// `get_many` pushes the filter/sort/pagination down to its native query dialect.
///
/// `Tx = MemoryTx`: a save under an open handle is staged until the [`MemoryTransactor`] commits,
/// so a projection write commits or rolls back with the events that produced it.
#[derive(Clone, Default)]
pub struct MemoryProjectionStore {
    inner: edax_memory::MemoryProjectionStore,
}

memory_view!(MemoryProjectionStore);

impl ProjectionStore for MemoryProjectionStore {
    type Tx = MemoryTx;

    fn get(&self, tx: &mut MemoryTx, id: &str) -> Result<Option<Entity>, Error> {
        Ok(self.inner.get(*tx, id)?.map(|row| Entity {
            id: id.to_string(),
            version: row.version,
            document: Value::Object(row.value),
        }))
    }

    fn save(&self, tx: &mut MemoryTx, entity: &Entity) -> Result<u64, Error> {
        // The kernel's document rule: an object, since a read carries the row key as its `"id"`.
        let document = edax_cqrs::document(&entity.id, entity.document.clone())?;
        self.inner.save(*tx, &entity.id, entity.version, document)
    }
}

impl ProjectionQuery for MemoryProjectionStore {
    fn get_many(&self, tx: &mut MemoryTx, query: &Query) -> Result<Page, Error> {
        self.inner.get_many(*tx, query)
    }
}

/// In-process [`IdempotencyStore`] for a single service (dev/tests) over
/// `edax_memory::MemoryIdempotencyStore`, which holds the kernel's record bytes. The wrapper encodes the
/// host's typed `O` on [`complete`](IdempotencyStore::complete) and decodes it on
/// [`claim`](IdempotencyStore::claim). Methods take `&self`, so it can be shared inside a middleware closure
/// and across a [`KeyOrderedDispatcher`](crate::KeyOrderedDispatcher)'s worker threads. With `Tx = MemoryTx`
/// a claim commits or rolls back with the events it guards, which makes a guarded handler effectively-once.
/// A result the codec cannot write is reported by `complete`; a stored record that will not read back as `O`
/// is the kernel's terminal codec failure ([`edax_cqrs::decode_prior`]), so the next claimant gets
/// [`ErrorCode::CachedFailure`](crate::ErrorCode::CachedFailure); a payload mismatch is still detected.
pub struct MemoryIdempotencyStore<O> {
    inner: edax_memory::MemoryIdempotencyStore,
    // `fn() -> O` rather than `O`: the store holds bytes, not an `O`, so it is `Send + Sync` whatever
    // `O` is — the consume path shares it across dispatcher worker threads.
    result: PhantomData<fn() -> O>,
}

memory_view!(MemoryIdempotencyStore<O> { result });

/// A claim under `token`, typed: the handle on a win, or the prior record decoded as `O`.
fn claim_typed<O: DeserializeOwned>(
    inner: &edax_memory::MemoryIdempotencyStore,
    token: MemoryTx,
    key: &str,
    payload_hash: u64,
) -> Result<Claim<O>, Error> {
    // A failed claim (the pending record would not encode, or the token names no open transaction)
    // claimed nothing, so it is reported: transient, and the caller retries or nacks. Folding it into a
    // `Prior` record would tell the caller it had replayed a cached failure that was never cached.
    Ok(match inner.claim(token, key, payload_hash)? {
        Claimed::Won(handle) => Claim::Won(ClaimHandle(handle)),
        // A record that is there but will not decode as `O` is a real cached outcome the caller must not
        // re-run: the kernel reads it as a terminal codec failure.
        Claimed::Prior(bytes) => Claim::Prior(decode_prior(&bytes, payload_hash)),
    })
}

/// Settle the claim `handle` names under `token` with `record`. A result the codec cannot write is
/// terminal and the caller must see it: the guard answers a failed `complete` by releasing the claim, so
/// the key does not stay pending.
fn complete_typed<O: Serialize>(
    inner: &edax_memory::MemoryIdempotencyStore,
    token: MemoryTx,
    key: &str,
    handle: &ClaimHandle,
    record: Record<O>,
) -> Result<(), Error> {
    let bytes = encode_record(&record)?;
    inner.complete(token, key, &handle.0, &bytes)
}

impl<O: Serialize + DeserializeOwned> IdempotencyStore<O> for MemoryIdempotencyStore<O> {
    type Tx = MemoryTx;

    fn claim(&self, tx: &mut MemoryTx, key: &str, payload_hash: u64) -> Result<Claim<O>, Error> {
        claim_typed(&self.inner, *tx, key, payload_hash)
    }

    fn complete(&self, tx: &mut MemoryTx, key: &str, handle: &ClaimHandle, record: Record<O>) -> Result<(), Error> {
        complete_typed(&self.inner, *tx, key, handle, record)
    }

    fn release(&self, tx: &mut MemoryTx, key: &str, handle: &ClaimHandle) -> Result<(), Error> {
        self.inner.release(*tx, key, &handle.0)
    }

    fn forget(&self, tx: &mut MemoryTx, key: &str) -> Result<(), Error> {
        self.inner.forget(*tx, key)
    }
}

impl<O> MemoryIdempotencyStore<O> {
    /// Lapse whatever `key` holds in committed state, as a lease or a retention TTL running out would.
    /// A test hook: the next claim wins, and the old claim's handle settles nothing.
    pub fn expire(&self, key: &str) -> bool {
        self.inner.expire(key)
    }

    /// Store `bytes` as `key`'s settled record, readable or not, in committed state. A test hook that
    /// rehearses a record another writer left behind: a torn write, another layout, a hand edit. The
    /// next claim reads it as the kernel does.
    ///
    /// # Errors
    /// [`ErrorCode::Concurrency`](crate::ErrorCode::Concurrency) when an open transaction holds `key`.
    pub fn put_raw(&self, key: &str, bytes: &[u8]) -> Result<(), Error> {
        self.inner.put_raw(key, bytes)
    }
}

/// The in-memory idempotency store on a database of its own, the ordering a Redis ledger has: `Tx = ()`,
/// so every claim, complete and release lands at once, outside the handler's unit of work. A terminal
/// failure is cached, and a handler's writes can commit while the record does not, so this is the store
/// to rehearse a split ledger on. [`expire`](Self::expire) lapses a claim the way Redis's TTL does, so a
/// takeover and a stale owner's late complete (refused by the handle fence) can be tested. `Clone` shares
/// the ledger. Not durable.
pub struct MemoryLedger<O>(MemoryIdempotencyStore<O>);

impl<O> Default for MemoryLedger<O> {
    fn default() -> Self {
        Self(MemoryIdempotencyStore::default())
    }
}

impl<O> Clone for MemoryLedger<O> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<O> MemoryLedger<O> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Lapse whatever `key` holds, as a claim TTL or a record TTL running out would.
    pub fn expire(&self, key: &str) -> bool {
        self.0.expire(key)
    }

    /// Store `bytes` as `key`'s settled record, readable or not. See [`MemoryIdempotencyStore::put_raw`].
    ///
    /// # Errors
    /// Never in practice: nothing opens a transaction on a ledger's own database.
    pub fn put_raw(&self, key: &str, bytes: &[u8]) -> Result<(), Error> {
        self.0.put_raw(key, bytes)
    }
}

impl<O: Serialize + DeserializeOwned> IdempotencyStore<O> for MemoryLedger<O> {
    type Tx = ();

    fn claim(&self, _tx: &mut (), key: &str, payload_hash: u64) -> Result<Claim<O>, Error> {
        claim_typed(&self.0.inner, MemoryTx::auto_commit(), key, payload_hash)
    }

    fn complete(&self, _tx: &mut (), key: &str, handle: &ClaimHandle, record: Record<O>) -> Result<(), Error> {
        complete_typed(&self.0.inner, MemoryTx::auto_commit(), key, handle, record)
    }

    fn release(&self, _tx: &mut (), key: &str, handle: &ClaimHandle) -> Result<(), Error> {
        self.0.inner.release(MemoryTx::auto_commit(), key, &handle.0)
    }

    fn forget(&self, _tx: &mut (), key: &str) -> Result<(), Error> {
        self.0.inner.forget(MemoryTx::auto_commit(), key)
    }
}

/// Dev/test in-memory [`SagaStore`] over `edax_memory::MemorySagaStore`, which holds instance state
/// as JSON and the saga outbox. The wrapper serializes the host's typed `S` on `save` and deserializes
/// it on `load`, so the optimistic-concurrency rule (a stale write conflicts, a completed instance takes
/// none) and the command id rule are the adapter's and shared with every SDK. `Clone` shares one backing
/// store. Not for production — a durable store enlists the write in the events' transaction.
///
/// `Tx = MemoryTx`: a write under an open handle is staged until the [`MemoryTransactor`] commits.
pub struct MemorySagaStore<S> {
    inner: edax_memory::MemorySagaStore,
    // `fn() -> S` rather than `S`: the store holds JSON, not an `S`, so it is `Send + Sync` whatever `S` is.
    state: PhantomData<fn() -> S>,
}

memory_view!(MemorySagaStore<S> { state });

impl<S: Serialize + DeserializeOwned> SagaStore for MemorySagaStore<S> {
    type State = S;
    type Tx = MemoryTx;

    fn load(&self, tx: &mut MemoryTx, saga: &str, id: &str) -> Result<Option<SagaInstance<S>>, Error> {
        let Some(row) = self.inner.load(*tx, saga, id)? else {
            return Ok(None);
        };
        Ok(Some(match row.value {
            SagaRow::Completed => SagaInstance::Completed { version: row.version },
            SagaRow::Active { state, folded } => SagaInstance::Running {
                state: serde_json::from_value(state).map_err(Error::codec)?,
                version: row.version,
                folded,
            },
        }))
    }

    fn save(
        &self,
        tx: &mut MemoryTx,
        saga: &str,
        id: &str,
        state: &S,
        version: u64,
        folded: &str,
        commands: &[CommandMessage],
    ) -> Result<u64, Error> {
        let state = serde_json::to_value(state).map_err(Error::codec)?;
        self.inner
            .save(*tx, saga, id, version, state, folded.to_owned(), commands.to_vec())
    }

    fn complete(
        &self,
        tx: &mut MemoryTx,
        saga: &str,
        id: &str,
        version: u64,
        commands: &[CommandMessage],
    ) -> Result<u64, Error> {
        self.inner.complete(*tx, saga, id, version, commands.to_vec())
    }

    fn unpublished(&self, saga: &str, limit: usize) -> Result<Vec<CommandMessage>, Error> {
        Ok(self.inner.unpublished(saga, limit))
    }

    fn mark_published(&self, saga: &str, ids: &[String]) -> Result<(), Error> {
        self.inner.mark_published(saga, ids);
        Ok(())
    }

    fn unpublished_count(&self, saga: &str) -> Result<usize, Error> {
        Ok(self.inner.unpublished_count(saga))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::draft;
    use crate::ErrorCode;
    use serde::Deserialize;
    use serde_json::json;

    /// `Claim` has no `PartialEq`; a record does, and `Won` is the absence of one. The claim is
    /// unwrapped here: these cases are about what the store *stores*, and a claim that fails to
    /// reach the store at all is its own case (`a_failed_claim_is_reported_not_disguised_as_a_record`).
    fn prior<O>(claim: Result<Claim<O>, Error>) -> Option<Record<O>> {
        match claim.expect("the in-memory store claims without failing here") {
            Claim::Won(_) => None,
            Claim::Prior(record) => Some(record),
        }
    }

    /// Serializes as a string but reads back as a number, so a stored success cannot decode as `O`.
    #[derive(Debug, PartialEq)]
    struct Lossy;
    impl Serialize for Lossy {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_str("x")
        }
    }
    impl<'de> Deserialize<'de> for Lossy {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            u8::deserialize(deserializer).map(|_| Lossy)
        }
    }

    /// A result the codec refuses to encode at all.
    #[derive(Debug, PartialEq)]
    struct Unencodable;
    impl Serialize for Unencodable {
        fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("no wire form"))
        }
    }
    impl<'de> Deserialize<'de> for Unencodable {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            Value::deserialize(deserializer).map(|_| Unencodable)
        }
    }

    #[test]
    fn idempotency_codec_failures_become_cached_failures() {
        // The seam is infallible, so a codec failure must still leave the key taken and say why: the
        // next claimant gets a terminal failure carrying the codec error, never `Won` (which would run
        // the handler twice) and never a pending record that stays in flight forever.
        const HASH: u64 = 11_234_062_831_410_934_249;

        // Stored success that cannot be read back as `O`: the failure is synthesized on `claim`.
        let store = MemoryIdempotencyStore::<Lossy>::new();
        let Ok(Claim::Won(handle)) = store.claim(&mut MemoryTx::auto_commit(), "k", HASH) else {
            panic!("a free key")
        };
        store
            .complete(&mut MemoryTx::auto_commit(), "k", &handle, Record::done(HASH, Lossy))
            .unwrap();
        let got = prior(store.claim(&mut MemoryTx::auto_commit(), "k", HASH)).expect("key stays taken");
        assert_eq!(got.payload_hash, HASH);
        let edax_cqrs::State::Failed { code, message: m } = &got.state else {
            panic!("want Failed, got {:?}", got.state)
        };
        assert_eq!(*code, Some(ErrorCode::Codec), "cached with the codec's own code");
        assert!(m.contains("not a valid replay value"), "{m}");

        // ...and the record keeps the *stored* hash, so a claimant carrying a different command is
        // still told the key was reused rather than handed a failure its command did not cause.
        let got = prior(store.claim(&mut MemoryTx::auto_commit(), "k", HASH ^ 1)).expect("key stays taken");
        assert_eq!(got.payload_hash, HASH, "the stored hash, not the claimant's");
        assert_eq!(edax_cqrs::outcome(&got, HASH ^ 1), edax_cqrs::Outcome::PayloadMismatch);

        // Success that cannot be encoded: `complete` reports the codec failure instead of writing a
        // stand-in record, and leaves the claim pending. The guard never gets here: it encodes the
        // result before `complete` and caches the codec failure (`idempotency.rs`), so this guards a
        // caller that writes to the store directly.
        let store = MemoryIdempotencyStore::<Unencodable>::new();
        let Ok(Claim::Won(handle)) = store.claim(&mut MemoryTx::auto_commit(), "k", HASH) else {
            panic!("a free key")
        };
        let wrote = store.complete(
            &mut MemoryTx::auto_commit(),
            "k",
            &handle,
            Record::done(HASH, Unencodable),
        );
        assert!(
            matches!(&wrote, Err(Error { code: ErrorCode::Codec, message: m }) if m == "no wire form"),
            "got {wrote:?}"
        );
        let got = prior(store.claim(&mut MemoryTx::auto_commit(), "k", HASH)).expect("key stays taken");
        assert_eq!(
            got.state,
            edax_cqrs::State::Pending,
            "nothing was written over the claim"
        );
    }

    #[test]
    fn an_unreadable_stored_record_replays_as_the_kernels_codec_failure() {
        // Bytes another writer left under the key (a torn write, a foreign layout, a wrong type) are a
        // cached outcome nobody can read: the kernel's `decode_prior` turns them into a terminal codec
        // failure. The stored hash is kept when it still reads, so a reused key is still a mismatch.
        const HASH: u64 = 11_234_062_831_410_934_249;
        for (name, bytes, claimant, want_hash, cause) in [
            (
                "torn_write",
                br#"{"payload_hash":11234062831410934249,"res"#.as_slice(),
                HASH,
                HASH,
                "EOF while parsing",
            ),
            (
                "wrong_type_keeps_the_stored_hash",
                br#"{"payload_hash":11234062831410934249,"error":42}"#.as_slice(),
                HASH ^ 1,
                HASH,
                "invalid type: integer `42`",
            ),
            (
                "result_not_an_o",
                br#"{"payload_hash":11234062831410934249,"result":"x","error":null,"pending":false}"#.as_slice(),
                HASH,
                HASH,
                "cached result is not a valid replay value",
            ),
        ] {
            let store = MemoryIdempotencyStore::<u64>::new();
            store.put_raw("k", bytes).unwrap();
            let got = prior(store.claim(&mut MemoryTx::auto_commit(), "k", claimant)).expect("key stays taken");
            assert_eq!(got.payload_hash, want_hash, "{name}");
            let edax_cqrs::State::Failed { code, message } = &got.state else {
                panic!("{name}: want Failed, got {:?}", got.state)
            };
            assert_eq!(*code, Some(ErrorCode::Codec), "{name}");
            assert!(
                message.starts_with("unreadable idempotency record: ") && message.contains(cause),
                "{name}: {message}"
            );
        }
    }

    #[test]
    fn put_raw_overwrites_a_committed_record_and_refuses_a_held_key() {
        let db = MemoryTransactor::new();
        let store = db.idempotency_store::<u64>();
        let Ok(Claim::Won(handle)) = store.claim(&mut MemoryTx::auto_commit(), "k", 1) else {
            panic!("a free key")
        };
        store
            .complete(&mut MemoryTx::auto_commit(), "k", &handle, Record::done(1, 5))
            .unwrap();
        store.put_raw("k", b"{").expect("a committed record is overwritten");
        let got = prior(store.claim(&mut MemoryTx::auto_commit(), "k", 1)).expect("key stays taken");
        assert!(
            matches!(
                got.state,
                edax_cqrs::State::Failed {
                    code: Some(ErrorCode::Codec),
                    ..
                }
            ),
            "{got:?}"
        );

        // A key an open transaction holds is not committed state: refused, and the holder keeps it.
        let mut tx = db.begin().unwrap();
        assert!(matches!(store.claim(&mut tx, "held", 1), Ok(Claim::Won(_))));
        let refused = store.put_raw("held", b"{");
        assert!(
            matches!(&refused, Err(Error { code: ErrorCode::Concurrency, message: m }) if m.contains("held by another open transaction")),
            "{refused:?}"
        );
        db.rollback(tx);

        let ledger = MemoryLedger::<u64>::new();
        ledger.put_raw("k", b"{").unwrap();
        let Ok(Claim::Prior(got)) = ledger.claim(&mut (), "k", 1) else {
            panic!("key stays taken")
        };
        assert!(
            matches!(
                got.state,
                edax_cqrs::State::Failed {
                    code: Some(ErrorCode::Codec),
                    ..
                }
            ),
            "{got:?}"
        );
    }

    #[test]
    fn a_projection_document_must_be_an_object() {
        // A read carries the row key as the document's `"id"`, so a scalar or an array could be saved
        // and never found: refused at the write, as the terminal codec error.
        let store = MemoryProjectionStore::new();
        for (name, document) in [
            ("array", json!(["ada@example.com"])),
            ("scalar", json!(36)),
            ("null", Value::Null),
        ] {
            let got = store.save(
                &mut MemoryTx::auto_commit(),
                &Entity {
                    id: "user-1".into(),
                    version: 0,
                    document,
                },
            );
            assert!(
                matches!(&got, Err(Error { code: ErrorCode::Codec, message: m }) if *m == edax_cqrs::document("user-1", Value::Null).unwrap_err().message),
                "{name}: {got:?}"
            );
        }
        assert!(
            store.get(&mut MemoryTx::auto_commit(), "user-1").unwrap().is_none(),
            "nothing was written"
        );
    }

    #[test]
    fn the_ledger_lands_at_once_and_its_fence_refuses_a_stale_owner() {
        // A ledger of its own: a claim lands outside any unit of work, so the transactor's rollback does
        // not take it back. After an expiry and a takeover the old owner's handle settles nothing.
        const HASH: u64 = 11_234_062_831_410_934_249;
        let ledger = MemoryLedger::<i64>::new();
        let db = MemoryTransactor::new();
        let out: Result<(), Error> = db.transaction(|_tx| {
            assert!(matches!(ledger.claim(&mut (), "k", HASH), Ok(Claim::Won(_))));
            Err(Error::store("the handler failed"))
        });
        assert!(out.is_err());
        let Ok(Claim::Prior(pending)) = ledger.claim(&mut (), "k", HASH) else {
            panic!("the claim outlived the rollback")
        };
        assert!(matches!(pending.state, edax_cqrs::State::Pending));

        assert!(ledger.expire("k"));
        let Ok(Claim::Won(live)) = ledger.claim(&mut (), "k", HASH) else {
            panic!("an expired key is free")
        };
        let stale = ClaimHandle(b"not this claim".to_vec());
        let refused = ledger.complete(&mut (), "k", &stale, Record::done(HASH, 1));
        assert!(
            matches!(
                &refused,
                Err(Error {
                    code: ErrorCode::Store,
                    ..
                })
            ),
            "{refused:?}"
        );
        ledger
            .release(&mut (), "k", &stale)
            .expect("a stale release is a no-op");
        ledger
            .complete(&mut (), "k", &live, Record::done(HASH, 2))
            .expect("the live claim settles");
        let Ok(Claim::Prior(done)) = ledger.clone().claim(&mut (), "k", HASH) else {
            panic!("settled")
        };
        assert!(matches!(done.state, edax_cqrs::State::Done(2)));
    }

    #[test]
    fn the_leak_and_backlog_diagnostics_count() {
        let db = MemoryTransactor::new();
        let events = db.event_store();
        assert_eq!((db.open_count(), events.unpublished_count()), (0, Ok(0)));
        let mut tx = db.begin().unwrap();
        events.append(&mut tx, "a", 0, vec![draft("a", "E", 1, &[])]).unwrap();
        assert_eq!(
            (db.open_count(), events.unpublished_count()),
            (1, Ok(0)),
            "staged: open, nothing waiting"
        );
        db.commit(tx).unwrap();
        assert_eq!(
            (db.open_count(), events.unpublished_count()),
            (0, Ok(1)),
            "committed: closed, one waiting"
        );
        events.mark_published(&["a-1".to_owned()]).unwrap();
        assert_eq!(events.unpublished_count(), Ok(0));
    }

    #[test]
    fn memory_stores_over_one_database_share_one_identity() {
        let db = MemoryTransactor::new();
        assert_eq!(
            db.identity(),
            db.event_store().identity(),
            "a store and its transactor are one database"
        );
        assert_eq!(db.identity(), db.clone().identity(), "a clone shares the database");
        assert_ne!(
            db.identity(),
            MemoryTransactor::new().identity(),
            "another transactor is another database"
        );
    }

    #[test]
    fn transaction_threads_one_tx_to_events_and_projection() {
        // Both stores over one transactor: a store built standalone has a database of its own, and
        // the event store's `tx` would be unknown to it.
        let db = MemoryTransactor::new();
        let events = db.event_store();
        let proj = db.projection_store();

        // One unit of work over the event store's Transactor; the same `tx` drives both writes.
        events
            .transaction(|tx| {
                events.append(tx, "a", 0, vec![draft("a", "E", 1, &[])])?;
                // Read-your-writes inside the unit of work: the append is visible before commit.
                assert_eq!(events.load(tx, "a", 0)?.len(), 1, "own write visible in the tx");
                proj.save(
                    tx,
                    &Entity {
                        id: "a".into(),
                        version: 0,
                        document: json!({"n": 1}),
                    },
                )?;
                assert_eq!(
                    proj.get(tx, "a")?.map(|e| e.version),
                    Some(1),
                    "own save visible in the tx"
                );
                // ...and to nobody else before the commit.
                assert!(
                    events.load(&mut MemoryTx::auto_commit(), "a", 0)?.is_empty(),
                    "invisible outside"
                );
                assert!(
                    proj.get(&mut MemoryTx::auto_commit(), "a")?.is_none(),
                    "invisible outside"
                );
                Ok::<(), Error>(())
            })
            .unwrap();

        // Both writes are visible after the commit.
        let mut tx = MemoryTx::auto_commit();
        assert_eq!(events.load(&mut tx, "a", 0).unwrap().len(), 1);
        assert_eq!(proj.get(&mut tx, "a").unwrap().unwrap().document["n"], json!(1));

        // A body that returns Err rolls back (no panic, error propagates), and its writes vanish.
        // Free-fn form is equivalent.
        let out = crate::transaction(&events, |tx| {
            events.append(tx, "a", 1, vec![draft("a", "E", 2, &[])])?;
            Err::<(), _>(Error::store("boom"))
        });
        assert!(matches!(
            out,
            Err(Error {
                code: ErrorCode::Store,
                ..
            })
        ));
        assert_eq!(
            events.load(&mut tx, "a", 0).unwrap().len(),
            1,
            "the rolled-back append is gone"
        );

        // A standalone store is its own database: the transactor's handle is unknown to it. That is
        // `BadHandle` — terminal, a wiring mistake — and the Python backend refuses a foreign handle
        // with the same verdict (`edax.backend.memory.MemoryTransactor.token`).
        let stranger = MemoryProjectionStore::new();
        let out: Result<_, Error> = db.transaction(|tx| stranger.get(tx, "a"));
        assert!(
            matches!(&out, Err(Error { code: ErrorCode::BadHandle, message: m }) if m.starts_with("unknown transaction")),
            "got {out:?}"
        );
        assert!(
            out.unwrap_err().is_terminal(),
            "a wiring mistake must not be retried forever"
        );
    }

    #[test]
    fn saga_store_codec_failures_are_named() {
        // A stored state that cannot be read back as `S` is a codec error, never a silently invented
        // instance; an `S` that cannot be encoded never reaches the adapter.
        let lossy = MemorySagaStore::<Lossy>::new();
        lossy
            .save(&mut MemoryTx::auto_commit(), "order", "k", &Lossy, 0, "", &[])
            .unwrap();
        assert!(matches!(
            lossy.load(&mut MemoryTx::auto_commit(), "order", "k"),
            Err(Error {
                code: ErrorCode::Codec,
                ..
            })
        ));

        let unencodable = MemorySagaStore::<Unencodable>::new();
        assert!(matches!(
            unencodable.save(&mut MemoryTx::auto_commit(), "order", "k", &Unencodable, 0, "", &[]),
            Err(Error {
                code: ErrorCode::Codec,
                ..
            })
        ));
        assert!(
            unencodable
                .load(&mut MemoryTx::auto_commit(), "order", "k")
                .unwrap()
                .is_none(),
            "nothing was written"
        );
    }
}

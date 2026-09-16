//! Dev/test in-memory backends, single process, not durable. [`MemoryTransactor`] is the transaction
//! boundary; [`MemoryEventStore`], [`MemoryProjectionStore`], [`MemoryIdempotencyStore`] and
//! [`MemorySagaStore`] wrap the shared `edax-memory` stores it hands out, converting only at the boundary:
//! `u64` versions to the adapters' `i64`, [`AdapterError`] to [`Error`], typed values to JSON or record
//! bytes, and [`MemoryTx`] to the adapters' token. For production, bring stores backed by a real database.

use std::marker::PhantomData;

use edax_core::AdapterError;
use edax_cqrs::{decode_record, encode_record};
use edax_event::EventEnvelope;
use edax_memory::NO_TX;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::command::{Claim, IdempotencyStore, Record};
use crate::saga::{SagaInstance, SagaStore};
use crate::{Entity, Error, EventStore, ProjectionStore, Query, Snapshot, Transactor};

/// The adapters' typed error to the SDK's. Field to field, no message parsing: the versions are
/// numbers on both sides, so no wording of the adapters' message can change what arrives here.
/// Exhaustive, so an adapter variant added later must be given an SDK variant before this compiles.
impl From<AdapterError> for Error {
    fn from(e: AdapterError) -> Self {
        match e {
            AdapterError::Concurrency { expected, stored } => {
                Error::Concurrency { expected: from_adapter(expected), stored: from_adapter(stored) }
            }
            AdapterError::Codec(message) => Error::Codec(message),
            AdapterError::Store(message) => Error::Store(message),
            AdapterError::BadHandle(message) => Error::BadHandle(message),
        }
    }
}

/// The in-memory backend's transaction handle: the token of a transaction [`MemoryTransactor::begin`]
/// opened, or [`MemoryTx::auto_commit()`] for a call outside any unit of work. A plain token, `Copy`,
/// so a host can keep the open transaction's handle wherever it needs it (a `Cell` a middleware's
/// tx provider reads, for instance). A handle used after its transaction ended, or against another
/// transactor's stores, is refused as unknown by every store method.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryTx(i64);

impl MemoryTx {
    /// No transaction: a write lands at once and a read sees only committed state. Also the
    /// `Default` handle, which is what [`idempotent`](crate::idempotent) uses when no tx is provided.
    pub fn auto_commit() -> MemoryTx {
        MemoryTx(NO_TX)
    }
}

impl Default for MemoryTx {
    fn default() -> Self {
        Self::auto_commit()
    }
}

/// The transaction boundary of the in-memory backend over `edax_memory::MemoryTransactor`: one database
/// whose four stores commit and roll back together. Open a unit of work with [`Transactor::transaction`] and
/// thread the `&mut MemoryTx` to every store; appends, snapshots, projection rows, idempotency records and
/// saga instances then land atomically on a clean exit or vanish on an error. Reads under the handle see its
/// own staged writes; nobody else sees them before the commit. `Clone` shares the database.
///
/// Isolation is by exclusion: a key an open transaction wrote is held until it ends, and a second writer gets
/// [`Error::Concurrency`] at once, not a wait. Every transactor and every standalone `new()` store owns a database.
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
        MemoryEventStore { inner: self.inner.event_store() }
    }

    /// The projection store over this database.
    pub fn projection_store(&self) -> MemoryProjectionStore {
        MemoryProjectionStore { inner: self.inner.projection_store() }
    }

    /// The idempotency store over this database, typed by the handler result it caches.
    pub fn idempotency_store<O>(&self) -> MemoryIdempotencyStore<O> {
        MemoryIdempotencyStore { inner: self.inner.idempotency_store(), result: PhantomData }
    }

    /// The saga store over this database, typed by the saga state it holds.
    pub fn saga_store<S>(&self) -> MemorySagaStore<S> {
        MemorySagaStore { inner: self.inner.saga_store(), state: PhantomData }
    }
}

impl Transactor for MemoryTransactor {
    type Tx = MemoryTx;

    fn begin(&self) -> Result<MemoryTx, Error> {
        Ok(MemoryTx(self.inner.begin()))
    }

    fn commit(&self, tx: MemoryTx) -> Result<(), Error> {
        self.inner.commit(tx.0).map_err(Error::from)
    }

    fn rollback(&self, tx: MemoryTx) {
        // `false` means the token was already closed: there was nothing to roll back, and the seam
        // has no failure to report for a rollback.
        self.inner.rollback(tx.0);
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
#[derive(Clone, Default)]
pub struct MemoryEventStore {
    inner: edax_memory::MemoryEventStore,
}

impl MemoryEventStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// The transactor this store enlists with.
    pub fn transactor(&self) -> MemoryTransactor {
        MemoryTransactor { inner: self.inner.transactor() }
    }
}

impl Transactor for MemoryEventStore {
    type Tx = MemoryTx;

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

/// The adapters' surface is `i64` (it is shaped for the binding, where unsigned types are awkward);
/// the SDK's seams are `u64`. A plain `as` cast wraps at the boundary — a version past `i64::MAX`
/// would arrive negative and sort before every real row. Saturating keeps the order monotonic, and
/// the bound is unreachable in practice for a version or a batch limit.
fn to_adapter(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// The inverse. A negative version can only come from adapter corruption; clamp rather than wrap to
/// a huge `u64`.
fn from_adapter(n: i64) -> u64 {
    u64::try_from(n).unwrap_or(0)
}

impl EventStore for MemoryEventStore {
    fn load(&self, tx: &mut MemoryTx, aggregate_id: &str, after: u64) -> Result<Vec<EventEnvelope>, Error> {
        self.inner.load(tx.0, aggregate_id, to_adapter(after)).map_err(Error::from)
    }

    fn load_snapshot(&self, tx: &mut MemoryTx, aggregate_id: &str) -> Result<Option<Snapshot>, Error> {
        Ok(self.inner.load_snapshot(tx.0, aggregate_id)?.map(|s| Snapshot {
            aggregate_id: aggregate_id.to_string(),
            type_name: s.type_name,
            version: from_adapter(s.version),
            data: s.data,
        }))
    }

    fn save_snapshot(&self, tx: &mut MemoryTx, snapshot: &Snapshot) -> Result<(), Error> {
        self.inner
            .save_snapshot(
                tx.0,
                &snapshot.aggregate_id,
                &snapshot.type_name,
                to_adapter(snapshot.version),
                snapshot.data.clone(),
            )
            .map_err(Error::from)
    }

    fn append(&self, tx: &mut MemoryTx, aggregate_id: &str, expected: u64, events: &[EventEnvelope]) -> Result<(), Error> {
        self.inner.append(tx.0, aggregate_id, to_adapter(expected), events.to_vec()).map_err(Error::from)
    }

    fn unpublished(&self, limit: usize) -> Result<Vec<EventEnvelope>, Error> {
        Ok(self.inner.unpublished(i64::try_from(limit).unwrap_or(i64::MAX)))
    }

    fn mark_published(&self, ids: &[String]) -> Result<(), Error> {
        self.inner.mark_published(ids.to_vec());
        Ok(())
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

impl MemoryProjectionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// The transactor this store enlists with.
    pub fn transactor(&self) -> MemoryTransactor {
        MemoryTransactor { inner: self.inner.transactor() }
    }
}

/// The adapter's stored-entity row (`{"id", "version", "document"}`). Typed rather than indexed out of
/// a `Value`: a row missing either field is a named codec error instead of a silent version `0` and a
/// null document, and an invented `0` would make the next save's conflict report a version the caller
/// never held.
#[derive(serde::Deserialize)]
struct StoredEntity {
    version: i64,
    document: Value,
}

/// Parse one adapter row. The caller's `id` is authoritative — it is the key the row was fetched by —
/// so the row's own `"id"` is ignored.
fn stored_entity(id: &str, raw: &str) -> Result<Entity, Error> {
    let stored: StoredEntity = serde_json::from_str(raw).map_err(|e| Error::Codec(e.to_string()))?;
    Ok(Entity { id: id.to_string(), version: from_adapter(stored.version), document: stored.document })
}

impl ProjectionStore for MemoryProjectionStore {
    type Tx = MemoryTx;

    fn get(&self, tx: &mut MemoryTx, id: &str) -> Result<Option<Entity>, Error> {
        match self.inner.get(tx.0, id).map_err(Error::from)? {
            Some(raw) => stored_entity(id, &raw).map(Some),
            None => Ok(None),
        }
    }

    fn save(&self, tx: &mut MemoryTx, entity: &mut Entity) -> Result<(), Error> {
        let doc = serde_json::to_string(&entity.document).map_err(|e| Error::Codec(e.to_string()))?;
        let next = self.inner.save(tx.0, &entity.id, to_adapter(entity.version), &doc).map_err(Error::from)?;
        entity.version = from_adapter(next);
        Ok(())
    }

    fn get_many(&self, tx: &mut MemoryTx, query: &Query) -> Result<Vec<Value>, Error> {
        let codec = |e: serde_json::Error| Error::Codec(e.to_string());
        let spec = serde_json::to_string(query).map_err(codec)?;
        let page = self.inner.get_many(tx.0, &spec).map_err(Error::from)?;
        serde_json::from_str(&page).map_err(codec)
    }
}

/// In-process [`IdempotencyStore`] for a single service (dev/tests) over
/// `edax_memory::MemoryIdempotencyStore`, which holds the kernel's record bytes. The wrapper encodes the
/// host's typed `O` on [`complete`](IdempotencyStore::complete) and decodes it on
/// [`claim`](IdempotencyStore::claim). Methods take `&self`, so it can be shared inside a middleware closure
/// and across a [`KeyOrderedDispatcher`](crate::KeyOrderedDispatcher)'s worker threads. With `Tx = MemoryTx`
/// a claim commits or rolls back with the events it guards, which makes a guarded handler effectively-once.
/// A record the codec cannot write or read back is kept as a terminal failure naming the codec error, never
/// dropped, so the next claimant gets [`Error::CachedFailure`]; a payload mismatch is still detected.
pub struct MemoryIdempotencyStore<O> {
    inner: edax_memory::MemoryIdempotencyStore,
    // `fn() -> O` rather than `O`: the store holds bytes, not an `O`, so it is `Send + Sync` whatever
    // `O` is — the consume path shares it across dispatcher worker threads.
    result: PhantomData<fn() -> O>,
}

impl<O> Default for MemoryIdempotencyStore<O> {
    fn default() -> Self {
        Self { inner: edax_memory::MemoryIdempotencyStore::new(), result: PhantomData }
    }
}

impl<O> Clone for MemoryIdempotencyStore<O> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), result: PhantomData }
    }
}

impl<O> MemoryIdempotencyStore<O> {
    pub fn new() -> Self {
        Self::default()
    }

    /// The transactor this store enlists with.
    pub fn transactor(&self) -> MemoryTransactor {
        MemoryTransactor { inner: self.inner.transactor() }
    }
}

/// The terminal record that stands in for one the codec could not marshal. The message is the codec
/// error in the form the middleware itself caches for a terminal handler error, so a claimant whose
/// payload matches gets a [`CachedFailure`](crate::Error::CachedFailure) naming the cause.
fn unreadable<O>(payload_hash: u64, cause: Error) -> Record<O> {
    Record::failed(payload_hash, cause.to_string())
}

/// The payload hash of a record that would not decode as `O`. Only the `Done` value is typed, so the
/// hash itself normally still parses — keep it, or the reuse check would be lost to the codec failure
/// and a claimant carrying a *different* command would be told its own command failed instead of that
/// the key was reused. Falls back to the claimant's hash when the bytes are not a record at all.
fn stored_hash(bytes: &[u8], claimant: u64) -> u64 {
    decode_record::<Value>(bytes).map_or(claimant, |r| r.payload_hash)
}

impl<O: Serialize + DeserializeOwned> IdempotencyStore<O> for MemoryIdempotencyStore<O> {
    type Tx = MemoryTx;

    fn claim(&self, tx: &mut MemoryTx, key: &str, payload_hash: u64) -> Result<Claim<O>, Error> {
        match self.inner.claim(tx.0, key, payload_hash) {
            Ok(None) => Ok(Claim::Won),
            // A record that is there but will not decode as `O` is a real cached outcome the caller
            // must not re-run, so it stays a `Prior` and reads back as a terminal codec failure.
            Ok(Some(bytes)) => Ok(Claim::Prior(
                decode_record(&bytes)
                    .unwrap_or_else(|e| unreadable(stored_hash(&bytes, payload_hash), Error::Codec(e))),
            )),
            // The adapter could not write the pending record (or the handle names no open
            // transaction). Nothing was claimed, so report the failure: transient, and the caller
            // retries or nacks. Folding it into a `Prior` record would tell the caller it had
            // replayed a cached failure that was never cached, and terminal at that.
            Err(e) => Err(Error::from(e)),
        }
    }

    fn complete(&self, tx: &mut MemoryTx, key: &str, record: Record<O>) -> Result<(), Error> {
        // A result the codec cannot write is terminal and the caller must see it: the guard
        // answers a failed `complete` by releasing the claim, so the key does not stay pending.
        let bytes = encode_record(&record).map_err(Error::Codec)?;
        self.inner.complete(tx.0, key, &bytes).map_err(Error::from)
    }

    fn release(&self, tx: &mut MemoryTx, key: &str) -> Result<(), Error> {
        self.inner.release(tx.0, key).map_err(Error::from)
    }
}

/// Dev/test in-memory [`SagaStore`] over `edax_memory::MemorySagaStore`, which holds instance state
/// as JSON. The wrapper serializes the host's typed `S` on `save` and deserializes it on `load`, so the
/// optimistic-concurrency rule (a stale `save` or `delete` conflicts, an absent `delete` is a no-op) is
/// the adapters' and shared with every SDK. `Clone` shares one backing store. Not for production — a
/// durable store enlists the write in the events' transaction.
///
/// `Tx = MemoryTx`: a write under an open handle is staged until the [`MemoryTransactor`] commits.
pub struct MemorySagaStore<S> {
    inner: edax_memory::MemorySagaStore,
    // `fn() -> S` rather than `S`: the store holds JSON, not an `S`, so it is `Send + Sync` whatever `S` is.
    state: PhantomData<fn() -> S>,
}

impl<S> Default for MemorySagaStore<S> {
    fn default() -> Self {
        Self { inner: edax_memory::MemorySagaStore::new(), state: PhantomData }
    }
}

impl<S> Clone for MemorySagaStore<S> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), state: PhantomData }
    }
}

impl<S> MemorySagaStore<S> {
    pub fn new() -> Self {
        Self::default()
    }

    /// The transactor this store enlists with.
    pub fn transactor(&self) -> MemoryTransactor {
        MemoryTransactor { inner: self.inner.transactor() }
    }
}

/// The adapter's stored-instance row (`{"id", "version", "state"}`). Typed for the same reason as
/// [`StoredEntity`]: a row missing a field is a named codec error, never an invented version `0`.
#[derive(serde::Deserialize)]
struct StoredInstance {
    version: i64,
    state: Value,
}

impl<S: Serialize + DeserializeOwned> SagaStore for MemorySagaStore<S> {
    type State = S;
    type Tx = MemoryTx;

    fn load(&self, tx: &mut MemoryTx, id: &str) -> Result<Option<SagaInstance<S>>, Error> {
        let Some(raw) = self.inner.load(tx.0, id).map_err(Error::from)? else { return Ok(None) };
        let codec = |e: serde_json::Error| Error::Codec(e.to_string());
        let stored: StoredInstance = serde_json::from_str(&raw).map_err(codec)?;
        let state: S = serde_json::from_value(stored.state).map_err(codec)?;
        Ok(Some(SagaInstance { state, version: from_adapter(stored.version) }))
    }

    fn save(&self, tx: &mut MemoryTx, id: &str, state: &S, version: u64) -> Result<u64, Error> {
        let json = serde_json::to_string(state).map_err(|e| Error::Codec(e.to_string()))?;
        self.inner.save(tx.0, id, to_adapter(version), &json).map(from_adapter).map_err(Error::from)
    }

    fn delete(&self, tx: &mut MemoryTx, id: &str, version: u64) -> Result<(), Error> {
        self.inner.delete(tx.0, id, to_adapter(version)).map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edax_event::Event;
    use serde::Deserialize;
    use serde_json::json;

    fn env(aggregate_id: &str, sequence: u64) -> EventEnvelope {
        EventEnvelope {
            id: format!("{aggregate_id}-{sequence}"),
            aggregate_id: aggregate_id.to_string(),
            event: Event { name: "E".into(), version: 1, data: Vec::new() },
            sequence: Some(sequence),
            namespace: Default::default(),
            metadata: Default::default(),
            timestamp: 0,
        }
    }

    /// `Claim` has no `PartialEq`; a record does, and `Won` is the absence of one. The claim is
    /// unwrapped here: these cases are about what the store *stores*, and a claim that fails to
    /// reach the store at all is its own case (`a_failed_claim_is_reported_not_disguised_as_a_record`).
    fn prior<O>(claim: Result<Claim<O>, Error>) -> Option<Record<O>> {
        match claim.expect("the in-memory store claims without failing here") {
            Claim::Won => None,
            Claim::Prior(record) => Some(record),
        }
    }

    #[test]
    fn version_conversion_saturates_instead_of_wrapping() {
        // A plain `as` cast sends a version past i64::MAX across as a negative, which would sort
        // before every real row; the inverse sends a negative back as a huge u64.
        assert_eq!(to_adapter(u64::MAX), i64::MAX);
        assert_eq!(to_adapter(0), 0);
        assert_eq!(to_adapter(7), 7);
        assert_eq!(from_adapter(i64::MAX), i64::MAX as u64);
        assert_eq!(from_adapter(-1), 0);
        assert_eq!(from_adapter(7), 7);
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
        let _ = store.claim(&mut MemoryTx::auto_commit(), "k", HASH);
        store.complete(&mut MemoryTx::auto_commit(), "k", Record::done(HASH, Lossy)).unwrap();
        let got = prior(store.claim(&mut MemoryTx::auto_commit(), "k", HASH)).expect("key stays taken");
        assert_eq!(got.payload_hash, HASH);
        let edax_cqrs::State::Failed(m) = &got.state else { panic!("want Failed, got {:?}", got.state) };
        assert!(m.starts_with("codec error: ") && m.contains("not a valid replay value"), "{m}");

        // ...and the record keeps the *stored* hash, so a claimant carrying a different command is
        // still told the key was reused rather than handed a failure its command did not cause.
        let got = prior(store.claim(&mut MemoryTx::auto_commit(), "k", HASH ^ 1)).expect("key stays taken");
        assert_eq!(got.payload_hash, HASH, "the stored hash, not the claimant's");
        assert_eq!(edax_cqrs::outcome(&got, HASH ^ 1), edax_cqrs::Outcome::PayloadMismatch);

        // Success that cannot be encoded: `complete` reports the codec failure instead of writing a
        // stand-in record. The guard answers that by releasing the claim, so the key is free again
        // rather than pending for the store's whole retention.
        let store = MemoryIdempotencyStore::<Unencodable>::new();
        let _ = store.claim(&mut MemoryTx::auto_commit(), "k", HASH);
        let wrote = store.complete(&mut MemoryTx::auto_commit(), "k", Record::done(HASH, Unencodable));
        assert!(matches!(&wrote, Err(Error::Codec(m)) if m == "no wire form"), "got {wrote:?}");
        store.release(&mut MemoryTx::auto_commit(), "k").expect("the guard releases the claim");
        assert!(matches!(store.claim(&mut MemoryTx::auto_commit(), "k", HASH), Ok(Claim::Won)), "the key is free");
    }

    #[test]
    fn adapter_error_maps_field_for_field() {
        // One case per adapter variant, plus the `i64` -> `u64` edges of the concurrency versions.
        // The conversion is a match on fields: there is no message to parse and no degraded path.
        struct Case {
            name: &'static str,
            input: AdapterError,
            want: fn(&Error) -> bool,
        }
        let cases = [
            Case {
                name: "concurrency carries both versions",
                input: AdapterError::Concurrency { expected: 3, stored: 5 },
                want: |e| matches!(e, Error::Concurrency { expected: 3, stored: 5 }),
            },
            Case {
                // A negative version can only come from adapter corruption; it clamps to 0 rather
                // than wrapping to a huge `u64` the caller would then retry against.
                name: "concurrency clamps a negative version",
                input: AdapterError::Concurrency { expected: -1, stored: i64::MAX },
                want: |e| matches!(e, Error::Concurrency { expected: 0, stored } if *stored == i64::MAX as u64),
            },
            Case {
                name: "codec keeps the bare message",
                input: AdapterError::Codec("bad json".to_owned()),
                want: |e| matches!(e, Error::Codec(m) if m == "bad json"),
            },
            Case {
                name: "store keeps the bare message",
                input: AdapterError::Store("connection to the event store was reset".to_owned()),
                want: |e| matches!(e, Error::Store(m) if m == "connection to the event store was reset"),
            },
        ];
        for c in cases {
            let got = Error::from(c.input.clone());
            assert!((c.want)(&got), "{}: got {got:?}", c.name);
            // The verdict is the code's on both sides, so the mapping cannot flip it.
            assert_eq!(got.code(), c.input.code(), "{}: code", c.name);
            assert_eq!(crate::command::is_terminal(&got), c.input.code().is_terminal(), "{}: verdict", c.name);
        }
    }

    #[test]
    fn stored_entity_branches() {
        // The adapter's row shape is a contract; a row that breaks it is a named codec error, never a
        // silently invented version 0 — which would make the next save's conflict report a version the
        // caller never held.
        struct Case {
            name: &'static str,
            raw: &'static str,
            /// The parsed `(version, document)`, or `Err` for a row that must be rejected.
            want: Result<(u64, Value), ()>,
        }
        let cases = [
            Case {
                name: "well formed row",
                raw: r#"{"id":"u1","version":3,"document":{"n":1}}"#,
                want: Ok((3, json!({"n": 1}))),
            },
            Case {
                name: "version 0 is a row saved once at v0",
                raw: r#"{"id":"u1","version":0,"document":{}}"#,
                want: Ok((0, json!({}))),
            },
            Case {
                name: "the row's own id is ignored, the lookup key wins",
                raw: r#"{"id":"other","version":1,"document":{}}"#,
                want: Ok((1, json!({}))),
            },
            Case {
                name: "negative version clamps rather than wrapping",
                raw: r#"{"id":"u1","version":-1,"document":{}}"#,
                want: Ok((0, json!({}))),
            },
            Case { name: "missing version", raw: r#"{"id":"u1","document":{}}"#, want: Err(()) },
            Case { name: "missing document", raw: r#"{"id":"u1","version":1}"#, want: Err(()) },
            Case { name: "not an object", raw: "[]", want: Err(()) },
            Case { name: "malformed json", raw: "{", want: Err(()) },
        ];
        for c in cases {
            let got = stored_entity("u1", c.raw);
            match c.want {
                Ok((version, document)) => {
                    let e = got.unwrap_or_else(|e| panic!("{}: {e}", c.name));
                    assert_eq!((e.id.as_str(), e.version, &e.document), ("u1", version, &document), "{}", c.name);
                }
                Err(()) => assert!(matches!(&got, Err(Error::Codec(_))), "{}: got {got:?}", c.name),
            }
        }
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
                events.append(tx, "a", 0, &[env("a", 1)])?;
                // Read-your-writes inside the unit of work: the append is visible before commit.
                assert_eq!(events.load(tx, "a", 0)?.len(), 1, "own write visible in the tx");
                let mut e = Entity { id: "a".into(), version: 0, document: json!({"id": "a", "n": 1}) };
                proj.save(tx, &mut e)?;
                assert_eq!(proj.get(tx, "a")?.map(|e| e.version), Some(1), "own save visible in the tx");
                // ...and to nobody else before the commit.
                assert!(events.load(&mut MemoryTx::auto_commit(), "a", 0)?.is_empty(), "invisible outside");
                assert!(proj.get(&mut MemoryTx::auto_commit(), "a")?.is_none(), "invisible outside");
                Ok(())
            })
            .unwrap();

        // Both writes are visible after the commit.
        let mut tx = MemoryTx::auto_commit();
        assert_eq!(events.load(&mut tx, "a", 0).unwrap().len(), 1);
        assert_eq!(proj.get(&mut tx, "a").unwrap().unwrap().document["n"], json!(1));

        // A body that returns Err rolls back (no panic, error propagates), and its writes vanish.
        // Free-fn form is equivalent.
        let out = crate::transaction(&events, |tx| {
            events.append(tx, "a", 1, &[env("a", 2)])?;
            Err::<(), _>(Error::Store("boom".into()))
        });
        assert!(matches!(out, Err(Error::Store(_))));
        assert_eq!(events.load(&mut tx, "a", 0).unwrap().len(), 1, "the rolled-back append is gone");

        // A standalone store is its own database: the transactor's handle is unknown to it. That is
        // `BadHandle` — terminal, a wiring mistake — and the Python backend refuses a foreign handle
        // with the same verdict (`edax.backend.memory.MemoryTransactor.token`).
        let stranger = MemoryProjectionStore::new();
        let out = db.transaction(|tx| stranger.get(tx, "a"));
        assert!(matches!(&out, Err(Error::BadHandle(m)) if m.starts_with("unknown transaction")), "got {out:?}");
        assert!(out.unwrap_err().code().is_terminal(), "a wiring mistake must not be retried forever");
    }

    #[test]
    fn saga_store_codec_failures_are_named() {
        // A stored state that cannot be read back as `S` is a codec error, never a silently invented
        // instance; an `S` that cannot be encoded never reaches the adapter.
        let lossy = MemorySagaStore::<Lossy>::new();
        lossy.save(&mut MemoryTx::auto_commit(), "k", &Lossy, 0).unwrap();
        assert!(matches!(lossy.load(&mut MemoryTx::auto_commit(), "k"), Err(Error::Codec(_))));

        let unencodable = MemorySagaStore::<Unencodable>::new();
        assert!(matches!(unencodable.save(&mut MemoryTx::auto_commit(), "k", &Unencodable, 0), Err(Error::Codec(_))));
        assert!(unencodable.load(&mut MemoryTx::auto_commit(), "k").unwrap().is_none(), "nothing was written");
    }
}

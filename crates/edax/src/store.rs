//! The write-side storage seam: an [`EventStore`] you implement against your own database, plus
//! the orchestration over it — a [`Repository`] that rehydrates and persists aggregates, and a
//! [`Relay`] that drains an [`Outbox`] to a publisher.
//!
//! The event outbox is a supertrait of [`EventStore`] rather than a seam of its own: a transactional
//! outbox is only worth the name if the event rows and the to-publish rows commit together, and one
//! store is the only place that atomicity can be guaranteed. A saga store has an outbox of its own,
//! for the commands its steps decided, and the same [`Relay`] drains it.

use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::Mutex;

use edax_core::{lock, tenant_key, Tenancy};
use edax_event::{
    check_extensions, originate, EventDraft, EventMessage, Extensions, Failure, Lineage, Message, Metadata, Retries,
    Verdict,
};

use crate::command::{code_of, retry_with, HandlerError};
use crate::pacing::Pacing;
use crate::{Aggregate, AggregateCodec, Error, EventCodec};

/// A marshaled aggregate at a given version, so a load can skip replaying the whole stream.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub aggregate_id: String,
    pub type_name: String,
    pub version: u64,
    pub data: Vec<u8>,
}

/// Owns the transaction boundary: begin a unit of work, commit it, or roll it back. The stores that
/// take a `&mut Tx` ([`EventStore`], [`ProjectionStore`](crate::ProjectionStore),
/// [`IdempotencyStore`](crate::IdempotencyStore)) are pure participants, so any number of them commit
/// together. An [`EventStore`] *is* a [`Transactor`]; a backend can also implement this on its own
/// and hand the same `Tx` to every store. Prefer [`Transactor::transaction`] over calling
/// begin/commit/rollback by hand.
pub trait Transactor {
    /// Backend transaction handle (a connection / transaction). Use `()` for a store that needs none.
    type Tx;

    /// The database this transactor opens units of work on. Two transactors over one database must
    /// return the same value, so [`transaction`] can refuse a second unit of work on it while one is
    /// open on this thread. The default, the transactor's address, holds only for a transactor that is
    /// its database; one that shares a pool with others overrides it (the memory backend returns its
    /// database's).
    fn identity(&self) -> usize {
        std::ptr::from_ref(self).addr()
    }

    fn begin(&self) -> Result<Self::Tx, Error>;
    fn commit(&self, tx: Self::Tx) -> Result<(), Error>;
    fn rollback(&self, tx: Self::Tx);

    /// Scoped unit of work: run `body`, commit on `Ok`, roll back on `Err`. Thread the `&mut Tx` to
    /// every store that should commit together (events, projection, idempotency record). `body` fails
    /// with any error an [`Error`] converts into — a [`HandlerError`] included, so
    /// a command dispatched inside keeps its verdict. A second unit of work on the same database while
    /// this one is open on the thread is refused; see [`transaction`].
    fn transaction<R, E: From<Error>>(&self, body: impl FnOnce(&mut Self::Tx) -> Result<R, E>) -> Result<R, E>
    where
        Self: Sized,
    {
        transaction(self, body)
    }
}

/// Holds the open handle until the unit of work settles it. Dropping it unsettled — the body
/// returned `Err`, or it unwound — rolls back. Without this, a panic would leak the handle: a
/// backend that holds the transaction's keys until it ends would hold them forever, and every
/// later write to those keys would fail with [`ErrorCode::Concurrency`](crate::ErrorCode::Concurrency).
struct Rollback<'a, T: Transactor> {
    transactor: &'a T,
    tx: Option<T::Tx>,
}

impl<T: Transactor> Rollback<'_, T> {
    /// Settle by committing. Consumes the guard, so its `Drop` finds no handle to roll back.
    fn commit(mut self) -> Result<(), Error> {
        match self.tx.take() {
            Some(tx) => self.transactor.commit(tx),
            None => Ok(()),
        }
    }
}

impl<T: Transactor> Drop for Rollback<'_, T> {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            self.transactor.rollback(tx);
        }
    }
}

thread_local! {
    /// The databases a [`transaction`] body is running on, on this thread, innermost last. A handler is
    /// a sync closure that runs on its caller's thread, so a dispatch made inside a body runs inside that
    /// unit of work: [`retry`](crate::retry) reads this to refuse it, and [`transaction`] to refuse a
    /// second unit of work on a database that already has one open.
    static OPEN_UNITS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

/// Marks a [`transaction`] body as running on this thread until dropped, panics included.
struct OpenUnit;

impl OpenUnit {
    /// Mark `database` open, or refuse it when a body on this thread already holds it.
    fn enter(database: usize) -> Result<Self, Error> {
        OPEN_UNITS.with_borrow_mut(|open| {
            if open.contains(&database) {
                // The inner unit of work would commit on its own, and the outer rollback could not take
                // it back. Terminal: the same wiring fails the same way on every retry.
                return Err(Error::bad_handle(
                    "a unit of work was opened on a database that already has one open on this thread: the inner \
                     one would commit independently of the outer one. Pass the open `&mut Tx` down (`save_in`, \
                     `load_in`, `dispatch` inside the body) instead of opening another",
                ));
            }
            open.push(database);
            Ok(OpenUnit)
        })
    }
}

impl Drop for OpenUnit {
    fn drop(&mut self) {
        OPEN_UNITS.with_borrow_mut(|open| {
            open.pop();
        });
    }
}

/// Whether a [`transaction`] body is running on this thread.
pub(crate) fn in_unit_of_work() -> bool {
    OPEN_UNITS.with_borrow(|open| !open.is_empty())
}

/// The free-function form of [`Transactor::transaction`]. A panic in `body` rolls back and keeps
/// unwinding; the caller sees the panic, the backend sees a settled transaction.
///
/// Opening it on a database that already has a unit of work open on this thread is refused with the
/// terminal [`ErrorCode::BadHandle`](crate::ErrorCode::BadHandle), before anything begins: the inner
/// unit of work would commit on its own, and the outer rollback could not take it back. So
/// [`Repository::save`] (which opens its own) inside a host's body is refused rather than committing
/// apart from it; [`Repository::save_in`] joins the open one. The check keys on
/// [`Transactor::identity`], so it catches every transactor over one database only when the backend
/// reports one identity for them. Two databases nest.
pub fn transaction<T, R, E: From<Error>>(tr: &T, body: impl FnOnce(&mut T::Tx) -> Result<R, E>) -> Result<R, E>
where
    T: Transactor,
{
    let open = OpenUnit::enter(tr.identity())?;
    let mut guard = Rollback { transactor: tr, tx: Some(tr.begin()?) };
    // `None` is unreachable: only `commit` takes the handle, and it consumes the guard.
    let outcome = guard.tx.as_mut().map(body);
    drop(open);
    match outcome {
        Some(Ok(r)) => {
            guard.commit()?;
            Ok(r)
        }
        Some(Err(e)) => Err(e),
        None => Err(Error::bad_handle("unit of work has no open transaction").into()),
    }
}

/// Run `body` in a unit of work, and on a transient failure run it again in a fresh one: up to
/// `attempts` runs (at least one), waiting each step of `pacing`. A terminal error returns at once.
///
/// This is the safe retry over a transaction. Each attempt commits or rolls back whole, so a second
/// attempt never re-applies the first one's staged writes. Use it to retry a dispatch through
/// [`idempotent_in`](crate::idempotent_in): [`retry`](crate::retry) refuses to run inside a
/// [`transaction`] body, for exactly that reason. The sleep blocks the calling thread.
pub fn retry_transaction<T, R>(
    tr: &T,
    attempts: usize,
    pacing: &Pacing,
    mut body: impl FnMut(&mut T::Tx) -> Result<R, HandlerError>,
) -> Result<R, HandlerError>
where
    T: Transactor,
{
    retry_with(attempts, pacing, || transaction(tr, &mut body))
}

/// A row an [`Outbox`] holds: what [`Relay`] marks it published by.
pub trait OutboxRow {
    /// The row's unique id: the message id.
    fn id(&self) -> &str;
}

/// Every message kind is a row, by its own id.
impl<M: Message> OutboxRow for M {
    fn id(&self) -> &str {
        Message::id(self)
    }
}

/// The read side of a transactional outbox: committed rows not yet published, and the acknowledgement
/// that prunes them. Generic over the row, so one store can hold the event outbox (`R` =
/// [`EventMessage`]) and the saga command outbox (`R` = [`CommandMessage`](crate::CommandMessage)) side by side.
pub trait Outbox<R> {
    /// Committed rows not yet published, oldest commit first, at most `limit`. Running more than one
    /// [`Relay`] over one store needs this call to lease or lock the rows it returns, or two relays
    /// read the same batch and publish it twice.
    fn unpublished(&self, limit: usize) -> Result<Vec<R>, Error>;
    /// Acknowledge published rows by id. Idempotent; an unknown id is ignored.
    fn mark_published(&self, ids: &[String]) -> Result<(), Error>;
}

/// The durable log: per-aggregate events with optimistic concurrency, aggregate snapshots, and a
/// transactional outbox — all under one transaction so committed state and to-publish records land
/// together.
///
/// The boundary itself lives on [`Transactor`] (the supertrait): begin a unit of work, thread the
/// `&mut Tx` through these writes, then commit so they land atomically. The outbox's read side is the
/// [`Outbox`] supertrait. Event ids are unique store-wide: an append carrying an id the store already
/// holds is refused, or marking one row published would prune the other unpublished.
pub trait EventStore: Transactor + Outbox<EventMessage> {
    /// Events for the aggregate with `sequence > after`, in order (`after` skips a snapshot).
    fn load(&self, tx: &mut Self::Tx, aggregate_id: &str, after: u64) -> Result<Vec<EventMessage>, Error>;

    fn load_snapshot(&self, tx: &mut Self::Tx, aggregate_id: &str) -> Result<Option<Snapshot>, Error>;
    fn save_snapshot(&self, tx: &mut Self::Tx, snapshot: &Snapshot) -> Result<(), Error>;

    /// Append `drafts` and enqueue the same events to the outbox, atomically. Return the stored
    /// events: each draft recorded with the time the store gives it, from its own clock (a database's
    /// `now()`, say), in unix nanoseconds. Under an open `tx` they are not committed yet.
    ///
    /// Return a [`ErrorCode::Concurrency`](crate::ErrorCode::Concurrency) error if the stored version
    /// differs from `expected_version`, and a `codec` error for a batch that is not dense or carries an
    /// event id the store already holds.
    fn append(
        &self,
        tx: &mut Self::Tx,
        aggregate_id: &str,
        expected_version: u64,
        drafts: Vec<EventDraft>,
    ) -> Result<Vec<EventMessage>, Error>;
}

/// Loads aggregates (snapshot then replay the rest) and saves them (append the pending events and, at
/// the [`snapshot_every`](Self::snapshot_every) interval, a fresh snapshot in one transaction; the
/// store also enqueues the outbox).
pub struct Repository<S, EC, AC> {
    store: S,
    events: EC,
    aggregates: AC,
    new_id: Box<dyn Fn() -> String + Send + Sync>,
    snapshot_every: u64,
    tenancy: Tenancy,
}

impl<S, EC, AC> Repository<S, EC, AC>
where
    S: EventStore,
    AC: AggregateCodec,
    EC: EventCodec<Domain = <AC::Aggregate as Aggregate>::Event>,
{
    pub fn new(store: S, events: EC, aggregates: AC) -> Self {
        Self { store, events, aggregates, new_id: Box::new(edax_core::guid), snapshot_every: 1, tenancy: Tenancy::Silo }
    }

    /// How this repository separates tenants, fixed here rather than per call, so no two calls of one
    /// service can key differently. The default is [`Tenancy::Silo`]: no tenant, no composed key.
    ///
    /// Under [`Tenancy::Pool`] every stream and snapshot key is the aggregate id composed with its
    /// tenant ([`edax_core::tenant_key`]), and a load or save that names no tenant is a terminal
    /// `codec` error rather than a read of the whole pool. Under [`Tenancy::Bridge`] a tenant is
    /// required and keys stay bare, because this repository's store already holds one tenant.
    pub fn tenancy(mut self, mode: Tenancy) -> Self {
        self.tenancy = mode;
        self
    }

    /// How often a save writes a snapshot: each time the aggregate's version crosses a multiple of
    /// `every`, so `1` (the default) snapshots on every save and `0` never does. The kernel's rule
    /// ([`edax_event::snapshot_due`]). A snapshot doubles the writes of the save that takes it; a
    /// stream that is cheap to replay wants a larger interval.
    pub fn snapshot_every(mut self, every: u64) -> Self {
        self.snapshot_every = every;
        self
    }

    /// Override the id source, for deterministic tests. Each id names one event, or roots the flow of
    /// a plain [`save`](Self::save). A closure, so a caller can capture a seeded counter instead of
    /// reaching for a global. The time an event is recorded at is the store's: see
    /// [`EventStore::append`].
    pub fn with_ids(mut self, new_id: impl Fn() -> String + Send + Sync + 'static) -> Self {
        self.new_id = Box::new(new_id);
        self
    }

    /// Rehydrate the aggregate (snapshot, then replay the events after it) in a unit of work of its
    /// own. Inside a transaction the caller already opened, use [`Repository::load_in`].
    pub fn load(&self, aggregate_id: &str) -> Result<AC::Aggregate, Error> {
        self.load_for(None, aggregate_id)
    }

    /// [`Repository::load`] for one tenant, in a unit of work of its own. The loaded aggregate carries
    /// the tenant ([`Root::tenant`]), so the save that follows stamps and keys the same one.
    pub fn load_for(&self, tenant: Option<&str>, aggregate_id: &str) -> Result<AC::Aggregate, Error> {
        self.store.transaction(|tx| self.load_for_in(tx, tenant, aggregate_id))
    }

    /// [`Repository::load`] inside the caller's transaction: the read sees that transaction's own
    /// appends, so a save then a load in one unit of work agree (read-your-writes).
    pub fn load_in(&self, tx: &mut S::Tx, aggregate_id: &str) -> Result<AC::Aggregate, Error> {
        self.load_for_in(tx, None, aggregate_id)
    }

    /// [`Repository::load_for`] inside the caller's transaction.
    pub fn load_for_in(&self, tx: &mut S::Tx, tenant: Option<&str>, aggregate_id: &str) -> Result<AC::Aggregate, Error> {
        let key = tenant_key(self.tenancy, tenant, aggregate_id)?;
        let mut agg = self.replay(tx, &key, aggregate_id)?;
        agg.root_mut().tenant = tenant.map(ToOwned::to_owned);
        Ok(agg)
    }

    /// Persist the aggregate's pending events in a unit of work of its own; return the committed
    /// events (publish them eagerly, or leave them to the [`Relay`]). The batch starts a new flow: one
    /// fresh [`Lineage`] every event shares. To continue a flow, pass its lineage to
    /// [`Repository::save_with`]. On a clean commit the aggregate is reset to its new version; on error it
    /// is left untouched, so a retry (worth it for a `store` error, not for a terminal `codec` error)
    /// re-sends the same pending events. Inside a transaction the caller already opened, use
    /// [`Repository::save_in`].
    pub fn save(&self, agg: &mut AC::Aggregate) -> Result<Vec<EventMessage>, Error> {
        self.save_with(agg, self.root_lineage(), Extensions::new(), Metadata::new())
    }

    /// [`Repository::save`] inside the caller's transaction, so the events, the snapshot and the
    /// outbox rows commit or roll back with whatever else the caller writes under `tx` — a
    /// projection row, an idempotency claim. The aggregate is reset as soon as the store accepts the
    /// writes, before the caller commits: after a rollback it no longer holds the events, so reload
    /// it rather than save it again. The returned events are not committed until the caller commits.
    pub fn save_in(&self, tx: &mut S::Tx, agg: &mut AC::Aggregate) -> Result<Vec<EventMessage>, Error> {
        self.save_with_in(tx, agg, self.root_lineage(), Extensions::new(), Metadata::new())
    }

    /// Like [`Repository::save`], with the context every event of the batch carries. `lineage` places the
    /// events in their flow: [`propagate`](crate::propagate) from the message that caused them, or
    /// [`originate`] from the ids an inbound request holds. `extensions` carry edax extension context,
    /// such as a trace parent, under the kernel's key rule; `metadata` is the application's own. Keep
    /// per-delivery and per-request ids here, not in the command payload, so they never change an
    /// idempotency hash.
    ///
    /// # Errors
    /// A `codec` error for an extension key outside the rule, before anything is written: the relay
    /// could never encode the event.
    pub fn save_with(
        &self,
        agg: &mut AC::Aggregate,
        lineage: Lineage,
        extensions: Extensions,
        metadata: Metadata,
    ) -> Result<Vec<EventMessage>, Error> {
        // Stage inside the transaction, reset outside it. This method owns the commit, so it waits for the
        // commit to land before clearing the aggregate: a failed commit leaves the pending events in place and
        // the caller can save again. `save_with_in` cannot, because its caller commits after it returns.
        let (events, current) = self.store.transaction(|tx| self.stage(tx, agg, &lineage, &extensions, &metadata))?;
        Self::reset(agg, current);
        Ok(events)
    }

    /// [`Repository::save_with`] inside the caller's transaction; see [`Repository::save_in`].
    pub fn save_with_in(
        &self,
        tx: &mut S::Tx,
        agg: &mut AC::Aggregate,
        lineage: Lineage,
        extensions: Extensions,
        metadata: Metadata,
    ) -> Result<Vec<EventMessage>, Error> {
        let (events, current) = self.stage(tx, agg, &lineage, &extensions, &metadata)?;
        Self::reset(agg, current);
        Ok(events)
    }

    /// The lineage of a batch no message caused: a new flow, minted from this repository's id source.
    fn root_lineage(&self) -> Lineage {
        originate(None, None, &(self.new_id)())
    }

    /// Drop the events the store accepted and pin the aggregate to its new version.
    fn reset(agg: &mut AC::Aggregate, current: u64) {
        let root = agg.root_mut();
        root.take_pending();
        root.version = current;
    }

    /// Marshal the pending events, append them and write the snapshot under `tx`. Returns the stored
    /// events and the aggregate's new version; resetting the aggregate is the caller's, because only
    /// the caller knows whether the commit has landed yet.
    fn stage(
        &self,
        tx: &mut S::Tx,
        agg: &AC::Aggregate,
        lineage: &Lineage,
        extensions: &Extensions,
        metadata: &Metadata,
    ) -> Result<(Vec<EventMessage>, u64), Error> {
        check_extensions(extensions)?;
        let root = agg.root();
        let initial = root.version;
        let current = root.current();
        let aggregate_id = root.id.clone();
        // The tenant comes from the aggregate the caller loaded, never from this call, so a save cannot
        // name a tenant the load did not.
        let tenant = root.tenant.clone();
        let key = tenant_key(self.tenancy, tenant.as_deref(), &aggregate_id)?;

        // Marshal the whole batch before the first write: a payload that cannot be written fails the
        // save outright, so a half-marshaled batch can never reach the store.
        let drafts: Vec<EventDraft> = root
            .pending()
            .iter()
            .enumerate()
            .map(|(i, event)| {
                let payload = self.events.marshal(event)?;
                Ok(EventDraft {
                    id: (self.new_id)(),
                    aggregate_id: aggregate_id.clone(),
                    tenant: tenant.clone(),
                    sequence: Some(initial.saturating_add(1).saturating_add(u64::try_from(i).unwrap_or(u64::MAX))),
                    name: payload.name,
                    version: payload.version,
                    data: payload.data,
                    correlation_id: lineage.correlation_id.clone(),
                    causation_id: lineage.causation_id.clone(),
                    occurred_at: None,
                    extensions: extensions.clone(),
                    metadata: metadata.clone(),
                })
            })
            .collect::<Result<_, Error>>()?;

        // Dumped before the first write, like the events: a snapshot that cannot be written fails the
        // save outright.
        let snapshot = if edax_event::snapshot_due(self.snapshot_every, initial, current) {
            let data = self.aggregates.dump(agg)?;
            Some(Snapshot { aggregate_id: key.clone(), type_name: self.aggregates.type_name().to_string(), version: current, data })
        } else {
            None
        };

        let events = self.store.append(tx, &key, initial, drafts)?;
        if let Some(snapshot) = snapshot {
            self.store.save_snapshot(tx, &snapshot)?;
        }
        Ok((events, current))
    }

    /// Rebuild from the snapshot and the events after it. A snapshot is a cache of the event log, so
    /// one this codec cannot use — another aggregate type's under the same id, or one whose `dump`
    /// format no longer loads — is ignored and the stream replays from version 0. The loaded tail is
    /// held to the kernel's dense-run rule, so a store that returns a gap or a repeat fails the load
    /// instead of folding the wrong state.
    fn replay(&self, tx: &mut S::Tx, key: &str, id: &str) -> Result<AC::Aggregate, Error> {
        let cached = self
            .store
            .load_snapshot(tx, key)?
            .filter(|s| s.type_name == self.aggregates.type_name())
            .and_then(|s| self.aggregates.load(id, s.version, Some(&s.data)).ok().map(|agg| (s.version, agg)));
        let (base, mut agg) = match cached {
            Some(found) => found,
            None => (0, self.aggregates.load(id, 0, None)?),
        };
        let events = self.store.load(tx, key, base)?;
        let sequences: Vec<Option<u64>> = events.iter().map(|e| e.sequence).collect();
        edax_event::check_batch(base, &sequences)?;
        // Dense from `base`, so the last sequence is the count past it.
        let count = u64::try_from(events.len()).unwrap_or(u64::MAX);
        for env in events {
            // Replay drives state directly — not apply(), which would re-queue these as pending.
            agg.transition(&self.events.unmarshal(&env.into_payload())?);
        }
        agg.root_mut().version = base.saturating_add(count);
        Ok(agg)
    }
}

/// A dead-letter sink for a [`Relay`]: takes the row that cannot be published and the failure that
/// killed it, and returns once the row is somewhere a human will look.
pub type RelaySink<R> = Box<dyn Fn(&R, &HandlerError) -> Result<(), HandlerError> + Send + Sync>;

/// Drains an [`Outbox`] to a publisher: the transactional-outbox publisher, for the event outbox
/// (`R` = [`EventMessage`], the default) and the saga command outbox (`R` = [`CommandMessage`](crate::CommandMessage)) alike.
///
/// `publish` must return only once the transport has durably accepted the row. Anything that merely
/// hands it on, such as an in-process queue or a dispatcher's `submit`, makes the outbox
/// at-most-once: the row is marked published while it is still only in memory.
///
/// A drain publishes rows in order and marks every row it settled, including on a failure part way:
/// publishing is at-least-once, so downstream deduplicates by row id. A failing row stops the batch,
/// because the rows behind it may be its aggregate's later events. Whether it is handed back or dead-
/// lettered is the kernel's retry policy ([`Retries`]), keyed by row id: a terminal failure, or a
/// transient one at the [`max_attempts`](Self::max_attempts)-th attempt, goes to the
/// [`on_dead`](Self::on_dead) sink and is marked, so one poison row cannot stall the outbox; a row
/// handed back waits its backoff step before the drain returns the error, so a host looping on
/// `drain` does not spin. With no sink a poison row is handed back on every drain, and its wait grows.
/// A drain whose store call fails (`unpublished` or `mark_published`) waits too, one step per
/// consecutive store failure, so a store that is down is not hammered either; a drain whose store
/// calls succeed starts that count over.
///
/// Overlapping drains of one relay take turns, so a timer and a "drain now" never publish one batch
/// twice. Two relays over one store need [`Outbox::unpublished`] to lease the rows it returns.
pub struct Relay<S, R = EventMessage> {
    store: S,
    /// Taken for a whole drain, so drains take turns.
    draining: Mutex<Draining>,
    max_attempts: u32,
    on_dead: Option<RelaySink<R>>,
    pacing: Pacing,
    _row: PhantomData<fn() -> R>,
}

/// What a drain holds for its whole run: the consecutive store failures (the backoff step of the next
/// one) and the kernel's retry policy, keyed by row id.
struct Draining {
    store_failures: u64,
    retries: Retries,
}

impl Draining {
    fn new(max_attempts: u32, sink: bool) -> Mutex<Self> {
        Mutex::new(Self { store_failures: 0, retries: Retries::new(max_attempts, sink) })
    }
}

impl<S: Outbox<R>, R: OutboxRow> Relay<S, R> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            draining: Draining::new(0, false),
            max_attempts: 0,
            on_dead: None,
            pacing: Pacing::default(),
            _row: PhantomData,
        }
    }

    /// Dead-letter a row whose publish failed transiently `n` times; `0` (the default) never does.
    pub fn max_attempts(self, n: u32) -> Self {
        let draining = Draining::new(n, self.on_dead.is_some());
        Self { max_attempts: n, draining, ..self }
    }

    /// Where a row that cannot be published goes, instead of stalling the rows behind it. A sink
    /// that fails hands the row back, and the next drain tries the sink again.
    pub fn on_dead(mut self, sink: impl Fn(&R, &HandlerError) -> Result<(), HandlerError> + Send + Sync + 'static) -> Self {
        self.on_dead = Some(Box::new(sink));
        self.draining = Draining::new(self.max_attempts, true);
        self
    }

    /// How a failed drain waits before it returns: the backoff policy, the sleep and the jitter.
    pub fn pacing(mut self, pacing: Pacing) -> Self {
        self.pacing = pacing;
        self
    }

    /// The outbox this relay drains.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Publish one batch; return how many rows it settled (published or dead-lettered). Call in a
    /// loop to keep draining. `publish` is synchronous; wrap the call in your async runtime for a
    /// network transport. Its error is a [`HandlerError`], so a saga command dispatched through a
    /// [`CommandBus`](crate::CommandBus) keeps its verdict.
    pub fn drain<F>(&self, batch: usize, publish: F) -> Result<usize, HandlerError>
    where
        F: FnMut(&R) -> Result<(), HandlerError>,
    {
        let mut draining = lock(&self.draining);
        let Draining { store_failures, retries } = &mut *draining;
        let (settled, failed) = match self.publish_batch(retries, batch, publish) {
            Ok(drained) => {
                *store_failures = 0;
                drained
            }
            Err(error) => {
                // The store failed, not a row: wait a step that grows while it stays down, so a host
                // looping on `drain` neither spins nor hammers it.
                self.pacing.wait(*store_failures);
                *store_failures = store_failures.saturating_add(1);
                return Err(error.into());
            }
        };
        match failed {
            Some((step, error)) => {
                self.pacing.wait(step);
                Err(error)
            }
            None => Ok(settled),
        }
    }

    /// Read a batch, publish it in order until a row fails, and mark every row settled. Returns how
    /// many rows settled and the failed row's backoff step and error; `Err` only for a store failure.
    fn publish_batch<F>(
        &self,
        retries: &mut Retries,
        batch: usize,
        mut publish: F,
    ) -> Result<(usize, Option<(u64, HandlerError)>), Error>
    where
        F: FnMut(&R) -> Result<(), HandlerError>,
    {
        let rows = self.store.unpublished(batch)?;
        let mut settled: Vec<String> = Vec::new();
        let mut failed: Option<(u64, HandlerError)> = None;
        for row in &rows {
            let Err(error) = publish(row) else {
                retries.succeeded(row.id());
                settled.push(row.id().to_owned());
                continue;
            };
            let verdict = retries.failed(row.id(), 0, Failure::of(code_of(&*error)));
            let step = match verdict {
                Verdict::Nack { step } => step,
                Verdict::Dead { attempt } => {
                    // The kernel answers `Dead` only for a relay built with a sink, so `None` here is
                    // unreachable and a sink that refused the row hands it back like any failure.
                    if self.on_dead.as_ref().is_some_and(|sink| sink(row, &error).is_ok()) {
                        retries.dead_lettered(row.id());
                        settled.push(row.id().to_owned());
                        continue;
                    }
                    retries.sink_failed(row.id(), attempt)
                }
            };
            failed = Some((step, error));
            break;
        }
        if !settled.is_empty() {
            self.store.mark_published(&settled)?;
        }
        Ok((settled.len(), failed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{JsonCodec, MemoryEventStore, MemoryTx, Root};
    use crate::ErrorCode;
    use crate::testkit::{draft, RECORDED_AT};
    use edax_event::Event;
    use serde::{Deserialize, Serialize};
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    // --- a tiny domain ---

    #[derive(Clone)]
    enum AccountEvent {
        Deposited(i64),
    }

    struct Account {
        root: Root<AccountEvent>,
        balance: i64,
    }

    impl Account {
        fn open(id: &str) -> Self {
            Self { root: Root::new(id, 0), balance: 0 }
        }
    }

    impl Aggregate for Account {
        type Event = AccountEvent;
        fn root(&self) -> &Root<Self::Event> {
            &self.root
        }
        fn root_mut(&mut self) -> &mut Root<Self::Event> {
            &mut self.root
        }
        fn transition(&mut self, event: &Self::Event) {
            match event {
                AccountEvent::Deposited(n) => self.balance += n,
            }
        }
    }

    struct AccountEvents;
    impl EventCodec for AccountEvents {
        type Domain = AccountEvent;
        fn marshal(&self, e: &AccountEvent) -> Result<Event, Error> {
            let AccountEvent::Deposited(n) = e;
            Ok(Event { name: "Deposited".into(), version: 1, data: n.to_string().into_bytes() })
        }
        fn unmarshal(&self, raw: &Event) -> Result<AccountEvent, Error> {
            let s = std::str::from_utf8(&raw.data).map_err(Error::codec)?;
            let n = s.parse::<i64>().map_err(Error::codec)?;
            Ok(AccountEvent::Deposited(n))
        }
    }

    struct AccountCodec;
    impl AggregateCodec for AccountCodec {
        type Aggregate = Account;
        fn type_name(&self) -> &str {
            "Account"
        }
        fn load(&self, id: &str, version: u64, data: Option<&[u8]>) -> Result<Account, Error> {
            let mut a = Account { root: Root::new(id, version), balance: 0 };
            if let Some(d) = data {
                let s = std::str::from_utf8(d).map_err(Error::codec)?;
                a.balance = s.parse().map_err(Error::codec)?;
            }
            Ok(a)
        }
        fn dump(&self, agg: &Account) -> Result<Vec<u8>, Error> {
            Ok(agg.balance.to_string().into_bytes())
        }
    }

    fn next_id() -> String {
        static N: AtomicU64 = AtomicU64::new(1);
        format!("e{}", N.fetch_add(1, Ordering::Relaxed))
    }

    fn repo(store: MemoryEventStore) -> Repository<MemoryEventStore, AccountEvents, AccountCodec> {
        Repository::new(store, AccountEvents, AccountCodec).with_ids(next_id)
    }

    /// A `Deposited` draft as another writer would append it behind the repository's back.
    fn deposited(aggregate_id: &str, sequence: u64, data: &[u8]) -> EventDraft {
        draft(aggregate_id, "Deposited", sequence, data)
    }

    #[test]
    fn save_assigns_sequences_and_resets_aggregate() {
        let r = repo(MemoryEventStore::new());
        let mut a = Account::open("acct-1");
        a.apply(AccountEvent::Deposited(10));
        a.apply(AccountEvent::Deposited(5));
        let envs = r.save(&mut a).unwrap();
        assert_eq!(envs.iter().map(|e| e.sequence).collect::<Vec<_>>(), vec![Some(1), Some(2)]);
        assert!(a.root().pending().is_empty());
        assert_eq!((a.root().version, a.root().pending().len()), (2, 0));
    }

    #[test]
    fn save_stamps_one_lineage_and_context_on_every_event() {
        // A plain save roots a new flow: every event of the batch shares one lineage, and the store records
        // them at its own clock. A refused context writes nothing. `save_with` carrying the lineage and
        // context it is handed is the model test's (`tests/model.rs`, `repository_matches_fold`).
        struct Case {
            name: &'static str,
            /// `None` saves plainly; `Some` passes this lineage and context to `save_with`.
            context: Option<(Lineage, Extensions, Metadata)>,
            want_err: Option<ErrorCode>,
        }
        let cases = [
            Case { name: "a plain save roots a new flow", context: None, want_err: None },
            Case {
                name: "an extension key outside the rule is refused before any write",
                context: Some((
                    crate::propagate("cmd-9", "corr-9"),
                    Extensions::from([("Traceparent".to_owned(), "x".to_owned())]),
                    Metadata::from([("tenant".to_owned(), "acme".to_owned())]),
                )),
                want_err: Some(ErrorCode::Codec),
            },
        ];
        for c in cases {
            let store = MemoryEventStore::new().with_clock(|| RECORDED_AT);
            let r = repo(store.clone());
            let mut a = Account::open("acct-l");
            a.apply(AccountEvent::Deposited(10));
            a.apply(AccountEvent::Deposited(5));
            let got = match c.context {
                None => r.save(&mut a),
                Some((lineage, extensions, metadata)) => r.save_with(&mut a, lineage, extensions, metadata),
            };
            if let Some(want) = c.want_err {
                assert_eq!(got.err().map(|e| e.code), Some(want), "{}", c.name);
                assert!(store.load(&mut MemoryTx::auto_commit(), "acct-l", 0).unwrap().is_empty(), "{}: nothing appended", c.name);
                assert_eq!(a.root().pending().len(), 2, "{}: the aggregate keeps its events", c.name);
                continue;
            }
            let events = got.unwrap_or_else(|e| panic!("{}: {e}", c.name));
            assert_eq!(events, store.load(&mut MemoryTx::auto_commit(), "acct-l", 0).unwrap(), "{}: returned what was stored", c.name);
            let first = &events[0];
            assert_eq!(first.causation_id, first.correlation_id, "{}: a root causes itself", c.name);
            assert!(events.iter().all(|e| e.id != first.correlation_id), "{}: the fresh id is no event's id", c.name);
            for e in &events {
                assert_eq!((&e.correlation_id, &e.causation_id), (&first.correlation_id, &first.causation_id), "{}: one lineage", c.name);
                assert_eq!((e.recorded_at, e.occurred_at), (RECORDED_AT, None), "{}: the store's time", c.name);
                assert!(e.extensions.is_empty() && e.metadata.is_empty(), "{}: no context", c.name);
            }
        }
    }

    #[test]
    fn reload_via_snapshot() {
        let r = repo(MemoryEventStore::new());
        let mut a = Account::open("acct-2");
        a.apply(AccountEvent::Deposited(10));
        a.apply(AccountEvent::Deposited(5));
        r.save(&mut a).unwrap(); // snapshots at version 2
        let b = r.load("acct-2").unwrap();
        assert_eq!(b.balance, 15);
        assert_eq!(b.root().version, 2);
    }

    #[test]
    fn snapshot_every_writes_one_where_a_save_crosses_its_interval() {
        // Two saves of one event each: the snapshot left behind, per interval. `1` takes both saves,
        // `2` the second, `3` neither, and `0` never snapshots.
        for (every, want) in [(1, Some(2)), (2, Some(2)), (3, None), (0, None)] {
            let store = MemoryEventStore::new();
            let r = repo(store.clone()).snapshot_every(every);
            let mut a = Account::open("acct-9");
            for amount in [10, 5] {
                a.apply(AccountEvent::Deposited(amount));
                r.save(&mut a).unwrap();
            }
            let snapshot = store.load_snapshot(&mut MemoryTx::auto_commit(), "acct-9").unwrap();
            assert_eq!(snapshot.map(|s| s.version), want, "every {every}");
            assert_eq!(r.load("acct-9").unwrap().balance, 15, "every {every}: the load replays what the snapshot lacks");
        }
    }

    #[test]
    fn load_pure_replay_no_snapshot() {
        let store = MemoryEventStore::new();
        // Seed events through the public append (no snapshot) so load() must pure-replay.
        store.append(&mut MemoryTx::auto_commit(), "acct-3", 0, vec![deposited("acct-3", 1, b"7"), deposited("acct-3", 2, b"3")]).unwrap();
        let a = repo(store).load("acct-3").unwrap();
        assert_eq!(a.balance, 10);
        assert_eq!(a.root().version, 2);
    }

    #[test]
    fn empty_save_appends_nothing_and_still_checks_the_version() {
        let store = MemoryEventStore::new();
        let r = repo(store.clone());
        let mut a = Account::open("acct-6");
        a.apply(AccountEvent::Deposited(4));
        r.save(&mut a).unwrap();

        let envs = r.save(&mut a).unwrap(); // zero-iteration marshal loop
        assert!(envs.is_empty());
        assert_eq!((a.root().version, a.root().pending().len()), (1, 0));
        assert_eq!(store.load(&mut MemoryTx::auto_commit(), "acct-6", 0).unwrap().len(), 1, "nothing appended");
        assert_eq!(store.unpublished(10).unwrap().len(), 1, "nothing enqueued");

        // The empty append is still a compare-and-set, so a stale caller that applied nothing is told
        // so — and its snapshot write is short-circuited rather than rolling the stored one backwards.
        let mut stale = r.load("acct-6").unwrap(); // at version 1
        let mut winner = r.load("acct-6").unwrap();
        winner.apply(AccountEvent::Deposited(6));
        r.save(&mut winner).unwrap(); // store -> version 2, balance 10
        let got = r.save(&mut stale);
        assert_eq!(got.err(), Some(Error::concurrency(1, 2)));
        let snap = store.load_snapshot(&mut MemoryTx::auto_commit(), "acct-6").unwrap().expect("the winner's snapshot");
        assert_eq!((snap.version, snap.data), (2, b"10".to_vec()));
    }

    #[test]
    fn load_replays_events_after_the_snapshot() {
        // Snapshot at v1, then two events land behind it (another writer): load must start from the
        // snapshot and fold only the tail, ending at the tail's last sequence.
        let store = MemoryEventStore::new();
        let r = repo(store.clone());
        let mut a = Account::open("acct-7");
        a.apply(AccountEvent::Deposited(10));
        r.save(&mut a).unwrap(); // snapshot at v1, balance 10

        store.append(&mut MemoryTx::auto_commit(), "acct-7", 1, vec![deposited("acct-7", 2, b"5"), deposited("acct-7", 3, b"7")]).unwrap();

        let b = r.load("acct-7").unwrap();
        assert_eq!(b.balance, 22);
        assert_eq!((b.root().version, b.root().pending().len()), (3, 0));
    }

    #[test]
    fn load_surfaces_a_codec_error_from_replay() {
        let store = MemoryEventStore::new();
        store.append(&mut MemoryTx::auto_commit(), "acct-8", 0, vec![deposited("acct-8", 1, b"not a number")]).unwrap();
        let got = repo(store).load("acct-8");
        assert!(matches!(got, Err(Error { code: ErrorCode::Codec, .. })), "got {:?}", got.err());
    }

    #[test]
    fn transaction_branches() {
        // The boundary's whole contract: commit on a clean exit, roll back on a raise, exactly one of
        // the two and only after `begin` succeeded. A spy pins *which* one ran and in what order;
        // what a commit or rollback does to the data is the memory backend's own test.
        /// `begin` / `commit` fail with the message given, if any; every call is logged in order.
        struct Spy {
            begin: Option<&'static str>,
            commit: Option<&'static str>,
            log: RefCell<Vec<&'static str>>,
        }
        impl Spy {
            fn record(&self, step: &'static str, failure: Option<&'static str>) -> Result<(), Error> {
                self.log.borrow_mut().push(step);
                failure.map_or(Ok(()), |m| Err(Error::store(m)))
            }
        }
        impl Transactor for Spy {
            type Tx = ();
            fn begin(&self) -> Result<(), Error> {
                self.record("begin", self.begin)
            }
            fn commit(&self, _tx: ()) -> Result<(), Error> {
                self.record("commit", self.commit)
            }
            fn rollback(&self, _tx: ()) {
                let _ = self.record("rollback", None);
            }
        }

        struct Case {
            name: &'static str,
            begin: Option<&'static str>,
            commit: Option<&'static str>,
            body: fn() -> Result<u8, Error>,
            want_log: &'static [&'static str],
            /// The value a clean unit of work yields, or the message the error must carry.
            want: Result<u8, &'static str>,
        }
        let cases = [
            Case {
                name: "clean body commits",
                begin: None,
                commit: None,
                body: || Ok(7),
                want_log: &["begin", "commit"],
                want: Ok(7),
            },
            Case {
                name: "failing body rolls back and never commits",
                begin: None,
                commit: None,
                body: || Err(Error::store("body gave up")),
                want_log: &["begin", "rollback"],
                want: Err("store: body gave up"),
            },
            Case {
                name: "a refused commit surfaces and does not also roll back",
                begin: None,
                commit: Some("commit refused"),
                body: || Ok(7),
                want_log: &["begin", "commit"],
                want: Err("store: commit refused"),
            },
            Case {
                name: "a refused begin runs neither the body nor a rollback",
                begin: Some("begin refused"),
                commit: None,
                body: || panic!("the body must not run without a transaction"),
                want_log: &["begin"],
                want: Err("store: begin refused"),
            },
        ];
        for c in cases {
            let spy = Spy { begin: c.begin, commit: c.commit, log: RefCell::new(Vec::new()) };
            let got = transaction(&spy, |_tx| (c.body)());
            assert_eq!(spy.log.borrow().as_slice(), c.want_log, "{}", c.name);
            match c.want {
                Ok(value) => assert_eq!(got.ok(), Some(value), "{}", c.name),
                Err(message) => assert_eq!(got.err().map(|e| e.to_string()).as_deref(), Some(message), "{}", c.name),
            }
        }
    }

    #[test]
    fn stale_expected_version_conflicts() {
        let store = MemoryEventStore::new();
        let r = repo(store.clone());
        let mut a = Account::open("acct-4");
        a.apply(AccountEvent::Deposited(1));
        r.save(&mut a).unwrap();

        let mut stale = r.load("acct-4").unwrap(); // initial 1
        let mut other = r.load("acct-4").unwrap();
        other.apply(AccountEvent::Deposited(2));
        r.save(&mut other).unwrap(); // store now at version 2

        stale.apply(AccountEvent::Deposited(3));
        match r.save(&mut stale) {
            Err(e) if e == Error::concurrency(1, 2) => {}
            other => panic!("expected concurrency error, got {:?}", other.err()),
        }

        // The rejected append short-circuits the snapshot write: the stored snapshot is still the
        // winner's (version 2, balance 3), never the loser's state — a snapshot may lag, never lead.
        let snap = store.load_snapshot(&mut MemoryTx::auto_commit(), "acct-4").unwrap().expect("winner's snapshot");
        assert_eq!((snap.version, snap.data), (2, b"3".to_vec()));
        assert_eq!(store.load(&mut MemoryTx::auto_commit(), "acct-4", 0).unwrap().len(), 2, "nothing appended by the loser");
    }

    // --- a domain whose codecs genuinely fail ---

    /// A posting keyed by `(account, leg)`. `serde_json` cannot write a non-string map key, so an
    /// event carrying entries really fails to marshal and a memo posting (`entries` empty) really
    /// succeeds — no test double pretends to fail.
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
    struct Posted {
        reference: String,
        entries: BTreeMap<(String, u8), i64>,
    }

    struct Ledger {
        root: Root<Posted>,
        balances: BTreeMap<(String, u8), i64>,
    }

    impl Aggregate for Ledger {
        type Event = Posted;
        fn root(&self) -> &Root<Self::Event> {
            &self.root
        }
        fn root_mut(&mut self) -> &mut Root<Self::Event> {
            &mut self.root
        }
        fn transition(&mut self, event: &Self::Event) {
            for (account, minor_units) in &event.entries {
                *self.balances.entry(account.clone()).or_default() += minor_units;
            }
        }
    }

    /// Snapshots the balances as JSON, so the snapshot seam fails on exactly the keys the event
    /// seam fails on.
    struct LedgerCodec;
    impl AggregateCodec for LedgerCodec {
        type Aggregate = Ledger;
        fn type_name(&self) -> &str {
            "Ledger"
        }
        fn load(&self, id: &str, version: u64, data: Option<&[u8]>) -> Result<Ledger, Error> {
            let balances = match data {
                Some(d) => serde_json::from_slice(d).map_err(Error::codec)?,
                None => BTreeMap::new(),
            };
            Ok(Ledger { root: Root::new(id, version), balances })
        }
        fn dump(&self, agg: &Ledger) -> Result<Vec<u8>, Error> {
            serde_json::to_vec(&agg.balances).map_err(Error::codec)
        }
    }

    #[test]
    fn save_reports_codec_failures_and_appends_nothing() {
        /// One posting line: account, leg, minor units.
        type Entry = (&'static str, u8, i64);

        fn posted(reference: &str, entries: &[Entry]) -> Posted {
            let entries = entries.iter().map(|(a, leg, n)| ((a.to_string(), *leg), *n)).collect();
            Posted { reference: reference.to_string(), entries }
        }

        struct Case {
            name: &'static str,
            /// Opening balances, which drive the snapshot codec.
            opening: &'static [Entry],
            /// Entries per applied event, which drive the event codec.
            events: &'static [&'static [Entry]],
            /// The sequences committed on success, or the message the error must carry.
            want: Result<&'static [u64], &'static str>,
        }
        let cases = [
            Case {
                name: "every seam writes: the batch commits",
                opening: &[],
                events: &[&[], &[]],
                want: Ok(&[1, 2]),
            },
            Case {
                name: "the only event fails to marshal",
                opening: &[],
                events: &[&[("cash", 0, 1_250)]],
                want: Err("codec: key must be a string"),
            },
            Case {
                name: "the third of three fails: no partial batch reaches the store",
                opening: &[],
                events: &[&[], &[], &[("cash", 0, 1_250)]],
                want: Err("codec: key must be a string"),
            },
            Case {
                name: "every event marshals but the snapshot does not",
                opening: &[("cash", 0, 1_250)],
                events: &[&[]],
                want: Err("codec: key must be a string"),
            },
        ];

        for (i, c) in cases.into_iter().enumerate() {
            let id = format!("ledger-{i}");
            let store = MemoryEventStore::new();
            let repo = Repository::new(store.clone(), JsonCodec::<Posted>::new("Posted", 1), LedgerCodec)
                .with_ids(next_id);
            let mut ledger = Ledger {
                root: Root::new(&id, 0),
                balances: posted("opening", c.opening).entries,
            };
            for (n, entries) in c.events.iter().enumerate() {
                ledger.apply(posted(&format!("inv-{n}"), entries));
            }

            let got = repo.save(&mut ledger);
            let appended = store.load(&mut MemoryTx::auto_commit(), &id, 0).unwrap();
            let snapshot = store.load_snapshot(&mut MemoryTx::auto_commit(), &id).unwrap();
            match c.want {
                Ok(sequences) => {
                    let events = got.unwrap_or_else(|e| panic!("{}: want Ok, got {e:?}", c.name));
                    let committed: Vec<u64> = events.iter().filter_map(|e| e.sequence).collect();
                    assert_eq!(committed, sequences, "{}", c.name);
                    assert_eq!(appended.len(), sequences.len(), "{}", c.name);
                    assert_eq!(store.unpublished(10).unwrap().len(), sequences.len(), "{}", c.name);
                    assert_eq!(snapshot.map(|s| s.data), Some(b"{}".to_vec()), "{}", c.name);
                    assert!(ledger.root().pending().is_empty(), "{}", c.name);
                    assert_eq!(ledger.root().version, sequences.len() as u64, "{}", c.name);
                }
                Err(message) => {
                    assert!(matches!(&got, Err(Error { code: ErrorCode::Codec, .. })), "{}: got {:?}", c.name, got.as_ref().err());
                    assert_eq!(got.err().map(|e| e.to_string()).as_deref(), Some(message), "{}", c.name);
                    assert!(appended.is_empty(), "{}: appended {appended:?}", c.name);
                    assert!(store.unpublished(10).unwrap().is_empty(), "{}: outbox not empty", c.name);
                    assert!(snapshot.is_none(), "{}: a snapshot was written", c.name);
                    // The aggregate is untouched, so the caller still owns the events it applied.
                    assert_eq!(ledger.root().pending().len(), c.events.len(), "{}", c.name);
                }
            }
        }
    }

    #[test]
    fn a_panic_inside_transaction_rolls_back_and_frees_the_stream() {
        // Without the drop guard the token stays in the adapter's open map, which holds the
        // stream's keys forever: every later append on it would fail with `Concurrency`.
        let store = MemoryEventStore::new();
        let deposit = |seq: u64| EventDraft { id: format!("panic-{seq}"), ..deposited("acct-panic", seq, b"1") };

        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<(), Error> = store.transaction(|tx| {
                store.append(tx, "acct-panic", 0, vec![deposit(1)])?;
                panic!("the handler blew up mid-transaction");
            });
        }));
        assert!(unwound.is_err(), "the panic keeps unwinding past transaction");

        // Rolled back, so the stream is still at version 0 and the same append lands.
        let second = store.append(&mut MemoryTx::auto_commit(), "acct-panic", 0, vec![deposit(1)]);
        assert!(second.is_ok(), "append after the panic: {:?}", second.err());
        let stored = store.load(&mut MemoryTx::auto_commit(), "acct-panic", 0).unwrap();
        assert_eq!(stored.len(), 1, "only the second append is stored");
    }

    #[test]
    fn outbox_relay_drains_then_idempotent() {
        let store = MemoryEventStore::new();
        let r = repo(store.clone());
        let mut a = Account::open("acct-5");
        a.apply(AccountEvent::Deposited(1));
        a.apply(AccountEvent::Deposited(2));
        r.save(&mut a).unwrap();

        let relay = Relay::new(store);
        let mut published: Vec<String> = Vec::new();
        let n = relay
            .drain(100, |env| {
                published.push(env.id.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(published.len(), 2);

        let n2 = relay.drain(100, |_env| Ok(())).unwrap();
        assert_eq!(n2, 0); // nothing left to publish
    }

    // --- the relay ---

    fn step_wait(step: u64) -> std::time::Duration {
        Pacing::recording().0.delay(step)
    }

    /// The event outbox with two fault hooks: every `unpublished` read is reported on `reads`, and
    /// `mark_published` fails while `fail_mark` is set (the relay crashed after the send).
    #[derive(Clone)]
    struct Hooked {
        inner: MemoryEventStore,
        reads: Option<Arc<Mutex<std::sync::mpsc::Sender<()>>>>,
        fail_mark: Arc<AtomicBool>,
    }

    impl Hooked {
        fn new(inner: MemoryEventStore) -> Self {
            Self { inner, reads: None, fail_mark: Arc::default() }
        }
    }

    impl Outbox<EventMessage> for Hooked {
        fn unpublished(&self, limit: usize) -> Result<Vec<EventMessage>, Error> {
            if let Some(reads) = &self.reads {
                let _ = reads.lock().expect("no panics").send(());
            }
            self.inner.unpublished(limit)
        }
        fn mark_published(&self, ids: &[String]) -> Result<(), Error> {
            if self.fail_mark.load(Ordering::SeqCst) {
                return Err(Error::store("connection lost"));
            }
            self.inner.mark_published(ids)
        }
    }

    /// A store holding `n` committed events of one account, all waiting in the outbox.
    fn outbox_of(n: u64) -> MemoryEventStore {
        let store = MemoryEventStore::new();
        let drafts: Vec<EventDraft> = (1..=n).map(|seq| deposited("acct-r", seq, b"1")).collect();
        store.append(&mut MemoryTx::auto_commit(), "acct-r", 0, drafts).unwrap();
        store
    }

    #[derive(Clone, Copy, Debug)]
    enum Publish {
        Ok,
        Transient,
        Terminal,
        Contended,
    }

    #[test]
    fn relay_drain_branches() {
        #[derive(Clone, Copy)]
        enum Sink {
            None,
            Takes,
            Fails,
        }
        struct Case {
            name: &'static str,
            /// How publishing each of the four rows goes, on every drain.
            rows: [Publish; 4],
            sink: Sink,
            max_attempts: u32,
            /// How many drains to run.
            drains: usize,
            want_results: &'static [Result<usize, &'static str>],
            want_left: &'static [&'static str],
            want_dead: &'static [&'static str],
            want_waits: &'static [u64],
        }
        use Publish::{Contended, Ok as Pub, Terminal, Transient};
        let base = Case {
            name: "",
            rows: [Pub, Pub, Pub, Pub],
            sink: Sink::None,
            max_attempts: 0,
            drains: 1,
            want_results: &[Ok(4)],
            want_left: &[],
            want_dead: &[],
            want_waits: &[],
        };
        let cases = [
            Case { name: "every row published and marked", ..base },
            Case {
                name: "a transient failure marks the published prefix and waits before it returns",
                rows: [Pub, Transient, Pub, Pub],
                want_results: &[Err("store: broker unavailable")],
                want_left: &["acct-r-2", "acct-r-3", "acct-r-4"],
                want_waits: &[0],
                ..base
            },
            Case {
                name: "the same row failing again waits a longer step",
                rows: [Transient, Pub, Pub, Pub],
                drains: 3,
                want_results: &[Err("store: broker unavailable"), Err("store: broker unavailable"), Err("store: broker unavailable")],
                want_left: &["acct-r-1", "acct-r-2", "acct-r-3", "acct-r-4"],
                want_waits: &[0, 1, 2],
                ..base
            },
            Case {
                name: "no sink: a poison row stays and its wait grows",
                rows: [Pub, Terminal, Pub, Pub],
                drains: 2,
                want_results: &[Err("codec: message too large"), Err("codec: message too large")],
                want_left: &["acct-r-2", "acct-r-3", "acct-r-4"],
                want_waits: &[0, 1],
                ..base
            },
            Case {
                name: "a sink takes a poison row and the rows behind it go out",
                rows: [Pub, Terminal, Pub, Pub],
                sink: Sink::Takes,
                want_results: &[Ok(4)],
                want_dead: &["acct-r-2"],
                ..base
            },
            Case {
                name: "a sink that fails hands the row back and gets it again next drain",
                rows: [Terminal, Pub, Pub, Pub],
                sink: Sink::Fails,
                drains: 2,
                want_results: &[Err("codec: message too large"), Err("codec: message too large")],
                want_left: &["acct-r-1", "acct-r-2", "acct-r-3", "acct-r-4"],
                want_dead: &["acct-r-1", "acct-r-1"],
                want_waits: &[0, 1],
                ..base
            },
            Case {
                name: "max_attempts dead-letters a row that keeps failing transiently",
                rows: [Transient, Pub, Pub, Pub],
                sink: Sink::Takes,
                max_attempts: 2,
                drains: 2,
                want_results: &[Err("store: broker unavailable"), Ok(4)],
                want_dead: &["acct-r-1"],
                want_waits: &[0],
                ..base
            },
            Case {
                name: "contention is handed back without spending an attempt",
                rows: [Contended, Pub, Pub, Pub],
                sink: Sink::Takes,
                max_attempts: 1,
                drains: 2,
                want_results: &[Err("in_progress: fulfill-7"), Err("in_progress: fulfill-7")],
                want_left: &["acct-r-1", "acct-r-2", "acct-r-3", "acct-r-4"],
                want_waits: &[0, 1],
                ..base
            },
        ];
        for c in cases {
            let store = outbox_of(4);
            let (pacing, waits) = Pacing::recording();
            let dead: Arc<Mutex<Vec<String>>> = Arc::default();
            let sink = Arc::clone(&dead);
            let mode = c.sink;
            let mut relay = Relay::new(store.clone()).pacing(pacing).max_attempts(c.max_attempts);
            if !matches!(mode, Sink::None) {
                relay = relay.on_dead(move |row: &EventMessage, _| {
                    sink.lock().expect("no panics").push(row.id.clone());
                    match mode {
                        Sink::Fails => Err("dead-letter table unavailable".into()),
                        _ => Ok(()),
                    }
                });
            }
            let rows = c.rows;
            let results: Vec<Result<usize, String>> = (0..c.drains)
                .map(|_| {
                    relay
                        .drain(10, |env| {
                            let index = env.sequence.unwrap_or(1).saturating_sub(1) as usize;
                            match rows.get(index).copied().unwrap_or(Pub) {
                                Pub => Ok(()),
                                Transient => Err(Error::store("broker unavailable").into()),
                                Terminal => Err(Error::codec("message too large").into()),
                                Contended => Err(Error::new(ErrorCode::InProgress, "fulfill-7").into()),
                            }
                        })
                        .map_err(|e| e.to_string())
                })
                .collect();
            let want: Vec<Result<usize, String>> = c.want_results.iter().map(|r| r.map_err(str::to_owned)).collect();
            assert_eq!(results, want, "{}: results", c.name);
            let left: Vec<String> = store.unpublished(10).unwrap().into_iter().map(|e| e.id).collect();
            assert_eq!(left, c.want_left, "{}: left in the outbox", c.name);
            assert_eq!(*dead.lock().unwrap(), c.want_dead, "{}: dead-lettered", c.name);
            let want_waits: Vec<_> = c.want_waits.iter().map(|&step| step_wait(step)).collect();
            assert_eq!(*waits.lock().unwrap(), want_waits, "{}: waits", c.name);
        }
    }

    /// The event outbox with a script per store call: each `unpublished` and each `mark_published`
    /// takes the next answer off its queue, `false` failing it, and succeeds once its queue is empty.
    struct Scripted {
        inner: MemoryEventStore,
        reads: Mutex<std::collections::VecDeque<bool>>,
        marks: Mutex<std::collections::VecDeque<bool>>,
    }

    impl Outbox<EventMessage> for Scripted {
        fn unpublished(&self, limit: usize) -> Result<Vec<EventMessage>, Error> {
            if !self.reads.lock().expect("no panics").pop_front().unwrap_or(true) {
                return Err(Error::store("database down"));
            }
            self.inner.unpublished(limit)
        }
        fn mark_published(&self, ids: &[String]) -> Result<(), Error> {
            if !self.marks.lock().expect("no panics").pop_front().unwrap_or(true) {
                return Err(Error::store("connection lost"));
            }
            self.inner.mark_published(ids)
        }
    }

    #[test]
    fn a_store_that_fails_backs_the_drain_off_and_a_store_that_recovers_starts_over() {
        // The relay's own store is down: a host looping on `drain` must wait a step that grows while
        // it stays down, never spin on it, and start over once a drain's store calls succeed. A row's
        // publish failure is the row's own step, kept apart from the store's.
        struct Drain {
            name: &'static str,
            /// Whether the first row's publish fails, transiently.
            publish_fails: bool,
            want: Result<usize, &'static str>,
            /// The backoff step this drain waits, if it waits.
            want_wait: Option<u64>,
        }
        let drains = [
            Drain { name: "the read fails: step 0", publish_fails: false, want: Err("store: database down"), want_wait: Some(0) },
            Drain { name: "it fails again: step 1", publish_fails: false, want: Err("store: database down"), want_wait: Some(1) },
            Drain { name: "the mark fails: the store's step 2", publish_fails: false, want: Err("store: connection lost"), want_wait: Some(2) },
            Drain { name: "the store answers, a row fails: the row's step 0", publish_fails: true, want: Err("store: broker unavailable"), want_wait: Some(0) },
            Drain { name: "the read fails after a recovery: step 0 again", publish_fails: false, want: Err("store: database down"), want_wait: Some(0) },
            Drain { name: "everything lands: no wait", publish_fails: false, want: Ok(4), want_wait: None },
            Drain { name: "the next store failure starts at step 0", publish_fails: false, want: Err("store: database down"), want_wait: Some(0) },
        ];
        let store = Scripted {
            inner: outbox_of(4),
            reads: Mutex::new([false, false, true, true, false, true, false].into()),
            marks: Mutex::new([false].into()),
        };
        let (pacing, waits) = Pacing::recording();
        let relay = Relay::new(store).pacing(pacing);
        for drain in drains {
            let before = waits.lock().unwrap().len();
            let got = relay.drain(10, |env| match drain.publish_fails && env.sequence == Some(1) {
                true => Err(Error::store("broker unavailable").into()),
                false => Ok(()),
            });
            assert_eq!(got.map_err(|e| e.to_string()), drain.want.map_err(str::to_owned), "{}", drain.name);
            let waited: Vec<_> = waits.lock().unwrap()[before..].to_vec();
            assert_eq!(waited, drain.want_wait.map(step_wait).into_iter().collect::<Vec<_>>(), "{}: wait", drain.name);
        }
        assert!(relay.store().inner.unpublished(10).unwrap().is_empty(), "every row was marked in the end");
    }

    #[test]
    fn overlapping_drains_of_one_relay_publish_each_row_once() {
        // A timer and a "drain now" on one relay. The first drain is held inside its publish; the
        // second is started meanwhile. It must not read the rows the first has not marked yet, so it
        // waits its turn and then finds nothing.
        let (read_tx, reads) = std::sync::mpsc::channel::<()>();
        let mut hooked = Hooked::new(outbox_of(2));
        hooked.reads = Some(Arc::new(Mutex::new(read_tx)));
        let relay = Arc::new(Relay::new(hooked).pacing(Pacing::recording().0));
        let published: Arc<Mutex<Vec<String>>> = Arc::default();
        let (entered_tx, entered) = std::sync::mpsc::channel::<()>();
        let (release_tx, release) = std::sync::mpsc::channel::<()>();
        let release = Arc::new(Mutex::new(release));

        let first = {
            let (relay, published, release) = (Arc::clone(&relay), Arc::clone(&published), Arc::clone(&release));
            std::thread::spawn(move || {
                relay.drain(10, |env| {
                    if published.lock().expect("no panics").is_empty() {
                        entered_tx.send(()).expect("the test waits");
                        release.lock().expect("no panics").recv().expect("the test releases");
                    }
                    published.lock().expect("no panics").push(env.id.clone());
                    Ok(())
                })
            })
        };
        reads.recv().expect("the first drain read its batch");
        entered.recv().expect("the first drain is publishing");
        let second = {
            let (relay, published) = (Arc::clone(&relay), Arc::clone(&published));
            std::thread::spawn(move || {
                relay.drain(10, |env| {
                    published.lock().expect("no panics").push(env.id.clone());
                    Ok(())
                })
            })
        };
        assert!(
            reads.recv_timeout(std::time::Duration::from_millis(200)).is_err(),
            "the second drain read the outbox while the first was still publishing"
        );
        release_tx.send(()).expect("the first drain waits");
        assert_eq!(first.join().expect("no panics").ok(), Some(2));
        assert_eq!(second.join().expect("no panics").ok(), Some(0));
        assert_eq!(*published.lock().unwrap(), ["acct-r-1", "acct-r-2"], "each row published once");
    }

    #[test]
    fn a_crash_after_the_publish_republishes_and_the_consumer_deduplicates() {
        // Lost acknowledgement: the rows went out but `mark_published` never landed. The next drain
        // publishes them again (at-least-once); a consumer that deduplicates by event id applies
        // each once.
        let hooked = Hooked::new(outbox_of(2));
        hooked.fail_mark.store(true, Ordering::SeqCst);
        let relay = Relay::new(hooked.clone()).pacing(Pacing::recording().0);
        let mut delivered: Vec<String> = Vec::new();
        let crashed = relay.drain(10, |env| {
            delivered.push(env.id.clone());
            Ok(())
        });
        assert_eq!(crashed.err().map(|e| e.to_string()).as_deref(), Some("store: connection lost"));
        hooked.fail_mark.store(false, Ordering::SeqCst);
        let resent = relay.drain(10, |env| {
            delivered.push(env.id.clone());
            Ok(())
        });
        assert_eq!(resent.ok(), Some(2));
        assert_eq!(delivered, ["acct-r-1", "acct-r-2", "acct-r-1", "acct-r-2"], "at-least-once");
        let applied: std::collections::BTreeSet<&String> = delivered.iter().collect();
        assert_eq!(applied.len(), 2, "the consumer's dedup by id applies each once");
        assert!(hooked.inner.unpublished(10).unwrap().is_empty());
    }

    // --- replay ---

    /// The event store, with `load` replaced by a fixed answer: a third-party store that breaks the
    /// gap-free promise.
    struct Lying {
        inner: MemoryEventStore,
        /// Answered verbatim instead of the stored stream, when set.
        load: Option<Vec<EventMessage>>,
    }

    impl Transactor for Lying {
        type Tx = MemoryTx;
        fn begin(&self) -> Result<MemoryTx, Error> {
            self.inner.begin()
        }
        fn commit(&self, tx: MemoryTx) -> Result<(), Error> {
            self.inner.commit(tx)
        }
        fn rollback(&self, tx: MemoryTx) {
            self.inner.rollback(tx);
        }
    }

    impl Outbox<EventMessage> for Lying {
        fn unpublished(&self, limit: usize) -> Result<Vec<EventMessage>, Error> {
            self.inner.unpublished(limit)
        }
        fn mark_published(&self, ids: &[String]) -> Result<(), Error> {
            self.inner.mark_published(ids)
        }
    }

    impl EventStore for Lying {
        fn load(&self, tx: &mut MemoryTx, id: &str, after: u64) -> Result<Vec<EventMessage>, Error> {
            match &self.load {
                Some(answer) => Ok(answer.clone()),
                None => self.inner.load(tx, id, after),
            }
        }
        fn load_snapshot(&self, tx: &mut MemoryTx, id: &str) -> Result<Option<Snapshot>, Error> {
            self.inner.load_snapshot(tx, id)
        }
        fn save_snapshot(&self, tx: &mut MemoryTx, snapshot: &Snapshot) -> Result<(), Error> {
            self.inner.save_snapshot(tx, snapshot)
        }
        fn append(&self, tx: &mut MemoryTx, id: &str, expected: u64, drafts: Vec<EventDraft>) -> Result<Vec<EventMessage>, Error> {
            self.inner.append(tx, id, expected, drafts)
        }
    }

    #[test]
    fn replay_branches() {
        struct Case {
            name: &'static str,
            /// The snapshot stored for the account, if any: `(type_name, version, data)`.
            snapshot: Option<(&'static str, u64, &'static [u8])>,
            /// What `load` answers, as `(sequence, amount)`; the stored stream is three deposits of 1.
            load: Option<&'static [(u64, &'static [u8])]>,
            want: Result<(i64, u64), &'static str>,
        }
        let cases = [
            Case { name: "no snapshot: replay from 0", snapshot: None, load: None, want: Ok((3, 3)) },
            Case { name: "own snapshot: replay the tail", snapshot: Some(("Account", 2, b"40")), load: None, want: Ok((41, 3)) },
            Case {
                name: "another aggregate type's snapshot is ignored",
                snapshot: Some(("Ledger", 2, b"40")),
                load: None,
                want: Ok((3, 3)),
            },
            Case {
                name: "a snapshot that does not load is ignored",
                snapshot: Some(("Account", 2, b"not a number")),
                load: None,
                want: Ok((3, 3)),
            },
            Case {
                name: "a store that returns a gap fails the load",
                snapshot: None,
                load: Some(&[(1, b"1"), (3, b"1")]),
                want: Err("codec: event 1 has sequence 3, expected 2"),
            },
            Case {
                name: "a store that repeats an event fails the load",
                snapshot: None,
                load: Some(&[(1, b"1"), (1, b"1")]),
                want: Err("codec: event 1 has sequence 1, expected 2"),
            },
            Case {
                name: "an unsequenced event fails the load",
                snapshot: None,
                load: Some(&[(0, b"1")]),
                want: Err("codec: event 0 has no sequence"),
            },
        ];
        for c in cases {
            let load = c.load.map(|rows| {
                rows.iter()
                    .map(|&(seq, data)| EventMessage { sequence: (seq > 0).then_some(seq), ..deposited("acct-s", seq, data).record(RECORDED_AT) })
                    .collect()
            });
            let store = Lying { inner: MemoryEventStore::new(), load };
            let stream: Vec<EventDraft> = (1..=3).map(|seq| deposited("acct-s", seq, b"1")).collect();
            let ids: Vec<String> = stream.iter().map(|e| e.id.clone()).collect();
            store.append(&mut MemoryTx::auto_commit(), "acct-s", 0, stream).unwrap();
            if let Some((type_name, version, data)) = c.snapshot {
                let snapshot = Snapshot { aggregate_id: "acct-s".into(), type_name: type_name.into(), version, data: data.to_vec() };
                store.save_snapshot(&mut MemoryTx::auto_commit(), &snapshot).unwrap();
            }
            // The outbox is untouched by a load: the three appended rows still wait, and marking clears them.
            assert_eq!(Outbox::unpublished(&store, 10).unwrap().len(), 3, "{}", c.name);
            store.mark_published(&ids).unwrap();
            let repo = Repository::new(store, AccountEvents, AccountCodec).with_ids(next_id);
            let got = repo.load("acct-s").map(|a| (a.balance, a.root().version)).map_err(|e| e.to_string());
            assert_eq!(got, c.want.map_err(str::to_owned), "{}", c.name);
        }
    }
}

//! The event store seam: the durable log an [`EventStore`] keeps, with its snapshots. Implement it against
//! your own database.
//!
//! [`append`](EventStore::append) stages the event outbox rows in the caller's transaction: a
//! transactional outbox is only worth the name if the event rows and the to-publish rows commit together.
//! Reading them back is another port, an [`Outbox`](crate::Outbox) of [`EventMessage`], which one store
//! may implement beside this one and a [`Relay`](crate::Relay) drains. A store fed to change data capture
//! need not implement it.

use edax_event::{EventDraft, EventMessage};

use crate::unit_of_work::Transactor;
use crate::Error;

/// A marshaled aggregate at a given version, so a load can skip replaying the whole stream.
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// Persisted tenant and immutable erasure owner.
    pub tenant: Option<String>,
    /// Immutable erasure owner.
    pub subject: Option<String>,
    /// The state encryption format and data-key generation.
    pub encryption: Option<edax_seal::EncryptionInfo>,
    pub aggregate_id: String,
    pub type_name: String,
    pub version: u64,
    pub data: Vec<u8>,
}

/// The durable log: per-aggregate events with optimistic concurrency and aggregate snapshots, and the
/// outbox rows each append stages, all under one transaction so committed state and to-publish records
/// land together.
///
/// The boundary itself lives on [`Transactor`] (the supertrait): begin a unit of work, thread the
/// `&mut Tx` through these writes, then commit so they land atomically. The outbox's read side is a port
/// of its own, [`Outbox`](crate::Outbox). Event ids are unique store-wide: an append carrying an id the store already
/// holds is refused, or marking one row published would prune the other unpublished.
///
/// The stream is named by the `aggregate_id` argument of [`append`](Self::append), [`load`](Self::load)
/// and [`load_snapshot`](Self::load_snapshot), and by [`Snapshot::aggregate_id`] for
/// [`save_snapshot`](Self::save_snapshot): never by a draft's own `aggregate_id`. Under
/// [`Tenancy::Pool`](crate::Tenancy::Pool) the repository passes the id composed with the tenant while every draft keeps the
/// bare id, so a store keying its rows by the draft merges two tenants' same-named aggregates.
pub trait EventStore: Transactor {
    /// Events for the aggregate with `sequence > after`, in order (`after` skips a snapshot).
    fn load(&self, tx: &mut Self::Tx, aggregate_id: &str, after: u64) -> Result<Vec<EventMessage>, Error>;

    fn load_snapshot(&self, tx: &mut Self::Tx, aggregate_id: &str) -> Result<Option<Snapshot>, Error>;
    fn save_snapshot(&self, tx: &mut Self::Tx, snapshot: &Snapshot) -> Result<(), Error>;

    /// Append `drafts` and stage the same events as outbox rows in `tx`, atomically. Return the stored
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

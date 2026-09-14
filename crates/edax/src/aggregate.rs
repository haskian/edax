//! The aggregate contract: an event-sourced entity, its version, and the codecs that bridge it to
//! the bytes the store persists.

use edax_event::Event;

use crate::Error;

/// State every aggregate carries: its id, the `version` last loaded from storage, and the events
/// applied since. Embed one and return it from [`Aggregate::root`]/[`Aggregate::root_mut`].
///
/// `pending` is private because it *is* the version: `current()` counts it. A host that pushed to
/// it directly would desync the two, and the snapshot would then lead the log.
pub struct Root<E> {
    pub id: String,
    /// The version the aggregate was loaded at; the pending events come after it.
    pub version: u64,
    pending: Vec<E>,
}

impl<E> Root<E> {
    pub fn new(id: impl Into<String>, version: u64) -> Self {
        Self { id: id.into(), version, pending: Vec::new() }
    }

    /// The prospective version: the loaded one plus the events not yet persisted.
    pub fn current(&self) -> u64 {
        self.version + self.pending.len() as u64
    }

    /// The events applied but not yet persisted, in order.
    pub fn pending(&self) -> &[E] {
        &self.pending
    }

    /// Take the pending events, leaving none. The caller now owns persisting them.
    pub fn take_pending(&mut self) -> Vec<E> {
        std::mem::take(&mut self.pending)
    }
}

/// An event-sourced aggregate: state rebuilt by replaying its events.
pub trait Aggregate {
    /// The domain event type this aggregate applies.
    type Event;

    fn root(&self) -> &Root<Self::Event>;
    fn root_mut(&mut self) -> &mut Root<Self::Event>;

    /// Mutate state for one event. Called both on replay (loading) and on [`Aggregate::apply`].
    fn transition(&mut self, event: &Self::Event);

    /// Record a new event: transition, then queue it as pending for the next save. Queuing is what
    /// bumps [`Root::current`].
    fn apply(&mut self, event: Self::Event) {
        self.transition(&event);
        self.root_mut().pending.push(event);
    }
}

/// Bridges a domain event and its marshaled [`Event`] (name / version / payload bytes).
pub trait EventCodec {
    type Domain;

    /// Serialization is fallible in both directions, so both directions report it: a payload the
    /// codec cannot write must reach the caller as [`Error::Codec`] — terminal, dead-lettered —
    /// instead of panicking inside whichever host thread happened to call [`save`](crate::Repository::save).
    fn marshal(&self, event: &Self::Domain) -> Result<Event, Error>;
    fn unmarshal(&self, raw: &Event) -> Result<Self::Domain, Error>;
}

/// Bridges an aggregate and its snapshot bytes.
pub trait AggregateCodec {
    type Aggregate: Aggregate;

    /// Stable type label recorded on each snapshot.
    fn type_name(&self) -> &str;

    /// Rebuild an aggregate at `version`; `data == None` means none stored yet (empty at v0).
    fn load(&self, id: &str, version: u64, data: Option<&[u8]>) -> Result<Self::Aggregate, Error>;

    /// Serialize the aggregate's state. Fallible for the same reason as [`EventCodec::marshal`],
    /// and so an implementor has the same error channel `load` already has.
    fn dump(&self, agg: &Self::Aggregate) -> Result<Vec<u8>, Error>;
}

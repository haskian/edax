//! The aggregate contract: an event-sourced entity, its version, and the codecs that bridge it to
//! the bytes the store persists.

use edax_event::Payload;

use crate::Error;

/// State every aggregate carries: its id, the `version` last loaded from storage, the tenant it belongs
/// to, and the events applied since. Embed one and return it from [`Aggregate::root`]/[`Aggregate::root_mut`].
///
/// Every field is private and read through its getter. The [`Repository`](crate::Repository) that loads
/// and saves the aggregate is the only writer: `pending` *is* the version (`current()` counts it), and the
/// version and the tenant are what storage last said. A host that set any of them would desync the
/// aggregate from its stream, and the snapshot would then lead the log or land under another tenant.
pub struct Root<E> {
    id: String,
    version: u64,
    tenant: Option<String>,
    pending: Vec<E>,
    subject: Option<String>,
    pub(crate) prepared: Option<crate::sealing::PreparedSave>,
}

impl<E> Root<E> {
    /// The root of aggregate `id` at `version`, with no tenant and nothing pending: what an
    /// [`AggregateCodec::load`] builds.
    pub fn new(id: impl Into<String>, version: u64) -> Self {
        Self {
            id: id.into(),
            version,
            tenant: None,
            pending: Vec::new(),
            subject: None,
            prepared: None,
        }
    }

    /// The aggregate's id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The version the aggregate was loaded at; the pending events come after it.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The tenant this instance belongs to, set by the [`Repository`](crate::Repository) that loaded it
    /// and stamped onto every event it saves. `None` outside a multi-tenant deployment.
    ///
    /// [`Root::new`] leaves it empty and the repository fills it from the call that loaded the aggregate,
    /// which is what keeps a load and the save that follows on one tenant. So under
    /// [`Tenancy::Pool`](crate::Tenancy::Pool) or [`Tenancy::Bridge`](crate::Tenancy::Bridge) a new
    /// aggregate starts from [`Repository::load`](crate::Repository::load) with a tenant, which returns it
    /// empty at version 0 with the tenant set; one built with [`Root::new`] has none, and its save is a
    /// `codec` error. The Python and Go SDKs add `create`, which does the same without reading; this
    /// reference model leaves it out, as it pins no shared decision.
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    /// Immutable erasure owner once this aggregate has committed state.
    pub fn subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }

    pub fn set_subject(&mut self, subject: Option<&str>) -> Result<(), Error> {
        edax_event::sealing::check_owner(self.version > 0, self.subject(), subject)?;
        if self.prepared.is_some() && self.subject() != subject {
            return Err(Error::new(
                edax_core::ErrorCode::BadHandle,
                "pending prepared write owns its subject",
            ));
        }
        self.subject = subject.map(ToOwned::to_owned);
        Ok(())
    }

    pub(crate) fn restore_subject(&mut self, subject: Option<&str>) {
        self.subject = subject.map(ToOwned::to_owned);
    }

    /// The prospective version: the loaded one plus the events not yet persisted.
    pub fn current(&self) -> u64 {
        self.version
            .saturating_add(u64::try_from(self.pending.len()).unwrap_or(u64::MAX))
    }

    /// The events applied but not yet persisted, in order.
    pub fn pending(&self) -> &[E] {
        &self.pending
    }

    /// Pin the root to what storage holds: the stream is at `version`, and nothing is pending.
    pub(crate) fn settle(&mut self, version: u64) {
        self.pending.clear();
        self.prepared = None;
        self.version = version;
    }

    /// Stamp the tenant the repository loaded the aggregate for.
    pub(crate) fn set_tenant(&mut self, tenant: Option<&str>) {
        self.tenant = tenant.map(ToOwned::to_owned);
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

/// Bridges a domain value, an event or a command, and its marshaled [`Payload`] (name / version / payload
/// bytes).
pub trait PayloadCodec {
    type Domain;

    /// Serialization is fallible in both directions, so both directions report it: a payload the
    /// codec cannot write must reach the caller as a `codec` error — terminal, dead-lettered —
    /// instead of panicking inside whichever host thread happened to call [`save`](crate::Repository::save).
    fn marshal(&self, event: &Self::Domain) -> Result<Payload, Error>;
    fn unmarshal(&self, raw: &Payload) -> Result<Self::Domain, Error>;
}

/// [`PayloadCodec`] in its event role, as a [`Repository`](crate::Repository) takes it.
pub use self::PayloadCodec as EventCodec;

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

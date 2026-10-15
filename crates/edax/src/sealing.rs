//! Optional encryption orchestration. Production providers own durable keys and revocation fences.

use std::sync::{Arc, Condvar, Mutex};
use std::{
    marker::PhantomData,
    rc::Rc,
    thread::{self, ThreadId},
};

use edax_core::{lock, Error, ErrorCode};
use edax_event::{sealing as kernel, CommandMessage, EventDraft, EventMessage};
pub use edax_seal::{Cipher, EncryptionInfo, KeyMaterial, KeyScope, KeyStatus, ReservedKey, SealedValue};

use crate::Snapshot;

/// A provider admission fence. Dropping it releases the fence after storage commit or rollback.
/// Implementations must block revocation and cleanup while any admitted guard is alive.
pub trait KeyGuard {}

#[derive(Default)]
struct Fence {
    state: Mutex<(Option<ThreadId>, usize)>,
    changed: Condvar,
}
struct FenceGuard<'a> {
    fence: &'a Fence,
    _not_send: PhantomData<Rc<()>>,
}
impl KeyGuard for FenceGuard<'_> {}
impl Fence {
    fn enter(&self, nested: bool) -> Result<FenceGuard<'_>, Error> {
        let caller = thread::current().id();
        let mut state = lock(&self.state);
        while state.0.is_some() && state.0 != Some(caller) {
            state = self.changed.wait(state).unwrap_or_else(|poison| poison.into_inner());
        }
        if !nested && state.0 == Some(caller) {
            return Err(Error::new(ErrorCode::BadHandle, "cannot revoke inside admitted work"));
        }
        state.1 = state
            .1
            .checked_add(1)
            .ok_or_else(|| Error::new(ErrorCode::BadHandle, "scope guard nesting overflow"))?;
        state.0 = Some(caller);
        Ok(FenceGuard {
            fence: self,
            _not_send: PhantomData,
        })
    }
}
impl Drop for FenceGuard<'_> {
    fn drop(&mut self) {
        let mut state = lock(&self.fence.state);
        state.1 = state.1.saturating_sub(1);
        if state.1 == 0 {
            state.0 = None;
            self.fence.changed.notify_all();
        }
    }
}

/// Synchronous external key-provider port. Resolution never creates keys. Reservations atomically
/// account for all writers; tenant revocation also blocks current and future child subjects.
/// Returned material belongs to one operation and must not be cached by the SDK or provider client.
pub trait KeyProvider: Send + Sync {
    fn reserve(&self, scope: &KeyScope, cipher: Cipher) -> Result<ReservedKey, Error>;
    fn resolve(&self, scope: &KeyScope, cipher: Cipher, key_id: &str) -> Result<KeyMaterial, Error>;
    fn fingerprint(&self, scope: &KeyScope) -> Result<ReservedKey, Error>;
    fn resolve_fingerprint(&self, scope: &KeyScope, key_id: &str) -> Result<KeyMaterial, Error>;
    fn revoke(&self, scope: &KeyScope) -> Result<KeyStatus, Error>;
    fn destroy(&self, scope: &KeyScope) -> Result<KeyStatus, Error>;
    fn status(&self, scope: &KeyScope) -> Result<KeyStatus, Error>;
    fn hold(&self, scope: &KeyScope) -> Result<Box<dyn KeyGuard + '_>, Error>;
}

/// Process-local reference provider. Losing the instance loses keys and permanent tombstones.
/// Its conservative global fence supports nested admitted work on the same thread.
#[derive(Default)]
pub struct MemoryKeys {
    keys: Mutex<edax_seal::MemoryKeyProvider>,
    fence: Fence,
}

impl MemoryKeys {
    pub fn new() -> Self {
        Self::default()
    }

    /// A smaller AES budget is useful for conformance tests of generation rotation.
    pub fn with_aes_limit(limit: u64) -> Result<Self, Error> {
        Ok(Self {
            keys: Mutex::new(edax_seal::MemoryKeyProvider::with_aes_limit(limit)?),
            fence: Fence::default(),
        })
    }
}

impl KeyProvider for MemoryKeys {
    fn reserve(&self, scope: &KeyScope, cipher: Cipher) -> Result<ReservedKey, Error> {
        lock(&self.keys).reserve(scope, cipher)
    }
    fn resolve(&self, scope: &KeyScope, cipher: Cipher, key_id: &str) -> Result<KeyMaterial, Error> {
        lock(&self.keys).resolve(scope, cipher, key_id)
    }
    fn fingerprint(&self, scope: &KeyScope) -> Result<ReservedKey, Error> {
        lock(&self.keys).fingerprint_key(scope)
    }
    fn resolve_fingerprint(&self, scope: &KeyScope, key_id: &str) -> Result<KeyMaterial, Error> {
        lock(&self.keys).resolve_fingerprint(scope, key_id)
    }
    fn revoke(&self, scope: &KeyScope) -> Result<KeyStatus, Error> {
        let _fence = self.fence.enter(false)?;
        lock(&self.keys).revoke(scope)
    }
    fn destroy(&self, scope: &KeyScope) -> Result<KeyStatus, Error> {
        let _fence = self.fence.enter(false)?;
        lock(&self.keys).destroy(scope)
    }
    fn status(&self, scope: &KeyScope) -> Result<KeyStatus, Error> {
        lock(&self.keys).status(scope)
    }
    fn hold(&self, scope: &KeyScope) -> Result<Box<dyn KeyGuard + '_>, Error> {
        let fence = self.fence.enter(true)?;
        check_write_status(self.status(scope)?)?;
        Ok(Box::new(fence))
    }
}

fn check_write_status(status: KeyStatus) -> Result<(), Error> {
    kernel::check_key_write_status(status.as_str())
}

/// Immutable provider configuration shared by repositories and message processors.
pub struct Sealing {
    keys: Arc<dyn KeyProvider>,
    key_namespace: String,
    cipher: Cipher,
    allow_plaintext: bool,
}

impl Sealing {
    pub fn new(keys: Arc<dyn KeyProvider>, key_namespace: impl Into<String>, cipher: Cipher) -> Result<Self, Error> {
        let key_namespace = key_namespace.into();
        KeyScope {
            key_namespace: key_namespace.clone(),
            tenant: None,
            subject: None,
        }
        .check()?;
        Ok(Self {
            keys,
            key_namespace,
            cipher,
            allow_plaintext: false,
        })
    }

    /// Explicit historical-plaintext migration policy. Strict rejection is the default.
    pub fn allow_plaintext(mut self, allow: bool) -> Self {
        self.allow_plaintext = allow;
        self
    }
    pub fn keys(&self) -> &dyn KeyProvider {
        self.keys.as_ref()
    }
    pub fn key_namespace(&self) -> &str {
        &self.key_namespace
    }
    pub fn cipher(&self) -> Cipher {
        self.cipher
    }

    pub fn scope(&self, tenant: Option<&str>, subject: Option<&str>) -> Result<KeyScope, Error> {
        kernel::protected_scope(&self.key_namespace, tenant, subject)
    }

    /// Apply the migration policy and permanent revocation without allocating keys.
    /// Historical records without an owner remain subject to their tenant's tombstone.
    pub fn check_legacy(&self, tenant: Option<&str>, subject: Option<&str>) -> Result<(), Error> {
        kernel::check_plaintext(self.allow_plaintext)?;
        let scope = self.legacy_scope(tenant, subject)?;
        check_write_status(self.keys.status(&scope)?)
    }

    /// Scope for authenticated legacy reads; an absent historical subject uses the tenant fence.
    pub fn legacy_scope(&self, tenant: Option<&str>, subject: Option<&str>) -> Result<KeyScope, Error> {
        let scope = KeyScope {
            key_namespace: self.key_namespace.clone(),
            tenant: tenant.map(str::to_owned),
            subject: subject.map(str::to_owned),
        };
        scope.check()?;
        Ok(scope)
    }

    /// Keep this guard alive through the final commit, including external participant effects.
    pub fn hold(&self, scope: &KeyScope) -> Result<Box<dyn KeyGuard + '_>, Error> {
        if scope.key_namespace != self.key_namespace {
            return Err(Error::codec("key namespace does not match sealing configuration"));
        }
        self.keys.hold(scope)
    }

    pub fn check_active(&self, scope: &KeyScope) -> Result<(), Error> {
        kernel::check_key_status(self.keys.status(scope)?.as_str())
    }

    pub(crate) fn prepare(
        &self,
        tenant: Option<&str>,
        subject: Option<&str>,
    ) -> Result<(EncryptionInfo, KeyMaterial), Error> {
        let scope = self.scope(tenant, subject)?;
        let reserved = self.keys.reserve(&scope, self.cipher)?;
        let info = EncryptionInfo::fresh(self.key_namespace.clone(), reserved.key_id, self.cipher)?;
        Ok((info, reserved.key))
    }

    pub(crate) fn resolve(
        &self,
        tenant: Option<&str>,
        subject: Option<&str>,
        info: &EncryptionInfo,
    ) -> Result<KeyMaterial, Error> {
        let scope = kernel::message_scope(&self.key_namespace, tenant, subject, info)?;
        let key = self.keys.resolve(&scope, info.cipher, &info.key_id)?;
        self.check_active(&scope)?;
        Ok(key)
    }

    pub fn seal_event(&self, draft: EventDraft) -> Result<EventDraft, Error> {
        let (info, key) = self.prepare(draft.tenant.as_deref(), draft.subject.as_deref())?;
        kernel::seal_event(draft, info, key.as_bytes())
    }

    pub fn open_event(&self, event: &EventMessage) -> Result<Vec<u8>, Error> {
        match event.encryption.as_ref() {
            Some(info) => {
                let key = self.resolve(event.tenant.as_deref(), event.subject.as_deref(), info)?;
                kernel::open_event(event, key.as_bytes())
            }
            None => {
                self.check_legacy(event.tenant.as_deref(), event.subject.as_deref())?;
                Ok(event.data.clone())
            }
        }
    }

    /// `None` means only that trusted scoped key resolution confirmed destruction.
    pub fn open_event_delivery(&self, event: &EventMessage) -> Result<Option<Vec<u8>>, Error> {
        let Some(info) = event.encryption.as_ref() else {
            return match self.check_legacy(event.tenant.as_deref(), event.subject.as_deref()) {
                Err(error) if error.code == ErrorCode::Shredded => Ok(None),
                result => result.map(|()| Some(event.data.clone())),
            };
        };
        let key = match self.resolve(event.tenant.as_deref(), event.subject.as_deref(), info) {
            Err(error) if error.code == ErrorCode::Shredded => return Ok(None),
            result => result?,
        };
        kernel::open_event(event, key.as_bytes()).map(Some)
    }

    pub fn seal_command(&self, command: CommandMessage) -> Result<CommandMessage, Error> {
        let (info, key) = self.prepare(command.tenant.as_deref(), command.subject.as_deref())?;
        kernel::seal_command(command, info, key.as_bytes())
    }

    pub fn open_command(&self, command: &CommandMessage) -> Result<Vec<u8>, Error> {
        match command.encryption.as_ref() {
            Some(info) => {
                let key = self.resolve(command.tenant.as_deref(), command.subject.as_deref(), info)?;
                kernel::open_command(command, key.as_bytes())
            }
            None => {
                self.check_legacy(command.tenant.as_deref(), command.subject.as_deref())?;
                Ok(command.data.clone())
            }
        }
    }

    pub fn open_command_delivery(&self, command: &CommandMessage) -> Result<Option<Vec<u8>>, Error> {
        let Some(info) = command.encryption.as_ref() else {
            return match self.check_legacy(command.tenant.as_deref(), command.subject.as_deref()) {
                Err(error) if error.code == ErrorCode::Shredded => Ok(None),
                result => result.map(|()| Some(command.data.clone())),
            };
        };
        let key = match self.resolve(command.tenant.as_deref(), command.subject.as_deref(), info) {
            Err(error) if error.code == ErrorCode::Shredded => return Ok(None),
            result => result?,
        };
        kernel::open_command(command, key.as_bytes()).map(Some)
    }

    pub fn seal_state(&self, context: &StateContext<'_>, plaintext: &[u8]) -> Result<SealedValue, Error> {
        let (info, key) = self.prepare(context.tenant, context.subject)?;
        let aad = context.aad(&info)?;
        let data = edax_seal::seal(&info, key.as_bytes(), &aad, plaintext)?;
        Ok(SealedValue { encryption: info, data })
    }

    pub fn open_state(
        &self,
        context: &StateContext<'_>,
        info: Option<&EncryptionInfo>,
        data: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let Some(info) = info else {
            self.check_legacy(context.tenant, context.subject)?;
            return Ok(data.to_vec());
        };
        let key = self.resolve(context.tenant, context.subject, info)?;
        edax_seal::open(info, key.as_bytes(), &context.aad(info)?, data)
    }
}

/// Identity and public processing context authenticated beside stored application state.
pub struct StateContext<'a> {
    pub kind: &'a str,
    pub identity: &'a str,
    pub type_name: &'a str,
    pub version: u64,
    pub tenant: Option<&'a str>,
    pub subject: Option<&'a str>,
    pub marks: &'a str,
    pub completed: bool,
}

impl StateContext<'_> {
    fn aad(&self, info: &EncryptionInfo) -> Result<Vec<u8>, Error> {
        kernel::state_context(
            self.kind,
            self.identity,
            self.type_name,
            self.version,
            self.tenant,
            self.subject,
            self.marks,
            self.completed,
            info,
        )
    }
}

/// Retained only in memory until a protected save is reconciled or committed.
#[derive(Clone, Debug)]
pub(crate) struct PreparedSave {
    pub signature: Vec<u8>,
    pub drafts: Vec<EventDraft>,
    pub snapshot: Option<Snapshot>,
    pub initial: u64,
    pub version: u64,
    pub attempted: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_restore_reads_respect_subject_and_tenant_tombstones() {
        for subject in [Some("person-42"), None] {
            let keys = Arc::new(MemoryKeys::new());
            let sealing = Sealing::new(keys.clone(), "production-eu", Cipher::default())
                .unwrap()
                .allow_plaintext(true);
            let mut event = crate::testkit::account::deposited("account-42", 1, b"private-restored-data").record(42);
            event.tenant = Some("tenant-7".into());
            event.subject = subject.map(str::to_owned);
            let command = CommandMessage {
                id: "command-1".into(),
                idempotency_key: None,
                aggregate_id: Some("account-42".into()),
                tenant: event.tenant.clone(),
                subject: event.subject.clone(),
                encryption: None,
                name: "Deposit".into(),
                version: 1,
                data: event.data.clone(),
                correlation_id: "flow-1".into(),
                causation_id: "flow-1".into(),
                issued_at: 42,
                extensions: Default::default(),
                metadata: Default::default(),
            };
            let state = StateContext {
                kind: "snapshot",
                identity: "account-42",
                type_name: "Account",
                version: 1,
                tenant: Some("tenant-7"),
                subject,
                marks: "",
                completed: false,
            };
            let scope = KeyScope {
                key_namespace: "production-eu".into(),
                tenant: Some("tenant-7".into()),
                subject: subject.map(str::to_owned),
            };
            assert_eq!(sealing.open_event(&event).unwrap(), event.data);
            assert_eq!(sealing.open_command(&command).unwrap(), command.data);
            assert_eq!(sealing.open_state(&state, None, &event.data).unwrap(), event.data);
            assert_eq!(
                keys.status(&scope).unwrap(),
                KeyStatus::Unknown,
                "reads must not create keys"
            );

            keys.revoke(&scope).unwrap();
            assert_eq!(sealing.open_event(&event).unwrap_err().code, ErrorCode::KeyRevoking);
            assert_eq!(sealing.open_command(&command).unwrap_err().code, ErrorCode::KeyRevoking);
            assert_eq!(
                sealing.open_state(&state, None, &event.data).unwrap_err().code,
                ErrorCode::KeyRevoking
            );
            assert_eq!(
                sealing.open_event_delivery(&event).unwrap_err().code,
                ErrorCode::KeyRevoking
            );
            assert_eq!(
                sealing.open_command_delivery(&command).unwrap_err().code,
                ErrorCode::KeyRevoking
            );

            keys.destroy(&scope).unwrap();
            assert_eq!(sealing.open_event(&event).unwrap_err().code, ErrorCode::Shredded);
            assert_eq!(sealing.open_command(&command).unwrap_err().code, ErrorCode::Shredded);
            assert_eq!(
                sealing.open_state(&state, None, &event.data).unwrap_err().code,
                ErrorCode::Shredded
            );
            assert!(sealing.open_event_delivery(&event).unwrap().is_none());
            assert!(sealing.open_command_delivery(&command).unwrap().is_none());
            if subject.is_none() {
                event.subject = Some("restored-child".into());
                assert_eq!(sealing.open_event(&event).unwrap_err().code, ErrorCode::Shredded);
            }
            let strict = Sealing::new(keys.clone(), "production-eu", Cipher::default()).unwrap();
            assert_eq!(strict.open_event_delivery(&event).unwrap_err().code, ErrorCode::Codec);
        }
    }

    #[test]
    fn memory_provider_drains_admitted_work_before_destruction() {
        use std::sync::mpsc;
        use std::time::Duration;
        let keys = Arc::new(MemoryKeys::new());
        let scope = KeyScope {
            key_namespace: "production-eu".into(),
            tenant: Some("tenant-7".into()),
            subject: Some("person-42".into()),
        };
        let guard = keys.hold(&scope).unwrap();
        keys.reserve(&scope, Cipher::default()).unwrap();
        let (started, ready) = mpsc::channel();
        let (finished, result) = mpsc::channel();
        let worker_keys = keys.clone();
        let worker_scope = scope.clone();
        let worker = std::thread::spawn(move || {
            started.send(()).unwrap();
            finished.send(worker_keys.destroy(&worker_scope)).unwrap();
        });
        ready.recv().unwrap();
        assert!(matches!(
            result.recv_timeout(Duration::from_millis(20)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert_eq!(keys.status(&scope).unwrap(), KeyStatus::Active);
        drop(guard);
        assert_eq!(
            result.recv_timeout(Duration::from_secs(2)).unwrap().unwrap(),
            KeyStatus::Destroyed
        );
        worker.join().unwrap();
        assert_eq!(
            keys.reserve(&scope, Cipher::default()).unwrap_err().code,
            ErrorCode::Shredded
        );
    }

    #[test]
    fn delivery_skip_requires_destroyed_scoped_generation() {
        let keys = Arc::new(MemoryKeys::new());
        let sealing = Sealing::new(keys.clone(), "production-eu", Cipher::default()).unwrap();
        let mut draft = crate::testkit::account::deposited("account-42", 1, b"314159");
        draft.subject = Some("person-42".into());
        let event = sealing.seal_event(draft).unwrap().record(42);
        assert_eq!(sealing.open_event_delivery(&event).unwrap(), Some(b"314159".to_vec()));
        let scope = sealing.scope(None, Some("person-42")).unwrap();
        keys.revoke(&scope).unwrap();
        assert_eq!(
            sealing.open_event_delivery(&event).unwrap_err().code,
            ErrorCode::KeyRevoking
        );
        keys.destroy(&scope).unwrap();
        assert!(sealing.open_event_delivery(&event).unwrap().is_none());
        let mut missing = event.clone();
        missing.encryption.as_mut().unwrap().key_id = "missing-generation".into();
        assert_eq!(
            sealing.open_event_delivery(&missing).unwrap_err().code,
            ErrorCode::KeyMissing
        );
        let mut wrong_subject = event.clone();
        wrong_subject.subject = Some("person-43".into());
        assert_eq!(
            sealing.open_event_delivery(&wrong_subject).unwrap_err().code,
            ErrorCode::KeyUnauthorized
        );
        let wrong_namespace = Sealing::new(keys, "staging-eu", Cipher::default()).unwrap();
        assert_eq!(
            wrong_namespace.open_event_delivery(&event).unwrap_err().code,
            ErrorCode::Codec
        );
    }
}

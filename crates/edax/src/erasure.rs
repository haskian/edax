//! Tracked erasure orchestration. Production stores persist operations and the control outbox atomically.
//! Participant callbacks must fence writes, drain work, and clean plaintext including restore copies.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use edax_core::{lock, Error, ErrorCode};
use edax_event::erasure as kernel;
pub use edax_event::erasure::{ErasureOperation, ErasureStage};
use edax_seal::{KeyScope, KeyStatus};

use crate::sealing::KeyProvider;

/// An immutable operation and its optimistic concurrency revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredErasure {
    pub version: u64,
    pub operation: ErasureOperation,
}

/// Stable control notification, independent of payload encryption keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErasureNotification {
    pub operation_id: String,
    pub record: String,
}

/// Durable workflow port. `advance` applies a kernel transition with CAS and atomically enqueues
/// its notification. A retry must preserve the existing operation's immutable participant set.
pub trait ErasureStore: Send + Sync {
    fn create(&self, candidate: ErasureOperation) -> Result<StoredErasure, Error>;
    fn get(&self, scope: &KeyScope, request_id: &str) -> Result<Option<StoredErasure>, Error>;
    fn get_by_id(&self, operation_id: &str) -> Result<Option<StoredErasure>, Error>;
    fn advance(
        &self,
        previous: &StoredErasure,
        action: &str,
        participant: Option<&str>,
    ) -> Result<StoredErasure, Error>;
    fn pending(&self, limit: usize) -> Result<Vec<StoredErasure>, Error> {
        self.pending_after(limit, None)
    }
    /// Read unfinished operations strictly after this ID, in ascending order.
    /// Recovery workers advance to the last returned ID and reset to `None` after an empty page.
    /// Resetting revisits blocked operations and discovers new IDs inserted before the cursor.
    fn pending_after(&self, limit: usize, after: Option<&str>) -> Result<Vec<StoredErasure>, Error>;
    fn unpublished(&self, limit: usize) -> Result<Vec<ErasureNotification>, Error>;
    fn mark_published(&self, operation_id: &str) -> Result<(), Error>;
}

#[derive(Default)]
struct MemoryRecords {
    operations: BTreeMap<String, StoredErasure>,
    requests: BTreeMap<(Vec<u8>, String), String>,
    notifications: BTreeMap<String, ErasureNotification>,
}

/// Process-local reference operation store. Restart loses its records and notifications.
#[derive(Default)]
pub struct MemoryErasureStore {
    records: Mutex<MemoryRecords>,
}
impl MemoryErasureStore {
    pub fn new() -> Self {
        Self::default()
    }
}
impl ErasureStore for MemoryErasureStore {
    fn create(&self, candidate: ErasureOperation) -> Result<StoredErasure, Error> {
        let initial = ErasureOperation::new(
            candidate.operation_id().into(),
            candidate.request_id().into(),
            candidate.scope().clone(),
            candidate.required_participants().clone(),
        )?;
        if candidate != initial {
            return Err(Error::bad_handle("an erasure store creates only initial operations"));
        }
        let request = (candidate.scope().encode()?, candidate.request_id().to_owned());
        let mut records = lock(&self.records);
        if let Some(existing) = records.requests.get(&request).and_then(|id| records.operations.get(id)) {
            kernel::check_request(&existing.operation, &candidate)?;
            return Ok(existing.clone());
        }
        if records.operations.contains_key(candidate.operation_id()) {
            return Err(Error::bad_handle("erasure operation ID is already in use"));
        }
        let stored = StoredErasure {
            version: 1,
            operation: candidate,
        };
        records
            .requests
            .insert(request, stored.operation.operation_id().to_owned());
        records
            .operations
            .insert(stored.operation.operation_id().to_owned(), stored.clone());
        Ok(stored)
    }
    fn get(&self, scope: &KeyScope, request_id: &str) -> Result<Option<StoredErasure>, Error> {
        let request = (scope.encode()?, request_id.to_owned());
        let records = lock(&self.records);
        Ok(records
            .requests
            .get(&request)
            .and_then(|id| records.operations.get(id))
            .cloned())
    }
    fn get_by_id(&self, operation_id: &str) -> Result<Option<StoredErasure>, Error> {
        Ok(lock(&self.records).operations.get(operation_id).cloned())
    }
    fn advance(
        &self,
        previous: &StoredErasure,
        action: &str,
        participant: Option<&str>,
    ) -> Result<StoredErasure, Error> {
        let mut records = lock(&self.records);
        if records.operations.get(previous.operation.operation_id()) != Some(previous) {
            return Err(Error::new(ErrorCode::Concurrency, "erasure operation revision changed"));
        }
        let operation = kernel::apply(previous.operation.clone(), action, participant)?;
        let notification = kernel::notification(&previous.operation, &operation)?;
        let version = previous
            .version
            .checked_add(1)
            .ok_or_else(|| Error::bad_handle("erasure revision exhausted"))?;
        let stored = StoredErasure { version, operation };
        if let Some(record) = notification {
            let operation_id = stored.operation.operation_id().to_owned();
            records
                .notifications
                .insert(operation_id.clone(), ErasureNotification { operation_id, record });
        }
        records
            .operations
            .insert(stored.operation.operation_id().to_owned(), stored.clone());
        Ok(stored)
    }
    fn pending_after(&self, limit: usize, after: Option<&str>) -> Result<Vec<StoredErasure>, Error> {
        Ok(lock(&self.records)
            .operations
            .range::<str, _>((after.map_or(Bound::Unbounded, Bound::Excluded), Bound::Unbounded))
            .map(|(_, row)| row)
            .filter(|row| row.operation.stage() != ErasureStage::Complete)
            .take(limit)
            .cloned()
            .collect())
    }
    fn unpublished(&self, limit: usize) -> Result<Vec<ErasureNotification>, Error> {
        Ok(lock(&self.records)
            .notifications
            .values()
            .take(limit)
            .cloned()
            .collect())
    }
    fn mark_published(&self, operation_id: &str) -> Result<(), Error> {
        lock(&self.records).notifications.remove(operation_id);
        Ok(())
    }
}

/// Idempotent destination cleanup. Success confirms fencing, draining, and plaintext cleanup.
pub type ErasureCleanup = Arc<dyn Fn(&KeyScope) -> Result<(), Error> + Send + Sync>;

/// Synchronous reference coordinator. Effects may repeat after a crash; confirmations never precede effects.
pub struct ErasureCoordinator {
    keys: Arc<dyn KeyProvider>,
    store: Arc<dyn ErasureStore>,
    participants: BTreeMap<String, ErasureCleanup>,
    required: BTreeSet<String>,
}
impl ErasureCoordinator {
    pub fn new(
        keys: Arc<dyn KeyProvider>,
        store: Arc<dyn ErasureStore>,
        participants: BTreeMap<String, ErasureCleanup>,
    ) -> Self {
        let required = participants.keys().cloned().collect();
        Self {
            keys,
            store,
            participants,
            required,
        }
    }
    /// Declare offline participants too. Every registered callback must remain required.
    /// Existing requests retain their set; repeating `erase` with a changed set is refused.
    pub fn with_required_participants(mut self, required: BTreeSet<String>) -> Self {
        self.required = required;
        self
    }
    pub fn erase(&self, scope: &KeyScope, request_id: &str) -> Result<ErasureOperation, Error> {
        self.check_participants()?;
        let operation = ErasureOperation::new(
            edax_core::guid(),
            request_id.into(),
            scope.clone(),
            self.required.clone(),
        )?;
        let stored = self.store.create(operation)?;
        self.resume(stored.operation.scope(), stored.operation.request_id())
    }
    pub fn status(&self, scope: &KeyScope, request_id: &str) -> Result<Option<ErasureOperation>, Error> {
        Ok(self.store.get(scope, request_id)?.map(|row| row.operation))
    }
    fn require(&self, scope: &KeyScope, request_id: &str) -> Result<StoredErasure, Error> {
        self.store
            .get(scope, request_id)?
            .ok_or_else(|| Error::bad_handle("erasure operation does not exist"))
    }
    fn check_participants(&self) -> Result<(), Error> {
        kernel::validate_participants(&self.required, &self.participants.keys().cloned().collect())
    }
    pub fn acknowledge(
        &self,
        scope: &KeyScope,
        request_id: &str,
        participant: &str,
    ) -> Result<ErasureOperation, Error> {
        loop {
            let stored = self.require(scope, request_id)?;
            match self.store.advance(&stored, "acknowledge", Some(participant)) {
                Err(error) if error.code == ErrorCode::Concurrency => continue,
                result => return result.map(|row| row.operation),
            }
        }
    }
    /// Resume using the stored participant set. Missing callbacks remain pending for explicit acknowledgement.
    /// Attempt every pending cleanup, persist successes, then return the first cleanup error unchanged.
    /// Provider and store failures also propagate; callers must retry unfinished operations.
    pub fn resume(&self, scope: &KeyScope, request_id: &str) -> Result<ErasureOperation, Error> {
        self.check_participants()?;
        let mut cleanup_error = None;
        'resume: loop {
            let mut stored = self.require(scope, request_id)?;
            let action = match stored.operation.stage() {
                ErasureStage::Persisted => {
                    if !confirmed("revoke", self.keys.revoke(stored.operation.scope())?)? {
                        return Ok(stored.operation);
                    }
                    "revoke"
                }
                ErasureStage::Revoked => "notify",
                ErasureStage::Notified => {
                    let pending: Vec<_> = stored
                        .operation
                        .required_participants()
                        .difference(stored.operation.acknowledgements())
                        .cloned()
                        .collect();
                    for participant in pending {
                        let Some(cleanup) = self.participants.get(&participant) else {
                            continue;
                        };
                        if let Err(error) = cleanup(stored.operation.scope()) {
                            cleanup_error.get_or_insert(error);
                            continue;
                        }
                        match self.store.advance(&stored, "acknowledge", Some(&participant)) {
                            Err(error) if error.code == ErrorCode::Concurrency => continue 'resume,
                            result => stored = result?,
                        }
                    }
                    if let Some(error) = cleanup_error {
                        return Err(error);
                    }
                    if !stored.operation.can_destroy() {
                        return Ok(stored.operation);
                    }
                    if !confirmed("destroy", self.keys.destroy(stored.operation.scope())?)? {
                        return Ok(stored.operation);
                    }
                    "destroy"
                }
                ErasureStage::DestructionConfirmed => "complete",
                ErasureStage::Complete => return cleanup_error.map_or(Ok(stored.operation), Err),
            };
            match self.store.advance(&stored, action, None) {
                Err(error) if error.code == ErrorCode::Concurrency => continue,
                result => {
                    result?;
                }
            }
        }
    }
}

fn confirmed(action: &str, status: KeyStatus) -> Result<bool, Error> {
    kernel::provider_confirmed(action, status.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sealing::MemoryKeys;

    fn scope() -> KeyScope {
        KeyScope {
            key_namespace: "production".into(),
            tenant: Some("tenant".into()),
            subject: Some("person".into()),
        }
    }
    fn candidate(id: &str) -> ErasureOperation {
        ErasureOperation::new(
            id.into(),
            "request".into(),
            scope(),
            BTreeSet::from(["database".into()]),
        )
        .unwrap()
    }

    #[test]
    fn recovery_cursor_passes_blocked_operations_and_wraps_for_new_earlier_ids() {
        let store = MemoryErasureStore::new();
        for id in ["b-blocked", "c-completed", "d-pending"] {
            let mut row = store
                .create(ErasureOperation::new(id.into(), id.into(), scope(), BTreeSet::new()).unwrap())
                .unwrap();
            if id == "c-completed" {
                for action in ["revoke", "notify", "destroy", "complete"] {
                    row = store.advance(&row, action, None).unwrap();
                }
            }
        }
        let first = store.pending(1).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].operation.operation_id(), "b-blocked");
        let second = store.pending_after(1, Some(first[0].operation.operation_id())).unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].operation.operation_id(), "d-pending");
        store
            .create(ErasureOperation::new("a-new".into(), "a-new".into(), scope(), BTreeSet::new()).unwrap())
            .unwrap();
        assert!(store.pending_after(1, Some("d-pending")).unwrap().is_empty());
        assert_eq!(
            store.pending_after(1, None).unwrap()[0].operation.operation_id(),
            "a-new"
        );
        assert_eq!(
            store.pending_after(1, Some("bb-nonexistent")).unwrap()[0]
                .operation
                .operation_id(),
            "d-pending"
        );
        assert!(store.pending_after(0, None).unwrap().is_empty());
    }

    #[test]
    fn failed_cleanup_does_not_starve_healthy_participants() {
        let keys = Arc::new(MemoryKeys::new());
        let store = Arc::new(MemoryErasureStore::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let participants = ["a-failing", "b-healthy", "c-failing"]
            .into_iter()
            .map(|name| {
                let calls = calls.clone();
                let cleanup: ErasureCleanup = Arc::new(move |_| {
                    lock(&calls).push(name);
                    if name == "b-healthy" {
                        Ok(())
                    } else {
                        Err(Error::store(name))
                    }
                });
                (name.into(), cleanup)
            })
            .collect();
        let coordinator = ErasureCoordinator::new(keys.clone(), store.clone(), participants);
        for expected_calls in [
            vec!["a-failing", "b-healthy", "c-failing"],
            vec!["a-failing", "b-healthy", "c-failing", "a-failing", "c-failing"],
        ] {
            let error = coordinator.erase(&scope(), "request").unwrap_err();
            assert_eq!(error.code, ErrorCode::Store);
            assert_eq!(error.message, "a-failing");
            assert_eq!(*lock(&calls), expected_calls);
            let operation = store.get(&scope(), "request").unwrap().unwrap().operation;
            assert_eq!(operation.acknowledgements(), &BTreeSet::from(["b-healthy".into()]));
            assert_eq!(operation.stage(), ErasureStage::Notified);
            assert_eq!(keys.status(&scope()).unwrap(), KeyStatus::Revoking);
        }
    }

    #[test]
    fn omitted_registered_participant_is_refused_before_revocation() {
        let keys = Arc::new(MemoryKeys::new());
        let store = Arc::new(MemoryErasureStore::new());
        let cleanup: ErasureCleanup = Arc::new(|_| Ok(()));
        let coordinator = ErasureCoordinator::new(
            keys.clone(),
            store.clone(),
            BTreeMap::from([("database".into(), cleanup)]),
        )
        .with_required_participants(BTreeSet::new());
        assert_eq!(
            coordinator.erase(&scope(), "request").unwrap_err().code,
            ErrorCode::BadHandle
        );
        assert!(store.get(&scope(), "request").unwrap().is_none());
        store.create(candidate("operation")).unwrap();
        assert_eq!(
            coordinator.resume(&scope(), "request").unwrap_err().code,
            ErrorCode::BadHandle
        );
        assert_eq!(keys.status(&scope()).unwrap(), KeyStatus::Unknown);
        assert_eq!(
            store.get(&scope(), "request").unwrap().unwrap().operation.stage(),
            ErasureStage::Persisted
        );
    }

    #[test]
    fn cas_outbox_and_frozen_participants() {
        let store = MemoryErasureStore::new();
        let first = store.create(candidate("operation")).unwrap();
        assert_eq!(store.create(candidate("retry")).unwrap(), first);
        assert!(store.advance(&first, "notify", None).is_err());
        let revoked = store.advance(&first, "revoke", None).unwrap();
        assert_eq!(
            store.advance(&first, "revoke", None).unwrap_err().code,
            ErrorCode::Concurrency
        );
        assert!(store.unpublished(100).unwrap().is_empty());
        let notified = store.advance(&revoked, "notify", None).unwrap();
        assert_eq!(store.unpublished(100).unwrap().len(), 1);
        assert!(store.advance(&notified, "destroy", None).is_err());
        assert!(store.advance(&notified, "acknowledge", Some("unknown")).is_err());
        let changed = ErasureOperation::new("different".into(), "request".into(), scope(), BTreeSet::new()).unwrap();
        assert_eq!(store.create(changed).unwrap_err().code, ErrorCode::Rejected);
    }

    #[test]
    fn offline_participant_requires_ack_and_notifications_remain_after_completion() {
        let keys = Arc::new(MemoryKeys::new());
        let store = Arc::new(MemoryErasureStore::new());
        let cleanup: ErasureCleanup = Arc::new(|_| Ok(()));
        let coordinator = ErasureCoordinator::new(
            keys.clone(),
            store.clone(),
            BTreeMap::from([("database".into(), cleanup)]),
        )
        .with_required_participants(BTreeSet::from(["database".into(), "offline".into()]));
        let pending = coordinator.erase(&scope(), "request").unwrap();
        assert_eq!(pending.stage(), ErasureStage::Notified);
        assert_eq!(keys.status(&scope()).unwrap(), KeyStatus::Revoking);
        coordinator.acknowledge(&scope(), "request", "offline").unwrap();
        let completed = coordinator.resume(&scope(), "request").unwrap();
        assert_eq!(completed.stage(), ErasureStage::Complete);
        assert_eq!(keys.status(&scope()).unwrap(), KeyStatus::Destroyed);
        assert!(store.pending(100).unwrap().is_empty());
        assert_eq!(store.unpublished(100).unwrap().len(), 1);
        store.mark_published(completed.operation_id()).unwrap();
        assert_eq!(coordinator.erase(&scope(), "request").unwrap(), completed);
        assert!(store.unpublished(100).unwrap().is_empty());
    }

    struct LostResponse {
        store: MemoryErasureStore,
        action: &'static str,
        fired: Mutex<bool>,
    }
    impl ErasureStore for LostResponse {
        fn create(&self, candidate: ErasureOperation) -> Result<StoredErasure, Error> {
            self.store.create(candidate)
        }
        fn get(&self, scope: &KeyScope, request: &str) -> Result<Option<StoredErasure>, Error> {
            self.store.get(scope, request)
        }
        fn get_by_id(&self, id: &str) -> Result<Option<StoredErasure>, Error> {
            self.store.get_by_id(id)
        }
        fn advance(
            &self,
            previous: &StoredErasure,
            action: &str,
            participant: Option<&str>,
        ) -> Result<StoredErasure, Error> {
            let next = self.store.advance(previous, action, participant)?;
            let mut fired = lock(&self.fired);
            if action == self.action && !*fired {
                *fired = true;
                return Err(Error::new(ErrorCode::Store, "committed response lost"));
            }
            Ok(next)
        }
        fn pending_after(&self, limit: usize, after: Option<&str>) -> Result<Vec<StoredErasure>, Error> {
            self.store.pending_after(limit, after)
        }
        fn unpublished(&self, limit: usize) -> Result<Vec<ErasureNotification>, Error> {
            self.store.unpublished(limit)
        }
        fn mark_published(&self, id: &str) -> Result<(), Error> {
            self.store.mark_published(id)
        }
    }
    #[test]
    fn resumes_every_committed_stage_without_notification_duplicates() {
        for action in ["revoke", "notify", "acknowledge", "destroy", "complete"] {
            let keys = Arc::new(MemoryKeys::new());
            let store = Arc::new(LostResponse {
                store: MemoryErasureStore::new(),
                action,
                fired: Mutex::new(false),
            });
            let cleanup: ErasureCleanup = Arc::new(|_| Ok(()));
            let coordinator =
                ErasureCoordinator::new(keys, store.clone(), BTreeMap::from([("database".into(), cleanup)]));
            assert_eq!(
                coordinator.erase(&scope(), "request").unwrap_err().code,
                ErrorCode::Store
            );
            assert_eq!(
                coordinator.resume(&scope(), "request").unwrap().stage(),
                ErasureStage::Complete
            );
            assert_eq!(store.unpublished(100).unwrap().len(), 1);
        }
    }
    #[test]
    fn participant_concurrency_errors_are_not_workflow_retries() {
        let store = Arc::new(MemoryErasureStore::new());
        let cleanup: ErasureCleanup = Arc::new(|_| Err(Error::new(ErrorCode::Concurrency, "cleanup failed")));
        let coordinator = ErasureCoordinator::new(
            Arc::new(MemoryKeys::new()),
            store.clone(),
            BTreeMap::from([("database".into(), cleanup)]),
        );
        assert_eq!(
            coordinator.erase(&scope(), "request").unwrap_err().code,
            ErrorCode::Concurrency
        );
        let row = store.get(&scope(), "request").unwrap().unwrap();
        assert_eq!(row.operation.stage(), ErasureStage::Notified);
        assert!(row.operation.acknowledgements().is_empty());
    }
}

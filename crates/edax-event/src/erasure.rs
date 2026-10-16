//! Durable erasure decisions. The host persists the initial operation before revoking keys and
//! persists each confirmation after the corresponding effect succeeds. Notification confirmation
//! and insertion of its stable-ID control message into the outbox belong in one transaction.
//!
//! A participant acknowledgement means its destination fences are installed, prior work drained,
//! and plaintext cleanup finished. The kernel cannot verify that external evidence. The host must
//! serialize concurrent updates, including acknowledgements, with a lock or compare-and-set.

use std::collections::BTreeSet;

use edax_core::Error;
use edax_seal::KeyScope;
use serde::{Deserialize, Deserializer, Serialize};

edax_core::closed_names! {
    /// Confirmed progress; a requested or failed external effect never advances the stage.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ErasureStage {
        /// The operation must be stored before its first external effect.
        Persisted = "persisted",
        /// The provider confirmed permanent revocation of the scope.
        Revoked = "revoked",
        /// The control notification was durably enqueued.
        Notified = "notified",
        /// Every participant acknowledged cleanup and all key generations were destroyed.
        DestructionConfirmed = "destruction_confirmed",
        /// The erasure operation finished within its frozen participant set.
        Complete = "complete",
    }
}

edax_core::closed_names! {
    /// Confirmed workflow effects accepted by the erasure state machine.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ErasureAction {
        /// Confirm the provider's permanent revocation fence.
        Revoke = "revoke",
        /// Confirm the durable control notification enqueue.
        Notify = "notify",
        /// Confirm a required participant's cleanup.
        Acknowledge = "acknowledge",
        /// Confirm destruction of all scope key generations.
        Destroy = "destroy",
        /// Persist final completion.
        Complete = "complete",
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ErasureRecord {
    operation_id: String,
    request_id: String,
    scope: KeyScope,
    required_participants: BTreeSet<String>,
    acknowledgements: BTreeSet<String>,
    stage: ErasureStage,
}

/// Serializable operation with immutable scope, identifiers and required participants.
///
/// The operation ID also identifies its control message across retries. A durable operation store
/// must bind `(scope, request_id)` to one operation and refuse conflicting reuse of either identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ErasureOperation(ErasureRecord);

impl<'de> Deserialize<'de> for ErasureOperation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let operation = Self(ErasureRecord::deserialize(deserializer)?);
        operation.check().map_err(serde::de::Error::custom)?;
        Ok(operation)
    }
}

impl ErasureOperation {
    /// Prepare the immutable operation to persist before requesting revocation.
    pub fn new(
        operation_id: String,
        request_id: String,
        scope: KeyScope,
        required_participants: BTreeSet<String>,
    ) -> Result<Self, Error> {
        let operation = Self(ErasureRecord {
            operation_id,
            request_id,
            scope,
            required_participants,
            acknowledgements: BTreeSet::new(),
            stage: ErasureStage::Persisted,
        });
        operation.check()?;
        Ok(operation)
    }

    /// Stable operation and control-message identity.
    pub fn operation_id(&self) -> &str {
        &self.0.operation_id
    }

    /// Caller-supplied idempotency identity within the scope.
    pub fn request_id(&self) -> &str {
        &self.0.request_id
    }

    /// Erased scope; an absent subject revokes the tenant and all its subjects.
    pub fn scope(&self) -> &KeyScope {
        &self.0.scope
    }

    /// Required participants, frozen when the operation was created.
    pub fn required_participants(&self) -> &BTreeSet<String> {
        &self.0.required_participants
    }

    /// Participants whose fencing, draining and cleanup were confirmed.
    pub fn acknowledgements(&self) -> &BTreeSet<String> {
        &self.0.acknowledgements
    }

    /// Last confirmed stage.
    pub fn stage(&self) -> ErasureStage {
        self.0.stage
    }

    /// Record successful permanent revocation. Repeated confirmations do not move backwards.
    pub fn confirm_revocation(&mut self) -> Result<(), Error> {
        self.advance(ErasureStage::Persisted, ErasureStage::Revoked)
    }

    /// Record durable notification enqueue after revocation, atomically with its outbox row.
    pub fn confirm_notification(&mut self) -> Result<(), Error> {
        self.advance(ErasureStage::Revoked, ErasureStage::Notified)
    }

    /// Record one required participant's completed cleanup. Duplicate acknowledgements are harmless.
    pub fn acknowledge(&mut self, participant: &str) -> Result<(), Error> {
        if !self.0.required_participants.contains(participant) {
            return Err(Error::bad_handle("unknown erasure participant"));
        }
        if self.0.stage < ErasureStage::Notified {
            return Err(Error::bad_handle(
                "erasure cleanup acknowledgement precedes notification",
            ));
        }
        self.0.acknowledgements.insert(participant.to_owned());
        Ok(())
    }

    /// Whether the host may request destruction now. An offline participant keeps this false.
    pub fn can_destroy(&self) -> bool {
        self.0.stage == ErasureStage::Notified && self.cleanup_complete()
    }

    /// Record confirmed destruction of every encryption and fingerprint generation, never a request.
    pub fn confirm_destruction(&mut self) -> Result<(), Error> {
        if !self.cleanup_complete() {
            return Err(Error::bad_handle("erasure participants have not completed cleanup"));
        }
        self.advance(ErasureStage::Notified, ErasureStage::DestructionConfirmed)
    }

    /// Complete only after provider destruction confirmation and all participant acknowledgements.
    pub fn complete(&mut self) -> Result<(), Error> {
        self.advance(ErasureStage::DestructionConfirmed, ErasureStage::Complete)
    }

    fn cleanup_complete(&self) -> bool {
        self.0.acknowledgements == self.0.required_participants
    }

    fn advance(&mut self, required: ErasureStage, next: ErasureStage) -> Result<(), Error> {
        if self.0.stage < required {
            return Err(Error::bad_handle("erasure confirmation precedes its required stage"));
        }
        if self.0.stage < next {
            self.0.stage = next;
        }
        Ok(())
    }

    fn check(&self) -> Result<(), Error> {
        self.0.scope.check()?;
        if self.0.operation_id.is_empty() || self.0.request_id.is_empty() {
            return Err(Error::codec(
                "erasure operation and request identifiers must be nonempty",
            ));
        }
        if self.0.required_participants.iter().any(String::is_empty) {
            return Err(Error::codec("erasure participant identifiers must be nonempty"));
        }
        if !self.0.acknowledgements.is_subset(&self.0.required_participants) {
            return Err(Error::codec("unknown erasure participant acknowledgement"));
        }
        if self.0.stage < ErasureStage::Notified && !self.0.acknowledgements.is_empty() {
            return Err(Error::codec("erasure cleanup acknowledgement precedes notification"));
        }
        if self.0.stage >= ErasureStage::DestructionConfirmed && !self.cleanup_complete() {
            return Err(Error::codec("erasure destruction precedes participant cleanup"));
        }
        Ok(())
    }
}

/// Provider effects advance only after the provider confirms the requested durable lifecycle state.
pub fn provider_confirmed(action: &str, status: &str) -> Result<bool, Error> {
    let action = ErasureAction::by_name(action)
        .filter(|action| matches!(action, ErasureAction::Revoke | ErasureAction::Destroy))
        .ok_or_else(|| Error::bad_handle("unknown erasure provider action"))?;
    if edax_seal::KeyStatus::by_name(status).is_none() {
        super::sealing::check_key_status(status)?;
    }
    match action {
        ErasureAction::Revoke => Ok(matches!(status, "revoking" | "destroyed")),
        ErasureAction::Destroy => Ok(status == "destroyed"),
        _ => Err(Error::bad_handle("unknown erasure provider action")),
    }
}

/// A repeated request must preserve its scope and frozen cleanup participant set.
pub fn check_request(existing: &ErasureOperation, requested: &ErasureOperation) -> Result<(), Error> {
    if existing.scope() != requested.scope()
        || existing.request_id() != requested.request_id()
        || existing.required_participants() != requested.required_participants()
    {
        return Err(Error::new(
            edax_core::ErrorCode::Rejected,
            "erasure request conflicts with its stored operation",
        ));
    }
    Ok(())
}

/// Apply a confirmed durable effect. Hosts serialize concurrent acknowledgements with CAS.
pub fn apply(
    mut operation: ErasureOperation,
    action: &str,
    participant: Option<&str>,
) -> Result<ErasureOperation, Error> {
    let action = ErasureAction::by_name(action).ok_or_else(|| Error::bad_handle("unknown erasure workflow action"))?;
    match action {
        ErasureAction::Revoke => operation.confirm_revocation()?,
        ErasureAction::Notify => operation.confirm_notification()?,
        ErasureAction::Acknowledge => operation
            .acknowledge(participant.ok_or_else(|| Error::bad_handle("acknowledgement requires participant"))?)?,
        ErasureAction::Destroy => operation.confirm_destruction()?,
        ErasureAction::Complete => operation.complete()?,
    }
    Ok(operation)
}

/// Validate a persisted transition and build its control notification, without personal payload.
pub fn notification(previous: &ErasureOperation, next: &ErasureOperation) -> Result<Option<String>, Error> {
    check_request(previous, next)?;
    if previous.operation_id() != next.operation_id() || !previous.acknowledgements().is_subset(next.acknowledgements())
    {
        return Err(Error::bad_handle(
            "erasure update changes immutable identity or removes acknowledgements",
        ));
    }
    let from = previous.stage();
    let to = next.stage();
    if from != to
        && !matches!(
            (from, to),
            (ErasureStage::Persisted, ErasureStage::Revoked)
                | (ErasureStage::Revoked, ErasureStage::Notified)
                | (ErasureStage::Notified, ErasureStage::DestructionConfirmed)
                | (ErasureStage::DestructionConfirmed, ErasureStage::Complete)
        )
    {
        return Err(Error::bad_handle("erasure update skips or reverses a durable stage"));
    }
    Ok(if from == ErasureStage::Revoked && to == ErasureStage::Notified {
        Some(serde_json::json!({"type":"edax.erasure.requested", "operation_id":next.operation_id(), "scope":next.scope()}).to_string())
    } else {
        None
    })
}

/// Encode a validated operation without delegating its persisted representation to an SDK.
pub fn encode_operation(operation: &ErasureOperation) -> Result<String, Error> {
    operation.check()?;
    serde_json::to_string(operation).map_err(|_| Error::codec("cannot encode erasure operation"))
}

/// Decode and validate durable state without echoing untrusted record contents in an error.
pub fn decode_operation(record: &str) -> Result<ErasureOperation, Error> {
    serde_json::from_str(record).map_err(|_| Error::codec("invalid erasure operation"))
}

/// Every locally registered participant must be required; missing local callbacks may run remotely.
pub fn validate_participants(required: &BTreeSet<String>, registered: &BTreeSet<String>) -> Result<(), Error> {
    if required.iter().chain(registered).any(String::is_empty) {
        return Err(Error::bad_handle("erasure participant identifiers must be nonempty"));
    }
    if !registered.is_subset(required) {
        return Err(Error::bad_handle("registered erasure participant is not required"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use edax_core::ErrorCode;
    use serde_json::json;

    fn operation() -> ErasureOperation {
        ErasureOperation::new(
            "erase-42".into(),
            "request-42".into(),
            KeyScope {
                key_namespace: "production".into(),
                tenant: Some("clinic-7".into()),
                subject: Some("patient-42".into()),
            },
            BTreeSet::from(["patient-search".into()]),
        )
        .unwrap()
    }

    #[test]
    fn workflow_misuse_is_bad_handle_and_closed_names_cover_every_transition() {
        let original = operation();
        for (action, participant) in [
            ("unknown", None),
            ("notify", None),
            ("destroy", None),
            ("complete", None),
            ("acknowledge", None),
            ("acknowledge", Some("unknown")),
            ("acknowledge", Some("patient-search")),
        ] {
            assert_eq!(
                apply(original.clone(), action, participant).unwrap_err().code,
                ErrorCode::BadHandle
            );
        }
        let mut op = original.clone();
        for action in ErasureAction::ALL {
            assert_eq!(ErasureAction::by_name(action.as_str()), Some(*action));
            let previous = op.clone();
            op = apply(op, action.as_str(), Some("patient-search")).unwrap();
            assert_eq!(decode_operation(&encode_operation(&op).unwrap()).unwrap(), op);
            assert_eq!(
                notification(&previous, &op).unwrap().is_some(),
                *action == ErasureAction::Notify
            );
        }
        assert_eq!(op.stage(), ErasureStage::Complete);
        assert_eq!(notification(&original, &op).unwrap_err().code, ErrorCode::BadHandle);
        for stage in ErasureStage::ALL {
            assert_eq!(ErasureStage::by_name(stage.as_str()), Some(*stage));
            assert_eq!(serde_json::to_value(stage).unwrap(), stage.as_str());
        }
        for action in ["unknown", "notify", "acknowledge", "complete"] {
            assert_eq!(
                provider_confirmed(action, "active").unwrap_err().code,
                ErrorCode::BadHandle
            );
        }
        assert!(provider_confirmed("revoke", "revoking").unwrap());
        assert!(!provider_confirmed("destroy", "revoking").unwrap());
        assert!(provider_confirmed("destroy", "destroyed").unwrap());
    }

    #[test]
    fn malformed_persisted_operations_remain_codec_errors() {
        let valid = serde_json::to_value(operation()).unwrap();
        for (field, value) in [
            ("operation_id", json!("")),
            ("request_id", json!("")),
            ("stage", json!("unknown")),
            ("stage", json!("complete")),
            ("acknowledgements", json!(["unknown"])),
            ("acknowledgements", json!(["patient-search"])),
            ("required_participants", json!([""])),
        ] {
            let mut record = valid.clone();
            record[field] = value;
            assert_eq!(
                decode_operation(&record.to_string()).unwrap_err().code,
                ErrorCode::Codec,
                "{field}"
            );
        }
        let error = decode_operation("private malformed record").unwrap_err();
        assert_eq!(error.code, ErrorCode::Codec);
        assert_eq!(error.message, "invalid erasure operation");
    }

    #[test]
    fn participant_configuration_requires_registered_callbacks_but_allows_remote_workers() {
        for (required, registered, allowed) in [
            (vec![], vec![], true),
            (vec!["search"], vec!["search"], true),
            (vec!["search", "archive"], vec!["search"], true),
            (vec!["archive"], vec![], true),
            (vec![], vec!["search"], false),
            (vec!["archive"], vec!["search"], false),
            (vec![""], vec![], false),
            (vec!["search"], vec![""], false),
        ] {
            let required = required.into_iter().map(str::to_owned).collect();
            let registered = registered.into_iter().map(str::to_owned).collect();
            let result = validate_participants(&required, &registered);
            if allowed {
                result.unwrap();
            } else {
                assert_eq!(result.unwrap_err().code, ErrorCode::BadHandle);
            }
        }
    }
}

//! Authenticated message context. Producer-assigned fields are immutable after sealing.

use crate::{CommandMessage, EventDraft, EventMessage};
use edax_core::Error;
use edax_seal::{Aad, EncryptionInfo, KeyScope};

/// Validate the protected envelope on both sides of every codec boundary.
pub fn check_protection(subject: Option<&str>, info: Option<&EncryptionInfo>, data: &[u8]) -> Result<(), Error> {
    if let Some(subject) = subject {
        edax_core::check_subject(subject)?;
    }
    if let Some(info) = info {
        info.check()?;
        if subject.is_none() || data.len() < edax_seal::TAG_LEN {
            return Err(Error::codec(
                "encrypted payload requires a subject and authentication tag",
            ));
        }
    }
    Ok(())
}

/// Check the expected security boundary before consulting a configured provider.
pub fn message_scope(
    namespace: &str,
    tenant: Option<&str>,
    subject: Option<&str>,
    info: &EncryptionInfo,
) -> Result<KeyScope, Error> {
    info.check()?;
    if info.key_namespace != namespace {
        return Err(Error::codec("encryption namespace does not match configured namespace"));
    }
    protected_scope(namespace, tenant, subject)
}

/// Reject changing an owner once a protected stream or saga has committed.
pub fn check_owner(committed: bool, owner: Option<&str>, proposed: Option<&str>) -> Result<(), Error> {
    if let Some(subject) = proposed {
        edax_core::check_subject(subject)?;
    }
    if committed && owner != proposed {
        return Err(Error::codec("protected state owner is immutable"));
    }
    Ok(())
}

/// Exact version-one event context. `recorded_at` is deliberately excluded.
pub fn event_context(message: &EventMessage) -> Result<Vec<u8>, Error> {
    let info = message
        .encryption
        .as_ref()
        .ok_or_else(|| Error::codec("event is not encrypted"))?;
    info.check()?;
    let mut aad = Aad::new("edax.message.v1");
    aad.str("event")
        .encryption(info)
        .str(&message.id)
        .optional(message.tenant.as_deref())
        .optional(message.subject.as_deref())
        .str(&message.aggregate_id)
        .str(&message.name)
        .u32(message.version)
        .optional_u64(message.sequence)
        .str(&message.correlation_id)
        .str(&message.causation_id)
        .optional_i64(message.occurred_at)
        .map(&message.extensions)
        .map(&message.metadata);
    Ok(aad.finish())
}

/// Exact version-one command context, including intent identity and issuer time.
pub fn command_context(message: &CommandMessage) -> Result<Vec<u8>, Error> {
    let info = message
        .encryption
        .as_ref()
        .ok_or_else(|| Error::codec("command is not encrypted"))?;
    info.check()?;
    let mut aad = Aad::new("edax.message.v1");
    aad.str("command")
        .encryption(info)
        .str(&message.id)
        .optional(message.tenant.as_deref())
        .optional(message.subject.as_deref())
        .optional(message.aggregate_id.as_deref())
        .str(&message.name)
        .u32(message.version)
        .optional(message.idempotency_key.as_deref())
        .str(&message.correlation_id)
        .str(&message.causation_id)
        .i64(message.issued_at)
        .map(&message.extensions)
        .map(&message.metadata);
    Ok(aad.finish())
}

/// Seal a prepared draft once. The returned ciphertext is retained across write retries.
pub fn seal_event(mut draft: EventDraft, info: EncryptionInfo, key: &[u8]) -> Result<EventDraft, Error> {
    if draft.encryption.is_some() {
        return Err(Error::codec("event is already sealed"));
    }
    draft.encryption = Some(info.clone());
    let context = event_context(&draft.clone().record(0))?;
    message_scope(
        &info.key_namespace,
        draft.tenant.as_deref(),
        draft.subject.as_deref(),
        &info,
    )?;
    draft.data = edax_seal::seal(&info, key, &context, &draft.data)?;
    Ok(draft)
}

/// Authenticate and return payload bytes without modifying the raw envelope.
pub fn open_event(message: &EventMessage, key: &[u8]) -> Result<Vec<u8>, Error> {
    let context = event_context(message)?;
    let info = message
        .encryption
        .as_ref()
        .ok_or_else(|| Error::codec("event is not encrypted"))?;
    check_protection(message.subject.as_deref(), Some(info), &message.data)?;
    edax_seal::open(info, key, &context, &message.data)
}

/// Seal a command after all routing and producer context has been finalized.
pub fn seal_command(mut message: CommandMessage, info: EncryptionInfo, key: &[u8]) -> Result<CommandMessage, Error> {
    if message.encryption.is_some() {
        return Err(Error::codec("command is already sealed"));
    }
    message.encryption = Some(info.clone());
    let context = command_context(&message)?;
    message_scope(
        &info.key_namespace,
        message.tenant.as_deref(),
        message.subject.as_deref(),
        &info,
    )?;
    message.data = edax_seal::seal(&info, key, &context, &message.data)?;
    Ok(message)
}

/// Authenticate a command before payload decoding or upcasting.
pub fn open_command(message: &CommandMessage, key: &[u8]) -> Result<Vec<u8>, Error> {
    let context = command_context(message)?;
    let info = message
        .encryption
        .as_ref()
        .ok_or_else(|| Error::codec("command is not encrypted"))?;
    check_protection(message.subject.as_deref(), Some(info), &message.data)?;
    edax_seal::open(info, key, &context, &message.data)
}

/// Record-specific context for stored state: snapshots, saga application state, and the private parts
/// of ingestion rows, projection state and projection intents. Those three bind their storage key as
/// `identity`, which carries the generation, tenant and subject. Processing marks are public but
/// authenticated; completion and state version cannot be altered independently of the state.
#[allow(clippy::too_many_arguments)]
pub fn state_context(
    kind: &str,
    identity: &str,
    type_name: &str,
    version: u64,
    tenant: Option<&str>,
    subject: Option<&str>,
    marks: &str,
    completed: bool,
    info: &EncryptionInfo,
) -> Result<Vec<u8>, Error> {
    if !matches!(kind, "snapshot" | "saga" | "ingestion" | "projection" | "intent") {
        return Err(Error::codec("unknown protected state kind"));
    }
    message_scope(&info.key_namespace, tenant, subject, info)?;
    let mut aad = Aad::new("edax.state.v1");
    aad.str(kind)
        .encryption(info)
        .str(identity)
        .str(type_name)
        .u64(version)
        .optional(tenant)
        .optional(subject)
        .str(marks)
        .u32(u32::from(completed));
    Ok(aad.finish())
}

/// Explicit migration gate: absence of encryption never disables protected-reader policy.
pub fn check_plaintext(allow_plaintext: bool) -> Result<(), Error> {
    if allow_plaintext {
        Ok(())
    } else {
        Err(Error::codec("plaintext is forbidden by sealing policy"))
    }
}

/// Interpret a provider lifecycle status without confusing absence with destruction.
pub fn check_key_status(status: &str) -> Result<(), Error> {
    use edax_core::ErrorCode;
    let code = match status {
        "active" => return Ok(()),
        "unknown" => ErrorCode::KeyMissing,
        "destroyed" => ErrorCode::Shredded,
        "revoking" => ErrorCode::KeyRevoking,
        _ => return Err(Error::bad_handle("unknown provider lifecycle status")),
    };
    Err(Error::new(code, "key scope is not active"))
}

/// New writes may create a scope, but may never revive a revoked one.
pub fn check_key_write_status(status: &str) -> Result<(), Error> {
    if status == "unknown" {
        Ok(())
    } else {
        check_key_status(status)
    }
}

/// Validate a protected message or state owner before reserving key material.
pub fn protected_scope(namespace: &str, tenant: Option<&str>, subject: Option<&str>) -> Result<KeyScope, Error> {
    let subject = subject.ok_or_else(|| Error::codec("protected payload requires a subject"))?;
    let scope = KeyScope {
        key_namespace: namespace.to_owned(),
        tenant: tenant.map(str::to_owned),
        subject: Some(subject.to_owned()),
    };
    scope.check()?;
    Ok(scope)
}

/// Reconcile a retained prepared write after an uncertain commit. Stored time is store-assigned;
/// every producer field, identity and ciphertext must match. A partial batch is never re-sealed.
pub fn reconcile_prepared_events(drafts: &[EventDraft], stored: &[EventMessage]) -> Result<bool, Error> {
    if stored.is_empty() {
        return Ok(false);
    }
    if drafts.is_empty() || stored.len() < drafts.len() {
        return Err(Error::new(
            edax_core::ErrorCode::Concurrency,
            "prepared batch does not match stored stream",
        ));
    }
    for (draft, event) in drafts.iter().zip(stored) {
        if draft.clone().record(event.recorded_at) != *event {
            return Err(Error::new(
                edax_core::ErrorCode::Concurrency,
                "prepared batch does not match stored stream",
            ));
        }
    }
    Ok(true)
}

/// Persisted encrypted application state. The marker distinguishes it from historical host JSON.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProtectedState {
    /// Envelope marker and format version.
    pub edax_protected_state: u32,
    /// Persisted tenant.
    pub tenant: Option<String>,
    /// Immutable erasure owner.
    pub subject: Option<String>,
    /// Authenticated saga completion. Legacy protected running states omit this field.
    /// Readers must use this value in [`state_context`] and authenticate before admission.
    pub completed: bool,
    /// Public encryption context.
    pub encryption: EncryptionInfo,
    /// Ciphertext and tag; JSON base64.
    #[serde(with = "crate::codec::bytes_field")]
    pub data: Vec<u8>,
}

impl ProtectedState {
    /// Construct the one supported stored-state format in the owning kernel.
    pub fn new(
        tenant: Option<String>,
        subject: Option<String>,
        completed: bool,
        encryption: EncryptionInfo,
        data: Vec<u8>,
    ) -> Self {
        Self {
            edax_protected_state: 1,
            tenant,
            subject,
            completed,
            encryption,
            data,
        }
    }
}

/// Encode only a complete valid protected state envelope.
pub fn encode_protected_state(state: &ProtectedState) -> Result<String, Error> {
    if state.edax_protected_state != 1 {
        return Err(Error::codec("unknown protected state format"));
    }
    check_protection(state.subject.as_deref(), Some(&state.encryption), &state.data)?;
    serde_json::to_string(state).map_err(|_| Error::codec("cannot encode protected state"))
}

/// Historical host JSON has no reserved marker; a present marker must be complete and valid.
pub fn decode_protected_state(json: &str) -> Result<Option<ProtectedState>, Error> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|_| Error::codec("invalid saga state JSON"))?;
    if value.get("edax_protected_state").is_none() {
        return Ok(None);
    }
    let state: ProtectedState =
        serde_json::from_value(value).map_err(|_| Error::codec("invalid protected state envelope"))?;
    encode_protected_state(&state)?;
    Ok(Some(state))
}

#[cfg(test)]
mod protected_state_tests {
    use super::*;
    use edax_seal::Cipher;
    use serde_json::json;

    const KEY: [u8; 32] = [0x47; 32];

    fn context(state: &ProtectedState) -> Result<Vec<u8>, Error> {
        state_context(
            "saga",
            "workflow-7",
            "profile-notification",
            3,
            state.tenant.as_deref(),
            state.subject.as_deref(),
            "folded-marks",
            state.completed,
            &state.encryption,
        )
    }

    fn sealed(cipher: Cipher, completed: bool) -> Result<ProtectedState, Error> {
        let mut state = ProtectedState {
            edax_protected_state: 1,
            tenant: Some("tenant-7".into()),
            subject: Some("subject-7".into()),
            completed,
            encryption: EncryptionInfo {
                format_version: 1,
                key_namespace: "profiles".into(),
                key_id: "generation-7".into(),
                cipher,
                nonce: vec![0x24; cipher.nonce_len()],
            },
            data: Vec::new(),
        };
        state.data = edax_seal::seal(&state.encryption, &KEY, &context(&state)?, b"null")?;
        Ok(state)
    }

    #[test]
    fn completed_state_round_trips_with_owner_and_authenticated_completion() -> Result<(), Error> {
        for cipher in Cipher::ALL {
            for completed in [false, true] {
                let state = sealed(*cipher, completed)?;
                let encoded = encode_protected_state(&state)?;
                let mut decoded = decode_protected_state(&encoded)?
                    .ok_or_else(|| Error::codec("protected state was decoded as legacy JSON"))?;
                assert_eq!(decoded.completed, completed);
                assert_eq!(decoded.tenant, state.tenant);
                assert_eq!(decoded.subject, state.subject);
                assert_eq!(
                    edax_seal::open(&decoded.encryption, &KEY, &context(&decoded)?, &decoded.data)?,
                    b"null",
                );
                decoded.completed = !completed;
                assert!(edax_seal::open(&decoded.encryption, &KEY, &context(&decoded)?, &decoded.data).is_err());
            }
        }
        Ok(())
    }

    #[test]
    fn omitted_completion_is_a_malformed_protected_record() -> Result<(), Error> {
        let state = sealed(Cipher::XChaCha20Poly1305, false)?;
        let mut wire = serde_json::to_value(&state).map_err(Error::codec)?;
        if let serde_json::Value::Object(fields) = &mut wire {
            fields.remove("completed");
        }
        assert!(decode_protected_state(&wire.to_string()).is_err());
        Ok(())
    }

    #[test]
    fn malformed_completion_flags_never_downgrade_to_legacy_state() -> Result<(), Error> {
        let state = sealed(Cipher::XChaCha20Poly1305, true)?;
        for invalid in [json!(null), json!(0), json!(1), json!("true"), json!([]), json!({})] {
            let mut wire = serde_json::to_value(&state).map_err(Error::codec)?;
            if let serde_json::Value::Object(fields) = &mut wire {
                fields.insert("completed".into(), invalid);
            }
            let result = decode_protected_state(&wire.to_string());
            assert!(result.is_err(), "malformed completion was accepted: {wire}");
        }
        Ok(())
    }
}

//! Synchronous conversion surface for authenticated encryption. Provider I/O stays in the SDKs.

use crate::{names, to_wire, CommandMessage, EventDraft, EventMessage};
use std::sync::Mutex;

/// Encryption framing shared by messages and persisted state; contains no secret key material.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct EncryptionInfo {
    pub format_version: u32,
    pub key_namespace: String,
    pub key_id: String,
    pub cipher: String,
    pub nonce: Vec<u8>,
}

impl TryFrom<EncryptionInfo> for edax_seal::EncryptionInfo {
    type Error = edax_core::Error;

    fn try_from(info: EncryptionInfo) -> Result<Self, Self::Error> {
        let info = Self {
            format_version: info.format_version,
            key_namespace: info.key_namespace,
            key_id: info.key_id,
            cipher: edax_seal::Cipher::from_name(&info.cipher)?,
            nonce: info.nonce,
        };
        info.check()?;
        Ok(info)
    }
}

impl From<edax_seal::EncryptionInfo> for EncryptionInfo {
    fn from(info: edax_seal::EncryptionInfo) -> Self {
        Self {
            format_version: info.format_version,
            key_namespace: info.key_namespace,
            key_id: info.key_id,
            cipher: info.cipher.as_str().to_owned(),
            nonce: info.nonce,
        }
    }
}

/// Provider scope, independent of storage-tenancy key composition.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KeyScope {
    pub key_namespace: String,
    pub tenant: Option<String>,
    pub subject: Option<String>,
}

impl TryFrom<KeyScope> for edax_seal::KeyScope {
    type Error = edax_core::Error;

    fn try_from(scope: KeyScope) -> Result<Self, Self::Error> {
        let scope = Self {
            key_namespace: scope.key_namespace,
            tenant: scope.tenant,
            subject: scope.subject,
        };
        scope.check()?;
        Ok(scope)
    }
}

impl From<edax_seal::KeyScope> for KeyScope {
    fn from(scope: edax_seal::KeyScope) -> Self {
        Self {
            key_namespace: scope.key_namespace,
            tenant: scope.tenant,
            subject: scope.subject,
        }
    }
}

/// Reusable encrypted stored value; record-specific context is supplied by its owning kernel.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SealedValue {
    pub encryption: EncryptionInfo,
    pub data: Vec<u8>,
}

/// Validate a provider scope without creating or resolving a key.
pub fn check_key_scope(scope: KeyScope) -> Result<(), String> {
    edax_seal::KeyScope::try_from(scope).map(|_| ()).map_err(to_wire)
}

/// Injective, versioned encoding shared by every key provider.
pub fn encode_key_scope(scope: KeyScope) -> Result<Vec<u8>, String> {
    edax_seal::KeyScope::try_from(scope)
        .and_then(|scope| scope.encode())
        .map_err(to_wire)
}

/// Cipher names a host compares against its own enum.
pub fn encryption_ciphers() -> Vec<String> {
    names(edax_seal::Cipher::NAMES)
}

/// OS-random nonce of the required algorithm-specific size.
pub fn encryption_nonce(cipher: &str) -> Result<Vec<u8>, String> {
    edax_seal::Cipher::from_name(cipher)
        .and_then(edax_seal::new_nonce)
        .map_err(to_wire)
}

/// Low-level encryption with kernel-constructed record context. Reserve usage before calling.
pub fn seal_bytes(info: EncryptionInfo, key: &[u8], context: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, String> {
    let info = edax_seal::EncryptionInfo::try_from(info).map_err(to_wire)?;
    edax_seal::seal(&info, key, context, plaintext).map_err(to_wire)
}

/// Low-level authenticated opening; returns no plaintext until the full tag validates.
pub fn open_bytes(info: EncryptionInfo, key: &[u8], context: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, String> {
    let info = edax_seal::EncryptionInfo::try_from(info).map_err(to_wire)?;
    edax_seal::open(&info, key, context, ciphertext).map_err(to_wire)
}

/// The exact event context; recorded time is excluded by the event kernel.
pub fn event_encryption_context(message: EventMessage) -> Result<Vec<u8>, String> {
    let message = edax_event::EventMessage::try_from(message).map_err(to_wire)?;
    edax_event::sealing::event_context(&message).map_err(to_wire)
}

/// The exact command context, including intent identity and issuer time.
pub fn command_encryption_context(message: CommandMessage) -> Result<Vec<u8>, String> {
    let message = edax_event::CommandMessage::try_from(message).map_err(to_wire)?;
    edax_event::sealing::command_context(&message).map_err(to_wire)
}

/// Bind a stored value to its identity, owner, schema, version, and saga processing marks.
#[allow(clippy::too_many_arguments)] // the kernel's record context spread flat for alef
pub fn state_encryption_context(
    kind: &str,
    identity: &str,
    type_name: &str,
    version: u64,
    marks: &str,
    completed: bool,
    info: EncryptionInfo,
    tenant: Option<String>,
    subject: Option<String>,
) -> Result<Vec<u8>, String> {
    let info = edax_seal::EncryptionInfo::try_from(info).map_err(to_wire)?;
    edax_event::sealing::state_context(
        kind,
        identity,
        type_name,
        version,
        tenant.as_deref(),
        subject.as_deref(),
        marks,
        completed,
        &info,
    )
    .map_err(to_wire)
}

/// Seal a prepared draft once; retain its ciphertext across ambiguous commit retries.
pub fn seal_event(draft: EventDraft, info: EncryptionInfo, key: &[u8]) -> Result<EventDraft, String> {
    let draft = edax_event::EventDraft::try_from(draft).map_err(to_wire)?;
    let info = edax_seal::EncryptionInfo::try_from(info).map_err(to_wire)?;
    edax_event::sealing::seal_event(draft, info, key)
        .map(EventDraft::from)
        .map_err(to_wire)
}

/// Authenticate an event without changing the original raw envelope.
pub fn open_event(message: EventMessage, key: &[u8]) -> Result<Vec<u8>, String> {
    let message = edax_event::EventMessage::try_from(message).map_err(to_wire)?;
    edax_event::sealing::open_event(&message, key).map_err(to_wire)
}

/// Seal a command after every authenticated producer field has been finalized.
pub fn seal_command(message: CommandMessage, info: EncryptionInfo, key: &[u8]) -> Result<CommandMessage, String> {
    let message = edax_event::CommandMessage::try_from(message).map_err(to_wire)?;
    let info = edax_seal::EncryptionInfo::try_from(info).map_err(to_wire)?;
    edax_event::sealing::seal_command(message, info, key)
        .map(CommandMessage::from)
        .map_err(to_wire)
}

/// Authenticate a command before payload decoding, upcasting, or result replay.
pub fn open_command(message: CommandMessage, key: &[u8]) -> Result<Vec<u8>, String> {
    let message = edax_event::CommandMessage::try_from(message).map_err(to_wire)?;
    edax_event::sealing::open_command(&message, key).map_err(to_wire)
}

/// Validate the configured namespace before selecting any provider record.
pub fn encryption_scope(
    key_namespace: &str,
    info: EncryptionInfo,
    tenant: Option<String>,
    subject: Option<String>,
) -> Result<KeyScope, String> {
    let info = edax_seal::EncryptionInfo::try_from(info).map_err(to_wire)?;
    edax_event::sealing::message_scope(key_namespace, tenant.as_deref(), subject.as_deref(), &info)
        .map(KeyScope::from)
        .map_err(to_wire)
}

/// Check immutable ownership before a protected transition.
pub fn check_sealed_owner(committed: bool, owner: Option<String>, proposed: Option<String>) -> Result<(), String> {
    edax_event::sealing::check_owner(committed, owner.as_deref(), proposed.as_deref()).map_err(to_wire)
}

/// Refuse plaintext unless the reader explicitly enables the legacy-read policy.
pub fn check_plaintext_policy(allow_plaintext: bool) -> Result<(), String> {
    edax_event::sealing::check_plaintext(allow_plaintext).map_err(to_wire)
}

/// Apply the kernel's authoritative key-provider state verdict.
pub fn check_key_status(status: &str) -> Result<(), String> {
    edax_event::sealing::check_key_status(status).map_err(to_wire)
}

/// A new write may create an unknown scope; revoking and destroyed scopes remain fenced.
pub fn check_key_write_status(status: &str) -> Result<(), String> {
    edax_event::sealing::check_key_write_status(status).map_err(to_wire)
}

/// Validate the encryption write scope before reserving any provider material.
pub fn protected_scope(
    key_namespace: &str,
    tenant: Option<String>,
    subject: Option<String>,
) -> Result<KeyScope, String> {
    edax_event::sealing::protected_scope(key_namespace, tenant.as_deref(), subject.as_deref())
        .map(KeyScope::from)
        .map_err(to_wire)
}

/// Public reservation identity. Secret bytes cross separately through the byte-returning resolve APIs.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KeyReservation {
    pub key_id: String,
}

impl From<edax_seal::ReservedKey> for KeyReservation {
    fn from(reserved: edax_seal::ReservedKey) -> Self {
        Self {
            key_id: reserved.key_id,
        }
    }
}

fn key_status_name(status: edax_seal::KeyStatus) -> String {
    status.as_str().to_owned()
}

/// Lifecycle names; provider errors are separate from persisted lifecycle states.
pub fn key_statuses() -> Vec<String> {
    names(edax_seal::KeyStatus::NAMES)
}

/// Default cipher selected by the encryption protocol.
pub fn default_cipher() -> String {
    edax_seal::default_cipher().as_str().to_owned()
}

/// Complete encryption information with a fresh operating-system nonce.
pub fn fresh_encryption_info(key_namespace: String, key_id: String, cipher: &str) -> Result<EncryptionInfo, String> {
    edax_seal::EncryptionInfo::fresh(
        key_namespace,
        key_id,
        edax_seal::Cipher::from_name(cipher).map_err(to_wire)?,
    )
    .map(Into::into)
    .map_err(to_wire)
}

/// In-memory reference provider. One mutex serializes all reservations and revocations.
/// Restarting the process loses keys and tombstones; this is not a durable production provider.
pub struct MemoryKeys {
    inner: Mutex<edax_seal::MemoryKeyProvider>,
}

/// Create a process-local provider with no reusable host-side cache.
pub fn memory_keys() -> MemoryKeys {
    MemoryKeys {
        inner: Mutex::new(edax_seal::MemoryKeyProvider::new()),
    }
}

impl MemoryKeys {
    /// Reserve `count` writes under one generation before encrypting; failed writes still consume them.
    pub fn reserve(&self, scope: KeyScope, cipher: &str, count: u64) -> Result<KeyReservation, String> {
        let scope = edax_seal::KeyScope::try_from(scope).map_err(to_wire)?;
        let cipher = edax_seal::Cipher::from_name(cipher).map_err(to_wire)?;
        edax_core::lock(&self.inner)
            .reserve_many(&scope, cipher, count)
            .map(KeyReservation::from)
            .map_err(to_wire)
    }

    /// Resolve only the requested existing generation, exact scope, and encryption purpose.
    pub fn resolve(&self, scope: KeyScope, cipher: &str, key_id: &str) -> Result<Vec<u8>, String> {
        let scope = edax_seal::KeyScope::try_from(scope).map_err(to_wire)?;
        let cipher = edax_seal::Cipher::from_name(cipher).map_err(to_wire)?;
        edax_core::lock(&self.inner)
            .resolve(&scope, cipher, key_id)
            .map(|key| key.as_bytes().to_vec())
            .map_err(to_wire)
    }

    /// Reserve the stable fingerprint key, independent of every encryption generation.
    pub fn fingerprint(&self, scope: KeyScope) -> Result<KeyReservation, String> {
        let scope = edax_seal::KeyScope::try_from(scope).map_err(to_wire)?;
        edax_core::lock(&self.inner)
            .fingerprint_key(&scope)
            .map(KeyReservation::from)
            .map_err(to_wire)
    }

    /// Resolve only an existing fingerprint generation of this exact scope.
    pub fn resolve_fingerprint(&self, scope: KeyScope, key_id: &str) -> Result<Vec<u8>, String> {
        let scope = edax_seal::KeyScope::try_from(scope).map_err(to_wire)?;
        edax_core::lock(&self.inner)
            .resolve_fingerprint(&scope, key_id)
            .map(|key| key.as_bytes().to_vec())
            .map_err(to_wire)
    }

    /// Permanently reject reservations and reads while destruction is pending.
    pub fn revoke(&self, scope: KeyScope) -> Result<String, String> {
        let scope = edax_seal::KeyScope::try_from(scope).map_err(to_wire)?;
        edax_core::lock(&self.inner)
            .revoke(&scope)
            .map(key_status_name)
            .map_err(to_wire)
    }

    /// Destroy current and historical keys, retaining permanent ownership tombstones.
    pub fn destroy(&self, scope: KeyScope) -> Result<String, String> {
        let scope = edax_seal::KeyScope::try_from(scope).map_err(to_wire)?;
        edax_core::lock(&self.inner)
            .destroy(&scope)
            .map(key_status_name)
            .map_err(to_wire)
    }

    /// Read the authoritative scope state, including a tenant's revocation of future subjects.
    pub fn status(&self, scope: KeyScope) -> Result<String, String> {
        let scope = edax_seal::KeyScope::try_from(scope).map_err(to_wire)?;
        edax_core::lock(&self.inner)
            .status(&scope)
            .map(key_status_name)
            .map_err(to_wire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;

    #[test]
    fn memory_key_handle_reserves_resolves_and_preserves_revocation_verdicts() {
        let keys = memory_keys();
        let scope = KeyScope {
            key_namespace: "production-eu".into(),
            tenant: Some("bookshop".into()),
            subject: Some("customer-42".into()),
        };
        assert_eq!(keys.status(scope.clone()).unwrap(), "unknown");
        let reserved = keys.reserve(scope.clone(), "aes-256-gcm", 1).unwrap();
        assert_eq!(keys.status(scope.clone()).unwrap(), "active");
        let secret = keys.resolve(scope.clone(), "aes-256-gcm", &reserved.key_id).unwrap();
        assert_eq!(secret.len(), edax_seal::KEY_LEN);
        assert_eq!(
            serde_json::to_value(&reserved).unwrap(),
            serde_json::json!({"key_id": reserved.key_id})
        );
        let fingerprint = keys.fingerprint(scope.clone()).unwrap();
        assert_ne!(fingerprint.key_id, reserved.key_id);
        let fingerprint_secret = keys.resolve_fingerprint(scope.clone(), &fingerprint.key_id).unwrap();
        assert_ne!(fingerprint_secret, secret);
        assert_eq!(keys.revoke(scope.clone()).unwrap(), "revoking");
        assert!(keys
            .reserve(scope.clone(), "aes-256-gcm", 1)
            .is_err_and(|error| error.starts_with("key_revoking:")));
        assert_eq!(keys.destroy(scope.clone()).unwrap(), "destroyed");
        assert!(keys
            .resolve(scope.clone(), "aes-256-gcm", &reserved.key_id)
            .unwrap_err()
            .starts_with("shredded:"));
        assert!(keys
            .resolve_fingerprint(scope.clone(), &fingerprint.key_id)
            .unwrap_err()
            .starts_with("shredded:"));
        let mut wrong_scope = scope;
        wrong_scope.subject = Some("customer-43".into());
        assert!(keys
            .resolve(wrong_scope, "aes-256-gcm", &reserved.key_id)
            .unwrap_err()
            .starts_with("key_unauthorized:"));
    }

    fn info() -> EncryptionInfo {
        EncryptionInfo {
            format_version: 1,
            key_namespace: "production-eu".into(),
            key_id: "key-generation-1".into(),
            cipher: "xchacha20-poly1305".into(),
            nonce: vec![4; 24],
        }
    }

    #[test]
    fn protected_event_crosses_binding_and_both_codecs() {
        let mut draft = testkit::draft("event-protected", Some(1));
        draft.subject = Some("customer-42".into());
        let plaintext = draft.data.clone();
        let sealed = seal_event(draft, info(), &[7; 32]).unwrap();
        assert_ne!(sealed.data, plaintext);
        let event = crate::record_event(sealed, 42).unwrap();
        for format in ["json", "msgpack"] {
            let encoded = crate::encode_event(format, event.clone()).unwrap();
            let decoded = crate::decode_event(format, &encoded).unwrap();
            assert_eq!(decoded.subject.as_deref(), Some("customer-42"));
            assert_eq!(decoded.encryption.as_ref().unwrap().key_id, "key-generation-1");
            assert_eq!(open_event(decoded, &[7; 32]).unwrap(), plaintext);
        }
    }

    #[test]
    fn invalid_cipher_is_a_kernel_codec_error() {
        let mut invalid = info();
        invalid.cipher = "unsupported".into();
        let mut event = testkit::event_message("event-protected", Some(1));
        event.encryption = Some(invalid);
        assert_eq!(
            crate::encode_event("json", event),
            Err("codec: unsupported encryption cipher".into())
        );
    }

    #[test]
    fn command_authentication_preserves_original_envelope() {
        let mut command = testkit::command_message(Some("customer-42"));
        command.subject = Some("customer-42".into());
        let plaintext = command.data.clone();
        let sealed = seal_command(command, info(), &[8; 32]).unwrap();
        assert_eq!(open_command(sealed.clone(), &[8; 32]).unwrap(), plaintext);
        let mut tampered = sealed.clone();
        tampered.subject = Some("customer-43".into());
        assert!(open_command(tampered, &[8; 32]).unwrap_err().starts_with("codec:"));
        assert_ne!(sealed.data, plaintext);
    }
}

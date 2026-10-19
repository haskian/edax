//! In-memory reference provider. Serialize calls with one host lock when sharing it.
//!
//! This provider is intentionally not durable. Losing it loses every stored key and tombstone.
//! Production providers must persist the same atomic reservation and revocation decisions.

use crate::{Cipher, KeyScope, AES_GCM_MAX_INVOCATIONS};
use edax_core::{Error, ErrorCode};
use std::collections::HashMap;
use std::fmt;
use zeroize::Zeroizing;

/// Secret material is redacted in diagnostics and zeroized when its owner drops it.
#[derive(Clone)]
pub struct KeyMaterial(Zeroizing<Vec<u8>>);

impl KeyMaterial {
    /// Own provider-returned secret bytes and erase them when dropped.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, Error> {
        let bytes = Zeroizing::new(bytes);
        if bytes.len() != crate::KEY_LEN {
            return Err(Error::new(ErrorCode::Codec, "key must contain 32 bytes"));
        }
        Ok(Self(bytes))
    }

    /// Borrow only for the cryptographic operation; callers must not cache the bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl fmt::Debug for KeyMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KeyMaterial([REDACTED])")
    }
}

/// A single reserved encryption invocation, or a dedicated fingerprint key.
/// Destruction cannot recall a previously returned copy: the host must drain in-flight work.
#[derive(Debug)]
pub struct ReservedKey {
    /// Opaque immutable generation identifier. It is never interpreted as an address.
    pub key_id: String,
    /// Material owned by this operation, zeroized on drop.
    pub key: KeyMaterial,
}

edax_core::closed_names! {
    /// Provider-authoritative scope state. Missing material alone never implies destruction.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum KeyStatus {
        /// No generation or revocation record exists.
        Unknown = "unknown",
        /// At least one generation exists and new reservations are allowed.
        Active = "active",
        /// Creation and resolution are fenced while destruction is pending.
        Revoking = "revoking",
        /// Material was destroyed and the permanent creation fence remains.
        Destroyed = "destroyed",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Purpose {
    Encryption(Cipher),
    Fingerprint,
}

#[derive(Debug)]
struct Generation {
    scope: KeyScope,
    purpose: Purpose,
    material: Option<KeyMaterial>,
    reservations: u64,
}

/// Reference state machine: one instance is one atomic provider authority.
/// Never clone an instance or restore old state over its permanent tombstones.
#[derive(Debug)]
pub struct MemoryKeyProvider {
    generations: HashMap<String, Generation>,
    current: HashMap<(KeyScope, Purpose), String>,
    tombstones: HashMap<KeyScope, KeyStatus>,
    aes_limit: u64,
}

impl Default for MemoryKeyProvider {
    fn default() -> Self {
        Self {
            generations: HashMap::new(),
            current: HashMap::new(),
            tombstones: HashMap::new(),
            aes_limit: AES_GCM_MAX_INVOCATIONS,
        }
    }
}

impl MemoryKeyProvider {
    /// Create an empty, process-local provider.
    pub fn new() -> Self {
        Self::default()
    }

    /// Use a stricter per-generation AES budget. The format's ceiling cannot be raised.
    pub fn with_aes_limit(limit: u64) -> Result<Self, Error> {
        if limit == 0 || limit > AES_GCM_MAX_INVOCATIONS {
            return Err(Error::bad_handle("invalid AES key invocation limit"));
        }
        Ok(Self {
            aes_limit: limit,
            ..Self::default()
        })
    }

    /// Reserve before encryption, even if the later write fails. Exhausted generations remain readable.
    pub fn reserve(&mut self, scope: &KeyScope, cipher: Cipher) -> Result<ReservedKey, Error> {
        self.reserve_many(scope, cipher, 1)
    }

    /// Reserve `count` encryptions under one generation, for a batch that seals each value with its
    /// own nonce. Unused uses stay charged. A generation with fewer than `count` left rotates.
    pub fn reserve_many(&mut self, scope: &KeyScope, cipher: Cipher, count: u64) -> Result<ReservedKey, Error> {
        self.check_active(scope)?;
        let purpose = Purpose::Encryption(cipher);
        let limit = match cipher {
            Cipher::Aes256Gcm => self.aes_limit,
            Cipher::XChaCha20Poly1305 => u64::MAX,
        };
        if count == 0 || count > limit {
            return Err(Error::bad_handle(
                "key reservation count must be within the cipher's limit",
            ));
        }
        let identity = (scope.clone(), purpose);
        let current_id = self.current.get(&identity).cloned();
        if let Some(key_id) = current_id {
            if let Some(generation) = self.generations.get_mut(&key_id) {
                if limit - generation.reservations >= count {
                    generation.reservations += count;
                    return copy_key(&key_id, generation);
                }
            }
        }
        let key_id = self.create(scope, purpose)?;
        let generation = self
            .generations
            .get_mut(&key_id)
            .ok_or_else(|| Error::new(ErrorCode::KeyMissing, "reserved generation is missing"))?;
        generation.reservations = count;
        copy_key(&key_id, generation)
    }

    /// Resolve an existing generation. No read path creates keys or consumes the write budget.
    pub fn resolve(&self, scope: &KeyScope, cipher: Cipher, key_id: &str) -> Result<KeyMaterial, Error> {
        self.resolve_purpose(scope, Purpose::Encryption(cipher), key_id)
    }

    /// Return the stable, independently generated fingerprint key for a scope.
    pub fn fingerprint_key(&mut self, scope: &KeyScope) -> Result<ReservedKey, Error> {
        self.check_active(scope)?;
        let identity = (scope.clone(), Purpose::Fingerprint);
        let key_id = match self.current.get(&identity) {
            Some(key_id) => key_id.clone(),
            None => self.create(scope, Purpose::Fingerprint)?,
        };
        let generation = self
            .generations
            .get(&key_id)
            .ok_or_else(|| Error::new(ErrorCode::KeyMissing, "fingerprint generation is missing"))?;
        copy_key(&key_id, generation)
    }

    /// Resolve only a fingerprint generation belonging to the exact expected scope.
    pub fn resolve_fingerprint(&self, scope: &KeyScope, key_id: &str) -> Result<KeyMaterial, Error> {
        self.resolve_purpose(scope, Purpose::Fingerprint, key_id)
    }

    /// Permanently fence this scope. A tenant scope also fences every current and future subject.
    pub fn revoke(&mut self, scope: &KeyScope) -> Result<KeyStatus, Error> {
        scope.check()?;
        if self.status(scope)? == KeyStatus::Destroyed {
            return Ok(KeyStatus::Destroyed);
        }
        self.tombstones.insert(scope.clone(), KeyStatus::Revoking);
        Ok(KeyStatus::Revoking)
    }

    /// Destroy all matching historical encryption and fingerprint material, keeping ownership records.
    pub fn destroy(&mut self, scope: &KeyScope) -> Result<KeyStatus, Error> {
        self.revoke(scope)?;
        for generation in self.generations.values_mut() {
            if contains(scope, &generation.scope) {
                generation.material = None;
            }
        }
        self.current.retain(|(candidate, _), _| !contains(scope, candidate));
        self.tombstones.insert(scope.clone(), KeyStatus::Destroyed);
        Ok(KeyStatus::Destroyed)
    }

    /// Inspect scope state without creating anything. Parent tombstones govern absent descendants.
    pub fn status(&self, scope: &KeyScope) -> Result<KeyStatus, Error> {
        scope.check()?;
        let parent = KeyScope {
            subject: None,
            ..scope.clone()
        };
        let states = [self.tombstones.get(scope), self.tombstones.get(&parent)];
        if states.contains(&Some(&KeyStatus::Destroyed)) {
            return Ok(KeyStatus::Destroyed);
        }
        if states.contains(&Some(&KeyStatus::Revoking)) {
            return Ok(KeyStatus::Revoking);
        }
        if self.current.contains_key(&(scope.clone(), Purpose::Fingerprint))
            || Cipher::ALL.iter().any(|cipher| {
                self.current
                    .contains_key(&(scope.clone(), Purpose::Encryption(*cipher)))
            })
        {
            Ok(KeyStatus::Active)
        } else {
            Ok(KeyStatus::Unknown)
        }
    }

    fn check_active(&self, scope: &KeyScope) -> Result<(), Error> {
        match self.status(scope)? {
            KeyStatus::Destroyed => Err(Error::new(ErrorCode::Shredded, "key scope was destroyed")),
            KeyStatus::Revoking => Err(Error::new(ErrorCode::KeyRevoking, "key scope is revoking")),
            KeyStatus::Unknown | KeyStatus::Active => Ok(()),
        }
    }

    fn resolve_purpose(&self, scope: &KeyScope, purpose: Purpose, key_id: &str) -> Result<KeyMaterial, Error> {
        scope.check()?;
        let generation = self
            .generations
            .get(key_id)
            .ok_or_else(|| Error::new(ErrorCode::KeyMissing, "key generation is missing"))?;
        // Check ownership before tombstones: another scope's destroyed key must not cause a skip.
        if generation.scope != *scope || generation.purpose != purpose {
            return Err(Error::new(
                ErrorCode::KeyUnauthorized,
                "key generation does not match scope and purpose",
            ));
        }
        self.check_active(scope)?;
        generation
            .material
            .clone()
            .ok_or_else(|| Error::new(ErrorCode::KeyMissing, "key material is missing"))
    }

    fn create(&mut self, scope: &KeyScope, purpose: Purpose) -> Result<String, Error> {
        let mut random_id = [0u8; 32];
        getrandom::fill(&mut random_id)
            .map_err(|_| Error::new(ErrorCode::KeyUnavailable, "operating-system randomness unavailable"))?;
        let key_id: String = random_id.iter().map(|byte| format!("{byte:02x}")).collect();
        if self.generations.contains_key(&key_id) {
            return Err(Error::new(ErrorCode::KeyUnavailable, "key identifier collision"));
        }
        let material = KeyMaterial(Zeroizing::new(crate::new_key().map_err(|_| {
            Error::new(ErrorCode::KeyUnavailable, "operating-system randomness unavailable")
        })?));
        self.generations.insert(
            key_id.clone(),
            Generation {
                scope: scope.clone(),
                purpose,
                material: Some(material),
                reservations: 0,
            },
        );
        self.current.insert((scope.clone(), purpose), key_id.clone());
        Ok(key_id)
    }
}

fn contains(parent: &KeyScope, candidate: &KeyScope) -> bool {
    parent.key_namespace == candidate.key_namespace
        && parent.tenant == candidate.tenant
        && (parent.subject.is_none() || parent.subject == candidate.subject)
}

fn copy_key(key_id: &str, generation: &Generation) -> Result<ReservedKey, Error> {
    Ok(ReservedKey {
        key_id: key_id.to_owned(),
        key: generation
            .material
            .clone()
            .ok_or_else(|| Error::new(ErrorCode::KeyMissing, "key material is missing"))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_preserves_exact_scope_and_tombstone_precedence() {
        let mut keys = MemoryKeyProvider::with_aes_limit(1).unwrap();
        let tenant = KeyScope {
            key_namespace: "production".into(),
            tenant: Some("tenant-7".into()),
            subject: None,
        };
        let subject = KeyScope {
            subject: Some("customer-42".into()),
            ..tenant.clone()
        };
        let future = KeyScope {
            subject: Some("customer-43".into()),
            ..tenant.clone()
        };
        assert_eq!(keys.status(&subject).unwrap(), KeyStatus::Unknown);
        keys.fingerprint_key(&tenant).unwrap();
        assert_eq!(keys.status(&tenant).unwrap(), KeyStatus::Active);
        assert_eq!(keys.status(&subject).unwrap(), KeyStatus::Unknown);
        for cipher in Cipher::ALL {
            keys.reserve(&subject, *cipher).unwrap();
            keys.reserve(&subject, *cipher).unwrap();
            assert_eq!(keys.status(&subject).unwrap(), KeyStatus::Active);
        }
        keys.revoke(&subject).unwrap();
        assert_eq!(keys.status(&subject).unwrap(), KeyStatus::Revoking);
        assert_eq!(keys.status(&tenant).unwrap(), KeyStatus::Active);
        keys.destroy(&tenant).unwrap();
        for scope in [&tenant, &subject, &future] {
            assert_eq!(keys.status(scope).unwrap(), KeyStatus::Destroyed);
            assert_eq!(
                keys.reserve(scope, Cipher::default()).unwrap_err().code,
                ErrorCode::Shredded
            );
        }
        for status in KeyStatus::ALL {
            assert_eq!(KeyStatus::by_name(status.as_str()), Some(*status));
        }
        assert_eq!(KeyStatus::NAMES, &["unknown", "active", "revoking", "destroyed"]);
        assert_eq!(KeyStatus::by_name("unavailable"), None);
    }
}

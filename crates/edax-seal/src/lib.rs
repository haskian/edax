//! Authenticated encryption primitives and injective key scopes.
//!
//! This kernel performs no provider I/O. Callers reserve key usage before sealing and build
//! authenticated context from the immutable record fields. The low-level `seal` function accepts
//! an explicit nonce for interoperability; SDKs must use `new_nonce` and never reuse a nonce/key pair.

mod keys;
pub use keys::{KeyMaterial, KeyStatus, MemoryKeyProvider, ReservedKey};

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::Aes256Gcm;
use chacha20poly1305::XChaCha20Poly1305;
use edax_core::Error;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::BTreeMap;

/// Every supported algorithm uses a 256-bit key.
pub const KEY_LEN: usize = 32;
/// Every supported algorithm uses an untruncated 128-bit authentication tag.
pub const TAG_LEN: usize = 16;
/// Random-nonce AES-GCM invocation ceiling per key across all writers (SP 800-38D §8.3).
pub const AES_GCM_MAX_INVOCATIONS: u64 = 1 << 32;

edax_core::closed_names! {
    /// The authenticated-encryption algorithm named in a version-one envelope.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
    #[non_exhaustive]
    pub enum Cipher {
        /// AES-256-GCM with a 96-bit nonce and 128-bit authentication tag.
        #[serde(rename = "aes-256-gcm")]
        Aes256Gcm = "aes-256-gcm",
        /// XChaCha20-Poly1305 with a 192-bit nonce and 128-bit authentication tag.
        #[default]
        #[serde(rename = "xchacha20-poly1305")]
        XChaCha20Poly1305 = "xchacha20-poly1305",
    }
}

impl Cipher {
    /// Resolve only a supported algorithm; never fall back to another cipher.
    pub fn from_name(name: &str) -> Result<Self, Error> {
        Self::by_name(name).ok_or_else(|| Error::codec("unsupported encryption cipher"))
    }

    /// Exact nonce length required by the format.
    pub fn nonce_len(self) -> usize {
        match self {
            Self::Aes256Gcm => 12,
            Self::XChaCha20Poly1305 => 24,
        }
    }
}

/// The kernel's default for new encrypted records; readers use the recorded cipher.
pub fn default_cipher() -> Cipher {
    Cipher::default()
}

/// Public key reference and nonce. No secret or wrapped key is carried in an envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptionInfo {
    /// Encryption framing version, independent of the payload schema version.
    pub format_version: u32,
    /// Expected security domain of the configured provider.
    pub key_namespace: String,
    /// Immutable data-key generation identifier, never a provider address.
    pub key_id: String,
    /// Algorithm that sealed the bytes.
    pub cipher: Cipher,
    /// Algorithm-sized random nonce; base64 in JSON and native bytes in MessagePack.
    #[serde(with = "wire_bytes")]
    pub nonce: Vec<u8>,
}

impl EncryptionInfo {
    /// Build the current framing with a fresh OS-random nonce after reserving key usage.
    pub fn fresh(key_namespace: String, key_id: String, cipher: Cipher) -> Result<Self, Error> {
        key_name(&key_namespace, "key namespace")?;
        key_name(&key_id, "key id")?;
        Ok(Self {
            format_version: 1,
            key_namespace,
            key_id,
            cipher,
            nonce: new_nonce(cipher)?,
        })
    }

    /// Validate untrusted or locally constructed encryption information.
    pub fn check(&self) -> Result<(), Error> {
        if self.format_version != 1 {
            return Err(Error::codec("unsupported encryption format version"));
        }
        key_name(&self.key_namespace, "key namespace")?;
        key_name(&self.key_id, "key id")?;
        if self.nonce.len() != self.cipher.nonce_len() {
            return Err(Error::codec("invalid encryption nonce length"));
        }
        Ok(())
    }
}

/// Reusable encrypted bytes for stored state. The owning kernel supplies record-specific context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedValue {
    /// Public format and key reference.
    pub encryption: EncryptionInfo,
    /// Ciphertext followed by the full authentication tag.
    #[serde(with = "wire_bytes")]
    pub data: Vec<u8>,
}

impl SealedValue {
    /// Check framing before provider access; authentication still requires [`open`].
    pub fn check(&self) -> Result<(), Error> {
        self.encryption.check()?;
        if self.data.len() < TAG_LEN {
            return Err(Error::codec("encrypted payload is shorter than its authentication tag"));
        }
        Ok(())
    }
}

/// A provider scope always retains the tenant, including bridge and silo deployments.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct KeyScope {
    /// Deployment security domain.
    pub key_namespace: String,
    /// Optional tenant; absence is distinct from every tenant name.
    pub tenant: Option<String>,
    /// Optional erasure owner; absence selects the tenant-level scope.
    pub subject: Option<String>,
}

impl KeyScope {
    /// Validate each present identifier. An empty identifier is never absence.
    pub fn check(&self) -> Result<(), Error> {
        key_name(&self.key_namespace, "key namespace")?;
        if let Some(tenant) = &self.tenant {
            edax_core::check_tenant(tenant)?;
        }
        if let Some(subject) = &self.subject {
            edax_core::check_subject(subject)?;
        }
        Ok(())
    }

    /// Versioned, injective provider key: no delimiter inside a name can alias another scope.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.check()?;
        let mut context = Aad::new("edax.key-scope");
        context
            .str(&self.key_namespace)
            .optional(self.tenant.as_deref())
            .optional(self.subject.as_deref());
        Ok(context.finish())
    }

    /// Canonical lowercase hexadecimal storage identity for the versioned scope bytes.
    pub fn encode_hex(&self) -> Result<String, Error> {
        Ok(self.encode()?.iter().map(|byte| format!("{byte:02x}")).collect())
    }
}

/// A key namespace or key ID: nonempty, and free of NUL, which no store can hold alike.
fn key_name(value: &str, name: &str) -> Result<(), Error> {
    if value.is_empty() {
        return Err(Error::codec(format_args!("{name} is empty")));
    }
    if value.contains('\0') {
        return Err(Error::codec(format_args!("{name} holds a NUL character")));
    }
    Ok(())
}

/// Version-one associated-data encoding. Field order is defined by the owning record kernel.
/// Integers are big-endian. Strings/bytes and maps carry a u64 length; options carry a presence byte.
#[derive(Debug, Clone)]
pub struct Aad(Vec<u8>);

impl Aad {
    /// Prefix a format version and an unambiguous record-domain name.
    pub fn new(domain: &str) -> Self {
        let mut context = Self(Vec::new());
        context.u32(1).str(domain);
        context
    }

    /// Append a length-prefixed byte sequence.
    pub fn bytes(&mut self, value: &[u8]) -> &mut Self {
        self.u64(value.len() as u64);
        self.0.extend_from_slice(value);
        self
    }

    /// Append a length-prefixed UTF-8 string.
    pub fn str(&mut self, value: &str) -> &mut Self {
        self.bytes(value.as_bytes())
    }

    /// Append a four-byte integer.
    pub fn u32(&mut self, value: u32) -> &mut Self {
        self.0.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// Append an eight-byte unsigned integer.
    pub fn u64(&mut self, value: u64) -> &mut Self {
        self.0.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// Append an eight-byte signed integer, preserving nanoseconds exactly.
    pub fn i64(&mut self, value: i64) -> &mut Self {
        self.0.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// Append an explicit presence marker and optional string.
    pub fn optional(&mut self, value: Option<&str>) -> &mut Self {
        self.0.push(u8::from(value.is_some()));
        if let Some(value) = value {
            self.str(value);
        }
        self
    }

    /// Append an explicit presence marker and optional unsigned integer.
    pub fn optional_u64(&mut self, value: Option<u64>) -> &mut Self {
        self.0.push(u8::from(value.is_some()));
        if let Some(value) = value {
            self.u64(value);
        }
        self
    }

    /// Append an explicit presence marker and optional signed integer.
    pub fn optional_i64(&mut self, value: Option<i64>) -> &mut Self {
        self.0.push(u8::from(value.is_some()));
        if let Some(value) = value {
            self.i64(value);
        }
        self
    }

    /// Append a count and lexicographically sorted string key/value pairs.
    pub fn map(&mut self, value: &BTreeMap<String, String>) -> &mut Self {
        self.u64(value.len() as u64);
        for (key, value) in value {
            self.str(key).str(value);
        }
        self
    }

    /// Bind every encryption-information field to the record.
    pub fn encryption(&mut self, info: &EncryptionInfo) -> &mut Self {
        self.u32(info.format_version)
            .str(&info.key_namespace)
            .str(&info.key_id)
            .str(info.cipher.as_str())
            .bytes(&info.nonce)
    }

    /// Return the encoded associated data.
    pub fn finish(self) -> Vec<u8> {
        self.0
    }
}

/// Seal bytes with caller-provided authenticated context. The result includes the full tag.
/// The caller must reserve usage and ensure nonce uniqueness before calling this low-level API.
pub fn seal(info: &EncryptionInfo, key: &[u8], aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, Error> {
    crypt(info, key, aad, plaintext, true)
}

/// Authenticate before returning any plaintext. All authentication failures have one safe message.
pub fn open(info: &EncryptionInfo, key: &[u8], aad: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, Error> {
    if ciphertext.len() < TAG_LEN {
        return Err(Error::codec("encrypted payload is shorter than its authentication tag"));
    }
    crypt(info, key, aad, ciphertext, false)
}

fn crypt(info: &EncryptionInfo, key: &[u8], aad: &[u8], input: &[u8], encrypt: bool) -> Result<Vec<u8>, Error> {
    info.check()?;
    check_key(key)?;
    let payload = Payload { msg: input, aad };
    let result = match info.cipher {
        Cipher::Aes256Gcm => {
            let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| Error::codec("invalid encryption key length"))?;
            let nonce: [u8; 12] = info
                .nonce
                .as_slice()
                .try_into()
                .map_err(|_| Error::codec("invalid encryption nonce length"))?;
            if encrypt {
                cipher.encrypt(&nonce.into(), payload)
            } else {
                cipher.decrypt(&nonce.into(), payload)
            }
        }
        Cipher::XChaCha20Poly1305 => {
            let cipher =
                XChaCha20Poly1305::new_from_slice(key).map_err(|_| Error::codec("invalid encryption key length"))?;
            let nonce: [u8; 24] = info
                .nonce
                .as_slice()
                .try_into()
                .map_err(|_| Error::codec("invalid encryption nonce length"))?;
            if encrypt {
                cipher.encrypt(&nonce.into(), payload)
            } else {
                cipher.decrypt(&nonce.into(), payload)
            }
        }
    };
    result.map_err(|_| {
        Error::codec(if encrypt {
            "payload encryption failed"
        } else {
            "payload authentication failed"
        })
    })
}

fn check_key(key: &[u8]) -> Result<(), Error> {
    if key.len() != KEY_LEN {
        return Err(Error::codec("invalid encryption key length"));
    }
    Ok(())
}

/// Full HMAC-SHA-256, using a dedicated scope key rather than an encryption key.
pub fn fingerprint(key: &[u8], input: &[u8]) -> Result<Vec<u8>, Error> {
    check_key(key)?;
    let mut mac =
        <Hmac<Sha256> as Mac>::new_from_slice(key).map_err(|_| Error::codec("invalid fingerprint key length"))?;
    mac.update(input);
    Ok(mac.finalize().into_bytes().to_vec())
}

/// Generate key material using operating-system randomness. Providers own its lifetime.
pub fn new_key() -> Result<Vec<u8>, Error> {
    random_bytes(KEY_LEN)
}

/// Generate a fresh algorithm-sized nonce using operating-system randomness.
pub fn new_nonce(cipher: Cipher) -> Result<Vec<u8>, Error> {
    random_bytes(cipher.nonce_len())
}

fn random_bytes(length: usize) -> Result<Vec<u8>, Error> {
    let mut bytes = vec![0; length];
    getrandom::fill(&mut bytes).map_err(|_| Error::store("operating-system randomness unavailable"))?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_encryption_uses_kernel_framing_and_algorithm_nonce() {
        assert_eq!(default_cipher(), Cipher::XChaCha20Poly1305);
        for cipher in Cipher::ALL {
            let first = EncryptionInfo::fresh("production".into(), "generation-1".into(), *cipher).unwrap();
            let second = EncryptionInfo::fresh("production".into(), "generation-1".into(), *cipher).unwrap();
            first.check().unwrap();
            assert_eq!(first.format_version, 1);
            assert_eq!(first.cipher, *cipher);
            assert_eq!(first.nonce.len(), cipher.nonce_len());
            assert_ne!(first.nonce, second.nonce);
            for (namespace, key_id) in [("", "generation-1"), ("production", "")] {
                assert_eq!(
                    EncryptionInfo::fresh(namespace.into(), key_id.into(), *cipher)
                        .unwrap_err()
                        .code,
                    edax_core::ErrorCode::Codec
                );
            }
        }
    }

    #[test]
    fn scope_storage_bytes_are_fixed_for_each_optional_identity() {
        for (tenant, subject, expected) in [
            (
                None,
                None,
                "00000001000000000000000e656461782e6b65792d73636f7065000000000000000470726f640000",
            ),
            (
                Some("tenant"),
                None,
                "00000001000000000000000e656461782e6b65792d73636f7065000000000000000470726f6401000000000000000674656e616e7400",
            ),
            (
                None,
                Some("subject"),
                "00000001000000000000000e656461782e6b65792d73636f7065000000000000000470726f64000100000000000000077375626a656374",
            ),
            (
                Some("tenant"),
                Some("subject"),
                "00000001000000000000000e656461782e6b65792d73636f7065000000000000000470726f6401000000000000000674656e616e740100000000000000077375626a656374",
            ),
        ] {
            let scope = KeyScope {
                key_namespace: "prod".into(),
                tenant: tenant.map(str::to_owned),
                subject: subject.map(str::to_owned),
            };
            assert_eq!(scope.encode().unwrap(), hex::decode(expected).unwrap());
            assert_eq!(scope.encode_hex().unwrap(), expected);
        }
    }
}

/// The same byte representation used by edax message payloads.
mod wire_bytes {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&STANDARD.encode(bytes))
        } else {
            serializer.serialize_bytes(bytes)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        if deserializer.is_human_readable() {
            let text = String::deserialize(deserializer)?;
            STANDARD.decode(text).map_err(serde::de::Error::custom)
        } else {
            serde_bytes::ByteBuf::deserialize(deserializer).map(serde_bytes::ByteBuf::into_vec)
        }
    }
}

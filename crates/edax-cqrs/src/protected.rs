//! Protected command idempotency. Fingerprints bind canonical plaintext with a dedicated scope key;
//! successful results remain encrypted. Legacy `Record` and its wire format are unchanged.

use edax_core::{Error, ErrorCode};
use edax_seal::{Aad, EncryptionInfo, KeyScope, SealedValue};
use serde::{Deserialize, Serialize};

/// Full keyed command identity. The fingerprint key survives data-key rotation until erasure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fingerprint {
    /// Version-one fingerprints use `hmac-sha256` without truncation.
    pub algorithm: String,
    /// Immutable fingerprint-key identifier; never an encryption-key identifier.
    pub key_id: String,
    /// All 32 HMAC bytes. No unkeyed plaintext hash accompanies them.
    pub value: Vec<u8>,
}

impl Fingerprint {
    /// Validate the exact supported construction before comparison or persistence.
    pub fn check(&self) -> Result<(), Error> {
        if self.algorithm != "hmac-sha256" || self.key_id.is_empty() || self.value.len() != 32 {
            return Err(Error::codec("invalid protected command fingerprint"));
        }
        Ok(())
    }
}

/// Fingerprint the payload schema and RFC 8785 canonical command bytes with a dedicated scope key.
/// The message nonce, data-key generation, and transport instance ID do not affect command equality.
pub fn command_fingerprint(
    name: &str,
    version: u32,
    data: &[u8],
    key_id: &str,
    key: &[u8],
) -> Result<Fingerprint, Error> {
    let canonical = crate::canonical_json(data)
        .map_err(|_| Error::codec("protected command payload is not canonicalizable JSON"))?;
    let mut context = Aad::new("edax.command-fingerprint.v1");
    context.str(name).u32(version).bytes(&canonical);
    let fingerprint = Fingerprint {
        algorithm: "hmac-sha256".into(),
        key_id: key_id.to_owned(),
        value: edax_seal::fingerprint(key, &context.finish())?,
    };
    fingerprint.check()?;
    Ok(fingerprint)
}

/// Settlement has one shape at a time; failed records never hold arbitrary exception text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProtectedState {
    /// The fenced claimant has not completed.
    Pending,
    /// Authenticated encrypted handler result, including serialized null results.
    Done {
        /// Encrypted bytes plus public encryption information.
        result: SealedValue,
    },
    /// A terminal handler failure represented solely by its taxonomy code.
    Failed {
        /// A terminal, nonfatal code from the shared error taxonomy.
        #[serde(with = "error_code")]
        code: ErrorCode,
    },
}

/// Versioned protected ledger record. The owning store applies its normal atomic claim fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectedRecord {
    /// Protected ledger framing version; currently one.
    pub format_version: u32,
    /// Namespace, tenant, and erasure owner; never the storage-tenancy composed key alone.
    pub scope: KeyScope,
    /// The fully scoped ledger claim identity, including the guard identity.
    pub claim_id: String,
    /// Claim fencing token issued by the store. The erasure owner is `scope.subject`.
    pub owner: String,
    /// Full command identity under the scope's dedicated fingerprint key.
    pub fingerprint: Fingerprint,
    /// Pending, encrypted success, or safe terminal failure.
    pub state: ProtectedState,
}

/// The decision a duplicate reaches after scope, claim identity, and full fingerprint checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtectedReplay<'a> {
    /// Another fenced claimant still owns the work.
    Pending,
    /// Resolve the result's data key after checking revocation, then authenticate before decoding.
    Done(&'a SealedValue),
    /// Replay this safe terminal code, without a persisted exception message.
    Failed(ErrorCode),
}

edax_core::closed_names! {
    /// Closed replay decisions shared by every binding.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ProtectedReplayKind {
        /// The claimant is still running.
        Pending = "pending",
        /// Authenticate the encrypted result.
        Done = "done",
        /// Replay a stable failure code.
        Failed = "failed",
    }
}

impl ProtectedReplay<'_> {
    /// Stable decision name owned by the CQRS kernel.
    pub fn kind(&self) -> ProtectedReplayKind {
        match self {
            Self::Pending => ProtectedReplayKind::Pending,
            Self::Done(_) => ProtectedReplayKind::Done,
            Self::Failed(_) => ProtectedReplayKind::Failed,
        }
    }
}

impl ProtectedRecord {
    /// Construct a pending record after the store mints an owner token.
    pub fn pending(scope: KeyScope, claim_id: String, owner: String, fingerprint: Fingerprint) -> Result<Self, Error> {
        let record = Self {
            format_version: 1,
            scope,
            claim_id,
            owner,
            fingerprint,
            state: ProtectedState::Pending,
        };
        record.check()?;
        Ok(record)
    }

    /// Validate a decoded record before any decision or key-provider access.
    pub fn check(&self) -> Result<(), Error> {
        if self.format_version != 1 || self.claim_id.is_empty() || self.owner.is_empty() {
            return Err(Error::codec("invalid protected idempotency record identity"));
        }
        self.scope.check()?;
        self.fingerprint.check()?;
        match &self.state {
            ProtectedState::Done { result } => {
                result.check()?;
                self.check_namespace(&result.encryption)?;
            }
            ProtectedState::Failed { code } => check_failure(*code)?,
            ProtectedState::Pending => {}
        }
        Ok(())
    }

    /// Classify a prior record. A new fingerprint-key generation is deliberately a mismatch.
    pub fn replay(
        &self,
        scope: &KeyScope,
        claim_id: &str,
        fingerprint: &Fingerprint,
    ) -> Result<ProtectedReplay<'_>, Error> {
        self.check()?;
        scope.check()?;
        fingerprint.check()?;
        if &self.scope != scope || self.claim_id != claim_id {
            return Err(Error::codec("protected claim identity does not match"));
        }
        if &self.fingerprint != fingerprint {
            return Err(Error::new(
                ErrorCode::PayloadMismatch,
                "protected command fingerprint does not match",
            ));
        }
        Ok(match &self.state {
            ProtectedState::Pending => ProtectedReplay::Pending,
            ProtectedState::Done { result } => ProtectedReplay::Done(result),
            ProtectedState::Failed { code } => ProtectedReplay::Failed(*code),
        })
    }

    /// Prepare encrypted settlement for the current owner. Retain this record across commit retries.
    /// Providers must reserve key usage before this call; stores must fence the eventual write.
    pub fn seal_result(&self, owner: &str, info: EncryptionInfo, key: &[u8], plaintext: &[u8]) -> Result<Self, Error> {
        self.check_claimant(owner)?;
        let context = self.result_context(&info)?;
        let data = edax_seal::seal(&info, key, &context, plaintext)?;
        Ok(Self {
            state: ProtectedState::Done {
                result: SealedValue { encryption: info, data },
            },
            ..self.clone()
        })
    }

    /// Prepare a code-only terminal failure. Transient/fatal failures release their claims instead.
    pub fn fail(&self, owner: &str, code: ErrorCode) -> Result<Self, Error> {
        self.check_claimant(owner)?;
        check_failure(code)?;
        Ok(Self {
            state: ProtectedState::Failed { code },
            ..self.clone()
        })
    }

    /// Authenticate the result after the SDK checks scope revocation and resolves its key.
    pub fn open_result(&self, key: &[u8]) -> Result<Vec<u8>, Error> {
        self.check()?;
        match &self.state {
            ProtectedState::Done { result } => {
                let context = self.result_context(&result.encryption)?;
                edax_seal::open(&result.encryption, key, &context, &result.data)
            }
            ProtectedState::Pending => Err(Error::new(ErrorCode::InProgress, "protected command is pending")),
            ProtectedState::Failed { code } => Err(Error::new(*code, "protected command failed")),
        }
    }

    /// Bind the complete claim, owner, scope, fingerprint, successful settlement, and encryption info.
    pub fn result_context(&self, info: &EncryptionInfo) -> Result<Vec<u8>, Error> {
        self.check()?;
        info.check()?;
        self.check_namespace(info)?;
        let mut context = Aad::new("edax.ledger-result.v1");
        context
            .u32(self.format_version)
            .bytes(&self.scope.encode()?)
            .str(&self.claim_id)
            .str(&self.owner)
            .str("done")
            .str(&self.fingerprint.algorithm)
            .str(&self.fingerprint.key_id)
            .bytes(&self.fingerprint.value)
            .encryption(info);
        Ok(context.finish())
    }

    fn check_namespace(&self, info: &EncryptionInfo) -> Result<(), Error> {
        if info.key_namespace != self.scope.key_namespace {
            return Err(Error::codec(
                "result encryption namespace does not match protected claim",
            ));
        }
        Ok(())
    }

    fn check_claimant(&self, owner: &str) -> Result<(), Error> {
        self.check()?;
        if self.owner != owner || !matches!(self.state, ProtectedState::Pending) {
            return Err(Error::new(
                ErrorCode::Concurrency,
                "protected claim owner changed or claim already settled",
            ));
        }
        Ok(())
    }
}

fn check_failure(code: ErrorCode) -> Result<(), Error> {
    if !code.is_terminal() || code.is_fatal() {
        return Err(Error::codec(
            "protected ledger can cache only terminal nonfatal failure codes",
        ));
    }
    Ok(())
}

/// Encode only a valid protected record. No plaintext hash or exception text is admitted.
pub fn encode_record(record: &ProtectedRecord) -> Result<String, Error> {
    record.check()?;
    serde_json::to_string(record).map_err(|_| Error::codec("cannot encode protected idempotency record"))
}

/// Decode a protected record without echoing potentially sensitive malformed input into errors.
pub fn decode_record(json: &str) -> Result<ProtectedRecord, Error> {
    let record: ProtectedRecord =
        serde_json::from_str(json).map_err(|_| Error::codec("invalid protected idempotency record"))?;
    record.check()?;
    Ok(record)
}

mod error_code {
    use edax_core::ErrorCode;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(code: &ErrorCode, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(code.as_str())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<ErrorCode, D::Error> {
        let code = String::deserialize(deserializer)?;
        code.parse()
            .map_err(|_| serde::de::Error::custom("unknown protected failure code"))
    }
}

/// Preserve the logical command identity across the plaintext-to-protected migration.
/// Scope remains authenticated inside the record. Reusing an intent under a different scope fails
/// closed; changing storage keys here would bypass both legacy completions and in-flight claims.
pub fn claim_key(scope: &KeyScope, key: &str) -> Result<String, Error> {
    scope.check()?;
    Ok(key.to_owned())
}

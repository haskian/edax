//! The key namespace of one generation: every record an ingestion target or a projection writes sits
//! under its generation's length-prefixed name, so two generations never share a key.

use edax_core::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::canonical_json;

/// The unambiguous key prefix of `generation`: its byte length, the name and a colon.
pub fn prefix(generation: &str) -> Result<String, Error> {
    if generation.is_empty() || generation.contains('\0') {
        return Err(Error::bad_handle("a generation must be nonempty and contain no NUL"));
    }
    Ok(edax_core::scoped(generation, ""))
}

/// The key of the ordered JSON array `parts_json` under `generation`, its parts in canonical JSON.
pub fn key(generation: &str, parts_json: &str) -> Result<String, Error> {
    let namespace = prefix(generation)?;
    let parts: Value = serde_json::from_str(parts_json).map_err(Error::codec)?;
    if !parts.is_array() {
        return Err(Error::codec("generation key components must be a JSON array"));
    }
    let canonical = canonical_json(parts_json.as_bytes())?;
    let suffix = String::from_utf8(canonical).map_err(Error::codec)?;
    let key = edax_core::scoped(generation, &suffix);
    debug_assert!(key.starts_with(&namespace));
    if key.len() > MAX_KEY_BYTES {
        return Err(Error::codec(format_args!(
            "a generation key exceeds {MAX_KEY_BYTES} bytes"
        )));
    }
    Ok(key)
}

/// The half-open key range `[start, end)` of every key under `generation` whose canonical array starts
/// with `parts` and continues with a JSON string: what an erasure cleanup deletes. It renders `parts`
/// through [`key`], so the range matches the keys byte for byte (`COLLATE "C"`): after `parts` comes
/// the comma and the string's opening `"`, and `#`, the next byte, bounds it.
pub fn range(generation: &str, parts: &Value) -> Result<(String, String), Error> {
    let head = head(generation, parts)?;
    Ok((format!("{head},\""), format!("{head},#")))
}

/// [`range`] for keys that continue with any JSON value after `parts`, `null` included: a tenant's
/// keys whatever subject follows. After `parts` comes the comma, and `-`, the next byte, bounds it.
pub fn prefix_range(generation: &str, parts: &Value) -> Result<(String, String), Error> {
    let head = head(generation, parts)?;
    Ok((format!("{head},"), format!("{head}-")))
}

/// The key of `parts` without its closing bracket.
fn head(generation: &str, parts: &Value) -> Result<String, Error> {
    let open = key(generation, &parts.to_string())?;
    open.strip_suffix(']')
        .map(str::to_owned)
        .ok_or_else(|| Error::codec("a key range needs an array of parts"))
}

edax_core::closed_names! {
    /// How a generation stores the private part of a subject's data.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum AtRest {
        /// Sealed under the subject's key, so destroying the key erases it, backups included.
        Sealed = "sealed",
        /// Plaintext: an erasure cleanup participant must delete it.
        Plaintext = "plaintext",
    }
}

/// One stored part a host seals before it writes, or opens after it reads: the protected-state context
/// the kernel binds. `kind` is `ingestion`, `projection` or `intent`; `identity` is the part's storage
/// key, which carries the generation, tenant and subject; `version` is the row's sequence, 0 when it
/// has none. The scope is the key's tenant and subject, never a value the host supplies.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Part {
    /// Stored state kind, as `edax_event::sealing::state_context` names it.
    pub kind: String,
    /// Storage key the context binds.
    pub identity: String,
    /// Row sequence the context binds.
    pub version: u64,
    /// Tenant of the sealing scope.
    pub tenant: Option<String>,
    /// Erasure subject of the sealing scope.
    pub subject: String,
}

/// Refuse a subject's private part that a sealed generation would store as plaintext: it must be
/// absent or the protected-state envelope of `tenant` and `subject`. Opening still authenticates it.
pub fn check_sealed(private: Option<&Value>, tenant: Option<&str>, subject: &str) -> Result<(), Error> {
    let Some(private) = private.filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let sealed = edax_event::sealing::decode_protected_state(&private.to_string())?;
    if sealed.is_none_or(|state| state.tenant.as_deref() != tenant || state.subject.as_deref() != Some(subject)) {
        return Err(Error::bad_handle(
            "a subject's private data must arrive sealed under its own scope",
        ));
    }
    Ok(())
}

/// Refuse a tenant that cannot cross native string boundaries or be stored by Postgres.
pub fn check_tenant(tenant: Option<&str>) -> Result<(), Error> {
    tenant.map(edax_core::check_tenant).transpose()?;
    if tenant.is_some_and(|value| value.contains('\0')) {
        return Err(Error::codec("tenant holds a NUL character"));
    }
    Ok(())
}

/// Refuse an erasure subject a key cannot carry: the kernel's subject rule (1 to 256 bytes), and no NUL,
/// which Postgres cannot store. One rule for every key that names a subject.
pub fn check_subject(subject: &str) -> Result<(), Error> {
    edax_core::check_subject(subject)
}

/// The longest key every store accepts. A Postgres btree entry holds at most about 2.7 KB, so every
/// store refuses a longer key alike, as `codec`, rather than only the store that fails.
pub const MAX_KEY_BYTES: usize = 2048;

impl Part {
    /// The complete AAD contract for a stored ingestion, projection or intent part.
    /// Fixed fields preserve the bytes used by the original stored-part format.
    pub fn context(&self, info: &edax_seal::EncryptionInfo) -> Result<Vec<u8>, Error> {
        if !matches!(self.kind.as_str(), "ingestion" | "projection" | "intent") {
            return Err(Error::codec("unknown stored part kind"));
        }
        edax_event::sealing::state_context(
            &self.kind,
            &self.identity,
            "",
            self.version,
            self.tenant.as_deref(),
            Some(&self.subject),
            "",
            false,
            info,
        )
    }
}

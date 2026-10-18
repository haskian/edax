//! The key namespace of one generation: every record an ingestion target or a projection writes sits
//! under its generation's length-prefixed name, so two generations never share a key.

use edax_core::Error;
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

/// The longest key every store accepts. A Postgres btree entry holds at most about 2.7 KB, so every
/// store refuses a longer key alike, as `codec`, rather than only the store that fails.
pub const MAX_KEY_BYTES: usize = 2048;

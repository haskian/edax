//! The `edax-core` surface: backoff, the clock readers, the error taxonomy, tenancy and the raw
//! payload hash.

use edax_core::ErrorCode;

use crate::{error_code, names, tenancy_mode, to_wire};

/// A backoff policy, in milliseconds. See [`edax_core::Backoff`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct BackoffPolicy {
    /// The ceiling of the first retry's wait; each later attempt doubles it.
    pub base_millis: u64,
    /// The bound on any single wait.
    pub cap_millis: u64,
}

/// The toolkit's default backoff policy, so a host never restates its numbers.
pub fn default_backoff() -> BackoffPolicy {
    let policy = edax_core::Backoff::DEFAULT;
    BackoffPolicy {
        base_millis: policy.base_millis,
        cap_millis: policy.cap_millis,
    }
}

/// The wait in milliseconds before retry `attempt` (0-based) under the policy `(base_millis,
/// cap_millis)`: full jitter, `entropy` (a random `u64` the host draws per wait) reduced into
/// `0 ..= ceiling`. Flat arguments rather than a [`BackoffPolicy`], so a host calls it without
/// building a native object per wait. See [`edax_core::backoff_millis`].
pub fn backoff_millis(base_millis: u64, cap_millis: u64, attempt: u64, entropy: u64) -> u64 {
    edax_core::backoff_millis(
        edax_core::Backoff {
            base_millis,
            cap_millis,
        },
        attempt,
        entropy,
    )
}

/// A fresh time-ordered globally-unique id (UUID v7), as a string. Non-deterministic.
pub fn guid() -> String {
    edax_core::guid()
}

/// Current Unix time in nanoseconds, strictly increasing per process. Non-deterministic.
pub fn current_timestamp() -> i64 {
    edax_core::current_timestamp()
}

/// Every code of the error taxonomy, in the kernel's order. A host builds its exception table from
/// this list rather than restating it, so a code the kernel adds is a code the host must map.
pub fn error_codes() -> Vec<String> {
    names(ErrorCode::NAMES)
}

/// Whether a failure with the taxonomy `code` is terminal: it fails the same way on every retry, so an
/// idempotency guard caches it instead of releasing the claim. `None` is a failure no code classifies
/// (a host's own error), which gets the kernel's default, so a host never chooses one. A code the
/// kernel does not know is a `codec:` (terminal) error: the same name fails the same way every time.
/// The verdict on a wire string is [`split_error`]'s. See [`edax_core::is_terminal`].
pub fn error_verdict(code: Option<String>) -> Result<bool, String> {
    let code = error_code(code)?;
    Ok(edax_core::is_terminal(code))
}

/// A wire failure taken apart. A struct rather than a tuple: the generator sanitizes a tuple return
/// to `Vec<String>` and loses the field names.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SplitError {
    /// The taxonomy code: `store` when the wire form carries no code the kernel knows.
    pub code: String,
    /// The message after the separator, or the whole wire string when it carries no known code.
    pub message: String,
    /// The kernel's verdict for this failure.
    pub terminal: bool,
}

/// Split a wire failure `"<code>: <message>"` into its code, its message and its verdict.
///
/// An unknown code, or a string with no separator, is a `store` failure carrying the whole string
/// unchanged, so a host still logs the original text and decides nothing itself. The separator and
/// that default are the kernel's ([`edax_core::Error::from_wire`]); a host that split the string
/// itself would own a second copy of them. `testdata/golden/error_wire.json` pins every case.
pub fn split_error(wire: &str) -> SplitError {
    let error = edax_core::Error::from_wire(wire);
    SplitError {
        code: error.code.as_str().to_owned(),
        terminal: error.is_terminal(),
        message: error.message,
    }
}

/// The longest tenant id the key rule allows, in bytes. It is a function because the generator binds
/// functions, not constants. A host reads the limit from here instead of restating it. See
/// [`edax_core::TENANT_MAX`].
pub fn tenant_max() -> u32 {
    // 64 by definition, so the conversion never saturates.
    u32::try_from(edax_core::TENANT_MAX).unwrap_or(u32::MAX)
}

/// Every tenancy mode's name (`"silo"`, `"pool"`, `"bridge"`), in the kernel's order. A host checks its
/// own list of modes against this one, so a mode the kernel adds is a mode the host must name. See
/// [`edax_core::Tenancy::ALL`].
pub fn tenancy_modes() -> Vec<String> {
    names(edax_core::Tenancy::NAMES)
}

/// Refuse a tenant id that breaks the rule, as a `codec:` error. A tenant id is 1 to [`tenant_max`]
/// bytes; the charset is open, because `tenant_key` is injective over any bytes. See
/// [`edax_core::check_tenant`].
pub fn check_tenant(tenant: &str) -> Result<(), String> {
    edax_core::check_tenant(tenant).map_err(to_wire)
}

/// The key a seam is called with under the tenancy `mode` (`"silo"`, `"pool"` or `"bridge"`): the
/// `key` itself, or `key` composed with its tenant. A mode that requires a tenant and a call that
/// names none is a `codec:` error, so a pool can never read an uncomposed key. Pinned by
/// `testdata/golden/tenancy.json`. See [`edax_core::tenant_key`].
pub fn tenant_key(mode: &str, key: &str, tenant: Option<String>) -> Result<String, String> {
    let mode = tenancy_mode(mode)?;
    edax_core::tenant_key(mode, tenant.as_deref(), key).map_err(to_wire)
}

/// Whether the tenancy `mode` requires every message and keyed call to name a tenant, and whether it
/// composes that tenant into the key. One call, so a binding reads both rules from the kernel rather
/// than restating them. See [`edax_core::Tenancy`].
pub fn tenancy_rules(mode: &str) -> Result<TenancyRules, String> {
    let mode = tenancy_mode(mode)?;
    Ok(TenancyRules {
        requires_tenant: mode.requires_tenant(),
        composes_keys: mode.composes_keys(),
    })
}

/// What a tenancy mode demands, as [`tenancy_rules`] reports it.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct TenancyRules {
    /// Every message and keyed call must name a tenant.
    pub requires_tenant: bool,
    /// The tenant is composed into every key a seam is called with.
    pub composes_keys: bool,
}

/// The payload hash an idempotency record binds its key to, over arbitrary bytes (FNV-1a 64-bit). A
/// command's guard hashes with [`command_hash_json`](crate::command_hash_json) instead, which binds the name and canonicalizes
/// first. See [`edax_core::payload_hash`].
pub fn payload_hash(payload: &[u8]) -> u64 {
    edax_core::payload_hash(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_crosses_its_arguments_in_order() {
        // `(base, cap, attempt, entropy)` in that order: each swap changes the answer.
        let policy = default_backoff();
        assert_eq!(
            (policy.base_millis, policy.cap_millis),
            (100, 30_000),
            "the kernel's default"
        );
        assert_eq!(backoff_millis(100, 30_000, 0, 100), 100, "entropy at the first ceiling");
        assert_eq!(backoff_millis(100, 30_000, 1, 150), 150, "the second ceiling is 200");
        assert_eq!(
            backoff_millis(100, 30_000, 1_000, u64::MAX),
            u64::MAX % 30_001,
            "capped, no overflow"
        );
        assert_eq!(backoff_millis(100, 50, 0, 77), 77 % 51, "the cap bounds the base");
    }

    #[test]
    fn error_taxonomy_is_the_kernels() {
        // The list and every verdict are the kernel's own, in its order: a binding that builds its
        // exception table from these two calls cannot restate the taxonomy. One row per code.
        let codes = error_codes();
        assert_eq!(
            codes,
            ErrorCode::ALL.iter().map(|c| c.as_str().to_owned()).collect::<Vec<_>>()
        );
        for code in ErrorCode::ALL {
            assert_eq!(
                error_verdict(Some(code.as_str().to_owned())),
                Ok(code.is_terminal()),
                "{code}"
            );
        }
        // An unknown code is refused with the kernel's terminal `codec:` error: the same name fails the
        // same way every time, so a host must not read it as a transient store failure.
        assert_eq!(
            error_verdict(Some("timeout".to_owned())),
            Err("codec: unknown error code: timeout".to_owned())
        );
        assert_eq!(
            error_verdict(Some(String::new())),
            Err("codec: unknown error code: ".to_owned())
        );
        // The default for a failure no code classifies is the kernel's too.
        assert_eq!(
            error_verdict(None),
            Ok(edax_core::is_terminal(None)),
            "unclassified is the kernel's default"
        );
    }

    #[test]
    fn split_error_branches() {
        struct Case {
            name: &'static str,
            wire: &'static str,
            want: (&'static str, &'static str, bool),
        }
        let cases = [
            Case {
                name: "known_code",
                wire: "codec: bad json",
                want: ("codec", "bad json", true),
            },
            Case {
                name: "separator_inside_the_message",
                wire: "store: redis: connection reset",
                want: ("store", "redis: connection reset", false),
            },
            Case {
                name: "empty_message",
                wire: "concurrency: ",
                want: ("concurrency", "", false),
            },
            // No code the kernel knows: a store failure carrying the whole string, so the class and the
            // verdict are the kernel's default rather than the host's.
            Case {
                name: "unknown_code",
                wire: "timeout: deadline exceeded",
                want: ("store", "timeout: deadline exceeded", false),
            },
            Case {
                name: "no_separator",
                wire: "connection reset",
                want: ("store", "connection reset", false),
            },
            Case {
                name: "empty",
                wire: "",
                want: ("store", "", false),
            },
        ];
        for c in cases {
            let got = split_error(c.wire);
            assert_eq!(
                (got.code.as_str(), got.message.as_str(), got.terminal),
                c.want,
                "{}",
                c.name
            );
        }
    }

    #[test]
    fn clock_readers_are_unique_and_ordered() {
        assert_ne!(guid(), guid(), "ids are unique");
        assert!(
            current_timestamp() < current_timestamp(),
            "timestamps strictly increase"
        );
    }
}

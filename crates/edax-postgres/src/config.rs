//! The settings a Postgres store is built with, their defaults, and the `bigint` bound.
//!
//! The timeouts are layered so the server answers first: a lock wait ends at `lock_timeout_ms`, a
//! statement at `statement_timeout_ms`, and the client gives up on a call only at `op_timeout_ms`.
//! A store refuses any other order, because a client that cancels first never learns which of the
//! three ran out.

use edax_core::Error;
use serde::{Deserialize, Serialize};

/// What a store is built with. Every duration is in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settings {
    /// How long a call waits for a pooled connection.
    pub acquire_timeout_ms: i64,
    /// How long a statement waits on a row lock (`lock_timeout`).
    pub lock_timeout_ms: i64,
    /// How long one statement may run (`statement_timeout`).
    pub statement_timeout_ms: i64,
    /// How long the client waits for any one call before it gives up on it.
    pub op_timeout_ms: i64,
    /// How long a transaction may sit idle between statements
    /// (`idle_in_transaction_session_timeout`), so a frozen worker cannot hold a row forever.
    pub idle_in_transaction_ms: i64,
    /// How long a committed pending ledger claim lives before another claimant may take it over. Only
    /// a claim made outside a transaction ever commits pending.
    pub claim_ttl_ms: i64,
}

/// The defaults: a lock wait of two seconds inside a statement of five, inside a call of ten.
pub const DEFAULT: Settings = Settings {
    acquire_timeout_ms: 5_000,
    lock_timeout_ms: 2_000,
    statement_timeout_ms: 5_000,
    op_timeout_ms: 10_000,
    idle_in_transaction_ms: 60_000,
    claim_ttl_ms: 300_000,
};

impl Default for Settings {
    fn default() -> Self {
        DEFAULT
    }
}

/// How many times a claim reads the key again when the row it lost to vanished before the read.
pub const CLAIM_READS: u32 = 3;

/// Refuse the settings a store is built with, as a `bad_handle` error naming the first one refused:
/// a duration that is not positive, or timeouts out of order (lock wait, then statement, then call).
pub fn check_config(settings: &Settings) -> Result<(), Error> {
    let named = [
        ("acquire_timeout_ms", settings.acquire_timeout_ms),
        ("lock_timeout_ms", settings.lock_timeout_ms),
        ("statement_timeout_ms", settings.statement_timeout_ms),
        ("op_timeout_ms", settings.op_timeout_ms),
        ("idle_in_transaction_ms", settings.idle_in_transaction_ms),
        ("claim_ttl_ms", settings.claim_ttl_ms),
    ];
    if let Some((name, value)) = named.iter().find(|(_, value)| *value <= 0) {
        return Err(Error::bad_handle(format_args!("{name} must be positive, got {value}")));
    }
    if settings.lock_timeout_ms > settings.statement_timeout_ms {
        return Err(Error::bad_handle(format_args!(
            "lock_timeout_ms {} must not exceed statement_timeout_ms {}",
            settings.lock_timeout_ms, settings.statement_timeout_ms
        )));
    }
    if settings.statement_timeout_ms >= settings.op_timeout_ms {
        return Err(Error::bad_handle(format_args!(
            "statement_timeout_ms {} must be below op_timeout_ms {}, so the server answers first",
            settings.statement_timeout_ms, settings.op_timeout_ms
        )));
    }
    Ok(())
}

/// `value`, a `u64` version, sequence or limit, as the signed `bigint` a column holds. A value past
/// `i64::MAX` is a `codec` error: Postgres has no unsigned integer, and wrapping would store a
/// negative version.
pub fn check_bigint(what: &str, value: u64) -> Result<i64, Error> {
    i64::try_from(value).map_err(|_| Error::codec(format_args!("{what} {value} does not fit a Postgres bigint")))
}

/// `value` clamped into `bigint`, for a bound where any value past `i64::MAX` means "no bound": a
/// limit, an offset, a load's `after`.
pub(crate) fn clamp_bigint(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_config_branches() {
        let with = |f: fn(&mut Settings)| {
            let mut s = DEFAULT;
            f(&mut s);
            check_config(&s).map_err(|e| e.to_string())
        };
        let cases: Vec<(&str, Result<(), String>)> = vec![
            ("defaults", with(|_| {})),
            ("zero_lock", with(|s| s.lock_timeout_ms = 0)),
            ("negative_ttl", with(|s| s.claim_ttl_ms = -1)),
            ("lock_past_statement", with(|s| s.lock_timeout_ms = 6_000)),
            ("statement_at_op", with(|s| s.statement_timeout_ms = 10_000)),
        ];
        let want: Vec<Result<(), &str>> = vec![
            Ok(()),
            Err("bad_handle: lock_timeout_ms must be positive, got 0"),
            Err("bad_handle: claim_ttl_ms must be positive, got -1"),
            Err("bad_handle: lock_timeout_ms 6000 must not exceed statement_timeout_ms 5000"),
            Err(
                "bad_handle: statement_timeout_ms 10000 must be below op_timeout_ms 10000, so the server answers first",
            ),
        ];
        for ((name, got), want) in cases.into_iter().zip(want) {
            assert_eq!(got, want.map_err(str::to_owned), "{name}");
        }
    }

    #[test]
    fn bigint_bounds() {
        assert_eq!(check_bigint("version", 7), Ok(7));
        assert_eq!(check_bigint("version", i64::MAX as u64), Ok(i64::MAX));
        assert_eq!(
            check_bigint("version", i64::MAX as u64 + 1).map_err(|e| e.to_string()),
            Err("codec: version 9223372036854775808 does not fit a Postgres bigint".to_owned())
        );
        assert_eq!(clamp_bigint(u64::MAX), i64::MAX);
        assert_eq!(clamp_bigint(3), 3);
    }
}

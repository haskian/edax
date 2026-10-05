//! The Lua every ledger write runs, and what a Redis error reply means.
//!
//! Every write is one script, so the compare and the write are one atomic step: a `GET` then a
//! `SET` from the client would leave exactly the gap the fence exists to close. Each script compares
//! the stored value with the caller's claim handle **exactly** (see [`crate::ledger`]), never a
//! prefix of a width the caller chose. The forget script compares with the value the caller read. The bodies are constants, byte for byte, so two SDKs
//! registering a script share one `EVALSHA` digest instead of each caching its own reformatting.
//! `testdata/golden/redis_scripts.json` pins the bytes and `testdata/golden/redis_fence.json` pins
//! what each script does, row by row, for a test to replay against a real server.
//!
//! Every script is idempotent, so a client that re-sends one after a lost reply gets the answer the
//! first send would have had. Each touches one key, so each is safe on Redis Cluster.

use edax_core::{Error, ErrorCode};

/// Claim a free key. `KEYS[1]` is the key; `ARGV[1]` is the claim value
/// ([`claim_value`](crate::claim_value)), `ARGV[2]` the claim TTL in milliseconds.
///
/// Returns nil when the key was free and is now this caller's claim, else the value the key holds,
/// unchanged. A re-sent claim whose first send landed returns the caller's own claim value;
/// [`claim_outcome`](crate::claim_outcome) reads that as won.
pub const CLAIM_SCRIPT: &str = "
local held = redis.call('GET', KEYS[1])
if held then
  return held
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return false
";

/// Settle a claim with its outcome. `KEYS[1]` is the key; `ARGV[1]` is the claim handle, `ARGV[2]`
/// the settled value ([`settled_value`](crate::settled_value)), `ARGV[3]` the record TTL in
/// milliseconds.
///
/// Writes `ARGV[2]` and returns `1` while the key holds this claim, or already holds `ARGV[2]` (a
/// re-sent complete). Returns `0` and writes nothing when the key is free or holds anything else:
/// the claim lapsed, and the key is free or another claimant's.
pub const COMPLETE_SCRIPT: &str = "
local held = redis.call('GET', KEYS[1])
if held == ARGV[1] or held == ARGV[2] then
  redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
  return 1
end
return 0
";

/// Release a claim. `KEYS[1]` is the key; `ARGV[1]` is the claim handle.
///
/// Deletes the key and returns `1` only while it holds this claim. Returns `0` otherwise: the key is
/// free, another claimant holds it, or it holds this claim's settled record, whose `complete` landed
/// even though its reply was lost. That record is the outcome of committed work and stays.
pub const RELEASE_SCRIPT: &str = "
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
";

/// Forget a settled record. `KEYS[1]` is the key; `ARGV[1]` is the value the caller read from it
/// before [`forget_outcome`](crate::forget_outcome) decided to delete it.
///
/// Deletes the key and returns `1` only while it still holds that value. Returns `0` otherwise: the
/// key is free, or it changed since the read (the record lapsed and a new claim took the key), and
/// the new value stays. The body is [`RELEASE_SCRIPT`]'s, so both share one `EVALSHA` digest: each
/// deletes a key only while it holds the exact value the caller names.
pub const FORGET_SCRIPT: &str = RELEASE_SCRIPT;

/// Extend a claim's lease. `KEYS[1]` is the key; `ARGV[1]` is the claim handle, `ARGV[2]` the claim
/// TTL in milliseconds.
///
/// Resets the TTL and returns `1` only while the key holds this claim. Returns `0` otherwise, which
/// tells a worker its lease is gone before its side effects run, not after.
pub const EXTEND_SCRIPT: &str = "
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
";

/// The kernel error for a Redis error reply, `reply` being the whole reply text, prefix included
/// (`"WRONGTYPE Operation against a key holding the wrong kind of value"`).
///
/// The prefix decides the verdict, so every SDK sharing a ledger agrees on it:
/// - `WRONGTYPE`: the key holds something the ledger cannot read. Terminal `codec`, the code an
///   unreadable record replays under.
/// - `ERR`, `NOPERM`, `NOAUTH`, `WRONGPASS`: a refused command, script or credential. The same
///   request fails the same way every time, so terminal `bad_handle`.
/// - Anything else is transient `store`: a passing server state (`LOADING`, `BUSY`, `READONLY` during
///   a failover, `MASTERDOWN`, `CLUSTERDOWN`, `TRYAGAIN`, `MOVED`, `ASK`, `NOREPLICAS`, `OOM`,
///   `MISCONF`, `NOSCRIPT`) or a prefix this kernel does not know. That is `edax_core::is_terminal`'s
///   default: a wrongly transient failure is redelivered loudly, a wrongly terminal one is
///   dead-lettered.
pub fn reply_error(reply: &str) -> Error {
    let prefix = reply.split(' ').next().unwrap_or_default();
    let code = match prefix {
        "WRONGTYPE" => ErrorCode::Codec,
        "ERR" | "NOPERM" | "NOAUTH" | "WRONGPASS" => ErrorCode::BadHandle,
        _ => ErrorCode::Store,
    };
    Error::new(code, format_args!("redis replied {reply}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_error_branches() {
        let cases: [(&str, &str, ErrorCode); 7] = [
            (
                "wrong_type",
                "WRONGTYPE Operation against a key holding the wrong kind of value",
                ErrorCode::Codec,
            ),
            (
                "generic_err",
                "ERR invalid expire time in 'set' command",
                ErrorCode::BadHandle,
            ),
            (
                "no_permission",
                "NOPERM User idem has no permissions to run the 'evalsha' command",
                ErrorCode::BadHandle,
            ),
            (
                "read_only_replica",
                "READONLY You can't write against a read only replica.",
                ErrorCode::Store,
            ),
            (
                "loading",
                "LOADING Redis is loading the dataset in memory",
                ErrorCode::Store,
            ),
            (
                "unknown_prefix",
                "SOMEDAY a reply this kernel has never seen",
                ErrorCode::Store,
            ),
            ("empty", "", ErrorCode::Store),
        ];
        for (name, reply, code) in cases {
            let e = reply_error(reply);
            assert_eq!(e.code, code, "{name}");
            assert_eq!(e.message, format!("redis replied {reply}"), "{name}");
        }
    }

    /// Every script takes exactly one key and touches no other, which is what makes it cluster-safe.
    #[test]
    fn every_script_names_only_its_one_key() {
        for script in [
            CLAIM_SCRIPT,
            COMPLETE_SCRIPT,
            RELEASE_SCRIPT,
            FORGET_SCRIPT,
            EXTEND_SCRIPT,
        ] {
            assert!(script.contains("KEYS[1]") && !script.contains("KEYS[2]"), "{script}");
        }
    }
}

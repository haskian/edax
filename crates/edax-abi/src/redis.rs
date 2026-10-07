//! The `edax-redis` surface: the ledger's claim values, keys, settings, replies and scripts.

use crate::{claim_result, to_wire, ClaimResult};

/// A fresh claim's value for `payload_hash` under a new owner token, minted by the kernel and never
/// by the host. The value is also the claim's handle: pass it to the complete, release and extend
/// scripts. Non-deterministic. See [`edax_redis::new_claim`].
pub fn redis_new_claim(payload_hash: u64) -> Result<Vec<u8>, String> {
    edax_redis::new_claim(payload_hash).map_err(to_wire)
}

/// The value that settles the claim `claim`, over `record_json` as the host encoded it (with
/// [`encode_idempotency_record`](crate::encode_idempotency_record)). A handle the kernel never minted is a `codec:` error, and a pending
/// record a `bad_handle:` one. See [`edax_redis::settled_value`].
pub fn redis_settled_value(claim: &[u8], record_json: &str) -> Result<Vec<u8>, String> {
    edax_redis::settled_value(claim, record_json).map_err(to_wire)
}

/// Refuse a claim handle the kernel never minted, as a `codec:` error. See [`edax_redis::check_claim`].
pub fn redis_check_claim(claim: &[u8]) -> Result<(), String> {
    edax_redis::check_claim(claim).map_err(to_wire)
}

/// The Redis key for an idempotency key: `{prefix}{key}` as bytes. An empty prefix is a
/// `bad_handle:` error. See [`edax_redis::ledger_key`].
pub fn redis_ledger_key(prefix: &str, key: &str) -> Result<Vec<u8>, String> {
    edax_redis::ledger_key(prefix, key).map_err(to_wire)
}

/// Refuse the settings a ledger store is built with, as a `bad_handle:` error naming the first one
/// refused. See [`edax_redis::check_config`].
pub fn redis_check_config(
    prefix: &str,
    ttl_ms: i64,
    claim_ttl_ms: i64,
    op_timeout_ms: i64,
    heartbeat: bool,
) -> Result<(), String> {
    edax_redis::check_config(prefix, ttl_ms, claim_ttl_ms, op_timeout_ms, heartbeat).map_err(to_wire)
}

/// When beat `beat` of a heartbeat is due, in milliseconds of the clock `sent_at_ms` (the claim's send)
/// was read on. Rounded down, so a beat is never late. See [`edax_redis::heartbeat_due_ms`].
pub fn redis_heartbeat_due_ms(sent_at_ms: i64, claim_ttl_ms: i64, beat: u32) -> i64 {
    edax_redis::heartbeat_due_ms(sent_at_ms, claim_ttl_ms, beat)
}

/// What the complete script's reply means for `key`: `written` is whether it wrote the record. A
/// refused write is a `store:` error, since the outcome was not recorded. See
/// [`edax_redis::complete_outcome`].
pub fn redis_complete_outcome(key: &str, written: bool) -> Result<(), String> {
    edax_redis::complete_outcome(key, written).map_err(to_wire)
}

/// The error, in its wire form, a forget of `key` gives up with once the key changed on every read.
/// See [`edax_redis::forget_exhausted`].
pub fn redis_forget_exhausted(key: &str) -> String {
    to_wire(edax_redis::forget_exhausted(key))
}

/// The ledger settings a store is built with when its host overrides none, in milliseconds. A struct
/// rather than three calls, like [`RedisScripts`]. See [`edax_redis::DEFAULT_TTL_MS`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct RedisDefaults {
    /// The record TTL: the dedup window a settled outcome is replayed in.
    pub ttl_ms: i64,
    /// The claim TTL: how long a dead worker's claim blocks its key.
    pub claim_ttl_ms: i64,
    /// The deadline on each Redis call.
    pub op_timeout_ms: i64,
}

/// The kernel's ledger defaults. See [`RedisDefaults`].
pub fn redis_defaults() -> RedisDefaults {
    RedisDefaults {
        ttl_ms: edax_redis::DEFAULT_TTL_MS,
        claim_ttl_ms: edax_redis::DEFAULT_CLAIM_TTL_MS,
        op_timeout_ms: edax_redis::DEFAULT_OP_TIMEOUT_MS,
    }
}

/// How many times a forget reads a key that changed under it before it gives up as transient. A call
/// because the generator binds functions, not constants. See [`edax_redis::FORGET_READS`].
pub fn redis_forget_reads() -> u32 {
    edax_redis::FORGET_READS
}

/// What the claim script's `reply` to the caller's `claim` means: `won` with `claim` as the handle when
/// this caller holds the claim (the key was free, or the reply is the caller's own claim, re-sent after
/// a lost reply), else the record JSON that holds the key, for [`decode_prior_idempotency_record`](crate::decode_prior_idempotency_record). A
/// value no layout reads comes back as the kernel's unreadable record. `claim` comes first because the
/// reply is optional. See [`edax_redis::claim_outcome`].
pub fn redis_claim_outcome(claim: &[u8], reply: Option<Vec<u8>>) -> Result<ClaimResult, String> {
    edax_redis::claim_outcome(reply.as_deref(), claim)
        .map(claim_result)
        .map_err(to_wire)
}

/// The same claim verdict with an explicit presence bit for C callers. Alef's Go
/// marshaller represents both absent and empty optional byte slices as a null pointer.
/// The bit preserves an empty stored value so the kernel can refuse it as unreadable.
pub fn redis_claim_outcome_present(claim: &[u8], present: bool, reply: &[u8]) -> Result<ClaimResult, String> {
    edax_redis::claim_outcome(present.then_some(reply), claim)
        .map(claim_result)
        .map_err(to_wire)
}

/// The kernel error for a Redis error reply, prefix included, in its wire form
/// `"<code>: <message>"`. See [`edax_redis::reply_error`].
pub fn redis_reply_error(reply: &str) -> String {
    to_wire(edax_redis::reply_error(reply))
}

/// Whether forgetting `key`, which held `held` when it was read, deletes it: `true` means run the
/// forget script with `held` as its argument, `false` means the key was free and nothing is sent. A
/// pending claim is refused as `in_progress:`, naming `key`. See [`edax_redis::forget_outcome`].
pub fn redis_forget_outcome(key: &str, held: Option<Vec<u8>>) -> Result<bool, String> {
    Ok(
        match edax_redis::forget_outcome(key, held.as_deref()).map_err(to_wire)? {
            edax_redis::ForgetOutcome::Nothing => false,
            edax_redis::ForgetOutcome::Delete => true,
        },
    )
}

/// The forget verdict with an explicit presence bit, as in [`redis_claim_outcome_present`].
pub fn redis_forget_outcome_present(key: &str, present: bool, held: &[u8]) -> Result<bool, String> {
    Ok(matches!(
        edax_redis::forget_outcome(key, present.then_some(held)).map_err(to_wire)?,
        edax_redis::ForgetOutcome::Delete
    ))
}

/// The five kernel scripts, byte for byte, so every SDK's `EVALSHA` names one digest. A struct
/// rather than five calls, like [`SplitError`](crate::SplitError). See [`edax_redis::CLAIM_SCRIPT`] and its siblings.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct RedisScripts {
    pub claim: String,
    pub complete: String,
    pub release: String,
    pub extend: String,
    pub forget: String,
}

/// The five kernel scripts. See [`RedisScripts`].
pub fn redis_scripts() -> RedisScripts {
    RedisScripts {
        claim: edax_redis::CLAIM_SCRIPT.to_owned(),
        complete: edax_redis::COMPLETE_SCRIPT.to_owned(),
        release: edax_redis::RELEASE_SCRIPT.to_owned(),
        extend: edax_redis::EXTEND_SCRIPT.to_owned(),
        forget: edax_redis::FORGET_SCRIPT.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::*;
    use crate::*;

    /// What the boundary adds to the ledger kernel: the claim outcome crosses as an optional record.
    /// The value layout is the kernel's, pinned by `testdata/golden/redis_value.json`.
    #[test]
    fn redis_value_crosses_as_owner_and_record() {
        let claim = redis_new_claim(HASH).unwrap();
        assert_ne!(claim, redis_new_claim(HASH).unwrap(), "each claim mints its own owner");
        assert_eq!(redis_check_claim(&claim), Ok(()));

        // A settled record is passed through untouched, so a result never round-trips through a
        // second serializer on its way to the ledger.
        let done = encode_idempotency_record(HASH, false, RESULT, None, None).unwrap();
        let settled = redis_settled_value(&claim, &done).unwrap();
        assert_eq!(&settled[..33], &claim[..33], "the claim's own owner token");

        let outcome = |claim: &[u8], reply: Option<Vec<u8>>| {
            let got = redis_claim_outcome(claim, reply).unwrap();
            (got.won, got.handle, got.prior)
        };
        assert_eq!(outcome(&claim, None), (true, Some(claim.clone()), None), "a free key");
        assert_eq!(
            outcome(&claim, Some(claim.clone())),
            (true, Some(claim.clone()), None),
            "a re-sent claim"
        );
        assert_eq!(outcome(&claim, Some(settled)), (false, None, Some(done)));
        let (won, _, pending) = outcome(&redis_new_claim(HASH).unwrap(), Some(claim));
        assert!(!won, "another claim holds the key");
        assert_eq!(
            decode_idempotency_record(&pending.expect("the record that holds the key"))
                .unwrap()
                .state,
            "pending"
        );
    }

    /// The settings and the replies cross with the kernel's codes: a refused setting or command is a
    /// terminal `bad_handle:`, a passing server state a transient `store:`.
    #[test]
    fn redis_refusals_cross_with_the_kernels_codes() {
        assert_eq!(redis_ledger_key("idem:", "k"), Ok(b"idem:k".to_vec()));
        assert_eq!(redis_check_config("idem:", 1, 4, 1, true), Ok(()));
        assert_eq!(redis_heartbeat_due_ms(1_000, 6_000, 1), 3_000);
        assert_eq!(redis_forget_reads(), 3);
        assert_eq!(redis_complete_outcome("k", true), Ok(()));
        assert!(
            redis_complete_outcome("k", false).is_err_and(|e| e.starts_with("store: ")),
            "a refused complete is transient"
        );
        assert!(redis_forget_exhausted("k").starts_with("store: "));
        for refused in [
            redis_ledger_key("", "k").map(|_| ()),
            redis_check_config("", 1, 1, 1, false),
            redis_check_config("idem:", 0, 1, 1, false),
            redis_check_claim(b""),
            redis_settled_value(
                &redis_new_claim(HASH).unwrap(),
                &encode_idempotency_record(HASH, true, "null", None, None).unwrap(),
            )
            .map(|_| ()),
        ] {
            let wire = refused.unwrap_err();
            assert!(
                wire.starts_with("bad_handle: ") || wire.starts_with("codec: "),
                "{wire}"
            );
        }
        assert!(redis_reply_error("ERR invalid expire time").starts_with("bad_handle: "));
        assert!(redis_reply_error("READONLY replica").starts_with("store: "));
        let scripts = redis_scripts();
        assert_eq!(
            [scripts.claim, scripts.complete, scripts.release, scripts.extend],
            [
                edax_redis::CLAIM_SCRIPT,
                edax_redis::COMPLETE_SCRIPT,
                edax_redis::RELEASE_SCRIPT,
                edax_redis::EXTEND_SCRIPT
            ]
        );
    }
}

#[cfg(test)]
mod presence_tests {
    use super::*;

    #[test]
    fn explicit_presence_preserves_optional_bytes() {
        let claim = redis_new_claim(42).unwrap();
        for (name, value) in [
            ("absent", None),
            ("empty_stored_value", Some(Vec::new())),
            ("own_claim", Some(claim.clone())),
            ("unreadable_stored_value", Some(b"corrupt".to_vec())),
        ] {
            let present = value.is_some();
            let bytes = value.as_deref().unwrap_or_default();
            let expected = redis_claim_outcome(&claim, value.clone()).unwrap();
            let actual = redis_claim_outcome_present(&claim, present, bytes).unwrap();
            assert_eq!(actual.won, expected.won, "{name}");
            assert_eq!(actual.handle, expected.handle, "{name}");
            assert_eq!(actual.prior, expected.prior, "{name}");
            assert_eq!(
                redis_forget_outcome_present("payments:invoice-1042", present, bytes),
                redis_forget_outcome("payments:invoice-1042", value),
                "{name}"
            );
        }
    }
}

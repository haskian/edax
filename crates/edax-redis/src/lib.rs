//! edax-redis — the Redis idempotency-ledger kernel.
//!
//! A Redis ledger's bytes are a cross-language contract: SDKs pointed at one key space must agree
//! on the key, on what a stored value looks like, on the Lua each write runs and on what a reply
//! means, or one takes over a claim another still holds. Those decisions live here, in Rust, so
//! every SDK ships only the client calls around them and the TTL values it was configured with. No
//! Redis client library is involved.
//!
//! - [`ledger_key`] / [`check_config`] / [`DEFAULT_TTL_MS`] — the key, the settings a store refuses, and
//!   the defaults it is built with.
//! - [`heartbeat_due_ms`] — when each renewal of a held claim is sent.
//! - [`new_claim`] / [`claim_value`] — a claim's value, which is also its handle.
//! - [`settled_value`] / [`check_claim`] — the value that settles a claim, and the handle check.
//! - [`split_value`] — a stored value read back apart into its owner and its record.
//! - [`claim_outcome`] — whether a claim's reply means won or a prior record, as the backend-neutral
//!   `edax_cqrs::Claimed`.
//! - [`complete_outcome`] — whether a complete's reply recorded the outcome.
//! - [`forget_outcome`] / [`forget_exhausted`] — whether a forget deletes the value it read, or refuses
//!   a pending claim, and the error it gives up with when the key keeps changing.
//! - [`CLAIM_SCRIPT`] / [`COMPLETE_SCRIPT`] / [`RELEASE_SCRIPT`] / [`FORGET_SCRIPT`] /
//!   [`EXTEND_SCRIPT`] — the Lua, byte for byte, each comparing the stored value with the caller's
//!   handle, or the value it read, exactly.
//! - [`reply_error`] — the verdict on a Redis error reply.
//!
//! `testdata/golden/redis_value.json`, `redis_ledger.json`, `redis_scripts.json` and
//! `redis_fence.json` pin what is here.

// Every public item is a cross-language contract, so it states what it promises.
#![warn(missing_docs)]

pub mod ledger;
pub mod scripts;

pub use ledger::{
    check_claim, check_config, claim_outcome, claim_value, complete_outcome, forget_exhausted, forget_outcome,
    heartbeat_due_ms, ledger_key, new_claim, settled_value, split_value, ForgetOutcome, DEFAULT_CLAIM_TTL_MS,
    DEFAULT_OP_TIMEOUT_MS, DEFAULT_TTL_MS, FORGET_READS, HEARTBEATS_PER_TTL, LAYOUT_VERSION, MAX_TTL_MS, OWNER_LEN,
};
pub use scripts::{reply_error, CLAIM_SCRIPT, COMPLETE_SCRIPT, EXTEND_SCRIPT, FORGET_SCRIPT, RELEASE_SCRIPT};

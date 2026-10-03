//! edax-redis — the Redis idempotency-ledger kernel.
//!
//! A Redis ledger's bytes are a cross-language contract: SDKs pointed at one key space must agree
//! on the key, on what a stored value looks like, on the Lua each write runs and on what a reply
//! means, or one takes over a claim another still holds. Those decisions live here, in Rust, so
//! every SDK ships only the client calls around them and the TTL values it was configured with. No
//! Redis client library is involved.
//!
//! - [`ledger_key`] / [`check_config`] — the key, and the settings a store refuses.
//! - [`new_claim`] / [`new_owner`] / [`claim_value`] — a claim's value, which is also its handle.
//! - [`settled_value`] / [`check_claim`] — the value that settles a claim, and the handle check.
//! - [`split_value`] — a stored value read back apart into its owner and its record.
//! - [`claim_outcome`] — whether a claim's reply means won or a prior record.
//! - [`CLAIM_SCRIPT`] / [`COMPLETE_SCRIPT`] / [`RELEASE_SCRIPT`] / [`EXTEND_SCRIPT`] — the Lua, byte
//!   for byte, each comparing the stored value with the caller's handle exactly.
//! - [`reply_error`] — the verdict on a Redis error reply.
//!
//! `testdata/golden/redis_value.json`, `redis_ledger.json`, `redis_scripts.json` and
//! `redis_fence.json` pin what is here.

// Every public item is a cross-language contract, so it states what it promises.
#![warn(missing_docs)]

pub mod ledger;
pub mod scripts;

pub use ledger::{
    check_claim, check_config, claim_outcome, claim_value, ledger_key, new_claim, new_owner, settled_value,
    split_value, ClaimOutcome, HEARTBEATS_PER_TTL, LAYOUT_VERSION, MAX_TTL_MS, OWNER_LEN,
};
pub use scripts::{reply_error, CLAIM_SCRIPT, COMPLETE_SCRIPT, EXTEND_SCRIPT, RELEASE_SCRIPT};

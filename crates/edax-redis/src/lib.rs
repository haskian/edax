//! edax-redis — the Redis idempotency-ledger kernel.
//!
//! A Redis ledger's bytes are a cross-language contract: two SDKs pointed at one key space must
//! agree on what a stored value looks like and on the Lua a fenced write runs, or one takes over a
//! claim the other still holds. Those bytes live here, in Rust, so every SDK ships only the client
//! calls around them. No Redis client library is involved.
//!
//! - [`new_owner`] / [`OWNER_LEN`] — the fence token: a fixed-width lowercase hex string that
//!   prefixes every stored value.
//! - [`claim_value`] / [`record_value`] — the stored value, owner token then the kernel's
//!   idempotency-record JSON, under one key so one TTL governs both.
//! - [`split_value`] — the same value read back apart into its owner and its record.
//! - [`FENCED_SET_SCRIPT`] / [`FENCED_DEL_SCRIPT`] — the compare-and-write Lua, byte for byte, so
//!   two SDKs registering it land on one `EVALSHA` digest.
//!
//! Everything around those bytes is orchestration, and stays in each SDK: key composition
//! (`{prefix}{key}`), the TTL defaults, the claim-retry bound, and the `complete` / `release`
//! decision trees. `testdata/golden/redis_value.json` and `testdata/golden/redis_scripts.json` pin
//! what is here.

// Every public item is a cross-language contract, so it states what it promises.
#![warn(missing_docs)]
// This kernel runs inside a native binding: an unwinding panic crosses the host language's FFI
// boundary, where it cannot be caught. Every fallible step returns `AdapterError` instead.
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

pub mod ledger;

pub use ledger::{
    claim_value, new_owner, record_value, split_value, FENCED_DEL_SCRIPT, FENCED_SET_SCRIPT, OWNER_LEN,
};

//! The idempotency store view: claim, complete and release for one key.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use edax_core::{lock, AdapterError};
use serde_json::Value;

use crate::tx::{held_by_other, store_view, MemoryState, MemoryTransactor, WriteSet};

/// In-memory idempotency store for development and tests: claim / complete / release over the
/// transactor's map. Records are opaque JSON bytes. [`claim`](Self::claim) writes a *pending*
/// record atomically (insert-if-absent) and hands back any prior record so the caller decides replay /
/// payload-mismatch / still-in-progress; [`complete`](Self::complete) overwrites it with the terminal
/// outcome; [`release`](Self::release) drops it so a transient failure can retry. Under an open
/// token all three are staged and land or vanish with the transaction, so a claim rolled back with
/// its events releases the key. No TTL — a dev store keeps records for the process lifetime. Not for
/// production: a durable cross-process store is needed for effectively-once across hosts.
#[derive(Clone)]
pub struct MemoryIdempotencyStore {
    pub(crate) state: Arc<Mutex<MemoryState>>,
}

store_view!(MemoryIdempotencyStore, idempotency_store);

/// The record a losing claimant reads while another open transaction holds `key`: the holder's
/// record, re-masked as pending. The holder may still roll back, so its staged outcome is not one
/// to replay. Bytes that are not a kernel record cross as they are.
fn masked_pending(bytes: &[u8]) -> Vec<u8> {
    let Ok(record) = edax_cqrs::decode_record::<Value>(bytes) else { return bytes.to_vec() };
    edax_cqrs::encode_record(&edax_cqrs::Record::<Value>::pending(record.payload_hash)).unwrap_or_else(|_| bytes.to_vec())
}

/// The record another open transaction holds for `key`, if any. A staged *release* is not a hold:
/// the holder has nothing left to replay or to overwrite.
fn claim_held_by_other<'a>(others: &'a HashMap<i64, WriteSet>, key: &str) -> Option<&'a [u8]> {
    match held_by_other(others, |set| set.claims.get(key)) {
        Some(Some(record)) => Some(record),
        _ => None,
    }
}

/// Refuse a write to a key another open transaction holds: the holder's commit would overwrite a
/// `complete` staged elsewhere, and a losing claimant's `release` would free the holder's claim.
/// A claim carries no version, so the conflict reports none; the `concurrency` code is the whole
/// reload-and-retry signal.
fn refuse_held_claim(others: &HashMap<i64, WriteSet>, key: &str) -> Result<(), AdapterError> {
    match claim_held_by_other(others, key) {
        Some(_) => Err(AdapterError::Concurrency { expected: 0, stored: 0 }),
        None => Ok(()),
    }
}

impl MemoryIdempotencyStore {
    /// Atomically claim `key` under `tx` by writing the kernel's *pending* record only if absent.
    /// Returns `Ok(None)` if this caller won the claim, or `Ok(Some(prior_bytes))` if the key was
    /// already present — the caller hands the prior record to the kernel to decide replay, payload
    /// mismatch, or that the winner has not completed yet. A key held by another open transaction
    /// reads as its pending record. `Err` if the pending record cannot be encoded, which a fixed shape
    /// over a `u64` never is, or if `tx` names no open transaction.
    pub fn claim(&self, tx: i64, key: &str, payload_hash: u64) -> Result<Option<Vec<u8>>, AdapterError> {
        let pending = edax_cqrs::Record::<Value>::pending(payload_hash);
        let bytes = edax_cqrs::encode_record(&pending).map_err(AdapterError::codec)?;
        lock(&self.state).write(tx, |committed, others, own| {
            // A key another open transaction holds reads as pending; the committed record decides
            // once the holder ends.
            if let Some(record) = claim_held_by_other(others, key) {
                return Ok(Some(masked_pending(record)));
            }
            let prior = match own.claims.get(key) {
                Some(Some(record)) => Some(record.clone()),
                // Released in this transaction: the key is free again for it.
                Some(None) => None,
                None => committed.claims.get(key).cloned(),
            };
            if prior.is_none() {
                own.claims.insert(key.to_owned(), Some(bytes));
            }
            Ok(prior)
        })
    }

    /// Write the terminal record bytes for `key` (result or error) under `tx`, replacing the pending
    /// claim. A key another open transaction holds is a conflict: that transaction's commit would
    /// overwrite this record. `Err` also if `tx` names no open transaction.
    pub fn complete(&self, tx: i64, key: &str, record: &[u8]) -> Result<(), AdapterError> {
        lock(&self.state).write(tx, |_, others, own| {
            refuse_held_claim(others, key)?;
            own.claims.insert(key.to_owned(), Some(record.to_vec()));
            Ok(())
        })
    }

    /// Drop `key` under `tx`, releasing a claim (e.g. after a transient failure that should be
    /// retried). A key another open transaction holds is a conflict: releasing it would free the
    /// winner's claim. `Err` also if `tx` names no open transaction.
    pub fn release(&self, tx: i64, key: &str) -> Result<(), AdapterError> {
        lock(&self.state).write(tx, |_, others, own| {
            refuse_held_claim(others, key)?;
            own.claims.insert(key.to_owned(), None);
            Ok(())
        })
    }
}

#[cfg(test)]
// A test asserts on the value it just built; a panic there is the failure report.
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::tx::NO_TX;
    use serde_json::json;
    #[test]
    fn claim_wins_once_then_returns_the_prior_record() {
        let store = MemoryIdempotencyStore::new();
        let hash = 11_234_062_831_410_934_249u64; // a real 64-bit payload hash, past 2^53

        assert_eq!(store.claim(NO_TX, "cmd-key-1", hash), Ok(None), "first claim wins");

        // The pending record is the kernel's: explicit `pending` so a completed `null` result is not
        // mistaken for an in-flight claim.
        let pending = store.claim(NO_TX, "cmd-key-1", hash).unwrap().expect("second claim loses");
        let pending: Value = serde_json::from_slice(&pending).expect("pending record is JSON");
        assert_eq!(pending, json!({"payload_hash": hash, "result": null, "error": null, "pending": true}));

        // complete overwrites the pending claim with the terminal outcome.
        let done = br#"{"payload_hash":11234062831410934249,"result":{"invoice_id":"invoice-7"},"error":null,"pending":false}"#;
        store.complete(NO_TX, "cmd-key-1", done).unwrap();
        assert_eq!(store.claim(NO_TX, "cmd-key-1", hash).unwrap().as_deref(), Some(&done[..]));

        // release drops the key, so the next claim wins again.
        store.release(NO_TX, "cmd-key-1").unwrap();
        assert_eq!(store.claim(NO_TX, "cmd-key-1", hash), Ok(None), "released key is claimable");
        store.release(NO_TX, "cmd-key-404").unwrap(); // releasing an unknown key is a no-op
    }

    /// The claim is an insert-if-absent under one lock, so under real contention exactly one caller
    /// wins and every loser reads back the winner's pending record — the atomic-claim contract.
    #[test]
    fn concurrent_claims_have_one_winner() {
        let store = MemoryIdempotencyStore::new();
        let hash = 11_234_062_831_410_934_249u64;
        let outcomes: Vec<Option<Vec<u8>>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8).map(|_| s.spawn(|| store.claim(NO_TX, "cmd-key-1", hash))).collect();
            handles.into_iter().map(|h| h.join().expect("claimant thread").expect("claim encodes")).collect()
        });
        assert_eq!(outcomes.iter().filter(|o| o.is_none()).count(), 1, "exactly one claim wins");
        let pending = format!(r#"{{"payload_hash":{hash},"result":null,"error":null,"pending":true}}"#).into_bytes();
        for prior in outcomes.iter().flatten() {
            assert_eq!(prior, &pending, "every loser sees the winner's pending record");
        }
    }
}

//! The idempotency store view: claim, complete and release for one key, and forget for a settled one.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use edax_core::{lock, Error, ErrorCode, TokenMint};
use serde_json::Value;

use crate::tx::{held_by_other, store_view, Committed, Held, MemoryState, MemoryTransactor, WriteSet};

/// In-memory idempotency store for development and tests: claim / complete / release over the
/// transactor's map. Records are opaque JSON bytes. [`claim`](Self::claim) writes a *pending*
/// record atomically (insert-if-absent) and hands back either the handle that settles it or any prior
/// record, so the caller decides replay / payload-mismatch / still-in-progress;
/// [`complete`](Self::complete) overwrites the claim with the terminal outcome;
/// [`release`](Self::release) drops it so a transient failure can retry; [`forget`](Self::forget)
/// deletes a settled record so a re-driven message runs again. Under an open token all four are
/// staged and land or vanish with the transaction, so a claim rolled back with its events releases
/// the key.
///
/// Both settling writes are fenced on the claim's handle, as Redis fences on the exact claim value: they
/// act only while the key still holds the claim that handle names. A claim lapses only through
/// [`expire`](Self::expire), the test hook that stands in for a lease running out, so a test can
/// rehearse a takeover and a stale owner acting late. Records otherwise live for the process lifetime.
/// Not for production: a durable cross-process store is needed for effectively-once across hosts.
#[derive(Clone)]
pub struct MemoryIdempotencyStore {
    pub(crate) state: Arc<Mutex<MemoryState>>,
}

store_view!(MemoryIdempotencyStore, idempotency_store);

/// What [`MemoryIdempotencyStore::claim`] found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Claimed {
    /// This caller won the key: settle it with this handle.
    Won(Vec<u8>),
    /// The key was already held: the record bytes the kernel classifies.
    Prior(Vec<u8>),
}

/// Process-wide, so a handle from one claim never names another, across stores too: a lapsed claim and
/// the one that took its key over always differ.
static CLAIMS: TokenMint = TokenMint::new();

/// The handle naming claim `id`: opaque bytes to every caller.
fn handle_of(id: i64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

/// The record a losing claimant reads while another open transaction holds `key`: the holder's
/// record, re-masked as pending under the hash `edax_cqrs::decode_prior` reads from it. The holder may
/// still roll back, so its staged outcome is not one to replay, even one no reader can decode.
fn masked_pending(bytes: &[u8], claimant_hash: u64) -> Result<Vec<u8>, Error> {
    let held = edax_cqrs::decode_prior::<Value>(bytes, claimant_hash);
    edax_cqrs::encode_record(&edax_cqrs::Record::<Value>::pending(held.payload_hash))
}

/// The record another open transaction holds for `key`, if any: its staged record, or — for a
/// staged release of a committed claim — the committed claim it is about to delete. Either is a
/// hold, as a row lock would be in a durable ledger. A release of a key with nothing committed
/// stages nothing (see [`MemoryIdempotencyStore::release`]), so it holds nothing.
fn claim_held_by_other<'a>(committed: &'a Committed, others: &'a HashMap<i64, WriteSet>, key: &str) -> Option<&'a [u8]> {
    held_by_other(others, |set| set.claims.get(key)).and_then(|staged| {
        staged.as_ref().or_else(|| committed.claims.get(key)).map(|held| held.record.as_slice())
    })
}

/// What `key` holds as this transaction sees it: its own staged write, else the committed one.
fn view<'a>(committed: &'a Committed, own: &'a WriteSet, key: &str) -> Option<&'a Held> {
    match own.claims.get(key) {
        Some(staged) => staged.as_ref(),
        None => committed.claims.get(key),
    }
}

/// Refuse a write to a key another open transaction holds: the holder's commit would overwrite a
/// `complete` staged elsewhere, and a losing claimant's `release` would free the holder's claim.
/// A claim carries no version, so the conflict names the key instead; the `concurrency` code is the
/// whole reload-and-retry signal.
fn refuse_held_claim(committed: &Committed, others: &HashMap<i64, WriteSet>, key: &str) -> Result<(), Error> {
    match claim_held_by_other(committed, others, key) {
        Some(_) => Err(held_claim(key)),
        None => Ok(()),
    }
}

/// The conflict a write to a key another open transaction holds gets.
pub(crate) fn held_claim(key: &str) -> Error {
    Error::new(ErrorCode::Concurrency, format_args!("idempotency key {key} is held by another open transaction"))
}

/// The refusal of a `complete` whose handle no longer names the key's claim. A store error, as the Redis
/// fence reports it: the guard answers it with a release, which the same fence makes a no-op.
pub(crate) fn lapsed_claim(key: &str) -> Error {
    Error::store(format_args!(
        "idempotency key {key} no longer holds the claim this handle names: it lapsed and was taken over, or \
         was settled with another record"
    ))
}

impl Held {
    /// Whether this is the pending claim `handle` names.
    fn claimed_by(&self, handle: &[u8]) -> bool {
        self.claim.is_some_and(|id| handle_of(id) == handle)
    }
}

impl MemoryIdempotencyStore {
    /// Atomically claim `key` under `tx` by writing the kernel's *pending* record only if absent.
    /// Returns [`Claimed::Won`] with the handle that settles the claim, or [`Claimed::Prior`] with the
    /// record already present — the caller hands it to the kernel to decide replay, payload mismatch,
    /// or that the winner has not completed yet. A key held by another open transaction reads as its
    /// pending record. `Err` if the pending record cannot be encoded, which a fixed shape over a `u64`
    /// never is, or if `tx` names no open transaction.
    pub fn claim(&self, tx: i64, key: &str, payload_hash: u64) -> Result<Claimed, Error> {
        let pending = edax_cqrs::Record::<Value>::pending(payload_hash);
        let record = edax_cqrs::encode_record(&pending).map_err(Error::codec)?;
        lock(&self.state).write(tx, |committed, others, own| {
            // A key another open transaction holds reads as pending; the committed record decides
            // once the holder ends.
            if let Some(held) = claim_held_by_other(committed, others, key) {
                return masked_pending(held, payload_hash).map(Claimed::Prior);
            }
            if let Some(prior) = view(committed, own, key) {
                return Ok(Claimed::Prior(prior.record.clone()));
            }
            let id = CLAIMS.issue();
            own.claims.insert(key.to_owned(), Some(Held { record, claim: Some(id) }));
            Ok(Claimed::Won(handle_of(id)))
        })
    }

    /// Write the terminal record bytes for `key` (result or error) under `tx`, replacing the pending
    /// claim `handle` names. Idempotent: writing the record the key already settled on is a no-op. A key
    /// another open transaction holds is a conflict, because that transaction's commit would overwrite
    /// this record; a key that no longer holds this claim (it was expired and taken over, released, or
    /// settled differently) is refused as a store error and nothing is written. `Err` also if `tx` names
    /// no open transaction.
    pub fn complete(&self, tx: i64, key: &str, handle: &[u8], record: &[u8]) -> Result<(), Error> {
        lock(&self.state).write(tx, |committed, others, own| {
            refuse_held_claim(committed, others, key)?;
            match view(committed, own, key) {
                Some(held) if held.claimed_by(handle) => {
                    own.claims.insert(key.to_owned(), Some(Held { record: record.to_vec(), claim: None }));
                    Ok(())
                }
                Some(held) if held.claim.is_none() && held.record == record => Ok(()),
                _ => Err(lapsed_claim(key)),
            }
        })
    }

    /// Drop the claim `handle` names on `key` under `tx`, so a transient failure can retry. A key that
    /// no longer holds that claim is left alone: a settled record survives a release sent after a
    /// `complete` whose reply was lost, and a claim that took over a lapsed one survives its old owner.
    /// A key another open transaction holds is a conflict: releasing it would free the winner's claim.
    /// `Err` also if `tx` names no open transaction.
    ///
    /// With nothing committed for the key, the release drops this transaction's own staged claim and
    /// stages nothing: there is no row to delete, so there is nothing to hold, and a winner that
    /// commits meanwhile keeps its record. Over a committed claim it stages a delete, which holds
    /// the key until this transaction ends, as a row lock would.
    pub fn release(&self, tx: i64, key: &str, handle: &[u8]) -> Result<(), Error> {
        lock(&self.state).write(tx, |committed, others, own| {
            refuse_held_claim(committed, others, key)?;
            if !view(committed, own, key).is_some_and(|held| held.claimed_by(handle)) {
                return Ok(());
            }
            if committed.claims.contains_key(key) {
                own.claims.insert(key.to_owned(), None);
            } else {
                own.claims.remove(key);
            }
            Ok(())
        })
    }

    /// Delete the settled record `key` holds under `tx`, so the next claim wins and a re-driven
    /// message (one taken back from a dead-letter queue, say) runs its handler again. The record
    /// decides, read by the kernel: a pending claim is refused as `in_progress`, because its run may
    /// still complete; a completed or failed record is deleted, and so are bytes no reader can decode;
    /// an absent key is a no-op. A key another open transaction holds reads as pending, as it does for
    /// a claimant. `Err` also if `tx` names no open transaction.
    ///
    /// Not a [`release`](Self::release): a release is the claimant settling its own claim by handle,
    /// while a forget acts on an outcome that nobody owns any more, so it takes no handle. Over a
    /// committed record it stages a delete, which holds the key until this transaction ends.
    pub fn forget(&self, tx: i64, key: &str) -> Result<(), Error> {
        lock(&self.state).write(tx, |committed, others, own| {
            if claim_held_by_other(committed, others, key).is_some() {
                return Err(edax_cqrs::unsettled_claim(key));
            }
            let Some(held) = view(committed, own, key) else {
                return Ok(());
            };
            edax_cqrs::forget(key, &held.record)?;
            if committed.claims.contains_key(key) {
                own.claims.insert(key.to_owned(), None);
            } else {
                own.claims.remove(key);
            }
            Ok(())
        })
    }

    /// Lapse whatever `key` holds in committed state, as a lease or a retention TTL running out would:
    /// the next claim wins, and the old claim's handle settles nothing. A test hook, so memory can
    /// rehearse the takeover a store with expiring claims (Redis) produces. A write staged by an open
    /// transaction is not committed state, so it does not lapse. Returns whether anything lapsed.
    pub fn expire(&self, key: &str) -> bool {
        lock(&self.state).committed.claims.remove(key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::wire;
    use crate::tx::NO_TX;
    use serde_json::json;

    #[test]
    fn claim_wins_once_then_returns_the_prior_record() {
        let store = MemoryIdempotencyStore::new();
        let hash = 11_234_062_831_410_934_249u64; // a real 64-bit payload hash, past 2^53

        let Ok(Claimed::Won(handle)) = store.claim(NO_TX, "cmd-key-1", hash) else { panic!("first claim wins") };

        // The pending record is the kernel's: explicit `pending` so a completed `null` result is not
        // mistaken for an in-flight claim.
        let Ok(Claimed::Prior(pending)) = store.claim(NO_TX, "cmd-key-1", hash) else { panic!("second claim loses") };
        let pending: Value = serde_json::from_slice(&pending).expect("pending record is JSON");
        assert_eq!(pending, json!({"payload_hash": hash, "result": null, "error": null, "error_code": null, "pending": true}));

        // complete overwrites the pending claim with the terminal outcome.
        let done = br#"{"payload_hash":11234062831410934249,"result":{"invoice_id":"invoice-7"},"error":null,"pending":false}"#;
        store.complete(NO_TX, "cmd-key-1", &handle, done).unwrap();
        assert_eq!(store.claim(NO_TX, "cmd-key-1", hash), Ok(Claimed::Prior(done.to_vec())));

        // A record is nobody's claim: a release with the old handle keeps it, and an expiry frees it.
        store.release(NO_TX, "cmd-key-1", &handle).unwrap();
        assert_eq!(store.claim(NO_TX, "cmd-key-1", hash), Ok(Claimed::Prior(done.to_vec())));
        assert!(store.expire("cmd-key-1"));
        let Ok(Claimed::Won(again)) = store.claim(NO_TX, "cmd-key-1", hash) else { panic!("an expired key is claimable") };
        assert_ne!(again, handle, "every claim gets its own handle");
        store.release(NO_TX, "cmd-key-1", &again).unwrap();
        assert!(matches!(store.claim(NO_TX, "cmd-key-1", hash), Ok(Claimed::Won(_))), "a released key is claimable");
        store.release(NO_TX, "cmd-key-404", &again).unwrap(); // releasing an unknown key is a no-op
    }

    #[test]
    fn a_held_key_reads_as_pending_whatever_the_holder_staged() {
        // The holder may roll back, so a claimant never replays what it staged: a record or bytes no
        // reader can decode alike read as pending, under the hash `decode_prior` reads.
        const HASH: u64 = 11_234_062_831_410_934_249;
        let pending = |hash: u64| format!(r#"{{"payload_hash":{hash},"result":null,"error":null,"error_code":null,"pending":true}}"#).into_bytes();
        for (name, staged, claimant, want) in [
            ("a_settled_record", br#"{"payload_hash":11234062831410934249,"result":7,"error":null,"pending":false}"#.as_slice(), 1, pending(HASH)),
            ("a_wrong_type_keeps_its_hash", br#"{"payload_hash":11234062831410934249,"error":7}"#.as_slice(), 1, pending(HASH)),
            ("garbage_takes_the_claimants_hash", b"{not json".as_slice(), 1, pending(1)),
        ] {
            let store = MemoryIdempotencyStore::new();
            let holder = store.transactor().begin();
            let Ok(Claimed::Won(handle)) = store.claim(holder, "k", HASH) else { panic!("{name}: a free key") };
            store.complete(holder, "k", &handle, staged).unwrap();
            assert_eq!(store.claim(NO_TX, "k", claimant), Ok(Claimed::Prior(want)), "{name}");
        }
    }

    /// The claim is an insert-if-absent under one lock, so under real contention exactly one caller
    /// wins and every loser reads back the winner's pending record — the atomic-claim contract.
    #[test]
    fn concurrent_claims_have_one_winner() {
        let store = MemoryIdempotencyStore::new();
        let hash = 11_234_062_831_410_934_249u64;
        let outcomes: Vec<Claimed> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8).map(|_| s.spawn(|| store.claim(NO_TX, "cmd-key-1", hash))).collect();
            handles.into_iter().map(|h| h.join().expect("claimant thread").expect("claim encodes")).collect()
        });
        assert_eq!(outcomes.iter().filter(|o| matches!(o, Claimed::Won(_))).count(), 1, "exactly one claim wins");
        let pending = format!(r#"{{"payload_hash":{hash},"result":null,"error":null,"error_code":null,"pending":true}}"#).into_bytes();
        for outcome in &outcomes {
            if let Claimed::Prior(prior) = outcome {
                assert_eq!(prior, &pending, "every loser sees the winner's pending record");
            }
        }
    }

    #[test]
    fn forget_branches() {
        const HASH: u64 = 11_234_062_831_410_934_249;
        const DONE: &[u8] = br#"{"payload_hash":11234062831410934249,"result":{"invoice_id":"invoice-7"},"error":null,"error_code":null,"pending":false}"#;
        const FAILED: &[u8] = br#"{"payload_hash":11234062831410934249,"result":null,"error":"insufficient funds","error_code":"rejected","pending":false}"#;
        #[derive(Clone, Copy)]
        enum Seed {
            /// Nothing was ever claimed.
            Absent,
            /// A committed claim, still pending.
            Pending,
            /// A committed claim settled with this record.
            Settled(&'static [u8]),
            /// A pending claim another open transaction staged.
            HeldByAnother,
        }
        let in_progress = Err("in_progress: idempotency key req-A is claimed by a run that has not completed: forget it once the claim settles or lapses".to_owned());
        let cases: [(&str, Seed, Result<(), String>, bool); 6] = [
            ("absent_is_a_no_op", Seed::Absent, Ok(()), false),
            ("a_pending_claim_is_refused", Seed::Pending, in_progress.clone(), true),
            ("a_completed_record_is_deleted", Seed::Settled(DONE), Ok(()), false),
            ("a_failed_record_is_deleted", Seed::Settled(FAILED), Ok(()), false),
            // A cached codec failure in the kernel's reading: the re-drive a poisoned key needs.
            ("an_unreadable_record_is_deleted", Seed::Settled(b"{not json"), Ok(()), false),
            ("a_key_another_transaction_holds_is_refused", Seed::HeldByAnother, in_progress, true),
        ];
        for (name, seed, want, held_after) in cases {
            let store = MemoryIdempotencyStore::new();
            let holder = store.transactor().begin();
            match seed {
                Seed::Absent => {}
                Seed::Pending => assert!(matches!(store.claim(NO_TX, "req-A", HASH), Ok(Claimed::Won(_))), "{name}"),
                Seed::Settled(record) => {
                    let Ok(Claimed::Won(handle)) = store.claim(NO_TX, "req-A", HASH) else { panic!("{name}: a free key") };
                    store.complete(NO_TX, "req-A", &handle, record).unwrap();
                }
                Seed::HeldByAnother => assert!(matches!(store.claim(holder, "req-A", HASH), Ok(Claimed::Won(_))), "{name}"),
            }
            assert_eq!(wire(store.forget(NO_TX, "req-A")), want, "{name}");
            assert!(store.transactor().rollback(holder));
            let held = matches!(store.claim(NO_TX, "req-A", HASH), Ok(Claimed::Prior(_)));
            // A held-by-another key is free once its holder rolled back; anything else kept its record.
            let want_held = held_after && !matches!(seed, Seed::HeldByAnother);
            assert_eq!(held, want_held, "{name}: the key still holds a record");
        }
    }

    #[test]
    fn forget_is_staged_with_the_transaction() {
        // A rolled-back forget keeps the record; a committed one deletes it. A forget of a record this
        // transaction settled itself, with nothing committed, leaves nothing behind.
        const DONE: &[u8] = br#"{"payload_hash":7,"result":42,"error":null,"error_code":null,"pending":false}"#;
        let store = MemoryIdempotencyStore::new();
        let Ok(Claimed::Won(handle)) = store.claim(NO_TX, "req-A", 7) else { panic!("a free key") };
        store.complete(NO_TX, "req-A", &handle, DONE).unwrap();
        for commit in [false, true] {
            let tx = store.transactor().begin();
            store.forget(tx, "req-A").unwrap();
            assert!(matches!(store.claim(tx, "req-A", 7), Ok(Claimed::Won(_))), "commit {commit}: the transaction sees the delete");
            if commit {
                store.transactor().commit(tx).unwrap();
            } else {
                assert!(store.transactor().rollback(tx));
                assert_eq!(store.claim(NO_TX, "req-A", 7), Ok(Claimed::Prior(DONE.to_vec())), "a rollback keeps the record");
            }
        }
        // The committed transaction forgot the record and then won a fresh claim on the key.
        assert!(matches!(store.claim(NO_TX, "req-A", 7), Ok(Claimed::Prior(_))));

        let fresh = MemoryIdempotencyStore::new();
        let tx = fresh.transactor().begin();
        let Ok(Claimed::Won(handle)) = fresh.claim(tx, "req-B", 7) else { panic!("a free key") };
        fresh.complete(tx, "req-B", &handle, DONE).unwrap();
        fresh.forget(tx, "req-B").unwrap();
        fresh.transactor().commit(tx).unwrap();
        assert!(matches!(fresh.claim(NO_TX, "req-B", 7), Ok(Claimed::Won(_))), "nothing was left behind");
    }
}

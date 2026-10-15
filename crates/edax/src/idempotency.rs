//! The idempotency guard engine both write paths share: the [`IdempotencyStore`] seam, the claim it
//! grants, and `guarded`, the claim/run/settle orchestration behind [`idempotent`](crate::idempotent)
//! (commands) and [`idempotent_by_id`](crate::idempotent_by_id) (events). The kernel decides the ledger
//! key, the verdict on a prior record and what a failure caches.

use std::marker::PhantomData;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;

use edax_core::Tenancy;
use edax_cqrs::{claim_key, replay, settle_failure, Settle};
use edax_event::event_claim_key;
use serde::Serialize;

use crate::failure::{code_of, failure_message, reason_of, HandlerError};
use crate::Error;
// Kernel type; its wire shape and replay verdict are pinned by `testdata/golden/idempotency_record.json`.
pub use edax_cqrs::{Record, State};

/// Names one claim to the store that granted it. A fenced store (the Redis ledger) settles a key only
/// while the key still holds this exact claim, so a claim that lapsed and was taken over is never
/// settled by its old owner. A store with no fence hands back the empty handle and ignores it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClaimHandle(pub Vec<u8>);

/// The outcome of a [`claim`](IdempotencyStore::claim): this caller won (run the handler, then settle
/// with the handle), or a prior record already exists (pending, a cached result, or a cached error)
/// for the caller to act on.
pub enum Claim<O> {
    /// This caller holds the claim.
    Won(ClaimHandle),
    /// Another claim or a settled record holds the key.
    Prior(Record<O>),
}

/// Persists idempotency records behind an atomic claim. An in-process implementation is enough for a
/// single service; a cross-process store serializes the result itself. Methods take `&self` so a store
/// can be shared inside a middleware closure.
///
/// A store that persists the kernel's JSON record (`edax_cqrs::encode_record`) must round-trip `O`
/// unchanged. The codec is exact for integers inside the `i64`/`u64` range and reads anything wider back
/// as an `f64`, so a result carrying a wider integer must carry it as a string. It reads the record that
/// holds a key with `edax_cqrs::decode_prior`, so bytes no reader can decode as a `Record<O>` come back as
/// the kernel's terminal `codec` failure, which the guard replays as a cached failure in every SDK.
pub trait IdempotencyStore<O> {
    /// Backend transaction handle, like [`Transactor::Tx`](crate::Transactor::Tx). A store enlists the
    /// record in it, so the claim commits or rolls back with the events it guards; a rolled-back claim
    /// releases the key, which is what makes a guarded handler effectively-once. A store that needs no
    /// handle uses `()`. The middleware reads the handle from [`idempotent_in`](crate::idempotent_in) or from `Tx::default()`.
    type Tx;
    /// Atomically claim `key` with a pending record. [`Claim::Won`] if this caller won; [`Claim::Prior`]
    /// carries the winner's record so the caller decides replay, mismatch or in-progress. A store that
    /// cannot tell "I won" from "someone else holds this" must fail, or two handlers run. In a transaction a
    /// loser must block and read the winner's record, fail, or return it pending as a
    /// [`ErrorCode::InProgress`](crate::ErrorCode::InProgress) error.
    ///
    /// # Errors
    /// A [`ErrorCode::Store`](crate::ErrorCode::Store) error for a backend failure. It is transient: a retry or redelivery
    /// re-claims. Never fold it into a `Prior` record; the middleware reads that as a cached, terminal
    /// failure.
    fn claim(&self, tx: &mut Self::Tx, key: &str, payload_hash: u64) -> Result<Claim<O>, Error>;
    /// Write the terminal record for `key`, replacing the pending claim `handle` names, and only that
    /// claim. Idempotent: a second write of the same record over it settles on the same value. A
    /// [`ErrorCode::Store`](crate::ErrorCode::Store) error for a backend failure, or for a claim that lapsed and was taken over;
    /// the middleware answers it by releasing the claim so the key does not stay pending. A store whose
    /// writes join the caller's transaction never settles a key another open transaction holds, because
    /// that transaction's commit would overwrite the record: it blocks until that transaction ends, or
    /// refuses with a transient [`ErrorCode::Concurrency`](crate::ErrorCode::Concurrency) error and writes nothing (the in-memory store
    /// refuses).
    fn complete(&self, tx: &mut Self::Tx, key: &str, handle: &ClaimHandle, record: Record<O>) -> Result<(), Error>;
    /// Drop `key` while it still holds the claim `handle` names, so a transient failure can retry.
    /// Idempotent: releasing an absent key, a key whose committed record is another claimant's, or a key
    /// already settled is not a failure, and deletes nothing. A key another open transaction holds is
    /// treated as in [`complete`](Self::complete): deleting it would free that transaction's claim.
    fn release(&self, tx: &mut Self::Tx, key: &str, handle: &ClaimHandle) -> Result<(), Error>;
    /// Delete the settled record `key` holds, so the next claim wins and a message taken back from a
    /// dead-letter queue runs its handler again. The kernel reads the record ([`edax_cqrs::forget`]): a
    /// pending claim is refused as [`ErrorCode::InProgress`](crate::ErrorCode::InProgress) and stays,
    /// because its run may still complete; an absent key is a no-op. It enlists as `claim` does, and takes
    /// no handle: it acts on an outcome nobody owns any more. Call it through [`forget`] or
    /// [`forget_event`], which compose the key.
    fn forget(&self, tx: &mut Self::Tx, key: &str) -> Result<(), Error>;
}

/// A shared store is a store: a guard takes its store by value, so an `Arc` of one (a concrete store, or a
/// `dyn` one shared across guards) passes as it is, with no cast at the call site.
impl<O, S: IdempotencyStore<O> + ?Sized> IdempotencyStore<O> for Arc<S> {
    type Tx = S::Tx;

    fn claim(&self, tx: &mut Self::Tx, key: &str, payload_hash: u64) -> Result<Claim<O>, Error> {
        (**self).claim(tx, key, payload_hash)
    }

    fn complete(&self, tx: &mut Self::Tx, key: &str, handle: &ClaimHandle, record: Record<O>) -> Result<(), Error> {
        (**self).complete(tx, key, handle, record)
    }

    fn release(&self, tx: &mut Self::Tx, key: &str, handle: &ClaimHandle) -> Result<(), Error> {
        (**self).release(tx, key, handle)
    }

    fn forget(&self, tx: &mut Self::Tx, key: &str) -> Result<(), Error> {
        (**self).forget(tx, key)
    }
}

/// Delete the record the [`idempotent`](crate::idempotent) guard named `scope` keeps for `idempotency_key`,
/// so the next dispatch of that key runs its handler again: the re-drive of a dead-lettered command whose
/// failure was cached. Pass the guard's `tenancy` and the command's `tenant`; the record lives under the
/// key [`claim_key`] composes from them.
///
/// # Errors
/// The terminal `codec` error when the tenancy needs a tenant and none is named, instead of deleting
/// nothing; whatever [`IdempotencyStore::forget`] refuses.
pub fn forget<O, S: IdempotencyStore<O> + ?Sized>(
    store: &S,
    tx: &mut S::Tx,
    scope: &str,
    idempotency_key: &str,
    tenancy: Tenancy,
    tenant: Option<&str>,
) -> Result<(), Error> {
    store.forget(tx, &claim_key(tenancy, tenant, scope, idempotency_key)?)
}

/// Delete the record the [`idempotent_by_id`](crate::idempotent_by_id) guard named `scope` keeps for the
/// event `event_id`, so a redelivery of that event runs its handler again. The key is the kernel's
/// [`event_claim_key`]; the errors are [`forget`]'s.
///
/// # Errors
/// As [`forget`].
pub fn forget_event<O, S: IdempotencyStore<O> + ?Sized>(
    store: &S,
    tx: &mut S::Tx,
    scope: &str,
    event_id: &str,
    tenancy: Tenancy,
    tenant: Option<&str>,
) -> Result<(), Error> {
    store.forget(tx, &event_claim_key(tenancy, tenant, scope, event_id)?)
}

/// What both guards, [`idempotent_in`](crate::idempotent_in) and
/// [`idempotent_by_id_in`](crate::idempotent_by_id_in), wire around a handler: the store, the consumer's
/// scope, and `current`, which reads the handle off the message `M`. They differ only in how a message's
/// claim is derived. A clone is cheap, one per registered handler.
pub(crate) struct Guard<M, O, S: IdempotencyStore<O>> {
    store: Arc<S>,
    scope: Arc<str>,
    current: Arc<CurrentTx<M, S::Tx>>,
    result: PhantomData<fn() -> O>,
}

/// Reads the handle a guarded claim runs in off the message it guards.
type CurrentTx<M, T> = dyn Fn(&M) -> T + Send + Sync;

impl<M, O, S: IdempotencyStore<O>> Clone for Guard<M, O, S> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            scope: Arc::clone(&self.scope),
            current: Arc::clone(&self.current),
            result: PhantomData,
        }
    }
}

impl<M, O: Clone + Serialize, S: IdempotencyStore<O>> Guard<M, O, S> {
    pub(crate) fn new(
        store: S,
        scope: impl Into<String>,
        current: impl Fn(&M) -> S::Tx + Send + Sync + 'static,
    ) -> Self {
        Self {
            store: Arc::new(store),
            scope: Arc::from(scope.into()),
            current: Arc::new(current),
            result: PhantomData,
        }
    }

    /// The consumer this guard claims for, which every ledger key it composes carries.
    pub(crate) fn scope(&self) -> &str {
        &self.scope
    }

    /// Run `run` under the claim `(key, hash)`, in the handle `current` reads off `message`.
    pub(crate) fn run(
        &self,
        message: &M,
        key: &str,
        hash: u64,
        run: impl FnOnce() -> Result<O, HandlerError>,
    ) -> Result<O, HandlerError> {
        let mut tx = (self.current)(message);
        guarded(&*self.store, &mut tx, key, hash, run)
    }
}

/// Overwrite the pending claim on `key` with its terminal `record`. If that write fails, release the claim
/// instead of leaving the key pending for the store's whole retention. The write failure is what the caller
/// sees; a release failure on top of it replaces it.
pub(crate) fn complete_or_release<O, T>(
    store: &dyn IdempotencyStore<O, Tx = T>,
    tx: &mut T,
    key: &str,
    handle: &ClaimHandle,
    record: Record<O>,
) -> Result<(), Error> {
    match store.complete(tx, key, handle, record) {
        Ok(()) => Ok(()),
        Err(e) => {
            store.release(tx, key, handle)?;
            Err(e)
        }
    }
}

/// Run `run` once under an atomic claim on `key`, returning its result or the winner's cached one.
///
/// The one claim/run/settle orchestration behind both guards, [`idempotent`](crate::idempotent) and
/// [`idempotent_by_id`](crate::idempotent_by_id). They differ only in how the key and the hash are
/// derived; both report through the one [`HandlerError`] contract.
pub(crate) fn guarded<O, T>(
    store: &dyn IdempotencyStore<O, Tx = T>,
    tx: &mut T,
    key: &str,
    hash: u64,
    run: impl FnOnce() -> Result<O, HandlerError>,
) -> Result<O, HandlerError>
where
    O: Clone + Serialize,
{
    // A failed claim is never compensated: there is no handle to release with. A claim whose
    // response was lost is settled by the store's retention.
    match store.claim(tx, key, hash)? {
        // Won the claim. The run is caught so an unwinding handler cannot leave the key pending with
        // nobody left to settle it; every later dispatch of that key would then see `InProgress`.
        Claim::Won(handle) => match catch_unwind(AssertUnwindSafe(|| -> Result<O, HandlerError> {
            let result = run()?;
            // Encoded before `complete`, as the Python guard does. A result with no wire form fails the
            // same way on every run, so it settles below as the terminal `codec` failure and is cached.
            // Refused inside `complete` instead, it would release the claim and let a retry run the work
            // again.
            edax_cqrs::encode_record(&Record::done(hash, &result))?;
            Ok(result)
        })) {
            Ok(Ok(result)) => {
                complete_or_release(store, tx, key, &handle, Record::done(hash, result.clone()))?;
                Ok(result)
            }
            Ok(Err(e)) => {
                // The kernel's settle table: a terminal failure is cached, code and message, so a
                // redelivery replays it; anything else releases the claim so a retry (or redelivery)
                // can re-run the handler. Either write can be refused (the handler's own failed
                // statement may already have aborted the handle), and the handler's error is the
                // verdict the caller and the dispatcher act on, so a refused compensation is dropped
                // here exactly as the panic arm below drops it. The host's rollback or the store's
                // retention frees the claim instead.
                let _ = match settle_failure(hash, code_of(&*e), &failure_message(&*e), reason_of(&*e)) {
                    Settle::Cache(record) => complete_or_release(store, tx, key, &handle, record),
                    Settle::Release => store.release(tx, key, &handle),
                };
                Err(e)
            }
            // A panic is not a value the record can replay, so treat it as transient: release the claim
            // and keep unwinding. A refused release has no channel that would not shadow the panic.
            Err(panic) => {
                let _ = store.release(tx, key, &handle);
                resume_unwind(panic)
            }
        },
        // Lost the claim: the kernel answers from the winner's record, the result or the error to fail
        // with (a reused key, a claim in flight, or the cached failure with its original code).
        Claim::Prior(prior) => replay(&prior, hash, key)
            .cloned()
            .map_err(|e| Box::new(e) as HandlerError),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryLedger;
    use crate::{Reason, Rejected};
    use edax_core::ErrorCode;
    use serde_json::Value;

    /// `testdata/golden/idempotency_guard.json` replayed through this SDK's guard over its memory ledger:
    /// what one dispatch returns or raises, and what the key holds after it. The Python suite replays
    /// the same rows through its own guard.
    #[test]
    fn the_guard_settles_and_replays_as_the_golden_scenarios_say() {
        for row in edax_golden::read("idempotency_guard.json") {
            let name = row["name"].as_str().expect("named");
            let key = row["key"].as_str().expect("keyed");
            let hash = row["payload_hash"].as_u64().expect("hashed");
            let ledger = MemoryLedger::<Value>::new();
            if let Some(prior) = row["prior"].as_str() {
                ledger.put_raw(key, prior.as_bytes()).expect("a ledger takes raw bytes");
            }
            let run = row["run"].clone();
            let mut runs = 0;
            let got = guarded(&ledger, &mut (), key, hash, || {
                runs += 1;
                if let Some(result) = run.get("result") {
                    return Ok(result.clone());
                }
                let message = run["error"]["message"].as_str().expect("a failure has a message");
                let failure: HandlerError = match run["error"]["code"].as_str() {
                    Some(code) => Error::new(code.parse::<ErrorCode>().expect("a kernel code"), message).into(),
                    None => message.into(),
                };
                match run["error"]["reason"].as_str() {
                    Some(reason) => Err(Rejected::new(Reason::new(reason).expect("a reason"), failure).into()),
                    None => Err(failure),
                }
            });
            assert_eq!(runs, usize::from(!run.is_null()), "{name}: handler runs");
            match (row["want"].get("result"), got) {
                (Some(want), Ok(result)) => assert_eq!(&result, want, "{name}"),
                (None, Err(e)) => assert_eq!(Some(e.to_string().as_str()), row["want"]["error"].as_str(), "{name}"),
                (want, got) => panic!("{name}: want {want:?}, got {:?}", got.map_err(|e| e.to_string())),
            }
            // What the key holds: a fresh claim wins once the claim was released, else reads the record.
            let held = match ledger.claim(&mut (), key, hash).expect("the ledger answers") {
                Claim::Won(_) => None,
                Claim::Prior(record) => Some(record),
            };
            let want = row["want_record"]
                .as_str()
                .map(|bytes| edax_cqrs::decode_prior::<Value>(bytes.as_bytes(), hash));
            assert_eq!(held, want, "{name}: the record the key holds");
        }
    }
}

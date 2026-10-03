//! Command bus (CQRS write side): register handlers by command type, dispatch through a middleware
//! chain. The kernel decides payload canonicalization, hashing and the idempotency verdict.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;

use edax_core::ErrorCode;
use edax_cqrs::{cached_failure, command_hash, outcome, Outcome};
use serde::Serialize;

use crate::chain::wrap_outermost_first;
use crate::pacing::Pacing;
use crate::Error;
// Kernel type; its wire shape and replay verdict are pinned by `testdata/golden/idempotency_record.json`.
pub use edax_cqrs::{Record, State};

/// Implemented by every command. [`Command::command_type`] is a stable identifier used to route a
/// command to its handler.
///
/// The idempotency hash comes from the command *value*: serde serializes it, then the kernel
/// canonicalizes and hashes the bytes ([`edax_cqrs::command_hash`]), as pinned by
/// `testdata/golden/command_hash.json`. A command cannot supply the bytes, so `Serialize` declares
/// the hashed shape. The type name is routing metadata, not a field, so it stays out of the hash:
/// register each command type on the [`CommandBus`] as itself, never through an enum, whose serde
/// tag would enter the hash and split one command into two hashes across SDKs.
pub trait Command: Serialize {
    fn command_type(&self) -> &str;
}

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
/// as an `f64`, so a result carrying a wider integer must carry it as a string.
pub trait IdempotencyStore<O> {
    /// Backend transaction handle, like [`Transactor::Tx`](crate::Transactor::Tx). A store enlists the
    /// record in it, so the claim commits or rolls back with the events it guards; a rolled-back claim
    /// releases the key, which is what makes a guarded handler effectively-once. A store that needs no
    /// handle uses `()`. The middleware reads the handle from [`idempotent_in`] or from `Tx::default()`.
    type Tx;
    /// Atomically claim `key` with a pending record. [`Claim::Won(_)`] if this caller won; [`Claim::Prior`]
    /// carries the winner's record so the caller decides replay, mismatch or in-progress. A store that
    /// cannot tell "I won" from "someone else holds this" must fail, or two handlers run. In a transaction a
    /// loser must block and read the winner's record, fail, or return it pending as a
    /// [`ErrorCode::InProgress`] error.
    ///
    /// # Errors
    /// A [`ErrorCode::Store`] error for a backend failure. It is transient: a retry or redelivery
    /// re-claims. Never fold it into a `Prior` record; the middleware reads that as a cached, terminal
    /// failure.
    fn claim(&self, tx: &mut Self::Tx, key: &str, payload_hash: u64) -> Result<Claim<O>, Error>;
    /// Write the terminal record for `key`, replacing the pending claim `handle` names, and only that
    /// claim. Idempotent: a second write of the same record over it settles on the same value. A
    /// [`ErrorCode::Store`] error for a backend failure, or for a claim that lapsed and was taken over;
    /// the middleware answers it by releasing the claim so the key does not stay pending. A store whose
    /// writes join the caller's transaction never settles a key another open transaction holds, because
    /// that transaction's commit would overwrite the record: it blocks until that transaction ends, or
    /// refuses with a transient [`ErrorCode::Concurrency`] error and writes nothing (the in-memory store
    /// refuses).
    fn complete(&self, tx: &mut Self::Tx, key: &str, handle: &ClaimHandle, record: Record<O>) -> Result<(), Error>;
    /// Drop `key` while it still holds the claim `handle` names, so a transient failure can retry.
    /// Idempotent: releasing an absent key, a key whose committed record is another claimant's, or a key
    /// already settled is not a failure, and deletes nothing. A key another open transaction holds is
    /// treated as in [`complete`](Self::complete): deleting it would free that transaction's claim.
    fn release(&self, tx: &mut Self::Tx, key: &str, handle: &ClaimHandle) -> Result<(), Error>;
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
}

/// A handler's failure: any error, boxed. One contract for command and event handlers alike, so a
/// failure keeps its verdict whichever path it crosses — a saga that dispatches a command returns the
/// command's error with `?`. [`retry`], both idempotency guards and the dispatcher downcast it to
/// classify terminal (deterministic, so a redelivery cannot change it) against transient: an
/// [`Error`] by its code, a [`Terminal`] as terminal, anything else by the kernel's default for an
/// unclassified failure (transient). `Send + Sync` so it can cross a thread.
pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;
/// Runs a command and returns a result, or an error. `Send + Sync` so one bus can be shared by a
/// [`KeyOrderedDispatcher`](crate::KeyOrderedDispatcher)'s worker threads.
pub type Handler<C, O> = Box<dyn Fn(&C) -> Result<O, HandlerError> + Send + Sync>;
/// Wraps a handler, running logic before and/or after the inner call.
pub type Middleware<C, O> = Box<dyn Fn(Handler<C, O>) -> Handler<C, O> + Send + Sync>;

/// A host's own business rejection, marked terminal: "insufficient funds" fails the same way on every
/// retry, so the guard caches it (code `rejected`) and a dispatcher dead-letters it instead of
/// redelivering it forever. Wrap the host's error in it; without it a host error is unclassified and
/// therefore transient.
#[derive(Debug)]
pub struct Terminal(pub HandlerError);

impl Terminal {
    /// Mark `cause` terminal.
    pub fn new(cause: impl Into<HandlerError>) -> Self {
        Terminal(cause.into())
    }
}

impl std::fmt::Display for Terminal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Terminal {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.0)
    }
}

/// Wrap `handler` in `middleware`, the first outermost. Middleware is per command type on a
/// [`CommandBus`], because each one is typed by the command it reads and the result it caches.
pub fn wrap<C, O>(handler: Handler<C, O>, middleware: Vec<Middleware<C, O>>) -> Handler<C, O> {
    wrap_outermost_first(handler, &middleware)
}

/// One registration: the handler, type-erased, and the command and result types it was registered for.
struct Registered {
    types: (TypeId, TypeId),
    handler: Box<dyn Any + Send + Sync>,
}

/// Dispatches commands to handlers registered by command type. One bus routes any number of command
/// types, each registered as its own Rust type with its own result type, so no enum (and no serde tag)
/// stands between a command and its idempotency hash. Wrap each handler in its middleware with
/// [`wrap`] before registering it.
#[derive(Default)]
pub struct CommandBus {
    handlers: HashMap<String, Registered>,
}

impl CommandBus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install the handler for `command_type`: commands of type `C`, returning `O`.
    ///
    /// # Errors
    /// The terminal [`ErrorCode::AlreadyRegistered`] if the command type already has a handler.
    pub fn register<C: Command + 'static, O: 'static>(
        &mut self,
        command_type: impl Into<String>,
        handler: Handler<C, O>,
    ) -> Result<(), Error> {
        let command_type = command_type.into();
        if self.handlers.contains_key(&command_type) {
            return Err(Error::new(ErrorCode::AlreadyRegistered, command_type));
        }
        let registered = Registered { types: (TypeId::of::<C>(), TypeId::of::<O>()), handler: Box::new(handler) };
        self.handlers.insert(command_type, registered);
        Ok(())
    }

    /// Route `cmd` to the handler registered for `cmd.command_type()` and return its result.
    ///
    /// # Errors
    /// The terminal [`ErrorCode::HandlerNotFound`] when no handler is registered for the type, or when
    /// it was registered for another command or result type (a wiring mistake that fails the same way
    /// on every redelivery). Otherwise whatever the handler returned.
    pub fn dispatch<C: Command + 'static, O: 'static>(&self, cmd: &C) -> Result<O, HandlerError> {
        let command_type = cmd.command_type();
        let registered = self
            .handlers
            .get(command_type)
            .ok_or_else(|| Error::new(ErrorCode::HandlerNotFound, command_type))?;
        let handler = registered
            .handler
            .downcast_ref::<Handler<C, O>>()
            .filter(|_| registered.types == (TypeId::of::<C>(), TypeId::of::<O>()))
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::HandlerNotFound,
                    format_args!("{command_type} is registered for another command or result type"),
                )
            })?;
        handler(cmd)
    }
}

/// The taxonomy code a handler failure carries: an [`Error`]'s own, `rejected` for a [`Terminal`], and
/// none for anything else.
pub(crate) fn code_of(e: &(dyn std::error::Error + 'static)) -> Option<ErrorCode> {
    if let Some(err) = e.downcast_ref::<Error>() {
        return Some(err.code);
    }
    e.is::<Terminal>().then_some(ErrorCode::Rejected)
}

/// Whether an error is *terminal*: deterministic, so neither a retry nor a fresh claim can change the
/// outcome. Terminal means cache the failure and never re-run. Transient means release the claim and let a
/// retry or a redelivery re-run. Every consumer of a failure branches on this one function.
///
/// The verdict is the kernel's ([`edax_core::verdict`]): by the code for an [`Error`] or a
/// [`Terminal`], and the kernel's default for a failure no code classifies.
pub(crate) fn is_terminal(e: &(dyn std::error::Error + 'static)) -> bool {
    edax_core::verdict(code_of(e))
}

/// What a failure caches as its message: an [`Error`]'s message without its code (the code is cached
/// beside it), anything else as it displays.
fn failure_message(e: &(dyn std::error::Error + 'static)) -> String {
    e.downcast_ref::<Error>().map_or_else(|| e.to_string(), |err| err.message.clone())
}

/// Retry the handler up to `attempts` times (at least once), waiting each step of `pacing`, and return
/// the last error. A terminal error returns at once without spending attempts; a transient one is
/// retried. The wait blocks; wrap the dispatch in your async runtime for non-blocking retry.
///
/// A retry must re-run a whole unit of work, never a handler inside one: a second attempt in the same
/// transaction reads the first attempt's staged writes and applies them again, and one commit lands both.
/// So `retry` refuses to run inside a [`transaction`](crate::transaction) body on the same thread, with the
/// terminal [`ErrorCode::BadHandle`]. Under [`idempotent_in`], retry the whole unit of work with
/// [`retry_transaction`](crate::retry_transaction) instead. Inside [`idempotent`] with no transaction, every
/// attempt's writes commit on their own: at-least-once, so the handler must tolerate a re-run.
pub fn retry<C: 'static, O: 'static>(attempts: usize, pacing: Pacing) -> Middleware<C, O> {
    let attempts = attempts.max(1);
    let pacing = Arc::new(pacing);
    Box::new(move |next: Handler<C, O>| {
        let pacing = Arc::clone(&pacing);
        Box::new(move |cmd: &C| {
            if crate::store::in_unit_of_work() {
                return Err(Box::new(Error::bad_handle(
                    "retry was dispatched inside an open transaction: a second attempt would re-apply the first \
                     attempt's staged writes; retry the whole unit of work with retry_transaction",
                )));
            }
            let mut failures: usize = 0;
            loop {
                match next(cmd) {
                    Ok(out) => return Ok(out),
                    Err(e) if is_terminal(&*e) || failures.saturating_add(1) >= attempts => return Err(e),
                    Err(_) => {
                        // The backoff index is the number of failures so far: the first retry waits step 0.
                        pacing.wait(u64::try_from(failures).unwrap_or(u64::MAX));
                        failures = failures.saturating_add(1);
                    }
                }
            }
        })
    })
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
/// The one claim/run/settle orchestration behind both guards, [`idempotent`] and
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
    O: Clone,
{
    // A failed claim is never compensated: there is no handle to release with. A claim whose
    // response was lost is settled by the store's retention.
    match store.claim(tx, key, hash)? {
        // Won the claim. The run is caught so an unwinding handler cannot leave the key pending with
        // nobody left to settle it; every later dispatch of that key would then see `InProgress`.
        Claim::Won(handle) => match catch_unwind(AssertUnwindSafe(run)) {
            Ok(Ok(result)) => {
                complete_or_release(store, tx, key, &handle, Record::done(hash, result.clone()))?;
                Ok(result)
            }
            Ok(Err(e)) => {
                // The kernel's settle table: a terminal failure is cached, code and message, so a
                // redelivery replays it; anything else releases the claim so a retry (or redelivery)
                // can re-run the handler. Either
                // write can be refused — a handle the handler's own failed statement already aborted —
                // and the handler's error is the verdict the caller and the dispatcher act on, so a
                // refused compensation is dropped here exactly as the panic arm below drops it. The
                // host's rollback or the store's retention frees the claim instead.
                let _ = match edax_cqrs::settle(code_of(&*e)) {
                    edax_cqrs::Settle::Cache(code) => {
                        complete_or_release(store, tx, key, &handle, Record::failed(hash, code, failure_message(&*e)))
                    }
                    edax_cqrs::Settle::Release => store.release(tx, key, &handle),
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
        // Lost the claim: the kernel classifies the winner's record, and the kernel's errors report it.
        Claim::Prior(prior) => match outcome(&prior, hash) {
            Outcome::PayloadMismatch => Err(Box::new(Error::new(ErrorCode::PayloadMismatch, key))),
            // The winner has not completed — don't report a bogus result; signal in-progress.
            Outcome::InProgress => Err(Box::new(Error::new(ErrorCode::InProgress, key))),
            // Terminal: the winner already failed deterministically, so a retry or a broker
            // redelivery must stop here instead of nacking forever. The original code survives in the
            // replay's message.
            Outcome::Failed { code, message } => Err(Box::new(cached_failure(code, message))),
            Outcome::Succeeded(result) => Ok(result.clone()),
        },
    }
}

/// Deduplicate dispatches by idempotency key. `key` returns the key for a command, or `None` to bypass. The
/// kernel hashes the whole serialized command ([`edax_cqrs::command_hash`]) and binds that hash to the key,
/// so reusing the key with a different command errors. See `docs/sdk/idempotency.md`.
///
/// The first dispatch of a key wins the claim and runs the handler; a later one replays the prior record or
/// gets the transient [`ErrorCode::InProgress`] error while the winner runs. Verdicts surface as an
/// [`Error`] in the [`HandlerError`]. A terminal handler error is cached with its code; a transient one or a
/// panic releases the claim. The default handle gives at-least-once with dedup; [`idempotent_in`] commits
/// it with the events.
pub fn idempotent<C, O, S>(store: S, key: impl Fn(&C) -> Option<String> + Send + Sync + 'static) -> Middleware<C, O>
where
    C: Command + 'static,
    O: Clone + 'static,
    S: IdempotencyStore<O> + Send + Sync + 'static,
    S::Tx: Default + 'static,
{
    idempotent_in(store, key, |_| S::Tx::default())
}

/// [`idempotent`] over the host's transaction. `current` is called once per guarded dispatch, with the
/// command, and returns the handle for the claim, the terminal record and any release. Sync middleware has
/// no ambient transaction, so the host that opened the unit of work publishes its handle keyed by the
/// command; one slot shared by concurrent dispatchers is a race. The handler enlists its own writes in the
/// same handle, so the claim commits with the events and a rollback drops both. Outside a unit of work
/// `current` returns the auto-commit handle. Never wrap it in [`retry`]: retry the whole unit of work
/// with [`retry_transaction`](crate::retry_transaction).
pub fn idempotent_in<C, O, S>(
    store: S,
    key: impl Fn(&C) -> Option<String> + Send + Sync + 'static,
    current: impl Fn(&C) -> S::Tx + Send + Sync + 'static,
) -> Middleware<C, O>
where
    C: Command + 'static,
    O: Clone + 'static,
    S: IdempotencyStore<O> + Send + Sync + 'static,
    S::Tx: 'static,
{
    // The middleware closure runs once per registered handler; share `store`, `key` and `current` through
    // reference counts so each produced handler owns a cheap clone.
    let store = Arc::new(store);
    let key = Arc::new(key);
    let current = Arc::new(current);
    Box::new(move |next: Handler<C, O>| {
        let store = Arc::clone(&store);
        let key = Arc::clone(&key);
        let current = Arc::clone(&current);
        Box::new(move |cmd: &C| -> Result<O, HandlerError> {
            let Some(k) = key(cmd) else { return next(cmd) };
            // A command the kernel cannot hash is the kernel's terminal `codec` error; the message names the step.
            let hash = command_hash(cmd)?;
            let mut tx = current(cmd);
            guarded(&*store, &mut tx, &k, hash, || next(cmd))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryIdempotencyStore, MemoryTx};
    use edax_cqrs::{canonical_json, payload_hash};
    use serde::Deserialize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    // The in-memory idempotency store is shipped, not re-rolled for tests.
    type MemStore = MemoryIdempotencyStore<i64>;

    /// A call counter the handlers and test stores share. The command path is `Send + Sync`, so a
    /// `Cell` no longer fits; the API is the same.
    #[derive(Default)]
    struct Counter(AtomicUsize);

    impl Counter {
        fn get(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }

        fn set(&self, value: usize) {
            self.0.store(value, Ordering::SeqCst);
        }
    }

    /// A pacing that records each wait instead of sleeping, with a fixed draw so every wait is the
    /// kernel's delay for that draw.
    fn recorded() -> (Pacing, Arc<Mutex<Vec<Duration>>>) {
        let slept: Arc<Mutex<Vec<Duration>>> = Arc::default();
        let recorder = Arc::clone(&slept);
        let pacing = Pacing {
            sleep: Arc::new(move |d| recorder.lock().unwrap().push(d)),
            entropy: Arc::new(|| u64::MAX),
            ..Pacing::default()
        };
        (pacing, slept)
    }

    fn no_wait() -> Pacing {
        recorded().0
    }

    /// The wait the kernel computes for retry `attempt` under `recorded()`'s fixed draw.
    fn kernel_wait(attempt: u64) -> Duration {
        Duration::from_millis(edax_core::backoff_millis(edax_core::Backoff::DEFAULT, attempt, u64::MAX))
    }

    // --- a tiny command ---

    #[derive(Serialize)]
    struct Pay {
        id: String,
        amount: i64,
    }

    impl Command for Pay {
        fn command_type(&self) -> &str {
            "Pay"
        }
    }

    fn pay(id: &str, amount: i64) -> Pay {
        Pay { id: id.into(), amount }
    }

    /// A bus with one `Pay -> i64` handler, wrapped in `middleware`.
    fn pay_bus(middleware: Vec<Middleware<Pay, i64>>, handler: Handler<Pay, i64>) -> CommandBus {
        let mut bus = CommandBus::new();
        bus.register("Pay", wrap(handler, middleware)).unwrap();
        bus
    }

    /// A command whose fields are exactly the event body used on the consume path, so both
    /// idempotency guards hash the same bytes and read the same record.
    #[derive(Serialize)]
    struct Deposit {
        account_id: &'static str,
        amount_cents: u32,
        currency: &'static str,
    }

    impl Command for Deposit {
        fn command_type(&self) -> &str {
            "Deposit"
        }
    }

    fn deposit() -> Deposit {
        Deposit { account_id: "acct-1", amount_cents: 4200, currency: "USD" }
    }

    /// The `Deposit` fields, canonical already (keys sorted, no whitespace), so canonicalizing it is
    /// the identity — which lets the command path and the raw-bytes consume path share one hash.
    const DEPOSIT_BODY: &[u8] = br#"{"account_id":"acct-1","amount_cents":4200,"currency":"USD"}"#;

    /// The golden command, as the idiomatic Rust type. Fields are declared in an order that is
    /// neither the vector's nor alphabetical: the hash must not depend on declaration order.
    #[derive(Serialize, Deserialize)]
    struct PlaceOrder {
        request_id: String,
        weight_kg: f64,
        total_cents: i64,
        tags: Vec<String>,
        shipping: Shipping,
        reference: u64,
        order_id: String,
        note: Option<String>,
        customer: String,
        currency: String,
    }

    #[derive(Serialize, Deserialize)]
    struct Shipping {
        zip: Option<String>,
        city: String,
    }

    impl Command for PlaceOrder {
        fn command_type(&self) -> &str {
            "order.place"
        }
    }

    /// `testdata/golden/command_hash.json` asserted through the SDK layer: every case is rebuilt as
    /// a `PlaceOrder`, dispatched through the real `idempotent` middleware, and the hash the store
    /// received must be the vector's. The Python suite does the same from a dataclass, so this is
    /// the proof that both SDKs bind one key to one number for one command.
    #[test]
    fn command_hash_golden_holds_through_the_middleware() {
        let cases = edax_golden::read("command_hash.json");
        assert!(!cases.is_empty(), "the vector has cases");

        for case in &cases {
            let name = case["name"].as_str().expect("case has a name");
            // `type` routes; it is not a field of the command and takes no part in the hash.
            let cmd: PlaceOrder = match case["type"].as_str() {
                Some("order.place") => serde_json::from_value(case["command"].clone()).expect(name),
                other => panic!("{name}: no Rust command type for {other:?}"),
            };

            let store: Arc<dyn IdempotencyStore<(), Tx = MemoryTx> + Send + Sync> = Arc::new(MemoryIdempotencyStore::<()>::new());
            let mut bus = CommandBus::new();
            let guard = idempotent(Arc::clone(&store), |c: &PlaceOrder| Some(c.request_id.clone()));
            let runs = Arc::new(AtomicUsize::new(0));
            let counted = Arc::clone(&runs);
            let handler = move |_: &PlaceOrder| {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            };
            bus.register("order.place", wrap(Box::new(handler), vec![guard])).unwrap();
            let dispatched = bus.dispatch::<PlaceOrder, ()>(&cmd);

            // A refusal row: the guard fails as the kernel's terminal `codec` error, before any claim
            // or handler run, and leaves the key free. The row pins the code and the rule; the byte
            // offset in the message is the host encoder's, which writes fields in its own order.
            if let Some(error) = case["error"].as_str() {
                let refused = dispatched.expect_err(name);
                let rule = error.rsplit_once(" is outside ").map(|(_, rule)| rule).expect("a refusal names its rule");
                assert_eq!(ErrorCode::split(&refused.to_string()).map(|(code, _)| code), Some(ErrorCode::Codec), "{name}");
                assert!(refused.to_string().ends_with(rule), "{name}: {refused}");
                assert!(is_terminal(&*refused), "{name}: a refusal is terminal");
                assert_eq!(runs.load(Ordering::SeqCst), 0, "{name}: the handler never ran");
                assert!(matches!(store.claim(&mut MemoryTx::auto_commit(), &cmd.request_id, 0).unwrap(), Claim::Won(_)), "{name}: no claim was left");
                continue;
            }
            dispatched.expect(name);
            let expected = case["expected"].as_u64().expect("expected is an unsigned 64-bit hash");

            // The hash the middleware bound to the key is what a Python service would compare its
            // own against; a claim with any hash returns the winner's record.
            let stored = match store.claim(&mut MemoryTx::auto_commit(), &cmd.request_id, 0).unwrap() {
                Claim::Prior(record) => record.payload_hash,
                Claim::Won(_) => panic!("{name}: the dispatch left no record"),
            };
            assert_eq!(stored, expected, "{name}");
        }

        // The rule the vector replaces: a hand-written `payload()` returning the business body
        // without the key field. Those bytes must not reach the vector, or the gap is not closed.
        let mut body_only = cases[0]["command"].clone();
        body_only.as_object_mut().expect("command is an object").remove("request_id");
        let old_rule = payload_hash(&canonical_json(&serde_json::to_vec(&body_only).unwrap()).unwrap());
        assert_ne!(old_rule, cases[0]["expected"].as_u64().unwrap(), "body-only bytes reached the vector");
    }

    /// One bus routes two command types, each registered as its own struct with its own result
    /// type, and the guard hashes each as itself. Through an enum, serde's external tag
    /// (`{"Withdraw":{..}}`) would enter the hash, and a Python or plain-struct dispatch of the same
    /// command would read as a payload mismatch on a shared ledger.
    #[test]
    fn one_bus_routes_several_command_types_and_hashes_each_as_itself() {
        #[derive(Serialize)]
        struct Withdraw {
            account: &'static str,
            amount_cents: u32,
        }
        impl Command for Withdraw {
            fn command_type(&self) -> &str {
                "Withdraw"
            }
        }
        #[derive(Serialize)]
        enum BankCommand {
            Withdraw { account: &'static str, amount_cents: u32 },
        }

        let ledger = crate::MemoryTransactor::new();
        let receipts: Arc<dyn IdempotencyStore<String, Tx = MemoryTx> + Send + Sync> = Arc::new(ledger.idempotency_store::<String>());
        let balances: Arc<dyn IdempotencyStore<u32, Tx = MemoryTx> + Send + Sync> = Arc::new(ledger.idempotency_store::<u32>());
        let mut bus = CommandBus::new();
        let withdraw_guard = idempotent(Arc::clone(&receipts), |_: &Withdraw| Some("w-1".to_owned()));
        let withdraw: Handler<Withdraw, String> = Box::new(|w: &Withdraw| Ok(format!("receipt for {}", w.amount_cents)));
        bus.register("Withdraw", wrap(withdraw, vec![withdraw_guard])).unwrap();
        let deposit_guard = idempotent(Arc::clone(&balances), |_: &Deposit| Some("d-1".to_owned()));
        bus.register("Deposit", wrap(Box::new(|d: &Deposit| Ok(d.amount_cents)), vec![deposit_guard])).unwrap();

        let cmd = Withdraw { account: "a", amount_cents: 500 };
        assert_eq!(bus.dispatch::<_, String>(&cmd).unwrap(), "receipt for 500");
        assert_eq!(bus.dispatch::<_, u32>(&deposit()).unwrap(), 4200, "a second command type and result type");

        let stored = match receipts.claim(&mut MemoryTx::auto_commit(), "w-1", 0).unwrap() {
            Claim::Prior(record) => record.payload_hash,
            Claim::Won(_) => panic!("the dispatch left no record"),
        };
        assert_eq!(stored, command_hash(&cmd).unwrap(), "the struct's own hash");
        let tagged = command_hash(&BankCommand::Withdraw { account: "a", amount_cents: 500 }).unwrap();
        assert_ne!(stored, tagged, "the enum's tag would have changed the hash");
    }

    #[test]
    fn dispatch_branches() {
        let mut bus = pay_bus(vec![], Box::new(|c: &Pay| Ok(c.amount * 2)));
        assert_eq!(bus.dispatch::<Pay, i64>(&pay("a", 21)).unwrap(), 42, "registered: routes to the handler");

        #[derive(Serialize)]
        struct Other;
        impl Command for Other {
            fn command_type(&self) -> &str {
                "Other"
            }
        }
        // Each refusal is the kernel's terminal `handler_not_found`: a redelivery finds the same bus.
        let not_found = |e: HandlerError| match e.downcast_ref::<Error>() {
            Some(err) if err.code == ErrorCode::HandlerNotFound => err.message.clone(),
            _ => panic!("expected handler_not_found, got {e}"),
        };
        let unregistered = bus.dispatch::<Other, i64>(&Other).expect_err("nothing registered for Other");
        assert!(is_terminal(&*unregistered));
        assert_eq!(not_found(unregistered), "Other");
        let wrong_result = bus.dispatch::<Pay, String>(&pay("a", 1)).expect_err("Pay returns i64");
        assert_eq!(not_found(wrong_result), "Pay is registered for another command or result type");

        // A duplicate registration is the same terminal `already_registered` the router reports.
        let duplicate = bus.register("Pay", Box::new(|c: &Pay| Ok(c.amount))).unwrap_err();
        assert_eq!(duplicate, Error::new(ErrorCode::AlreadyRegistered, "Pay"));
    }

    /// The terminal/transient split is total over every error a handler can return: each code of
    /// the taxonomy as an `Error`, a host rejection marked `Terminal`, and an unclassified host
    /// error. `is_terminal` reads the kernel's table and its default, so this catches a code changing
    /// sides in either.
    #[test]
    fn is_terminal_classifies_every_error() {
        let mut cases: Vec<(String, HandlerError, bool, Option<ErrorCode>)> = ErrorCode::ALL
            .iter()
            .map(|&code| (code.as_str().to_owned(), Box::new(Error::new(code, "x")) as HandlerError, code.is_terminal(), Some(code)))
            .collect();
        cases.push(("host_rejection_marked_terminal".into(), Box::new(Terminal::new("insufficient funds")), true, Some(ErrorCode::Rejected)));
        cases.push(("unclassified_host_error_is_transient".into(), "ledger service returned 503".into(), false, None));
        for (name, error, want, code) in cases {
            assert_eq!(is_terminal(&*error), want, "{name}");
            assert_eq!(code_of(&*error), code, "{name}: code");
        }
        let rejection = Terminal::new("insufficient funds");
        assert_eq!(rejection.to_string(), "insufficient funds", "a wrapper adds no text");
        assert_eq!(std::error::Error::source(&rejection).map(ToString::to_string).as_deref(), Some("insufficient funds"));
    }

    /// The two idempotency guards must cache and release on **one** rule. Each lost-claim verdict is
    /// driven through both — `idempotent` on the command path, `idempotent_by_id` on the consume
    /// path — against the same seeded record, and both must report the same error with the same
    /// terminal/transient classification.
    #[test]
    fn both_idempotency_guards_report_one_verdict_per_outcome() {
        use edax_event::{Event, EventEnvelope};

        const KEY: &str = "acct-1-3";

        struct Case {
            name: &'static str,
            /// The record the winner left behind under `KEY`.
            prior: fn(u64) -> Record<()>,
            want: Error,
            want_terminal: bool,
        }
        let cases = [
            Case {
                name: "in_flight_claim",
                prior: Record::pending,
                want: Error::new(ErrorCode::InProgress, KEY),
                want_terminal: false,
            },
            Case {
                name: "cached_terminal_failure",
                prior: |h| Record::failed(h, ErrorCode::Codec, "unknown field `amount`"),
                want: Error::new(ErrorCode::CachedFailure, "codec: unknown field `amount`"),
                want_terminal: true,
            },
            Case {
                // The winner recorded a different payload under this key: flipping a bit of the hash
                // is exactly what a second, different command (or event body) would produce.
                name: "key_reused_with_another_payload",
                prior: |h| Record::done(h ^ 1, ()),
                want: Error::new(ErrorCode::PayloadMismatch, KEY),
                want_terminal: true,
            },
        ];

        // Settle `KEY` with `prior` the way a winner would: claim it, then complete with the handle.
        fn seed(store: &dyn IdempotencyStore<(), Tx = MemoryTx>, prior: Record<()>) {
            let Ok(Claim::Won(handle)) = store.claim(&mut MemoryTx::auto_commit(), KEY, prior.payload_hash) else {
                panic!("a fresh store's key is free")
            };
            store.complete(&mut MemoryTx::auto_commit(), KEY, &handle, prior).unwrap();
        }

        // Hash the way each path does: the command value is serialized and canonicalized, the event
        // body is hashed raw. The body is already canonical, so both land on one hash and one record.
        let hash = command_hash(&deposit()).unwrap();
        assert_eq!(hash, payload_hash(DEPOSIT_BODY), "the fixture body must already be canonical");

        for c in cases {
            // --- command path ---
            let command_store: Arc<dyn IdempotencyStore<(), Tx = MemoryTx> + Send + Sync> = Arc::new(MemoryIdempotencyStore::<()>::new());
            seed(&*command_store, (c.prior)(hash));
            let mut bus = CommandBus::new();
            let guard = idempotent(Arc::clone(&command_store), |_: &Deposit| Some(KEY.to_string()));
            bus.register("Deposit", wrap(Box::new(|_: &Deposit| Ok(())), vec![guard])).unwrap();
            let command_err = bus.dispatch::<Deposit, ()>(&deposit()).expect_err(c.name);

            // --- consume path ---
            let event_store: crate::EventIdempotencyStore<MemoryTx> = Arc::new(MemoryIdempotencyStore::<()>::new());
            seed(&*event_store, (c.prior)(hash));
            let mut router = crate::EventRouter::new().with(crate::idempotent_by_id(Arc::clone(&event_store)));
            router.register("Deposited", Box::new(|_: &EventEnvelope| Ok(()))).unwrap();
            let env = EventEnvelope {
                id: KEY.to_string(),
                aggregate_id: "acct-1".into(),
                event: Event { name: "Deposited".into(), version: 1, data: DEPOSIT_BODY.to_vec() },
                sequence: Some(3),
                namespace: Default::default(),
                metadata: Default::default(),
                timestamp: 1_714_564_800,
            };
            let event_err = router.dispatch(&env).expect_err(c.name);

            assert_eq!(command_err.downcast_ref::<Error>(), Some(&c.want), "{}: command path", c.name);
            assert_eq!(event_err.downcast_ref::<Error>(), Some(&c.want), "{}: consume path", c.name);
            assert_eq!(is_terminal(&*command_err), c.want_terminal, "{}: command class", c.name);
            assert_eq!(is_terminal(&*event_err), c.want_terminal, "{}: consume class", c.name);
        }
    }

    #[test]
    fn retry_branches() {
        struct Case {
            name: &'static str,
            attempts: usize,
            /// The handler fails this many times before it succeeds.
            failures: usize,
            /// How each failure is made.
            failure: fn() -> HandlerError,
            want_calls: usize,
            /// `Ok(amount)` or the text of the last handler error.
            want: Result<i64, &'static str>,
            /// Waits asked of the pacing, as retry attempt indexes.
            want_waits: &'static [u64],
        }
        fn transient() -> HandlerError {
            "transient".into()
        }
        fn codec() -> HandlerError {
            Error::codec("bad bytes").into()
        }
        fn rejected() -> HandlerError {
            Terminal::new("insufficient funds").into()
        }
        let cases = [
            Case {
                name: "succeeds after transient failures",
                attempts: 3,
                failures: 2,
                failure: transient,
                want_calls: 3,
                want: Ok(7),
                want_waits: &[0, 1],
            },
            Case {
                name: "terminal error returns at once without burning attempts",
                attempts: 3,
                failures: 1,
                failure: codec,
                want_calls: 1,
                want: Err("codec: bad bytes"),
                want_waits: &[],
            },
            Case {
                name: "a host rejection marked terminal is not retried",
                attempts: 3,
                failures: 1,
                failure: rejected,
                want_calls: 1,
                want: Err("insufficient funds"),
                want_waits: &[],
            },
            Case {
                name: "exhausted attempts return the last transient error",
                attempts: 2,
                failures: 5,
                failure: transient,
                want_calls: 2,
                want: Err("transient"),
                want_waits: &[0],
            },
            Case {
                name: "zero attempts clamps to one",
                attempts: 0,
                failures: 1,
                failure: transient,
                want_calls: 1,
                want: Err("transient"),
                want_waits: &[],
            },
        ];
        for c in cases {
            let calls = Arc::new(Counter::default());
            let calls_h = Arc::clone(&calls);
            let (failures, failure) = (c.failures, c.failure);
            let (pacing, slept) = recorded();
            let bus = pay_bus(
                vec![retry(c.attempts, pacing)],
                Box::new(move |cmd: &Pay| {
                    let n = calls_h.get() + 1;
                    calls_h.set(n);
                    if n > failures {
                        Ok(cmd.amount)
                    } else {
                        Err(failure())
                    }
                }),
            );
            let got = bus.dispatch::<Pay, i64>(&pay("x", 7));
            assert_eq!(calls.get(), c.want_calls, "{}: handler calls", c.name);
            match (c.want, got) {
                (Ok(want), Ok(got)) => assert_eq!(got, want, "{}", c.name),
                (Err(want), Err(e)) => assert_eq!(e.to_string(), want, "{}", c.name),
                (want, got) => panic!("{}: want {want:?}, got {:?}", c.name, got.map_err(|e| e.to_string())),
            }
            let want_waits: Vec<Duration> = c.want_waits.iter().map(|&a| kernel_wait(a)).collect();
            assert_eq!(*slept.lock().unwrap(), want_waits, "{}: the kernel's jittered schedule", c.name);
        }
    }

    #[test]
    fn idempotent_pre_claim_branches() {
        // Branches taken before any claim: a `None` key bypasses the guard entirely, and a command
        // the kernel cannot hash is rejected without a claim or a handler run.
        struct NotJson;
        impl Serialize for NotJson {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("card number must not leave the process"))
            }
        }
        impl Command for NotJson {
            fn command_type(&self) -> &str {
                "Pay"
            }
        }
        struct Case {
            name: &'static str,
            key: fn(&NotJson) -> Option<String>,
            want_calls: usize,
            /// The message fragment the caller must still see, or `None` for a clean dispatch.
            want_err: Option<&'static str>,
        }
        let cases = [
            Case {
                name: "no key: handler runs on every dispatch, nothing recorded",
                key: |_| None,
                want_calls: 2,
                want_err: None,
            },
            Case {
                name: "key but the command will not serialize: codec error, no claim, handler never runs",
                key: |_| Some("k1".into()),
                want_calls: 0,
                want_err: Some("not JSON-encodable: card number must not leave the process"),
            },
        ];
        for c in cases {
            let store = Arc::new(FaultStore::default());
            let calls = Arc::new(Counter::default());
            let calls_h = Arc::clone(&calls);
            let guard = idempotent(Arc::clone(&store) as Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync>, c.key);
            let handler: Handler<NotJson, i64> = Box::new(move |_: &NotJson| {
                calls_h.set(calls_h.get() + 1);
                Ok(1)
            });
            let mut bus = CommandBus::new();
            bus.register("Pay", wrap(handler, vec![guard])).unwrap();
            for _ in 0..2 {
                match (c.want_err, bus.dispatch::<NotJson, i64>(&NotJson)) {
                    (None, Ok(1)) => {}
                    (Some(want), Err(e)) => {
                        // The code matters as much as the text: a serialization failure is a
                        // marshaling failure, and `is_terminal` reads the code to stop retrying.
                        assert!(
                            matches!(e.downcast_ref::<Error>(), Some(err) if err.code == ErrorCode::Codec && err.message.contains(want)),
                            "{}: got {e}",
                            c.name
                        );
                        assert!(is_terminal(&*e), "{}: a command that will not serialize is terminal", c.name);
                    }
                    (want, got) => panic!("{}: want {want:?}, got {:?}", c.name, got.map_err(|e| e.to_string())),
                }
            }
            assert_eq!(calls.get(), c.want_calls, "{}: handler calls", c.name);
            assert_eq!(store.claims.get(), 0, "{}: no claim is taken", c.name);
        }
    }

    #[test]
    fn idempotent_claim_branches() {
        // One row per branch reached after the claim, over two dispatches of one command: the
        // winner's outcomes, and the three verdicts a loser reads off the winner's record.
        type Expect = Option<Error>;
        struct Case {
            name: &'static str,
            /// Per handler run: `None` returns the amount, `Some(f)` returns that error.
            errors: &'static [Option<fn() -> HandlerError>],
            /// `None` expects the dispatched amount, `Some(e)` that exact error.
            want: [Expect; 2],
            /// A concurrent dispatch holds the claim before the first one runs.
            held: bool,
            /// The second dispatch's amount; a different one reuses the key with another payload.
            redispatched: i64,
            want_calls: usize,
            want_claims: usize,
            want_releases: usize,
            /// The code the record caches, for a cached failure.
            want_cached_code: Option<ErrorCode>,
        }
        fn codec() -> HandlerError {
            Error::codec("bad payload").into()
        }
        fn rejected() -> HandlerError {
            Terminal::new("insufficient funds").into()
        }
        fn blip() -> HandlerError {
            "downstream blip".into()
        }
        let blip_error = || Error::new(ErrorCode::Store, "never matched");
        let cases = [
            Case {
                name: "first dispatch runs and the duplicate replays the cached result",
                errors: &[None],
                want: [None, None],
                held: false,
                redispatched: 10,
                want_calls: 1,
                want_claims: 2,
                want_releases: 0,
                want_cached_code: None,
            },
            Case {
                name: "claim held by a concurrent dispatch reports in progress",
                errors: &[],
                want: [Some(Error::new(ErrorCode::InProgress, "k1")), Some(Error::new(ErrorCode::InProgress, "k1"))],
                held: true,
                redispatched: 10,
                want_calls: 0,
                want_claims: 2,
                want_releases: 0,
                want_cached_code: None,
            },
            Case {
                name: "transient failure releases the claim so the next dispatch reruns",
                errors: &[Some(blip), None],
                want: [Some(blip_error()), None],
                held: false,
                redispatched: 10,
                want_calls: 2,
                want_claims: 2,
                want_releases: 1,
                want_cached_code: None,
            },
            Case {
                name: "terminal failure is cached with its code and the next dispatch replays it",
                errors: &[Some(codec)],
                want: [Some(Error::codec("bad payload")), Some(Error::new(ErrorCode::CachedFailure, "codec: bad payload"))],
                held: false,
                redispatched: 10,
                want_calls: 1,
                want_claims: 2,
                want_releases: 0,
                want_cached_code: Some(ErrorCode::Codec),
            },
            Case {
                name: "a host rejection is cached as rejected and replays as a cached failure",
                errors: &[Some(rejected)],
                want: [Some(blip_error()), Some(Error::new(ErrorCode::CachedFailure, "rejected: insufficient funds"))],
                held: false,
                redispatched: 10,
                want_calls: 1,
                want_claims: 2,
                want_releases: 0,
                want_cached_code: Some(ErrorCode::Rejected),
            },
            Case {
                name: "same key with a different payload fails with PayloadMismatch",
                errors: &[None],
                want: [None, Some(Error::new(ErrorCode::PayloadMismatch, "k1"))],
                held: false,
                redispatched: 99,
                want_calls: 1,
                want_claims: 2,
                want_releases: 0,
                want_cached_code: None,
            },
        ];
        for c in cases {
            let store = Arc::new(FaultStore::default());
            let calls = Arc::new(Counter::default());
            let calls_h = Arc::clone(&calls);
            let errors = c.errors;
            let guard = idempotent(Arc::clone(&store) as Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync>, |c: &Pay| {
                Some(c.id.clone())
            });
            let bus = pay_bus(
                vec![guard],
                Box::new(move |cmd: &Pay| {
                    let n = calls_h.get();
                    calls_h.set(n + 1);
                    match errors[n] {
                        Some(make) => Err(make()),
                        None => Ok(cmd.amount),
                    }
                }),
            );
            if c.held {
                let hash = command_hash(&pay("k1", 10)).unwrap();
                assert!(matches!(store.claim(&mut MemoryTx::auto_commit(), "k1", hash), Ok(Claim::Won(_))), "{}", c.name);
                store.claims.set(0);
            }

            for (amount, want) in [10, c.redispatched].into_iter().zip(c.want) {
                match (want, bus.dispatch::<Pay, i64>(&pay("k1", amount))) {
                    (None, Ok(got)) => assert_eq!(got, amount, "{}", c.name),
                    // A host error has no `Error` to compare; its text is the handler's own.
                    (Some(want), Err(e)) if e.downcast_ref::<Error>().is_none() => {
                        assert!(want.message == "never matched", "{}: unexpected host error {e}", c.name)
                    }
                    (Some(want), Err(e)) => assert_eq!(e.downcast_ref::<Error>(), Some(&want), "{}", c.name),
                    (want, got) => panic!("{}: want {want:?}, got {:?}", c.name, got.map_err(|e| e.to_string())),
                }
            }
            assert_eq!(calls.get(), c.want_calls, "{}: handler calls", c.name);
            assert_eq!(store.claims.get(), c.want_claims, "{}: claims", c.name);
            assert_eq!(store.releases.get(), c.want_releases, "{}: releases", c.name);
            if let Some(code) = c.want_cached_code {
                match store.inner.claim(&mut MemoryTx::auto_commit(), "k1", 0).unwrap() {
                    Claim::Prior(Record { state: State::Failed { code: cached, .. }, .. }) => {
                        assert_eq!(cached, Some(code), "{}: the cached code", c.name)
                    }
                    _ => panic!("{}: no cached failure", c.name),
                }
            }
        }
    }

    #[test]
    fn a_panicking_handler_releases_the_claim() {
        // An unwinding handler must not strand the claim: without the release, every later dispatch
        // of that key would read a pending record and fail with `InProgress` forever.
        let claims = Arc::new(Counter::default());
        let releases = Arc::new(Counter::default());
        let store: Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync> = Arc::new(FaultStore {
            claims: Arc::clone(&claims),
            releases: Arc::clone(&releases),
            ..Default::default()
        });
        let calls = Arc::new(Counter::default());
        let calls_h = Arc::clone(&calls);
        let bus = pay_bus(
            vec![idempotent(Arc::clone(&store), |c: &Pay| Some(c.id.clone()))],
            Box::new(move |c: &Pay| {
                calls_h.set(calls_h.get() + 1);
                if calls_h.get() == 1 {
                    panic!("handler bug");
                }
                Ok(c.amount)
            }),
        );

        let dispatched = catch_unwind(AssertUnwindSafe(|| bus.dispatch::<Pay, i64>(&pay("k1", 10))));
        assert!(dispatched.is_err(), "the panic reaches the caller");
        assert_eq!(releases.get(), 1, "the claim is released, not stranded");
        assert_eq!(bus.dispatch::<Pay, i64>(&pay("k1", 10)).unwrap(), 10, "the next dispatch re-runs the handler");
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn custom_middleware_wraps_in_order() {
        // An arbitrary user middleware (not retry/idempotent), wrapped first-outermost and run.
        let trace: Arc<Mutex<Vec<&'static str>>> = Arc::default();
        let tracing = |name: &'static str| -> Middleware<Pay, i64> {
            let trace = Arc::clone(&trace);
            Box::new(move |next: Handler<Pay, i64>| {
                let trace = Arc::clone(&trace);
                Box::new(move |cmd: &Pay| {
                    trace.lock().unwrap().push(name);
                    next(cmd)
                })
            })
        };
        let bus = pay_bus(vec![tracing("outer"), tracing("inner")], Box::new(|c: &Pay| Ok(c.amount)));
        assert_eq!(bus.dispatch::<Pay, i64>(&pay("a", 3)).unwrap(), 3);
        assert_eq!(bus.dispatch::<Pay, i64>(&pay("b", 4)).unwrap(), 4);
        assert_eq!(*trace.lock().unwrap(), ["outer", "inner", "outer", "inner"]);
    }

    #[test]
    fn replayed_cached_failure_is_terminal_and_stops_retrying() {
        let store: Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync> = Arc::new(MemStore::default());
        let calls = Arc::new(Counter::default());
        let calls_h = Arc::clone(&calls);
        let bus = pay_bus(
            vec![idempotent(Arc::clone(&store), |c: &Pay| Some(c.id.clone())), retry(3, no_wait())],
            Box::new(move |_c: &Pay| {
                calls_h.set(calls_h.get() + 1);
                Err(Error::codec("bad payload").into())
            }),
        );

        let cmd = pay("k1", 10);

        // Terminal handler failure: retry returns at once (1 call, not 3) and the outcome is cached.
        assert!(bus.dispatch::<Pay, i64>(&cmd).is_err());
        assert_eq!(calls.get(), 1);

        // Redelivery replays it as CachedFailure, itself terminal — so retry stops here instead of
        // burning attempts, and the broker stops redelivering a deterministically failing command.
        let replay = bus.dispatch::<Pay, i64>(&cmd).expect_err("cached failure");
        assert_eq!(replay.downcast_ref::<Error>(), Some(&Error::new(ErrorCode::CachedFailure, "codec: bad payload")));
        assert_eq!(calls.get(), 1); // handler never re-ran
    }

    /// The guard's test store: counts every seam call, and fails the ones it is told to. The counts
    /// make the guard's own behaviour observable — how many claims a nesting order takes, whether a
    /// compensation was attempted at all. `claim_fails` is a ledger the guard cannot reach;
    /// `settle_fails` is a SQL ledger whose compensating write is refused once the handler's own
    /// failed statement has aborted the transaction they share.
    #[derive(Default)]
    struct FaultStore {
        inner: MemStore,
        claims: Arc<Counter>,
        completes: Arc<Counter>,
        releases: Arc<Counter>,
        claim_fails: bool,
        settle_fails: bool,
    }

    impl IdempotencyStore<i64> for FaultStore {
        type Tx = MemoryTx;
        fn claim(&self, tx: &mut MemoryTx, key: &str, payload_hash: u64) -> Result<Claim<i64>, Error> {
            self.claims.set(self.claims.get() + 1);
            if self.claim_fails {
                return Err(Error::store("could not reach the ledger"));
            }
            self.inner.claim(tx, key, payload_hash)
        }
        fn complete(&self, tx: &mut MemoryTx, key: &str, handle: &ClaimHandle, record: Record<i64>) -> Result<(), Error> {
            self.completes.set(self.completes.get() + 1);
            if self.settle_fails {
                return Err(Error::store("current transaction is aborted"));
            }
            self.inner.complete(tx, key, handle, record)
        }
        fn release(&self, tx: &mut MemoryTx, key: &str, handle: &ClaimHandle) -> Result<(), Error> {
            self.releases.set(self.releases.get() + 1);
            if self.settle_fails {
                return Err(Error::store("current transaction is aborted"));
            }
            self.inner.release(tx, key, handle)
        }
    }

    #[test]
    fn retry_inside_idempotent_shares_one_claim() {
        let claims = Arc::new(Counter::default());
        let releases = Arc::new(Counter::default());
        let store: Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync> = Arc::new(FaultStore {
            claims: Arc::clone(&claims),
            releases: Arc::clone(&releases),
            ..Default::default()
        });

        let calls = Arc::new(Counter::default());
        let calls_h = Arc::clone(&calls);
        // Order matters: idempotent first (outermost), retry inside it. Every attempt then runs under
        // one claim; swapped, each retry would take and release its own, which the claim count shows.
        let (pacing, slept) = recorded();
        let bus = pay_bus(
            vec![idempotent(Arc::clone(&store), |c: &Pay| Some(c.id.clone())), retry(2, pacing)],
            Box::new(move |c: &Pay| {
                calls_h.set(calls_h.get() + 1);
                if calls_h.get() == 1 {
                    Err("transient downstream blip".into())
                } else {
                    Ok(c.amount)
                }
            }),
        );

        // One transient failure, one retry (one wait), one success.
        assert_eq!(bus.dispatch::<Pay, i64>(&pay("k1", 5)).unwrap(), 5);
        assert_eq!(calls.get(), 2);
        assert_eq!(claims.get(), 1, "both attempts ran under one claim");
        assert_eq!(releases.get(), 0, "the claim was never released — the retry succeeded under it");
        assert_eq!(*slept.lock().unwrap(), [kernel_wait(0)]);

        // The claim completed, so a redelivery replays the cached result without another attempt.
        assert_eq!(bus.dispatch::<Pay, i64>(&pay("k1", 5)).unwrap(), 5);
        assert_eq!(calls.get(), 2);
        assert_eq!(claims.get(), 2);
    }

    /// A claim that fails is reported as itself and never compensated. Nothing was taken, so there is
    /// nothing to release — and releasing would be worse than useless: the failure may be the backend
    /// reporting that *another* claimant holds the key, and the release would delete the winner's live
    /// claim. The handler must not run either, or the guard has waved through the very duplicate it
    /// exists to stop. `tests/test_unit_of_work.py` pins the same rule for the Python SDK.
    #[test]
    fn a_failed_claim_is_reported_and_never_compensated() {
        let releases = Arc::new(Counter::default());
        let calls = Arc::new(Counter::default());
        let store: Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync> =
            Arc::new(FaultStore { releases: Arc::clone(&releases), claim_fails: true, ..Default::default() });

        let calls_h = Arc::clone(&calls);
        let bus = pay_bus(
            vec![idempotent(store, |c: &Pay| Some(c.id.clone()))],
            Box::new(move |c: &Pay| {
                calls_h.set(calls_h.get() + 1);
                Ok(c.amount)
            }),
        );

        let err = bus.dispatch::<Pay, i64>(&pay("k1", 5)).expect_err("the claim failed");
        // The store's own verdict, not a `CachedFailure` standing in for it.
        assert_eq!(err.downcast_ref::<Error>(), Some(&Error::store("could not reach the ledger")));
        assert_eq!(releases.get(), 0, "a failed claim must not be compensated");
        assert_eq!(calls.get(), 0, "the handler must not run without a claim");
    }

    /// A compensation the store refuses must never replace the handler's error. That error is the
    /// verdict `retry` and the dispatcher branch on, so letting the store's failure through re-types a
    /// terminal domain error as a transient `store` one: `retry` re-runs a deterministic failure and a
    /// dispatcher nacks to `max_attempts` instead of dead-lettering on the first attempt. The panic arm
    /// of `guarded` already drops a refused release for the same reason.
    /// `tests/test_unit_of_work.py` pins the same rule for the Python SDK.
    #[test]
    fn a_refused_compensation_keeps_the_handlers_error() {
        for terminal in [true, false] {
            let completes = Arc::new(Counter::default());
            let releases = Arc::new(Counter::default());
            let calls = Arc::new(Counter::default());
            let store: Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync> = Arc::new(FaultStore {
                completes: Arc::clone(&completes),
                releases: Arc::clone(&releases),
                settle_fails: true,
                ..Default::default()
            });

            let calls_h = Arc::clone(&calls);
            // Codec is terminal, Store is transient: the two sides of the verdict.
            let raised = if terminal {
                Error::codec("the payload is malformed")
            } else {
                Error::store("the downstream service timed out")
            };
            let raised_h = raised.clone();
            let bus = pay_bus(
                vec![idempotent(store, |c: &Pay| Some(c.id.clone()))],
                Box::new(move |_c: &Pay| {
                    calls_h.set(calls_h.get() + 1);
                    Err(Box::new(raised_h.clone()) as HandlerError)
                }),
            );

            let err = bus.dispatch::<Pay, i64>(&pay("k1", 5)).expect_err("the handler failed");
            // The handler's own error, with its own verdict — not the store's refusal.
            assert_eq!(err.downcast_ref::<Error>(), Some(&raised));
            assert_eq!(is_terminal(&*err), terminal, "the handler's verdict survived the refusal");
            assert_eq!(calls.get(), 1, "the handler ran once");
            // A terminal failure is compensated by `complete`, which falls back to `release` when
            // refused; a transient failure only releases.
            let want = if terminal { (1, 1) } else { (0, 1) };
            assert_eq!((completes.get(), releases.get()), want, "the guard did compensate");
        }
    }

    /// A fenced store in the shape of the Redis ledger: each won claim gets its own handle, and a
    /// write settles the key only while the key still holds that handle's claim. `lapse` lets the
    /// current claim expire, the way a claim TTL does, so the next claim takes the key over. Every
    /// settle call is recorded with the handle it carried.
    #[derive(Default)]
    struct FencedStore {
        state: Mutex<FencedState>,
    }

    #[derive(Default)]
    struct FencedState {
        /// Per key: the handle of the claim holding it, or `None` once settled.
        holder: std::collections::HashMap<String, Option<ClaimHandle>>,
        settled: std::collections::HashMap<String, Record<i64>>,
        minted: usize,
        calls: Vec<(&'static str, ClaimHandle)>,
    }

    impl FencedStore {
        fn lapse(&self, key: &str) {
            self.state.lock().unwrap().holder.remove(key);
        }

        fn calls(&self) -> Vec<(&'static str, ClaimHandle)> {
            self.state.lock().unwrap().calls.clone()
        }

        fn holds(&self, key: &str, handle: &ClaimHandle) -> bool {
            self.state.lock().unwrap().holder.get(key) == Some(&Some(handle.clone()))
        }
    }

    impl IdempotencyStore<i64> for FencedStore {
        type Tx = MemoryTx;
        fn claim(&self, _tx: &mut MemoryTx, key: &str, payload_hash: u64) -> Result<Claim<i64>, Error> {
            let mut state = self.state.lock().unwrap();
            if let Some(holder) = state.holder.get(key) {
                return Ok(Claim::Prior(match holder {
                    Some(_) => Record::pending(payload_hash),
                    None => state.settled[key].clone(),
                }));
            }
            state.minted += 1;
            let handle = ClaimHandle(format!("claim-{}", state.minted).into_bytes());
            state.holder.insert(key.to_owned(), Some(handle.clone()));
            Ok(Claim::Won(handle))
        }
        fn complete(&self, _tx: &mut MemoryTx, key: &str, handle: &ClaimHandle, record: Record<i64>) -> Result<(), Error> {
            let mut state = self.state.lock().unwrap();
            state.calls.push(("complete", handle.clone()));
            if state.holder.get(key) != Some(&Some(handle.clone())) {
                return Err(Error::store("the claim was taken over"));
            }
            state.holder.insert(key.to_owned(), None);
            state.settled.insert(key.to_owned(), record);
            Ok(())
        }
        fn release(&self, _tx: &mut MemoryTx, key: &str, handle: &ClaimHandle) -> Result<(), Error> {
            let mut state = self.state.lock().unwrap();
            state.calls.push(("release", handle.clone()));
            if state.holder.get(key) == Some(&Some(handle.clone())) {
                state.holder.remove(key);
            }
            Ok(())
        }
    }

    /// The guard settles with the handle its own claim returned, on every path, and never with an
    /// empty or a guessed one. A guard that dropped the handle would leave a fenced store unable to
    /// tell this claim from the one that took the key over.
    #[test]
    fn the_guard_settles_with_the_handle_its_claim_returned() {
        let first = ClaimHandle(b"claim-1".to_vec());
        type Run = fn() -> Result<i64, HandlerError>;
        type Calls = Vec<(&'static str, ClaimHandle)>;
        let cases: [(&str, Run, Calls); 4] = [
            ("success_completes", || Ok(5), vec![("complete", first.clone())]),
            ("transient_failure_releases", || Err(Error::store("timeout").into()), vec![("release", first.clone())]),
            ("terminal_failure_caches", || Err(Error::codec("bad").into()), vec![("complete", first.clone())]),
            ("panic_releases", || panic!("handler bug"), vec![("release", first.clone())]),
        ];
        for (name, run, want) in cases {
            let store = FencedStore::default();
            let _ = catch_unwind(AssertUnwindSafe(|| guarded(&store, &mut MemoryTx::auto_commit(), "k", 7, run)));
            assert_eq!(store.calls(), want, "{name}");
        }
    }

    /// 06#F1 through the guard on one store: the claim lapses mid-handler and a second dispatch on
    /// the same store takes the key over. The first dispatch's `complete` is refused and its
    /// compensating release deletes nothing, so the new holder's claim survives and only the live
    /// claim settles the key.
    #[test]
    fn a_takeover_on_one_store_is_fenced_by_the_handle() {
        let store = FencedStore::default();
        let taker: std::cell::RefCell<Option<ClaimHandle>> = std::cell::RefCell::new(None);
        let got = guarded(&store, &mut MemoryTx::auto_commit(), "k", 7, || {
            store.lapse("k");
            match store.claim(&mut MemoryTx::auto_commit(), "k", 7)? {
                Claim::Won(handle) => *taker.borrow_mut() = Some(handle),
                Claim::Prior(_) => panic!("the lapsed key must be free"),
            }
            Ok(1)
        });
        let err = got.expect_err("the lapsed worker's complete is refused");
        assert_eq!(err.downcast_ref::<Error>(), Some(&Error::store("the claim was taken over")));
        let taker = taker.into_inner().unwrap();
        assert!(store.holds("k", &taker), "the stale release left the new holder's claim alone");
        let third = store.claim(&mut MemoryTx::auto_commit(), "k", 7);
        assert!(matches!(third, Ok(Claim::Prior(r)) if r.state == State::Pending), "a third claim loses to the live one");
        let stale = ClaimHandle(b"claim-1".to_vec());
        assert_eq!(store.calls(), [("complete", stale.clone()), ("release", stale)]);
        // The live claim still settles.
        store.complete(&mut MemoryTx::auto_commit(), "k", &taker, Record::done(7, 2)).unwrap();
        assert!(matches!(store.claim(&mut MemoryTx::auto_commit(), "k", 7), Ok(Claim::Prior(r)) if r.state == State::Done(2)));
    }
}

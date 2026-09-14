//! Command bus (CQRS write side): register handlers by command type, dispatch through a middleware
//! chain. The kernel decides payload canonicalization, hashing and the idempotency verdict.

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;

use edax_core::ErrorCode;
use edax_cqrs::{command_hash, outcome, Outcome};
use serde::Serialize;

use crate::chain::Chain;
// Kernel type; its wire shape and replay verdict are pinned by `testdata/golden/idempotency_record.json`.
pub use edax_cqrs::{Record, State};

/// Implemented by every command. [`Command::command_type`] is a stable identifier used to route a
/// command to its handler.
///
/// The idempotency hash comes from the command *value*: serde serializes it, then the kernel
/// canonicalizes and hashes the bytes ([`edax_cqrs::command_hash`]), as pinned by
/// `testdata/golden/command_hash.json`. A command cannot supply the bytes, so `Serialize` declares
/// the hashed shape. The type name is routing metadata, not a field, so it stays out of the hash.
pub trait Command: Serialize {
    fn command_type(&self) -> &str;
}

/// The outcome of a [`claim`](IdempotencyStore::claim): this caller won (run the handler), or a prior
/// record already exists (pending, a cached result, or a cached error) for the caller to act on.
pub enum Claim<O> {
    Won,
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
    /// Atomically claim `key` with a pending record. [`Claim::Won`] if this caller won; [`Claim::Prior`]
    /// carries the winner's record so the caller decides replay, mismatch or in-progress. A store that
    /// cannot tell "I won" from "someone else holds this" must fail, or two handlers run. In a transaction a
    /// loser must block and read the winner's record, fail, or return it pending as [`crate::Error::InProgress`].
    ///
    /// # Errors
    /// [`crate::Error::Store`] for a backend failure. It is transient: a retry or redelivery re-claims.
    /// Never fold it into a `Prior` record; the middleware reads that as a cached, terminal failure.
    fn claim(&self, tx: &mut Self::Tx, key: &str, payload_hash: u64) -> Result<Claim<O>, crate::Error>;
    /// Write the terminal record for `key`, replacing the pending claim. [`crate::Error::Store`] for a backend
    /// failure; the middleware answers it by releasing the claim so the key does not stay pending.
    fn complete(&self, tx: &mut Self::Tx, key: &str, record: Record<O>) -> Result<(), crate::Error>;
    /// Drop `key`, releasing the claim so a transient failure can retry. Idempotent: releasing an
    /// absent key is not a failure.
    fn release(&self, tx: &mut Self::Tx, key: &str) -> Result<(), crate::Error>;
}

/// The two ways a [`CommandBus::dispatch`] can fail: routing found no handler, or a handler ran and failed.
/// Every other condition, including the idempotency verdicts, is [`crate::Error`], the SDK's one taxonomy.
#[derive(Debug)]
#[non_exhaustive]
pub enum CommandBusError {
    /// No handler registered for the command type.
    NotFound(String),
    /// The handler itself failed.
    Handler(HandlerError),
}

impl std::fmt::Display for CommandBusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandBusError::NotFound(t) => write!(f, "no handler registered for command: {t}"),
            CommandBusError::Handler(e) => write!(f, "handler error: {e}"),
        }
    }
}

impl std::error::Error for CommandBusError {
    /// The wrapped handler failure, so a caller walks the chain instead of parsing `Display`.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CommandBusError::Handler(e) => Some(&**e),
            CommandBusError::NotFound(_) => None,
        }
    }
}

/// A handler's failure: any error, boxed. [`retry`] and [`idempotent`] downcast it to classify terminal
/// (deterministic, so a redelivery cannot change it) against transient. `Send + Sync` so it can cross a thread.
pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;
/// Runs a command and returns an opaque result, or an error. `Send + Sync` so one bus can be shared by a
/// [`KeyOrderedDispatcher`](crate::KeyOrderedDispatcher)'s worker threads.
pub type Handler<C, O> = Box<dyn Fn(&C) -> Result<O, HandlerError> + Send + Sync>;
/// Wraps a handler, running logic before and/or after the inner call.
pub type Middleware<C, O> = Box<dyn Fn(Handler<C, O>) -> Handler<C, O> + Send + Sync>;

/// Dispatches commands to handlers registered by command type, through a middleware chain. The chain
/// is baked around each handler at register time (first middleware is outermost).
pub struct CommandBus<C, O> {
    chain: Chain<Handler<C, O>, Middleware<C, O>>,
}

impl<C, O> Default for CommandBus<C, O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C, O> CommandBus<C, O> {
    pub fn new() -> Self {
        Self { chain: Chain::new() }
    }

    /// Add a middleware to the chain. Middleware added first is outermost when handlers run.
    pub fn with(mut self, mw: Middleware<C, O>) -> Self {
        self.chain.push_middleware(mw);
        self
    }

    /// Register a middleware on an existing bus, the non-consuming form of [`CommandBus::with`]. The chain is
    /// baked at [`register`](CommandBus::register) time, so add middleware before the handlers it wraps.
    pub fn add_middleware(&mut self, mw: Middleware<C, O>) {
        self.chain.push_middleware(mw);
    }

    /// Install a handler for a command type, baking the middleware chain around it (outermost first).
    ///
    /// # Errors
    /// [`crate::Error::AlreadyRegistered`] if the command type already has a handler.
    pub fn register(&mut self, command_type: impl Into<String>, handler: Handler<C, O>) -> Result<(), crate::Error> {
        self.chain.register(command_type.into(), handler)
    }

    /// Route `cmd` to its handler. Returns [`CommandBusError::NotFound`] if none is registered; a handler
    /// failure is wrapped in [`CommandBusError::Handler`].
    pub fn dispatch(&self, cmd: &C) -> Result<O, CommandBusError>
    where
        C: Command,
    {
        let handler = self
            .chain
            .get(cmd.command_type())
            .ok_or_else(|| CommandBusError::NotFound(cmd.command_type().to_string()))?;
        handler(cmd).map_err(CommandBusError::Handler)
    }
}

/// Whether an error is *terminal*: deterministic, so neither a retry nor a fresh claim can change the
/// outcome. Terminal means cache the failure and never re-run. Transient means release the claim and let a
/// retry or a redelivery re-run. Both guards branch on this; a nested [`CommandBusError::Handler`] is
/// classified by the error it wraps.
///
/// The verdict is the kernel's ([`ErrorCode::is_terminal`]), pinned by the `error_taxonomy` golden vector.
/// This function only names the code for each SDK error type; both matches are exhaustive, so a new variant
/// fails to compile instead of defaulting to transient. An unowned error is transient, safe under at-least-once.
pub(crate) fn is_terminal(e: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(err) = e.downcast_ref::<crate::Error>() {
        return err.code().is_terminal();
    }
    if let Some(err) = e.downcast_ref::<CommandBusError>() {
        return match err {
            CommandBusError::NotFound(_) => ErrorCode::HandlerNotFound.is_terminal(),
            CommandBusError::Handler(inner) => is_terminal(&**inner),
        };
    }
    false
}

/// Retry the handler up to `attempts` times (at least once) using the kernel's backoff schedule, returning
/// the last error. A terminal error returns at once without spending attempts; a transient one is retried.
/// Place inside [`idempotent`] so retries share a single claim. The sleep blocks; wrap the dispatch in your
/// async runtime for non-blocking retry.
pub fn retry<C: 'static, O: 'static>(attempts: usize) -> Middleware<C, O> {
    let attempts = attempts.max(1);
    Box::new(move |next: Handler<C, O>| {
        Box::new(move |cmd: &C| {
            let mut attempt: usize = 1;
            loop {
                match next(cmd) {
                    Ok(out) => return Ok(out),
                    Err(e) if is_terminal(&*e) || attempt >= attempts => return Err(e),
                    Err(_) => {
                        // Backoff index is the number of failures so far: the first retry sleeps backoff(0).
                        let failures = u32::try_from(attempt - 1).unwrap_or(u32::MAX);
                        std::thread::sleep(Duration::from_millis(edax_core::backoff_millis(failures)));
                        attempt += 1;
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
    record: Record<O>,
) -> Result<(), crate::Error> {
    match store.complete(tx, key, record) {
        Ok(()) => Ok(()),
        Err(e) => {
            store.release(tx, key)?;
            Err(e)
        }
    }
}

/// Run `run` once under an atomic claim on `key`, returning its result or the winner's cached one.
///
/// The one claim/run/settle orchestration behind both guards, [`idempotent`] and
/// [`idempotent_by_id`](crate::idempotent_by_id). They differ only in how the key and the hash are derived
/// and in the error type they report, so `terminal` and `lift` carry that difference.
pub(crate) fn guarded<O, T, E>(
    store: &dyn IdempotencyStore<O, Tx = T>,
    tx: &mut T,
    key: &str,
    hash: u64,
    run: impl FnOnce() -> Result<O, E>,
    terminal: &dyn Fn(&E) -> bool,
    lift: &dyn Fn(crate::Error) -> E,
) -> Result<O, E>
where
    O: Clone,
    E: std::fmt::Display,
{
    // A failed claim is never compensated: nothing was taken. A release here could delete another
    // claimant's live claim. A claim whose response was lost is settled by the store's retention.
    match store.claim(tx, key, hash).map_err(lift)? {
        // Won the claim. The run is caught so an unwinding handler cannot leave the key pending with
        // nobody left to settle it; every later dispatch of that key would then see `InProgress`.
        Claim::Won => match catch_unwind(AssertUnwindSafe(run)) {
            Ok(Ok(result)) => {
                complete_or_release(store, tx, key, Record::done(hash, result.clone())).map_err(lift)?;
                Ok(result)
            }
            Ok(Err(e)) => {
                // Terminal failure: cache it so a redelivery replays it. Transient failure: release
                // the claim so a retry (or redelivery) can re-run the handler.
                if terminal(&e) {
                    complete_or_release(store, tx, key, Record::failed(hash, e.to_string())).map_err(lift)?;
                } else {
                    store.release(tx, key).map_err(lift)?;
                }
                Err(e)
            }
            // A panic is not a value the record can replay, so treat it as transient: release the claim
            // and keep unwinding. A refused release has no channel that would not shadow the panic.
            Err(panic) => {
                let _ = store.release(tx, key);
                resume_unwind(panic)
            }
        },
        // Lost the claim: the kernel classifies the winner's record; this maps the verdict onto the error type.
        Claim::Prior(prior) => match outcome(&prior, hash) {
            Outcome::PayloadMismatch => Err(lift(crate::Error::PayloadMismatch(key.to_owned()))),
            // The winner has not completed — don't report a bogus result; signal in-progress.
            Outcome::InProgress => Err(lift(crate::Error::InProgress(key.to_owned()))),
            // Terminal: the winner already failed deterministically, so a retry or a broker
            // redelivery must stop here instead of nacking forever.
            Outcome::Failed(err) => Err(lift(crate::Error::CachedFailure(err.to_owned()))),
            Outcome::Succeeded(result) => Ok(result.clone()),
        },
    }
}

/// Deduplicate dispatches by idempotency key. `key` returns the key for a command, or `None` to bypass. The
/// kernel hashes the whole serialized command ([`edax_cqrs::command_hash`]) and binds that hash to the key,
/// so reusing the key with a different command errors. Place outside [`retry`]. See `docs/sdk/idempotency.md`.
///
/// The first dispatch of a key wins the claim and runs the handler; a later one replays the prior record or
/// gets the transient [`crate::Error::InProgress`] while the winner runs. Verdicts surface as [`crate::Error`]
/// inside [`CommandBusError::Handler`]. A terminal handler error is cached; a transient one or a panic releases
/// the claim. The default handle gives at-least-once with dedup; [`idempotent_in`] commits it with the events.
pub fn idempotent<C: Command + 'static, O: Clone + 'static, T: Default + 'static>(
    store: Arc<dyn IdempotencyStore<O, Tx = T> + Send + Sync>,
    key: impl Fn(&C) -> Option<String> + Send + Sync + 'static,
) -> Middleware<C, O> {
    idempotent_in(store, key, |_| T::default())
}

/// [`idempotent`] over the host's transaction. `current` is called once per guarded dispatch, with the
/// command, and returns the handle for the claim, the terminal record and any release. Sync middleware has
/// no ambient transaction, so the host that opened the unit of work publishes its handle keyed by the
/// command; one slot shared by concurrent dispatchers is a race. The handler enlists its own writes in the
/// same handle, so the claim commits with the events and a rollback drops both. Outside a unit of work
/// `current` returns the auto-commit handle.
pub fn idempotent_in<C: Command + 'static, O: Clone + 'static, T: 'static>(
    store: Arc<dyn IdempotencyStore<O, Tx = T> + Send + Sync>,
    key: impl Fn(&C) -> Option<String> + Send + Sync + 'static,
    current: impl Fn(&C) -> T + Send + Sync + 'static,
) -> Middleware<C, O> {
    // The middleware closure runs once per registered handler; share `store`, `key` and `current` through
    // reference counts so each produced handler owns a cheap clone.
    let key = Arc::new(key);
    let current = Arc::new(current);
    Box::new(move |next: Handler<C, O>| {
        let store = Arc::clone(&store);
        let key = Arc::clone(&key);
        let current = Arc::clone(&current);
        Box::new(move |cmd: &C| -> Result<O, HandlerError> {
            let Some(k) = key(cmd) else { return next(cmd) };
            // A command the kernel cannot hash is the terminal `crate::Error::Codec`; the message names the step.
            let hash = command_hash(cmd).map_err(crate::Error::Codec)?;
            let mut tx = current(cmd);
            guarded(
                &*store,
                &mut tx,
                &k,
                hash,
                || next(cmd),
                &|e: &HandlerError| is_terminal(&**e),
                &|e: crate::Error| -> HandlerError { Box::new(e) },
            )
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryIdempotencyStore, MemoryTx};
    use edax_cqrs::{canonical_json, payload_hash};
    use serde::Deserialize;
    use serde_json::Value;
        use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

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
        let path = edax_golden::golden_dir().join("command_hash.json");
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        let cases: Vec<Value> = serde_json::from_str(&raw).expect("command_hash.json is a JSON array");
        assert!(!cases.is_empty(), "the vector has cases");

        for case in &cases {
            let name = case["name"].as_str().expect("case has a name");
            let expected = case["expected"].as_u64().expect("expected is an unsigned 64-bit hash");
            // `type` routes; it is not a field of the command and takes no part in the hash.
            let cmd: PlaceOrder = match case["type"].as_str() {
                Some("order.place") => serde_json::from_value(case["command"].clone()).expect(name),
                other => panic!("{name}: no Rust command type for {other:?}"),
            };

            let store: Arc<dyn IdempotencyStore<(), Tx = MemoryTx> + Send + Sync> = Arc::new(MemoryIdempotencyStore::<()>::new());
            let mut bus: CommandBus<PlaceOrder, ()> = CommandBus::new()
                .with(idempotent(Arc::clone(&store), |c: &PlaceOrder| Some(c.request_id.clone())));
            bus.register("order.place", Box::new(|_: &PlaceOrder| Ok(()))).unwrap();
            bus.dispatch(&cmd).expect(name);

            // The hash the middleware bound to the key is what a Python service would compare its
            // own against; a claim with any hash returns the winner's record.
            let stored = match store.claim(&mut MemoryTx::auto_commit(), &cmd.request_id, 0).unwrap() {
                Claim::Prior(record) => record.payload_hash,
                Claim::Won => panic!("{name}: the dispatch left no record"),
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

    #[test]
    fn dispatch_routes_to_handler() {
        let mut bus: CommandBus<Pay, i64> = CommandBus::new();
        bus.register("Pay", Box::new(|c: &Pay| Ok(c.amount * 2))).unwrap();
        assert_eq!(bus.dispatch(&pay("a", 21)).unwrap(), 42);
    }

    #[test]
    fn unknown_command_is_not_found() {
        #[derive(Serialize)]
        struct Other;
        impl Command for Other {
            fn command_type(&self) -> &str {
                "Other"
            }
        }
        let bus: CommandBus<Other, i64> = CommandBus::new();
        match bus.dispatch(&Other) {
            Err(CommandBusError::NotFound(t)) => assert_eq!(t, "Other"),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_registration_errors() {
        let mut bus: CommandBus<Pay, i64> = CommandBus::new();
        bus.register("Pay", Box::new(|_| Ok(0))).unwrap();
        // One taxonomy: the command bus reports a duplicate with the same variant `EventRouter`
        // does, which is the single `AlreadyRegistered` the Python SDK raises for either path.
        match bus.register("Pay", Box::new(|_| Ok(0))) {
            Err(crate::Error::AlreadyRegistered(t)) => assert_eq!(t, "Pay"),
            other => panic!("expected AlreadyRegistered, got {other:?}"),
        }
    }

    /// The terminal/transient split is total over both error types `retry` and `idempotent` branch
    /// on: every variant of `crate::Error` and of `CommandBusError` has a row here, each asserting
    /// the side it must land on. `is_terminal` maps both enums to the kernel's `ErrorCode`
    /// exhaustively, so a new variant fails to compile there; this table is what catches an
    /// *existing* variant changing sides — in the SDK's mapping or in the kernel's table.
    #[test]
    fn is_terminal_classifies_every_error() {
        let boxed = |e: CommandBusError| HandlerError::from(e);
        let cases: Vec<(&str, HandlerError, bool)> = vec![
            // --- crate::Error: all seven variants ---
            ("codec_is_terminal", Box::new(crate::Error::Codec("invalid type: string, expected u64".into())), true),
            ("concurrency_is_transient", Box::new(crate::Error::Concurrency { expected: 4, stored: 7 }), false),
            ("store_is_transient", Box::new(crate::Error::Store("connection reset by peer".into())), false),
            ("already_registered_is_terminal", Box::new(crate::Error::AlreadyRegistered("Deposited".into())), true),
            ("payload_mismatch_is_terminal", Box::new(crate::Error::PayloadMismatch("acct-1-3".into())), true),
            ("in_progress_is_transient", Box::new(crate::Error::InProgress("acct-1-3".into())), false),
            (
                "cached_failure_is_terminal",
                Box::new(crate::Error::CachedFailure("codec error: invalid type: string, expected u64".into())),
                true,
            ),
            // --- CommandBusError: both variants ---
            ("not_found_is_terminal", boxed(CommandBusError::NotFound("Deposit".into())), true),
            (
                "nested_handler_takes_the_inner_class_terminal",
                boxed(CommandBusError::Handler(Box::new(crate::Error::Codec("unknown field `amount`".into())))),
                true,
            ),
            (
                "nested_handler_takes_the_inner_class_transient",
                boxed(CommandBusError::Handler(Box::new(crate::Error::Store("connection reset by peer".into())))),
                false,
            ),
            // --- not ours: a host error carries no classification, so it stays retryable ---
            ("unclassified_host_error_is_transient", "ledger service returned 503".into(), false),
        ];
        for (name, error, want) in cases {
            assert_eq!(is_terminal(&*error), want, "{name}");
        }
    }

    /// The two idempotency guards must cache and release on **one** rule. Each lost-claim verdict is
    /// driven through both — `idempotent` on the command path, `idempotent_by_id` on the consume
    /// path — against the same seeded record, and both must report the same `crate::Error` variant
    /// with the same terminal/transient classification. Two tables that merely agreed today would
    /// pass the per-path tests and fail this one the moment they drifted.
    #[test]
    fn both_idempotency_guards_report_one_verdict_per_outcome() {
        use edax_event::{Event, EventEnvelope};

        const KEY: &str = "acct-1-3";

        struct Case {
            name: &'static str,
            /// The record the winner left behind under `KEY`.
            prior: fn(u64) -> Record<()>,
            /// The variant both paths must report, and the class both must give it.
            want: fn(&crate::Error) -> bool,
            want_terminal: bool,
        }
        let cases = [
            Case {
                name: "in_flight_claim",
                prior: Record::pending,
                want: |e| matches!(e, crate::Error::InProgress(k) if k == KEY),
                want_terminal: false,
            },
            Case {
                name: "cached_terminal_failure",
                prior: |h| Record::failed(h, "codec error: unknown field `amount`"),
                want: |e| matches!(e, crate::Error::CachedFailure(m) if m == "codec error: unknown field `amount`"),
                want_terminal: true,
            },
            Case {
                // The winner recorded a different payload under this key: flipping a bit of the hash
                // is exactly what a second, different command (or event body) would produce.
                name: "key_reused_with_another_payload",
                prior: |h| Record::done(h ^ 1, ()),
                want: |e| matches!(e, crate::Error::PayloadMismatch(k) if k == KEY),
                want_terminal: true,
            },
        ];

        // Hash the way each path does: the command value is serialized and canonicalized, the event
        // body is hashed raw. The body is already canonical, so both land on one hash and one record.
        let hash = command_hash(&deposit()).unwrap();
        assert_eq!(hash, payload_hash(DEPOSIT_BODY), "the fixture body must already be canonical");

        for c in cases {
            // --- command path ---
            let command_store: Arc<dyn IdempotencyStore<(), Tx = MemoryTx> + Send + Sync> = Arc::new(MemoryIdempotencyStore::<()>::new());
            command_store.complete(&mut MemoryTx::auto_commit(), KEY, (c.prior)(hash)).unwrap();
            let mut bus: CommandBus<Deposit, ()> =
                CommandBus::new().with(idempotent(Arc::clone(&command_store), |_: &Deposit| Some(KEY.to_string())));
            bus.register("Deposit", Box::new(|_: &Deposit| Ok(()))).unwrap();
            let command_err = match bus.dispatch(&deposit()) {
                Err(CommandBusError::Handler(e)) => e,
                other => panic!("{}: expected a handler error, got {:?}", c.name, other.map_err(|e| e.to_string())),
            };

            // --- consume path ---
            let event_store: crate::EventIdempotencyStore<MemoryTx> = Arc::new(MemoryIdempotencyStore::<()>::new());
            event_store.complete(&mut MemoryTx::auto_commit(), KEY, (c.prior)(hash)).unwrap();
            let mut router = crate::EventRouter::new();
            router.add_middleware(crate::idempotent_by_id(Arc::clone(&event_store)));
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

            let command_err = command_err.downcast_ref::<crate::Error>().expect(c.name);
            assert!((c.want)(command_err), "{}: command path reported {command_err}", c.name);
            assert!((c.want)(&event_err), "{}: consume path reported {event_err}", c.name);
            assert_eq!(command_err.to_string(), event_err.to_string(), "{}: one message", c.name);
            assert_eq!(is_terminal(command_err), c.want_terminal, "{}: command class", c.name);
            assert_eq!(is_terminal(&event_err), c.want_terminal, "{}: consume class", c.name);
        }
    }

    #[test]
    fn retry_branches() {
        struct Case {
            name: &'static str,
            attempts: usize,
            /// The handler fails this many times before it succeeds.
            failures: usize,
            terminal: bool,
            want_calls: usize,
            /// `Ok(amount)` or the message of the last handler error.
            want: Result<i64, &'static str>,
        }
        let cases = [
            Case {
                name: "succeeds after transient failures",
                attempts: 3,
                failures: 2,
                terminal: false,
                want_calls: 3,
                want: Ok(7),
            },
            Case {
                name: "terminal error returns at once without burning attempts",
                attempts: 3,
                failures: 1,
                terminal: true,
                want_calls: 1,
                want: Err("codec error: bad bytes"),
            },
            Case {
                name: "exhausted attempts return the last transient error",
                attempts: 2,
                failures: 5,
                terminal: false,
                want_calls: 2,
                want: Err("transient"),
            },
            Case {
                name: "zero attempts clamps to one",
                attempts: 0,
                failures: 1,
                terminal: false,
                want_calls: 1,
                want: Err("transient"),
            },
        ];
        for c in cases {
            let calls = Arc::new(Counter::default());
            let calls_h = Arc::clone(&calls);
            let (failures, terminal) = (c.failures, c.terminal);
            let mut bus: CommandBus<Pay, i64> = CommandBus::new().with(retry(c.attempts));
            bus.register(
                "Pay",
                Box::new(move |cmd: &Pay| {
                    let n = calls_h.get() + 1;
                    calls_h.set(n);
                    if n > failures {
                        Ok(cmd.amount)
                    } else if terminal {
                        Err(crate::Error::Codec("bad bytes".into()).into())
                    } else {
                        Err("transient".into())
                    }
                }),
            )
            .unwrap();
            let got = bus.dispatch(&pay("x", 7));
            assert_eq!(calls.get(), c.want_calls, "{}: handler calls", c.name);
            match (c.want, got) {
                (Ok(want), Ok(got)) => assert_eq!(got, want, "{}", c.name),
                (Err(want), Err(CommandBusError::Handler(e))) => assert_eq!(e.to_string(), want, "{}", c.name),
                (want, got) => panic!("{}: want {want:?}, got {:?}", c.name, got.map_err(|e| e.to_string())),
            }
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
            let store = Arc::new(CountingStore {
                inner: MemStore::default(),
                claims: Arc::new(Counter::default()),
                releases: Arc::new(Counter::default()),
            });
            let calls = Arc::new(Counter::default());
            let calls_h = Arc::clone(&calls);
            let mut bus: CommandBus<NotJson, i64> =
                CommandBus::new().with(idempotent(Arc::clone(&store) as Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync>, c.key));
            bus.register(
                "Pay",
                Box::new(move |_: &NotJson| {
                    calls_h.set(calls_h.get() + 1);
                    Ok(1)
                }),
            )
            .unwrap();
            for _ in 0..2 {
                match (c.want_err, bus.dispatch(&NotJson)) {
                    (None, Ok(1)) => {}
                    (Some(want), Err(CommandBusError::Handler(e))) => {
                        // The variant matters as much as the text: a serialization failure is a
                        // marshaling failure, and `is_terminal` reads the variant to stop retrying.
                        assert!(
                            matches!(e.downcast_ref::<crate::Error>(), Some(crate::Error::Codec(m)) if m.contains(want)),
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
        // winner's three outcomes, and the three verdicts a loser reads off the winner's record.
        /// What one dispatch should return: `None` expects the dispatched amount, `Some(f)` an error
        /// matching `f`. Each matcher asserts the concrete variant, not just its text — the variant is
        /// what [`is_terminal`] branches on.
        type Matcher = fn(&(dyn std::error::Error + 'static)) -> bool;
        type Expect = Option<Matcher>;
        struct Case {
            name: &'static str,
            /// Per handler run: `None` returns the amount, `Some(f)` returns that error.
            errors: &'static [Option<fn() -> HandlerError>],
            want: [Expect; 2],
            /// A concurrent dispatch holds the claim before the first one runs.
            held: bool,
            /// The second dispatch's amount; a different one reuses the key with another payload.
            redispatched: i64,
            want_calls: usize,
            want_claims: usize,
            want_releases: usize,
        }
        fn codec() -> HandlerError {
            crate::Error::Codec("bad payload".into()).into()
        }
        fn blip() -> HandlerError {
            "downstream blip".into()
        }
        fn is_blip(e: &(dyn std::error::Error + 'static)) -> bool {
            e.to_string() == "downstream blip"
        }
        fn is_codec(e: &(dyn std::error::Error + 'static)) -> bool {
            matches!(e.downcast_ref::<crate::Error>(), Some(crate::Error::Codec(m)) if m == "bad payload")
        }
        fn is_in_progress(e: &(dyn std::error::Error + 'static)) -> bool {
            matches!(e.downcast_ref::<crate::Error>(), Some(crate::Error::InProgress(k)) if k == "k1")
                && e.to_string().contains("in-flight")
        }
        fn is_cached_failure(e: &(dyn std::error::Error + 'static)) -> bool {
            matches!(e.downcast_ref::<crate::Error>(), Some(crate::Error::CachedFailure(m)) if m.contains("bad payload"))
                && e.to_string().contains("replayed cached handler failure")
        }
        fn is_mismatch(e: &(dyn std::error::Error + 'static)) -> bool {
            matches!(e.downcast_ref::<crate::Error>(), Some(crate::Error::PayloadMismatch(k)) if k == "k1")
                && e.to_string().contains("reused with a different payload")
        }
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
            },
            Case {
                name: "claim held by a concurrent dispatch reports in progress",
                errors: &[],
                want: [Some(is_in_progress), Some(is_in_progress)],
                held: true,
                redispatched: 10,
                want_calls: 0,
                want_claims: 2,
                want_releases: 0,
            },
            Case {
                name: "transient failure releases the claim so the next dispatch reruns",
                errors: &[Some(blip), None],
                want: [Some(is_blip), None],
                held: false,
                redispatched: 10,
                want_calls: 2,
                want_claims: 2,
                want_releases: 1,
            },
            Case {
                name: "terminal failure is cached and the next dispatch replays it",
                errors: &[Some(codec)],
                want: [Some(is_codec), Some(is_cached_failure)],
                held: false,
                redispatched: 10,
                want_calls: 1,
                want_claims: 2,
                want_releases: 0,
            },
            Case {
                name: "same key with a different payload fails with PayloadMismatch",
                errors: &[None],
                want: [None, Some(is_mismatch)],
                held: false,
                redispatched: 99,
                want_calls: 1,
                want_claims: 2,
                want_releases: 0,
            },
        ];
        for c in cases {
            let store = Arc::new(CountingStore {
                inner: MemStore::default(),
                claims: Arc::new(Counter::default()),
                releases: Arc::new(Counter::default()),
            });
            let calls = Arc::new(Counter::default());
            let calls_h = Arc::clone(&calls);
            let errors = c.errors;
            let mut bus: CommandBus<Pay, i64> = CommandBus::new().with(idempotent(
                Arc::clone(&store) as Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync>,
                |c: &Pay| Some(c.id.clone()),
            ));
            bus.register(
                "Pay",
                Box::new(move |cmd: &Pay| {
                    let n = calls_h.get();
                    calls_h.set(n + 1);
                    match errors[n] {
                        Some(make) => Err(make()),
                        None => Ok(cmd.amount),
                    }
                }),
            )
            .unwrap();
            if c.held {
                let hash = command_hash(&pay("k1", 10)).unwrap();
                assert!(matches!(store.claim(&mut MemoryTx::auto_commit(), "k1", hash), Ok(Claim::Won)), "{}", c.name);
                store.claims.set(0);
            }

            for (amount, want) in [10, c.redispatched].into_iter().zip(c.want) {
                let got = bus.dispatch(&pay("k1", amount));
                match (want, got) {
                    (None, Ok(got)) => assert_eq!(got, amount, "{}", c.name),
                    (Some(matches), Err(CommandBusError::Handler(e))) => {
                        assert!(matches(&*e), "{}: unexpected error {e}", c.name)
                    }
                    (want, got) => panic!("{}: want {:?}, got {got:?}", c.name, want.map(|_| "an error")),
                }
            }
            assert_eq!(calls.get(), c.want_calls, "{}: handler calls", c.name);
            assert_eq!(store.claims.get(), c.want_claims, "{}: claims", c.name);
            assert_eq!(store.releases.get(), c.want_releases, "{}: releases", c.name);
        }
    }

    #[test]
    fn a_panicking_handler_releases_the_claim() {
        // An unwinding handler must not strand the claim: without the release, every later dispatch
        // of that key would read a pending record and fail with `InProgress` forever.
        let claims = Arc::new(Counter::default());
        let releases = Arc::new(Counter::default());
        let store: Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync> = Arc::new(CountingStore {
            inner: MemStore::default(),
            claims: Arc::clone(&claims),
            releases: Arc::clone(&releases),
        });
        let calls = Arc::new(Counter::default());
        let calls_h = Arc::clone(&calls);
        let mut bus: CommandBus<Pay, i64> =
            CommandBus::new().with(idempotent(Arc::clone(&store), |c: &Pay| Some(c.id.clone())));
        bus.register(
            "Pay",
            Box::new(move |c: &Pay| {
                calls_h.set(calls_h.get() + 1);
                if calls_h.get() == 1 {
                    panic!("handler bug");
                }
                Ok(c.amount)
            }),
        )
        .unwrap();

        let dispatched = catch_unwind(AssertUnwindSafe(|| bus.dispatch(&pay("k1", 10))));
        assert!(dispatched.is_err(), "the panic reaches the caller");
        assert_eq!(releases.get(), 1, "the claim is released, not stranded");
        assert_eq!(bus.dispatch(&pay("k1", 10)).unwrap(), 10, "the next dispatch re-runs the handler");
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn custom_middleware_registers_and_runs() {
        // An arbitrary user middleware (not retry/idempotent) registered on an existing bus and run.
        let seen = Arc::new(Counter::default());
        let seen_mw = Arc::clone(&seen);
        let count: Middleware<Pay, i64> = Box::new(move |next: Handler<Pay, i64>| {
            let seen = Arc::clone(&seen_mw);
            Box::new(move |cmd: &Pay| {
                seen.set(seen.get() + 1);
                next(cmd)
            }) as Handler<Pay, i64>
        });

        let mut bus: CommandBus<Pay, i64> = CommandBus::new();
        bus.add_middleware(count);
        bus.register("Pay", Box::new(|c: &Pay| Ok(c.amount))).unwrap();

        assert_eq!(bus.dispatch(&pay("a", 3)).unwrap(), 3);
        assert_eq!(bus.dispatch(&pay("b", 4)).unwrap(), 4);
        assert_eq!(seen.get(), 2); // middleware ran on every dispatch
    }

    #[test]
    fn replayed_cached_failure_is_terminal_and_stops_retrying() {
        let store: Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync> = Arc::new(MemStore::default());
        let calls = Arc::new(Counter::default());
        let calls_h = Arc::clone(&calls);

        let mut bus: CommandBus<Pay, i64> = CommandBus::new()
            .with(idempotent(Arc::clone(&store), |c: &Pay| Some(c.id.clone())))
            .with(retry(3));
        bus.register(
            "Pay",
            Box::new(move |_c: &Pay| {
                calls_h.set(calls_h.get() + 1);
                Err(crate::Error::Codec("bad payload".into()).into())
            }),
        )
        .unwrap();

        let cmd = pay("k1", 10);

        // Terminal handler failure: retry returns at once (1 call, not 3) and the outcome is cached.
        assert!(bus.dispatch(&cmd).is_err());
        assert_eq!(calls.get(), 1);

        // Redelivery replays it as CachedFailure, itself terminal — so retry stops here instead of
        // burning attempts, and the broker stops redelivering a deterministically failing command.
        match bus.dispatch(&cmd) {
            Err(CommandBusError::Handler(e)) => match e.downcast_ref::<crate::Error>() {
                Some(crate::Error::CachedFailure(m)) => assert!(m.contains("bad payload"), "{m}"),
                _ => panic!("expected CachedFailure, got {e}"),
            },
            other => panic!("expected cached failure, got {other:?}"),
        }
        assert_eq!(calls.get(), 1); // handler never re-ran
    }

    /// Counts claims so the nesting order is observable: `idempotent` outside, `retry` inside means
    /// every attempt runs under **one** claim. Swapped, each retry would take (and release) its own.
    struct CountingStore {
        inner: MemStore,
        claims: Arc<Counter>,
        releases: Arc<Counter>,
    }

    impl IdempotencyStore<i64> for CountingStore {
        type Tx = MemoryTx;
        fn claim(&self, tx: &mut MemoryTx, key: &str, payload_hash: u64) -> Result<Claim<i64>, crate::Error> {
            self.claims.set(self.claims.get() + 1);
            self.inner.claim(tx, key, payload_hash)
        }
        fn complete(&self, tx: &mut MemoryTx, key: &str, record: Record<i64>) -> Result<(), crate::Error> {
            self.inner.complete(tx, key, record)
        }
        fn release(&self, tx: &mut MemoryTx, key: &str) -> Result<(), crate::Error> {
            self.releases.set(self.releases.get() + 1);
            self.inner.release(tx, key)
        }
    }

    #[test]
    fn retry_inside_idempotent_shares_one_claim() {
        let claims = Arc::new(Counter::default());
        let releases = Arc::new(Counter::default());
        let store: Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync> = Arc::new(CountingStore {
            inner: MemStore::default(),
            claims: Arc::clone(&claims),
            releases: Arc::clone(&releases),
        });

        let calls = Arc::new(Counter::default());
        let calls_h = Arc::clone(&calls);
        // Order matters: idempotent first (outermost), retry inside it.
        let mut bus: CommandBus<Pay, i64> = CommandBus::new()
            .with(idempotent(Arc::clone(&store), |c: &Pay| Some(c.id.clone())))
            .with(retry(2));
        bus.register(
            "Pay",
            Box::new(move |c: &Pay| {
                calls_h.set(calls_h.get() + 1);
                if calls_h.get() == 1 {
                    Err("transient downstream blip".into())
                } else {
                    Ok(c.amount)
                }
            }),
        )
        .unwrap();

        // One transient failure, one retry (a single backoff_millis(0) sleep), one success.
        let started = std::time::Instant::now();
        assert_eq!(bus.dispatch(&pay("k1", 5)).unwrap(), 5);
        assert_eq!(calls.get(), 2);
        assert_eq!(claims.get(), 1, "both attempts ran under one claim");
        assert_eq!(releases.get(), 0, "the claim was never released — the retry succeeded under it");
        assert!(started.elapsed() >= Duration::from_millis(edax_core::backoff_millis(0)));

        // The claim completed, so a redelivery replays the cached result without another attempt.
        assert_eq!(bus.dispatch(&pay("k1", 5)).unwrap(), 5);
        assert_eq!(calls.get(), 2);
        assert_eq!(claims.get(), 2);
    }

    /// A store that cannot take the claim at all, and counts what the guard did about it.
    struct ClaimFails {
        inner: MemStore,
        releases: Arc<Counter>,
    }

    impl IdempotencyStore<i64> for ClaimFails {
        type Tx = MemoryTx;
        fn claim(&self, _tx: &mut MemoryTx, _key: &str, _hash: u64) -> Result<Claim<i64>, crate::Error> {
            Err(crate::Error::Store("could not reach the ledger".into()))
        }
        fn complete(&self, tx: &mut MemoryTx, key: &str, record: Record<i64>) -> Result<(), crate::Error> {
            self.inner.complete(tx, key, record)
        }
        fn release(&self, tx: &mut MemoryTx, key: &str) -> Result<(), crate::Error> {
            self.releases.set(self.releases.get() + 1);
            self.inner.release(tx, key)
        }
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
            Arc::new(ClaimFails { inner: MemStore::default(), releases: Arc::clone(&releases) });

        let calls_h = Arc::clone(&calls);
        let mut bus: CommandBus<Pay, i64> =
            CommandBus::new().with(idempotent(store, |c: &Pay| Some(c.id.clone())));
        bus.register(
            "Pay",
            Box::new(move |c: &Pay| {
                calls_h.set(calls_h.get() + 1);
                Ok(c.amount)
            }),
        )
        .unwrap();

        let err = bus.dispatch(&pay("k1", 5)).expect_err("the claim failed");
        let CommandBusError::Handler(source) = err else { panic!("expected a wrapped handler failure") };
        // The store's own verdict, not a `CachedFailure` standing in for it.
        let store_error = source.downcast_ref::<crate::Error>().expect("the SDK error survives the box");
        assert!(matches!(store_error, crate::Error::Store(m) if m.contains("reach the ledger")), "got {store_error}");
        assert_eq!(releases.get(), 0, "a failed claim must not be compensated");
        assert_eq!(calls.get(), 0, "the handler must not run without a claim");
    }
}

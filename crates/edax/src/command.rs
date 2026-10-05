//! Command bus (CQRS write side): register handlers by command type, dispatch a [`CommandMessage`]
//! through a middleware chain, and guard a dispatch by the message's idempotency key. The message is the
//! unit of dispatch: middleware receives it with the command, and the handler receives the command alone.
//! The kernel decides payload canonicalization, hashing, the received-message key and the idempotency
//! verdict; the guard engine is [`crate::idempotency`].

use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::sync::Arc;

use edax_core::{ErrorCode, Tenancy};
use edax_cqrs::{claim_key, command_hash};
use edax_event::{check_extensions, originate, CommandMessage, Extensions, Lineage, Metadata, Payload, FIRST_VERSION};
use serde::Serialize;

use crate::aggregate::PayloadCodec;
use crate::chain::{register_once, wrap_outermost_first};
use crate::failure::HandlerError;
use crate::idempotency::{Guard, IdempotencyStore};
use crate::pacing::Pacing;
use crate::unit_of_work::{in_unit_of_work, retry_with};
use crate::Error;

/// Implemented by every command: its type's name, [`Command::TYPE`], routes it to its handler, and
/// [`Command::Output`] is what that handler returns, so a dispatch is typed at compile time.
///
/// The idempotency hash covers the type name and the command *value*: serde serializes the value, then
/// the kernel wraps it with the name, canonicalizes and hashes the bytes ([`edax_cqrs::command_hash`]),
/// as pinned by `testdata/golden/command_hash.json`. A command cannot supply the bytes, so `Serialize`
/// declares the hashed shape. The name is hashed beside the value, never inside it. Each command is its
/// own type with one name, so no enum, whose serde tag would enter the value and split one command into
/// two hashes across SDKs, can be dispatched.
///
/// A dispatch whose result type is not the command's `Output` does not compile:
///
/// ```compile_fail
/// use edax::{Command, CommandBus, DispatchOptions};
///
/// #[derive(serde::Serialize)]
/// struct Pay { amount: i64 }
///
/// impl Command for Pay {
///     const TYPE: &'static str = "Pay";
///     type Output = i64;
/// }
///
/// let bus = CommandBus::new();
/// let receipt: Result<String, _> = bus.dispatch(&Pay { amount: 7 }, DispatchOptions::default());
/// ```
pub trait Command: Serialize {
    /// The command's stable name: the bus routes by it, the message carries it as `name`, and the
    /// idempotency hash covers it.
    const TYPE: &'static str;
    /// What the command's handler returns, and what a guard caches for a replay.
    type Output;
}

/// The host's command handler: it takes the command alone and returns its [`Command::Output`], or an
/// error. `Send + Sync` so one bus can be shared by a
/// [`KeyOrderedDispatcher`](crate::KeyOrderedDispatcher)'s worker threads.
pub type Handler<C> = Box<dyn Fn(&C) -> Result<<C as Command>::Output, HandlerError> + Send + Sync>;
/// One step of a dispatch, as middleware sees it: the [`CommandMessage`] being dispatched and the command
/// it carries. A guard reads the idempotency key and the tenant off the message, so it cannot miss them.
pub type Next<C> = Box<dyn Fn(&CommandMessage, &C) -> Result<<C as Command>::Output, HandlerError> + Send + Sync>;
/// Wraps the next step of a dispatch, running logic before and/or after it.
pub type Middleware<C> = Box<dyn Fn(Next<C>) -> Next<C> + Send + Sync>;

/// How [`CommandBus::dispatch`] builds the message it runs: the request's idempotency key (`None` runs
/// unguarded), the aggregate that orders it, the tenant it belongs to, its lineage (a new flow when
/// `None`), and its extensions and metadata. Every field defaults to empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DispatchOptions {
    /// Names the request: two messages with one key are one request, and a guard replays the second.
    pub idempotency_key: Option<String>,
    /// The aggregate the command targets, which orders it on a broker.
    pub aggregate_id: Option<String>,
    /// Whose command it is: a guard composes its ledger key from it under a pool.
    pub tenant: Option<String>,
    /// The flow the command continues; `None` starts a new one rooted at the message.
    pub lineage: Option<Lineage>,
    /// Edax extension context, under keys the kernel's rule accepts.
    pub extensions: Extensions,
    /// The application's own context.
    pub metadata: Metadata,
}

impl DispatchOptions {
    /// A dispatch of the request named `key`, with every other field empty.
    pub fn keyed(key: impl Into<String>) -> Self {
        Self {
            idempotency_key: Some(key.into()),
            ..Self::default()
        }
    }
}

/// The [`CommandMessage`] for `cmd`: what [`CommandBus::dispatch`] runs, and what a host publishes to a
/// broker. `name`, `version` and `data` are what `codec.marshal(cmd)` returns, `id` a fresh
/// [`guid`](edax_core::guid), and `issued_at` the kernel clock. Build a message another process runs with
/// [`CommandBus::dispatch_message`] with the codec that consumer reads it with: under another codec the two
/// disagree on the version and the encoding, so the consumer upcasts data that is already current, or
/// cannot read it at all.
///
/// # Errors
/// The terminal `codec` error for a command the codec cannot write, a codec that names it other than
/// [`Command::TYPE`] (a dispatch finds its handler by the type and a received message by its name, so the
/// two would run one command through different handlers), or an extension key outside the kernel's rule.
pub fn command_message<C, CC>(cmd: &C, codec: &CC, options: DispatchOptions) -> Result<CommandMessage, Error>
where
    C: Command,
    CC: PayloadCodec<Domain = C> + ?Sized,
{
    let payload = codec.marshal(cmd)?;
    if payload.name != C::TYPE {
        return Err(Error::codec(format_args!(
            "the codec names a {:?} command {:?}; a message name must be its type",
            C::TYPE,
            payload.name
        )));
    }
    check_extensions(&options.extensions)?;
    let id = edax_core::guid();
    let lineage = options.lineage.unwrap_or_else(|| originate(None, None, &id));
    Ok(CommandMessage {
        id,
        idempotency_key: options.idempotency_key,
        aggregate_id: options.aggregate_id,
        tenant: options.tenant,
        name: payload.name,
        version: payload.version,
        data: payload.data,
        correlation_id: lineage.correlation_id,
        causation_id: lineage.causation_id,
        issued_at: edax_core::current_timestamp(),
        extensions: options.extensions,
        metadata: options.metadata,
    })
}

thread_local! {
    /// Whether the dispatch running on this thread received its message
    /// ([`CommandBus::dispatch_message`]) rather than built it ([`CommandBus::dispatch`]). A handler is a
    /// sync closure on its caller's thread, so a guard inside the chain reads the dispatch it runs in.
    static RECEIVED: Cell<bool> = const { Cell::new(false) };
}

/// Marks the dispatch running on this thread as received or not until dropped, panics included, then
/// gives the outer dispatch its mark back, so a dispatch nested in a handler reads its own.
struct Dispatching {
    outer: bool,
}

impl Dispatching {
    fn new(received: bool) -> Self {
        Self {
            outer: RECEIVED.replace(received),
        }
    }
}

impl Drop for Dispatching {
    fn drop(&mut self) {
        RECEIVED.set(self.outer);
    }
}

/// The key a guard claims for `msg`: the kernel's [`received_key`](edax_cqrs::received_key) when the
/// running dispatch received it, else its own idempotency key, `None` running it unguarded.
fn guard_key(msg: &CommandMessage) -> Option<&str> {
    if RECEIVED.get() {
        return Some(edax_cqrs::received_key(msg.idempotency_key.as_deref(), &msg.id));
    }
    msg.idempotency_key.as_deref()
}

/// `handler` wrapped in `middleware`, the first outermost: the chain a dispatch runs. Middleware is per
/// command type on a [`CommandBus`], because each one is typed by the command it reads and the result it
/// caches.
fn wrap<C: Command + 'static>(handler: Handler<C>, middleware: &[Middleware<C>]) -> Next<C> {
    let step: Next<C> = Box::new(move |_: &CommandMessage, cmd: &C| handler(cmd));
    wrap_outermost_first(step, middleware)
}

/// One command type's registration: the chain a dispatch runs, and the codec its messages cross the wire
/// with, both ways.
struct Registered<C: Command> {
    chain: Next<C>,
    codec: Box<dyn PayloadCodec<Domain = C> + Send + Sync>,
}

/// The codec of a command type registered for in-process dispatch only: it writes the command's JSON at
/// the first schema version, as a Python bus without a codec does, and reads nothing.
struct InProcess<C>(PhantomData<fn() -> C>);

impl<C: Command> PayloadCodec for InProcess<C> {
    type Domain = C;

    fn marshal(&self, cmd: &C) -> Result<Payload, Error> {
        let data = serde_json::to_vec(cmd)
            .map_err(|e| Error::codec(format_args!("{} is not JSON-encodable: {e}", C::TYPE)))?;
        Ok(Payload {
            name: C::TYPE.to_owned(),
            version: FIRST_VERSION,
            data,
        })
    }

    fn unmarshal(&self, _: &Payload) -> Result<C, Error> {
        Err(Error::bad_handle(format_args!(
            "{} was registered without a codec, so the bus reads no received message of it; register it with register_with_codec",
            C::TYPE
        )))
    }
}

/// Dispatches command messages to handlers registered by command type. One bus routes any number of
/// command types, each registered as its own Rust type with its own [`Command::Output`], so no enum (and no
/// serde tag) stands between a command and its idempotency hash.
#[derive(Default)]
pub struct CommandBus {
    /// Each command type's [`Registered`], type-erased, under its [`Command::TYPE`].
    handlers: HashMap<&'static str, Box<dyn Any + Send + Sync>>,
}

impl CommandBus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install `handler` for commands of type `C`, under [`Command::TYPE`], wrapped in `middleware`, the
    /// first outermost, for in-process dispatch only, as a Python bus built without a codec: the message
    /// [`dispatch`](Self::dispatch) builds carries the command's JSON at the first schema version, and
    /// [`dispatch_message`](Self::dispatch_message) refuses a received one with the fatal
    /// [`ErrorCode::BadHandle`]. A command that crosses the wire registers with
    /// [`register_with_codec`](Self::register_with_codec).
    ///
    /// # Errors
    /// The terminal [`ErrorCode::AlreadyRegistered`] if the command type already has a handler.
    pub fn register<C: Command + 'static>(
        &mut self,
        handler: Handler<C>,
        middleware: Vec<Middleware<C>>,
    ) -> Result<(), Error> {
        self.register_with_codec(handler, InProcess::<C>(PhantomData), middleware)
    }

    /// [`register`](Self::register) with `codec`, the type's wire contract, as Python's
    /// `CommandBus(codec=)` is the bus's: [`dispatch`](Self::dispatch) builds its message with it and
    /// [`dispatch_message`](Self::dispatch_message) reads one with it, so the message a middleware
    /// forwards is one a consumer reading with the same codec runs.
    ///
    /// # Errors
    /// The terminal [`ErrorCode::AlreadyRegistered`] if the command type already has a handler.
    pub fn register_with_codec<C, CC>(
        &mut self,
        handler: Handler<C>,
        codec: CC,
        middleware: Vec<Middleware<C>>,
    ) -> Result<(), Error>
    where
        C: Command + 'static,
        CC: PayloadCodec<Domain = C> + Send + Sync + 'static,
    {
        register_once(&mut self.handlers, C::TYPE, || {
            Box::new(Registered {
                chain: wrap(handler, &middleware),
                codec: Box::new(codec),
            })
        })
    }

    /// Run `cmd` as the message [`command_message`] builds from it, the codec `C` was registered with and
    /// `options`, and return the handler's [`Command::Output`]. Pass an idempotency key for a request a
    /// client may send twice; [`idempotent`] claims it.
    ///
    /// # Errors
    /// The terminal [`ErrorCode::HandlerNotFound`] when no handler is registered under `C::TYPE`, or when
    /// another Rust type registered under the same name (a wiring mistake that fails the same way on every
    /// redelivery); the terminal `codec` error of [`command_message`]. Otherwise whatever the chain
    /// returned.
    pub fn dispatch<C: Command + 'static>(&self, cmd: &C, options: DispatchOptions) -> Result<C::Output, HandlerError> {
        let registered = self.registered::<C>()?;
        let msg = command_message(cmd, &*registered.codec, options)?;
        let _dispatching = Dispatching::new(false);
        (registered.chain)(&msg, cmd)
    }

    /// Run a received [`CommandMessage`]: off a broker, or from a command outbox. The codec `C` was
    /// registered with turns it back into the command by `name` and `version`; the message runs as
    /// received, its id, lineage and key what the sender set. A guard claims a message with no idempotency
    /// key under its id, the kernel's [`received_key`](edax_cqrs::received_key): a broker delivers at
    /// least once, both copies carry one id, and a guard replays the second.
    ///
    /// # Errors
    /// The terminal [`ErrorCode::HandlerNotFound`] for a message whose name is not `C::TYPE`, or a type
    /// with no handler; the terminal `codec` error of a message the codec cannot read. Otherwise whatever
    /// the chain returned.
    pub fn dispatch_message<C: Command + 'static>(&self, msg: &CommandMessage) -> Result<C::Output, HandlerError> {
        if msg.name != C::TYPE {
            return Err(Error::new(
                ErrorCode::HandlerNotFound,
                format_args!("message {:?} is no {:?} command", msg.name, C::TYPE),
            )
            .into());
        }
        let registered = self.registered::<C>()?;
        let cmd = registered.codec.unmarshal(&msg.payload())?;
        let _dispatching = Dispatching::new(true);
        (registered.chain)(msg, &cmd)
    }

    /// What `C` was registered with.
    fn registered<C: Command + 'static>(&self) -> Result<&Registered<C>, Error> {
        let registered = self
            .handlers
            .get(C::TYPE)
            .ok_or_else(|| Error::new(ErrorCode::HandlerNotFound, C::TYPE))?;
        registered.downcast_ref::<Registered<C>>().ok_or_else(|| {
            Error::new(
                ErrorCode::HandlerNotFound,
                format_args!("{} is registered for another command type", C::TYPE),
            )
        })
    }
}

/// Retry the handler up to `attempts` times, waiting each step of `pacing`, and return the last error.
/// `attempts` counts runs, and a count of zero is unrepresentable, as the kernel's
/// [`edax_event::retry_step`] takes it. A terminal error returns at once without spending attempts; a transient one is
/// retried. The wait blocks; wrap the dispatch in your async runtime for non-blocking retry.
///
/// A retry must re-run a whole unit of work, never a handler inside one: a second attempt in the same
/// transaction reads the first attempt's staged writes and applies them again, and one commit lands both.
/// So `retry` refuses to run inside a [`transaction`](crate::transaction) body on the same thread, with the
/// terminal [`ErrorCode::BadHandle`]. Under [`idempotent_in`], retry the whole unit of work with
/// [`retry_transaction`](crate::retry_transaction) instead. Inside [`idempotent`] with no transaction, every
/// attempt's writes commit on their own: at-least-once, so the handler must tolerate a re-run.
pub fn retry<C: Command + 'static>(attempts: NonZeroU32, pacing: Pacing) -> Middleware<C> {
    let pacing = Arc::new(pacing);
    Box::new(move |next: Next<C>| {
        let pacing = Arc::clone(&pacing);
        Box::new(move |msg: &CommandMessage, cmd: &C| {
            if in_unit_of_work() {
                return Err(Box::new(Error::bad_handle(
                    "retry was dispatched inside an open transaction: a second attempt would re-apply the first \
                     attempt's staged writes; retry the whole unit of work with retry_transaction",
                )));
            }
            retry_with(attempts, &pacing, || next(msg, cmd))
        })
    })
}

/// Deduplicate dispatches by the idempotency key of the message each one runs ([`Next`]). A message
/// [`CommandBus::dispatch`] built with none runs unguarded; one [`CommandBus::dispatch_message`] received
/// with none is claimed under its id ([`received_key`](edax_cqrs::received_key)). The guard claims the kernel's [`claim_key`] for `scope`, `tenancy`, the key and the
/// message's tenant, so two guards with different scopes on one store (an ingress and a consumer, say)
/// never replay each other's outcome, and a Python guard of the same scope claims the same row. Under
/// [`Tenancy::Pool`] the key carries the tenant, so one request id reused by two tenants claims two rows.
/// Under a pool or a bridge, a message that names no tenant is the terminal `codec` error, raised before
/// any claim. The guard binds the command's idempotency hash (see [`Command`]) to the claim, so reusing the
/// key with a different command errors. See `docs/sdk/idempotency.md`.
///
/// The first dispatch of a key wins the claim and runs the handler; a later one replays the prior record or
/// gets the transient [`ErrorCode::InProgress`] error while the winner runs. Verdicts surface as an
/// [`Error`] in the [`HandlerError`]. A terminal handler error is cached with its code, and so is a result
/// with no wire form (as `codec`); a transient one or a panic releases the claim. The default handle gives
/// at-least-once with dedup; [`idempotent_in`] commits it with the events.
pub fn idempotent<C, S>(store: S, scope: impl Into<String>, tenancy: Tenancy) -> Middleware<C>
where
    C: Command + 'static,
    C::Output: Clone + Serialize + 'static,
    S: IdempotencyStore<C::Output> + Send + Sync + 'static,
    S::Tx: Default + 'static,
{
    idempotent_in(store, scope, tenancy, |_| S::Tx::default())
}

/// [`idempotent`] over the host's transaction. `current` is called once per guarded dispatch, with the
/// message, and returns the handle for the claim, the terminal record and any release. Sync middleware has
/// no ambient transaction, so the host that opened the unit of work publishes its handle keyed by the
/// message; one slot shared by concurrent dispatchers is a race. The handler enlists its own writes in the
/// same handle, so the claim commits with the events and a rollback drops both. A terminal failure's
/// cached record is written through that handle too, so it lands if the host commits after the error;
/// Python's one-commit guard never writes one. Outside a unit of work `current` returns the auto-commit
/// handle. Never wrap it in [`retry`]: retry the whole unit of work with
/// [`retry_transaction`](crate::retry_transaction).
pub fn idempotent_in<C, S>(
    store: S,
    scope: impl Into<String>,
    tenancy: Tenancy,
    current: impl Fn(&CommandMessage) -> S::Tx + Send + Sync + 'static,
) -> Middleware<C>
where
    C: Command + 'static,
    C::Output: Clone + Serialize + 'static,
    S: IdempotencyStore<C::Output> + Send + Sync + 'static,
    S::Tx: 'static,
{
    let guard = Guard::new(store, scope, current);
    Box::new(move |next: Next<C>| {
        let guard = guard.clone();
        Box::new(
            move |msg: &CommandMessage, cmd: &C| -> Result<C::Output, HandlerError> {
                let Some(key) = guard_key(msg) else {
                    return next(msg, cmd);
                };
                // A key the tenancy refuses, or a command the kernel cannot hash, is the kernel's terminal
                // `codec` error, raised before the claim; the message names the step.
                let claim = claim_key(tenancy, msg.tenant.as_deref(), guard.scope(), key)?;
                let hash = command_hash(C::TYPE, cmd)?;
                guard.run(msg, &claim, hash, || next(msg, cmd))
            },
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failure::{code_of, Terminal};
    use crate::idempotency::{guarded, Claim, ClaimHandle, Record, State};
    use crate::{DispatchOptions, JsonCodec, Next};
    use crate::{EventMessage, MemoryIdempotencyStore, MemoryTx};
    use edax_cqrs::cached_failure;
    use edax_cqrs::{canonical_json, payload_hash};
    use serde::Deserialize;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// The kernel's verdict on a handler failure's code ([`edax_core::is_terminal`]). The SDK never branches
    /// on it, since every consumer hands the code to a kernel decision instead; only these tests read it.
    fn is_terminal(e: &(dyn std::error::Error + 'static)) -> bool {
        edax_core::is_terminal(code_of(e))
    }

    fn attempts(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).expect("a positive attempt count")
    }

    // The in-memory idempotency store is shipped, not re-rolled for tests.
    type MemStore = MemoryIdempotencyStore<i64>;

    /// The scope every command guard here claims under.
    const SCOPE: &str = "payments";

    /// The ledger key a guard scoped [`SCOPE`] claims for idempotency key `key`.
    fn claimed(key: &str) -> String {
        claim_key(Tenancy::Silo, None, SCOPE, key).unwrap()
    }

    /// A call counter the handlers and test stores share. Atomic, because the command path is
    /// `Send + Sync`.
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

    /// The wait the kernel computes for retry `attempt` under `Pacing::recording()`'s fixed draw.
    fn kernel_wait(attempt: u64) -> Duration {
        Duration::from_millis(edax_core::backoff_millis(
            edax_core::Backoff::DEFAULT,
            attempt,
            u64::MAX,
        ))
    }

    // --- a tiny command ---

    #[derive(Serialize, Deserialize)]
    struct Pay {
        id: String,
        amount: i64,
    }

    impl Command for Pay {
        const TYPE: &'static str = "Pay";
        type Output = i64;
    }

    /// The options of a dispatch of `cmd` keyed by its own id: a client's request id.
    fn by_id(cmd: &Pay) -> DispatchOptions {
        DispatchOptions::keyed(cmd.id.clone())
    }

    fn pay(id: &str, amount: i64) -> Pay {
        Pay { id: id.into(), amount }
    }

    /// The wire contract of `Pay`: its JSON at the first schema version.
    fn pay_codec() -> JsonCodec<Pay> {
        JsonCodec::new(Pay::TYPE)
    }

    /// A bus with one `Pay -> i64` handler, wrapped in `middleware`, that reads a received `Pay` too.
    fn pay_bus(middleware: Vec<Middleware<Pay>>, handler: Handler<Pay>) -> CommandBus {
        let mut bus = CommandBus::new();
        bus.register_with_codec(handler, pay_codec(), middleware).unwrap();
        bus
    }

    /// A command whose fields are the event body used on the consume path. The two guards hash it
    /// differently (the command's hash covers its name too), and each reaches one verdict per record.
    #[derive(Serialize)]
    struct Deposit {
        account_id: &'static str,
        amount_cents: u32,
        currency: &'static str,
    }

    impl Command for Deposit {
        const TYPE: &'static str = "Deposit";
        type Output = ();
    }

    fn deposit() -> Deposit {
        Deposit {
            account_id: "acct-1",
            amount_cents: 4200,
            currency: "USD",
        }
    }

    /// The `Deposit` fields as the consume path's event body.
    const DEPOSIT_BODY: &[u8] = br#"{"account_id":"acct-1","amount_cents":4200,"currency":"USD"}"#;

    /// The golden command, as the idiomatic Rust type. Fields are declared in an order that is
    /// neither the vector's nor alphabetical: the hash must not depend on declaration order.
    #[derive(Serialize, Deserialize)]
    struct PlaceOrder {
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

    /// The golden command under each type name the vector gives it. The name routes and is hashed beside
    /// the value, never inside it: each wrapper is transparent, so its value is the order's own shape.
    #[derive(Serialize)]
    #[serde(transparent)]
    struct Placed(PlaceOrder);

    impl Command for Placed {
        const TYPE: &'static str = "order.place";
        type Output = ();
    }

    #[derive(Serialize)]
    #[serde(transparent)]
    struct Quoted(PlaceOrder);

    impl Command for Quoted {
        const TYPE: &'static str = "order.quote";
        type Output = ();
    }

    /// The vector's host-value command: its decimal and instant fields are the SDK's host types, which
    /// canonicalize the text the vector's markers wrap as the command is built.
    #[derive(Serialize, Deserialize)]
    struct Withdraw {
        account: String,
        amount: crate::Decimal,
        at: crate::Instant,
    }

    impl Command for Withdraw {
        const TYPE: &'static str = "Withdraw";
        type Output = ();
    }

    /// `testdata/golden/command_hash.json` asserted through the SDK layer: every case is rebuilt as
    /// a `PlaceOrder` under its row's type name, dispatched through the real `idempotent` middleware,
    /// and the hash the store received must be the vector's. The Python suite does the same from a
    /// dataclass, so this is the proof that both SDKs bind one key to one number for one command.
    #[test]
    fn command_hash_golden_holds_through_the_middleware() {
        let cases = edax_golden::read("command_hash.json");
        assert!(!cases.is_empty(), "the vector has cases");

        type Ledger = Arc<dyn IdempotencyStore<(), Tx = MemoryTx> + Send + Sync>;
        /// Dispatch `cmd` once through a fresh guard keyed `name`: the outcome, how many times the handler
        /// ran, and the ledger the guard wrote.
        fn guarded<C: Command<Output = ()> + 'static>(
            cmd: &C,
            name: &str,
        ) -> (Result<(), HandlerError>, Arc<AtomicUsize>, Ledger) {
            let store: Ledger = Arc::new(MemoryIdempotencyStore::<()>::new());
            let mut bus = CommandBus::new();
            let guard = idempotent(Arc::clone(&store), SCOPE, Tenancy::Silo);
            let runs = Arc::new(AtomicUsize::new(0));
            let counted = Arc::clone(&runs);
            let handler: Handler<C> = Box::new(move |_: &C| {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            });
            bus.register(handler, vec![guard]).unwrap();
            (bus.dispatch(cmd, DispatchOptions::keyed(name)), runs, store)
        }

        for case in &cases {
            let name = case["name"].as_str().expect("case has a name");
            // A refusal row pins the code and the rule. An integer's byte offset in the message is the
            // host encoder's, which writes fields in its own order, so only the rule after it is compared.
            let rule = case["error"]
                .as_str()
                .map(|error| error.rsplit_once(" is outside ").map_or(error, |(_, rule)| rule));
            let command = edax_golden::host_text(&case["command"]);
            let (dispatched, runs, store) = match case["type"].as_str().expect("case has a type") {
                Placed::TYPE => guarded(&Placed(serde_json::from_value(command).expect(name)), name),
                Quoted::TYPE => guarded(&Quoted(serde_json::from_value(command).expect(name)), name),
                // A host value the kernel refuses cannot be built, so no dispatch can carry it.
                Withdraw::TYPE => match serde_json::from_value::<Withdraw>(command) {
                    Ok(withdraw) => guarded(&withdraw, name),
                    Err(refused) => {
                        let rule = rule.expect("a host value fails to build only on a refusal row");
                        assert!(refused.to_string().ends_with(rule), "{name}: {refused}");
                        continue;
                    }
                },
                other => panic!("{name}: no command type {other:?} in this test"),
            };

            // A refusal row: the guard fails as the kernel's terminal `codec` error, before any claim
            // or handler run, and leaves the key free.
            if let Some(rule) = rule {
                let refused = dispatched.expect_err(name);
                assert_eq!(
                    refused.downcast_ref::<Error>().map(|e| e.code),
                    Some(ErrorCode::Codec),
                    "{name}"
                );
                assert!(refused.to_string().ends_with(rule), "{name}: {refused}");
                assert!(is_terminal(&*refused), "{name}: a refusal is terminal");
                assert_eq!(runs.load(Ordering::SeqCst), 0, "{name}: the handler never ran");
                assert!(
                    matches!(
                        store.claim(&mut MemoryTx::auto_commit(), &claimed(name), 0).unwrap(),
                        Claim::Won(_)
                    ),
                    "{name}: no claim was left"
                );
                continue;
            }
            dispatched.expect(name);
            let expected = case["expected"].as_u64().expect("expected is an unsigned 64-bit hash");

            // The hash the middleware bound to the key is what a Python service would compare its
            // own against; a claim with any hash returns the winner's record.
            let stored = match store.claim(&mut MemoryTx::auto_commit(), &claimed(name), 0).unwrap() {
                Claim::Prior(record) => record.payload_hash,
                Claim::Won(_) => panic!("{name}: the dispatch left no record"),
            };
            assert_eq!(stored, expected, "{name}");
        }

        // A hash over the value alone, without the name, must not match the vector: the name is part
        // of the hash.
        let value_only = payload_hash(&canonical_json(&serde_json::to_vec(&cases[0]["command"]).unwrap()).unwrap());
        assert_ne!(
            value_only,
            cases[0]["expected"].as_u64().unwrap(),
            "the value alone reached the vector"
        );
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
            const TYPE: &'static str = "Withdraw";
            type Output = String;
        }
        #[derive(Serialize)]
        enum BankCommand {
            Withdraw { account: &'static str, amount_cents: u32 },
        }

        let ledger = crate::MemoryTransactor::new();
        let receipts: Arc<dyn IdempotencyStore<String, Tx = MemoryTx> + Send + Sync> =
            Arc::new(ledger.idempotency_store::<String>());
        let balances: Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync> =
            Arc::new(ledger.idempotency_store::<i64>());
        let mut bus = CommandBus::new();
        let withdraw_guard = idempotent(Arc::clone(&receipts), SCOPE, Tenancy::Silo);
        let withdraw: Handler<Withdraw> = Box::new(|w: &Withdraw| Ok(format!("receipt for {}", w.amount_cents)));
        bus.register(withdraw, vec![withdraw_guard]).unwrap();
        let pay_guard = idempotent(Arc::clone(&balances), SCOPE, Tenancy::Silo);
        bus.register(Box::new(|p: &Pay| Ok(p.amount * 2)), vec![pay_guard])
            .unwrap();

        let cmd = Withdraw {
            account: "a",
            amount_cents: 500,
        };
        let receipt: String = bus.dispatch(&cmd, DispatchOptions::keyed("w-1")).unwrap();
        assert_eq!(receipt, "receipt for 500");
        assert_eq!(
            bus.dispatch(&pay("p", 21), DispatchOptions::keyed("p-1")).unwrap(),
            42,
            "a second command type and result type"
        );

        let stored = match receipts
            .claim(&mut MemoryTx::auto_commit(), &claimed("w-1"), 0)
            .unwrap()
        {
            Claim::Prior(record) => record.payload_hash,
            Claim::Won(_) => panic!("the dispatch left no record"),
        };
        assert_eq!(stored, command_hash("Withdraw", &cmd).unwrap(), "the struct's own hash");
        let tagged = command_hash(
            "Withdraw",
            &BankCommand::Withdraw {
                account: "a",
                amount_cents: 500,
            },
        )
        .unwrap();
        assert_ne!(stored, tagged, "the enum's tag would have changed the hash");
    }

    #[test]
    fn dispatch_branches() {
        let mut bus = pay_bus(vec![], Box::new(|c: &Pay| Ok(c.amount * 2)));
        assert_eq!(
            bus.dispatch(&pay("a", 21), DispatchOptions::default()).unwrap(),
            42,
            "registered: routes to the handler"
        );

        #[derive(Serialize)]
        struct Other;
        impl Command for Other {
            const TYPE: &'static str = "Other";
            type Output = i64;
        }
        /// Another Rust type under `Pay`'s name: a wiring mistake the type system cannot see.
        #[derive(Serialize)]
        struct Impostor;
        impl Command for Impostor {
            const TYPE: &'static str = "Pay";
            type Output = i64;
        }
        // Each refusal is the kernel's terminal `handler_not_found`: a redelivery finds the same bus.
        let not_found = |e: HandlerError| match e.downcast_ref::<Error>() {
            Some(err) if err.code == ErrorCode::HandlerNotFound => err.message.clone(),
            _ => panic!("expected handler_not_found, got {e}"),
        };
        let unregistered = bus
            .dispatch(&Other, DispatchOptions::default())
            .expect_err("nothing registered for Other");
        assert!(is_terminal(&*unregistered));
        assert_eq!(not_found(unregistered), "Other");
        let impostor = bus
            .dispatch(&Impostor, DispatchOptions::default())
            .expect_err("Pay is registered for Pay");
        assert_eq!(not_found(impostor), "Pay is registered for another command type");

        // A duplicate registration is the same terminal `already_registered` the router reports.
        let duplicate = bus.register(Box::new(|c: &Pay| Ok(c.amount)), vec![]).unwrap_err();
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
            .map(|&code| {
                (
                    code.as_str().to_owned(),
                    Box::new(Error::new(code, "x")) as HandlerError,
                    code.is_terminal(),
                    Some(code),
                )
            })
            .collect();
        cases.push((
            "host_rejection_marked_terminal".into(),
            Box::new(Terminal::new("insufficient funds")),
            true,
            Some(ErrorCode::Rejected),
        ));
        cases.push((
            "unclassified_host_error_is_transient".into(),
            "ledger service returned 503".into(),
            false,
            None,
        ));
        for (name, error, want, code) in cases {
            assert_eq!(is_terminal(&*error), want, "{name}");
            assert_eq!(code_of(&*error), code, "{name}: code");
        }
        let rejection = Terminal::new("insufficient funds");
        assert_eq!(rejection.to_string(), "insufficient funds", "a wrapper adds no text");
        assert_eq!(
            std::error::Error::source(&rejection)
                .map(ToString::to_string)
                .as_deref(),
            Some("insufficient funds")
        );
    }

    /// The two idempotency guards must cache and release on **one** rule. Each lost-claim verdict is
    /// driven through both — `idempotent` on the command path, `idempotent_by_id` on the consume
    /// path — against the same kind of seeded record, and both must report the same verdict with the
    /// same terminal/transient classification. Each path claims its own key and hashes its own way.
    #[test]
    fn both_idempotency_guards_report_one_verdict_per_outcome() {
        const KEY: &str = "acct-1-3";

        struct Case {
            name: &'static str,
            /// The record the winner left behind under the claimed key.
            prior: fn(u64) -> Record<()>,
            /// The error a claimant of the given key gets.
            want: fn(&str) -> Error,
            want_terminal: bool,
        }
        let cases = [
            Case {
                name: "in_flight_claim",
                prior: Record::pending,
                want: |key| Error::new(ErrorCode::InProgress, key),
                want_terminal: false,
            },
            Case {
                name: "cached_terminal_failure",
                prior: |h| Record::failed(h, ErrorCode::Codec, "unknown field `amount`"),
                want: |_| Error::new(ErrorCode::CachedFailure, "codec: unknown field `amount`"),
                want_terminal: true,
            },
            Case {
                // The winner recorded a different payload under this key: flipping a bit of the hash
                // is exactly what a second, different command (or event body) would produce.
                name: "key_reused_with_another_payload",
                prior: |h| Record::done(h ^ 1, ()),
                want: |key| Error::new(ErrorCode::PayloadMismatch, key),
                want_terminal: true,
            },
        ];

        // Leave `key` holding `prior` the way a winner would: claim it, then complete a settled record
        // with the handle. A pending prior is the claim itself, which no store takes as a record.
        fn seed(store: &dyn IdempotencyStore<(), Tx = MemoryTx>, key: &str, prior: Record<()>) {
            let Ok(Claim::Won(handle)) = store.claim(&mut MemoryTx::auto_commit(), key, prior.payload_hash) else {
                panic!("a fresh store's key is free")
            };
            if prior.state != edax_cqrs::State::Pending {
                store
                    .complete(&mut MemoryTx::auto_commit(), key, &handle, prior)
                    .unwrap();
            }
        }

        let command_key = claimed(KEY);
        let event_key =
            edax_event::event_idempotency(&crate::testkit::event("acct-1", "Deposited", 3), Tenancy::Silo, SCOPE)
                .unwrap()
                .key;
        let command_hash = command_hash("Deposit", &deposit()).unwrap();
        let event_hash = payload_hash(DEPOSIT_BODY);

        for c in cases {
            // --- command path ---
            let command_store: Arc<dyn IdempotencyStore<(), Tx = MemoryTx> + Send + Sync> =
                Arc::new(MemoryIdempotencyStore::<()>::new());
            seed(&*command_store, &command_key, (c.prior)(command_hash));
            let mut bus = CommandBus::new();
            let guard = idempotent(Arc::clone(&command_store), SCOPE, Tenancy::Silo);
            bus.register(Box::new(|_: &Deposit| Ok(())), vec![guard]).unwrap();
            let command_err = bus.dispatch(&deposit(), DispatchOptions::keyed(KEY)).expect_err(c.name);

            // --- consume path ---
            let event_store: Arc<dyn IdempotencyStore<(), Tx = MemoryTx> + Send + Sync> =
                Arc::new(MemoryIdempotencyStore::<()>::new());
            seed(&*event_store, &event_key, (c.prior)(event_hash));
            let mut router = crate::EventRouter::new(vec![crate::idempotent_by_id(
                Arc::clone(&event_store),
                SCOPE,
                Tenancy::Silo,
            )]);
            router
                .register("Deposited", Box::new(|_: &EventMessage| Ok(())))
                .unwrap();
            let mut env = crate::testkit::event("acct-1", "Deposited", 3);
            env.data = DEPOSIT_BODY.to_vec();
            let event_err = router.dispatch(&env).expect_err(c.name);

            assert_eq!(
                command_err.downcast_ref::<Error>(),
                Some(&(c.want)(&command_key)),
                "{}: command path",
                c.name
            );
            assert_eq!(
                event_err.downcast_ref::<Error>(),
                Some(&(c.want)(&event_key)),
                "{}: consume path",
                c.name
            );
            assert_eq!(is_terminal(&*command_err), c.want_terminal, "{}: command class", c.name);
            assert_eq!(is_terminal(&*event_err), c.want_terminal, "{}: consume class", c.name);
        }
    }

    #[test]
    fn retry_branches() {
        struct Case {
            name: &'static str,
            attempts: u32,
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
        ];
        for c in cases {
            let calls = Arc::new(Counter::default());
            let calls_h = Arc::clone(&calls);
            let (failures, failure) = (c.failures, c.failure);
            let (pacing, slept) = Pacing::recording();
            let bus = pay_bus(
                vec![retry(attempts(c.attempts), pacing)],
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
            let got = bus.dispatch(&pay("x", 7), DispatchOptions::default());
            assert_eq!(calls.get(), c.want_calls, "{}: handler calls", c.name);
            match (c.want, got) {
                (Ok(want), Ok(got)) => assert_eq!(got, want, "{}", c.name),
                (Err(want), Err(e)) => assert_eq!(e.to_string(), want, "{}", c.name),
                (want, got) => panic!("{}: want {want:?}, got {:?}", c.name, got.map_err(|e| e.to_string())),
            }
            let want_waits: Vec<Duration> = c.want_waits.iter().map(|&a| kernel_wait(a)).collect();
            assert_eq!(
                *slept.lock().unwrap(),
                want_waits,
                "{}: the kernel's jittered schedule",
                c.name
            );
        }
    }

    #[test]
    fn idempotent_pre_claim_branches() {
        // A command serde cannot write as JSON has no message, so every dispatch of it is the terminal
        // `codec` error before the chain runs, keyed or not: no claim, no handler run. Python's
        // `command_message` refuses it the same way.
        struct NotJson;
        impl Serialize for NotJson {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("card number must not leave the process"))
            }
        }
        impl Command for NotJson {
            const TYPE: &'static str = "Pay";
            type Output = i64;
        }
        struct Case {
            name: &'static str,
            options: fn() -> DispatchOptions,
        }
        let cases = [
            Case {
                name: "unkeyed: the message cannot be built",
                options: DispatchOptions::default,
            },
            Case {
                name: "keyed: refused before the guard claims",
                options: || DispatchOptions::keyed("k1"),
            },
        ];
        let want = "not JSON-encodable: card number must not leave the process";
        for c in cases {
            let store = Arc::new(FaultStore::default());
            let calls = Arc::new(Counter::default());
            let calls_h = Arc::clone(&calls);
            let guard = idempotent(
                Arc::clone(&store) as Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync>,
                SCOPE,
                Tenancy::Silo,
            );
            let handler: Handler<NotJson> = Box::new(move |_: &NotJson| {
                calls_h.set(calls_h.get() + 1);
                Ok(1)
            });
            let mut bus = CommandBus::new();
            bus.register(handler, vec![guard]).unwrap();
            for _ in 0..2 {
                let e = bus.dispatch(&NotJson, (c.options)()).expect_err(c.name);
                // The code matters as much as the text: a serialization failure is a marshaling
                // failure, and `is_terminal` reads the code to stop retrying.
                assert!(
                    matches!(e.downcast_ref::<Error>(), Some(err) if err.code == ErrorCode::Codec && err.message.contains(want)),
                    "{}: got {e}",
                    c.name
                );
                assert!(
                    is_terminal(&*e),
                    "{}: a command that will not serialize is terminal",
                    c.name
                );
            }
            assert_eq!(calls.get(), 0, "{}: the handler never runs", c.name);
            assert_eq!(store.claims.get(), 0, "{}: no claim is taken", c.name);
        }
    }

    #[test]
    fn idempotent_keys_the_claim_by_the_guards_tenancy() {
        // Two tenants dispatch one request id with one payload over one shared store. The guard's
        // tenancy decides whether they claim one row or two, and refuses a command with no tenant
        // where the mode requires one, before any claim.
        // The tenant travels on the message, beside the command: never part of the hash.
        #[derive(Serialize)]
        struct Open {
            owner: &'static str,
        }
        impl Command for Open {
            const TYPE: &'static str = "Open";
            type Output = i64;
        }
        struct Case {
            name: &'static str,
            tenancy: Tenancy,
            tenants: [Option<&'static str>; 2],
            want_runs: usize,
            /// The row the first dispatch claimed, or `None` when it was refused.
            want_key: Option<&'static str>,
        }
        let cases = [
            Case {
                name: "a pool claims one row per tenant",
                tenancy: Tenancy::Pool,
                tenants: [Some("acme"), Some("globex")],
                want_runs: 2,
                want_key: Some("4:acme:edax:cmd:8:payments:req-1"),
            },
            Case {
                name: "a silo never keys by the tenant",
                tenancy: Tenancy::Silo,
                tenants: [Some("acme"), Some("globex")],
                want_runs: 1,
                want_key: Some("edax:cmd:8:payments:req-1"),
            },
            Case {
                name: "a bridge keeps the key bare",
                tenancy: Tenancy::Bridge,
                tenants: [Some("acme"), Some("globex")],
                want_runs: 1,
                want_key: Some("edax:cmd:8:payments:req-1"),
            },
            Case {
                name: "a pool refuses a command without a tenant before the claim",
                tenancy: Tenancy::Pool,
                tenants: [None, None],
                want_runs: 0,
                want_key: None,
            },
            Case {
                name: "a bridge refuses a command without a tenant before the claim",
                tenancy: Tenancy::Bridge,
                tenants: [None, None],
                want_runs: 0,
                want_key: None,
            },
        ];
        for c in cases {
            let store = Arc::new(FaultStore::default());
            let calls = Arc::new(Counter::default());
            let calls_h = Arc::clone(&calls);
            let guard = idempotent(
                Arc::clone(&store) as Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync>,
                SCOPE,
                c.tenancy,
            );
            let handler: Handler<Open> = Box::new(move |_: &Open| {
                calls_h.set(calls_h.get() + 1);
                Ok(7)
            });
            let mut bus = CommandBus::new();
            bus.register(handler, vec![guard]).unwrap();
            for tenant in c.tenants {
                let options = DispatchOptions {
                    tenant: tenant.map(ToOwned::to_owned),
                    ..DispatchOptions::keyed("req-1")
                };
                let got = bus.dispatch(&Open { owner: "ada" }, options);
                match c.want_key {
                    Some(_) => assert_eq!(got.ok(), Some(7), "{}: the same request, the same answer", c.name),
                    None => assert!(
                        matches!(got, Err(e) if e.downcast_ref::<Error>().is_some_and(|e| e.code == ErrorCode::Codec)),
                        "{}",
                        c.name
                    ),
                }
            }
            assert_eq!(calls.get(), c.want_runs, "{}: handler runs", c.name);
            match c.want_key {
                Some(key) => assert!(
                    matches!(
                        store.inner.claim(&mut MemoryTx::auto_commit(), key, 0),
                        Ok(Claim::Prior(_))
                    ),
                    "{}: the claimed row",
                    c.name
                ),
                None => assert_eq!(store.claims.get(), 0, "{}: no claim is taken", c.name),
            }
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
                want: [
                    Some(Error::new(ErrorCode::InProgress, claimed("k1"))),
                    Some(Error::new(ErrorCode::InProgress, claimed("k1"))),
                ],
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
                want: [
                    Some(Error::codec("bad payload")),
                    Some(Error::new(ErrorCode::CachedFailure, "codec: bad payload")),
                ],
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
                want: [
                    Some(blip_error()),
                    Some(Error::new(ErrorCode::CachedFailure, "rejected: insufficient funds")),
                ],
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
                want: [None, Some(Error::new(ErrorCode::PayloadMismatch, claimed("k1")))],
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
            let guard = idempotent(
                Arc::clone(&store) as Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync>,
                SCOPE,
                Tenancy::Silo,
            );
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
                let hash = command_hash("Pay", &pay("k1", 10)).unwrap();
                assert!(
                    matches!(
                        store.claim(&mut MemoryTx::auto_commit(), &claimed("k1"), hash),
                        Ok(Claim::Won(_))
                    ),
                    "{}",
                    c.name
                );
                store.claims.set(0);
            }

            for (amount, want) in [10, c.redispatched].into_iter().zip(c.want) {
                match (want, bus.dispatch(&pay("k1", amount), DispatchOptions::keyed("k1"))) {
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
                match store
                    .inner
                    .claim(&mut MemoryTx::auto_commit(), &claimed("k1"), 0)
                    .unwrap()
                {
                    Claim::Prior(Record {
                        state: State::Failed { code: cached, .. },
                        ..
                    }) => {
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
            vec![idempotent(Arc::clone(&store), SCOPE, Tenancy::Silo)],
            Box::new(move |c: &Pay| {
                calls_h.set(calls_h.get() + 1);
                if calls_h.get() == 1 {
                    panic!("handler bug");
                }
                Ok(c.amount)
            }),
        );

        let dispatched = catch_unwind(AssertUnwindSafe(|| bus.dispatch(&pay("k1", 10), by_id(&pay("k1", 10)))));
        assert!(dispatched.is_err(), "the panic reaches the caller");
        assert_eq!(releases.get(), 1, "the claim is released, not stranded");
        assert_eq!(
            bus.dispatch(&pay("k1", 10), by_id(&pay("k1", 10))).unwrap(),
            10,
            "the next dispatch re-runs the handler"
        );
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn custom_middleware_wraps_in_order() {
        // An arbitrary user middleware (not retry/idempotent), wrapped first-outermost and run.
        let trace: Arc<Mutex<Vec<&'static str>>> = Arc::default();
        let tracing = |name: &'static str| -> Middleware<Pay> {
            let trace = Arc::clone(&trace);
            Box::new(move |next: Next<Pay>| {
                let trace = Arc::clone(&trace);
                Box::new(move |msg: &CommandMessage, cmd: &Pay| {
                    trace.lock().unwrap().push(name);
                    next(msg, cmd)
                })
            })
        };
        let bus = pay_bus(
            vec![tracing("outer"), tracing("inner")],
            Box::new(|c: &Pay| Ok(c.amount)),
        );
        assert_eq!(bus.dispatch(&pay("a", 3), by_id(&pay("a", 3))).unwrap(), 3);
        assert_eq!(bus.dispatch(&pay("b", 4), by_id(&pay("b", 4))).unwrap(), 4);
        assert_eq!(*trace.lock().unwrap(), ["outer", "inner", "outer", "inner"]);
    }

    #[test]
    fn replayed_cached_failure_is_terminal_and_stops_retrying() {
        let store: Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync> = Arc::new(MemStore::default());
        let calls = Arc::new(Counter::default());
        let calls_h = Arc::clone(&calls);
        let bus = pay_bus(
            vec![
                idempotent(Arc::clone(&store), SCOPE, Tenancy::Silo),
                retry(attempts(3), Pacing::instant()),
            ],
            Box::new(move |_c: &Pay| {
                calls_h.set(calls_h.get() + 1);
                Err(Error::codec("bad payload").into())
            }),
        );

        let cmd = pay("k1", 10);

        // Terminal handler failure: retry returns at once (1 call, not 3) and the outcome is cached.
        assert!(bus.dispatch(&cmd, by_id(&cmd)).is_err());
        assert_eq!(calls.get(), 1);

        // Redelivery replays it as CachedFailure, itself terminal — so retry stops here instead of
        // burning attempts, and the broker stops redelivering a deterministically failing command.
        let replay = bus.dispatch(&cmd, by_id(&cmd)).expect_err("cached failure");
        assert_eq!(
            replay.downcast_ref::<Error>(),
            Some(&Error::new(ErrorCode::CachedFailure, "codec: bad payload"))
        );
        assert_eq!(calls.get(), 1); // handler never re-ran
    }

    #[test]
    fn an_unreadable_record_replays_as_a_cached_codec_failure_and_never_runs_the_handler() {
        // A torn write holds the key. The guard replays the kernel's codec failure, terminal, so retry
        // stops at once, every redelivery replays it again, and the handler never runs for the key.
        let store = Arc::new(MemStore::default());
        store
            .put_raw(&claimed("k1"), br#"{"payload_hash":7,"result":{"#)
            .unwrap();
        let calls = Arc::new(Counter::default());
        let calls_h = Arc::clone(&calls);
        let bus = pay_bus(
            vec![
                idempotent(Arc::clone(&store), SCOPE, Tenancy::Silo),
                retry(attempts(3), Pacing::instant()),
            ],
            Box::new(move |c: &Pay| {
                calls_h.set(calls_h.get() + 1);
                Ok(c.amount)
            }),
        );
        let want = Error::new(
            ErrorCode::CachedFailure,
            "codec: unreadable idempotency record: EOF while parsing an object at line 1 column 28",
        );
        for attempt in ["first delivery", "redelivery"] {
            let replay = bus.dispatch(&pay("k1", 10), by_id(&pay("k1", 10))).expect_err(attempt);
            assert_eq!(replay.downcast_ref::<Error>(), Some(&want), "{attempt}");
        }
        assert_eq!(calls.get(), 0, "the handler never ran");
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
        fn complete(
            &self,
            tx: &mut MemoryTx,
            key: &str,
            handle: &ClaimHandle,
            record: Record<i64>,
        ) -> Result<(), Error> {
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
        fn forget(&self, tx: &mut MemoryTx, key: &str) -> Result<(), Error> {
            self.inner.forget(tx, key)
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
        let (pacing, slept) = Pacing::recording();
        let bus = pay_bus(
            vec![
                idempotent(Arc::clone(&store), SCOPE, Tenancy::Silo),
                retry(attempts(2), pacing),
            ],
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
        assert_eq!(bus.dispatch(&pay("k1", 5), by_id(&pay("k1", 5))).unwrap(), 5);
        assert_eq!(calls.get(), 2);
        assert_eq!(claims.get(), 1, "both attempts ran under one claim");
        assert_eq!(
            releases.get(),
            0,
            "the claim was never released — the retry succeeded under it"
        );
        assert_eq!(*slept.lock().unwrap(), [kernel_wait(0)]);

        // The claim completed, so a redelivery replays the cached result without another attempt.
        assert_eq!(bus.dispatch(&pay("k1", 5), by_id(&pay("k1", 5))).unwrap(), 5);
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
        let store: Arc<dyn IdempotencyStore<i64, Tx = MemoryTx> + Send + Sync> = Arc::new(FaultStore {
            releases: Arc::clone(&releases),
            claim_fails: true,
            ..Default::default()
        });

        let calls_h = Arc::clone(&calls);
        let bus = pay_bus(
            vec![idempotent(store, SCOPE, Tenancy::Silo)],
            Box::new(move |c: &Pay| {
                calls_h.set(calls_h.get() + 1);
                Ok(c.amount)
            }),
        );

        let err = bus
            .dispatch(&pay("k1", 5), by_id(&pay("k1", 5)))
            .expect_err("the claim failed");
        // The store's own verdict, not a `CachedFailure` standing in for it.
        assert_eq!(
            err.downcast_ref::<Error>(),
            Some(&Error::store("could not reach the ledger"))
        );
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
                vec![idempotent(store, SCOPE, Tenancy::Silo)],
                Box::new(move |_c: &Pay| {
                    calls_h.set(calls_h.get() + 1);
                    Err(Box::new(raised_h.clone()) as HandlerError)
                }),
            );

            let err = bus
                .dispatch(&pay("k1", 5), by_id(&pay("k1", 5)))
                .expect_err("the handler failed");
            // The handler's own error, with its own verdict — not the store's refusal.
            assert_eq!(err.downcast_ref::<Error>(), Some(&raised));
            assert_eq!(
                is_terminal(&*err),
                terminal,
                "the handler's verdict survived the refusal"
            );
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
        fn complete(
            &self,
            _tx: &mut MemoryTx,
            key: &str,
            handle: &ClaimHandle,
            record: Record<i64>,
        ) -> Result<(), Error> {
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
        fn forget(&self, _tx: &mut MemoryTx, key: &str) -> Result<(), Error> {
            let mut state = self.state.lock().unwrap();
            if state.holder.get(key) == Some(&None) {
                state.holder.remove(key);
                state.settled.remove(key);
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
            (
                "transient_failure_releases",
                || Err(Error::store("timeout").into()),
                vec![("release", first.clone())],
            ),
            (
                "terminal_failure_caches",
                || Err(Error::codec("bad").into()),
                vec![("complete", first.clone())],
            ),
            (
                "panic_releases",
                || panic!("handler bug"),
                vec![("release", first.clone())],
            ),
        ];
        for (name, run, want) in cases {
            let store = FencedStore::default();
            let _ = catch_unwind(AssertUnwindSafe(|| {
                guarded(&store, &mut MemoryTx::auto_commit(), "k", 7, run)
            }));
            assert_eq!(store.calls(), want, "{name}");
        }
    }

    /// Through the guard on one store: the claim lapses mid-handler and a second dispatch on
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
        assert_eq!(
            err.downcast_ref::<Error>(),
            Some(&Error::store("the claim was taken over"))
        );
        let taker = taker.into_inner().unwrap();
        assert!(
            store.holds("k", &taker),
            "the stale release left the new holder's claim alone"
        );
        let third = store.claim(&mut MemoryTx::auto_commit(), "k", 7);
        assert!(
            matches!(third, Ok(Claim::Prior(r)) if r.state == State::Pending),
            "a third claim loses to the live one"
        );
        let stale = ClaimHandle(b"claim-1".to_vec());
        assert_eq!(store.calls(), [("complete", stale.clone()), ("release", stale)]);
        // The live claim still settles.
        store
            .complete(&mut MemoryTx::auto_commit(), "k", &taker, Record::done(7, 2))
            .unwrap();
        assert!(
            matches!(store.claim(&mut MemoryTx::auto_commit(), "k", 7), Ok(Claim::Prior(r)) if r.state == State::Done(2))
        );
    }

    /// Dispatch `cmd` twice under one key through a guard over a memory ledger, its handler returning
    /// `result`, and check the first dispatch fails with `codec` and the second replays that cached
    /// failure without running the handler.
    fn assert_cached_as_a_codec_failure<C>(name: &str, cmd: &C, result: C::Output)
    where
        C: Command + 'static,
        C::Output: Clone + std::fmt::Debug + Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        let runs = Arc::new(Counter::default());
        let counted = Arc::clone(&runs);
        let handler: Handler<C> = Box::new(move |_: &C| {
            counted.0.fetch_add(1, Ordering::SeqCst);
            Ok(result.clone())
        });
        let mut bus = CommandBus::new();
        let guard = idempotent(MemoryIdempotencyStore::<C::Output>::new(), SCOPE, Tenancy::Silo);
        bus.register(handler, vec![guard]).unwrap();

        let first = bus.dispatch(cmd, DispatchOptions::keyed("req-1")).expect_err(name);
        let first = first.downcast_ref::<Error>().expect("a kernel error");
        assert_eq!(first.code, ErrorCode::Codec, "{name}: got {first}");

        let second = bus
            .dispatch(cmd, DispatchOptions::keyed("req-1"))
            .expect_err("the redelivery replays the failure");
        let second = second.downcast_ref::<Error>().expect("a kernel error");
        assert_eq!(
            second,
            &cached_failure(Some(ErrorCode::Codec), &first.message),
            "{name}"
        );
        assert_eq!(runs.get(), 1, "{name}: the handler ran once");
    }

    /// A result with no JSON form (a map keyed by bytes), or one holding an integer past 64 bits, fails the
    /// same way on every run, so the guard caches it as the terminal `codec` failure and a redelivery
    /// replays it. Releasing it instead would let the redelivery run the handler, and its side effects, a
    /// second time; storing the wide integer would let another SDK replay it as the nearest double. The
    /// Python twins are `test_unit_of_work.py`'s
    /// `split_unencodable_result_rolls_back_and_is_cached_as_a_codec_failure` and `test_command.py`'s
    /// `test_cached_result_integers_are_exact_only_within_64_bits`.
    #[test]
    fn a_result_no_record_holds_is_cached_as_a_codec_failure_and_never_reruns() {
        /// `Pay`, answered with a balance keyed by account bytes.
        #[derive(Serialize)]
        #[serde(transparent)]
        struct Balances(Pay);
        impl Command for Balances {
            const TYPE: &'static str = "Pay";
            type Output = HashMap<Vec<u8>, u8>;
        }
        /// `Pay`, answered with a total wider than 64 bits.
        #[derive(Serialize)]
        #[serde(transparent)]
        struct Total(Pay);
        impl Command for Total {
            const TYPE: &'static str = "Pay";
            type Output = u128;
        }
        assert_cached_as_a_codec_failure(
            "a map keyed by bytes",
            &Balances(pay("req-1", 100)),
            HashMap::from([(b"acct-1".to_vec(), 1)]),
        );
        assert_cached_as_a_codec_failure("an integer past 64 bits", &Total(pay("req-1", 100)), 1u128 << 64);
    }

    /// `testdata/golden/command_received_key.json` through the Rust bus: a received message is claimed
    /// under its idempotency key, or its id when it has none, so the same message delivered twice runs
    /// its handler once, and the chain sees the message as received, its key unchanged. The Python suite
    /// replays the same rows through its own bus (`test_cross_sdk.py`).
    #[test]
    fn a_received_message_is_claimed_under_the_kernels_received_key() {
        for case in edax_golden::read("command_received_key.json") {
            let expected = case["expected"].as_str().expect("expected is a key");
            let mut msg = command_message(&pay("x", 7), &pay_codec(), DispatchOptions::default()).unwrap();
            msg.id = case["id"].as_str().expect("an id").to_owned();
            msg.idempotency_key = case["idempotency_key"].as_str().map(ToOwned::to_owned);
            let store = Arc::new(MemStore::default());
            let runs = Arc::new(Counter::default());
            let counted = Arc::clone(&runs);
            let seen: Arc<Mutex<Vec<Option<String>>>> = Arc::default();
            let record = Arc::clone(&seen);
            let recording: Middleware<Pay> = Box::new(move |next: Next<Pay>| {
                let record = Arc::clone(&record);
                Box::new(move |msg: &CommandMessage, cmd: &Pay| {
                    record.lock().unwrap().push(msg.idempotency_key.clone());
                    next(msg, cmd)
                })
            });
            let bus = pay_bus(
                vec![recording, idempotent(Arc::clone(&store), SCOPE, Tenancy::Silo)],
                Box::new(move |c: &Pay| {
                    counted.set(counted.get() + 1);
                    Ok(c.amount)
                }),
            );
            for delivery in ["first delivery", "redelivery"] {
                assert_eq!(
                    bus.dispatch_message::<Pay>(&msg).unwrap(),
                    7,
                    "{expected:?}: {delivery}"
                );
            }
            assert_eq!(runs.get(), 1, "{expected:?}: the redelivery replays");
            assert_eq!(
                *seen.lock().unwrap(),
                vec![msg.idempotency_key.clone(); 2],
                "{expected:?}: the key as received"
            );
            let claimed = claim_key(Tenancy::Silo, None, SCOPE, expected).unwrap();
            assert!(
                matches!(
                    store.claim(&mut MemoryTx::auto_commit(), &claimed, 0),
                    Ok(Claim::Prior(_))
                ),
                "{expected:?}: the claimed row"
            );
        }
    }

    /// The message a sender builds carries its codec's name, version and data, so a consumer reading with
    /// the same versioned codec runs it as sent: stamped at version 1, the consumer would raise data that
    /// is already at version 2 and refuse it. Python's `command_message(cmd, codec=...)` is the twin.
    #[test]
    fn a_message_carries_its_codecs_version_both_ways() {
        // Version 2 of `Pay` renamed `cents` to `amount`: the step refuses a payload without `cents`.
        let v2 = || {
            let rename = |data: Vec<u8>| {
                let mut body: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_slice(&data).map_err(Error::codec)?;
                let cents = body
                    .remove("cents")
                    .ok_or_else(|| Error::codec("a v1 Pay carries cents"))?;
                body.insert("amount".to_owned(), cents);
                serde_json::to_vec(&body).map_err(Error::codec)
            };
            JsonCodec::<Pay>::new(Pay::TYPE)
                .with_upcaster(crate::Upcaster::new().step(1, rename).unwrap())
                .unwrap()
        };
        let seen: Arc<Mutex<Vec<u32>>> = Arc::default();
        let record = Arc::clone(&seen);
        let recording: Middleware<Pay> = Box::new(move |next: Next<Pay>| {
            let record = Arc::clone(&record);
            Box::new(move |msg: &CommandMessage, cmd: &Pay| {
                record.lock().unwrap().push(msg.version);
                next(msg, cmd)
            })
        });
        let mut bus = CommandBus::new();
        bus.register_with_codec(Box::new(|c: &Pay| Ok(c.amount)), v2(), vec![recording])
            .unwrap();

        let sent = command_message(&pay("p-1", 500), &v2(), DispatchOptions::default()).unwrap();
        assert_eq!(
            (sent.version, sent.data.as_slice()),
            (2, br#"{"id":"p-1","amount":500}"#.as_slice()),
            "the codec's version and data"
        );
        assert_eq!(
            bus.dispatch_message::<Pay>(&sent).unwrap(),
            500,
            "the consumer runs it without an upcast"
        );
        assert_eq!(bus.dispatch(&pay("p-2", 700), DispatchOptions::default()).unwrap(), 700);
        assert_eq!(
            *seen.lock().unwrap(),
            [2, 2],
            "a dispatch hands its middleware the codec's message too"
        );

        let renamed = command_message(
            &pay("p-1", 500),
            &JsonCodec::<Pay>::new("Payment"),
            DispatchOptions::default(),
        );
        assert_eq!(
            renamed.map_err(|e| e.to_string()),
            Err(r#"codec: the codec names a "Pay" command "Payment"; a message name must be its type"#.to_owned()),
            "a dispatch routes by the type and a received message by its name"
        );

        // Registered for in-process dispatch only: the message is the command's JSON at version 1, and a
        // received one is a wiring mistake the dispatcher stops on.
        let mut local = CommandBus::new();
        local.register(Box::new(|c: &Pay| Ok(c.amount)), vec![]).unwrap();
        assert_eq!(local.dispatch(&pay("p-3", 9), DispatchOptions::default()).unwrap(), 9);
        let refused = local.dispatch_message::<Pay>(&sent).expect_err("no codec reads it");
        assert_eq!(
            refused.downcast_ref::<Error>().map(|e| e.code),
            Some(ErrorCode::BadHandle)
        );
    }

    #[test]
    fn dispatch_message_branches() {
        // Each refusal is terminal and runs nothing: the same message fails the same way on redelivery.
        let bus = pay_bus(vec![], Box::new(|c: &Pay| Ok(c.amount)));
        let message = |name: &str, data: &[u8]| CommandMessage {
            name: name.to_owned(),
            data: data.to_vec(),
            ..command_message(&pay("x", 7), &pay_codec(), DispatchOptions::default()).unwrap()
        };
        let code = |got: Result<i64, HandlerError>| got.map_err(|e| e.downcast_ref::<Error>().map(|e| e.code));
        let cases = [
            (
                "the message its codec reads runs",
                message("Pay", br#"{"id":"x","amount":7}"#),
                Ok(7),
            ),
            (
                "a message of another command type is not this one's",
                message("Refund", br#"{"id":"x","amount":7}"#),
                Err(Some(ErrorCode::HandlerNotFound)),
            ),
            (
                "a payload the codec cannot read",
                message("Pay", br#"{"amount":"seven"}"#),
                Err(Some(ErrorCode::Codec)),
            ),
        ];
        for (name, msg, want) in cases {
            assert_eq!(code(bus.dispatch_message::<Pay>(&msg)), want, "{name}");
        }
    }

    /// A command consumer: received command messages fed through the key-ordered dispatcher into the
    /// bus, as Python's dispatcher carries commands too. One aggregate's commands run in order on one
    /// worker, a broker's redelivery replays the guard's record and is acked, and a terminal failure
    /// is dead-lettered with the command message itself.
    #[test]
    fn the_key_ordered_dispatcher_carries_commands_to_the_bus() {
        let ran: Arc<Mutex<Vec<(String, i64)>>> = Arc::default();
        let log = Arc::clone(&ran);
        let bus = pay_bus(
            vec![idempotent(Arc::new(MemStore::default()), SCOPE, Tenancy::Silo)],
            Box::new(move |c: &Pay| {
                if c.amount < 0 {
                    return Err(Terminal::new("a payment is never negative").into());
                }
                log.lock().unwrap().push((c.id.clone(), c.amount));
                Ok(c.amount)
            }),
        );
        let dead: Arc<Mutex<Vec<String>>> = Arc::default();
        let sunk = Arc::clone(&dead);
        let dispatcher = crate::KeyOrderedDispatcher::builder(2, move |msg: &CommandMessage| {
            bus.dispatch_message::<Pay>(msg).map(drop)
        })
        .on_dead(move |letter: crate::DeadLetter<CommandMessage>| {
            sunk.lock().unwrap().push(letter.message.name);
            Ok(())
        })
        .build()
        .expect("the dispatcher's workers spawn");
        let message = |id: &str, amount: i64| {
            let options = DispatchOptions {
                aggregate_id: Some("acct-1".to_owned()),
                ..DispatchOptions::default()
            };
            command_message(&pay(id, amount), &pay_codec(), options).unwrap()
        };
        let first = message("p-1", 10);
        let settled: Arc<Mutex<Vec<String>>> = Arc::default();
        for msg in [
            first.clone(),
            message("p-2", 20),
            first,
            message("p-3", -5),
            message("p-4", 40),
        ] {
            let (acked, nacked) = (Arc::clone(&settled), Arc::clone(&settled));
            let id = msg.id.clone();
            let (ack_id, nack_id) = (format!("{id}:ack"), format!("{id}:nack"));
            let delivery = crate::Delivery::new(
                msg,
                move || acked.lock().unwrap().push(ack_id),
                move || nacked.lock().unwrap().push(nack_id),
            );
            dispatcher.submit(delivery).expect("the dispatcher is running");
        }
        dispatcher.stop();

        let ran = ran.lock().unwrap().clone();
        assert_eq!(
            ran,
            [("p-1".to_owned(), 10), ("p-2".to_owned(), 20), ("p-4".to_owned(), 40)],
            "in order, the redelivery replayed"
        );
        assert!(
            settled.lock().unwrap().iter().all(|s| s.ends_with(":ack")),
            "every delivery acked: {:?}",
            settled.lock().unwrap()
        );
        assert_eq!(
            *dead.lock().unwrap(),
            [Pay::TYPE],
            "the rejected command was dead-lettered"
        );
    }
}

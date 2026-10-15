//! Read-side event router. [`EventRouter`] dispatches an event to the handler registered for its name,
//! through a middleware chain, and plugs into a [`crate::dispatcher::KeyOrderedDispatcher`].
//! [`idempotent_by_id`] claims the event id under the consumer's scope, so a handler's side effect is
//! effectively-once.

use std::cell::RefCell;
use std::sync::Arc;

use edax_event::{event_idempotency, EventMessage};

use crate::chain::Chain;
use crate::failure::HandlerError;
use crate::idempotency::{Guard, IdempotencyStore};
use crate::sealing::Sealing;
use crate::{Error, Tenancy};

/// Handles one committed event. `Send + Sync` so a router can sit behind a worker pool. It fails with
/// the same [`HandlerError`] a command handler does, so a saga that dispatches a command returns the
/// command's failure with `?` and its verdict survives to the dispatcher.
pub type EventHandler = Box<dyn Fn(&EventMessage) -> Result<(), HandlerError> + Send + Sync>;
pub type EventMiddleware = Box<dyn Fn(EventHandler) -> EventHandler + Send + Sync>;

/// Routes a committed event to the handler for its event name, through a middleware chain baked around each
/// handler at register time (first middleware is outermost); the unhandled fallback is wrapped too. Pass
/// `|event| router.deliver(event).map(|_| ())` to a [`crate::dispatcher::KeyOrderedDispatcher`] to keep
/// same-aggregate ordering and settle deliveries whose keys have been destroyed.
///
/// The middleware is given once, to [`new`](Self::new), so a handler can never be registered before a
/// middleware that should wrap it, such as [`idempotent_by_id`]. There is no way to add one later:
///
/// ```compile_fail
/// let mut router = edax::EventRouter::new(Vec::new());
/// router.register("Deposited", Box::new(|_| Ok(()))).unwrap();
/// let router = router.with(Box::new(|next| next));
/// ```
pub struct EventRouter {
    chain: Chain<EventHandler, EventMiddleware>,
    on_unhandled: Option<EventHandler>,
    sealing: Option<Arc<Sealing>>,
}

thread_local! {
    static OPENED_EVENT: RefCell<Option<(EventMessage, EventMessage)>> = const { RefCell::new(None) };
    static ROUTING_EVENT: RefCell<Option<(EventMessage, EventMessage)>> = const { RefCell::new(None) };
}

struct Routing(Option<(EventMessage, EventMessage)>);
impl Drop for Routing {
    fn drop(&mut self) {
        ROUTING_EVENT.replace(self.0.take());
    }
}

struct OpenedEvent(Option<(EventMessage, EventMessage)>);
impl Drop for OpenedEvent {
    fn drop(&mut self) {
        OPENED_EVENT.replace(self.0.take());
    }
}

pub(crate) fn original_event(view: &EventMessage) -> EventMessage {
    OPENED_EVENT.with_borrow(|pair| match pair {
        Some((raw, opened)) if opened == view => raw.clone(),
        _ => view.clone(),
    })
}

fn opened_handler(handler: EventHandler) -> EventHandler {
    Box::new(move |raw| {
        check_authenticated_event(raw)?;
        match ROUTING_EVENT.with_borrow(Clone::clone) {
            Some((original, opened)) => {
                let _view = OpenedEvent(OPENED_EVENT.replace(Some((original, opened.clone()))));
                handler(&opened)
            }
            None => handler(raw),
        }
    })
}

fn check_authenticated_event(event: &EventMessage) -> Result<(), Error> {
    if ROUTING_EVENT.with_borrow(|pair| pair.as_ref().is_some_and(|(raw, _)| raw != event)) {
        return Err(Error::codec("middleware changed the authenticated event envelope"));
    }
    Ok(())
}

impl Default for EventRouter {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl EventRouter {
    /// A router whose every handler, and the fallback, runs inside `middleware`, the first one outermost.
    pub fn new(middleware: Vec<EventMiddleware>) -> Self {
        Self {
            chain: Chain::new(middleware),
            on_unhandled: None,
            sealing: None,
        }
    }

    /// Authenticate protected deliveries before middleware and decode only the handler's view.
    pub fn sealing(mut self, sealing: Arc<Sealing>) -> Self {
        self.sealing = Some(sealing);
        self
    }

    /// Fallback for events with no registered handler, wrapped in the chain. Without one they are ignored.
    pub fn on_unhandled(&mut self, handler: EventHandler) {
        self.on_unhandled = Some(self.chain.wrap(opened_handler(handler)));
    }

    /// Register a handler for an event name, baking the middleware chain around it. The terminal
    /// [`ErrorCode::AlreadyRegistered`](crate::ErrorCode::AlreadyRegistered) if the name already has a
    /// handler.
    pub fn register(&mut self, name: impl Into<String>, handler: EventHandler) -> Result<(), Error> {
        self.chain.register(name.into(), opened_handler(handler))
    }

    /// Route `event` to the handler for `event.name`. An unregistered name is ignored unless a fallback is set.
    /// Confirmed destruction raises `Shredded`. Consumers use [`Self::deliver`] to settle destroyed rows.
    pub fn dispatch(&self, event: &EventMessage) -> Result<(), HandlerError> {
        self.dispatch_received(event, false).map(|_| ())
    }

    fn dispatch_received(&self, event: &EventMessage, delivery: bool) -> Result<bool, HandlerError> {
        let mut opened_pair = None;
        let _guard = if let Some(sealing) = &self.sealing {
            crate::command::check_protected_transaction()?;
            let data = if delivery {
                let Some(data) = sealing.open_event_delivery(event)? else {
                    return Ok(true);
                };
                data
            } else {
                sealing.open_event(event)?
            };
            let scope = if event.encryption.is_some() {
                sealing.scope(event.tenant.as_deref(), event.subject.as_deref())?
            } else {
                sealing.legacy_scope(event.tenant.as_deref(), event.subject.as_deref())?
            };
            let guard = match sealing.hold(&scope) {
                Err(error) if delivery && error.code == crate::ErrorCode::Shredded => return Ok(true),
                result => result?,
            };
            let mut opened = event.clone();
            opened.data = data;
            opened.encryption = None;
            opened_pair = Some((event.clone(), opened));
            Some(guard)
        } else {
            if event.encryption.is_some() {
                return Err(Error::bad_handle("encrypted events require router sealing").into());
            }
            None
        };
        let _routing = Routing(ROUTING_EVENT.replace(opened_pair));
        match self.chain.get(&event.name).or(self.on_unhandled.as_ref()) {
            Some(handler) => handler(event),
            None => Ok(()),
        }?;
        Ok(false)
    }

    /// Return true only when trusted opening or admission confirms destruction and skips delivery.
    /// Handler errors, including `Shredded`, remain errors.
    pub fn deliver(&self, event: &EventMessage) -> Result<bool, HandlerError> {
        self.dispatch_received(event, true)
    }
}

/// Run a handler at most once per event id, so a broker redelivery does not repeat its side effect. The key and
/// hash are the kernel's ([`edax_event::event_idempotency`]): `event.id` under `scope`, composed with the
/// event's tenant under [`Tenancy::Pool`], and FNV-1a over the raw `event.data` bytes; the `()` result is stored
/// as `null`. `scope` names this consumer, so two consumers guarding over one store each run their handler
/// once. Under a pool or a bridge an event that names no tenant is the terminal `codec` error, before any
/// claim. A redelivery of a completed event returns `Ok(())`. A claim another worker holds gives the
/// transient `in_progress` error, which the dispatcher nacks. A transient handler failure, or a panic,
/// releases the claim; a terminal one is cached with its code and replays as the terminal `cached_failure`.
/// Two events sharing an id with different bodies give the terminal `payload_mismatch`. The default handle
/// gives at-least-once; [`idempotent_by_id_in`] commits the claim with the writes. The store is taken by
/// value, as [`idempotent`](crate::idempotent) takes it; pass an `Arc` of one to share it. See
/// `docs/sdk/idempotency.md`.
pub fn idempotent_by_id<S>(store: S, scope: impl Into<String>, tenancy: Tenancy) -> EventMiddleware
where
    S: IdempotencyStore<()> + Send + Sync + 'static,
    S::Tx: Default + 'static,
{
    idempotent_by_id_in(store, scope, tenancy, |_| S::Tx::default())
}

/// [`idempotent_by_id`] over the host's transaction. `current` is called once per guarded event, with that event,
/// and returns the handle for the claim, the terminal record and any release. The handler enlists its own writes
/// in the same handle, so the claim commits with them and a rollback drops both. Outside a unit of work
/// `current` returns the auto-commit handle and the guarantee falls back to at-least-once. `current` runs on
/// worker threads, so it must be `Send + Sync`, and it takes the event: one slot every worker reads is a race.
pub fn idempotent_by_id_in<S>(
    store: S,
    scope: impl Into<String>,
    tenancy: Tenancy,
    current: impl Fn(&EventMessage) -> S::Tx + Send + Sync + 'static,
) -> EventMiddleware
where
    S: IdempotencyStore<()> + Send + Sync + 'static,
    S::Tx: 'static,
{
    let guard = Guard::new(store, scope, current);
    Box::new(move |next: EventHandler| {
        let guard = guard.clone();
        Box::new(move |event: &EventMessage| -> Result<(), HandlerError> {
            check_authenticated_event(event)?;
            let rule = event_idempotency(event, tenancy, guard.scope())?;
            guard.run(event, &rule.key, rule.payload_hash, || next(event))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatcher::{Delivery, KeyOrderedDispatcher};
    use crate::idempotency::{Claim, ClaimHandle, Record};
    use crate::testkit::event;
    use crate::ErrorCode;
    use crate::{MemoryIdempotencyStore, MemoryTransactor, MemoryTx, Transactor};
    use std::collections::HashMap;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex};

    const DEPOSITED: &[u8] =
        br#"{"account_id":"acct-1","amount_cents":4200,"currency":"USD","occurred_at":"2024-05-01T12:00:00Z"}"#;
    /// The consumer every guard here is, unless a case names another.
    const SCOPE: &str = "balances";
    /// The ledger key that consumer claims for [`deposited`] (`event_idempotency.json`).
    const KEY: &str = "edax:evt:8:balances:acct-1-3";

    /// One committed event; every case redelivers this exact event (or one sharing its id).
    fn deposited(data: &[u8]) -> EventMessage {
        let mut env = event("acct-1", "Deposited", 3);
        env.data = data.to_vec();
        env.metadata.insert("trace".into(), "abc".into());
        env
    }

    /// A middleware that records entering and leaving, so nesting order is observable.
    fn tracing(name: &'static str, trace: &Arc<Mutex<Vec<String>>>) -> EventMiddleware {
        let trace = Arc::clone(trace);
        Box::new(move |next: EventHandler| {
            let trace = Arc::clone(&trace);
            Box::new(move |env: &EventMessage| {
                trace.lock().unwrap().push(format!(">{name}"));
                next(env)?;
                trace.lock().unwrap().push(format!("<{name}"));
                Ok(())
            })
        })
    }

    #[test]
    fn duplicate_registration_is_rejected() {
        let mut router = EventRouter::default();
        router.register("Created", Box::new(|_| Ok(()))).unwrap();
        let err = router.register("Created", Box::new(|_| Ok(()))).unwrap_err();
        assert!(
            matches!(&err, Error { code: ErrorCode::AlreadyRegistered, message: n } if n == "Created"),
            "got {err}"
        );
    }

    #[test]
    fn dispatch_branches() {
        // Each branch of `dispatch`: a registered name, an unregistered name with a fallback, an
        // unregistered name without one, and a handler error propagating unchanged.
        struct Case {
            name: &'static str,
            event: &'static str,
            fallback: bool,
            want_named: u32,
            want_unhandled: u32,
            want_err: Option<&'static str>,
        }
        let cases = [
            Case {
                name: "registered name routes to its handler",
                event: "Created",
                fallback: true,
                want_named: 1,
                want_unhandled: 0,
                want_err: None,
            },
            Case {
                name: "unregistered name routes to the fallback",
                event: "Deleted",
                fallback: true,
                want_named: 0,
                want_unhandled: 1,
                want_err: None,
            },
            Case {
                name: "unregistered name without a fallback is ignored",
                event: "Deleted",
                fallback: false,
                want_named: 0,
                want_unhandled: 0,
                want_err: None,
            },
            Case {
                name: "handler error propagates",
                event: "Broken",
                fallback: true,
                want_named: 0,
                want_unhandled: 0,
                want_err: Some("projection down"),
            },
        ];
        for c in cases {
            let named = Arc::new(Mutex::new(0u32));
            let unhandled = Arc::new(Mutex::new(0u32));
            let mut router = EventRouter::default();
            let n = Arc::clone(&named);
            router
                .register(
                    "Created",
                    Box::new(move |_: &EventMessage| {
                        *n.lock().unwrap() += 1;
                        Ok(())
                    }),
                )
                .unwrap();
            router
                .register(
                    "Broken",
                    Box::new(|_: &EventMessage| Err(Error::store("projection down").into())),
                )
                .unwrap();
            if c.fallback {
                let u = Arc::clone(&unhandled);
                router.on_unhandled(Box::new(move |_: &EventMessage| {
                    *u.lock().unwrap() += 1;
                    Ok(())
                }));
            }

            let got = router.dispatch(&event("a", c.event, 1));
            match (c.want_err, got) {
                (None, Ok(())) => {}
                (Some(want), Err(e)) => assert_eq!(e.downcast_ref::<Error>(), Some(&Error::store(want)), "{}", c.name),
                (want, got) => panic!("{}: want {want:?}, got {got:?}", c.name),
            }
            assert_eq!(*named.lock().unwrap(), c.want_named, "{}: named handler calls", c.name);
            assert_eq!(
                *unhandled.lock().unwrap(),
                c.want_unhandled,
                "{}: fallback calls",
                c.name
            );
        }
    }

    #[test]
    fn chain_branches() {
        // The chain is baked around every handler the router calls: a registered handler and the
        // fallback both run inside it, first middleware outermost; an ignored event runs nothing.
        struct Case {
            name: &'static str,
            event: &'static str,
            fallback: bool,
            want: &'static [&'static str],
        }
        let cases = [
            Case {
                name: "first middleware is outermost",
                event: "Deposited",
                fallback: true,
                want: &[">a", ">b", "handler", "<b", "<a"],
            },
            Case {
                name: "fallback runs through the chain",
                event: "Closed",
                fallback: true,
                want: &[">a", ">b", "fallback", "<b", "<a"],
            },
            Case {
                name: "ignored event runs no middleware",
                event: "Closed",
                fallback: false,
                want: &[],
            },
        ];
        for c in cases {
            let trace = Arc::new(Mutex::new(Vec::new()));
            let mut router = EventRouter::new(vec![tracing("a", &trace), tracing("b", &trace)]);
            let t = Arc::clone(&trace);
            router
                .register(
                    "Deposited",
                    Box::new(move |_: &EventMessage| {
                        t.lock().unwrap().push("handler".into());
                        Ok(())
                    }),
                )
                .unwrap();
            if c.fallback {
                let t = Arc::clone(&trace);
                router.on_unhandled(Box::new(move |_: &EventMessage| {
                    t.lock().unwrap().push("fallback".into());
                    Ok(())
                }));
            }
            router.dispatch(&event("acct-1", c.event, 3)).unwrap();
            assert_eq!(*trace.lock().unwrap(), c.want, "{}", c.name);
        }
    }

    /// Counts claims and releases so the wrapper's store traffic is observable per branch.
    struct CountingStore {
        inner: MemoryIdempotencyStore<()>,
        claims: AtomicUsize,
        releases: AtomicUsize,
    }

    impl CountingStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: MemoryIdempotencyStore::new(),
                claims: AtomicUsize::new(0),
                releases: AtomicUsize::new(0),
            })
        }
    }

    impl IdempotencyStore<()> for CountingStore {
        type Tx = MemoryTx;
        fn claim(&self, tx: &mut MemoryTx, key: &str, payload_hash: u64) -> Result<Claim<()>, Error> {
            self.claims.fetch_add(1, Ordering::SeqCst);
            self.inner.claim(tx, key, payload_hash)
        }
        fn complete(
            &self,
            tx: &mut MemoryTx,
            key: &str,
            handle: &ClaimHandle,
            record: Record<()>,
        ) -> Result<(), Error> {
            self.inner.complete(tx, key, handle, record)
        }
        fn release(&self, tx: &mut MemoryTx, key: &str, handle: &ClaimHandle) -> Result<(), Error> {
            self.releases.fetch_add(1, Ordering::SeqCst);
            self.inner.release(tx, key, handle)
        }
        fn forget(&self, tx: &mut MemoryTx, key: &str) -> Result<(), Error> {
            self.inner.forget(tx, key)
        }
    }

    #[test]
    fn idempotent_by_id_branches() {
        // One row per branch of the event-side semantics table, over two deliveries of one event.
        /// What one delivery should return: `None` expects `Ok`, `Some(e)` exactly that error.
        type Expect = Option<Error>;
        struct Case {
            name: &'static str,
            /// Per handler run: `None` returns `Ok`, `Some(f)` returns that error.
            errors: &'static [Option<fn() -> HandlerError>],
            want: [Expect; 2],
            /// Another worker holds the claim before the first delivery.
            held: bool,
            /// The second delivery's body.
            redelivered: &'static [u8],
            want_calls: usize,
            want_claims: usize,
            want_releases: usize,
        }
        fn codec() -> HandlerError {
            Error::codec("bad bytes").into()
        }
        fn store_down() -> HandlerError {
            Error::store("projection down").into()
        }
        let cases = [
            Case {
                name: "first delivery runs and a completed duplicate is skipped",
                errors: &[None],
                want: [None, None],
                held: false,
                redelivered: DEPOSITED,
                want_calls: 1,
                want_claims: 2,
                want_releases: 0,
            },
            Case {
                name: "claim held by another worker fails with InProgress",
                errors: &[],
                want: [
                    Some(Error::new(ErrorCode::InProgress, KEY)),
                    Some(Error::new(ErrorCode::InProgress, KEY)),
                ],
                held: true,
                redelivered: DEPOSITED,
                want_calls: 0,
                want_claims: 2,
                want_releases: 0,
            },
            Case {
                name: "transient failure releases the claim so the redelivery reruns",
                errors: &[Some(store_down), None],
                want: [Some(Error::store("projection down")), None],
                held: false,
                redelivered: DEPOSITED,
                want_calls: 2,
                want_claims: 2,
                want_releases: 1,
            },
            Case {
                name: "terminal failure is cached and the redelivery replays it",
                errors: &[Some(codec)],
                want: [
                    Some(Error::codec("bad bytes")),
                    Some(Error::new(ErrorCode::CachedFailure, "codec: ")),
                ],
                held: false,
                redelivered: DEPOSITED,
                want_calls: 1,
                want_claims: 2,
                want_releases: 0,
            },
            Case {
                name: "same id with a different body fails with PayloadMismatch",
                errors: &[None],
                want: [None, Some(Error::new(ErrorCode::PayloadMismatch, KEY))],
                held: false,
                redelivered: br#"{"account_id":"acct-1","amount_cents":9900,"currency":"USD"}"#,
                want_calls: 1,
                want_claims: 2,
                want_releases: 0,
            },
        ];
        for c in cases {
            let store = CountingStore::new();
            let calls = Arc::new(AtomicUsize::new(0));
            let calls_h = Arc::clone(&calls);
            let errors = c.errors;
            let handler: EventHandler = Box::new(move |_: &EventMessage| {
                let n = calls_h.fetch_add(1, Ordering::SeqCst);
                match errors[n] {
                    Some(make) => Err(make()),
                    None => Ok(()),
                }
            });
            let guarded = idempotent_by_id(Arc::clone(&store), SCOPE, Tenancy::Silo)(handler);
            if c.held {
                let hash = edax_cqrs::payload_hash(DEPOSITED);
                assert!(
                    matches!(store.claim(&mut MemoryTx::auto_commit(), KEY, hash), Ok(Claim::Won(_))),
                    "{}",
                    c.name
                );
                store.claims.store(0, Ordering::SeqCst);
            }

            for (env, want) in [deposited(DEPOSITED), deposited(c.redelivered)].iter().zip(c.want) {
                let got = guarded(env);
                match (want, got) {
                    (None, Ok(())) => {}
                    (Some(want), Err(e)) => assert_eq!(e.downcast_ref::<Error>(), Some(&want), "{}", c.name),
                    (want, got) => panic!("{}: want {want:?}, got {got:?}", c.name),
                }
            }
            assert_eq!(calls.load(Ordering::SeqCst), c.want_calls, "{}: handler calls", c.name);
            assert_eq!(store.claims.load(Ordering::SeqCst), c.want_claims, "{}: claims", c.name);
            assert_eq!(
                store.releases.load(Ordering::SeqCst),
                c.want_releases,
                "{}: releases",
                c.name
            );
        }
    }

    #[test]
    fn the_claim_is_keyed_by_scope_and_tenant() {
        // Two deliveries of one event id over one shared store, each through a guard of its own: the
        // scope, tenancy and tenant decide whether the second finds the first's record.
        type Delivery = (&'static str, Tenancy, Option<&'static str>);
        struct Case {
            name: &'static str,
            deliveries: [Delivery; 2],
            want_runs: [usize; 2],
            want_claims: usize,
            want_error: Option<ErrorCode>,
        }
        let (silo, pool, bridge) = (Tenancy::Silo, Tenancy::Pool, Tenancy::Bridge);
        let cases = [
            Case {
                name: "two consumers on one store each run their handler",
                deliveries: [("balances", silo, None), ("receipts", silo, None)],
                want_runs: [1, 1],
                want_claims: 2,
                want_error: None,
            },
            Case {
                name: "one consumer skips its own redelivery",
                deliveries: [("balances", silo, None), ("balances", silo, None)],
                want_runs: [1, 0],
                want_claims: 2,
                want_error: None,
            },
            Case {
                name: "a pool keeps two tenants' events apart",
                deliveries: [("balances", pool, Some("acme")), ("balances", pool, Some("globex"))],
                want_runs: [1, 1],
                want_claims: 2,
                want_error: None,
            },
            Case {
                name: "a bridge keeps the key bare",
                deliveries: [("balances", bridge, Some("acme")), ("balances", bridge, Some("globex"))],
                want_runs: [1, 0],
                want_claims: 2,
                want_error: None,
            },
            Case {
                name: "a pool refuses an event without a tenant before the claim",
                deliveries: [("balances", pool, None), ("balances", pool, None)],
                want_runs: [0, 0],
                want_claims: 0,
                want_error: Some(ErrorCode::Codec),
            },
        ];
        for c in cases {
            let store = CountingStore::new();
            let mut runs = Vec::new();
            for (scope, mode, tenant) in c.deliveries {
                let calls = Arc::new(AtomicUsize::new(0));
                let counted = Arc::clone(&calls);
                let handler: EventHandler = Box::new(move |_: &EventMessage| {
                    counted.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                });
                let guarded = idempotent_by_id(Arc::clone(&store), scope, mode)(handler);
                let mut env = deposited(DEPOSITED);
                env.tenant = tenant.map(ToOwned::to_owned);
                let code = guarded(&env)
                    .err()
                    .and_then(|e| e.downcast_ref::<Error>().map(|e| e.code));
                assert_eq!(code, c.want_error, "{}", c.name);
                runs.push(calls.load(Ordering::SeqCst));
            }
            assert_eq!(runs, c.want_runs, "{}: handler runs", c.name);
            assert_eq!(store.claims.load(Ordering::SeqCst), c.want_claims, "{}: claims", c.name);
        }
    }

    #[test]
    fn a_panicking_handler_releases_the_claim() {
        // The dispatcher swallows a handler panic and nacks, so a claim leaked by the unwind would
        // make every redelivery read a pending record: `InProgress` forever, no progress possible.
        let store = CountingStore::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_h = Arc::clone(&calls);
        let handler: EventHandler = Box::new(move |_: &EventMessage| {
            if calls_h.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("handler bug");
            }
            Ok(())
        });
        let guarded = idempotent_by_id(Arc::clone(&store), SCOPE, Tenancy::Silo)(handler);
        let env = deposited(DEPOSITED);

        assert!(
            catch_unwind(AssertUnwindSafe(|| guarded(&env))).is_err(),
            "the panic reaches the dispatcher"
        );
        assert_eq!(
            store.releases.load(Ordering::SeqCst),
            1,
            "the claim is released, not stranded"
        );

        // The redelivery re-runs the handler instead of reading the winner's stranded claim.
        guarded(&env).expect("the redelivery runs and completes");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn router_plugs_into_key_ordered_dispatcher() {
        // The documented topology: EventRouter behind KeyOrderedDispatcher. This only compiles because
        // the router's boxed handlers are Send + Sync (the dispatcher requires Send + Sync + 'static).
        let hits = Arc::new(Mutex::new(0u32));
        let h = Arc::clone(&hits);
        let mut router = EventRouter::default();
        router
            .register(
                "Created",
                Box::new(move |_e: &EventMessage| {
                    *h.lock().unwrap() += 1;
                    Ok(())
                }),
            )
            .unwrap();
        let router = Arc::new(router);

        let dispatcher = KeyOrderedDispatcher::new(2, move |e: &EventMessage| router.dispatch(e))
            .expect("the dispatcher's workers spawn");
        for (seq, name) in [(1, "Created"), (2, "Ignored")] {
            // An event with no handler is a no-op that acks.
            dispatcher
                .submit(Delivery::new(event("a", name, seq), || {}, || {}))
                .expect("the dispatcher is running");
        }
        dispatcher.stop();
        assert_eq!(*hits.lock().unwrap(), 1);
    }

    #[test]
    fn redelivery_through_the_dispatcher_is_skipped_and_acked() {
        // End to end: the broker redelivers after a lost ack; the guarded router skips the duplicate
        // and the worker acks it, so the broker drops it instead of nacking forever.
        let calls = Arc::new(AtomicUsize::new(0));
        let settled: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
        let store = Arc::new(MemoryIdempotencyStore::<()>::new());
        let mut router = EventRouter::new(vec![idempotent_by_id(store, SCOPE, Tenancy::Silo)]);
        let c = Arc::clone(&calls);
        router
            .register(
                "Deposited",
                Box::new(move |_: &EventMessage| {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
            )
            .unwrap();
        let router = Arc::new(router);

        let dispatcher = KeyOrderedDispatcher::new(1, move |e: &EventMessage| router.dispatch(e))
            .expect("the dispatcher's workers spawn");
        for _ in 0..2 {
            let (a, n) = (Arc::clone(&settled), Arc::clone(&settled));
            dispatcher
                .submit(Delivery::new(
                    deposited(DEPOSITED),
                    move || a.lock().unwrap().push("ack"),
                    move || n.lock().unwrap().push("nack"),
                ))
                .expect("the dispatcher is running");
        }
        dispatcher.stop();

        assert_eq!(calls.load(Ordering::SeqCst), 1, "the handler ran once");
        assert_eq!(
            *settled.lock().unwrap(),
            ["ack", "ack"],
            "the duplicate is acked, not nacked"
        );
    }

    /// The consume-path twin of the command path's `idempotent_in`: the claim commits with the
    /// host's unit of work, so a rollback frees the key and the redelivery re-runs the handler
    /// instead of replaying an outcome that never landed. Two workers run at once, each under its
    /// own open transaction, which is what the per-event `current` exists for: with one shared
    /// slot a peer overwrites the handle between the write and the claim.
    /// `tests/test_router.py`'s `test_claim_enlists_in_the_ambient_tx` is the same rule in the
    /// Python SDK.
    #[test]
    fn each_worker_claims_under_its_own_open_transaction() {
        let db = MemoryTransactor::new();
        let store = db.idempotency_store::<()>();
        // The host's map from a delivery to the handle its own worker opened. `MemoryTx` is `Copy`,
        // so it hands out a value, not a borrow.
        let open: Arc<Mutex<HashMap<String, MemoryTx>>> = Arc::new(Mutex::new(HashMap::new()));
        let runs: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));

        let counter = Arc::clone(&runs);
        let handler: EventHandler = Box::new(move |env: &EventMessage| {
            *counter
                .lock()
                .expect("no worker panics")
                .entry(env.id.clone())
                .or_insert(0) += 1;
            Ok(())
        });
        let published = Arc::clone(&open);
        let guarded = Arc::new(idempotent_by_id_in(
            store,
            SCOPE,
            Tenancy::Silo,
            move |env: &EventMessage| {
                published
                    .lock()
                    .expect("no worker panics")
                    .get(&env.id)
                    .copied()
                    .unwrap_or_else(MemoryTx::auto_commit)
            },
        )(handler));

        // Both transactions are open before either claims, so a shared slot would hand one worker
        // the other's handle.
        let both_open = Arc::new(Barrier::new(2));
        let workers: Vec<_> = [("acct-1", false), ("acct-2", true)]
            .into_iter()
            .map(|(aggregate, roll_back)| {
                let (db, open, guarded, both_open) = (
                    db.clone(),
                    Arc::clone(&open),
                    Arc::clone(&guarded),
                    Arc::clone(&both_open),
                );
                std::thread::spawn(move || {
                    let env = event(aggregate, "Deposited", 1);
                    let unit = db.transaction(|tx| {
                        open.lock().expect("no worker panics").insert(env.id.clone(), *tx);
                        both_open.wait();
                        guarded(&env)?;
                        // Stands in for a later write in the same unit of work failing after the
                        // guard has already staged its claim.
                        if roll_back {
                            return Err(Error::store("a later write in the same unit of work failed").into());
                        }
                        Ok::<(), HandlerError>(())
                    });
                    assert_eq!(unit.is_err(), roll_back, "{aggregate}");
                })
            })
            .collect();
        for worker in workers {
            worker.join().expect("no worker panics");
        }

        // Both units of work are over, so a redelivery runs under the auto-commit handle.
        open.lock().expect("no worker panics").clear();
        for aggregate in ["acct-1", "acct-2"] {
            guarded(&event(aggregate, "Deposited", 1)).expect("the redelivery settles");
        }
        let runs = runs.lock().expect("no worker panics").clone();
        assert_eq!(
            runs.get("acct-1-1"),
            Some(&1),
            "the committed claim skips the redelivery"
        );
        assert_eq!(
            runs.get("acct-2-1"),
            Some(&2),
            "the rolled-back claim re-runs the handler"
        );
    }

    /// Historical records remain readable, but a new rejection retains only its stable code.
    #[test]
    fn a_rejection_from_the_historical_golden_row_is_now_persisted_code_only() {
        let row = edax_golden::read("idempotency_record.json")
            .into_iter()
            .find(|c| c["name"] == "cached_rejection_written_by_rust")
            .expect("the vector carries the Rust-written rejection");
        let message = row["error"].as_str().expect("a failed row has a message").to_owned();
        let store = Arc::new(MemoryIdempotencyStore::<()>::new());
        let handler: EventHandler = Box::new(move |_: &EventMessage| Err(crate::Terminal::new(message.clone()).into()));
        let guarded = idempotent_by_id(Arc::clone(&store), SCOPE, Tenancy::Silo)(handler);
        let mut env = event("acct-1", "Deposited", 1);
        env.id = "evt-1".into();
        env.data = b"cmd-a".to_vec();
        let err = guarded(&env).expect_err("the handler rejects");
        assert!(
            err.downcast_ref::<crate::Terminal>().is_some(),
            "the caller sees its own rejection"
        );

        let key = event_idempotency(&env, Tenancy::Silo, SCOPE).unwrap().key;
        let stored = match store.claim(&mut MemoryTx::auto_commit(), &key, 0).unwrap() {
            Claim::Prior(record) => record,
            Claim::Won(_) => panic!("nothing was cached"),
        };
        let bytes = edax_cqrs::encode_record(&stored).unwrap();
        assert!(
            matches!(stored.state, crate::State::Failed { code: Some(ErrorCode::Rejected), ref message } if message.is_empty())
        );
        assert!(!std::str::from_utf8(&bytes)
            .unwrap()
            .contains(row["error"].as_str().unwrap()));
    }

    #[test]
    fn unconfigured_router_refuses_encrypted_event_before_handlers() {
        use crate::sealing::{Cipher, MemoryKeys};
        let sealing = Sealing::new(Arc::new(MemoryKeys::new()), "test", Cipher::default()).unwrap();
        let mut draft = crate::testkit::account::deposited("account-42", 1, DEPOSITED);
        draft.subject = Some("owner".into());
        let raw = sealing.seal_event(draft).unwrap().record(42);
        let mut router = EventRouter::default();
        router
            .register("Deposited", Box::new(|_| panic!("encrypted event reached handler")))
            .unwrap();
        assert_eq!(
            crate::failure::code_of(router.dispatch(&raw).unwrap_err().as_ref()),
            Some(edax_core::ErrorCode::BadHandle)
        );
        assert_eq!(
            crate::failure::code_of(router.deliver(&raw).unwrap_err().as_ref()),
            Some(edax_core::ErrorCode::BadHandle)
        );
    }

    #[test]
    fn nested_plain_router_retains_the_outer_handlers_raw_provenance() {
        use crate::sealing::{Cipher, MemoryKeys};
        let sealing = Arc::new(Sealing::new(Arc::new(MemoryKeys::new()), "production-eu", Cipher::default()).unwrap());
        let mut draft = crate::testkit::account::deposited("account-42", 1, DEPOSITED);
        draft.subject = Some("person-42".into());
        let raw = sealing.seal_event(draft).unwrap().record(42);
        let expected = raw.clone();
        let mut inner = EventRouter::default();
        inner
            .register(
                "Deposited",
                Box::new(move |opened| {
                    assert_eq!(original_event(opened), expected);
                    Ok(())
                }),
            )
            .unwrap();
        let expected = raw.clone();
        let mut outer = EventRouter::default().sealing(sealing);
        outer
            .register(
                "Deposited",
                Box::new(move |opened| {
                    inner.dispatch(opened)?;
                    assert_eq!(original_event(opened), expected);
                    Ok(())
                }),
            )
            .unwrap();
        outer.deliver(&raw).unwrap();
        assert!(OPENED_EVENT.with_borrow(|pair| pair.is_none()));
        assert!(ROUTING_EVENT.with_borrow(|pair| pair.is_none()));
    }

    #[test]
    fn protected_router_admission_opens_once_and_fences_legacy() {
        use crate::command::admission_test_keys::ObservedKeys;
        use crate::sealing::Cipher;
        for (name, legacy, tamper, destroy, delivery) in [
            ("delivery opens once", false, false, false, true),
            ("direct opens once", false, false, false, false),
            ("legacy without owner", true, false, false, true),
            ("changed envelope refused", false, true, false, true),
            ("destroyed at admission settles", false, false, true, true),
            ("direct destruction raises", false, false, true, false),
        ] {
            let keys = Arc::new(ObservedKeys::default());
            let sealing = Arc::new(
                Sealing::new(keys.clone(), "production-eu", Cipher::default())
                    .unwrap()
                    .allow_plaintext(true),
            );
            let mut draft = crate::testkit::account::deposited("account-42", 1, DEPOSITED);
            if !legacy {
                draft.subject = Some("person-42".into());
                draft = sealing.seal_event(draft).unwrap();
            }
            let event = draft.record(42);
            let mut middleware: Vec<EventMiddleware> = vec![];
            if tamper {
                middleware.push(Box::new(|next| {
                    Box::new(move |raw| {
                        let mut changed = raw.clone();
                        changed.id.push_str("-changed");
                        next(&changed)
                    })
                }));
            }
            middleware.push(idempotent_by_id(MemoryIdempotencyStore::new(), SCOPE, Tenancy::Silo));
            let calls = Arc::new(AtomicUsize::new(0));
            let counted = calls.clone();
            let mut router = EventRouter::new(middleware).sealing(sealing);
            router
                .register(
                    "Deposited",
                    Box::new(move |opened| {
                        assert_eq!(opened.data, DEPOSITED);
                        assert!(opened.encryption.is_none());
                        counted.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }),
                )
                .unwrap();
            keys.destroy_on_hold.store(destroy, Ordering::SeqCst);
            let result = if delivery {
                router.deliver(&event)
            } else {
                router.dispatch(&event).map(|()| false)
            };
            if tamper || destroy && !delivery {
                assert_eq!(
                    result.unwrap_err().downcast_ref::<Error>().unwrap().code,
                    if destroy { ErrorCode::Shredded } else { ErrorCode::Codec },
                    "{name}"
                );
            } else {
                assert_eq!(result.unwrap(), destroy, "{name}");
            }
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(!tamper && !destroy), "{name}");
            assert_eq!(keys.resolutions.load(Ordering::SeqCst), usize::from(!legacy), "{name}");
        }
    }

    #[test]
    fn protected_router_keeps_ciphertext_in_guards_and_opens_only_handler_view() {
        use crate::sealing::{Cipher, KeyProvider, MemoryKeys};
        let keys = Arc::new(MemoryKeys::new());
        let sealing = Arc::new(Sealing::new(keys.clone(), "production-eu", Cipher::default()).unwrap());
        let mut draft = crate::testkit::account::deposited("account-42", 1, DEPOSITED);
        draft.subject = Some("person-42".into());
        let event = sealing.seal_event(draft).unwrap().record(42);
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let inspect: EventMiddleware = Box::new(|next| {
            Box::new(move |raw| {
                assert!(raw.encryption.is_some());
                assert_ne!(raw.data, DEPOSITED);
                next(raw)
            })
        });
        let mut router = EventRouter::new(vec![
            inspect,
            idempotent_by_id(MemoryIdempotencyStore::new(), SCOPE, Tenancy::Silo),
        ]);
        router
            .register(
                "Deposited",
                Box::new(move |opened| {
                    assert!(opened.encryption.is_none());
                    assert_eq!(opened.data, DEPOSITED);
                    counted.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
            )
            .unwrap();
        let router = router.sealing(sealing.clone());
        assert!(!router.deliver(&event).unwrap());
        assert!(!router.deliver(&event).unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(event.encryption.is_some());
        let scope = sealing.scope(None, Some("person-42")).unwrap();
        keys.destroy(&scope).unwrap();
        assert!(router.deliver(&event).unwrap());
        assert_eq!(
            router
                .dispatch(&event)
                .unwrap_err()
                .downcast_ref::<Error>()
                .unwrap()
                .code,
            ErrorCode::Shredded
        );
    }

    #[test]
    fn protected_router_never_turns_handler_errors_into_skips() {
        use crate::sealing::{Cipher, MemoryKeys};
        let sealing = Arc::new(Sealing::new(Arc::new(MemoryKeys::new()), "production-eu", Cipher::default()).unwrap());
        let mut draft = crate::testkit::account::deposited("account-42", 1, DEPOSITED);
        draft.subject = Some("person-42".into());
        let event = sealing.seal_event(draft).unwrap().record(42);
        let mut router = EventRouter::default().sealing(sealing);
        router
            .register(
                "Deposited",
                Box::new(|_| Err(Error::new(ErrorCode::Shredded, "handler failed").into())),
            )
            .unwrap();
        assert_eq!(
            router
                .deliver(&event)
                .unwrap_err()
                .downcast_ref::<Error>()
                .unwrap()
                .code,
            ErrorCode::Shredded
        );
        assert_eq!(
            router
                .dispatch(&deposited(DEPOSITED))
                .unwrap_err()
                .downcast_ref::<Error>()
                .unwrap()
                .code,
            ErrorCode::Codec
        );
    }
}

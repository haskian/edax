"""Event router: dispatch an event message to the handler registered for its event name, through a middleware
chain. The read-side mirror of :class:`~edax.command.CommandBus`. No kernel is involved in routing: which
worker handles an event is already decided by its ordering key. Pass ``router.dispatch`` to a
:class:`~edax.dispatcher.KeyOrderedDispatcher` and events route by name with same-aggregate order preserved,
and :func:`idempotent_by_id` is the consume-path middleware. See ``docs/sdk/consuming.md``."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from . import _native
from ._chain import Registry, require_async
from ._observer import wrap
from ._wiring import wiring_str
from .causation import caused_by
from .idempotency import Guard, IdempotencyStore, guarded
from .message import EventMessage
from .observe import Observer
from .tenancy import Tenancy
from .unit_of_work import Transactor, TxT

__all__ = [
    "EventHandler",
    "EventMiddleware",
    "EventRouter",
    "event_claim_key",
    "forget_event",
    "idempotent_by_id",
]


#: An event handler: a coroutine function, as a command handler is, so no blocking call hides in the loop.
EventHandler = Callable[[EventMessage], Awaitable[None]]
EventMiddleware = Callable[[EventHandler], EventHandler]


class EventRouter:
    """Register handlers by event name; dispatch decoded event messages to them. ``middleware`` wraps every
    handler at :meth:`register` time, first middleware outermost. An event with no registered handler goes to
    ``on_unhandled``, also wrapped, or is ignored if that is unset; an ignored event runs no middleware.
    Every handler is a coroutine function, as on :class:`~edax.command.CommandBus`: a sync one raises
    :class:`TypeError` when it is wired, not when the first event reaches it."""

    def __init__(self, *middleware: EventMiddleware, on_unhandled: EventHandler | None = None) -> None:
        self._handlers: Registry[EventHandler] = Registry(middleware)
        if on_unhandled is not None:
            require_async(on_unhandled, "on_unhandled")
        self._on_unhandled = self._handlers.wrap(on_unhandled) if on_unhandled is not None else None

    def register(self, name: str, handler: EventHandler) -> None:
        """Install ``handler`` for events named ``name``, wrapped in the middleware. A second handler for
        one name raises the terminal :class:`~edax.errors.AlreadyRegistered`, and a handler that is not a
        coroutine function :class:`TypeError`."""
        require_async(handler, f"event handler for {name!r}")
        self._handlers.register(name, handler)

    def on(self, name: str) -> Callable[[EventHandler], EventHandler]:
        """Decorator form of :meth:`register`. Returns the bare handler, not the wrapped one."""

        def deco(fn: EventHandler) -> EventHandler:
            self.register(name, fn)
            return fn

        return deco

    async def dispatch(self, msg: EventMessage) -> None:
        """Route ``msg`` to the handler for ``msg.name``. The handler runs under the ambient lineage
        :func:`~edax.causation.propagate` of ``msg``, so an event it saves continues the flow."""
        handler = self._handlers.get(msg.name)
        if handler is None:
            handler = self._on_unhandled
        if handler is not None:
            with caused_by(msg):
                await handler(msg)


def idempotent_by_id(
    store: IdempotencyStore[TxT],
    unit_of_work: Transactor[TxT] | None = None,
    *,
    scope: str,
    tenancy: Tenancy,
    observer: Observer | None = None,
) -> EventMiddleware:
    """Run a handler at most once per event ``id``, so a broker redelivery does not repeat its side effect.
    Composes anywhere a handler is accepted. The key and the hash are the kernel's rule
    (``testdata/golden/event_idempotency.json``): ``msg.id`` under ``scope``, and FNV-1a over the raw
    ``msg.data`` bytes.

    ``scope`` names this consumer, usually the read model or the side effect it guards: ``"balances"``,
    ``"receipts"``. Each consumer keeps its own record of the events it handled, so two consumers guarding
    over one ledger each run their handler once, instead of the second skipping what the first did.
    ``tenancy`` is how the deployment separates tenants, as on :func:`~edax.command.idempotent`. Under
    :attr:`~edax.tenancy.Tenancy.POOL` the key carries the event's ``tenant``. Under a pool or a bridge,
    an event that names no tenant raises :class:`~edax.errors.CodecError` before any claim.

    Without ``unit_of_work`` the store enlists in the ambient :func:`~edax.unit_of_work.current_tx`, which
    exists only if the host opened it outside this guard: a dispatcher worker has none. ``unit_of_work``
    closes that gap, so a failure of the commit releases the claim.

    Unlike :func:`~edax.command.idempotent`, no caller awaits an event handler's result, so a redelivery of
    a completed event skips silently. A redelivery while another worker holds the claim raises transient
    :class:`~edax.errors.InProgress`; a transient handler failure releases the claim, a terminal one is
    cached and replays as terminal :class:`~edax.errors.CachedFailure`, and two events sharing an ``id``
    with different bodies raise :class:`~edax.errors.PayloadMismatch`. See ``docs/sdk/consuming.md``.

    ``observer`` receives a :meth:`~edax.observe.Observer.transaction` per ``unit_of_work`` the guard opens,
    a :meth:`~edax.observe.Observer.claim` verdict for every lost claim and a
    :meth:`~edax.observe.Observer.settled` for every won one; ``None`` is the no-op observer."""
    wiring_str(scope, "idempotent_by_id(scope=...), which names the consumer,")
    observe = wrap(observer)

    def mw(next_: EventHandler) -> EventHandler:
        async def handler(msg: EventMessage) -> None:
            rule = _native.event_idempotency(msg, tenancy.value, scope)
            guard = Guard(store, msg.name, rule.key, rule.payload_hash, observe)
            await guarded(guard, lambda: next_(msg), unit_of_work)

        return handler

    return mw


def event_claim_key(scope: str, event_id: str, *, tenancy: Tenancy, tenant: str | None = None) -> str:
    """The ledger key the :func:`idempotent_by_id` guard named ``scope`` claims for the event ``event_id``,
    in a deployment that separates tenants by ``tenancy``. Under :attr:`~edax.tenancy.Tenancy.POOL` the key
    carries the event's ``tenant``. Under a mode that requires a tenant, a call that names none raises
    :class:`~edax.errors.CodecError`. The composition is the kernel's, the same key the guard claims
    (``testdata/golden/event_idempotency.json``)."""
    return _native.event_claim_key(tenancy.value, scope, event_id, tenant)


async def forget_event(
    store: IdempotencyStore[TxT],
    scope: str,
    event_id: str,
    *,
    tenancy: Tenancy,
    tenant: str | None = None,
    tx: TxT | None = None,
) -> None:
    """Delete the record the :func:`idempotent_by_id` guard named ``scope`` keeps for the event ``event_id``,
    so a redelivery of that event runs its handler again. Use it to re-drive a dead-lettered event whose
    terminal failure was cached. Pass the guard's ``tenancy`` and the event's ``tenant``: the record lives
    under the key :func:`event_claim_key` composes from them. ``tenancy`` has no default, as on
    :func:`~edax.idempotency.forget`. The *Forget* clause of :class:`~edax.idempotency.IdempotencyStore` says
    what happens to a claim still in flight and to a key with no record."""
    await store.forget(event_claim_key(scope, event_id, tenancy=tenancy, tenant=tenant), tx)

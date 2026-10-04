"""Event router: dispatch an event message to the handler registered for its event name, through a middleware
chain. The read-side mirror of :class:`~edax.command.CommandBus`. No kernel is involved in routing: which
worker handles an event is already decided by its ordering key. Pass ``router.dispatch`` to a
:class:`~edax.dispatcher.KeyOrderedDispatcher` and events route by name with same-aggregate order preserved,
and :func:`idempotent_by_id` is the consume-path middleware. See ``docs/sdk/consuming.md``."""

from __future__ import annotations

from collections.abc import Callable

from . import _edax, errors
from ._async import MaybeAwaitable, maybe_await
from .aggregate import EventMessage
from .causation import caused_by
from .command import IdempotencyStore, _compose, _Guard, _guarded
from .errors import AlreadyRegistered
from .observe import Observer, _wrap
from .store import Transactor
from .tenancy import Tenancy

__all__ = [
    "EventHandler",
    "EventMiddleware",
    "EventRouter",
    "idempotent_by_id",
]


EventHandler = Callable[[EventMessage], MaybeAwaitable[None]]
EventMiddleware = Callable[[EventHandler], EventHandler]


class EventRouter:
    """Register handlers by event name; dispatch decoded event messages to them. ``middleware`` wraps every
    handler at :meth:`register` time, first middleware outermost. An event with no registered handler goes to
    ``on_unhandled``, also wrapped, or is ignored if that is unset; an ignored event runs no middleware."""

    def __init__(self, *middleware: EventMiddleware, on_unhandled: EventHandler | None = None) -> None:
        self._handlers: dict[str, EventHandler] = {}
        self._middleware = middleware
        self._on_unhandled = self._wrap(on_unhandled) if on_unhandled is not None else None

    def _wrap(self, handler: EventHandler) -> EventHandler:
        return _compose(self._middleware, handler)

    def register(self, name: str, handler: EventHandler) -> None:
        if name in self._handlers:
            raise AlreadyRegistered(f"event handler already registered for {name!r}")
        self._handlers[name] = self._wrap(handler)

    def on(self, name: str) -> Callable[[EventHandler], EventHandler]:
        """Decorator form of :meth:`register`. Returns the bare handler, not the wrapped one."""

        def deco(fn: EventHandler) -> EventHandler:
            self.register(name, fn)
            return fn

        return deco

    async def dispatch(self, msg: EventMessage) -> None:
        """Route ``msg`` to the handler for ``msg.name``. The handler runs under the ambient lineage
        :func:`~edax.causation.propagate` of ``msg``, so an event it saves continues the flow."""
        handler = self._handlers.get(msg.name, self._on_unhandled)
        if handler is not None:
            with caused_by(msg):
                await maybe_await(handler(msg))


def idempotent_by_id(
    store: IdempotencyStore,
    unit_of_work: Transactor | None = None,
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
    ``tenancy`` is how the deployment separates tenants, as on :func:`~edax.command.idempotent`: under
    :attr:`~edax.tenancy.Tenancy.POOL` the key carries the event's ``tenant``, and under a pool or a bridge
    an event that names no tenant raises :class:`~edax.errors.CodecError` before any claim.

    Without ``unit_of_work`` the store enlists in the ambient :func:`~edax.store.current_tx`, which
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
    if not isinstance(scope, str):
        raise TypeError(
            f"idempotent_by_id(scope=...) names the consumer and must be a str, got {type(scope).__name__}"
        )
    observe = _wrap(observer)

    def mw(next_: EventHandler) -> EventHandler:
        async def handler(msg: EventMessage) -> None:
            with errors.mapped():
                rule = _edax.event_idempotency(msg, tenancy.value, scope)
            guard = _Guard(store, msg.name, rule.key, rule.payload_hash, observe)
            await _guarded(guard, lambda: maybe_await(next_(msg)), unit_of_work)

        return handler

    return mw

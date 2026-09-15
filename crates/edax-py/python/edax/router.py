"""Event router: dispatch an envelope to the handler registered for its event name, through a middleware
chain. The read-side mirror of :class:`~edax.command.CommandBus`. No kernel is involved in routing: which
worker handles an event is already decided by ``partition``/``rendezvous``. Pass ``router.dispatch`` to a
:class:`~edax.dispatcher.KeyOrderedDispatcher` and events route by name with same-aggregate order preserved,
and :func:`idempotent_by_id` is the consume-path middleware. See ``docs/sdk/consuming.md``."""

from __future__ import annotations

from typing import Callable, Optional

from . import _edax
from ._async import MaybeAwaitable, maybe_await
from .aggregate import EventEnvelope
from .command import IdempotencyStore, _guarded
from .errors import AlreadyRegistered
from .observe import Observer, _wrap
from .store import Transactor

EventHandler = Callable[[EventEnvelope], MaybeAwaitable[None]]
EventMiddleware = Callable[[EventHandler], EventHandler]


class EventRouter:
    """Register handlers by event name; dispatch decoded envelopes to them. ``middleware`` wraps every
    handler at :meth:`register` time, first middleware outermost. An event with no registered handler goes to
    ``on_unhandled``, also wrapped, or is ignored if that is unset; an ignored event runs no middleware."""

    def __init__(self, *middleware: EventMiddleware, on_unhandled: Optional[EventHandler] = None) -> None:
        self._handlers: dict[str, EventHandler] = {}
        self._middleware = middleware
        self._on_unhandled = self._wrap(on_unhandled) if on_unhandled is not None else None

    def _wrap(self, handler: EventHandler) -> EventHandler:
        for mw in reversed(self._middleware):
            handler = mw(handler)
        return handler

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

    async def dispatch(self, env: EventEnvelope) -> None:
        """Route ``env`` to the handler for ``env.event.name``."""
        handler = self._handlers.get(env.event.name, self._on_unhandled)
        if handler is not None:
            await maybe_await(handler(env))


def idempotent_by_id(
    store: IdempotencyStore,
    unit_of_work: Optional[Transactor] = None,
    *,
    observer: Optional[Observer] = None,
) -> EventMiddleware:
    """Run a handler at most once per event ``id``, so a broker redelivery does not repeat its side effect.
    Composes anywhere a handler is accepted. The key is ``env.id``, the hash the kernel's over the raw
    ``event.data`` bytes. Without ``unit_of_work`` the store enlists in the ambient
    :func:`~edax.store.current_tx`, which exists only if the host opened it outside this guard: a dispatcher
    worker has none. ``unit_of_work`` closes that gap, so a failure of the commit releases the claim.

    Unlike :func:`~edax.command.idempotent`, no caller awaits an event handler's result, so a redelivery of
    a completed event skips silently. A redelivery while another worker holds the claim raises transient
    :class:`~edax.errors.InProgress`; a transient handler failure releases the claim, a terminal one is
    cached and replays as terminal :class:`~edax.errors.CachedFailure`, and two events sharing an ``id``
    with different bodies raise :class:`~edax.errors.PayloadMismatch`. See ``docs/sdk/consuming.md``.

    ``observer`` receives a :meth:`~edax.observe.Observer.transaction` per ``unit_of_work`` the guard opens,
    a :meth:`~edax.observe.Observer.claim` verdict for every lost claim and a
    :meth:`~edax.observe.Observer.settled` for every won one; ``None`` is the no-op observer."""
    observe = _wrap(observer)

    def mw(next_: EventHandler) -> EventHandler:
        async def handler(env: EventEnvelope) -> None:
            hash_ = _edax.payload_hash(env.event.data)
            await _guarded(
                store, env.event.name, env.id, hash_, lambda: maybe_await(next_(env)), unit_of_work, observe
            )

        return handler

    return mw

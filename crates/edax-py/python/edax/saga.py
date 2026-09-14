"""Saga / process manager: react to committed events, keep per-correlation state, emit commands. Where an
aggregate decides one entity's next state, a saga coordinates a workflow across several. Each event it cares
about is routed to a correlation (one running instance), folded into that instance's state, and may emit
commands. :class:`SagaRunner` wires :class:`Saga` and :class:`SagaStore` to a command dispatcher. See
``docs/sdk/sagas.md``."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional, Protocol

from ._async import MaybeAwaitable, maybe_await
from .aggregate import EventEnvelope
from .store import Tx

__all__ = ["Completed", "Saga", "SagaInstance", "SagaRunner", "SagaStore"]


class _Completed(Enum):
    """Sentinel: the instance has finished and the runner deletes it. Distinct from a ``None`` state, which
    a saga may keep. An enum member keeps its identity through copy and pickle."""

    COMPLETED = "Completed"

    def __repr__(self) -> str:
        return "Completed"


Completed = _Completed.COMPLETED


@dataclass
class SagaInstance:
    """A running saga instance: its accumulated ``state`` and the ``version`` it was loaded at."""

    state: Any
    version: int = 0


class Saga(Protocol):
    def correlation_id(self, env: EventEnvelope) -> Optional[str]:
        """Which running instance this event belongs to, or ``None`` to ignore it."""
        ...

    def handle(self, state: Any, env: EventEnvelope) -> tuple[Any, list[Any]]:
        """Fold ``env`` into the instance; ``state`` is ``None`` for a new one. Return ``(new_state,
        commands)``, where a ``new_state`` of :data:`Completed` finishes and removes the instance."""
        ...


class SagaStore(Protocol):
    """Per-correlation instance state with optimistic concurrency. What a backend must guarantee:

    * ``load`` returns the stored :class:`SagaInstance` (state plus version) or ``None``.
    * ``save`` persists at ``version + 1`` only if the store is still at ``version``, else raises
      :class:`~edax.errors.ConcurrencyError`. ``delete`` removes at ``version``; absent is a no-op.
    * With ``tx`` given, or :func:`~edax.store.current_tx` set, a durable backend enlists the write in that
      unit of work.

    A saga whose ``correlation_id`` is the aggregate id has no concurrent writer, because the dispatcher
    serializes same-``aggregate_id`` events; the version is what lets a cross-aggregate correlation catch
    one. The runner dispatches commands before it saves state, so a crash in between re-emits them and an
    idempotent command bus absorbs the duplicates. That pair is not atomic; making it so needs an outbox."""

    async def load(self, id: str, tx: Optional[Tx] = None) -> Optional[SagaInstance]: ...
    async def save(self, id: str, state: Any, version: int, tx: Optional[Tx] = None) -> int: ...
    async def delete(self, id: str, version: int, tx: Optional[Tx] = None) -> None: ...


Dispatch = Callable[[Any], MaybeAwaitable[Any]]


class SagaRunner:
    """Drives a :class:`Saga` over committed events: route to a correlation, fold, dispatch commands,
    persist at the loaded version. Pass :meth:`on_event` to an ``EventRouter`` or ``KeyOrderedDispatcher``,
    and ``CommandBus.dispatch`` behind :func:`~edax.command.idempotent` as ``dispatch``."""

    def __init__(self, saga: Saga, store: SagaStore, dispatch: Dispatch) -> None:
        self._saga = saga
        self._store = store
        self._dispatch = dispatch

    async def on_event(self, env: EventEnvelope, tx: Optional[Tx] = None) -> None:
        cid = self._saga.correlation_id(env)
        if cid is None:
            return
        # `tx` goes through as given: the store falls back to the ambient `current_tx()` itself.
        instance = await self._store.load(cid, tx)
        version = instance.version if instance is not None else 0
        state = instance.state if instance is not None else None
        new_state, commands = self._saga.handle(state, env)
        # Dispatch before persisting: at-least-once, with duplicates absorbed by an idempotent bus.
        for cmd in commands:
            await maybe_await(self._dispatch(cmd))
        if new_state is Completed:
            # Nothing loaded: skip the delete. A versioned delete would turn a no-op into a write, which a
            # concurrent creator of the same correlation would then conflict with.
            if instance is not None:
                await self._store.delete(cid, version, tx)
        else:
            await self._store.save(cid, new_state, version, tx)

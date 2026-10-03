"""Saga / process manager: react to committed events, keep per-correlation state, emit commands. Where an
aggregate decides one entity's next state, a saga coordinates a workflow across several. Each event it cares
about is routed to a correlation (one running instance), folded into that instance's state, and may emit
commands. :class:`SagaRunner` folds an event and persists the step; a :class:`~edax.store.Relay` over the
store sends the commands. See ``docs/sdk/sagas.md``.

The runner never sends a command itself. It saves the new state *and* the commands the fold decided in one
versioned write, the saga outbox, and the relay sends them once that write has committed. A fold that loses
the version race sends nothing: its commands roll back with its state, and the redelivery folds the winner's
state. A crash after the commit re-sends from the outbox instead of losing the command, so a command is sent
at least once per committed step and its consumer deduplicates it by the command id. Completion writes a
tombstone: a late or redelivered event finds the instance completed and is ignored, and a writer that loaded
the instance before it completed conflicts."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol

from . import _edax, errors
from ._async import MaybeAwaitable, maybe_await
from .aggregate import EventEnvelope
from .observe import Observer
from .pacing import Pacing
from .store import EventCodec, Relay, RelaySink, Tx

__all__ = ["Completed", "Saga", "SagaCommand", "SagaInstance", "SagaRunner", "SagaStore"]

#: One command in a saga's outbox: its ``id`` (``saga_command_id`` of the step that decided it), the
#: ``correlation_id`` of the instance, the ``command`` marshaled by the runner's codec as an
#: :class:`~edax.aggregate.Event`, and the ``timestamp`` the step wrote it at. The native record, so it
#: crosses into the in-memory store with no conversion.
SagaCommand = _edax.SagaCommand


class _Completed(Enum):
    """Sentinel: the instance has finished, and the runner writes its tombstone. Distinct from a ``None``
    state, which a saga may keep. An enum member keeps its identity through copy and pickle."""

    COMPLETED = "Completed"

    def __repr__(self) -> str:
        return "Completed"


Completed = _Completed.COMPLETED


@dataclass
class SagaInstance:
    """A stored saga instance: its accumulated ``state`` and the ``version`` a write is conditional on.
    ``completed`` marks a tombstone: the workflow has finished, ``state`` is ``None``, and no write lands on
    it again."""

    state: Any
    version: int = 0
    completed: bool = False


class Saga(Protocol):
    """Parameters are positional-only for the same reason as :class:`~edax.store.AggregateCodec`'s: the
    runner calls them positionally, so an implementation may name them after its own workflow."""

    def correlation_id(self, env: EventEnvelope, /) -> str | None:
        """Which running instance this event belongs to, or ``None`` to ignore it."""
        ...

    def handle(self, state: Any, env: EventEnvelope, /) -> tuple[Any, list[Any]]:
        """Fold ``env`` into the instance; ``state`` is ``None`` for a new one. Return ``(new_state,
        commands)``, where a ``new_state`` of :data:`Completed` finishes the instance. Pure: the runner may
        discard the result when the write loses a race."""
        ...


class SagaStore(Protocol):
    """Per-correlation instance state, under optimistic concurrency, and the saga outbox. What a backend
    must guarantee:

    * ``load`` returns the stored :class:`SagaInstance` (a tombstone has ``completed=True``) or ``None``.
    * ``save`` and ``complete`` write at ``version + 1`` only if the store is still at ``version`` (an absent
      instance is at version 0), and return the new version; otherwise they raise
      :class:`~edax.errors.ConcurrencyError` and write nothing. A completed instance takes no write at all.
    * The step's ``commands`` are written in the same write as the state, so they commit or roll back
      together. Each carries the id ``saga_command_id(id, version + 1, index)``; a store may key its outbox
      on it.
    * ``complete`` keeps the row as a tombstone at its new version rather than deleting it, so a stale
      writer conflicts and a late event cannot start the workflow again. A durable store purges tombstones by
      retention, longer than any redelivery window.
    * ``unpublished`` returns committed unsent commands, oldest commit first; ``mark_published`` prunes
      them by id, idempotently. Both run outside any ``tx``, as :class:`~edax.store.Relay` calls them.
    * With ``tx`` given, or :func:`~edax.store.current_tx` set, a durable backend enlists the write in that
      unit of work.

    A saga whose ``correlation_id`` is the aggregate id has no concurrent writer, because the dispatcher
    serializes same-``aggregate_id`` events; the version is what lets a cross-aggregate correlation catch
    one. ``SagaStoreContract`` in :mod:`edax.testing` holds a backend to every clause."""

    async def load(self, id: str, tx: Tx | None = None) -> SagaInstance | None: ...
    async def save(
        self, id: str, state: Any, version: int, commands: Sequence[SagaCommand], tx: Tx | None = None
    ) -> int: ...
    async def complete(
        self, id: str, version: int, commands: Sequence[SagaCommand], tx: Tx | None = None
    ) -> int: ...
    async def unpublished(self, limit: int = 100) -> list[SagaCommand]: ...
    async def mark_published(self, ids: Sequence[str]) -> None: ...


Dispatch = Callable[[Any], MaybeAwaitable[Any]]


class SagaRunner:
    """Drives a :class:`Saga` over committed events: route to a correlation, fold, and persist the step,
    state and commands in one write at the loaded version. Pass :meth:`on_event` to an ``EventRouter`` or
    ``KeyOrderedDispatcher``, and send the commands with :meth:`relay`. ``commands`` marshals the saga's
    command objects into the outbox and back, like an event codec: ``json_codec({"Fulfill": Fulfill})``.
    ``now`` stamps each outbox row (default: the kernel clock)."""

    def __init__(
        self,
        saga: Saga,
        store: SagaStore,
        commands: EventCodec,
        *,
        now: Callable[[], int] | None = None,
    ) -> None:
        self._saga = saga
        self._store = store
        self._commands = commands
        self._now = now or _edax.current_timestamp

    async def on_event(self, env: EventEnvelope, tx: Tx | None = None) -> None:
        """Handle one committed event: load the correlation's instance, fold the event, and write the new
        state (or the tombstone) with the commands the fold emitted. ``tx`` goes through as given: the store
        falls back to the ambient :func:`~edax.store.current_tx` itself. A
        :class:`~edax.errors.ConcurrencyError` means another worker advanced the instance first; it is
        transient, so the event is redelivered and folds the winner's state, and nothing this fold decided
        is ever sent."""
        cid = self._saga.correlation_id(env)
        if cid is None:
            return
        instance = await self._store.load(cid, tx)
        if instance is not None and instance.completed:
            # The kernel's write for any fold of a completed instance is `ignore` (pinned by
            # `testdata/golden/saga.json`), so the fold is not even run.
            return
        loaded = "absent" if instance is None else "active"
        version = 0 if instance is None else instance.version
        new_state, commands = self._saga.handle(None if instance is None else instance.state, env)
        completes = new_state is Completed
        with errors.mapped():
            write = _edax.saga_write(loaded, version, completes, bool(commands))
        if write.action not in ("save", "complete"):
            return
        rows = self._rows(cid, write.expected + 1, commands)
        if write.action == "save":
            await self._store.save(cid, new_state, write.expected, rows, tx)
        else:
            await self._store.complete(cid, write.expected, rows, tx)

    def _rows(self, cid: str, version: int, commands: Sequence[Any]) -> list[SagaCommand]:
        """The outbox rows for the commands a write at ``version`` sends."""
        timestamp = self._now()
        with errors.mapped():
            return [
                SagaCommand(
                    _edax.saga_command_id(cid, version, index),
                    cid,
                    self._commands.marshal(command),
                    timestamp,
                )
                for index, command in enumerate(commands)
            ]

    def command(self, row: SagaCommand) -> Any:
        """Decode an outbox row back into the command the saga emitted."""
        return self._commands.unmarshal(row.command)

    def relay(
        self,
        dispatch: Dispatch,
        *,
        on_dead: RelaySink | None = None,
        max_attempts: int = 0,
        pacing: Pacing | None = None,
        observer: Observer | None = None,
    ) -> Relay:
        """A :class:`~edax.store.Relay` over this runner's command outbox that decodes each row and hands the
        command to ``dispatch`` (``CommandBus.dispatch``, behind :func:`~edax.command.idempotent`). The relay
        is at-least-once, so key the guard on the command's own identity, or publish the rows yourself with
        ``Relay(store, publish)`` and key on ``row.id``. A terminal command failure dead-letters the row
        through ``on_dead``; see :class:`~edax.store.Relay` for the rest of the policy."""

        async def publish(row: SagaCommand) -> None:
            await maybe_await(dispatch(self.command(row)))

        return Relay(
            self._store, publish, observer=observer, on_dead=on_dead, max_attempts=max_attempts, pacing=pacing
        )

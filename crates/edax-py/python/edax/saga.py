"""Saga / process manager: react to committed events, keep per-instance state, emit commands. Where an
aggregate decides one entity's next state, a saga coordinates a workflow across several. Each event it cares
about is routed to one running instance, folded into that instance's state, and may emit commands.
:class:`SagaRunner` folds an event and persists the step; a :class:`~edax.store.Relay` over the store sends
the commands. See ``docs/sdk/sagas.md``.

The runner never sends a command itself. It saves the new state *and* the commands the fold decided in one
versioned write, the saga outbox, and the relay sends them once that write has committed. A fold that loses
the version race sends nothing: its commands roll back with its state, and the redelivery folds the winner's
state. A crash after the commit re-sends from the outbox instead of losing the command, so a command is sent
at least once per committed step, and its consumer deduplicates it by the command's idempotency key,
which is the row's ``saga_command_id`` under the saga's name.
Completion writes a tombstone: a late or redelivered event finds the instance completed and is ignored, and
a writer that loaded the instance before it completed conflicts. A running instance remembers what it
folded, so an event redelivered after its step committed (a lost ack, a crash before the offset commit) is
skipped instead of folded again under new command ids.

A command that dies in the relay (its handler raises :class:`~edax.errors.Rejected`) saves no event, so
nothing would tell the instance. Wire :meth:`SagaRunner.command_failed` as the relay's ``on_dead``: it folds
a :data:`SAGA_COMMAND_FAILED` event into the instance that sent the command, and the saga compensates."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, Protocol, TypedDict, cast

from . import _edax, errors
from .aggregate import CommandMessage, EventMessage
from .causation import caused_by
from .command import CommandBus
from .errors import _carried_code
from .observe import Observer, SagaSkip, _wrap
from .pacing import Pacing
from .store import EventCodec, Relay, RelaySink, Tx

__all__ = [
    "SAGA_COMMAND_FAILED",
    "Completed",
    "Saga",
    "SagaCommandFailed",
    "SagaInstance",
    "SagaRunner",
    "SagaStore",
]

#: The name of the event :meth:`SagaRunner.command_failed` folds when a command the saga sent died.
SAGA_COMMAND_FAILED: Final[str] = _edax.saga_command_failed_name()


class SagaCommandFailed(TypedDict):
    """The JSON payload of a :data:`SAGA_COMMAND_FAILED` event: ``json.loads(msg.data)``. ``command_id`` is
    the dead command's outbox id, ``command`` its name, ``code`` the failure's taxonomy code (``"rejected"``
    for a business rejection, ``None`` when no code classifies it), and ``error`` its message."""

    command_id: str
    command: str
    code: str | None
    error: str


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
    it again. ``folded`` is the kernel's record of the events the instance folded (``saga_admit``), opaque
    text the store keeps as the last ``save`` wrote it, empty for a tombstone."""

    state: Any
    version: int = 0
    completed: bool = False
    folded: str = ""


class Saga(Protocol):
    """Parameters are positional-only for the same reason as :class:`~edax.store.AggregateCodec`'s: the
    runner calls them positionally, so an implementation may name them after its own workflow."""

    # A read-only property, as `Command.type` is: a `ClassVar[str]` or a plain attribute satisfies it.
    @property
    def name(self) -> str:
        """The saga's name, stable across deploys. It scopes the idempotency key of every command the saga
        sends, so two sagas that share an instance id and a bus never replay each other's commands."""
        ...

    def instance_id(self, msg: EventMessage, /) -> str | None:
        """The id of the running instance this event belongs to, or ``None`` to ignore it. Not the event's
        ``correlation_id``: that names the flow, and one flow may drive several instances. The instance id
        becomes the ``aggregate_id`` of every command the instance sends, so its commands keep their
        order."""
        ...

    def handle(self, state: Any, msg: EventMessage, /) -> tuple[Any, list[Any]]:
        """Fold ``msg`` into the instance; ``state`` is ``None`` for a new one. Return ``(new_state,
        commands)``, where a ``new_state`` of :data:`Completed` finishes the instance. Pure: the runner may
        discard the result when the write loses a race."""
        ...


class SagaStore(Protocol):
    """Per-instance state, under optimistic concurrency, and the saga outbox. What a backend
    must guarantee:

    * ``load`` returns the stored :class:`SagaInstance` (a tombstone has ``completed=True``) or ``None``.
    * ``save`` and ``complete`` write at ``version + 1`` only if the store is still at ``version`` (an absent
      instance is at version 0), and return the new version; otherwise they raise
      :class:`~edax.errors.ConcurrencyError` and write nothing. A completed instance takes no write at all.
    * The step's ``commands`` are written in the same write as the state, so they commit or roll back
      together. Each is the kernel's :class:`~edax.aggregate.CommandMessage` for that write, with the id
      ``saga_command_id(id, version + 1, index)`` and the instance id as its ``aggregate_id``; a store may
      key its outbox on the id.
    * ``save`` keeps ``folded`` as it is, and ``load`` returns it unchanged as
      :attr:`SagaInstance.folded`: the runner skips an event it already folded by it. A store that drops
      it repeats a step on every redelivery.
    * ``complete`` keeps the row as a tombstone at its new version rather than deleting it, so a stale
      writer conflicts and a late event cannot start the workflow again. A durable store purges tombstones by
      retention, longer than any redelivery window.
    * ``unpublished`` returns committed unsent commands, oldest commit first; ``mark_published`` prunes
      them by id, idempotently. Both run outside any ``tx``, as :class:`~edax.store.Relay` calls them.
    * With ``tx`` given, or :func:`~edax.store.current_tx` set, a durable backend enlists the write in that
      unit of work.

    A saga whose ``instance_id`` is the aggregate id has no concurrent writer, because the dispatcher
    serializes same-``aggregate_id`` events; the version is what lets a cross-aggregate correlation catch
    one. ``SagaStoreContract`` in :mod:`edax.testing` holds a backend to every clause."""

    async def load(self, id: str, tx: Tx | None = None) -> SagaInstance | None: ...
    async def save(
        self,
        id: str,
        state: Any,
        version: int,
        folded: str,
        commands: Sequence[CommandMessage],
        tx: Tx | None = None,
    ) -> int: ...
    async def complete(
        self, id: str, version: int, commands: Sequence[CommandMessage], tx: Tx | None = None
    ) -> int: ...
    async def unpublished(self, limit: int = 100) -> list[CommandMessage]: ...
    async def mark_published(self, ids: Sequence[str]) -> None: ...


class SagaRunner:
    """Drives a :class:`Saga` over committed events: route to an instance, fold, and persist the step,
    state and commands in one write at the loaded version. Pass :meth:`on_event` to an ``EventRouter`` or
    ``KeyOrderedDispatcher``, and send the commands with :meth:`relay`. ``commands`` marshals the saga's
    command objects into the outbox and back, like an event codec: ``json_codec({"Fulfill": Fulfill})``.
    ``now`` stamps each command's ``issued_at`` (default: the kernel clock).

    Where one store holds every tenant, compose the tenant into the saga's own
    :meth:`Saga.instance_id`, with :func:`~edax.tenancy.tenant_key`: two tenants' instances of one saga
    would otherwise fold into each other. The runner keys by what that returns, so a row's id and the
    instance key stay one string. Every command a step emits inherits the folded event's ``tenant``,
    which is what keeps the claim its bus makes inside the same tenant.

    Every event the runner routes to an instance and does not fold is reported to ``observer``
    (:meth:`~edax.observe.Observer.saga_skipped`)."""

    def __init__(
        self,
        saga: Saga,
        store: SagaStore,
        commands: EventCodec,
        *,
        now: Callable[[], int] | None = None,
        observer: Observer | None = None,
    ) -> None:
        self._saga = saga
        self._store = store
        self._commands = commands
        self._now = now or _edax.current_timestamp
        self._observer = _wrap(observer)

    async def on_event(self, msg: EventMessage, tx: Tx | None = None) -> None:
        """Handle one committed event: load its instance, fold the event, and write the new
        state (or the tombstone) with the commands the fold emitted. ``tx`` goes through as given: the store
        falls back to the ambient :func:`~edax.store.current_tx` itself. A
        :class:`~edax.errors.ConcurrencyError` means another worker advanced the instance first; it is
        transient, so the event is redelivered and folds the winner's state, and nothing this fold decided
        is ever sent. An event the instance already folded (redelivered after its step committed) is
        skipped: no fold, no write, no command. So is an event whose sequence is below the one the instance
        folded from its aggregate: in order delivery folded it before, but a dead-lettered event re-driven
        after a later one lands there too, and never folds. The observer hears every skip.

        Each command is a child of ``msg``: it carries the event's ``correlation_id``, and the event's id
        as its ``causation_id``. The fold runs under the ambient lineage of ``msg`` too."""
        with caused_by(msg):
            instance_id = self._saga.instance_id(msg)
            if instance_id is not None:
                await self._fold(instance_id, msg, tx)

    async def command_failed(self, row: CommandMessage, error: BaseException, tx: Tx | None = None) -> None:
        """The ``on_dead`` sink for this runner's command relay: fold the :data:`SAGA_COMMAND_FAILED` event
        for the dead ``row`` into the instance that sent it, so the saga can compensate. The event's
        payload is :class:`SagaCommandFailed`: the command's id and name, and the failure's code and
        message. Its ``aggregate_id`` is the instance, its ``causation_id`` the command, and it is
        unsequenced; the kernel builds it (``saga_command_failed``).

        The fold skips the event as :meth:`on_event` would: a completed instance folds nothing, so a saga
        that compensates stays open until its commands have run. A sink that runs twice for one row folds
        once, whatever folded between them: the instance keeps every failure it folded. A failed fold
        raises, so the relay hands the row back and the sink runs again."""
        with errors.mapped():
            failed = _edax.saga_command_failed(row, str(error), self._now(), _carried_code(error))
        with caused_by(failed):
            await self._fold(failed.aggregate_id, failed, tx)

    async def _fold(self, instance_id: str, msg: EventMessage, tx: Tx | None) -> None:
        instance = await self._store.load(instance_id, tx)
        completed, folded = (False, "") if instance is None else (instance.completed, instance.folded)
        with errors.mapped():
            admit = _edax.saga_admit(completed, folded, msg)
        if admit.action != "fold":
            # A tombstone, an event folded already, or one below the mark: no step for it. Pinned by
            # `testdata/golden/saga.json`.
            self._observer.saga_skipped(self._saga.name, cast("SagaSkip", admit.action), msg.id)
            return
        new_state, commands = self._saga.handle(None if instance is None else instance.state, msg)
        loaded = None if instance is None else instance.version
        with errors.mapped():
            write = _edax.saga_write(new_state is Completed, bool(commands), loaded)
        if write.action == "nothing":
            return
        rows = self._rows(instance_id, write.version, commands, msg)
        if write.action == "save":
            await self._store.save(instance_id, new_state, write.expected, admit.folded, rows, tx)
        else:
            await self._store.complete(instance_id, write.expected, rows, tx)

    def _rows(
        self, instance_id: str, version: int, commands: Sequence[Any], cause: EventMessage
    ) -> list[CommandMessage]:
        """The outbox rows for the commands a write at ``version`` sends: the kernel's command messages,
        each a child of ``cause``. Pinned by ``testdata/golden/saga.json``."""
        issued_at = self._now()
        payloads = [self._commands.marshal(command) for command in commands]
        saga, cause_id, cause_correlation_id = self._saga.name, cause.id, cause.correlation_id
        with errors.mapped():
            return [
                _edax.saga_command(
                    saga,
                    instance_id,
                    version,
                    index,
                    payload,
                    cause_id,
                    cause_correlation_id,
                    issued_at,
                    cause.tenant,
                )
                for index, payload in enumerate(payloads)
            ]

    def command(self, row: CommandMessage) -> Any:
        """Decode an outbox row back into the command the saga emitted."""
        return self._commands.unmarshal(row)

    def relay(
        self,
        bus: CommandBus,
        *,
        on_dead: RelaySink | None = None,
        max_attempts: int = 0,
        pacing: Pacing | None = None,
        observer: Observer | None = None,
    ) -> Relay:
        """A :class:`~edax.store.Relay` over this runner's command outbox that runs each row on ``bus`` with
        :meth:`~edax.command.CommandBus.dispatch_message`, decoded by this runner's codec. Each row's
        ``idempotency_key`` is its ``saga_command_id`` under the saga's name, so an
        :func:`~edax.command.idempotent` guard on ``bus`` replays a row the relay sends again after a crash
        instead of running its handler twice. The
        handler runs under the ambient lineage :func:`~edax.causation.propagate` of the row, so the events
        it saves stay in the saga's flow. To send the rows to a broker instead, use ``Relay(store,
        producer.publish)`` and run them with ``dispatch_message`` on the consumer. Pass ``on_dead``: a
        terminal command failure then dead-letters the row through it, while without a sink the row is
        handed back on every drain and holds up the rows behind it. A terminal failure saves no event, so
        :meth:`command_failed` is the sink that tells the instance: ``runner.relay(bus,
        on_dead=runner.command_failed)``. See :class:`~edax.store.Relay` for the rest of the policy."""

        async def publish(row: CommandMessage) -> None:
            await bus.dispatch_message(row, self._commands)

        return Relay(
            self._store, publish, observer=observer, on_dead=on_dead, max_attempts=max_attempts, pacing=pacing
        )

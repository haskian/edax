"""Saga / process manager: react to committed events, keep per-instance state, emit commands. Where an
aggregate decides one entity's next state, a saga coordinates a workflow across several. Each event it cares
about is routed to one running instance, folded into that instance's state, and may emit commands.
:class:`SagaRunner` folds an event and persists the step; a :class:`~edax.outbox.Relay` over the store sends
the commands. See ``docs/sdk/sagas.md``.

The runner never sends a command itself. It saves the new state *and* the commands the fold decided in one
versioned write, the saga outbox, and the relay sends them once that write has committed. A fold that loses
the version race sends nothing: its commands roll back with its state, and the redelivery folds the winner's
state. A crash after the commit re-sends from the outbox instead of losing the command, so a command is sent
at least once per committed step, and its consumer deduplicates it by the command's idempotency key,
which is the row's ``saga_command_id``. That id names the saga, so two sagas sharing an instance id never
share a command id.
Completion writes a tombstone: a late or redelivered event finds the instance completed and is ignored, and
a writer that loaded the instance before it completed conflicts. A running instance remembers what it
folded, so an event redelivered after its step committed (a lost ack, a crash before the offset commit) is
skipped instead of folded again under new command ids.

A command that dies in the relay (its handler raises :class:`~edax.errors.Rejected`) saves no event, so
nothing would tell the instance. Wire :meth:`SagaRunner.command_failed` as the relay's ``on_dead``: it folds
a :data:`SAGA_COMMAND_FAILED` event into the instance that sent the command, and the saga compensates."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, Generic, Literal, Protocol, TypeAlias, TypedDict, get_args

from . import _json, _names, _native
from ._observer import wrap
from ._units import transaction_guard
from ._values import give_value_semantics, replace
from .aggregate import PayloadCodec
from .causation import caused_by
from .command import CommandBus
from .delivery import DeadLetter
from .errors import BadHandle, Shredded, code_of
from .message import CommandMessage, EventMessage
from .observe import Observer, SagaSkip
from .outbox import Publish
from .sealing import SealedValue, Sealing, cached_event, original_event
from .unit_of_work import TxT, TxT_contra

__all__ = [
    "SAGA_COMMAND_FAILED",
    "Completed",
    "CompletedInstance",
    "ProtectedSagaState",
    "RunningInstance",
    "Saga",
    "SagaCommandFailed",
    "SagaInstance",
    "SagaOutbox",
    "SagaRunner",
    "SagaStore",
]

#: The name of the event :meth:`SagaRunner.command_failed` folds when a command the saga sent died.
SAGA_COMMAND_FAILED: Final[str] = _native.saga_command_failed_name()

#: The kernel's answers to whether a step folds an event: ``fold``, or one of the skips an observer hears.
_SagaAdmit = Literal["fold", SagaSkip]
_SAGA_ADMITS: tuple[_SagaAdmit, ...] = get_args(_SagaAdmit)
#: The kernel's writes for a step that folded.
_SagaWrite = Literal["nothing", "save", "complete"]
_SAGA_WRITES: tuple[_SagaWrite, ...] = get_args(_SagaWrite)
_names.check(_SAGA_ADMITS, _native.saga_admit_actions(), "saga admit actions")
_names.check(_SAGA_WRITES, _native.saga_write_actions(), "saga write actions")


class SagaCommandFailed(TypedDict):
    """The JSON payload of a :data:`SAGA_COMMAND_FAILED` event: ``json.loads(msg.data)``. ``command_id`` is
    the dead command's outbox id, ``command`` its name, ``code`` the failure's taxonomy code (``"rejected"``
    for a business rejection, ``None`` when no code classifies it), and ``error`` its message.
    Protected commands retain only the stable code: their ``error`` is empty."""

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

# The kernel owns the serialized envelope and its validation. Adapters preserve this record without
# decoding the ciphertext as application JSON.
ProtectedSagaState = _native.ProtectedState
give_value_semantics(ProtectedSagaState)


@dataclass(frozen=True)
class RunningInstance:
    """A stored running saga instance: its accumulated ``state`` and the ``version`` a write is conditional
    on. ``folded`` is the kernel's record of the events the instance folded (``saga_admit``), opaque text the
    store keeps as the last ``save`` wrote it. With sealing, ``state`` is a
    :class:`ProtectedSagaState` until the runner authenticates and opens it for the handler."""

    state: Any
    version: int
    folded: str = ""


@dataclass(frozen=True)
class CompletedInstance:
    """A stored completed saga instance: a tombstone that keeps its ``version``, so no write lands on it
    again. It has no state: the workflow has finished."""

    version: int
    protected: ProtectedSagaState | None = None
    folded: str = ""


#: A stored saga instance, running or completed: the Rust SDK's ``SagaInstance`` enum.
SagaInstance: TypeAlias = RunningInstance | CompletedInstance


class Saga(Protocol):
    """Parameters are positional-only for the same reason as :class:`~edax.aggregate.AggregateCodec`'s: the
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


class SagaStore(Protocol[TxT_contra]):
    """Per-instance state, under optimistic concurrency, and the saga outbox, both keyed by the saga's
    name. What a backend must guarantee:

    * Every row and every outbox command belongs to the saga ``saga`` names: key both by ``(saga, id)``.
      Two sagas over one database, as they must be for their writes to join the events' unit of work,
      then never fold into each other's instance under one instance id, nor send each other's commands.
    * ``load`` returns the stored instance, a :class:`RunningInstance` or the :class:`CompletedInstance`
      tombstone, or ``None``.
    * ``save`` and ``complete`` write at ``version + 1`` only if the store is still at ``version`` (an absent
      instance is at version 0), and return the new version; otherwise they raise
      :class:`~edax.errors.ConcurrencyError` and write nothing. A completed instance takes no write at all.
    * The step's ``commands`` are written in the same write as the state, so they commit or roll back
      together. Each is the kernel's :class:`~edax.message.CommandMessage` for that write, with the id
      ``saga_command_id(saga, id, version + 1, index)`` and the instance id as its ``aggregate_id``; a store
      may key its outbox on the id.
    * ``save`` keeps ``folded`` as it is, and ``load`` returns it unchanged as
      :attr:`RunningInstance.folded`: the runner skips an event it already folded by it. A store that drops
      it repeats a step on every redelivery.
    * ``complete`` keeps the row as a tombstone at its new version rather than deleting it, so a stale
      writer conflicts and a late event cannot start the workflow again. A durable store purges tombstones by
      retention, longer than any redelivery window.
    * ``unpublished`` returns the saga's committed unsent commands, oldest commit first;
      ``mark_published`` prunes them by id, idempotently, and ``unpublished_count`` counts them without
      paging. All three run outside any ``tx``, as a :class:`~edax.outbox.Relay` over :class:`SagaOutbox`
      calls them.
    * With ``tx`` given, or :func:`~edax.unit_of_work.current_tx` set, a durable backend enlists the write in
      that
      unit of work.

    A saga whose ``instance_id`` is the aggregate id has no concurrent writer, because the dispatcher
    serializes same-``aggregate_id`` events; the version is what lets a cross-aggregate correlation catch
    one. ``SagaStoreContract`` in :mod:`edax.testing` holds a backend to every clause."""

    async def load(self, saga: str, id: str, tx: TxT_contra | None = None) -> SagaInstance | None: ...
    async def save(
        self,
        saga: str,
        id: str,
        state: Any,
        version: int,
        folded: str,
        commands: Sequence[CommandMessage],
        tx: TxT_contra | None = None,
    ) -> int: ...
    async def complete(
        self,
        saga: str,
        id: str,
        version: int,
        commands: Sequence[CommandMessage],
        tx: TxT_contra | None = None,
    ) -> int: ...
    async def unpublished(self, saga: str, limit: int) -> list[CommandMessage]: ...
    async def mark_published(self, saga: str, ids: Sequence[str]) -> None: ...
    async def unpublished_count(self, saga: str) -> int: ...


class SagaOutbox:
    """One saga's command outbox over its store: the :class:`~edax.outbox.Outbox` a
    :class:`~edax.outbox.Relay` drains. :meth:`SagaRunner.outbox` builds it, so the relay reads the rows of
    the saga the runner writes and no other."""

    def __init__(self, store: SagaStore[Any], saga: str) -> None:
        self._store = store
        self._saga = saga

    async def unpublished(self, limit: int) -> list[CommandMessage]:
        return await self._store.unpublished(self._saga, limit)

    async def mark_published(self, ids: Sequence[str]) -> None:
        await self._store.mark_published(self._saga, ids)

    async def unpublished_count(self) -> int:
        """How many of the saga's committed commands wait to be sent: a
        :class:`~edax.outbox.CountedOutbox`."""
        return await self._store.unpublished_count(self._saga)


class SagaRunner(Generic[TxT]):
    """Drives a :class:`Saga` over committed events: route to an instance, fold, and persist the step,
    state and commands in one write at the loaded version. Pass :meth:`on_event` to an ``EventRouter`` or
    ``KeyOrderedDispatcher``, and send the commands with a :class:`~edax.outbox.Relay` over :meth:`outbox`
    that publishes through :meth:`publish_to`. ``commands`` marshals the saga's
    command objects into the outbox and back, like an event codec: ``json_codec({"Fulfill": Fulfill})``.
    ``now`` stamps each command's ``issued_at`` (default: the kernel clock).

    ``sealing`` authenticates incoming events and encrypts persisted application state and outgoing
    commands. The first committed state fixes its tenant and subject; conflicting owners are rejected
    before the handler runs. Processing marks remain plaintext and authenticated. Historical plaintext
    state requires the sealing configuration's explicit migration option.

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
        store: SagaStore[TxT],
        commands: PayloadCodec,
        *,
        now: Callable[[], int] | None = None,
        observer: Observer | None = None,
        sealing: Sealing | None = None,
    ) -> None:
        self._saga = saga
        self._store = store
        self._commands = commands
        self._now = now or _native.current_timestamp
        self._observer = wrap(observer)
        self._sealing = sealing

    async def on_event(self, msg: EventMessage, tx: TxT | None = None) -> None:
        """Handle one committed event: load its instance, fold the event, and write the new
        state (or the tombstone) with the commands the fold emitted. ``tx`` goes through as given: the store
        falls back to the ambient :func:`~edax.unit_of_work.current_tx` itself. A
        :class:`~edax.errors.ConcurrencyError` means another worker advanced the instance first; it is
        transient, so the event is redelivered and folds the winner's state, and nothing this fold decided
        is ever sent. An event the instance already folded (redelivered after its step committed) is
        skipped: no fold, no write, no command. So is an event whose sequence is below the one the instance
        folded from its aggregate. Under in-order delivery that event folded before. A dead-lettered event
        re-driven after a later one lands there too, and never folds. The observer hears every skip.

        Each command is a child of ``msg``: it carries the event's ``correlation_id``, and the event's id
        as its ``causation_id``. The fold runs under the ambient lineage of ``msg`` too."""
        raw = original_event(msg)
        sealing = self._sealing
        if sealing is not None:
            data = cached_event(raw, sealing)
            if data is None:
                opened = await sealing.open_event_for_consumer(raw)
                if opened is None:
                    return
                data = opened.data
            _native.protected_scope(sealing.key_namespace, raw.tenant, raw.subject)
            msg = replace(raw, data=data, encryption=None)
        elif raw.encryption is not None:
            raise BadHandle("encrypted saga events require configured sealing")
        async with AsyncExitStack() as resources:
            if sealing is not None:
                try:
                    await resources.enter_async_context(
                        transaction_guard(self._store, sealing.write_guard(raw.tenant, raw.subject), tx)
                    )
                except Shredded:
                    # Only the trusted guard acquisition, or the provider's own status below, can skip an
                    # erasure race. Other exceptions below this boundary propagate unchanged.
                    return
                await resources.enter_async_context(sealing.skip_destroyed(raw.tenant, raw.subject))
            with caused_by(msg):
                instance_id = self._saga.instance_id(msg)
                if instance_id is not None:
                    await self._fold(instance_id, msg, tx, raw=raw)

    async def command_failed(self, dead: DeadLetter[CommandMessage], tx: TxT | None = None) -> None:
        """The ``on_dead`` sink for this runner's command relay: fold the :data:`SAGA_COMMAND_FAILED` event
        for the dead row into the instance that sent it, so the saga can compensate. The event's
        payload is :class:`SagaCommandFailed`: the command's id and name, and the failure's code and
        message. Its ``aggregate_id`` is the instance, its ``causation_id`` the command, and it is
        unsequenced; the kernel builds it (``saga_command_failed``).

        The fold skips the event as :meth:`on_event` would: a completed instance folds nothing, so a saga
        that compensates stays open until its commands have run. A sink that runs twice for one row folds
        once, whatever folded between them: the instance keeps every failure it folded. A failed fold
        raises, so the relay hands the row back and the sink runs again."""
        sealing = self._sealing
        if sealing is not None:
            opened = await sealing.open_command_for_consumer(dead.message)
            if opened is None:
                return
        elif dead.message.encryption is not None:
            raise BadHandle("encrypted saga commands require configured sealing")
        async with AsyncExitStack() as resources:
            if sealing is not None:
                try:
                    await resources.enter_async_context(
                        transaction_guard(
                            self._store, sealing.write_guard(dead.message.tenant, dead.message.subject), tx
                        )
                    )
                except Shredded:
                    return
                await resources.enter_async_context(
                    sealing.skip_destroyed(dead.message.tenant, dead.message.subject)
                )
            failed = _native.saga_command_failed(
                dead.message, str(dead.failure), self._now(), code_of(dead.failure)
            )
            with caused_by(failed):
                await self._fold(failed.aggregate_id, failed, tx, raw=failed)

    async def _fold(self, instance_id: str, msg: EventMessage, tx: TxT | None, *, raw: EventMessage) -> None:
        saga = self._saga.name
        instance = await self._store.load(saga, instance_id, tx)
        # What was loaded crosses flat, and the kernel builds one `Loaded` of it for both calls below.
        state: Any = None
        match instance:
            case None:
                loaded, completed, folded = None, False, ""
            case CompletedInstance(version=version, protected=protected, folded=marks):
                loaded, completed, folded = version, True, marks
                state = protected
            case RunningInstance(state=state, version=version, folded=folded):
                loaded, completed = version, False
            case _:
                _names.unreachable(instance)
        sealing = self._sealing
        if isinstance(state, ProtectedSagaState):
            if sealing is None:
                raise BadHandle("protected saga state requires configured sealing")
            _native.check_sealed_owner(True, state.tenant, raw.tenant)
            _native.check_sealed_owner(True, state.subject, raw.subject)
            completed = state.completed
            state = _json.decode(
                await sealing.open_state(
                    "saga",
                    instance_id,
                    saga,
                    _names.given(loaded, "loaded saga version"),
                    SealedValue(encryption=state.encryption, data=state.data),
                    state.tenant,
                    state.subject,
                    folded,
                    state.completed,
                )
            )
        elif sealing is not None and instance is not None:
            await sealing.check_legacy(raw.tenant, raw.subject)
        if sealing is not None and not completed:
            _native.protected_scope(sealing.key_namespace, raw.tenant, raw.subject)
        admit = _native.saga_admit(saga, completed, folded, raw, loaded)
        # Every kernel decision is named, so a variant this SDK does not know fails loudly instead of
        # landing in a neighbouring arm. Pinned by `testdata/golden/saga.json`.
        action = _names.named(admit.action, _SAGA_ADMITS, "saga admit action")
        match action:
            case "fold":
                pass
            case "completed" | "repeat" | "below_mark":
                # A tombstone, an event folded already, or one below the mark: no step for it.
                self._observer.saga_skipped(saga, action, msg.id)
                return
            case _:
                _names.unreachable(action)
        new_state, commands = self._saga.handle(state, msg)
        write = _native.saga_write(new_state is Completed, bool(commands), completed, loaded)
        written = _names.named(write.action, _SAGA_WRITES, "saga write action")
        match written:
            case "nothing":
                return
            case "save":
                expected, rows = self._written(write, instance_id, commands, msg)
                folded_marks = _names.given(admit.folded, "folded marks")
                if sealing is not None:
                    sealed = await sealing.seal_state(
                        "saga",
                        instance_id,
                        saga,
                        _names.given(write.version, "saga write version"),
                        _json.encode_bytes(new_state),
                        raw.tenant,
                        raw.subject,
                        folded_marks,
                    )
                    new_state = ProtectedSagaState(
                        tenant=raw.tenant,
                        subject=raw.subject,
                        encryption=sealed.encryption,
                        data=sealed.data,
                        completed=False,
                    )
                    rows = [await sealing.seal_command(row) for row in rows]
                await self._store.save(saga, instance_id, new_state, expected, folded_marks, rows, tx)
            case "complete":
                expected, rows = self._written(write, instance_id, commands, msg)
                if sealing is not None:
                    rows = [await sealing.seal_command(row) for row in rows]
                    folded_marks = _names.given(admit.folded, "folded marks")
                    sealed = await sealing.seal_state(
                        "saga",
                        instance_id,
                        saga,
                        _names.given(write.version, "saga write version"),
                        b"null",
                        raw.tenant,
                        raw.subject,
                        folded_marks,
                        True,
                    )
                    tombstone = ProtectedSagaState(
                        tenant=raw.tenant,
                        subject=raw.subject,
                        encryption=sealed.encryption,
                        data=sealed.data,
                        completed=True,
                    )
                    await self._store.save(saga, instance_id, tombstone, expected, folded_marks, rows, tx)
                else:
                    await self._store.complete(saga, instance_id, expected, rows, tx)
            case _:
                _names.unreachable(written)

    def _written(
        self, write: _native.SagaWrite, instance_id: str, commands: Sequence[Any], cause: EventMessage
    ) -> tuple[int, list[CommandMessage]]:
        """What a ``save`` or ``complete`` write passes its store: the version it expects, and the outbox
        rows of the commands it sends at the version it writes."""
        expected = _names.given(write.expected, "expected version")
        version = _names.given(write.version, "version")
        return expected, self._rows(instance_id, version, commands, cause)

    def _rows(
        self, instance_id: str, version: int, commands: Sequence[Any], cause: EventMessage
    ) -> list[CommandMessage]:
        """The outbox rows for the commands a write at ``version`` sends: the kernel's command messages,
        each a child of ``cause``. Pinned by ``testdata/golden/saga.json``."""
        issued_at = self._now()
        payloads = [self._commands.marshal(command) for command in commands]
        saga, cause_id, cause_correlation_id = self._saga.name, cause.id, cause.correlation_id
        return [
            _native.saga_command(
                saga,
                instance_id,
                version,
                index,
                payload,
                cause_id,
                cause_correlation_id,
                issued_at,
                cause.tenant,
                cause.subject,
            )
            for index, payload in enumerate(payloads)
        ]

    def command(self, row: CommandMessage) -> Any:
        """Decode an outbox row back into the command the saga emitted."""
        if row.encryption is not None:
            raise BadHandle("decode encrypted saga commands through a sealing-enabled CommandBus")
        return self._commands.unmarshal(row)

    def outbox(self) -> SagaOutbox:
        """This saga's command outbox: the rows its steps committed, for a :class:`~edax.outbox.Relay`."""
        return SagaOutbox(self._store, self._saga.name)

    def publish_to(self, bus: CommandBus) -> Publish[CommandMessage]:
        """The ``publish`` of a :class:`~edax.outbox.Relay` over :meth:`outbox` that runs each row on ``bus``
        with :meth:`~edax.command.CommandBus.dispatch_message`, decoded by the bus's codec:
        ``Relay(runner.outbox(), runner.publish_to(bus), on_dead=runner.command_failed)``. A bus built with no
        codec raises the terminal :class:`~edax.errors.BadHandle` here, at wiring. Each row's
        ``idempotency_key`` is its ``saga_command_id``, which names the saga, so an
        :func:`~edax.command.idempotent` guard on ``bus`` replays a row the relay sends again after a crash
        instead of running its handler twice. The handler runs under the ambient lineage
        :func:`~edax.causation.propagate` of the row, so the events it saves stay in the saga's flow. To send
        the rows to a broker instead, use ``Relay(runner.outbox(), producer.publish)`` and run them with
        ``dispatch_message`` on the consumer. Pass the relay an ``on_dead``: a terminal command failure then
        dead-letters the row through it, while without a sink the row is handed back on every drain and
        holds up its instance's rows behind it. A terminal failure saves no event, so :meth:`command_failed`
        is the sink that tells the instance."""
        if bus.codec is None:
            raise BadHandle(
                "publish_to runs each row on the bus's codec: build it with CommandBus(codec=...)"
            )

        async def publish(row: CommandMessage) -> None:
            await bus.deliver_message(row)

        return publish

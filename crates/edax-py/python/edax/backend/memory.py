"""In-memory backends for every seam: dev and tests, single process, not durable.

:class:`MemoryTransactor` is the transaction boundary: one native database holding every seam, whose
stores are Python views over it. A store built with no transactor gets a database of its own; stores built
over one transactor share it, so their writes under one ``tx`` commit or roll back together. The maps, the
concurrency checks, the outbox, the atomic claim, the journal and the broker's settlement accounting live in
the native ``edax-memory`` crate, shared by every SDK; Python holds only the transaction's token. The
native objects are bound to the thread that built them: a call from another thread raises the terminal
:class:`~edax.errors.BadHandle`."""

from __future__ import annotations

import asyncio
import logging
import weakref
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, Protocol, TypeVar, cast

from .. import _json, _native
from .._owned import ThreadBound
from .._units import enlisted
from ..broker import consume
from ..delivery import Delivery
from ..dispatcher import KeyOrderedDispatcher
from ..errors import BadHandle
from ..idempotency import Claimed, Record, decode_claim, encode_record
from ..message import CommandMessage, EventDraft, EventMessage, message_kind
from ..projection import Entity
from ..query import Page, Query
from ..saga import CompletedInstance, ProtectedSagaState, RunningInstance, SagaInstance
from ..store import Snapshot
from ..tenancy import Tenancy
from ..unit_of_work import Tx

__all__ = [
    "MemoryBroker",
    "MemoryCommandOutbox",
    "MemoryEventStore",
    "MemoryIdempotencyStore",
    "MemoryLedger",
    "MemoryProjectionStore",
    "MemorySagaStore",
    "MemoryTransactor",
    "MemoryTx",
]

logger = logging.getLogger("edax.backend.memory")

#: The row an outbox view reads: an event or a command.
R = TypeVar("R", EventMessage, CommandMessage)

# The token that names no transaction: a write under it lands at once, a read under it sees
# committed state only. Read from the native binding, not restated, so the value has one owner.
_NO_TX: int = _native.no_tx()


@dataclass(frozen=True)
class MemoryTx:
    """The handle :meth:`MemoryTransactor.begin` yields: the transactor it belongs to and the
    native token of the open transaction. Every memory store over that transactor accepts it."""

    transactor: MemoryTransactor
    token: int


class MemoryTransactor:
    """:class:`~edax.unit_of_work.Transactor` over the native in-memory database. ``async with transaction(t)
    as tx`` opens a unit of work: every store built over this transactor stages its writes under ``tx``, reads
    its own staged writes back, and nobody else sees them until the block exits cleanly. An exception rolls
    them all back. Isolation is by exclusion, not by blocking: a key one open transaction wrote is held until
    it ends, and a second writer gets :class:`~edax.errors.ConcurrencyError` at once, because an in-memory
    store driven from one event loop cannot park a task on a row lock. An idempotency claim on a held key
    loses and reads the holder's record as pending."""

    def __init__(self) -> None:
        self._native = ThreadBound(_native.memory_transactor(), "MemoryTransactor")

    def _database(self) -> _native.MemoryTransactor:
        """The native database, on the thread that built it: what this module's store views call."""
        return self._native.get()

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[MemoryTx]:
        token = self._database().begin()
        try:
            yield MemoryTx(self, token)
        except BaseException:
            # A cancelled task (a wait_for timeout, shutdown) is a rollback too: nothing of the
            # unit of work may land. A false return means the token was already closed, which
            # only a handle kept past its block could cause; there is nothing left to drop.
            self._database().rollback(token)
            raise
        self._database().commit(token)

    def open_count(self) -> int:
        """How many transactions are open on this database. A leak diagnostic: a token that is
        neither committed nor rolled back holds every key it staged as a conflict for the process
        lifetime. A count that never returns to its idle level names a unit of work that escaped its
        ``async with``."""
        return self._database().open_count()

    def auto_commit(self) -> MemoryTx:
        """An explicit no-transaction handle: a call made with it lands at once and sees committed
        state only, whatever the ambient :func:`~edax.unit_of_work.current_tx` is. For a reader that must
        look past an open unit of work, such as a test probing what has landed."""
        return MemoryTx(self, _NO_TX)

    def token(self, tx: Tx | None) -> int:
        """The native token for ``tx``, else for the ambient :func:`~edax.unit_of_work.current_tx` of this
        database, else the auto-commit token. A handle of another transactor is refused: a store cannot
        enlist in a unit of work its database knows nothing about, and going on would silently drop the
        atomicity the caller asked for."""
        # The Rust store refuses an unknown token with the same terminal code (`edax_memory::unknown_tx`).
        handle = enlisted(self, tx, MemoryTx, "MemoryTransactor")
        return _NO_TX if handle is None else handle.token


class _StoreView:
    """What the store views share: the database they enlist with, given or of their own."""

    def __init__(self, transactor: MemoryTransactor | None = None) -> None:
        self._transactor = transactor if transactor is not None else MemoryTransactor()

    @property
    def transactor(self) -> MemoryTransactor:
        """The transactor this store enlists with; build the other stores over it to share it."""
        return self._transactor

    def _db(self) -> _native.MemoryTransactor:
        """The native database, on the thread that built it."""
        return self._transactor._database()

    def _token(self, tx: Tx | None) -> int:
        """The native token a call under ``tx`` runs with: see :meth:`MemoryTransactor.token`."""
        return self._transactor.token(tx)


class _NativeOutbox(Protocol[R]):
    """The outbox read side a native seam has: the event store's, and the command outbox's."""

    def unpublished(self, limit: int) -> list[R]: ...
    def mark_published(self, ids: list[str]) -> None: ...
    def unpublished_count(self) -> int: ...


class _OutboxView(_StoreView, Generic[R]):
    """The :class:`~edax.outbox.Outbox` read side over a native seam, which the event store and the command
    outbox share."""

    def _seam(self) -> _NativeOutbox[R]:
        raise NotImplementedError

    async def unpublished(self, limit: int) -> list[R]:
        # A negative limit cannot cross the unsigned boundary: CodecError.
        return self._seam().unpublished(limit)

    async def mark_published(self, ids: Sequence[str]) -> None:
        self._seam().mark_published(list(ids))

    async def unpublished_count(self) -> int:
        """How many committed unmarked outbox rows are waiting, counted without paging them."""
        return self._seam().unpublished_count()


class MemoryEventStore(_OutboxView[EventMessage]):
    """:class:`~edax.store.EventStore` over the native store: events + snapshots + outbox. Drafts go in as
    the native :class:`~edax.message.EventDraft` and come back as :class:`~edax.message.EventMessage`.
    ``now`` is the store's clock: ``append`` stamps every draft's ``recorded_at`` with one reading of it
    (default: the kernel clock, Unix nanoseconds). An ``EventStore`` is a
    :class:`~edax.unit_of_work.Transactor`: ``begin()`` is the :class:`MemoryTransactor`'s, so ``async with
    transaction(store)`` opens the database's unit of work. The outbox is read by the relay outside any
    transaction and holds committed rows only."""

    def __init__(
        self, transactor: MemoryTransactor | None = None, *, now: Callable[[], int] | None = None
    ) -> None:
        super().__init__(transactor)
        # The native store cannot call back into Python, so the view reads the clock and hands it in.
        self._now = now or _native.current_timestamp

    def _seam(self) -> _native.MemoryEventStore:
        """The native event store of this view's database, on the thread that built it."""
        return _native.handle(self._db().event_store())

    def begin(self) -> AbstractAsyncContextManager[MemoryTx]:
        return self._transactor.begin()

    async def load(self, tx: Tx, aggregate_id: str, after: int = 0) -> list[EventMessage]:
        return self._seam().load(self._token(tx), aggregate_id, after)

    async def load_snapshot(self, tx: Tx, aggregate_id: str) -> Snapshot | None:
        r = self._seam().load_snapshot(self._token(tx), aggregate_id)
        return (
            None
            if r is None
            else Snapshot(aggregate_id, r.type_name, r.version, r.data, r.tenant, r.subject, r.encryption)
        )

    async def save_snapshot(self, tx: Tx, snapshot: Snapshot) -> None:
        self._seam().save_snapshot_record(
            self._token(tx),
            snapshot.aggregate_id,
            _native.SnapshotResult(
                type_name=snapshot.type_name,
                version=snapshot.version,
                data=snapshot.data,
                tenant=snapshot.tenant,
                subject=snapshot.subject,
                encryption=snapshot.encryption,
            ),
        )

    async def append(
        self, tx: Tx, aggregate_id: str, expected_version: int, drafts: Sequence[EventDraft]
    ) -> list[EventMessage]:
        recorded_at = self._now()
        # Native `concurrency:` is ConcurrencyError, anything else StoreError.
        return self._seam().append(self._token(tx), aggregate_id, expected_version, list(drafts), recorded_at)


class MemoryIdempotencyStore(_StoreView):
    """:class:`~edax.idempotency.IdempotencyStore` over the native atomic claim, sharing the transactor's
    database. Records cross as JSON. With ``tx`` given, or :func:`~edax.unit_of_work.current_tx` of its
    transactor set, the claim and the terminal record are staged in that unit of work: a rollback releases the
    key, so the next dispatch runs the handler again instead of replaying an outcome that never landed. For
    the ordering of a ledger on its own database (Redis), use :class:`MemoryLedger`.

    ``complete`` and ``release`` are fenced on the handle ``claim`` returned, as the Redis ledger fences on
    the claim value, and :meth:`expire` lapses a key the way a claim TTL does: a test rehearses a takeover,
    and a stale owner's late ``complete`` is refused with :class:`~edax.errors.StoreError`."""

    # `token` refuses a handle of another transactor, which is the refusal the contract requires of
    # a store that declares this: the guard skips its compensation here, so silently auto-committing
    # a foreign unit of work would leave a `done` record for writes that rolled back.
    joins_transaction: ClassVar[bool] = True

    def _seam(self) -> _native.MemoryIdempotencyStore:
        """The native idempotency store of this view's database, on the thread that built it."""
        return _native.handle(self._db().idempotency_store())

    async def claim(self, key: str, payload_hash: int, tx: Tx | None = None) -> Claimed:
        claimed = self._seam().claim(self._token(tx), key, payload_hash)
        return decode_claim(claimed, payload_hash)

    async def complete(self, key: str, handle: bytes, record: Record, tx: Tx | None = None) -> None:
        # A handle that names no claim on `key` any more: the fence's StoreError.
        self._seam().complete(self._token(tx), key, handle, encode_record(record))

    async def release(self, key: str, handle: bytes, tx: Tx | None = None) -> None:
        self._seam().release(self._token(tx), key, handle)

    async def forget(self, key: str, tx: Tx | None = None) -> None:
        # The native store reads the record with the kernel: a pending claim, or a key another open unit
        # of work holds, is `in_progress:`, raised as InProgress.
        self._seam().forget(self._token(tx), key)

    def expire(self, key: str) -> bool:
        """Lapse whatever ``key`` holds in committed state, as a claim or record TTL running out would: the
        next claim wins, and the old claim's handle settles nothing. A test hook; ``False`` when nothing was
        held. A claim staged by an open unit of work is not committed, so it does not lapse."""
        return self._seam().expire(key)

    def put_raw(self, key: str, raw: str) -> None:
        """Store ``raw`` as ``key``'s settled record in committed state, readable or not. A test hook that
        rehearses a record another writer left behind (a torn write, another layout, a hand edit); the next
        claim reads it as the kernel does. A key an open unit of work holds raises the transient
        :class:`~edax.errors.ConcurrencyError` and is left alone."""
        self._seam().put_raw(key, raw)


class MemoryLedger:
    """:class:`~edax.idempotency.IdempotencyStore` on a database of its own: the ordering a Redis ledger has.
    It takes ``tx`` and ignores it, every call lands at once, and it declares ``joins_transaction`` false, so
    the guard runs ``claim; begin; run; commit; complete`` with hand compensation, caches a terminal
    failure (a redelivery raises :class:`~edax.errors.CachedFailure`), and can leave a claim over work that
    landed when the record's reply is lost. :meth:`expire` lapses a claim as the Redis claim TTL does, so a
    takeover and a stale owner's refused late ``complete`` can be rehearsed. Test on the ordering you
    deploy: this one for a Redis ledger, :class:`MemoryIdempotencyStore` for a ledger in your database.

    ``store`` is the claim store the ledger runs outside any ``tx`` (default: one on a new database). It
    has no transactor of its own to share, so no store can join its writes."""

    joins_transaction: ClassVar[bool] = False

    def __init__(self, store: MemoryIdempotencyStore | None = None) -> None:
        self._store = store if store is not None else MemoryIdempotencyStore()
        # Every call runs under the store's no-transaction handle, whatever `tx` or ambient tx is given.
        self._no_tx = self._store.transactor.auto_commit()

    async def claim(self, key: str, payload_hash: int, tx: Tx | None = None) -> Claimed:
        return await self._store.claim(key, payload_hash, self._no_tx)

    async def complete(self, key: str, handle: bytes, record: Record, tx: Tx | None = None) -> None:
        await self._store.complete(key, handle, record, self._no_tx)

    async def release(self, key: str, handle: bytes, tx: Tx | None = None) -> None:
        await self._store.release(key, handle, self._no_tx)

    async def forget(self, key: str, tx: Tx | None = None) -> None:
        await self._store.forget(key, self._no_tx)

    def expire(self, key: str) -> bool:
        """:meth:`MemoryIdempotencyStore.expire`: lapse ``key`` as the Redis claim TTL running out does."""
        return self._store.expire(key)

    def put_raw(self, key: str, raw: str) -> None:
        """:meth:`MemoryIdempotencyStore.put_raw`: store ``raw`` as ``key``'s settled record, unchecked."""
        self._store.put_raw(key, raw)


class MemoryProjectionStore(_StoreView):
    """:class:`~edax.projection.ProjectionQuery` over the native store; ``get_many`` runs the kernel query,
    so it is the reference for how a durable backend must page. Documents cross as JSON, and every read
    carries its row key as ``"id"``. A save under ``tx`` (or the ambient
    :func:`~edax.unit_of_work.current_tx`) is staged until the transactor commits."""

    def _seam(self) -> _native.MemoryProjectionStore:
        """The native projection store of this view's database, on the thread that built it."""
        return _native.handle(self._db().projection_store())

    async def get(self, id: str, tx: Tx | None = None) -> Entity | None:
        row = self._seam().get(self._token(tx), id)
        return None if row is None else Entity(id, row.version, _json.decode(row.document_json))

    async def save(self, entity: Entity, tx: Tx | None = None) -> int:
        # Native `concurrency:` is ConcurrencyError, anything else StoreError.
        return self._seam().save(self._token(tx), entity.id, entity.version, _json.encode(entity.document))

    async def get_many(self, query: Query = Query(), *, tx: Tx | None = None) -> Page:
        # A malformed spec/cursor is tagged `codec:`: terminal.
        page = self._seam().get_many(self._token(tx), query.to_json())
        return Page.from_kernel(page)


class MemorySagaStore(_StoreView):
    """:class:`~edax.saga.SagaStore` over the native store, so the saga rule (a stale ``save`` or
    ``complete`` raises :class:`~edax.errors.ConcurrencyError`, a completed instance takes no write, the
    step's commands commit with its state, each saga's rows and commands are its own) is the one every SDK
    runs. State crosses as JSON, so it must be JSON-serializable and comes back as a copy, never the object
    that was saved; commands cross as the native :class:`~edax.message.CommandMessage`. A write under
    ``tx`` (or the ambient :func:`~edax.unit_of_work.current_tx`) is staged until the transactor commits. Not
    durable."""

    def _seam(self) -> _native.MemorySagaStore:
        """The native saga store of this view's database, on the thread that built it."""
        return _native.handle(self._db().saga_store())

    async def load(self, saga: str, id: str, tx: Tx | None = None) -> SagaInstance | None:
        row = self._seam().load(self._token(tx), saga, id)
        if row is None:
            return None
        if row.completed:
            return CompletedInstance(row.version)
        protected = _native.decode_protected_state(row.state_json).value
        if protected is not None and protected.completed:
            return CompletedInstance(row.version, protected, row.folded)
        state = protected if protected is not None else _json.decode(row.state_json)
        return RunningInstance(state, row.version, row.folded)

    async def save(
        self,
        saga: str,
        id: str,
        state: Any,
        version: int,
        folded: str,
        commands: Sequence[CommandMessage],
        tx: Tx | None = None,
    ) -> int:
        # Native `concurrency:` is ConcurrencyError, anything else StoreError.
        encoded = (
            _native.encode_protected_state(state)
            if isinstance(state, ProtectedSagaState)
            else _json.encode(state)
        )
        return self._seam().save(self._token(tx), saga, id, version, encoded, folded, list(commands))

    async def complete(
        self, saga: str, id: str, version: int, commands: Sequence[CommandMessage], tx: Tx | None = None
    ) -> int:
        return self._seam().complete(self._token(tx), saga, id, version, list(commands))

    async def unpublished(self, saga: str, limit: int) -> list[CommandMessage]:
        # A negative limit cannot cross the unsigned boundary: CodecError.
        return self._seam().unpublished(saga, limit)

    async def mark_published(self, saga: str, ids: Sequence[str]) -> None:
        self._seam().mark_published(saga, list(ids))

    async def unpublished_count(self, saga: str) -> int:
        """How many of ``saga``'s committed unsent commands are waiting, counted without paging them."""
        return self._seam().unpublished_count(saga)


class MemoryCommandOutbox(_OutboxView[CommandMessage]):
    """A command outbox over the native store: the :class:`~edax.outbox.CommandOutbox` a
    :class:`~edax.repository.Repository` writes, and the :class:`~edax.outbox.Outbox` a
    :class:`~edax.outbox.Relay` drains. Build it over the event store's transactor, so ``enqueue`` under the
    save's ``tx`` commits or rolls back with the events. ``unpublished`` returns committed rows only, oldest
    first. A ``tx`` of another transactor raises :class:`~edax.errors.BadHandle`. Not durable."""

    def _seam(self) -> _native.MemoryCommandOutbox:
        """The native command outbox of this view's database, on the thread that built it."""
        return _native.handle(self._db().command_outbox())

    async def enqueue(self, tx: Tx, commands: Sequence[CommandMessage]) -> None:
        self._seam().enqueue(self._token(tx), list(commands))


class _Log:
    """The native log every view of one broker shares, and the wakeup of every view parked on it: a
    message, a rewind or a crash in one group can make a message fetchable for any view."""

    def __init__(self, partitions: int, tenancy: Tenancy) -> None:
        # Zero partitions is the kernel's `codec:` refusal.
        self.native = ThreadBound(_native.memory_broker(partitions, tenancy.value), "MemoryBroker")
        # Weak, so a group view nobody holds any more drops its wakeup instead of keeping it forever.
        self.wakeups: weakref.WeakSet[asyncio.Event] = weakref.WeakSet()
        # Strong, while a poll waits on it: the parked task is reachable only through its event's waiter,
        # so a run task the host kept no reference to would otherwise be collected mid-wait.
        self.parked: set[asyncio.Event] = set()

    def wake(self) -> None:
        for event in self.wakeups:
            event.set()


class MemoryBroker:
    """Both :class:`~edax.broker.Producer` and :class:`~edax.broker.Consumer` over the native log, so
    ``Relay(store, broker.publish)`` and ``broker.run(dispatcher)`` talk without a broker. ``publish``
    takes an event or a command and returns once it is in the log, keyed as
    :class:`~edax.backend.kafka.KafkaProducer` keys it under ``tenancy``: the kernel's ordering key, composed
    with the message's tenant where the mode composes keys. A pooled message with no tenant raises
    :class:`~edax.errors.CodecError`, as on Kafka. Nothing survives the process, and nothing is ever
    deleted.

    It behaves like a Kafka topic, so a handler that only survives a queue fails here first:

    * a nack rewinds the partition: the nacked message and every later one, acked or not, come back in
      order, and the deliveries of it already handed out are retired (``Delivery.live``), so the
      dispatcher skips them;
    * :meth:`crash` drops this group's in-flight work and redelivers everything above its committed
      watermark, the way a consumer restart or a rebalance does;
    * :meth:`group` reads the same log as another consumer group, which gets every message: a projection
      and a saga each get their own;
    * with ``partitions`` above 1, order holds per key, not across keys, and a key lands on the partition
      Kafka gives it, so two tenants' same-named aggregates under a pool take Kafka's partitions too.

    The log, the settlement and the commits are the native machine's, shared with every SDK, so
    :meth:`empty` reports the same "nothing left to do" a durable broker does. Only the waiting is
    Python's, because the native poll never blocks: :meth:`poll` loops over it and parks on an
    :class:`asyncio.Event` between empty peeks."""

    def __init__(
        self,
        partitions: int = 1,
        *,
        tenancy: Tenancy = Tenancy.SILO,
        _view_of: tuple[_Log, str] | None = None,
    ) -> None:
        # `_view_of` is :meth:`group`'s: the shared log and the group a view reads it as.
        self._log, self._group = _view_of if _view_of is not None else (_Log(partitions, tenancy), "default")
        self._queued = asyncio.Event()
        self._log.wakeups.add(self._queued)

    def group(self, name: str) -> MemoryBroker:
        """The same log, read as consumer group ``name``: it gets every message, settles on its own, and
        starts at the beginning of the log."""
        return MemoryBroker(_view_of=(self._log, name))

    async def publish(self, msg: EventMessage | CommandMessage) -> None:
        native = self._log.native.get()
        # `message_kind` refuses a non-message with TypeError, so each cast names the type it checked. A
        # pooled message with no tenant is the key rule's `codec:` refusal.
        if message_kind(msg) == "event":
            native.publish_event(cast(EventMessage, msg))
        else:
            native.publish_command(cast(CommandMessage, msg))
        self._log.wake()

    async def run(self, dispatcher: KeyOrderedDispatcher[Any]) -> None:
        """:func:`~edax.broker.consume` this broker into ``dispatcher``: it has no work of its own around the
        loop."""
        await consume(self, dispatcher)

    async def poll(self) -> Delivery[EventMessage | CommandMessage] | None:
        while True:
            # Clear before the peek, never after: a publish that lands after the peek sets the
            # flag, and clearing then would erase the wakeup the peek missed.
            self._queued.clear()
            polled = self._log.native.get().try_poll(self._group)
            if polled is not None:
                # The native poll fills exactly one of the two.
                msg: EventMessage | CommandMessage | None = polled.event or polled.command
                if msg is None:
                    raise BadHandle(f"memory broker poll {polled.token} carried no message")
                return self._delivery(polled.token, msg)
            self._log.parked.add(self._queued)
            try:
                await self._queued.wait()
            finally:
                self._log.parked.discard(self._queued)

    def _delivery(
        self, token: int, msg: EventMessage | CommandMessage
    ) -> Delivery[EventMessage | CommandMessage]:
        native, group = self._log.native, self._group

        # The machine refuses a second settlement of a token, and one a rewind or a crash retired, and
        # says so with `False`. The refusal is logged at debug, not raised: nothing is lost either way,
        # and rewinds and rebalances make it routine. The Kafka consumer logs it alike.
        async def ack() -> None:
            if not native.get().ack(group, token):
                logger.debug("edax memory broker: ack of a settled or retired delivery %d", token)

        async def nack() -> None:
            if not native.get().nack(group, token):
                logger.debug("edax memory broker: nack of a settled or retired delivery %d", token)
                return
            self._log.wake()  # the rewind made messages fetchable for a parked poller

        def live() -> bool:
            return native.get().is_live(group, token)

        return Delivery(msg, ack, nack, live=live)

    def crash(self) -> None:
        """Simulate the death of this group's consumer, or a rebalance: every delivery it holds is retired,
        and every message above its committed watermark is fetched again."""
        self._log.native.get().crash(self._group)
        self._log.wake()

    def empty(self) -> bool:
        """True only when this group has committed past every message: nothing queued, nothing in flight."""
        return self._log.native.get().is_empty(self._group)

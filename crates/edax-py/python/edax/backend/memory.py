"""In-memory backends for every seam: dev and tests, single process, not durable.

:class:`MemoryTransactor` is the transaction boundary: one native database holding every seam, whose four
stores are Python views over it. A store built with no transactor gets a database of its own; stores built
over one transactor share it, so their writes under one ``tx`` commit or roll back together. The maps, the
concurrency checks, the outbox, the atomic claim, the journal and the broker's settlement accounting live in
the native ``edax-memory`` crate, shared by every SDK; Python holds only the transaction's token. The
native objects are bound to the thread that built them: a call from another thread raises the terminal
:class:`~edax.errors.BadHandle`."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, ClassVar

from .. import _edax, _json, errors
from .._owned import ThreadBound
from ..aggregate import EventEnvelope
from ..broker import _ConsumerLoop
from ..command import Claimed, Record, Won, decode_record, encode_record
from ..dispatcher import Delivery
from ..errors import BadHandle
from ..filter import Filter
from ..projection import Entity
from ..query import Page, SortKey, build_spec
from ..saga import SagaCommand, SagaInstance
from ..store import Snapshot, Tx, current_tx

__all__ = [
    "MemoryBroker",
    "MemoryEventStore",
    "MemoryIdempotencyStore",
    "MemoryLedger",
    "MemoryProjectionStore",
    "MemorySagaStore",
    "MemoryTransactor",
    "MemoryTx",
]

logger = logging.getLogger("edax.backend.memory")

# The token that names no transaction: a write under it lands at once, a read under it sees
# committed state only. Read from the native binding, not restated, so the value has one owner.
_NO_TX: int = _edax.no_tx()


@dataclass(frozen=True)
class MemoryTx:
    """The handle :meth:`MemoryTransactor.transaction` yields: the transactor it belongs to and the
    native token of the open transaction. Every memory store over that transactor accepts it."""

    transactor: MemoryTransactor
    token: int


class MemoryTransactor:
    """:class:`~edax.store.Transactor` over the native in-memory database. ``async with transaction(t) as tx``
    opens a unit of work: every store built over this transactor stages its writes under ``tx``, reads its
    own staged writes back, and nobody else sees them until the block exits cleanly. An exception rolls them
    all back. Isolation is by exclusion, not by blocking: a key one open transaction wrote is held until it
    ends, and a second writer gets :class:`~edax.errors.ConcurrencyError` at once, because an in-memory
    store driven from one event loop cannot park a task on a row lock. An idempotency claim on a held key
    loses and reads the holder's record as pending."""

    def __init__(self) -> None:
        self._native = ThreadBound(_edax.memory_transactor(), "MemoryTransactor")

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[MemoryTx]:
        token = self._native.get().begin()
        try:
            yield MemoryTx(self, token)
        except BaseException:
            # A cancelled task (a wait_for timeout, shutdown) is a rollback too: nothing of the
            # unit of work may land. A false return means the token was already closed, which
            # only a handle kept past its block could cause; there is nothing left to drop.
            self._native.get().rollback(token)
            raise
        with errors.mapped():
            self._native.get().commit(token)

    def open_count(self) -> int:
        """How many transactions are open on this database. A leak diagnostic: a token that is
        neither committed nor rolled back is not merely a held key, it is a permanent conflict on
        every key it staged, for the process lifetime. A count that never returns to its idle level
        names a unit of work that escaped its ``async with``."""
        return self._native.get().open_count()

    def auto_commit(self) -> MemoryTx:
        """An explicit no-transaction handle: a call made with it lands at once and sees committed
        state only, whatever the ambient :func:`~edax.store.current_tx` is. For a reader that must
        look past an open unit of work, such as a test probing what has landed."""
        return MemoryTx(self, _NO_TX)

    def token(self, tx: Tx | None) -> int:
        """The native token for ``tx``, else for the ambient :func:`~edax.store.current_tx` of this
        database, else the auto-commit token. A handle of another transactor is refused: a store cannot
        enlist in a unit of work its database knows nothing about, and going on would silently drop the
        atomicity the caller asked for."""
        if tx is None:
            tx = current_tx(self)
        if tx is None:
            return _NO_TX
        if isinstance(tx, MemoryTx) and tx.transactor is self:
            return tx.token
        # `BadHandle`, not `StoreError`: the same handle fails the same way on every retry, so a
        # transient verdict would have a host redeliver a wiring mistake forever. The Rust store
        # refuses an unknown token with the same terminal code (`edax_memory::unknown_tx`).
        raise BadHandle(
            f"tx {tx!r} is not a transaction of this MemoryTransactor: build every memory store "
            "that must commit together over one MemoryTransactor"
        )


class _StoreView:
    """What the four store views share: the database they enlist with, given or of their own."""

    def __init__(self, transactor: MemoryTransactor | None = None) -> None:
        self._transactor = transactor if transactor is not None else MemoryTransactor()

    @property
    def transactor(self) -> MemoryTransactor:
        """The transactor this store enlists with; build the other stores over it to share it."""
        return self._transactor


class MemoryEventStore(_StoreView):
    """:class:`~edax.store.EventStore` over the native store: events + snapshots + outbox. Events
    cross as the native :class:`~edax.aggregate.EventEnvelope` in both directions. An
    ``EventStore`` is a :class:`~edax.store.Transactor`: ``transaction()`` is the
    :class:`MemoryTransactor`'s, so ``async with transaction(store)`` opens the database's unit of
    work. The outbox is read by the relay outside any transaction and holds committed rows only."""

    def transaction(self) -> AbstractAsyncContextManager[MemoryTx]:
        return self._transactor.transaction()

    async def load(self, tx: Tx, aggregate_id: str, after: int = 0) -> list[EventEnvelope]:
        with errors.mapped():
            return self._transactor._native.get().load_events(self._transactor.token(tx), aggregate_id, after)

    async def load_snapshot(self, tx: Tx, aggregate_id: str) -> Snapshot | None:
        with errors.mapped():
            r = self._transactor._native.get().load_snapshot(self._transactor.token(tx), aggregate_id)
        return None if r is None else Snapshot(aggregate_id, r.type_name, r.version, r.data)

    async def save_snapshot(self, tx: Tx, snapshot: Snapshot) -> None:
        with errors.mapped():
            self._transactor._native.get().save_snapshot(
                self._transactor.token(tx),
                snapshot.aggregate_id,
                snapshot.type_name,
                snapshot.version,
                snapshot.data,
            )

    async def append(
        self, tx: Tx, aggregate_id: str, expected_version: int, events: Sequence[EventEnvelope]
    ) -> None:
        with errors.mapped():  # native `concurrency:` -> ConcurrencyError, else StoreError
            self._transactor._native.get().append_events(
                self._transactor.token(tx), aggregate_id, expected_version, list(events)
            )

    async def unpublished(self, limit: int = 100) -> list[EventEnvelope]:
        with errors.mapped():  # a negative limit cannot cross the unsigned boundary: CodecError
            return self._transactor._native.get().unpublished(limit)

    async def mark_published(self, ids: Sequence[str]) -> None:
        with errors.mapped():
            self._transactor._native.get().mark_published(list(ids))

    async def unpublished_count(self) -> int:
        """How many committed unmarked outbox rows are waiting, counted without paging them."""
        return self._transactor._native.get().unpublished_count()


class MemoryIdempotencyStore(_StoreView):
    """:class:`~edax.command.IdempotencyStore` over the native atomic claim, sharing the transactor's
    database. Records cross as JSON. With ``tx`` given, or :func:`~edax.store.current_tx` of its transactor
    set, the claim and the terminal record are staged in that unit of work: a rollback releases the key, so
    the next dispatch runs the handler again instead of replaying an outcome that never landed. For the
    ordering of a ledger on its own database (Redis), use :class:`MemoryLedger`.

    ``complete`` and ``release`` are fenced on the handle ``claim`` returned, as the Redis ledger fences on
    the claim value, and :meth:`expire` lapses a key the way a claim TTL does: a test rehearses a takeover,
    and a stale owner's late ``complete`` is refused with :class:`~edax.errors.StoreError`."""

    # `token` refuses a handle of another transactor, which is the refusal the contract requires of
    # a store that declares this: the guard skips its compensation here, so silently auto-committing
    # a foreign unit of work would leave a `done` record for writes that rolled back.
    joins_transaction: ClassVar[bool] = True

    def _token(self, tx: Tx | None) -> int:
        return self._transactor.token(tx)

    async def claim(self, key: str, payload_hash: int, tx: Tx | None = None) -> Claimed:
        with errors.mapped():
            claimed = self._transactor._native.get().claim(self._token(tx), key, payload_hash)
        return Won(claimed.handle) if claimed.won else decode_record(claimed.prior)

    async def complete(self, key: str, handle: bytes, record: Record, tx: Tx | None = None) -> None:
        with errors.mapped():  # a handle that names no claim on `key` any more: the fence's StoreError
            self._transactor._native.get().complete(self._token(tx), key, handle, encode_record(record))

    async def release(self, key: str, handle: bytes, tx: Tx | None = None) -> None:
        with errors.mapped():
            self._transactor._native.get().release(self._token(tx), key, handle)

    def expire(self, key: str) -> bool:
        """Lapse whatever ``key`` holds in committed state, as a claim or record TTL running out would: the
        next claim wins, and the old claim's handle settles nothing. A test hook; ``False`` when nothing was
        held. A claim staged by an open unit of work is not committed, so it does not lapse."""
        return self._transactor._native.get().expire_claim(key)


class MemoryLedger(MemoryIdempotencyStore):
    """:class:`~edax.command.IdempotencyStore` on a database of its own: the ordering a Redis ledger has.
    It takes ``tx`` and ignores it, every call lands at once, and it declares no ``joins_transaction``, so
    the guard runs ``claim; begin; run; commit; complete`` with hand compensation, caches a terminal
    failure (a redelivery raises :class:`~edax.errors.CachedFailure`), and can leave a claim over work that
    landed when the record's reply is lost. :meth:`~MemoryIdempotencyStore.expire` lapses a claim as the
    Redis claim TTL does, so a takeover and a stale owner's refused late ``complete`` can be rehearsed. Test
    on the ordering you deploy: this one for a Redis ledger, :class:`MemoryIdempotencyStore` for a ledger
    in your database."""

    joins_transaction: ClassVar[bool] = False

    def __init__(self) -> None:
        super().__init__(MemoryTransactor())

    def _token(self, tx: Tx | None) -> int:
        return _NO_TX


class MemoryProjectionStore(_StoreView):
    """:class:`~edax.projection.ProjectionStore` over the native store; ``get_many`` runs the
    kernel query, so it is the reference for how a durable backend must page. Documents cross as
    JSON. A save under ``tx`` (or the ambient :func:`~edax.store.current_tx`) is staged until the
    transactor commits."""

    async def get(self, id: str, tx: Tx | None = None) -> Entity | None:
        with errors.mapped():
            row = self._transactor._native.get().get_projection(self._transactor.token(tx), id)
        return None if row is None else Entity(id, row.version, _json.decode(row.document_json))

    async def save(self, entity: Entity, tx: Tx | None = None) -> None:
        with errors.mapped():  # native `concurrency:` -> ConcurrencyError, else StoreError
            entity.version = self._transactor._native.get().save_projection(
                self._transactor.token(tx), entity.id, entity.version, _json.encode(entity.document)
            )

    async def get_many(
        self,
        *,
        filter: Filter | None = None,
        sort: list[SortKey] | None = None,
        offset: int = 0,
        limit: int | None = None,
        after: str | None = None,
        tx: Tx | None = None,
    ) -> Page:
        spec = build_spec(filter=filter, sort=sort, offset=offset, limit=limit, after=after)
        with errors.mapped():  # a malformed spec/cursor is tagged `codec:` -> terminal
            page = self._transactor._native.get().query_projections(
                self._transactor.token(tx), _json.encode(spec)
            )
        return Page.from_kernel(page)


class MemorySagaStore(_StoreView):
    """:class:`~edax.saga.SagaStore` over the native store, so the saga rule (a stale ``save`` or
    ``complete`` raises :class:`~edax.errors.ConcurrencyError`, a completed instance takes no write, the
    step's commands commit with its state) is the one every SDK runs. State crosses as JSON, so it must be
    JSON-serializable and comes back as a copy, never the object that was saved; commands cross as the
    native :data:`~edax.saga.SagaCommand`. A write under ``tx`` (or the ambient
    :func:`~edax.store.current_tx`) is staged until the transactor commits. Not durable."""

    async def load(self, id: str, tx: Tx | None = None) -> SagaInstance | None:
        with errors.mapped():
            row = self._transactor._native.get().load_saga(self._transactor.token(tx), id)
        return None if row is None else SagaInstance(_json.decode(row.state_json), row.version, row.completed)

    async def save(
        self, id: str, state: Any, version: int, commands: Sequence[SagaCommand], tx: Tx | None = None
    ) -> int:
        with errors.mapped():  # native `concurrency:` -> ConcurrencyError, else StoreError
            return self._transactor._native.get().save_saga(
                self._transactor.token(tx), id, version, _json.encode(state), list(commands)
            )

    async def complete(
        self, id: str, version: int, commands: Sequence[SagaCommand], tx: Tx | None = None
    ) -> int:
        with errors.mapped():
            return self._transactor._native.get().complete_saga(
                self._transactor.token(tx), id, version, list(commands)
            )

    async def unpublished(self, limit: int = 100) -> list[SagaCommand]:
        with errors.mapped():
            return self._transactor._native.get().unsent_saga_commands(limit)

    async def mark_published(self, ids: Sequence[str]) -> None:
        with errors.mapped():
            self._transactor._native.get().mark_saga_commands_sent(list(ids))


class _Log:
    """The native log every view of one broker shares, and the wakeup of every view parked on it: a
    message, a rewind or a crash in one group can make a message fetchable for any view."""

    def __init__(self, partitions: int) -> None:
        with errors.mapped():  # zero partitions is the kernel's `codec:` refusal
            self.native = ThreadBound(_edax.memory_broker(partitions), "MemoryBroker")
        self.wakeups: list[asyncio.Event] = []

    def wake(self) -> None:
        for event in self.wakeups:
            event.set()


class MemoryBroker(_ConsumerLoop):
    """Both :class:`~edax.broker.Producer` and :class:`~edax.broker.Consumer` over the native log, so
    ``Relay(store, broker.publish)`` and ``broker.run(dispatcher)`` talk without a broker. ``publish``
    returns once the envelope is in the log. Nothing survives the process, and nothing is ever deleted.

    It behaves like a Kafka topic, so a handler that only survives a queue fails here first:

    * a nack rewinds the partition: the nacked message and every later one, acked or not, come back in
      order, and the deliveries of it already handed out are retired (``Delivery.live``), so the
      dispatcher skips them;
    * :meth:`crash` drops this group's in-flight work and redelivers everything above its committed
      watermark, the way a consumer restart or a rebalance does;
    * :meth:`group` reads the same log as another consumer group, which gets every message: a projection
      and a saga each get their own;
    * with ``partitions`` above 1, order holds per key, not across keys.

    The log, the settlement and the commits are the native machine's, shared with every SDK, so
    :meth:`empty` reports the same "nothing left to do" a durable broker does. Only the waiting is
    Python's, because the native poll never blocks: :meth:`poll` loops over it and parks on an
    :class:`asyncio.Event` between empty peeks."""

    def __init__(self, partitions: int = 1) -> None:
        self._attach(_Log(partitions), "default")

    def _attach(self, log: _Log, group: str) -> None:
        self._log = log
        self._group = group
        self._queued = asyncio.Event()
        log.wakeups.append(self._queued)

    def group(self, name: str) -> MemoryBroker:
        """The same log, read as consumer group ``name``: it gets every message, settles on its own, and
        starts at the beginning of the log."""
        view = MemoryBroker.__new__(MemoryBroker)
        view._attach(self._log, name)
        return view

    async def publish(self, env: EventEnvelope) -> None:
        self._log.native.get().publish(env)
        self._log.wake()

    async def poll(self) -> Delivery | None:
        while True:
            # Clear before the peek, never after: a publish that lands after the peek sets the
            # flag, and clearing then would erase the wakeup the peek missed.
            self._queued.clear()
            with errors.mapped():
                polled = self._log.native.get().try_poll(self._group)
            if polled is not None:
                return self._delivery(polled.token, polled.envelope)
            await self._queued.wait()

    def _delivery(self, token: int, env: EventEnvelope) -> Delivery:
        native, group = self._log.native, self._group

        # The machine refuses a second settlement of a token, and one a rewind or a crash retired, and
        # says so with `False`. The refusal is logged, not raised: nothing is lost either way.
        async def ack() -> None:
            if not native.get().ack(group, token):
                logger.warning("edax memory broker: ack of a settled or retired delivery %d", token)

        async def nack() -> None:
            if not native.get().nack(group, token):
                logger.warning("edax memory broker: nack of a settled or retired delivery %d", token)
                return
            self._log.wake()  # the rewind made messages fetchable for a parked poller

        def live() -> bool:
            return native.get().is_live(group, token)

        return Delivery(env, ack, nack, live=live)

    def crash(self) -> None:
        """Simulate the death of this group's consumer, or a rebalance: every delivery it holds is retired,
        and every message above its committed watermark is fetched again."""
        self._log.native.get().crash(self._group)
        self._log.wake()

    def empty(self) -> bool:
        """True only when this group has committed past every message: nothing queued, nothing in flight."""
        return self._log.native.get().is_empty(self._group)

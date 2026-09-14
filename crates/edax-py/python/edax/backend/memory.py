"""In-memory backends for every seam: dev and tests, single process, not durable.

:class:`MemoryTransactor` is the transaction boundary: one native database holding every seam, whose four
stores are Python views over it. A store built with no transactor gets a database of its own; stores built
over one transactor share it, so their writes under one ``tx`` commit or roll back together. The maps, the
concurrency checks, the outbox, the atomic claim, the journal and the broker's settlement accounting live in
the native ``edax-adapters`` crate, shared by every SDK; Python holds only the transaction's token. The
native objects are ``unsendable``: do not share one across OS threads."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, ClassVar, Optional

from .. import _edax, errors
from ..aggregate import EventEnvelope
from ..broker import _ConsumerLoop
from ..command import Record, decode_record, encode_record
from ..dispatcher import Delivery
from ..errors import BadHandle
from ..filter import Filter
from ..projection import Entity
from ..query import Document, SortKey, build_spec
from ..saga import SagaInstance
from ..store import Snapshot, Tx, current_tx

__all__ = [
    "InMemoryBroker",
    "MemoryEventStore",
    "MemoryIdempotencyStore",
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
    """:class:`~edax.store.Transactor` over the native in-memory database. ``async with with_tx(t) as tx``
    opens a unit of work: every store built over this transactor stages its writes under ``tx``, reads its
    own staged writes back, and nobody else sees them until the block exits cleanly. An exception rolls them
    all back. Isolation is by exclusion, not by blocking: a key one open transaction wrote is held until it
    ends, and a second writer gets :class:`~edax.errors.ConcurrencyError` at once, because an in-memory
    store driven from one event loop cannot park a task on a row lock. An idempotency claim on a held key
    loses and reads the holder's record as pending."""

    def __init__(self) -> None:
        self._inner = _edax.memory_transactor()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[MemoryTx]:
        token = self._inner.begin()
        try:
            yield MemoryTx(self, token)
        except BaseException:
            # A cancelled task (a wait_for timeout, shutdown) is a rollback too: nothing of the
            # unit of work may land. A false return means the token was already closed, which
            # only a handle kept past its block could cause; there is nothing left to drop.
            self._inner.rollback(token)
            raise
        with errors.mapped():
            self._inner.commit(token)

    def open_count(self) -> int:
        """How many transactions are open on this database. A leak diagnostic: a token that is
        neither committed nor rolled back is not merely a held key, it is a permanent conflict on
        every key it staged, for the process lifetime. A count that never returns to its idle level
        names a unit of work that escaped its ``async with``."""
        return self._inner.open_count()

    def auto_commit(self) -> MemoryTx:
        """An explicit no-transaction handle: a call made with it lands at once and sees committed
        state only, whatever the ambient :func:`~edax.store.current_tx` is. For a reader that must
        look past an open unit of work, such as a test probing what has landed."""
        return MemoryTx(self, _NO_TX)

    def token(self, tx: Optional[Tx]) -> int:
        """The native token for ``tx``, else for the ambient :func:`~edax.store.current_tx`, else
        the auto-commit token. A handle of another transactor is refused: a store cannot enlist in
        a unit of work its database knows nothing about, and going on would silently drop the
        atomicity the caller asked for."""
        if tx is None:
            tx = current_tx()
        if tx is None:
            return _NO_TX
        if isinstance(tx, MemoryTx) and tx.transactor is self:
            return tx.token
        # `BadHandle`, not `StoreError`: the same handle fails the same way on every retry, so a
        # transient verdict would have a host redeliver a wiring mistake forever. The Rust adapter
        # refuses an unknown token with the same terminal code (`edax_adapters::unknown_tx`).
        raise BadHandle(
            f"tx {tx!r} is not a transaction of this MemoryTransactor: build every memory store "
            "that must commit together over one MemoryTransactor"
        )


def _own(transactor: Optional[MemoryTransactor]) -> MemoryTransactor:
    """The given transactor, else a database of the store's own."""
    return transactor if transactor is not None else MemoryTransactor()


class MemoryEventStore:
    """:class:`~edax.store.EventStore` over the native store: events + snapshots + outbox. Events
    cross as the native :class:`~edax.aggregate.EventEnvelope` in both directions. An
    ``EventStore`` is a :class:`~edax.store.Transactor`: ``transaction()`` is the
    :class:`MemoryTransactor`'s, so ``async with with_tx(store)`` opens the database's unit of
    work. The outbox is read by the relay outside any transaction and holds committed rows only."""

    def __init__(self, transactor: Optional[MemoryTransactor] = None) -> None:
        self._transactor = _own(transactor)

    @property
    def transactor(self) -> MemoryTransactor:
        """The transactor this store enlists with; build the other stores over it to share it."""
        return self._transactor

    def transaction(self) -> AbstractAsyncContextManager[MemoryTx]:
        return self._transactor.transaction()

    async def load(self, tx: Tx, aggregate_id: str, after: int = 0) -> list[EventEnvelope]:
        with errors.mapped():
            page = await self._transactor._inner.load_events(self._transactor.token(tx), aggregate_id, after)
        return list(page.items)

    async def load_snapshot(self, tx: Tx, aggregate_id: str) -> Optional[Snapshot]:
        with errors.mapped():
            r = await self._transactor._inner.load_snapshot(self._transactor.token(tx), aggregate_id)
        return Snapshot(aggregate_id, r.type_name, r.version, bytes(r.data)) if r.present else None

    async def save_snapshot(self, tx: Tx, snapshot: Snapshot) -> None:
        with errors.mapped():
            await self._transactor._inner.save_snapshot(
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
            await self._transactor._inner.append_events(
                self._transactor.token(tx), aggregate_id, expected_version, list(events)
            )

    async def unpublished(self, limit: int = 100) -> list[EventEnvelope]:
        with errors.mapped():
            page = await self._transactor._inner.unpublished(limit)
        return list(page.items)

    async def mark_published(self, ids: Sequence[str]) -> None:
        with errors.mapped():
            await self._transactor._inner.mark_published(list(ids))


class MemoryIdempotencyStore:
    """:class:`~edax.command.IdempotencyStore` over the native atomic claim. Records cross as JSON.
    With ``tx`` given, or :func:`~edax.store.current_tx` set, the claim and the terminal record
    are staged in that unit of work: a rollback releases the key, so the next dispatch runs the
    handler again instead of replaying an outcome that never landed. Cross-process:
    :class:`~edax.backend.redis.RedisIdempotencyStore`."""

    # `token` refuses a handle of another transactor, which is the refusal the contract requires of
    # a store that declares this: the guard skips its compensation here, so silently auto-committing
    # a foreign unit of work would leave a `done` record for writes that rolled back.
    joins_transaction: ClassVar[bool] = True

    def __init__(self, transactor: Optional[MemoryTransactor] = None) -> None:
        self._transactor = _own(transactor)

    @property
    def transactor(self) -> MemoryTransactor:
        return self._transactor

    async def claim(self, key: str, payload_hash: int, tx: Optional[Tx] = None) -> Optional[Record]:
        with errors.mapped():
            prior = await self._transactor._inner.claim(self._transactor.token(tx), key, payload_hash)
        return decode_record(prior) if prior is not None else None

    async def complete(self, key: str, record: Record, tx: Optional[Tx] = None) -> None:
        with errors.mapped():
            await self._transactor._inner.complete(self._transactor.token(tx), key, encode_record(record))

    async def release(self, key: str, tx: Optional[Tx] = None) -> None:
        with errors.mapped():
            await self._transactor._inner.release(self._transactor.token(tx), key)


class MemoryProjectionStore:
    """:class:`~edax.projection.ProjectionStore` over the native store; ``get_many`` runs the
    kernel query, so it is the reference for how a durable backend must page. Documents cross as
    JSON. A save under ``tx`` (or the ambient :func:`~edax.store.current_tx`) is staged until the
    transactor commits."""

    def __init__(self, transactor: Optional[MemoryTransactor] = None) -> None:
        self._transactor = _own(transactor)

    @property
    def transactor(self) -> MemoryTransactor:
        return self._transactor

    async def get(self, id: str, tx: Optional[Tx] = None) -> Optional[Entity]:
        with errors.mapped():
            raw = await self._transactor._inner.get_projection(self._transactor.token(tx), id)
        if raw is None:
            return None
        d = json.loads(raw)
        return Entity(d["id"], d["version"], d["document"])

    async def save(self, entity: Entity, tx: Optional[Tx] = None) -> None:
        with errors.mapped():  # native `concurrency:` -> ConcurrencyError, else StoreError
            entity.version = await self._transactor._inner.save_projection(
                self._transactor.token(tx), entity.id, entity.version, json.dumps(entity.document)
            )

    async def get_many(
        self,
        *,
        filter: Optional[Filter] = None,
        sort: Optional[list[SortKey]] = None,
        offset: int = 0,
        limit: Optional[int] = None,
        after: Optional[str] = None,
        tx: Optional[Tx] = None,
    ) -> list[Document]:
        spec = build_spec(filter=filter, sort=sort, offset=offset, limit=limit, after=after)
        with errors.mapped():  # a malformed spec/cursor is tagged `codec:` -> terminal
            page = await self._transactor._inner.query_projections(
                self._transactor.token(tx), json.dumps(spec)
            )
        return json.loads(page)


class MemorySagaStore:
    """:class:`~edax.saga.SagaStore` over the native store, so the optimistic-concurrency rule (a
    stale ``save`` or ``delete`` raises :class:`~edax.errors.ConcurrencyError`, an absent ``delete``
    is a no-op) is the one every SDK runs. State crosses as JSON, so it must be JSON-serializable
    and comes back as a copy, never the object that was saved. A write under ``tx`` (or the
    ambient :func:`~edax.store.current_tx`) is staged until the transactor commits. Not durable."""

    def __init__(self, transactor: Optional[MemoryTransactor] = None) -> None:
        self._transactor = _own(transactor)

    @property
    def transactor(self) -> MemoryTransactor:
        return self._transactor

    async def load(self, id: str, tx: Optional[Tx] = None) -> Optional[SagaInstance]:
        with errors.mapped():
            raw = await self._transactor._inner.load_saga(self._transactor.token(tx), id)
        if raw is None:
            return None
        row = json.loads(raw)
        return SagaInstance(row["state"], row["version"])

    async def save(self, id: str, state: Any, version: int, tx: Optional[Tx] = None) -> int:
        with errors.mapped():  # native `concurrency:` -> ConcurrencyError, else StoreError
            return await self._transactor._inner.save_saga(
                self._transactor.token(tx), id, version, json.dumps(state)
            )

    async def delete(self, id: str, version: int, tx: Optional[Tx] = None) -> None:
        with errors.mapped():
            await self._transactor._inner.delete_saga(self._transactor.token(tx), id, version)


class InMemoryBroker(_ConsumerLoop):
    """Both :class:`~edax.broker.Producer` and :class:`~edax.broker.Consumer` over the native broker state
    machine, so ``Relay(store, broker.publish)`` and ``broker.run(dispatcher)`` talk without a broker.
    ``publish`` returns once the envelope is queued; FIFO gives per-key order; ``nack`` re-queues at the
    tail, which the contract permits. Nothing survives the process.

    The queue, the order and the settlement accounting are the native machine's, shared with every SDK: a
    polled delivery is unsettled until it is acked or nacked, so :meth:`empty` reports the same "nothing
    left to do" a durable broker does. Only the waiting is Python's, because the native poll never blocks:
    :meth:`poll` loops over it and parks on an :class:`asyncio.Event` between empty peeks."""

    def __init__(self) -> None:
        self._inner = _edax.memory_broker()
        self._queued = asyncio.Event()

    async def publish(self, env: EventEnvelope) -> None:
        self._inner.publish(env)
        self._queued.set()

    async def poll(self) -> Optional[Delivery]:
        while True:
            # Clear before the peek, never after: a publish that lands after the peek sets the
            # flag, and clearing then would erase the wakeup the peek missed.
            self._queued.clear()
            polled = self._inner.try_poll()
            if polled.present:
                return self._delivery(polled.token, polled.envelope)
            await self._queued.wait()

    def _delivery(self, token: int, env: EventEnvelope) -> Delivery:
        # The machine refuses a second settlement of the same token and says so with `False`, so a
        # double ack from a buggy caller cannot drive the outstanding count below what is really in
        # flight. The refusal is logged, not raised: the delivery is already settled either way.
        async def ack() -> None:
            if not self._inner.ack(token):
                logger.warning("edax memory broker: ack of already-settled delivery %d", token)

        async def nack() -> None:
            if not self._inner.nack(token):
                logger.warning("edax memory broker: nack of already-settled delivery %d", token)
                return
            self._queued.set()  # the requeue is a new message for a parked poller

        return Delivery(env, ack, nack)

    def empty(self) -> bool:
        """True only when nothing is queued *and* every polled delivery has been settled."""
        return self._inner.is_empty()

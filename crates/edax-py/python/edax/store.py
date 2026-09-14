"""Storage seam plus save/load orchestration. The kernel owns no I/O, so persistence is the SDK's job. This
module is the seam: an :class:`EventStore` you back with your own database, a :class:`Repository` that
rehydrates and persists aggregates, and a :class:`Relay` that drains the outbox. No concrete store ships
here. The outbox lives on :class:`EventStore` because a transactional outbox holds only if the event rows
and the to-publish rows commit together."""

from __future__ import annotations

import asyncio
import contextvars
from collections.abc import AsyncIterable, AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, Callable, Generic, Optional, Protocol, TypeVar

from . import _edax
from ._async import MaybeAwaitable, maybe_await
from .aggregate import Aggregate, Event, EventEnvelope, Version
from .errors import ConcurrencyError

Tx = Any  # opaque backend transaction handle (a connection / transaction); the store defines it

# Invariant: the aggregate is returned by `load` and consumed by `dump`, so neither variance is sound.
A = TypeVar("A", bound=Aggregate)

__all__ = [
    "AggregateCodec",
    "ConcurrencyError",  # the seam's conflict error
    "EventCodec",
    "EventStore",
    "Relay",
    "Repository",
    "Snapshot",
    "Transactor",
    "current_tx",
    "pump",
    "transaction",
    "with_tx",
]


class Transactor(Protocol):
    """Owns the transaction boundary: begin a unit of work, commit it, or roll it back. The stores that take
    a ``tx`` (:class:`EventStore`, :class:`~edax.projection.ProjectionStore`,
    :class:`~edax.command.IdempotencyStore`) are pure participants, so any number of them commit together.
    What a backend must guarantee:

    - ``transaction()`` returns an async context manager: a clean exit commits, an exception rolls back and
      re-raises, and nothing written through the yielded ``tx`` is visible elsewhere before commit.
    - The yielded ``tx`` is one backend handle every participant store accepts, atomic as a set.
    - A participant handed the same ``tx`` reads its own uncommitted writes.
    - The SDK never nests: stores join the ambient :func:`current_tx` rather than open a second unit of
      work, so no savepoints are needed. Nesting ``with_tx`` yourself is undefined.
    - The handle is used from one asyncio task at a time; no thread safety required.

    :class:`~edax.backend.memory.MemoryTransactor` implements it, so a test can assert a raise left
    nothing behind."""

    def transaction(self) -> AbstractAsyncContextManager[Tx]:
        """A unit of work: commit on clean exit, roll back if the body raises."""
        ...


_current_tx: contextvars.ContextVar[Optional[Tx]] = contextvars.ContextVar("edax_current_tx", default=None)


def current_tx() -> Optional[Tx]:
    """The tx of the innermost :func:`with_tx`, else ``None``. A store handed ``tx=None`` reads this."""
    return _current_tx.get()


@asynccontextmanager
async def with_tx(transactor: Transactor) -> AsyncIterator[Tx]:
    """Run a unit of work over ``transactor``: yield the tx, commit on clean exit, roll back if the body
    raises. The tx is published to :func:`current_tx` for the duration, so participants given no explicit tx
    join it. A task created inside the block keeps the tx after the block exits, which the
    :class:`Transactor` contract forbids: start long-lived tasks outside a unit of work."""
    async with transactor.transaction() as tx:
        token = _current_tx.set(tx)
        try:
            yield tx
        finally:
            _current_tx.reset(token)


transaction = with_tx


@dataclass
class Snapshot:
    """A marshaled aggregate at a given version, so a load can skip replaying the whole stream."""

    aggregate_id: str
    type_name: str
    version: int
    data: bytes


class EventStore(Protocol):
    """The durable log: per-aggregate events with optimistic concurrency, aggregate snapshots, and a
    transactional outbox, all under one transaction so state and to-publish rows land together.
    What a backend must guarantee; worked examples in ``docs/backends/README.md``.

    - *Versions.* An aggregate's version is the ``sequence`` of its last stored event, ``0`` for an empty
      stream. ``Repository`` assigns ``sequence = expected_version + 1 .. + n``; a store never renumbers.
    - *Optimistic concurrency.* ``append`` succeeds only if the stored version equals ``expected_version``,
      else it raises :class:`ConcurrencyError` and writes nothing. Enforce that with a unique constraint or
      a row lock in the database, never a read-then-write in Python.
    - *Atomicity and durability.* Within one ``append`` the event rows, their outbox rows and any snapshot
      saved in the same ``tx`` commit together; a rollback removes all of them and a commit survives a crash.
    - *Reads.* ``load`` returns every event with ``sequence > after``, ascending, gap-free, including this
      ``tx``'s own uncommitted appends. ``load_snapshot`` returns the latest snapshot or ``None``, and may
      lag the stream but never lead it.
    - *Outbox.* ``unpublished`` returns only committed unmarked rows, oldest commit first, at minimum one
      aggregate's rows in ``sequence`` order. Rows stay until ``mark_published``, so publishing is
      at-least-once and downstream dedups by ``env.id``; ``mark_published`` is idempotent and durable. Both
      run outside any ``tx``. Poll them with :class:`Relay`, one per store unless ``unpublished`` leases the
      rows, or feed change data capture to :func:`pump`, where the stream position is the ack.
    - *Errors.* :class:`ConcurrencyError` only for a version mismatch; any other failure raises
      :class:`~edax.errors.StoreError`, or :class:`~edax.errors.CodecError` for a marshal failure. Called
      from one asyncio loop; no thread safety required."""

    def transaction(self) -> AbstractAsyncContextManager[Tx]:
        """A unit of work: commit on clean exit, roll back if the body raises."""
        ...

    async def load(self, tx: Tx, aggregate_id: str, after: int = 0) -> list[EventEnvelope]:
        """Events with ``sequence > after``, ascending, gap-free, including ``tx``'s own appends."""
        ...

    async def load_snapshot(self, tx: Tx, aggregate_id: str) -> Optional[Snapshot]:
        """The latest snapshot for the aggregate, or ``None``. May lag the stream; never lead it."""
        ...

    async def save_snapshot(self, tx: Tx, snapshot: Snapshot) -> None:
        """Upsert the aggregate's snapshot (last writer wins), committed with ``tx``."""
        ...

    async def append(
        self, tx: Tx, aggregate_id: str, expected_version: int, events: Sequence[EventEnvelope]
    ) -> None:
        """Compare-and-set append: if the stored version equals ``expected_version``, write ``events`` and
        their outbox rows atomically in ``tx``; else raise :class:`ConcurrencyError` and write nothing."""
        ...

    async def unpublished(self, limit: int = 100) -> list[EventEnvelope]:
        """Up to ``limit`` committed unmarked outbox rows, oldest first. A host running more than one relay
        must make this lease or lock the rows it returns."""
        ...

    async def mark_published(self, ids: Sequence[str]) -> None:
        """Durably acknowledge outbox rows by envelope id. Idempotent; unknown ids are ignored."""
        ...


class EventCodec(Protocol):
    """Bridges a domain event and its marshaled :class:`~edax.aggregate.Event` (name/version/bytes)."""

    def marshal(self, event: Any) -> Event: ...
    def unmarshal(self, event: Event) -> Any: ...


class AggregateCodec(Protocol[A]):
    """Bridges an aggregate and its snapshot bytes. Generic in the aggregate, so :class:`Repository` hands
    your own class back with no downcast."""

    type: str

    def load(self, id: str, version: int, data: Optional[bytes]) -> A:
        """Rebuild an aggregate at ``version``; ``data is None`` means none stored yet (empty at v0)."""
        ...

    def dump(self, agg: A) -> bytes: ...


class Repository(Generic[A]):
    """Loads aggregates (snapshot, then replay the rest) and saves them (append the pending events and a
    fresh snapshot in one transaction; the store also enqueues the outbox). ``load`` and ``save`` take an
    optional ``tx``; without one they join the unit of work the caller opened with :func:`with_tx`, if any,
    else run in their own, so the events commit with a projection or idempotency write in the same
    transaction and nothing ever nests."""

    def __init__(
        self,
        store: EventStore,
        events: EventCodec,
        aggregates: AggregateCodec[A],
        *,
        new_id: Optional[Callable[[], str]] = None,
        now: Optional[Callable[[], int]] = None,
    ) -> None:
        self._store = store
        self._events = events
        self._aggregates = aggregates
        self._new_id = new_id or _edax.guid  # time-ordered id; override for a deterministic clock
        self._now = now or _edax.current_timestamp

    @asynccontextmanager
    async def _enlist(self, tx: Optional[Tx]) -> AsyncIterator[Tx]:
        """Yield ``tx``, else the ambient :func:`current_tx`, else a unit of work of our own."""
        if tx is None:
            tx = current_tx()
        if tx is not None:
            yield tx
        else:
            async with with_tx(self._store) as own:
                yield own

    async def load(self, aggregate_id: str, *, tx: Optional[Tx] = None) -> A:
        """Rehydrate the aggregate: snapshot, then replay the events after it."""
        async with self._enlist(tx) as tx:
            snap = await self._store.load_snapshot(tx, aggregate_id)
            base = snap.version if snap is not None else 0
            agg = self._aggregates.load(aggregate_id, base, snap.data if snap is not None else None)
            last = base
            for env in await self._store.load(tx, aggregate_id, after=base):
                # transition(), not apply(): a replayed event must not be re-queued as pending
                agg.transition(self._events.unmarshal(env.event))
                if env.sequence is not None:
                    last = env.sequence
            agg.sequence = Version(last)
            return agg

    async def save(
        self,
        aggregate: A,
        *,
        namespace: Optional[dict[str, str]] = None,
        metadata: Optional[dict[str, str]] = None,
        tx: Optional[Tx] = None,
    ) -> list[EventEnvelope]:
        """Persist the pending events and a snapshot; return the envelopes, for the :class:`Relay` or for
        an eager publish."""
        initial = aggregate.sequence.initial
        current = aggregate.sequence.current
        envs: list[EventEnvelope] = [
            EventEnvelope(
                id=self._new_id(),
                aggregate_id=aggregate.id,
                event=self._events.marshal(e),
                timestamp=self._now(),
                sequence=initial + i,
                namespace=namespace or {},
                metadata=metadata or {},
            )
            for i, e in enumerate(aggregate.pending, start=1)
        ]
        async with self._enlist(tx) as tx:
            await self._store.append(tx, aggregate.id, initial, envs)
            snapshot = Snapshot(
                aggregate.id, self._aggregates.type, current, self._aggregates.dump(aggregate)
            )
            await self._store.save_snapshot(tx, snapshot)
        aggregate.pending.clear()
        aggregate.sequence = Version(current)
        return envs


Publish = Callable[[EventEnvelope], MaybeAwaitable[None]]


class Relay:
    """Drains the store's outbox to ``publish`` by polling: the transactional-outbox publisher. ``publish``
    may be sync or async, and must return only after the transport has durably accepted the event (a broker
    client's leader ack, or stronger). The batch is marked published once every row in it has been
    published, so a ``publish`` that returns earlier loses the event. One relay per store unless your
    ``unpublished`` leases rows; for change data capture use :func:`pump`."""

    def __init__(self, store: EventStore, publish: Publish) -> None:
        self._store = store
        self._publish = publish
        # Overlapping drains of one relay would read the same unleased rows twice. Serializing them costs
        # nothing, and it bounds this relay only: two relays still need leased rows.
        self._draining = asyncio.Lock()

    async def drain(self, batch: int = 100) -> int:
        """Publish one batch; return how many. Concurrent calls on one relay are serialized, not
        overlapped."""
        async with self._draining:
            rows = await self._store.unpublished(batch)
            for env in rows:
                await maybe_await(self._publish(env))
            if rows:
                await self._store.mark_published([env.id for env in rows])
            return len(rows)


async def pump(source: AsyncIterable[EventEnvelope], publish: Publish) -> int:
    """Change-data-capture outbox drain: publish every envelope from ``source`` until it ends; return how
    many. There is no ``mark_published``: the stream position is the ack, so commit it only after ``pump``
    has passed the row. ``publish`` raising stops the pump; resume from the last committed position."""
    n = 0
    async for env in source:
        await maybe_await(publish(env))
        n += 1
    return n

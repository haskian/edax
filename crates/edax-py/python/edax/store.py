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
from time import perf_counter_ns
from typing import Any, Callable, Generic, Optional, Protocol, TypeVar

from . import _edax
from ._async import MaybeAwaitable, maybe_await
from .aggregate import Aggregate, Event, EventEnvelope, Version
from .errors import BadHandle, ConcurrencyError
from .observe import Observer, _wrap

Tx = Any  # opaque backend transaction handle (a connection / transaction); the store defines it

# Invariant: the aggregate is returned by `load` and consumed by `dump`, so neither variance is sound.
A = TypeVar("A", bound=Aggregate)

__all__ = [
    "AggregateCodec",
    "ConcurrencyError",  # the seam's conflict error
    "DrainStats",
    "EventCodec",
    "EventStore",
    "Relay",
    "Repository",
    "Snapshot",
    "Transactor",
    "current_tx",
    "pump",
    "transaction",
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
      work, so no savepoints are needed. A second :func:`transaction` on the same transactor raises
      :class:`~edax.errors.BadHandle` rather than opening one; a transactor over another database nests.
    - The handle is used from one asyncio task at a time; no thread safety required.

    :class:`~edax.backend.memory.MemoryTransactor` implements it, so a test can assert a raise left
    nothing behind."""

    def transaction(self) -> AbstractAsyncContextManager[Tx]:
        """A unit of work: commit on clean exit, roll back if the body raises."""
        ...


_current_tx: contextvars.ContextVar[Optional[Tx]] = contextvars.ContextVar("edax_current_tx", default=None)
# Who opened the ambient tx, so a second `transaction()` on the same transactor is refused while one
# over a *different* transactor (a second database) still nests legitimately.
_current_transactor: contextvars.ContextVar[Optional[Transactor]] = contextvars.ContextVar(
    "edax_current_transactor", default=None
)


def current_tx() -> Optional[Tx]:
    """The tx of the innermost :func:`transaction`, else ``None``. A store handed ``tx=None`` reads this."""
    return _current_tx.get()


@asynccontextmanager
async def _transaction(transactor: Transactor, observe: Observer) -> AsyncIterator[Tx]:
    """:func:`transaction` over an already-guarded observer, for the orchestration objects that hold one."""
    if _current_transactor.get() is transactor:
        # The Transactor contract says the SDK never nests. A second unit of work on the same transactor
        # commits on its own clean exit, and the outer rollback cannot take those writes back. BadHandle,
        # not StoreError, so a dispatcher dead-letters the wiring mistake instead of redelivering it.
        # The check is by transactor identity: two transactors over one pool are two objects, so this
        # catches composition of service functions, not every way to reach one database twice.
        raise BadHandle(
            "transaction() was opened while this transactor already has one open: the inner unit of work "
            "would commit independently of the outer one. Pass the ambient tx down, or open the "
            "transaction around both callers, not in each"
        )
    started = perf_counter_ns()
    outcome = "rolled_back"
    try:
        async with transactor.transaction() as tx:
            tx_token = _current_tx.set(tx)
            owner_token = _current_transactor.set(transactor)
            try:
                yield tx
            finally:
                _current_tx.reset(tx_token)
                _current_transactor.reset(owner_token)
        outcome = "committed"
    finally:
        observe.transaction(outcome, perf_counter_ns() - started)


def transaction(
    transactor: Transactor, *, observer: Optional[Observer] = None
) -> AbstractAsyncContextManager[Tx]:
    """Run a unit of work over ``transactor``: yield the tx, commit on clean exit, roll back if the body
    raises. The tx is published to :func:`current_tx` for the duration, so participants given no explicit tx
    join it. A task created inside the block keeps the tx after the block exits, which the
    :class:`Transactor` contract forbids: start long-lived tasks outside a unit of work.

    Opening this while the same transactor already has a unit of work open raises
    :class:`~edax.errors.BadHandle`: the inner one would commit on its own clean exit and the outer
    rollback could not take it back. Two service functions that each open a unit of work compose by
    passing the tx down, or by moving the :func:`transaction` out to their caller. A second transactor,
    over another database, nests as before, and neither one's commit is atomic with the other's.

    ``observer`` receives one :meth:`~edax.observe.Observer.transaction` per unit of work, ``committed`` or
    ``rolled_back``, with its duration; ``None`` is the no-op observer."""
    return _transaction(transactor, _wrap(observer))


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
    - *Backlog (optional).* ``async def unpublished_count(self) -> int`` returns how many committed
      unmarked rows are waiting, outside any ``tx``: ``SELECT count(*)`` over the unmarked rows.
      :attr:`Relay.last` reports the drains that finished, so it stops moving when the relay does, and a
      stalled relay then looks exactly like an idle one. The depth is what tells them apart. It is not
      declared on this Protocol, because a required method would break every store that predates it: a
      host reads it with ``getattr``, and ``EventStoreContract.supports_unpublished_count`` runs its
      clause for a store that has it.
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
    """Bridges a domain event and its marshaled :class:`~edax.aggregate.Event` (name/version/bytes).
    Parameters are positional-only, so an implementation may name them after its own event type."""

    def marshal(self, event: Any, /) -> Event: ...
    def unmarshal(self, event: Event, /) -> Any: ...


class AggregateCodec(Protocol[A]):
    """Bridges an aggregate and its snapshot bytes. Generic in the aggregate, so :class:`Repository` hands
    your own class back with no downcast. Parameters are positional-only, so an implementation may name
    them after its own aggregate: a structural match compares parameter *names* otherwise, and
    ``dump(self, account)`` would not satisfy ``dump(self, agg)``."""

    type: str

    def load(self, id: str, version: int, data: Optional[bytes], /) -> A:
        """Rebuild an aggregate at ``version``; ``data is None`` means none stored yet (empty at v0)."""
        ...

    def dump(self, agg: A, /) -> bytes: ...


class Repository(Generic[A]):
    """Loads aggregates (snapshot, then replay the rest) and saves them (append the pending events and a
    fresh snapshot in one transaction; the store also enqueues the outbox). ``load`` and ``save`` take an
    optional ``tx``; without one they join the unit of work the caller opened with :func:`transaction`, if
    any, else run in their own, so the events commit with a projection or idempotency write in the same
    transaction and nothing ever nests.

    ``observer`` receives one :meth:`~edax.observe.Observer.transaction` per unit of work the repository
    opens itself. A call that joined the caller's ``tx`` reports nothing; that caller's :func:`transaction`
    does. ``None`` is the no-op observer."""

    def __init__(
        self,
        store: EventStore,
        events: EventCodec,
        aggregates: AggregateCodec[A],
        *,
        new_id: Optional[Callable[[], str]] = None,
        now: Optional[Callable[[], int]] = None,
        observer: Optional[Observer] = None,
    ) -> None:
        self._store = store
        self._events = events
        self._aggregates = aggregates
        self._new_id = new_id or _edax.guid  # time-ordered id; override for a deterministic clock
        self._now = now or _edax.current_timestamp
        self._observer = _wrap(observer)

    @asynccontextmanager
    async def _enlist(self, tx: Optional[Tx]) -> AsyncIterator[Tx]:
        """Yield ``tx``, else the ambient :func:`current_tx`, else a unit of work of our own."""
        if tx is None:
            tx = current_tx()
        if tx is not None:
            yield tx
        else:
            async with _transaction(self._store, self._observer) as own:
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


@dataclass(frozen=True)
class DrainStats:
    """What the last completed :meth:`Relay.drain` did. ``oldest_timestamp`` is the ``timestamp`` of
    the oldest row in the batch, or ``None`` for an empty batch; against the reader's clock it is the
    outbox lag a health check reads."""

    published: int
    duration_ns: int
    oldest_timestamp: Optional[int]


class Relay:
    """Drains the store's outbox to ``publish`` by polling: the transactional-outbox publisher. ``publish``
    may be sync or async, and must return only after the transport has durably accepted the event (a broker
    client's leader ack, or stronger). The batch is marked published once every row in it has been
    published, so a ``publish`` that returns earlier loses the event. One relay per store unless your
    ``unpublished`` leases rows; for change data capture use :func:`pump`.

    Every completed drain is reported to ``observer`` (:meth:`~edax.observe.Observer.drain`) and kept in
    :attr:`last`; a drain that raises is reported through :meth:`~edax.observe.Observer.drain_failed` and
    leaves :attr:`last` as it was."""

    def __init__(self, store: EventStore, publish: Publish, *, observer: Optional[Observer] = None) -> None:
        self._store = store
        self._publish = publish
        self._observer = _wrap(observer)
        # Overlapping drains of one relay would read the same unleased rows twice. Serializing them costs
        # nothing, and it bounds this relay only: two relays still need leased rows.
        self._draining = asyncio.Lock()
        #: The last completed drain, ``None`` until the first one finishes. A drain that raised leaves it.
        self.last: Optional[DrainStats] = None

    async def drain(self, batch: int = 100) -> int:
        """Publish one batch; return how many. Concurrent calls on one relay are serialized, not
        overlapped."""
        async with self._draining:
            started = perf_counter_ns()
            try:
                rows = await self._store.unpublished(batch)
                for env in rows:
                    await maybe_await(self._publish(env))
                if rows:
                    await self._store.mark_published([env.id for env in rows])
            except Exception as e:
                # Nothing was marked, so the batch is drained again; reported so a stuck relay is a
                # counter, not only a log line at the caller.
                self._observer.drain_failed(e)
                raise
            oldest = min((env.timestamp for env in rows), default=None)
            self.last = DrainStats(len(rows), perf_counter_ns() - started, oldest)
            self._observer.drain(self.last.published, self.last.duration_ns, self.last.oldest_timestamp)
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

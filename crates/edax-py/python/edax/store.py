"""Storage seam plus save/load orchestration. The kernel owns no I/O, so persistence is the SDK's job. This
module is the seam: an :class:`EventStore` you back with your own database, a :class:`Repository` that
rehydrates and persists aggregates, and a :class:`Relay` that drains the outbox. No concrete store ships
here. The outbox lives on :class:`EventStore` because a transactional outbox holds only if the event rows
and the to-publish rows commit together."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
from collections.abc import AsyncIterable, AsyncIterator, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from time import perf_counter_ns
from types import MappingProxyType
from typing import Any, Generic, Protocol, TypeVar, cast

from . import _edax, errors
from ._async import MaybeAwaitable, maybe_await
from ._owned import ThreadBound
from .aggregate import Aggregate, Event, EventEnvelope, Version
from .errors import BadHandle, ConcurrencyError, _carried_code
from .observe import Observer, TransactionOutcome, _wrap
from .pacing import Pacing

logger = logging.getLogger("edax.store")

Tx = Any  # opaque backend transaction handle (a connection / transaction); the store defines it

# Invariant: the aggregate is returned by `load` and consumed by `dump`, so neither variance is sound.
A = TypeVar("A", bound=Aggregate)

__all__ = [
    "AggregateCodec",
    "ConcurrencyError",  # the seam's conflict error
    "DrainStats",
    "EventCodec",
    "EventStore",
    "Outbox",
    "OutboxRow",
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
    - The SDK never nests: stores join the ambient :func:`current_tx` of their own transactor rather than
      open a second unit of work, so no savepoints are needed. A second :func:`transaction` on the same
      transactor raises :class:`~edax.errors.BadHandle` rather than opening one; a transactor over another
      database nests, and each store still sees only its own database's tx.
    - *Identity.* The ambient tx is keyed by transactor. An object whose ``transaction()`` only delegates to
      another transactor (an :class:`EventStore` over a shared pool, say) names that transactor as its
      ``transactor`` attribute, so ``transaction(store)`` and ``transaction(store.transactor)`` open the same
      unit of work and every store over the pool finds it.
    - The handle is used from one asyncio task at a time; no thread safety required.

    :class:`~edax.backend.memory.MemoryTransactor` implements it, so a test can assert a raise left
    nothing behind."""

    def transaction(self) -> AbstractAsyncContextManager[Tx]:
        """A unit of work: commit on clean exit, roll back if the body raises."""
        ...


# One open tx per database, keyed by the identity of the transactor that owns it (an `id`, since a
# transactor need not be hashable; the open `transaction` holds the object, so the id cannot be reused
# meanwhile). One slot for every database would hand a store of one database the tx of another as soon as
# two nest, which writes through the wrong connection. Each `transaction` sets a new mapping, never mutates.
_ambient: contextvars.ContextVar[Mapping[int, Tx]] = contextvars.ContextVar(
    "edax_ambient_tx", default=MappingProxyType({})
)

# How deep a chain of `transactor` attributes may run before it is a cycle, not a delegation.
_MAX_DELEGATION = 8


def _database(transactor: object) -> object:
    """The transactor that owns ``transactor``'s units of work: follow its ``transactor`` attribute while it
    names another object. The ambient is keyed by this, so a store and the transactor it delegates to share
    one key."""
    for _ in range(_MAX_DELEGATION):
        owner = getattr(transactor, "transactor", None)
        if owner is None or owner is transactor:
            return transactor
        transactor = owner
    raise BadHandle(f"{transactor!r}: its `transactor` attributes form a cycle, so it names no database")


def current_tx(transactor: object) -> Tx | None:
    """The tx of the open :func:`transaction` over ``transactor``'s database, else ``None``. A store handed
    ``tx=None`` reads this with its own transactor, so it never sees another database's tx."""
    return _ambient.get().get(id(_database(transactor)))


def _in_unit_of_work() -> bool:
    """Whether any :func:`transaction` is open in this task, over any database."""
    return bool(_ambient.get())


def _leave_every_unit_of_work() -> None:
    """Start this task with no ambient tx. A task copies its creator's context, so a long-lived task started
    inside :func:`transaction` would otherwise keep using that tx from a second task."""
    _ambient.set({})


@asynccontextmanager
async def _transaction(transactor: Transactor, observe: Observer) -> AsyncIterator[Tx]:
    """:func:`transaction` over an already-guarded observer, for the orchestration objects that hold one."""
    database = id(_database(transactor))
    open_now = _ambient.get()
    if database in open_now:
        # The Transactor contract says the SDK never nests. A second unit of work on the same database
        # commits on its own clean exit, and the outer rollback cannot take those writes back. BadHandle,
        # not StoreError, so a dispatcher dead-letters the wiring mistake instead of redelivering it.
        # The check is by transactor identity: two transactors over one pool that do not name a shared
        # `transactor` are two objects, so this catches composition of service functions, not every way
        # to reach one database twice.
        raise BadHandle(
            "transaction() was opened while this transactor already has one open: the inner unit of work "
            "would commit independently of the outer one. Pass the ambient tx down, or open the "
            "transaction around both callers, not in each"
        )
    started = perf_counter_ns()
    outcome: TransactionOutcome = "rolled_back"
    try:
        async with transactor.transaction() as tx:
            token = _ambient.set({**open_now, database: tx})
            try:
                yield tx
            finally:
                _ambient.reset(token)
        outcome = "committed"
    finally:
        observe.transaction(outcome, perf_counter_ns() - started)


def transaction(
    transactor: Transactor, *, observer: Observer | None = None
) -> AbstractAsyncContextManager[Tx]:
    """Run a unit of work over ``transactor``: yield the tx, commit on clean exit, roll back if the body
    raises. The tx is published to :func:`current_tx` for ``transactor``'s database for the duration, so
    participants of that database given no explicit tx join it. A task created inside the block keeps the
    tx after the block exits, which the :class:`Transactor` contract forbids: start long-lived tasks outside
    a unit of work.

    Opening this while the same transactor already has a unit of work open raises
    :class:`~edax.errors.BadHandle`: the inner one would commit on its own clean exit and the outer
    rollback could not take it back. Two service functions that each open a unit of work compose by
    passing the tx down, or by moving the :func:`transaction` out to their caller. A second transactor,
    over another database, nests: each database's stores join that database's tx, and neither commit is
    atomic with the other's.

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


class OutboxRow(Protocol):
    """A row an :class:`Outbox` holds: an :class:`~edax.aggregate.EventEnvelope`, or a
    :class:`~edax.saga.SagaCommand`. ``id`` is what it is marked by; ``timestamp`` is when it was written."""

    @property
    def id(self) -> str: ...
    @property
    def timestamp(self) -> int: ...


R = TypeVar("R", bound=OutboxRow)


class Outbox(Protocol[R]):
    """The read side of a transactional outbox, as :class:`Relay` drains it: committed rows not yet
    published, oldest commit first, and the acknowledgement that prunes them. An :class:`EventStore` is one
    (its rows are envelopes); a saga store's command outbox is another (see
    :meth:`~edax.saga.SagaRunner.relay`)."""

    async def unpublished(self, limit: int = 100) -> list[R]: ...
    async def mark_published(self, ids: Sequence[str]) -> None: ...


class EventStore(Protocol):
    """The durable log: per-aggregate events with optimistic concurrency, aggregate snapshots, and a
    transactional outbox, all under one transaction so state and to-publish rows land together.
    What a backend must guarantee; worked examples in ``docs/backends/README.md``.

    - *Versions.* An aggregate's version is the ``sequence`` of its last stored event, ``0`` for an empty
      stream. ``Repository`` assigns ``sequence = expected_version + 1 .. + n``; a store never renumbers.
    - *Optimistic concurrency.* ``append`` succeeds only if the stored version equals ``expected_version``,
      else it raises :class:`ConcurrencyError` and writes nothing. Enforce that with a unique constraint or
      a row lock in the database, never a read-then-write in Python.
    - *Dense batches.* A batch must carry ``expected_version + 1, + 2, ...`` in order, every envelope
      sequenced; anything else raises :class:`~edax.errors.CodecError` and writes nothing. Call
      :func:`check_sequence_batch`, the kernel's rule, rather than restating it.
    - *Atomicity and durability.* Within one ``append`` the event rows, their outbox rows and any snapshot
      saved in the same ``tx`` commit together; a rollback removes all of them and a commit survives a crash.
    - *Unique ids.* An envelope id is unique store-wide: an ``append`` carrying an id the store already
      holds raises :class:`~edax.errors.CodecError` and writes nothing, or marking one row published would
      prune the other unpublished. A primary key on the id gives it.
    - *Reads.* ``load`` returns every event with ``sequence > after``, ascending, gap-free, including this
      ``tx``'s own uncommitted appends; :class:`Repository` checks the run with the kernel's dense-batch
      rule and fails the load on a gap or a repeat. ``load_snapshot`` returns the latest snapshot or
      ``None``. A snapshot may lag the stream but never lead it: ``save_snapshot`` refuses one past the
      stream's version as ``tx`` sees it with :class:`~edax.errors.CodecError`, and keeps the higher
      version when an older one arrives late.
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

    async def load_snapshot(self, tx: Tx, aggregate_id: str) -> Snapshot | None:
        """The latest snapshot for the aggregate, or ``None``. May lag the stream; never lead it."""
        ...

    async def save_snapshot(self, tx: Tx, snapshot: Snapshot) -> None:
        """Store the aggregate's snapshot, committed with ``tx``: refused past the stream's version, and
        never replacing a newer one."""
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


def check_sequence_batch(expected_version: int, sequences: Sequence[int | None]) -> None:
    """The kernel's dense-batch rule an :meth:`EventStore.append` must enforce: every envelope carries a
    sequence, and the batch runs ``expected_version + 1, + 2, ...`` in order. A gap, a repeat, a
    reordering or an unsequenced envelope raises terminal :class:`~edax.errors.CodecError` naming the
    0-based batch index, because the same batch fails the same way on every retry. An empty batch passes.
    Versions and sequences are unsigned 64-bit, so a negative one is a :class:`~edax.errors.CodecError` too,
    and a batch that would run past ``2**64 - 1`` is refused rather than clamped.
    Pinned by ``testdata/golden/sequence_batch.json``."""
    with errors.mapped():
        _edax.check_sequence_batch(expected_version, list(sequences))


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

    def load(self, id: str, version: int, data: bytes | None, /) -> A:
        """Rebuild an aggregate at ``version``; ``data is None`` means none stored yet (empty at v0)."""
        ...

    def dump(self, agg: A, /) -> bytes: ...


class Repository(Generic[A]):
    """Loads aggregates (snapshot, then replay the rest) and saves them (append the pending events and a
    fresh snapshot in one transaction; the store also enqueues the outbox). ``load`` and ``save`` take an
    optional ``tx``; without one they join the unit of work the caller opened with :func:`transaction`, if
    any, else run in their own, so the events commit with a projection or idempotency write in the same
    transaction and nothing ever nests.

    ``snapshot_every`` is how often a save writes a snapshot: each time the aggregate's version crosses a
    multiple of it, so ``1`` (the default) snapshots on every save and ``0`` never does. A snapshot is a
    cache that doubles the writes of the save that takes it; a stream that is cheap to replay wants a
    larger interval. A value below ``0`` raises :class:`ValueError`.

    ``observer`` receives one :meth:`~edax.observe.Observer.transaction` per unit of work the repository
    opens itself. A call that joined the caller's ``tx`` reports nothing; that caller's :func:`transaction`
    does. ``None`` is the no-op observer."""

    def __init__(
        self,
        store: EventStore,
        events: EventCodec,
        aggregates: AggregateCodec[A],
        *,
        new_id: Callable[[], str] | None = None,
        now: Callable[[], int] | None = None,
        observer: Observer | None = None,
        snapshot_every: int = 1,
    ) -> None:
        if snapshot_every < 0:
            raise ValueError(f"snapshot_every is a count of versions, at least 0, got {snapshot_every}")
        self._snapshot_every = snapshot_every
        self._store = store
        self._events = events
        self._aggregates = aggregates
        self._new_id = new_id or _edax.guid  # time-ordered id; override for a deterministic clock
        self._now = now or _edax.current_timestamp
        self._observer = _wrap(observer)

    @asynccontextmanager
    async def _enlist(self, tx: Tx | None) -> AsyncIterator[Tx]:
        """Yield ``tx``, else the ambient :func:`current_tx` of the store's database, else a unit of work of
        our own."""
        if tx is None:
            tx = current_tx(self._store)
        if tx is not None:
            yield tx
        else:
            async with _transaction(self._store, self._observer) as own:
                yield own

    async def load(self, aggregate_id: str, *, tx: Tx | None = None) -> A:
        """Rehydrate the aggregate: snapshot, then replay the events after it.

        A snapshot is a cache of the event log, so one this codec cannot use — another aggregate type's
        under the same id, or one whose ``dump`` format ``load`` no longer reads — is ignored and the stream
        replays from version 0. The replayed run is held to the kernel's dense-batch rule, so a store that
        returns a gap or a repeat raises :class:`~edax.errors.CodecError` instead of folding the wrong
        state."""
        async with self._enlist(tx) as tx:
            snap = await self._store.load_snapshot(tx, aggregate_id)
            cached = self._from_snapshot(aggregate_id, snap)
            base, agg = cached if cached is not None else (0, self._aggregates.load(aggregate_id, 0, None))
            envs = await self._store.load(tx, aggregate_id, after=base)
            check_sequence_batch(base, [env.sequence for env in envs])
            for env in envs:
                # transition(), not apply(): a replayed event must not be re-queued as pending
                agg.transition(self._events.unmarshal(env.event))
            # Dense from `base`, so the last sequence is the count past it.
            agg.version = Version(base + len(envs))
            return agg

    def _snapshot_due(self, initial: int, current: int) -> bool:
        """Whether a save from ``initial`` to ``current`` crosses a multiple of ``snapshot_every``; a save
        with nothing pending crosses none."""
        every = self._snapshot_every
        return every > 0 and current // every > initial // every

    def _from_snapshot(self, aggregate_id: str, snap: Snapshot | None) -> tuple[int, A] | None:
        """The aggregate rebuilt from ``snap`` and the version it stands at, or ``None`` to replay from 0."""
        if snap is None:
            return None
        if snap.type_name != self._aggregates.type:
            logger.warning(
                "edax repository: ignoring the %r snapshot of %r, which this %r codec does not read; "
                "replaying from version 0",
                snap.type_name,
                aggregate_id,
                self._aggregates.type,
            )
            return None
        try:
            return snap.version, self._aggregates.load(aggregate_id, snap.version, snap.data)
        except Exception:
            logger.warning(
                "edax repository: the snapshot of %r at version %d does not load; replaying from version 0",
                aggregate_id,
                snap.version,
                exc_info=True,
            )
            return None

    async def save(
        self,
        aggregate: A,
        *,
        namespace: dict[str, str] | None = None,
        metadata: dict[str, str] | None = None,
        tx: Tx | None = None,
    ) -> list[EventEnvelope]:
        """Persist the pending events, and a snapshot when the version crosses a multiple of
        ``snapshot_every``; return the envelopes, for the :class:`Relay` or for an eager publish."""
        initial = aggregate.version.initial
        current = aggregate.version.current
        events = [self._events.marshal(e) for e in aggregate.pending]
        # A value the binding cannot convert (a lone surrogate in an id, a non-str metadata value) fails
        # the same way on every retry, so it is the terminal CodecError, not a bare TypeError.
        with errors.mapped():
            envs: list[EventEnvelope] = [
                EventEnvelope(
                    id=self._new_id(),
                    aggregate_id=aggregate.id,
                    event=event,
                    timestamp=self._now(),
                    sequence=initial + i,
                    namespace=namespace or {},
                    metadata=metadata or {},
                )
                for i, event in enumerate(events, start=1)
            ]
        async with self._enlist(tx) as tx:
            await self._store.append(tx, aggregate.id, initial, envs)
            if self._snapshot_due(initial, current):
                snapshot = Snapshot(
                    aggregate.id, self._aggregates.type, current, self._aggregates.dump(aggregate)
                )
                await self._store.save_snapshot(tx, snapshot)
        aggregate.pending.clear()
        aggregate.version = Version(current)
        return envs


Publish = Callable[[Any], MaybeAwaitable[None]]
#: A dead-letter sink for a :class:`Relay`: takes the row that cannot be published and the error that killed
#: it, and returns once the row is somewhere a human will look.
RelaySink = Callable[[Any, BaseException], MaybeAwaitable[None]]


@dataclass(frozen=True)
class DrainStats:
    """What the last completed :meth:`Relay.drain` did. ``oldest_timestamp`` is the ``timestamp`` of
    the oldest row in the batch, or ``None`` for an empty batch; against the reader's clock it is the
    outbox lag a health check reads. ``dead_lettered`` counts the rows the drain handed to ``on_dead``
    instead of publishing them."""

    published: int
    duration_ns: int
    oldest_timestamp: int | None
    dead_lettered: int = 0


class Relay:
    """Drains an :class:`Outbox` to ``publish`` by polling: the transactional-outbox publisher, for the event
    outbox and a saga's command outbox alike. ``publish`` may be sync or async, and must return only after
    the transport has durably accepted the row (every in-sync replica: Kafka ``acks="all"``, not the leader
    alone); one that returns earlier loses the row. For change data capture use :func:`pump`.

    A drain publishes the batch in order and marks every row it settled, also when a row fails part way:
    publishing is at-least-once, so downstream deduplicates by row id. A failing row stops the batch,
    because the rows behind it may be its aggregate's later events. Whether it is handed back or dead-
    lettered is the kernel's retry policy, keyed by row id: a terminal failure, or a transient one at the
    ``max_attempts``-th attempt (``0``: never), goes to ``on_dead`` and is marked, so one poison row cannot
    stall the outbox; a row handed back waits one step of ``pacing`` before the drain re-raises, and the step
    grows each time the same row fails, so a host looping on :meth:`drain` does not spin. With no ``on_dead``
    a poison row is handed back on every drain. :class:`~edax.errors.InProgress` (a guarded command whose key
    another dispatcher holds) is contention: handed back without spending an attempt. A drain whose store
    call fails (``unpublished`` or ``mark_published``) waits too before it re-raises, one step per consecutive
    store failure, so a store that is down is neither spun on nor hammered; a drain whose store calls succeed
    starts that count over.

    Concurrent drains of one relay take turns, so a timer and a "drain now" never publish one batch twice.
    Two relays over one store need ``unpublished`` to lease the rows it returns.

    Every completed drain is reported to ``observer`` (:meth:`~edax.observe.Observer.drain`) and kept in
    :attr:`last`; a drain that raises is reported through :meth:`~edax.observe.Observer.drain_failed` and
    leaves :attr:`last` as it was."""

    def __init__(
        self,
        store: Outbox[Any],
        publish: Publish,
        *,
        observer: Observer | None = None,
        on_dead: RelaySink | None = None,
        max_attempts: int = 0,
        pacing: Pacing | None = None,
    ) -> None:
        self._store = store
        self._publish = publish
        self._observer = _wrap(observer)
        self._on_dead = on_dead
        self._max_attempts = max_attempts
        self._pacing = pacing or Pacing()
        self._retries: ThreadBound[_edax.Retries] | None = None
        # Overlapping drains of one relay would read the same unleased rows twice. Serializing them costs
        # nothing, and it bounds this relay only: two relays still need leased rows.
        self._draining = asyncio.Lock()
        # Consecutive drains whose store call failed: the backoff step of the next one. Read and written
        # under `_draining` only.
        self._store_failures = 0
        #: The last completed drain, ``None`` until the first one finishes. A drain that raised leaves it.
        self.last: DrainStats | None = None

    def _policy(self) -> _edax.Retries:
        """The kernel's retry policy, built on first use on the loop's thread: the native handle is bound
        to the thread that builds it, and a relay may be built on another."""
        if self._retries is None:
            sink = self._on_dead is not None
            self._retries = ThreadBound(_edax.retries(self._max_attempts, sink), "Relay")
        return self._retries.get()

    async def drain(self, batch: int = 100) -> int:
        """Publish one batch; return how many rows it settled, published or dead-lettered. Concurrent calls
        on one relay are serialized, not overlapped."""
        async with self._draining:
            started = perf_counter_ns()
            try:
                rows = await self._store.unpublished(batch)
                settled, published, dead, failure = await self._publish_in_order(rows)
                if settled:
                    await self._store.mark_published(settled)
            except Exception as e:
                # The store failed, not a row. Reported so a stuck relay is a counter, not only a log line
                # at the caller, and waited out with a step that grows while the store stays down.
                self._observer.drain_failed(e)
                await self._pacing.wait(self._store_failures)
                self._store_failures += 1
                raise
            self._store_failures = 0
            if failure is not None:
                step, error = failure
                self._observer.drain_failed(error)
                await self._pacing.wait(step)
                raise error
            oldest = min((row.timestamp for row in rows), default=None)
            self.last = DrainStats(published, perf_counter_ns() - started, oldest, dead)
            self._observer.drain(self.last.published, self.last.duration_ns, self.last.oldest_timestamp)
            return published + dead

    async def run(self, *, idle_s: float = 1.0, stop: asyncio.Event | None = None, batch: int = 100) -> None:
        """Drain until ``stop`` is set, or forever when it is ``None`` (cancel the task to end it). Drains
        back to back while rows flow; after a drain that settled nothing, waits ``idle_s`` or until ``stop``.
        A failed drain, a row's or the store's, has already waited its backoff step and reported
        :meth:`~edax.observe.Observer.drain_failed`, so the loop logs it and drains again: one bad row or a
        store that is down never ends the relay, and never spins the event loop. A cancel ends it at once."""
        while stop is None or not stop.is_set():
            try:
                settled = await self.drain(batch)
            except Exception:
                logger.warning("edax relay: drain failed; the next drain retries it", exc_info=True)
                continue
            if settled:
                continue
            if stop is None:
                await asyncio.sleep(idle_s)
            else:
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), idle_s)

    async def _publish_in_order(
        self, rows: Sequence[Any]
    ) -> tuple[list[str], int, int, tuple[int, Exception] | None]:
        """Publish ``rows`` until one fails. Returns the ids of the rows settled, how many were published and
        dead-lettered, and the failed row's backoff step and error, if one failed."""
        policy = self._policy()
        settled: list[str] = []
        published = dead = 0
        for row in rows:
            try:
                await maybe_await(self._publish(row))
            except Exception as e:  # noqa: BLE001 - the retry policy classifies any publish failure
                step = await self._settle_failure(policy, row, e)
                if step is None:
                    settled.append(row.id)
                    dead += 1
                    continue
                return settled, published, dead, (step, e)
            policy.succeeded(row.id)
            settled.append(row.id)
            published += 1
        return settled, published, dead, None

    async def _settle_failure(self, policy: _edax.Retries, row: Any, error: Exception) -> int | None:
        """Ask the policy what a failed row gets: ``None`` when ``on_dead`` took it, else the backoff step to
        wait before handing it back. The kernel reads the error's code."""
        verdict = policy.failed(row.id, 0, _carried_code(error))
        if not verdict.dead:
            return verdict.step
        logger.warning(
            "edax relay: dead-lettering outbox row %s after %d attempt(s)", row.id, verdict.attempt
        )
        # The kernel answers `dead` only to a policy built with a sink, so a dead verdict means one is set.
        on_dead = cast("RelaySink", self._on_dead)
        try:
            await maybe_await(on_dead(row, error))
        except Exception:
            logger.exception(
                "edax relay: the dead-letter sink failed for %s; the next drain retries it", row.id
            )
            return policy.sink_failed(row.id, verdict.attempt)
        policy.dead_lettered(row.id)
        return None


async def pump(source: AsyncIterable[EventEnvelope], publish: Publish) -> int:
    """Change-data-capture outbox drain: publish every envelope from ``source`` until it ends; return how
    many. There is no ``mark_published``: the stream position is the ack, so commit it only after ``pump``
    has passed the row. ``publish`` raising stops the pump; resume from the last committed position."""
    n = 0
    async for env in source:
        await maybe_await(publish(env))
        n += 1
    return n

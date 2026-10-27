"""The transactional outbox: the :class:`Outbox` read side a :class:`Relay` drains, the
:class:`CommandOutbox` write side a save sends commands through, and :func:`pump` for change data
capture. Every write port stages its outbox rows in the caller's ``tx``; the read side is always this one
separate port, which one object may implement beside a write port. Events, saga commands, projection
effect intents and ingestion rejections all drain through one :class:`Relay`."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterable, Callable, Sequence
from dataclasses import dataclass
from time import perf_counter_ns
from typing import Final, Generic, Protocol, TypeVar, runtime_checkable

from . import _names, _native, errors
from ._async import MaybeAwaitable, maybe_await
from ._observer import wrap
from ._settle import Dead, HandBack, Stop, dead_letter, drain_policy, row_verdict
from ._wiring import is_int
from .delivery import DeadLetter, DeadLetterSink
from .errors import code_of
from .message import CommandMessage, EventMessage
from .observe import Observer
from .pacing import Pacing
from .unit_of_work import TxT_contra

logger = logging.getLogger("edax.outbox")

__all__ = [
    "DEFAULT_BATCH",
    "CommandOutbox",
    "CountedOutbox",
    "DrainStats",
    "Outbox",
    "OutboxRow",
    "Publish",
    "Relay",
    "check_page",
    "pump",
]


class OutboxRow(Protocol):
    """What a :class:`Relay` reads of a row: ``id`` marks it and keys its retry budget; ``tenant`` and
    ``aggregate_id`` order it, through :func:`~edax.message.ordering_key` (a row with no aggregate is ordered
    by its own id). :class:`~edax.message.EventMessage`, :class:`~edax.message.CommandMessage`,
    :class:`~edax.projection.EffectIntent` and :class:`~edax.ingestion.Rejection` are rows."""

    @property
    def id(self) -> str: ...
    @property
    def tenant(self) -> str | None: ...
    @property
    def aggregate_id(self) -> str | None: ...


R = TypeVar("R", bound=OutboxRow)

#: How many rows a :class:`Relay` drain reads at most, unless its caller says otherwise. The one default:
#: every :meth:`Outbox.unpublished` takes its ``limit`` from its caller.
DEFAULT_BATCH: Final[int] = 100


class Outbox(Protocol[R]):
    """The read side of a transactional outbox, as :class:`Relay` drains it: the rows a write port staged
    in its ``tx``, once committed. The event outbox an :class:`~edax.store.EventStore`'s ``append`` writes
    is one (its rows are event messages). Others: a saga's command outbox (:class:`~edax.saga.SagaOutbox`),
    the read side of a :class:`CommandOutbox` and a :class:`~edax.projection.ProjectionStateStore` (effect
    intents). What a backend must guarantee:

    - *Committed only.* ``unpublished`` returns committed unmarked rows, never a row a ``tx`` staged and
      may still roll back, each as the write port stored it, every field intact.
    - *Order.* Oldest commit first, at minimum one aggregate's rows in ``sequence`` order, and ``limit``
      decides the page.
    - *Acknowledgement.* Rows stay until ``mark_published``, so publishing is at-least-once and downstream
      dedups by row ``id``. ``mark_published`` is idempotent and durable; an unknown id and an empty list
      are no-ops.
    - Both run outside any ``tx``. One :class:`Relay` per outbox, unless ``unpublished`` leases the rows it
      returns. Feed change data capture to :func:`pump` instead, where the stream position is the ack.

    ``OutboxContract`` in :mod:`edax.testing` holds a backend to every clause.

    :class:`~edax.ingestion.RejectionOutbox` also works with :class:`Relay`, but has a stricter
    acknowledgement contract: decoded source/fingerprint pairs must name an existing rejection with
    the same fingerprint. It does not implement the general unknown-id no-op."""

    async def unpublished(self, limit: int) -> list[R]: ...
    async def mark_published(self, ids: Sequence[str]) -> None: ...


@runtime_checkable
class CountedOutbox(Protocol):
    """An :class:`Outbox` that also counts its backlog: how many committed unmarked rows wait now, outside
    any ``tx`` (``SELECT count(*)`` over the unmarked rows). :attr:`Relay.last` reports the drains that
    finished, so it stops moving when the relay does, and a stalled relay then looks exactly like an idle
    one; the depth tells them apart. Optional: a host checks ``isinstance(outbox, CountedOutbox)``, and
    ``OutboxContract`` runs its count clause for an outbox that is one."""

    async def unpublished_count(self) -> int: ...


class CommandOutbox(Protocol[TxT_contra]):
    """The write side of a command outbox: :meth:`~edax.repository.Repository.save` stores the commands a save
    sends in the same transaction as its events, so both commit or neither does. A :class:`Relay` drains the
    read side, an :class:`Outbox` of :class:`~edax.message.CommandMessage`, once the transaction has
    committed.

    ``enqueue`` writes the messages under ``tx`` as unpublished rows keyed by message id. ``tx`` is the
    event store's: a backend over another database than the event store refuses it with the terminal
    :class:`~edax.errors.BadHandle`, because the two writes would not commit together.
    :class:`~edax.backend.memory.MemoryCommandOutbox` is one.

    ``after_event`` is the id of the last event the same save appended, or ``None``. Each command waits
    behind it: the outbox holds the command back while that event is still unpublished, so a relay never
    sends a command before the event it follows from."""

    async def enqueue(
        self, tx: TxT_contra, commands: Sequence[CommandMessage], *, after_event: str | None = None
    ) -> None: ...


#: Publishes one row of an :class:`Outbox` of ``R``, and returns once the transport has durably accepted it.
Publish = Callable[[R], MaybeAwaitable[None]]


@dataclass(frozen=True)
class DrainStats:
    """What the last completed :meth:`Relay.drain` did. ``oldest_timestamp`` is when the oldest row in the
    batch was written, its ``recorded_at`` (an event) or ``issued_at`` (a command), or ``None`` for an
    empty batch or one whose rows carry no time; against the reader's clock it is the
    outbox lag a health check reads. ``dead_lettered`` counts the rows the drain handed to ``on_dead``
    instead of publishing them."""

    published: int
    duration_ns: int
    oldest_timestamp: int | None
    dead_lettered: int = 0


class Relay(Generic[R]):
    """Drains an :class:`Outbox` of ``R`` to ``publish`` by polling: the transactional-outbox publisher, for
    every :class:`Outbox` alike. ``publish`` may be sync or async, and must return
    only after the transport has durably accepted the row (every in-sync replica: Kafka ``acks="all"``, not
    the leader alone); one that returns earlier loses the row. For change data capture use :func:`pump`.

    A drain publishes the batch in order and marks every row it settled, also when a row fails part way:
    publishing is at-least-once, so downstream deduplicates by row id. Order is promised per key, the row's
    tenant and :func:`~edax.message.ordering_key`: a failing row holds back the rows behind it of its own key,
    which may be its aggregate's later events, and the other keys go on. Which rows a drain publishes, and
    what a failure does, is the kernel's drain rule, pinned by ``testdata/golden/relay_drain.json``. Whether a
    failed row is handed back or dead-lettered is its retry policy, keyed by row id: a terminal failure, or a
    transient one at the ``max_attempts``-th attempt (``0``: never), goes to ``on_dead`` and is marked, so one
    poison row cannot stall its key. ``on_dead`` takes the row as a :class:`~edax.delivery.DeadLetter`, with
    the attempt that killed it and the publish failure. A row the sink took but the drain could not mark
    starts over on the next drain: the relay forgot its attempts, so it publishes the row again with a fresh
    budget. The row may then reach the transport as well as the sink, or the sink a second time, so the sink
    deduplicates by row id as downstream does. A drain
    that handed a row back waits the largest step of ``pacing`` among those rows before it re-raises the
    first one's failure, and the step grows each time the same row fails, so a host looping on
    :meth:`drain` does not spin. With no ``on_dead`` a poison row is handed back on every drain. A second
    transient failure with no successful publish between them stops the drain: the broker is likely down,
    and the rest would only fail alike. A fatal failure (:func:`~edax.errors.is_fatal`:
    a refused credential, topic or setting) hands the row back uncounted, never to ``on_dead``, and the drain
    re-raises it at once: every row behind it would fail alike, so :meth:`run` ends on it rather than move
    the outbox into the dead-letter queue. An ``on_dead`` that raises a fatal error stops the drain the same
    way, and the drain re-raises the sink's error. :class:`~edax.errors.InProgress` (a guarded command whose
    key another dispatcher holds) is contention: its key is held back without spending an attempt. A drain
    whose store call fails (``unpublished`` or ``mark_published``) waits too before it re-raises, one step per
    consecutive store failure, so a store that is down is neither spun on nor hammered; a drain whose store
    calls succeed starts that count over.

    Concurrent drains of one relay take turns, so a timer and a "drain now" never publish one batch twice.
    Two relays over one store need ``unpublished`` to lease the rows it returns.

    Every completed drain is reported to ``observer`` (:meth:`~edax.observe.Observer.drain`) and kept in
    :attr:`last`; a drain that raises is reported through :meth:`~edax.observe.Observer.drain_failed`, with
    the rows it published and dead-lettered before the failure, and leaves :attr:`last` as it was.
    A ``max_attempts`` the kernel cannot carry raises :class:`~edax.errors.BadHandle` at construction."""

    def __init__(
        self,
        store: Outbox[R],
        publish: Publish[R],
        *,
        observer: Observer | None = None,
        on_dead: DeadLetterSink[R] | None = None,
        max_attempts: int = 0,
        pacing: Pacing | None = None,
    ) -> None:
        self._store = store
        self._publish = publish
        self._observer = wrap(observer)
        self._on_dead = on_dead
        self._pacing = pacing or Pacing()
        self._drain = drain_policy(max_attempts, on_dead is not None)
        # Overlapping drains of one relay would read the same unleased rows twice. Serializing them costs
        # nothing, and it bounds this relay only: two relays still need leased rows.
        self._draining = asyncio.Lock()
        #: The last completed drain, ``None`` until the first one finishes. A drain that raised leaves it.
        self.last: DrainStats | None = None

    async def drain(self, batch: int = DEFAULT_BATCH) -> int:
        """Publish one batch; return how many rows it settled, published or dead-lettered. Concurrent calls
        on one relay are serialized, not overlapped."""
        async with self._draining:
            started = perf_counter_ns()
            drain = self._drain.get()
            # What the drain did before a store failure, so `drain_failed` loses none of it.
            published = dead = 0
            try:
                rows = await self._store.unpublished(batch)
                drained, failures = await self._publish_admitted(drain, rows)
                published, dead = drained.published, drained.dead_lettered
                if drained.settled:
                    await self._store.mark_published(drained.settled)
            except Exception as e:
                # The store failed, not a row. Reported so a stuck relay is a counter, not only a log line
                # at the caller, and waited out with a step that grows while the store stays down.
                self._observer.drain_failed(e, published, dead)
                await self._pacing.wait(drain.store_failed())
                raise
            drain.store_answered()
            if drained.raise_id is not None:
                error = failures[drained.raise_id]
                self._observer.drain_failed(error, published, dead)
                # No step for a fatal failure: the host stops on it, so a wait would only delay the report.
                if drained.wait_step is not None:
                    await self._pacing.wait(drained.wait_step)
                raise error
            oldest = min((t for t in map(_written_at, rows) if t is not None), default=None)
            last = self.last = DrainStats(published, perf_counter_ns() - started, oldest, dead)
            self._observer.drain(last.published, last.dead_lettered, last.duration_ns, last.oldest_timestamp)
            return published + dead

    async def run(
        self, *, idle_s: float = 1.0, stop: asyncio.Event | None = None, batch: int = DEFAULT_BATCH
    ) -> None:
        """Drain until ``stop`` is set, or forever when it is ``None`` (cancel the task to end it). Drains
        back to back while rows flow; after a drain that settled nothing, waits ``idle_s`` or until ``stop``.
        A failed drain, a row's or the store's, has already waited its backoff step and reported
        :meth:`~edax.observe.Observer.drain_failed`, so the loop logs it and drains again: one bad row or a
        store that is down never ends the relay, and never spins the event loop. A fatal failure
        (:func:`~edax.errors.is_fatal`) ends it, raised: every drain would fail alike until the wiring is
        fixed. A cancel ends it at once."""
        stop = stop or asyncio.Event()  # with none given, nothing sets it: a cancel ends the loop
        while not stop.is_set():
            try:
                settled = await self.drain(batch)
            except Exception as e:
                if errors.is_fatal(e):
                    raise
                logger.warning("edax relay: drain failed; the next drain retries it", exc_info=True)
                continue
            if settled:
                # A store and a publisher that never suspend would drain a whole backlog without handing
                # the event loop back, so timers, a cancel and `stop` would all wait for the last row.
                await asyncio.sleep(0)
            else:
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), idle_s)

    async def _publish_admitted(
        self, drain: _native.Drain, rows: Sequence[R]
    ) -> tuple[_native.Drained, dict[str, BaseException]]:
        """Publish, in order, every row of ``rows`` the kernel's ``drain`` admits, and report each outcome to
        it. Returns what the drain did, and each failed row's error by id, for the one the drain raises."""
        _begin(drain, rows)
        failures: dict[str, BaseException] = {}
        for row in rows:
            row_id = row.id
            if not drain.admits(row_id):
                continue
            try:
                await maybe_await(self._publish(row))
            except Exception as e:  # noqa: BLE001 - the drain rule classifies any publish failure
                failures[row_id] = await self._settle_failure(drain, row, row_id, e)
                continue
            drain.published(row_id)
        return drain.finish(), failures

    async def _settle_failure(
        self, drain: _native.Drain, row: R, row_id: str, error: Exception
    ) -> BaseException:
        """Perform what the drain rule answers for a failed row: hand it back, stop, or dead-letter it and
        report how ``on_dead`` took it. The kernel reads the error's code. Returns the failure the drain
        raises for this row: ``error``, or the sink's own when its fatal refusal stopped the drain, since
        that is the wiring to fix."""
        match row_verdict(drain.failed(row_id, code_of(error))):
            case HandBack() | Stop():
                return error
            case Dead(attempt=attempt):
                refusal, refused = await dead_letter(
                    logger, self._on_dead, DeadLetter(row, attempt, error), row_id, drain.sunk
                )
                # `None`: the sink took the row, which the drain settles.
                stopped = refused is not None and isinstance(row_verdict(refused), Stop)
                return refusal if stopped and refusal is not None else error
            case unknown:
                _names.unreachable(unknown)


def check_page(limit: int) -> int:
    """``limit`` as an outbox page size; a non-positive one is the terminal
    :class:`~edax.errors.BadHandle`."""
    if not is_int(limit) or limit <= 0:
        raise errors.BadHandle("outbox page limit must be positive")
    return limit


def _begin(drain: _native.Drain, rows: Sequence[OutboxRow]) -> None:
    """Hand the kernel's ``drain`` the keys of the page it decides over; it reads nothing else of a row. One
    outbox holds one kind of row, so a page that mixes events with commands is a backend fault: the terminal
    :class:`~edax.errors.CodecError`."""
    events = sum(isinstance(row, EventMessage) for row in rows)
    if 0 < events < len(rows):
        raise errors.CodecError(f"an outbox page mixes {events} events with {len(rows) - events} commands")
    drain.begin([_native.DrainRow(row.id, row.tenant, row.aggregate_id) for row in rows])


def _written_at(row: OutboxRow) -> int | None:
    """When an outbox row was written: an event's ``recorded_at``, a command's ``issued_at``; ``None`` for a
    row that carries no time."""
    if isinstance(row, EventMessage):
        return row.recorded_at
    if isinstance(row, CommandMessage):
        return row.issued_at
    return None


async def pump(source: AsyncIterable[R], publish: Publish[R]) -> int:
    """Change-data-capture outbox drain: publish every message from ``source`` until it ends; return how
    many. There is no ``mark_published``: the stream position is the ack, so commit it only after ``pump``
    has passed the row. ``publish`` raising stops the pump; resume from the last committed position."""
    n = 0
    async for msg in source:
        await maybe_await(publish(msg))
        n += 1
    return n

"""Asyncio ordered-parallel consumer: same aggregate id -> same worker (ordered), different ids
run concurrently."""

from __future__ import annotations

import asyncio
import enum
import logging
from dataclasses import dataclass
from time import perf_counter_ns
from typing import Callable, Optional, Union

from . import _edax
from ._async import MaybeAwaitable, maybe_await
from .aggregate import EventEnvelope
from .errors import InProgress, is_terminal
from .observe import Observer, _wrap, error_type
from .store import _current_tx

logger = logging.getLogger("edax.dispatcher")

#: How a backoff step is waited out. The schedule is the kernel's and a host must not bend it, but a
#: test asserting which steps were asked for should not pay them in wall time, so this is the seam a
#: test patches. The Rust dispatcher takes the same injection through its builder.
_sleep = asyncio.sleep

Handler = Callable[[EventEnvelope], MaybeAwaitable[None]]
AckFn = Callable[[], MaybeAwaitable[None]]
DeadLetterFn = Callable[["Delivery", BaseException], MaybeAwaitable[None]]


@dataclass
class Delivery:
    """A broker delivery: the event plus the two handles that settle it. The transport handle (a Kafka
    offset, an SQS receipt) is per-delivery state, not stable across redelivery, so the adapter captures it
    in the ``on_ack`` / ``on_nack`` closures instead of on the persisted :class:`EventEnvelope`. A
    :class:`KeyOrderedDispatcher` acks on success and nacks if the handler raises."""

    envelope: EventEnvelope
    on_ack: AckFn
    on_nack: AckFn
    attempt: int = 0  # the broker's own redelivery count if it keeps one, else 0; see max_attempts

    async def ack(self) -> None:
        await maybe_await(self.on_ack())

    async def nack(self) -> None:
        await maybe_await(self.on_nack())


class _Stop(enum.Enum):
    """Queue sentinel: the worker that takes it finishes and returns."""

    TOKEN = enum.auto()


# What a worker queue carries: a bare envelope, a settleable delivery, or the stop sentinel.
_QueueItem = Union[EventEnvelope, Delivery, _Stop]


@dataclass(frozen=True)
class DispatcherStats:
    """A point-in-time reading of a :class:`KeyOrderedDispatcher`, for a health check or a gauge.
    ``queue_depth`` is one entry per worker (the backpressure signal); ``in_flight`` counts items a
    worker has taken but not finished with; ``attempts_tracked`` / ``contended_tracked`` are the
    per-message failure and contention maps, which only shrink when the message comes back."""

    queue_depth: list[int]
    in_flight: int
    attempts_tracked: int
    contended_tracked: int
    running: bool


class KeyOrderedDispatcher:
    """Fans one broker partition or key-shared subscription out to a local pool of asyncio workers. Worker
    is ``partition(aggregate_id, workers)``, so one aggregate's events stay on one worker in FIFO order while
    different aggregates progress in parallel. ``max_queue`` bounds each worker's queue for backpressure
    (``submit`` awaits when full); ``0`` makes it unbounded. Per-key order holds for successful deliveries
    only: a nack returns the delivery to the broker tail, so a key that ever nacks is reordered against
    itself and handlers must tolerate that.

    A raising handler nacks unless ``on_dead`` is set. With it, a terminal error dead-letters on the first
    attempt whatever ``max_attempts`` says, and a transient one at the ``max_attempts``-th failed attempt
    (``0`` disables the ceiling). Every nack waits one step of the kernel's backoff schedule first, or a
    handler failing on a downed dependency polls the broker as fast as it redelivers.
    :class:`~edax.errors.InProgress` waits the same way but never counts, because it is contention rather
    than a failed attempt. The wait blocks that worker and every key routed to it, because per-key ordering
    forbids skipping ahead. Attempt counting and sink behaviour: ``docs/sdk/consuming.md``.

    ``observer`` receives one :meth:`~edax.observe.Observer.delivery` per settled delivery and a
    :meth:`~edax.observe.Observer.settlement_failed` for an ack or nack that raised; ``None`` is the
    no-op observer. A bare envelope is fire-and-forget: nothing settles it, so nothing is reported.
    """

    def __init__(
        self,
        workers: int,
        handler: Handler,
        *,
        max_queue: int = 1024,
        max_attempts: int = 0,
        on_dead: Optional[DeadLetterFn] = None,
        observer: Optional[Observer] = None,
    ) -> None:
        self.handler = handler
        self._max_attempts = max_attempts
        self._on_dead = on_dead
        self._observer = _wrap(observer)
        self._queues: list[asyncio.Queue[_QueueItem]] = [
            asyncio.Queue[_QueueItem](maxsize=max_queue) for _ in range(max(1, workers))
        ]
        self._tasks: list[asyncio.Task[None]] = []
        # Items taken off a queue whose handler has not returned (or, for a Delivery, not settled).
        # `qsize()` stops counting an item the moment a worker takes it, so `idle()` needs this too.
        self._in_flight = 0
        # Entries for nacked messages that never come back stay until the id is seen again.
        self._attempts: dict[str, int] = {}
        # Backoff step per message losing a claim race. Separate from `_attempts`: contention is not a
        # failed attempt, and the same unbounded-entry caveat applies.
        self._contended: dict[str, int] = {}

    def stats(self) -> DispatcherStats:
        """A snapshot for a health check or a gauge; cheap, no locking, read from the loop's thread."""
        return DispatcherStats(
            queue_depth=[q.qsize() for q in self._queues],
            in_flight=self._in_flight,
            attempts_tracked=len(self._attempts),
            contended_tracked=len(self._contended),
            running=bool(self._tasks),
        )

    def idle(self) -> bool:
        """Every queue is empty and no worker holds an item."""
        stats = self.stats()
        return stats.in_flight == 0 and not any(stats.queue_depth)

    @property
    def on_dead(self) -> Optional[DeadLetterFn]:
        """The dead-letter sink, or ``None``. A :class:`~edax.broker.Consumer` that must settle a message
        no handler may see — a payload that will not decode — routes it here, the same exit a terminal
        handler failure takes. Set at construction: workers read it, so it is not swapped mid-run."""
        return self._on_dead

    @property
    def observer(self) -> Observer:
        """The guarded observer, for the consumer loop that settles poison messages outside
        :meth:`submit` and still has to report them; never ``None``."""
        return self._observer

    def _worker_index(self, aggregate_id: str) -> int:
        return _edax.partition(aggregate_id, len(self._queues))

    async def _run(self, queue: asyncio.Queue[_QueueItem]) -> None:
        # A task copies its creator's context, so a dispatcher started inside `transaction` would hand every
        # handler that caller's tx from a second task, which the Transactor contract forbids.
        _current_tx.set(None)
        while True:
            item = await queue.get()
            if item is _Stop.TOKEN:
                queue.task_done()
                return
            # A raising handler (or ack/nack) must not kill the worker and strand its queue.
            self._in_flight += 1
            try:
                if isinstance(item, Delivery):
                    await self._settle(item)
                else:
                    # Bare envelope: fire-and-forget, so no settlement and no `Observer.delivery` either;
                    # a host wanting numbers for this path times the handler with `observe.timing`.
                    await maybe_await(self.handler(item))
            except Exception:
                # Named per arm: a bare envelope is fire-and-forget, so nothing settles it and
                # "settling failed" would send an operator to the wrong layer.
                what = "settling a delivery" if isinstance(item, Delivery) else "a fire-and-forget handler"
                logger.exception("edax dispatcher: %s failed; worker continues", what)
            finally:
                self._in_flight -= 1
                queue.task_done()

    async def _settle(self, delivery: Delivery) -> None:
        """Run the handler, settle the delivery by its outcome, and report that outcome once. The
        duration is taken before any settlement, so it covers the handler only and a latency histogram
        over it is not polluted by the backoff wait."""
        attempt = max(delivery.attempt, self._attempts.get(delivery.envelope.id, 0)) + 1
        started = perf_counter_ns()
        error: Optional[Exception] = None
        try:
            await maybe_await(self.handler(delivery.envelope))
        except Exception as e:  # noqa: BLE001 - handler failed -> nack or dead-letter below
            error = e
        duration_ns = perf_counter_ns() - started
        # A settlement that raises (`on_dead`, the nack) propagates to the worker's catch-all with the
        # delivery neither acked nor nacked; reported as such so the arm is visible, not silent.
        outcome = "unsettled"
        try:
            outcome = await self._ack_nack_or_dead(delivery, error, attempt)
        finally:
            cause = error_type(error) if error is not None else None
            self._observer.delivery(delivery.envelope.event.name, outcome, attempt, duration_ns, cause)

    async def _ack_nack_or_dead(self, delivery: Delivery, error: Optional[Exception], attempt: int) -> str:
        """Settle by the handler's outcome and return which arm settled it: ``ack``, ``nack``, ``dead``,
        ``in_progress``, or ``unsettled`` for an ack that did not land (a raising nack or ``on_dead``
        propagates instead)."""
        env_id = delivery.envelope.id
        if error is None:
            acked = await self._ack_or_log(delivery)
            self._contended.pop(env_id, None)
            self._attempts.pop(env_id, None)
            return "ack" if acked else "unsettled"
        if isinstance(error, InProgress):
            # Contention, not a failed attempt: wait one backoff step, then redeliver without touching the
            # attempt budget. Nacking at once would poll the broker until the claim's TTL.
            step = self._contended.get(env_id, 0)
            self._contended[env_id] = step + 1
            await self._nack_after_backoff(delivery, step)
            return "in_progress"
        self._contended.pop(env_id, None)
        terminal = is_terminal(error)
        dead = terminal or (0 < self._max_attempts <= attempt)
        if dead and self._on_dead is not None:
            logger.warning(
                "edax dispatcher: dead-lettering %s after %d attempt(s) (terminal=%s)",
                env_id,
                attempt,
                terminal,
            )
            # on_dead raising propagates before any ack/nack: the broker redelivers.
            await maybe_await(self._on_dead(delivery, error))
            acked = await self._ack_or_log(delivery)
            self._attempts.pop(env_id, None)
            return "dead" if acked else "unsettled"
        # Whether redelivery reorders against already-acked same-aggregate messages is the broker's.
        # Recorded whatever the ceiling, so the backoff below grows across redeliveries: with the
        # default `max_attempts=0` an unrecorded attempt is always 1 and the wait a flat first step.
        self._attempts[env_id] = attempt
        # Transient failure: waited the same way as the InProgress arm, one step of the kernel's
        # schedule before handing the delivery back.
        await self._nack_after_backoff(delivery, attempt - 1)
        return "nack"

    async def _nack_after_backoff(self, delivery: Delivery, step: int) -> None:
        """Wait one step of the kernel's backoff schedule, then nack. Both failure arms wait: nacking at
        once has a handler on a downed dependency polling the broker as fast as it redelivers, and on a
        seeking transport every nack re-reads the partition tail. The wait blocks this worker, and
        therefore every key routed to it — per-key ordering forbids skipping ahead. A nack that raises
        leaves the delivery unsettled and propagates to the worker's catch-all, as an `on_dead` raise
        does."""
        await _sleep(_edax.backoff_millis(step) / 1000)
        try:
            await delivery.nack()
        except Exception as e:
            self._observer.settlement_failed(delivery.envelope.event.name, delivery.envelope.id, e)
            raise

    async def _ack_or_log(self, delivery: Delivery) -> bool:
        """Ack, or report the failure as what it is: a settlement problem, not a handler failure.
        Returns whether the ack landed.

        The handler already did its work, so this is not ``on_dead``'s business either — a dead-letter
        queue would then hold an event that was handled successfully. Unsettled means the broker
        redelivers, and an idempotent handler absorbs that. Swallowed here rather than in the worker's
        catch-all so the bookkeeping below it still runs and the log names the real cause.
        """
        try:
            await delivery.ack()
        except Exception as e:
            self._observer.settlement_failed(delivery.envelope.event.name, delivery.envelope.id, e)
            logger.warning(
                "edax dispatcher: ack of %s failed after a successful handler; the broker will redeliver it",
                delivery.envelope.id,
                exc_info=True,
            )
            return False
        return True

    def start(self) -> None:
        if self._tasks:
            # A second worker on one queue would consume it concurrently and break per-key order.
            raise RuntimeError("dispatcher already started")
        self._tasks = [asyncio.create_task(self._run(q)) for q in self._queues]

    async def submit(self, item: Union[EventEnvelope, Delivery]) -> None:
        """Submit a bare envelope (fire-and-forget) or a :class:`Delivery` (acked/nacked by outcome)."""
        env = item.envelope if isinstance(item, Delivery) else item
        await self._queues[self._worker_index(env.aggregate_id)].put(item)

    async def stop(self) -> None:
        """Signal workers to finish queued events, then wait for the drain. Idempotent."""
        if not self._tasks:
            # A second stop would queue a sentinel no worker takes, so a later `join()` would hang.
            return
        tasks, self._tasks = self._tasks, []
        for q in self._queues:
            await q.put(_Stop.TOKEN)
        await asyncio.gather(*tasks)

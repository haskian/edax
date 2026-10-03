"""Asyncio ordered-parallel consumer: same aggregate id -> same worker (ordered), different ids
run concurrently."""

from __future__ import annotations

import asyncio
import enum
import logging
from collections.abc import Callable
from dataclasses import dataclass
from time import perf_counter_ns
from typing import Any

from . import _edax
from ._async import MaybeAwaitable, maybe_await
from ._owned import ThreadBound
from .aggregate import EventEnvelope
from .errors import InProgress, is_terminal
from .observe import DeliveryOutcome, Observer, _wrap, error_type
from .pacing import Pacing
from .store import _leave_every_unit_of_work

__all__ = [
    "AckFn",
    "DeadLetterFn",
    "Delivery",
    "DispatcherStats",
    "Handler",
    "KeyOrderedDispatcher",
    "LiveFn",
]


logger = logging.getLogger("edax.dispatcher")


def _worker_cancelled() -> bool:
    """Whether the running task itself has been asked to cancel, as against a handler awaiting a
    future someone else cancelled. ``Task.cancelling`` exists from 3.11; on 3.10 every
    ``CancelledError`` counts as the worker's own, so a worker can still end that way there, and
    :attr:`DispatcherStats.running` reports it."""
    task = asyncio.current_task()
    cancelling = getattr(task, "cancelling", None)
    return cancelling is None or cancelling() > 0


Handler = Callable[[EventEnvelope], MaybeAwaitable[None]]
AckFn = Callable[[], MaybeAwaitable[None]]
LiveFn = Callable[[], bool]
DeadLetterFn = Callable[["Delivery", BaseException], MaybeAwaitable[None]]


def _always_live() -> bool:
    """The liveness of a delivery whose transport never retires one it handed out."""
    return True


@dataclass
class Delivery:
    """A broker delivery: the event plus the handles that settle it. The transport handle (a Kafka
    offset, an SQS receipt) is per-delivery state, not stable across redelivery, so the adapter captures it
    in the ``on_ack`` / ``on_nack`` closures instead of on the persisted :class:`EventEnvelope`. A
    :class:`KeyOrderedDispatcher` acks on success and nacks if the handler raises.

    ``live`` tells whether the transport still counts on this delivery. A log-based transport (Kafka, the
    in-memory broker) retires every delivery it handed out from a partition a nack rewinds or a rebalance
    takes away, and fetches those messages again. A worker skips a delivery that is no longer live: no
    handler run and no settlement, so a later event of a key never runs ahead of the redelivered earlier
    one. ``origin`` is the transport's own record of the message, for a dead-letter sink that republishes
    it (a Kafka consumer sets a :class:`~edax.backend.kafka.KafkaFetched`); ``None`` when there is none."""

    envelope: EventEnvelope
    on_ack: AckFn
    on_nack: AckFn
    attempt: int = 0  # the broker's own redelivery count if it keeps one, else 0; see max_attempts
    live: LiveFn = _always_live
    origin: Any = None

    async def ack(self) -> None:
        await maybe_await(self.on_ack())

    async def nack(self) -> None:
        await maybe_await(self.on_nack())


class _Stop(enum.Enum):
    """Queue sentinel: the worker that takes it finishes and returns."""

    TOKEN = enum.auto()


# What a worker queue carries: a bare envelope, a settleable delivery, or the stop sentinel.
_QueueItem = EventEnvelope | Delivery | _Stop


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
    #: Every worker is alive. ``False`` before :meth:`KeyOrderedDispatcher.start`, after
    #: :meth:`~KeyOrderedDispatcher.stop`, and once any worker has died: a dead worker strands every
    #: key routed to it, so a health check must read this, not just "was started".
    running: bool
    #: How many workers are alive, against ``len(queue_depth)`` started.
    workers_alive: int
    #: Handlers running right now: a subset of ``in_flight``, without the items waiting out a backoff step
    #: or a settlement. A rebalance waits for this to reach zero before another member takes the keys.
    handling: int


class KeyOrderedDispatcher:
    """Fans one broker partition or key-shared subscription out to a local pool of asyncio workers. Worker
    is ``partition(aggregate_id, workers)``, so one aggregate's events stay on one worker in FIFO order while
    different aggregates progress in parallel. ``max_queue`` bounds each worker's queue for backpressure
    (``submit`` awaits when full); ``0`` makes it unbounded. Per-key order holds through a nack on a
    transport that rewinds, as a log does: the nack seeks the partition back, the transport retires the
    deliveries of it already handed out, and a worker skips a delivery that is no longer
    :attr:`~Delivery.live`, so the redelivered earlier event runs first. On a transport that requeues a
    nacked delivery behind later ones instead, a key that nacks is reordered against itself.

    A raising handler nacks unless ``on_dead`` is set. With it, a terminal error dead-letters on the first
    attempt whatever ``max_attempts`` says, and a transient one at the ``max_attempts``-th failed attempt
    (``0`` disables the ceiling). A sink that raises nacks the delivery the same way, so the redelivery
    gives it another try. Every nack waits one step of ``pacing`` first (default: the kernel's jittered
    backoff on ``asyncio.sleep``), or a handler failing on a downed dependency polls the broker as fast as
    it redelivers.
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
        on_dead: DeadLetterFn | None = None,
        observer: Observer | None = None,
        pacing: Pacing | None = None,
    ) -> None:
        if workers < 1:
            raise ValueError(f"a dispatcher needs at least one worker, got {workers}")
        self.handler = handler
        self._pacing = pacing or Pacing()
        self._max_attempts = max_attempts
        self._retries: ThreadBound[_edax.Retries] | None = None
        self._on_dead = on_dead
        self._observer = _wrap(observer)
        self._queues: list[asyncio.Queue[_QueueItem]] = [
            asyncio.Queue[_QueueItem](maxsize=max_queue) for _ in range(workers)
        ]
        self._tasks: list[asyncio.Task[None]] = []
        # Set by `stop()` and cleared by `start()`: a submit in between would queue an item no worker
        # will ever take, so it is refused.
        self._stopping = False
        # Items taken off a queue whose handler has not returned (or, for a Delivery, not settled).
        # `qsize()` stops counting an item the moment a worker takes it, so `idle()` needs this too.
        self._in_flight = 0
        self._handling = 0

    def _policy(self) -> _edax.Retries:
        """The kernel's retry and dead-letter policy: when a failure is handed back, after which backoff
        step, and when it dies. Built on first use, on the loop's thread, because the native handle is
        bound to the thread that builds it and a dispatcher may be built on another. Entries for nacked
        messages that never come back stay until the id is seen again."""
        if self._retries is None:
            self._retries = ThreadBound(_edax.retries(self._max_attempts), "KeyOrderedDispatcher")
        return self._retries.get()

    def stats(self) -> DispatcherStats:
        """A snapshot for a health check or a gauge; cheap, no locking, read from the loop's thread."""
        alive = sum(not task.done() for task in self._tasks)
        tracked = self._policy().tracked()
        return DispatcherStats(
            queue_depth=[q.qsize() for q in self._queues],
            in_flight=self._in_flight,
            attempts_tracked=tracked.attempts,
            contended_tracked=tracked.contended,
            running=bool(self._tasks) and alive == len(self._tasks),
            workers_alive=alive,
            handling=self._handling,
        )

    def idle(self) -> bool:
        """Every queue is empty and no worker holds an item."""
        stats = self.stats()
        return stats.in_flight == 0 and not any(stats.queue_depth)

    @property
    def on_dead(self) -> DeadLetterFn | None:
        """The dead-letter sink, or ``None``. A :class:`~edax.broker.Consumer` that must settle a message
        no handler may see — a payload that will not decode — routes it here, the same exit a terminal
        handler failure takes. Set at construction: workers read it, so it is not swapped mid-run."""
        return self._on_dead

    @property
    def pacing(self) -> Pacing:
        """How a nack waits; a :class:`~edax.broker.Consumer` loop waits the same way after a failed poll."""
        return self._pacing

    def _worker_index(self, aggregate_id: str) -> int:
        return _edax.partition(aggregate_id, len(self._queues))

    async def _run(self, queue: asyncio.Queue[_QueueItem]) -> None:
        # A task copies its creator's context, so a dispatcher started inside `transaction` would hand every
        # handler that caller's tx from a second task, which the Transactor contract forbids.
        _leave_every_unit_of_work()
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
                    await self._handle(item)
            except Exception:
                # Named per arm: a bare envelope is fire-and-forget, so nothing settles it and
                # "settling failed" would send an operator to the wrong layer.
                what = "settling a delivery" if isinstance(item, Delivery) else "a fire-and-forget handler"
                logger.exception("edax dispatcher: %s failed; worker continues", what)
            finally:
                self._in_flight -= 1
                queue.task_done()

    async def _handle(self, env: EventEnvelope) -> None:
        """Run the handler, counted in :attr:`DispatcherStats.handling` while it runs."""
        self._handling += 1
        try:
            await maybe_await(self.handler(env))
        finally:
            self._handling -= 1

    async def _settle(self, delivery: Delivery) -> None:
        """Run the handler, settle the delivery by its outcome, and report that outcome once. The
        duration is taken before any settlement, so it covers the handler only and a latency histogram
        over it is not polluted by the backoff wait."""
        attempt = self._policy().attempt(delivery.envelope.id, delivery.attempt)
        if not delivery.live():
            # A rewind or a rebalance retired it before this worker took it: the transport fetches it
            # again behind the earlier event of its key, so running it now would reorder the key.
            self._observer.delivery(delivery.envelope.event.name, "stale", attempt, 0, None)
            return
        started = perf_counter_ns()
        error: BaseException | None = None
        try:
            await self._handle(delivery.envelope)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as e:
            if isinstance(e, asyncio.CancelledError) and _worker_cancelled():
                # This worker is being stopped, not the handler failing: hand the delivery back
                # before the cancel ends the worker, so it is redelivered rather than stranded.
                await asyncio.shield(self._nack_quietly(delivery))
                raise
            # Any other BaseException (a `CancelledError` from a future someone else cancelled, a
            # native `PanicException`) fails this delivery only. Letting it end the worker would
            # strand every key routed to it, with nothing to notice.
            error = e
        duration_ns = perf_counter_ns() - started
        cause = error_type(error) if error is not None else None
        if not delivery.live():
            # Retired while the handler ran: its ack or nack would settle nothing, and a dead-letter
            # would be repeated by the redelivered copy. That copy settles it instead.
            self._observer.delivery(delivery.envelope.event.name, "stale", attempt, duration_ns, cause)
            return
        # A settlement that raises (the ack, the nack) propagates to the worker's catch-all with the
        # delivery neither acked nor nacked; reported as such so the arm is visible, not silent.
        outcome: DeliveryOutcome = "unsettled"
        try:
            outcome = await self._ack_nack_or_dead(delivery, error, attempt)
        finally:
            self._observer.delivery(delivery.envelope.event.name, outcome, attempt, duration_ns, cause)

    async def _ack_nack_or_dead(
        self, delivery: Delivery, error: BaseException | None, attempt: int
    ) -> DeliveryOutcome:
        """Settle by the handler's outcome as the kernel's retry policy answers, and return which arm settled
        it: ``ack``, ``nack``, ``dead``, ``in_progress``, ``sink_failed``, or ``unsettled`` for an ack that
        did not land (a raising nack propagates instead)."""
        env_id = delivery.envelope.id
        policy = self._policy()
        if error is None:
            acked = await self._ack_or_log(delivery)
            policy.succeeded(env_id)
            return "ack" if acked else "unsettled"
        if isinstance(error, InProgress):
            # Contention, not a failed attempt: the policy waits its own step and never counts it toward
            # the ceiling. Nacking at once would poll the broker until the claim's TTL.
            verdict = policy.failed(env_id, delivery.attempt, "contended", self._on_dead is not None)
            await self._nack_after_backoff(delivery, verdict.step)
            return "in_progress"
        # A BaseException is never terminal: nothing says it repeats, so it is retried like a
        # transient failure.
        terminal = isinstance(error, Exception) and is_terminal(error)
        failure = "terminal" if terminal else "transient"
        verdict = policy.failed(env_id, delivery.attempt, failure, self._on_dead is not None)
        if verdict.dead and self._on_dead is not None:
            logger.warning(
                "edax dispatcher: dead-lettering %s after %d attempt(s) (terminal=%s)",
                env_id,
                verdict.attempt,
                terminal,
            )
            if not await self._sink(self._on_dead, delivery, error, verdict.attempt):
                return "sink_failed"
            acked = await self._ack_or_log(delivery)
            policy.dead_lettered(env_id)
            return "dead" if acked else "unsettled"
        # Whether redelivery reorders against already-acked same-aggregate messages is the broker's.
        # Transient failure: waited the same way as the InProgress arm, one step of the kernel's
        # schedule before handing the delivery back.
        await self._nack_after_backoff(delivery, verdict.step)
        return "nack"

    async def _sink(
        self, on_dead: DeadLetterFn, delivery: Delivery, error: BaseException, attempt: int
    ) -> bool:
        """Hand ``delivery`` to ``on_dead``; return whether the sink took it. A sink that raises nacks the
        delivery after one backoff step, the transient path, and the policy keeps the attempt so the
        redelivery reaches the sink again with a longer wait. Leaving it unsettled instead would pin a
        log-based broker for good: nothing redelivers a delivery while its consumer lives."""
        try:
            await maybe_await(on_dead(delivery, error))
        except asyncio.CancelledError:
            # Stopped mid-sink: hand the delivery back before the cancel ends the worker.
            await asyncio.shield(self._nack_quietly(delivery))
            raise
        except Exception:
            logger.exception(
                "edax dispatcher: the dead-letter sink failed for %s; nacking it for another try",
                delivery.envelope.id,
            )
            step = self._policy().sink_failed(delivery.envelope.id, attempt)
            await self._nack_after_backoff(delivery, step)
            return False
        return True

    async def settle_poison(self, delivery: Delivery, error: BaseException) -> DeliveryOutcome:
        """Settle a polled message no handler may see — a payload that will not decode — the way a
        terminal handler failure settles: ``on_dead``, then ack. With no sink, or a sink that raises, it is
        nacked after one backoff step, counted by its id like a failed attempt, so one poison message
        cannot spin the consumer against the broker. Reported through
        :meth:`~edax.observe.Observer.undecodable`. Returns ``dead``, ``nack`` or ``sink_failed``.

        A :class:`~edax.broker.Consumer` loop calls this in place of :meth:`submit`; routing the message
        through :meth:`submit` would hand a handler an envelope with no event in it."""
        env_id = delivery.envelope.id
        self._observer.undecodable(env_id, error)
        policy = self._policy()
        verdict = policy.failed(env_id, delivery.attempt, "terminal", self._on_dead is not None)
        if verdict.dead and self._on_dead is not None:
            if not await self._sink(self._on_dead, delivery, error, verdict.attempt):
                return "sink_failed"
            await delivery.ack()
            policy.dead_lettered(env_id)
            return "dead"
        logger.error(
            "edax dispatcher: poison message %s with no on_dead wired; nacking it after a backoff step",
            env_id,
        )
        await self._nack_after_backoff(delivery, verdict.step)
        return "nack"

    async def _nack_after_backoff(self, delivery: Delivery, step: int) -> None:
        """Wait one step of the backoff schedule, then nack. Every failure arm waits: nacking at
        once has a handler on a downed dependency polling the broker as fast as it redelivers, and on a
        seeking transport every nack re-reads the partition tail. The wait blocks this worker, and
        therefore every key routed to it — per-key ordering forbids skipping ahead. A cancel during the
        wait nacks at once and re-raises, so a stopped worker strands nothing. A nack that raises
        leaves the delivery unsettled and propagates to the worker's catch-all."""
        try:
            await self._pacing.wait(step)
        except asyncio.CancelledError:
            await asyncio.shield(self._nack_quietly(delivery))
            raise
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

    async def _nack_quietly(self, delivery: Delivery) -> None:
        """Hand a delivery back on shutdown. A nack that fails is reported, never raised: the
        shutdown goes on, and the delivery stays unsettled until the transport redelivers it."""
        try:
            await delivery.nack()
        except Exception as e:  # noqa: BLE001 - reported to the observer; the shutdown goes on
            self._observer.settlement_failed(delivery.envelope.event.name, delivery.envelope.id, e)

    def _on_worker_done(self, task: asyncio.Task[None]) -> None:
        """Log a worker that ended while the dispatcher was meant to be running. Its keys are
        stranded from here on; :attr:`DispatcherStats.running` reads ``False`` until a restart."""
        if self._stopping:
            return
        error = None if task.cancelled() else task.exception()
        logger.error(
            "edax dispatcher: a worker ended while running; every key routed to it is stranded",
            exc_info=error,
        )

    def start(self) -> None:
        if self._tasks:
            # A second worker on one queue would consume it concurrently and break per-key order.
            raise RuntimeError("dispatcher already started")
        self._stopping = False
        self._tasks = [asyncio.create_task(self._run(q)) for q in self._queues]
        for task in self._tasks:
            task.add_done_callback(self._on_worker_done)

    async def submit(self, item: EventEnvelope | Delivery) -> None:
        """Submit a bare envelope (fire-and-forget) or a :class:`Delivery` (acked/nacked by outcome).
        Before :meth:`start` the item waits in its queue. After :meth:`stop` it is refused with
        :class:`RuntimeError`, because no worker would ever take it."""
        if self._stopping:
            raise RuntimeError("dispatcher is stopped: a submitted item would never be handled or settled")
        env = item.envelope if isinstance(item, Delivery) else item
        await self._queues[self._worker_index(env.aggregate_id)].put(item)

    async def stop(self, timeout: float | None = None) -> None:
        """Refuse further submits, let the workers finish what is queued, and wait up to ``timeout``
        seconds (``None`` waits for the drain). At the deadline, or if ``stop`` itself is cancelled, the
        workers still running are cancelled: a delivery a handler held is nacked, and so is every
        delivery still queued, so nothing submitted is left unsettled. Returns only once no worker is
        left, so a :meth:`start` after it never runs two workers on one queue. Idempotent."""
        self._stopping = True
        tasks = self._tasks
        if not tasks:
            return
        try:
            await asyncio.wait_for(self._drain(tasks), timeout)
        except asyncio.TimeoutError:
            logger.warning(
                "edax dispatcher: stop() passed its %ss deadline; cancelling %d worker(s)",
                timeout,
                sum(not task.done() for task in tasks),
            )
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._nack_queued()
            self._tasks = []

    async def _drain(self, tasks: list[asyncio.Task[None]]) -> None:
        # A dead worker takes no sentinel, and its queue may be full: skip it, or the put blocks.
        for queue, task in zip(self._queues, tasks, strict=True):
            if not task.done():
                await queue.put(_Stop.TOKEN)
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _nack_queued(self) -> None:
        """Hand back every delivery left in a queue once no worker is left to take it."""
        for queue in self._queues:
            while not queue.empty():
                item = queue.get_nowait()
                queue.task_done()
                if isinstance(item, Delivery):
                    await self._nack_quietly(item)

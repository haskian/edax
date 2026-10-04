"""Asyncio ordered-parallel consumer: same ordering key -> same worker (ordered), different keys
run concurrently."""

from __future__ import annotations

import asyncio
import enum
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from time import perf_counter_ns
from typing import Any, cast

from . import _edax, errors
from ._async import MaybeAwaitable, maybe_await
from .aggregate import CommandMessage, EventMessage, ordering_key
from .causation import _leave_every_lineage, caused_by
from .errors import CodecError, InProgress, _carried_code
from .observe import DeliveryOutcome, Observer, _wrap, error_type
from .pacing import Pacing
from .store import _dead_letter, _leave_every_unit_of_work, _retry_policy

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


# `Any`, not the message union: a handler is written against one kind, `router.dispatch` takes an
# `EventMessage` only, and a parameter narrower than the union would not be assignable.
Handler = Callable[[Any], MaybeAwaitable[None]]
AckFn = Callable[[], MaybeAwaitable[None]]
LiveFn = Callable[[], bool]
DeadLetterFn = Callable[["Delivery", BaseException], MaybeAwaitable[None]]


def _always_live() -> bool:
    """The liveness of a delivery whose transport never retires one it handed out."""
    return True


@dataclass
class Delivery:
    """A broker delivery: the message, an :class:`~edax.aggregate.EventMessage` or a
    :class:`~edax.aggregate.CommandMessage`, plus the handles that settle it. The transport handle (a Kafka
    offset, an SQS receipt) is per-delivery state, not stable across redelivery, so the adapter captures it
    in the ``on_ack`` / ``on_nack`` closures instead of on the message. A
    :class:`KeyOrderedDispatcher` acks on success and nacks if the handler raises.

    ``live`` tells whether the transport still counts on this delivery. A log-based transport (Kafka, the
    in-memory broker) retires every delivery it handed out from a partition a nack rewinds or a rebalance
    takes away, and fetches those messages again. A worker skips a delivery that is no longer live: no
    handler run and no settlement, so a later event of a key never runs ahead of the redelivered earlier
    one. ``origin`` is the transport's own record of the message, for a dead-letter sink that republishes
    it (a Kafka consumer sets a :class:`~edax.backend.kafka.KafkaFetched`); ``None`` when there is none."""

    message: EventMessage | CommandMessage
    on_ack: AckFn
    on_nack: AckFn
    attempt: int = 0  # the broker's own redelivery count if it keeps one, else 0; see max_attempts
    live: LiveFn = _always_live
    origin: Any = None
    #: Whether :meth:`ack` or :meth:`nack` has been called: a worker stopped before either hands it back.
    settled: bool = field(default=False, init=False)

    async def ack(self) -> None:
        self.settled = True
        await maybe_await(self.on_ack())

    async def nack(self) -> None:
        self.settled = True
        await maybe_await(self.on_nack())


class _Stop(enum.Enum):
    """Queue sentinel: the worker that takes it finishes and returns."""

    TOKEN = enum.auto()


# What a worker queue carries: a bare message, a settleable delivery, or the stop sentinel.
_QueueItem = EventMessage | CommandMessage | Delivery | _Stop


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
    is ``partition(ordering_key(msg), workers)``: the kernel's ordering key is the aggregate id, or a
    command's own id when it names no aggregate. One aggregate's messages stay on one worker in FIFO order
    while different aggregates progress in parallel. The handler runs under the ambient lineage
    :func:`~edax.causation.propagate` of the message, so what it saves continues the flow. ``max_queue``
    bounds each worker's queue for backpressure (``submit`` awaits when full); ``0`` makes it unbounded.
    Per-key order holds through a nack on a transport that rewinds, as a log does: the nack seeks the
    partition back, the transport retires the deliveries of it already handed out, and a worker skips a
    delivery that is no longer :attr:`~Delivery.live`, so the redelivered earlier event runs first. On a
    transport that requeues a nacked delivery behind later ones instead, a key that nacks is reordered
    against itself.

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
    no-op observer. A bare message is fire-and-forget: nothing settles it, so nothing is reported.
    ``workers < 1`` raises :class:`ValueError`, and so does a ``max_attempts`` the kernel cannot carry
    (negative, or past an unsigned 32-bit integer).
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
        # The kernel's retry and dead-letter policy: when a failure is handed back, after which backoff
        # step, and when it dies. Entries for nacked messages that never come back stay until the id is
        # seen again.
        self._retries = _retry_policy(max_attempts, on_dead is not None, "KeyOrderedDispatcher")
        # The policy's map sizes as plain values, copied after every settlement on the loop's thread, so
        # `stats()` never touches the native handle: read from a metrics thread first, it would bind
        # the handle to that thread and fail every delivery after.
        self._tracked = _edax.RetriesTracked(0, 0)
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

    def stats(self) -> DispatcherStats:
        """A snapshot for a health check or a gauge; cheap and without locking. It reads plain values
        only, so a metrics thread may call it too; read off the loop, it may be one settlement behind."""
        alive = sum(not task.done() for task in self._tasks)
        tracked = self._tracked
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
        no handler may see (a payload that will not decode) routes it here, the same exit a terminal
        handler failure takes. Set at construction: workers read it, so it is not swapped mid-run."""
        return self._on_dead

    @property
    def observer(self) -> Observer:
        """The observer this dispatcher reports to, guarded. The consumer that feeds it reports its own
        faults here too: a failed poll, a failed commit, a revoke past its deadline."""
        return self._observer

    @property
    def pacing(self) -> Pacing:
        """How a nack waits; a :class:`~edax.broker.Consumer` loop waits the same way after a failed poll."""
        return self._pacing

    def _worker_index(self, msg: EventMessage | CommandMessage) -> int:
        with errors.mapped():
            return _edax.partition(ordering_key(msg), len(self._queues))

    async def _run(self, queue: asyncio.Queue[_QueueItem]) -> None:
        # A task copies its creator's context, so a dispatcher started inside `transaction` would hand every
        # handler that caller's tx from a second task, which the Transactor contract forbids. The same
        # holds for the caller's lineage: each handler gets its own message's.
        _leave_every_unit_of_work()
        _leave_every_lineage()
        while True:
            item = await queue.get()
            if item is _Stop.TOKEN:
                queue.task_done()
                return
            # A raising handler (or ack/nack) must not kill the worker and strand its queue.
            self._in_flight += 1
            try:
                if isinstance(item, Delivery):
                    # A cancel that stops this worker reaches `_settle`, which hands the delivery back.
                    await self._settle(item)
                else:
                    # Bare message: fire-and-forget, so no settlement and no `Observer.delivery` either;
                    # a host wanting numbers for this path times the handler with `observe.timing`.
                    await self._handle(item)
            except Exception:
                # Named per arm: a bare message is fire-and-forget, so nothing settles it and
                # "settling failed" would send an operator to the wrong layer.
                what = "settling a delivery" if isinstance(item, Delivery) else "a fire-and-forget handler"
                logger.exception("edax dispatcher: %s failed; worker continues", what)
            finally:
                self._in_flight -= 1
                queue.task_done()

    async def _handle(self, msg: EventMessage | CommandMessage) -> None:
        """Run the handler under ``msg``'s lineage, counted in :attr:`DispatcherStats.handling` while it
        runs."""
        self._handling += 1
        try:
            with caused_by(msg):
                await maybe_await(self.handler(msg))
        finally:
            self._handling -= 1

    async def _settle(self, delivery: Delivery) -> None:
        """Run the handler, settle the delivery by its outcome, and report that outcome once. The
        duration is taken before any settlement, so it covers the handler only and a latency histogram
        over it is not polluted by the backoff wait."""
        attempt = self._retries.get().attempt(delivery.message.id, delivery.attempt)
        if not delivery.live():
            # A rewind or a rebalance retired it before this worker took it: the transport fetches it
            # again behind the earlier event of its key, so running it now would reorder the key.
            self._report(delivery, "stale", attempt, 0, None)
            return
        started = perf_counter_ns()
        duration_ns: int | None = None
        cause: str | None = None
        # A settlement that raises (the ack, the nack) propagates to the worker's catch-all with the
        # delivery neither acked nor nacked; reported as such so the arm is visible, not silent.
        outcome: DeliveryOutcome = "unsettled"
        try:
            error: BaseException | None = None
            try:
                await self._handle(delivery.message)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as e:
                if isinstance(e, asyncio.CancelledError) and _worker_cancelled():
                    raise  # this worker is being stopped, not the handler failing: handed back below
                # Any other BaseException (a `CancelledError` from a future someone else cancelled, a
                # native `PanicException`) fails this delivery only. Letting it end the worker would
                # strand every key routed to it, with nothing to notice.
                error = e
            duration_ns = perf_counter_ns() - started
            cause = error_type(error) if error is not None else None
            if not delivery.live():
                # Retired while the handler ran: its ack or nack would settle nothing, and a dead-letter
                # would be repeated by the redelivered copy. That copy settles it instead.
                outcome = "stale"
                return
            outcome = await self._ack_nack_or_dead(delivery, error, attempt)
        except asyncio.CancelledError:
            # This worker is being stopped mid-settlement, in the handler, the sink or a backoff wait:
            # hand the delivery back before the cancel ends the worker, so it is redelivered rather
            # than stranded. One that already settled has nothing left to hand back.
            if not delivery.settled and await asyncio.shield(self._nack_quietly(delivery)):
                outcome = "stopped"
            raise
        finally:
            self._tracked = self._retries.get().tracked()
            if duration_ns is None:  # the handler itself was cut short
                duration_ns = perf_counter_ns() - started
            self._report(delivery, outcome, attempt, duration_ns, cause)

    def _report(
        self, delivery: Delivery, outcome: DeliveryOutcome, attempt: int, duration_ns: int, cause: str | None
    ) -> None:
        msg = delivery.message
        self._observer.delivery(msg.name, msg.id, msg.correlation_id, outcome, attempt, duration_ns, cause)

    async def _ack_nack_or_dead(
        self, delivery: Delivery, error: BaseException | None, attempt: int
    ) -> DeliveryOutcome:
        """Settle by the handler's outcome as the kernel's retry policy answers, and return which arm settled
        it: ``ack``, ``nack``, ``dead``, ``in_progress``, ``sink_failed``, or ``unsettled`` for an ack that
        did not land (a raising nack propagates instead)."""
        msg_id = delivery.message.id
        policy = self._retries.get()
        if error is None:
            acked = await self._ack_or_log(delivery)
            policy.succeeded(msg_id)
            return "ack" if acked else "unsettled"
        # The kernel reads the failure's code: `in_progress` is contention, which waits its own step and
        # never counts toward the ceiling, since nacking at once would poll the broker until the claim's
        # TTL. A BaseException that is not an Exception (a cancel from elsewhere, a native panic) carries
        # no code: nothing says it repeats, so it is retried like a transient failure.
        code = _carried_code(error) if isinstance(error, Exception) else None
        verdict = policy.failed(msg_id, delivery.attempt, code)
        if verdict.dead:
            if not await self._sink(delivery, error, verdict.attempt):
                return "sink_failed"
            acked = await self._ack_or_log(delivery)
            return "dead" if acked else "unsettled"
        # Whether redelivery reorders against already-acked same-aggregate messages is the broker's. Every
        # failure that is handed back waits one step of the kernel's schedule first.
        await self._nack_after_backoff(delivery, verdict.step)
        return "in_progress" if isinstance(error, InProgress) else "nack"

    async def _sink(self, delivery: Delivery, error: BaseException, attempt: int) -> bool:
        """Hand ``delivery`` to ``on_dead``; return whether the sink took it. A sink that raises nacks the
        delivery after one backoff step, the transient path, and the policy keeps the attempt so the
        redelivery reaches the sink again with a longer wait. Leaving it unsettled instead would pin a
        log-based broker for good: nothing redelivers a delivery while its consumer lives."""
        # The kernel answers `dead` only to a policy built with a sink, so a dead verdict means one is set.
        on_dead = cast("DeadLetterFn", self._on_dead)
        policy = self._retries.get()
        step = await _dead_letter(
            logger, policy, delivery.message.id, attempt, lambda: on_dead(delivery, error)
        )
        if step is None:
            return True
        await self._nack_after_backoff(delivery, step)
        return False

    async def settle_poison(self, delivery: Delivery, error: BaseException) -> DeliveryOutcome:
        """Settle a polled message no handler may see (a payload that will not decode) the way a
        terminal handler failure settles: ``on_dead``, then ack. With no sink, or a sink that raises, it is
        nacked after one backoff step, counted by its id like a failed attempt, so one poison message
        cannot spin the consumer against the broker. Reported through
        :meth:`~edax.observe.Observer.undecodable`. Returns ``dead``, ``nack`` or ``sink_failed``.

        A :class:`~edax.broker.Consumer` loop calls this in place of :meth:`submit`; routing the message
        through :meth:`submit` would hand a handler a message with no payload it can read."""
        msg_id = delivery.message.id
        self._observer.undecodable(msg_id, error)
        try:
            # A payload that will not decode fails alike on every delivery: terminal by construction,
            # whatever exception the decoder raised.
            verdict = self._retries.get().failed(msg_id, delivery.attempt, CodecError.code)
            if not verdict.dead:
                logger.error(
                    "edax dispatcher: poison message %s with no on_dead wired; nacking it after a backoff",
                    msg_id,
                )
                await self._nack_after_backoff(delivery, verdict.step)
                return "nack"
            if not await self._sink(delivery, error, verdict.attempt):
                return "sink_failed"
            await delivery.ack()
            return "dead"
        except asyncio.CancelledError:
            # The consumer loop is stopped mid-settlement, in the sink or the backoff wait: hand the
            # message back before the cancel ends it, as a worker does.
            if not delivery.settled:
                await asyncio.shield(self._nack_quietly(delivery))
            raise
        finally:
            self._tracked = self._retries.get().tracked()

    async def _nack_after_backoff(self, delivery: Delivery, step: int) -> None:
        """Wait one step of the backoff schedule, then nack. Every failure arm waits: nacking at
        once has a handler on a downed dependency polling the broker as fast as it redelivers, and on a
        seeking transport every nack re-reads the partition tail. The wait blocks this worker, and
        therefore every key routed to it: per-key ordering forbids skipping ahead. A cancel during the
        wait reaches the worker, which nacks the delivery before it ends, so a stopped worker strands
        nothing. A nack that raises leaves the delivery unsettled and propagates to the worker's
        catch-all."""
        await self._pacing.wait(step)
        try:
            await delivery.nack()
        except Exception as e:
            self._observer.settlement_failed(delivery.message.name, delivery.message.id, e)
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
            self._observer.settlement_failed(delivery.message.name, delivery.message.id, e)
            logger.warning(
                "edax dispatcher: ack of %s failed after a successful handler; the broker will redeliver it",
                delivery.message.id,
                exc_info=True,
            )
            return False
        return True

    async def _nack_quietly(self, delivery: Delivery) -> bool:
        """Hand a delivery back on shutdown; return whether the nack landed. A nack that fails is
        reported, never raised: the shutdown goes on, and the delivery stays unsettled until the
        transport redelivers it."""
        try:
            await delivery.nack()
        except Exception as e:  # noqa: BLE001 - reported to the observer; the shutdown goes on
            self._observer.settlement_failed(delivery.message.name, delivery.message.id, e)
            return False
        return True

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

    async def submit(self, item: EventMessage | CommandMessage | Delivery) -> None:
        """Submit a bare message (fire-and-forget) or a :class:`Delivery` (acked/nacked by outcome).
        Before :meth:`start` the item waits in its queue. After :meth:`stop` it is refused with
        :class:`RuntimeError`, because no worker would ever take it."""
        if self._stopping:
            raise RuntimeError("dispatcher is stopped: a submitted item would never be handled or settled")
        msg = item.message if isinstance(item, Delivery) else item
        await self._queues[self._worker_index(msg)].put(item)

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
        drain = asyncio.ensure_future(self._drain(tasks))
        try:
            # `wait`, not `wait_for`: at the deadline it cancels nothing, so the count below is still true.
            done, _ = await asyncio.wait({drain}, timeout=timeout)
            if not done:
                logger.warning(
                    "edax dispatcher: stop() passed its %ss deadline; cancelling %d worker(s)",
                    timeout,
                    sum(not task.done() for task in tasks),
                )
        finally:
            drain.cancel()
            for task in tasks:
                task.cancel()
            await asyncio.gather(drain, *tasks, return_exceptions=True)
            await self._nack_queued()
            self._tasks = []

    async def _drain(self, tasks: list[asyncio.Task[None]]) -> None:
        # A dead worker takes no sentinel, and its queue may be full: skip it, or the put blocks.
        for queue, task in zip(self._queues, tasks, strict=True):
            if not task.done():
                await queue.put(_Stop.TOKEN)
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _nack_queued(self) -> None:
        """Hand back every delivery left in a queue once no worker is left to take it, and report it
        ``stopped``: its handler never ran."""
        for queue in self._queues:
            while not queue.empty():
                item = queue.get_nowait()
                queue.task_done()
                if isinstance(item, Delivery):
                    attempt = self._retries.get().attempt(item.message.id, item.attempt)
                    outcome: DeliveryOutcome = "stopped" if await self._nack_quietly(item) else "unsettled"
                    self._report(item, outcome, attempt, 0, None)

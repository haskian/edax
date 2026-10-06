"""Asyncio ordered-parallel consumer: same ordering key -> same worker (ordered), different keys
run concurrently."""

from __future__ import annotations

import asyncio
import contextlib
import enum
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from time import perf_counter_ns
from typing import Any, Generic

from . import _names, _native
from ._async import MaybeAwaitable, maybe_await
from ._lineage import leave_every_lineage
from ._observer import wrap
from ._settle import Dead, Nack, Stop, dead_letter, is_work_failure, retry_policy, retry_verdict
from ._units import leave_every_unit_of_work
from ._wiring import wiring_int, wiring_u32
from .causation import caused_by
from .delivery import DeadLetter, DeadLetterSink, Delivery, M, Undecodable
from .errors import BadHandle, CodecError, code_of
from .message import ordering_key
from .observe import DeliveryOutcome, Observer, error_type
from .pacing import Pacing

__all__ = [
    "DispatcherStats",
    "DispatcherStopped",
    "Handler",
    "KeyOrderedDispatcher",
]


logger = logging.getLogger("edax.dispatcher")


class DispatcherStopped(RuntimeError):
    """:meth:`KeyOrderedDispatcher.submit` after :meth:`~KeyOrderedDispatcher.stop`: no worker would ever take
    the item. A consumer loop that polled a delivery hands it back to the transport instead."""


#: A handler of the message kind ``M`` a dispatcher carries: ``router.dispatch`` is a
#: ``Handler[EventMessage]``, a command consumer's is a ``Handler[CommandMessage]``. Async, as an
#: :class:`~edax.router.EventRouter` handler and a command handler are, so no blocking call hides in the loop.
Handler = Callable[[M], Awaitable[None]]
#: What a dispatcher settles: a delivery its handler runs, or a poison record it dead-letters unrun.
_Settled = Delivery[Any] | Undecodable


class _Stop(enum.Enum):
    """Queue sentinel: the worker that takes it finishes and returns."""

    TOKEN = enum.auto()


# What a worker queue carries: a delivery, or the stop sentinel.
_QueueItem = Delivery[Any] | _Stop


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


class KeyOrderedDispatcher(Generic[M]):
    """Fans one broker partition or key-shared subscription out to a local pool of asyncio workers. It carries
    one message kind ``M``, the one its handler takes: ``KeyOrderedDispatcher[EventMessage]`` behind an
    ``EventRouter``, ``KeyOrderedDispatcher[CommandMessage]`` behind a command consumer. Worker
    is ``partition(ordering_key(msg), workers)``: the kernel's ordering key is the aggregate id, or a
    command's own id when it names no aggregate. One aggregate's messages stay on one worker in FIFO order
    while different aggregates progress in parallel. The handler runs under the ambient lineage
    :func:`~edax.causation.propagate` of the message, so what it saves continues the flow. ``max_queue``
    bounds each worker's queue for backpressure (``submit`` awaits when full); ``0`` makes it unbounded.
    Per-key order holds through a nack on a transport that rewinds, as a log does: the nack seeks the
    partition back, the transport retires the deliveries of it already handed out, and a worker skips a
    delivery that is no longer :attr:`~edax.delivery.Delivery.live`, so the redelivered earlier event runs
    first. On a transport that requeues a nacked delivery behind later ones instead, a key that nacks is
    reordered against itself.

    A raising handler nacks unless ``on_dead`` is set. With it, a terminal error dead-letters (``on_dead``
    takes a :class:`~edax.delivery.DeadLetter`) on the first attempt whatever ``max_attempts`` says, and a
    transient one at the ``max_attempts``-th failed attempt (``0`` disables the ceiling). A sink that raises
    nacks the delivery the same way, so the redelivery gives it another try. Every nack waits one step of
    ``pacing`` first (default: the kernel's jittered backoff on ``asyncio.sleep``), or a handler failing on
    a downed dependency polls the broker as fast as it redelivers.
    :class:`~edax.errors.InProgress` waits the same way but never counts, because it is contention rather
    than a failed attempt. The wait blocks that worker and every key routed to it, because per-key ordering
    forbids skipping ahead. Attempt counting and sink behaviour: ``docs/sdk/consuming.md``.

    ``observer`` receives one :meth:`~edax.observe.Observer.delivery` per settled delivery and a
    :meth:`~edax.observe.Observer.settlement_failed` for an ack or nack that raised; ``None`` is the
    no-op observer.
    ``workers`` below 1 or not an ``int``, and a ``max_attempts`` the kernel cannot carry (not an int,
    negative, or past an unsigned 32-bit integer), raise the terminal :class:`~edax.errors.BadHandle`, as
    the Rust SDK's ``build`` refuses zero workers.

    A fatal failure (:func:`~edax.errors.is_fatal`: a wiring fault such as a refused credential) stops the
    dispatcher: the delivery is nacked at once, uncounted and never dead-lettered, every worker hands back
    what it has not run (reported ``stopped``), and :meth:`submit` raises the failure, so the consumer loop
    feeding it ends on it instead of moving the topic into the dead-letter queue one message at a time.
    An ``on_dead`` that raises a fatal error stops it the same way, on the sink's error.
    """

    def __init__(
        self,
        workers: int,
        handler: Handler[M],
        *,
        max_queue: int = 1024,
        max_attempts: int = 0,
        on_dead: DeadLetterSink[M | Undecodable] | None = None,
        observer: Observer | None = None,
        pacing: Pacing | None = None,
    ) -> None:
        if wiring_int(workers, "workers") < 1:
            raise BadHandle(f"a dispatcher needs at least one worker, got {workers}")
        self._handler = handler
        self._pacing = pacing or Pacing()
        # The kernel's retry and dead-letter policy: when a failure is handed back, after which backoff
        # step, and when it dies. Entries for nacked messages that never come back stay until the id is
        # seen again.
        self._retries = retry_policy(max_attempts, on_dead is not None, "KeyOrderedDispatcher")
        # The policy's map sizes as plain values, copied on the loop's thread after every settlement.
        # `stats()` reads these and never touches the native handle. Read from a metrics thread first,
        # the handle would bind to that thread and fail every delivery after.
        self._tracked = _native.RetriesTracked(0, 0)
        self._on_dead = on_dead
        self._observer = wrap(observer)
        self._queues: list[asyncio.Queue[_QueueItem]] = [
            asyncio.Queue[_QueueItem](maxsize=max_queue) for _ in range(workers)
        ]
        self._tasks: list[asyncio.Task[None]] = []
        # Set by `stop()` and cleared by `start()`: a submit in between would queue an item no worker
        # will ever take, so it is refused.
        self._stopping = False
        # Deliveries taken off a queue that have not settled.
        # `qsize()` stops counting an item the moment a worker takes it, so `idle()` needs this too.
        self._in_flight = 0
        self._handling = 0
        # Set while no handler runs, so a rebalance waits for the running ones without polling.
        self._no_handler = asyncio.Event()
        self._no_handler.set()
        # Submits admitted before `stop()` whose `put` has not landed (a full queue). `stop()` waits for
        # them, or one would land in a queue no worker reads, neither handled nor settled.
        self._putting = 0
        # The first fatal failure a handler raised. Once set, workers hand back what they have not run and
        # `submit` raises it: every message would fail alike until the wiring is fixed.
        self._fatal: BaseException | None = None

    def stats(self) -> DispatcherStats:
        """A snapshot for a health check or a gauge; cheap and without locking. It reads plain values
        only, so a metrics thread may call it too. Read off the loop, it may be one settlement behind."""
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
    def on_dead(self) -> DeadLetterSink[M | Undecodable] | None:
        """The dead-letter sink, or ``None``. It takes a :class:`~edax.delivery.DeadLetter` of the message
        that died, or of the :class:`~edax.delivery.Undecodable` poison record a consumer loop settles
        through :meth:`settle_poison`: the same exit a terminal handler failure takes. Set at
        construction: workers read it, so it is not swapped mid-run."""
        return self._on_dead

    async def wait_handlers(self, timeout: float) -> int:
        """Wait until no handler is running, or ``timeout`` seconds pass; return how many still run, ``0``
        once none does. A rebalance waits on it before another member takes the keys. Woken by each handler
        that ends, so it neither polls nor waits past the last one."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        # A woken wait checks again: another handler may have started between the set that woke it and
        # its resume, and a count read then would report an overrun long before the deadline.
        while self._handling and (left := deadline - loop.time()) > 0:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._no_handler.wait(), left)
        # At the deadline the count says how many handlers still run.
        return self._handling

    @property
    def observer(self) -> Observer:
        """The observer this dispatcher reports to, guarded. The consumer that feeds it reports its own
        faults here too: a failed poll, a failed commit, a revoke past its deadline."""
        return self._observer

    @property
    def pacing(self) -> Pacing:
        """How a nack waits; a :class:`~edax.broker.Consumer` loop waits the same way after a failed poll."""
        return self._pacing

    def _worker_index(self, msg: M) -> int:
        return _native.partition(ordering_key(msg), len(self._queues))

    async def _run(self, queue: asyncio.Queue[_QueueItem]) -> None:
        # A task copies its creator's context, so a dispatcher started inside `transaction` would hand every
        # handler that caller's tx from a second task, which the Transactor contract forbids. The same
        # holds for the caller's lineage: each handler gets its own message's.
        leave_every_unit_of_work()
        leave_every_lineage()
        while True:
            item = await queue.get()
            if item is _Stop.TOKEN:
                queue.task_done()
                return
            # A raising handler (or ack/nack) must not kill the worker and strand its queue.
            self._in_flight += 1
            try:
                # A cancel that stops this worker reaches `_settle`, which hands the delivery back.
                await self._settle(item)
            except Exception:
                logger.exception("edax dispatcher: settling a delivery failed; worker continues")
            finally:
                self._in_flight -= 1
                queue.task_done()

    async def _handle(self, msg: M) -> None:
        """Run the handler under ``msg``'s lineage, counted in :attr:`DispatcherStats.handling` while it
        runs."""
        self._handling += 1
        self._no_handler.clear()
        try:
            with caused_by(msg):
                # Typed async; an untyped host's sync handler still settles rather than fail every delivery.
                outcome: MaybeAwaitable[None] = self._handler(msg)
                await maybe_await(outcome)
        finally:
            self._handling -= 1
            if not self._handling:
                self._no_handler.set()

    async def _settle(self, delivery: Delivery[M]) -> None:
        """Run the handler, settle the delivery by its outcome, and report that outcome once. The
        duration is taken before any settlement, so it covers the handler only and a latency histogram
        over it is not polluted by the backoff wait."""
        attempt = self._retries.get().attempt(delivery.message.id, delivery.attempt)
        if not delivery.live():
            # A rewind or a rebalance retired it before this worker took it: the transport fetches it
            # again behind the earlier event of its key, so running it now would reorder the key.
            self._report(delivery, "retired", attempt, 0, None)
            return
        if self._fatal is not None:
            # Stopped by a fatal failure: this delivery would fail alike, so it goes back unrun.
            handed_back = await self._nack_quietly(delivery)
            self._report(delivery, "stopped" if handed_back else "unsettled", attempt, 0, None)
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
            except BaseException as e:
                if not is_work_failure(e):
                    raise  # an interrupt, or this worker being stopped: handed back below
                error = e
            duration_ns = perf_counter_ns() - started
            cause = error_type(error) if error is not None else None
            if not delivery.live():
                # Retired while the handler ran: its ack or nack would settle nothing, and a dead-letter
                # would be repeated by the redelivered copy. That copy settles it instead.
                outcome = "retired"
                return
            outcome = await self._ack_nack_or_dead(delivery, error, attempt)
        except asyncio.CancelledError:
            # This worker is being stopped mid-settlement, in the handler, the sink or a backoff wait.
            if await self._hand_back(delivery):
                outcome = "stopped"
            raise
        finally:
            self._tracked = self._retries.get().tracked()
            if duration_ns is None:  # the handler itself was cut short
                duration_ns = perf_counter_ns() - started
            self._report(delivery, outcome, attempt, duration_ns, cause)

    def _report(
        self,
        delivery: Delivery[Any],
        outcome: DeliveryOutcome,
        attempt: int,
        duration_ns: int,
        cause: str | None,
    ) -> None:
        msg = delivery.message
        self._observer.delivery(msg.name, msg.id, msg.correlation_id, outcome, attempt, duration_ns, cause)

    async def _ack_nack_or_dead(
        self, delivery: Delivery[M], error: BaseException | None, attempt: int
    ) -> DeliveryOutcome:
        """Settle by the handler's outcome as the kernel's retry policy answers, and return which arm settled
        it: ``ack``, ``nack``, ``dead``, ``in_progress``, ``sink_failed``, ``stopped``, or ``unsettled`` for
        an ack that did not land (a raising nack propagates instead)."""
        if error is None:
            acked = await self._ack_or_log(delivery)
            self._retries.get().succeeded(delivery.message.id)
            return "ack" if acked else "unsettled"
        # The kernel reads the failure's code: `in_progress` is contention, which waits its own step and
        # never counts toward the ceiling, since nacking at once would poll the broker until the claim's
        # TTL. A failure with no code (a cancel from elsewhere, a native panic) is retried as transient.
        return await self._failed(delivery, error, code_of(error))

    async def _failed(self, item: _Settled, error: BaseException, code: str | None) -> DeliveryOutcome:
        """Settle a failed delivery, or a poison record, as the kernel's retry policy answers for ``code``:
        dead-letter then ack, nack after one backoff step, or stop the dispatcher and nack at once. Returns
        ``dead``, ``nack``, ``in_progress``, ``sink_failed``, ``stopped``, or ``unsettled`` for an ack that
        did not land; a raising nack propagates."""
        match retry_verdict(self._retries.get().failed(item.id, item.attempt, code)):
            case Dead(attempt=attempt):
                return await self._dead_letter(item, error, attempt)
            case Stop():
                return await self._stop(item, error)
            case Nack(step=step, contended=contended):
                # Whether redelivery reorders against already-acked same-aggregate messages is the broker's.
                # Every failure that is handed back waits one step of the kernel's schedule first.
                await self._nack_after_backoff(item, step)
                # The kernel names the failure it read: contention is reported apart from a counted nack.
                return "in_progress" if contended else "nack"
            case unknown:
                _names.unreachable(unknown)

    async def _dead_letter(self, item: _Settled, error: BaseException, attempt: int) -> DeliveryOutcome:
        """Hand ``item`` to ``on_dead`` as a :class:`~edax.delivery.DeadLetter`, then ack it: ``dead``, or
        ``unsettled`` for an ack that did not land. A sink that raises nacks the delivery after one backoff
        step, the transient path, and the policy keeps the attempt so the redelivery reaches the sink again
        with a longer wait: ``sink_failed``.
        Leaving it unsettled instead would pin a log-based broker for good: nothing redelivers a delivery
        while its consumer lives. A sink that raises a fatal error stops the dispatcher like a fatal handler
        failure: ``stopped``."""
        dead = DeadLetter(item.dead_message, attempt, error, item.origin)
        refusal, step = await dead_letter(logger, self._on_dead, dead, item.id, self._retries.get().sunk)
        if refusal is None:
            acked = await self._ack_or_log(item)
            return "dead" if acked else "unsettled"
        if step is None:
            return await self._stop(item, refusal)
        await self._nack_after_backoff(item, step)
        return "sink_failed"

    async def _stop(self, item: _Settled, error: BaseException) -> DeliveryOutcome:
        """Stop the dispatcher on the fatal ``error`` and nack ``item`` at once: ``stopped``."""
        # Recorded before the nack, so a worker that takes the next delivery meanwhile hands it back unrun.
        # No backoff step: nothing is redelivered to this dispatcher until the wiring is fixed.
        if self._fatal is None:
            self._fatal = error
        await self._nack(item)
        return "stopped"

    async def settle_poison(self, poison: Undecodable) -> DeliveryOutcome:
        """Settle a polled record no handler may see (a payload that will not decode) the way a terminal
        handler failure settles: ``on_dead`` gets it as the :class:`~edax.delivery.DeadLetter` of the
        :class:`~edax.delivery.Undecodable`, then it is acked. With no sink, or a sink that raises, it is
        nacked after one backoff step, counted by its id like a failed attempt, so one poison message
        cannot spin the consumer against the broker. Reported through
        :meth:`~edax.observe.Observer.undecodable`. Returns ``dead``, ``nack``, ``sink_failed``, or
        ``unsettled`` for an ack that did not land, which :meth:`~edax.observe.Observer.settlement_failed`
        reports.

        It always settles the poison, so the caller never hands it back. A sink that raises a fatal error
        stops the dispatcher: the poison is nacked at once and that error raised. After a fatal failure,
        or after :meth:`stop`, the sink is never called: the poison is nacked and the fatal failure, or
        :class:`DispatcherStopped`, raised, as :meth:`submit` refuses a delivery.

        :func:`~edax.broker.consume` calls this for every :class:`~edax.delivery.Undecodable` a consumer
        polls, in place of :meth:`submit`: no handler could read it."""
        refusal = self._refusal()
        if refusal is not None:
            # A fatal failure may have shown the sink itself to be broken, and a stopped dispatcher takes
            # no more work: the poison goes back for the consumer that runs once the wiring is fixed.
            await self._nack_quietly(poison)
            raise refusal
        self._observer.undecodable(poison.id, poison.error)
        if self._on_dead is None:
            logger.error(
                "edax dispatcher: poison message %s with no on_dead wired; nacking it after a backoff",
                poison.id,
            )
        try:
            # A payload that will not decode fails alike on every delivery: terminal by construction,
            # whatever exception the decoder raised.
            outcome = await self._failed(poison, poison.error, CodecError.code)
        except asyncio.CancelledError:
            # The consumer loop is stopped mid-settlement, in the sink or the backoff wait.
            await self._hand_back(poison)
            raise
        finally:
            self._tracked = self._retries.get().tracked()
        if outcome == "stopped" and self._fatal is not None:
            # The sink refused fatally and `_stop` already nacked the poison: every poison after it would
            # meet the same sink, so the consumer loop stops on it, as `submit` makes it stop.
            raise self._fatal
        return outcome

    async def _nack_after_backoff(self, item: _Settled, step: int) -> None:
        """Wait one step of the backoff schedule, then nack. Every failure arm waits: nacking at
        once has a handler on a downed dependency polling the broker as fast as it redelivers, and on a
        seeking transport every nack re-reads the partition tail. The wait blocks this worker, and
        therefore every key routed to it: per-key ordering forbids skipping ahead. A cancel during the
        wait reaches the worker, which nacks the delivery before it ends, so a stopped worker strands
        nothing. A nack that raises leaves the delivery unsettled and propagates to the worker's
        catch-all."""
        await self._pacing.wait(step)
        await self._nack(item)

    async def _nack(self, item: _Settled) -> None:
        """Nack at once. A nack that raises is reported, leaves the delivery unsettled and propagates to
        the worker's catch-all."""
        try:
            await item.nack()
        except Exception as e:
            self._observer.settlement_failed(item.name, item.id, e)
            raise

    async def _ack_or_log(self, item: _Settled) -> bool:
        """Ack, or report the failure as what it is: a settlement problem, not a handler failure.
        Returns whether the ack landed.

        The delivery was already settled, by a handler that did its work or by the dead-letter sink, so this
        is not ``on_dead``'s business either: a dead-letter queue would then hold a message twice, or one
        that was handled successfully. Unsettled means the broker redelivers, and an idempotent handler
        absorbs that. Swallowed here rather than in the worker's catch-all so the bookkeeping below it
        still runs and the log names the real cause.
        """
        try:
            await item.ack()
        except Exception as e:
            self._observer.settlement_failed(item.name, item.id, e)
            logger.warning(
                "edax dispatcher: ack of %s failed; the broker will redeliver it", item.id, exc_info=True
            )
            return False
        return True

    async def _hand_back(self, item: _Settled) -> bool:
        """Nack ``item`` before a cancel ends its settlement, so it is redelivered rather than stranded;
        return whether it was handed back. One that already settled has nothing left to hand back."""
        return not item.settled and await asyncio.shield(self._nack_quietly(item))

    async def _nack_quietly(self, item: _Settled) -> bool:
        """Hand a delivery back on shutdown; return whether the nack landed. A nack that fails is
        reported, never raised: the shutdown goes on, and the delivery stays unsettled until the
        transport redelivers it."""
        try:
            await item.nack()
        except Exception as e:  # noqa: BLE001 - reported to the observer; the shutdown goes on
            self._observer.settlement_failed(item.name, item.id, e)
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
        # A restart is the host's answer to a fatal failure: the wiring was fixed.
        self._fatal = None
        self._tasks = [asyncio.create_task(self._run(q)) for q in self._queues]
        for task in self._tasks:
            task.add_done_callback(self._on_worker_done)

    async def submit(self, delivery: Delivery[M]) -> None:
        """Submit a :class:`Delivery`, acked, nacked or dead-lettered by the handler's outcome. There is no
        fire-and-forget form: a handler failure always reaches a settlement, never a log line alone. A
        test that watches only the handler submits :func:`edax.testing.delivery`.
        Before :meth:`start` the delivery waits in its queue. After :meth:`stop` it is refused with
        :class:`DispatcherStopped`, because no worker would ever take it. After a fatal failure it is refused
        with that failure, for the caller to stop on; the caller still owns the delivery and hands it back. A
        ``Delivery.attempt`` the kernel cannot carry (not an unsigned 32-bit integer) is refused the same way,
        with the fatal :class:`~edax.errors.BadHandle`: the transport adapter counts wrong."""
        refusal = self._refusal()
        if refusal is not None:
            raise refusal
        if not isinstance(delivery, Delivery):
            # A bare message has nothing to settle it, so its handler's failure would go nowhere.
            raise BadHandle(
                f"submit takes a Delivery, got {type(delivery).__name__}: wrap it, or use testing.delivery"
            )
        # The kernel counts attempts as an unsigned 32-bit integer. Refused here, while the caller still owns
        # the delivery: a worker would fail on it before any settlement and pin a log partition.
        wiring_u32(delivery.attempt, "Delivery.attempt")
        queue = self._queues[self._worker_index(delivery.message)]
        self._putting += 1
        try:
            await queue.put(delivery)
        finally:
            self._putting -= 1

    def _refusal(self) -> BaseException | None:
        """Why this dispatcher takes no more work: the fatal failure that stopped it, or
        :class:`DispatcherStopped` after :meth:`stop`; ``None`` while it runs."""
        if self._fatal is not None:
            return self._fatal
        if self._stopping:
            return DispatcherStopped(
                "dispatcher is stopped: a submitted item would never be handled or settled"
            )
        return None

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
        ``stopped``: its handler never ran. A submit blocked on a full queue lands only once a slot frees
        and its task runs again, so the drain repeats until every admitted submit has landed."""
        while True:
            for queue in self._queues:
                while not queue.empty():
                    item = queue.get_nowait()
                    queue.task_done()
                    if isinstance(item, Delivery):
                        attempt = self._retries.get().attempt(item.message.id, item.attempt)
                        outcome: DeliveryOutcome = (
                            "stopped" if await self._nack_quietly(item) else "unsettled"
                        )
                        self._report(item, outcome, attempt, 0, None)
            # Checked with no await before the return, so no submit lands unseen.
            if not self._putting and all(queue.empty() for queue in self._queues):
                return
            await asyncio.sleep(0)

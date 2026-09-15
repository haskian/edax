"""Asyncio ordered-parallel consumer: same aggregate id -> same worker (ordered), different ids
run concurrently."""

from __future__ import annotations

import asyncio
import enum
import logging
from dataclasses import dataclass
from typing import Callable, Optional, Union

from . import _edax
from ._async import MaybeAwaitable, maybe_await
from .aggregate import EventEnvelope
from .errors import InProgress, is_terminal
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
    """

    def __init__(
        self,
        workers: int,
        handler: Handler,
        *,
        max_queue: int = 1024,
        max_attempts: int = 0,
        on_dead: Optional[DeadLetterFn] = None,
    ) -> None:
        self.handler = handler
        self._max_attempts = max_attempts
        self._on_dead = on_dead
        self._queues: list[asyncio.Queue[_QueueItem]] = [
            asyncio.Queue[_QueueItem](maxsize=max_queue) for _ in range(max(1, workers))
        ]
        self._tasks: list[asyncio.Task[None]] = []
        # Entries for nacked messages that never come back stay until the id is seen again.
        self._attempts: dict[str, int] = {}
        # Backoff step per message losing a claim race. Separate from `_attempts`: contention is not a
        # failed attempt, and the same unbounded-entry caveat applies.
        self._contended: dict[str, int] = {}

    @property
    def on_dead(self) -> Optional[DeadLetterFn]:
        """The dead-letter sink, or ``None``. A :class:`~edax.broker.Consumer` that must settle a message
        no handler may see — a payload that will not decode — routes it here, the same exit a terminal
        handler failure takes. Set at construction: workers read it, so it is not swapped mid-run."""
        return self._on_dead

    def _worker_index(self, aggregate_id: str) -> int:
        return _edax.partition(aggregate_id, len(self._queues))

    async def _run(self, queue: asyncio.Queue[_QueueItem]) -> None:
        # A task copies its creator's context, so a dispatcher started inside `with_tx` would hand every
        # handler that caller's tx from a second task, which the Transactor contract forbids.
        _current_tx.set(None)
        while True:
            item = await queue.get()
            if item is _Stop.TOKEN:
                queue.task_done()
                return
            # A raising handler (or ack/nack) must not kill the worker and strand its queue.
            try:
                if isinstance(item, Delivery):
                    await self._settle(item)
                else:
                    await maybe_await(self.handler(item))  # bare envelope: no settlement
            except Exception:
                # Named per arm: a bare envelope is fire-and-forget, so nothing settles it and
                # "settling failed" would send an operator to the wrong layer.
                what = "settling a delivery" if isinstance(item, Delivery) else "a fire-and-forget handler"
                logger.exception("edax dispatcher: %s failed; worker continues", what)
            finally:
                queue.task_done()

    async def _settle(self, delivery: Delivery) -> None:
        """Run the handler, then ack, nack or dead-letter the delivery by outcome."""
        env_id = delivery.envelope.id
        try:
            await maybe_await(self.handler(delivery.envelope))
        except InProgress:
            # Contention, not a failed attempt: wait one backoff step, then redeliver without touching the
            # attempt budget. Nacking at once would poll the broker until the claim's TTL.
            step = self._contended.get(env_id, 0)
            self._contended[env_id] = step + 1
            await self._nack_after_backoff(delivery, step)
            return
        except Exception as e:  # noqa: BLE001 - handler failed -> nack or dead-letter
            self._contended.pop(env_id, None)
            attempt = max(delivery.attempt, self._attempts.get(env_id, 0)) + 1
            terminal = is_terminal(e)
            dead = terminal or (0 < self._max_attempts <= attempt)
            if dead and self._on_dead is not None:
                logger.warning(
                    "edax dispatcher: dead-lettering %s after %d attempt(s) (terminal=%s)",
                    env_id,
                    attempt,
                    terminal,
                )
                # on_dead raising propagates before any ack/nack: the broker redelivers.
                await maybe_await(self._on_dead(delivery, e))
                await self._ack_or_log(delivery)
                self._attempts.pop(env_id, None)
                return
            # Whether redelivery reorders against already-acked same-aggregate messages is the broker's.
            # Recorded whatever the ceiling, so the backoff below grows across redeliveries: with the
            # default `max_attempts=0` an unrecorded attempt is always 1 and the wait a flat first step.
            self._attempts[env_id] = attempt
            # Transient failure: waited the same way as the InProgress arm, one step of the kernel's
            # schedule before handing the delivery back.
            await self._nack_after_backoff(delivery, attempt - 1)
            return
        await self._ack_or_log(delivery)
        self._contended.pop(env_id, None)
        self._attempts.pop(env_id, None)

    async def _nack_after_backoff(self, delivery: Delivery, step: int) -> None:
        """Wait one step of the kernel's backoff schedule, then nack. Both failure arms wait: nacking at
        once has a handler on a downed dependency polling the broker as fast as it redelivers, and on a
        seeking transport every nack re-reads the partition tail. The wait blocks this worker, and
        therefore every key routed to it — per-key ordering forbids skipping ahead."""
        await _sleep(_edax.backoff_millis(step) / 1000)
        await delivery.nack()

    async def _ack_or_log(self, delivery: Delivery) -> None:
        """Ack, or report the failure as what it is: a settlement problem, not a handler failure.

        The handler already did its work, so this is not ``on_dead``'s business either — a dead-letter
        queue would then hold an event that was handled successfully. Unsettled means the broker
        redelivers, and an idempotent handler absorbs that. Swallowed here rather than in the worker's
        catch-all so the bookkeeping below it still runs and the log names the real cause.
        """
        try:
            await delivery.ack()
        except Exception:
            logger.warning(
                "edax dispatcher: ack of %s failed after a successful handler; the broker will redeliver it",
                delivery.envelope.id,
                exc_info=True,
            )

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

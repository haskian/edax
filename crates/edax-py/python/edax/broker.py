"""The broker seam (``Producer`` / ``Consumer``). A producer publishes an encoded message, an event or a
command, to a topic; wire it to a :class:`~edax.outbox.Relay` to drain a transactional outbox. A consumer
turns each record into a :class:`~edax.delivery.Delivery`, and :func:`consume` submits it to a
:class:`~edax.dispatcher.KeyOrderedDispatcher`. The kernel's ordering key (the aggregate id, or a command's
own id when it names no aggregate) is the message key, so a broker that preserves per-key order delivers an
aggregate's messages in order and the dispatcher pool keeps them serialized on one worker. See
``docs/backends/README.md``."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Protocol, TypeAlias

from . import _native
from .delivery import Delivery, M, Undecodable
from .dispatcher import DispatcherStopped, KeyOrderedDispatcher
from .errors import is_fatal
from .message import CommandMessage, EventMessage

__all__ = [
    "Consumer",
    "KeyFn",
    "Polled",
    "Producer",
    "consume",
]


logger = logging.getLogger("edax.broker")

KeyFn = Callable[[EventMessage | CommandMessage], str]

#: What one :meth:`Consumer.poll` returns: a delivery of the message kind ``M`` the consumer carries, a poison
#: record no handler may see, or ``None`` at the end of the stream.
Polled: TypeAlias = Delivery[M] | Undecodable | None


class Producer(Protocol):
    """Publishes a message, an event or a command, to the broker (encode + send). What an adapter must
    guarantee:

    * ``publish`` returns only after every in-sync replica has the message (Kafka ``acks=all``), not the
      leader alone. Returning early lets the relay mark the outbox row published before the message is
      durable: a leader that fails before a follower copied it loses the event, and the outbox row says
      it was sent.
    * On failure ``publish`` raises: the relay marks the rows before it and hands that row and the rest back.
      Never swallow errors.
    * The message key is the kernel's ``ordering_key`` (the aggregate id, else a command's id), or a
      function of it, so a per-key-ordered broker delivers one aggregate's events in ``sequence`` order.
    * Delivery is at-least-once: a crash between publish and ``mark_published`` publishes the message
      again, so consumers dedup by ``msg.id`` (:func:`~edax.router.idempotent_by_id`), or drop a stale
      ``sequence`` with :func:`~edax.is_stale`.
    * ``publish`` is called serially by one relay; it need not be thread-safe or reentrant.
    """

    async def publish(self, msg: EventMessage | CommandMessage) -> None: ...


class Consumer(Protocol[M]):
    """Receives one delivery at a time; :func:`consume` submits them to a :class:`KeyOrderedDispatcher` of
    the same message kind ``M``. A ``Consumer[EventMessage]`` feeds an event dispatcher, and a transport that
    carries both kinds is a ``Consumer[EventMessage | CommandMessage]``. What an adapter must guarantee:

    * ``poll`` returns the next message as a :class:`~edax.delivery.Delivery`, or ``None`` at end of
      stream. ``Delivery.attempt`` is the broker's own redelivery count if it keeps one, else ``0``.
    * A payload that will not decode returns an :class:`~edax.delivery.Undecodable` and does not raise, or
      the loop dies on an unsettleable message and the same bytes redeliver forever.
    * Per-key order: messages for one ordering key arrive as published. Across keys, any order.
    * Ack means exactly this message. The dispatcher acks out of order, so an offset-based broker must track
      a per-partition watermark and commit only below the lowest unacked offset. A bare "commit current
      position" acknowledges messages still in flight and loses them on restart.
    * Nack means redeliver. A log-based broker rewinds the partition, so the nacked message and every later
      one, acked or not, come back in order; it retires the deliveries it already handed out
      (``Delivery.live``) so a later one never runs ahead. Handlers tolerate the duplicates.
    * Nothing redelivers a delivery left unsettled while the consumer lives: the loop settles every one.
    * Each delivery is settled at most once, ack or nack, from any worker task on the same loop; a retired
      one is not settled at all.
    * Overall semantics are at-least-once: a crash before the commit redelivers, so handlers are idempotent.
    """

    async def poll(self) -> Polled[M]: ...


async def consume(consumer: Consumer[M], dispatcher: KeyOrderedDispatcher[M]) -> None:
    """Submit each :meth:`Consumer.poll` result to ``dispatcher`` until cancelled or ``poll`` returns
    ``None``: the one consumer loop every backend shares, so an adapter writes only ``poll``. An
    :class:`~edax.delivery.Undecodable` goes to :meth:`~edax.dispatcher.KeyOrderedDispatcher.settle_poison`
    instead, so no handler sees it.

    It raises a fatal ``poll`` failure (:func:`~edax.errors.is_fatal`), and the fatal failure that stopped
    the dispatcher, after handing the polled delivery back. Any other ``poll`` failure waits a backoff step
    that grows while the polls keep failing, the kernel's streak, then polls again. A backend with work of
    its own around the loop, such as :class:`~edax.backend.kafka.KafkaConsumer`'s offset commits, runs it
    from its own ``run``."""
    # The kernel's count of failed polls in a row, for the backoff step; a poll that returns resets it.
    # Local to this call, so the native handle never leaves the thread that built it.
    streak = _native.handle(_native.streak())
    # Whether the streak has a failure to reset: a healthy poll then skips the native call.
    failing = False
    while True:
        try:
            polled = await consumer.poll()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            dispatcher.observer.poll_failed(e)
            # A fatal failure (the wiring) fails every poll alike: stop on it, as a relay does.
            if is_fatal(e):
                raise
            # Any other must not end the loop, because nothing would poll again: a middleware whose
            # key service blipped, or a broker that refused one fetch. The poll already handed its
            # message back.
            step = streak.failed()
            failing = True
            logger.warning(
                "edax consumer: poll failed (%d in a row); retrying after a backoff step",
                step + 1,
                exc_info=True,
            )
            await dispatcher.pacing.wait(step)
            continue
        if failing:
            streak.answered()
            failing = False
        if polled is None:
            return
        if isinstance(polled, Undecodable):
            try:
                await dispatcher.settle_poison(polled)
            except DispatcherStopped:
                # Stopped under a running loop: `settle_poison` handed the poison back, and nothing
                # polled after it could be settled either.
                return
            except Exception as e:
                # A fatal failure (a sink or a transport that refuses the wiring) fails every poison
                # alike: stop on it, as a fatal `submit` refusal stops the loop.
                if is_fatal(e):
                    raise
                # Any other is a nack that failed (a transport fault). The loop goes on; the record is
                # the transport's to bring back on a rebalance or a restart.
                logger.exception(
                    "edax consumer: settling poison message %s failed; the loop continues", polled.id
                )
            continue
        try:
            await dispatcher.submit(polled)
        except DispatcherStopped:
            # The dispatcher was stopped under a running loop: no worker will take the delivery this
            # poll booked, so hand it back. Nothing polled after it could be handled either.
            await polled.nack()
            return
        except asyncio.CancelledError:
            # Cancelled after `poll` took the message and before a worker owns it: settle it, or it
            # stays unsettled and the transport's watermark pins on it.
            await polled.nack()
            raise
        except Exception:
            # The dispatcher refused it: a fatal failure stopped it, and every message would fail
            # alike. Hand this one back and end the loop on the failure.
            await polled.nack()
            raise

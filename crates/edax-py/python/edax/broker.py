"""The broker seam (``Producer`` / ``Consumer``). A producer publishes an encoded message, an event or a
command, to a topic; wire it to a :class:`~edax.store.Relay` to drain a transactional outbox. A consumer
turns each record into a :class:`~edax.dispatcher.Delivery` and submits it to a
:class:`~edax.dispatcher.KeyOrderedDispatcher`. The kernel's ordering key (the aggregate id, or a command's
own id when it names no aggregate) is the message key, so a broker that preserves per-key order delivers an
aggregate's messages in order and the dispatcher pool keeps them serialized on one worker. See
``docs/backends/README.md``."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any, Protocol

from .aggregate import CommandMessage, EventMessage
from .dispatcher import AckFn, Delivery, KeyOrderedDispatcher, LiveFn, _always_live
from .errors import is_terminal

__all__ = [
    "Consumer",
    "KeyFn",
    "Producer",
    "UNDECODABLE",
    "UndecodableDelivery",
]


logger = logging.getLogger("edax.broker")

KeyFn = Callable[[EventMessage | CommandMessage], str]

#: Event name on the stand-in message of an undecodable record, so a dead-letter sink can tell a poison
#: message from a real event and still see the original bytes.
UNDECODABLE = "edax.undecodable"


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


class Consumer(Protocol):
    """Receives one delivery at a time and runs a submit loop into a :class:`KeyOrderedDispatcher`.
    What an adapter must guarantee:

    * ``poll`` returns the next message as a :class:`~edax.dispatcher.Delivery`, or ``None`` at end of
      stream. ``Delivery.attempt`` is the broker's own redelivery count if it keeps one, else ``0``.
    * A payload that will not decode returns an :class:`UndecodableDelivery` and does not raise, or the
      loop dies on an unsettleable message and the same bytes redeliver forever.
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
    * ``run`` submits deliveries until cancelled or ``poll`` returns ``None``; the mixin provides it.
    """

    async def poll(self) -> Delivery | None: ...
    async def run(self, dispatcher: KeyOrderedDispatcher) -> None: ...


class UndecodableDelivery(Delivery):
    """A polled message whose payload would not decode: a poison message. Terminal by construction, so
    :meth:`_ConsumerLoop.run` dead-letters it instead of submitting it. No handler may see it:
    :attr:`message` is only a stand-in event carrying the raw payload under the name :data:`UNDECODABLE`."""

    def __init__(
        self,
        payload: bytes,
        message_id: str,
        on_ack: AckFn,
        on_nack: AckFn,
        error: BaseException,
        *,
        live: LiveFn = _always_live,
        origin: Any = None,
    ) -> None:
        # `message_id` is the only identifier an undecodable payload has, so it becomes the stand-in id.
        # Every other field is unknown: it lives inside the payload that would not decode.
        stand_in = EventMessage(
            id=message_id,
            aggregate_id="",
            name=UNDECODABLE,
            version=0,
            data=payload,
            correlation_id="",
            causation_id="",
            recorded_at=0,
            extensions={},
            metadata={},
        )
        super().__init__(stand_in, on_ack, on_nack, live=live, origin=origin)
        self.error = error


class _ConsumerLoop:
    """Mixin: :meth:`run` submits each :meth:`poll` result until cancelled or ``poll`` returns ``None``."""

    async def poll(self) -> Delivery | None:  # pragma: no cover - overridden
        raise NotImplementedError

    async def run(self, dispatcher: KeyOrderedDispatcher) -> None:
        # Consecutive failed polls, for the backoff step; a poll that returns resets it.
        failures = 0
        while True:
            try:
                delivery = await self.poll()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                dispatcher.observer.poll_failed(e)
                # A terminal failure repeats on every poll: fail fast rather than loop on it.
                if is_terminal(e):
                    raise
                # A transient one (a record middleware whose key service blipped) must not end the
                # loop, because nothing would poll again. The poll already handed its message back.
                logger.warning(
                    "edax consumer: poll failed (%d in a row); retrying after a backoff step",
                    failures + 1,
                    exc_info=True,
                )
                await dispatcher.pacing.wait(failures)
                failures += 1
                continue
            failures = 0
            if delivery is None:
                return
            if isinstance(delivery, UndecodableDelivery):
                try:
                    await dispatcher.settle_poison(delivery, delivery.error)
                except Exception:
                    # The nack itself failed (a transport fault). The loop goes on; the delivery is the
                    # transport's to bring back on a rebalance or a restart.
                    logger.exception(
                        "edax consumer: settling poison message %s failed; the loop continues",
                        delivery.message.id,
                    )
                continue
            try:
                await dispatcher.submit(delivery)
            except asyncio.CancelledError:
                # Cancelled after `poll` took the message and before a worker owns it: settle it, or it
                # stays unsettled and the transport's watermark pins on it.
                await delivery.nack()
                raise

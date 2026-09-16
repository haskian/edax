"""The broker seam (``Producer`` / ``Consumer``). A producer publishes an encoded envelope to a topic; wire it
to a :class:`~edax.store.Relay` to drain the transactional outbox. A consumer turns each message into a
:class:`~edax.dispatcher.Delivery` and submits it to a :class:`~edax.dispatcher.KeyOrderedDispatcher`. The
aggregate id is the message key, so a broker that preserves per-key order delivers an aggregate's events in
order and the dispatcher pool keeps them serialized on one worker. See ``docs/backends/README.md``."""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Optional, Protocol

from ._async import maybe_await
from .aggregate import Event, EventEnvelope
from .dispatcher import AckFn, Delivery, KeyOrderedDispatcher

logger = logging.getLogger("edax.broker")

KeyFn = Callable[[EventEnvelope], str]

#: Event name on the stand-in envelope of an undecodable message, so a dead-letter sink can tell a poison
#: message from a real event and still see the original bytes.
UNDECODABLE = "edax.undecodable"


class Producer(Protocol):
    """Publishes an envelope to the broker (encode + send). What an adapter must guarantee:

    * ``publish`` returns only after the broker has durably accepted the message, leader ack or stronger.
      Returning early lets the relay mark the outbox row published before the message exists: a lost event.
    * On failure ``publish`` raises and the relay leaves the whole batch unmarked. Never swallow errors.
    * The message key is ``aggregate_id``, or a function of it, so a per-key-ordered broker delivers one
      aggregate's events in ``sequence`` order.
    * Delivery is at-least-once: a crash between publish and ``mark_published`` publishes the envelope
      again, so consumers dedup by ``env.id`` or by ``dedup`` on ``sequence``.
    * ``publish`` is called serially by one relay; it need not be thread-safe or reentrant.
    """

    async def publish(self, env: EventEnvelope) -> None: ...


class Consumer(Protocol):
    """Receives one delivery at a time and runs a submit loop into a :class:`KeyOrderedDispatcher`.
    What an adapter must guarantee:

    * ``poll`` returns the next message as a :class:`~edax.dispatcher.Delivery`, or ``None`` at end of
      stream. ``Delivery.attempt`` is the broker's own redelivery count if it keeps one, else ``0``.
    * A payload that will not decode returns an :class:`UndecodableDelivery` and does not raise, or the
      loop dies on an unsettleable message and the same bytes redeliver forever.
    * Per-key order: messages for one ``aggregate_id`` arrive as published. Across keys, any order.
    * Ack means exactly this message. The dispatcher acks out of order, so an offset-based broker must track
      a per-partition watermark and commit only below the lowest unacked offset. A bare "commit current
      position" acknowledges messages still in flight and loses them on restart.
    * Nack means redeliver, possibly behind later same-key messages already acked. Handlers tolerate that.
    * Each delivery is settled exactly once, ack or nack, from any worker task on the same loop.
    * Overall semantics are at-least-once: a crash before ``ack`` redelivers, so handlers are idempotent.
    * ``run`` submits deliveries until cancelled or ``poll`` returns ``None``; the mixin provides it.
    """

    async def poll(self) -> Optional[Delivery]: ...
    async def run(self, dispatcher: KeyOrderedDispatcher) -> None: ...


class UndecodableDelivery(Delivery):
    """A polled message whose payload would not decode: a poison message. Terminal by construction, so
    :meth:`_ConsumerLoop.run` dead-letters it instead of submitting it. No handler may see it:
    :attr:`envelope` is only a stand-in carrying the raw payload under :data:`UNDECODABLE`."""

    def __init__(
        self, payload: bytes, message_id: str, on_ack: AckFn, on_nack: AckFn, error: BaseException
    ) -> None:
        # `message_id` is the only identifier an undecodable payload has, so it becomes the stand-in id.
        envelope = EventEnvelope(
            id=message_id,
            aggregate_id="",  # unknown: the key lives inside the payload that would not decode
            event=Event(UNDECODABLE, 0, payload),
            namespace={},
            metadata={},
            timestamp=0,
            sequence=None,
        )
        super().__init__(envelope, on_ack, on_nack)
        self.error = error


async def _dead_letter(dispatcher: KeyOrderedDispatcher, poison: UndecodableDelivery) -> None:
    """Settle a poison message through the dispatcher's dead-letter hook, the route a terminal handler
    failure takes. The ack is the point: without it the transport's watermark pins on the message and the
    consumer crash-loops after every restart. With no hook wired the message nacks, loud but never lost."""
    # Read through the public accessors, because routing the poison message through `submit` would hand
    # a handler an envelope with no event in it. Reported here, the one point every adapter's poison
    # message passes, so a `Consumer` needs no observer of its own.
    dispatcher.observer.undecodable(poison.envelope.id, poison.error)
    on_dead = dispatcher.on_dead
    if on_dead is None:
        await poison.nack()
        return
    await maybe_await(on_dead(poison, poison.error))
    await poison.ack()


class _ConsumerLoop:
    """Mixin: :meth:`run` submits each :meth:`poll` result until cancelled or ``poll`` returns ``None``."""

    async def poll(self) -> Optional[Delivery]:  # pragma: no cover - overridden
        raise NotImplementedError

    async def run(self, dispatcher: KeyOrderedDispatcher) -> None:
        while True:
            delivery = await self.poll()
            if delivery is None:
                return
            try:
                if isinstance(delivery, UndecodableDelivery):
                    try:
                        await _dead_letter(dispatcher, delivery)
                    except Exception:
                        # The sink failed, not the loop. Same outcome as a raising `on_dead` on the
                        # dispatcher route: the message stays unsettled, the broker redelivers it on its
                        # session timeout or a restart, and the sink gets another try. Nacking instead
                        # would re-read every offset above the poison on a seeking transport, and the
                        # poison route has no attempt budget to stop it.
                        logger.exception(
                            "edax consumer: dead-lettering poison message %s failed; the loop continues",
                            delivery.envelope.id,
                        )
                else:
                    await dispatcher.submit(delivery)
            except asyncio.CancelledError:
                # Cancelled after `poll` took the message: settle it, or it stays unsettled until the
                # broker session times out and the transport's watermark pins on it. One rule for both
                # routes, so a change to it cannot land on only one.
                await delivery.nack()
                raise

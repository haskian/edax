"""Kafka producer and consumer adapters over a caller-owned ``aiokafka`` client (``pip install
edax[kafka]``), so edax imports no broker package. The aggregate id is the message key, so a partition
preserves per-aggregate order and the :class:`~edax.dispatcher.KeyOrderedDispatcher` pool keeps those events
serialized on one worker. See ``docs/backends/kafka.md``."""

from __future__ import annotations

from typing import Any, Optional

from ..aggregate import EventEnvelope, Serializer
from ..broker import KeyFn, UndecodableDelivery, _aggregate_key, _ConsumerLoop
from ..dispatcher import Delivery
from ..errors import CodecError


class KafkaProducer:
    """Publishes envelopes to a Kafka topic through a caller-owned, started ``AIOKafkaProducer``."""

    def __init__(
        self, client: Any, serializer: Serializer, topic: str, *, key: KeyFn = _aggregate_key
    ) -> None:
        self._client = client
        self._serializer = serializer
        self._topic = topic
        self._key = key

    async def publish(self, env: EventEnvelope) -> None:
        await self._client.send_and_wait(
            self._topic, value=self._serializer.encode(env), key=self._key(env).encode()
        )


class KafkaConsumer(_ConsumerLoop):
    """Consumes a Kafka topic through a caller-owned, subscribed ``AIOKafkaConsumer``.

    The dispatcher acks in worker order, not fetch order, so a bare ``commit()`` would skip past a
    still-unacked offset and lose it on restart. Instead ``ack`` commits a per-partition watermark: the
    lowest offset still in flight, or one past the highest delivered when none remain. ``nack`` seeks the
    partition back for redelivery. The bookkeeping holds only while the partition is assigned: a revoked
    partition leaves its in-flight offsets unsettled here and the new owner redelivers them from the last
    committed watermark, which is at-least-once. A payload the serializer rejects becomes an
    :class:`~edax.broker.UndecodableDelivery` after its offset is booked, so the loop can dead-letter it and
    the watermark advances past it. ``getone`` blocks by design; per-request deadlines are the client's."""

    def __init__(self, client: Any, serializer: Serializer) -> None:
        self._client = client
        self._serializer = serializer
        # Per TopicPartition: unsettled offsets, the highest delivered, and the last committed watermark.
        self._inflight: dict[Any, set[int]] = {}
        self._max_delivered: dict[Any, int] = {}
        self._committed: dict[Any, int] = {}

    async def poll(self) -> Optional[Delivery]:
        from aiokafka import TopicPartition  # lazy: the base package imports no broker library

        msg = await self._client.getone()
        tp = TopicPartition(msg.topic, msg.partition)
        offset = msg.offset
        # Book the offset before decoding: an undecodable payload still has to be settled, and an unbooked
        # offset is either committed past by the next ack or replayed into the same failure forever.
        self._inflight.setdefault(tp, set()).add(offset)
        self._max_delivered[tp] = max(self._max_delivered.get(tp, -1), offset)

        async def ack() -> None:
            inflight = self._inflight.get(tp) or set()
            inflight.discard(offset)
            watermark = min(inflight) if inflight else self._max_delivered[tp] + 1
            # A commit reordered under asyncio at worst re-commits a lower safe watermark; this guard keeps
            # the record monotonic so a later, higher ack is never skipped.
            if watermark > self._committed.get(tp, -1):
                await self._client.commit({tp: watermark})
                self._committed[tp] = max(self._committed.get(tp, -1), watermark)

        async def nack() -> None:
            # The offset stays in flight until its redelivered copy acks, so a later ack cannot commit past
            # it. Seek to the lowest unsettled offset: seek() sets the partition's one fetch position, so
            # nacking 5 then 7 would move it to 7 and 5 would never come back.
            inflight = self._inflight.get(tp) or {offset}
            self._client.seek(tp, min(inflight))

        try:
            env = self._serializer.decode(msg.value)
        except Exception as e:  # noqa: BLE001 - whatever the serializer raises, it is one poison message
            poison = CodecError(f"kafka {msg.topic}/{msg.partition} offset {offset} did not decode: {e}")
            poison.__cause__ = e  # `raise ... from e` is unavailable: the error is carried, not raised
            # Topic, partition and offset together are the only identity a poison message has. The
            # offset alone repeats on every partition, so two poison messages would share one id and
            # a dead-letter queue keyed on it would keep the first and reject the second.
            return UndecodableDelivery(msg.value, f"{msg.topic}/{msg.partition}/{offset}", ack, nack, poison)
        return Delivery(env, ack, nack)

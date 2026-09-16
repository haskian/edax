"""Kafka producer and consumer adapters over a caller-owned ``aiokafka`` client (``pip install
edax[kafka]``), so edax imports no broker package. The record — its key, its four headers and the offset
watermark that settles it — is the ``edax-kafka`` kernel's; this module makes the client calls around it.
The aggregate id is the message key, so a partition preserves per-aggregate order and the
:class:`~edax.dispatcher.KeyOrderedDispatcher` pool keeps those events serialized on one worker.
See ``docs/backends/kafka.md``."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Optional, Protocol

from .. import _edax
from ..aggregate import EventEnvelope, KernelSerializer, Serializer
from ..broker import KeyFn, UndecodableDelivery, _ConsumerLoop
from ..dispatcher import Delivery
from ..errors import CodecError

# The kernel's record structs, re-exported here so a middleware builds one without importing `_edax`.
KafkaRecord = _edax.KafkaRecord
KafkaHeader = _edax.KafkaHeader


class RecordMiddleware(Protocol):
    """Rewrites a record on its way to or from the broker. Synchronous, bytes to bytes: an async hook
    (a KMS call) is a later addition if a host asks for one.

    :meth:`on_publish` runs in order, after the kernel has built the record and before the send.
    :meth:`on_poll` runs in reverse, after the fetch and before the content-type lookup, so a pair of
    middlewares wraps the record like an onion. Use one to add a ``traceparent`` or tenant header, or
    to compress or encrypt the value — rewrite ``content-type`` on the way out and restore it on the
    way in, so the consumer still routes to the serializer that wrote the bytes. A
    :class:`~edax.errors.CodecError` raised on poll marks the message poison; any other exception
    propagates as transient, so the message redelivers."""

    def on_publish(self, record: KafkaRecord) -> KafkaRecord: ...
    def on_poll(self, record: KafkaRecord) -> KafkaRecord: ...


class KafkaProducer:
    """Publishes envelopes to a Kafka topic through a caller-owned, started ``AIOKafkaProducer``.
    The serializer supplies the value bytes and the media type that names them; the kernel builds the
    record around both, so the key rule and the header set stay the same in every SDK."""

    def __init__(
        self,
        client: Any,
        topic: str,
        *,
        serializer: Optional[Serializer] = None,
        key: Optional[KeyFn] = None,
        middleware: Sequence[RecordMiddleware] = (),
    ) -> None:
        self._client = client
        self._topic = topic
        self._serializer: Serializer = serializer or KernelSerializer("json")
        self._key = key
        self._middleware: tuple[RecordMiddleware, ...] = tuple(middleware)

    async def publish(self, env: EventEnvelope) -> None:
        # No key function means no override: the kernel applies its own default rule (the aggregate
        # id), which is the one the golden vector pins. Computing it here would fork that rule.
        record = _edax.kafka_record(
            env,
            self._serializer.encode(env),
            self._serializer.content_type,
            self._key(env) if self._key else None,
        )
        for m in self._middleware:
            record = m.on_publish(record)
        await self._client.send_and_wait(
            self._topic,
            value=record.value,
            key=record.key,
            headers=[(h.key, h.value) for h in record.headers],
        )


@dataclass(frozen=True)
class PartitionStats:
    """One partition's consumer bookkeeping: offsets delivered but not yet settled, the highest offset
    delivered, and the last committed watermark (``-1`` before the first commit)."""

    unsettled: int
    max_delivered: int
    committed: int


class KafkaConsumer(_ConsumerLoop):
    """Consumes a Kafka topic through a caller-owned, subscribed ``AIOKafkaConsumer``.

    The dispatcher acks in worker order, not fetch order, so a bare ``commit()`` would skip past a
    still-unacked offset and lose it on restart. Instead each poll books its offset with the
    ``edax-kafka`` tracker and settles by the token it gets back: the kernel computes the watermark to
    commit (the lowest offset still in flight, or one past the highest delivered when none remain),
    refuses a token a later :meth:`reset` invalidated, and keeps the committed record monotonic.
    ``nack`` seeks the partition back for redelivery. The bookkeeping holds only while the partition is
    assigned: a revoked partition leaves its in-flight offsets unsettled here and the new owner
    redelivers them from the last committed watermark, which is at-least-once; a rebalance listener
    calls :meth:`reset` for it. A payload no serializer decodes becomes an
    :class:`~edax.broker.UndecodableDelivery` after its offset is booked, so the loop can dead-letter it
    and the watermark advances past it. ``getone`` blocks by design; per-request deadlines are the
    client's."""

    def __init__(
        self,
        client: Any,
        *,
        serializer: Optional[Serializer] = None,
        serializers: Sequence[Serializer] = (),
        middleware: Sequence[RecordMiddleware] = (),
    ) -> None:
        self._client = client
        # The default decodes a record that carries no `content-type` header at all, and claims its
        # own media type in the registry like every other serializer.
        self._default: Serializer = serializer or KernelSerializer("json")
        self._by_type: dict[str, Serializer] = {s.content_type: s for s in (self._default, *serializers)}
        self._middleware: tuple[RecordMiddleware, ...] = tuple(middleware)
        self._tracker = _edax.offset_tracker()

    def stats(self) -> dict[Any, PartitionStats]:
        """A snapshot of the bookkeeping, keyed by ``TopicPartition``, for a health check or a gauge."""
        from aiokafka import TopicPartition  # lazy: the base package imports no broker library

        return {
            TopicPartition(row.topic, row.partition): PartitionStats(
                row.unsettled, row.max_delivered, row.committed
            )
            for row in self._tracker.stats().items
        }

    def reset(self, tp: Any) -> None:
        """Drop the bookkeeping for a revoked partition. Its unsettled offsets are the new owner's to
        redeliver from the last committed watermark, and a stale watermark here would otherwise refuse
        a lower, correct commit if the partition is assigned back."""
        self._tracker.reset(tp.topic, tp.partition)

    async def poll(self) -> Optional[Delivery]:
        from aiokafka import TopicPartition  # lazy: the base package imports no broker library

        msg = await self._client.getone()
        tp = TopicPartition(msg.topic, msg.partition)
        offset = msg.offset
        # Book the offset before decoding: an undecodable payload still has to be settled, and an unbooked
        # offset is either committed past by the next ack or replayed into the same failure forever.
        token = self._tracker.book(msg.topic, msg.partition, offset)

        async def ack() -> None:
            decision = self._tracker.ack(token)
            if decision.commit:
                await self._client.commit({tp: decision.offset})
                # Recorded only once the client's commit returned, so a failed commit is not counted as
                # done. The monotonic guard that keeps a later, higher ack from being skipped by a
                # commit that returned out of order is the kernel's.
                self._tracker.committed(msg.topic, msg.partition, decision.offset)

        async def nack() -> None:
            # The offset stays in flight until its redelivered copy acks, so a later ack cannot commit past
            # it. The kernel picks the lowest unsettled offset: seek() sets the partition's one fetch
            # position, so nacking 5 then 7 would move it to 7 and 5 would never come back.
            decision = self._tracker.nack(token)
            if decision.seek:
                self._client.seek(tp, decision.offset)

        record = KafkaRecord(msg.key or b"", msg.value, [KafkaHeader(k, v) for k, v in (msg.headers or ())])
        try:
            for m in reversed(self._middleware):
                record = m.on_poll(record)
            content_type = _edax.kafka_content_type(record.headers)
            if content_type and content_type not in self._by_type:
                # Never fall back to the default: decoding msgpack bytes as the host's own format is
                # how a wrong envelope reaches a handler instead of the dead-letter sink.
                raise CodecError(
                    f"kafka {msg.topic}/{msg.partition} offset {offset}: "
                    f"no serializer for content type {content_type!r}"
                )
            serializer = self._by_type[content_type] if content_type else self._default
            env = serializer.decode(record.value)
        except CodecError as e:
            poison = CodecError(f"kafka {msg.topic}/{msg.partition} offset {offset} did not decode: {e}")
            poison.__cause__ = e  # `raise ... from e` is unavailable: the error is carried, not raised
            # Topic, partition and offset together are the only identity a poison message has. The
            # offset alone repeats on every partition, so two poison messages would share one id and
            # a dead-letter queue keyed on it would keep the first and reject the second.
            message_id = _edax.kafka_poison_id(msg.topic, msg.partition, offset)
            return UndecodableDelivery(msg.value, message_id, ack, nack, poison)
        return Delivery(env, ack, nack)

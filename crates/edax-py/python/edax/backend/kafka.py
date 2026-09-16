"""Kafka producer and consumer adapters over a caller-owned ``aiokafka`` client (``pip install
edax[kafka]``), so edax imports no broker package. The record — its key, its four headers and the offset
watermark that settles it — is the ``edax-kafka`` kernel's; this module makes the client calls around it.
The aggregate id is the message key, so a partition preserves per-aggregate order and the
:class:`~edax.dispatcher.KeyOrderedDispatcher` pool keeps those events serialized on one worker.
See ``docs/backends/kafka.md``."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Protocol

from .. import _edax
from ..aggregate import EventEnvelope, KernelSerializer, Serializer
from ..broker import KeyFn, UndecodableDelivery, _ConsumerLoop
from ..dispatcher import Delivery
from ..errors import CodecError

if TYPE_CHECKING:  # the base package imports no broker library; this name is for the annotations only
    from aiokafka import TopicPartition

__all__ = [
    "KafkaConsumer",
    "KafkaHeader",
    "KafkaProducer",
    "KafkaRecord",
    "PartitionStats",
    "RecordMiddleware",
]

# The kernel's record structs, re-exported here so a middleware builds one without importing `_edax`.
KafkaRecord = _edax.KafkaRecord
KafkaHeader = _edax.KafkaHeader
#: One partition's consumer bookkeeping as the kernel reports it: ``unsettled`` offsets delivered and not
#: yet settled, the highest ``max_delivered``, and the last ``committed`` watermark (``-1`` before the
#: first commit), under the ``topic`` and ``partition`` they belong to.
PartitionStats = _edax.PartitionStats


def _topic_partition(topic: str, partition: int) -> TopicPartition:
    """The client's own key type for a partition. Imported lazily, because the base package installs no
    broker library and must import without one."""
    from aiokafka import TopicPartition

    return TopicPartition(topic, partition)


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
            self._serializer.media_type,
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


class KafkaConsumer(_ConsumerLoop):
    """Consumes a Kafka topic through a caller-owned, subscribed ``AIOKafkaConsumer``.

    ``getone`` blocks by design; per-request deadlines are the client's. The offset bookkeeping holds
    only while the partition is assigned, so a rebalance listener must call :meth:`reset` for a revoked
    one. ``serializer`` is the default: it decodes a record that carries no ``content-type`` header and
    claims its own media type in the registry, which ``serializers`` extends with one entry per media
    type. ``middleware`` rewrites each record on the way in, in reverse order. See
    ``docs/backends/kafka.md``."""

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
        self._by_type: dict[str, Serializer] = {s.media_type: s for s in (self._default, *serializers)}
        self._middleware: tuple[RecordMiddleware, ...] = tuple(middleware)
        self._tracker = _edax.offset_tracker()

    def stats(self) -> dict[TopicPartition, PartitionStats]:
        """A snapshot of the bookkeeping, keyed by ``TopicPartition``, for a health check or a gauge."""
        return {_topic_partition(row.topic, row.partition): row for row in self._tracker.stats().items}

    def reset(self, tp: Any) -> None:
        """Drop the bookkeeping for a revoked partition. Its unsettled offsets are the new owner's to
        redeliver from the last committed watermark, and a stale watermark here would otherwise refuse
        a lower, correct commit if the partition is assigned back."""
        self._tracker.reset(tp.topic, tp.partition)

    async def poll(self) -> Optional[Delivery]:
        msg = await self._client.getone()
        # `ack` and `nack` settle long after this frame returns; they capture these three, never `msg`,
        # so an unsettled delivery does not pin a whole fetched record in memory.
        topic, partition, offset = msg.topic, msg.partition, msg.offset
        tp = _topic_partition(topic, partition)
        # Book the offset before decoding: an undecodable payload still has to be settled, and an unbooked
        # offset is either committed past by the next ack or replayed into the same failure forever.
        token = self._tracker.book(topic, partition, offset)
        # Topic, partition and offset together are the only identity a poison message has. The offset
        # alone repeats on every partition, so two poison messages would share one id and a dead-letter
        # queue keyed on it would keep the first and reject the second.
        poison_id = _edax.kafka_poison_id(topic, partition, offset)

        async def ack() -> None:
            decision = self._tracker.ack(token)
            if decision.commit:
                await self._client.commit({tp: decision.offset})
                # Recorded only once the client's commit returned, so a failed commit is not counted as
                # done. The monotonic guard that keeps a later, higher ack from being skipped by a
                # commit that returned out of order is the kernel's.
                self._tracker.committed(topic, partition, decision.offset)

        async def nack() -> None:
            # The offset stays in flight until its redelivered copy acks, so a later ack cannot commit past
            # it. The kernel picks the lowest unsettled offset: seek() sets the partition's one fetch
            # position, so nacking 5 then 7 would move it to 7 and 5 would never come back.
            decision = self._tracker.nack(token)
            if decision.seek:
                self._client.seek(tp, decision.offset)

        def poison(cause: BaseException) -> UndecodableDelivery:
            """The fetched bytes as a dead-letterable delivery, settled by the offset already booked."""
            error = CodecError(f"kafka {poison_id} did not decode: {cause}")
            error.__cause__ = cause  # `raise ... from e` is unavailable: the error is carried, not raised
            return UndecodableDelivery(msg.value, poison_id, ack, nack, error)

        # A Kafka header value is nullable; aiokafka hands a null one over as `None`.
        value, headers = msg.value, [KafkaHeader(k, v or b"") for k, v in (msg.headers or ())]
        if self._middleware:
            # The kernel record exists for the middleware to rewrite; building one with no middleware to
            # run would copy the payload and the headers across the binding for nothing.
            record = KafkaRecord(msg.key or b"", value, headers)
            try:
                for m in reversed(self._middleware):
                    record = m.on_poll(record)
            except CodecError as e:
                # Only a codec failure is poison here. A hook whose key service is down fails
                # transiently, so anything else propagates and the message redelivers.
                return poison(e)
            except Exception:
                # The offset is already booked. Left unsettled it pins the partition watermark, so
                # every later ack commits nothing, and the fetch position has moved past the record,
                # so it would never come back. Nack seeks back to it.
                await nack()
                raise
            value, headers = record.value, record.headers

        content_type = _edax.kafka_content_type(headers)
        serializer = self._by_type.get(content_type) if content_type else self._default
        if serializer is None:
            # Never fall back to the default: decoding msgpack bytes as the host's own format is how a
            # wrong envelope reaches a handler instead of the dead-letter sink.
            return poison(CodecError(f"no serializer for content type {content_type!r}"))
        try:
            env = serializer.decode(value)
        except Exception as e:  # noqa: BLE001 - every failure of a pure decode is terminal
            # A third-party serializer raises its own error types, and a decode that fails once fails
            # the same way on every redelivery, so the record is poison whatever it raised.
            return poison(e)
        return Delivery(env, ack, nack)

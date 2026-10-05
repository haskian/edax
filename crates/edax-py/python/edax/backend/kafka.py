"""Kafka producer and consumer adapters over a caller-owned ``aiokafka`` client (``pip install
edax[kafka]``), so edax imports no broker package. The ``edax-kafka`` kernel owns the record: its key, its
``content-type`` header, the route a fetched record takes by it, the partition it lands on, the offset
watermark that settles it and the dead-letter record. This module makes the client calls around it. The
kernel's ordering key (the aggregate id, else a command's own id) is the message key, and the producer
places it on Java's murmur2 partition explicitly, so every producer on the topic, in any language, keeps one
aggregate on one partition and in order; the :class:`~edax.dispatcher.KeyOrderedDispatcher` pool then keeps
those messages serialized on one worker, and a nack rewinds the partition without reordering it. See
``docs/backends/kafka.md``."""

from __future__ import annotations

import asyncio
import logging
import warnings
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Protocol

from .. import _edax
from .._owned import ThreadBound
from ..aggregate import (
    CommandMessage,
    EventMessage,
    KernelSerializer,
    MessageKind,
    Serializer,
    message_kind,
)
from ..broker import KeyFn, UndecodableDelivery, _ConsumerLoop
from ..dispatcher import Delivery, KeyOrderedDispatcher
from ..errors import CodecError, mapped
from ..observe import error_type
from ..tenancy import Tenancy

if TYPE_CHECKING:  # the base package imports no broker library; these names are for the annotations only
    from aiokafka import ConsumerRebalanceListener, TopicPartition  # type: ignore[import-untyped]

__all__ = [
    "CONTENT_TYPE",
    "ConsumerClient",
    "KafkaConsumer",
    "KafkaDeadLetterSink",
    "KafkaFetched",
    "KafkaHeader",
    "KafkaProducer",
    "KafkaRecord",
    "PartitionStats",
    "ProducerClient",
    "RecordMiddleware",
]

logger = logging.getLogger("edax.backend.kafka")

#: How often a revoke looks at the handlers still running.
_DRAIN_POLL_S = 0.01

#: How long one send may take, by default, on the producer and on the dead-letter sink alike.
_PUBLISH_TIMEOUT_S: Final = 30.0

# The kernel's record structs, re-exported here so a middleware builds one without importing `_edax`.
KafkaRecord = _edax.KafkaRecord
KafkaHeader = _edax.KafkaHeader
#: One partition's consumer bookkeeping as the kernel reports it: ``unsettled`` offsets delivered and not
#: yet settled, how many of them are still ``live`` (a handler may still settle them; a rewind retired
#: the rest), the highest ``max_delivered``, and the last ``committed`` watermark (``None`` before the
#: first commit), under the ``topic`` and ``partition`` they belong to.
PartitionStats = _edax.PartitionStats
#: The header naming a record value's media type, the one header edax writes. A middleware that rewrites
#: the value rewrites this header by this name, so the consumer still routes to the right serializer.
CONTENT_TYPE: str = _edax.kafka_content_type_header()


class ProducerClient(Protocol):
    """The two ``AIOKafkaProducer`` calls :class:`KafkaProducer` makes, as it makes them: positional
    where it passes a value by position, keyword-only where it names one, so a client whose method takes
    more parameters in between (``AIOKafkaProducer`` has ``timestamp_ms``) still fits. Any client with
    these works."""

    async def partitions_for(self, topic: str, /) -> Collection[int]: ...

    async def send_and_wait(
        self,
        topic: str,
        /,
        *,
        value: bytes,
        key: bytes,
        partition: int,
        headers: Sequence[tuple[str, bytes]],
    ) -> Any: ...


class ConsumerClient(Protocol):
    """The three ``AIOKafkaConsumer`` calls :class:`KafkaConsumer` makes. Any client with these works."""

    async def getone(self) -> Any: ...

    async def commit(self, offsets: dict[TopicPartition, int], /) -> Any: ...

    def seek(self, partition: TopicPartition, offset: int, /) -> Any: ...


def _topic_partition(topic: str, partition: int) -> TopicPartition:
    """The client's own key type for a partition. Imported lazily, because the base package installs no
    broker library and must import without one."""
    from aiokafka import TopicPartition

    return TopicPartition(topic, partition)


def _consumer_stopped(e: Exception) -> bool:
    """Whether ``getone`` raised because the client was stopped: the end of the stream, not a failure."""
    try:
        from aiokafka.errors import ConsumerStoppedError  # type: ignore[import-untyped]
    except ImportError:  # a client that is not aiokafka has no such error
        return False
    return isinstance(e, ConsumerStoppedError)


def _check_producer(client: ProducerClient) -> None:
    """Warn when the client acks on the leader alone. The outbox marks a row published once ``publish``
    returns, so an ack that a failed leader takes with it is a lost event. The probe reads a private
    attribute of ``AIOKafkaProducer``, so a different client is not judged."""
    acks = getattr(getattr(client, "_sender", None), "_acks", None)
    if acks is not None and acks != -1:
        warnings.warn(
            f"KafkaProducer: the client acks with acks={acks!r}; a leader failover loses an acked outbox "
            "event. Build it with enable_idempotence=True (which implies acks='all'), or acks='all'.",
            stacklevel=3,
        )


def _check_consumer(client: ConsumerClient) -> None:
    """Refuse a client configuration that loses or never commits offsets. Both probes read private
    attributes of ``AIOKafkaConsumer``; a different client is not judged."""
    if getattr(client, "_enable_auto_commit", False):
        raise ValueError(
            "KafkaConsumer: the client auto-commits its fetch position, which commits past deliveries still "
            "in flight and loses them on a restart. Build it with enable_auto_commit=False."
        )
    if getattr(client, "_group_id", "") is None:
        raise ValueError(
            "KafkaConsumer: the client has no group_id, so every commit fails and nothing is ever "
            "committed. Build it with a group_id."
        )


def _check_listener(client: ConsumerClient) -> None:
    """Refuse a client the group assigns partitions to but that has no rebalance listener. Without one a
    revoked partition keeps its bookkeeping. Once its watermark is due, every commit names it, the client
    refuses the whole commit, and no partition commits again until a restart. The probe reads a private
    attribute of ``AIOKafkaConsumer``; a different client, or one assigned its partitions by hand, is not
    judged."""
    subscription = getattr(client, "_subscription", None)
    if subscription is not None and subscription.partitions_auto_assigned() and subscription.listener is None:
        raise ValueError(
            "KafkaConsumer.run: the client is subscribed without a rebalance listener, so a revoked "
            "partition keeps its bookkeeping and blocks every later commit. Subscribe with "
            "listener=consumer.rebalance_listener()."
        )


async def _send(client: ProducerClient, topic: str, record: KafkaRecord, timeout_s: float) -> None:
    """Send ``record`` to ``topic`` and wait for the ack, raising a transient :class:`TimeoutError` after
    ``timeout_s``. With ``enable_idempotence=True`` aiokafka never expires a batch, so without this bound a
    send to a broker that stopped answering waits forever. A send cut off at the deadline may still land:
    the batch stays in the client, so the caller's retry can write a second copy, which consumers dedup by
    ``msg.id``."""
    try:
        await asyncio.wait_for(_place_and_send(client, topic, record), timeout_s)
    except asyncio.TimeoutError as e:
        raise TimeoutError(
            f"kafka send to {topic!r}: no ack within {timeout_s}s; the record may still land, so a retry "
            "may write it twice"
        ) from e


async def _place_and_send(client: ProducerClient, topic: str, record: KafkaRecord) -> None:
    """Send ``record`` to ``topic`` on the kernel's partition for its key: Java's default partitioner over
    the final key and the topic's partition count from the client's metadata. One placement for the
    producer and the dead-letter sink."""
    partitions = await client.partitions_for(topic)
    key = record.key  # each read of a native field copies it
    with mapped():  # a topic reported with no partitions is the kernel's terminal `codec:` error
        partition = _edax.kafka_partition_for(key, len(partitions))
    await client.send_and_wait(
        topic,
        value=record.value,
        key=key,
        partition=partition,
        headers=[(h.key, h.value) for h in record.headers],
    )


@dataclass(frozen=True)
class KafkaFetched:
    """A record as :class:`KafkaConsumer` fetched it, and where it sat. It is the record before any
    middleware, so a republish runs through the middleware again. Carried as
    :attr:`~edax.dispatcher.Delivery.origin`, for a dead-letter sink. A null key, value or header value is
    empty bytes here, the kernel's rule.

    ``timestamp`` is the record's broker timestamp in Unix milliseconds, as the client reports it, and
    ``timestamp_type`` says whose clock it is: ``0`` the producer's (``CreateTime``), ``1`` the broker's
    (``LogAppendTime``). Both are ``None`` when the client reports none."""

    topic: str
    partition: int
    offset: int
    key: bytes
    value: bytes
    headers: tuple[tuple[str, bytes], ...]
    timestamp: int | None = None
    timestamp_type: int | None = None

    def record(self) -> KafkaRecord:
        """The fetched record as the kernel's struct."""
        return KafkaRecord(self.key, self.value, [KafkaHeader(k, v) for k, v in self.headers])

    def dead_letter(self, error: BaseException) -> KafkaRecord:
        """The record a dead-letter topic gets: this one verbatim, plus the ``edax-dlq-*`` headers naming
        its topic, partition, offset and ``error``. The coordinates are its identity, so a sink that sees
        the same offset twice after a redelivery can dedup on them."""
        return _edax.kafka_dead_letter_record(
            self.record(), self.topic, self.partition, self.offset, f"{error_type(error)}: {error}"
        )


class KafkaDeadLetterSink:
    """An ``on_dead`` sink that publishes a dead delivery's record to a dead-letter topic through a
    caller-owned, started ``AIOKafkaProducer``: the record as fetched, plus where it came from and why
    (:meth:`KafkaFetched.dead_letter`), on the kernel's partition for its key. A raise (the topic is
    down) nacks the delivery for another try. A delivery that did not come from a
    :class:`KafkaConsumer` carries no record to republish, so it is refused with :class:`TypeError`.
    ``publish_timeout_s`` bounds each send, as on :class:`KafkaProducer`."""

    def __init__(
        self, client: ProducerClient, topic: str, *, publish_timeout_s: float = _PUBLISH_TIMEOUT_S
    ) -> None:
        _check_producer(client)
        self._client = client
        self._topic = topic
        self._publish_timeout_s = publish_timeout_s

    async def __call__(self, delivery: Delivery, error: BaseException) -> None:
        fetched = delivery.origin
        if not isinstance(fetched, KafkaFetched):
            raise TypeError(f"delivery {delivery.message.id} carries no Kafka record to dead-letter")
        await _send(self._client, self._topic, fetched.dead_letter(error), self._publish_timeout_s)


class RecordMiddleware(Protocol):
    """Rewrites a record on its way to or from the broker. Synchronous, bytes to bytes.

    :meth:`on_publish` runs in order, after the kernel has built the record and before the send.
    :meth:`on_poll` runs in reverse, after the fetch and before the content-type lookup, so a pair of
    middlewares wraps the record like an onion. Use one to compress or encrypt the value. Trace context
    needs none: it rides in the message's ``extensions``, inside the value. Rewrite ``content-type`` on the
    way out and restore it on the way in, so the consumer still routes to the serializer that wrote the
    bytes. A :class:`~edax.errors.CodecError` raised on poll marks the message poison; any other exception
    propagates as transient, so the message redelivers."""

    def on_publish(self, record: KafkaRecord) -> KafkaRecord: ...
    def on_poll(self, record: KafkaRecord) -> KafkaRecord: ...


def _check_kind(serializer: Serializer, kind: MessageKind, role: str) -> Serializer:
    """``serializer``, if it encodes ``kind``; a :class:`ValueError` at wiring otherwise."""
    if serializer.kind != kind:
        raise ValueError(f"{role} must be a {kind} serializer, got one of kind {serializer.kind!r}")
    return serializer


class KafkaProducer:
    """Publishes events and commands to a Kafka topic through a caller-owned, started ``AIOKafkaProducer``.
    ``serializer`` encodes an :class:`~edax.aggregate.EventMessage` and ``command_serializer`` a
    :class:`~edax.aggregate.CommandMessage` (default: the kernel's JSON of each kind). A serializer supplies
    the value bytes and the media type that names them; the kernel builds the record around both, so the
    key rule and the header set stay the same in every SDK.

    The partition is the kernel's too: Java's default partitioner over the record's final key (after the
    middleware) and the topic's partition count from the client's metadata, passed as ``partition=``. The
    client's own partitioner never runs, so this producer places a key exactly where a Java producer,
    Kafka Connect or a Rust edax shell does, whatever partitioner the client library defaults to.

    ``tenancy`` is how the deployment separates tenants, fixed here as it is on a
    :class:`~edax.store.Repository`. The kernel keys the record. Under :attr:`~edax.tenancy.Tenancy.POOL`
    the key is the message's ordering key composed with its ``tenant``, so two tenants' same-named
    aggregates take different partitions. Every SDK producing to the topic places one aggregate on one
    partition (``testdata/golden/kafka_record.json``). A pooled message with no tenant raises
    :class:`~edax.errors.CodecError`. An explicit ``key`` replaces that rule outright, and then composing
    it is the caller's own job. An exception the ``key`` function raises propagates unchanged.

    The relay marks an outbox row published once :meth:`publish` returns, so the client must ack on every
    in-sync replica: build it with ``enable_idempotence=True``, which implies ``acks="all"`` and keeps a
    retried send from writing a duplicate. A client that acks on the leader alone gets a warning here.

    ``publish_timeout_s`` bounds each :meth:`publish`, metadata lookup and ack included, because an
    idempotent aiokafka producer never stops retrying a send to a broker that stopped answering. At the
    deadline :meth:`publish` raises a transient :class:`TimeoutError`. The record may still land, so the
    relay's retry can publish it twice. Consumers already dedup that second copy by ``msg.id``."""

    def __init__(
        self,
        client: ProducerClient,
        topic: str,
        *,
        serializer: Serializer | None = None,
        command_serializer: Serializer | None = None,
        key: KeyFn | None = None,
        middleware: Sequence[RecordMiddleware] = (),
        tenancy: Tenancy = Tenancy.SILO,
        publish_timeout_s: float = _PUBLISH_TIMEOUT_S,
    ) -> None:
        _check_producer(client)
        self._client = client
        self._topic = topic
        self._publish_timeout_s = publish_timeout_s
        self._serializer = _check_kind(serializer or KernelSerializer("json"), "event", "serializer")
        self._commands = _check_kind(
            command_serializer or KernelSerializer("json", "command"), "command", "command_serializer"
        )
        self._key = key
        self._tenancy = tenancy
        self._middleware: tuple[RecordMiddleware, ...] = tuple(middleware)

    async def publish(self, msg: EventMessage | CommandMessage) -> None:
        serializer = self._serializer if message_kind(msg) == "event" else self._commands
        value = serializer.encode(msg)
        # Host code, so outside `mapped()`: its own TypeError stays a transient failure.
        key = self._key(msg) if self._key else None
        # The kernel keys the record: the override, else the ordering key under this producer's tenancy.
        tenancy, media_type = self._tenancy.value, serializer.media_type
        with mapped():
            if isinstance(msg, EventMessage):
                record = _edax.kafka_event_record(tenancy, msg, value, media_type, key)
            else:
                record = _edax.kafka_command_record(tenancy, msg, value, media_type, key)
        for m in self._middleware:
            record = m.on_publish(record)
        await _send(self._client, self._topic, record, self._publish_timeout_s)


class KafkaConsumer(_ConsumerLoop):
    """Consumes a Kafka topic through a caller-owned, subscribed ``AIOKafkaConsumer``.

    ``serializer`` is the default (the kernel's JSON events unless given): it claims its own media type in
    the registry, and an event serializer also decodes a record that carries no ``content-type`` header.
    ``serializers`` extends the registry with one entry per media type, of either kind. The kernel routes
    each record by its header, and the delivery carries what the chosen serializer decoded, an
    :class:`~edax.aggregate.EventMessage` or a :class:`~edax.aggregate.CommandMessage`. A record of a media
    type nobody registered is poison. So is a header-less record when the default decodes commands: a
    command always names its kind, so a command consumer takes ``serializer=KernelSerializer("json",
    "command")``. ``middleware`` rewrites each record on the way in, in reverse
    order. See ``docs/backends/kafka.md``.

    The client must not auto-commit and must have a group, or this raises :class:`ValueError`. :meth:`run`
    needs a dispatcher with ``on_dead``: on Kafka a nack rewinds the partition, so a message that fails for
    good with nowhere to go stalls its partition forever. A nack seeks the partition back to its lowest
    unsettled offset and retires the deliveries of it already queued, so a key keeps its order.

    Acks never wait on the broker. :meth:`run` commits what the kernel says is due every
    ``commit_interval_s`` seconds in one call, under a ``commit_timeout_s`` deadline, and once more when it
    ends; :meth:`flush` commits on demand. A rebalance needs :meth:`rebalance_listener` passed to
    ``subscribe``: it commits, resets the revoked partitions, and waits for their running handlers.
    :meth:`run` refuses a subscribed client without a listener.

    The offset bookkeeping is native state bound to the thread that built the consumer. Build it on
    the thread that runs it: a call from any other thread raises the terminal
    :class:`~edax.errors.BadHandle`."""

    def __init__(
        self,
        client: ConsumerClient,
        *,
        serializer: Serializer | None = None,
        serializers: Sequence[Serializer] = (),
        middleware: Sequence[RecordMiddleware] = (),
        commit_interval_s: float = 1.0,
        commit_timeout_s: float = 10.0,
    ) -> None:
        _check_consumer(client)
        self._client = client
        # The default decodes a record that carries no `content-type` header at all, and claims its
        # own media type in the registry like every other serializer. A header-less record is read as
        # an event only: a command always names its kind.
        self._default: Serializer = serializer or KernelSerializer("json")
        self._serializers: list[Serializer] = [self._default, *serializers]
        self._media_types: list[str] = [s.media_type for s in self._serializers]
        with mapped():  # a media type claimed twice is the kernel's `already_registered:` refusal
            _edax.kafka_check_media_types(self._media_types)
        self._middleware: tuple[RecordMiddleware, ...] = tuple(middleware)
        self._tracker = ThreadBound(_edax.offset_tracker(), "KafkaConsumer")
        self._commit_interval_s = commit_interval_s
        self._commit_timeout_s = commit_timeout_s
        # The dispatcher `run` feeds, so a rebalance drain can wait for its running handlers too.
        self._dispatcher: KeyOrderedDispatcher | None = None

    def stats(self) -> dict[TopicPartition, PartitionStats]:
        """A snapshot of the bookkeeping, keyed by ``TopicPartition``, for a health check or a gauge."""
        return {_topic_partition(row.topic, row.partition): row for row in self._tracker.get().stats()}

    def reset(self, tp: TopicPartition) -> None:
        """Drop the bookkeeping for a revoked partition and retire its deliveries, so the dispatcher skips
        the ones still queued. Its unsettled offsets are the new owner's to redeliver from the last
        committed watermark, and a stale watermark here would otherwise refuse a lower, correct commit if
        the partition is assigned back. :meth:`rebalance_listener` calls it after committing."""
        self._tracker.get().reset(tp.topic, tp.partition)

    async def flush(self) -> None:
        """Commit every partition whose watermark moved, in one call, under the commit deadline. Raises
        what the commit raised, or :class:`TimeoutError` at the deadline; nothing is recorded then, so the
        same watermarks are due again. Call it after ``dispatcher.stop()`` to commit the drained work
        before closing the client."""
        due = self._tracker.get().due()
        if not due:
            return
        offsets = {_topic_partition(d.topic, d.partition): d.offset for d in due}
        # The deadline bounds aiokafka's own retry loop, which waits for a coordinator without end.
        await asyncio.wait_for(self._client.commit(offsets), self._commit_timeout_s)
        for d in due:
            self._tracker.get().committed(d.topic, d.partition, d.offset)

    async def _flush_quietly(self) -> None:
        """Flush, and log a failure rather than raise it: the commit is retried at the next interval, and a
        lost commit only means redelivery after a restart."""
        try:
            await self.flush()
        except Exception as e:
            logger.warning("edax kafka: commit failed; the watermarks stay due", exc_info=True)
            # Absent for a flush called outside `run`, whose failure the log line carries.
            if self._dispatcher is not None:
                self._dispatcher.observer.commit_failed(e)

    async def _commit_loop(self) -> None:
        while True:
            await asyncio.sleep(self._commit_interval_s)
            await self._flush_quietly()

    async def run(self, dispatcher: KeyOrderedDispatcher) -> None:
        """Poll and submit until cancelled or the client stops, committing on the interval. Refuses a
        dispatcher without ``on_dead``, and a client subscribed without :meth:`rebalance_listener`, with
        :class:`ValueError`, before it polls anything. It checks the listener here rather than at build,
        because the client subscribes with this consumer's listener after the consumer is built."""
        if dispatcher.on_dead is None:
            raise ValueError(
                "KafkaConsumer.run needs a dispatcher with on_dead: a nack rewinds the partition, so a "
                "message that fails for good with no sink stalls its partition forever. Wire a "
                "KafkaDeadLetterSink or another sink."
            )
        _check_listener(self._client)
        self._dispatcher = dispatcher
        committer = asyncio.create_task(self._commit_loop())
        try:
            await super().run(dispatcher)
        finally:
            committer.cancel()
            await asyncio.gather(committer, return_exceptions=True)
            try:
                # Still attached, so a failed last commit reaches the observer, not only the log.
                await asyncio.shield(self._flush_quietly())
            finally:
                self._dispatcher = None

    def rebalance_listener(self, drain_timeout_s: float = 30.0) -> ConsumerRebalanceListener:
        """An ``aiokafka`` rebalance listener for ``subscribe([...], listener=...)``. On revoke it commits
        what is due and resets the revoked partitions, so the deliveries still queued are skipped rather
        than drained. It then waits, up to ``drain_timeout_s``, until no handler of the dispatcher
        :meth:`run` feeds is running, so no key is handled by two members at once. The new owner starts
        from the commit, so the skipped work and the handlers that were still running run again there:
        at-least-once. A handler still running at the deadline overlaps the new owner, so keep the
        deadline above the slowest handler and below the client's ``max_poll_interval_ms``. On assign it
        resets the assigned partitions, in case a revoke was never announced."""
        from aiokafka import ConsumerRebalanceListener

        consumer = self

        class _Listener(ConsumerRebalanceListener):  # type: ignore[misc]  # aiokafka ships no stubs
            async def on_partitions_revoked(self, revoked: Collection[TopicPartition]) -> None:
                await consumer._revoke(revoked, drain_timeout_s)

            async def on_partitions_assigned(self, assigned: Collection[TopicPartition]) -> None:
                for tp in assigned:
                    consumer.reset(tp)

        return _Listener()

    async def _revoke(self, revoked: Collection[TopicPartition], timeout: float) -> None:
        # Commit what has settled while the partitions are still ours.
        await self._flush_quietly()
        # Retire everything of the revoked partitions at once: the queued deliveries are skipped rather
        # than drained, so a deep queue cannot outlast the rebalance. The new owner fetches them again
        # from the commit.
        for tp in revoked:
            self.reset(tp)
        # A handler already running finishes, and its ack settles nothing. Wait for it, so the new owner
        # never runs a key this member is still running. Polled rather than signalled: a retired handler
        # finishing settles nothing, so no settlement would announce it.
        dispatcher = self._dispatcher
        if dispatcher is None:
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while running := dispatcher.stats().handling:
            remaining = deadline - loop.time()
            if remaining <= 0:
                logger.warning(
                    "edax kafka: %d handler(s) still running %ss after a revoke; the new owner may overlap",
                    running,
                    timeout,
                )
                dispatcher.observer.revoke_overrun(running)
                return
            await asyncio.sleep(min(_DRAIN_POLL_S, remaining))

    async def poll(self) -> Delivery | None:
        try:
            msg = await self._client.getone()
        except Exception as e:
            if _consumer_stopped(e):
                return None
            raise
        # `ack` and `nack` settle long after this frame returns; they capture these, never `msg`.
        topic, partition, offset = msg.topic, msg.partition, msg.offset
        # Book the offset before decoding: an undecodable payload still has to be settled, and an unbooked
        # offset is either committed past by the next ack or replayed into the same failure forever. Booked
        # before the next await, so a nack's rewind retires it if it lands later.
        with mapped():  # a negative offset is the kernel's terminal `codec:` refusal
            token = self._tracker.get().book(topic, partition, offset)
        # A null key, value or header value crosses as empty bytes: the kernel record's rule, which also
        # routes a null content-type like an empty one.
        fetched = KafkaFetched(
            topic,
            partition,
            offset,
            msg.key or b"",
            msg.value or b"",
            tuple((k, v or b"") for k, v in (msg.headers or ())),
            getattr(msg, "timestamp", None),
            getattr(msg, "timestamp_type", None),
        )

        def live() -> bool:
            return self._tracker.get().is_live(token)

        def ack() -> None:
            # A stale ack settles nothing, by the kernel's token rule; the committer commits what is due.
            self._tracker.get().ack(token)

        def nack() -> None:
            # The kernel rewinds the partition to its lowest unsettled offset and retires every delivery
            # of it already handed out; seek() sets the partition's one fetch position.
            target = self._tracker.get().nack(token).seek
            if target is not None:
                self._client.seek(_topic_partition(topic, partition), target)

        def poison(cause: BaseException) -> UndecodableDelivery:
            """The fetched bytes as a dead-letterable delivery, settled by the offset already booked."""
            # Topic, partition and offset together are the only identity a poison message has. The offset
            # alone repeats on every partition, so two poison messages would share one id and a
            # dead-letter queue keyed on it would keep the first and reject the second.
            poison_id = _edax.kafka_poison_id(topic, partition, offset)
            error = CodecError(f"kafka {poison_id} did not decode: {cause}")
            error.__cause__ = cause  # `raise ... from e` is unavailable: the error is carried, not raised
            return UndecodableDelivery(fetched.value, poison_id, ack, nack, error, live=live, origin=fetched)

        if self._middleware:
            # The kernel record exists for the middleware to rewrite; building one with no middleware to
            # run would copy the payload across the binding for nothing.
            record = fetched.record()
            try:
                for m in reversed(self._middleware):
                    record = m.on_poll(record)
            except CodecError as e:
                # Only a codec failure is poison here. A hook whose key service is down fails
                # transiently, so anything else propagates and the message redelivers.
                return poison(e)
            except Exception:
                # The offset is already booked. Left unsettled it pins the partition watermark, and the
                # fetch position has moved past the record, so it would never come back. Nack seeks back
                # to it; the consumer loop waits a backoff step before it polls again.
                nack()
                raise
            value, headers = record.value, record.headers
        else:
            value, headers = fetched.value, [KafkaHeader(k, v) for k, v in fetched.headers]

        route = _edax.kafka_route(headers, self._media_types, self._default.kind)
        if route.registered is not None:
            serializer = self._serializers[route.registered]
        elif route.unknown is not None:
            # Never fall back to the default: decoding msgpack bytes as the host's own format is how a
            # wrong message reaches a handler instead of the dead-letter sink.
            return poison(CodecError(f"no serializer for content type {route.unknown!r}"))
        elif route.headerless:
            return poison(
                CodecError("the record has no readable content-type header, and a command always names one")
            )
        else:
            serializer = self._default
        try:
            decoded = serializer.decode(value)
        except Exception as e:  # noqa: BLE001 - every failure of a pure decode is terminal
            # A third-party serializer raises its own error types, and a decode that fails once fails
            # the same way on every redelivery, so the record is poison whatever it raised.
            return poison(e)
        return Delivery(decoded, ack, nack, live=live, origin=fetched)

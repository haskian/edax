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
import math
import warnings
from collections.abc import Awaitable, Collection, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Protocol, cast, get_args

from .. import _native, errors
from .._deadline import within
from .._owned import ThreadBound
from .._wiring import I64_MAX, is_int
from ..broker import KeyFn, consume
from ..delivery import DeadLetter, Delivery, Undecodable
from ..dispatcher import KeyOrderedDispatcher
from ..errors import BadHandle, CodecError
from ..message import CommandMessage, EventMessage, MessageKind, message_kind
from ..observe import error_type
from ..serializer import KernelSerializer, Serializer
from ..tenancy import Tenancy

if TYPE_CHECKING:  # the base package imports no broker library; these names are for the annotations only
    from aiokafka import ConsumerRebalanceListener, TopicPartition  # type: ignore[import-untyped]

__all__ = [
    "CONTENT_TYPE",
    "ConsumerClient",
    "FetchedRecord",
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

#: How long one send may take, by default, on the producer and on the dead-letter sink alike.
_PUBLISH_TIMEOUT_S: Final = 30.0
#: How often the consumer commits what is due, by default.
_COMMIT_INTERVAL_S: Final = 1.0
#: How long one commit may take, by default.
_COMMIT_TIMEOUT_S: Final = 10.0
#: How long a revoke waits, by default, for the handlers still running.
_DRAIN_TIMEOUT_S: Final = 30.0

# The kernel's record structs, re-exported here so a middleware builds one without importing `_edax`.
KafkaRecord = _native.KafkaRecord
KafkaHeader = _native.KafkaHeader
#: One partition's consumer bookkeeping as the kernel reports it: ``unsettled`` offsets delivered and not
#: yet settled, how many of them are still ``live`` (a handler may still settle them; a rewind retired
#: the rest), the highest ``max_delivered``, and the last ``committed`` watermark (``None`` before the
#: first commit), under the ``topic`` and ``partition`` they belong to.
PartitionStats = _native.PartitionStats
#: The header naming a record value's media type, the one header edax writes. A middleware that rewrites
#: the value rewrites this header by this name, so the consumer still routes to the right serializer.
CONTENT_TYPE: str = _native.kafka_content_type_header()


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


class FetchedRecord(Protocol):
    """A record as the client hands it over, read-only: aiokafka's ``ConsumerRecord`` is one. ``timestamp``
    is the broker timestamp in Unix milliseconds and ``timestamp_type`` whose clock it is, ``None`` when the
    client reports none. A null key, value or header value is ``None``."""

    @property
    def topic(self) -> str: ...
    @property
    def partition(self) -> int: ...
    @property
    def offset(self) -> int: ...
    @property
    def key(self) -> bytes | None: ...
    @property
    def value(self) -> bytes | None: ...
    @property
    def headers(self) -> Sequence[tuple[str, bytes | None]] | None: ...
    @property
    def timestamp(self) -> int | None: ...
    @property
    def timestamp_type(self) -> int | None: ...


class ConsumerClient(Protocol):
    """The three ``AIOKafkaConsumer`` calls :class:`KafkaConsumer` makes. Any client with these works."""

    async def getone(self) -> FetchedRecord: ...

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
    """Warn as the kernel warns about the client's ``acks`` (anything but all): the outbox marks a row
    published once ``publish`` returns, so an ack that a failed leader takes with it is a lost event. The
    probe reads a private attribute of ``AIOKafkaProducer``, which holds ``acks`` as a number, so a
    different client is not judged. Build it with ``enable_idempotence=True``, which implies
    ``acks="all"``."""
    acks = getattr(getattr(client, "_sender", None), "_acks", None)
    if not is_int(acks):
        return
    warning = _native.kafka_acks_warning(acks)
    if warning is not None:
        warnings.warn(f"KafkaProducer: {warning}", stacklevel=3)


def _check_consumer(client: ConsumerClient) -> None:
    """Refuse, as the kernel refuses, a client that auto-commits or has no group: it loses or never
    commits offsets. Both probes read private attributes of ``AIOKafkaConsumer`` (``enable_auto_commit=False``
    and a ``group_id`` are the fix); a fact a different client does not expose is passed as the safe value,
    so that client is not judged."""
    _native.kafka_check_consumer(
        bool(getattr(client, "_enable_auto_commit", False)), getattr(client, "_group_id", "") is not None
    )


def _check_listener(client: ConsumerClient) -> None:
    """Refuse a client the group assigns partitions to but that has no rebalance listener. Without one a
    revoked partition keeps its bookkeeping. Once its watermark is due, every commit names it, the client
    refuses the whole commit, and no partition commits again until a restart. The probe reads a private
    attribute of ``AIOKafkaConsumer``; a different client, or one assigned its partitions by hand, is not
    judged."""
    subscription = getattr(client, "_subscription", None)
    if subscription is not None and subscription.partitions_auto_assigned() and subscription.listener is None:
        raise BadHandle(
            "KafkaConsumer.run: the client is subscribed without a rebalance listener, so a revoked "
            "partition keeps its bookkeeping and blocks every later commit. Subscribe with "
            "listener=consumer.rebalance_listener()."
        )


def _whole_ms(name: str, seconds: float) -> int:
    """``seconds`` as the whole milliseconds the kernel checks, rounded toward zero, so a duration under a
    millisecond is ``0`` and refused. A value that is no finite number of seconds, or whose count the
    kernel's signed 64-bit milliseconds cannot carry, is the terminal :class:`~edax.errors.BadHandle`, named
    by ``name``."""
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not math.isfinite(seconds):
        raise BadHandle(f"{name} must be a finite number of seconds, got {seconds!r}")
    millis = int(seconds * 1000)
    # Past the i64 range the value would fail to cross into the kernel as a `CodecError`, not as wiring.
    if abs(millis) > I64_MAX:
        raise BadHandle(f"{name} is too large for the kernel's milliseconds, got {seconds!r}")
    return millis


def _check_durations(**seconds: float) -> None:
    """Refuse, as the kernel refuses, each duration in ``seconds``, named by its ``*_s`` setting, that is not
    at least a whole millisecond: the terminal :class:`~edax.errors.BadHandle`, raised at wiring."""
    _native.kafka_check_config(
        **{name.removesuffix("_s") + "_ms": _whole_ms(name, value) for name, value in seconds.items()}
    )


@contextmanager
def _classified() -> Iterator[None]:
    """Re-raise a broker error from a client call inside as the kernel classifies its Kafka protocol error
    code (aiokafka's ``errno``): one record the broker refuses as the terminal
    :class:`~edax.errors.CodecError`, so it is dead-lettered; a client the broker refuses (authorization,
    an invalid topic, an unsupported version) as the fatal :class:`~edax.errors.BadHandle`, which stops the
    relay or the consumer; any other code as a transient :class:`~edax.errors.StoreError`. A failure with
    no protocol code (a client-side timeout, a lost connection) passes unchanged and stays unclassified,
    which is transient. aiokafka's own ``retriable`` flag is not read: it calls a client timeout final."""
    try:
        yield
    except OSError:
        raise  # its errno is the operating system's, not a Kafka protocol error code
    except Exception as e:
        error_code = getattr(e, "errno", None)
        if not is_int(error_code):
            raise
        raise errors.from_wire(_native.kafka_broker_error(error_code, str(e))) from e


async def _send(client: ProducerClient, topic: str, record: KafkaRecord, timeout_s: float) -> None:
    """Send ``record`` to ``topic`` and wait for the ack, raising the transient
    :class:`~edax.errors.StoreError` after ``timeout_s``. With ``enable_idempotence=True`` aiokafka never
    expires a batch, so without this bound a send to a broker that stopped answering waits forever. A send
    cut off at the deadline may still land: the batch stays in the client, so the caller's retry can write
    a second copy, which consumers dedup by ``msg.id``."""
    await within(
        _place_and_send(client, topic, record),
        timeout_s,
        f"kafka send to {topic!r}",
        after="the record may still land, so a retry may write it twice",
    )


async def _place_and_send(client: ProducerClient, topic: str, record: KafkaRecord) -> None:
    """Send ``record`` to ``topic`` on the kernel's partition for its key: Java's default partitioner over
    the final key and the topic's partition count from the client's metadata. One placement for the
    producer and the dead-letter sink."""
    with _classified():
        partitions = await client.partitions_for(topic)
    key = record.key  # each read of a native field copies it
    # A topic reported with no partitions is the kernel's terminal `codec:` error.
    partition = _native.kafka_partition_for(key, len(partitions))
    with _classified():
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
    :attr:`~edax.delivery.Delivery.origin`, for a dead-letter sink. A null key, value or header value is
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
        the same offset twice after a redelivery can dedup on them. The kernel renders ``edax-dlq-error``
        from the code ``error`` carries (``unclassified`` when it carries none) and its message, which names
        a host exception's class, so every SDK writes one form."""
        code = errors.code_of(error)
        message = str(error) if code is not None else f"{error_type(error)}: {error}"
        # A code is the kernel's own, so the refusal of an unknown one cannot fire.
        return _native.kafka_dead_letter_record(
            self.record(), self.topic, self.partition, self.offset, message, code
        )


class KafkaDeadLetterSink:
    """An ``on_dead`` sink that publishes a dead delivery's record to a dead-letter topic through a
    caller-owned, started ``AIOKafkaProducer``: the record as fetched, plus where it came from and why
    (:meth:`KafkaFetched.dead_letter`), on the kernel's partition for its key. A raise (the topic is
    down) nacks the delivery for another try. A dead-letter topic that refuses the record itself (too
    large: the terminal :class:`~edax.errors.CodecError`) gets the kernel's stub instead, the value dropped
    and the coordinates kept as the pointer to it, so the source partition moves on rather than hand the
    record back forever. A delivery that did not come from a :class:`KafkaConsumer` carries no record to
    republish: a wiring mistake, refused with :class:`~edax.errors.BadHandle`.
    ``publish_timeout_s`` bounds each send, as on :class:`KafkaProducer`, and is refused the same way."""

    def __init__(
        self, client: ProducerClient, topic: str, *, publish_timeout_s: float = _PUBLISH_TIMEOUT_S
    ) -> None:
        _check_durations(publish_timeout_s=publish_timeout_s)
        _check_producer(client)
        self._client = client
        self._topic = topic
        self._publish_timeout_s = publish_timeout_s

    async def __call__(self, dead: DeadLetter[Any]) -> None:
        fetched = dead.origin
        if not isinstance(fetched, KafkaFetched):
            raise BadHandle(f"dead letter of {dead.message!r} carries no Kafka record to republish")
        record = fetched.dead_letter(dead.failure)
        try:
            await _send(self._client, self._topic, record, self._publish_timeout_s)
        except CodecError:
            logger.warning(
                "edax kafka: %s refused the dead letter of %s/%d/%d; sending it without its value",
                self._topic,
                fetched.topic,
                fetched.partition,
                fetched.offset,
                exc_info=True,
            )
            await _send(
                self._client, self._topic, _native.kafka_dead_letter_stub(record), self._publish_timeout_s
            )


class RecordMiddleware(Protocol):
    """Rewrites a record on its way to or from the broker. Synchronous, bytes to bytes.

    :meth:`on_publish` runs in order, after the kernel has built the record and before the send.
    :meth:`on_poll` runs in reverse, after the fetch and before the content-type lookup, so a pair of
    middlewares wraps the record like an onion. Use one to compress or encrypt the value. Trace context
    needs none: it rides in the message's ``extensions``, inside the value. Rewrite ``content-type`` on the
    way out and restore it on the way in, so the consumer still routes to the serializer that wrote the
    bytes. On poll, a terminal error (:class:`~edax.errors.CodecError`, or a host exception that declares
    ``terminal = True``) marks the message poison, so it is dead-lettered; a fatal one
    (:class:`~edax.errors.BadHandle`) ends :meth:`KafkaConsumer.run`; any other exception is transient, so
    the message is handed back and polled again after a backoff step."""

    def on_publish(self, record: KafkaRecord) -> KafkaRecord: ...
    def on_poll(self, record: KafkaRecord) -> KafkaRecord: ...


def _check_kind(serializer: Serializer, kind: MessageKind, role: str) -> Serializer:
    """``serializer``, if it encodes ``kind``; the fatal :class:`~edax.errors.BadHandle` at wiring
    otherwise."""
    if serializer.kind != kind:
        raise BadHandle(f"{role} must be a {kind} serializer, got one of kind {serializer.kind!r}")
    return serializer


class KafkaProducer:
    """Publishes events and commands to a Kafka topic through a caller-owned, started ``AIOKafkaProducer``.
    ``serializer`` encodes an :class:`~edax.message.EventMessage` and ``command_serializer`` a
    :class:`~edax.message.CommandMessage` (default: the kernel's JSON of each kind). A serializer supplies
    the value bytes and the media type that names them; the kernel builds the record around both, so the
    key rule and the header set stay the same in every SDK.

    The partition is the kernel's too: Java's default partitioner over the record's final key (after the
    middleware) and the topic's partition count from the client's metadata, passed as ``partition=``. The
    client's own partitioner never runs, so this producer places a key exactly where a Java producer,
    Kafka Connect or a Rust edax shell does, whatever partitioner the client library defaults to.

    ``tenancy`` is how the deployment separates tenants, fixed here as it is on a
    :class:`~edax.repository.Repository`. The kernel keys the record. Under :attr:`~edax.tenancy.Tenancy.POOL`
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
    deadline :meth:`publish` raises the transient :class:`~edax.errors.StoreError`, as every edax backend
    does. The record may still land, so the relay's retry can publish it twice. Consumers already dedup
    that second copy by ``msg.id``. A deadline
    under a millisecond, or one that is no finite number, raises :class:`~edax.errors.BadHandle` here.

    A broker error is classified by its Kafka protocol error code: a record the broker refuses (too large,
    corrupt) raises the terminal :class:`~edax.errors.CodecError`, which the relay dead-letters; a refused
    client (authorization, an invalid topic) raises the fatal :class:`~edax.errors.BadHandle`, which ends
    :meth:`~edax.outbox.Relay.run`; anything else stays transient."""

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
        _check_durations(publish_timeout_s=publish_timeout_s)
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
        # The kind is read once and picks both the serializer and the record rule. `message_kind` refuses a
        # non-message with TypeError, so each cast names the type it checked.
        event = message_kind(msg) == "event"
        serializer = self._serializer if event else self._commands
        value = serializer.encode(msg)
        # Host code: its own TypeError stays a transient failure.
        key = self._key(msg) if self._key else None
        # The kernel keys the record: the override, else the ordering key under this producer's tenancy.
        tenancy, media_type = self._tenancy.value, serializer.media_type
        record = (
            _native.kafka_event_record(tenancy, cast(EventMessage, msg), value, media_type, key)
            if event
            else _native.kafka_command_record(tenancy, cast(CommandMessage, msg), value, media_type, key)
        )
        for m in self._middleware:
            record = m.on_publish(record)
        await _send(self._client, self._topic, record, self._publish_timeout_s)


class _OffsetCommitter:
    """The consumer's offset bookkeeping and its commits. The kernel's tracker books every fetched offset
    and settles it; a flush commits the watermarks it says are due, in one call, under the commit
    deadline."""

    def __init__(self, client: ConsumerClient, commit_timeout_s: float) -> None:
        self._client = client
        self._commit_timeout_s = commit_timeout_s
        # Built by its first use, on the thread that runs the consumer, which need not be the one building it.
        self.tracker = ThreadBound.lazy(_native.offset_tracker, "KafkaConsumer")
        # aiokafka frees its own commit lock while a commit backs off to retry, so a newer watermark could
        # land first and the older one, retried, would move the broker back. So one commit runs at a time,
        # and each reads what is due only after the one before it ended.
        self._serial = asyncio.Lock()

    # Neither builds the tracker: a gauge scraped on another thread before the first poll would bind it
    # to that thread, and the consumer's own poll would then refuse it. Unbuilt, it has booked nothing.
    def stats(self) -> dict[TopicPartition, PartitionStats]:
        tracker = self.tracker.peek()
        rows = () if tracker is None else tracker.stats()
        return {_topic_partition(row.topic, row.partition): row for row in rows}

    def reset(self, tp: TopicPartition) -> None:
        tracker = self.tracker.peek()
        if tracker is not None:
            tracker.reset(tp.topic, tp.partition)

    async def revoke(self, revoked: Collection[TopicPartition]) -> None:
        """Give up ``revoked`` and commit each partition's last due watermark. The kernel drops the
        bookkeeping in the step that reads the watermark, so nothing settles between the read and the drop."""
        async with self._serial:
            tracker = self.tracker.get()
            last = [due for tp in revoked if (due := tracker.revoke(tp.topic, tp.partition)) is not None]
            await self._commit(last)

    async def flush(self) -> None:
        async with self._serial:
            await self._commit(self.tracker.get().due())

    async def _commit(self, due: Sequence[_native.KafkaDue]) -> None:
        """Commit ``due`` in one call, under the commit deadline, and report each row back. A row of a
        partition given up already reports nothing: the kernel tracks it no more."""
        if not due:
            return
        offsets = {_topic_partition(d.topic, d.partition): d.offset for d in due}
        # The deadline bounds aiokafka's own retry loop, which waits for a coordinator without end.
        with _classified():
            await within(self._client.commit(offsets), self._commit_timeout_s, "kafka commit")
        for d in due:
            self.tracker.get().committed(d.topic, d.partition, d.offset)


@dataclass(frozen=True)
class _Poison:
    """A fetched record no handler may see, and why: it is dead-lettered, never retried."""

    cause: BaseException


class _RecordDecoder:
    """Turns a fetched record into a message: the middleware rewrites it, the kernel routes it by its
    ``content-type`` header, and the chosen serializer decodes it. A record that fails the same way on
    every redelivery comes back as :class:`_Poison`."""

    def __init__(
        self,
        serializer: Serializer | None,
        serializers: Sequence[Serializer],
        middleware: Sequence[RecordMiddleware],
    ) -> None:
        # The default decodes a record that carries no `content-type` header at all, and claims its
        # own media type in the registry like every other serializer. A header-less record is read as
        # an event only: a command always names its kind.
        self._default: Serializer = serializer or KernelSerializer("json")
        self._serializers: list[Serializer] = [self._default, *serializers]
        for registered in self._serializers:
            # The kernel routes every record by the default's kind, and a delivery carries what a serializer
            # decoded: an unknown kind would fail each poll after its offset is booked, pinning the partition.
            if registered.kind not in get_args(MessageKind):
                raise BadHandle(f"a consumer serializer must be of a message kind, got {registered.kind!r}")
        self._media_types: list[str] = [s.media_type for s in self._serializers]
        # A media type claimed twice is the kernel's `already_registered:` refusal.
        _native.kafka_check_media_types(self._media_types)
        self._middleware: tuple[RecordMiddleware, ...] = tuple(middleware)

    def __call__(self, fetched: KafkaFetched) -> EventMessage | CommandMessage | _Poison:
        """The message ``fetched`` carries, or why it is poison."""
        rewritten = self._rewrite(fetched)
        return rewritten if isinstance(rewritten, _Poison) else self._decode(*rewritten)

    def _rewrite(self, fetched: KafkaFetched) -> tuple[bytes, list[KafkaHeader]] | _Poison:
        """The record's value and headers after the middleware. A middleware failure is read as the kernel
        reads its code: a terminal one fails alike on every redelivery, so the record is poison; a fatal
        one (a hook wired wrong) and a transient one (a key service that is down) propagate."""
        if not self._middleware:
            return fetched.value, [KafkaHeader(k, v) for k, v in fetched.headers]
        # The kernel record exists for the middleware to rewrite; building one with no middleware to
        # run would copy the payload across the binding for nothing.
        record = fetched.record()
        try:
            for m in reversed(self._middleware):
                record = m.on_poll(record)
        except Exception as e:
            if errors.failure_kind(e) == "terminal":
                return _Poison(e)
            raise
        return record.value, record.headers

    def _decode(self, value: bytes, headers: list[KafkaHeader]) -> EventMessage | CommandMessage | _Poison:
        # The default's kind is checked at build, so the kernel's `codec:` refusal cannot fire.
        route = _native.kafka_route(headers, self._media_types, self._default.kind)
        if route.registered is not None:
            serializer = self._serializers[route.registered]
        elif route.unknown is not None:
            # Never fall back to the default: decoding msgpack bytes as the host's own format is how a
            # wrong message reaches a handler instead of the dead-letter sink.
            return _Poison(CodecError(f"no serializer for content type {route.unknown!r}"))
        elif route.headerless:
            return _Poison(
                CodecError("the record has no readable content-type header, and a command always names one")
            )
        else:
            serializer = self._default
        try:
            return serializer.decode(value)
        except Exception as e:  # noqa: BLE001 - every failure of a pure decode is terminal
            # A third-party serializer raises its own error types, and a decode that fails once fails
            # the same way on every redelivery, so the record is poison whatever it raised.
            return _Poison(e)


class KafkaConsumer:
    """Consumes a Kafka topic through a caller-owned, subscribed ``AIOKafkaConsumer``.

    ``serializer`` is the default (the kernel's JSON events unless given): it claims its own media type in
    the registry, and an event serializer also decodes a record that carries no ``content-type`` header.
    ``serializers`` extends the registry with one entry per media type, of either kind. The kernel routes
    each record by its header, and the delivery carries what the chosen serializer decoded, an
    :class:`~edax.message.EventMessage` or a :class:`~edax.message.CommandMessage`. A record of a media
    type nobody registered is poison. So is a header-less record when the default decodes commands: a
    command always names its kind, so a command consumer takes ``serializer=KernelSerializer("json",
    "command")``. ``middleware`` rewrites each record on the way in, in reverse
    order. See ``docs/backends/kafka.md``.

    The client must not auto-commit and must have a group, and every serializer's ``kind`` must be a message
    kind, or this raises :class:`~edax.errors.BadHandle`.
    :meth:`run` needs a dispatcher with ``on_dead``: on Kafka a nack rewinds the partition, so a message that
    fails for good with nowhere to go stalls its partition forever. A nack seeks the partition back to its
    lowest unsettled offset and retires the deliveries of it already queued, so a key keeps its order.

    Acks never wait on the broker. :meth:`run` commits what the kernel says is due every
    ``commit_interval_s`` seconds in one call, under a ``commit_timeout_s`` deadline, and once more when it
    ends; :meth:`flush` commits on demand. A rebalance needs :meth:`rebalance_listener` passed to
    ``subscribe``: it commits each revoked partition's last watermark as it drops the partition, and waits
    for the running handlers.
    :meth:`run` refuses a subscribed client without a listener. Each duration must be at least a millisecond
    and finite, or it raises :class:`~edax.errors.BadHandle` at build.

    A broker error on a fetch or a commit is classified by its Kafka protocol error code. A refused client
    (authorization, an invalid topic) is the fatal :class:`~edax.errors.BadHandle`: :meth:`run` ends on it,
    from the poll as from the commit loop.

    The offset bookkeeping is native state bound to the thread that first uses it, the one that runs the
    consumer: a call from any other thread raises the terminal :class:`~edax.errors.BadHandle`."""

    def __init__(
        self,
        client: ConsumerClient,
        *,
        serializer: Serializer | None = None,
        serializers: Sequence[Serializer] = (),
        middleware: Sequence[RecordMiddleware] = (),
        commit_interval_s: float = _COMMIT_INTERVAL_S,
        commit_timeout_s: float = _COMMIT_TIMEOUT_S,
    ) -> None:
        _check_durations(commit_interval_s=commit_interval_s, commit_timeout_s=commit_timeout_s)
        _check_consumer(client)
        self._client = client
        self._decoder = _RecordDecoder(serializer, serializers, middleware)
        self._offsets = _OffsetCommitter(client, commit_timeout_s)
        self._commit_interval_s = commit_interval_s
        # The dispatcher `run` feeds, so a rebalance drain can wait for its running handlers too.
        self._dispatcher: KeyOrderedDispatcher[Any] | None = None

    def stats(self) -> dict[TopicPartition, PartitionStats]:
        """A snapshot of the bookkeeping, keyed by ``TopicPartition``, for a health check or a gauge."""
        return self._offsets.stats()

    def reset(self, tp: TopicPartition) -> None:
        """Drop the bookkeeping for a revoked partition and retire its deliveries, so the dispatcher skips
        the ones still queued. Its unsettled offsets are the new owner's to redeliver from the last
        committed watermark, and a stale watermark here would otherwise refuse a lower, correct commit if
        the partition is assigned back. :meth:`rebalance_listener` calls it for every assigned partition, in
        case a revoke was never announced; a revoke itself drops a partition with its commit."""
        self._offsets.reset(tp)

    async def flush(self) -> None:
        """Commit every partition whose watermark moved, in one call, under the commit deadline. Raises
        what the commit raised, or the transient :class:`~edax.errors.StoreError` at the deadline; nothing
        is recorded then, so the same watermarks are due again. Call it after ``dispatcher.stop()`` to
        commit the drained work before closing the client."""
        await self._offsets.flush()

    async def _flush_quietly(self) -> Exception | None:
        """Flush, and log a failure rather than raise it: the commit is retried at the next interval, and a
        lost commit only means redelivery after a restart. Returns the failure, ``None`` once it landed."""
        return await self._quietly(self.flush())

    async def _quietly(self, commit: Awaitable[None]) -> Exception | None:
        """Await ``commit``, and log and observe a failure rather than raise it. Returns the failure,
        ``None`` once it landed."""
        try:
            await commit
        except Exception as e:
            logger.warning("edax kafka: commit failed; the watermarks stay due", exc_info=True)
            # Absent for a flush called outside `run`, whose failure the log line carries.
            if self._dispatcher is not None:
                self._dispatcher.observer.commit_failed(e)
            return e
        return None

    async def _commit_loop(self) -> None:
        """Commit on the interval until cancelled. A fatal commit failure ends the loop, raised: every later
        commit would fail alike, and :meth:`run` stops on it."""
        while True:
            await asyncio.sleep(self._commit_interval_s)
            failure = await self._flush_quietly()
            if failure is not None and errors.is_fatal(failure):
                raise failure

    async def run(self, dispatcher: KeyOrderedDispatcher[Any]) -> None:
        """Poll and submit until cancelled or the client stops, committing on the interval. Refuses a
        dispatcher without ``on_dead``, and a client subscribed without :meth:`rebalance_listener`, with
        :class:`~edax.errors.BadHandle`, before it polls anything. It checks the listener here rather than at
        build, because the client subscribes with this consumer's listener after the consumer is built."""
        _native.kafka_check_dead_sink(dispatcher.on_dead is not None)
        _check_listener(self._client)
        self._dispatcher = dispatcher
        committer = asyncio.create_task(self._commit_loop())
        polling = asyncio.ensure_future(consume(self, dispatcher))
        try:
            done, _ = await asyncio.wait({polling, committer}, return_when=asyncio.FIRST_COMPLETED)
            if polling not in done:
                # The commit loop ends only on a fatal commit failure: stop polling and raise it.
                polling.cancel()
                await asyncio.gather(polling, return_exceptions=True)
                committer.result()
            polling.result()
        finally:
            for task in (polling, committer):
                task.cancel()
            await asyncio.gather(polling, committer, return_exceptions=True)
            try:
                # Still attached, so a failed last commit reaches the observer, not only the log.
                await asyncio.shield(self._flush_quietly())
            finally:
                self._dispatcher = None

    def rebalance_listener(self, drain_timeout_s: float = _DRAIN_TIMEOUT_S) -> ConsumerRebalanceListener:
        """An ``aiokafka`` rebalance listener for ``subscribe([...], listener=...)``. On revoke the kernel
        hands back each revoked partition's last due watermark as it drops the partition, and it commits
        them, so the deliveries still queued are skipped rather than drained. It then waits, up to
        ``drain_timeout_s``, until no handler of the dispatcher :meth:`run` feeds is running, so the new
        owner rarely runs a key this member still runs. A member the group evicts loses its partitions
        before this listener runs, and no wait covers that. The new owner starts
        from the commit, so the skipped work and the handlers that were still running run again there:
        at-least-once. A handler still running at the deadline overlaps the new owner, so keep the
        deadline above the slowest handler and below the client's ``max_poll_interval_ms``. On assign it
        resets the assigned partitions, in case a revoke was never announced. A deadline under a millisecond,
        or one that is no finite number, raises :class:`~edax.errors.BadHandle`: with no wait, every revoke
        would hand a running key to the new owner."""
        _check_durations(drain_timeout_s=drain_timeout_s)
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
        # The kernel hands back each revoked partition's last watermark and retires everything of it at
        # once: the queued deliveries are skipped rather than drained, so a deep queue cannot outlast the
        # rebalance. The new owner fetches them again from the commit, made while the partitions are
        # still ours. A failed commit only means more redelivery there.
        await self._quietly(self._offsets.revoke(revoked))
        # A handler already running finishes, and its ack settles nothing. Wait for it, so the new owner
        # never runs a key this member is still running.
        dispatcher = self._dispatcher
        if dispatcher is None:
            return
        running = await dispatcher.wait_handlers(timeout)
        if running:
            logger.warning(
                "edax kafka: %d handler(s) still running %ss after a revoke; the new owner may overlap",
                running,
                timeout,
            )
            dispatcher.observer.revoke_overrun(running)

    async def poll(self) -> Delivery[EventMessage | CommandMessage] | Undecodable | None:
        try:
            with _classified():
                msg = await self._client.getone()
        except Exception as e:
            if _consumer_stopped(e):
                return None
            raise
        # Book the offset before decoding: an undecodable payload still has to be settled, and an unbooked
        # offset is either committed past by the next ack or replayed into the same failure forever. Booked
        # before the next await, so a nack's rewind retires it if it lands later. A negative offset is the
        # kernel's terminal `codec:` refusal.
        booked = _Booked(self._client, self._offsets.tracker, msg.topic, msg.partition, msg.offset)
        try:
            # A null key, value or header value crosses as empty bytes: the kernel record's rule, which also
            # routes a null content-type like an empty one.
            fetched = KafkaFetched(
                msg.topic,
                msg.partition,
                msg.offset,
                msg.key or b"",
                msg.value or b"",
                tuple((k, v or b"") for k, v in (msg.headers or ())),
                msg.timestamp,
                msg.timestamp_type,
            )
            decoded = self._decoder(fetched)
            if isinstance(decoded, _Poison):
                return booked.poison(fetched, decoded.cause)
            return Delivery(decoded, booked.ack, booked.nack, live=booked.live, origin=fetched)
        except Exception:
            # The offset is already booked. Left unsettled it pins the partition watermark, and the
            # fetch position has moved past the record, so it would never come back. Nack seeks back
            # to it; the consumer loop waits a backoff step before it polls again. A failure before the
            # book would instead let the next ack commit past the record, so everything that can fail
            # runs after it, here.
            booked.nack()
            raise


class _Booked:
    """One fetched record's booked offset and the handles that settle it. They run long after the poll
    returned, so they hold the record's coordinates and token, never the record."""

    def __init__(
        self,
        client: ConsumerClient,
        tracker: ThreadBound[_native.OffsetTracker],
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        self._client = client
        self._tracker = tracker
        self._topic, self._partition, self._offset = topic, partition, offset
        self._token = self._tracker.get().book(topic, partition, offset)

    def live(self) -> bool:
        return self._tracker.get().is_live(self._token)

    # A stale token (a rewind or a rebalance retired it) settles nothing, by the kernel's token rule,
    # and loses nothing: the redelivered copy settles the offset. Logged at debug, as the memory
    # broker does, since rebalances make it routine.
    def ack(self) -> None:
        # The committer commits what is due.
        if not self._tracker.get().ack(self._token):
            logger.debug("edax kafka: ack of a settled or retired delivery %d", self._token)

    def nack(self) -> None:
        # The kernel rewinds the partition to its lowest unsettled offset and retires every delivery
        # of it already handed out; seek() sets the partition's one fetch position.
        target = self._tracker.get().nack(self._token).seek
        if target is None:
            logger.debug("edax kafka: nack of a settled or retired delivery %d", self._token)
            return
        self._client.seek(_topic_partition(self._topic, self._partition), target)

    def poison(self, fetched: KafkaFetched, cause: BaseException) -> Undecodable:
        """The fetched bytes as a dead-letterable record, settled by the offset already booked."""
        # Topic, partition and offset together are the only identity a poison message has. The offset
        # alone repeats on every partition, so two poison messages would share one id and a
        # dead-letter queue keyed on it would keep the first and reject the second.
        poison_id = _native.kafka_poison_id(self._topic, self._partition, self._offset)
        error = CodecError(f"kafka {poison_id} did not decode: {cause}")
        error.__cause__ = cause  # `raise ... from e` is unavailable: the error is carried, not raised
        return Undecodable(poison_id, fetched.value, error, self.ack, self.nack, origin=fetched)

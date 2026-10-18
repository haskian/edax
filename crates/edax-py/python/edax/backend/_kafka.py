"""Shared Kafka client contracts, decoding and generation-fenced offset lifecycle."""

from __future__ import annotations

import asyncio
import logging
import math
import traceback
import warnings
from collections.abc import Awaitable, Callable, Collection, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, get_args

from .. import _native, errors
from .._deadline import within
from .._owned import ThreadBound
from .._wiring import I64_MAX, is_int
from ..delivery import Undecodable
from ..errors import BadHandle, CodecError
from ..message import CommandMessage, EventMessage, MessageKind
from ..observe import error_type
from ..serializer import KernelSerializer, Serializer

if TYPE_CHECKING:
    from aiokafka import ConsumerRebalanceListener, TopicPartition  # type: ignore[import-untyped]

logger = logging.getLogger("edax.backend.kafka")
KafkaRecord = _native.KafkaRecord
KafkaHeader = _native.KafkaHeader
PartitionStats = _native.PartitionStats


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
    """The three ``AIOKafkaConsumer`` calls :class:`KafkaConsumer` makes.

    Custom clients using the rebalance listener must also preserve its ordering: invoke the
    assignment callback before ``getone`` can return records from the new assignment. The callback fences
    pending fetches and resets delivery tokens before the new records are admitted.
    """

    async def getone(self) -> FetchedRecord: ...

    async def commit(self, offsets: dict[TopicPartition, int], /) -> Any: ...

    def seek(self, partition: TopicPartition, offset: int, /) -> Any: ...


def topic_partition(topic: str, partition: int) -> TopicPartition:
    """The client's own key type for a partition. Imported lazily, because the base package installs no
    broker library and must import without one."""
    from aiokafka import TopicPartition

    return TopicPartition(topic, partition)


def consumer_stopped(e: Exception) -> bool:
    """Whether ``getone`` raised because the client was stopped: the end of the stream, not a failure."""
    try:
        from aiokafka.errors import ConsumerStoppedError  # type: ignore[import-untyped]
    except ImportError:  # a client that is not aiokafka has no such error
        return False
    return isinstance(e, ConsumerStoppedError)


def check_producer(client: ProducerClient) -> None:
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


def check_consumer(client: ConsumerClient) -> None:
    """Refuse, as the kernel refuses, a client that auto-commits or has no group: it loses or never
    commits offsets. Both probes read private attributes of ``AIOKafkaConsumer`` (``enable_auto_commit=False``
    and a ``group_id`` are the fix); a fact a different client does not expose is passed as the safe value,
    so that client is not judged."""
    _native.kafka_check_consumer(
        bool(getattr(client, "_enable_auto_commit", False)), getattr(client, "_group_id", "") is not None
    )


def check_listener(client: ConsumerClient) -> None:
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


def whole_ms(name: str, seconds: float) -> int:
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


def check_durations(**seconds: float) -> None:
    """Refuse, as the kernel refuses, each duration in ``seconds``, named by its ``*_s`` setting, that is not
    at least a whole millisecond: the terminal :class:`~edax.errors.BadHandle`, raised at wiring."""
    _native.kafka_check_config(
        **{name.removesuffix("_s") + "_ms": whole_ms(name, value) for name, value in seconds.items()}
    )


@contextmanager
def classified() -> Iterator[None]:
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


async def send(client: ProducerClient, topic: str, record: KafkaRecord, timeout_s: float) -> None:
    """Send ``record`` to ``topic`` and wait for the ack, raising the transient
    :class:`~edax.errors.StoreError` after ``timeout_s``. With ``enable_idempotence=True`` aiokafka never
    expires a batch, so without this bound a send to a broker that stopped answering waits forever. A send
    cut off at the deadline may still land: the batch stays in the client, so the caller's retry can write
    a second copy, which consumers dedup by ``msg.id``."""
    await within(
        place_and_send(client, topic, record),
        timeout_s,
        f"kafka send to {topic!r}",
        after="the record may still land, so a retry may write it twice",
    )


async def place_and_send(client: ProducerClient, topic: str, record: KafkaRecord) -> None:
    """Send ``record`` to ``topic`` on the kernel's partition for its key: Java's default partitioner over
    the final key and the topic's partition count from the client's metadata. One placement for the
    producer and the dead-letter sink."""
    with classified():
        partitions = await client.partitions_for(topic)
    key = record.key  # each read of a native field copies it
    # A topic reported with no partitions is the kernel's terminal `codec:` error.
    partition = _native.kafka_partition_for(key, len(partitions))
    with classified():
        await client.send_and_wait(
            topic,
            value=record.value,
            key=key,
            partition=partition,
            headers=[(h.key, h.value) for h in record.headers],
        )


COMMIT_TIMEOUT_S = 10.0
DRAIN_TIMEOUT_S = 30.0


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

    @classmethod
    def of(cls, record: FetchedRecord) -> KafkaFetched:
        """Normalize a client record using the kernel's null-to-empty byte convention."""
        return cls(
            record.topic,
            record.partition,
            record.offset,
            record.key or b"",
            record.value or b"",
            tuple((k, v or b"") for k, v in (record.headers or ())),
            record.timestamp,
            record.timestamp_type,
        )

    def record(self) -> KafkaRecord:
        """The fetched record as the kernel's struct."""
        return KafkaRecord(self.key, self.value, [KafkaHeader(k, v) for k, v in self.headers])

    def dead_letter(self, error: BaseException) -> KafkaRecord:
        """The fetched record with prior error headers removed and four ``edax-dlq-*`` headers added.
        These name its topic, partition, offset and error code (``unclassified`` when absent).
        Exception text is never persisted. The coordinates identify the record, so a sink that sees
        the same offset twice after a redelivery can dedup on them."""
        code = errors.code_of(error)
        message = str(error) if code is not None else f"{error_type(error)}: {error}"
        # A code is the kernel's own, so the refusal of an unknown one cannot fire.
        return _native.kafka_dead_letter_record(
            self.record(), self.topic, self.partition, self.offset, message, code
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


class OffsetCommitter:
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
        self._pending: asyncio.Task[None] | None = None
        self._interrupted = False

    def unfinished(self) -> asyncio.Task[None] | None:
        """The actual broker commit still holding serialization, including cancelled waiters."""
        return self._pending if self._pending is not None and not self._pending.done() else None

    def check_idle(self) -> None:
        """An interrupted waiter must not admit new work while its client commit is unresolved. The
        failure is transient: the next poll or commit retries once the client call ends."""
        if self._interrupted and self._pending is not None and not self._pending.done():
            raise errors.StoreError("kafka commit still running after its deadline or a cancellation")

    # Neither builds the tracker: a gauge scraped on another thread before the first poll would bind it
    # to that thread, and the consumer's own poll would then refuse it. Unbuilt, it has booked nothing.
    def stats(self) -> dict[TopicPartition, PartitionStats]:
        tracker = self.tracker.peek()
        rows = () if tracker is None else tracker.stats()
        return {topic_partition(row.topic, row.partition): row for row in rows}

    def reset(self, tp: TopicPartition) -> None:
        tracker = self.tracker.peek()
        if tracker is not None:
            tracker.reset(tp.topic, tp.partition)

    def retire(self, revoked: Collection[TopicPartition]) -> list[_native.KafkaDue]:
        """Give up ``revoked`` and commit each partition's last due watermark. The kernel drops the
        bookkeeping in the step that reads the watermark, so nothing settles between the read and the drop."""
        tracker = self.tracker.get()
        return [due for tp in revoked if (due := tracker.revoke(tp.topic, tp.partition)) is not None]

    async def commit_retired(self, last: Sequence[_native.KafkaDue]) -> None:
        async with self._serial:
            self.check_idle()
            await self._commit(last)

    async def flush(self) -> None:
        async with self._serial:
            self.check_idle()
            await self._commit(self.tracker.get().due())

    async def _commit(self, due: Sequence[_native.KafkaDue]) -> None:
        """Commit ``due`` in one call, under the commit deadline, and report each row back. A row of a
        partition given up already reports nothing: the kernel tracks it no more."""
        if not due:
            return
        task = asyncio.create_task(self.send(due))
        self._pending = task
        self._interrupted = False
        task.add_done_callback(retrieve_failure)
        try:
            done, _ = await asyncio.wait({task}, timeout=self._commit_timeout_s)
            if not done:
                raise errors.StoreError(f"kafka commit: no reply within {self._commit_timeout_s * 1000:g} ms")
            task.result()
        finally:
            # aiokafka shields its request: cancelling this task would hide live broker I/O.
            # Retain the entire client call until it finishes, and fence admission in the meantime.
            if not task.done():
                self._interrupted = True

    async def send(self, due: Sequence[_native.KafkaDue]) -> None:
        offsets = {topic_partition(d.topic, d.partition): d.offset for d in due}
        with classified():
            await self._client.commit(offsets)
        for d in due:
            self.tracker.get().committed_generation(d.topic, d.partition, d.offset, d.generation)


def record_bytes(record: FetchedRecord) -> int:
    """The bytes ``record`` counts against ``max_record_bytes``, measured by the kernel."""
    headers = [KafkaHeader(key, value or b"") for key, value in record.headers or ()]
    return _native.kafka_record_bytes(
        KafkaRecord(record.key or b"", record.value or b"", headers), record.topic
    )


def clear_failure_frames(error: BaseException) -> None:
    """Release the frame locals of ``error`` and every cause behind it, keeping types, messages and
    traceback locations. A saved failure must not keep an already-released batch alive."""
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        failure = pending.pop()
        if id(failure) in seen:
            continue
        seen.add(id(failure))
        if failure.__traceback__ is not None:
            traceback.clear_frames(failure.__traceback__)
        for cause in (failure.__cause__, failure.__context__, getattr(failure, "cause", None)):
            if isinstance(cause, BaseException):
                pending.append(cause)


def rebalance_listener(
    on_revoked: Callable[[Collection[TopicPartition]], Awaitable[None]],
    on_assigned: Callable[[Collection[TopicPartition]], Awaitable[None]],
) -> ConsumerRebalanceListener:
    """An aiokafka rebalance listener that hands both callbacks to a runner. Imported lazily, because
    the base package installs no broker library."""
    from aiokafka import ConsumerRebalanceListener

    class Listener(ConsumerRebalanceListener):  # type: ignore[misc]
        async def on_partitions_revoked(self, revoked: Collection[TopicPartition]) -> None:
            await on_revoked(revoked)

        async def on_partitions_assigned(self, assigned: Collection[TopicPartition]) -> None:
            await on_assigned(assigned)

    return Listener()


def retrieve_failure(task: asyncio.Future[Any]) -> None:
    """Consume a detached operation's exception without losing its lifetime tracking."""
    if not task.cancelled():
        task.exception()


@dataclass(frozen=True)
class Poison:
    """A fetched record no handler may see, and why: it is dead-lettered, never retried."""

    cause: BaseException


class RecordDecoder:
    """Turns a fetched record into a message: the middleware rewrites it, the kernel routes it by its
    ``content-type`` header, and the chosen serializer decodes it. A record that fails the same way on
    every redelivery comes back as :class:`Poison`."""

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

    def __call__(self, fetched: KafkaFetched) -> EventMessage | CommandMessage | Poison:
        """The message ``fetched`` carries, or why it is poison."""
        rewritten = self._rewrite(fetched)
        return rewritten if isinstance(rewritten, Poison) else self._decode(*rewritten)

    def _rewrite(self, fetched: KafkaFetched) -> tuple[bytes, list[KafkaHeader]] | Poison:
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
                return Poison(e)
            raise
        return record.value, record.headers

    def _decode(self, value: bytes, headers: list[KafkaHeader]) -> EventMessage | CommandMessage | Poison:
        # The default's kind is checked at build, so the kernel's `codec:` refusal cannot fire.
        route = _native.kafka_route(headers, self._media_types, self._default.kind)
        if route.registered is not None:
            serializer = self._serializers[route.registered]
        elif route.unknown is not None:
            # Never fall back to the default: decoding msgpack bytes as the host's own format is how a
            # wrong message reaches a handler instead of the dead-letter sink.
            return Poison(CodecError(f"no serializer for content type {route.unknown!r}"))
        elif route.headerless:
            return Poison(
                CodecError("the record has no readable content-type header, and a command always names one")
            )
        else:
            serializer = self._default
        try:
            return serializer.decode(value)
        except Exception as e:  # noqa: BLE001 - every failure of a pure decode is terminal
            # A third-party serializer raises its own error types, and a decode that fails once fails
            # the same way on every redelivery, so the record is poison whatever it raised.
            return Poison(e)


class Booked:
    """One fetched record's booked offset and the handles that settle it. They run long after the poll
    returned, so they hold the record's coordinates and token, never the record."""

    def __init__(
        self,
        tracker: ThreadBound[_native.OffsetTracker],
        topic: str,
        partition: int,
        offset: int,
        rewind: Callable[[TopicPartition, int], None],
    ) -> None:
        self._tracker = tracker
        self._topic, self._partition, self._offset = topic, partition, offset
        self._token = self._tracker.get().book(topic, partition, offset)
        self._rewind = rewind

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
        self._rewind(topic_partition(self._topic, self._partition), target)

    def poison(self, fetched: KafkaFetched, cause: BaseException) -> Undecodable:
        """The fetched bytes as a dead-letterable record, settled by the offset already booked."""
        # Topic, partition and offset together are the only identity a poison message has. The offset
        # alone repeats on every partition, so two poison messages would share one id and a
        # dead-letter queue keyed on it would keep the first and reject the second.
        poison_id = _native.kafka_poison_id(self._topic, self._partition, self._offset)
        error = CodecError(f"kafka {poison_id} did not decode: {cause}")
        error.__cause__ = cause  # `raise ... from e` is unavailable: the error is carried, not raised
        return Undecodable(poison_id, fetched.value, error, self.ack, self.nack, origin=fetched)

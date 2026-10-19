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
from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING, Any, Final, cast

from .. import _native, errors
from ..broker import KeyFn, consume
from ..delivery import DeadLetter, Delivery, Undecodable
from ..dispatcher import KeyOrderedDispatcher
from ..errors import BadHandle, CodecError
from ..message import CommandMessage, EventMessage, MessageKind, message_kind
from ..serializer import KernelSerializer, Serializer
from ..tenancy import Tenancy

if TYPE_CHECKING:  # the base package imports no broker library; these names are for the annotations only
    from aiokafka import ConsumerRebalanceListener, TopicPartition  # type: ignore[import-untyped]

from . import _kafka
from ._kafka import (
    ConsumerClient,
    FetchedRecord,
    KafkaFetched,
    ProducerClient,
    RecordMiddleware,
)

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
        _kafka.check_durations(publish_timeout_s=publish_timeout_s)
        _kafka.check_producer(client)
        self._client = client
        self._topic = topic
        self._publish_timeout_s = publish_timeout_s

    async def __call__(self, dead: DeadLetter[Any]) -> None:
        fetched = dead.origin
        if not isinstance(fetched, KafkaFetched):
            raise BadHandle(f"dead letter of {dead.message!r} carries no Kafka record to republish")
        record = fetched.dead_letter(dead.failure)
        try:
            await _kafka.send(self._client, self._topic, record, self._publish_timeout_s)
        except CodecError:
            logger.warning(
                "edax kafka: %s refused the dead letter of %s/%d/%d; sending it without its value",
                self._topic,
                fetched.topic,
                fetched.partition,
                fetched.offset,
                exc_info=True,
            )
            await _kafka.send(
                self._client, self._topic, _native.kafka_dead_letter_stub(record), self._publish_timeout_s
            )


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
        _kafka.check_durations(publish_timeout_s=publish_timeout_s)
        _kafka.check_producer(client)
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
        await _kafka.send(self._client, self._topic, record, self._publish_timeout_s)


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
        commit_timeout_s: float = _kafka.COMMIT_TIMEOUT_S,
    ) -> None:
        _kafka.check_durations(commit_interval_s=commit_interval_s, commit_timeout_s=commit_timeout_s)
        _kafka.check_consumer(client)
        self._client = client
        self._decoder = _kafka.RecordDecoder(serializer, serializers, middleware)
        self._offsets = _kafka.OffsetCommitter(client, commit_timeout_s)
        self._commit_interval_s = commit_interval_s
        # The dispatcher `run` feeds, so a rebalance drain can wait for its running handlers too.
        self._dispatcher: KeyOrderedDispatcher[Any] | None = None
        self._poll_serial = asyncio.Lock()
        self._fetch_task: asyncio.Task[tuple[FetchedRecord, _kafka.Booked | None]] | None = None
        self._revision = 0
        self._changed: dict[TopicPartition, int] = {}
        self._revoked: set[TopicPartition] = set()
        self._awaiting_assignment = False
        self._rewinds: dict[TopicPartition, int] = {}
        self._stopped: BadHandle | None = None
        self._poll_stopped = False
        self._fetch_ready = asyncio.Event()
        self._fetch_ready.set()

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
        self._invalidate(tp)
        self._revoked.discard(tp)
        self._rewinds.pop(tp, None)

    def _invalidate(self, tp: TopicPartition) -> None:
        self._revision += 1
        self._changed[tp] = self._revision

    def _rewind(self, tp: TopicPartition, target: int) -> None:
        self._invalidate(tp)
        # Keep the target until the old fetch ends: it may advance the client after this seek.
        self._rewinds[tp] = target
        self._client.seek(tp, target)
        if self._fetch_task is None:
            self._rewinds.pop(tp, None)
        elif not self._fetch_task.done():
            self._fetch_task.cancel()

    async def flush(self) -> None:
        """Commit every partition whose watermark moved, in one call, under the commit deadline. Raises
        what the commit raised, or the transient :class:`~edax.errors.StoreError` at the deadline; nothing
        is recorded until the client replies. A deadline or caller cancellation leaves the client call
        running, and new polls and commits raise the transient ``StoreError`` until it finishes. Call
        this after ``dispatcher.stop()`` to commit the drained work before closing the client."""
        self._check_running()
        await self._offsets.flush()

    def _check_running(self) -> None:
        if self._stopped is not None:
            raise self._stopped
        self._offsets.check_idle()

    async def _flush_quietly(self) -> Exception | None:
        """Flush, and log a failure rather than raise it: the commit is retried at the next interval, and a
        lost commit only means redelivery after a restart. Returns the failure, ``None`` once it landed."""
        try:
            await self.flush()
        except Exception as e:  # noqa: BLE001 - _commit_failed logs and observes it
            self._commit_failed(e)
            return e
        return None

    def _commit_failed(self, error: BaseException) -> None:
        logger.warning("edax kafka: commit failed; the watermarks stay due", exc_info=error)
        # Absent for a flush called outside `run`, whose failure the log line carries.
        if self._dispatcher is not None:
            self._dispatcher.observer.commit_failed(error)

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
        _kafka.check_listener(self._client)
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

    def rebalance_listener(
        self, drain_timeout_s: float = _kafka.DRAIN_TIMEOUT_S
    ) -> ConsumerRebalanceListener:
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
        _kafka.check_durations(drain_timeout_s=drain_timeout_s)
        from aiokafka import ConsumerRebalanceListener

        consumer = self

        class _Listener(ConsumerRebalanceListener):  # type: ignore[misc]  # aiokafka ships no stubs
            async def on_partitions_revoked(self, revoked: Collection[TopicPartition]) -> None:
                await consumer._revoke(revoked, drain_timeout_s)

            async def on_partitions_assigned(self, assigned: Collection[TopicPartition]) -> None:
                consumer._check_running()
                consumer._fetch_ready.clear()
                try:
                    fetch = consumer._fetch_task
                    for tp in assigned:
                        consumer._invalidate(tp)
                    if fetch is not None and not consumer._awaiting_assignment:
                        # Reading client position after cancelling a fetch can already be too late.
                        consumer._offsets.retire(assigned)
                        fetch.cancel()
                        consumer._stopped = BadHandle(
                            "kafka assignment overlapped a fetch without revoke; restart client"
                        )
                        raise consumer._stopped
                    await consumer._finish_fetch(drain_timeout_s)
                    for tp in assigned:
                        consumer.reset(tp)
                except (Exception, asyncio.CancelledError):
                    if consumer._stopped is None:
                        consumer._stopped = BadHandle("kafka assignment interrupted; stop this consumer")
                    raise
                finally:
                    consumer._awaiting_assignment = False
                    consumer._fetch_ready.set()

        return _Listener()

    async def _revoke(self, revoked: Collection[TopicPartition], timeout: float) -> None:
        # The kernel hands back each revoked partition's last watermark and retires everything of it at
        # once: the queued deliveries are skipped rather than drained, so a deep queue cannot outlast the
        # rebalance. The new owner fetches them again from the commit, made while the partitions are
        # still ours. A failed commit only means more redelivery there.
        deadline = asyncio.get_running_loop().time() + timeout
        self._awaiting_assignment = False
        self._fetch_ready.clear()
        for tp in revoked:
            self._invalidate(tp)
            self._revoked.add(tp)
            self._rewinds.pop(tp, None)
        fetch = self._fetch_task
        if fetch is not None and not fetch.done():
            fetch.cancel()
        try:
            drained = await self._offsets.drain_revoked(revoked, () if fetch is None else (fetch,), deadline)
            if drained.commit_failure is not None:
                self._commit_failed(drained.commit_failure)
            if drained.unfinished:
                self._stopped = BadHandle(
                    "kafka revoke deadline expired with unfinished operations; stop this consumer"
                )
            # A handler already running finishes, and its ack settles nothing. Wait for it, so the new
            # owner rarely runs a key this member is still running.
            dispatcher = self._dispatcher
            if dispatcher is not None:
                running = await dispatcher.wait_handlers(max(0, deadline - asyncio.get_running_loop().time()))
                if running:
                    logger.warning(
                        "edax kafka: %d handler(s) still running %ss after a revoke; "
                        "the new owner may overlap",
                        running,
                        timeout,
                    )
                    dispatcher.observer.revoke_overrun(running)
            # The next assignment may gain partitions absent from this revoke, including an empty revoke.
            self._awaiting_assignment = self._stopped is None
        except asyncio.CancelledError:
            self._stopped = BadHandle("kafka revoke cancelled with unfinished operations; stop this consumer")
            raise
        finally:
            # An idle aiokafka member needs another fetch before it can rejoin. Interrupted callbacks
            # also wake pollers, which then observe _stopped instead of waiting forever.
            self._fetch_ready.set()

    async def _finish_fetch(self, timeout: float) -> None:
        task = self._fetch_task
        if task is None or task.done():
            return
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if not done:
            self._stopped = BadHandle("kafka assignment has an unfinished fetch; stop this consumer")
            self._fetch_ready.set()
            raise self._stopped

    async def poll(self) -> Delivery[EventMessage | CommandMessage] | Undecodable | None:
        async with self._poll_serial:
            return await self._poll()

    async def _read(self) -> tuple[FetchedRecord, _kafka.Booked | None]:
        revision = self._revision
        msg = await self._client.getone()
        tp = _kafka.topic_partition(msg.topic, msg.partition)
        if tp in self._revoked or self._changed.get(tp, 0) > revision:
            return msg, None
        # Booking is in the fetch task, before its result yields to the polling task.
        return msg, _kafka.Booked(self._offsets.tracker, msg.topic, msg.partition, msg.offset, self._rewind)

    async def _poll(self) -> Delivery[EventMessage | CommandMessage] | Undecodable | None:
        self._check_running()
        if self._poll_stopped:
            raise BadHandle("kafka poll cancelled; create a new consumer before restarting consumption")
        try:
            while True:
                await self._fetch_ready.wait()
                self._check_running()
                for tp, target in tuple(self._rewinds.items()):
                    self._client.seek(tp, target)
                    del self._rewinds[tp]
                task = asyncio.create_task(self._read())
                self._fetch_task = task
                task.add_done_callback(_kafka.retrieve_failure)
                try:
                    with _kafka.classified():
                        await asyncio.wait({task})
                        if task.cancelled():
                            continue
                        msg, booked = task.result()
                except asyncio.CancelledError:
                    self._poll_stopped = True
                    task.cancel()
                    try:
                        await asyncio.sleep(0)
                    finally:
                        # Repeated cancellation must not reopen admission before recovery completes.
                        if task.done():
                            if not task.cancelled():
                                _, abandoned = task.result()
                                if abandoned is not None:
                                    abandoned.nack()
                            self._poll_stopped = False
                    raise
                finally:
                    if task.done():
                        self._fetch_task = None
                try:
                    self._check_running()
                except Exception:
                    # A concurrent commit can lose its waiter while this fetch finishes. The
                    # application never received this booking, so rewind it before refusing the poll.
                    if booked is not None:
                        booked.nack()
                    raise
                if booked is not None and booked.live():
                    break
                tp = _kafka.topic_partition(msg.topic, msg.partition)
                if tp in self._rewinds or tp in self._revoked:
                    continue
                # A synchronous external reset gives no authoritative assignment start to seek to.
                self._stopped = BadHandle(
                    "kafka fetch crossed an assignment without a safe rewind; restart this consumer"
                )
                raise self._stopped
        except Exception as e:
            if _kafka.consumer_stopped(e):
                return None
            raise
        # Book the offset before decoding: an undecodable payload still has to be settled, and an unbooked
        # offset is either committed past by the next ack or replayed into the same failure forever. Booked
        # before the next await, so a nack's rewind retires it if it lands later. A negative offset is the
        # kernel's terminal `codec:` refusal.
        try:
            # A null key, value or header value crosses as empty bytes: the kernel record's rule, which also
            # routes a null content-type like an empty one.
            fetched = KafkaFetched.of(msg)
            decoded = self._decoder(fetched)
            if isinstance(decoded, _kafka.Poison):
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

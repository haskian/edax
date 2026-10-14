"""Bounded Kafka microbatches with atomic append/upsert sinks.

The client is caller-owned. Subscribe with this runner's listener and disable auto commit.
A stopped runner must not be restarted: stop its client and wait for ``wait_closed`` first.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import traceback
from collections import deque
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast

from .. import _json, _native, errors
from ..delivery import DeadLetter
from ..errors import BadHandle, CodecError
from ..ingestion import BatchCommitError, BatchInput, BatchSettings, BatchSink, MappedRow, RejectionPublisher
from ..message import EventMessage
from ..pacing import Pacing
from ..serializer import KernelSerializer, Serializer
from ._kafka import (
    COMMIT_TIMEOUT_S,
    DRAIN_TIMEOUT_S,
    Booked,
    FetchedRecord,
    KafkaFetched,
    OffsetCommitter,
    Poison,
    RecordDecoder,
    RecordMiddleware,
    check_consumer,
    check_durations,
    check_listener,
    retrieve_failure,
)

if TYPE_CHECKING:
    from aiokafka import ConsumerRebalanceListener, TopicPartition  # type: ignore[import-untyped]

T = TypeVar("T")
logger = logging.getLogger("edax.backend.kafka_ingest")
_SERIALIZED_COPIES = 3


class BatchConsumerClient(Protocol):
    """Separate batch/flow-control surface; the single-record consumer is unchanged."""

    async def getmany(
        self, *partitions: TopicPartition, timeout_ms: int = 0, max_records: int | None = None
    ) -> Mapping[TopicPartition, Sequence[FetchedRecord]]: ...
    def assignment(self) -> Collection[TopicPartition]: ...
    async def position(self, partition: TopicPartition) -> int: ...
    def pause(self, *partitions: TopicPartition) -> None: ...
    def resume(self, *partitions: TopicPartition) -> None: ...
    def seek(self, partition: TopicPartition, offset: int, /) -> Any: ...
    async def commit(self, offsets: dict[TopicPartition, int], /) -> Any: ...


@dataclass
class _Partition:
    generation: int
    frontier: int
    items: list[BatchInput] = field(default_factory=list)
    bookings: list[Booked] = field(default_factory=list)
    bytes: int = 0
    serialized_bytes: int = 0
    opened: float = 0
    task: asyncio.Task[None] | None = None
    backing_off: bool = False
    ready_at: float = 0
    # Counts survive eviction; one bounded entry per original in this partition's recovery chain.
    attempts: dict[str, int] = field(default_factory=dict)
    # Retry budgets belong to a batch's source boundaries, including across eviction.
    retry_batches: dict[str, tuple[str, str]] = field(default_factory=dict)


class KafkaIngestion:
    """Microbatch complete events into a :class:`~edax.ingestion.BatchSink`.

    ``mapper`` is synchronous and returns a complete ``MappedRow`` (``None`` value deletes).
    Append mode uses the event ID as its key, independently of the mapper's row key.
    ``on_dead`` receives the original Kafka record after its rejection is committed. It must
    return only after durable publication. The outbox may also be drained independently.

    ``retained_bytes`` accounts for serialized originals, prepared inputs and transaction copies.
    Broker prefetch, Python object overhead and arbitrary callback allocations are excluded.
    """

    def __init__(
        self,
        client: BatchConsumerClient,
        sink: BatchSink,
        mapper: Callable[[EventMessage], MappedRow],
        on_dead: Callable[[DeadLetter[Any]], Awaitable[None]],
        *,
        settings: BatchSettings | None = None,
        serializer: Serializer | None = None,
        serializers: Sequence[Serializer] = (),
        middleware: Sequence[RecordMiddleware] = (),
        pacing: Pacing | None = None,
        operation_timeout_s: float = 30,
        commit_timeout_s: float = COMMIT_TIMEOUT_S,
        drain_timeout_s: float = DRAIN_TIMEOUT_S,
    ) -> None:
        self.client, self.sink, self.mapper = client, sink, mapper
        self.settings = settings or BatchSettings()
        self._config = self.settings.json
        self._pacing = pacing or Pacing()
        check_durations(
            publish_timeout_s=operation_timeout_s,
            commit_timeout_s=commit_timeout_s,
            drain_timeout_s=drain_timeout_s,
        )
        # These checks only inspect the client's configuration, not its fetch interface.
        check_consumer(cast(Any, client))
        self._offsets = OffsetCommitter(cast(Any, client), commit_timeout_s)
        self._decoder = RecordDecoder(serializer, serializers, middleware)
        self._canonical = KernelSerializer()
        self._timeout, self._drain_timeout = operation_timeout_s, drain_timeout_s
        self._on_dead = on_dead
        self.publisher = RejectionPublisher(sink, self._publish)
        self._parts: dict[TopicPartition, _Partition] = {}
        self._rotation: deque[TopicPartition] = deque()
        self._generation = 0
        self._epochs: dict[TopicPartition, int] = {}
        self._records = self._bytes = 0
        self._fetch_reserved = 0
        self.peak_records = self.peak_bytes = 0
        # Reserve callback/encoding expansion BEFORE invoking either. Six copies cover the raw
        # JSON escaping, canonical content, mapped value, ABI JSON and driver parameter encoding.
        self._reserve = _native.batch_reserve(self._config)
        self._rejection: str = _json.decode(_native.batch_recover("invalid", 1, 1, 1))["rejection"]
        self._slots = asyncio.Semaphore(self.settings.concurrency)
        self._operations: set[asyncio.Task[Any]] = set()
        self._workers: set[asyncio.Task[None]] = set()
        self._fetch: asyncio.Task[Any] | None = None
        self._fetch_partition: TopicPartition | None = None
        self._running = False
        self._started = False
        self._failure: BaseException | None = None
        self._changed = asyncio.Event()
        self._assignment = asyncio.Lock()

    @property
    def retained_records(self) -> int:
        return self._records

    @property
    def retained_bytes(self) -> int:
        return self._bytes + self._fetch_reserved

    def rebalance_listener(self) -> ConsumerRebalanceListener:
        """Install this listener before starting the client's subscription."""
        from aiokafka import ConsumerRebalanceListener

        runner = self

        class Listener(ConsumerRebalanceListener):  # type: ignore[misc]
            async def on_partitions_revoked(self, revoked: Collection[TopicPartition]) -> None:
                await runner._revoke(revoked)

            async def on_partitions_assigned(self, assigned: Collection[TopicPartition]) -> None:
                await runner._assign(assigned)

        return Listener()

    async def _assign(self, assigned: Collection[TopicPartition]) -> None:
        async with self._assignment:
            self._check()
            for tp in sorted(assigned, key=lambda p: (p.topic, p.partition)):
                if tp in self._parts:
                    continue
                # A retained partition's fetch cannot mutate this new partition's position.
                if self._fetch_partition == tp and self._fetch is not None and not self._fetch.done():
                    self._fail(BadHandle("assignment overlapped an unresolved fetch; stop the client"))
                    self._check()
                epoch = self._epochs.get(tp, 0)
                position = await self._bounded(self.client.position(tp))
                if self._epochs.get(tp, 0) != epoch:
                    continue
                self._generation += 1
                self._offsets.reset(tp)
                self._parts[tp] = _Partition(self._generation, position)
                self._rotation.append(tp)
            self._changed.set()

    async def _revoke(self, revoked: Collection[TopicPartition]) -> None:
        deadline = asyncio.get_running_loop().time() + self._drain_timeout
        # Retire synchronously, before commit serialization or any task drain.
        last = self._offsets.retire(revoked)
        workers: set[asyncio.Task[Any]] = set()
        for tp in revoked:
            self._epochs[tp] = self._epochs.get(tp, 0) + 1
            part = self._parts.pop(tp, None)
            if part is not None:
                part.generation = -1
                if part.task is not None:
                    workers.add(part.task)
                    part.task.cancel()
                else:
                    self._release(part)
        self._rotation = deque(tp for tp in self._rotation if tp in self._parts)
        if self._fetch_partition in revoked and self._fetch is not None and not self._fetch.done():
            self._fetch.cancel()
            workers.add(self._fetch)
        commit = asyncio.create_task(self._offsets.commit_retired(last))
        self._track(commit)
        workers.add(commit)
        try:
            done, pending = await asyncio.wait(
                workers, timeout=max(0, deadline - asyncio.get_running_loop().time())
            )
            for task in done:
                if not task.cancelled() and task.exception() is not None:
                    error = cast(BaseException, task.exception())
                    if task is not commit or errors.is_fatal(error):
                        self._fail(error)
                    else:
                        logger.warning(
                            "kafka revoke commit failed; uncommitted records will replay", exc_info=error
                        )
            if pending:
                commit.cancel()
                self._fail(BadHandle("revoke deadline expired; stop the client and await wait_closed"))
        except asyncio.CancelledError:
            commit.cancel()
            self._fail(BadHandle("revoke cancelled with unresolved operations"))
            raise
        finally:
            self._changed.set()

    def _track(self, task: asyncio.Task[Any]) -> None:
        self._operations.add(task)
        task.add_done_callback(self._operations.discard)
        task.add_done_callback(retrieve_failure)

    def _fail(self, error: BaseException) -> None:
        if self._failure is None:
            self._failure = error
        self._changed.set()

    def _check(self) -> None:
        if self._failure is not None:
            raise self._failure
        self._offsets.check_idle()

    async def _bounded(self, call: Awaitable[T]) -> T:
        task = asyncio.ensure_future(call)
        self._track(cast(asyncio.Task[Any], task))
        try:
            done, _ = await asyncio.wait({task}, timeout=self._timeout)
            if not done:
                self._fail(BadHandle("ingestion operation deadline expired; stop and await wait_closed"))
                self._check()
            return task.result()
        finally:
            if not task.done():
                task.cancel()

    async def _operation(self, call: Awaitable[T]) -> T:
        task = asyncio.ensure_future(call)
        self._track(cast(asyncio.Task[Any], task))
        try:
            done, _ = await asyncio.wait({task}, timeout=self._timeout)
            if not done:
                self._fail(BadHandle("ingestion operation deadline expired; stop and await wait_closed"))
                task.cancel()
                self._check()
            return task.result()
        finally:
            if not task.done():
                task.cancel()
                # The worker retains its slot and every data credit until the operation ends.
                # Shield repeated cancellation too: cancellation never proves rollback/release.
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        continue
                    except Exception:  # noqa: BLE001 - failure is retrieved by the owning task
                        break

    def _rewind(self, tp: TopicPartition, offset: int) -> None:
        part = self._parts.get(tp)
        if part is None:
            return
        self._generation += 1
        part.generation = self._generation
        # Kernel nack chooses the earliest unsettled original, not the retained suffix.
        self.client.seek(tp, offset)
        part.frontier = offset

    def _release(self, part: _Partition) -> None:
        self._records -= len(part.items)
        self._bytes -= part.bytes
        part.items.clear()
        part.bookings.clear()
        part.bytes = part.serialized_bytes = 0
        part.opened = 0
        self._changed.set()

    def _room(self) -> bool:
        return bool(_native.batch_admit(self._records, self.retained_bytes, 1, self._reserve, self._config))

    def _evict_retry(self) -> bool:
        for part in self._parts.values():
            if part.backing_off and part.task is not None:
                part.task.cancel()
                return True
        return False

    async def _fetch_one(self, tp: TopicPartition, part: _Partition) -> FetchedRecord | None:
        generation = part.generation
        self._fetch_reserved = self._reserve
        self.peak_bytes = max(self.peak_bytes, self.retained_bytes)
        task = asyncio.create_task(self.client.getmany(tp, timeout_ms=0, max_records=1))
        self._fetch = task
        self._fetch_partition = tp
        self._track(task)
        task.add_done_callback(self._fetched)
        try:
            done, _ = await asyncio.wait({task}, timeout=self._timeout)
            if not done:
                self._fail(BadHandle("fetch deadline expired; stop and await wait_closed"))
                task.cancel()
                self._check()
            if task.cancelled():
                if self._parts.get(tp) is not part or part.generation != generation:
                    return None
                raise BadHandle("batch fetch cancelled outside a rebalance")
            result = task.result()
        finally:
            if task.done():
                self._fetch = None
                self._fetch_partition = None
        # No await between the generation check and booking in _admit.
        if self._parts.get(tp) is not part or part.generation != generation:
            if self._parts.get(tp) is part:
                self.client.seek(tp, part.frontier)
            return None
        records = result.get(tp, ())
        if any(other != tp and values for other, values in result.items()) or len(records) > 1:
            raise BadHandle("batch client violated requested partition or max_records")
        return records[0] if records else None

    def _fetched(self, task: asyncio.Task[Any]) -> None:
        # A cancelled fetch retains its reservation until the client actually releases the data.
        self._fetch_reserved = 0
        if not self._running and self._fetch is task:
            self._fetch = None
            self._fetch_partition = None
        self._changed.set()

    def _raw(self, record: FetchedRecord) -> tuple[str, KafkaFetched, bool]:
        fetched = KafkaFetched.of(record)
        source = _json.encode(
            {
                "topic": fetched.topic,
                "partition": fetched.partition,
                "offset": fetched.offset,
                "timestamp": fetched.timestamp,
                "timestamp_type": fetched.timestamp_type,
            }
        )
        raw = _native.kafka_ingestion_raw(fetched.record(), source, self.settings.max_record_bytes)
        oversized = bool(_json.decode(raw)["oversized"])
        if oversized:
            fetched = replace(fetched, key=b"", value=b"", headers=())
        return raw, fetched, oversized

    def _prepare(self, record: FetchedRecord) -> BatchInput:
        raw, fetched, oversized = self._raw(record)
        target = self.sink.target
        fallback = _native.kafka_poison_id(record.topic, record.partition, record.offset)
        tenant: str | None = None
        stream = event_id = row_key = fallback
        sequence: int | None = None
        content: Any = None
        value: Any = None
        failure: str | None = None
        try:
            if oversized:
                raise CodecError("record exceeds max_record_bytes; original retained at source coordinates")
            decoded = self._decoder(fetched)
            if isinstance(decoded, Poison):
                raise CodecError(str(decoded.cause)) from decoded.cause
            if not isinstance(decoded, EventMessage):
                raise CodecError("ingestion requires an event message")
            tenant, stream, event_id, sequence = (
                decoded.tenant,
                decoded.aggregate_id,
                decoded.id,
                decoded.sequence,
            )
            canonical = self._canonical.encode(decoded)
            if len(canonical) > self.settings.max_mapped_bytes:
                raise CodecError("decoded event exceeds max_mapped_bytes")
            # The native envelope preserves exact i64 nanoseconds and sequences.
            content = canonical.decode()
            mapped = self.mapper(decoded)
            if inspect.isawaitable(mapped):
                raise BadHandle("ingestion mapper must be synchronous")
            if not isinstance(mapped, MappedRow):
                raise BadHandle("ingestion mapper must return MappedRow")
            mapped_json = _json.encode(mapped)
            if len(canonical) + len(mapped_json.encode()) > self.settings.max_mapped_bytes:
                raise CodecError("mapped event exceeds max_mapped_bytes")
            # Snapshot host values so a mapper cannot mutate already admitted originals.
            snapshot = _json.decode(mapped_json)
            row_key, value = snapshot["key"], snapshot["value"]
        except Exception as error:
            if errors.failure_kind(error) != "terminal":
                raise
            failure = f"{errors.code_of(error) or 'codec'}: {error}"
            content, value = None, None
        return target.input(
            record.topic,
            record.partition,
            record.offset,
            tenant,
            event_id if target.mode == "append" else row_key,
            stream,
            event_id,
            sequence,
            content,
            value,
            raw,
            failure,
            _json.decode(raw)["sha256"],
        )

    def _admit(self, tp: TopicPartition, part: _Partition, record: FetchedRecord) -> bool:
        booked = Booked(
            cast(Any, self.client),
            self._offsets.tracker,
            record.topic,
            record.partition,
            record.offset,
            self._rewind,
        )
        try:
            item = self._prepare(record)
            serialized_bytes = len(_json.encode(item).encode())
            # A later isolated rejection replaces the failure field. Reserve its bounded
            # kernel diagnostic now, before any partition can spend the remaining capacity.
            rejection_bytes = len(_json.encode(replace(item, failure=self._rejection)).encode())
            charge = _SERIALIZED_COPIES * max(serialized_bytes, rejection_bytes)
            if charge > self._reserve:
                raise BadHandle("prepared record exceeds reserved serialized memory")
            if not _native.batch_admit(self._records, self._bytes, 1, charge, self._config):
                raise BadHandle("ingestion admission invariant violated")
            if not part.items:
                part.opened = asyncio.get_running_loop().time()
            part.items.append(item)
            part.bookings.append(booked)
            part.bytes += charge
            part.serialized_bytes += serialized_bytes
            self._records += 1
            self._bytes += charge
            self.peak_records, self.peak_bytes = (
                max(self.peak_records, self._records),
                max(self.peak_bytes, self._bytes),
            )
            return True
        except BaseException as error:
            booked.nack()
            # Nack retires every booking in the open batch. Release its prepared copies and
            # refetch the entire uncommitted prefix; retry counts survive that rewind.
            self._release(part)
            if not isinstance(error, Exception):
                raise
            source = _native.ingest_source_key(
                self.sink.target.generation,
                self.sink.target.source_incarnation,
                record.topic,
                record.partition,
                record.offset,
            )
            attempt = part.attempts.get(source, 0) + 1
            kind = "fatal" if errors.failure_kind(error) == "fatal" else "retry"
            decision = _json.decode(_native.batch_recover(kind, attempt, 1, self.settings.max_attempts))
            if decision["action"] != "retry":
                raise
            part.attempts[source] = attempt
            part.retry_batches[source] = (source, source)
            delay = self._pacing.delay(decision["step"])
            part.ready_at = asyncio.get_running_loop().time() + delay
            return False

    def _launch(self, tp: TopicPartition, part: _Partition) -> None:
        if not part.items or part.task is not None:
            return
        self.client.pause(tp)
        task = asyncio.create_task(self._work(tp, part))
        part.task = task
        self._workers.add(task)
        task.add_done_callback(lambda finished: self._finished(tp, part, finished))

    def _finished(self, tp: TopicPartition, part: _Partition, task: asyncio.Task[None]) -> None:
        # Done callbacks also run when a task was cancelled before its first instruction.
        self._workers.discard(task)
        retrieve_failure(task)
        self._release(part)
        part.task = None
        part.backing_off = False
        if self._parts.get(tp) is part and self._failure is None:
            self.client.resume(tp)

    @staticmethod
    def _nack(part: _Partition) -> None:
        # A successfully isolated left child may already have settled the first booking.
        for booked in part.bookings:
            if booked.live():
                booked.nack()
                break

    async def _work(self, tp: TopicPartition, part: _Partition) -> None:
        try:
            await self._execute(tp, part, tuple(part.items), tuple(part.bookings))
        except asyncio.CancelledError:
            self._nack(part)
            raise
        except Exception as error:  # noqa: BLE001 - stop admission and preserve the original failure
            self._fail(error)
            self._nack(part)

    async def _execute(
        self,
        tp: TopicPartition,
        part: _Partition,
        inputs: tuple[BatchInput, ...],
        bookings: tuple[Booked, ...],
    ) -> None:
        while True:
            self._check()
            if not all(booked.live() for booked in bookings):
                return
            try:
                async with self._slots:
                    result = await self._operation(self.sink.commit(inputs))
                    if len(result.outcomes) != len(inputs):
                        raise BadHandle("sink returned an invalid outcome count")
                    for item, outcome in zip(inputs, result.outcomes, strict=True):
                        if outcome == "rejected":
                            await self._operation(
                                self.publisher.publish(item.source, result.rejections[item.source])
                            )
                        elif outcome != "accepted":
                            raise BadHandle("sink returned an unknown ingestion outcome")
                    # Do not partially acknowledge a batch whose publication can still fail.
                    for item, booked in zip(inputs, bookings, strict=True):
                        booked.ack()
                        part.attempts.pop(item.source, None)
                        part.retry_batches.pop(item.source, None)
                # This frontier covers actual originals, including legitimate source offset gaps.
                if inputs and self._parts.get(tp) is part:
                    due = self._offsets.tracker.get().due()
                    for row in due:
                        if row.topic == tp.topic and row.partition == tp.partition:
                            part.frontier = max(part.frontier, row.offset)
                await self._flush()
                return
            except Exception as error:
                kind = (
                    error.kind
                    if isinstance(error, BatchCommitError)
                    else ("fatal" if errors.failure_kind(error) == "fatal" else "retry")
                )
                attempt = 1 + max((part.attempts.get(item.source, 0) for item in inputs), default=0)
                decision = _json.decode(
                    _native.batch_recover_with_rejections(
                        kind,
                        attempt,
                        len(inputs),
                        self.settings.max_attempts,
                        all(item.failure is not None for item in inputs),
                    )
                )
                action = decision["action"]
                if action == "stop":
                    raise
                if action == "split":
                    middle = decision["split"]
                    batch = (inputs[0].source, inputs[-1].source)
                    for item in inputs:
                        if part.retry_batches.get(item.source) == batch:
                            part.attempts.pop(item.source, None)
                            part.retry_batches.pop(item.source, None)
                    await self._execute(tp, part, inputs[:middle], bookings[:middle])
                    await self._execute(tp, part, inputs[middle:], bookings[middle:])
                    return
                if action == "reject":
                    # The singleton child already executed and rolled back before this verdict.
                    inputs = tuple(replace(item, failure=decision["rejection"]) for item in inputs)
                    continue
                if action != "retry":
                    raise BadHandle("unknown batch recovery action") from error
                for item in inputs:
                    part.attempts[item.source] = attempt
                    # A refetched parent retry must not replace an existing child's scope.
                    part.retry_batches.setdefault(item.source, (inputs[0].source, inputs[-1].source))
                delay = self._pacing.delay(decision["step"])
                part.ready_at = asyncio.get_running_loop().time() + delay
                part.backing_off = True
                try:
                    await self._pacing.sleep(delay)
                    part.ready_at = 0
                finally:
                    part.backing_off = False

    async def _flush(self) -> None:
        for attempt in range(1, self.settings.max_attempts + 1):
            try:
                await self._offsets.flush()
                return
            except Exception as error:
                kind = "fatal" if errors.failure_kind(error) == "fatal" else "retry"
                decision = _json.decode(_native.batch_recover(kind, attempt, 1, self.settings.max_attempts))
                if decision["action"] != "retry":
                    self._fail(error)
                    raise
                await self._pacing.wait(decision["step"])

    async def _publish(self, source: str, row: dict[str, Any]) -> None:
        raw = _json.decode(row["raw"])
        record = _native.kafka_ingestion_rejection_record(row["raw"], source)
        fetched = KafkaFetched(
            raw["topic"],
            raw["partition"],
            raw["offset"],
            record.key,
            record.value,
            tuple((h.key, h.value) for h in record.headers),
            raw["timestamp"],
            raw["timestamp_type"],
        )
        await self._on_dead(DeadLetter(source, 1, errors.from_wire(row["error"]), fetched))

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """Run until stopped, cancelled, or a fatal/exhausted failure. Never restart this object."""
        if self._started:
            raise BadHandle("KafkaIngestion is single-use")
        self._started = self._running = True
        check_listener(cast(Any, self.client))
        stop = stop or asyncio.Event()
        stop_waiter = asyncio.create_task(stop.wait())
        stop_waiter.add_done_callback(lambda _: self._changed.set())
        try:
            await self._assign(self.client.assignment())
            while not stop.is_set():
                self._check()
                progressed = False
                for tp in tuple(self._rotation):
                    part = self._parts.get(tp)
                    if part is None:
                        continue
                    self._rotation.rotate(-1)
                    if part.task is not None or part.ready_at > asyncio.get_running_loop().time():
                        continue
                    age = int((asyncio.get_running_loop().time() - part.opened) * 1000) if part.items else 0
                    if part.items and (
                        _native.batch_cut(len(part.items), part.serialized_bytes, age, self._config)
                        or not self._room()
                    ):
                        self._launch(tp, part)
                        continue
                    if not self._room():
                        self._evict_retry()
                        continue
                    record = await self._fetch_one(tp, part)
                    try:
                        if record is not None:
                            progressed = self._admit(tp, part, record) or progressed
                    finally:
                        # Oversized originals become bounded stubs. Do not retain their payloads
                        # in this suspended frame while a sink or another partition waits.
                        record = None
                if not progressed and not stop.is_set():
                    self._changed.clear()
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self._changed.wait(), self.settings.flush_ms / 1000)
            for tp, part in tuple(self._parts.items()):
                self._launch(tp, part)
            if self._workers:
                _, pending = await asyncio.wait(self._workers, timeout=self._drain_timeout)
                if pending:
                    raise BadHandle("ingestion drain deadline expired")
            self._check()
            await self._flush()
        finally:
            self._running = False
            # Retire before cancellation. Late effects remain replay-safe but cannot settle tokens.
            self._offsets.retire(tuple(self._parts))
            for task in tuple(self._workers):
                task.cancel()
            for part in self._parts.values():
                if part.task is None:
                    self._release(part)
            if self._fetch is not None:
                self._fetch.cancel()
            stop_waiter.cancel()
            await asyncio.gather(stop_waiter, return_exceptions=True)

    async def wait_closed(self) -> None:
        """Wait for actual release of operations and memory, including cancellation suppressors."""
        while True:
            # gather() can complete synchronously for finished tasks. Their queued callbacks
            # must run to release credits and remove those tasks from the lifetime sets.
            await asyncio.sleep(0)
            pending = self._offsets.unfinished()
            tasks = set(self._workers) | self._operations
            if pending is not None and not pending.done():
                tasks.add(pending)
            if not tasks:
                # Preserve exception types, messages and traceback locations, but release completed
                # frame locals: a saved sink exception must not retain an already-released batch.
                errors_to_clear = [self._failure] if self._failure is not None else []
                seen: set[int] = set()
                while errors_to_clear:
                    error = errors_to_clear.pop()
                    if id(error) in seen:
                        continue
                    seen.add(id(error))
                    if error.__traceback__ is not None:
                        traceback.clear_frames(error.__traceback__)
                    for cause in (error.__cause__, error.__context__, getattr(error, "cause", None)):
                        if isinstance(cause, BaseException):
                            errors_to_clear.append(cause)
                return
            await asyncio.gather(*tasks, return_exceptions=True)

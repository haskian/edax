"""Bounded Kafka microbatches with atomic append/upsert sinks.

The client is caller-owned. Subscribe with this runner's listener and disable auto commit.
A stopped runner must not be restarted: stop its client and wait for ``wait_closed`` first.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast

from .. import _json, _native, _values, errors
from ..batch import BatchSettings, recover
from ..delivery import DeadLetter
from ..errors import BadHandle, CodecError, Shredded
from ..ingestion import BatchInput, BatchResult, BatchSink, MappedRow, Rejection
from ..message import EventMessage
from ..pacing import Pacing
from ..sealing import Scope, Sealing, joined_scope_guards, provider_deadline
from ..serializer import KernelSerializer, Serializer
from . import _kafka
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
    from aiokafka import ConsumerRebalanceListener, TopicPartition

T = TypeVar("T")
logger = logging.getLogger("edax.backend.kafka_ingest")


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
    # The open batch's sealing view: each scope's keys are read once and dropped with the batch.
    keys: Sealing | None = None
    # Consecutive contention waits; paces them, and resets once the partition makes progress.
    contended: int = 0


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
        sealing: Sealing | None = None,
    ) -> None:
        self.client, self.sink, self.mapper = client, sink, mapper
        # The sink's sealing opens records too: a runner that cannot see a destroyed key would neither
        # stage its records erased nor fence their commit.
        if sealing is not None and sink.sealing is not None:
            sealing.check_compatible(sink.sealing)
        self.sealing = sealing if sealing is not None else sink.sealing
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
        # The diagnostic of a rolled-back, isolable codec failure: the kernel's bounded rejection text.
        self._rejection: str = _json.decode(_native.batch_recover(1, 0, 1, 1, True, True, "codec"))[
            "rejection"
        ]
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
        return _kafka.rebalance_listener(self._revoke, self._assign)

    async def _assign(self, assigned: Collection[TopicPartition]) -> None:
        try:
            await self._assigned(assigned)
        except BaseException as error:
            self._fail(error)
            raise

    async def _assigned(self, assigned: Collection[TopicPartition]) -> None:
        async with self._assignment:
            await self._idle()
            for tp in sorted(assigned, key=lambda p: (p.topic, p.partition)):
                if tp in self._parts:
                    continue
                # A retained partition's fetch cannot mutate this new partition's position.
                if self._fetch_partition == tp and self._fetch is not None and not self._fetch.done():
                    self._fail(BadHandle("assignment overlapped an unresolved fetch; stop the client"))
                    self._check()
                epoch = self._epochs.get(tp, 0)
                with _kafka.classified():
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
        workers: set[asyncio.Future[Any]] = set()
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
        try:
            drained = await self._offsets.drain_revoked(revoked, workers, deadline)
            for task in drained.done:
                if not task.cancelled() and task.exception() is not None:
                    self._fail(cast(BaseException, task.exception()))
            failure = drained.commit_failure
            if failure is not None and errors.is_fatal(failure):
                self._fail(failure)
            elif failure is not None:
                logger.warning(
                    "kafka revoke commit failed; uncommitted records will replay", exc_info=failure
                )
            if drained.unfinished:
                self._fail(BadHandle("revoke deadline expired; stop the client and await wait_closed"))
        except asyncio.CancelledError:
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

    async def _idle(self) -> None:
        self._check()
        await self._offsets.wait_idle(self._drain_timeout)
        self._check()

    async def _bounded(self, call: Awaitable[T]) -> T:
        """Await ``call`` under the operation deadline; past it, stop admission and cancel the call."""
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

    async def _operation(self, call: Awaitable[T]) -> T:
        """:meth:`_bounded`, holding the caller until the call has actually ended."""
        started = False

        async def invoke() -> T:
            nonlocal started
            started = True
            # Assign inside the child: eager task factories start it before ensure_future returns.
            joined.task = asyncio.current_task()
            return await call

        with joined_scope_guards() as joined:
            task = asyncio.ensure_future(invoke())
        try:
            return await self._bounded(task)
        finally:
            if not task.done():
                # The worker retains its slot and credits through actual completion.
                await _kafka.settled(task)
            if not started and inspect.iscoroutine(call):
                call.close()

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
        part.keys = None
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
            with _kafka.classified():
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
        # No wakeup: the loop awaits its own fetch, and waking it would only make it spin.
        self._fetch_reserved = 0
        if not self._running and self._fetch is task:
            self._fetch = None
            self._fetch_partition = None

    async def _keepalive(self) -> None:
        if self._fetch is not None and not self._fetch.done():
            return
        generations = {tp: part.generation for tp, part in self._parts.items()}
        task = asyncio.create_task(
            _kafka.poll_activity(
                self.client, lambda tp: tp not in self._parts or self._parts[tp].task is None
            )
        )
        self._fetch, self._fetch_partition = task, None
        self._track(task)
        try:
            result = await self._bounded(task)
            for tp, records in result.items():
                if not records:
                    continue
                part = self._parts.get(tp)
                # A late old-generation result cannot move the replacement assignment's position.
                # A newly assigned partition has not had a normal fetch while this slot was occupied.
                if part is not None and generations.get(tp, part.generation) == part.generation:
                    self.client.seek(tp, part.frontier)
        finally:
            if task.done() and self._fetch is task:
                self._fetch = None

    def _raw(self, record: FetchedRecord) -> tuple[str, KafkaFetched, dict[str, Any]]:
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
        decoded = _json.decode(raw)
        if decoded["oversized"]:
            fetched = replace(fetched, key=b"", value=b"", headers=())
        return raw, fetched, decoded

    async def _prepare(self, part: _Partition, record: FetchedRecord) -> BatchInput:
        """The original for ``record``. With sealing, the mapper reads the opened event; a destroyed key
        makes an erased original."""
        raw, fetched, decoded_raw = self._raw(record)
        oversized = bool(decoded_raw["oversized"])
        target = self.sink.target
        fallback = _native.kafka_poison_id(record.topic, record.partition, record.offset)
        tenant: str | None = None
        subject: str | None = None
        stream = event_id = row_key = fallback
        sequence: int | None = None
        content: Any = None
        private: Any = None
        public: Any = None
        failure: str | None = None
        erased = False
        fingerprint: str | None = None

        def built() -> BatchInput:
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
                private,
                raw,
                failure,
                decoded_raw["sha256"],
                public=public,
                subject=subject,
                erased=erased,
                fingerprint=fingerprint,
            )

        try:
            if oversized:
                raise CodecError("record exceeds max_record_bytes; original retained at source coordinates")
            decoded = self._decoder(fetched)
            if isinstance(decoded, Poison):
                raise CodecError(str(decoded.cause)) from decoded.cause
            if not isinstance(decoded, EventMessage):
                raise CodecError("ingestion requires an event message")
            tenant, subject, stream, event_id, sequence = (
                decoded.tenant,
                decoded.subject,
                decoded.aggregate_id,
                decoded.id,
                decoded.sequence,
            )
            if target.partition is not None:
                failures = _json.decode(
                    _native.ingest_partition_failures(target.manifest, _json.encode([built()]))
                )
                if failures[0] is not None:
                    raise errors.from_wire(failures[0])
            canonical = self._canonical.encode(decoded)
            if len(canonical) > self.settings.max_mapped_bytes:
                raise CodecError("decoded event exceeds max_mapped_bytes")
            # The native envelope preserves exact i64 nanoseconds and sequences. A sealed one keeps its
            # ciphertext: replays compare the stored envelope, never a plaintext copy of it.
            content = canonical.decode()
            message = decoded
            if self.sealing is not None:
                if part.keys is None:
                    part.keys = self.sealing.for_batch(self._timeout)
                opened = await part.keys.open_event_for_consumer(decoded)
                if opened is None:
                    erased = True
                    return built()
                message = _values.replace(decoded, data=opened.data, encryption=None)
            mapped = self.mapper(message)
            if inspect.isawaitable(mapped):
                raise BadHandle("ingestion mapper must be synchronous")
            if not isinstance(mapped, MappedRow):
                raise BadHandle("ingestion mapper must return MappedRow")
            mapped_json = _json.encode(mapped)
            if len(canonical) + len(mapped_json.encode()) > self.settings.max_mapped_bytes:
                raise CodecError("mapped event exceeds max_mapped_bytes")
            # Snapshot host values so a mapper cannot mutate already admitted originals.
            snapshot = _json.decode(mapped_json)
            row_key, private, public = snapshot["key"], snapshot["private"], snapshot["public"]
            if part.keys is not None and subject is not None:
                try:
                    key = await part.keys.fingerprint_key(tenant, subject)
                except Shredded:
                    # Destroyed after the record opened: the provider confirms it, as the open does.
                    erased, private, public = True, None, None
                    return built()
                state = _json.encode({"content": content, "private": private, "public": public})
                fingerprint = _native.ingest_keyed_fingerprint(key, state)
            # Inside the try: an input the kernel refuses becomes a rejection, as a decode failure does.
            return built()
        except Exception as error:
            if errors.failure_kind(error) != "terminal":
                raise
            return self._rejected(record, raw, decoded_raw["sha256"], error)

    def _rejected(
        self, record: FetchedRecord, raw: str, raw_fingerprint: str, error: Exception
    ) -> BatchInput:
        # Preparation has not written anything. The kernel supplies a bounded code-only diagnostic;
        # a source-derived fallback cannot repeat the invalid producer/mapper identity that failed.
        decision = _json.decode(
            _native.batch_recover(1, 0, 1, self.settings.max_attempts, True, True, errors.code_of(error))
        )
        if decision["action"] != "reject":
            raise error
        fallback = _native.kafka_poison_id(record.topic, record.partition, record.offset)
        return self.sink.target.input(
            record.topic,
            record.partition,
            record.offset,
            None,
            fallback,
            fallback,
            fallback,
            None,
            None,
            None,
            raw,
            decision["rejection"],
            raw_fingerprint,
        )

    async def _admit(self, tp: TopicPartition, part: _Partition, record: FetchedRecord) -> bool:
        booked = Booked(
            self._offsets.tracker,
            record.topic,
            record.partition,
            record.offset,
            self._rewind,
        )
        try:
            with provider_deadline(self._timeout):
                item = await self._prepare(part, record)
            if self._parts.get(tp) is not part or not booked.live():
                # Revoked while the keys were read: the new owner fetches the record again.
                return False
            serialized_bytes = len(_json.encode(item).encode())
            # A later isolated rejection replaces the failure field. Reserve its bounded
            # kernel diagnostic now, before any partition can spend the remaining capacity.
            rejection_bytes = len(_json.encode(replace(item, failure=self._rejection)).encode())
            try:
                charge = _native.batch_charge(max(serialized_bytes, rejection_bytes), self._config)
            except CodecError as error:
                raw, _, decoded_raw = self._raw(record)
                item = self._rejected(record, raw, decoded_raw["sha256"], error)
                serialized_bytes = len(_json.encode(item).encode())
                charge = _native.batch_charge(serialized_bytes, self._config)
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
            source = self.sink.target.source_key(record.topic, record.partition, record.offset)
            attempt = part.attempts.get(source, 0) + 1
            # Nothing was written, but admission has no rejection path: a repeating failure stops.
            decision = recover(
                error, self.settings, attempt=attempt, count=1, isolable=False, waits=part.contended + 1
            )
            if decision["action"] not in ("retry", "wait"):
                raise
            if decision["action"] == "wait":
                part.contended += 1
            else:
                part.attempts[source] = attempt
            part.retry_batches[source] = (source, source)
            delay = self._pacing.delay(decision["step"])
            part.ready_at = asyncio.get_running_loop().time() + delay
            return False

    def _launch(self, tp: TopicPartition, part: _Partition) -> None:
        if not part.items or part.task is not None:
            return
        self.client.pause(tp)
        part.keys = None  # plaintext key copies are no longer needed after admission
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
            await self._idle()
            if not all(booked.live() for booked in bookings):
                return
            try:
                async with self._slots:
                    inputs, result = await self._commit(part, inputs)
                    if len(result.outcomes) != len(inputs):
                        raise BadHandle("sink returned an invalid outcome count")
                    confirmed: dict[str, str] = {}
                    for item, outcome in zip(inputs, result.outcomes, strict=True):
                        if outcome == "rejected":
                            record = result.rejections[item.source]
                            if not record["published"]:
                                await self._operation(
                                    self.publish_rejection(Rejection.saved(item.source, record))
                                )
                                confirmed[item.source] = record["fingerprint"]
                        elif outcome not in ("accepted", "expired"):
                            raise BadHandle("sink returned an unknown ingestion outcome")
                    # One statement marks every rejection this batch published.
                    await self._operation(self.sink.published(confirmed))
                    # Do not partially acknowledge a batch whose publication can still fail.
                    for item, booked in zip(inputs, bookings, strict=True):
                        booked.ack()
                        part.attempts.pop(item.source, None)
                        part.retry_batches.pop(item.source, None)
                    part.contended = 0
                # This frontier covers actual originals, including legitimate source offset gaps.
                if inputs and self._parts.get(tp) is part:
                    due = self._offsets.tracker.get().due()
                    for row in due:
                        if row.topic == tp.topic and row.partition == tp.partition:
                            part.frontier = max(part.frontier, row.offset)
            except Exception as error:
                attempt = 1 + max((part.attempts.get(item.source, 0) for item in inputs), default=0)
                # Originals that already carry a rejection cannot be rejected again.
                decision = recover(
                    error,
                    self.settings,
                    attempt=attempt,
                    count=len(inputs),
                    isolable=not all(item.failure is not None for item in inputs),
                    waits=part.contended + 1,
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
                    # The rejection keeps the original raw record; the mapped values that failed go.
                    inputs = tuple(
                        replace(item, failure=decision["rejection"], content=None, private=None, public=None)
                        for item in inputs
                    )
                    continue
                if action not in ("retry", "wait"):
                    raise BadHandle("unknown batch recovery action") from error
                # A wait is contention: it spends no attempt, and each one waits longer.
                if action == "wait":
                    part.contended += 1
                for item in inputs if action == "retry" else ():
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
                continue
            # These originals are already acknowledged. Offset failures have their own retry budget
            # and must never re-enter sink recovery or turn committed originals into rejections.
            await self._flush()
            return

    async def _commit(
        self, part: _Partition, inputs: tuple[BatchInput, ...]
    ) -> tuple[tuple[BatchInput, ...], BatchResult]:
        """Commit ``inputs`` under the write fence of every subject they write; the originals of a subject
        the provider reports destroyed commit erased."""
        if self.sealing is None:
            if any(item.subject is not None and not item.erased and item.failure is None for item in inputs):
                # No fence: a commit landing after an erasure cleanup would bring the subject's rows back.
                raise BadHandle(
                    "a subject's rows need sealing= on the runner or the sink for the erasure fence"
                )
            return inputs, await self._operation(self.sink.commit(inputs))
        written = [item for item in inputs if not item.erased and item.failure is None]
        scopes: set[Scope] = {(item.tenant, item.subject) for item in written}
        with provider_deadline(self._timeout):
            return await self._fenced_commit(inputs, written, scopes)

    async def _fenced_commit(
        self, inputs: tuple[BatchInput, ...], written: list[BatchInput], scopes: set[Scope]
    ) -> tuple[tuple[BatchInput, ...], BatchResult]:
        assert self.sealing is not None
        async with self.sealing.fenced(scopes, timeout_s=self._timeout) as destroyed:
            final = tuple(
                replace(item, erased=True, private=None, public=None, fingerprint=None)
                if item in written and (item.tenant, item.subject) in destroyed
                else item
                for item in inputs
            )
            # The sink call is shielded until it really ends, so the fence is never released early.
            return final, await self._operation(self.sink.commit(final))

    async def _flush(self) -> None:
        for attempt in range(1, self.settings.max_attempts + 1):
            try:
                await self._idle()
                await self._offsets.flush()
                return
            except Exception as error:
                decision = recover(error, self.settings, attempt=attempt, count=1, isolable=False)
                if decision["action"] not in ("retry", "wait"):
                    self._fail(error)
                    raise
                await self._pacing.wait(decision["step"])

    async def publish_rejection(self, rejection: Rejection) -> None:
        """Hand ``rejection`` to ``on_dead`` as the original Kafka record. The runner calls it before it
        acknowledges a batch; ``Relay(RejectionOutbox(sink), runner.publish_rejection)`` drains a backlog."""
        raw = _json.decode(rejection.raw)
        record = _native.kafka_ingestion_rejection_record(rejection.raw, rejection.source)
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
        await self._on_dead(DeadLetter(rejection.source, 1, errors.from_wire(rejection.error), fetched))

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
                # Clear before the pass, so a wakeup that arrives during it (a finished worker,
                # a rebalance, a stop) ends the wait below at once instead of being lost.
                self._changed.clear()
                await self._idle()
                progressed = False
                last_admitted: TopicPartition | None = None
                for tp in tuple(self._rotation):
                    part = self._parts.get(tp)
                    if part is None:
                        continue
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
                        if record is not None and await self._admit(tp, part, record):
                            progressed = True
                            last_admitted = tp
                    finally:
                        # Oversized originals become bounded stubs. Do not retain their payloads
                        # in this suspended frame while a sink or another partition waits.
                        record = None
                if last_admitted is not None and last_admitted in self._rotation:
                    # Capacity waiters get the next freed credit, ahead of busy earlier partitions.
                    self._rotation.rotate(-self._rotation.index(last_admitted) - 1)
                if not progressed and not stop.is_set():
                    await self._keepalive()
                    await _kafka.wait_event(self._changed, self.settings.flush_ms / 1000)
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
                if self._failure is not None:
                    _kafka.clear_failure_frames(self._failure)
                return
            # Cancelling this waiter must not detach a client's shielded broker request.
            await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))

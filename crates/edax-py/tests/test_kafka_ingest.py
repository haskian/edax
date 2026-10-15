"""Microbatch lifecycle tests against a Kafka-shaped source with retained replay history."""

from __future__ import annotations

import asyncio
import gc
import json
import weakref
from collections import defaultdict
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import pytest
from aiokafka import TopicPartition
from conftest import IdSerializer, KafkaMsg, build_message
from edax.backend.kafka import KafkaRecord
from edax.backend.kafka_ingest import KafkaIngestion
from edax.delivery import DeadLetter
from edax.errors import BadHandle
from edax.ingestion import (
    BatchCommitError,
    BatchInput,
    BatchResult,
    BatchSettings,
    IngestionTarget,
    MappedRow,
    MemoryIngestionSink,
)
from edax.message import EventMessage

TP = TopicPartition("orders", 0)


class EventIds(IdSerializer):
    def __init__(self, events: Sequence[EventMessage]) -> None:
        self.events = {event.id: event for event in events}

    def decode(self, wire: bytes) -> EventMessage:
        return self.events[wire.decode()]


class BatchClient:
    def __init__(self, records: Sequence[KafkaMsg]) -> None:
        self.logs: dict[TopicPartition, list[KafkaMsg]] = defaultdict(list)
        for record in records:
            self.logs[TopicPartition(record.topic, record.partition)].append(record)
        self.assigned = set(self.logs)
        self.positions: dict[TopicPartition, int] = defaultdict(int)
        self.paused: set[TopicPartition] = set()
        self.commits: list[dict[TopicPartition, int]] = []
        self.seeks: list[tuple[TopicPartition, int]] = []

    def assignment(self) -> set[TopicPartition]:
        return set(self.assigned)

    async def position(self, partition: TopicPartition) -> int:
        return self.positions[partition]

    def pause(self, *partitions: TopicPartition) -> None:
        self.paused.update(partitions)

    def resume(self, *partitions: TopicPartition) -> None:
        self.paused.difference_update(partitions)

    def seek(self, partition: TopicPartition, offset: int) -> None:
        self.seeks.append((partition, offset))
        self.positions[partition] = offset

    async def commit(self, offsets: dict[TopicPartition, int]) -> None:
        self.commits.append(dict(offsets))

    async def getmany(
        self, *partitions: TopicPartition, timeout_ms: int = 0, max_records: int | None = None
    ) -> dict[TopicPartition, list[KafkaMsg]]:
        assert max_records == 1
        result = {}
        for partition in partitions:
            if partition not in self.assigned or partition in self.paused:
                continue
            for record in self.logs[partition]:
                if record.offset >= self.positions[partition]:
                    result[partition] = [record]
                    self.positions[partition] = record.offset + 1
                    break
        return result


def settings(**changes: int) -> BatchSettings:
    values = {
        "max_records": 4,
        "max_bytes": 8192,
        "flush_ms": 5,
        "concurrency": 4,
        "retained_records": 20,
        "retained_bytes": 131072,
        "max_record_bytes": 4096,
        "max_mapped_bytes": 4096,
        "max_attempts": 2,
    }
    return BatchSettings(**(values | changes))


def events(count: int, partition: int = 0) -> list[EventMessage]:
    return [
        build_message(
            f"order-{partition}",
            "OrderState",
            i + 1,
            id=f"p{partition}-{i}",
            data=b'{"status":"paid"}',
            tenant="tenant-7",
        )
        for i in range(count)
    ]


def records(messages: Sequence[EventMessage], partition: int = 0) -> list[KafkaMsg]:
    return [
        KafkaMsg("orders", partition, offset, b"order", message.id.encode(), [])
        for offset, message in enumerate(messages)
    ]


def target() -> IngestionTarget:
    return IngestionTarget("upsert", "order-view-v1", "cluster-a:orders-v1", "order-state-v1")


def mapper(event: EventMessage) -> MappedRow:
    return MappedRow(event.aggregate_id, {"event_id": event.id, "status": "paid"})


async def no_dead(letter: DeadLetter[Any]) -> None:
    raise AssertionError(f"unexpected dead letter: {letter}")


async def until(predicate: Callable[[], bool], timeout: float = 1) -> None:
    async def wait() -> None:
        while not predicate():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(wait(), timeout)


@asynccontextmanager
async def running(runner: KafkaIngestion) -> AsyncIterator[asyncio.Task[None]]:
    task = asyncio.create_task(runner.run(asyncio.Event()))
    try:
        yield task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.wait_for(runner.wait_closed(), 1)


class RecordingSink(MemoryIngestionSink):
    def __init__(self) -> None:
        super().__init__(target())
        self.batches: list[tuple[str, ...]] = []

    async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
        self.batches.append(tuple(item.event_id for item in inputs))
        return await super().commit(inputs)


async def test_batches_originals_and_replays_tombstones() -> None:
    messages = events(4)
    sink = RecordingSink()

    def mapped(event: EventMessage) -> MappedRow:
        return MappedRow(event.aggregate_id, None if event.sequence == 4 else {"event_id": event.id})

    for _ in range(2):
        client = BatchClient(records(messages))
        runner = KafkaIngestion(
            client, sink, mapped, no_dead, settings=settings(flush_ms=100), serializer=EventIds(messages)
        )
        async with running(runner):
            await until(lambda client=client: any(batch.get(TP) == 4 for batch in client.commits))
        assert runner.retained_records == runner.retained_bytes == 0
    stored = await sink.get("tenant-7", "order-0")
    assert stored is not None and stored["sequence"] == 4 and stored["value"] is None
    assert sink.batches == [tuple(message.id for message in messages)] * 2
    assert await sink.pending() == {}


async def test_failed_dead_publication_never_acknowledges_rejection() -> None:
    messages = events(1)
    client = BatchClient(records(messages))
    sink = MemoryIngestionSink(target())
    called: list[DeadLetter[Any]] = []

    def invalid(event: EventMessage) -> MappedRow:
        from edax.errors import CodecError

        raise CodecError("invalid mapped order")

    async def unavailable(letter: DeadLetter[Any]) -> None:
        called.append(letter)
        raise OSError("dead-letter broker unavailable")

    runner = KafkaIngestion(
        client, sink, invalid, unavailable, settings=settings(max_records=1), serializer=EventIds(messages)
    )
    async with running(runner) as task:
        with pytest.raises(OSError, match="dead-letter broker unavailable"):
            await asyncio.wait_for(asyncio.shield(task), 1)
    assert len(called) == 2
    assert client.commits == []
    assert len(await sink.pending()) == 1


async def test_four_retries_cannot_starve_a_fifth_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    from edax import _native

    # Backoffs stay pending throughout the assertion; healthy work must gain admission by eviction.
    monkeypatch.setattr(_native, "backoff_millis", lambda *args: 10_000)
    healthy = asyncio.Event()
    failed: set[str] = set()

    class OutageSink(RecordingSink):
        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            if inputs[0].stream != "order-4":
                failed.add(inputs[0].stream)
                raise BatchCommitError("retry", OSError("partition unavailable"))
            result = await super().commit(inputs)
            healthy.set()
            return result

    messages = [event for p in range(5) for event in events(1, p)]
    client = BatchClient([record for p in range(5) for record in records(events(1, p), p)])
    sink = OutageSink()
    runner = KafkaIngestion(
        client,
        sink,
        mapper,
        no_dead,
        settings=settings(max_records=1, retained_records=4, max_attempts=20),
        serializer=EventIds(messages),
    )
    async with running(runner):
        await asyncio.wait_for(healthy.wait(), 1)
        await until(lambda: any(batch.get(TopicPartition("orders", 4)) == 1 for batch in client.commits))
    assert failed == {f"order-{p}" for p in range(4)}
    assert runner.peak_records <= 4
    assert runner.peak_bytes <= runner.settings.retained_bytes
    assert await sink.get("tenant-7", "order-4") is not None


async def test_known_rollback_bisects_originals_and_executes_each_child() -> None:
    attempts: list[tuple[tuple[str, bool], ...]] = []
    dead: list[DeadLetter[Any]] = []

    class ConstraintSink(MemoryIngestionSink):
        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            attempts.append(tuple((item.event_id, item.failure is not None) for item in inputs))
            if any(item.event_id == "p0-2" and item.failure is None for item in inputs):
                raise BatchCommitError("isolate", ValueError("row constraint"))
            return await super().commit(inputs)

    async def publish(letter: DeadLetter[Any]) -> None:
        dead.append(letter)

    messages = events(4)
    client = BatchClient(records(messages))
    sink = ConstraintSink(target())
    runner = KafkaIngestion(client, sink, mapper, publish, settings=settings(), serializer=EventIds(messages))
    async with running(runner):
        await until(lambda: any(batch.get(TP) == 4 for batch in client.commits))
    assert attempts == [
        (("p0-0", False), ("p0-1", False), ("p0-2", False), ("p0-3", False)),
        (("p0-0", False), ("p0-1", False)),
        (("p0-2", False), ("p0-3", False)),
        (("p0-2", False),),
        (("p0-2", True),),
        (("p0-3", False),),
    ]
    assert len(dead) == 1
    assert await sink.pending() == {}


async def test_uncertain_commit_exhaustion_replays_without_dead_letter_or_ack() -> None:
    class LostReplySink(RecordingSink):
        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            await super().commit(inputs)
            raise BatchCommitError("uncertain", OSError("commit reply lost"))

    messages = events(2)
    client = BatchClient(records(messages))
    sink = LostReplySink()
    runner = KafkaIngestion(
        client, sink, mapper, no_dead, settings=settings(max_records=2), serializer=EventIds(messages)
    )
    async with running(runner) as task:
        with pytest.raises(BatchCommitError, match="commit reply lost"):
            await asyncio.wait_for(asyncio.shield(task), 1)
    assert sink.batches == [("p0-0", "p0-1")] * 2
    assert client.commits == []
    assert await sink.pending() == {}
    stored = await sink.get("tenant-7", "order-0")
    assert stored is not None and stored["sequence"] == 2


async def test_late_fetch_cannot_be_booked_after_reassignment() -> None:
    entered, release = asyncio.Event(), asyncio.Event()

    class HeldFetch(BatchClient):
        async def getmany(
            self, *partitions: TopicPartition, timeout_ms: int = 0, max_records: int | None = None
        ) -> dict[TopicPartition, list[KafkaMsg]]:
            entered.set()
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    continue
            return await super().getmany(*partitions, timeout_ms=timeout_ms, max_records=max_records)

    messages = events(1)
    client = HeldFetch(records(messages))
    sink = RecordingSink()
    runner = KafkaIngestion(
        client,
        sink,
        mapper,
        no_dead,
        settings=settings(max_records=1),
        serializer=EventIds(messages),
        drain_timeout_s=0.02,
    )
    async with running(runner) as task:
        try:
            await asyncio.wait_for(entered.wait(), 1)
            listener = runner.rebalance_listener()
            await listener.on_partitions_revoked({TP})
            assert runner.retained_bytes >= runner._reserve
            with pytest.raises(BadHandle, match="revoke deadline"):
                await listener.on_partitions_assigned({TP})
        finally:
            release.set()
        with pytest.raises(BadHandle):
            await asyncio.wait_for(asyncio.shield(task), 1)
    assert sink.batches == []
    assert client.commits == []
    assert runner.retained_records == 0
    assert runner.retained_bytes == 0
    assert runner._fetch is None


async def test_cancelled_sink_keeps_memory_credits_until_actual_release() -> None:
    entered, release = asyncio.Event(), asyncio.Event()

    class HeldSink(MemoryIngestionSink):
        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            entered.set()
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    continue
            return await super().commit(inputs)

    messages = events(1)
    client = BatchClient(records(messages))
    sink = HeldSink(target())
    runner = KafkaIngestion(
        client,
        sink,
        mapper,
        no_dead,
        settings=settings(max_records=1),
        serializer=EventIds(messages),
        drain_timeout_s=0.02,
    )
    async with running(runner):
        try:
            await asyncio.wait_for(entered.wait(), 1)
            retained = runner.retained_bytes
            assert runner.retained_records == 1 and retained > 0
            await runner.rebalance_listener().on_partitions_revoked({TP})
            assert runner.retained_records == 1
            assert runner.retained_bytes == retained
            worker = next(iter(runner._workers))
            worker.cancel()
            await asyncio.sleep(0)
            worker.cancel()
            await asyncio.sleep(0)
            assert runner.retained_records == 1
            assert runner.retained_bytes == retained
            assert client.commits == []
        finally:
            release.set()
        await until(lambda: runner.retained_records == 0)
    assert client.commits == []
    assert runner.retained_bytes == 0


async def test_revoke_before_worker_first_instruction_releases_credits() -> None:
    messages = events(1)
    client = BatchClient(records(messages))
    sink = RecordingSink()
    runner = KafkaIngestion(
        client, sink, mapper, no_dead, settings=settings(max_records=1), serializer=EventIds(messages)
    )
    await runner._assign({TP})
    part = runner._parts[TP]
    runner._admit(TP, part, client.logs[TP][0])
    assert runner.retained_records == 1 and runner.retained_bytes > 0
    runner._launch(TP, part)
    # Calling revoke immediately retires and cancels the task before the event loop can start it.
    await runner.rebalance_listener().on_partitions_revoked({TP})
    await runner.wait_closed()
    assert sink.batches == []
    assert runner.retained_records == runner.retained_bytes == 0
    assert client.commits == []


async def test_retry_eviction_after_left_child_commit_rewinds_first_live_right_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from edax import _native

    monkeypatch.setattr(_native, "backoff_millis", lambda *args: 10_000)

    class SplitThenRetrySink(RecordingSink):
        retry: bool = True

        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            if len(inputs) == 4:
                raise BatchCommitError("isolate", ValueError("confirmed row-local rollback"))
            if inputs[0].event_id == "p0-2" and self.retry:
                raise BatchCommitError("retry", OSError("right child temporarily unavailable"))
            return await super().commit(inputs)

    messages = events(4)
    client = BatchClient(records(messages))
    sink = SplitThenRetrySink()
    runner = KafkaIngestion(client, sink, mapper, no_dead, settings=settings(), serializer=EventIds(messages))
    await runner._assign({TP})
    part = runner._parts[TP]
    for record in client.logs[TP]:
        runner._admit(TP, part, record)
    client.positions[TP] = 4
    runner._launch(TP, part)
    await until(lambda: part.backing_off)
    assert any(batch.get(TP) == 2 for batch in client.commits)
    assert runner._evict_retry()
    await until(lambda: part.task is None)
    assert client.seeks == [(TP, 2)], "settled left originals cannot choose the rewind frontier"
    assert runner.retained_records == runner.retained_bytes == 0
    assert part.ready_at > asyncio.get_running_loop().time(), "eviction must preserve the retry deadline"
    # Expire the synthetic ten-second deadline without delaying the test suite.
    part.ready_at = asyncio.get_running_loop().time()
    sink.retry = False
    async with running(runner):
        await until(lambda: any(batch.get(TP) == 4 for batch in client.commits))
    assert sink.batches == [("p0-0", "p0-1"), ("p0-2", "p0-3")]


async def test_cooperative_fetch_revoke_keeps_other_partition_running() -> None:
    entered = asyncio.Event()

    class CooperativeFetch(BatchClient):
        async def getmany(
            self, *partitions: TopicPartition, timeout_ms: int = 0, max_records: int | None = None
        ) -> dict[TopicPartition, list[KafkaMsg]]:
            if TP in partitions:
                entered.set()
                await asyncio.Future()
            return await super().getmany(*partitions, timeout_ms=timeout_ms, max_records=max_records)

    messages = [*events(1, 0), *events(1, 1)]
    client = CooperativeFetch([*records(events(1, 0), 0), *records(events(1, 1), 1)])
    sink = RecordingSink()
    runner = KafkaIngestion(
        client, sink, mapper, no_dead, settings=settings(max_records=1), serializer=EventIds(messages)
    )
    async with running(runner) as task:
        await asyncio.wait_for(entered.wait(), 1)
        client.assigned.remove(TP)
        await runner.rebalance_listener().on_partitions_revoked({TP})
        await until(lambda: any(batch.get(TopicPartition("orders", 1)) == 1 for batch in client.commits))
        assert not task.done(), "normal fetch cancellation or rotation shrink stopped the runner"
    assert sink.batches == [("p1-0",)]
    assert all(TP not in batch for batch in client.commits)


async def test_position_reply_after_revoke_cannot_resurrect_assignment() -> None:
    entered, release = asyncio.Event(), asyncio.Event()

    class HeldPosition(BatchClient):
        async def position(self, partition: TopicPartition) -> int:
            entered.set()
            await release.wait()
            return await super().position(partition)

    messages = events(1)
    client = HeldPosition(records(messages))
    runner = KafkaIngestion(
        client,
        RecordingSink(),
        mapper,
        no_dead,
        settings=settings(max_records=1),
        serializer=EventIds(messages),
    )
    listener = runner.rebalance_listener()
    assigning = asyncio.create_task(listener.on_partitions_assigned({TP}))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        client.assigned.remove(TP)
        await listener.on_partitions_revoked({TP})
    finally:
        release.set()
    result = await asyncio.wait_for(asyncio.gather(assigning, return_exceptions=True), 1)
    assert result[0] is None or isinstance(result[0], BadHandle)
    assert TP not in runner._parts, "a stale position reply installed a revoked partition"
    assert TP not in runner._rotation
    await runner.wait_closed()
    assert client.commits == []


async def test_idle_runner_retries_broker_commit_without_repeating_sink() -> None:
    class FailingCommit(BatchClient):
        calls: int = 0

        async def commit(self, offsets: dict[TopicPartition, int]) -> None:
            self.calls += 1
            if self.calls == 1:
                raise OSError("temporary broker commit failure")
            await super().commit(offsets)

    messages = events(1)
    client = FailingCommit(records(messages))
    sink = RecordingSink()
    runner = KafkaIngestion(
        client, sink, mapper, no_dead, settings=settings(max_records=1), serializer=EventIds(messages)
    )
    async with running(runner) as task:
        await until(lambda: client.commits == [{TP: 1}])
        assert not task.done()
    assert client.calls == 2
    assert sink.batches == [("p0-0",)], "broker commit retry must not rerun committed sink work"


async def test_exact_i64_sequence_source_offset_and_nanosecond_event_envelope() -> None:
    maximum = 2**63 - 1
    recorded_ns = 1_750_000_000_123_456_789
    message = build_message(
        "order-0",
        "OrderState",
        maximum,
        id="wide-sequence-event",
        tenant="tenant-7",
        data=b'{"status":"paid"}',
        recorded_at=recorded_ns,
        occurred_at=recorded_ns,
    )
    fetched = KafkaMsg("orders", 0, maximum - 1, b"order", message.id.encode(), [])
    client = BatchClient([fetched])
    client.positions[TP] = maximum - 1
    sink = RecordingSink()
    runner = KafkaIngestion(
        client, sink, mapper, no_dead, settings=settings(max_records=1), serializer=EventIds([message])
    )
    async with running(runner):
        await until(lambda: any(batch.get(TP) == maximum for batch in client.commits))
    stored = await sink.get("tenant-7", "order-0")
    assert stored is not None and stored["sequence"] == maximum
    assert await sink.pending() == {}

    # MAX itself has no representable next broker offset, but its provenance still encodes exactly.
    prepared = runner._prepare(fetched._replace(offset=maximum))
    assert prepared.source == sink.target.key(
        [
            "source",
            sink.target.source_incarnation,
            "orders",
            0,
            str(maximum),
        ]
    )
    assert prepared.sequence == maximum
    assert prepared.failure is None
    assert str(recorded_ns) in prepared.content
    assert (await sink.commit([prepared])).outcomes == ("accepted",)


async def test_wait_closed_preserves_commit_until_client_completion() -> None:
    entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class HeldCommit(BatchClient):
        calls: int = 0

        async def commit(self, offsets: dict[TopicPartition, int]) -> None:
            self.calls += 1
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
            await super().commit(offsets)

    messages = events(1)
    client = HeldCommit(records(messages))
    runner = KafkaIngestion(
        client,
        RecordingSink(),
        mapper,
        no_dead,
        settings=settings(max_records=1),
        serializer=EventIds(messages),
    )
    task = asyncio.create_task(runner.run(asyncio.Event()))
    closing: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 1)
        closing = asyncio.create_task(runner.wait_closed())
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(closing), 0.02)
        assert not cancelled.is_set()
        assert client.calls == 1
        assert client.commits == []
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.wait_for(closing if closing is not None else runner.wait_closed(), 1)
    assert client.commits == [{TP: 1}]
    assert runner.retained_records == runner.retained_bytes == 0


@pytest.mark.parametrize("oversize", ["headers", "base64", "mapped"])
async def test_oversize_originals_and_mapper_outputs_become_durable_rejections(oversize: str) -> None:
    messages = events(1)
    fetched = records(messages)
    if oversize == "headers":
        fetched[0] = fetched[0]._replace(headers=[("large", b"x" * 20_000)])
    if oversize == "base64":
        fetched[0] = fetched[0]._replace(value=b"x" * 3000)
    client = BatchClient(fetched)
    raw_inputs: list[str] = []

    class CaptureSink(MemoryIngestionSink):
        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            raw_inputs.extend(item.raw for item in inputs)
            return await super().commit(inputs)

    sink = CaptureSink(target())
    dead: list[DeadLetter[Any]] = []

    def mapped(event: EventMessage) -> MappedRow:
        if oversize in {"headers", "base64"}:
            raise AssertionError("oversized source must not reach the mapper")
        return MappedRow(event.aggregate_id, {"state": "x" * 20_000})

    async def publish(letter: DeadLetter[Any]) -> None:
        dead.append(letter)

    runner = KafkaIngestion(
        client, sink, mapped, publish, settings=settings(max_records=1), serializer=EventIds(messages)
    )
    async with running(runner):
        await until(lambda: any(batch.get(TP) == 1 for batch in client.commits))
    assert len(dead) == 1
    if oversize in {"headers", "base64"}:
        raw = json.loads(raw_inputs[0])
        assert raw["oversized"] is True
        assert len(raw["sha256"]) == 64
        assert all(character in "0123456789abcdef" for character in raw["sha256"])
        assert dead[0].origin.value == b""
    assert await sink.get("tenant-7", "order-0") is None
    assert await sink.pending() == {}
    assert runner.peak_bytes <= runner.settings.retained_bytes


async def test_oversized_original_is_released_before_waiting_for_sink() -> None:
    entered, release = asyncio.Event(), asyncio.Event()

    @dataclass
    class LargeRecord:
        value: bytes
        topic: str = "orders"
        partition: int = 0
        offset: int = 0
        key: bytes = b"order"
        headers: tuple[tuple[str, bytes], ...] = ()
        timestamp: int | None = None
        timestamp_type: int | None = None

    original: weakref.ReferenceType[LargeRecord] | None = None

    class ReleasingClient(BatchClient):
        async def getmany(self, *partitions: TopicPartition, **kwargs: Any) -> Any:
            nonlocal original
            if original is not None:
                return {}
            fetched = LargeRecord(b"x" * 20_000_000)
            original = weakref.ref(fetched)
            return {TP: [fetched]}

    class HeldSink(RecordingSink):
        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            entered.set()
            await release.wait()
            return await super().commit(inputs)

    client = ReleasingClient([])
    client.assigned.add(TP)
    runner = KafkaIngestion(client, HeldSink(), mapper, no_dead, settings=settings(max_records=1))
    async with running(runner):
        try:
            await asyncio.wait_for(entered.wait(), 5)
            gc.collect()
            assert original is not None and original() is None
            assert 0 < runner.retained_bytes <= runner.settings.retained_bytes
        finally:
            release.set()


async def test_partial_revoke_does_not_cancel_retained_partition_fetch() -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    retained = TopicPartition("orders", 1)

    class HeldOther(BatchClient):
        async def getmany(self, *partitions: TopicPartition, **kwargs: Any) -> Any:
            if retained in partitions and not release.is_set():
                entered.set()
                await release.wait()
            return await super().getmany(*partitions, **kwargs)

    messages = [*events(1, 0), *events(1, 1)]
    client = HeldOther([*records(events(1, 0)), *records(events(1, 1), 1)])
    sink = RecordingSink()
    runner = KafkaIngestion(
        client, sink, mapper, no_dead, settings=settings(max_records=1), serializer=EventIds(messages)
    )
    async with running(runner) as task:
        try:
            await asyncio.wait_for(entered.wait(), 1)
            client.assigned.remove(TP)
            listener = runner.rebalance_listener()
            await listener.on_partitions_revoked({TP})
            await listener.on_partitions_assigned({retained})
            assert runner._fetch is not None and not runner._fetch.cancelled()
            release.set()
            await until(lambda: any(batch.get(retained) == 1 for batch in client.commits))
            assert not task.done()
        finally:
            release.set()
    assert ("p1-0",) in sink.batches


@pytest.mark.parametrize("exhaust", [False, True])
@pytest.mark.parametrize("stage", ["mapper", "middleware"])
async def test_preparation_retry_rewinds_open_batch_without_starving_healthy_partition(
    exhaust: bool,
    stage: str,
) -> None:
    attempts = 0

    def fail(event_id: str) -> None:
        nonlocal attempts
        if event_id == "p0-1":
            attempts += 1
            if exhaust or attempts == 1:
                raise OSError("temporary preparation outage")

    def transient(event: EventMessage) -> MappedRow:
        fail(event.id)
        return mapper(event)

    class TransientMiddleware:
        def on_poll(self, record: KafkaRecord) -> KafkaRecord:
            fail(record.value.decode())
            return record

        def on_publish(self, record: KafkaRecord) -> KafkaRecord:
            return record

    messages = [*events(2), *events(1, 1)]
    client = BatchClient([*records(events(2)), *records(events(1, 1), 1)])
    sink = RecordingSink()
    runner = KafkaIngestion(
        client,
        sink,
        transient if stage == "mapper" else mapper,
        no_dead,
        settings=settings(max_attempts=3),
        serializer=EventIds(messages),
        middleware=[TransientMiddleware()] if stage == "middleware" else [],
    )
    async with running(runner) as task:
        await until(lambda: any(batch.get(TopicPartition("orders", 1)) == 1 for batch in client.commits))
        if exhaust:
            with pytest.raises(OSError, match="temporary preparation outage"):
                await asyncio.wait_for(asyncio.shield(task), 1)
        else:
            await until(lambda: any(batch.get(TP) == 2 for batch in client.commits))
            assert not task.done()
    assert attempts == (3 if exhaust else 2)
    assert (TP, 0) in client.seeks, "the uncommitted prefix must be replayed too"
    assert await sink.pending() == {}
    if exhaust:
        assert all(TP not in batch for batch in client.commits)
        assert await sink.get("tenant-7", "order-0") is None
    else:
        assert sink.batches.count(("p0-0", "p0-1")) == 1
    assert runner.retained_records == runner.retained_bytes == 0


async def test_eviction_preserves_retry_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    from edax import _native

    monkeypatch.setattr(_native, "backoff_millis", lambda *args: 10_000)

    class UnavailableSink(RecordingSink):
        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            self.batches.append(tuple(item.event_id for item in inputs))
            raise BatchCommitError("retry", OSError("persistent sink outage"))

    messages = events(1)
    client = BatchClient(records(messages))
    sink = UnavailableSink()
    runner = KafkaIngestion(
        client,
        sink,
        mapper,
        no_dead,
        settings=settings(max_records=1, max_attempts=3),
        serializer=EventIds(messages),
    )
    async with running(runner) as task:
        for attempt in (1, 2):
            await until(lambda: TP in runner._parts and runner._parts[TP].backing_off)
            part = runner._parts[TP]
            assert len(sink.batches) == attempt
            assert runner._evict_retry()
            await until(lambda part=part: part.task is None)
            assert part.ready_at > asyncio.get_running_loop().time()
            assert runner.retained_records == runner.retained_bytes == 0
            part.ready_at = asyncio.get_running_loop().time()
            runner._changed.set()
        with pytest.raises(BatchCommitError, match="persistent sink outage"):
            await asyncio.wait_for(asyncio.shield(task), 1)
    assert sink.batches == [("p0-0",)] * 3
    assert client.commits == []
    assert client.seeks[:2] == [(TP, 0), (TP, 0)]


async def test_stop_flushes_residual_partial_batch() -> None:
    messages = events(2)
    client = BatchClient(records(messages))
    sink = RecordingSink()
    runner = KafkaIngestion(
        client, sink, mapper, no_dead, settings=settings(flush_ms=10_000), serializer=EventIds(messages)
    )
    stop = asyncio.Event()
    task = asyncio.create_task(runner.run(stop))
    try:
        await until(lambda: runner.retained_records == 2)
        assert sink.batches == []
        stop.set()
        await asyncio.wait_for(asyncio.shield(task), 1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.wait_for(runner.wait_closed(), 1)
    assert sink.batches == [("p0-0", "p0-1")]
    assert client.commits == [{TP: 2}]
    assert runner.retained_records == runner.retained_bytes == 0


async def test_stop_enforces_drain_deadline_and_retains_held_sink_credits() -> None:
    entered, release = asyncio.Event(), asyncio.Event()

    class HeldSink(RecordingSink):
        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            entered.set()
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    continue
            return await super().commit(inputs)

    messages = events(1)
    client = BatchClient(records(messages))
    sink = HeldSink()
    runner = KafkaIngestion(
        client,
        sink,
        mapper,
        no_dead,
        settings=settings(flush_ms=10_000),
        serializer=EventIds(messages),
        drain_timeout_s=0.02,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(runner.run(stop))
    try:
        await until(lambda: runner.retained_records == 1)
        stop.set()
        await asyncio.wait_for(entered.wait(), 1)
        with pytest.raises(BadHandle, match="drain deadline"):
            await asyncio.wait_for(asyncio.shield(task), 1)
        assert runner.retained_records == 1 and runner.retained_bytes > 0
        assert client.commits == []
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.wait_for(runner.wait_closed(), 1)
    assert client.commits == []
    assert runner.retained_records == runner.retained_bytes == 0


@pytest.mark.parametrize("fatal", [False, True], ids=["exhaustion", "fatal"])
async def test_broker_commit_failure_stops_without_repeating_sink(fatal: bool) -> None:
    class FailedCommit(BatchClient):
        calls: int = 0

        async def commit(self, offsets: dict[TopicPartition, int]) -> None:
            self.calls += 1
            if fatal:
                raise BadHandle("broker credentials invalid")
            raise OSError("broker unavailable")

    messages = events(1)
    client = FailedCommit(records(messages))
    sink = RecordingSink()
    runner = KafkaIngestion(
        client, sink, mapper, no_dead, settings=settings(max_records=1), serializer=EventIds(messages)
    )
    async with running(runner) as task:
        with pytest.raises(BadHandle if fatal else OSError, match="broker"):
            await asyncio.wait_for(asyncio.shield(task), 1)
    assert client.calls == (1 if fatal else 2)
    assert sink.batches == [("p0-0",)]
    assert client.commits == []
    assert await sink.pending() == {}


async def test_revoke_clean_commit_failure_allows_reassignment_and_replay() -> None:
    from aiokafka.errors import CommitFailedError

    class MembershipLost(BatchClient):
        calls: int = 0

        async def commit(self, offsets: dict[TopicPartition, int]) -> None:
            self.calls += 1
            if self.calls == 1:
                raise CommitFailedError()
            await super().commit(offsets)

    messages = events(1)
    client = MembershipLost(records(messages))
    sink = RecordingSink()
    runner = KafkaIngestion(
        client, sink, mapper, no_dead, settings=settings(max_records=1), serializer=EventIds(messages)
    )
    await runner._assign({TP})
    part = runner._parts[TP]
    runner._admit(TP, part, client.logs[TP][0])
    await sink.commit(tuple(part.items))
    part.bookings[0].ack()
    await runner.rebalance_listener().on_partitions_revoked({TP})
    assert runner._failure is None, "a completed revoke commit failure only requires replay"
    async with running(runner) as task:
        await until(lambda: client.commits == [{TP: 1}])
        assert not task.done()
    assert client.calls == 2
    assert sink.batches == [("p0-0",)] * 2
    assert await sink.get("tenant-7", "order-0") is not None
    assert runner.retained_records == runner.retained_bytes == 0


@pytest.mark.parametrize(
    ("case", "fatal"),
    [
        ("poison", False),
        ("command", False),
        ("async_mapper", True),
        ("invalid_return", True),
        ("fatal_mapper", True),
    ],
)
async def test_prepare_failure_classification(case: str, fatal: bool) -> None:
    from conftest import build_command
    from edax.errors import CodecError
    from edax.message import CommandMessage

    messages = events(1)
    dead: list[DeadLetter[Any]] = []
    mapper_calls = 0
    awaitable: asyncio.Future[MappedRow] = asyncio.get_running_loop().create_future()
    awaitable.set_result(mapper(messages[0]))

    class MalformedSerializer(IdSerializer):
        def decode(self, wire: bytes) -> Any:
            if case == "poison":
                raise CodecError("invalid event envelope")
            if case == "command":
                command: CommandMessage = build_command(
                    "PayOrder", aggregate_id="order-0", tenant="tenant-7", data=b'{"status":"paid"}'
                )
                return command
            return messages[0]

    def mapped(event: EventMessage) -> Any:
        nonlocal mapper_calls
        mapper_calls += 1
        if case == "async_mapper":
            return awaitable
        if case == "invalid_return":
            return {"status": "paid"}
        if case == "fatal_mapper":
            raise BadHandle("mapper configuration invalid")
        raise AssertionError("invalid envelope must not reach mapper")

    async def publish(letter: DeadLetter[Any]) -> None:
        dead.append(letter)

    client = BatchClient(records(messages))
    sink = RecordingSink()
    runner = KafkaIngestion(
        client, sink, mapped, publish, settings=settings(max_records=1), serializer=MalformedSerializer()
    )
    async with running(runner) as task:
        if fatal:
            with pytest.raises(BadHandle, match="mapper"):
                await asyncio.wait_for(asyncio.shield(task), 1)
        else:
            await until(lambda: client.commits == [{TP: 1}])
            assert not task.done()
    assert mapper_calls == (1 if fatal else 0)
    assert len(dead) == (0 if fatal else 1)
    assert client.commits == ([] if fatal else [{TP: 1}])
    assert await sink.get("tenant-7", "order-0") is None
    assert await sink.pending() == {}


@pytest.mark.parametrize("kind", ["isolate", "invalid"])
async def test_rejected_input_isolation_exhausts_without_rejecting_forever(kind: str) -> None:
    from edax.errors import CodecError

    class FailedRejectionSink(RecordingSink):
        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            assert all(item.failure is not None for item in inputs)
            self.batches.append(tuple(item.event_id for item in inputs))
            raise BatchCommitError(kind, ValueError("rejection row constraint"))

    def invalid(event: EventMessage) -> MappedRow:
        raise CodecError("invalid order payload")

    messages = events(1)
    client = BatchClient(records(messages))
    sink = FailedRejectionSink()
    runner = KafkaIngestion(
        client,
        sink,
        invalid,
        no_dead,
        settings=settings(max_records=1, max_attempts=3),
        serializer=EventIds(messages),
    )
    async with running(runner) as task:
        with pytest.raises(BatchCommitError, match="rejection row constraint"):
            await asyncio.wait_for(asyncio.shield(task), 1)
    assert sink.batches == [("p0-0",)] * 3
    assert client.commits == []
    assert await sink.pending() == {}


@pytest.mark.parametrize("limits", [(4096, 1024), (1024, 4096)], ids=["full_to_stub", "stub_to_full"])
async def test_rejected_source_replays_across_raw_size_limits(limits: tuple[int, int]) -> None:
    from edax.errors import CodecError

    messages = events(1)
    fetched = records(messages)[0]._replace(headers=[("order-context", b"x" * 1500)])
    sink = MemoryIngestionSink(target())

    def invalid(event: EventMessage) -> MappedRow:
        raise CodecError("invalid order payload")

    for limit in limits:
        runner = KafkaIngestion(
            BatchClient([fetched]),
            sink,
            invalid,
            no_dead,
            settings=settings(max_record_bytes=limit),
            serializer=EventIds(messages),
        )
        prepared = runner._prepare(fetched)
        result = await sink.commit([prepared])
        assert result.outcomes == ("rejected",)
    assert len(await sink.pending()) == 1
    assert await sink.get("tenant-7", "order-0") is None


async def test_admission_snapshots_reused_mutable_mapper_values() -> None:
    from decimal import Decimal

    from edax import _json

    messages = events(2)
    fetched = records(messages)
    client = BatchClient(fetched)
    sink = MemoryIngestionSink(IngestionTarget("append", "events-v1", "source-v1", "mapper-v1"))
    lines: list[dict[str, Any]] = [{"quantity": 1}]
    shared: dict[str, Any] = {"event_id": "", "lines": lines, "total": Decimal("9.50")}

    def reuse(event: EventMessage) -> MappedRow:
        shared["event_id"] = event.id
        return MappedRow(event.id, shared)

    runner = KafkaIngestion(client, sink, reuse, no_dead, settings=settings(), serializer=EventIds(messages))
    await runner._assign({TP})
    part = runner._parts[TP]
    assert runner._admit(TP, part, fetched[0])
    first_encoded = _json.encode(part.items[0])
    assert runner._admit(TP, part, fetched[1])
    assert _json.encode(part.items[0]) == first_encoded
    admitted_encoded = _json.encode(part.items)
    charged_bytes = runner.retained_bytes

    lines[0]["quantity"] = "x" * runner.settings.retained_bytes
    assert _json.encode(part.items) == admitted_encoded
    assert runner.retained_bytes == charged_bytes
    assert len(admitted_encoded.encode()) <= charged_bytes
    result = await sink.commit(part.items)
    assert result.outcomes == ("accepted", "accepted")
    for message in messages:
        row = await sink.get("tenant-7", message.id)
        assert row is not None
        assert row["value"] == {"event_id": message.id, "lines": [{"quantity": 1}], "total": "9.5"}

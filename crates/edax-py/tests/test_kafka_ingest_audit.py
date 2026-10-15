"""Healthy-partition admission and native mapper validation regressions."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from aiokafka import TopicPartition
from edax.delivery import DeadLetter
from edax.errors import BadHandle, CodecError
from edax.ingestion import MappedRow
from edax.message import EventMessage
from test_kafka_ingest import (
    TP,
    BatchClient,
    EventIds,
    KafkaIngestion,
    RecordingSink,
    events,
    mapper,
    no_dead,
    records,
    running,
    settings,
    until,
)


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize("capacity", ["records", "bytes"])
async def test_busy_healthy_partitions_share_admission(concurrency: int, capacity: str) -> None:
    messages = [event for partition in range(5) for event in events(100, partition)]
    client = BatchClient(
        [record for partition in range(5) for record in records(events(100, partition), partition)]
    )
    sink = RecordingSink()
    runner = KafkaIngestion(
        client,
        sink,
        mapper,
        no_dead,
        settings=settings(
            max_records=1,
            retained_records=2 if capacity == "records" else 20,
            retained_bytes=131072 if capacity == "records" else 60000,
            concurrency=concurrency,
        ),
        serializer=EventIds(messages),
    )
    async with running(runner):
        await until(lambda: len(sink.batches) >= 20)
        for partition in range(5):
            assert any(batch.get(TopicPartition("orders", partition), 0) > 0 for batch in client.commits)
            assert await sink.get("tenant-7", f"order-{partition}") is not None
        assert runner.peak_records <= runner.settings.retained_records
        assert runner.peak_bytes <= runner.settings.retained_bytes


async def test_native_mapped_value_refusal_is_published_before_healthy_progress() -> None:
    messages = events(2)
    client = BatchClient(records(messages))
    sink = RecordingSink()
    dead: list[DeadLetter[Any]] = []
    mapped_calls: list[str] = []

    def mapped(event: EventMessage) -> MappedRow:
        mapped_calls.append(event.id)
        value: Any = {"status": "paid", "event_id": event.id}
        if event.id == messages[0].id:
            for _ in range(130):
                value = [value]
        return MappedRow(event.aggregate_id, value)

    async def publish(letter: DeadLetter[Any]) -> None:
        assert client.commits == []
        dead.append(letter)

    runner = KafkaIngestion(
        client,
        sink,
        mapped,
        publish,
        settings=settings(max_records=2, max_attempts=1),
        serializer=EventIds(messages),
    )
    async with running(runner) as task:
        await until(lambda: task.done() or client.commits == [{TP: 2}])
        if task.done():
            await task
    assert mapped_calls == [message.id for message in messages]
    assert len(dead) == 1 and isinstance(dead[0].failure, CodecError)
    assert dead[0].origin.offset == 0
    assert client.commits == [{TP: 2}]
    assert await sink.pending() == {}
    stored = await sink.get("tenant-7", "order-0")
    assert stored is not None and stored["sequence"] == 2


async def test_native_invalid_row_identity_still_stops_ingestion() -> None:
    messages = events(1)
    client = BatchClient(records(messages))
    sink = RecordingSink()
    runner = KafkaIngestion(
        client,
        sink,
        lambda event: MappedRow("", {"status": "paid"}),
        no_dead,
        settings=settings(max_records=1),
        serializer=EventIds(messages),
    )
    async with running(runner) as task:
        await until(task.done)
        with pytest.raises(BadHandle, match="row identity"):
            await task
    assert client.commits == []
    assert await sink.pending() == {}


@pytest.mark.parametrize("completes_before_drain", [True, False], ids=["completes", "drain_expires"])
async def test_revoke_waits_for_client_commit_after_commit_deadline(completes_before_drain: bool) -> None:
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class HeldCommit(BatchClient):
        async def commit(self, offsets: dict[TopicPartition, int]) -> None:
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
            await super().commit(offsets)

    messages = events(1)
    client = HeldCommit(records(messages))
    sink = RecordingSink()
    runner = KafkaIngestion(
        client,
        sink,
        mapper,
        no_dead,
        settings=settings(max_records=1),
        serializer=EventIds(messages),
        commit_timeout_s=0.01,
        drain_timeout_s=0.1,
    )
    await runner._assign({TP})
    part = runner._parts[TP]
    runner._admit(TP, part, client.logs[TP][0])
    await sink.commit(part.items)
    part.bookings[0].ack()
    revoking = asyncio.create_task(runner.rebalance_listener().on_partitions_revoked({TP}))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(revoking), 0.03)
        assert not cancelled.is_set()
        if completes_before_drain:
            release.set()
            await asyncio.wait_for(revoking, 1)
            assert runner._failure is None
            await runner.rebalance_listener().on_partitions_assigned({TP})
            assert TP in runner._parts
        else:
            await asyncio.wait_for(revoking, 1)
            with pytest.raises(BadHandle, match="revoke deadline"):
                await runner.rebalance_listener().on_partitions_assigned({TP})
            assert runner._offsets.unfinished() is not None
            assert client.commits == []
        assert not cancelled.is_set()
    finally:
        release.set()
        await asyncio.gather(revoking, return_exceptions=True)
        await asyncio.wait_for(runner.wait_closed(), 1)
    assert client.commits == [{TP: 1}]
    assert runner.retained_records == runner.retained_bytes == 0
    if not completes_before_drain:
        with pytest.raises(BadHandle, match="revoke deadline"):
            await runner.rebalance_listener().on_partitions_assigned({TP})

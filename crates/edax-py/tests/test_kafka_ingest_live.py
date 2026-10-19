"""Kafka to PostgreSQL ingestion with real broker offsets and durable sink state.

Set both EDAX_KAFKA_BOOTSTRAP and EDAX_POSTGRES_URL. Every test owns a unique topic and schema;
cleanup deletes only those resources. No external broker or database is provisioned by this module.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AsyncExitStack
from typing import Any
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from conftest import build_message
from edax.backend.ingest_postgres import PostgresIngestionSink, reference_ingestion_ddl
from edax.backend.kafka import KafkaProducer
from edax.backend.kafka_ingest import KafkaIngestion
from edax.backend.postgres import PostgresTransactor
from edax.batch import BatchSettings
from edax.delivery import DeadLetter
from edax.ingestion import IngestionTarget, MappedRow
from edax.message import EventMessage

BOOTSTRAP = os.environ.get("EDAX_KAFKA_BOOTSTRAP")
POSTGRES_URL = os.environ.get("EDAX_POSTGRES_URL")
if not BOOTSTRAP or not POSTGRES_URL:
    pytest.skip(
        "both EDAX_KAFKA_BOOTSTRAP and EDAX_POSTGRES_URL are required for live ingestion",
        allow_module_level=True,
    )

asyncpg = pytest.importorskip("asyncpg")


@pytest.fixture
async def resources() -> AsyncIterator[tuple[str, Any, str, AIOKafkaProducer]]:
    assert BOOTSTRAP is not None and POSTGRES_URL is not None
    suffix = uuid4().hex
    topic, schema = f"edax-ingest-{suffix}", f"ingest_live_{suffix}"
    async with AsyncExitStack() as cleanup:
        admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        await admin.start()
        cleanup.push_async_callback(admin.close)
        await admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        cleanup.push_async_callback(admin.delete_topics, [topic])

        pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=5)
        cleanup.push_async_callback(pool.close)
        await pool.execute(f'CREATE SCHEMA "{schema}"')
        cleanup.push_async_callback(pool.execute, f'DROP SCHEMA "{schema}" CASCADE')
        await pool.execute(reference_ingestion_ddl(schema))

        producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, enable_idempotence=True)
        await producer.start()
        cleanup.push_async_callback(producer.stop)
        yield topic, pool, schema, producer


class CommittedConsumer(AIOKafkaConsumer):
    """Record commits completed by this client, so an existing group offset cannot fake replay."""

    def __init__(self, group: str) -> None:
        super().__init__(
            bootstrap_servers=BOOTSTRAP,
            group_id=group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self.completed: list[dict[TopicPartition, int]] = []

    async def commit(self, offsets: Any = None) -> None:
        await super().commit(offsets)
        self.completed.append(dict(offsets or {}))


def map_order(event: EventMessage) -> MappedRow:
    state = json.loads(event.data)
    return MappedRow(
        event.aggregate_id,
        {
            "id": event.aggregate_id,
            "status": state["status"],
            "sequence": event.sequence,
        },
    )


async def ingest_through(
    topic: str,
    group: str,
    sink: PostgresIngestionSink,
    start: int,
    end: int,
    publish: Callable[[DeadLetter[Any]], Awaitable[None]],
) -> None:
    client = CommittedConsumer(group)
    tp = TopicPartition(topic, 0)
    client.assign([tp])
    await client.start()
    client.seek(tp, start)
    runner = KafkaIngestion(client, sink, map_order, publish, settings=BatchSettings(max_records=20))
    stop = asyncio.Event()
    task = asyncio.create_task(runner.run(stop))
    try:

        async def wait_for_commit() -> None:
            while not any(offsets.get(tp) == end for offsets in client.completed):
                if task.done():
                    await task
                    raise AssertionError("runner exited before committing the requested frontier")
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_commit(), 15)
        assert await client.committed(tp) == end
        stop.set()
        await asyncio.wait_for(task, 5)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        try:
            await client.stop()
        finally:
            await asyncio.wait_for(runner.wait_closed(), 5)
    assert runner.retained_records == runner.retained_bytes == 0


async def test_live_upsert_replay_and_durable_conflict_rejection(
    resources: tuple[str, Any, str, AIOKafkaProducer],
) -> None:
    topic, pool, schema, client = resources
    group = f"edax-ingest-group-{uuid4().hex}"
    sink = PostgresIngestionSink(
        PostgresTransactor(pool, schema=schema),
        IngestionTarget("upsert", "order-view-v1", f"test-cluster:{topic}", "order-state-v1"),
        schema=schema,
    )
    producer = KafkaProducer(client, topic)
    dead: list[DeadLetter[Any]] = []

    async def publish(letter: DeadLetter[Any]) -> None:
        dead.append(letter)

    for sequence in range(1, 21):
        await producer.publish(
            build_message(
                "order-42",
                "OrderState",
                sequence,
                id=f"order-42-{sequence}",
                tenant="tenant-7",
                data=b'{"status":"paid"}',
            )
        )
    await ingest_through(topic, group, sink, 0, 20, publish)
    first = await sink.get("tenant-7", "order-42")
    assert first is not None
    assert first["sequence"] == 20
    assert first["private"] == {"id": "order-42", "status": "paid", "sequence": 20}
    assert dead == []

    # A new client seeks behind the committed group offset. Require a fresh commit from that client.
    await ingest_through(topic, group, sink, 0, 20, publish)
    assert await sink.get("tenant-7", "order-42") == first
    assert await sink.pending() == {}
    assert dead == []

    await producer.publish(
        build_message(
            "order-42",
            "OrderState",
            20,
            id="conflicting-order-42-20",
            tenant="tenant-7",
            data=b'{"status":"cancelled"}',
        )
    )
    await ingest_through(topic, group, sink, 20, 21, publish)
    assert await sink.get("tenant-7", "order-42") == first
    assert len(dead) == 1
    assert await sink.pending() == {}
    source = sink.target.key(["source", sink.target.source_incarnation, topic, 0, "20"])
    rejection_text = await pool.fetchval(
        f'SELECT value::text FROM "{schema}".edax_ingest_rejections WHERE key = $1',
        source,
    )
    rejection = json.loads(rejection_text)
    assert rejection["published"] is True
    assert rejection["error"].startswith("rejected: ")
    assert json.loads(rejection["raw"])["offset"] == 20


async def test_live_group_rebalance_and_long_sink_wait_preserve_progress(
    resources: tuple[str, Any, str, AIOKafkaProducer],
) -> None:
    from edax.ingestion import BatchInput, BatchResult, MemoryIngestionSink

    topic, _, _, raw_producer = resources
    entered = asyncio.Event()

    class SlowFirstSink(MemoryIngestionSink):
        first = True

        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            if self.first:
                self.first = False
                entered.set()
                # A real aiokafka member leaves after this idle interval unless the runner polls.
                await asyncio.sleep(2.5)
            return await super().commit(inputs)

    sink = SlowFirstSink(IngestionTarget("append", "group-view-v1", topic, "order-state-v1"))
    producer = KafkaProducer(raw_producer, topic)
    for index in range(12):
        await producer.publish(
            build_message(
                f"order-{index}",
                "OrderState",
                1,
                id=f"order-{index}-paid",
                tenant="tenant-7",
                data=b'{"status":"paid"}',
            )
        )
    group = f"edax-ingest-rebalance-{uuid4().hex}"
    clients: list[AIOKafkaConsumer] = []
    runners: list[KafkaIngestion] = []
    tasks: list[asyncio.Task[None]] = []
    stops: list[asyncio.Event] = []
    dead: list[DeadLetter[Any]] = []

    async def publish(letter: DeadLetter[Any]) -> None:
        dead.append(letter)

    async def member() -> None:
        client = AIOKafkaConsumer(
            bootstrap_servers=BOOTSTRAP,
            group_id=group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_interval_ms=1000,
            session_timeout_ms=6000,
            heartbeat_interval_ms=200,
        )
        run = KafkaIngestion(client, sink, map_order, publish, settings=BatchSettings(max_records=2))
        client.subscribe([topic], listener=run.rebalance_listener())
        clients.append(client)
        runners.append(run)
        await client.start()
        stop = asyncio.Event()
        stops.append(stop)
        tasks.append(asyncio.create_task(run.run(stop)))

    tp = TopicPartition(topic, 0)
    try:
        await member()
        await asyncio.wait_for(entered.wait(), 10)
        # Stay idle longer than max_poll_interval before inducing a real group rebalance.
        await asyncio.sleep(1.2)
        assert not tasks[0].done() and tp in runners[0]._parts
        await member()

        async def complete() -> None:
            while await clients[0].committed(tp) != 12:
                for task in tasks:
                    if task.done():
                        await task
                        raise AssertionError("runner ended during a routine group rebalance")
                await asyncio.sleep(0.02)

        await asyncio.wait_for(complete(), 20)
        assert dead == []
        for index in range(12):
            assert await sink.get("tenant-7", f"order-{index}-paid") is not None
        assert all(run._failure is None for run in runners)
    finally:
        for stop in stops:
            stop.set()
        if tasks:
            await asyncio.wait(tasks, timeout=5)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for client in clients:
            await client.stop()
        for run in runners:
            await asyncio.wait_for(run.wait_closed(), 5)

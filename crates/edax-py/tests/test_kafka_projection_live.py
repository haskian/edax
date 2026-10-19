"""Real Kafka identity/retention boundaries with a PostgreSQL-authoritative projection checkpoint.

Requires EDAX_KAFKA_BOOTSTRAP, EDAX_KAFKA_CONTAINER and EDAX_POSTGRES_URL. The named
container must expose Kafka's public tools and reach its advertised bootstrap address.
Each case owns its topic and schema. No broker rollback or unsafe election is performed:
that controlled deployment assumption supplies the history proof, not an invented UUID.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from dataclasses import dataclass, replace
from typing import Literal
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import OffsetOutOfRangeError
from edax import BadHandle, _json
from edax.backend.kafka_projection import KafkaProjection
from edax.backend.postgres import PostgresTransactor
from edax.backend.projection_postgres import PostgresProjectionStateStore, reference_projection_state_ddl
from edax.projection import ProjectionTarget, SourceProof
from test_kafka_projection import decode, wire
from test_projection_state import event, reducer

BOOTSTRAP = os.environ.get("EDAX_KAFKA_BOOTSTRAP")
CONTAINER = os.environ.get("EDAX_KAFKA_CONTAINER")
POSTGRES_URL = os.environ.get("EDAX_POSTGRES_URL")
if not BOOTSTRAP or not CONTAINER or not POSTGRES_URL:
    pytest.skip("live projections require Kafka, its CLI container and PostgreSQL", allow_module_level=True)

asyncpg = pytest.importorskip("asyncpg")


async def kafka_tool(tool: str, *arguments: str, stdin: bytes | None = None) -> str:
    assert CONTAINER is not None and BOOTSTRAP is not None
    process = await asyncio.create_subprocess_exec(
        "docker",
        "exec",
        "-i",
        CONTAINER,
        f"/opt/kafka/bin/{tool}.sh",
        *arguments,
        "--bootstrap-server",
        BOOTSTRAP,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(stdin), 20)
    except BaseException:
        process.kill()
        await process.wait()
        raise
    assert process.returncode == 0, stderr.decode()
    return stdout.decode()


class VerifiedSource:
    def __init__(self, client: AIOKafkaConsumer, history: str) -> None:
        self.client, self.history = client, history

    async def verify(self, partition: TopicPartition) -> SourceProof:
        cluster, topic = await asyncio.gather(
            kafka_tool("kafka-cluster", "cluster-id"),
            kafka_tool("kafka-topics", "--describe", "--topic", partition.topic),
        )
        cluster_match = re.search(r"Cluster ID: ([A-Za-z0-9_-]+)", cluster)
        topic_match = re.search(r"TopicId: ([A-Za-z0-9_-]+)", topic)
        assert cluster_match and topic_match, (cluster, topic)
        identity = f"{cluster_match[1]}/{topic_match[1]}"
        # This test uses read_uncommitted and nontransactional records, so the high watermark
        # is readable. A read_committed deployment must instead prove its last stable offset.
        beginning, end = await asyncio.gather(
            self.client.beginning_offsets([partition]), self.client.end_offsets([partition])
        )
        return SourceProof(identity, self.history, beginning[partition], end[partition])


@dataclass
class Resources:
    topic: str
    admin: AIOKafkaAdminClient
    producer: AIOKafkaProducer
    client: AIOKafkaConsumer
    source: VerifiedSource
    store: PostgresProjectionStateStore


@pytest.fixture
async def resources() -> AsyncIterator[Resources]:
    assert BOOTSTRAP is not None and POSTGRES_URL is not None
    suffix = uuid4().hex
    topic, schema = f"edax-projection-{suffix}", f"projection_live_{suffix}"
    async with AsyncExitStack() as cleanup:
        admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        await admin.start()
        cleanup.push_async_callback(admin.close)
        await admin.create_topics(
            [
                NewTopic(
                    topic,
                    num_partitions=1,
                    replication_factor=1,
                    topic_configs={"cleanup.policy": "delete", "unclean.leader.election.enable": "false"},
                )
            ]
        )
        cleanup.push_async_callback(admin.delete_topics, [topic])
        producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, enable_idempotence=True)
        await producer.start()
        cleanup.push_async_callback(producer.stop)
        client = AIOKafkaConsumer(
            bootstrap_servers=BOOTSTRAP,
            group_id=f"projection-{suffix}",
            enable_auto_commit=False,
            auto_offset_reset="none",
            isolation_level="read_uncommitted",
        )
        client.assign([TopicPartition(topic, 0)])
        await client.start()
        cleanup.push_async_callback(client.stop)
        source = VerifiedSource(client, f"controlled-no-rollback-{suffix}")
        proof = await source.verify(TopicPartition(topic, 0))
        target = ProjectionTarget(
            "balance-v1",
            proof.identity,
            proof.history,
            topic,
            "balance-reducer-1",
            "envelope-1",
            "account-key-1",
            "receipt-effect-1",
            {0: 0},
        )
        pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=3)
        cleanup.push_async_callback(pool.close)
        await pool.execute(f'CREATE SCHEMA "{schema}"')
        cleanup.push_async_callback(pool.execute, f'DROP SCHEMA "{schema}" CASCADE')
        await pool.execute(reference_projection_state_ddl(schema))
        store = PostgresProjectionStateStore(PostgresTransactor(pool, schema=schema), target, schema=schema)
        await store.initialize()
        yield Resources(topic, admin, producer, client, source, store)


@pytest.mark.parametrize("discontinuity", ["retention", "topic_recreation"])
async def test_real_source_loss_cannot_advance_database_checkpoint(
    resources: Resources, discontinuity: Literal["retention", "topic_recreation"]
) -> None:
    topic, client, store = resources.topic, resources.client, resources.store
    partition = TopicPartition(topic, 0)
    runner = KafkaProjection(
        client, store, resources.source, decode, reducer, operation_timeout_s=30, lease_ms=90_000
    )
    listener = runner.rebalance_listener()
    await listener.on_partitions_assigned({partition})
    assigned = runner._parts[partition]
    try:
        # Idle passes must not continually discard the real client's prefetch buffer.
        for _ in range(2):
            await runner._partition(partition, assigned)
        assert assigned.lease is not None and assigned.lease.next_offset == 0
        for sequence in range(1, 4):
            await resources.producer.send_and_wait(
                topic, wire(event(sequence, f"credit-{sequence}")), partition=0
            )

        async def consume() -> None:
            while assigned.lease is None or assigned.lease.next_offset < 3:
                await runner._partition(partition, assigned)
                await asyncio.sleep(0.01)

        await asyncio.wait_for(consume(), 60)
        assert assigned.lease is not None
        assert (await store.recover(assigned.lease)).next_offset == 3
        state = await store.get("bank-eu", "account-42")
        assert state is not None and state["public"]["balance_minor"] == 3750
        assert await client.committed(partition) is None, "database progress needs no broker commit"
        original_identity = (await resources.source.verify(partition)).identity

        # An unrelated aggregate has no prior sequence to expose its complete loss.
        await resources.producer.send_and_wait(
            topic, wire(replace(event(), aggregate="account-43")), partition=0
        )
        if discontinuity == "retention":
            deleted = await kafka_tool(
                "kafka-delete-records",
                "--offset-json-file",
                "/dev/stdin",
                stdin=_json.encode(
                    {"partitions": [{"topic": topic, "partition": 0, "offset": 4}], "version": 1}
                ).encode(),
            )
            assert "low_watermark: 4" in deleted
            proof = await resources.source.verify(partition)
            assert proof.identity == original_identity and proof.beginning == 4
            expected = "history is missing"
        else:
            await resources.admin.delete_topics([topic])

            async def deleted_topic() -> None:
                while topic in await resources.admin.list_topics():
                    await asyncio.sleep(0.05)

            await asyncio.wait_for(deleted_topic(), 20)
            await resources.admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
            assert (await resources.source.verify(partition)).identity != original_identity
            expected = "continuity is unproven"

        refused = (BadHandle, OffsetOutOfRangeError) if discontinuity == "topic_recreation" else BadHandle
        with pytest.raises(refused) as failure:
            await runner._partition(partition, assigned)
        if isinstance(failure.value, OffsetOutOfRangeError):
            # The old client's prefetched position can fail before the next identity proof.
            # Only the recreated partition's out-of-range fetch is an equivalent refusal.
            offsets = failure.value.args[0]
            assert set(offsets) == {partition} and offsets[partition] >= 3
        else:
            assert expected in str(failure.value)
        assert (await store.recover(assigned.lease)).next_offset == 3
        assert await store.get("bank-eu", "account-42") == state
        assert await store.get("bank-eu", "account-43") is None
        assert runner.retained_records == runner.retained_bytes == 0
    finally:
        await listener.on_partitions_revoked({partition})
        await runner.wait_closed()

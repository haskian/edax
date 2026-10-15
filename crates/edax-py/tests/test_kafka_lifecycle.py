"""Deterministic fetch/commit interleavings across assignment and rewind boundaries."""

from __future__ import annotations

import asyncio

import edax
import pytest
from aiokafka import TopicPartition
from conftest import (
    FakeKafkaClient,
    FakeKafkaCluster,
    IdSerializer,
    KafkaMsg,
    build_message,
    delivered,
    publish_all,
)
from edax.backend.kafka import KafkaConsumer

TP = TopicPartition("t", 0)


class HeldCommit(FakeKafkaClient):
    def __init__(self, *, ignore_cancel: bool = False) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.calls = 0
        self.ignore_cancel = ignore_cancel

    async def commit(self, offsets: dict[TopicPartition, int]) -> None:
        self.calls += 1
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            if not self.ignore_cancel:
                raise
            await self.release.wait()
        await super().commit(offsets)


class HeldFetch(FakeKafkaClient):
    def __init__(self) -> None:
        super().__init__()
        self.hold = False
        self.fetched = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def getone(self) -> KafkaMsg:
        record = await super().getone()
        if self.hold:
            self.hold = False
            self.fetched.set()
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled.set()
                await self.release.wait()
        return record


async def seed(client: FakeKafkaClient, count: int = 3) -> None:
    await publish_all(
        client,
        "t",
        IdSerializer(),
        [build_message("account-42", "BalanceReplaced", i + 1, id=str(i)) for i in range(count)],
    )


async def test_revoke_retires_deliveries_before_waiting_for_an_older_commit() -> None:
    client = HeldCommit()
    await seed(client)
    consumer = KafkaConsumer(client, serializer=IdSerializer())
    first, second = [delivered(await consumer.poll()) for _ in range(2)]
    await first.ack()
    flush = asyncio.create_task(consumer.flush())
    await client.started.wait()
    revoke = asyncio.create_task(consumer.rebalance_listener().on_partitions_revoked([TP]))
    await asyncio.sleep(0)
    assert not second.live(), "retirement waited behind the suspended commit"
    await second.ack()
    client.release.set()
    await asyncio.wait_for(asyncio.gather(flush, revoke), 1)
    assert client.commits == [1, 1]
    assert consumer.stats() == {}


async def test_old_commit_completion_does_not_update_a_reassigned_partition() -> None:
    client = HeldCommit()
    await seed(client)
    consumer = KafkaConsumer(client, serializer=IdSerializer())
    await delivered(await consumer.poll()).ack()
    flush = asyncio.create_task(consumer.flush())
    await client.started.wait()
    consumer.reset(TP)
    client.seek(TP, 0)
    replay = delivered(await consumer.poll())
    client.release.set()
    await flush
    assert consumer.stats()[TP].committed is None
    await replay.ack()
    await consumer.flush()
    assert client.commits == [1, 1], "old completion suppressed the new generation's commit"


async def test_idle_revoke_allows_the_fetch_needed_to_rejoin() -> None:
    class IdleClient(FakeKafkaClient):
        def __init__(self) -> None:
            super().__init__()
            self.needs_rejoin = False
            self.fetch_resumed = asyncio.Event()
            self.assigned = asyncio.Event()

        async def getone(self) -> KafkaMsg:
            if self.needs_rejoin:
                self.fetch_resumed.set()
                await self.assigned.wait()
            return await super().getone()

    client = IdleClient()
    await seed(client)
    consumer = KafkaConsumer(client, serializer=IdSerializer())
    stale = delivered(await consumer.poll())
    listener = consumer.rebalance_listener()
    client.needs_rejoin = True
    await listener.on_partitions_revoked([TP])
    assert not stale.live()

    async def rejoin_after_fetch() -> None:
        # aiokafka defers rejoining an idle member until a fetch resets its idle time.
        await client.fetch_resumed.wait()
        await listener.on_partitions_assigned([TP])
        client.seek(TP, 0)
        client.needs_rejoin = False
        client.assigned.set()

    rejoining = asyncio.create_task(rejoin_after_fetch())
    polling = asyncio.create_task(consumer.poll())
    try:
        await asyncio.wait_for(asyncio.gather(rejoining, polling), 1)
        replay = delivered(polling.result())
        assert replay.message.id == "0"
        await stale.ack()
        await consumer.flush()
        assert client.commits == []
        await replay.ack()
        await consumer.flush()
        assert client.commits == [1]
    finally:
        for task in (rejoining, polling):
            task.cancel()
        await asyncio.gather(rejoining, polling, return_exceptions=True)


async def test_revoke_deadline_keeps_the_client_commit_owned() -> None:
    client = HeldCommit(ignore_cancel=True)
    await seed(client)
    consumer = KafkaConsumer(client, serializer=IdSerializer(), commit_timeout_s=0.01)
    first, second = [delivered(await consumer.poll()) for _ in range(2)]
    await first.ack()
    flush = asyncio.create_task(consumer.flush())
    await client.started.wait()
    await asyncio.wait_for(consumer.rebalance_listener(0.005).on_partitions_revoked([TP]), 0.2)
    assert not second.live()
    with pytest.raises(edax.StoreError):
        await flush
    assert not client.cancelled.is_set()
    with pytest.raises(edax.BadHandle):
        await consumer.flush()
    with pytest.raises(edax.BadHandle):
        await consumer.poll()
    assert client.calls == 1, "a second commit started while the first remained unresolved"
    client.release.set()
    pending = consumer._offsets._pending
    assert pending is not None
    await pending
    assert consumer.stats() == {}


async def test_late_fetch_after_rewind_refetches_the_entire_unsettled_prefix() -> None:
    client = HeldFetch()
    await seed(client)
    consumer = KafkaConsumer(client, serializer=IdSerializer())
    first = delivered(await consumer.poll())
    client.hold = True
    polling = asyncio.create_task(consumer.poll())
    await client.fetched.wait()  # offset 1 was consumed by the client but not booked
    await first.nack()
    await client.cancelled.wait()
    client.release.set()
    replay = delivered(await asyncio.wait_for(polling, 1))
    assert replay.message.id == "0", "the discarded offset 1 was mistaken for the safe frontier"
    assert not first.live()
    await replay.ack()
    for expected in ("1", "2"):
        delivery = delivered(await consumer.poll())
        assert delivery.message.id == expected
        await delivery.ack()
    await consumer.flush()
    assert client.commits == [3]


async def test_fetch_that_outlives_revoke_cannot_book_under_a_new_assignment() -> None:
    client = HeldFetch()
    await seed(client)
    consumer = KafkaConsumer(client, serializer=IdSerializer())
    client.hold = True
    polling = asyncio.create_task(consumer.poll())
    await client.fetched.wait()
    listener = consumer.rebalance_listener(0.005)
    await asyncio.wait_for(listener.on_partitions_revoked([TP]), 0.2)
    with pytest.raises(edax.BadHandle):
        await listener.on_partitions_assigned([TP])
    client.release.set()
    with pytest.raises(edax.BadHandle):
        await asyncio.wait_for(polling, 1)
    assert consumer.stats() == {}
    assert client.commits == []


async def test_assignment_without_revoke_cannot_skip_a_suspended_fetch() -> None:
    client = HeldFetch()
    await seed(client)
    consumer = KafkaConsumer(client, serializer=IdSerializer())
    client.hold = True
    polling = asyncio.create_task(consumer.poll())
    await client.fetched.wait()
    with pytest.raises(edax.BadHandle, match="without revoke"):
        await consumer.rebalance_listener().on_partitions_assigned([TP])
    client.release.set()
    with pytest.raises(edax.BadHandle):
        await asyncio.wait_for(polling, 1)
    with pytest.raises(edax.BadHandle):
        await consumer.flush()
    assert consumer.stats() == {}
    assert client.commits == [], "an unhandled offset must never disappear behind a later commit"


async def test_cancelling_revoke_cancels_its_queued_commit() -> None:
    client = HeldCommit()
    await seed(client)
    consumer = KafkaConsumer(client, serializer=IdSerializer())
    first, second, third = [delivered(await consumer.poll()) for _ in range(3)]
    await first.ack()
    flush = asyncio.create_task(consumer.flush())
    await client.started.wait()
    await second.ack()
    listener = consumer.rebalance_listener()
    revoke = asyncio.create_task(listener.on_partitions_revoked([TP]))
    while third.live():
        await asyncio.sleep(0)
    revoke.cancel()
    with pytest.raises(asyncio.CancelledError):
        await revoke
    with pytest.raises(edax.BadHandle):
        await listener.on_partitions_assigned([TP])
    client.release.set()
    await flush
    await asyncio.sleep(0)
    assert client.calls == 1, "the cancelled revoke issued its queued watermark after the callback ended"
    assert client.commits == [1]


async def test_commit_timeout_waits_for_the_original_call_before_resuming() -> None:
    client = HeldCommit()
    await seed(client)
    consumer = KafkaConsumer(client, serializer=IdSerializer(), commit_timeout_s=0.001)
    await delivered(await consumer.poll()).ack()
    with pytest.raises(edax.StoreError):
        await consumer.flush()
    with pytest.raises(edax.BadHandle, match="commit still running"):
        await consumer.flush()
    assert not client.cancelled.is_set()
    client.release.set()
    pending = consumer._offsets.unfinished()
    assert pending is not None
    await pending
    await consumer.flush()
    assert client.calls == 1
    assert client.commits == [1]
    assert delivered(await consumer.poll()).message.id == "1"


async def test_repeated_poll_cancellation_cannot_reopen_admission_with_a_live_fetch() -> None:
    client = HeldFetch()
    await seed(client)
    consumer = KafkaConsumer(client, serializer=IdSerializer())
    client.hold = True
    polling = asyncio.create_task(consumer.poll())
    await client.fetched.wait()
    try:
        polling.cancel()
        # The second cancellation interrupts the first cancellation's cleanup yield.
        asyncio.get_running_loop().call_soon(polling.cancel)
        with pytest.raises(asyncio.CancelledError):
            await polling
        await client.cancelled.wait()
        with pytest.raises(edax.BadHandle, match="poll cancelled"):
            await consumer.poll()
        assert client.commits == []
    finally:
        client.release.set()
        if consumer._fetch_task is not None:
            await asyncio.wait_for(consumer._fetch_task, 1)
    assert client.commits == []


@pytest.mark.parametrize("previous", [(TP,), ()], ids=["gained-partition", "empty-assignment"])
async def test_revoke_cycle_allows_a_new_partition_during_a_waiting_fetch(
    previous: tuple[TopicPartition, ...],
) -> None:
    class WaitingClient(FakeKafkaClient):
        def __init__(self) -> None:
            super().__init__(FakeKafkaCluster(2))
            self.waiting = asyncio.Event()
            self.assigned = asyncio.Event()
            self.hold = False

        async def getone(self) -> KafkaMsg:
            if self.hold:
                self.waiting.set()
                await self.assigned.wait()
            return await super().getone()

    source = FakeKafkaClient()
    await seed(source)
    client = WaitingClient()
    gained = TopicPartition("t", 1)
    client.cluster.logs[("t", 0)] = list(source.log)
    client.cluster.logs[("t", 1)] = [row._replace(partition=1) for row in source.log]
    client.assign(previous)
    consumer = KafkaConsumer(client, serializer=IdSerializer())
    listener = consumer.rebalance_listener()
    await listener.on_partitions_assigned(previous)
    stale = delivered(await consumer.poll()) if previous else None
    await listener.on_partitions_revoked(previous)
    client.assign([])
    client.hold = True
    polling = asyncio.create_task(consumer.poll())
    try:
        await asyncio.wait_for(client.waiting.wait(), 1)
        client.assign([gained])
        await listener.on_partitions_assigned([gained])
        client.assigned.set()
        fresh = delivered(await asyncio.wait_for(polling, 1))
        assert fresh.origin.partition == gained.partition
        assert fresh.message.id == "0"
        if stale is not None:
            assert not stale.live()
            await stale.ack()
        await consumer.flush()
        assert client.cluster.committed == {}
        await fresh.ack()
        await consumer.flush()
        assert client.cluster.committed == {gained: 1}
    finally:
        polling.cancel()
        await asyncio.gather(polling, return_exceptions=True)


async def test_cancelled_assignment_stops_admission_after_its_fetch_finishes() -> None:
    client = HeldFetch()
    await seed(client)
    consumer = KafkaConsumer(client, serializer=IdSerializer())
    listener = consumer.rebalance_listener()
    await listener.on_partitions_revoked([TP])
    client.hold = True
    polling = asyncio.create_task(consumer.poll())
    assigning: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(client.fetched.wait(), 1)
        assigning = asyncio.create_task(listener.on_partitions_assigned([TP]))
        await asyncio.wait_for(client.cancelled.wait(), 1)
        assigning.cancel()
        with pytest.raises(asyncio.CancelledError):
            await assigning
        with pytest.raises(edax.BadHandle, match="assignment interrupted"):
            await consumer.flush()
        client.release.set()
        with pytest.raises(edax.BadHandle, match="assignment interrupted"):
            await asyncio.wait_for(polling, 1)
        with pytest.raises(edax.BadHandle, match="assignment interrupted"):
            await listener.on_partitions_assigned([TP])
        assert consumer.stats() == {}
        assert client.commits == []
    finally:
        client.release.set()
        for task in (polling, assigning):
            if task is not None:
                task.cancel()
        await asyncio.gather(polling, *([assigning] if assigning is not None else []), return_exceptions=True)

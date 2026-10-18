"""A client commit owns its lifetime even when aiokafka shields the broker request."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import edax
import pytest
from aiokafka import TopicPartition
from aiokafka.consumer.group_coordinator import GroupCoordinator
from conftest import FakeKafkaClient, IdSerializer, build_message, delivered, publish_all
from edax.backend.kafka import KafkaConsumer

TP = TopicPartition("t", 0)


@pytest.mark.parametrize("interrupt", ["deadline", "caller-cancel"])
@pytest.mark.parametrize("failed_reply", [False, True], ids=["late-success", "late-failure"])
async def test_client_commit_outlives_its_waiter_without_overlapping_another_commit(
    interrupt: str,
    failed_reply: bool,
) -> None:
    class Coordinator:
        def __init__(self) -> None:
            self._commit_lock = asyncio.Lock()
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.finished = asyncio.Event()
            self.calls = 0
            self.client_cancelled = False

        async def ensure_coordinator_known(self) -> None:
            pass

        async def _do_commit_offsets(self, assignment: Any, offsets: dict[TopicPartition, int]) -> None:
            self.calls += 1
            self.started.set()
            try:
                await self.release.wait()
                if failed_reply and self.calls == 1:
                    raise RuntimeError("late commit failure")
                await FakeKafkaClient.commit(client, offsets)
            finally:
                self.finished.set()

    class Client(FakeKafkaClient):
        async def commit(self, offsets: dict[TopicPartition, int]) -> None:
            try:
                # Exercise aiokafka's real commit lock and shield. Only the network call is held.
                await GroupCoordinator.commit_offsets(cast(GroupCoordinator, coordinator), None, offsets)
            except asyncio.CancelledError:
                coordinator.client_cancelled = True
                raise

    coordinator = Coordinator()
    client = Client()
    await publish_all(client, "t", IdSerializer(), [build_message("account-42", "BalanceReplaced", 1)])
    consumer = KafkaConsumer(client, serializer=IdSerializer(), commit_timeout_s=0.01)
    await delivered(await consumer.poll()).ack()
    flush = asyncio.create_task(consumer.flush())
    await coordinator.started.wait()
    try:
        if interrupt == "caller-cancel":
            flush.cancel()
            with pytest.raises(asyncio.CancelledError):
                await flush
        else:
            with pytest.raises(edax.StoreError):
                await flush
        assert not coordinator.client_cancelled, "cancellation detaches aiokafka's shielded request"
        with pytest.raises(edax.StoreError, match="commit still running"):
            await consumer.flush()
        with pytest.raises(edax.StoreError, match="commit still running"):
            await consumer.poll()
        assert coordinator.calls == 1
        # A late completion must still carry the generation it started under.
        consumer.reset(TP)
        coordinator.release.set()
        pending = consumer._offsets.unfinished()
        assert pending is not None
        await asyncio.gather(pending, return_exceptions=True)
        assert consumer.stats() == {}
        client.seek(TP, 0)
        await delivered(await consumer.poll()).ack()
        await consumer.flush()
        assert coordinator.calls == 2
        assert client.commits == ([1] if failed_reply else [1, 1])
        assert consumer.stats()[TP].committed == 1
    finally:
        coordinator.release.set()
        await asyncio.wait_for(coordinator.finished.wait(), 1)
        await asyncio.gather(flush, return_exceptions=True)
        pending = consumer._offsets.unfinished()
        if pending is not None:
            await asyncio.gather(pending, return_exceptions=True)


async def test_cancelling_revoke_during_handler_drain_releases_poll_waiters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Draining:
        def __init__(self) -> None:
            self.started = asyncio.Event()

        async def wait_handlers(self, timeout: float) -> int:
            self.started.set()
            await asyncio.Event().wait()
            return 0

    consumer = KafkaConsumer(FakeKafkaClient(), serializer=IdSerializer())
    draining = Draining()
    monkeypatch.setattr(consumer, "_dispatcher", draining)
    revoke = asyncio.create_task(consumer.rebalance_listener().on_partitions_revoked([TP]))
    await draining.started.wait()
    polling = asyncio.create_task(consumer.poll())
    revoke.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await revoke
        with pytest.raises(edax.BadHandle, match="revoke cancelled"):
            await asyncio.wait_for(polling, 0.1)
        with pytest.raises(edax.BadHandle, match="revoke cancelled"):
            await consumer.flush()
    finally:
        polling.cancel()
        await asyncio.gather(polling, return_exceptions=True)


async def test_revoke_deadline_accounts_for_commit_after_its_waiter_times_out() -> None:
    class Client(FakeKafkaClient):
        def __init__(self) -> None:
            super().__init__()
            self.release = asyncio.Event()

        async def commit(self, offsets: dict[TopicPartition, int]) -> None:
            await self.release.wait()
            await super().commit(offsets)

    client = Client()
    await publish_all(client, "t", IdSerializer(), [build_message("account-42", "BalanceReplaced", 1)])
    consumer = KafkaConsumer(client, serializer=IdSerializer(), commit_timeout_s=0.001)
    await delivered(await consumer.poll()).ack()
    listener = consumer.rebalance_listener(0.01)
    try:
        await listener.on_partitions_revoked([TP])
        client.release.set()
        pending = consumer._offsets.unfinished()
        assert pending is not None
        await pending
        # Finishing the late commit must not reopen a consumer whose revoke already overran.
        with pytest.raises(edax.BadHandle, match="revoke deadline"):
            await listener.on_partitions_assigned([TP])
    finally:
        client.release.set()
        pending = consumer._offsets.unfinished()
        if pending is not None:
            await pending

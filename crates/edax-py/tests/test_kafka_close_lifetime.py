"""Cancelling a shutdown waiter never cancels the operations it still owns."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from aiokafka import TopicPartition
from aiokafka.consumer.group_coordinator import GroupCoordinator
from edax.errors import StoreError
from test_kafka_fold import TP as FOLD_TP
from test_kafka_fold import runner as fold_runner
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
    settings,
)


async def test_cancelled_ingestion_close_waiter_preserves_shielded_client_commit() -> None:
    release, finished, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class Coordinator:
        def __init__(self) -> None:
            self._commit_lock = asyncio.Lock()

        async def ensure_coordinator_known(self) -> None:
            pass

        async def _do_commit_offsets(self, assignment: Any, offsets: dict[TopicPartition, int]) -> None:
            try:
                await release.wait()
                await BatchClient.commit(client, offsets)
            finally:
                finished.set()

    class Client(BatchClient):
        async def commit(self, offsets: dict[TopicPartition, int]) -> None:
            try:
                await GroupCoordinator.commit_offsets(coordinator, None, offsets)
            except asyncio.CancelledError:
                cancelled.set()
                raise

    coordinator = Coordinator()
    messages = events(1)
    client, sink = Client(records(messages)), RecordingSink()
    run = KafkaIngestion(
        client,
        sink,
        mapper,
        no_dead,
        settings=settings(max_records=1),
        serializer=EventIds(messages),
        commit_timeout_s=0.001,
    )
    await run._assign({TP})
    part = run._parts[TP]
    run._admit(TP, part, client.logs[TP][0])
    await sink.commit(part.items)
    part.bookings[0].ack()
    try:
        with pytest.raises(StoreError):
            await run._offsets.flush()
        for _ in range(2):
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(run.wait_closed(), 0.01)
            assert not cancelled.is_set()
            assert run._offsets.unfinished() is not None
            assert not finished.is_set()
        assert client.commits == []
    finally:
        release.set()
        await asyncio.wait_for(finished.wait(), 1)
        await asyncio.wait_for(run.wait_closed(), 1)
        run._release(part)
    assert client.commits == [{TP: 1}]
    assert run._offsets.stats()[TP].committed == 1


async def test_cancelled_fold_close_waiter_preserves_unresolved_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, _, _, store = await fold_runner()
    run.timeout = 0.001
    release, cancelled = asyncio.Event(), asyncio.Event()
    original_commit = store.commit
    cancellations = 0

    async def held_commit(*args: Any, **kwargs: Any) -> Any:
        nonlocal cancellations
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancellations += 1
            cancelled.set()
            await release.wait()
        return await original_commit(*args, **kwargs)

    monkeypatch.setattr(store, "commit", held_commit)
    try:
        with pytest.raises(StoreError, match="deadline"):
            await run._partition(FOLD_TP, run._parts[FOLD_TP])
        await asyncio.wait_for(cancelled.wait(), 1)
        for _ in range(2):
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(run.wait_closed(), 0.01)
            assert cancellations == 1
            assert run._operations
            assert run.retained_records == 3
            assert run.retained_bytes > 0
        assert await store.get("bank-eu", "account-42") is None
    finally:
        release.set()
        await asyncio.wait_for(run.wait_closed(), 1)
    assert run.retained_records == run.retained_bytes == 0
    assert await store.get("bank-eu", "account-42") is None

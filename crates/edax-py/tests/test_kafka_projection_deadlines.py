"""Operation deadlines preserve healthy progress and input ownership."""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from aiokafka import TopicPartition
from edax import _json
from edax.backend.kafka_projection import KafkaProjection
from edax.batch import BatchSettings
from edax.errors import StoreError
from edax.projection import MemoryProjectionStateStore, ProjectionEvent
from test_kafka_projection import Client, Source, decode, runner
from test_projection_state import event, proof, reducer, target

BLOCKED = TopicPartition("account-events", 0)
HEALTHY = TopicPartition("account-events", 1)


class PartitionClient:
    def __init__(self) -> None:
        self.cursors = {BLOCKED: 0, HEALTHY: 0}
        self.seeks: list[tuple[Any, int]] = []
        self._auto_offset_reset = "none"

    def assignment(self) -> set[Any]:
        return set(self.cursors)

    def seek(self, tp: Any, offset: int) -> None:
        self.cursors[tp] = offset
        self.seeks.append((tp, offset))

    async def getmany(self, tp: Any, **kwargs: Any) -> dict[Any, Any]:
        if self.cursors[tp]:
            return {tp: []}
        self.cursors[tp] = 1
        value = replace(event(), aggregate=f"account-{tp.partition}")
        return {
            tp: [
                SimpleNamespace(
                    topic=tp.topic,
                    partition=tp.partition,
                    offset=0,
                    key=value.aggregate.encode(),
                    value=_json.encode(value).encode(),
                    headers=[],
                )
            ]
        }


@pytest.mark.parametrize("blocked_operation", ["source", "fetch"])
async def test_cancellation_suppression_does_not_starve_healthy_partition(
    blocked_operation: str,
) -> None:
    entered, cancelled, release, committed = [asyncio.Event() for _ in range(4)]

    async def hold() -> None:
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()

    class SlowSource(Source):
        async def verify(self, tp: Any) -> Any:
            if blocked_operation == "source" and tp == BLOCKED:
                await hold()
            return proof()

    class SlowClient(PartitionClient):
        async def getmany(self, *partitions: Any, **kwargs: Any) -> dict[Any, Any]:
            tp = partitions[0]
            if blocked_operation == "fetch" and tp == BLOCKED:
                await hold()
            return await super().getmany(tp, **kwargs)

    class Store(MemoryProjectionStateStore):
        async def commit(self, *args: Any, **kwargs: Any) -> Any:
            result = await super().commit(*args, **kwargs)
            if result.partition == 1:
                committed.set()
            return result

    store, client = Store(target()), SlowClient()
    await store.initialize()
    run = KafkaProjection(client, store, SlowSource(), decode, reducer, operation_timeout_s=0.02)
    await run.rebalance_listener().on_partitions_assigned([BLOCKED, HEALTHY])
    task = asyncio.create_task(run.run())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.wait_for(cancelled.wait(), 1)
        await asyncio.wait_for(committed.wait(), 1)
        assert not release.is_set()
        assert run._parts[BLOCKED].unresolved is not None
        assert await store.get("bank-eu", "account-0") is None
        saved = await store.get("bank-eu", "account-1")
        assert saved is not None
        assert saved["value"]["balance_minor"] == 1250
        observed_lease = run._parts[HEALTHY].lease
        assert observed_lease is not None
        assert (await store.recover(observed_lease)).next_offset == 1
    finally:
        await run.stop()
        release.set()
        await asyncio.wait_for(run.wait_closed(), 1)
        await asyncio.gather(task, return_exceptions=True)
    assert await store.get("bank-eu", "account-0") is None
    assert run.retained_records == run.retained_bytes == 0


async def test_timed_out_commit_retains_credits_until_operation_releases_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, _, _, store = await runner()
    run.timeout = 0.02
    cancelled, release = asyncio.Event(), asyncio.Event()
    original_commit = store.commit

    async def held_commit(*args: Any, **kwargs: Any) -> Any:
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()
        return await original_commit(*args, **kwargs)

    monkeypatch.setattr(store, "commit", held_commit)
    closing: asyncio.Task[Any] | None = None
    try:
        with pytest.raises(StoreError, match="deadline"):
            await run._partition(BLOCKED, run._parts[BLOCKED])
        await asyncio.wait_for(cancelled.wait(), 1)
        assert run.retained_records == 3
        assert run.retained_bytes > 0
        observed_lease = run._parts[BLOCKED].lease
        assert observed_lease is not None
        assert observed_lease.next_offset == 0
        closing = asyncio.create_task(run.wait_closed())
        await asyncio.sleep(0)
        assert not closing.done()
    finally:
        release.set()
        if closing is not None:
            await asyncio.wait_for(closing, 1)
        else:
            await asyncio.wait_for(run.wait_closed(), 1)
    assert await store.get("bank-eu", "account-42") is None
    assert run.retained_records == run.retained_bytes == 0


async def test_elapsed_flush_age_cuts_before_fetching_another_record() -> None:
    client, store = Client(), MemoryProjectionStateStore(target())
    await store.initialize()
    decoded: list[int] = []

    def small_bounded_decode(record: Any) -> ProjectionEvent:
        decoded.append(record.offset)
        time.sleep(0.003)
        return decode(record)

    run = KafkaProjection(
        client, store, Source(), small_bounded_decode, reducer, settings=BatchSettings(flush_ms=1)
    )
    await run.rebalance_listener().on_partitions_assigned([BLOCKED])
    await run._partition(BLOCKED, run._parts[BLOCKED])
    assert decoded == [0]
    assert client.cursor == 1
    observed_lease = run._parts[BLOCKED].lease
    assert observed_lease is not None
    assert observed_lease.next_offset == 1
    saved = await store.get("bank-eu", "account-42")
    assert saved is not None
    assert saved["value"]["balance_minor"] == 1250
    assert run.retained_records == run.retained_bytes == 0


async def test_reassigned_partition_does_not_overlap_an_unresolved_old_fetch() -> None:
    release_old, old_consumed = asyncio.Event(), asyncio.Event()

    class SharedCursorClient(Client):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0
            for record in self.records:
                record.value = _json.encode(
                    replace(
                        event(event_id=f"credit-{record.offset}"),
                        aggregate=f"independent-{record.offset}",
                    )
                ).encode()

        async def getmany(self, *partitions: Any, **kwargs: Any) -> dict[Any, Any]:
            self.calls += 1
            call = self.calls
            if call == 1:
                try:
                    await release_old.wait()
                except asyncio.CancelledError:
                    await release_old.wait()
            result = await super().getmany(*partitions, **kwargs)
            if call == 1:
                old_consumed.set()
            elif call == 2:
                release_old.set()
                await old_consumed.wait()
            return result

    client, store = SharedCursorClient(), MemoryProjectionStateStore(target())
    await store.initialize()
    run = KafkaProjection(client, store, Source(), decode, reducer, operation_timeout_s=0.02)
    listener = run.rebalance_listener()
    await listener.on_partitions_assigned([BLOCKED])
    try:
        with pytest.raises(StoreError, match="deadline"):
            await run._partition(BLOCKED, run._parts[BLOCKED])
        await listener.on_partitions_revoked([BLOCKED])
        await listener.on_partitions_assigned([BLOCKED])
        # If the old operation remains outstanding, a new batch must not use the shared cursor.
        await run._partition(BLOCKED, run._parts[BLOCKED])
        if client.calls == 1:
            release_old.set()
            await run.wait_closed()
            await run._partition(BLOCKED, run._parts[BLOCKED])
        assert await store.get("bank-eu", "independent-1") is not None
        observed_lease = run._parts[BLOCKED].lease
        assert observed_lease is not None
        assert observed_lease.next_offset == 3
    finally:
        release_old.set()
        await asyncio.wait_for(run.wait_closed(), 1)

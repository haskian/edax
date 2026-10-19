"""Runner recovery preserves database progress and isolates failed partitions."""

from __future__ import annotations

import asyncio
import gc
import weakref
from collections.abc import Awaitable, Callable
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from aiokafka import TopicPartition
from edax.backend.kafka_projection import KafkaProjection
from edax.errors import CodecError, StoreError
from edax.projection import MemoryProjectionStateStore, ProjectionEvent, ProjectionInput, ProjectionResult
from test_kafka_projection import TP, Client, Source, decode, runner, wire
from test_projection_state import event, reducer, target


async def eventually(condition: Callable[[], Awaitable[bool]]) -> None:
    async def poll() -> None:
        while not await condition():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(poll(), 2)


async def test_reassignment_releases_previous_owner_and_resumes_database_cursor() -> None:
    run, client, _, store = await runner()
    records = client.records
    client.records = records[:1]
    await run._partition(TP, run._parts[TP])
    previous = run._parts[TP].lease
    assert previous is not None and previous.next_offset == 1

    listener = run.rebalance_listener()
    await listener.on_partitions_revoked({TP})
    await listener.on_partitions_assigned({TP})
    client.records = records
    await run._partition(TP, run._parts[TP])
    current = run._parts[TP].lease
    assert current is not None
    assert current.epoch > previous.epoch and current.owner != previous.owner
    assert current.next_offset == 3
    assert client.seeks == [0, 1]  # a new assignment resumes the durable cursor
    state = await store.get("bank-eu", "account-42")
    assert state is not None and state["public"]["balance_minor"] == 3750
    await run.wait_closed()


async def test_expired_current_lease_refences_and_preserves_committed_progress() -> None:
    now = [0]
    client = Client()
    store = MemoryProjectionStateStore(target(), clock=lambda: now[0])
    await store.initialize()
    run = KafkaProjection(client, store, Source(), decode, reducer)
    await run.rebalance_listener().on_partitions_assigned({TP})
    records = client.records
    client.records = records[:1]
    await run._partition(TP, run._parts[TP])
    previous = run._parts[TP].lease
    assert previous is not None

    now[0] = previous.expires_ms + 1
    client.records = records
    await run._partition(TP, run._parts[TP])
    current = run._parts[TP].lease
    assert current is not None
    assert current.epoch > previous.epoch and current.owner == previous.owner
    assert current.next_offset == 3 and current.expires_ms > now[0]
    assert client.seeks == [0]  # healthy refresh preserves the already-correct fetch position
    state = await store.get("bank-eu", "account-42")
    assert state is not None and state["public"]["balance_minor"] == 3750
    await run.wait_closed()


class PartitionClient(Client):
    def __init__(self) -> None:
        super().__init__()
        self.healthy = TopicPartition(TP.topic, 1)
        self.assigned.add(self.healthy)
        self.positions = {tp: 0 for tp in self.assigned}
        values = {
            TP: event(2, "missing-credit-1"),
            self.healthy: replace(event(), aggregate="account-43"),
        }
        self.by_partition = {
            tp: [
                SimpleNamespace(
                    topic=tp.topic,
                    partition=tp.partition,
                    offset=0,
                    key=value.aggregate.encode(),
                    value=wire(value),
                    headers=[],
                    timestamp=0,
                    timestamp_type=0,
                )
            ]
            for tp, value in values.items()
        }

    def seek(self, tp: Any, offset: int) -> None:
        self.positions[tp] = offset

    async def getmany(self, *partitions: Any, **kwargs: Any) -> dict[Any, Any]:
        tp = partitions[0]
        result = [record for record in self.by_partition[tp] if record.offset >= self.positions[tp]][:1]
        if result:
            self.positions[tp] = result[-1].offset + 1
        return {tp: result}


async def test_poison_partition_halts_without_stopping_healthy_partition() -> None:
    client, store = PartitionClient(), MemoryProjectionStateStore(target())
    await store.initialize()
    run = KafkaProjection(client, store, Source(), decode, reducer)
    await run.rebalance_listener().on_partitions_assigned(client.assignment())
    task = asyncio.create_task(run.run())
    try:

        async def processed() -> bool:
            healthy = run._parts[client.healthy].lease
            # The runner records the committed lease one loop turn after the store commits it.
            return TP in run.partition_failures and healthy is not None and healthy.next_offset == 1

        await eventually(processed)
        assert not task.done()
        assert isinstance(run.partition_failures[TP], CodecError)
        assert "sequence gap" in str(run.partition_failures[TP])
        assert await store.get("bank-eu", "account-42") is None
        observed_lease = run._parts[TP].lease
        assert observed_lease is not None
        assert observed_lease.next_offset == 0
        observed_lease = run._parts[client.healthy].lease
        assert observed_lease is not None
        assert observed_lease.next_offset == 1
        state = await store.get("bank-eu", "account-43")
        assert state is not None and state["public"]["balance_minor"] == 1250
    finally:
        await run.stop()
        await run.wait_closed()


async def test_completed_failure_tracebacks_release_originals_and_reducer_payloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, _, _, store = await runner()
    inputs: list[weakref.ReferenceType[ProjectionInput]] = []
    decoded: list[weakref.ReferenceType[ProjectionEvent]] = []
    original_commit = store.commit

    async def observed_commit(*args: Any, **kwargs: Any) -> Any:
        inputs.extend(weakref.ref(item) for item in args[1])
        return await original_commit(*args, **kwargs)

    def poison(state: Any, value: ProjectionEvent) -> ProjectionResult:
        decoded.append(weakref.ref(value))
        raise CodecError("invalid credit delta")

    monkeypatch.setattr(store, "commit", observed_commit)
    run.reducer = poison
    task = asyncio.create_task(run.run())
    try:

        async def failed() -> bool:
            return TP in run.partition_failures

        await eventually(failed)
    finally:
        await run.stop()
        await run.wait_closed()
        await asyncio.gather(task, return_exceptions=True)

    gc.collect()
    assert inputs and decoded
    assert all(reference() is None for reference in inputs + decoded)
    assert run.retained_records == run.retained_bytes == 0
    failure = run.partition_failures[TP]
    assert str(failure) == "invalid credit delta"
    trace = failure.__traceback__
    while trace is not None:
        assert not trace.tb_frame.f_locals.get("originals")
        trace = trace.tb_next


async def test_expired_refresh_commit_with_lost_reply_recovers_same_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, client, _, store = await runner()
    now = [0]
    store._clock = lambda: now[0]
    records = client.records
    client.records = records[:1]
    await run._partition(TP, run._parts[TP])
    previous = run._parts[TP].lease
    assert previous is not None
    now[0] = previous.expires_ms + 1
    original_refresh = store.refresh

    async def lost_reply(*args: Any, **kwargs: Any) -> Any:
        await original_refresh(*args, **kwargs)
        raise StoreError("lost refresh commit reply")

    monkeypatch.setattr(store, "refresh", lost_reply)
    run._parts[TP].renew_at = 0  # half the lease has passed: the idle partition extends it
    with pytest.raises(StoreError, match="lost refresh commit reply"):
        await run._partition(TP, run._parts[TP])
    monkeypatch.setattr(store, "refresh", original_refresh)
    client.records = records
    await run._partition(TP, run._parts[TP])
    recovered = run._parts[TP].lease
    assert recovered is not None
    assert recovered.epoch == previous.epoch + 1
    assert recovered.next_offset == 3 and recovered.owner == previous.owner
    state = await store.get("bank-eu", "account-42")
    assert state is not None and state["public"]["balance_minor"] == 3750
    await run.wait_closed()

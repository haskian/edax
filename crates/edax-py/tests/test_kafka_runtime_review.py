"""Regression cases for lifecycle failures reproduced by the merged PR review."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any

import pytest
from aiokafka import TopicPartition
from edax.backend import _kafka
from edax.backend.kafka_projection import KafkaProjection
from edax.batch import BatchSettings
from edax.errors import CodecError, StoreError
from edax.pacing import Backoff, Pacing
from edax.projection import MemoryProjectionStateStore
from test_kafka_ingest import (
    TP as INGEST_TP,
)
from test_kafka_ingest import (
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
from test_kafka_projection import TP, Client, Source, decode, runner
from test_projection_state import reducer, target


@pytest.mark.parametrize("interrupt", ["revoke", "deadline"])
async def test_ingestion_waits_for_interrupted_commit_without_repeating_sink(interrupt: str) -> None:
    entered, release = asyncio.Event(), asyncio.Event()

    class HeldCommit(BatchClient):
        async def commit(self, offsets: dict[TopicPartition, int]) -> None:
            entered.set()
            await release.wait()
            await super().commit(offsets)

    messages = events(1)
    client, sink = HeldCommit(records(messages)), RecordingSink()
    run = KafkaIngestion(
        client,
        sink,
        mapper,
        no_dead,
        settings=settings(max_records=1),
        serializer=EventIds(messages),
        commit_timeout_s=0.01,
        drain_timeout_s=0.3,
        pacing=Pacing(Backoff(1, 2)),
    )
    async with running(run) as task:
        await asyncio.wait_for(entered.wait(), 1)
        revoking = None
        if interrupt == "revoke":
            revoking = asyncio.create_task(run.rebalance_listener().on_partitions_revoked({INGEST_TP}))
        await asyncio.sleep(0.025)
        assert not task.done()
        assert run._offsets.unfinished() is not None
        release.set()
        if revoking is not None:
            await asyncio.wait_for(revoking, 1)
        await until(lambda: bool(client.commits))
        await asyncio.sleep(0.02)
        assert not task.done() and run._failure is None
        assert sink.batches == [(messages[0].id,)]
        assert all(batch == {INGEST_TP: 1} for batch in client.commits)
        assert all(not part.attempts and not part.retry_batches for part in run._parts.values())


async def test_assignment_error_is_visible_even_when_listener_caller_swallows_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = events(1)
    client = BatchClient(records(messages))
    client.assigned.clear()
    run = KafkaIngestion(client, RecordingSink(), mapper, no_dead, serializer=EventIds(messages))
    async with running(run) as task:
        await asyncio.sleep(0)

        async def broken_position(tp: TopicPartition) -> int:
            raise StoreError("coordinator unavailable")

        monkeypatch.setattr(client, "position", broken_position)
        client.assigned.add(INGEST_TP)
        # aiokafka logs listener errors; the runner must still observe them.
        with suppress(StoreError):
            await run.rebalance_listener().on_partitions_assigned({INGEST_TP})
        await until(task.done)
        with pytest.raises(StoreError, match="coordinator unavailable"):
            await task


@pytest.mark.parametrize("empty_assignment", [False, True])
async def test_ingestion_idle_activity_preserves_pause_and_rejoins(empty_assignment: bool) -> None:
    messages = events(1)

    class GroupClient(BatchClient):
        activity = 0
        run: KafkaIngestion

        async def getmany(self, *partitions: Any, **options: Any) -> dict[Any, Any]:
            if not partitions:
                self.activity += 1
                if not self.assigned:
                    self.assigned.add(INGEST_TP)
                    await self.run.rebalance_listener().on_partitions_assigned({INGEST_TP})
                    # A newly assigned record can escape the earlier pause snapshot.
                    return await super().getmany(INGEST_TP, **options)
            return await super().getmany(*partitions, **options)

    client = GroupClient(records(messages))
    sink = RecordingSink()
    run = client.run = KafkaIngestion(client, sink, mapper, no_dead, serializer=EventIds(messages))
    if empty_assignment:
        client.assigned.clear()
        await run._keepalive()
        assert client.positions[INGEST_TP] == 0
        assert run._parts[INGEST_TP].frontier == 0
    else:
        await run._assign({INGEST_TP})
        client.pause(INGEST_TP)
        await run._keepalive()
        assert INGEST_TP in client.paused
        assert client.positions[INGEST_TP] == 0
    assert client.activity == 1
    assert client.commits == [] and sink.batches == []


async def test_late_keepalive_cannot_seek_over_replacement_assignment() -> None:
    messages = events(1)

    class Reassigning(BatchClient):
        run: KafkaIngestion

        async def getmany(self, *partitions: Any, **options: Any) -> dict[Any, Any]:
            await self.run.rebalance_listener().on_partitions_revoked({INGEST_TP})
            self.positions[INGEST_TP] = 50
            await self.run.rebalance_listener().on_partitions_assigned({INGEST_TP})
            return {INGEST_TP: self.logs[INGEST_TP]}

    client = Reassigning(records(messages))
    run = client.run = KafkaIngestion(client, RecordingSink(), mapper, no_dead, serializer=EventIds(messages))
    await run._assign({INGEST_TP})
    await run._keepalive()
    assert client.positions[INGEST_TP] == 50
    assert client.seeks == []


async def test_projection_empty_after_seek_does_not_reset_poison_attempts() -> None:
    class ColdAfterSeek(Client):
        cold = False

        def seek(self, tp: Any, offset: int) -> None:
            super().seek(tp, offset)
            self.cold = True

        async def getmany(self, *partitions: Any, **options: Any) -> dict[Any, Any]:
            if self.cold:
                self.cold = False
                return {}
            return await super().getmany(*partitions, **options)

    client, store = ColdAfterSeek(), MemoryProjectionStateStore(target())
    client.records[0].value = b"not an event"
    await store.initialize()
    run = KafkaProjection(
        client,
        store,
        Source(),
        decode,
        reducer,
        settings=BatchSettings(max_attempts=3),
        pacing=Pacing(Backoff(1, 2)),
    )
    await run.rebalance_listener().on_partitions_assigned({TP})
    task = asyncio.create_task(run.run())
    try:
        await until(lambda: TP in run.partition_failures)
        assert isinstance(run.partition_failures[TP], CodecError)
        assert f"{TP.topic}/0@0" in str(run.partition_failures[TP])
        assert len(client.seeks) == 3
        assert await store.get("bank-eu", "account-42") is None
    finally:
        await run.stop()
        await run.wait_closed()
        await task


async def test_projection_stop_returns_to_host_and_wait_closed_does_not_wait_for_host() -> None:
    run, client, _, _ = await runner()
    client.records.clear()
    returned, finish_host = asyncio.Event(), asyncio.Event()

    async def host() -> None:
        await run.run()
        returned.set()
        await finish_host.wait()

    task = asyncio.create_task(host())
    await until(lambda: run._task is not None)
    try:
        await run.stop()
        await asyncio.wait_for(returned.wait(), 1)
        await asyncio.wait_for(run.wait_closed(), 1)
        assert not task.done()
    finally:
        finish_host.set()
        await task


async def test_projection_external_cancellation_remains_visible() -> None:
    run, client, _, _ = await runner()
    client.records.clear()
    task = asyncio.create_task(run.run())
    await until(lambda: run._task is not None)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(run.wait_closed(), 1)
    assert run._stopped.is_set()


async def test_projection_duplicate_assignment_preserves_owner_and_retry_budget() -> None:
    run, _, _, _ = await runner()
    assigned = run._parts[TP]
    assigned.attempts = 2
    await run.rebalance_listener().on_partitions_assigned({TP})
    assert run._parts[TP] is assigned and assigned.attempts == 2


async def test_settling_operation_propagates_repeated_waiter_cancel_after_completion() -> None:
    release = asyncio.Event()
    operation = asyncio.create_task(release.wait())
    waiter = asyncio.create_task(_kafka.settled(operation))
    await asyncio.sleep(0)
    waiter.cancel()
    await asyncio.sleep(0)
    assert not waiter.done() and not operation.cancelled()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert operation.done() and not operation.cancelled()


@pytest.mark.parametrize("sealing_enabled", [False, True])
async def test_unresolved_commit_retains_credits_without_stalling_healthy_partition(
    sealing_enabled: bool,
) -> None:
    from edax.sealing import MemoryKeys, Sealing
    from test_kafka_projection_deadlines import BLOCKED, HEALTHY, PartitionClient

    entered, release = asyncio.Event(), asyncio.Event()

    class HeldStore(MemoryProjectionStateStore):
        async def commit(self, *args: Any, **options: Any) -> Any:
            if args[0].partition == BLOCKED.partition:
                entered.set()
                while not release.is_set():
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        continue  # a driver can retain I/O after the cancellation request
            return await super().commit(*args, **options)

    store = HeldStore(target())
    await store.initialize()
    sealing = Sealing(MemoryKeys(), "accounts", allow_plaintext=True) if sealing_enabled else None
    run = KafkaProjection(
        PartitionClient(),
        store,
        Source(),
        decode,
        reducer,
        sealing=sealing,
        operation_timeout_s=0.02,
        lease_ms=1000,
    )
    await run.rebalance_listener().on_partitions_assigned([BLOCKED, HEALTHY])
    task = asyncio.create_task(run.run())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await until(lambda: run._parts[HEALTHY].lease is not None)
        assert run._parts[BLOCKED].unresolved is not None
        assert run.retained_records >= 1 and run.retained_bytes > 0
        if sealing_enabled:
            # MemoryKeys intentionally serializes every writer. Its resource contention must not
            # prevent another partition acquiring/staging; actual commits resume after release.
            assert await store.get("bank-eu", "account-1") is None
            release.set()
        await until(lambda: (lease := run._parts[HEALTHY].lease) is not None and lease.next_offset == 1)
        assert await store.get("bank-eu", "account-1") is not None
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await run.stop()
        await asyncio.wait_for(run.wait_closed(), 1)
        await asyncio.gather(task, return_exceptions=True)
    assert run.retained_records == run.retained_bytes == 0


async def test_prepared_representation_over_reserve_rejects_one_source() -> None:
    from conftest import build_message
    from edax.delivery import DeadLetter
    from edax.errors import Rejected
    from edax.ingestion import MappedRow

    limit = 1_048_576
    messages = [
        build_message("\\" * int(limit * 0.49), "OrderState", 1, id="huge", tenant="tenant-7"),
        *events(1),
    ]
    client, sink = BatchClient(records(messages)), RecordingSink()
    dead: list[DeadLetter[Any]] = []

    async def publish(letter: DeadLetter[Any]) -> None:
        dead.append(letter)

    run = KafkaIngestion(
        client,
        sink,
        lambda event: MappedRow("order", {"event_id": event.id}),
        publish,
        settings=BatchSettings(max_record_bytes=65_536, max_mapped_bytes=limit, max_records=2),
        serializer=EventIds(messages),
    )
    async with running(run) as task:
        await until(lambda: task.done() or any(batch.get(INGEST_TP) == 2 for batch in client.commits))
        if task.done():
            await task
    assert len(dead) == 1 and isinstance(dead[0].failure, Rejected)
    assert dead[0].origin.offset == 0
    assert await sink.get("tenant-7", "order") is not None
    assert run.peak_bytes <= run.settings.retained_bytes


async def test_stop_before_owned_loop_starts_returns_normally() -> None:
    run, client, _, _ = await runner()
    client.records.clear()
    task = asyncio.create_task(run.run())
    await asyncio.sleep(0)  # run created its owned task; that task has not run its first instruction
    assert run._task is not None and not run._loop_started
    await asyncio.gather(run.stop(), run.stop())
    await asyncio.wait_for(task, 1)
    await run.wait_closed()
    assert task.done() and not task.cancelled()


async def test_external_cancel_racing_requested_stop_is_not_swallowed() -> None:
    run, client, _, _ = await runner()
    client.records.clear()
    task = asyncio.create_task(run.run())
    await until(lambda: run._loop_started)
    stopping = asyncio.create_task(run.stop())
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(stopping, 1)
    await asyncio.wait_for(run.wait_closed(), 1)


@pytest.mark.parametrize(
    "change",
    [{"aggregate_id": ""}, {"tenant": "x" * 2050}, {"subject": "person\x00bad"}],
    ids=["empty-stream", "oversize-tenant", "nul-subject"],
)
async def test_invalid_event_identity_is_a_source_rejection(change: dict[str, str]) -> None:
    from edax import _values
    from edax.delivery import DeadLetter
    from edax.errors import Rejected
    from edax.ingestion import MappedRow

    messages = events(2)
    messages[0] = _values.replace(messages[0], **change)
    client, sink = BatchClient(records(messages)), RecordingSink()
    dead: list[DeadLetter[Any]] = []

    async def publish(letter: DeadLetter[Any]) -> None:
        dead.append(letter)

    run = KafkaIngestion(
        client,
        sink,
        lambda event: MappedRow(event.aggregate_id or "fallback", {"status": "paid"}),
        publish,
        settings=settings(max_records=2),
        serializer=EventIds(messages),
    )
    async with running(run) as task:
        await until(lambda: task.done() or any(batch.get(INGEST_TP) == 2 for batch in client.commits))
        if task.done():
            await task
    assert len(dead) == 1 and isinstance(dead[0].failure, Rejected)
    assert dead[0].origin.offset == 0
    assert await sink.get("tenant-7", "order-0") is not None

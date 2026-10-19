"""A batch's staging time must not consume its commit lease."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any

import pytest
from aiokafka import TopicPartition
from edax.backend.kafka_projection import KafkaProjection
from edax.errors import BadHandle, StoreError
from edax.projection import MemoryProjectionStateStore, ProjectionLease
from test_kafka_projection import TP, Client, Source, decode, runner
from test_kafka_projection_recovery import eventually
from test_projection_state import proof, reducer, target


@pytest.mark.parametrize("takeover", [False, True], ids=["still-owned", "new-owner"])
async def test_staging_expiry_refreshes_only_the_current_owner(takeover: bool) -> None:
    now = [0]
    store = MemoryProjectionStateStore(target(), clock=lambda: now[0])
    await store.initialize()
    successor: list[ProjectionLease] = []

    class StagingSource(Source):
        calls = 0

        async def verify(self, tp: Any) -> Any:
            self.calls += 1
            now[0] += 70
            # The proof before the commit lands after the 60ms lease expired while the batch staged.
            if self.calls == 2 and takeover:
                successor.append(await store.acquire(0, "replacement-owner", proof()))
            await asyncio.sleep(0)
            return await super().verify(tp)

    client = Client()
    run = KafkaProjection(
        client, store, StagingSource(), decode, reducer, operation_timeout_s=0.05, lease_ms=60
    )
    await run.rebalance_listener().on_partitions_assigned({TP})
    assigned = run._parts[TP]
    task = asyncio.create_task(run.run())
    try:

        async def settled() -> bool:
            # The commit lands in the store before the runner records the lease it returned.
            return bool(run.partition_failures) or (
                assigned.lease is not None and assigned.lease.next_offset == 3
            )

        await eventually(settled)
        assert run._parts[TP] is assigned and TP in client.assignment()
        assert not task.done()
        if takeover:
            assert isinstance(run.partition_failures[TP], BadHandle)
            assert await store.get("bank-eu", "account-42") is None
            assert (await store.recover(successor[0])).owner == "replacement-owner"
        else:
            assert run.partition_failures == {}
            saved = await store.get("bank-eu", "account-42")
            assert saved is not None and saved["value"]["balance_minor"] == 3750
            assert assigned.lease is not None
            assert assigned.lease.epoch == 2 and assigned.lease.next_offset == 3
    finally:
        await run.stop()
        await run.wait_closed()
    assert run.retained_records == run.retained_bytes == 0


async def test_assignment_lost_during_commit_releases_lease_without_committing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, store = Client(), MemoryProjectionStateStore(target())
    await store.initialize()
    run = KafkaProjection(client, store, Source(), decode, reducer)
    await run.rebalance_listener().on_partitions_assigned({TP})
    original_commit = store.commit

    async def revoked_during_commit(*args: Any, **kwargs: Any) -> ProjectionLease:
        client.assigned.clear()
        return await original_commit(*args, **kwargs)

    monkeypatch.setattr(store, "commit", revoked_during_commit)
    with pytest.raises(BadHandle, match="obsolete"):
        await run._partition(TP, run._parts[TP])
    await run.wait_closed()
    assert await store.get("bank-eu", "account-42") is None
    assert run.retained_records == run.retained_bytes == 0
    # A released lease allows immediate takeover, without waiting for expiry.
    successor = await store.acquire(0, "replacement-owner", proof())
    assert successor.owner == "replacement-owner" and successor.next_offset == 0


async def test_commit_past_its_deadline_recovers_without_applying_twice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = MemoryProjectionStateStore(target())
    await store.initialize()
    run = KafkaProjection(Client(), store, Source(), decode, reducer, operation_timeout_s=0.05, lease_ms=60)
    await run.rebalance_listener().on_partitions_assigned({TP})
    original_commit = store.commit
    commit_started = asyncio.Event()

    async def slow_commit(*args: Any, **kwargs: Any) -> ProjectionLease:
        commit_started.set()
        await asyncio.sleep(0.07)
        return await original_commit(*args, **kwargs)

    monkeypatch.setattr(store, "commit", slow_commit)
    try:
        with pytest.raises(StoreError, match="deadline"):
            await run._partition(TP, run._parts[TP])
        await run.wait_closed()
        assert commit_started.is_set()
        assert await store.get("bank-eu", "account-42") is None
        monkeypatch.setattr(store, "commit", original_commit)
        await run._partition(TP, run._parts[TP])
        saved = await store.get("bank-eu", "account-42")
        assert saved is not None and saved["value"]["balance_minor"] == 3750
    finally:
        await run.wait_closed()
    assert run.retained_records == run.retained_bytes == 0


async def test_assignment_that_returns_reacquires_instead_of_stopping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, client, _, store = await runner()
    assigned = run._parts[TP]
    await run._partition(TP, assigned)
    first = assigned.lease
    original_refresh = store.refresh

    async def drop_assignment(*args: Any, **kwargs: Any) -> ProjectionLease:
        refreshed = await original_refresh(*args, **kwargs)
        client.assigned = set()
        return refreshed

    monkeypatch.setattr(store, "refresh", drop_assignment)
    assigned.renew_at = 0  # half the lease has passed: the idle partition extends it
    await run._partition(TP, assigned)
    await run.wait_closed()
    assert assigned.lease is None, "a lease the runner released is not reused"
    monkeypatch.setattr(store, "refresh", original_refresh)
    client.assigned = {TP}
    await run._partition(TP, assigned)
    assert assigned.failure is None
    assert assigned.lease is not None and first is not None and assigned.lease.epoch > first.epoch


async def test_timed_out_commit_keeps_the_live_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    store = MemoryProjectionStateStore(target())
    await store.initialize()
    run = KafkaProjection(Client(), store, Source(), decode, reducer, operation_timeout_s=0.05, lease_ms=1000)
    await run.rebalance_listener().on_partitions_assigned({TP})
    assigned = run._parts[TP]
    original_commit = store.commit

    async def ignores_cancellation(*args: Any, **kwargs: Any) -> ProjectionLease:
        committed = await original_commit(*args, **kwargs)
        with suppress(asyncio.CancelledError):
            await asyncio.sleep(0.1)
        return committed

    monkeypatch.setattr(store, "commit", ignores_cancellation)
    with pytest.raises(StoreError, match="deadline"):
        await run._partition(TP, assigned)
    await run.wait_closed()
    assert assigned.lease is not None, "a deadline alone must not release a live assignment's lease"
    monkeypatch.setattr(store, "commit", original_commit)
    await run._partition(TP, assigned)
    assert assigned.failure is None
    state = await store.get("bank-eu", "account-42")
    assert state is not None and state["value"]["balance_minor"] == 3750


async def test_stop_releases_every_lease_while_a_rebalance_changes_partitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, _, _, store = await runner()
    await run._partition(TP, run._parts[TP])
    original_release = store.release

    async def release_during_rebalance(lease: ProjectionLease) -> None:
        await run.rebalance_listener().on_partitions_assigned({TopicPartition(TP.topic, 1)})
        await original_release(lease)

    monkeypatch.setattr(store, "release", release_during_rebalance)
    task = asyncio.create_task(run.run())
    await asyncio.sleep(0.02)
    await run.stop()
    await run.wait_closed()
    assert task.done() and not (not task.cancelled() and task.exception())
    assert (await store.acquire(0, "successor", proof())).owner == "successor"


async def test_run_stops_on_the_callers_event_and_releases_its_lease() -> None:
    run, _, _, store = await runner()
    stop = asyncio.Event()
    task = asyncio.create_task(run.run(stop))
    committed = asyncio.Event()

    async def watch() -> None:
        while (await store.get("bank-eu", "account-42")) is None:
            await asyncio.sleep(0.005)
        committed.set()

    await asyncio.wait_for(watch(), 1)
    stop.set()
    await asyncio.wait_for(task, 1)
    await run.wait_closed()
    assert (await store.acquire(0, "successor", proof())).owner == "successor"


async def test_an_idle_partition_touches_neither_source_nor_store_until_half_its_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, _, source, store = await runner()
    assigned = run._parts[TP]
    await run._partition(TP, assigned)
    verified: list[Any] = []
    original_verify, original_refresh = source.verify, store.refresh

    async def counted_verify(tp: Any) -> Any:
        verified.append(tp)
        return await original_verify(tp)

    refreshed: list[Any] = []

    async def counted_refresh(*args: Any, **kwargs: Any) -> ProjectionLease:
        refreshed.append(args)
        return await original_refresh(*args, **kwargs)

    monkeypatch.setattr(source, "verify", counted_verify)
    monkeypatch.setattr(store, "refresh", counted_refresh)
    await run._partition(TP, assigned)
    assert (verified, refreshed) == ([], [])
    assigned.renew_at = 0
    await run._partition(TP, assigned)
    assert (len(verified), len(refreshed)) == (1, 1)

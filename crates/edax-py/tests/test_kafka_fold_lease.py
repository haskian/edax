"""A batch's staging time must not consume its commit lease."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from edax.backend.kafka_fold import KafkaFold
from edax.errors import BadHandle, StoreError
from edax.fold import FoldLease, MemoryFoldStore
from test_fold import proof, reducer, target
from test_kafka_fold import TP, Client, Source, decode
from test_kafka_fold_recovery import eventually


@pytest.mark.parametrize("takeover", [False, True], ids=["still-owned", "new-owner"])
async def test_staging_expiry_refreshes_only_the_current_owner(takeover: bool) -> None:
    now = [0]
    store = MemoryFoldStore(target(), clock=lambda: now[0])
    await store.initialize()
    successor: list[FoldLease] = []

    class StagingSource(Source):
        calls = 0

        async def verify(self, tp: Any) -> Any:
            self.calls += 1
            now[0] += 35
            # Each proof consumes less than the 50ms operation budget. The two
            # proofs after acquisition together exceed the valid 60ms lease.
            if self.calls == 3 and takeover:
                successor.append(await store.acquire(0, "replacement-owner", proof()))
            await asyncio.sleep(0)
            return await super().verify(tp)

    client = Client()
    run = KafkaFold(client, store, StagingSource(), decode, reducer, operation_timeout_s=0.05, lease_ms=60)
    await run.rebalance_listener().on_partitions_assigned({TP})
    assigned = run._parts[TP]
    task = asyncio.create_task(run.run())
    try:

        async def settled() -> bool:
            return bool(run.partition_failures) or await store.get("bank-eu", "account-42") is not None

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


async def test_assignment_lost_after_refresh_releases_lease_without_committing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, store = Client(), MemoryFoldStore(target())
    await store.initialize()
    run = KafkaFold(client, store, Source(), decode, reducer)
    await run.rebalance_listener().on_partitions_assigned({TP})
    original_refresh = store.refresh

    async def revoked_reply(*args: Any, **kwargs: Any) -> FoldLease:
        refreshed = await original_refresh(*args, **kwargs)
        client.assigned.clear()
        return refreshed

    monkeypatch.setattr(store, "refresh", revoked_reply)
    await run._partition(TP, run._parts[TP])
    await run.wait_closed()
    assert await store.get("bank-eu", "account-42") is None
    assert run.retained_records == run.retained_bytes == 0
    # A released refreshed lease allows immediate takeover, without waiting for expiry.
    successor = await store.acquire(0, "replacement-owner", proof())
    assert successor.owner == "replacement-owner" and successor.next_offset == 0


async def test_precommit_refresh_and_commit_share_one_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    store = MemoryFoldStore(target())
    await store.initialize()
    run = KafkaFold(Client(), store, Source(), decode, reducer, operation_timeout_s=0.05, lease_ms=60)
    await run.rebalance_listener().on_partitions_assigned({TP})
    original_refresh, original_commit = store.refresh, store.commit
    commit_started = asyncio.Event()

    async def slow_refresh(*args: Any, **kwargs: Any) -> FoldLease:
        refreshed = await original_refresh(*args, **kwargs)
        await asyncio.sleep(0.035)
        return refreshed

    async def slow_commit(*args: Any, **kwargs: Any) -> FoldLease:
        commit_started.set()
        await asyncio.sleep(0.035)
        return await original_commit(*args, **kwargs)

    monkeypatch.setattr(store, "refresh", slow_refresh)
    monkeypatch.setattr(store, "commit", slow_commit)
    try:
        with pytest.raises(StoreError, match="deadline"):
            await run._partition(TP, run._parts[TP])
        await run.wait_closed()
        assert commit_started.is_set()
        assert await store.get("bank-eu", "account-42") is None
        monkeypatch.setattr(store, "refresh", original_refresh)
        monkeypatch.setattr(store, "commit", original_commit)
        await run._partition(TP, run._parts[TP])
        saved = await store.get("bank-eu", "account-42")
        assert saved is not None and saved["value"]["balance_minor"] == 3750
    finally:
        await run.wait_closed()
    assert run.retained_records == run.retained_bytes == 0

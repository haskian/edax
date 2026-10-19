"""A runner's provider follows structured commit overrides, while the store owns its fence."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar

import pytest
from edax.errors import BadHandle
from edax.projection import (
    AssignmentGuard,
    MemoryProjectionStateStore,
    ProjectionInput,
    ProjectionLease,
    ProjectionSession,
    Reducer,
    SourceProof,
)
from test_brokerless_ingestion import NonReentrantKeys
from test_kafka_projection import TP
from test_kafka_projection_sealing import balance, projection, sealed_records
from test_projection_state import proof, target

T = TypeVar("T")


@pytest.mark.parametrize("wrapper", ["task", "wait_for"])
@pytest.mark.parametrize("store_provider", [False, True])
async def test_child_commit_owns_one_guard_through_actual_persistence(
    wrapper: str, store_provider: bool
) -> None:
    keys = NonReentrantKeys()
    sealing, records = await sealed_records(keys)
    pending, persist = asyncio.Event(), asyncio.Event()

    class Store(MemoryProjectionStateStore):
        committing = False

        async def commit(
            self,
            lease: ProjectionLease,
            inputs: Sequence[ProjectionInput],
            proof: SourceProof,
            reducer: Reducer,
            *,
            lease_ms: int = 30_000,
            is_current: AssignmentGuard = lambda: True,
        ) -> ProjectionLease:
            self.committing = True
            call = super().commit(lease, inputs, proof, reducer, lease_ms=lease_ms, is_current=is_current)
            try:
                if wrapper == "task":
                    return await asyncio.create_task(call)
                return await asyncio.wait_for(call, 1)
            finally:
                self.committing = False

        async def _transaction(self, call: Callable[[ProjectionSession], Awaitable[T]]) -> T:
            async def before_persistence(session: ProjectionSession) -> T:
                result = await call(session)
                if self.committing:
                    assert keys.lock.locked() and keys.acquisitions == 1
                    pending.set()
                    await persist.wait()
                    assert keys.lock.locked()
                return result

            return await super()._transaction(before_persistence)

    store = Store(target(), sealing=sealing if store_provider else None)
    runner, _ = await projection(sealing, records, balance([]), store)
    writing = asyncio.create_task(runner._partition(TP, runner._parts[TP]))
    revoking: asyncio.Task[Any] | None = None
    try:
        await asyncio.wait_for(pending.wait(), 1)
        revoking = asyncio.create_task(keys.revoke(sealing.scope("bank-eu", "person-42")))
        await asyncio.sleep(0)
        assert not revoking.done() and keys.acquisitions == 1
        persist.set()
        await asyncio.wait_for(writing, 1)
        assert await asyncio.wait_for(revoking, 1) == "revoking"
        row = await store.get("bank-eu", "account-42", "person-42")
        assert row is not None and row["sequence"] == 1
        assert row["public"]["balance_minor"] == 1250
        assert keys.acquisitions == 1 and not keys.lock.locked()
        assert store.sealing is (sealing if store_provider else None)
    finally:
        persist.set()
        await asyncio.gather(writing, return_exceptions=True)
        if revoking is not None:
            await asyncio.gather(revoking, return_exceptions=True)
        await runner.wait_closed()


async def test_runner_provider_context_does_not_admit_another_store() -> None:
    keys = NonReentrantKeys()
    sealing, records = await sealed_records(keys)
    other = MemoryProjectionStateStore(target("unconfigured-other-store"))
    await other.initialize()
    other_lease = await other.acquire(0, "other-worker", proof())
    refused = False

    class Store(MemoryProjectionStateStore):
        async def commit(
            self,
            lease: ProjectionLease,
            inputs: Sequence[ProjectionInput],
            proof: SourceProof,
            reducer: Reducer,
            *,
            lease_ms: int = 30_000,
            is_current: AssignmentGuard = lambda: True,
        ) -> ProjectionLease:
            nonlocal refused
            with pytest.raises(BadHandle, match="sealing="):
                await asyncio.create_task(other.commit(other_lease, inputs, proof, reducer))
            refused = True
            return await super().commit(
                lease, inputs, proof, reducer, lease_ms=lease_ms, is_current=is_current
            )

    store = Store(target())
    runner, _ = await projection(sealing, records, balance([]), store)
    await runner._partition(TP, runner._parts[TP])
    assert refused and keys.acquisitions == 1
    assert await other.recover(other_lease) == other_lease
    assert await other.get("bank-eu", "account-42", "person-42") is None
    assert (await store.get("bank-eu", "account-42", "person-42")) is not None
    await runner.wait_closed()


def test_provider_context_does_not_retain_native_store() -> None:
    import gc
    import weakref
    from contextvars import copy_context

    from edax.sealing import MemoryKeys, Sealing

    sealing = Sealing(MemoryKeys(), "accounts")
    store = MemoryProjectionStateStore(target())
    reference = weakref.ref(store)
    with store._using_sealing(sealing):
        snapshot = copy_context()
    del store
    gc.collect()
    assert snapshot.run(reference) is None

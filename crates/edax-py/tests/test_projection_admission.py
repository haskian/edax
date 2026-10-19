"""Direct projection commits retain authoritative scope admission through persistence."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any, TypeVar

import pytest
from conftest import postgres_schema
from edax.backend.postgres import PostgresTransactor
from edax.backend.projection_postgres import PostgresProjectionStateStore, reference_projection_state_ddl
from edax.errors import BadHandle, KeyRevoking, Shredded
from edax.projection import (
    MemoryProjectionStateStore,
    ProjectionInput,
    ProjectionResult,
    ProjectionSession,
    ProjectionStateStore,
)
from edax.sealing import KeyScope, MemoryKeys, Sealing
from test_projection_state import event, proof, target

T = TypeVar("T")


@pytest.fixture(params=["memory", "postgres"])
async def make_store(request: pytest.FixtureRequest) -> AsyncIterator[Callable[..., ProjectionStateStore]]:
    if request.param == "memory":
        yield lambda **kwargs: MemoryProjectionStateStore(**kwargs)
    else:
        async with postgres_schema("projection_admission", reference_projection_state_ddl) as (pool, schema):
            tx = PostgresTransactor(pool, schema=schema)
            yield lambda **kwargs: PostgresProjectionStateStore(tx, schema=schema, **kwargs)


@pytest.mark.parametrize(
    ("case", "subject", "at_rest", "private", "error"),
    [
        ("destroyed", "person-7", "plaintext", {"email": "ana@example.com"}, Shredded),
        ("revoking", "person-7", "plaintext", {"email": "ana@example.com"}, KeyRevoking),
        ("destroyed", "person-7", "sealed", None, Shredded),
        ("tenant_destroyed", None, "plaintext", {"email": "ana@example.com"}, Shredded),
        ("no_provider", "person-7", "sealed", None, BadHandle),
        ("erased", "person-7", "plaintext", None, None),
        ("healthy", "person-7", "plaintext", {"email": "ana@example.com"}, None),
    ],
)
async def test_direct_commit_admission(
    make_store: Callable[..., ProjectionStateStore],
    case: str,
    subject: str | None,
    at_rest: str,
    private: Any,
    error: type[Exception] | None,
) -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "accounts")
    store = make_store(
        target=replace(target(), at_rest=at_rest), sealing=None if case == "no_provider" else sealing
    )
    await store.initialize()
    lease = await store.acquire(0, "worker", proof())
    scope = sealing.scope("bank-eu", subject)
    if case in {"destroyed", "tenant_destroyed", "erased"}:
        await keys.destroy(scope)
    elif case == "revoking":
        await keys.revoke(scope)
    original = ProjectionInput(0, replace(event(), subject=subject), erased=case == "erased")
    called = 0

    def reduce(state: Any, value: Any) -> ProjectionResult:
        nonlocal called
        called += 1
        return ProjectionResult(private=private, public={"balance_minor": 1250})

    if error is not None:
        with pytest.raises(error):
            await store.commit(lease, [original], proof(), reduce)
        assert await store.recover(lease) == lease
        assert await store.get("bank-eu", "account-42", subject) is None
        assert called == 0
    else:
        committed = await store.commit(lease, [original], proof(), reduce)
        assert committed.next_offset == 1
        row = await store.get("bank-eu", "account-42", subject)
        if case == "erased":
            assert row is None and called == 0
        else:
            assert row is not None and row["private"] == private and called == 1
    assert not keys._guard.locked()


class NonReentrantKeys(MemoryKeys):
    def __init__(self) -> None:
        super().__init__()
        self.admissions = 0
        self.held = False

    @asynccontextmanager
    async def guard(self, scope: KeyScope) -> AsyncIterator[None]:
        assert not self.held, "an owned scope must borrow the runner's non-reentrant guard"
        self.admissions += 1
        self.held = True
        try:
            async with super().guard(scope):
                yield
        finally:
            self.held = False


@pytest.mark.parametrize("store_provider", [True, False])
async def test_runner_scope_is_admitted_once(store_provider: bool) -> None:
    from test_kafka_projection import TP
    from test_kafka_projection_sealing import balance, projection, sealed_records

    keys = NonReentrantKeys()
    sealing, records = await sealed_records(keys)
    store = MemoryProjectionStateStore(target(), sealing=sealing if store_provider else None)
    seen: list[Any] = []
    runner, _ = await projection(sealing, records, balance(seen), store)
    await runner._partition(TP, runner._parts[TP])
    assert len(seen) == 1
    assert keys.admissions == 1 and not keys.held
    row = await store.get("bank-eu", "account-42", "person-42")
    assert row is not None and row["sequence"] == 1


async def test_cancellation_keeps_admission_until_persistence_settles(
    make_store: Callable[..., ProjectionStateStore], monkeypatch: pytest.MonkeyPatch
) -> None:
    started, cancelled, finish = asyncio.Event(), asyncio.Event(), asyncio.Event()
    keys = MemoryKeys()
    sealing = Sealing(keys, "accounts")
    store = make_store(target=replace(target(), at_rest="plaintext"), sealing=sealing)
    await store.initialize()
    lease = await store.acquire(0, "worker", proof())
    transaction = store._transaction

    async def slow_transaction(call: Callable[[ProjectionSession], Awaitable[T]]) -> T:
        async def pending(session: ProjectionSession) -> T:
            result = await call(session)
            started.set()
            try:
                await finish.wait()
            except asyncio.CancelledError:
                cancelled.set()
                await finish.wait()
            return result

        return await transaction(pending)

    monkeypatch.setattr(store, "_transaction", slow_transaction)
    writing = asyncio.create_task(
        store.commit(
            lease,
            [ProjectionInput(0, replace(event(), subject="person-7"))],
            proof(),
            lambda state, value: ProjectionResult(private={"email": "ana@example.com"}),
        )
    )
    await asyncio.wait_for(started.wait(), 1)
    writing.cancel()
    await asyncio.wait_for(cancelled.wait(), 1)
    assert keys._guard.locked()
    destroying = asyncio.create_task(keys.destroy(sealing.scope("bank-eu", "person-7")))
    await asyncio.sleep(0)
    assert not destroying.done()
    finish.set()
    assert (await asyncio.wait_for(writing, 1)).next_offset == 1
    assert await asyncio.wait_for(destroying, 1) == "destroyed"
    assert not keys._guard.locked()


@pytest.mark.parametrize("erased_at_fence", [False, True])
@pytest.mark.parametrize("outer_fence", [False, True])
async def test_postgres_transaction_admits_once_before_reduction(
    erased_at_fence: bool, outer_fence: bool
) -> None:
    from contextlib import nullcontext

    from test_transaction_fence import FencingKeys

    async with postgres_schema("projection_tx_admission", reference_projection_state_ddl) as (pool, schema):
        tx = PostgresTransactor(pool, schema=schema)
        keys = FencingKeys(tx)
        keys.erase_at_fence = erased_at_fence
        sealing = Sealing(keys, "accounts", write_fence="transaction")
        store = PostgresProjectionStateStore(
            tx, replace(target(), at_rest="plaintext"), schema=schema, sealing=sealing
        )
        await store.initialize()
        lease = await store.acquire(0, "worker", proof())
        called = False

        def reduce(state: Any, value: Any) -> ProjectionResult:
            nonlocal called
            called = True
            return ProjectionResult(private={"email": "ana@example.com"})

        async with sealing.write_guard("bank-eu", "person-7") if outer_fence else nullcontext():
            committing = store.commit(
                lease, [ProjectionInput(0, replace(event(), subject="person-7"))], proof(), reduce
            )
            if erased_at_fence:
                with pytest.raises(Shredded):
                    await committing
            else:
                assert (await committing).next_offset == 1
        assert len(keys.fences) == 1
        assert keys.fences[0][1] == [sealing.scope("bank-eu", "person-7")]
        assert called is not erased_at_fence
        assert (await store.recover(lease)).next_offset == (0 if erased_at_fence else 1)
        assert not keys._guard.locked()

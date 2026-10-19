"""Transaction-mode erasure admission for independently committed state stores."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any, Literal
from uuid import uuid4

import pytest
from edax.backend.erasure_postgres import PostgresErasureStore
from edax.backend.erasure_postgres import reference_ddl as erasure_ddl
from edax.backend.ingest_postgres import PostgresIngestionSink, reference_ingestion_ddl
from edax.backend.postgres import PostgresTransactionFailure, PostgresTransactor
from edax.backend.projection_postgres import PostgresProjectionStateStore, reference_projection_state_ddl
from edax.errors import Rejected, Shredded
from edax.ingestion import BatchSink, IngestionTarget, MemoryIngestionSink
from edax.projection import MemoryProjectionStateStore, ProjectionInput, ProjectionStateStore
from edax.sealing import KeyScope, MemoryKeys, Sealing
from test_projection_state import event, initial_cutover, proof, reducer, target


class FencedKeys(MemoryKeys):
    def __init__(self, transactor: object, erasure: PostgresErasureStore | None = None) -> None:
        super().__init__()
        self.transactor = transactor
        self.erasure = erasure
        self.fence_calls = 0

    async def fence(self, tx: Any, scopes: Sequence[KeyScope]) -> None:
        assert self.erasure is not None, "memory writes must hold the provider guard"
        self.fence_calls += 1
        await self.erasure.guard_in_transaction(tx.connection, *scopes)


@asynccontextmanager
async def postgres_database() -> AsyncIterator[tuple[Any, str, PostgresTransactor]]:
    url = os.environ.get("EDAX_POSTGRES_URL")
    if url is None:
        if os.environ.get("EDAX_REQUIRE_POSTGRES") == "1":
            pytest.fail("EDAX_POSTGRES_URL required")
        pytest.skip("EDAX_POSTGRES_URL not set")
    asyncpg = pytest.importorskip("asyncpg")
    pool = await asyncpg.create_pool(url, min_size=1, max_size=4)
    schema = "erasure_state_" + uuid4().hex
    try:
        await pool.execute(f'CREATE SCHEMA "{schema}"')
        await pool.execute(erasure_ddl(schema))
        await pool.execute(reference_ingestion_ddl(schema))
        await pool.execute(reference_projection_state_ddl(schema))
        yield pool, schema, PostgresTransactor(pool, schema=schema)
    finally:
        await pool.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await pool.close()


@pytest.fixture
async def database() -> AsyncIterator[tuple[Any, str, PostgresTransactor]]:
    async with postgres_database() as value:
        yield value


@pytest.fixture(params=["memory", "postgres"])
async def backend(request: Any) -> AsyncIterator[tuple[Any, str, Any, FencedKeys]]:
    if request.param == "memory":
        yield None, "", None, FencedKeys(object())
        return
    async with postgres_database() as (pool, schema, db):
        yield pool, schema, db, FencedKeys(db, PostgresErasureStore(pool, schema=schema))


async def revoke(keys: FencedKeys, scope: KeyScope) -> None:
    if keys.erasure is not None:
        await keys.erasure.revoke(scope)
    await keys.destroy(scope)


async def test_projection_commit_refuses_erased_scope_without_persisting_any_effect(
    backend: tuple[Any, str, Any, FencedKeys],
) -> None:
    _, schema, db, keys = backend
    store: ProjectionStateStore = (
        MemoryProjectionStateStore(target())
        if db is None
        else PostgresProjectionStateStore(db, target(), schema=schema)
    )
    await store.initialize()
    await store.activate(initial_cutover(store.target))
    lease = await store.acquire(0, "writer", proof())
    sealing = Sealing(keys, "production", write_fence="transaction")
    scope = sealing.scope("bank-eu", "person-42")
    await revoke(keys, scope)

    with pytest.raises(Shredded if db is None else Rejected):
        async with sealing.write_guard(scope.tenant, scope.subject):
            await store.commit(lease, [ProjectionInput(0, event())], proof(), reducer)

    assert await store.get("bank-eu", "account-42") is None
    assert (await store.recover(lease)).next_offset == 0
    assert await store.unpublished(100) == []
    if db is not None:
        assert keys.fence_calls == 1


@pytest.mark.parametrize("mode", ["append", "upsert"])
async def test_ingestion_commit_refuses_erased_scope_without_state_or_rejections(
    backend: tuple[Any, str, Any, FencedKeys], mode: Literal["append", "upsert"]
) -> None:
    _, schema, db, keys = backend
    spec = IngestionTarget(mode, "private", "cluster/source", "private-mapper")
    sink: BatchSink = (
        MemoryIngestionSink(spec) if db is None else PostgresIngestionSink(db, spec, schema=schema)
    )
    row_key = "event-1" if mode == "append" else "person-42"
    originals = [
        spec.input(
            "events",
            0,
            offset,
            "bank-eu",
            row_key,
            "person-42",
            "event-1",
            1,
            {"email": "private@example.test"},
            {"email": "private@example.test"},
            '{"email":"private@example.test"}',
            failure,
        )
        for offset, failure in enumerate((None, "codec: mapper refused event"))
    ]
    sealing = Sealing(keys, "production", write_fence="transaction")
    scope = sealing.scope("bank-eu", "person-42")
    await revoke(keys, scope)

    with pytest.raises(Shredded if db is None else PostgresTransactionFailure) as refused:
        async with sealing.write_guard(scope.tenant, scope.subject):
            await sink.commit(originals)

    assert await sink.get(scope.tenant, row_key) is None
    assert await sink.pending() == {}
    if db is not None:
        assert keys.fence_calls == 1
        failure = refused.value
        assert isinstance(failure, PostgresTransactionFailure)
        assert isinstance(failure.cause, Rejected)
        assert failure.rolled_back and not failure.commit_started


@pytest.mark.parametrize("outcome", ["commit", "cancel"])
async def test_owned_transaction_retains_foreign_provider_guard_through_completion(
    database: tuple[Any, str, PostgresTransactor], outcome: str
) -> None:
    pool, schema, db = database
    await pool.execute(f'CREATE TABLE "{schema}".plaintext (value text)')
    staged, finish = asyncio.Event(), asyncio.Event()
    guard_events: list[str] = []
    rows_when_released: list[int] = []

    class ForeignKeys(FencedKeys):
        @asynccontextmanager
        async def guard(self, scope: KeyScope) -> AsyncIterator[None]:
            async with super().guard(scope):
                guard_events.append("entered")
                try:
                    yield
                finally:
                    rows_when_released.append(
                        await pool.fetchval(f'SELECT count(*) FROM "{schema}".plaintext')
                    )
                    guard_events.append("released")

    keys = ForeignKeys(object())  # A distinct database requires a guard, never keys.fence().
    sealing = Sealing(keys, "production", write_fence="transaction")

    async def write(conn: Any) -> None:
        assert guard_events == ["entered"]
        await conn.execute(f'INSERT INTO "{schema}".plaintext VALUES ($1)', "private@example.test")
        staged.set()
        await finish.wait()

    async def writer() -> None:
        async with sealing.write_guard("bank-eu", "person-42"):
            await db.run_owned(write)

    writing = asyncio.create_task(writer())
    try:
        await asyncio.wait_for(staged.wait(), 3)
        assert guard_events == ["entered"]
        if outcome == "cancel":
            writing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await writing
        else:
            finish.set()
            await asyncio.wait_for(writing, 3)
    finally:
        if not writing.done():
            writing.cancel()
        await asyncio.gather(writing, return_exceptions=True)
    assert guard_events == ["entered", "released"]
    assert rows_when_released == [1 if outcome == "commit" else 0]


@pytest.mark.parametrize("outcome", ["commit", "cancel"])
async def test_owned_transaction_holds_destination_fence_until_commit_or_rollback(
    database: tuple[Any, str, PostgresTransactor], outcome: str
) -> None:
    pool, schema, db = database
    await pool.execute(f'CREATE TABLE "{schema}".plaintext (value text)')
    erasure = PostgresErasureStore(pool, schema=schema)
    keys = FencedKeys(db, erasure)
    sealing = Sealing(keys, "production", write_fence="transaction")
    scope = sealing.scope("bank-eu", "person-42")
    staged, finish = asyncio.Event(), asyncio.Event()

    async def write(conn: Any) -> None:
        await conn.execute(f'INSERT INTO "{schema}".plaintext VALUES ($1)', "private@example.test")
        staged.set()
        await finish.wait()

    async def writer() -> None:
        async with sealing.write_guard(scope.tenant, scope.subject):
            await db.run_owned(write)

    writing = asyncio.create_task(writer())
    async with pool.acquire() as cleaner:

        async def cleanup() -> str:
            async with cleaner.transaction():
                await erasure.revoke_in_transaction(cleaner, scope)
                return await cleaner.execute(f'DELETE FROM "{schema}".plaintext')

        async def blocked_on_fence() -> None:
            while not await pool.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted)",
                cleaner.get_server_pid(),
            ):
                await asyncio.sleep(0.01)

        cleaning: asyncio.Task[str] | None = None
        try:
            await asyncio.wait_for(staged.wait(), 3)
            cleaning = asyncio.create_task(cleanup())
            await asyncio.wait_for(blocked_on_fence(), 3)
            assert not cleaning.done(), "erasure must wait for the admitted destination writer"
            if outcome == "cancel":
                writing.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await writing
            else:
                finish.set()
                await asyncio.wait_for(writing, 3)
            assert await asyncio.wait_for(cleaning, 3) == ("DELETE 1" if outcome == "commit" else "DELETE 0")
        finally:
            for task in (writing, cleaning):
                if task is not None:
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
    assert keys.fence_calls == 1
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".plaintext') == 0

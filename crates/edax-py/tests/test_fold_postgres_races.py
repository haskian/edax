"""Real PostgreSQL lock, rollback and lost-acknowledgement boundaries for folds."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Literal, cast
from uuid import uuid4

import pytest
from edax import _json
from edax.backend.fold_postgres import PostgresFoldStore, reference_fold_ddl
from edax.backend.kafka_fold import KafkaFold
from edax.backend.postgres import PgConnection, PostgresTransactionFailure, PostgresTransactor
from edax.errors import BadHandle
from edax.fold import FoldEvent, FoldInput, FoldResult
from test_fold import event, initial_cutover, proof, reducer, target
from test_kafka_fold import TP, Client, Source, decode

URL = os.environ.get("EDAX_POSTGRES_URL")
if URL is None:
    if os.environ.get("EDAX_REQUIRE_POSTGRES") == "1":
        raise RuntimeError("EDAX_REQUIRE_POSTGRES=1 but EDAX_POSTGRES_URL is not set")
    pytest.skip("EDAX_POSTGRES_URL is not set: no live Postgres", allow_module_level=True)

asyncpg = pytest.importorskip("asyncpg")


class _FaultConnection:
    def __init__(self, connection: Any, faults: _FaultPool) -> None:
        self.connection, self.faults = connection, faults
        self.wrote_delta = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.connection, name)

    async def execute(self, query: str, *args: Any) -> str:
        faults = self.faults
        if query == faults.lock_sql and faults.staged.is_set():
            faults.waiter_pid = self.connection.get_server_pid()
            faults.waiter_started.set()
        result: str = await self.connection.execute(query, *args)
        if query == faults.write_sql:
            writes = _json.decode(args[0])
            if any(value.get("kind") == "intent" for value in writes.values()):
                self.wrote_delta = True
                if faults.pause_write:
                    faults.pause_write = False
                    faults.staged.set()
                    await faults.release.wait()
        if query.strip().upper().endswith("COMMIT") and self.wrote_delta and faults.lose_commit:
            faults.lose_commit = False
            # The server has committed. Only the acknowledgement is lost to the adapter.
            raise ConnectionError("fold COMMIT acknowledgement lost")
        return result


class _FaultPool:
    def __init__(self, pool: Any, store: PostgresFoldStore) -> None:
        self.pool = pool
        self.lock_sql, self.write_sql = store._sql["lock"], store._sql["write"]
        self.pause_write = False
        self.lose_commit = False
        self.staged, self.release, self.waiter_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.waiter_pid: int | None = None

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncIterator[PgConnection]:
        async with self.pool.acquire(timeout=timeout) as connection:
            yield cast(PgConnection, _FaultConnection(connection, self))


@pytest.fixture
async def database() -> AsyncIterator[tuple[Any, PostgresFoldStore, _FaultPool, str]]:
    pool = await asyncpg.create_pool(URL, min_size=1, max_size=5)
    schema = f"fold_race_{uuid4().hex}"
    await pool.execute(f'CREATE SCHEMA "{schema}"')
    try:
        await pool.execute(reference_fold_ddl(schema))
        transactor = PostgresTransactor(pool, schema=schema)
        store = PostgresFoldStore(transactor, target(), schema=schema)
        await store.initialize()
        await store.activate(initial_cutover(store.target))
        faults = _FaultPool(pool, store)
        transactor._pool = faults
        yield pool, store, faults, schema
    finally:
        await pool.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


async def _snapshot(pool: Any, schema: str) -> dict[str, Any]:
    rows = await pool.fetch(f'SELECT key, value::text AS value FROM "{schema}".edax_fold')
    return {row["key"]: _json.decode(row["value"]) for row in rows}


async def _wait_for_server_lock(pool: Any, faults: _FaultPool) -> None:
    await asyncio.wait_for(faults.waiter_started.wait(), 1)

    async def blocked() -> None:
        while not await pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks "
            "WHERE pid = $1 AND locktype = 'advisory' AND NOT granted)",
            faults.waiter_pid,
        ):
            await asyncio.sleep(0.005)

    await asyncio.wait_for(blocked(), 1)


@pytest.mark.parametrize("operation", ["takeover", "freeze"])
@pytest.mark.parametrize("writer_outcome", ["commit", "cancel"])
async def test_generation_lock_serializes_takeover_and_freeze(
    database: tuple[Any, PostgresFoldStore, _FaultPool, str],
    operation: Literal["takeover", "freeze"],
    writer_outcome: Literal["commit", "cancel"],
) -> None:
    pool, store, faults, _ = database
    lease = await store.acquire(0, "writer-a", proof(), lease_ms=500)
    faults.pause_write = True
    writer = asyncio.create_task(store.commit(lease, [FoldInput(0, event())], proof(), reducer))
    contender: asyncio.Task[Any] | None = None
    try:
        await asyncio.wait_for(faults.staged.wait(), 1)

        # Expiry does not bypass the transaction lock already held by writer A.
        async def expired() -> None:
            while (
                await pool.fetchval("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
                <= lease.expires_ms
            ):
                await asyncio.sleep(0.005)

        await asyncio.wait_for(expired(), 1)
        contender = asyncio.create_task(
            store.acquire(0, "writer-b", proof()) if operation == "takeover" else store.freeze()
        )
        await _wait_for_server_lock(pool, faults)
        assert not contender.done()
        if writer_outcome == "cancel":
            writer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await writer
        else:
            faults.release.set()
            assert (await writer).next_offset == 1
        result = await asyncio.wait_for(contender, 2)
        expected = 1 if writer_outcome == "commit" else 0
        if operation == "takeover":
            assert result.epoch == lease.epoch + 1
            assert result.next_offset == expected
            assert result.owner == "writer-b"
        else:
            assert result.cutoffs == {0: expected, 1: 0}
        state = await store.get("bank-eu", "account-42")
        assert (state is not None) == bool(expected)
        if state is not None:
            assert state["sequence"] == 1 and state["value"]["balance_minor"] == 1250
        assert len(await store.pending()) == expected
        with pytest.raises(BadHandle, match=r"stale fold|frozen"):
            await store.commit(lease, [FoldInput(1, event(2, "credit-2"))], proof(), reducer)
    finally:
        faults.release.set()
        for task in (writer, contender):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (writer, contender) if task is not None), return_exceptions=True
        )


async def test_cancel_after_first_staged_delta_rolls_back_every_record(
    database: tuple[Any, PostgresFoldStore, _FaultPool, str],
) -> None:
    pool, store, faults, schema = database
    lease = await store.acquire(0, "writer-a", proof())
    before = await _snapshot(pool, schema)
    faults.pause_write = True
    writer = asyncio.create_task(
        store.commit(lease, [FoldInput(0, event()), FoldInput(1, event(2, "credit-2"))], proof(), reducer)
    )
    try:
        await asyncio.wait_for(faults.staged.wait(), 1)
        assert await _snapshot(pool, schema) == before, "uncommitted state must remain invisible"
        writer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await writer
    finally:
        faults.release.set()
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)
    # Exact equality covers both dedup indexes, state, intents, checkpoint and manifest.
    assert await _snapshot(pool, schema) == before
    assert await store.recover(lease) == lease
    assert not await store.pending()
    committed = await store.commit(lease, [FoldInput(0, event())], proof(), reducer)
    assert committed.next_offset == 1
    assert len(await store.pending()) == 1


async def test_real_commit_with_lost_response_recovers_runner_without_reducing_again(
    database: tuple[Any, PostgresFoldStore, _FaultPool, str],
) -> None:
    pool, store, faults, schema = database
    client, source = Client(), Source()
    reduced: list[str] = []

    def tracked(state: Any, value: FoldEvent) -> FoldResult:
        reduced.append(value.event_id)
        return reducer(state, value)

    runner = KafkaFold(client, store, source, decode, tracked)
    await runner.rebalance_listener().on_partitions_assigned({TP})
    assigned = runner._parts[TP]
    faults.lose_commit = True
    with pytest.raises(PostgresTransactionFailure) as failed:
        await runner._partition(TP, assigned)
    assert failed.value.commit_started is True
    assert isinstance(failed.value.cause, ConnectionError)
    assert assigned.lease is not None and assigned.lease.next_offset == 0
    committed_records = await _snapshot(pool, schema)
    assert len(await store.pending()) == 3
    assert reduced == ["credit-1", "credit-2", "credit-3"]
    await runner._partition(TP, assigned)
    assert client.seeks == [0, 3], "recovery must seek the committed database cursor"
    assert assigned.lease.next_offset == 3
    assert reduced == ["credit-1", "credit-2", "credit-3"]
    state = await store.get("bank-eu", "account-42")
    assert state and state["sequence"] == 3 and state["value"]["balance_minor"] == 3750
    after = await _snapshot(pool, schema)
    # Renewal may change the lease expiry, but cannot replace any data or intent.
    for key, value in committed_records.items():
        if "expires_ms" not in value:
            assert after[key] == value
    assert after.keys() == committed_records.keys()
    assert runner.retained_records == runner.retained_bytes == 0

"""Ingestion transactions against a live PostgreSQL server, each test in an isolated schema."""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any, Literal, cast
from uuid import uuid4

import pytest
from edax import _json, _native
from edax.backend.ingest_postgres import PostgresIngestionSink, reference_ingestion_ddl
from edax.backend.postgres import PgConnection, PostgresTransactionFailure, PostgresTransactor
from edax.batch import BatchSettings, recover
from edax.errors import BadHandle, CodecError, ConcurrencyError
from edax.ingestion import (
    BatchInput,
    BatchResult,
    BatchSink,
    IngestionTarget,
    MemoryIngestionSink,
    Rejection,
    RejectionOutbox,
)
from edax.outbox import Relay
from edax.testing import TransactionalBatchSinkContract
from edax.unit_of_work import transaction

URL = os.environ.get("EDAX_POSTGRES_URL")
if URL is None:
    if os.environ.get("EDAX_REQUIRE_POSTGRES") == "1":
        raise RuntimeError("EDAX_REQUIRE_POSTGRES=1 but EDAX_POSTGRES_URL is not set")
    pytest.skip("EDAX_POSTGRES_URL is not set: no live Postgres", allow_module_level=True)

asyncpg = pytest.importorskip("asyncpg")


@pytest.fixture
async def database() -> AsyncIterator[tuple[Any, PostgresTransactor, str]]:
    pool = await asyncpg.create_pool(URL, min_size=1, max_size=5)
    schema = f"ingest_{uuid4().hex}"
    await pool.execute(f'CREATE SCHEMA "{schema}"')
    try:
        await pool.execute(reference_ingestion_ddl(schema))
        yield pool, PostgresTransactor(pool, schema=schema), schema
    finally:
        await pool.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


def sink_for(
    database: tuple[Any, PostgresTransactor, str], mode: Literal["append", "upsert"] = "upsert"
) -> PostgresIngestionSink:
    _, transactor, schema = database
    target = IngestionTarget(
        mode=mode, generation="orders-v1", source_incarnation="cluster-a/orders-1", mapper_id="order-state-v1"
    )
    return PostgresIngestionSink(transactor, target, schema=schema)


def record(
    sink: PostgresIngestionSink,
    offset: int,
    *,
    stream: str = "order-42",
    sequence: int = 1,
    state: str | None = "placed",
    event_id: str | None = None,
    failure: str | None = None,
) -> BatchInput:
    event_id = event_id or f"event-{offset}"
    key = event_id if sink.target.mode == "append" else "order-summary-42"
    return sink.target.input(
        topic="orders",
        partition=0,
        offset=offset,
        tenant="shop-a",
        row_key=key,
        stream=stream,
        event_id=event_id,
        sequence=sequence,
        content={"name": "OrderChanged", "aggregate_id": stream, "sequence": sequence, "state": state},
        private=None if state is None else {"order_id": "order-42", "state": state},
        raw=f"orders:0:{offset}:{stream}:{sequence}:{state}",
        failure=failure,
    )


async def test_append_replay_and_conflicting_identity(database: tuple[Any, PostgresTransactor, str]) -> None:
    sink = sink_for(database, "append")
    original = record(sink, 0)
    assert (await sink.commit([original])).outcomes == ("accepted",)
    assert (await sink.commit([original])).outcomes == ("accepted",)
    conflict = record(sink, 1, event_id=original.event_id, state="cancelled")
    assert (await sink.commit([conflict])).outcomes == ("rejected",)
    stored = await sink.get("shop-a", original.event_id)
    assert stored is not None and stored["private"]["state"] == "placed"
    assert list(await sink.pending()) == [conflict.source]


async def test_rejected_original_cannot_claim_owner_on_replay(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    sink = sink_for(database)
    poison = record(sink, 0, failure="codec: incomplete full-state event")
    assert (await sink.commit([poison])).outcomes == ("rejected",)
    assert await sink.get("shop-a", "order-summary-42") is None
    other_owner = record(sink, 1, stream="replacement-order", state="paid")
    result = await sink.commit([replace(poison, failure=None), other_owner])
    assert result.outcomes == ("rejected", "accepted")
    stored = await sink.get("shop-a", "order-summary-42")
    assert stored is not None and stored["stream"] == "replacement-order"


async def test_tombstone_and_older_updates_keep_owner(database: tuple[Any, PostgresTransactor, str]) -> None:
    sink = sink_for(database)
    placed, deleted = record(sink, 0), record(sink, 1, sequence=3, state=None)
    assert (await sink.commit([placed, deleted])).outcomes == ("accepted", "accepted")
    stale = record(sink, 2, sequence=2, state="paid")
    competing_owner = record(sink, 3, stream="other-order", sequence=4)
    assert (await sink.commit([stale, competing_owner])).outcomes == ("accepted", "rejected")
    stored = await sink.get("shop-a", "order-summary-42")
    assert stored is not None and stored["sequence"] == 3 and stored["private"] is None


async def test_concurrent_first_owner_is_preserved(database: tuple[Any, PostgresTransactor, str]) -> None:
    first, second = sink_for(database), sink_for(database)
    a, b = record(first, 0, stream="first-order"), record(second, 1, stream="second-order")
    results = await asyncio.gather(first.commit([a]), second.commit([b]))
    assert sorted(result.outcomes[0] for result in results) == ["accepted", "rejected"]
    winner = a if results[0].outcomes == ("accepted",) else b
    stored = await first.get("shop-a", "order-summary-42")
    assert stored is not None and stored["stream"] == winner.stream


async def test_concurrent_rejection_cannot_become_an_accepted_stale_replay(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    first = sink_for(database)
    _, transactor, schema = database
    before_target_read, resume = asyncio.Event(), asyncio.Event()

    class PausedSink(PostgresIngestionSink):
        async def _read(self, conn: PgConnection, role: str, keys: Sequence[str]) -> dict[str, Any]:
            if role == "state_metadata":
                before_target_read.set()
                await resume.wait()
            return await super()._read(conn, role, keys)

    second = PausedSink(transactor, first.target, schema=schema)
    original = record(first, 0)
    conflict = record(first, 1, state="cancelled")
    newer = record(first, 2, sequence=2, state="paid")
    await first.commit([original])
    await second.pending()  # Bind before racing, so manifest insertion cannot serialize the calls.

    replay = asyncio.create_task(second.commit([conflict]))
    try:
        await asyncio.wait_for(before_target_read.wait(), 5)
        decided = await asyncio.wait_for(first.commit([conflict, newer]), 5)
    finally:
        resume.set()
        repeated = await asyncio.wait_for(replay, 5)

    assert decided.outcomes == ("rejected", "accepted")
    assert repeated.outcomes == ("rejected",)
    assert repeated.rejections == decided.rejections
    # The caller still owes publication before acknowledging this source coordinate.
    assert repeated.rejections[conflict.source]["published"] is False
    assert await first.pending() == repeated.rejections
    await second.published({conflict.source: repeated.rejections[conflict.source]["fingerprint"]})
    assert await first.pending() == {}
    assert (await first.commit([conflict])).rejections[conflict.source]["published"] is True


@pytest.mark.parametrize("mode", ["append", "upsert"])
async def test_concurrent_absent_target_insert_rolls_back_then_replays(
    database: tuple[Any, PostgresTransactor, str], mode: Literal["append", "upsert"]
) -> None:
    first = sink_for(database, mode)
    _, transactor, schema = database
    both_read = asyncio.Event()
    readers = 0

    class ConcurrentSink(PostgresIngestionSink):
        async def _read(self, conn: PgConnection, role: str, keys: Sequence[str]) -> dict[str, Any]:
            nonlocal readers
            rows = await super()._read(conn, role, keys)
            if role.endswith("_metadata"):
                assert rows == {}
                readers += 1
                if readers == 2:
                    both_read.set()
                await asyncio.wait_for(both_read.wait(), 5)
            return rows

    sinks = [ConcurrentSink(transactor, first.target, schema=schema) for _ in range(2)]
    for sink in sinks:
        await sink.pending()
    original = record(first, 0)
    results = await asyncio.wait_for(
        asyncio.gather(*(sink.commit([original]) for sink in sinks), return_exceptions=True), 5
    )
    failures = [result for result in results if isinstance(result, BaseException)]
    assert len(failures) == 1
    failure = failures[0]
    assert isinstance(failure, PostgresTransactionFailure)
    assert isinstance(failure.cause, ConcurrencyError)
    assert failure.rolled_back and not failure.commit_started
    assert (await first.commit([original])).outcomes == ("accepted",)
    assert await first.pending() == {}


async def test_locked_target_does_not_block_an_unrelated_ingestion_row(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    first = sink_for(database)
    _, transactor, schema = database
    locked, resume = asyncio.Event(), asyncio.Event()

    class PausedSink(PostgresIngestionSink):
        async def _read(self, conn: PgConnection, role: str, keys: Sequence[str]) -> dict[str, Any]:
            rows = await super()._read(conn, role, keys)
            if role == "state_metadata":
                locked.set()
                await resume.wait()
            return rows

    held = PausedSink(transactor, first.target, schema=schema)
    original = record(first, 0)
    unrelated = replace(
        record(first, 10, stream="other-order"), key=first.target.row_key("shop-a", "other-summary")
    )
    await first.commit([original, unrelated])
    await held.pending()
    task = asyncio.create_task(held.commit([record(first, 1, sequence=2, state="paid")]))
    try:
        await asyncio.wait_for(locked.wait(), 5)
        update = replace(record(first, 11, stream="other-order", sequence=2), key=unrelated.key)
        other_result = await asyncio.wait_for(first.commit([update]), 5)
        assert other_result.outcomes == ("accepted",)
    finally:
        resume.set()
        await asyncio.wait_for(task, 5)


async def test_published_rejection_survives_original_replay(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    sink = sink_for(database)
    poison = record(sink, 0, failure="codec: bad event")
    await sink.commit([poison])
    pending = await sink.pending()
    assert pending[poison.source]["published"] is False
    with pytest.raises(BadHandle, match="fingerprint"):
        await sink.published({poison.source: "incorrect"})
    await sink.published({poison.source: pending[poison.source]["fingerprint"]})
    assert await sink.pending() == {}
    replay = await sink.commit([poison])
    assert replay.outcomes == ("rejected",)
    assert replay.rejections[poison.source]["published"] is True
    assert await sink.pending() == {}


@pytest.mark.parametrize("case", ["unknown_source", "changed_fingerprint"])
async def test_rejection_outbox_preserves_strict_publication_checks(
    database: tuple[Any, PostgresTransactor, str], case: str
) -> None:
    sink = sink_for(database)
    rejected = record(sink, 0, failure="codec: invalid order")
    await sink.commit([rejected])
    outbox = RejectionOutbox(sink)
    pending = await outbox.unpublished(100)
    changed = (
        replace(pending[0], source=record(sink, 1).source)
        if case == "unknown_source"
        else replace(pending[0], fingerprint="changed")
    )
    with pytest.raises(BadHandle):
        await outbox.mark_published([changed.id])
    assert await outbox.unpublished(100) == pending
    await outbox.mark_published([pending[0].id])
    await outbox.mark_published([pending[0].id])
    assert await outbox.unpublished(100) == []


@pytest.mark.parametrize("case", ["foreign_source", "foreign_fingerprint", "unknown_source"])
async def test_publication_mark_cannot_escape_target_generation(
    database: tuple[Any, PostgresTransactor, str], case: str
) -> None:
    pool, transactor, schema = database
    original = sink_for(database)
    poison = record(original, 0, failure="codec: invalid original order")
    result = await original.commit([poison])
    other = PostgresIngestionSink(transactor, replace(original.target, generation="orders-v2"), schema=schema)
    source = poison.source if case != "unknown_source" else record(other, 1).source
    fingerprint = result.rejections[poison.source]["fingerprint"]
    if case == "foreign_fingerprint":
        fingerprint = "incorrect"
    before = await pool.fetch(f'SELECT key, value FROM "{schema}".edax_ingest_rejections ORDER BY key')
    with pytest.raises(BadHandle, match=r"missing or.*fingerprint"):
        await other.published({source: fingerprint})
    after = await pool.fetch(f'SELECT key, value FROM "{schema}".edax_ingest_rejections ORDER BY key')
    assert after == before
    assert other._bound is False
    assert await original.pending() == result.rejections
    await original.published({poison.source: result.rejections[poison.source]["fingerprint"]})
    assert await original.pending() == {}


@pytest.mark.parametrize("limit", [True, False, 0, -1, 1.5, "1", None])
async def test_pending_refuses_invalid_page_limits(
    database: tuple[Any, PostgresTransactor, str], limit: Any
) -> None:
    sink = sink_for(database)
    rejected = record(sink, 0, failure="codec: invalid order")
    await sink.commit([rejected])
    pending = await sink.pending()
    with pytest.raises(BadHandle, match="page limit"):
        await sink.pending(limit)
    assert await sink.pending() == pending


async def test_pending_page_advances_after_confirmed_publication(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    sink = sink_for(database)
    rejected = [record(sink, offset, failure="codec: invalid order") for offset in range(2)]
    await sink.commit(rejected)
    pending = await sink.pending()
    assert list(pending) == [item.source for item in rejected]
    for item in rejected:
        page = await sink.pending(1)
        assert page == {item.source: pending[item.source]}
        assert page[item.source]["published"] is False
        await sink.published({item.source: page[item.source]["fingerprint"]})
        replay = await sink.commit([item])
        assert replay.rejections[item.source]["published"] is True
    assert await sink.pending(1) == {}


async def test_pending_index_skips_retained_published_history(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    pool, _, schema = database
    sink = sink_for(database)
    poison = record(sink, 0, failure="codec: invalid order")
    row = (await sink.commit([poison])).rejections[poison.source]
    history = [
        _native.ingest_source_key(sink.target.generation, sink.target.source_incarnation, "orders", 0, offset)
        for offset in range(1, 10_001)
    ]
    await pool.execute(
        f'INSERT INTO "{schema}".edax_ingest_rejections (key, value) '
        "SELECT source, $2::jsonb FROM jsonb_array_elements_text($1::jsonb) AS sources(source)",
        _json.encode(history),
        _json.encode({**row, "published": True}),
    )
    await pool.execute(f'ANALYZE "{schema}".edax_ingest_rejections')
    statements = _json.decode(_native.postgres_ingest_statements(_json.encode({"schema": schema})))

    assert await sink.pending(1) == {poison.source: row}
    await sink.published({poison.source: row["fingerprint"]})
    assert await sink.pending(1) == {}
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".edax_ingest_rejections') == 10_002

    explained = _json.decode(
        await pool.fetchval(
            "EXPLAIN (ANALYZE, FORMAT JSON) " + statements["pending"], sink.target.prefix, "100"
        )
    )
    nodes = [explained[0]["Plan"]]
    scanned_indexes: list[str] = []
    while nodes:
        node = nodes.pop()
        assert node.get("Rows Removed by Filter", 0) == 0, node
        assert node["Node Type"] != "Seq Scan", node
        if "Index Name" in node:
            scanned_indexes.append(node["Index Name"])
        nodes.extend(node.get("Plans", []))
    assert scanned_indexes
    for index in scanned_indexes:
        assert await pool.fetchval(
            "SELECT i.indpred IS NOT NULL FROM pg_catalog.pg_index i "
            "JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = $1 AND c.relname = $2",
            schema,
            index,
        )


async def test_ambient_transaction_and_changed_manifest_are_refused(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    _, transactor, schema = database
    sink = sink_for(database)
    original = record(sink, 0)
    async with transaction(transactor):
        with pytest.raises(BadHandle, match="ambient"):
            await sink.commit([original])
    assert await sink.get("shop-a", "order-summary-42") is None
    await sink.commit([original])
    changed = PostgresIngestionSink(
        transactor, replace(sink.target, mapper_id="order-state-v2"), schema=schema
    )
    with pytest.raises(BadHandle):
        await changed.commit([record(changed, 1, sequence=2)])
    stored = await sink.get("shop-a", "order-summary-42")
    assert stored is not None and stored["sequence"] == 1


class CommitReplyLost(Exception):
    # Looks record-local, but the successful COMMIT makes every subsequent error uncertain.
    sqlstate = "22003"


class _FaultConnection:
    def __init__(self, connection: Any, pool: _FaultPool) -> None:
        self.connection = connection
        self.pool = pool

    def __getattr__(self, name: str) -> Any:
        return getattr(self.connection, name)

    async def execute(self, query: str, *args: Any) -> str:
        if self.pool.rollback_fails and query.strip().upper().endswith("ROLLBACK"):
            raise ConnectionError("rollback reply unavailable")
        result = await self.connection.execute(query, *args)
        if self.pool.lose_commit and query.strip().upper().endswith("COMMIT"):
            self.pool.lose_commit = False
            raise CommitReplyLost("commit succeeded but its response was lost")
        return result


class _FaultPool:
    def __init__(self, pool: Any, *, lose_commit: bool = False, rollback_fails: bool = False) -> None:
        self.pool = pool
        self.lose_commit = lose_commit
        self.rollback_fails = rollback_fails

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncIterator[PgConnection]:
        async with self.pool.acquire(timeout=timeout) as connection:
            yield cast(PgConnection, _FaultConnection(connection, self))


async def test_lost_commit_reply_retries_original_without_duplicate(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    pool, transactor, _ = database
    transactor._pool = _FaultPool(pool, lose_commit=True)
    sink = sink_for(database)
    original = record(sink, 0)
    with pytest.raises(PostgresTransactionFailure) as failed:
        await sink.commit([original])
    assert failed.value.commit_started
    assert recover(failed.value, BatchSettings(), attempt=1, count=1, isolable=True)["action"] == "retry"
    assert sink._bound is False, "a lost COMMIT reply cannot cache manifest binding"
    stored = await sink.get("shop-a", "order-summary-42")
    assert stored is not None and stored["sequence"] == 1
    assert (await sink.commit([original])).outcomes == ("accepted",)
    assert sink._bound is True
    assert await sink.pending() == {}


@pytest.mark.parametrize("rollback_fails,action", [(False, "split"), (True, "retry")])
async def test_constraint_failure_requires_confirmed_rollback(
    database: tuple[Any, PostgresTransactor, str], rollback_fails: bool, action: str
) -> None:
    pool, transactor, schema = database
    await pool.execute(
        f'ALTER TABLE "{schema}".edax_ingest_state ADD CHECK '
        "(value->'private'->>'state' IS DISTINCT FROM 'forbidden')"
    )
    if rollback_fails:
        transactor._pool = _FaultPool(pool, rollback_fails=True)
    sink = sink_for(database)
    with pytest.raises(PostgresTransactionFailure) as failed:
        await sink.commit([record(sink, 0), record(sink, 1, sequence=2, state="forbidden")])
    assert isinstance(failed.value.cause, CodecError)
    assert recover(failed.value, BatchSettings(), attempt=1, count=2, isolable=True)["action"] == action
    assert await sink.get("shop-a", "order-summary-42") is None
    assert await sink.pending() == {}


@pytest.mark.parametrize(
    "change",
    [
        "ENABLE ROW LEVEL SECURITY",
        "ALTER COLUMN value DROP NOT NULL",
        "DROP CONSTRAINT edax_ingest_state_pkey",
        "ADD COLUMN required text NOT NULL",
        "RENAME COLUMN value TO other_value",
    ],
)
async def test_incompatible_schema_fails_before_writes(
    database: tuple[Any, PostgresTransactor, str], change: str
) -> None:
    pool, _, schema = database
    await pool.execute(f'ALTER TABLE "{schema}".edax_ingest_state {change}')
    sink = sink_for(database)
    with pytest.raises(BadHandle, match="contract"):
        await sink.commit([record(sink, 0)])
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".edax_ingest_state') == 0


async def test_bound_sink_does_not_serialize_unrelated_rows(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    pool, _, _ = database
    sink = sink_for(database)
    blocked = record(sink, 0)
    await sink.commit([blocked])
    blocked = replace(record(sink, 2), key=blocked.key, sequence=2)
    independent = replace(record(sink, 1), key=sink.target.key(["upsert", "shop-a", None, "other-summary"]))
    async with pool.acquire() as connection:
        await connection.execute("BEGIN")
        # Hold the committed row the next batch must lock, as a concurrent writer of that key would.
        _, _, schema = database
        await connection.fetch(
            f'SELECT 1 FROM "{schema}".edax_ingest_state WHERE key = $1 FOR UPDATE', blocked.key
        )
        task = asyncio.create_task(sink.commit([blocked]))
        try:
            for _ in range(100):
                waiting = await connection.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'transactionid' AND NOT granted)"
                )
                if waiting:
                    break
                await asyncio.sleep(0.005)
            assert waiting, "the first transaction must reach its held row lock"
            result = await asyncio.wait_for(sink.commit([independent]), timeout=1)
            assert result.outcomes == ("accepted",)
        finally:
            await connection.execute("ROLLBACK")
            await task
    stored = await sink.get("shop-a", "other-summary")
    assert stored is not None and stored["sequence"] == 1


async def test_shared_append_events_bind_manifest_despite_different_state_tables(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    pool, transactor, schema = database
    first = sink_for(database, "append")
    original = record(first, 0)
    await first.commit([original])
    await pool.execute(
        f'CREATE TABLE "{schema}".alternate_state (LIKE "{schema}".edax_ingest_state INCLUDING ALL)'
    )
    second = PostgresIngestionSink(
        transactor,
        replace(first.target, mapper_id="order-state-v2"),
        schema=schema,
        tables={"ingest_state": "alternate_state"},
    )
    different = record(second, 1)
    with pytest.raises(BadHandle):
        await second.commit([different])
    assert await first.get("shop-a", different.event_id) is None
    stored = await first.get("shop-a", original.event_id)
    assert stored is not None and stored["private"]["state"] == "placed"
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".alternate_state') == 0


@pytest.mark.parametrize("change", ["mode", "mapper_and_table"])
async def test_shared_rejections_bind_contract_across_target_tables(
    database: tuple[Any, PostgresTransactor, str], change: str
) -> None:
    pool, transactor, schema = database
    first = sink_for(database, "append")
    poison = record(first, 0, failure="codec: invalid under original contract")
    assert (await first.commit([poison])).outcomes == ("rejected",)
    pending = await first.pending()
    tables = {}
    changed = replace(first.target, mode="upsert")
    if change == "mapper_and_table":
        await pool.execute(
            f'CREATE TABLE "{schema}".alternate_events (LIKE "{schema}".edax_ingest_events INCLUDING ALL)'
        )
        tables = {"ingest_events": "alternate_events"}
        changed = replace(first.target, mapper_id="order-state-v2")
    second = PostgresIngestionSink(transactor, changed, schema=schema, tables=tables)
    with pytest.raises(BadHandle):
        await second.commit([record(second, 0)])
    assert second._bound is False
    table = "alternate_events" if change == "mapper_and_table" else "edax_ingest_state"
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') == 0
    assert await first.pending() == pending
    # A matching sink may still replay and publish the original rejection.
    replay = sink_for(database, "append")
    assert (await replay.commit([poison])).outcomes == ("rejected",)
    await replay.published({poison.source: pending[poison.source]["fingerprint"]})
    assert await replay.pending() == {}


@pytest.mark.parametrize("operation", ["pending", "published", "get", "drain"])
async def test_read_and_publication_entry_points_refuse_changed_contract(
    database: tuple[Any, PostgresTransactor, str], operation: str
) -> None:
    _, transactor, schema = database
    original = sink_for(database)
    poison = record(original, 0, failure="codec: invalid original event")
    await original.commit([poison])
    pending = await original.pending()
    changed = PostgresIngestionSink(
        transactor, replace(original.target, mapper_id="order-state-v2"), schema=schema
    )
    delivered: list[str] = []

    async def publish(row: Rejection) -> None:
        delivered.append(row.source)

    with pytest.raises(BadHandle, match="contract"):
        if operation == "pending":
            await changed.pending()
        elif operation == "published":
            await changed.published({poison.source: pending[poison.source]["fingerprint"]})
        elif operation == "get":
            await changed.get("shop-a", "order-summary-42")
        else:
            await Relay(RejectionOutbox(changed), publish).drain()
    assert changed._bound is False
    assert delivered == []
    assert await original.pending() == pending


@pytest.mark.parametrize("mode", ["append", "upsert"])
async def test_planning_reads_metadata_without_historical_payload(
    database: tuple[Any, PostgresTransactor, str],
    mode: Literal["append", "upsert"],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = sink_for(database, mode)
    original = replace(record(sink, 0), private={"order_id": "order-42", "history": "x" * (1024 * 1024)})
    await sink.commit([original])
    native_plan = _native.ingest_plan
    observed: list[dict[str, Any]] = []

    def plan(mode: str, inputs: str, stored: str, rejected: str) -> str:
        observed.append(_json.decode(stored))
        return native_plan(mode, inputs, stored, rejected)

    monkeypatch.setattr(_native, "ingest_plan", plan)
    if mode == "upsert":
        result = await sink.commit([record(sink, 1, sequence=0)])
    else:
        result = await sink.commit([original])
    assert result.outcomes == ("accepted",)
    assert len(observed) == 1
    assert set(observed[0][original.key]) == {"stream", "sequence", "fingerprint"}
    assert len(_json.encode(observed[0])) < 1024
    row_key = original.event_id if mode == "append" else "order-summary-42"
    stored = await sink.get("shop-a", row_key)
    assert stored is not None and stored["private"] == original.private


@pytest.mark.parametrize("backend", ["postgres", "memory"])
async def test_replay_uses_current_raw_without_replacing_durable_original(
    database: tuple[Any, PostgresTransactor, str], backend: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    postgres = sink_for(database)
    sink: BatchSink = postgres if backend == "postgres" else MemoryIngestionSink(postgres.target)
    original = replace(
        record(postgres, 0, failure="codec: invalid order"),
        raw=_json.encode({"topic": "orders", "partition": 0, "offset": 0, "value": "x" * (1024 * 1024)}),
        raw_fingerprint=_native.ingest_raw_fingerprint("stable original source record"),
    )
    await sink.commit([original])
    durable = await sink.pending()
    replay = replace(original, raw=_json.encode({"topic": "orders", "offset": 0, "oversized": True}))
    native_plan = _native.ingest_plan
    read_sizes: list[int] = []

    def plan(mode: str, inputs: str, stored: str, rejected: str) -> str:
        read_sizes.append(len(rejected))
        return native_plan(mode, inputs, stored, rejected)

    monkeypatch.setattr(_native, "ingest_plan", plan)
    result = await sink.commit([replay])
    assert result.outcomes == ("rejected",)
    assert result.rejections[replay.source] == {**durable[replay.source], "raw": replay.raw}
    assert await sink.pending() == durable
    if backend == "postgres":
        assert len(read_sizes) == 1 and read_sizes[0] < 1024
    with pytest.raises(BadHandle, match="different raw"):
        changed = _native.ingest_raw_fingerprint("another source record")
        await sink.commit([replace(replay, raw_fingerprint=changed)])
    await sink.published({replay.source: result.rejections[replay.source]["fingerprint"]})
    published = await sink.commit([replay])
    assert published.rejections[replay.source]["published"] is True
    assert await sink.pending() == {}


@pytest.mark.parametrize("mode", ["append", "upsert"])
async def test_numeric_state_preserves_binary64_on_insert_and_update(
    database: tuple[Any, PostgresTransactor, str], mode: Literal["append", "upsert"]
) -> None:
    sink = sink_for(database, mode)
    memory = MemoryIngestionSink(sink.target)
    numbers = [5.808820077685227e16, -5.808820077685227e16, 1e21, 1e23]
    for offset, number in enumerate(numbers):
        original = replace(
            record(sink, offset, sequence=offset + 1),
            private={"total": number, "nested": [number, None]},
            public={"balance": number},
        )
        for target in (memory, sink):
            assert (await target.commit([original])).outcomes == ("accepted",)
        key = original.event_id if mode == "append" else "order-summary-42"
        expected = await memory.get("shop-a", key)
        actual = await sink.get("shop-a", key)
        assert actual == expected
        assert actual is not None
        assert type(actual["public"]["balance"]) is float
        assert actual["public"]["balance"] == number


async def test_ingestion_row_lock_wait_is_uncounted_contention(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    from edax.errors import InProgress

    pool, _, schema = database
    sink = sink_for(database)
    saved = record(sink, 0)
    await sink.commit([saved])
    async with pool.acquire() as connection, connection.transaction():
        await connection.fetchval(
            f'SELECT key FROM "{schema}".edax_ingest_state WHERE key=$1 FOR UPDATE', saved.key
        )
        with pytest.raises(PostgresTransactionFailure) as failed:
            await sink.commit([record(sink, 1, sequence=2)])
        assert isinstance(failed.value.cause, InProgress)
        assert failed.value.rolled_back and not failed.value.commit_started
    assert (await sink.commit([record(sink, 1, sequence=2)])).outcomes == ("accepted",)


@pytest.mark.parametrize("rollback", [False, True], ids=["commit", "rollback"])
async def test_stage_joins_unit_and_keeps_result_provisional(
    database: tuple[Any, PostgresTransactor, str], rollback: bool
) -> None:
    from edax.ingestion import BatchResult, StagedBatchResult

    _, transactor, _ = database
    sink = sink_for(database)
    accepted = record(sink, 0)
    rejected = record(sink, 1, sequence=2, failure="codec: invalid reading")
    with pytest.raises(RuntimeError) if rollback else contextlib.nullcontext():
        async with transaction(transactor) as tx:
            result = await sink.stage([accepted, rejected])
            assert isinstance(result, StagedBatchResult) and not isinstance(result, BatchResult)
            assert result.outcomes == ("accepted", "rejected")
            staged_row = await sink.get("shop-a", "order-summary-42", tx=tx)
            assert staged_row is not None and staged_row["sequence"] == 1
            # An independent connection sees neither the staged payload nor its rejection.
            pool, _, schema = database
            assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".edax_ingest_state') == 0
            assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".edax_ingest_rejections') == 0
            if rollback:
                raise RuntimeError("handler failed after staging")
    assert (await sink.get("shop-a", "order-summary-42") is None) == rollback
    assert bool(await sink.pending()) != rollback
    # Rollback must not cache a manifest that never committed.
    assert (await sink.commit([accepted])).outcomes == ("accepted",)


async def test_stage_requires_live_transaction_in_its_own_task(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    _, transactor, _ = database
    sink = sink_for(database)
    original = record(sink, 0)
    with pytest.raises(BadHandle):
        await sink.stage([original])
    async with transaction(transactor) as tx:
        with pytest.raises(BadHandle):
            await asyncio.create_task(sink.stage([original]))
        with pytest.raises(BadHandle):
            await sink.stage([original], tx=object())
    with pytest.raises(BadHandle):
        await sink.stage([original], tx=tx)
    assert await sink.get("shop-a", "order-summary-42") is None


@pytest.mark.parametrize("operation", ["stage", "get"])
async def test_ingestion_refuses_explicit_handle_different_from_ambient(
    database: tuple[Any, PostgresTransactor, str], operation: str
) -> None:
    from edax.sealing import MemoryKeys, Sealing
    from test_brokerless_ingestion import reading

    pool, transactor, schema = database
    keys = MemoryKeys()
    sealing = Sealing(keys, "clinic-keys")
    target = IngestionTarget("append", "pulse-v1", "bridge-v1", "pulse-reading-v1")
    sink = PostgresIngestionSink(transactor, target, schema=schema, sealing=sealing)
    item = reading(target)
    async with transactor.begin() as explicit:
        async with transaction(transactor) as ambient:
            with pytest.raises(BadHandle, match="differs from the ambient transaction"):
                if operation == "stage":
                    await sink.stage([item], tx=explicit)
                else:
                    await sink.get("clinic-a", "reading-1", "patient-42", tx=explicit)
            assert not explicit.aborted and not ambient.aborted
        # The ambient unit's resources are gone, but the supplied transaction remains open.
        # Admission must have refused the write before it could retain a fence on that unit.
        await keys.revoke(sealing.scope("clinic-a", "patient-42"))
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".edax_ingest_events') == 0
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".edax_ingest_rejections') == 0


@pytest.mark.parametrize("rollback", [False, True], ids=["commit", "rollback"])
async def test_stage_retains_erasure_guard_until_outer_end(
    database: tuple[Any, PostgresTransactor, str], rollback: bool
) -> None:
    from edax.sealing import MemoryKeys, Sealing

    pool, transactor, schema = database
    keys = MemoryKeys()
    sealing = Sealing(keys, "clinic-keys")
    target = IngestionTarget("append", "pulse-v1", "bridge-v1", "pulse-reading-v1")
    sink = PostgresIngestionSink(transactor, target, schema=schema, sealing=sealing)
    from test_brokerless_ingestion import reading

    original = reading(target)
    scope = sealing.scope("clinic-a", "patient-42")
    started = asyncio.Event()
    may_revoke = asyncio.Event()

    async def revoke() -> str:
        await may_revoke.wait()
        started.set()
        return await keys.revoke(scope)

    revoking = asyncio.create_task(revoke())
    with pytest.raises(RuntimeError) if rollback else contextlib.nullcontext():
        async with transaction(transactor):
            # Forging redundant host metadata must not move the kernel-key scope fence.
            staged = await sink.stage([replace(original, subject="different-patient", tenant=None)])
            assert staged.outcomes == ("accepted",)
            may_revoke.set()
            await started.wait()
            await asyncio.sleep(0)
            assert not revoking.done(), "revocation must wait after stage() returns"
            if rollback:
                raise RuntimeError("outer rollback")
    await asyncio.wait_for(revoking, 1)
    # Inspect ciphertext only: revocation now prevents opening the row.
    row_count = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".edax_ingest_events WHERE key=$1', original.key
    )
    assert row_count == (0 if rollback else 1)


async def test_caught_stage_failure_aborts_prior_staged_writes(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    from edax.errors import StoreError

    _, transactor, _ = database
    sink = sink_for(database)
    original = record(sink, 0)
    with pytest.raises(StoreError, match="rolled back"):
        async with transaction(transactor):
            await sink.stage([original])
            with pytest.raises(BadHandle):
                await sink.stage([replace(original, key="wrong-target")])
    assert await sink.get("shop-a", "order-summary-42") is None


@pytest.mark.parametrize("placement", ["outside", "inside"])
@pytest.mark.parametrize("rollback", [False, True], ids=["commit", "rollback"])
async def test_stage_reuses_dispatch_guard_through_transaction_end(
    database: tuple[Any, PostgresTransactor, str], placement: str, rollback: bool
) -> None:
    from edax._units import current_transaction_guard
    from edax.sealing import Sealing
    from test_brokerless_ingestion import NonReentrantKeys, reading

    pool, transactor, schema = database
    keys = NonReentrantKeys()
    sealing = Sealing(keys, "clinic-keys")
    target = IngestionTarget("append", "pulse-v1", "bridge-v1", "pulse-reading-v1")
    sink = PostgresIngestionSink(transactor, target, schema=schema, sealing=sealing)
    item = reading(target)

    async def stage_twice() -> None:
        for _ in range(2):
            assert (await sink.stage([item])).outcomes == ("accepted",)
        assert keys.acquisitions == 1

    async def write() -> None:
        async with contextlib.AsyncExitStack() as resources:
            if placement == "outside":
                await resources.enter_async_context(
                    current_transaction_guard(sealing.write_guard("clinic-a", "patient-42"))
                )
            await resources.enter_async_context(transaction(transactor))
            if placement == "inside":
                async with current_transaction_guard(sealing.write_guard("clinic-a", "patient-42")):
                    await stage_twice()
            else:
                await stage_twice()
            assert keys.lock.locked(), "dispatch return must not release the transaction's fence"
            if rollback:
                raise RuntimeError("handler failed after dispatch")

    with pytest.raises(RuntimeError, match="handler failed") if rollback else contextlib.nullcontext():
        await asyncio.wait_for(write(), 2)
    assert keys.acquisitions == 1 and not keys.lock.locked()
    count = await pool.fetchval(f'SELECT count(*) FROM "{schema}".edax_ingest_events WHERE key=$1', item.key)
    assert count == (0 if rollback else 1)


async def test_stage_refuses_lexical_guard_that_ends_before_transaction(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    from edax.errors import StoreError
    from edax.sealing import Sealing
    from test_brokerless_ingestion import NonReentrantKeys, reading

    pool, transactor, schema = database
    keys = NonReentrantKeys()
    sealing = Sealing(keys, "clinic-keys")
    target = IngestionTarget("append", "pulse-v1", "bridge-v1", "pulse-reading-v1")
    sink = PostgresIngestionSink(transactor, target, schema=schema, sealing=sealing)

    async def write() -> None:
        async with transaction(transactor) as tx:
            async with sealing.write_guard("clinic-a", "patient-42"):
                with pytest.raises(BadHandle, match="scope guard does not cover this transaction"):
                    await sink.stage([reading(target)])
                assert tx.aborted
            assert not keys.lock.locked()

    with pytest.raises(StoreError, match="rolled back"):
        await asyncio.wait_for(write(), 2)
    assert keys.acquisitions == 1
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".edax_ingest_events') == 0
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".edax_ingest_rejections') == 0


class TestPostgresIngestionContract(TransactionalBatchSinkContract):
    @pytest.fixture(params=["append", "upsert"])
    def ingestion_sink(
        self, database: tuple[Any, PostgresTransactor, str], request: pytest.FixtureRequest
    ) -> PostgresIngestionSink:
        return sink_for(database, request.param)

    @pytest.fixture
    def transactor(self, database: tuple[Any, PostgresTransactor, str]) -> PostgresTransactor:
        return database[1]


async def test_explicit_read_checks_manifest(database: tuple[Any, PostgresTransactor, str]) -> None:
    _, transactor, schema = database
    sink = sink_for(database)
    await sink.commit([record(sink, 0)])
    incompatible = PostgresIngestionSink(
        transactor, replace(sink.target, mapper_id="different"), schema=schema
    )
    with pytest.raises(BadHandle):
        async with transaction(transactor) as tx:
            await incompatible.get("shop-a", "order-summary-42", tx=tx)


async def test_cancelled_stage_rolls_back_all_writes(
    database: tuple[Any, PostgresTransactor, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _, transactor, _ = database
    sink = sink_for(database)
    applied = asyncio.Event()
    apply = sink._apply

    async def suspend_after_apply(conn: PgConnection, originals: Sequence[BatchInput]) -> BatchResult:
        result = await apply(conn, originals)
        applied.set()
        await asyncio.Event().wait()
        return result

    monkeypatch.setattr(sink, "_apply", suspend_after_apply)

    async def write() -> None:
        async with transaction(transactor):
            await sink.stage([record(sink, 0), record(sink, 1, failure="codec: invalid reading")])

    writer = asyncio.create_task(write())
    await asyncio.wait_for(applied.wait(), 1)
    writer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await writer
    assert await sink.get("shop-a", "order-summary-42") is None
    assert await sink.pending() == {}


@pytest.mark.parametrize("mode", ["append", "upsert"])
async def test_staged_and_owned_writers_share_one_conflict_contract(
    database: tuple[Any, PostgresTransactor, str], mode: Literal["append", "upsert"]
) -> None:
    _, transactor, _ = database
    sink = sink_for(database, mode)
    await sink.commit([])

    async def write(index: int) -> tuple[int, str]:
        item = record(sink, index, state=f"reading-{index}", event_id="shared-reading")
        for attempt in range(4):
            try:
                if index % 2:
                    return index, (await sink.commit([item])).outcomes[0]
                async with transaction(transactor):
                    result = await sink.stage([item])
                return index, result.outcomes[0]
            except PostgresTransactionFailure as failure:
                assert failure.rolled_back and not failure.commit_started
                assert isinstance(failure.cause, ConcurrencyError)
                if attempt == 3:
                    raise
            except ConcurrencyError:
                if attempt == 3:
                    raise
        raise AssertionError("bounded transaction retry exhausted")

    outcomes = await asyncio.gather(*(write(index) for index in range(16)))
    winners = [index for index, outcome in outcomes if outcome == "accepted"]
    assert len(winners) == 1
    assert sum(outcome == "rejected" for _, outcome in outcomes) == 15
    key = "shared-reading" if mode == "append" else "order-summary-42"
    saved = await sink.get("shop-a", key)
    assert saved is not None and saved["private"]["state"] == f"reading-{winners[0]}"
    assert len(await sink.pending()) == 15


async def test_rolled_back_stage_does_not_cache_manifest_binding(
    database: tuple[Any, PostgresTransactor, str],
) -> None:
    _, transactor, schema = database
    first = sink_for(database)
    with pytest.raises(RuntimeError, match="rollback"):
        async with transaction(transactor):
            await first.stage([record(first, 0)])
            raise RuntimeError("rollback")
    replacement = PostgresIngestionSink(
        transactor, replace(first.target, mapper_id="new-mapper"), schema=schema
    )
    assert (await replacement.commit([record(replacement, 0)])).outcomes == ("accepted",)
    with pytest.raises(BadHandle):
        await first.commit([record(first, 0)])


@pytest.mark.parametrize("managed", [True, False], ids=["ambient-unit", "explicit-handle"])
async def test_stage_database_fence_drains_before_revocation(
    database: tuple[Any, PostgresTransactor, str], managed: bool
) -> None:
    from edax.backend.erasure_postgres import PostgresErasureStore
    from edax.backend.erasure_postgres import reference_ddl as reference_erasure_ddl
    from edax.errors import Rejected
    from edax.sealing import Sealing
    from test_brokerless_ingestion import reading
    from test_erasure_postgres import PostgresFence, wait_for_blocked_lock

    pool, transactor, schema = database
    await pool.execute(reference_erasure_ddl(schema))
    erasure = PostgresErasureStore(pool, schema=schema)
    sealing = Sealing(PostgresFence(transactor, erasure), "clinic-keys", write_fence="transaction")
    target = IngestionTarget("append", "pulse-v1", "bridge-v1", "pulse-v1")
    sink = PostgresIngestionSink(transactor, target, schema=schema, sealing=sealing)
    item = reading(target)
    scope = sealing.scope("clinic-a", "patient-42")
    async with pool.acquire() as cleaner:

        async def revoke() -> None:
            async with cleaner.transaction():
                await erasure.revoke_in_transaction(cleaner, scope)

        cleaner_task = None
        try:
            async with transaction(transactor) if managed else transactor.begin() as tx:
                assert (await sink.stage([item], tx=None if managed else tx)).outcomes == ("accepted",)
                cleaner_task = asyncio.create_task(revoke())
                await wait_for_blocked_lock(pool, cleaner.get_server_pid())
                assert not cleaner_task.done()
            await asyncio.wait_for(cleaner_task, 3)
        finally:
            if cleaner_task is not None and not cleaner_task.done():
                cleaner_task.cancel()
                await asyncio.gather(cleaner_task, return_exceptions=True)
    with pytest.raises(Rejected):
        async with transaction(transactor):
            await sink.stage([item])

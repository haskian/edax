"""Partitioned append ingestion against PostgreSQL, with application-owned child tables."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from dataclasses import replace
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from edax.backend.ingest_postgres import PostgresIngestionSink, reference_ingestion_ddl
from edax.backend.postgres import PostgresTransactionFailure, PostgresTransactor
from edax.batch import BatchSettings, recover
from edax.errors import BadHandle, CodecError, StoreError
from edax.ingestion import BatchInput, IngestionTarget, MappedRow
from edax.unit_of_work import transaction

URL = os.environ.get("EDAX_POSTGRES_URL")
if URL is None:
    if os.environ.get("EDAX_REQUIRE_POSTGRES") == "1":
        raise RuntimeError("EDAX_REQUIRE_POSTGRES=1 but EDAX_POSTGRES_URL is not set")
    pytest.skip("EDAX_POSTGRES_URL is not set: no live Postgres", allow_module_level=True)

asyncpg = pytest.importorskip("asyncpg")
Database = tuple[Any, PostgresTransactor, str]


@pytest.fixture
async def database() -> AsyncIterator[Database]:
    schema = f"partition_{uuid4().hex}"
    pool = await asyncpg.create_pool(
        URL, min_size=1, max_size=6, server_settings={"application_name": schema}
    )
    await pool.execute(f'CREATE SCHEMA "{schema}"')
    try:
        await pool.execute(reference_ingestion_ddl(schema, partitioned=True))
        for name, lower, upper in (
            ("early", "1970-01-01 00:00:00+00", "1970-01-01 00:00:00.002+00"),
            ("later", "1970-01-01 00:00:00.002+00", "1970-01-01 00:00:00.004+00"),
        ):
            await pool.execute(
                f'CREATE TABLE "{schema}"."{name}" PARTITION OF "{schema}".edax_ingest_events '
                f"FOR VALUES FROM ('{lower}') TO ('{upper}')"
            )
        yield pool, PostgresTransactor(pool, schema=schema), schema
    finally:
        await pool.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


def sink_for(database: Database, generation: str = "pulse-v1") -> PostgresIngestionSink:
    _, transactor, schema = database
    target = IngestionTarget("append", generation, "device-42-v1", "pulse-v1", partition="event_id_uuid7")
    return PostgresIngestionSink(transactor, target, schema=schema)


def event_id(milliseconds: int, ordinal: int = 1) -> str:
    time = f"{milliseconds:012x}"
    return f"{time[:8]}-{time[8:]}-7000-8000-{ordinal:012x}"


def record(
    sink: PostgresIngestionSink,
    source: int,
    *,
    milliseconds: int = 1,
    identity: str | None = None,
    pulse: int = 72,
) -> BatchInput:
    identity = identity or event_id(milliseconds, source + 1)
    content = {"reading_id": identity, "device_id": "device-42", "pulse_bpm": pulse}
    return sink.target.input_from_source(
        f"device-42/reading-{source}",
        event_id=identity,
        stream="device-42",
        row=MappedRow(identity, public=content),
        content=content,
        raw=json.dumps(content),
        tenant="clinic-a",
    )


async def test_replay_does_not_write_and_retention_expires_it(database: Database) -> None:
    pool, _, schema = database
    sink = sink_for(database)
    original = record(sink, 0)
    assert (await sink.commit([original])).outcomes == ("accepted",)
    before = await pool.fetchrow(
        f'SELECT xmin::text, value::text FROM "{schema}".edax_ingest_events WHERE key=$1', original.key
    )
    assert (await sink.commit([original])).outcomes == ("accepted",)
    after = await pool.fetchrow(
        f'SELECT xmin::text, value::text FROM "{schema}".edax_ingest_events WHERE key=$1', original.key
    )
    assert before == after
    assert await sink.advance_horizon(2000) == 2000
    await pool.execute(f'DROP TABLE "{schema}".early')
    assert (await sink.commit([original])).outcomes == ("expired",)
    assert await sink.get("clinic-a", original.event_id) is None
    assert await sink.pending() == {}


async def test_floor_is_exact_monotonic_and_inherited_by_new_generations(database: Database) -> None:
    sink = sink_for(database)
    assert await sink.advance_horizon(2000) == 2000
    assert await sink.advance_horizon(1000) == 2000
    originals = [record(sink, 0), record(sink, 1, milliseconds=2), record(sink, 2, milliseconds=3)]
    assert (await sink.commit(originals)).outcomes == ("expired", "accepted", "accepted")
    assert await sink.advance_horizon(2001) == 2001
    newer = sink_for(database, "pulse-v2")
    assert (await newer.commit([record(newer, 0, milliseconds=2)])).outcomes == ("expired",)
    assert await newer.get("clinic-a", event_id(2)) is None


async def test_retained_rejection_wins_after_expiry_and_keeps_publication(database: Database) -> None:
    pool, _, schema = database
    sink = sink_for(database)
    original = record(sink, 0)
    conflict = record(sink, 1, identity=original.event_id, pulse=90)
    assert (await sink.commit([original, conflict])).outcomes == ("accepted", "rejected")
    pending = await sink.pending()
    assert pending[conflict.source]["published"] is False
    assert await sink.advance_horizon(2000) == 2000
    await pool.execute(f'DROP TABLE "{schema}".early')
    replay = await sink.commit([conflict])
    assert replay.outcomes == ("rejected",)
    assert replay.rejections == pending
    await sink.published({conflict.source: pending[conflict.source]["fingerprint"]})
    assert (await sink.commit([conflict])).rejections[conflict.source]["published"] is True
    assert await sink.pending() == {}
    with pytest.raises(BadHandle, match="source coordinate reused"):
        await sink.commit([replace(conflict, raw_fingerprint="0" * 64)])


async def test_invalid_identity_and_preparation_failure_remain_durable(database: Database) -> None:
    sink = sink_for(database)
    assert await sink.advance_horizon(2000) == 2000
    malformed = record(sink, 0, identity="not-a-uuid")
    failed = replace(record(sink, 1, identity="source-derived-fallback"), failure="codec: invalid source")
    first = await sink.commit([malformed, failed])
    assert first.outcomes == ("rejected", "rejected")
    assert set(await sink.pending()) == {malformed.source, failed.source}
    assert (await sink.commit([malformed, failed])).rejections == first.rejections


async def test_alternate_rejections_cannot_create_another_control_authority(database: Database) -> None:
    pool, transactor, schema = database
    sink = sink_for(database)
    assert await sink.advance_horizon(2000) == 2000
    await pool.execute(
        f'CREATE TABLE "{schema}".other_rejections (LIKE "{schema}".edax_ingest_rejections INCLUDING ALL)'
    )
    alternate = PostgresIngestionSink(
        transactor, sink.target, schema=schema, tables={"ingest_rejections": "other_rejections"}
    )
    with pytest.raises(BadHandle, match="control row"):
        await alternate.commit([record(alternate, 0)])
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".edax_ingest_events') == 0


async def test_missing_control_row_fails_closed_even_with_cached_binding(database: Database) -> None:
    pool, _, schema = database
    sink = sink_for(database)
    await sink.commit([record(sink, 0)])
    await pool.execute(f'DELETE FROM "{schema}".edax_ingest_partition_control')
    with pytest.raises(BadHandle, match="control row"):
        await sink.commit([record(sink, 1, milliseconds=2)])
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".edax_ingest_events') == 1


async def test_missing_partition_retries_without_poison_rejection(database: Database) -> None:
    pool, _, schema = database
    sink = sink_for(database)
    with pytest.raises(PostgresTransactionFailure) as failure:
        await sink.commit([record(sink, 0), record(sink, 1, milliseconds=4)])
    assert isinstance(failure.value.cause, StoreError)
    assert failure.value.rolled_back and not failure.value.commit_started
    decision = recover(failure.value, BatchSettings(), attempt=1, count=2, isolable=True)
    assert decision["action"] == "retry"
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".edax_ingest_events') == 0
    assert await sink.pending() == {}


async def test_staged_writer_blocks_advancement_and_later_writer_observes_floor(database: Database) -> None:
    pool, transactor, schema = database
    sink = sink_for(database)
    staged, release = asyncio.Event(), asyncio.Event()

    async def writer() -> None:
        async with transaction(transactor):
            assert (await sink.stage([record(sink, 0)])).outcomes == ("accepted",)
            staged.set()
            await release.wait()

    async def wait_for_gate() -> None:
        while not await pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid) "
            "WHERE a.application_name=$1 AND l.locktype='advisory' AND NOT l.granted)",
            schema,
        ):
            await asyncio.sleep(0.01)

    active = asyncio.create_task(writer())
    tasks: list[asyncio.Task[Any]] = [active]
    try:
        await asyncio.wait_for(staged.wait(), 5)
        advancing = asyncio.create_task(sink.advance_horizon(2000))
        tasks.append(advancing)
        await asyncio.wait_for(wait_for_gate(), 5)
        late = asyncio.create_task(sink_for(database).commit([record(sink, 1)]))
        tasks.append(late)
        assert not advancing.done()
        release.set()
        await asyncio.wait_for(active, 5)
        assert await asyncio.wait_for(advancing, 5) == 2000
        assert (await asyncio.wait_for(late, 5)).outcomes == ("expired",)
        assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".edax_ingest_events') == 1
    finally:
        release.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_horizon_advance_refuses_ambient_transaction(database: Database) -> None:
    _, transactor, _ = database
    sink = sink_for(database)
    async with transaction(transactor):
        with pytest.raises(BadHandle):
            await sink.advance_horizon(2000)
    assert await sink.advance_horizon(0) == 0


async def test_full_uuid7_timestamp_is_stored_exactly_without_host_datetime(database: Database) -> None:
    pool, _, schema = database
    await pool.execute(
        f'CREATE TABLE "{schema}".far_future PARTITION OF "{schema}".edax_ingest_events '
        "FOR VALUES FROM ('1971-01-01 00:00:00+00') TO (MAXVALUE)"
    )
    sink = sink_for(database)
    maximum = (1 << 48) - 1
    original = record(sink, 0, milliseconds=maximum)
    assert (await sink.commit([original])).outcomes == ("accepted",)
    timestamp = await pool.fetchval(
        f"SELECT (extract(epoch FROM partition_at) * 1000000)::text "
        f'FROM "{schema}".edax_ingest_events WHERE key=$1',
        original.key,
    )
    assert Decimal(timestamp) == maximum * 1000
    assert (await sink.commit([original])).outcomes == ("accepted",)
    assert await sink.get("clinic-a", original.event_id) is not None
    assert await sink.advance_horizon(maximum * 1000 + 1) == maximum * 1000 + 1
    assert (await sink.commit([original])).outcomes == ("expired",)


async def test_unknown_advance_commit_requires_retry_before_retention(database: Database) -> None:
    from test_ingest_postgres import _FaultPool

    pool, transactor, _ = database
    sink = sink_for(database)
    transactor._pool = _FaultPool(pool, lose_commit=True)
    with pytest.raises(PostgresTransactionFailure) as failed:
        await sink.advance_horizon(2000)
    assert failed.value.commit_started
    assert await sink.advance_horizon(2000) == 2000
    assert (await sink.commit([record(sink, 0)])).outcomes == ("expired",)


async def test_missing_nested_partition_is_retryable_but_unnamed_check_is_terminal(
    database: Database,
) -> None:
    pool, _, schema = database
    await pool.execute(
        f'CREATE TABLE "{schema}".nested PARTITION OF "{schema}".edax_ingest_events '
        "FOR VALUES FROM ('1970-01-01 00:00:00.004+00') TO ('1970-01-01 00:00:00.006+00') "
        "PARTITION BY RANGE (partition_at)"
    )
    sink = sink_for(database)
    with pytest.raises(PostgresTransactionFailure) as missing:
        await sink.commit([record(sink, 0, milliseconds=4)])
    assert isinstance(missing.value.cause, StoreError)
    assert recover(missing.value, BatchSettings(), attempt=1, count=1, isolable=True)["action"] == "retry"
    await pool.execute(f'ALTER TABLE "{schema}".edax_ingest_events ADD CHECK (value IS NULL)')
    with pytest.raises(PostgresTransactionFailure) as checked:
        await sink.commit([record(sink, 0)])
    assert isinstance(checked.value.cause, CodecError)
    assert await sink.pending() == {}

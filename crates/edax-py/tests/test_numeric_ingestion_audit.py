"""Original numeric tokens must not round before shared identity checks."""

import os
from collections.abc import AsyncIterator
from dataclasses import replace
from typing import Any
from uuid import uuid4

import pytest
from edax.backend.ingest_postgres import PostgresIngestionSink, reference_ingestion_ddl
from edax.backend.postgres import PostgresTransactionFailure, PostgresTransactor
from edax.errors import CodecError
from edax.ingestion import BatchSink, IngestionTarget, MemoryIngestionSink
from edax.projection import ProjectionInput, ProjectionIntent, ProjectionResult, ProjectionStateStore
from test_ingestion import original
from test_projection_state import event, proof, target
from test_projection_state import stores as stores


@pytest.fixture(params=["memory", "postgres"])
async def sink(request: Any) -> AsyncIterator[BatchSink]:
    destination = IngestionTarget("append", "numeric-orders", "orders-source", "mapper-v1")
    if request.param == "memory":
        yield MemoryIngestionSink(destination)
        return
    url = os.environ.get("EDAX_POSTGRES_URL")
    if url is None:
        if os.environ.get("EDAX_REQUIRE_POSTGRES") == "1":
            pytest.fail("EDAX_POSTGRES_URL required")
        pytest.skip("EDAX_POSTGRES_URL not set")
    import asyncpg

    pool = await asyncpg.create_pool(url, min_size=1, max_size=2)
    schema = f"numeric_{uuid4().hex}"
    await pool.execute(f'CREATE SCHEMA "{schema}"')
    try:
        await pool.execute(reference_ingestion_ddl(schema))
        yield PostgresIngestionSink(PostgresTransactor(pool, schema=schema), destination, schema=schema)
    finally:
        await pool.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


@pytest.mark.parametrize("number", [2**64, 2**64 + 1, 10**21, 10**21 + 1, -(10**21) - 1])
async def test_ingestion_rejects_original_unsafe_integer(sink: BatchSink, number: int) -> None:
    first = replace(original(sink.target), private={"order_id": "order-42", "total_minor": number})
    result = await sink.commit([first])
    assert result.outcomes == ("rejected",)
    assert "64-bit range" in result.rejections[first.source]["error"]
    assert await sink.get("tenant-7", first.event_id) is None
    assert first.source in await sink.pending()


@pytest.mark.parametrize("field", ["payload", "metadata", "state", "intent"])
@pytest.mark.parametrize("number", [10**21, 10**21 + 1, -(10**21) - 1])
async def test_projection_state_refuses_original_integer_before_any_mutation(
    stores: Any, field: str, number: int
) -> None:
    store: ProjectionStateStore = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker-a", proof())
    value = event()
    if field in {"payload", "metadata"}:
        value = replace(value, **{field: {"amount_minor": number}})

    def reduce(state: Any, received: Any) -> ProjectionResult:
        return ProjectionResult(
            public={"balance_minor": number if field == "state" else 1250},
            intents=(ProjectionIntent("receipt", "notifications", {"amount_minor": number}),)
            if field == "intent"
            else (),
        )

    with pytest.raises((CodecError, PostgresTransactionFailure), match="safe integers") as caught:
        await store.commit(lease, [ProjectionInput(0, value)], proof(), reduce)
    if isinstance(caught.value, PostgresTransactionFailure):
        assert isinstance(caught.value.cause, CodecError)
        assert caught.value.rolled_back and not caught.value.commit_started
    assert await store.get("bank-eu", "account-42") is None
    assert (await store.recover(lease)).next_offset == 0
    assert not await store.unpublished(100)

"""Cleanup participants of plaintext targets, against the memory and Postgres sinks and stores: one case per
branch of a scope's delete, plus the resweep after a restore and the revoke in the cleanup's transaction."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, replace
from typing import Any
from uuid import uuid4

import pytest
from edax import _json
from edax.backend.erasure_postgres import PostgresErasureStore, reference_ddl
from edax.backend.ingest_postgres import PostgresIngestionSink, reference_ingestion_ddl
from edax.backend.postgres import PostgresTransactor
from edax.backend.projection_postgres import PostgresProjectionStateStore, reference_projection_state_ddl
from edax.erasure import ErasureCoordinator, MemoryErasureStore, ScopeCleanup
from edax.errors import Rejected
from edax.ingestion import IngestionTarget, MemoryIngestionSink
from edax.projection import MemoryProjectionStateStore, ProjectionInput, ProjectionResult
from edax.sealing import KeyScope, MemoryKeys
from test_projection_state import event, proof, target

URL = os.environ.get("EDAX_POSTGRES_URL")
TENANTS = ("shop-a", "shop-b")
#: Rows per tenant: two of person-7, one of person-8 and one with no subject.
OWNERS: tuple[tuple[str | None, str], ...] = (
    ("person-7", "row-1"),
    ("person-7", "row-2"),
    ("person-8", "row-3"),
    (None, "row-4"),
)


def scope(tenant: str, subject: str | None) -> KeyScope:
    return KeyScope(key_namespace="customers", tenant=tenant, subject=subject)


@dataclass
class Participant:
    """One plaintext target with rows of two tenants, behind the cleanup it returns."""

    cleanup: Callable[[], ScopeCleanup]
    write: Callable[[], Awaitable[None]]
    #: The rows still stored, as ``(tenant, subject, row)``.
    rows: Callable[[], Awaitable[set[tuple[str, str | None, str]]]]


def ingestion(sink: MemoryIngestionSink | PostgresIngestionSink) -> Participant:
    async def write() -> None:
        inputs = [
            sink.target.input(
                "customers",
                0,
                offset,
                tenant,
                row,
                row,
                f"event-{offset}",
                1,
                {"id": f"event-{offset}"},
                {"email": f"{row}@example.com"},
                _json.encode({"offset": offset}),
                subject=subject,
            )
            for offset, (tenant, (subject, row)) in enumerate(
                (tenant, owner) for tenant in TENANTS for owner in OWNERS
            )
        ]
        await sink.commit(inputs)

    async def rows() -> set[tuple[str, str | None, str]]:
        return {
            (tenant, subject, row)
            for tenant in TENANTS
            for subject, row in OWNERS
            if await sink.get(tenant, row, subject) is not None
        }

    return Participant(sink.cleanup, write, rows)


def projection(store: MemoryProjectionStateStore | PostgresProjectionStateStore) -> Participant:
    async def write() -> None:
        await store.initialize()
        lease = await store.acquire(0, "worker-a", proof())
        lease = await store.recover(lease)
        inputs = [
            ProjectionInput(
                lease.next_offset + offset,
                replace(event(), tenant=tenant, aggregate=row, event_id=f"{tenant}-{row}", subject=subject),
            )
            for offset, (tenant, (subject, row)) in enumerate(
                (tenant, owner) for tenant in TENANTS for owner in OWNERS
            )
        ]
        await store.commit(
            lease, inputs, proof(), lambda _, e: ProjectionResult(private={"email": e.aggregate})
        )

    async def rows() -> set[tuple[str, str | None, str]]:
        return {
            (tenant, subject, row)
            for tenant in TENANTS
            for subject, row in OWNERS
            if await store.get(tenant, row, subject) is not None
        }

    return Participant(store.cleanup, write, rows)


INGESTION = IngestionTarget(
    mode="upsert",
    generation="customers-v1",
    source_incarnation="cluster-a/customers-1",
    mapper_id="customer-v1",
    at_rest="plaintext",
)
PROJECTION = replace(target(), at_rest="plaintext")


@pytest.fixture(params=["memory-ingestion", "memory-projection", "postgres-ingestion", "postgres-projection"])
async def participant(request: pytest.FixtureRequest) -> AsyncIterator[Participant]:
    kind = request.param
    if kind == "memory-ingestion":
        yield ingestion(MemoryIngestionSink(INGESTION))
        return
    if kind == "memory-projection":
        yield projection(MemoryProjectionStateStore(PROJECTION))
        return
    if URL is None:
        pytest.skip("EDAX_POSTGRES_URL is not set: no live Postgres")
    async with postgres() as (pool, schema):
        tx = PostgresTransactor(pool, schema=schema)
        if kind == "postgres-ingestion":
            yield ingestion(PostgresIngestionSink(tx, INGESTION, schema=schema))
        else:
            yield projection(PostgresProjectionStateStore(tx, PROJECTION, schema=schema))


class postgres:
    """An isolated schema holding the ingestion, projection and erasure tables."""

    async def __aenter__(self) -> tuple[Any, str]:
        asyncpg = pytest.importorskip("asyncpg")
        self.pool = await asyncpg.create_pool(URL, min_size=1, max_size=4)
        self.schema = f"cleanup_{uuid4().hex}"
        await self.pool.execute(f'CREATE SCHEMA "{self.schema}"')
        for ddl in (reference_ingestion_ddl, reference_projection_state_ddl, reference_ddl):
            await self.pool.execute(ddl(self.schema))
        return self.pool, self.schema

    async def __aexit__(self, *_: object) -> None:
        await self.pool.execute(f'DROP SCHEMA "{self.schema}" CASCADE')
        await self.pool.close()


EVERY = {(tenant, subject, row) for tenant in TENANTS for subject, row in OWNERS}


@dataclass(frozen=True)
class Case:
    name: str
    scope: KeyScope
    deleted: int
    kept: set[tuple[str, str | None, str]]


CASES = [
    Case(
        "subject_scope_deletes_only_that_subject",
        scope("shop-a", "person-7"),
        2,
        EVERY - {("shop-a", "person-7", "row-1"), ("shop-a", "person-7", "row-2")},
    ),
    Case(
        "tenant_scope_deletes_every_row_of_the_tenant",
        scope("shop-a", None),
        4,
        {row for row in EVERY if row[0] == "shop-b"},
    ),
    Case("unknown_subject_deletes_nothing", scope("shop-a", "person-9"), 0, EVERY),
]


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
async def test_resweep_deletes_what_a_missing_participant_left(participant: Participant, case: Case) -> None:
    await participant.write()
    store = MemoryErasureStore()
    # The coordinator knows the participant but has no cleanup for it: it revokes and deletes nothing.
    operation = await ErasureCoordinator(MemoryKeys(), store, {"rows": None}).erase(case.scope, "request-1")
    assert operation.stage == "notified" and await participant.rows() == EVERY
    cleanup = participant.cleanup()
    assert await cleanup.resweep(store) == case.deleted
    assert await participant.rows() == case.kept
    assert await cleanup.resweep(store) == 0


async def test_registered_cleanup_completes_the_erasure(participant: Participant) -> None:
    await participant.write()
    store = MemoryErasureStore()
    cleanup = participant.cleanup()
    operation = await ErasureCoordinator(MemoryKeys(), store, {"rows": cleanup}).erase(
        scope("shop-a", "person-7"), "request-1"
    )
    assert operation.stage == "complete"
    assert await participant.rows() == CASES[0].kept
    assert await cleanup.resweep(store) == 0


async def test_postgres_cleanup_revokes_in_its_transaction() -> None:
    if URL is None:
        pytest.skip("EDAX_POSTGRES_URL is not set: no live Postgres")
    async with postgres() as (pool, schema):
        erasure = PostgresErasureStore(pool, schema=schema)
        sink = PostgresIngestionSink(PostgresTransactor(pool, schema=schema), INGESTION, schema=schema)
        rows = ingestion(sink)
        await rows.write()
        await sink.cleanup(erasure)(scope("shop-a", "person-7"))
        assert ("shop-a", "person-7", "row-1") not in await rows.rows()
        # The tombstone committed with the delete: a fenced writer of the scope is refused.
        with pytest.raises(Rejected):
            async with erasure.writer_guard(scope("shop-a", "person-7")):
                pass
        # The Postgres erasure store lists the revoked scope for a sweep, paging past what it read.
        coordinator = ErasureCoordinator(MemoryKeys(), erasure, {"rows": None})
        await coordinator.erase(scope("shop-b", "person-8"), "request-1")
        await coordinator.erase(scope("shop-b", None), "request-2")
        assert [stored.operation.stage for stored in await erasure.revoked(1)] == ["notified"]
        assert await sink.cleanup().resweep(erasure, limit=1) == 4  # person-8, then the rest of shop-b
        assert {row for row in await rows.rows() if row[0] == "shop-b"} == set()

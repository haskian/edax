"""Cleanup participants of plaintext targets, against the memory and Postgres sinks and stores: one case per
branch of a scope's delete, plus the resweep after a restore and the revoke in the cleanup's transaction."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, replace
from typing import Any

import pytest
from conftest import postgres_schema
from edax import _json, _native
from edax.backend.erasure_postgres import PostgresErasureStore, reference_ddl
from edax.backend.ingest_postgres import PostgresIngestionSink, reference_ingestion_ddl
from edax.backend.postgres import PostgresTransactor
from edax.backend.projection_postgres import PostgresProjectionStateStore, reference_projection_state_ddl
from edax.erasure import ErasureCoordinator, ErasureOperation, ErasureStore, MemoryErasureStore, ScopeCleanup
from edax.errors import Rejected
from edax.ingestion import IngestionTarget, MemoryIngestionSink
from edax.projection import MemoryProjectionStateStore, ProjectionInput, ProjectionResult
from edax.sealing import KeyScope, MemoryKeys, Sealing
from test_projection_state import event, proof, target

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

    #: The participant's ``cleanup``; Postgres takes the erasure store whose revoke it runs in its delete.
    cleanup: Callable[..., ScopeCleanup]
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
        # Admit fixture writes without binding the later cleanup to a default namespace.
        with store._using_sealing(Sealing(MemoryKeys(), "customers")):
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
    async with postgres() as (pool, schema):
        tx = PostgresTransactor(pool, schema=schema)
        if kind == "postgres-ingestion":
            yield ingestion(PostgresIngestionSink(tx, INGESTION, schema=schema))
        else:
            yield projection(PostgresProjectionStateStore(tx, PROJECTION, schema=schema))


def postgres() -> AbstractAsyncContextManager[tuple[Any, str]]:
    """An isolated schema holding the ingestion, projection and erasure tables."""
    return postgres_schema("cleanup", reference_ingestion_ddl, reference_projection_state_ddl, reference_ddl)


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
    cleanup = participant.cleanup(key_namespace="customers")
    assert await cleanup.resweep(store) == case.deleted
    assert await participant.rows() == case.kept
    assert await cleanup.resweep(store) == 0


async def test_registered_cleanup_completes_the_erasure(participant: Participant) -> None:
    await participant.write()
    store = MemoryErasureStore()
    cleanup = participant.cleanup(key_namespace="customers")
    operation = await ErasureCoordinator(MemoryKeys(), store, {"rows": cleanup}).erase(
        scope("shop-a", "person-7"), "request-1"
    )
    assert operation.stage == "complete"
    assert await participant.rows() == CASES[0].kept
    assert await cleanup.resweep(store) == 0


@pytest.mark.parametrize("kind", ["ingestion", "projection"])
async def test_postgres_cleanup_revokes_in_its_transaction(kind: str) -> None:
    async with postgres() as (pool, schema):
        erasure = PostgresErasureStore(pool, schema=schema)
        tx = PostgresTransactor(pool, schema=schema)
        rows = (
            ingestion(PostgresIngestionSink(tx, INGESTION, schema=schema))
            if kind == "ingestion"
            else projection(PostgresProjectionStateStore(tx, PROJECTION, schema=schema))
        )
        await rows.write()
        await rows.cleanup(erasure, key_namespace="customers")(scope("shop-a", "person-7"))
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
        assert (
            await rows.cleanup(key_namespace="customers").resweep(erasure, limit=1) == 4
        )  # person-8, then the rest of shop-b
        assert {row for row in await rows.rows() if row[0] == "shop-b"} == set()


@pytest.mark.parametrize("backend", ["memory", "postgres"])
async def test_unrevoked_operation_is_not_swept(backend: str) -> None:
    """An operation still at ``persisted`` has no confirmed revocation: a writer may still be fenced
    in, so a sweep must not delete for it yet."""
    participant = ingestion(MemoryIngestionSink(INGESTION))
    await participant.write()
    candidate = ErasureOperation(
        _native.erasure_create(scope("shop-a", "person-7"), "operation-1", "request-1", ["rows"])
    )
    if backend == "memory":
        store: ErasureStore = MemoryErasureStore()
        await store.create(candidate)
        assert await store.revoked() == []
        assert await participant.cleanup(key_namespace="customers").resweep(store) == 0
    else:
        async with postgres() as (pool, schema):
            store = PostgresErasureStore(pool, schema=schema)
            await store.create(candidate)
            assert await store.revoked() == []
            assert await participant.cleanup(key_namespace="customers").resweep(store) == 0
    assert await participant.rows() == EVERY


async def test_cleanup_is_bound_to_one_key_namespace(participant: Participant) -> None:
    from edax.errors import BadHandle

    await participant.write()
    with pytest.raises(BadHandle, match="requires key_namespace"):
        participant.cleanup()
    cleanup = participant.cleanup(key_namespace="customers")
    foreign = KeyScope(key_namespace="other-deployment", tenant="shop-a", subject="person-7")
    with pytest.raises(BadHandle, match="another key namespace"):
        await cleanup(foreign)
    operations = MemoryErasureStore()
    await ErasureCoordinator(MemoryKeys(), operations, {}).erase(foreign, "foreign-erasure")
    assert await cleanup.resweep(operations) == 0
    assert await participant.rows() == EVERY


@pytest.mark.parametrize("limit", [0, -1, True])
async def test_cleanup_and_revoked_pages_require_a_positive_limit(limit: int) -> None:
    from edax.errors import BadHandle

    cleanup = MemoryIngestionSink(INGESTION).cleanup(key_namespace="customers")
    with pytest.raises(BadHandle):
        await cleanup.resweep(MemoryErasureStore(), limit=limit)
    with pytest.raises(BadHandle):
        await MemoryErasureStore().revoked(limit)
    async with postgres() as (pool, schema):
        with pytest.raises(BadHandle):
            await PostgresErasureStore(pool, schema=schema).revoked(limit)


@pytest.mark.parametrize("kind", ["ingestion", "projection"])
async def test_paged_cleanup_commits_progress_and_reinstalls_destination_revocation(
    kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from edax.errors import StoreError

    async with postgres() as (pool, schema):
        erasure = PostgresErasureStore(pool, schema=schema)
        tx = PostgresTransactor(pool, schema=schema)
        store = (
            PostgresIngestionSink(tx, INGESTION, schema=schema)
            if kind == "ingestion"
            else PostgresProjectionStateStore(tx, PROJECTION, schema=schema)
        )
        rows = ingestion(store) if isinstance(store, PostgresIngestionSink) else projection(store)
        await rows.write()
        method = "_typed" if kind == "ingestion" else "_owned"
        own = getattr(store, method)
        pages = 0

        async def interrupted(call: Any) -> Any:
            nonlocal pages
            if call.__name__ == "run":
                pages += 1
                if pages == 2:
                    raise StoreError("injected interruption after one committed cleanup page")
            return await own(call)

        monkeypatch.setattr(store, method, interrupted)
        cleanup = store.cleanup(erasure, key_namespace="customers", limit=1)
        operations = MemoryErasureStore()
        coordinator = ErasureCoordinator(MemoryKeys(), operations, {"rows": cleanup})
        erased = scope("shop-a", "person-7")
        with pytest.raises(StoreError, match="injected interruption"):
            await coordinator.erase(erased, "paged-erasure")
        pending = await coordinator.status(erased, "paged-erasure")
        assert pending is not None and pending.stage == "notified" and not pending.acknowledgements
        assert len([row for row in await rows.rows() if row[:2] == ("shop-a", "person-7")]) == 1
        with pytest.raises(Rejected):
            async with erasure.writer_guard(erased):
                pass
        # A destination restore may lose its tombstone independently of the workflow store.
        await pool.execute(f'DELETE FROM "{schema}".edax_erasure_revocations')
        result = await coordinator.resume(erased, "paged-erasure")
        assert result.stage == "complete" and result.acknowledgements == {"rows"}
        assert await rows.rows() == CASES[0].kept
        with pytest.raises(Rejected):
            async with erasure.writer_guard(erased):
                pass
        assert pages == 4  # first committed page, interruption, final row, empty completion page


def test_cleanup_infers_sealing_namespace_and_refuses_override() -> None:
    from edax.errors import BadHandle
    from edax.sealing import Sealing

    sink = MemoryIngestionSink(INGESTION, sealing=Sealing(MemoryKeys(), key_namespace="customers"))
    assert sink.cleanup().key_namespace == "customers"
    with pytest.raises(BadHandle, match="differs"):
        sink.cleanup(key_namespace="other-deployment")

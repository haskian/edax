"""Live Postgres evidence for durable erasure, transactional fences, and encrypted persistence.

Every test owns a random schema. Set EDAX_POSTGRES_URL to opt in; no public tables are changed.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
from collections.abc import AsyncIterator, Sequence
from typing import Any, ClassVar
from uuid import uuid4

import edax
import pytest
from conftest import CreateUser
from edax import _native
from edax.backend.erasure_postgres import PostgresErasureStore
from edax.backend.erasure_postgres import reference_ddl as erasure_ddl
from edax.backend.memory import MemoryEventStore
from edax.backend.postgres import (
    PostgresCommandOutbox,
    PostgresEventStore,
    PostgresProjectionStore,
    PostgresSagaStore,
    PostgresTransactor,
    reference_ddl,
)
from edax.erasure import ErasureCoordinator, ErasureOperation
from edax.errors import BadHandle, ConcurrencyError, EdaxError, Rejected, Shredded
from edax.projection import Entity
from edax.saga import ProtectedSagaState, RunningInstance
from edax.sealing import Cipher, KeyScope, MemoryKeys, Sealing
from edax.unit_of_work import transaction

URL = os.environ.get("EDAX_POSTGRES_URL")
if URL is None:
    if os.environ.get("EDAX_REQUIRE_POSTGRES") == "1":
        raise RuntimeError("EDAX_REQUIRE_POSTGRES=1 but EDAX_POSTGRES_URL is not set")
    pytest.skip("EDAX_POSTGRES_URL is not set: no live Postgres", allow_module_level=True)
asyncpg = pytest.importorskip("asyncpg")


@pytest.fixture
async def database() -> AsyncIterator[tuple[Any, str]]:
    schema = "edax_crypto_" + uuid4().hex
    pool = await asyncpg.create_pool(URL, min_size=1, max_size=8)
    try:
        await pool.execute(f'CREATE SCHEMA "{schema}"')
        await pool.execute(reference_ddl(schema))
        await pool.execute(erasure_ddl(schema))
        yield pool, schema
    finally:
        await pool.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await pool.close()


def scope(subject: str | None = "person") -> KeyScope:
    return KeyScope(key_namespace="production", tenant="tenant", subject=subject)


def candidate(
    operation_id: str = "operation", participants: tuple[str, ...] = ("database",)
) -> ErasureOperation:
    return ErasureOperation(_native.erasure_create(scope(), operation_id, "request", list(participants)))


async def test_operations_cas_and_notification_survive_adapter_recreation(database: tuple[Any, str]) -> None:
    pool, schema = database
    store = PostgresErasureStore(pool, schema=schema)
    await store.check()
    await store.check()
    initial = await store.create(candidate())
    assert initial.version == 1
    assert await store.create(candidate("retry-id")) == initial
    with pytest.raises(Rejected):
        await store.create(candidate("different", ("changed",)))
    assert await store.get(scope(), "request") == initial
    assert await store.get_by_id(initial.operation.operation_id) == initial
    assert await store.get(scope("another"), "request") is None
    assert await store.pending() == [initial]
    with pytest.raises(BadHandle):
        await store.advance(initial, "notify")
    revoked = await store.advance(initial, "revoke")
    with pytest.raises(ConcurrencyError):
        await store.advance(initial, "revoke")
    assert await store.unpublished() == []
    # Failure to enqueue the control message must roll back the stage update in the same statement.
    await pool.execute(
        f'ALTER TABLE "{schema}".edax_erasure_notifications ADD CONSTRAINT fail_enqueue CHECK (false)'
    )
    with pytest.raises(EdaxError):
        await store.advance(revoked, "notify")
    assert await store.get(scope(), "request") == revoked
    assert await store.unpublished() == []
    await pool.execute(f'ALTER TABLE "{schema}".edax_erasure_notifications DROP CONSTRAINT fail_enqueue')
    notified = await store.advance(revoked, "notify")
    restarted = PostgresErasureStore(pool, schema=schema)
    assert await restarted.get(scope(), "request") == notified
    notifications = await restarted.unpublished()
    assert len(notifications) == 1 and notifications[0].operation_id == "operation"
    with pytest.raises(ConcurrencyError):
        await restarted.advance(revoked, "notify")
    assert await restarted.unpublished() == notifications
    with pytest.raises(BadHandle):
        await restarted.advance(notified, "acknowledge", "unknown")
    acknowledged = await restarted.advance(notified, "acknowledge", "database")
    destroyed = await restarted.advance(acknowledged, "destroy")
    completed = await restarted.advance(destroyed, "complete")
    assert completed.operation.stage == "complete" and await restarted.pending() == []
    await restarted.mark_published("operation")
    assert await restarted.unpublished() == []
    assert await restarted.create(candidate("another-retry")) == completed
    assert await restarted.unpublished() == []


async def wait_for_blocked_lock(pool: Any, pid: int) -> None:
    async def wait() -> None:
        while not await pool.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted)", pid
        ):
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait(), 3)


@pytest.mark.parametrize("erase_tenant", [False, True])
async def test_cleanup_drains_committing_writer_before_deleting_plaintext(
    database: tuple[Any, str], erase_tenant: bool
) -> None:
    pool, schema = database
    store = PostgresErasureStore(pool, schema=schema)
    await pool.execute(f'CREATE TABLE "{schema}".plaintext (value text)')
    async with pool.acquire() as writer, pool.acquire() as cleaner:
        writer_tx = writer.transaction()
        await writer_tx.start()
        await store.guard_in_transaction(writer, scope())
        await writer.execute(f'INSERT INTO "{schema}".plaintext VALUES ($1)', "private@example.test")

        async def cleanup() -> None:
            async with cleaner.transaction():
                await store.revoke_in_transaction(cleaner, scope(None) if erase_tenant else scope())
                await cleaner.execute(f'DELETE FROM "{schema}".plaintext')

        task = asyncio.create_task(cleanup())
        try:
            await wait_for_blocked_lock(pool, cleaner.get_server_pid())
            assert not task.done()
            await writer_tx.commit()
            await asyncio.wait_for(task, 3)
        finally:
            if writer.is_in_transaction():
                await writer_tx.rollback()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".plaintext') == 0
    with pytest.raises(Rejected):
        async with store.writer_guard(scope()):
            pytest.fail("revoked scope admitted a writer")
    if not erase_tenant:
        async with store.writer_guard(scope("unrelated")):
            pass


class PostgresFence(MemoryKeys):
    """Memory key material, fenced inside the application's Postgres transaction by the erasure store."""

    def __init__(self, transactor: PostgresTransactor, erasure: PostgresErasureStore) -> None:
        super().__init__()
        self.transactor = transactor
        self.erasure = erasure

    async def fence(self, tx: Any, scopes: Sequence[KeyScope]) -> None:
        await self.erasure.guard_in_transaction(tx.connection, *scopes)


@pytest.mark.parametrize("outcome", ["commit", "connection-killed"])
async def test_transaction_fence_drains_its_writer_and_dies_with_its_connection(
    database: tuple[Any, str], outcome: str
) -> None:
    pool, schema = database
    erasure = PostgresErasureStore(pool, schema=schema)
    db = PostgresTransactor(pool, schema=schema)
    sealing = Sealing(PostgresFence(db, erasure), "production", write_fence="transaction")
    await pool.execute(f'CREATE TABLE "{schema}".plaintext (value text)')
    staged = asyncio.Event()
    commit = asyncio.Event()
    writer_pid: list[int] = []

    async def writer() -> None:
        # One transaction, one connection: no second pool, and the lock dies with the write.
        async with sealing.write_guard("tenant", "person"), transaction(db) as tx:
            writer_pid.append(await tx.connection.fetchval("SELECT pg_backend_pid()"))
            await tx.connection.execute(
                f'INSERT INTO "{schema}".plaintext VALUES ($1)', "private@example.test"
            )
            staged.set()
            await commit.wait()

    writing = asyncio.create_task(writer())
    async with pool.acquire() as cleaner:

        async def cleanup() -> int:
            async with cleaner.transaction():
                await erasure.revoke_in_transaction(cleaner, scope())
                return int((await cleaner.execute(f'DELETE FROM "{schema}".plaintext')).split()[-1])

        await asyncio.wait_for(staged.wait(), 3)
        cleaning = asyncio.create_task(cleanup())
        try:
            await wait_for_blocked_lock(pool, cleaner.get_server_pid())
            assert not cleaning.done(), "cleanup must wait for the admitted writer"
            if outcome == "commit":
                commit.set()
                await asyncio.wait_for(writing, 3)
            else:
                await pool.execute("SELECT pg_terminate_backend($1)", writer_pid[0])
                commit.set()
                with pytest.raises(EdaxError):
                    await asyncio.wait_for(writing, 3)
            assert await asyncio.wait_for(cleaning, 3) == int(outcome == "commit")
        finally:
            commit.set()
            for task in (writing, cleaning):
                if not task.done():
                    task.cancel()
            await asyncio.gather(writing, cleaning, return_exceptions=True)
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".plaintext') == 0
    with pytest.raises(Rejected):
        async with sealing.write_guard("tenant", "person"), transaction(db):
            pytest.fail("revoked scope admitted a writer")


async def test_tenant_revocation_blocks_new_child_waiting_for_commit(database: tuple[Any, str]) -> None:
    pool, schema = database
    store = PostgresErasureStore(pool, schema=schema)
    async with pool.acquire() as revoker, pool.acquire() as child:
        parent_tx = revoker.transaction()
        await parent_tx.start()
        await store.revoke_in_transaction(revoker, scope(None))

        async def new_child() -> None:
            async with child.transaction():
                await store.guard_in_transaction(child, scope("never-seen-before"))

        task = asyncio.create_task(new_child())
        try:
            await wait_for_blocked_lock(pool, child.get_server_pid())
            await parent_tx.commit()
            with pytest.raises(Rejected):
                await asyncio.wait_for(task, 3)
        finally:
            if revoker.is_in_transaction():
                await parent_tx.rollback()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def test_guards_reject_autocommit_stale_snapshots_and_preserve_rollback(
    database: tuple[Any, str],
) -> None:
    pool, schema = database
    store = PostgresErasureStore(pool, schema=schema)
    async with pool.acquire() as connection:
        with pytest.raises(BadHandle):
            await store.guard_in_transaction(connection, scope())
        async with connection.transaction():
            with pytest.raises(BadHandle):
                await store.guard_in_transaction(connection)
        with pytest.raises(BadHandle):
            await store.revoke_in_transaction(connection, scope())
        async with connection.transaction(isolation="repeatable_read"):
            with pytest.raises(BadHandle):
                await store.guard_in_transaction(connection, scope())
        tx = connection.transaction()
        await tx.start()
        await store.revoke_in_transaction(connection, scope())
        await tx.rollback()
    async with store.writer_guard(scope()):
        pass


async def test_coordinator_cleanup_uses_database_fence_and_survives_offline_participant(
    database: tuple[Any, str],
) -> None:
    pool, schema = database
    store = PostgresErasureStore(pool, schema=schema)
    keys = MemoryKeys()
    await pool.execute(f'CREATE TABLE "{schema}".plaintext (value text)')
    async with store.writer_guard(scope()) as connection:
        await connection.execute(f'INSERT INTO "{schema}".plaintext VALUES ($1)', "private@example.test")

    async def cleanup(owner: KeyScope) -> None:
        async with pool.acquire() as connection, connection.transaction():
            await store.revoke_in_transaction(connection, owner)
            await connection.execute(f'DELETE FROM "{schema}".plaintext')

    coordinator = ErasureCoordinator(keys, store, {"database": cleanup, "offline": None})
    pending = await coordinator.erase(scope(), "request")
    assert pending.stage == "notified" and pending.acknowledgements == {"database"}
    assert len(await store.unpublished()) == 1
    assert await pool.fetchval(f'SELECT count(*) FROM "{schema}".plaintext') == 0
    restarted = ErasureCoordinator(keys, PostgresErasureStore(pool, schema=schema), {})
    assert (await restarted.resume(scope(), "request")).stage == "notified"
    await restarted.acknowledge(scope(), "request", "offline")
    assert (await restarted.resume(scope(), "request")).stage == "complete"
    assert await keys.status(scope()) == "destroyed"
    with pytest.raises(Rejected):
        async with store.writer_guard(scope()):
            pytest.fail("cleanup failed to install permanent database fence")


PRIVATE = "pg-sensitive-customer@example.test"
EVENTS = edax.json_codec({"EmailChanged": CreateUser})
COMMANDS = edax.json_codec({CreateUser.type: CreateUser})


class Profile(edax.Aggregate):
    def __init__(self, id: str, initial: int = 0) -> None:
        super().__init__(id, initial)
        self.email = ""

    def transition(self, event: CreateUser) -> None:
        self.email = event.email


class Profiles:
    type = "Profile"

    def load(self, id: str, version: int, data: bytes | None) -> Profile:
        profile = Profile(id, version)
        profile.email = "" if data is None else json.loads(data)["email"]
        return profile

    def dump(self, profile: Profile) -> bytes:
        return json.dumps({"email": profile.email}).encode()


class NotificationSaga:
    name: ClassVar[str] = "notification"

    def __init__(self) -> None:
        self.seen: list[Any] = []

    def instance_id(self, msg: edax.EventMessage) -> str:
        return "workflow"

    def handle(self, state: Any, msg: edax.EventMessage) -> tuple[Any, list[CreateUser]]:
        self.seen.append(state)
        email = json.loads(bytes(msg.data))["email"]
        return {"email": email}, [CreateUser(email)]


@pytest.mark.parametrize("cipher", ["aes-256-gcm", "xchacha20-poly1305"])
async def test_protected_repository_snapshots_and_sagas_roundtrip_without_plaintext_at_rest(
    database: tuple[Any, str], cipher: Cipher
) -> None:
    pool, schema = database
    db = PostgresTransactor(pool, schema=schema)
    store = PostgresEventStore(db)
    commands = PostgresCommandOutbox(db)
    sagas = PostgresSagaStore(db)
    keys = MemoryKeys()
    sealing = Sealing(keys, "production", cipher=cipher)
    repo = edax.Repository(store, EVENTS, Profiles(), commands=commands, sealing=sealing)
    profile = await repo.load("profile", tenant="tenant", subject="person")
    profile.apply(CreateUser(PRIVATE))
    command = edax.command_message(CreateUser(PRIVATE), codec=COMMANDS)
    events = await repo.save(profile, commands=[command])
    await db.check()
    assert events == await store.unpublished(100)
    assert events[0].subject == "person" and events[0].encryption is not None
    snapshot = await store.load_snapshot(None, "profile")
    assert snapshot is not None and snapshot.encryption is not None and snapshot.subject == "person"
    loaded = await repo.load("profile", tenant="tenant")
    assert loaded.email == PRIVATE and loaded.subject == "person"
    saga = NotificationSaga()
    runner = edax.SagaRunner(saga, sagas, COMMANDS, sealing=sealing)
    await runner.on_event(events[0])
    persisted = await sagas.load(saga.name, "workflow")
    assert isinstance(persisted, RunningInstance) and isinstance(persisted.state, ProtectedSagaState)
    loaded.apply(CreateUser(PRIVATE))
    second = (await repo.save(loaded))[0]
    await runner.on_event(second)
    assert saga.seen == [None, {"email": PRIVATE}]
    outbox = await sagas.unpublished(saga.name, 100)
    assert len(outbox) == 2 and all(row.encryption is not None for row in outbox)
    assert PRIVATE.encode() in await sealing.open_command(outbox[0])
    for table in (
        "edax_events",
        "edax_snapshots",
        "edax_command_outbox",
        "edax_saga_instances",
        "edax_saga_outbox",
    ):
        serialized = await pool.fetchval(
            f'SELECT coalesce(json_agg(t)::text,\'[]\') FROM "{schema}"."{table}" t'
        )
        assert PRIVATE not in serialized and base64.b64encode(PRIVATE.encode()).decode() not in serialized
        assert PRIVATE.encode().hex() not in serialized  # json_agg renders bytea columns as hex.
    await keys.revoke(scope())
    await keys.destroy(scope())
    with pytest.raises(Shredded):
        await repo.load("profile", tenant="tenant")
    await runner.on_event(second)
    assert len(saga.seen) == 2


async def test_erasure_schema_check_refuses_incompatible_tables_without_repair(
    database: tuple[Any, str],
) -> None:
    pool, schema = database
    store = PostgresErasureStore(pool, schema=schema)
    await store.check()
    await pool.execute(
        f'ALTER TABLE "{schema}".edax_erasure_notifications ALTER COLUMN notification DROP NOT NULL'
    )
    with pytest.raises(BadHandle, match="notification must be NOT NULL"):
        await store.check()
    assert (
        await pool.fetchval(
            "SELECT is_nullable FROM information_schema.columns "
            "WHERE table_schema=$1 AND table_name='edax_erasure_notifications' AND column_name='notification'",
            schema,
        )
        == "YES"
    )


ERASURE_TABLES = {
    "erasure_operations": "erasure_ops",
    "erasure_notifications": "erasure_outbox",
    "erasure_revocations": "erasure_tombstones",
}


async def test_erasure_tables_take_the_hosts_names(database: tuple[Any, str]) -> None:
    pool, schema = database
    named = f"{schema}_named"
    await pool.execute(f'CREATE SCHEMA "{named}"')
    try:
        await pool.execute(erasure_ddl(named, ERASURE_TABLES))
        store = PostgresErasureStore(pool, schema=named, tables=ERASURE_TABLES)
        await store.check()
        stored = await store.create(candidate())
        assert await pool.fetchval(f'SELECT operation_id FROM "{named}".erasure_ops') == "operation"
        assert [item.operation.operation_id for item in await store.pending()] == ["operation"]
        assert stored.version == 1
        tables = await pool.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=$1", named
        )
        assert {row["table_name"] for row in tables} == set(ERASURE_TABLES.values())
        # The default names are other tables: the store reads only the ones it was given.
        with pytest.raises(BadHandle, match="does not exist"):
            await PostgresErasureStore(pool, schema=named).check()
        with pytest.raises(BadHandle, match="unknown postgres erasure role: operations"):
            PostgresErasureStore(pool, schema=named, tables={"operations": "ops"})
    finally:
        await pool.execute(f'DROP SCHEMA IF EXISTS "{named}" CASCADE')


async def test_completed_protected_saga_has_retention_timestamp_and_retains_owner(
    database: tuple[Any, str],
) -> None:
    pool, schema = database
    store = PostgresSagaStore(PostgresTransactor(pool, schema=schema))
    sealing = Sealing(MemoryKeys(), "production")
    sealed = await sealing.seal_state(
        "saga", "workflow", "notification", 1, b"null", "tenant", "person", "", True
    )
    protected = ProtectedSagaState(
        tenant="tenant", subject="person", completed=True, encryption=sealed.encryption, data=sealed.data
    )
    await store.save("notification", "workflow", protected, 0, "", [])
    row = await pool.fetchrow(f'SELECT completed, completed_at FROM "{schema}".edax_saga_instances')
    assert row["completed"] is True and row["completed_at"] is not None
    loaded = await store.load("notification", "workflow")
    assert isinstance(loaded, edax.CompletedInstance) and loaded.protected == protected
    with pytest.raises(ConcurrencyError):
        await store.save("notification", "workflow", {"email": PRIVATE}, 1, "", [])


async def test_plaintext_saga_update_preserves_json_escaped_nul(database: tuple[Any, str]) -> None:
    pool, schema = database
    store = PostgresSagaStore(PostgresTransactor(pool, schema=schema))
    await store.save("notification", "workflow", {"message": "before\x00after"}, 0, "", [])
    assert await store.save("notification", "workflow", {"message": "updated"}, 1, "", []) == 2


@pytest.mark.parametrize(
    ("subjects", "refused"),
    [(("revoked", "active"), True), (("active", "revoked"), True), (("active", "another"), False)],
    ids=["revoked-scope-first", "revoked-scope-last", "all-active"],
)
async def test_guard_for_several_scopes_refuses_when_any_one_is_revoked(
    database: tuple[Any, str], subjects: tuple[str, str], refused: bool
) -> None:
    pool, schema = database
    store = PostgresErasureStore(pool, schema=schema)
    await store.revoke(scope("revoked"))
    async with pool.acquire() as connection, connection.transaction():
        if refused:
            with pytest.raises(Rejected):
                await store.guard_in_transaction(connection, *map(scope, subjects))
        else:
            await store.guard_in_transaction(connection, *map(scope, subjects))


async def test_handler_write_through_a_store_alone_is_refused_after_revoke(database: tuple[Any, str]) -> None:
    pool, schema = database
    erasure = PostgresErasureStore(pool, schema=schema)
    db = PostgresTransactor(pool, schema=schema)
    keys = PostgresFence(db, erasure)
    sealing = Sealing(keys, "production", write_fence="transaction")
    repo = edax.Repository(MemoryEventStore(), EVENTS, Profiles(), sealing=sealing)
    profile = await repo.load("profile", tenant="tenant", subject="person")
    profile.apply(CreateUser(PRIVATE))
    raw = (await repo.save(profile))[0]
    projections = PostgresProjectionStore(db, "profiles")
    read = asyncio.Event()
    erased = asyncio.Event()
    router = edax.EventRouter(sealing=sealing)

    async def project(msg: edax.EventMessage) -> None:
        document = json.loads(bytes(msg.data))
        read.set()
        await erased.wait()
        # No unit of work: the store writes in a short transaction of its own, which takes the fence.
        await projections.save(Entity("profile", 0, document))

    router.register(raw.name, project)
    handling = asyncio.create_task(router.dispatch(raw))
    try:
        await asyncio.wait_for(read.wait(), 3)
        await asyncio.wait_for(erasure.revoke(scope()), 3)  # nothing admitted: no wait
        erased.set()
        with pytest.raises(Rejected):
            await asyncio.wait_for(handling, 3)
    finally:
        erased.set()
        await asyncio.gather(handling, return_exceptions=True)
    assert await projections.get("profile") is None


@pytest.mark.parametrize("timeout", ["lock_timeout", "statement_timeout"])
async def test_erasure_fence_wait_is_uncounted_contention(database: tuple[Any, str], timeout: str) -> None:
    from edax.errors import InProgress

    pool, schema = database
    store = PostgresErasureStore(pool, schema=schema)
    async with store.writer_guard(scope()), pool.acquire() as connection:
        with pytest.raises(InProgress, match="erasure_fence"):
            async with connection.transaction(isolation="read_committed"):
                await connection.execute(f"SET LOCAL {timeout} = '20ms'")
                await store.revoke_in_transaction(connection, scope())
    # Failed lock acquisition cannot install a partial revocation.
    async with store.writer_guard(scope()):
        pass

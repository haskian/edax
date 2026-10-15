"""The Postgres stores against a live server: every packaged contract, the kernel's query vectors through
``get_many``, and the faults only a real server has: a lock wait, an aborted transaction, a relay lease, a
second transactor. Set ``EDAX_POSTGRES_URL`` (a throwaway database, ``postgresql://postgres:edax@localhost:55433/
postgres``) to run them; ``EDAX_REQUIRE_POSTGRES=1`` makes a missing URL fail instead of skip, as CI sets it.
The SQL itself is held to the kernel's reference applier in ``crates/edax-postgres/live-check``."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from typing import Any, TypeVar

import pytest
from conftest import golden
from edax._units import leave_every_unit_of_work
from edax._values import fields_of
from edax.backend.postgres import (
    PostgresCommandOutbox,
    PostgresEventStore,
    PostgresIdempotencyStore,
    PostgresProjectionStore,
    PostgresSagaStore,
    PostgresTransactor,
    PostgresTx,
    RelayLease,
    default_settings,
    reference_ddl,
)
from edax.errors import BadHandle, CodecError, ConcurrencyError, EdaxError, InProgress, StoreError, from_wire
from edax.filter import gte
from edax.idempotency import Claimed, Record, Won
from edax.message import EventDraft
from edax.projection import Entity
from edax.query import Page, Query, sort_key
from edax.query import query as kernel_query
from edax.saga import RunningInstance, SagaOutbox
from edax.testing import (
    CommandOutboxContract,
    EventOutboxContract,
    EventStoreContract,
    IdempotencyStoreContract,
    ProjectionStoreContract,
    SagaOutboxContract,
    SagaStoreContract,
)
from edax.testing._concurrency import RolledBack, unique_id
from edax.testing.messages import command, saga_command
from edax.unit_of_work import transaction
from hypothesis import given, settings
from hypothesis import strategies as st

T = TypeVar("T")

URL = os.environ.get("EDAX_POSTGRES_URL")
if URL is None:
    if os.environ.get("EDAX_REQUIRE_POSTGRES") == "1":
        raise RuntimeError("EDAX_REQUIRE_POSTGRES=1 but EDAX_POSTGRES_URL is not set")
    pytest.skip("EDAX_POSTGRES_URL is not set: no live Postgres", allow_module_level=True)

asyncpg = pytest.importorskip("asyncpg")

SCHEMA = "edax_test"


async def provision(pool: Any, schema: str, tables: dict[str, str] | None = None) -> None:
    """Create ``schema`` afresh with the reference DDL: what a host's migration tool does."""
    await pool.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE; CREATE SCHEMA "{schema}"')
    await pool.execute(reference_ddl(schema, tables))


@pytest.fixture(scope="session")
def provisioned() -> None:
    """The test schema, provisioned once per session."""

    async def run() -> None:
        pool = await asyncpg.create_pool(URL, min_size=1, max_size=1)
        try:
            await provision(pool, SCHEMA)
        finally:
            await pool.close()

    asyncio.run(run())


@pytest.fixture
async def pool(provisioned: None) -> AsyncIterator[Any]:
    pool = await asyncpg.create_pool(
        URL, min_size=1, max_size=12, server_settings={"deadlock_timeout": "50ms"}
    )
    try:
        yield pool
    finally:
        await pool.close()


def tables(db: PostgresTransactor) -> dict[str, str]:
    """Each table ``db`` resolved, schema-qualified, by its role: the names the kernel renders its
    statements over."""
    resolved = json.loads(db.tables)
    return {role: f'"{resolved["schema"]}"."{name}"' for role, name in resolved["names"].items()}


@pytest.fixture
async def db(pool: Any) -> AsyncIterator[PostgresTransactor]:
    transactor = PostgresTransactor(pool, schema=SCHEMA, settings=default_settings(lock_timeout_ms=1_000))
    await pool.execute(f"TRUNCATE {', '.join(tables(transactor).values())}")
    yield transactor


class TestPostgresEventStore(EventStoreContract):
    @pytest.fixture
    def event_store(self, db: PostgresTransactor) -> Any:
        return PostgresEventStore(db)


class TestPostgresEventOutbox(EventOutboxContract):
    @pytest.fixture
    def event_store(self, db: PostgresTransactor) -> Any:
        return PostgresEventStore(db)

    @pytest.fixture
    def outbox(self, event_store: PostgresEventStore) -> Any:
        return event_store


class TestPostgresProjectionStore(ProjectionStoreContract):
    @pytest.fixture
    def projection_store(self, db: PostgresTransactor) -> Any:
        return PostgresProjectionStore(db, "accounts")

    @pytest.fixture
    def projection_store_named(self, db: PostgresTransactor) -> Any:
        return lambda name: PostgresProjectionStore(db, projection=name)

    @pytest.fixture
    def transactor(self, db: PostgresTransactor) -> Any:
        return db


class TestPostgresSagaStore(SagaStoreContract):
    @pytest.fixture
    def saga_store(self, db: PostgresTransactor) -> Any:
        return PostgresSagaStore(db)

    @pytest.fixture
    def transactor(self, db: PostgresTransactor) -> Any:
        return db


class TestPostgresSagaOutbox(SagaOutboxContract):
    @pytest.fixture
    def saga_store(self, db: PostgresTransactor) -> Any:
        return PostgresSagaStore(db)

    @pytest.fixture
    def outbox(self, saga_store: PostgresSagaStore) -> Any:
        return SagaOutbox(saga_store, "order")

    @pytest.fixture
    def transactor(self, db: PostgresTransactor) -> Any:
        return db


class TestPostgresCommandOutbox(CommandOutboxContract):
    @pytest.fixture
    def command_outbox(self, db: PostgresTransactor) -> Any:
        return PostgresCommandOutbox(db)

    @pytest.fixture
    def outbox(self, command_outbox: PostgresCommandOutbox) -> Any:
        return command_outbox

    @pytest.fixture
    def event_store(self, db: PostgresTransactor) -> Any:
        return PostgresEventStore(db)


class TestPostgresIdempotencyStore(IdempotencyStoreContract):
    # A committed pending claim lapses after the claim TTL; a settled record lapses when its retention
    # purge deletes it. The hook does each now.
    claims_expire = True
    stores_raw_records = True

    async def expire_claim(self, idempotency_store: Any, key: str) -> None:
        db = idempotency_store.transactor
        ledger = tables(db)["ledger"]
        await db._pool.execute(f"DELETE FROM {ledger} WHERE key = $1 AND owner IS NULL", key)
        await db._pool.execute(
            f"UPDATE {ledger} SET claimed_at = '-infinity' WHERE key = $1 AND owner IS NOT NULL", key
        )

    async def put_raw_record(self, idempotency_store: Any, key: str, raw: str) -> None:
        db = idempotency_store.transactor
        await db._pool.execute(
            f"INSERT INTO {tables(db)['ledger']} (key, record, claimed_at) VALUES ($1, $2, now())", key, raw
        )

    @pytest.fixture
    def idempotency_store(self, db: PostgresTransactor) -> Any:
        return PostgresIdempotencyStore(db)

    @pytest.fixture
    def transactor(self, db: PostgresTransactor) -> Any:
        return db


def draft(aggregate: str, sequence: int, **fields: Any) -> EventDraft:
    base: dict[str, Any] = {
        "id": f"{aggregate}-{sequence}",
        "aggregate_id": aggregate,
        "name": "Deposited",
        "version": 1,
        "data": b'{"amount": 5}',
        "correlation_id": "corr",
        "causation_id": "cause",
        "extensions": {"traceparent": "00-ab"},
        "metadata": {"note": "a\x00b"},
        "sequence": sequence,
    }
    return EventDraft(**{**base, **fields})


async def contender(
    db: PostgresTransactor, work: Callable[[PostgresTx], Awaitable[T]]
) -> tuple[asyncio.Task[T], int]:
    """``work`` started in a unit of work of its own, in a task of its own (another worker), and the server
    pid of its connection, which :func:`wait_for_lock` watches."""
    opened: asyncio.Future[int] = asyncio.get_running_loop().create_future()

    async def run() -> T:
        leave_every_unit_of_work()
        async with transaction(db) as tx:
            conn: Any = tx.connection
            opened.set_result(conn.get_server_pid())
            return await work(tx)

    task = asyncio.create_task(run())
    return task, await asyncio.wait_for(opened, 3)


async def wait_for_lock(pool: Any, statement: str, pid: int | None = None) -> None:
    """Wait, three seconds at most, until session ``pid`` (any session when ``None``) waits on a lock in the
    kernel statement tagged ``statement``: the order a race needs, read from the server rather than guessed
    with a sleep."""

    async def waiting() -> None:
        while not await pool.fetchval(
            "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE ($1::int IS NULL OR pid = $1) "
            "AND wait_event_type = 'Lock' AND query LIKE $2)",
            pid,
            f"/* edax:{statement} */%",
        ):
            await asyncio.sleep(0.01)

    await asyncio.wait_for(waiting(), 3)


async def test_append_stamps_the_database_clock_and_keeps_every_field(
    db: PostgresTransactor, pool: Any
) -> None:
    store = PostgresEventStore(db)
    async with transaction(store) as tx:
        before = await pool.fetchval("SELECT floor(extract(epoch FROM clock_timestamp()) * 1e9)::bigint")
        drafts = [draft("acct-1", 1, tenant="acme"), draft("acct-1", 2, occurred_at=7)]
        stored = await store.append(tx, "acct-1", 0, drafts)
    assert stored[0].recorded_at == stored[1].recorded_at, "one clock reading per append"
    assert stored[0].recorded_at >= before - 1_000
    async with transaction(store) as tx:
        loaded = await store.load(tx, "acct-1")
    # The append returns only its clock reading and the kernel stamps the drafts with it, so every field it
    # hands back must be what the rows hold.
    assert [fields_of(event) for event in stored] == [fields_of(event) for event in loaded]
    assert loaded[0].tenant == "acme"
    assert loaded[0].metadata == {"note": "a\x00b"}, "a NUL survives inside the metadata text"
    assert loaded[1].occurred_at == 7


async def test_a_caught_failure_rolls_the_whole_unit_of_work_back(db: PostgresTransactor) -> None:
    """A statement the server refuses (here an event id already stored) aborts the transaction. A host that
    catches the error and exits cleanly must not be told its earlier writes committed."""
    store, projections = PostgresEventStore(db), PostgresProjectionStore(db, "accounts")
    async with transaction(store) as tx:
        await store.append(tx, "acct-1", 0, [draft("acct-1", 1)])
    with pytest.raises(StoreError, match="aborted the transaction"):
        async with transaction(store) as tx:
            await projections.save(Entity("acct-1", 0, {"balance": 5}), tx=tx)
            with pytest.raises(CodecError):
                await store.append(tx, "acct-1", 1, [draft("acct-1", 2, id="acct-1-1")])
    assert await projections.get("acct-1") is None, "the save before the failure rolled back"


async def test_a_kernel_refusal_leaves_the_unit_of_work_usable(db: PostgresTransactor) -> None:
    store, projections = PostgresEventStore(db), PostgresProjectionStore(db, "accounts")
    async with transaction(store) as tx:
        await projections.save(Entity("acct-1", 0, {"balance": 5}), tx=tx)
        with pytest.raises(ConcurrencyError):
            await projections.save(Entity("acct-1", 0, {"balance": 6}), tx=tx)
    entity = await projections.get("acct-1")
    assert entity is not None and entity.document == {"id": "acct-1", "balance": 5}


async def test_wiring_mistakes_are_refused(db: PostgresTransactor, pool: Any) -> None:
    with pytest.raises(BadHandle, match="already owns this pool"):
        PostgresTransactor(pool, schema=SCHEMA)
    with pytest.raises(TypeError):
        PostgresProjectionStore(pool, "accounts")
    with pytest.raises(BadHandle):
        PostgresTransactor(object(), schema="Bad")  # ty: ignore[invalid-argument-type]
    with pytest.raises(BadHandle):
        # A lock wait past the statement timeout: the kernel refuses the set.
        PostgresTransactor(object(), settings=default_settings(lock_timeout_ms=60_000))  # ty: ignore[invalid-argument-type]
    with pytest.raises(BadHandle):
        default_settings(bogus_ms=1)


async def _from_another_task(
    db: PostgresTransactor, call: Callable[[PostgresTx], Coroutine[Any, Any, object]]
) -> None:
    async with transaction(db) as tx:
        await asyncio.create_task(call(tx))


async def _after_the_block(
    db: PostgresTransactor, call: Callable[[PostgresTx], Coroutine[Any, Any, object]]
) -> None:
    async with transaction(db) as tx:
        pass
    await call(tx)


@pytest.mark.parametrize(
    ("name", "use", "refusal"),
    [
        ("other_task", _from_another_task, "task that opened it"),
        ("closed_handle", _after_the_block, "has ended"),
    ],
)
async def test_a_handle_out_of_its_unit_of_work_is_refused(
    db: PostgresTransactor,
    name: str,
    use: Callable[[PostgresTransactor, Callable[[PostgresTx], Coroutine[Any, Any, object]]], Awaitable[None]],
    refusal: str,
) -> None:
    """A handle from another task, or one whose block has ended, would share one connection between two
    tasks or write outside the transaction the caller asked for: every store refuses it before any I/O."""
    store, projections = PostgresEventStore(db), PostgresProjectionStore(db, "accounts")
    with pytest.raises(BadHandle, match=refusal):
        await use(db, lambda tx: store.load(tx, "acct-1"))
    with pytest.raises(BadHandle, match=refusal):
        await use(db, lambda tx: projections.get("acct-1", tx=tx))


async def _complete_done(ledger: PostgresIdempotencyStore, key: str, won: Won, tx: PostgresTx) -> None:
    await ledger.complete(key, won.handle, Record.done(7, {"ok": True}), tx=tx)


async def _abandon(ledger: PostgresIdempotencyStore, key: str, won: Won, tx: PostgresTx) -> None:
    raise RolledBack


async def _release(ledger: PostgresIdempotencyStore, key: str, won: Won, tx: PostgresTx) -> None:
    await ledger.release(key, won.handle, tx=tx)


@pytest.mark.parametrize(
    ("name", "settle", "want", "after"),
    [
        ("holder_commits_done", _complete_done, Record.done(7, {"ok": True}), Record.done(7, {"ok": True})),
        ("holder_rolls_back", _abandon, Won, Record.pending(7)),
        ("holder_releases_and_commits", _release, Won, Record.pending(7)),
    ],
)
async def test_a_claim_waiting_on_an_open_holder(
    db: PostgresTransactor,
    pool: Any,
    name: str,
    settle: Callable[[PostgresIdempotencyStore, str, Won, PostgresTx], Awaitable[None]],
    want: Record | type[Won],
    after: Record,
) -> None:
    """A contender's claim waits on the row a holder's open unit of work inserted, within the lock wait, and
    reads how the holder ended: its outcome once committed, or the key free once rolled back or released."""
    ledger = PostgresIdempotencyStore(db)
    key = unique_id("req")
    with contextlib.suppress(RolledBack):
        async with transaction(db) as tx:
            won = await ledger.claim(key, 7, tx=tx)
            assert isinstance(won, Won)

            async def claim(other: PostgresTx) -> Claimed:
                return await ledger.claim(key, 7, tx=other)

            contending, pid = await contender(db, claim)
            await wait_for_lock(pool, "claim_insert", pid)
            assert not contending.done(), "the contender waits on the holder's row"
            await settle(ledger, key, won, tx)
    got = await asyncio.wait_for(contending, 3)
    if want is Won:
        assert isinstance(got, Won) and got != won, "the contender won the key"
    else:
        assert got == want, "the contender replays the holder's outcome"
    assert await ledger.claim(key, 7) == after, "the key holds what the winner left"


async def test_a_claim_waiting_on_an_open_holder_is_in_progress_then_replays(pool: Any) -> None:
    """The zombie window: a holder keeps a claim in an open unit of work longer than a contender's lock wait.
    The contender gets InProgress, which a dispatcher retries uncounted, and once the holder commits its
    outcome the contender replays it."""
    db = PostgresTransactor(pool, schema=SCHEMA, settings=default_settings(lock_timeout_ms=200))
    ledger = PostgresIdempotencyStore(db)
    key = unique_id("req")
    async with transaction(db) as tx:
        won = await ledger.claim(key, 7, tx=tx)
        assert isinstance(won, Won)

        async def contend() -> Any:
            leave_every_unit_of_work()  # another worker: none of this task's units of work
            async with transaction(db):
                return await ledger.claim(key, 7)

        with pytest.raises(InProgress):
            await asyncio.create_task(contend())
        await ledger.complete(key, won.handle, Record.done(7, {"ok": True}), tx=tx)
    assert await ledger.claim(key, 7) == Record.done(7, {"ok": True})


async def test_only_the_lease_holder_drains(db: PostgresTransactor) -> None:
    store = PostgresEventStore(db)
    async with transaction(store) as tx:
        await store.append(tx, "acct-1", 0, [draft("acct-1", 1)])
    first = PostgresEventStore(db, lease=RelayLease("relay-a", ttl_ms=300))
    second = PostgresEventStore(db, lease=RelayLease("relay-b", ttl_ms=300))
    assert [m.id for m in await first.unpublished(10)] == ["acct-1-1"]
    assert await second.unpublished(10) == [], "another relay holds the lease"
    assert len(await first.unpublished(10)) == 1, "the holder renews"
    await asyncio.sleep(0.4)
    assert len(await second.unpublished(10)) == 1, "a lapsed lease is taken over"
    assert await first.unpublished(10) == []


#: An outbox's rows as a relay holding ``lease`` reads them: their ids, or ``[]`` while another relay holds it.
Drain = Callable[[PostgresTransactor, RelayLease], Awaitable[list[str]]]


async def _drain_events(db: PostgresTransactor, lease: RelayLease) -> list[str]:
    return [m.id for m in await PostgresEventStore(db, lease=lease).unpublished(10)]


async def _drain_commands(db: PostgresTransactor, lease: RelayLease) -> list[str]:
    return [c.id for c in await PostgresCommandOutbox(db, lease=lease).unpublished(10)]


def _drain_saga(saga: str) -> Drain:
    async def drain(db: PostgresTransactor, lease: RelayLease) -> list[str]:
        return [c.id for c in await PostgresSagaStore(db, lease=lease).unpublished(saga, 10)]

    return drain


DRAINS: dict[str, Drain] = {
    "event_outbox": _drain_events,
    "command_outbox": _drain_commands,
    "saga_outbox_order": _drain_saga("order"),
    "saga_outbox_billing": _drain_saga("billing"),
}


async def _fill_every_outbox(db: PostgresTransactor) -> dict[str, list[str]]:
    """One committed row in each outbox of :data:`DRAINS`, written as the stores write them; their ids."""
    events, commands, sagas = PostgresEventStore(db), PostgresCommandOutbox(db), PostgresSagaStore(db)
    sent = [command("order-1")]
    order = [saga_command("order", "order-1", 1, 0)]
    billing = [saga_command("billing", "invoice-1", 1, 0)]
    async with transaction(db) as tx:
        stored = await events.append(tx, "acct-1", 0, [draft("acct-1", 1)])
        await commands.enqueue(tx, sent)
        await sagas.save("order", "order-1", {"step": "placed"}, 0, "", order, tx=tx)
        await sagas.save("billing", "invoice-1", {"step": "billed"}, 0, "", billing, tx=tx)
    return {
        "event_outbox": [stored[0].id],
        "command_outbox": [sent[0].id],
        "saga_outbox_order": [order[0].id],
        "saga_outbox_billing": [billing[0].id],
    }


@pytest.mark.parametrize("held", list(DRAINS))
async def test_each_outbox_has_its_own_lease(db: PostgresTransactor, held: str) -> None:
    """A relay holding one outbox's lease keeps every other relay off that outbox, and off no other: a relay
    per outbox (or per saga) drains in parallel."""
    want = await _fill_every_outbox(db)
    assert await DRAINS[held](db, RelayLease("relay-a")) == want[held], "relay-a takes the lease"
    assert await DRAINS[held](db, RelayLease("relay-b")) == [], "relay-a holds this outbox"
    for other, drain in DRAINS.items():
        if other != held:
            assert await drain(db, RelayLease("relay-b")) == want[other], f"the {held} lease holds {other}"


@pytest.mark.parametrize(
    "in_unit_of_work", [True, False], ids=["in_unit_of_work_waits", "standalone_does_not_lock"]
)
async def test_lock_reads(db: PostgresTransactor, pool: Any, in_unit_of_work: bool) -> None:
    """With ``lock_reads``, a ``get`` inside a unit of work holds the row until it ends, so a second writer
    waits for the first and writes on top of it instead of failing its compare-and-set. A ``get`` outside
    any unit of work locks nothing, so it answers at once while the row is held."""
    store = PostgresProjectionStore(db, "accounts", lock_reads=True)
    await store.save(Entity("acct-1", 0, {"balance": 5}))
    async with transaction(db) as tx:
        held = await store.get("acct-1", tx=tx)
        assert held is not None and held.version == 1
        if in_unit_of_work:

            async def read_then_save(other: PostgresTx) -> tuple[Entity | None, int]:
                seen = await store.get("acct-1", tx=other)
                assert seen is not None
                topped = {"balance": seen.document["balance"] + 10}
                return seen, await store.save(Entity("acct-1", seen.version, topped), tx=other)

            reading, pid = await contender(db, read_then_save)
            await wait_for_lock(pool, "projection_get", pid)
            assert not reading.done(), "the second reader waits on the held row"
            assert await store.save(Entity("acct-1", 1, {"balance": 6}), tx=tx) == 2
        else:

            async def read_alone() -> Entity | None:
                leave_every_unit_of_work()
                return await store.get("acct-1")

            seen = await asyncio.wait_for(asyncio.create_task(read_alone()), 3)
            assert seen == Entity("acct-1", 1, {"id": "acct-1", "balance": 5}), "read while the row is held"
            assert await store.save(Entity("acct-1", 1, {"balance": 6}), tx=tx) == 2
    if in_unit_of_work:
        seen, written = await asyncio.wait_for(reading, 3)
        assert seen == Entity("acct-1", 2, {"id": "acct-1", "balance": 6}), (
            "it read the first writer's commit"
        )
        assert written == 3
        assert await store.get("acct-1") == Entity("acct-1", 3, {"id": "acct-1", "balance": 16})
    else:
        assert await store.get("acct-1") == Entity("acct-1", 2, {"id": "acct-1", "balance": 6})


async def test_two_steps_of_one_saga_instance_wait_and_both_land(db: PostgresTransactor, pool: Any) -> None:
    """Inside a unit of work ``load`` holds the instance's row, so a second step of the same instance waits
    for the first to commit, then loads its version and writes on top: both steps' state and commands land,
    in order, and neither fails its compare-and-set."""
    sagas = PostgresSagaStore(db)
    await sagas.save("order", "order-1", {"step": "placed"}, 0, "", [])
    first, second = saga_command("order", "order-1", 2, 0), saga_command("order", "order-1", 3, 0)
    async with transaction(db) as tx:
        loaded = await sagas.load("order", "order-1", tx=tx)
        assert isinstance(loaded, RunningInstance) and loaded.version == 1

        async def step(other: PostgresTx) -> tuple[Any, int]:
            instance = await sagas.load("order", "order-1", tx=other)
            assert isinstance(instance, RunningInstance)
            state = {"step": "shipped", "after": instance.state["step"]}
            return instance, await sagas.save(
                "order", "order-1", state, instance.version, "", [second], tx=other
            )

        stepping, pid = await contender(db, step)
        await wait_for_lock(pool, "saga_load", pid)
        assert not stepping.done(), "the second step waits on the held instance"
        assert await sagas.save("order", "order-1", {"step": "paid"}, 1, "", [first], tx=tx) == 2
    instance, written = await asyncio.wait_for(stepping, 3)
    assert instance == RunningInstance({"step": "paid"}, 2, ""), "it loaded the first step's commit"
    assert written == 3
    final = await sagas.load("order", "order-1")
    assert final == RunningInstance({"step": "shipped", "after": "paid"}, 3, "")
    assert [c.id for c in await sagas.unpublished("order", 10)] == [first.id, second.id]


async def test_the_reference_schema_meets_the_contract(db: PostgresTransactor) -> None:
    for store in (
        PostgresEventStore(db, lease=RelayLease("r")),
        PostgresCommandOutbox(db),
        PostgresSagaStore(db),
    ):
        assert store.transactor is db
    PostgresProjectionStore(db, "accounts")
    PostgresIdempotencyStore(db)
    await db.check()


async def test_the_hosts_names_and_constraint_names_are_its_own(pool: Any) -> None:
    """A host names its tables and its constraints as it likes. The contract check finds which index guards
    which key, so a lost append race is still read as a version conflict. The loser read the stream's
    version before the winner committed, so only the unique index catches it, and its whole batch rolls back
    with its outbox rows."""
    schema, names = "edax_named", {"events": "ledger_events", "event_outbox": "ledger_event_outbox"}
    await provision(pool, schema, names)
    await pool.execute(
        f'ALTER TABLE "{schema}".ledger_events RENAME CONSTRAINT ledger_events_stream_sequence_key TO stream_order_unique'
    )
    db = PostgresTransactor(pool, schema=schema, tables=names)
    store = PostgresEventStore(db)
    waiting = await store.unpublished_count()
    async with transaction(db) as tx:
        won = await store.append(tx, "acct-1", 0, [draft("acct-1", 1, id="evt-winner")])

        async def append(other: PostgresTx) -> object:
            batch = [draft("acct-1", 1, id="evt-loser-1"), draft("acct-1", 2, id="evt-loser-2")]
            return await store.append(other, "acct-1", 0, batch)

        losing, pid = await contender(db, append)
        await wait_for_lock(pool, "append", pid)
        assert not losing.done(), "the loser waits on the winner's index entry"
    with pytest.raises(ConcurrencyError, match="23505 on events_stream_sequence"):
        await asyncio.wait_for(losing, 3)
    assert [m.id for m in await store.unpublished(10)] == [m.id for m in won] == ["evt-winner"]
    assert await store.unpublished_count() == waiting + 1, "the losing batch left no outbox row"


async def test_a_table_off_the_contract_is_refused_before_any_statement(pool: Any) -> None:
    schema = "edax_broken"
    await provision(pool, schema)
    await pool.execute(f'ALTER TABLE "{schema}".edax_ledger ALTER COLUMN record TYPE json USING record::json')
    await pool.execute(f'ALTER TABLE "{schema}".edax_ledger DROP CONSTRAINT edax_ledger_pkey')
    await pool.execute(f'DROP TABLE "{schema}".edax_projections')
    db = PostgresTransactor(pool, schema=schema)
    ledger = PostgresIdempotencyStore(db)
    with pytest.raises(BadHandle) as refused:
        await ledger.claim("k", 1)
    assert "record is json" in str(refused.value) and "no unique index on exactly (key)" in str(refused.value)
    # A failed check stays pending: the next call checks again, and names every table off the contract.
    with pytest.raises(BadHandle, match=r"edax_projections.*does not exist"):
        await PostgresProjectionStore(db, "p").get("x")


@pytest.mark.parametrize("case", golden("postgres_query.json"), ids=lambda c: c["name"])
async def test_query_vectors_through_get_many(db: PostgresTransactor, case: dict[str, Any]) -> None:
    store = PostgresProjectionStore(db, "golden")
    for document in case["documents"]:
        await store.save(Entity(document["id"], 0, document))
    query = Query(**case["query"])
    if case["error"] is not None or case["page_error"] is not None:
        with pytest.raises(type(from_wire(case["error"] or case["page_error"]))):
            await store.get_many(query)
        return
    want = case["expected_page"]
    assert await store.get_many(query) == Page(rows=want["rows"], next=want["next"])


_values = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**63), max_value=2**64 - 1),
    st.floats(allow_nan=False, allow_infinity=False),
    st.sampled_from(["", "a", "B", "é", "ｱ", "\U0001f980", "7", "07", "9.5", "10", "1E+2", "n/a"]),
    st.lists(st.integers(-3, 3), max_size=2),
    st.fixed_dictionaries({"x": st.integers(-3, 3)}),
)
_paths = st.sampled_from(["a", "b", "n.x", "missing", "id"])
_sort_keys = st.lists(
    st.builds(
        lambda path, desc, decimal: sort_key(path, desc, decimal=decimal),
        _paths,
        st.booleans(),
        st.booleans(),
    ),
    max_size=3,
)


@settings(max_examples=100, deadline=None)
@given(
    docs=st.lists(
        st.fixed_dictionaries(
            {}, optional={"a": _values, "b": _values, "n": st.fixed_dictionaries({"x": _values})}
        ),
        max_size=12,
    ),
    sort=_sort_keys,
    limit=st.integers(1, 5),
    bound=_values,
)
def test_get_many_pages_as_the_kernel_does(
    provisioned: None, docs: list[dict[str, Any]], sort: list[Any], limit: int, bound: Any
) -> None:
    """Random documents, a random sort and a range filter, walked page by page through Postgres and through
    the kernel's reference applier over the same stored documents."""

    async def run() -> None:
        pool = await asyncpg.create_pool(URL, min_size=1, max_size=2)
        try:
            db = PostgresTransactor(pool, schema=SCHEMA)
            store = PostgresProjectionStore(db, unique_id("h"))
            for i, doc in enumerate(docs):
                await store.save(Entity(f"d{i:02}", 0, doc))
            stored = [e.document for i in range(len(docs)) if (e := await store.get(f"d{i:02}")) is not None]
            only = None if bound is None else gte("a", bound)
            after: str | None = None
            for _ in range(20):
                q = Query(filter=only, sort=sort, limit=limit, after=after)
                try:
                    want = kernel_query(stored, q)
                except EdaxError as e:
                    with pytest.raises(type(e)):
                        await store.get_many(q)
                    return
                assert await store.get_many(q) == want
                if want.next is None or not sort:
                    return
                after = want.next
        finally:
            await pool.close()

    asyncio.run(run())


@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
@pytest.mark.parametrize("unit_of_work", [False, True], ids=["standalone", "unit_of_work"])
async def test_transactions_force_read_committed(
    provisioned: None, isolation: str, unit_of_work: bool
) -> None:
    async with asyncpg.create_pool(
        URL,
        min_size=1,
        max_size=1,
        server_settings={"default_transaction_isolation": isolation},
    ) as pool:
        db = PostgresTransactor(pool, schema=SCHEMA)

        async def read(conn: Any) -> str:
            return await conn.fetchval("SHOW transaction_isolation")

        if unit_of_work:
            async with transaction(db):
                assert await db.run("load", None, read) == "read committed"
        else:
            assert await db.run("load", None, read) == "read committed"
        assert await pool.fetchval("SHOW default_transaction_isolation") == isolation


async def test_idle_timeout_frees_lock_and_restores_pooled_settings(provisioned: None) -> None:
    async with asyncpg.create_pool(
        URL,
        min_size=1,
        max_size=1,
        server_settings={"idle_in_transaction_session_timeout": "7s"},
    ) as pool:
        db = PostgresTransactor(pool, schema=SCHEMA, settings=default_settings(idle_in_transaction_ms=100))
        ledger = PostgresIdempotencyStore(db)
        await db.check()
        key = unique_id("idle")
        disconnected = asyncio.Event()
        with pytest.raises(StoreError):
            async with transaction(db) as tx:
                conn: Any = tx.connection
                conn.add_termination_listener(lambda _: disconnected.set())
                assert isinstance(await ledger.claim(key, 7), Won)
                await asyncio.wait_for(disconnected.wait(), 3)
                await ledger.claim(key, 7)
        assert isinstance(await ledger.claim(key, 7), Won), "the terminated transaction freed its claim"
        assert await pool.fetchval("SHOW idle_in_transaction_session_timeout") == "7s"
        assert await pool.fetchval("SHOW lock_timeout") == "0"
        assert await pool.fetchval("SHOW statement_timeout") == "0"
        assert pool.get_idle_size() == pool.get_size() == 1


async def test_completion_waiting_on_takeover_rollback_keeps_original_owner(db: PostgresTransactor) -> None:
    ledger = PostgresIdempotencyStore(db)
    key = "takeover-rollback"
    won = await ledger.claim(key, 7)
    assert isinstance(won, Won)
    pool: Any = db._pool
    async with pool.acquire() as conn:
        takeover = conn.transaction()
        await takeover.start()
        await conn.execute(f"UPDATE {tables(db)['ledger']} SET owner = $2 WHERE key = $1", key, "new-owner")
        completing = asyncio.create_task(ledger.complete(key, won.handle, Record.done(7, {"paid": True})))
        try:
            await wait_for_lock(pool, "ledger_hold")
            assert not completing.done()
        finally:
            await takeover.rollback()
        await completing
    assert await ledger.claim(key, 7) == Record.done(7, {"paid": True})


async def test_double_cancellation_leaves_guard_release_running_and_pool_reusable(
    db: PostgresTransactor, monkeypatch: pytest.MonkeyPatch
) -> None:
    from edax.idempotency import Guard, guarded
    from edax.observe import Observer

    ledger = PostgresIdempotencyStore(db)
    started, releasing, released, allow_release = (asyncio.Event() for _ in range(4))
    release = ledger.release
    key = "double-cancel-guard"

    async def delayed_release(key: str, handle: bytes, tx: Any = None) -> None:
        releasing.set()
        await allow_release.wait()
        await release(key, handle, tx)
        released.set()

    monkeypatch.setattr(ledger, "release", delayed_release)

    async def run() -> None:
        async with transaction(db):
            started.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(guarded(Guard(ledger, "Charge", key, 7, Observer()), run, None))
    await asyncio.wait_for(started.wait(), 3)
    task.cancel()
    await asyncio.wait_for(releasing.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    allow_release.set()
    await asyncio.wait_for(released.wait(), 3)
    assert isinstance(await ledger.claim(key, 7), Won)
    pool: Any = db._pool
    assert await pool.fetchval("SELECT 1") == 1
    assert pool.get_idle_size() == pool.get_size()


async def test_startup_refuses_lock_timeout_before_deadlock_detection(provisioned: None) -> None:
    async with asyncpg.create_pool(
        URL, min_size=1, max_size=1, server_settings={"deadlock_timeout": "1s"}
    ) as pool:
        db = PostgresTransactor(pool, schema=SCHEMA, settings=default_settings(lock_timeout_ms=200))
        with pytest.raises(BadHandle, match="deadlock_timeout"):
            await db.check()

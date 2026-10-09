"""The Postgres stores against a live server: every packaged contract, the kernel's query vectors through
``get_many``, and the faults only a real server has: a lock wait, an aborted transaction, a relay lease, a
second transactor. Set ``EDAX_POSTGRES_URL`` (a throwaway database, ``postgresql://postgres:edax@localhost:55433/
postgres``) to run them; ``EDAX_REQUIRE_POSTGRES=1`` makes a missing URL fail instead of skip, as CI sets it.
The SQL itself is held to the kernel's reference applier in ``crates/edax-postgres/live-check``."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from typing import Any

import pytest
from edax._units import leave_every_unit_of_work
from edax.backend.postgres import (
    PostgresCommandOutbox,
    PostgresEventStore,
    PostgresIdempotencyStore,
    PostgresProjectionStore,
    PostgresSagaStore,
    PostgresTransactor,
    RelayLease,
    default_settings,
    reference_ddl,
)
from edax.errors import BadHandle, CodecError, ConcurrencyError, EdaxError, InProgress, StoreError, from_wire
from edax.filter import gte
from edax.idempotency import Record, Won
from edax.message import EventDraft
from edax.projection import Entity
from edax.query import Page, Query, sort_key
from edax.query import query as kernel_query
from edax.saga import SagaOutbox
from edax.testing import (
    CommandOutboxContract,
    EventOutboxContract,
    EventStoreContract,
    IdempotencyStoreContract,
    ProjectionStoreContract,
    SagaOutboxContract,
    SagaStoreContract,
)
from edax.unit_of_work import transaction
from hypothesis import given, settings
from hypothesis import strategies as st

URL = os.environ.get("EDAX_POSTGRES_URL")
if URL is None:
    if os.environ.get("EDAX_REQUIRE_POSTGRES") == "1":
        raise RuntimeError("EDAX_REQUIRE_POSTGRES=1 but EDAX_POSTGRES_URL is not set")
    pytest.skip("EDAX_POSTGRES_URL is not set: no live Postgres", allow_module_level=True)

asyncpg = pytest.importorskip("asyncpg")

SCHEMA = "edax_test"
TABLES = (
    "edax_event_outbox",
    "edax_events",
    "edax_snapshots",
    "edax_command_outbox",
    "edax_projections",
    "edax_ledger",
    "edax_saga_outbox",
    "edax_saga_instances",
    "edax_relay_leases",
)
GOLDEN = os.path.join(os.path.dirname(__file__), "..", "..", "..", "testdata", "golden")


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


@pytest.fixture
async def db(pool: Any) -> AsyncIterator[PostgresTransactor]:
    transactor = PostgresTransactor(pool, schema=SCHEMA, settings=default_settings(lock_timeout_ms=1_000))
    tables = ", ".join(f'"{SCHEMA}".{table}' for table in TABLES)
    await pool.execute(f"TRUNCATE {tables}")
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
        pool = idempotency_store.transactor._pool
        await pool.execute(f'DELETE FROM "{SCHEMA}".edax_ledger WHERE key = $1 AND owner IS NULL', key)
        await pool.execute(
            f"UPDATE \"{SCHEMA}\".edax_ledger SET claimed_at = '-infinity' WHERE key = $1 AND owner IS NOT NULL",
            key,
        )

    async def put_raw_record(self, idempotency_store: Any, key: str, raw: str) -> None:
        await idempotency_store.transactor._pool.execute(
            f'INSERT INTO "{SCHEMA}".edax_ledger (key, record, claimed_at) VALUES ($1, $2, now())', key, raw
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


async def test_append_stamps_the_database_clock_and_keeps_every_field(
    db: PostgresTransactor, pool: Any
) -> None:
    store = PostgresEventStore(db)
    async with transaction(store) as tx:
        before = await pool.fetchval("SELECT floor(extract(epoch FROM clock_timestamp()) * 1e9)::bigint")
        stored = await store.append(tx, "acct-1", 0, [draft("acct-1", 1), draft("acct-1", 2, occurred_at=7)])
    assert stored[0].recorded_at == stored[1].recorded_at, "one clock reading per append"
    assert stored[0].recorded_at >= before - 1_000
    async with transaction(store) as tx:
        loaded = await store.load(tx, "acct-1")
    assert loaded == stored
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


async def test_a_handle_used_from_another_task_is_refused(db: PostgresTransactor) -> None:
    projections = PostgresProjectionStore(db, "accounts")
    async with transaction(db) as tx:
        with pytest.raises(BadHandle, match="task that opened it"):
            await asyncio.create_task(projections.get("acct-1", tx=tx))


async def test_a_claim_waiting_on_an_open_holder_is_in_progress_then_replays(pool: Any) -> None:
    """The zombie window: a holder keeps a claim in an open unit of work longer than a contender's lock wait.
    The contender gets InProgress, which a dispatcher retries uncounted, and once the holder commits its
    outcome the contender replays it."""
    db = PostgresTransactor(pool, schema=SCHEMA, settings=default_settings(lock_timeout_ms=200))
    ledger = PostgresIdempotencyStore(db)
    key = f"req-{os.urandom(4).hex()}"
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
    which key, so a lost append race is still read as a version conflict."""
    schema, names = "edax_named", {"events": "ledger_events", "event_outbox": "ledger_event_outbox"}
    await provision(pool, schema, names)
    await pool.execute(
        f'ALTER TABLE "{schema}".ledger_events RENAME CONSTRAINT ledger_events_stream_sequence_key TO stream_order_unique'
    )
    db = PostgresTransactor(pool, schema=schema, tables=names)
    store = PostgresEventStore(db)
    outcomes = await asyncio.gather(
        *(_append_in_its_own_task(store, f"evt-{i}") for i in range(2)), return_exceptions=True
    )
    assert sorted(type(o).__name__ for o in outcomes) == ["ConcurrencyError", "list"], outcomes


async def _append_in_its_own_task(store: PostgresEventStore, event_id: str) -> Any:
    leave_every_unit_of_work()
    async with transaction(store) as tx:
        appended = await store.append(tx, "acct-1", 0, [draft("acct-1", 1, id=event_id)])
        await asyncio.sleep(0.2)  # both stage before either commits
        return appended


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


def _golden(name: str) -> list[dict[str, Any]]:
    with open(os.path.join(GOLDEN, name), encoding="utf-8") as f:
        cases: list[dict[str, Any]] = json.load(f)
    return cases


@pytest.mark.parametrize("case", _golden("postgres_query.json"), ids=lambda c: c["name"])
async def test_query_vectors_through_get_many(db: PostgresTransactor, case: dict[str, Any]) -> None:
    store = PostgresProjectionStore(db, "golden")
    for document in case["documents"]:
        await store.save(Entity(document["id"], 0, document))
    spec = case["query"]
    query = Query(
        filter=spec.get("filter"),
        sort=spec.get("sort", ()),
        offset=spec.get("offset", 0),
        limit=spec.get("limit"),
        after=spec.get("after"),
    )
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
            store = PostgresProjectionStore(db, f"h-{os.urandom(6).hex()}")
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
        key = f"idle-{os.urandom(4).hex()}"
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
        await conn.execute(f'UPDATE "{SCHEMA}".edax_ledger SET owner = $2 WHERE key = $1', key, "new-owner")
        completing = asyncio.create_task(ledger.complete(key, won.handle, Record.done(7, {"paid": True})))

        async def wait_for_lock() -> None:
            while not await pool.fetchval(
                "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
                "AND query LIKE '/* edax:ledger_hold */%')"
            ):
                await asyncio.sleep(0.01)

        try:
            await asyncio.wait_for(wait_for_lock(), 2)
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

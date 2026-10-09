"""The Postgres adapter's own decisions, without a server: every failure typed by the kernel's verdict, the
settings, the relay lease, and the wiring refusals. The live suite (``test_postgres.py``) needs a server."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from conftest import golden, wire_of
from edax import _native
from edax.backend import postgres as pg
from edax.errors import BadHandle, CodecError, ConcurrencyError, EdaxError, InProgress, StoreError
from edax.message import CommandMessage, EventDraft
from edax.query import Query
from edax.unit_of_work import transaction

_PUBLIC = '{"schema": "public"}'


class ServerError(Exception):
    """What asyncpg raises for a server error: a SQLSTATE and, for a violation, the constraint."""

    def __init__(self, sqlstate: str, constraint: str | None = None) -> None:
        super().__init__(f"server says {sqlstate}")
        self.sqlstate = sqlstate
        self.constraint_name = constraint


class ConnectionDoesNotExistError(Exception):
    """Named like asyncpg's client error for a closed connection."""


@pytest.mark.parametrize(
    ("name", "op", "error", "want"),
    [
        (
            "lost_append_race",
            "append",
            ServerError("23505", "edax_events_stream_sequence_key"),
            ConcurrencyError,
        ),
        ("reused_event_id", "append", ServerError("23505", "edax_events_pkey"), CodecError),
        ("claim_lock_wait", "claim", ServerError("55P03"), InProgress),
        ("settle_lock_wait", "settle", ServerError("55P03"), ConcurrencyError),
        ("missing_table", "load", ServerError("42P01"), BadHandle),
        ("serialization", "projection_save", ServerError("40001"), StoreError),
        ("client_timeout", "load", asyncio.TimeoutError(), StoreError),
        ("closed_connection", "load", ConnectionDoesNotExistError("gone"), StoreError),
        ("os_error", "load", OSError("refused"), StoreError),
        ("unencodable_parameter", "load", ValueError("bad text"), CodecError),
        ("anything_else", "load", RuntimeError("?"), StoreError),
    ],
)
def test_every_failure_is_typed_by_the_kernel(name: str, op: Any, error: Exception, want: type) -> None:
    # The index names the reference schema gives the events' two unique keys, as the table check maps them.
    unique = {"edax_events_stream_sequence_key": "events_stream_sequence", "edax_events_pkey": "events_id"}
    assert type(pg._typed(op, error, unique)) is want, name


def test_settings_start_from_the_kernels_defaults() -> None:
    settings = pg.default_settings(lock_timeout_ms=250)
    assert settings.lock_timeout_ms == 250
    assert settings.statement_timeout_ms == pg.default_settings().statement_timeout_ms
    with pytest.raises(BadHandle):
        pg.default_settings(lock_timeout=1)
    with pytest.raises(BadHandle):
        pg.default_settings(lock_timeout_ms=True)


def test_a_lease_needs_an_owner_and_a_positive_length() -> None:
    assert pg.RelayLease("relay-a").ttl_ms > 0
    with pytest.raises(BadHandle):
        pg.RelayLease("relay-a", ttl_ms=0)
    with pytest.raises(BadHandle):
        pg.RelayLease(7)  # ty: ignore[invalid-argument-type]


class FakePool:
    """A pool no statement ever reaches: the refusals below all come before any I/O."""

    def acquire(self, *, timeout: float | None = None) -> Any:
        raise AssertionError("no I/O expected")


async def test_wiring_refusals_come_before_any_io() -> None:
    pool = FakePool()
    db = pg.PostgresTransactor(pool, schema="edax")
    with pytest.raises(BadHandle, match="already owns this pool"):
        pg.PostgresTransactor(pool)
    with pytest.raises(TypeError):
        pg.PostgresEventStore(pool)  # ty: ignore[invalid-argument-type]
    with pytest.raises(BadHandle):
        pg.PostgresTransactor(FakePool(), schema="Edax")
    with pytest.raises(BadHandle):
        pg.PostgresTransactor(FakePool(), settings=pg.default_settings(lock_timeout_ms=60_000))
    store = pg.PostgresProjectionStore(db, "accounts")
    from edax.backend.memory import MemoryTransactor

    async with transaction(MemoryTransactor()) as foreign:
        with pytest.raises(BadHandle, match="not a transaction of this PostgresTransactor"):
            await store.get("a", tx=foreign)
    # A spec the kernel refuses never reaches the pool.
    with pytest.raises(CodecError):
        await store.get_many(Query(after="cursor"))


def test_reference_ddl_is_the_kernels() -> None:
    ddl = pg.reference_ddl("tenant_7", {"events": "t7_events"})
    assert 'CREATE TABLE "tenant_7"."t7_events"' in ddl
    assert 'CREATE TABLE "tenant_7"."edax_ledger"' in ddl, "unnamed roles take the default"
    with pytest.raises(BadHandle):
        pg.reference_ddl("public", {"evnts": "x"})


def test_a_unique_violation_is_read_by_the_key_its_index_guards() -> None:
    race = ServerError("23505", "stream_order_unique")
    assert (
        type(pg._typed("append", race, {"stream_order_unique": "events_stream_sequence"})) is ConcurrencyError
    )
    assert type(pg._typed("append", race, {})) is CodecError, (
        "an index the check did not map is no version race"
    )


# --- The kernel's Postgres vectors, replayed through the binding: what a second SDK sharing the database
# --- must reproduce. The live suite and `crates/edax-postgres/live-check` run the SQL itself.


def _draft(row: dict[str, Any]) -> EventDraft:
    return EventDraft(
        id=row["id"],
        aggregate_id=row["aggregate_id"],
        name=row["name"],
        version=row["version"],
        data=bytes.fromhex(row["data"]),
        correlation_id=row["correlation_id"],
        causation_id=row["causation_id"],
        extensions=json.loads(row["extensions"]),
        metadata=json.loads(row["metadata"]),
        tenant=row["tenant"],
        sequence=row["sequence"],
        occurred_at=row["occurred_at"],
    )


def _command(row: dict[str, Any]) -> CommandMessage:
    return CommandMessage(
        id=row["id"],
        name=row["name"],
        version=row["version"],
        data=bytes.fromhex(row["data"]),
        correlation_id=row["correlation_id"],
        causation_id=row["causation_id"],
        issued_at=row["issued_at"],
        extensions=json.loads(row["extensions"]),
        metadata=json.loads(row["metadata"]),
        idempotency_key=row["idempotency_key"],
        aggregate_id=row["aggregate_id"],
        tenant=row["tenant"],
    )


def _refusal(call: Any) -> str | None:
    try:
        call()
    except EdaxError as e:
        return wire_of(e)
    return None


def test_statements_vector() -> None:
    want = {case["name"]: case["sql"] for case in golden("postgres_statements.json")}
    assert json.loads(_native.postgres_statements(_PUBLIC)) == want


@pytest.mark.parametrize("case", golden("postgres_contract.json"), ids=lambda c: c["name"])
def test_contract_vector(case: dict[str, Any]) -> None:
    """The table contract, the reference schema, the table names a host may choose, the verdict on catalog
    replies and the server checks, as the kernel answers them through the binding."""
    if "contract" in case:
        assert case["version"] == _native.postgres_contract_version()
    elif "ddl" in case:
        assert _native.postgres_reference_ddl(case["tables"]) == case["ddl"]
    elif "catalog" in case:
        check = lambda: _native.postgres_check_catalog(case["tables"], case["roles"], case["catalog"])  # noqa: E731
        if case.get("error") is not None:
            assert _refusal(check) == case["error"]
        else:
            assert json.loads(check()) == case["compatibility"]
    elif "tables" in case:
        assert _refusal(lambda: _native.postgres_tables(case["tables"])) == case["error"]
    else:

        def check() -> None:
            _native.postgres_check_server(
                case["server_encoding"],
                case["server_version_num"],
                case["deadlock_timeout_ms"],
                pg.PostgresSettings(**case["settings"]),
            )

        assert _refusal(check) == case["error"]


@pytest.mark.parametrize("case", golden("postgres_error.json"), ids=lambda c: c["name"])
def test_error_vector(case: dict[str, Any]) -> None:
    if "sqlstate" in case:
        got = _native.postgres_verdict(case["op"], case["sqlstate"], case["unique"], case["message"])
    else:
        got = _native.postgres_client_verdict(case["op"], case["kind"], case["message"])
    assert got == case["error"]


@pytest.mark.parametrize("case", golden("postgres_ledger.json"), ids=lambda c: c["name"])
def test_ledger_vector(case: dict[str, Any]) -> None:
    if case["name"] == "pending_record":
        params = json.loads(_native.postgres_claim_params("k", "o", case["payload_hash"]))
        assert params[2] == case["record"]
        return
    if "row" not in case:
        return  # pending_cannot_settle: replayed by the own_claim row below
    row = None if case["row"] is None else json.dumps(case["row"])
    handle = case["handle"].encode()
    read = _native.postgres_claim_read(row)
    claim = {"vanished": "vanished", "prior": {"prior": read.value}, "take_over": {"take_over": read.value}}
    assert claim[read.action] == case["claim"]
    complete = case["complete"]
    assert (
        _refusal(lambda: _native.postgres_complete_write("order-7", row, handle, case["settling"]))
        == (complete["error"])
    )
    if complete["error"] is None:
        assert _native.postgres_complete_write("order-7", row, handle, case["settling"]) == complete["write"]
    assert _native.postgres_release_write(row, handle) == case["release"]["write"]
    forget = case["forget"]
    assert _refusal(lambda: _native.postgres_forget_write("order-7", row)) == forget["error"]
    if forget["error"] is None:
        assert _native.postgres_forget_write("order-7", row) == forget["write"]


@pytest.mark.parametrize("case", golden("postgres_rows.json"), ids=lambda c: c["name"])
def test_rows_vector(case: dict[str, Any]) -> None:
    name = case["name"]
    if name == "append_params":
        drafts = [_draft(row) for row in json.loads(case["params"][2])]
        got = json.loads(_native.postgres_append_params(case["params"][0], case["expected"], drafts))
        assert got == case["params"]
    elif name == "append_result":
        events = _native.postgres_append_result(case["expected"], len(case["events"]), case["result"])
        assert [e.id for e in events] == case["events"]
    elif name == "append_result_conflict":
        assert (
            _refusal(lambda: _native.postgres_append_result(case["expected"], 1, case["result"]))
            == case["error"]
        )
    elif name == "event_row":
        assert len(_native.postgres_decode_events([case["row"]])) == case["decoded"]
    elif name == "commands_param":
        commands = [_command(row) for row in json.loads(case["params"])]
        assert _native.postgres_commands_param(commands) == case["params"]
        rows = [json.dumps(row) for row in json.loads(case["params"])]
        assert [c.id for c in _native.postgres_decode_commands(rows)] == case["decoded"]
    elif name == "ids_param_drops_nul":
        assert _native.postgres_ids_param(["a", "b\x00"]) == case["params"]
    elif name == "bound_param_clamps":
        assert [_native.postgres_bound_param(5), _native.postgres_bound_param(2**64 - 1)] == case["params"]
    elif name == "snapshot_params":
        assert (
            json.loads(_native.postgres_snapshot_params("invoice-7", "Invoice", 3, b"\x01\x02"))
            == case["params"]
        )
    elif name == "snapshot_leads":
        assert _refusal(lambda: _native.postgres_snapshot_result("invoice-7", 4, 3)) == case["error"]
    elif name == "projection_conflict":
        assert _refusal(lambda: _native.postgres_projection_result("acct-1", 2, None)) == case["error"]
    elif name in ("saga_insert_params", "saga_complete_params"):
        saga, instance, *rest = case["params"]
        version = 0 if name == "saga_insert_params" else int(rest.pop(0))
        state, folded, _completed, commands_json = rest
        commands = [_command(row) for row in json.loads(commands_json)]
        got = json.loads(_native.postgres_saga_params(saga, instance, version, state, folded, commands))
        assert got == case["params"]
    elif name in ("saga_completed", "saga_conflict"):
        assert (
            _refusal(lambda: _native.postgres_saga_result("ship", "order-1", 3, case["result"]))
            == case["error"]
        )
    elif name == "saga_foreign_command":
        assert case["error"].startswith("codec:")
    elif name == "stored_document_exact":
        assert _native.postgres_stored_document(case["id"], case["document"]) == case["stored"]
    elif name in ("append_refuses_a_gap", "append_refuses_nul", "stored_document_nul"):
        assert case["error"].startswith("codec:")
    else:
        raise AssertionError(f"a postgres_rows.json case no replay covers: {name}")


@pytest.mark.parametrize("case", golden("postgres_query.json"), ids=lambda c: c["name"])
def test_query_vector_compiles_alike(case: dict[str, Any]) -> None:
    spec = json.dumps(case["query"])
    if case["error"] is not None:
        assert _refusal(lambda: _native.postgres_compile_query(_PUBLIC, "accounts", spec)) == case["error"]
        return
    compiled = _native.postgres_compile_query(_PUBLIC, "accounts", spec)
    assert compiled.sql == case["sql"]
    assert json.loads(compiled.params_json) == case["params"]


async def test_failed_termination_preserves_the_original_store_error() -> None:
    asyncpg = pytest.importorskip("asyncpg")
    db = pg.PostgresTransactor(FakePool())

    class ReleasedConnection:
        def terminate(self) -> None:
            raise asyncpg.InterfaceError("connection has been released back to the pool")

    async def rollback() -> None:
        raise asyncpg.InterfaceError("connection is closed")

    original = StoreError("postgres load: connection is closed")
    with pytest.raises(StoreError) as caught:
        try:
            raise original
        except StoreError:
            await db._end(ReleasedConnection(), rollback)  # ty: ignore[invalid-argument-type]
            raise
    assert caught.value is original


async def test_second_cancellation_during_rollback_terminates_connection() -> None:
    db = pg.PostgresTransactor(FakePool())
    running, rolling_back = asyncio.Event(), asyncio.Event()

    class Connection:
        terminated = False

        def terminate(self) -> None:
            self.terminated = True

    conn = Connection()

    async def rollback() -> None:
        rolling_back.set()
        await asyncio.Event().wait()

    async def work() -> None:
        try:
            running.set()
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await db._end(conn, rollback)  # ty: ignore[invalid-argument-type]
            raise

    task = asyncio.create_task(work())
    await asyncio.wait_for(running.wait(), 1)
    task.cancel()
    await asyncio.wait_for(rolling_back.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert conn.terminated

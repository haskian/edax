"""The Postgres adapter's own decisions, without a server: every failure typed by the kernel's verdict, the
settings, the relay lease, and the wiring refusals. The live suite (``test_postgres.py``) needs a server."""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

import pytest
from conftest import golden, wire_of
from edax import _native
from edax.backend import postgres as pg
from edax.errors import BadHandle, CodecError, ConcurrencyError, EdaxError, InProgress, StoreError
from edax.message import CommandMessage, EventDraft
from edax.query import Query
from edax.testing.messages import drafted
from edax.unit_of_work import transaction

_PUBLIC = '{"schema": "public"}'


class ServerError(Exception):
    """What asyncpg raises for a server error: a SQLSTATE and, for a violation, the constraint."""

    def __init__(self, sqlstate: str, constraint: str | None = None) -> None:
        super().__init__(f"server says {sqlstate}")
        self.sqlstate = sqlstate
        self.constraint_name = constraint


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
        ("os_error", "load", OSError("refused"), StoreError),
        ("unencodable_parameter", "load", ValueError("bad text"), CodecError),
        ("anything_else", "load", RuntimeError("?"), StoreError),
    ],
)
def test_every_failure_is_typed_by_the_kernel(name: str, op: Any, error: Exception, want: type) -> None:
    # The index names the reference schema gives the events' two unique keys, as the table check maps them.
    unique = {"edax_events_stream_sequence_key": "events_stream_sequence", "edax_events_pkey": "events_id"}
    assert type(pg.postgres_error(op, error, unique)) is want, name


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
        type(pg.postgres_error("append", race, {"stream_order_unique": "events_stream_sequence"}))
        is ConcurrencyError
    )
    assert type(pg.postgres_error("append", race, {})) is CodecError, (
        "an index the check did not map is no version race"
    )


# --- The kernel's Postgres vectors, replayed through the binding: what a second SDK sharing the database
# --- must reproduce. The live suite and `crates/edax-postgres/live-check` run the SQL itself.


def _draft(row: dict[str, Any]) -> EventDraft:
    """The draft a vector's event row holds, read back by the kernel."""
    return drafted(_native.postgres_decode_events([json.dumps(row)])[0])


def _command(row: dict[str, Any]) -> CommandMessage:
    """The command a vector's command row holds, read back by the kernel."""
    return _native.postgres_decode_commands([json.dumps(row)])[0]


def _answer(answer: str | _native.PostgresCall) -> dict[str, Any]:
    """A parameter call's answer as its vector holds it: the parameters, and the statement when it chose one."""
    if isinstance(answer, str):
        return {"params": json.loads(answer)}
    return {"statement": answer.statement, "params": json.loads(answer.params_json)}


def _refusal(call: Any) -> str | None:
    try:
        call()
    except EdaxError as e:
        return wire_of(e)
    return None


def test_statements_vector() -> None:
    cases = golden("postgres_statements.json")
    want = {case["name"]: case["sql"] for case in cases if "settings" not in case}
    assert json.loads(_native.postgres_statements(_PUBLIC)) == want
    for case in cases:
        if "settings" in case:
            assert _native.postgres_begin(pg.PostgresSettings(**case["settings"])) == case["sql"]


@pytest.mark.parametrize("case", golden("postgres_contract.json"), ids=lambda c: c["name"])
def test_contract_vector(case: dict[str, Any]) -> None:
    """The table contract, the reference schema, the table names a host may choose, the verdict on catalog
    replies and the server checks, as the kernel answers them through the binding."""
    if "contract" in case:
        assert f"table contract {case['version']}:" in _native.postgres_reference_ddl(_PUBLIC)
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
    if "attempt" in case:
        read = lambda: _native.postgres_claim_read(case["key"], case["attempt"], None)  # noqa: E731
        assert _refusal(read) == case["error"]
        if case["error"] is None:
            assert read().action == case["action"]
        return
    if case["name"] == "settle_params":
        settle = _native.postgres_settle_params(case["key"], case["handle"].encode(), case["record"])
        assert json.loads(settle) == case["params"]
        return
    if "row" not in case:
        return  # pending_cannot_settle: replayed by the own_claim row below
    row = None if case["row"] is None else json.dumps(case["row"])
    handle = case["handle"].encode()
    read = _native.postgres_claim_read("order-7", 1, row)
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
    if case.get("kind") == "sealed_event":
        event = _native.postgres_decode_events([case["row"]])[0]
        assert json.loads(_native.encode_event("json", event)) == case["message"]
        draft = drafted(event)
        assert (
            json.loads(_native.postgres_append_params(case["stream"], case["expected"], [draft]))
            == case["params"]
        )
        appended = _native.postgres_append_result(case["expected"], [draft], case["result"])[0]
        assert json.loads(_native.encode_event("json", appended)) == case["message"]
        assert _native.open_event(event, bytes.fromhex(case["key_hex"])) == bytes.fromhex(
            case["plaintext_hex"]
        )
    elif case.get("kind") == "sealed_command":
        command = _native.postgres_decode_commands([case["row"]])[0]
        assert json.loads(_native.encode_command("json", command)) == case["message"]
        assert json.loads(_native.postgres_enqueue_params([command], None)) == case["params"]
        assert _native.open_command(command, bytes.fromhex(case["key_hex"])) == bytes.fromhex(
            case["plaintext_hex"]
        )
    elif case.get("kind") == "sealed_snapshot":
        snapshot = _native.postgres_decode_snapshot(case["row"])
        stored = json.loads(case["row"])
        assert (snapshot.type_name, snapshot.version, snapshot.tenant, snapshot.subject) == (
            stored["type_name"],
            stored["version"],
            stored["tenant"],
            stored["subject"],
        )
        assert bytes(snapshot.data).hex() == stored["data"]
        assert snapshot.encryption is not None
        assert (
            json.loads(_native.postgres_protected_snapshot_params(case["stream"], snapshot)) == case["params"]
        )
        aad = _native.state_encryption_context(
            "snapshot",
            case["stream"],
            snapshot.type_name,
            snapshot.version,
            "",
            False,
            snapshot.encryption,
            snapshot.tenant,
            snapshot.subject,
        )
        assert bytes(aad).hex() == case["aad_hex"]
        assert _native.open_bytes(
            snapshot.encryption, bytes.fromhex(case["key_hex"]), aad, snapshot.data
        ) == bytes.fromhex(case["plaintext_hex"])
    elif case.get("kind") == "erasure_writer_locks":
        scopes = [_native.KeyScope(**scope) for scope in case["scopes"]]
        assert _refusal(lambda: _native.postgres_erasure_writer_locks(scopes)) == case["error"]
        if case["error"] is None:
            assert json.loads(_native.postgres_erasure_writer_locks(scopes)) == case["want"]
    elif "call" in case:
        call = getattr(_native, f"postgres_{case['call']}")
        assert _refusal(lambda: call(*case["args"])) == case["error"]
        if case["error"] is None:
            assert _answer(call(*case["args"])) == case["want"]
    elif name == "append_params":
        drafts = [_draft(row) for row in json.loads(case["params"][2])]
        got = json.loads(_native.postgres_append_params(case["params"][0], case["expected"], drafts))
        assert got == case["params"]
    elif name == "append_result":
        drafts = [_draft(row) for row in case["draft_rows"]]
        events = _native.postgres_append_result(case["expected"], drafts, case["result"])
        assert [{"id": e.id, "recorded_at": e.recorded_at} for e in events] == case["events"]
    elif name == "append_result_conflict":
        drafts = [_draft(row) for row in case["draft_rows"]]
        result = lambda: _native.postgres_append_result(case["expected"], drafts, case["result"])  # noqa: E731
        assert _refusal(result) == case["error"]
    elif name == "event_row":
        assert len(_native.postgres_decode_events([case["row"]])) == case["decoded"]
    elif name == "enqueue_params":
        rows = json.loads(case["params"][0])
        assert (
            json.loads(_native.postgres_enqueue_params([_command(row) for row in rows], case["after_event"]))
            == case["params"]
        )
        decoded = _native.postgres_decode_commands([json.dumps(row) for row in rows])
        assert [c.id for c in decoded] == case["decoded"]
    elif name == "snapshot_params":
        assert (
            json.loads(_native.postgres_snapshot_params("invoice-7", "Invoice", 3, b"\x01\x02"))
            == case["params"]
        )
    elif name == "snapshot_leads":
        assert _refusal(lambda: _native.postgres_snapshot_result("invoice-7", 4, 3)) == case["error"]
    elif name == "projection_conflict":
        assert _refusal(lambda: _native.postgres_projection_result("acct-1", 2, None)) == case["error"]
    elif name in ("saga_insert", "saga_complete", "saga_foreign_command", "saga_refuses_nul_in_metadata"):
        commands = [_command(row) for row in case["command_rows"]]
        args = (case["saga"], case["id"], case["version"], case["state"], case["folded"])
        assert _refusal(lambda: _native.postgres_saga_write(*args, commands)) == case.get("error")
        if case.get("error") is None:
            assert _answer(_native.postgres_saga_write(*args, commands)) == case["want"]
    elif name == "enqueue_refuses_nul_in_metadata":
        commands = [_command(row) for row in case["command_rows"]]
        assert _refusal(lambda: _native.postgres_enqueue_params(commands, None)) == case["error"]
    elif name in ("saga_completed", "saga_conflict"):
        assert (
            _refusal(lambda: _native.postgres_saga_result("ship", "order-1", 3, case["result"]))
            == case["error"]
        )
    elif name in ("stored_document_exact", "stored_document_nul"):
        # A projection save binds the document as the kernel stores it, last.
        save = lambda: _native.postgres_projection_save("accounts", case["id"], 0, case["document"])  # noqa: E731
        assert _refusal(save) == case.get("error")
        if case.get("error") is None:
            assert json.loads(save().params_json)[-1] == case["stored"]
    elif name in (
        "append_refuses_a_gap",
        "append_refuses_another_aggregate",
        "append_refuses_nul",
        "append_refuses_nul_in_metadata",
    ):
        drafts = [_draft(row) for row in case["draft_rows"]]
        append_params = lambda: _native.postgres_append_params(case["stream"], case["expected"], drafts)  # noqa: E731
        assert _refusal(append_params) == case["error"]
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
        async def execute(self, query: str) -> str:
            raise asyncpg.InterfaceError("connection is closed")

        def terminate(self) -> None:
            raise asyncpg.InterfaceError("connection has been released back to the pool")

    original = StoreError("postgres load: connection is closed")
    with pytest.raises(StoreError) as caught:
        try:
            raise original
        except StoreError:
            await db._end(ReleasedConnection())  # ty: ignore[invalid-argument-type]
            raise
    assert caught.value is original


async def test_second_cancellation_during_rollback_terminates_connection() -> None:
    db = pg.PostgresTransactor(FakePool())
    running, rolling_back = asyncio.Event(), asyncio.Event()

    class Connection:
        terminated = False

        async def execute(self, query: str) -> str:
            rolling_back.set()
            await asyncio.Event().wait()
            return "ROLLBACK"

        def terminate(self) -> None:
            self.terminated = True

    conn = Connection()

    async def work() -> None:
        try:
            running.set()
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await db._end(conn)  # ty: ignore[invalid-argument-type]
            raise

    task = asyncio.create_task(work())
    await asyncio.wait_for(running.wait(), 1)
    task.cancel()
    await asyncio.wait_for(rolling_back.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert conn.terminated


# --- The adapter's I/O paths a live server does not reach on demand: a connection that answers each kernel
# --- statement by its tag, as a script says.

#: A scripted reply that never comes: the call waits until its deadline cancels it.
HANG = object()


class ScriptedConnection:
    """Answers each kernel statement by its tag (``/* edax:<tag> */``) with the next reply ``script`` lists
    for that tag, and records every tag sent (one per round trip), the tags it ran and how each transaction
    ended. The table check reads a server and a catalog that meet the contract: the kernel's
    ``compatible_default`` vector."""

    def __init__(self, script: dict[str, list[Any]]) -> None:
        compatible = next(c for c in golden("postgres_contract.json") if c["name"] == "compatible_default")
        # deadlock_timeout 1 ms: below every lock wait the tests set.
        self._fixed: dict[str, Any] = {
            "server": ("UTF8", 170004, 1),
            "catalog": compatible["catalog"],
        }
        self._script = {tag: list(replies) for tag, replies in script.items()}
        self.sent: list[str] = []
        self.ran: list[str] = []
        self.transactions: list[list[str]] = []

    async def _reply(self, query: str) -> Any:
        tag = query.split("*/", 1)[0].removeprefix("/* edax:").strip()
        self.sent.append(tag)
        if tag == "begin":
            self.transactions.append([tag])
            return "SET"
        if tag in ("commit", "rollback"):
            self.transactions[-1].append(tag)
            return tag.upper()
        if tag in self._fixed:
            return self._fixed[tag]
        self.ran.append(tag)
        replies = self._script.get(tag)
        assert replies, f"no scripted reply left for {tag}"
        reply = replies.pop(0)
        if reply is HANG:
            await asyncio.Event().wait()
        return reply

    async def fetch(self, query: str, *args: Any) -> Any:
        return await self._reply(query)

    async def fetchrow(self, query: str, *args: Any) -> Any:
        return await self._reply(query)

    async def fetchval(self, query: str, *args: Any) -> Any:
        return await self._reply(query)

    async def execute(self, query: str, *args: Any) -> Any:
        return await self._reply(query)

    def terminate(self) -> None:
        self.transactions[-1].append("terminated")


class ScriptedPool:
    """A pool that lends its one scripted connection."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self.conn = conn

    def acquire(self, *, timeout: float | None = None) -> Any:
        return contextlib.nullcontext(self.conn)


def _ledger_row(name: str) -> str:
    """The row ``claim_read`` answers for the kernel's ledger vector ``name``."""
    case = next(c for c in golden("postgres_ledger.json") if c["name"] == name)
    return json.dumps(case["row"])


@pytest.mark.parametrize(
    ("name", "script", "want", "error", "ran"),
    [
        (
            "vanished_then_free",
            {"claim_insert": [None, True], "claim_read": [None]},
            pg.Won,
            None,
            ["claim_insert", "claim_read", "claim_insert"],
        ),
        (
            "take_over_lost_then_prior",
            {
                "claim_insert": [None, None],
                "claim_read": ["lapsed_claim", "other_record"],
                "claim_take_over": [None],
            },
            pg.Record.done(7, 1),
            None,
            ["claim_insert", "claim_read", "claim_take_over", "claim_insert", "claim_read"],
        ),
        (
            "vanished_every_read",
            {"claim_insert": [None] * 3, "claim_read": [None] * 3},
            None,
            "kept changing under 3 reads",
            ["claim_insert", "claim_read"] * 3,
        ),
    ],
)
async def test_the_claim_loop(
    name: str, script: dict[str, list[Any]], want: Any, error: str | None, ran: list[str]
) -> None:
    """A claim that loses its insert reads the row again, at most the kernel's three times: a row that
    vanished is claimed afresh, a lapsed one is taken over (or, lost to another claimant, read again), and a
    key that keeps changing is the transient StoreError, rolled back."""
    reads = {"claim_read": [None if r is None else _ledger_row(r) for r in script.get("claim_read", [])]}
    conn = ScriptedConnection({**script, **reads})
    ledger = pg.PostgresIdempotencyStore(pg.PostgresTransactor(ScriptedPool(conn)))
    if error is not None:
        with pytest.raises(StoreError, match=error):
            await ledger.claim("order-7", 7)
        assert conn.transactions[-1] == ["begin", "rollback"]
    else:
        got = await ledger.claim("order-7", 7)
        assert isinstance(got, pg.Won) if want is pg.Won else got == want
        assert conn.transactions[-1] == ["begin", "commit"]
    assert conn.ran == ran, name


async def test_a_missed_deadline_inside_a_unit_of_work_aborts_it() -> None:
    """A call cancelled at its deadline may have reached the server, so the transaction is in an unknown
    state: the unit of work is rolled back and the block raises, even when the host caught the timeout."""
    conn = ScriptedConnection({"claim_insert": [HANG]})
    settings = pg.default_settings(lock_timeout_ms=10, statement_timeout_ms=20, op_timeout_ms=40)
    db = pg.PostgresTransactor(ScriptedPool(conn), settings=settings)
    ledger = pg.PostgresIdempotencyStore(db)
    with pytest.raises(StoreError, match="aborted the transaction"):
        async with transaction(db) as tx:
            with pytest.raises(StoreError, match="no reply within 40 ms"):
                await ledger.claim("order-7", 7, tx=tx)
    assert conn.transactions == [["begin", "commit"], ["begin", "rollback"]], (
        "the check, then the unit of work"
    )


async def test_a_call_alone_takes_three_round_trips() -> None:
    """A store's call outside any unit of work sends the kernel's ``begin`` (the transaction and its timeouts
    in one simple query), its statement and the commit: nothing else."""
    conn = ScriptedConnection({"claim_insert": [True]})
    ledger = pg.PostgresIdempotencyStore(pg.PostgresTransactor(ScriptedPool(conn)))
    await ledger.transactor.check()
    conn.sent.clear()
    assert isinstance(await ledger.claim("order-7", 7), pg.Won)
    assert conn.sent == ["begin", "claim_insert", "commit"]


async def test_a_transaction_that_fails_to_open_is_rolled_back() -> None:
    """A ``begin`` the server refuses part way may leave the transaction open: it is rolled back before the
    connection goes back to the pool, and the failure is typed by the kernel."""

    class RefusedBegin(ScriptedConnection):
        async def _reply(self, query: str) -> Any:
            reply = await super()._reply(query)
            if self.sent[-1] == "begin":
                raise ServerError("22023")
            return reply

    conn = RefusedBegin({})
    db = pg.PostgresTransactor(ScriptedPool(conn))
    with pytest.raises(EdaxError):
        async with transaction(db):
            pass
    assert conn.transactions == [["begin", "rollback"]]

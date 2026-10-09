"""Postgres-backed stores for every durable seam, over a caller-owned ``asyncpg`` pool: the event store with
its snapshots and outbox, the command outbox, read-model projections with the kernel's query pushed down to
SQL, the idempotency ledger and the saga store. The ``edax-postgres`` kernel owns every decision: the tables,
the SQL, how a value is laid out in a row, how a query becomes SQL, what a server error means and the ledger's
fence. This module adds the I/O: connections, the unit of work, deadlines. See ``docs/backends/postgres.md``.

Every store is built over one :class:`PostgresTransactor`, the one transaction boundary of its pool, so the
stores' writes under one ``tx`` commit or roll back together. Every transaction runs at ``READ COMMITTED``,
the level each compare-and-set here is written for, with the kernel's lock, statement and idle timeouts set
for that transaction only."""

from __future__ import annotations

import asyncio
import json
import logging
import weakref
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal, Protocol, TypeVar, get_args

from .. import _json, _names, _native, errors
from .._deadline import within
from .._wiring import wiring_int, wiring_str
from ..errors import BadHandle, EdaxError, StoreError
from ..idempotency import Claimed, Record, Won, decode_prior, encode_record
from ..message import CommandMessage, EventDraft, EventMessage
from ..projection import Entity
from ..query import Page, Query
from ..saga import CompletedInstance, RunningInstance, SagaInstance
from ..store import Snapshot
from ..unit_of_work import Tx, current_tx

__all__ = [
    "PgConnection",
    "PgPool",
    "PostgresCommandOutbox",
    "PostgresEventStore",
    "PostgresIdempotencyStore",
    "PostgresProjectionStore",
    "PostgresSagaStore",
    "PostgresSettings",
    "PostgresTransactor",
    "PostgresTx",
    "RelayLease",
    "TableRole",
    "reference_ddl",
]

logger = logging.getLogger("edax.backend.postgres")

T = TypeVar("T")

#: The settings a transactor is built with, in milliseconds: the kernel's record, so its defaults and its
#: checks are the kernel's. ``PostgresSettings(...)`` takes every field; start from ``default_settings()``.
PostgresSettings = _native.PostgresSettings

#: The operations a server error is read against: the kernel's, checked at import.
_Op = Literal[
    "check",
    "transaction",
    "append",
    "load",
    "save_snapshot",
    "outbox",
    "enqueue",
    "projection_read",
    "projection_save",
    "claim",
    "settle",
    "saga_load",
    "saga_write",
]
_names.check(get_args(_Op), _native.postgres_ops(), "postgres operations")

#: The failures a client reports without a SQLSTATE: the kernel's, checked at import.
_ClientFailure = Literal["timeout", "connection", "encode", "other"]
_names.check(get_args(_ClientFailure), _native.postgres_client_failures(), "postgres client failures")

#: The tables a host names, by role: the kernel's, checked at import. A role the host does not name takes its
#: default, ``edax_`` and the role.
TableRole = Literal[
    "events",
    "snapshots",
    "event_outbox",
    "command_outbox",
    "projections",
    "ledger",
    "saga_instances",
    "saga_outbox",
    "relay_leases",
]
_names.check(get_args(TableRole), _native.postgres_table_roles(), "postgres table roles")


def _tables_json(schema: str, tables: Mapping[str, str] | None) -> str:
    """The tables in ``schema``, named by ``tables`` where it names a role, as the kernel resolves and
    validates them: a name outside the identifier rule or an unknown role raises
    :class:`~edax.errors.BadHandle`."""
    wire = {"schema": wiring_str(schema, "schema"), "names": dict(tables or {})}
    return _native.postgres_tables(json.dumps(wire))


def reference_ddl(schema: str = "public", tables: Mapping[str, str] | None = None) -> str:
    """A schema that meets edax's table contract, for the tables named as a :class:`PostgresTransactor`
    names them: a starting point to copy into your own migration (Flyway, Alembic, sqitch). edax never runs
    it. Storage settings, foreign keys and query indexes are yours to add; ``docs/backends/postgres.md``
    suggests some."""
    return _native.postgres_reference_ddl(_tables_json(schema, tables))


# How many times a claim reads a key again when the row it lost to vanished first: the kernel's.
_CLAIM_READS: int = _native.postgres_claim_reads()


def default_settings(**overrides: int) -> PostgresSettings:
    """The kernel's default settings, with ``overrides`` by field name: a two-second lock wait inside a
    five-second statement inside a ten-second call, a minute idle in a transaction, a five-second wait for a
    pooled connection and a five-minute claim TTL. An unknown field raises :class:`~edax.errors.BadHandle`."""
    defaults = _native.postgres_default_settings()
    # Every setting is a duration in milliseconds, named `*_ms`: read the fields off the kernel's record.
    fields = {name: getattr(defaults, name) for name in dir(defaults) if name.endswith("_ms")}
    unknown = sorted(set(overrides) - set(fields))
    if unknown:
        raise BadHandle(f"postgres settings have no field {unknown[0]!r}")
    for name, value in overrides.items():
        fields[name] = wiring_int(value, name)
    return PostgresSettings(**fields)


class PgTransaction(Protocol):
    """The part of an ``asyncpg`` transaction a unit of work drives."""

    async def start(self) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...


class PgConnection(Protocol):
    """The part of an ``asyncpg.Connection`` the stores use. Every statement binds text parameters and
    ``None``, which the kernel's statements cast."""

    async def fetch(self, query: str, *args: Any) -> list[Any]: ...
    async def fetchrow(self, query: str, *args: Any) -> Any: ...
    async def fetchval(self, query: str, *args: Any) -> Any: ...
    async def execute(self, query: str, *args: Any) -> str: ...
    def transaction(self, *, isolation: str | None = None) -> PgTransaction: ...
    def terminate(self) -> None: ...


class PgPool(Protocol):
    """The part of an ``asyncpg.Pool`` the transactor uses: a connection, held for one block."""

    def acquire(self, *, timeout: float | None = None) -> AbstractAsyncContextManager[PgConnection]: ...


@dataclass(frozen=True)
class RelayLease:
    """Lets several relays share one outbox: only the relay holding the outbox's lease reads rows, and it
    renews when less than half the lease remains. ``owner`` names this relay, unique per process.
    Set ``ttl_ms`` to at least twice the slowest batch publish time and twice the relay's idle interval,
    with extra time for database and network delays. The half-life starts at the statement's clock
    reading. A relay paused past expiry can still publish duplicates: this is not a fencing token.
    Rows are never leased one by one, which could reorder one aggregate's events."""

    owner: str
    ttl_ms: int = 30_000

    def __post_init__(self) -> None:
        wiring_str(self.owner, "lease owner")
        if wiring_int(self.ttl_ms, "lease ttl_ms") <= 0:
            raise BadHandle(f"lease ttl_ms must be positive, got {self.ttl_ms}")


@dataclass(eq=False)
class PostgresTx:
    """The handle :meth:`PostgresTransactor.begin` yields: one connection inside one transaction. Used from
    the task that opened it, one call at a time. A server error inside it aborts the transaction, so the
    handle remembers it: a block that then exits cleanly is rolled back and raises
    :class:`~edax.errors.StoreError` rather than report a commit that never happened."""

    transactor: PostgresTransactor
    connection: PgConnection = field(repr=False)
    task: asyncio.Task[Any] | None = field(repr=False)
    aborted: bool = False
    closed: bool = False


# One transactor per pool: two would be two transaction identities over one pool, and a store of one would
# auto-commit on a second connection inside the other's unit of work, and could wait on its own row lock.
_TRANSACTORS: dict[int, weakref.ref[PostgresTransactor]] = {}


class PostgresTransactor:
    """:class:`~edax.unit_of_work.Transactor` over a caller-owned ``asyncpg`` pool:
    ``async with transaction(t) as tx`` opens a ``READ COMMITTED`` transaction on one pooled connection, and
    every store built over this transactor joins it. A store given no ``tx`` outside any unit of work runs its
    call in a short transaction of its own.

    edax provisions no table. Your migration tool creates them (:func:`reference_ddl` writes a starting
    point); ``schema`` and ``tables`` say where they are, ``tables`` naming a table per role where it differs
    from ``edax_<role>``. Before the first statement on a table, the transactor holds it to edax's table
    contract (:meth:`check`). ``settings`` are the kernel's timeouts; a set it refuses raises
    :class:`~edax.errors.BadHandle`. A second transactor over the same pool raises
    :class:`~edax.errors.BadHandle`: build every store over this one."""

    def __init__(
        self,
        pool: PgPool,
        *,
        schema: str = "public",
        tables: Mapping[str, str] | None = None,
        settings: PostgresSettings | None = None,
    ) -> None:
        settings = settings if settings is not None else default_settings()
        _native.postgres_check_config(settings)
        # The kernel refuses a name outside its rule, an unknown role or two roles under one name.
        self._tables = _tables_json(schema, tables)
        self._sql: dict[str, str] = json.loads(_native.postgres_statements(self._tables))
        existing = _TRANSACTORS.get(id(pool))
        if existing is not None and existing() is not None:
            raise BadHandle(
                "a PostgresTransactor already owns this pool: build every Postgres store over that one, so "
                "their writes share one unit of work"
            )
        _TRANSACTORS[id(pool)] = weakref.ref(self, lambda _: _TRANSACTORS.pop(id(pool), None))
        self._pool = pool
        self._schema = schema
        self._settings = settings
        self._op_timeout_s = settings.op_timeout_ms / 1000
        self._acquire_timeout_s = settings.acquire_timeout_ms / 1000
        # The roles the stores over this transactor use, the ones already held to the contract, and the
        # unique key each checked unique index guards, by the index's name: what a violation is read by.
        self._roles: set[str] = set()
        self._checked: set[str] = set()
        self._unique: dict[str, str] = {}
        self._checking: asyncio.Lock | None = None

    @property
    def schema(self) -> str:
        """The Postgres schema the tables live in."""
        return self._schema

    @property
    def tables(self) -> str:
        """The tables, every role named, as the kernel's JSON wire form."""
        return self._tables

    def statement(self, name: str) -> str:
        """The kernel's statement ``name``, rendered for this transactor's tables."""
        return self._sql[name]

    def use(self, roles: Iterable[str]) -> None:
        """Note that a store over this transactor uses the tables of ``roles``: the next call holds them to
        the contract first. Store-internal."""
        self._roles.update(roles)

    async def check(self) -> None:
        """Hold the server and every table the stores over this transactor use to edax's table contract,
        now: what a service runs at startup to fail fast. Every mismatch is listed in one
        :class:`~edax.errors.BadHandle`. Read-only; a store checks its tables before its first call anyway."""
        await self._ensure(force=True)

    async def _ensure(self, *, force: bool = False) -> None:
        """Check the roles not checked yet, once, before any statement on them runs."""
        if not force and self._roles <= self._checked:
            return
        if self._checking is None:
            self._checking = asyncio.Lock()
        async with self._checking:
            pending = sorted(self._roles - self._checked) if not force else sorted(self._roles)
            if not pending and not force:
                return

            async def checked(conn: PgConnection) -> str:
                row = await conn.fetchrow(self._sql["server"])
                _native.postgres_check_server(row[0], row[1], row[2], self._settings)
                param = _native.postgres_catalog_param(self._tables, pending)
                catalog: str = await conn.fetchval(self._sql["catalog"], param)
                return _native.postgres_check_catalog(self._tables, pending, catalog)

            compatibility = json.loads(await self._alone("check", checked, ensure=False))
            self._unique.update(compatibility["unique"])
            self._checked.update(pending)

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[PostgresTx]:
        await self._ensure()
        async with self._acquire() as conn:
            transaction = conn.transaction(isolation="read_committed")
            tx = PostgresTx(self, conn, asyncio.current_task())
            await self._call("transaction", tx, lambda: self._start(transaction, conn))
            try:
                yield tx
            except BaseException:
                tx.closed = True
                await self._end(conn, transaction.rollback)
                raise
            tx.closed = True
            if tx.aborted:
                await self._end(conn, transaction.rollback)
                raise StoreError(
                    "postgres unit of work: a statement inside it failed, which aborted the transaction; "
                    "it was rolled back and nothing was committed"
                )
            await self._guarded("transaction", transaction.commit())

    async def _start(self, transaction: PgTransaction, conn: PgConnection) -> None:
        await transaction.start()
        settings = self._settings
        await conn.fetchrow(
            self._sql["settings"],
            str(settings.lock_timeout_ms),
            str(settings.statement_timeout_ms),
            str(settings.idle_in_transaction_ms),
        )

    async def _end(self, conn: PgConnection, rollback: Callable[[], Awaitable[None]]) -> None:
        """Roll back; a connection that cannot (a cancel landed mid-statement) is closed instead, which the
        server reads as a rollback, and the pool replaces it."""
        try:
            await within(rollback(), self._op_timeout_s, "postgres rollback")
        except (Exception, asyncio.CancelledError) as error:
            logger.warning("edax postgres: rollback failed; closing the connection", exc_info=True)
            try:
                conn.terminate()
            except Exception:
                # A dropped connection may already have detached its pooled proxy.
                logger.warning("edax postgres: connection termination failed", exc_info=True)
            if isinstance(error, asyncio.CancelledError):
                raise

    def _acquire(self) -> AbstractAsyncContextManager[PgConnection]:
        return _Acquired(self._pool, self._acquire_timeout_s)

    def _handle(self, tx: Tx | None) -> PostgresTx | None:
        """The open handle a call under ``tx`` runs in: ``tx``, else the ambient unit of work of this
        transactor, else ``None`` for a call of its own. A handle of another transactor, a closed one, or one
        used from another task raises :class:`~edax.errors.BadHandle`: enlisting anyway would drop the
        atomicity the caller asked for, or share one connection between two tasks."""
        if tx is None:
            tx = current_tx(self)
        if tx is None:
            return None
        if not isinstance(tx, PostgresTx) or tx.transactor is not self:
            raise BadHandle(
                f"tx {tx!r} is not a transaction of this PostgresTransactor: build every Postgres store that "
                "must commit together over one transactor"
            )
        if tx.closed:
            raise BadHandle("this Postgres unit of work has ended: its handle takes no more calls")
        if tx.task is not asyncio.current_task():
            raise BadHandle(
                "a Postgres unit of work is used from the task that opened it, one call at a time"
            )
        return tx

    async def run(self, op: _Op, tx: Tx | None, call: Callable[[PgConnection], Awaitable[T]]) -> T:
        """Run ``call`` on the connection of ``tx`` (or the ambient unit of work), else in a short transaction
        of its own, so a call that reads then writes holds its row lock for both. Store-internal."""
        handle = self._handle(tx)
        if handle is not None:
            await self._ensure()
            return await self._call(op, handle, lambda: call(handle.connection))
        return await self._alone(op, call)

    async def _alone(
        self, op: _Op, call: Callable[[PgConnection], Awaitable[T]], *, ensure: bool = True
    ) -> T:
        """Run ``call`` in a transaction of its own on a pooled connection, outside any unit of work."""
        if ensure:
            await self._ensure()
        async with self._acquire() as conn:
            transaction = conn.transaction(isolation="read_committed")
            await self._guarded("transaction", self._start(transaction, conn))
            try:
                result = await self._guarded(op, call(conn))
            except BaseException:
                await self._end(conn, transaction.rollback)
                raise
            await self._guarded("transaction", transaction.commit())
            return result

    async def _call(self, op: _Op, tx: PostgresTx, call: Callable[[], Awaitable[T]]) -> T:
        """``call`` inside the unit of work ``tx``. A failure the server or the client reported, a missed
        deadline or a cancel aborts the transaction, so ``tx`` remembers it. A kernel decision raised between
        statements (a lapsed claim, a version conflict read from a row) leaves it usable."""
        try:
            return await self._guarded(op, call(), tx)
        except EdaxError as e:
            if isinstance(e.__cause__, asyncio.TimeoutError | TimeoutError):
                tx.aborted = True
            raise
        except BaseException:
            tx.aborted = True
            raise

    async def _guarded(self, op: _Op, call: Awaitable[T], tx: PostgresTx | None = None) -> T:
        """``call`` under the transactor's deadline, every failure the server or the client reports typed by
        the kernel: a server error by its SQLSTATE and constraint, a client one by its kind."""
        what = f"postgres {op}"

        async def sent() -> T:
            try:
                return await call
            except EdaxError:
                raise
            except Exception as e:
                if tx is not None:
                    tx.aborted = True
                raise _typed(op, e, self._unique) from e

        return await within(sent(), self._op_timeout_s, what)


class _Acquired:
    """A pooled connection for one block, waited for at most ``timeout_s``. A wait that runs out is the
    transient :class:`~edax.errors.StoreError`."""

    def __init__(self, pool: PgPool, timeout_s: float) -> None:
        self._held = pool.acquire(timeout=timeout_s)

    async def __aenter__(self) -> PgConnection:
        try:
            return await self._held.__aenter__()
        except EdaxError:
            raise
        except Exception as e:
            raise _typed("transaction", e) from e

    async def __aexit__(self, *exc: Any) -> bool | None:
        return await self._held.__aexit__(*exc)


def _typed(op: _Op, e: Exception, unique: Mapping[str, str] | None = None) -> EdaxError:
    """``e``, an error from the client or the server, as the kernel's typed error. A unique violation is
    read by the key its index guards, which ``unique`` maps the index's name to (the table check's answer)."""
    sqlstate = getattr(e, "sqlstate", None)
    if isinstance(sqlstate, str):
        constraint = getattr(e, "constraint_name", None)
        key = (unique or {}).get(constraint) if isinstance(constraint, str) else None
        return errors.from_wire(_native.postgres_verdict(op, sqlstate, key, str(e)))
    return errors.from_wire(
        _native.postgres_client_verdict(op, _client_failure(e), f"{type(e).__name__}: {e}")
    )


def _client_failure(e: Exception) -> _ClientFailure:
    """The kind of a failure the client reports without a SQLSTATE."""
    if isinstance(e, asyncio.TimeoutError | TimeoutError):
        return "timeout"
    if isinstance(e, OSError | ConnectionError) or "Connection" in type(e).__name__:
        return "connection"
    # asyncpg's client-side DataError (a parameter it cannot encode) is a ValueError.
    if isinstance(e, ValueError | UnicodeError):
        return "encode"
    return "other"


def _params(params_json: str) -> list[str | None]:
    params: list[str | None] = json.loads(params_json)
    return params


class _StoreView:
    """What every store shares: the transactor it enlists with, and the tables it uses (``_roles``), which
    the transactor holds to the contract before the store's first call."""

    _roles: ClassVar[tuple[TableRole, ...]] = ()

    def __init__(self, transactor: PostgresTransactor) -> None:
        if not isinstance(transactor, PostgresTransactor):
            raise TypeError(
                f"a Postgres store is built over a PostgresTransactor, not {type(transactor).__name__}: "
                "build one PostgresTransactor per pool and every store over it"
            )
        self._transactor = transactor
        transactor.use(self._roles)

    @property
    def transactor(self) -> PostgresTransactor:
        """The transactor this store enlists with: ``transaction(store)`` opens its unit of work."""
        return self._transactor

    def _sql(self, name: str) -> str:
        return self._transactor.statement(name)


class _OutboxReads(_StoreView):
    """An outbox read side: run on a connection of its own, never inside a unit of work, so it sees only
    committed rows; with a :class:`RelayLease`, only while this relay holds the outbox's lease."""

    _outbox: ClassVar[str]

    def __init__(self, transactor: PostgresTransactor, *, lease: RelayLease | None = None) -> None:
        super().__init__(transactor)
        self._lease = lease
        if lease is not None:
            transactor.use(("relay_leases",))

    async def _unpublished(self, statement: str, scope: list[str], lease_name: str, limit: int) -> list[str]:
        limit_text = _native.postgres_bound_param(wiring_int(limit, "limit"))
        args: list[str] = [*scope, limit_text]
        if self._lease is not None:
            statement += "_leased"
            args += [lease_name, self._lease.owner, str(self._lease.ttl_ms)]

        async def read(conn: PgConnection) -> list[str]:
            return [row[0] for row in await conn.fetch(self._sql(statement), *args)]

        return await self._transactor._alone("outbox", read)

    async def _mark(self, statement: str, scope: list[str], ids: Sequence[str]) -> None:
        if not ids:
            return
        ids_param = _native.postgres_ids_param(list(ids))

        async def mark(conn: PgConnection) -> None:
            await conn.execute(self._sql(statement), *scope, ids_param)

        await self._transactor._alone("outbox", mark)

    async def _count(self, statement: str, scope: list[str]) -> int:
        async def count(conn: PgConnection) -> int:
            n: int = await conn.fetchval(self._sql(statement), *scope)
            return n

        return await self._transactor._alone("outbox", count)


class PostgresEventStore(_OutboxReads):
    """:class:`~edax.store.EventStore` on Postgres, with its outbox: :class:`~edax.outbox.Outbox` and
    :class:`~edax.outbox.CountedOutbox` of :class:`~edax.message.EventMessage`. ``append`` checks the stream's
    version in the statement, and the ``(stream, sequence)`` constraint catches the writer that raced past it:
    :class:`~edax.errors.ConcurrencyError`, which aborts the unit of work, so a caller that catches it cannot
    commit half a batch. Every event is stamped with the database's clock, and its outbox row is written in
    the same statement, in sequence order. ``begin()`` is the transactor's."""

    _roles: ClassVar[tuple[TableRole, ...]] = ("events", "snapshots", "event_outbox")

    def begin(self) -> AbstractAsyncContextManager[PostgresTx]:
        return self._transactor.begin()

    async def load(self, tx: Tx, aggregate_id: str, after: int = 0) -> list[EventMessage]:
        after_text = _native.postgres_bound_param(after)

        async def load(conn: PgConnection) -> list[str]:
            return [row[0] for row in await conn.fetch(self._sql("load"), aggregate_id, after_text)]

        rows = await self._transactor.run("load", tx, load)
        return _native.postgres_decode_events(rows)

    async def load_snapshot(self, tx: Tx, aggregate_id: str) -> Snapshot | None:
        async def load(conn: PgConnection) -> str | None:
            row: str | None = await conn.fetchval(self._sql("load_snapshot"), aggregate_id)
            return row

        row = await self._transactor.run("load", tx, load)
        if row is None:
            return None
        snapshot = _native.postgres_decode_snapshot(row)
        return Snapshot(aggregate_id, snapshot.type_name, snapshot.version, snapshot.data)

    async def save_snapshot(self, tx: Tx, snapshot: Snapshot) -> None:
        params = _params(
            _native.postgres_snapshot_params(
                snapshot.aggregate_id, snapshot.type_name, snapshot.version, snapshot.data
            )
        )

        async def save(conn: PgConnection) -> int:
            stream_version: int = await conn.fetchval(self._sql("save_snapshot"), *params)
            return stream_version

        stream_version = await self._transactor.run("save_snapshot", tx, save)
        _native.postgres_snapshot_result(snapshot.aggregate_id, snapshot.version, stream_version)

    async def append(
        self, tx: Tx, aggregate_id: str, expected_version: int, drafts: Sequence[EventDraft]
    ) -> list[EventMessage]:
        # The dense-batch rule, the bigint bound and the NUL refusal are the kernel's, before any I/O.
        params = _params(_native.postgres_append_params(aggregate_id, expected_version, list(drafts)))

        async def append(conn: PgConnection) -> str:
            result: str = await conn.fetchval(self._sql("append"), *params)
            return result

        result = await self._transactor.run("append", tx, append)
        return _native.postgres_append_result(expected_version, len(drafts), result)

    async def unpublished(self, limit: int) -> list[EventMessage]:
        rows = await self._unpublished("event_unpublished", [], "events", limit)
        return _native.postgres_decode_events(rows)

    async def mark_published(self, ids: Sequence[str]) -> None:
        await self._mark("event_mark_published", [], ids)

    async def unpublished_count(self) -> int:
        """How many committed unmarked outbox rows wait, counted without paging them."""
        return await self._count("event_unpublished_count", [])


class PostgresCommandOutbox(_OutboxReads):
    """A command outbox on Postgres: the :class:`~edax.outbox.CommandOutbox` a
    :class:`~edax.repository.Repository` writes under the save's ``tx``, and the :class:`~edax.outbox.Outbox`
    a :class:`~edax.outbox.Relay` drains. A ``tx`` of another transactor raises
    :class:`~edax.errors.BadHandle`; a command id already waiting raises :class:`~edax.errors.CodecError`."""

    _roles: ClassVar[tuple[TableRole, ...]] = ("command_outbox",)

    async def enqueue(self, tx: Tx, commands: Sequence[CommandMessage]) -> None:
        if not commands:
            self._transactor._handle(tx)  # a foreign handle is refused even for an empty batch
            return
        param = _native.postgres_commands_param(list(commands))

        async def enqueue(conn: PgConnection) -> None:
            await conn.execute(self._sql("command_enqueue"), param)

        await self._transactor.run("enqueue", tx, enqueue)

    async def unpublished(self, limit: int) -> list[CommandMessage]:
        rows = await self._unpublished("command_unpublished", [], "commands", limit)
        return _native.postgres_decode_commands(rows)

    async def mark_published(self, ids: Sequence[str]) -> None:
        await self._mark("command_mark_published", [], ids)

    async def unpublished_count(self) -> int:
        """How many committed unsent commands wait, counted without paging them."""
        return await self._count("command_unpublished_count", [])


class PostgresProjectionStore(_StoreView):
    """:class:`~edax.projection.ProjectionQuery` on Postgres for the read model ``projection``: documents in
    ``jsonb``, a compare-and-set on ``version``, and ``get_many`` compiled by the kernel to one SQL statement
    that pages exactly as the kernel's reference applier does. A document is stored as the kernel writes it,
    so numbers compare at the value the kernel compares. With ``lock_reads``, a ``get`` inside a unit of work
    holds the row until it ends, so contending writers wait for each other instead of failing their
    compare-and-set."""

    _roles: ClassVar[tuple[TableRole, ...]] = ("projections",)

    def __init__(self, transactor: PostgresTransactor, projection: str, *, lock_reads: bool = False) -> None:
        super().__init__(transactor)
        self._projection = wiring_str(projection, "projection")
        self._lock_reads = lock_reads

    @property
    def projection(self) -> str:
        """The read model this store holds."""
        return self._projection

    async def get(self, id: str, tx: Tx | None = None) -> Entity | None:
        locked = self._lock_reads and self._transactor._handle(tx) is not None
        statement = self._sql("projection_get_locked" if locked else "projection_get")

        async def get(conn: PgConnection) -> Any:
            return await conn.fetchrow(statement, self._projection, id)

        row = await self._transactor.run("projection_read", tx, get)
        return None if row is None else Entity(id, row[0], _json.decode(row[1]))

    async def save(self, entity: Entity, tx: Tx | None = None) -> int:
        document = _native.postgres_stored_document(entity.id, _json.encode(entity.document))
        version = _native.postgres_bound_param(wiring_int(entity.version, "version"))

        async def save(conn: PgConnection) -> int | None:
            written: int | None
            if entity.version == 0:
                written = await conn.fetchval(
                    self._sql("projection_insert"), self._projection, entity.id, document
                )
            else:
                written = await conn.fetchval(
                    self._sql("projection_update"), self._projection, entity.id, version, document
                )
            return written

        written = await self._transactor.run("projection_save", tx, save)
        return _native.postgres_projection_result(entity.id, entity.version, written)

    async def get_many(self, query: Query = Query(), *, tx: Tx | None = None) -> Page:
        spec = query.to_json()
        # A spec or a cursor the kernel refuses is its terminal CodecError, before any I/O.
        compiled = _native.postgres_compile_query(self._transactor.tables, self._projection, spec)
        params = _params(compiled.params_json)

        async def read(conn: PgConnection) -> list[str]:
            return [row[0] for row in await conn.fetch(compiled.sql, *params)]

        rows = await self._transactor.run("projection_read", tx, read)
        return Page.from_kernel(_native.postgres_page(spec, rows))


class PostgresIdempotencyStore(_StoreView):
    """:class:`~edax.idempotency.IdempotencyStore` on Postgres, in the database of the work it guards:
    ``joins_transaction``, so the guard claims, runs and records in one commit, effectively-once. A claim is
    an ``INSERT … ON CONFLICT DO NOTHING``: a claimant of a key another open unit of work holds waits for it
    on the row, then reads its record, or reports :class:`~edax.errors.InProgress` when the lock wait runs
    out. ``complete``, ``release`` and ``forget`` lock the row and write only what the kernel's fence answers.

    A claim made outside any unit of work commits pending, and lapses after the settings' ``claim_ttl_ms``,
    so a crashed claimant does not hold its key forever; that mode is at-least-once."""

    _roles: ClassVar[tuple[TableRole, ...]] = ("ledger",)

    joins_transaction: ClassVar[bool] = True

    async def claim(self, key: str, payload_hash: int, tx: Tx | None = None) -> Claimed:
        owner = _native.postgres_new_owner()
        params = _params(_native.postgres_claim_params(key, owner, payload_hash))
        pending = params[2]
        ttl = str(self._transactor._settings.claim_ttl_ms)

        async def claim(conn: PgConnection) -> Claimed:
            for _ in range(_CLAIM_READS):
                if await conn.fetchval(self._sql("claim_insert"), *params):
                    return Won(owner.encode())
                read = _native.postgres_claim_read(await conn.fetchval(self._sql("claim_read"), key, ttl))
                if read.action == "prior":
                    return decode_prior(_names.given(read.value, "prior record"), payload_hash)
                if read.action == "take_over":
                    lapsed = _names.given(read.value, "lapsed owner")
                    if await conn.fetchval(self._sql("claim_take_over"), key, owner, pending, lapsed):
                        return Won(owner.encode())
            raise StoreError(f"postgres claim of {key!r}: the key kept changing under {_CLAIM_READS} reads")

        return await self._transactor.run("claim", tx, claim)

    async def _hold(self, conn: PgConnection, key: str) -> str | None:
        row: str | None = await conn.fetchval(self._sql("ledger_hold"), key)
        return row

    async def complete(self, key: str, handle: bytes, record: Record, tx: Tx | None = None) -> None:
        body = encode_record(record)

        async def complete(conn: PgConnection) -> None:
            # A pending record is the kernel's BadHandle and a lapsed claim its StoreError: nothing written.
            if _native.postgres_complete_write(key, await self._hold(conn, key), handle, body):
                await conn.execute(self._sql("ledger_settle"), key, handle.decode(), body)

        await self._transactor.run("settle", tx, complete)

    async def release(self, key: str, handle: bytes, tx: Tx | None = None) -> None:
        async def release(conn: PgConnection) -> None:
            if _native.postgres_release_write(await self._hold(conn, key), handle):
                await conn.execute(self._sql("ledger_delete"), key)

        await self._transactor.run("settle", tx, release)

    async def forget(self, key: str, tx: Tx | None = None) -> None:
        async def forget(conn: PgConnection) -> None:
            # A pending claim is the kernel's InProgress, and stays.
            if _native.postgres_forget_write(key, await self._hold(conn, key)):
                await conn.execute(self._sql("ledger_delete"), key)

        await self._transactor.run("settle", tx, forget)


class PostgresSagaStore(_OutboxReads):
    """:class:`~edax.saga.SagaStore` on Postgres: instances keyed by ``(saga, id)`` under a compare-and-set
    on their version, a completed instance kept as a tombstone, and each step's commands written in the same
    statement as its state. Inside a unit of work ``load`` holds the instance's row until it ends, so two
    steps of one instance wait for each other instead of failing their write. State crosses as JSON."""

    _roles: ClassVar[tuple[TableRole, ...]] = ("saga_instances", "saga_outbox")

    def __init__(self, transactor: PostgresTransactor, *, lease: RelayLease | None = None) -> None:
        super().__init__(transactor, lease=lease)

    async def load(self, saga: str, id: str, tx: Tx | None = None) -> SagaInstance | None:
        locked = self._transactor._handle(tx) is not None
        statement = self._sql("saga_load_locked" if locked else "saga_load")

        async def load(conn: PgConnection) -> str | None:
            row: str | None = await conn.fetchval(statement, saga, id)
            return row

        row = await self._transactor.run("saga_load", tx, load)
        if row is None:
            return None
        instance = _native.postgres_decode_saga(row)
        if instance.completed:
            return CompletedInstance(instance.version)
        state = _json.decode(_names.given(instance.state_json, "saga state"))
        return RunningInstance(state, instance.version, instance.folded)

    async def _write(
        self,
        saga: str,
        id: str,
        version: int,
        state: str | None,
        folded: str,
        commands: Sequence[CommandMessage],
        tx: Tx | None,
    ) -> int:
        params = _params(_native.postgres_saga_params(saga, id, version, state, folded, list(commands)))
        statement = self._sql("saga_insert" if version == 0 else "saga_update")

        async def write(conn: PgConnection) -> str:
            result: str = await conn.fetchval(statement, *params)
            return result

        result = await self._transactor.run("saga_write", tx, write)
        return _native.postgres_saga_result(saga, id, version, result)

    async def save(
        self,
        saga: str,
        id: str,
        state: Any,
        version: int,
        folded: str,
        commands: Sequence[CommandMessage],
        tx: Tx | None = None,
    ) -> int:
        encoded = _json.encode(state)
        return await self._write(saga, id, version, encoded, folded, commands, tx)

    async def complete(
        self, saga: str, id: str, version: int, commands: Sequence[CommandMessage], tx: Tx | None = None
    ) -> int:
        return await self._write(saga, id, version, None, "", commands, tx)

    async def unpublished(self, saga: str, limit: int) -> list[CommandMessage]:
        rows = await self._unpublished("saga_unpublished", [saga], f"saga:{saga}", limit)
        return _native.postgres_decode_commands(rows)

    async def mark_published(self, saga: str, ids: Sequence[str]) -> None:
        await self._mark("saga_mark_published", [saga], ids)

    async def unpublished_count(self, saga: str) -> int:
        """How many of ``saga``'s committed unsent commands wait, counted without paging them."""
        return await self._count("saga_unpublished_count", [saga])

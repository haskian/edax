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
from .._units import ambient_tx, enlisted, own_transaction
from .._wiring import wiring_int, wiring_str
from ..errors import BadHandle, CodecError, EdaxError, StoreError, TransactionFailure
from ..idempotency import Claimed, Record, Won, decode_prior, encode_record
from ..message import CommandMessage, EventDraft, EventMessage
from ..projection import Entity
from ..query import Page, Query
from ..saga import CompletedInstance, ProtectedSagaState, RunningInstance, SagaInstance
from ..store import Snapshot
from ..unit_of_work import Tx

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
    "PostgresTransactionFailure",
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
    "projection_state",
    "ingest",
    "erasure_fence",
]
_names.check(get_args(_Op), _native.postgres_ops(), "postgres operations")
#: The operations that only read: a short transaction of their own takes no pending write fence.
_READS: frozenset[_Op] = frozenset({"check", "load", "projection_read", "saga_load"})

#: The failures a client reports without a SQLSTATE: the kernel's, checked at import.
_ClientFailure = Literal["encode", "other"]
_names.check(get_args(_ClientFailure), _native.postgres_client_failures(), "postgres client failures")

#: What a claim that lost its insert does with the row it then read: the kernel's, checked at import.
_ClaimAction = Literal["prior", "take_over", "vanished"]
_CLAIM_ACTIONS: tuple[_ClaimAction, ...] = get_args(_ClaimAction)
_names.check(_CLAIM_ACTIONS, _native.postgres_claim_actions(), "postgres claim actions")

#: The outboxes a relay drains: the kernel's, checked at import.
_Outbox = Literal["event", "command", "saga"]
_names.check(get_args(_Outbox), _native.postgres_outboxes(), "postgres outboxes")

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


def default_settings(**overrides: int) -> PostgresSettings:
    """The kernel's default settings, with ``overrides`` by field name: a two-second lock wait inside a
    five-second statement inside a ten-second call, a minute idle in a transaction, a five-second wait for a
    pooled connection and a five-minute claim TTL. An unknown field raises :class:`~edax.errors.BadHandle`."""
    checked = {name: wiring_int(value, name) for name, value in overrides.items()}
    return _native.postgres_settings(json.dumps(checked))


class PgConnection(Protocol):
    """The part of an ``asyncpg.Connection`` the stores use. Every statement binds text parameters and
    ``None``, which the kernel's statements cast. ``execute`` with no parameters sends a simple query, which
    may hold several statements: how a transaction opens in one round trip."""

    async def fetch(self, query: str, *args: Any) -> list[Any]: ...
    async def fetchrow(self, query: str, *args: Any) -> Any: ...
    async def fetchval(self, query: str, *args: Any) -> Any: ...
    async def execute(self, query: str, *args: Any) -> str: ...
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
        _native.postgres_check_lease(
            wiring_str(self.owner, "lease owner"), wiring_int(self.ttl_ms, "lease ttl_ms")
        )


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


class PostgresTransactionFailure(TransactionFailure):
    """PostgreSQL transaction failure with the shared cause and recovery evidence contract."""


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
        # What opens each transaction, its timeouts rendered in; the kernel refuses settings out of order.
        self._begin = _native.postgres_begin(settings)
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
        async with self._lock():
            await self._check(sorted(self._roles))

    async def _ensure(self) -> None:
        """Check the roles not checked yet, once, before any statement on them runs."""
        if self._roles <= self._checked:
            return
        async with self._lock():
            pending = sorted(self._roles - self._checked)
            if pending:
                await self._check(pending)

    def _lock(self) -> asyncio.Lock:
        """The lock one table check at a time holds, made on first use inside the running loop."""
        if self._checking is None:
            self._checking = asyncio.Lock()
        return self._checking

    async def _check(self, roles: list[str]) -> None:
        """Hold the server and the tables of ``roles`` to the contract, in a transaction of its own."""

        async def checked(conn: PgConnection) -> str:
            await self.check_server(conn)
            param = _native.postgres_catalog_param(self._tables, roles)
            catalog: str = await conn.fetchval(self._sql["catalog"], param)
            return _native.postgres_check_catalog(self._tables, roles, catalog)

        async with self._transact() as tx:
            compatibility = json.loads(await self._call("check", tx, lambda: checked(tx.connection)))
        self._unique.update(compatibility["unique"])
        self._checked.update(roles)

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[PostgresTx]:
        await self._ensure()
        async with self._transact() as tx:
            yield tx

    @asynccontextmanager
    async def _transact(self) -> AsyncIterator[PostgresTx]:
        """One transaction on a pooled connection, with the kernel's timeouts set for it alone: committed on a
        clean exit, rolled back when the block raises, a statement inside it failed or it did not open. Holds
        no table to the contract.

        The kernel's ``begin`` opens it and sets its timeouts in one simple query, so a call alone costs three
        round trips. asyncpg reads the open transaction from the server's reply, as it does one it opened
        itself."""
        async with self._acquire() as conn:
            tx = PostgresTx(self, conn, asyncio.current_task())
            try:
                await self._call("transaction", tx, lambda: conn.execute(self._begin))
                yield tx
            except BaseException:
                tx.closed = True
                await self._end(conn)
                raise
            tx.closed = True
            if tx.aborted:
                await self._end(conn)
                raise StoreError(
                    "postgres unit of work: a statement inside it failed, which aborted the transaction; "
                    "it was rolled back and nothing was committed"
                )
            await self._call("transaction", tx, lambda: conn.execute(self._sql["commit"]))

    async def _end(self, conn: PgConnection) -> bool:
        """Roll back; a connection that cannot (a cancel landed mid-statement) is closed instead, which the
        server reads as a rollback, and the pool replaces it. Only a confirmed reply returns true."""
        try:
            await within(conn.execute(self._sql["rollback"]), self._op_timeout_s, "postgres rollback")
        except (Exception, asyncio.CancelledError) as error:
            logger.warning("edax postgres: rollback failed; closing the connection", exc_info=True)
            try:
                conn.terminate()
            except Exception:
                # A dropped connection may already have detached its pooled proxy.
                logger.warning("edax postgres: connection termination failed", exc_info=True)
            if isinstance(error, asyncio.CancelledError):
                raise
            return False
        return True

    def _acquire(self) -> AbstractAsyncContextManager[PgConnection]:
        return _Acquired(self._pool, self._acquire_timeout_s)

    def _handle(self, tx: Tx | None) -> PostgresTx | None:
        """The open handle a call under ``tx`` runs in: ``tx``, else the ambient unit of work of this
        transactor, else ``None`` for a call of its own. A handle of another transactor, a closed one, or one
        used from another task raises :class:`~edax.errors.BadHandle`: enlisting anyway would drop the
        atomicity the caller asked for, or share one connection between two tasks."""
        handle = enlisted(self, tx, PostgresTx, "PostgresTransactor")
        if handle is None:
            return None
        if handle.closed:
            raise BadHandle("this Postgres unit of work has ended: its handle takes no more calls")
        if handle.task is not asyncio.current_task():
            raise BadHandle(
                "a Postgres unit of work is used from the task that opened it, one call at a time"
            )
        return handle

    def require_transaction(self, tx: Tx | None = None) -> PostgresTx:
        """Validate an explicit/ambient handle without opening a transaction. Store-internal."""
        handle = self._handle(tx)
        if handle is None:
            raise BadHandle("this operation requires an open Postgres transaction")
        if handle.aborted:
            raise BadHandle("this Postgres transaction has aborted and must roll back")
        ambient = ambient_tx(self)
        if ambient is not None and handle is not ambient:
            raise BadHandle(
                "this explicit Postgres transaction differs from the ambient transaction: "
                "scope fences must remain attached to the transaction that writes"
            )
        return handle

    async def run(
        self, op: _Op, tx: Tx | None, call: Callable[[PgConnection], Awaitable[T]], *, fenced: bool = True
    ) -> T:
        """Run ``call`` on the connection of ``tx`` (or the ambient unit of work), else in a short transaction
        of its own, so a call that reads then writes holds its row lock for both. ``fenced=False``: the call
        writes no payload (it removes rows), so no erasure must wait for it. Store-internal."""
        handle = self._handle(tx)
        if handle is None:
            return await self._alone(op, call, fenced=fenced)
        await self._ensure()
        return await self._call(op, handle, lambda: call(handle.connection))

    async def call(self, call: Awaitable[T]) -> T:
        """Await one adapter statement under the deadline, preserving its driver error. Store-internal."""
        return await within(call, self._op_timeout_s, "postgres statement")

    async def check_server(self, conn: PgConnection) -> None:
        """Check a server on an already-owned connection. Store-internal."""
        row = await self.call(conn.fetchrow(self._sql["server"]))
        _native.postgres_check_server(row[0], row[1], row[2], self._settings)

    async def run_owned(self, call: Callable[[PgConnection], Awaitable[T]], *, op: _Op | None = None) -> T:
        """Run an adapter call in its own transaction, refusing an ambient unit of work.

        The adapter uses :meth:`call` for each statement. Failures retain their rollback/commit evidence so
        its kernel can choose recovery; cancellation stays cancellation. With ``op``, a failure's cause is
        the kernel's typed verdict for that operation, the raw error its ``__cause__``; without, the raw
        error. Store-internal.
        """
        if self._handle(None) is not None:
            raise BadHandle(
                "postgres operation requires its own transaction, outside an ambient unit of work"
            )
        rolled_back = False
        commit_started = False

        @asynccontextmanager
        async def opened() -> AsyncIterator[PostgresTx]:
            nonlocal rolled_back, commit_started
            async with self._acquire() as conn:
                tx = PostgresTx(self, conn, asyncio.current_task())
                try:
                    await self.call(conn.execute(self._begin))
                    yield tx
                    commit_started = True
                    await self.call(conn.execute(self._sql["commit"]))
                except BaseException:
                    rolled_back = await self._end(conn)
                    raise
                finally:
                    tx.closed = True

        try:
            async with own_transaction(self, opened=opened()) as tx:
                return await call(tx.connection)
        except BadHandle:
            raise
        except Exception as error:
            cause: Exception = error
            if op is not None and not isinstance(error, EdaxError):
                cause = postgres_error(op, error)
                cause.__cause__ = error
            raise PostgresTransactionFailure(
                cause, rolled_back=rolled_back, commit_started=commit_started
            ) from error

    async def _alone(
        self, op: _Op, call: Callable[[PgConnection], Awaitable[T]], *, fenced: bool = True
    ) -> T:
        """Run ``call`` in a transaction of its own on a pooled connection, outside any unit of work. A write
        takes this task's pending write fences inside it, as a unit of work does."""
        opened = own_transaction(self) if fenced and op not in _READS else self.begin()
        async with opened as tx:
            return await self._call(op, tx, lambda: call(tx.connection))

    async def _call(self, op: _Op, tx: PostgresTx, call: Callable[[], Awaitable[T]]) -> T:
        """``call`` inside the transaction ``tx``, under the transactor's deadline, every failure the server
        or the client reports typed by the kernel: a server error by its SQLSTATE and constraint, a client one
        by its kind. Such a failure, a missed deadline or a cancel aborts the transaction, so ``tx`` remembers
        it. A kernel decision raised between statements (a lapsed claim, a version conflict read from a row)
        leaves it usable."""
        try:
            return await within(call(), self._op_timeout_s, f"postgres {op}")
        except EdaxError as e:
            # The deadline's own error: the cancelled call may have reached the server.
            if isinstance(e.__cause__, asyncio.TimeoutError | TimeoutError):
                tx.aborted = True
            raise
        except Exception as e:
            tx.aborted = True
            raise postgres_error(op, e, self._unique) from e
        except BaseException:
            tx.aborted = True
            raise


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
            raise postgres_error("transaction", e) from e

    async def __aexit__(self, *exc: Any) -> bool | None:
        return await self._held.__aexit__(*exc)


def postgres_error(op: _Op, e: Exception, unique: Mapping[str, str] | None = None) -> EdaxError:
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
    """The kind of a failure the client reports without a SQLSTATE. asyncpg's client-side ``DataError``, a
    parameter it cannot encode, is a ``ValueError``."""
    return "encode" if isinstance(e, ValueError) else "other"


def postgres_params(params_json: str) -> list[str | None]:
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

    async def _fetch(self, op: _Op, tx: Tx | None, name: str, *params: str | None) -> list[Any]:
        """The rows statement ``name`` returns for ``params``, run under ``tx`` as the transactor runs a
        call."""
        sql = self._sql(name)
        return await self._transactor.run(op, tx, lambda conn: conn.fetch(sql, *params))

    async def _fetchval(self, op: _Op, tx: Tx | None, name: str, *params: str | None) -> Any:
        """The first column of the first row statement ``name`` returns for ``params``, as :meth:`_fetch`
        runs it."""
        sql = self._sql(name)
        return await self._transactor.run(op, tx, lambda conn: conn.fetchval(sql, *params))


class _OutboxReads(_StoreView):
    """An outbox read side: run on a connection of its own, never inside a unit of work, so it sees only
    committed rows; with a :class:`RelayLease`, only while this relay holds the outbox's lease."""

    _outbox: ClassVar[_Outbox]

    def __init__(self, transactor: PostgresTransactor, *, lease: RelayLease | None = None) -> None:
        super().__init__(transactor)
        self._lease = lease
        if lease is not None:
            transactor.use(("relay_leases",))

    async def _rows(self, call: _native.PostgresCall) -> list[Any]:
        """The rows the kernel's ``call`` returns, run in a transaction of its own. The relay's reads and
        publication marks hold no payload, so they take no write fence."""
        sql, params = self._sql(call.statement), postgres_params(call.params_json)
        return await self._transactor._alone("outbox", lambda conn: conn.fetch(sql, *params), fenced=False)

    async def _unpublished(self, saga: str | None, limit: int) -> list[str]:
        lease = self._lease
        owner, ttl_ms = (None, None) if lease is None else (lease.owner, lease.ttl_ms)
        call = _native.postgres_unpublished(self._outbox, wiring_int(limit, "limit"), saga, owner, ttl_ms)
        return [row[0] for row in await self._rows(call)]

    async def _mark(self, saga: str | None, ids: Sequence[str]) -> None:
        if ids:
            await self._rows(_native.postgres_mark_published(self._outbox, list(ids), saga))

    async def _count(self, saga: str | None) -> int:
        rows = await self._rows(_native.postgres_unpublished_count(self._outbox, saga))
        count: int = rows[0][0]
        return count


class PostgresEventStore(_OutboxReads):
    """:class:`~edax.store.EventStore` on Postgres, with its outbox: :class:`~edax.outbox.Outbox` and
    :class:`~edax.outbox.CountedOutbox` of :class:`~edax.message.EventMessage`. ``append`` checks the stream's
    version in the statement, and the ``(stream, sequence)`` constraint catches the writer that raced past it:
    :class:`~edax.errors.ConcurrencyError`, which aborts the unit of work, so a caller that catches it cannot
    commit half a batch. Every event is stamped with the database's clock, and its outbox row is written in
    the same statement, in sequence order. ``begin()`` is the transactor's."""

    _roles: ClassVar[tuple[TableRole, ...]] = ("events", "snapshots", "event_outbox")
    _outbox: ClassVar[_Outbox] = "event"

    def begin(self) -> AbstractAsyncContextManager[PostgresTx]:
        return self._transactor.begin()

    async def load(self, tx: Tx, aggregate_id: str, after: int = 0) -> list[EventMessage]:
        params = postgres_params(_native.postgres_load_params(aggregate_id, after))
        rows = await self._fetch("load", tx, "load", *params)
        return _native.postgres_decode_events([row[0] for row in rows])

    async def load_snapshot(self, tx: Tx, aggregate_id: str) -> Snapshot | None:
        params = postgres_params(_native.postgres_load_snapshot_params(aggregate_id))
        row: str | None = await self._fetchval("load", tx, "load_snapshot", *params)
        if row is None:
            return None
        snapshot = _native.postgres_decode_snapshot(row)
        return Snapshot(
            aggregate_id,
            snapshot.type_name,
            snapshot.version,
            snapshot.data,
            snapshot.tenant,
            snapshot.subject,
            snapshot.encryption,
        )

    async def save_snapshot(self, tx: Tx, snapshot: Snapshot) -> None:
        params = postgres_params(
            _native.postgres_protected_snapshot_params(
                snapshot.aggregate_id,
                _native.SnapshotResult(
                    type_name=snapshot.type_name,
                    version=snapshot.version,
                    data=snapshot.data,
                    tenant=snapshot.tenant,
                    subject=snapshot.subject,
                    encryption=snapshot.encryption,
                ),
            )
        )
        stream_version: int = await self._fetchval("save_snapshot", tx, "save_snapshot", *params)
        _native.postgres_snapshot_result(snapshot.aggregate_id, snapshot.version, stream_version)

    async def append(
        self, tx: Tx, aggregate_id: str, expected_version: int, drafts: Sequence[EventDraft]
    ) -> list[EventMessage]:
        # The dense-batch rule, the bigint bound and the NUL refusal are the kernel's, before any I/O.
        batch = list(drafts)
        params = postgres_params(_native.postgres_append_params(aggregate_id, expected_version, batch))
        result: str = await self._fetchval("append", tx, "append", *params)
        # The statement returns its clock reading, not the events: the kernel stamps the drafts with it.
        return _native.postgres_append_result(expected_version, batch, result)

    async def unpublished(self, limit: int) -> list[EventMessage]:
        return _native.postgres_decode_events(await self._unpublished(None, limit))

    async def mark_published(self, ids: Sequence[str]) -> None:
        await self._mark(None, ids)

    async def unpublished_count(self) -> int:
        """How many committed unmarked outbox rows wait, counted without paging them."""
        return await self._count(None)


class PostgresCommandOutbox(_OutboxReads):
    """A command outbox on Postgres: the :class:`~edax.outbox.CommandOutbox` a
    :class:`~edax.repository.Repository` writes under the save's ``tx``, and the :class:`~edax.outbox.Outbox`
    a :class:`~edax.outbox.Relay` drains. A ``tx`` of another transactor raises
    :class:`~edax.errors.BadHandle`; a command id already waiting raises :class:`~edax.errors.CodecError`."""

    _roles: ClassVar[tuple[TableRole, ...]] = ("command_outbox",)
    _outbox: ClassVar[_Outbox] = "command"

    async def enqueue(self, tx: Tx, commands: Sequence[CommandMessage]) -> None:
        if not commands:
            self._transactor._handle(tx)  # a foreign handle is refused even for an empty batch
            return
        params = postgres_params(_native.postgres_enqueue_params(list(commands)))
        await self._fetchval("enqueue", tx, "command_enqueue", *params)

    async def unpublished(self, limit: int) -> list[CommandMessage]:
        return _native.postgres_decode_commands(await self._unpublished(None, limit))

    async def mark_published(self, ids: Sequence[str]) -> None:
        await self._mark(None, ids)

    async def unpublished_count(self) -> int:
        """How many committed unsent commands wait, counted without paging them."""
        return await self._count(None)


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

    async def get(self, id: str, tx: Tx | None = None) -> Entity | None:
        locked = self._lock_reads and self._transactor._handle(tx) is not None
        call = _native.postgres_projection_get(self._projection, id, locked)
        rows = await self._fetch("projection_read", tx, call.statement, *postgres_params(call.params_json))
        return Entity(id, rows[0][0], _json.decode(rows[0][1])) if rows else None

    async def save(self, entity: Entity, tx: Tx | None = None) -> int:
        version = wiring_int(entity.version, "version")
        # The kernel picks the insert or the compare-and-set, and binds the document as it stores it.
        call = _native.postgres_projection_save(
            self._projection, entity.id, version, _json.encode(entity.document)
        )
        written: int | None = await self._fetchval(
            "projection_save", tx, call.statement, *postgres_params(call.params_json)
        )
        return _native.postgres_projection_result(entity.id, entity.version, written)

    async def get_many(self, query: Query = Query(), *, tx: Tx | None = None) -> Page:
        spec = query.to_json()
        # A spec or a cursor the kernel refuses is its terminal CodecError, before any I/O.
        compiled = _native.postgres_compile_query(self._transactor.tables, self._projection, spec)
        params = postgres_params(compiled.params_json)

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
        params = postgres_params(_native.postgres_claim_params(key, owner, payload_hash))
        read_params = postgres_params(
            _native.postgres_claim_read_params(key, self._transactor._settings.claim_ttl_ms)
        )

        async def claim(conn: PgConnection) -> Claimed:
            attempt = 0
            while True:
                attempt += 1
                if await conn.fetchval(self._sql("claim_insert"), *params):
                    return Won(owner.encode())
                row = await conn.fetchval(self._sql("claim_read"), *read_params)
                # The kernel gives the claim up once the key kept changing under its last read.
                read = _native.postgres_claim_read(key, attempt, row)
                action = _names.named(read.action, _CLAIM_ACTIONS, "postgres claim action")
                match action:
                    case "prior":
                        return decode_prior(_names.given(read.value, "prior record"), payload_hash)
                    case "take_over":
                        lapsed = _names.given(read.value, "lapsed owner")
                        if await conn.fetchval(self._sql("claim_take_over"), *params, lapsed):
                            return Won(owner.encode())
                        # Another claimant took the lapsed claim first: the row this claim read is gone.
                        _native.postgres_claim_read(key, attempt, None)
                    case "vanished":
                        pass
                    case _:
                        _names.unreachable(action)

        return await self._transactor.run("claim", tx, claim)

    async def _hold(self, conn: PgConnection, key_params: list[str | None]) -> str | None:
        row: str | None = await conn.fetchval(self._sql("ledger_hold"), *key_params)
        return row

    async def complete(self, key: str, handle: bytes, record: Record, tx: Tx | None = None) -> None:
        body = encode_record(record)
        key_params = postgres_params(_native.postgres_ledger_key_params(key))

        async def complete(conn: PgConnection) -> None:
            # A pending record is the kernel's BadHandle and a lapsed claim its StoreError: nothing written.
            if _native.postgres_complete_write(key, await self._hold(conn, key_params), handle, body):
                settle = postgres_params(_native.postgres_settle_params(key, handle, body))
                await conn.execute(self._sql("ledger_settle"), *settle)

        await self._transactor.run("settle", tx, complete)

    async def release(self, key: str, handle: bytes, tx: Tx | None = None) -> None:
        key_params = postgres_params(_native.postgres_ledger_key_params(key))

        async def release(conn: PgConnection) -> None:
            if _native.postgres_release_write(await self._hold(conn, key_params), handle):
                await conn.execute(self._sql("ledger_delete"), *key_params)

        await self._transactor.run("settle", tx, release, fenced=False)

    async def forget(self, key: str, tx: Tx | None = None) -> None:
        key_params = postgres_params(_native.postgres_ledger_key_params(key))

        async def forget(conn: PgConnection) -> None:
            # A pending claim is the kernel's InProgress, and stays.
            if _native.postgres_forget_write(key, await self._hold(conn, key_params)):
                await conn.execute(self._sql("ledger_delete"), *key_params)

        await self._transactor.run("settle", tx, forget, fenced=False)


class PostgresSagaStore(_OutboxReads):
    """:class:`~edax.saga.SagaStore` on Postgres: instances keyed by ``(saga, id)`` under a compare-and-set
    on their version, a completed instance kept as a tombstone, and each step's commands written in the same
    statement as its state. Inside a unit of work ``load`` holds the instance's row until it ends, so two
    steps of one instance wait for each other instead of failing their write. State crosses as JSON."""

    _roles: ClassVar[tuple[TableRole, ...]] = ("saga_instances", "saga_outbox")
    _outbox: ClassVar[_Outbox] = "saga"

    async def load(self, saga: str, id: str, tx: Tx | None = None) -> SagaInstance | None:
        # Inside a unit of work the read holds the row, so two steps of one instance wait for each other.
        call = _native.postgres_saga_load(saga, id, self._transactor._handle(tx) is not None)
        row: str | None = await self._fetchval(
            "saga_load", tx, call.statement, *postgres_params(call.params_json)
        )
        if row is None:
            return None
        instance = _native.postgres_decode_saga(row)
        if instance.completed and instance.state_json is None:
            return CompletedInstance(instance.version)
        state_json = _names.given(instance.state_json, "saga state")
        protected = _native.decode_protected_state(state_json).value
        if protected is not None and protected.completed:
            return CompletedInstance(instance.version, protected, instance.folded)
        if instance.completed:
            raise CodecError("saga completion disagrees with authenticated state")
        state = protected if protected is not None else _json.decode(state_json)
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
        call = _native.postgres_saga_write(saga, id, version, state, folded, list(commands))
        result: str = await self._fetchval(
            "saga_write", tx, call.statement, *postgres_params(call.params_json)
        )
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
        encoded = (
            _native.encode_protected_state(state)
            if isinstance(state, ProtectedSagaState)
            else _json.encode(state)
        )
        return await self._write(saga, id, version, encoded, folded, commands, tx)

    async def complete(
        self, saga: str, id: str, version: int, commands: Sequence[CommandMessage], tx: Tx | None = None
    ) -> int:
        return await self._write(saga, id, version, None, "", commands, tx)

    async def unpublished(self, saga: str, limit: int) -> list[CommandMessage]:
        return _native.postgres_decode_commands(await self._unpublished(saga, limit))

    async def mark_published(self, saga: str, ids: Sequence[str]) -> None:
        await self._mark(saga, ids)

    async def unpublished_count(self, saga: str) -> int:
        """How many of ``saga``'s committed unsent commands wait, counted without paging them."""
        return await self._count(saga)

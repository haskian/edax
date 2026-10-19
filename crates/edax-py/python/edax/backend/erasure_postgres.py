"""Postgres erasure operation store and transactional destination fences.

The caller owns an asyncpg-shaped pool and applies :func:`reference_ddl` through its migration tool.
Protected application writes must use the same connection and transaction as their write guard.
This store holds no keys and cannot coordinate a separate application's unfenced writes.
"""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import AsyncIterator, Awaitable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Protocol, TypeVar, cast

from .. import _native
from .._wiring import wiring_u64
from ..erasure import ErasureAction, ErasureNotification, ErasureOperation, StoredErasure
from ..errors import BadHandle, EdaxError
from ..sealing import KeyScope
from .postgres import postgres_error, postgres_params

__all__ = ["PgErasureConnection", "PgErasurePool", "PostgresErasureStore", "reference_ddl"]

T = TypeVar("T")


class PgErasureConnection(Protocol):
    """The asyncpg connection operations needed by the store and destination guard."""

    async def execute(self, query: str, *args: str | None) -> str: ...
    async def fetchval(self, query: str, *args: str | None) -> str | int | None: ...
    async def fetch(self, query: str, *args: str | None) -> Sequence[Sequence[str]]: ...
    def transaction(self, *, isolation: str) -> AbstractAsyncContextManager[None]: ...
    def is_in_transaction(self) -> bool: ...
    def terminate(self) -> None: ...


class PgErasurePool(Protocol):
    """A caller-owned pool; configure its normal connection and statement timeouts too."""

    def acquire(self, *, timeout: float) -> AbstractAsyncContextManager[PgErasureConnection]: ...


def reference_ddl(schema: str = "public") -> str:
    """Return the kernel's migration starting point. No database changes occur here."""
    return _native.postgres_erasure_reference_ddl(schema)


class PostgresErasureStore:
    """Durable operations and control outbox, with permanent local scope tombstones.

    Concurrent workers compare-and-set operation revisions; a failed update never enqueues a control
    message. Notifications are published at least once. ``mark_published`` belongs after publication.
    Every database effect has an operation timeout, including transaction entry, commit and rollback.
    """

    def __init__(self, pool: PgErasurePool, *, schema: str = "public", timeout: float = 10.0) -> None:
        if not math.isfinite(timeout) or timeout <= 0:
            raise BadHandle("Postgres erasure timeout must be finite and positive")
        self._schema = schema
        self._pool = pool
        self._timeout = timeout
        self._sql = cast(dict[str, str], json.loads(_native.postgres_erasure_statements(schema)))

    async def check(self) -> None:
        """Read the catalog and refuse incompatible control tables without provisioning them."""
        async with self._transaction() as connection:
            catalog = await self._await(connection.fetchval(self._sql["check_schema"]))
            _native.postgres_erasure_check_catalog(self._schema, cast(str, catalog))

    async def _await(self, call: Awaitable[T]) -> T:
        try:
            return await asyncio.wait_for(call, self._timeout)
        except EdaxError:
            raise
        except Exception as error:
            raise postgres_error("transaction", error) from error

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[PgErasureConnection]:
        held = self._pool.acquire(timeout=self._timeout)
        connection = await self._await(held.__aenter__())
        try:
            transaction = connection.transaction(isolation="read_committed")
            try:
                await self._await(transaction.__aenter__())
            except BaseException:
                connection.terminate()
                raise
            try:
                yield connection
            except BaseException as error:
                try:
                    await self._await(transaction.__aexit__(type(error), error, error.__traceback__))
                except BaseException:
                    connection.terminate()
                    raise
                raise
            else:
                try:
                    await self._await(transaction.__aexit__(None, None, None))
                except BaseException:
                    # A failed commit can be ambiguous; discard this connection and reload on retry.
                    connection.terminate()
                    raise
        finally:
            await self._await(held.__aexit__(None, None, None))

    @staticmethod
    def _decode(row: str) -> StoredErasure:
        wire = json.loads(_native.postgres_erasure_decode_operation(row))
        return StoredErasure(wire["version"], ErasureOperation(json.dumps(wire["operation"])))

    async def create(self, candidate: ErasureOperation) -> StoredErasure:
        params = postgres_params(_native.postgres_erasure_create_params(candidate.record))
        async with self._transaction() as connection:
            await self._await(connection.fetchval(self._sql["create_operation"], *params))
            row = await self._await(
                connection.fetchval(
                    self._sql["load_operation"],
                    *postgres_params(
                        _native.postgres_erasure_load_params(candidate.scope, candidate.request_id)
                    ),
                )
            )
            if row is None:
                raise BadHandle("erasure operation ID is already bound to another request")
            stored = self._decode(cast(str, row))
            _native.erasure_check_request(stored.operation.record, candidate.record)
            return stored

    async def get(self, scope: KeyScope, request_id: str) -> StoredErasure | None:
        async with self._transaction() as connection:
            row = await self._await(
                connection.fetchval(
                    self._sql["load_operation"],
                    *postgres_params(_native.postgres_erasure_load_params(scope, request_id)),
                )
            )
            return None if row is None else self._decode(cast(str, row))

    async def get_by_id(self, operation_id: str) -> StoredErasure | None:
        async with self._transaction() as connection:
            row = await self._await(connection.fetchval(self._sql["get_operation"], operation_id))
            return None if row is None else self._decode(cast(str, row))

    async def advance(
        self, previous: StoredErasure, action: ErasureAction, participant: str | None = None
    ) -> StoredErasure:
        operation = ErasureOperation(_native.erasure_apply(previous.operation.record, action, participant))
        params = postgres_params(_native.postgres_erasure_update_params(previous.to_json(), operation.record))
        async with self._transaction() as connection:
            revision = await self._await(connection.fetchval(self._sql["update_operation"], *params))
            version = _native.postgres_erasure_update_result(cast(int | None, revision))
            return StoredErasure(version, operation)

    async def pending(self, limit: int = 100, *, after: str | None = None) -> list[StoredErasure]:
        async with self._transaction() as connection:
            rows = await self._await(
                connection.fetch(self._sql["pending_operations"], str(wiring_u64(limit, "limit")), after)
            )
            return [self._decode(row[0]) for row in rows]

    async def revoked(self, limit: int = 100, *, after: str | None = None) -> list[StoredErasure]:
        async with self._transaction() as connection:
            rows = await self._await(
                connection.fetch(self._sql["revoked_operations"], str(wiring_u64(limit, "limit")), after)
            )
            return [self._decode(row[0]) for row in rows]

    async def unpublished(self, limit: int = 100) -> list[ErasureNotification]:
        async with self._transaction() as connection:
            rows = await self._await(
                connection.fetch(self._sql["pending_notifications"], str(wiring_u64(limit, "limit")))
            )
            return [ErasureNotification(row[0]) for row in rows]

    async def mark_published(self, operation_id: str) -> None:
        async with self._transaction() as connection:
            await self._await(connection.execute(self._sql["mark_published"], operation_id))

    async def _locks(self, connection: PgErasureConnection, calls_json: str) -> None:
        calls = cast(list[dict[str, object]], json.loads(calls_json))
        for call in calls:
            await self._await(
                connection.execute(
                    self._sql[cast(str, call["statement"])], *cast(list[str | None], call["params"])
                )
            )

    async def guard_in_transaction(self, connection: PgErasureConnection, *scopes: KeyScope) -> None:
        """Fence writes to ``scopes`` on this connection through its existing transaction's commit.

        Call before reading sensitive state or taking application row locks. Pass every scope the
        transaction writes in one call: the kernel then takes their locks in one order for every writer.
        The kernel refuses isolation modes whose snapshot could miss revocation after a lock wait.
        Autocommit is refused.
        """
        if not scopes:
            raise BadHandle("an erasure write guard requires a scope")
        if not connection.is_in_transaction():
            raise BadHandle("an erasure write guard requires an open transaction")
        await self._locks(connection, _native.postgres_erasure_writer_locks(list(scopes)))
        for scope in scopes:
            row = await self._await(
                connection.fetchval(
                    self._sql["check_revoked"], *postgres_params(_native.postgres_erasure_guard_params(scope))
                )
            )
            _native.postgres_erasure_check_guard(cast(str, row))

    @asynccontextmanager
    async def writer_guard(self, scope: KeyScope) -> AsyncIterator[PgErasureConnection]:
        """Open a guarded transaction. Execute application writes using the yielded connection."""
        async with self._transaction() as connection:
            await self.guard_in_transaction(connection, scope)
            yield connection

    async def revoke_in_transaction(self, connection: PgErasureConnection, scope: KeyScope) -> None:
        """Drain guarded writers and install a tombstone; delete plaintext before committing this tx."""
        if not connection.is_in_transaction():
            raise BadHandle("erasure cleanup requires an open transaction")
        await self._locks(connection, _native.postgres_erasure_revocation_locks(scope))
        await self._await(connection.execute(self._sql["revoke"], _native.postgres_erasure_scope_id(scope)))

    async def revoke(self, scope: KeyScope) -> None:
        """Drain existing guarded writers and permanently block subsequent writes in this database."""
        async with self._transaction() as connection:
            await self.revoke_in_transaction(connection, scope)

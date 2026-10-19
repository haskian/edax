"""PostgreSQL projection state storage. Hosts own the schema and consistent backup/restore procedure."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, TypeVar, cast

from .. import _json, _native
from ..erasure import ScopeCleanup
from ..errors import BadHandle, StoreError
from ..outbox import check_page
from ..projection import (
    EffectIntent,
    ProjectionSession,
    ProjectionStateStore,
    ProjectionTarget,
)
from ..sealing import KeyScope, Sealing
from .erasure_postgres import PgErasureConnection, PostgresErasureStore
from .postgres import PgConnection, PostgresTransactionFailure, PostgresTransactor

T = TypeVar("T")

__all__ = ["PostgresProjectionStateStore", "reference_projection_state_ddl"]


def _statements(schema: str, table: str, intents: str) -> dict[str, str]:
    config = _json.encode({"schema": schema, "table": table, "intents": intents})
    return cast(dict[str, str], _json.decode(_native.postgres_projection_statements(config)))


def reference_projection_state_ddl(
    schema: str = "public",
    table: str = "edax_projection_state",
    intents: str = "edax_projection_intents",
) -> str:
    """DDL for the host migration tool; no table is provisioned at runtime."""
    return _statements(schema, table, intents)["ddl"]


class _Session(ProjectionSession):
    def __init__(self, store: PostgresProjectionStateStore, connection: PgConnection) -> None:
        super().__init__(store.target)
        self._store, self._connection = store, connection

    async def _lock(self, exclusive: bool, partition: int | None) -> None:
        await self._store._lock(self._connection, exclusive, partition)

    async def _read(self, keys: Sequence[str]) -> Mapping[str, Any]:
        rows = await self._store._call(self._connection.fetch(self._store._sql["read"], _json.encode(keys)))
        return {row["key"]: _json.decode(row["value"]) for row in rows}

    async def _clock(self) -> int:
        return cast(int, await self._store._call(self._connection.fetchval(self._store._sql["clock"])))

    async def flush(self) -> None:
        """Write every change of the transaction: one statement per kind."""
        store, conn = self._store, self._connection
        if self.inserts:
            await store._call(conn.execute(store._sql["insert"], _json.encode(self.inserts)))
        if self.updates:
            changed = await store._call(conn.fetchval(store._sql["update"], _json.encode(self.updates)))
            if changed != len(self.updates):
                raise StoreError("a projection record vanished inside its own transaction")
        if self.intents:
            await store._call(conn.execute(store._sql["insert_intents"], _json.encode(self.intents)))


class PostgresProjectionStateStore(ProjectionStateStore):
    """One generation's state and effect intents in two host-provisioned tables.

    Each transaction reads its records with one statement and writes each kind of change with one more.
    Partition work shares the generation lock and takes its partition's own lock, so partitions commit in
    parallel; initialization, freeze and activation hold the generation lock alone.
    """

    def __init__(
        self,
        transactor: PostgresTransactor,
        target: ProjectionTarget,
        *,
        schema: str = "public",
        table: str = "edax_projection_state",
        intents: str = "edax_projection_intents",
        sealing: Sealing | None = None,
    ) -> None:
        super().__init__(target, sealing=sealing)
        self._transactor = transactor
        self._sql = _statements(schema, table, intents)
        self._checked = False

    async def _call(self, call: Awaitable[T]) -> T:
        return await self._transactor.call(call)

    async def _lock(self, conn: PgConnection, exclusive: bool, partition: int | None) -> None:
        manifest = self.target.manifest_key
        await self._call(conn.execute(self._sql["lock_exclusive" if exclusive else "lock_shared"], manifest))
        if partition is not None:
            await self._call(conn.execute(self._sql["lock_partition"], manifest, partition))

    async def _owned(self, call: Callable[[PgConnection], Awaitable[T]]) -> T:
        async def checked(conn: PgConnection) -> T:
            if not self._checked:
                await self._transactor.check_server(conn)
                if not await self._call(conn.fetchval(self._sql["check"])):
                    raise BadHandle("postgres projection tables do not meet the kernel contract")
            return await call(conn)

        # The kernel reads the SQLSTATE: a lost lock wait is contention, a missing table is fatal.
        # Callers get that typed verdict; a retry reloads the cursor, so no commit evidence is needed.
        try:
            result = await self._transactor.run_owned(checked, op="projection_state")
        except PostgresTransactionFailure as failure:
            raise failure.cause from failure
        self._checked = True
        return result

    async def _transaction(self, call: Callable[[ProjectionSession], Awaitable[T]]) -> T:
        async def execute(conn: PgConnection) -> T:
            session = _Session(self, conn)
            result = await call(session)
            await session.flush()
            return result

        return await self._owned(execute)

    async def unpublished(self, limit: int) -> list[EffectIntent]:
        check_page(limit)

        async def read(conn: PgConnection) -> list[Any]:
            await self._lock(conn, False, None)
            rows = await self._call(
                conn.fetch(self._sql["pending"], self.target.manifest_key, self.target.generation, str(limit))
            )
            return [_json.decode(row["value"]) for row in rows]

        return await self._delivered(await self._owned(read))

    async def mark_published(self, ids: Sequence[str]) -> None:
        if not ids:
            return

        async def delete(conn: PgConnection) -> None:
            await self._lock(conn, False, None)
            keys = [self.target.intent_key(intent_id) for intent_id in ids]
            await self._call(
                conn.execute(
                    self._sql["mark_published"],
                    _json.encode(keys),
                    self.target.manifest_key,
                    self.target.generation,
                )
            )

        await self._owned(delete)

    def cleanup(self, erasure: PostgresErasureStore | None = None) -> ScopeCleanup:
        """The erasure participant that deletes a scope's state: a subject's, or a whole tenant's. A
        ``plaintext`` target needs it; sealed state is erased by destroying the key. With ``erasure``,
        the store of a ``write_fence="transaction"`` provider on this database, one transaction revokes
        the scope, draining fenced writers, and then deletes. Receipts stay: they hold keyed
        fingerprints, which the destroyed key leaves untestable."""

        async def delete(scope: KeyScope) -> int:
            start, end = self.target.state_scope_range(scope)

            async def run(conn: PgConnection) -> int:
                if erasure is not None:
                    await erasure.revoke_in_transaction(cast(PgErasureConnection, conn), scope)
                return cast(int, await self._call(conn.fetchval(self._sql["delete_scope"], start, end)))

            return await self._owned(run)

        return ScopeCleanup(delete)

    async def prune(self, limit: int = 1000) -> int:
        check_page(limit)

        async def delete(conn: PgConnection) -> int:
            await self._lock(conn, False, None)
            return cast(
                int,
                await self._call(
                    conn.fetchval(
                        self._sql["prune"], self.target.manifest_key, self.target.generation, str(limit)
                    )
                ),
            )

        return await self._owned(delete)

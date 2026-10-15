"""PostgreSQL fold storage. Hosts own the schema and consistent backup/restore procedure."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar, cast

from .. import _json, _native
from ..errors import BadHandle
from ..fold import FoldSession, FoldStore, FoldTarget, OutboxIntent, outbox_intent
from .postgres import PgConnection, PostgresTransactor

T = TypeVar("T")


def _statements(schema: str, table: str) -> dict[str, str]:
    return cast(
        dict[str, str],
        _json.decode(_native.postgres_fold_statements(_json.encode({"schema": schema, "table": table}))),
    )


def reference_fold_ddl(schema: str = "public", table: str = "edax_fold") -> str:
    """DDL for the host migration tool; no table is provisioned at runtime."""
    return _statements(schema, table)["ddl"]


class _Session:
    def __init__(self, store: PostgresFoldStore, connection: PgConnection) -> None:
        self.store, self.connection = store, connection

    async def execute(self, **operation: Any) -> Any:
        store, conn = self.store, self.connection
        request = store.target.request(**operation)
        keys = _native.fold_keys(request)
        rows = await store._transactor.call(conn.fetch(store._sql["read"], keys))
        records = {row["key"]: _json.decode(row["value"]) for row in rows}
        now = await store._transactor.call(conn.fetchval(store._sql["clock"]))
        plan = _json.decode(_native.fold_transition(request, _json.encode(records), now))
        if plan["writes"]:
            await store._transactor.call(conn.execute(store._sql["write"], _json.encode(plan["writes"])))
        if plan["suppression"] is not None:
            await store._transactor.call(
                conn.execute(store._sql["suppress"], store.target.prefix, _json.encode(plan["suppression"]))
            )
        return plan["result"]


class PostgresFoldStore(FoldStore):
    """Serializes one generation's atomic folds and cutovers, including absent state keys.

    This initial implementation favors a single explicit generation lock over concurrent
    per-partition transactions. Different generations can make progress independently.
    """

    def __init__(
        self,
        transactor: PostgresTransactor,
        target: FoldTarget,
        *,
        schema: str = "public",
        table: str = "edax_fold",
    ) -> None:
        super().__init__(target)
        self._transactor = transactor
        self._sql = _statements(schema, table)
        self._checked = False

    async def _owned(self, call: Callable[[PgConnection], Awaitable[T]]) -> T:
        async def locked(conn: PgConnection) -> T:
            if not self._checked:
                await self._transactor.check_server(conn)
                if not await self._transactor.call(conn.fetchval(self._sql["check"])):
                    raise BadHandle("postgres fold table does not meet the kernel contract")
            await self._transactor.call(conn.execute(self._sql["lock"], self.target.manifest_key))
            return await call(conn)

        result = await self._transactor.run_owned(locked)
        self._checked = True
        return result

    async def _transaction(self, call: Callable[[FoldSession], Awaitable[T]]) -> T:
        async def execute(conn: PgConnection) -> T:
            return await call(_Session(self, conn))

        return await self._owned(execute)

    async def pending(self, limit: int = 100) -> Sequence[OutboxIntent]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise BadHandle("outbox page limit must be positive")

        async def read(conn: PgConnection) -> Sequence[OutboxIntent]:
            # Initialization is explicit; this validates existing manifest/checkpoints only.
            manifest = await self._transactor.call(
                conn.fetch(self._sql["read"], _json.encode([self.target.manifest_key]))
            )
            if not manifest:
                raise BadHandle("fold generation is not initialized")
            await _Session(self, conn).execute(op="initialize")
            rows = await self._transactor.call(
                conn.fetch(self._sql["pending"], self.target.manifest_key, self.target.prefix, str(limit))
            )
            return [outbox_intent(_json.decode(row["value"])) for row in rows]

        return await self._owned(read)

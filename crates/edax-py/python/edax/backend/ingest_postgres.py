"""Atomic append/upsert ingestion over a caller-owned Postgres transactor.

The kernel owns keys, decisions, statements and error recovery. This adapter owns each
transaction; a result is returned only after the database confirms its commit.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, TypeVar, cast

from .. import _json, _native
from ..errors import BadHandle
from ..ingestion import BatchCommitError, BatchInput, BatchResult, IngestionTarget
from ..outbox import check_page
from .postgres import PgConnection, PostgresTransactionFailure, PostgresTransactor

__all__ = ["PostgresIngestionSink", "reference_ingestion_ddl"]

T = TypeVar("T")


def _statements(schema: str, tables: Mapping[str, str] | None) -> dict[str, str]:
    return cast(
        dict[str, str],
        _json.decode(
            _native.postgres_ingest_statements(_json.encode({"schema": schema, "names": dict(tables or {})}))
        ),
    )


def reference_ingestion_ddl(schema: str = "public", tables: Mapping[str, str] | None = None) -> str:
    """DDL for the host's migration tool. The sink never creates tables."""
    return _statements(schema, tables)["ddl"]


def _sqlstate(error: Exception) -> str | None:
    """Acquisition and deadline wrappers preserve the driver error as their cause."""
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        state = getattr(current, "sqlstate", None)
        if isinstance(state, str):
            return state
        current = current.__cause__
    return None


class PostgresIngestionSink:
    """Durable append/upsert sink, with an independent rejection outbox.

    One target generation binds an immutable mode, source incarnation and mapper identity.
    Callers own migrations and the pool. Calls inside an ambient unit of work are refused.
    """

    def __init__(
        self,
        transactor: PostgresTransactor,
        target: IngestionTarget,
        *,
        schema: str = "public",
        tables: Mapping[str, str] | None = None,
    ) -> None:
        self._transactor = transactor
        self._target = target
        self._role = "events" if target.mode == "append" else "state"
        self._sql = _statements(schema, tables)
        self._manifest_key = target.key(["manifest"])
        self._checked = False
        self._bound = False

    @property
    def target(self) -> IngestionTarget:
        return self._target

    async def _call(self, call: Awaitable[T]) -> T:
        return await self._transactor.call(call)

    async def _check(self, conn: PgConnection) -> None:
        if self._checked:
            return
        await self._transactor.check_server(conn)
        if not await self._call(conn.fetchval(self._sql["check"])):
            raise BadHandle("postgres ingestion tables do not meet the kernel's table contract")
        self._checked = True

    async def _own(self, call: Callable[[PgConnection], Awaitable[T]]) -> T:
        needs_bind = not self._bound

        async def checked(conn: PgConnection) -> T:
            await self._check(conn)
            if needs_bind:
                # Every entry point must bind both stores before reusing a durable decision.
                for role in (self._role, "rejections"):
                    manifests = await self._read(conn, role, [self._manifest_key])
                    previous = manifests.get(self._manifest_key)
                    _native.ingest_bind_contract(_json.encode(previous), self.target.manifest)
                    if previous is None:
                        # Insert unless another first bind did; then compare whatever committed.
                        manifest = {self._manifest_key: _json.decode(self.target.manifest)}
                        await self._call(conn.execute(self._sql[f"bind_{role}"], _json.encode(manifest)))
                        bound = (await self._read(conn, role, [self._manifest_key])).get(self._manifest_key)
                        _native.ingest_bind_contract(_json.encode(bound), self.target.manifest)
            return await call(conn)

        try:
            result = await self._transactor.run_owned(checked)
        except PostgresTransactionFailure as failure:
            error = failure.cause
            kind = _native.postgres_ingest_recovery(
                failure.rolled_back and not failure.commit_started, _sqlstate(error)
            )
            raise BatchCommitError(kind, error) from failure
        # Only a confirmed commit proves the durable manifest exists.
        self._bound = True
        return result

    async def _read(self, conn: PgConnection, role: str, keys: Sequence[str]) -> dict[str, Any]:
        rows = await self._call(conn.fetch(self._sql[f"read_{role}"], _json.encode(keys)))
        return {row["key"]: _json.decode(row["value"]) for row in rows}

    async def _insert(self, conn: PgConnection, role: str, values: Mapping[str, Any]) -> None:
        if values:
            await self._call(conn.execute(self._sql[f"insert_{role}"], _json.encode(values)))

    async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
        """Commit accepted state and durable rejections together; retain each original outcome."""
        originals = tuple(inputs)
        self.target.validate(originals)
        encoded = _json.encode(originals)
        sources = _json.encode(
            [{"source": item.source, "fingerprinted": item.raw_fingerprint is not None} for item in originals]
        )
        keys = [item.key for item in originals]

        async def apply(conn: PgConnection) -> BatchResult:
            # Both reads lock the rows they find, in key order, before the plan reads them.
            rows = await self._call(conn.fetch(self._sql["read_rejections_for_plan"], sources))
            rejected = {row["key"]: _json.decode(row["value"]) for row in rows}
            stored = await self._read(conn, f"{self._role}_metadata", keys)
            plan_text = _native.ingest_plan(
                self.target.mode, encoded, _json.encode(stored), _json.encode(rejected)
            )
            plan = _json.decode(plan_text)
            writes: dict[str, Any] = plan["writes"]
            changed = {key: value for key, value in writes.items() if key in stored}
            if changed:
                await self._call(conn.execute(self._sql[f"update_{self._role}"], _json.encode(changed)))
            await self._insert(conn, self._role, {k: v for k, v in writes.items() if k not in stored})
            # A rejection is decided once: only one this batch decided is written.
            await self._insert(
                conn, "rejections", {k: v for k, v in plan["rejections"].items() if k not in rejected}
            )
            return BatchResult.from_json(plan_text)

        return await self._own(apply)

    async def pending(self, limit: int = 100) -> dict[str, dict[str, Any]]:
        """Committed unpublished rejections. Publication is at least once."""
        check_page(limit)

        async def read(conn: PgConnection) -> dict[str, dict[str, Any]]:
            rows = await self._call(conn.fetch(self._sql["pending"], self.target.prefix, str(limit)))
            return {row["key"]: _json.decode(row["value"]) for row in rows}

        return await self._own(read)

    async def published(self, confirmed: Mapping[str, str]) -> None:
        """Record confirmed publication of each source, guarded by its original raw fingerprint, in one
        statement. A source that is unknown, or whose fingerprint changed, raises
        :class:`~edax.errors.BadHandle` and marks nothing."""
        if not confirmed:
            return

        async def mark(conn: PgConnection) -> None:
            marked = await self._call(
                conn.fetchval(self._sql["mark_published"], _json.encode(dict(confirmed)), self.target.prefix)
            )
            if marked != len(confirmed):
                raise BadHandle("rejection is missing or its source fingerprint changed")

        await self._own(mark)

    async def get(self, tenant: str | None, row_key: str) -> dict[str, Any] | None:
        """Read one committed event/state envelope, including a retained deletion marker."""
        key = self.target.row_key(tenant, row_key)

        async def read(conn: PgConnection) -> dict[str, Any] | None:
            return (await self._read(conn, self._role, [key])).get(key)

        return await self._own(read)

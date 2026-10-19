"""Atomic append/upsert ingestion over a caller-owned Postgres transactor.

The kernel owns keys, decisions, statements and error recovery. This adapter owns each
``commit`` transaction; ``stage`` joins a caller transaction and returns provisional outcomes.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, TypeVar, cast

from .. import _json, _native
from ..erasure import ScopeCleanup
from ..errors import BadHandle
from ..ingestion import (
    BatchInput,
    BatchResult,
    IngestionTarget,
    StagedBatchResult,
    open_private,
    prepare_ingestion,
)
from ..outbox import check_page
from ..sealing import KeyScope, Sealing
from ..unit_of_work import Tx
from .erasure_postgres import PgErasureConnection, PostgresErasureStore
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


class PostgresIngestionSink:
    """Durable append/upsert sink, with an independent rejection outbox.

    One target generation binds an immutable mode, source incarnation and mapper identity.
    Callers own migrations and the pool. ``commit`` refuses an ambient unit of work; ``stage``
    requires one (or an explicit live transaction).
    ``sealing`` fences writes and seals each subject's ``private`` part of a sealed target, and
    opens it in :meth:`get`.
    """

    def __init__(
        self,
        transactor: PostgresTransactor,
        target: IngestionTarget,
        *,
        schema: str = "public",
        tables: Mapping[str, str] | None = None,
        sealing: Sealing | None = None,
    ) -> None:
        self._transactor = transactor
        self._target = target
        self.sealing = sealing
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
        """Run ``call`` in its own transaction. A failure raises :class:`PostgresTransactionFailure`: the
        kernel's typed verdict as its cause, with the rollback evidence batch recovery reads."""

        async def checked(conn: PgConnection) -> T:
            await self._bind(conn)
            return await call(conn)

        result = await self._transactor.run_owned(checked, op="ingest")
        # Only a confirmed commit proves the durable manifest exists.
        self._bound = True
        return result

    async def _bind(self, conn: PgConnection) -> None:
        await self._check(conn)
        if self._bound:
            return
        # Staging never caches this: an outer rollback can discard either manifest.
        for role in (self._role, "rejections"):
            manifests = await self._read(conn, role, [self._manifest_key])
            previous = manifests.get(self._manifest_key)
            _native.ingest_bind_contract(_json.encode(previous), self.target.manifest)
            if previous is None:
                manifest = {self._manifest_key: _json.decode(self.target.manifest)}
                await self._call(conn.execute(self._sql[f"bind_{role}"], _json.encode(manifest)))
                bound = (await self._read(conn, role, [self._manifest_key])).get(self._manifest_key)
                _native.ingest_bind_contract(_json.encode(bound), self.target.manifest)

    async def _typed(self, call: Callable[[PgConnection], Awaitable[T]]) -> T:
        """:meth:`_own` for callers that need no commit evidence: they get the kernel's typed verdict."""
        try:
            return await self._own(call)
        except PostgresTransactionFailure as failure:
            raise failure.cause from failure

    async def _read(self, conn: PgConnection, role: str, keys: Sequence[str]) -> dict[str, Any]:
        rows = await self._call(conn.fetch(self._sql[f"read_{role}"], _json.encode(keys)))
        return {row["key"]: _json.decode(row["value"]) for row in rows}

    async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
        """Commit accepted state and rejections in an owned transaction; refuse an ambient unit."""
        async with prepare_ingestion(self.target, self.sealing, inputs) as originals:
            return await self._own(lambda conn: self._apply(conn, originals))

    async def stage(self, inputs: Sequence[BatchInput], *, tx: Tx | None = None) -> StagedBatchResult:
        """Join an existing transaction. Outcomes stay provisional until the outer commit.

        After handle admission, a failure aborts the transaction even if the caller catches it. Erasure
        fences remain held until commit/rollback; use ``transaction(transactor)`` with guard-based
        key providers. This operation never acknowledges sources or publishes rejections.
        """
        handle = self._transactor.require_transaction(tx)
        try:
            async with prepare_ingestion(
                self.target, self.sealing, inputs, transactor=self._transactor, tx=handle
            ) as originals:

                async def apply(conn: PgConnection) -> BatchResult:
                    await self._bind(conn)
                    return await self._apply(conn, originals)

                result = await self._transactor.run("ingest", handle, apply)
                return StagedBatchResult(result.outcomes, result.rejections)
        except BaseException:
            handle.aborted = True
            raise

    async def _apply(self, conn: PgConnection, originals: Sequence[BatchInput]) -> BatchResult:
        encoded = _json.encode(originals)
        sources = _json.encode([item.source for item in originals])
        keys = [item.key for item in originals]
        # Lock target rows first. A fresh rejection read must follow any wait for a target
        # writer, whose commit may have rejected this source and advanced the row together.
        # Missing targets still use plain INSERT: a concurrent first write aborts this plan.
        stored = await self._read(conn, f"{self._role}_metadata", keys)
        rows = await self._call(conn.fetch(self._sql["read_rejections_for_plan"], sources))
        rejected = {row["key"]: _json.decode(row["value"]) for row in rows}
        stored_text, rejected_text = _json.encode(stored), _json.encode(rejected)
        plan_text = _native.ingest_plan(self.target.mode, encoded, stored_text, rejected_text)
        writes: dict[str, str] = _json.decode(
            _native.postgres_ingest_write_plan(plan_text, stored_text, rejected_text)
        )
        # Preserve the kernel's exact-number parameter text across the I/O boundary.
        for parameter, statement in (
            ("updates", f"update_{self._role}"),
            ("inserts", f"insert_{self._role}"),
            ("rejections", "insert_rejections"),
        ):
            if writes[parameter] != "{}":
                await self._call(conn.execute(self._sql[statement], writes[parameter]))
        return BatchResult.from_json(plan_text)

    def cleanup(
        self,
        erasure: PostgresErasureStore | None = None,
        *,
        key_namespace: str | None = None,
        limit: int = 1000,
    ) -> ScopeCleanup:
        """The erasure participant that deletes a scope's rows: a subject's, or a whole tenant's. A
        ``plaintext`` target or subjectless state needs it. With ``erasure``, the destination fence store,
        the first page revokes and drains writers before deleting. Further pages commit independently;
        a retry reinstalls revocation before continuing. ``key_namespace`` defaults to the sink's sealing
        namespace and is required without sealing. Rejections retain the raw source record."""

        check_page(limit)

        async def delete(scope: KeyScope) -> int:
            start, end = self.target.scope_range(scope)
            total, first = 0, True
            while True:

                async def run(conn: PgConnection, revoke: bool = first) -> int:
                    if revoke and erasure is not None:
                        await erasure.revoke_in_transaction(cast(PgErasureConnection, conn), scope)
                    return cast(
                        int,
                        await self._call(
                            conn.fetchval(self._sql[f"delete_scope_{self._role}"], start, end, str(limit))
                        ),
                    )

                removed = await self._typed(run)
                total += removed
                # Only a committed first page proves that the destination tombstone exists.
                first = False
                if removed < limit:
                    return total

        return ScopeCleanup(delete, sealing=self.sealing, key_namespace=key_namespace)

    async def pending(self, limit: int = 100) -> dict[str, dict[str, Any]]:
        """Committed unpublished rejections. Publication is at least once."""
        check_page(limit)

        async def read(conn: PgConnection) -> dict[str, dict[str, Any]]:
            rows = await self._call(conn.fetch(self._sql["pending"], self.target.prefix, str(limit)))
            return {row["key"]: _json.decode(row["value"]) for row in rows}

        return await self._typed(read)

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

        await self._typed(mark)

    async def get(
        self, tenant: str | None, row_key: str, subject: str | None = None, *, tx: Tx | None = None
    ) -> dict[str, Any] | None:
        """Read one committed envelope, or this explicit ``tx``'s writes. With no ``tx`` this
        keeps the owned-transaction contract and refuses an ambient unit of work."""
        key = self.target.row_key(tenant, row_key, subject)

        async def read(conn: PgConnection) -> dict[str, Any] | None:
            return (await self._read(conn, self._role, [key])).get(key)

        if tx is None:
            row = await self._typed(read)
        else:
            handle = self._transactor.require_transaction(tx)

            async def checked(conn: PgConnection) -> dict[str, Any] | None:
                await self._bind(conn)
                return await read(conn)

            try:
                row = await self._transactor.run("ingest", handle, checked)
            except BaseException:
                handle.aborted = True
                raise
        return await open_private(self.target, self.sealing, key, row)

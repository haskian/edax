"""Prove the packaged conformance suite fails a store that breaks the clause.

`test_contract_memory.py` shows the suites pass a correct backend. That alone is worth little: a
suite that asserts nothing also passes. Each case here breaks one clause the way a real backend
breaks it, then asserts the corresponding contract method fails.

Every store below is a plausible mistake, not a strawman — a signed column for an unsigned hash, a
`>=` where the contract says `>`, a version advanced by a failed save.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from types import SimpleNamespace
from typing import Any, cast

import pytest
from edax import _edax
from edax._values import replace as replace_message
from edax.backend.memory import (
    MemoryCommandOutbox,
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemoryLedger,
    MemoryProjectionStore,
    MemorySagaStore,
    MemoryTransactor,
)
from edax.errors import CodecError, ConcurrencyError, EdaxError, StoreError
from edax.idempotency import Claimed, Done, Failed, Pending, Record, Won, decode_record
from edax.message import CommandMessage, EventMessage
from edax.projection import Entity, ProjectionQuery
from edax.query import Page, Query
from edax.saga import RunningInstance, SagaInstance, SagaOutbox
from edax.store import Snapshot
from edax.testing import (
    CommandOutboxContract,
    Delegating,
    EventOutboxContract,
    EventStoreContract,
    IdempotencyStoreContract,
    ProjectionStoreContract,
    SagaOutboxContract,
    SagaStoreContract,
    recorded,
)
from edax.unit_of_work import current_tx


class InclusiveLoad(Delegating):
    """`load(after=n)` returns `sequence >= n` instead of `> n`. The off-by-one that replays one
    event per load, so every projection double-counts its oldest unapplied event."""

    async def load(self, tx: Any, aggregate_id: str, after: int = 0) -> Any:
        rows = await self._inner.load(tx, aggregate_id, after=max(0, after - 1))
        return rows


class CountsPublishedRows(Delegating):
    """`unpublished_count` counts every outbox row, marked or not — `SELECT count(*)` without the
    `WHERE published_at IS NULL`. The backlog then only ever grows, so the health check it exists
    for is red forever."""

    def __init__(self, inner: Any) -> None:
        super().__init__(inner)
        self._ever = 0

    async def append(self, tx: Any, aggregate_id: str, expected: int, events: Any) -> list[EventMessage]:
        stored = await self._inner.append(tx, aggregate_id, expected, events)
        self._ever += len(stored)
        return stored

    async def unpublished_count(self) -> int:
        return self._ever


class LeakyOutbox(Delegating):
    """`unpublished` reads rows the transaction has not committed. A relay then publishes an event
    whose state change may still roll back."""

    def __init__(self, inner: Any, db: MemoryTransactor) -> None:
        super().__init__(inner)
        self._staged: list[Any] = []

    async def append(self, tx: Any, aggregate_id: str, expected: int, events: Any) -> list[EventMessage]:
        stored = await self._inner.append(tx, aggregate_id, expected, events)
        self._staged.extend(stored)
        return stored

    async def unpublished(self, limit: int) -> Any:
        committed = await self._inner.unpublished(limit=limit)
        return list(committed) + [e for e in self._staged if e not in committed]


class ReturnsTheOldVersion(Delegating):
    """`save` persists but returns the version it was given, so the caller's next save conflicts with
    the row it just wrote itself."""

    async def save(self, entity: Entity, tx: Any = None) -> int:
        await self._inner.save(entity, tx)
        return entity.version


class GetDropsTheRowKey(Delegating):
    """`get` returns the stored document alone, while `get_many` returns it keyed: the two reads disagree
    on what a row is, and a document saved with a stale `id` reads back with it."""

    async def get(self, id: str, tx: Any = None) -> Entity | None:
        found: Entity | None = await self._inner.get(id, tx)
        if found is None:
            return None
        return replace(found, document={k: v for k, v in found.document.items() if k != "id"})


class WrapsAnyDocument(Delegating):
    """`save` stores a list or a scalar under a wrapper key instead of refusing it: the query can key it
    now, but the caller's document is not what comes back."""

    async def save(self, entity: Entity, tx: Any = None) -> int:
        if not isinstance(entity.document, dict):
            entity = replace(entity, document={"value": entity.document})
        saved: int = await self._inner.save(entity, tx)
        return saved


class IgnoresAPendingRecord(Delegating):
    """`complete` with a pending record returns as if it settled the claim, and writes nothing: the
    caller believes the outcome landed."""

    async def complete(self, key: str, handle: bytes, record: Record, tx: Any = None) -> None:
        if isinstance(record.state, Pending):
            return
        await self._inner.complete(key, handle, record, tx)


class AlwaysWins(Delegating):
    """`claim` reports a win for a key another caller already holds — a store that cannot tell "I
    won" from "someone else holds this". Two handlers then run for one key."""

    async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Claimed:
        await self._inner.claim(key, payload_hash, tx)
        return Won()


class SignedHash(Delegating):
    """The payload hash stored in a signed 64-bit column and read back without a cast. The write keeps the
    bits, and the read re-signs them: everything passes until a hash above 2^63 appears, and then a genuine
    duplicate reads as `PayloadMismatch`."""

    async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Claimed:
        prior = await self._inner.claim(key, payload_hash, tx)
        if isinstance(prior, Won) or prior.payload_hash < 2**63:
            return prior
        return replace(prior, payload_hash=prior.payload_hash - 2**64)


class SnapshotsWithoutChecks(Delegating):
    """Snapshots in a plain upsert table, last writer wins, no version check: the store a snapshot "is
    only a cache" argument produces. A snapshot ahead of the stream then blocks every append, and a slow
    writer's older one rolls the cache back."""

    def __init__(self, inner: Any) -> None:
        super().__init__(inner)
        self._snapshots: dict[str, Snapshot] = {}

    async def save_snapshot(self, tx: Any, snapshot: Snapshot) -> None:
        self._snapshots[snapshot.aggregate_id] = snapshot

    async def load_snapshot(self, tx: Any, aggregate_id: str) -> Snapshot | None:
        return self._snapshots.get(aggregate_id)


class KeysRowsByAggregateAndId(Delegating):
    """The outbox keyed by `(aggregate_id, id)` instead of `id`: a second aggregate's event under a taken
    id is stored, and marking the first one published by id prunes both."""

    async def append(self, tx: Any, aggregate_id: str, expected: int, events: Any) -> list[EventMessage]:
        try:
            return await self._inner.append(tx, aggregate_id, expected, events)
        except CodecError:
            renamed = [replace_message(e, id=f"{e.id}@{aggregate_id}") for e in events]
            return await self._inner.append(tx, aggregate_id, expected, renamed)


class DeletesOnComplete:
    """The saga store as it was: completion deletes the row, so the version starts over at 0. A writer
    that loaded the instance before it completed then succeeds (ABA), and a late event starts the
    workflow again."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], tuple[int, Any]] = {}
        self._outbox: dict[str, list[Any]] = {}

    async def load(self, saga: str, id: str, tx: Any = None) -> SagaInstance | None:
        row = self._rows.get((saga, id))
        return None if row is None else RunningInstance(row[1], row[0])

    async def _write(self, saga: str, id: str, version: int, commands: Any) -> int:
        stored = self._rows.get((saga, id), (0, None))[0]
        if stored != version:
            raise ConcurrencyError(f"expected {version}, stored {stored}")
        self._outbox.setdefault(saga, []).extend(commands)
        return version + 1

    async def save(
        self, saga: str, id: str, state: Any, version: int, folded: str, commands: Any, tx: Any = None
    ) -> int:
        new = await self._write(saga, id, version, commands)
        self._rows[(saga, id)] = (new, state)
        return new

    async def complete(self, saga: str, id: str, version: int, commands: Any, tx: Any = None) -> int:
        new = await self._write(saga, id, version, commands)
        del self._rows[(saga, id)]
        return new

    async def unpublished(self, saga: str, limit: int) -> list[Any]:
        return self._outbox.get(saga, [])[:limit]

    async def mark_published(self, saga: str, ids: Any) -> None:
        self._outbox[saga] = [c for c in self._outbox.get(saga, []) if c.id not in set(ids)]


class KeysSagasByInstance(Delegating):
    """One table for every saga, keyed by the instance id alone: the schema before sagas were named. Two
    sagas over one database then fold into each other's instance, and share one outbox."""

    async def load(self, saga: str, id: str, tx: Any = None) -> SagaInstance | None:
        return await self._inner.load("all", id, tx)

    async def save(
        self, saga: str, id: str, state: Any, version: int, folded: str, commands: Any, tx: Any = None
    ) -> int:
        return await self._inner.save("all", id, state, version, folded, commands, tx)

    async def complete(self, saga: str, id: str, version: int, commands: Any, tx: Any = None) -> int:
        return await self._inner.complete("all", id, version, commands, tx)

    async def unpublished(self, saga: str, limit: int) -> list[Any]:
        return await self._inner.unpublished("all", limit)

    async def mark_published(self, saga: str, ids: Any) -> None:
        await self._inner.mark_published("all", ids)


class SendsBeforeTheWrite(Delegating):
    """Commands enqueued on their own, before the version check and outside the unit of work: the
    dispatch-before-persist runner moved into the store. A fold that loses the race, or rolls back, still
    sends."""

    def __init__(self, inner: Any) -> None:
        super().__init__(inner)
        self._sent: list[Any] = []

    async def save(
        self, saga: str, id: str, state: Any, version: int, folded: str, commands: Any, tx: Any = None
    ) -> int:
        self._sent.extend(commands)
        return await self._inner.save(saga, id, state, version, folded, [], tx)

    async def complete(self, saga: str, id: str, version: int, commands: Any, tx: Any = None) -> int:
        self._sent.extend(commands)
        return await self._inner.complete(saga, id, version, [], tx)

    async def unpublished(self, saga: str, limit: int) -> list[Any]:
        return (await self._inner.unpublished(saga, limit) + self._sent)[:limit]


class MarksTheWholeInstance(Delegating):
    """`mark_published` prunes every command of the marked command's instance: a `DELETE ... WHERE
    aggregate_id = ?`. The commands decided after it are dropped unsent."""

    async def mark_published(self, saga: str, ids: Any) -> None:
        marked = set(ids)
        rows = await self._inner.unpublished(saga, 1000)
        instances = {c.aggregate_id for c in rows if c.id in marked}
        await self._inner.mark_published(
            saga, [c.id for c in rows if c.aggregate_id in instances] + list(ids)
        )


class KeysByTheDraft(Delegating):
    """Files each event under its own `aggregate_id` column instead of the key `append` was called with: a
    schema whose stream key is the event's aggregate id. Under a pool the drafts keep the bare id, so two
    tenants' same-named aggregates land in one stream."""

    async def append(self, tx: Any, aggregate_id: str, expected: int, events: Any) -> list[EventMessage]:
        stream = events[0].aggregate_id if events else aggregate_id
        return await self._inner.append(tx, stream, expected, events)


class SagaDropsTheTenant(Delegating):
    """The saga outbox persists the command columns it was written for, from before the tenant existed. A
    pooled bus then refuses every command the saga sends."""

    async def save(
        self, saga: str, id: str, state: Any, version: int, folded: str, commands: Any, tx: Any = None
    ) -> int:
        return await self._inner.save(
            saga, id, state, version, folded, [replace_message(c, tenant=None) for c in commands], tx
        )

    async def complete(self, saga: str, id: str, version: int, commands: Any, tx: Any = None) -> int:
        return await self._inner.complete(
            saga, id, version, [replace_message(c, tenant=None) for c in commands], tx
        )


class OutboxDropsTheTenant(Delegating):
    """The command outbox without a tenant column: the relay then publishes commands a pooled producer
    cannot key."""

    async def enqueue(self, tx: Any, commands: Any) -> None:
        await self._inner.enqueue(tx, [replace_message(c, tenant=None) for c in commands])


class OutboxIgnoresTheTx(Delegating):
    """Writes every command at once, in a connection of its own, whatever ``tx`` it is handed: the dual
    write the outbox exists to remove. A command is sent though its events rolled back, and a ``tx`` of
    another database is never noticed."""

    def __init__(self, inner: MemoryCommandOutbox, db: MemoryTransactor) -> None:
        super().__init__(inner)
        self._auto_commit = db.auto_commit()

    async def enqueue(self, tx: Any, commands: Any) -> None:
        await self._inner.enqueue(self._auto_commit, commands)


class OutboxUpserts(Delegating):
    """``INSERT ... ON CONFLICT (id) DO UPDATE``: a repeated id replaces the waiting row instead of being
    refused, so a second command under one id silently overwrites the first."""

    async def enqueue(self, tx: Any, commands: Any) -> None:
        try:
            await self._inner.enqueue(tx, commands)
        except CodecError:
            await self._inner.mark_published([c.id for c in commands])
            await self._inner.enqueue(tx, commands)


class OutboxNewestFirst(Delegating):
    """``ORDER BY issued_at DESC``: the relay sends a later decision about an aggregate ahead of an
    earlier one."""

    async def unpublished(self, limit: int) -> list[Any]:
        return list(reversed(await self._inner.unpublished(1000)))[:limit]


class OutboxRefusesUnknownIds(Delegating):
    """``mark_published`` checks each id and raises for one it does not hold, so the relay's re-mark
    after a lost response fails the drain."""

    async def mark_published(self, ids: Any) -> None:
        waiting = {c.id for c in await self._inner.unpublished(1000)}
        if not set(ids) <= waiting:
            raise StoreError(f"unknown command ids: {sorted(set(ids) - waiting)}")
        await self._inner.mark_published(ids)


class DropsMetadata(Delegating):
    """Persists the columns it recognised and quietly drops `metadata`: the application's context is gone
    at the first replay, and nothing reports an error."""

    async def append(self, tx: Any, aggregate_id: str, expected: int, events: Any) -> list[EventMessage]:
        stripped = [replace_message(e, metadata={}) for e in events]
        return await self._inner.append(tx, aggregate_id, expected, stripped)


class RestampsOnRead(Delegating):
    """Reads ``recorded_at`` from the clock at query time, a computed column, rather than the time the row
    was written: the outbox lag and every age a reader computes come out wrong."""

    async def load(self, tx: Any, aggregate_id: str, after: int = 0) -> list[EventMessage]:
        rows = await self._inner.load(tx, aggregate_id, after)
        return [replace_message(m, recorded_at=_edax.current_timestamp()) for m in rows]


class ShortcutsEmptyAppend(Delegating):
    """Returns early for an empty batch instead of checking the version. A stale caller then believes
    it holds the current version, and its next save overwrites someone else's work."""

    async def append(self, tx: Any, aggregate_id: str, expected: int, events: Any) -> list[EventMessage]:
        if not events:
            return []
        return await self._inner.append(tx, aggregate_id, expected, events)


class RenumbersTheBatch(Delegating):
    """Assigns the batch its own dense sequences (an auto-increment column) instead of refusing a batch
    the caller numbered wrong. The version check still holds, so nothing else in the suite notices; the
    stream then disagrees with the drafts the caller numbered."""

    async def append(self, tx: Any, aggregate_id: str, expected: int, events: Any) -> list[EventMessage]:
        renumbered = [replace_message(e, sequence=expected + i) for i, e in enumerate(events, start=1)]
        return await self._inner.append(tx, aggregate_id, expected, renumbered)


class WritesThenChecks(Delegating):
    """Inserts the batch at the stored version, then checks the caller's expected version and refuses. The
    refusal is right, but the rows stay staged in the caller's transaction, and a caller that handles the
    `ConcurrencyError` inside it commits them."""

    async def append(self, tx: Any, aggregate_id: str, expected: int, events: Any) -> list[EventMessage]:
        stored = await self._inner.load(tx, aggregate_id)
        version = stored[-1].sequence if stored else 0
        renumbered = [replace_message(e, sequence=version + i) for i, e in enumerate(events, start=1)]
        appended = await self._inner.append(tx, aggregate_id, version, renumbered)
        if version != expected:
            raise ConcurrencyError(f"expected version {expected}, stored {version}")
        return appended


class DropsTheErrorCode(Delegating):
    """Keeps a cached failure's message but not its code — a ledger schema from before the column
    existed. Every replay then loses the original code in every SDK sharing the ledger."""

    async def complete(self, key: str, handle: bytes, record: Record, tx: Any = None) -> None:
        if isinstance(record.state, Failed):
            record = Record.failed(record.payload_hash, record.state.error)
        await self._inner.complete(key, handle, record, tx)


class DropsTheErrorReason(Delegating):
    """Keeps a cached rejection's code but not its reason: a ledger that rebuilds the record from the
    fields it knows. A replay then cannot tell ``not_found`` from another rejection."""

    async def complete(self, key: str, handle: bytes, record: Record, tx: Any = None) -> None:
        if isinstance(record.state, Failed):
            record = Record.failed(record.payload_hash, record.state.error, record.state.error_code)
        await self._inner.complete(key, handle, record, tx)


class RefusesAHeldKeyAsAStoreError(Delegating):
    """Refuses a settlement over a key another open transaction holds, but as a ``StoreError``, the error a
    driver raises for a lock timeout when nobody maps it. The guard reads that as a store fault, not as
    the conflict the seam names."""

    async def complete(self, key: str, handle: bytes, record: Record, tx: Any = None) -> None:
        try:
            await self._inner.complete(key, handle, record, tx)
        except ConcurrencyError as e:
            raise StoreError(f"lock wait timeout on {key}") from e

    async def release(self, key: str, handle: bytes, tx: Any = None) -> None:
        try:
            await self._inner.release(key, handle, tx)
        except ConcurrencyError as e:
            raise StoreError(f"lock wait timeout on {key}") from e


class DecodesStrictly(Delegating):
    """Reads its rows with the strict ``decode_record`` instead of ``decode_prior``, as the Python stores did
    before: an unreadable row raises ``CodecError`` on every claim, while the Rust SDK sharing the ledger
    replays it as a cached failure."""

    async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Claimed:
        claimed = self._inner._seam().claim(self._inner._token(tx), key, payload_hash)
        if claimed.won:
            assert claimed.handle is not None
            return Won(claimed.handle)
        assert claimed.prior is not None
        return decode_record(claimed.prior)


class ReadsAnUnreadableRowAsFree(DecodesStrictly):
    """Treats a row it cannot decode as absent and claims over it, the ``except: pass`` a hand-rolled store
    writes: the handler runs again for a key whose outcome is already cached."""

    async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Claimed:
        try:
            return await super().claim(key, payload_hash, tx)
        except CodecError:
            self._inner.expire(key)
            return await self._inner.claim(key, payload_hash, tx)


class NoneResultReadsAsPending(Delegating):
    """Cannot tell "completed with no value" from "still pending": it stores the result column as NULL
    and infers pending from it. A handler returning `None` then re-runs
    on every redelivery."""

    async def complete(self, key: str, handle: bytes, record: Record, tx: Any = None) -> None:
        if record.state == Done(None):
            return  # "nothing to store", so the pending claim stays
        await self._inner.complete(key, handle, record, tx)


class ReadsThenWrites:
    """`claim` reads the key, then writes it in a second call: `SELECT`, then `INSERT`, with no unique
    constraint between them. Two concurrent claimants both read "free", and two handlers run."""

    def __init__(self) -> None:
        self.records: dict[str, Record] = {}

    async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Claimed:
        prior = self.records.get(key)
        await asyncio.sleep(0)  # the round trip between the read and the write
        if prior is not None:
            return prior
        self.records[key] = Record.pending(payload_hash)
        return Won()


class ExpiringLedger:
    """A ledger whose claims expire, with the fence as a parameter, so each takeover clause is shown failing
    the fence a real backend got wrong. ``by_key`` keeps one token per key in the store instance, as the
    Redis store did before claim handles: a second claim on the key in one process replaces the first's
    token.
    ``by_owner`` fences on the claim's owner, so it also matches the settled record that owner wrote, as
    the old prefix fence did. ``by_claim`` is the correct fence, the positive control."""

    joins_transaction = False

    def __init__(self, fence: str) -> None:
        self.fence = fence
        self.records: dict[str, tuple[bytes, Record]] = {}
        self._owned: dict[str, bytes] = {}
        self._minted = 0

    def _owner(self, key: str, handle: bytes) -> bytes | None:
        return self._owned.get(key) if self.fence == "by_key" else handle

    async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Claimed:
        held = self.records.get(key)
        if held is not None:
            return held[1]
        self._minted += 1
        owner = f"claim-{self._minted}".encode()
        self.records[key] = (owner, Record.pending(payload_hash))
        self._owned[key] = owner
        return Won(owner)

    async def complete(self, key: str, handle: bytes, record: Record, tx: Any = None) -> None:
        held, owner = self.records.get(key), self._owner(key, handle)
        if held is None or held[0] != owner:
            raise StoreError("the claim expired and was taken over")
        self.records[key] = (held[0], record)

    async def release(self, key: str, handle: bytes, tx: Any = None) -> None:
        held, owner = self.records.get(key), self._owner(key, handle)
        if (
            held is not None
            and held[0] == owner
            and (self.fence != "by_claim" or isinstance(held[1].state, Pending))
        ):
            del self.records[key]


class ClaimIgnoresAmbientTx(Delegating):
    """Joins an explicit ``tx`` but auto-commits when given none, while still declaring
    ``joins_transaction``. A guard with no ``unit_of_work`` claims with no ``tx`` inside the host's own
    transaction, so in production the claim outlives the rollback and the key stays pending for good."""

    def __init__(self, inner: MemoryIdempotencyStore, db: MemoryTransactor) -> None:
        super().__init__(inner)
        self._auto_commit = db.auto_commit()

    async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Claimed:
        return await self._inner.claim(key, payload_hash, tx if tx is not None else self._auto_commit)


class AutoCommitsAForeignTx(Delegating):
    """Declares ``joins_transaction`` but writes a handle it does not know as if none was given: an
    auto-commit. The guard hands it a unit of work of another database; the claim then lands outside the
    guard's transaction, a rollback leaves it pending, and the key is stuck."""

    def __init__(self, inner: MemoryIdempotencyStore, db: MemoryTransactor) -> None:
        super().__init__(inner)
        self._db = db

    def _own(self, tx: Any) -> Any:
        return tx if getattr(tx, "transactor", None) is self._db else self._db.auto_commit()

    async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Claimed:
        return await self._inner.claim(key, payload_hash, self._own(tx))

    async def complete(self, key: str, handle: bytes, record: Record, tx: Any = None) -> None:
        await self._inner.complete(key, handle, record, self._own(tx))

    async def release(self, key: str, handle: bytes, tx: Any = None) -> None:
        await self._inner.release(key, handle, self._own(tx))


class DropsFolded(Delegating):
    """Persists the state and the version and drops ``folded``, a column the schema never got. Every event
    redelivered after its step committed is folded again, and its commands go out under new ids."""

    async def save(
        self, saga: str, id: str, state: Any, version: int, folded: str, commands: Any, tx: Any = None
    ) -> int:
        return await self._inner.save(saga, id, state, version, "", commands, tx)


class SagaSaveIgnoresAmbientTx(Delegating):
    """The saga store's version of the same mistake: the runner's save lands when the unit of work
    that decided it rolls back."""

    def __init__(self, inner: MemorySagaStore, db: MemoryTransactor) -> None:
        super().__init__(inner)
        self._auto_commit = db.auto_commit()

    async def save(
        self, saga: str, id: str, state: Any, version: int, folded: str, commands: Any, tx: Any = None
    ) -> int:
        return await self._inner.save(
            saga, id, state, version, folded, commands, tx if tx is not None else self._auto_commit
        )


class ProjectionSaveIgnoresAmbientTx(Delegating):
    """The projection store's version: a read-model write survives the rollback of the event that
    caused it."""

    def __init__(self, inner: MemoryProjectionStore, db: MemoryTransactor) -> None:
        super().__init__(inner)
        self._auto_commit = db.auto_commit()

    async def save(self, entity: Entity, tx: Any = None) -> int:
        saved: int = await self._inner.save(entity, tx if tx is not None else self._auto_commit)
        return saved


class DropsNext(Delegating):
    """`get_many` returns the rows but never a `next`, as a store written against a list-returning
    seam would. A reader following `next` stops after the first page and silently loses the rest."""

    async def get_many(self, query: Query = Query(), *, tx: Any = None) -> Page:
        page = await self._inner.get_many(query, tx=tx)
        return Page(page.rows, None)


class EndlessNext(Delegating):
    """`get_many` resumes at or before its own last row, so `next` never runs out: the `>=` where the
    keyset needs `>`. The clause must fail it in bounded time rather than page forever."""

    async def get_many(self, query: Query = Query(), *, tx: Any = None) -> Page:
        page = await self._inner.get_many(replace(query, after=None), tx=tx)
        return Page(page.rows, query.after or page.next)


class OwnCursor(Delegating):
    """`get_many` mints its own cursor (an offset) instead of the kernel's. It pages within this store
    but no other service can read it, and it carries no sort, so nothing refuses it under another."""

    async def get_many(self, query: Query = Query(), *, tx: Any = None) -> Page:
        start = query.offset + (int(query.after) if query.after else 0)
        page = await self._inner.get_many(replace(query, offset=start, after=None), tx=tx)
        more = page.next is not None
        return Page(page.rows, str(start + len(page.rows)) if more else None)


class DropsTheRowKey(Delegating):
    """`get_many` returns the stored document alone — `SELECT doc` where the key is its own column.
    Rows then carry whatever `id` a writer happened to save, so a filter or a cursor over `id` reads
    the wrong rows."""

    async def get_many(self, query: Query = Query(), *, tx: Any = None) -> Page:
        page = await self._inner.get_many(query, tx=tx)
        return Page([{k: v for k, v in row.items() if k != "id"} for row in page.rows], page.next)


class CheckThenActEventStore:
    """A check-then-act store: the version check is a read in Python, then a database round trip, then the
    insert, staged in the unit of work and applied on commit. Every clause that runs one writer at a time
    passes it; two writers at version 0 both commit sequence 1, a forked stream."""

    def __init__(self) -> None:
        self.rows: dict[str, list[EventMessage]] = {}

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[list[tuple[str, list[EventMessage]]]]:
        staged: list[tuple[str, list[EventMessage]]] = []
        yield staged  # an exception skips the apply: the rollback
        for agg, events in staged:
            self.rows.setdefault(agg, []).extend(events)

    def _view(self, tx: Any, agg: str) -> list[EventMessage]:
        mine = [e for a, es in (tx or []) if a == agg for e in es]
        return self.rows.get(agg, []) + mine

    async def load(self, tx: Any, aggregate_id: str, after: int = 0) -> list[EventMessage]:
        await asyncio.sleep(0)
        return [e for e in self._view(tx, aggregate_id) if (e.sequence or 0) > after]

    async def append(
        self, tx: Any, aggregate_id: str, expected_version: int, events: Any
    ) -> list[EventMessage]:
        current = self._view(tx, aggregate_id)
        version = current[-1].sequence if current else 0
        if version != expected_version:
            raise ConcurrencyError(f"expected {expected_version}, stored {version}")
        await asyncio.sleep(0)  # the database round trip between the check and the insert
        stored = [recorded(d, _edax.current_timestamp()) for d in events]
        tx.append((aggregate_id, stored))
        return stored


class OverlayDb:
    """A dict database with transactions as overlays, applied on commit, for the read-then-write mistakes
    of the projection and saga seams. ``leaky`` lets every reader see every open transaction's overlay. An
    overlay entry of ``None`` is a staged delete."""

    def __init__(self, leaky: bool = False) -> None:
        self.committed: dict[str, Any] = {}
        self.open: dict[int, dict[str, Any]] = {}
        self.leaky = leaky
        self._minted = 0

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[int]:
        self._minted += 1
        tx = self._minted
        self.open[tx] = {}
        try:
            yield tx
        except BaseException:
            del self.open[tx]
            raise
        for key, held in self.open.pop(tx).items():
            if held is None:
                self.committed.pop(key, None)
            else:
                self.committed[key] = held

    def read(self, tx: Any, key: str) -> Any:
        overlays = list(self.open.values()) if self.leaky else [self.open.get(tx, {})]
        for overlay in overlays:
            if key in overlay:
                return overlay[key]
        return self.committed.get(key)

    async def write(self, tx: Any, key: str, version: int, value: Any) -> int:
        stored = self.read(tx, key)
        current = 0 if stored is None else stored[0]
        if current != version:
            raise ConcurrencyError(f"expected {version}, stored {current}")
        await asyncio.sleep(0)  # the round trip between the read and the write
        target = self.open.get(tx, self.committed)
        target[key] = (version + 1, value)
        return version + 1


class CheckThenActProjectionStore:
    def __init__(self, db: OverlayDb) -> None:
        self.db = db
        self.transactor = db

    async def get(self, id: str, tx: Any = None) -> Entity | None:
        row = self.db.read(tx if tx is not None else current_tx(self.db), id)
        return None if row is None else Entity(id, row[0], row[1])

    async def save(self, entity: Entity, tx: Any = None) -> int:
        tx = tx if tx is not None else current_tx(self.db)
        return await self.db.write(tx, entity.id, entity.version, entity.document)


class CheckThenActSagaStore:
    def __init__(self, db: OverlayDb) -> None:
        self.db = db
        self.transactor = db
        self.outbox: list[Any] = []

    async def load(self, saga: str, id: str, tx: Any = None) -> SagaInstance | None:
        row = self.db.read(tx if tx is not None else current_tx(self.db), f"{saga}/{id}")
        return None if row is None else RunningInstance(row[1], row[0])

    async def save(
        self, saga: str, id: str, state: Any, version: int, folded: str, commands: Any, tx: Any = None
    ) -> int:
        tx = tx if tx is not None else current_tx(self.db)
        new = await self.db.write(tx, f"{saga}/{id}", version, state)
        self.outbox.extend(commands)  # enqueued at once: the read-then-write's second half
        return new

    async def unpublished(self, saga: str, limit: int) -> list[Any]:
        return self.outbox[:limit]


class LeakyEventStore(Delegating):
    """``load`` reads every open transaction's staged appends, as a ``READ UNCOMMITTED`` connection does: a
    projector then folds an event that may still roll back."""

    def __init__(self, inner: MemoryEventStore) -> None:
        super().__init__(inner)
        self._staged: list[EventMessage] = []

    async def append(self, tx: Any, aggregate_id: str, expected: int, events: Any) -> list[EventMessage]:
        stored = await self._inner.append(tx, aggregate_id, expected, events)
        self._staged.extend(stored)
        return stored

    async def load(self, tx: Any, aggregate_id: str, after: int = 0) -> Any:
        rows = await self._inner.load(tx, aggregate_id, after)
        seen = {e.id for e in rows}
        return rows + [e for e in self._staged if e.aggregate_id == aggregate_id and e.id not in seen]


class OverlayLedger(OverlayDb):
    """A joining ledger over transaction overlays, with the two mistakes a hand-rolled one makes as
    switches. ``stale_reads``: a claimant reads only the committed rows and its own, so it wins a key
    another open transaction claimed (a snapshot read with no row lock). ``release_shadows``: a release
    stages a delete even with nothing committed, so the releaser's re-claim
    reads its own delete instead of a concurrent winner's record, and its commit erases that record. With
    both off the overlay is correct for the release clause, the positive control."""

    joins_transaction = True

    def __init__(self, *, release_shadows: bool) -> None:
        super().__init__()
        self.release_shadows = release_shadows

    @property
    def transactor(self) -> OverlayLedger:
        return self

    def _overlay(self, tx: Any) -> dict[str, tuple[bytes, Record] | None]:
        return self.open.get(tx if tx is not None else current_tx(self), {})

    def _held(self, tx: Any, key: str) -> tuple[bytes, Record] | None:
        held: tuple[bytes, Record] | None = self.read(tx if tx is not None else current_tx(self), key)
        return held

    async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Claimed:
        held = self._held(tx, key)
        if held is not None:
            return held[1]
        self._minted += 1
        handle = f"claim-{self._minted}".encode()
        self._overlay(tx)[key] = (handle, Record.pending(payload_hash))
        return Won(handle)

    async def complete(self, key: str, handle: bytes, record: Record, tx: Any = None) -> None:
        held = self._held(tx, key)
        if held is None or held[0] != handle:
            raise StoreError("the claim is not this handle's")
        self._overlay(tx)[key] = (handle, record)

    async def release(self, key: str, handle: bytes, tx: Any = None) -> None:
        held = self._held(tx, key)
        if held is None or held[0] != handle:
            return
        overlay = self._overlay(tx)
        if self.release_shadows or key in self.committed:
            overlay[key] = None
        else:
            overlay.pop(key, None)


class RowLocks:
    """The row lock a SQL database takes on a written key: held by the writing unit of work until it ends,
    so a second writer of the key *waits* for it instead of failing at once. This is what makes the
    contract's race clauses run their bounded-wait path, the one a blocking SQL store takes."""

    def __init__(self) -> None:
        self._held: dict[str, int] = {}
        self._freed = asyncio.Condition()

    async def lock(self, tx: int, key: str) -> None:
        async with self._freed:
            await self._freed.wait_for(lambda: self._held.get(key, tx) == tx)
            self._held[key] = tx

    async def end(self, tx: int) -> None:
        async with self._freed:
            for key in [k for k, owner in self._held.items() if owner == tx]:
                del self._held[key]
            self._freed.notify_all()


class LockingEventStore:
    """An event store with row locks: a second appender of a stream waits for the first to end, then checks
    the committed version and conflicts. Correct, and blocking: the race clause must pass it by its bounded
    wait, not deadlock on it. ``victim`` makes the first unit of work the one the database aborts at
    commit, as a deadlock detector may."""

    def __init__(self, *, victim: bool = False) -> None:
        self.rows: dict[str, list[EventMessage]] = {}
        self.locks = RowLocks()
        self.victim = victim
        self._minted = 0

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[tuple[int, list[tuple[str, list[EventMessage]]]]]:
        self._minted += 1
        tx = self._minted
        staged: list[tuple[str, list[EventMessage]]] = []
        try:
            yield tx, staged
            if self.victim and tx == 1:
                raise ConcurrencyError("chosen as the deadlock victim at commit")
            for agg, events in staged:
                self.rows.setdefault(agg, []).extend(events)
        finally:
            await self.locks.end(tx)

    async def load(self, tx: Any, aggregate_id: str, after: int = 0) -> list[EventMessage]:
        mine = [e for a, es in tx[1] if a == aggregate_id for e in es] if tx else []
        return [e for e in self.rows.get(aggregate_id, []) + mine if (e.sequence or 0) > after]

    async def append(
        self, tx: Any, aggregate_id: str, expected_version: int, events: Any
    ) -> list[EventMessage]:
        await self.locks.lock(tx[0], aggregate_id)
        current = await self.load(tx, aggregate_id)
        version = current[-1].sequence if current else 0
        if version != expected_version:
            raise ConcurrencyError(f"expected {expected_version}, stored {version}")
        stored = [recorded(d, _edax.current_timestamp()) for d in events]
        tx[1].append((aggregate_id, stored))
        return stored


class LockingLedger(OverlayLedger):
    """The overlay ledger with row locks: a claimant of a key another open unit of work claimed waits for it
    to end, then reads what it committed. Correct, and blocking. ``timeout`` makes the wait end in the
    contract's other allowed answer, a ``StoreError`` (a lock timeout). ``deferred`` is the mistake of a
    unique check deferred to the commit: a claimant of a held key wins at once, and only its commit waits
    for the holder. The guard runs the handler before that commit, so the handler runs twice."""

    def __init__(self, *, timeout: bool = False, deferred: bool = False) -> None:
        super().__init__(release_shadows=False)
        self.locks = RowLocks()
        self.timeout = timeout
        self.deferred = deferred

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[int]:
        opened: list[int] = []
        try:
            async with super().begin() as tx:
                opened.append(tx)
                yield tx
                if self.deferred:  # the commit checks every key it staged
                    for key in list(self.open[tx]):
                        await self._lock(tx, key)
        finally:
            # Released once the overlay settled: after the commit applied, or the rollback dropped it.
            for tx in opened:
                await self.locks.end(tx)

    async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Claimed:
        tx = tx if tx is not None else current_tx(self)
        held_elsewhere = any(key in overlay for other, overlay in self.open.items() if other != tx)
        if held_elsewhere and self.timeout:
            raise StoreError("lock wait timeout exceeded")
        if not (held_elsewhere and self.deferred):
            await self._lock(tx, key)
        return await super().claim(key, payload_hash, tx)

    async def complete(self, key: str, handle: bytes, record: Record, tx: Any = None) -> None:
        tx = tx if tx is not None else current_tx(self)
        await self._lock(tx, key)
        await super().complete(key, handle, record, tx)

    async def release(self, key: str, handle: bytes, tx: Any = None) -> None:
        tx = tx if tx is not None else current_tx(self)
        await self._lock(tx, key)
        await super().release(key, handle, tx)

    async def _lock(self, tx: Any, key: str) -> None:
        if isinstance(tx, int):  # outside a unit of work a write takes no lock
            await self.locks.lock(tx, key)


def fixtures(**values: Any) -> Any:
    """A stand-in for pytest's ``request``: a clause resolves its optional fixtures lazily."""
    return SimpleNamespace(getfixturevalue=lambda name: values[name])


class _ExpiringContract(IdempotencyStoreContract):
    """The takeover clauses run only for a store whose claims expire."""

    claims_expire = True

    async def expire_claim(self, idempotency_store: Any, key: str) -> None:
        del idempotency_store.records[key]


class _RawContract(IdempotencyStoreContract):
    """The unreadable-record clause runs only for a store that persists the kernel's record JSON."""

    stores_raw_records = True

    async def put_raw_record(self, idempotency_store: Any, key: str, raw: str) -> None:
        getattr(idempotency_store, "_inner", idempotency_store).put_raw(key, raw)


#: What a broken store's failure may look like: a clause's own assertion, a ``pytest.raises`` that did not
#: fire, or a seam's typed error the clause did not expect. A ``TypeError`` or an ``AttributeError`` is a
#: broken rig, not a caught store, so it does not count.
CAUGHT: tuple[type[BaseException], ...] = (AssertionError, pytest.fail.Exception, EdaxError)


async def _fails(coro: Any) -> BaseException:
    """The contract method must fail, with one of :data:`CAUGHT`; returns the failure."""
    try:
        await coro
    except CAUGHT as caught:
        return caught
    raise AssertionError("the contract passed a store that breaks its clause")


@dataclass(frozen=True)
class Broken:
    """One plausible mistake: the store that makes it, and the clause that must catch it."""

    name: str
    contract: Callable[[], Any]
    clause: str
    #: Builds the broken store over a fresh memory database.
    store: Callable[[MemoryTransactor], Any]
    #: The optional fixtures the clause resolves, built over the same database and store.
    fixtures: Callable[[MemoryTransactor, Any], dict[str, Any]] = lambda db, store: {}
    #: Extra positional arguments, for a parametrized clause.
    args: tuple[Any, ...] = ()
    #: A fragment of the failure the clause must report, so a catch for another reason does not count.
    expect: str = ""


BROKEN: list[Broken] = [
    Broken(
        "inclusive_load",
        EventStoreContract,
        "test_load_after_skips_what_the_caller_already_has",
        lambda db: InclusiveLoad(MemoryEventStore(db)),
    ),
    Broken(
        "outbox_leaks_uncommitted_rows",
        EventOutboxContract,
        "test_a_rolled_back_row_never_shows",
        lambda db: LeakyOutbox(MemoryEventStore(db), db),
        lambda db, store: {"event_store": store},
    ),
    Broken(
        "backlog_count_ignores_mark_published",
        EventOutboxContract,
        "test_unpublished_count_tracks_what_is_waiting",
        lambda db: CountsPublishedRows(MemoryEventStore(db)),
        lambda db, store: {"event_store": store},
    ),
    Broken(
        "event_outbox_pages_newest_first",
        EventOutboxContract,
        "test_unpublished_is_oldest_commit_first",
        lambda db: OutboxNewestFirst(MemoryEventStore(db)),
        lambda db, store: {"event_store": MemoryEventStore(db)},
    ),
    Broken(
        "projections_share_one_namespace",
        ProjectionStoreContract,
        "test_two_read_models_share_no_rows",
        lambda db: MemoryProjectionStore(db, projection="read_model"),
        lambda db, store: {"projection_store_named": lambda _name: store},
        expect="the other read model's id is absent",
    ),
    Broken(
        "drops_next",
        ProjectionStoreContract,
        "test_get_many_pages_every_row_once_by_its_next_cursor",
        lambda db: DropsNext(MemoryProjectionStore(db, projection="read_model")),
    ),
    Broken(
        "endless_next",
        ProjectionStoreContract,
        "test_get_many_pages_every_row_once_by_its_next_cursor",
        lambda db: EndlessNext(MemoryProjectionStore(db, projection="read_model")),
    ),
    Broken(
        "row_key_is_not_the_id",
        ProjectionStoreContract,
        "test_get_many_returns_the_row_key_as_the_id",
        lambda db: DropsTheRowKey(MemoryProjectionStore(db, projection="read_model")),
        expect="the id is the row key",
    ),
    Broken(
        "own_cursor_is_not_the_kernels",
        ProjectionStoreContract,
        "test_next_is_the_kernels_cursor",
        lambda db: OwnCursor(MemoryProjectionStore(db, projection="read_model")),
    ),
    Broken(
        "own_cursor_ignores_the_sort",
        ProjectionStoreContract,
        "test_a_cursor_minted_under_another_sort_is_refused",
        lambda db: OwnCursor(MemoryProjectionStore(db, projection="read_model")),
    ),
    Broken(
        "save_returns_the_old_version",
        ProjectionStoreContract,
        "test_save_returns_the_new_version_and_get_reads_the_keyed_row",
        lambda db: ReturnsTheOldVersion(MemoryProjectionStore(db, projection="read_model")),
        expect="the new version",
    ),
    Broken(
        "get_drops_the_row_key",
        ProjectionStoreContract,
        "test_save_returns_the_new_version_and_get_reads_the_keyed_row",
        lambda db: GetDropsTheRowKey(MemoryProjectionStore(db, projection="read_model")),
        expect="keyed by the row",
    ),
    Broken(
        "projection_wraps_any_document",
        ProjectionStoreContract,
        "test_a_refused_save_writes_nothing",
        lambda db: WrapsAnyDocument(MemoryProjectionStore(db, projection="read_model")),
        args=("a_list",),
    ),
    Broken(
        "ledger_ignores_a_pending_record",
        IdempotencyStoreContract,
        "test_a_pending_record_cannot_settle_a_claim",
        lambda db: IgnoresAPendingRecord(MemoryIdempotencyStore(db)),
    ),
    Broken(
        "ledger_wins_twice",
        IdempotencyStoreContract,
        "test_a_second_claim_loses_and_reads_the_winners_record",
        lambda db: AlwaysWins(MemoryIdempotencyStore(db)),
    ),
    Broken(
        "signed_hash_column",
        IdempotencyStoreContract,
        "test_the_record_keeps_the_payload_hash_it_was_given",
        lambda db: SignedHash(MemoryIdempotencyStore(db)),
        expect="stored losslessly",
    ),
    Broken(
        "snapshot_leads_the_stream",
        EventStoreContract,
        "test_the_latest_snapshot_is_kept_and_never_leads_the_stream",
        lambda db: SnapshotsWithoutChecks(MemoryEventStore(db)),
        args=("leads_the_stream",),
    ),
    Broken(
        "older_snapshot_replaces_newer",
        EventStoreContract,
        "test_the_latest_snapshot_is_kept_and_never_leads_the_stream",
        lambda db: SnapshotsWithoutChecks(MemoryEventStore(db)),
        args=("an_older_save_never_rolls_back",),
    ),
    Broken(
        "outbox_keyed_by_aggregate_and_id",
        EventStoreContract,
        "test_an_event_id_already_stored_is_refused",
        lambda db: KeysRowsByAggregateAndId(MemoryEventStore(db)),
    ),
    Broken(
        "saga_deletes_on_completion_tombstone",
        SagaStoreContract,
        "test_complete_writes_a_versioned_tombstone",
        lambda db: DeletesOnComplete(),
    ),
    Broken(
        "saga_deletes_on_completion_takes_writes",
        SagaStoreContract,
        "test_a_completed_instance_takes_no_write",
        lambda db: DeletesOnComplete(),
    ),
    Broken(
        "saga_sends_before_the_write",
        SagaStoreContract,
        "test_the_steps_commands_commit_with_its_state",
        lambda db: SendsBeforeTheWrite(MemorySagaStore(db)),
    ),
    Broken(
        "saga_sends_before_the_rolled_back_write",
        SagaStoreContract,
        "test_a_rolled_back_step_leaves_no_command",
        lambda db: SendsBeforeTheWrite(MemorySagaStore(db)),
        lambda db, store: {"transactor": db},
    ),
    Broken(
        "saga_keyed_by_the_instance_alone",
        SagaStoreContract,
        "test_two_sagas_in_one_database_share_neither_an_instance_nor_a_command",
        lambda db: KeysSagasByInstance(MemorySagaStore(db)),
    ),
    Broken(
        "saga_outbox_marked_by_instance",
        SagaOutboxContract,
        "test_mark_published_is_idempotent_and_ignores_unknown_ids",
        lambda db: SagaOutbox(cast("Any", MarksTheWholeInstance(MemorySagaStore(db))), "order"),
        lambda db, store: {"saga_store": MemorySagaStore(db)},
    ),
    Broken(
        "drops_metadata",
        EventStoreContract,
        "test_every_draft_field_survives_the_round_trip",
        lambda db: DropsMetadata(MemoryEventStore(db)),
    ),
    Broken(
        "restamps_recorded_at_on_read",
        EventStoreContract,
        "test_every_draft_field_survives_the_round_trip",
        lambda db: RestampsOnRead(MemoryEventStore(db)),
    ),
    Broken(
        "empty_append_skips_the_version_check",
        EventStoreContract,
        "test_append_is_compare_and_set_on_a_dense_batch",
        lambda db: ShortcutsEmptyAppend(MemoryEventStore(db)),
        args=("empty_and_ahead",),
    ),
    Broken(
        "append_writes_then_checks",
        EventStoreContract,
        "test_append_is_compare_and_set_on_a_dense_batch",
        lambda db: WritesThenChecks(MemoryEventStore(db)),
        args=("stale_batch",),
    ),
    Broken(
        "renumbers_a_gapped_batch",
        EventStoreContract,
        "test_append_is_compare_and_set_on_a_dense_batch",
        lambda db: RenumbersTheBatch(MemoryEventStore(db)),
        args=("gap",),
    ),
    Broken(
        "renumbers_an_unsequenced_batch",
        EventStoreContract,
        "test_append_is_compare_and_set_on_a_dense_batch",
        lambda db: RenumbersTheBatch(MemoryEventStore(db)),
        args=("unsequenced",),
    ),
    Broken(
        "ledger_drops_the_error_code",
        IdempotencyStoreContract,
        "test_a_settled_record_replays_instead_of_a_rerun",
        lambda db: DropsTheErrorCode(MemoryIdempotencyStore(db)),
        args=("failed_with_its_code",),
    ),
    Broken(
        "ledger_drops_the_error_reason",
        IdempotencyStoreContract,
        "test_a_settled_record_replays_instead_of_a_rerun",
        lambda db: DropsTheErrorReason(MemoryIdempotencyStore(db)),
        args=("failed_with_its_reason",),
    ),
    Broken(
        "ledger_raises_on_an_unreadable_row",
        _RawContract,
        "test_an_unreadable_record_replays_as_a_cached_codec_failure",
        lambda db: DecodesStrictly(MemoryIdempotencyStore(db)),
        args=("garbage",),
        expect="expected value at line 1 column 1",
    ),
    Broken(
        "ledger_reads_an_unreadable_row_as_free",
        _RawContract,
        "test_an_unreadable_record_replays_as_a_cached_codec_failure",
        lambda db: ReadsAnUnreadableRowAsFree(MemoryIdempotencyStore(db)),
        args=("garbage",),
        expect="want the kernel's cached codec failure, got Won",
    ),
    Broken(
        "none_result_left_pending",
        IdempotencyStoreContract,
        "test_a_settled_record_replays_instead_of_a_rerun",
        lambda db: NoneResultReadsAsPending(MemoryIdempotencyStore(db)),
        args=("done_with_none",),
    ),
    Broken(
        "claim_ignores_the_ambient_tx",
        IdempotencyStoreContract,
        "test_a_claim_with_no_tx_joins_the_ambient_transaction",
        lambda db: ClaimIgnoresAmbientTx(MemoryIdempotencyStore(db), db),
        lambda db, store: {"transactor": db},
    ),
    Broken(
        "auto_commits_a_foreign_tx",
        IdempotencyStoreContract,
        "test_a_handle_of_another_database_is_refused",
        lambda db: AutoCommitsAForeignTx(MemoryIdempotencyStore(db), db),
    ),
    Broken(
        "settles_over_a_held_key",
        IdempotencyStoreContract,
        "test_a_key_another_open_transaction_holds_is_never_settled_over_it",
        lambda db: OverlayLedger(release_shadows=False),
        lambda db, store: {"transactor": store},
        args=("complete",),
        expect="landed on a key another open transaction held",
    ),
    Broken(
        "refuses_a_held_key_as_a_store_error",
        IdempotencyStoreContract,
        "test_a_key_another_open_transaction_holds_is_never_settled_over_it",
        lambda db: RefusesAHeldKeyAsAStoreError(MemoryIdempotencyStore(db)),
        lambda db, store: {"transactor": db},
        args=("complete",),
        expect="lock wait timeout",
    ),
    Broken(
        "saga_drops_the_folded_marks",
        SagaStoreContract,
        "test_the_folded_marks_round_trip",
        lambda db: DropsFolded(MemorySagaStore(db)),
        expect="the marks come back as they were saved",
    ),
    Broken(
        "saga_save_ignores_the_ambient_tx",
        SagaStoreContract,
        "test_a_save_with_no_tx_joins_the_ambient_transaction",
        lambda db: SagaSaveIgnoresAmbientTx(MemorySagaStore(db), db),
        lambda db, store: {"transactor": db},
    ),
    Broken(
        "projection_save_ignores_the_ambient_tx",
        ProjectionStoreContract,
        "test_a_save_with_no_tx_joins_the_ambient_transaction",
        lambda db: ProjectionSaveIgnoresAmbientTx(MemoryProjectionStore(db, projection="read_model"), db),
        lambda db, store: {"transactor": db},
    ),
    Broken(
        "ledger_reads_then_writes",
        IdempotencyStoreContract,
        "test_concurrent_claims_have_exactly_one_winner",
        lambda db: ReadsThenWrites(),
    ),
    Broken(
        "fence_by_key",
        _ExpiringContract,
        "test_a_lapsed_claim_settles_nothing_for_its_old_owner",
        lambda db: ExpiringLedger("by_key"),
    ),
    Broken(
        "fence_by_owner",
        _ExpiringContract,
        "test_a_release_after_complete_keeps_the_record",
        lambda db: ExpiringLedger("by_owner"),
    ),
    # The race and isolation clauses: each store passes every one-call-at-a-time clause.
    Broken(
        "event_store_checks_then_acts",
        EventStoreContract,
        "test_two_writers_at_one_version_land_exactly_one_append",
        lambda db: CheckThenActEventStore(),
        expect="exactly one writer lands",
    ),
    Broken(
        "projection_store_checks_then_acts",
        ProjectionStoreContract,
        "test_two_writers_at_one_version_land_exactly_one_save",
        lambda db: CheckThenActProjectionStore(OverlayDb()),
        lambda db, store: {"transactor": store.db},
        expect="exactly one writer lands",
    ),
    Broken(
        "saga_store_checks_then_acts",
        SagaStoreContract,
        "test_two_steps_at_one_version_land_exactly_one_write",
        lambda db: CheckThenActSagaStore(OverlayDb()),
        lambda db, store: {"transactor": store.db},
        expect="exactly one step lands",
    ),
    Broken(
        "event_store_reads_uncommitted",
        EventStoreContract,
        "test_an_uncommitted_append_is_invisible_to_another_transaction",
        lambda db: LeakyEventStore(MemoryEventStore(db)),
        expect="saw a staged append",
    ),
    Broken(
        "projection_store_reads_uncommitted",
        ProjectionStoreContract,
        "test_an_uncommitted_save_is_invisible_to_another_transaction",
        lambda db: CheckThenActProjectionStore(OverlayDb(leaky=True)),
        lambda db, store: {"transactor": store.db},
        expect="saw a staged row",
    ),
    Broken(
        "saga_store_reads_uncommitted",
        SagaStoreContract,
        "test_an_uncommitted_step_is_invisible_to_another_transaction",
        lambda db: CheckThenActSagaStore(OverlayDb(leaky=True)),
        lambda db, store: {"transactor": store.db},
        expect="saw a staged step",
    ),
    Broken(
        "ledger_wins_a_key_another_open_transaction_holds",
        IdempotencyStoreContract,
        "test_a_claim_staged_in_an_open_transaction_is_never_won_by_another",
        lambda db: OverlayLedger(release_shadows=False),
        lambda db, store: {"transactor": store},
        expect="won a key another open transaction held",
    ),
    Broken(
        # The commit waits for the holder, so the contender's unit of work ends only after the rollback:
        # the clause must judge when the claim answered, not when its unit of work ended.
        "ledger_wins_a_held_key_at_once_and_defers_its_check_to_the_commit",
        IdempotencyStoreContract,
        "test_a_claim_staged_in_an_open_transaction_is_never_won_by_another",
        lambda db: LockingLedger(deferred=True),
        lambda db, store: {"transactor": store},
        expect="won a key another open transaction held",
    ),
    Broken(
        "release_shadows_a_concurrent_winner",
        IdempotencyStoreContract,
        "test_a_release_with_nothing_committed_leaves_a_concurrent_winners_record",
        lambda db: OverlayLedger(release_shadows=True),
        lambda db, store: {"transactor": store},
        expect="must replay the winner",
    ),
    # Tenancy: each store passes every clause that sends no tenant or keys by the bare id.
    Broken(
        "event_store_keys_by_the_draft",
        EventStoreContract,
        "test_the_stream_is_named_by_the_argument_not_the_draft",
        lambda db: KeysByTheDraft(MemoryEventStore(db)),
        expect="under the key",
    ),
    Broken(
        "saga_outbox_drops_the_tenant",
        SagaStoreContract,
        "test_the_steps_commands_commit_with_its_state",
        lambda db: SagaDropsTheTenant(MemorySagaStore(db)),
        expect="its tenant",
    ),
    # The command outbox.
    Broken(
        "command_outbox_drops_the_tenant",
        CommandOutboxContract,
        "test_enqueued_commands_commit_and_roll_back_with_the_events",
        lambda db: OutboxDropsTheTenant(MemoryCommandOutbox(db)),
        lambda db, store: {"event_store": MemoryEventStore(db), "outbox": store},
        expect="the tenant included",
    ),
    Broken(
        "command_outbox_sends_a_rolled_back_command",
        CommandOutboxContract,
        "test_enqueued_commands_commit_and_roll_back_with_the_events",
        lambda db: OutboxIgnoresTheTx(MemoryCommandOutbox(db), db),
        lambda db, store: {"event_store": MemoryEventStore(db), "outbox": store},
        expect="a staged command is not one to send",
    ),
    Broken(
        "command_outbox_writes_under_a_foreign_tx",
        CommandOutboxContract,
        "test_a_handle_of_another_database_is_refused",
        lambda db: OutboxIgnoresTheTx(MemoryCommandOutbox(db), db),
        lambda db, store: {"outbox": store},
        expect="BadHandle",
    ),
    Broken(
        "command_outbox_upserts_a_repeated_id",
        CommandOutboxContract,
        "test_an_id_already_waiting_is_refused",
        lambda db: OutboxUpserts(MemoryCommandOutbox(db)),
        lambda db, store: {"event_store": MemoryEventStore(db), "outbox": store},
        expect="CodecError",
    ),
    Broken(
        "command_outbox_pages_newest_first",
        CommandOutboxContract,
        "test_unpublished_is_oldest_commit_first",
        lambda db: OutboxNewestFirst(MemoryCommandOutbox(db)),
        lambda db, store: {"event_store": MemoryEventStore(db), "command_outbox": store},
    ),
    Broken(
        "command_outbox_refuses_an_unknown_id",
        CommandOutboxContract,
        "test_mark_published_is_idempotent_and_ignores_unknown_ids",
        lambda db: OutboxRefusesUnknownIds(MemoryCommandOutbox(db)),
        lambda db, store: {"event_store": MemoryEventStore(db), "command_outbox": store},
        expect="unknown command ids",
    ),
]


@pytest.mark.parametrize("case", BROKEN, ids=lambda c: c.name)
async def test_the_contract_fails_a_store_that_breaks_its_clause(case: Broken) -> None:
    db = MemoryTransactor()
    store = case.store(db)
    clause = getattr(case.contract(), case.clause)
    takes_request = "request" in inspect.signature(clause).parameters
    request = (fixtures(**case.fixtures(db, store)),) if takes_request else ()
    caught = await _fails(clause(store, *request, *case.args))
    assert case.expect in str(caught), f"caught for another reason: {caught!r}"


@pytest.mark.parametrize(
    "contract, clause, store, needs, args",
    [
        (
            EventStoreContract,
            "test_two_writers_at_one_version_land_exactly_one_append",
            lambda: MemoryEventStore(),
            lambda store: {},
            (),
        ),
        (
            IdempotencyStoreContract,
            "test_a_release_with_nothing_committed_leaves_a_concurrent_winners_record",
            lambda: OverlayLedger(release_shadows=False),
            lambda store: {"transactor": store},
            (),
        ),
        (
            _ExpiringContract,
            "test_a_lapsed_claim_settles_nothing_for_its_old_owner",
            lambda: ExpiringLedger("by_claim"),
            lambda store: {},
            (),
        ),
        (
            _ExpiringContract,
            "test_a_release_after_complete_keeps_the_record",
            lambda: ExpiringLedger("by_claim"),
            lambda store: {},
            (),
        ),
        (
            EventStoreContract,
            "test_two_writers_at_one_version_land_exactly_one_append",
            lambda: LockingEventStore(),
            lambda store: {},
            (),
        ),
        (
            EventStoreContract,
            "test_two_writers_at_one_version_land_exactly_one_append",
            lambda: LockingEventStore(victim=True),
            lambda store: {},
            (),
        ),
        (
            IdempotencyStoreContract,
            "test_a_claim_staged_in_an_open_transaction_is_never_won_by_another",
            lambda: LockingLedger(),
            lambda store: {"transactor": store},
            (),
        ),
        (
            IdempotencyStoreContract,
            "test_a_claim_staged_in_an_open_transaction_is_never_won_by_another",
            lambda: LockingLedger(timeout=True),
            lambda store: {"transactor": store},
            (),
        ),
        (
            IdempotencyStoreContract,
            "test_a_release_with_nothing_committed_leaves_a_concurrent_winners_record",
            lambda: LockingLedger(),
            lambda store: {"transactor": store},
            (),
        ),
        (
            IdempotencyStoreContract,
            "test_a_key_another_open_transaction_holds_is_never_settled_over_it",
            lambda: LockingLedger(),
            lambda store: {"transactor": store},
            ("complete",),
        ),
        (
            _RawContract,
            "test_an_unreadable_record_replays_as_a_cached_codec_failure",
            lambda: MemoryIdempotencyStore(),
            lambda store: {},
            ("torn_write",),
        ),
    ],
    ids=[
        "memory_event_store_races",
        "overlay_ledger_releases",
        "fence_by_claim_takeover",
        "fence_by_claim_release",
        "a_row_locked_event_store_races_by_the_bounded_wait",
        "a_deadlock_victim_first_writer_still_lands_one",
        "a_row_locked_ledger_parks_the_contender_until_the_rollback",
        "a_ledger_whose_contender_times_out",
        "a_row_locked_ledger_releases_with_the_winner_parked",
        "a_row_locked_ledger_parks_the_settler_until_the_holder_ends",
        "a_ledger_that_reads_with_decode_prior",
    ],
)
async def test_the_same_clause_passes_the_store_without_the_mistake(
    contract: Callable[[], Any],
    clause: str,
    store: Callable[[], Any],
    needs: Callable[[Any], dict[str, Any]],
    args: tuple[Any, ...],
) -> None:
    """The positive controls: the same clause over the same rig without the mistake passes, so each catch
    above is the mistake's doing and not the rig's."""
    instance = store()
    method = getattr(contract(), clause)
    takes_request = "request" in inspect.signature(method).parameters
    await method(instance, *((fixtures(**needs(instance)),) if takes_request else ()), *args)


async def test_a_rig_error_is_not_a_catch() -> None:
    """``_fails`` counts only a clause's verdict: a store that blows up for an unrelated reason (a typo, a
    missing method) is a broken rig, and passing it as "caught" would hide that the clause never ran."""

    async def rig_bug() -> None:
        raise TypeError("the rig called a method with the wrong arguments")

    with pytest.raises(TypeError):
        await _fails(rig_bug())
    caught = await _fails(_assertion())
    assert isinstance(caught, AssertionError)


async def _assertion() -> None:
    raise AssertionError("a clause's own verdict")


async def test_the_paging_clauses_skip_a_store_that_is_no_projection_query() -> None:
    """A store with ``get`` and ``save`` only: the suite reads that it pages nothing by the kernel's query
    and skips the paging clauses, as it skips the count clause of an outbox that counts nothing."""
    store = CheckThenActProjectionStore(OverlayDb())
    assert not isinstance(store, ProjectionQuery)
    for clause in (
        ProjectionStoreContract().test_get_many_pages_every_row_once_by_its_next_cursor,
        ProjectionStoreContract().test_next_is_the_kernels_cursor,
        ProjectionStoreContract().test_a_cursor_minted_under_another_sort_is_refused,
    ):
        with pytest.raises(pytest.skip.Exception):
            await clause(store)


async def test_the_explicit_tx_clause_alone_passes_a_store_that_ignores_the_ambient_transaction() -> None:
    """The gap the ambient clause closes: the explicit clause passes this store, so only the new
    clause tells it apart from a correct one."""
    db = MemoryTransactor()
    # A delegating wrapper reaches the seam's other methods through `__getattr__`, which no checker reads.
    store: Any = ClaimIgnoresAmbientTx(MemoryIdempotencyStore(db), db)
    await IdempotencyStoreContract().test_a_rollback_frees_the_claim(store, fixtures(transactor=db))


async def test_the_transaction_clauses_skip_a_backend_with_no_unit_of_work() -> None:
    """A backend that declares ``supports_transactions = False`` has no unit of work to join, race in or
    isolate, so those clauses skip rather than demand a ``transactor`` fixture it cannot supply."""
    db = MemoryTransactor()
    projections, sagas = ProjectionStoreContract(), SagaStoreContract()
    projections.supports_transactions = sagas.supports_transactions = False
    no_fixtures = fixtures()
    projection_clauses = (
        projections.test_a_save_with_no_tx_joins_the_ambient_transaction,
        projections.test_get_sees_this_transactions_own_save,
        projections.test_two_writers_at_one_version_land_exactly_one_save,
        projections.test_an_uncommitted_save_is_invisible_to_another_transaction,
    )
    saga_clauses = (
        sagas.test_a_save_with_no_tx_joins_the_ambient_transaction,
        sagas.test_a_rolled_back_step_leaves_no_command,
        sagas.test_two_steps_at_one_version_land_exactly_one_write,
        sagas.test_an_uncommitted_step_is_invisible_to_another_transaction,
    )
    for clause in projection_clauses:
        with pytest.raises(pytest.skip.Exception):
            await clause(MemoryProjectionStore(db, projection="read_model"), no_fixtures)
    for clause in saga_clauses:
        with pytest.raises(pytest.skip.Exception):
            await clause(MemorySagaStore(db), no_fixtures)


async def test_the_shared_rollback_clauses_skip_a_ledger_of_its_own() -> None:
    contract, ledger, no_fixtures = IdempotencyStoreContract(), MemoryLedger(), fixtures()
    for clause in (
        contract.test_a_rollback_frees_the_claim,
        contract.test_a_claim_with_no_tx_joins_the_ambient_transaction,
        contract.test_a_claim_staged_in_an_open_transaction_is_never_won_by_another,
        contract.test_a_release_with_nothing_committed_leaves_a_concurrent_winners_record,
    ):
        with pytest.raises(pytest.skip.Exception):
            await clause(ledger, no_fixtures)


async def test_the_snapshot_clauses_skip_a_backend_without_snapshots() -> None:
    events = EventStoreContract()
    events.supports_snapshots = False
    with pytest.raises(pytest.skip.Exception):
        await events.test_the_latest_snapshot_is_kept_and_never_leads_the_stream(
            MemoryEventStore(MemoryTransactor()), "lags_the_stream"
        )


class _Uncounted:
    """An outbox that pages and marks but keeps no count, as one fed to change data capture may."""

    def __init__(self, inner: SagaOutbox) -> None:
        self._inner = inner

    async def unpublished(self, limit: int) -> list[CommandMessage]:
        return await self._inner.unpublished(limit)

    async def mark_published(self, ids: Sequence[str]) -> None:
        await self._inner.mark_published(ids)


async def test_the_count_clause_skips_an_outbox_that_does_not_count() -> None:
    """A store fed to change data capture, or one that cannot count cheaply, has no ``unpublished_count``:
    the clause skips rather than fails it."""
    store = MemorySagaStore(MemoryTransactor())
    with pytest.raises(pytest.skip.Exception):
        await SagaOutboxContract().test_unpublished_count_tracks_what_is_waiting(
            _Uncounted(SagaOutbox(store, "order")), fixtures(saga_store=store)
        )


async def test_the_rollback_clauses_skip_a_backend_without_rollback() -> None:
    events, outbox = EventStoreContract(), EventOutboxContract()
    events.supports_transactions = outbox.supports_transactions = False
    store = MemoryEventStore(MemoryTransactor())
    with pytest.raises(pytest.skip.Exception):
        await events.test_a_rollback_removes_the_appended_events(store)
    with pytest.raises(pytest.skip.Exception):
        await outbox.test_a_rolled_back_row_never_shows(store, fixtures(event_store=store))


async def test_the_takeover_clauses_skip_a_store_whose_claims_never_expire() -> None:
    contract = IdempotencyStoreContract()
    for clause in (
        contract.test_a_lapsed_claim_settles_nothing_for_its_old_owner,
        contract.test_a_release_after_complete_keeps_the_record,
        contract.test_a_lapsed_record_frees_the_key,
    ):
        with pytest.raises(pytest.skip.Exception):
            await clause(MemoryIdempotencyStore())
    with pytest.raises(NotImplementedError):
        await contract.expire_claim(MemoryIdempotencyStore(), "k")


async def test_the_unreadable_record_clause_skips_a_store_that_stores_no_raw_records() -> None:
    contract = IdempotencyStoreContract()
    with pytest.raises(pytest.skip.Exception):
        await contract.test_an_unreadable_record_replays_as_a_cached_codec_failure(
            MemoryIdempotencyStore(), "garbage"
        )
    with pytest.raises(NotImplementedError):
        await contract.put_raw_record(MemoryIdempotencyStore(), "k", "{")

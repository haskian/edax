"""Prove the packaged conformance suite fails a store that breaks the clause.

`test_contract_memory.py` shows the suites pass a correct backend. That alone is worth little: a
suite that asserts nothing also passes. Each case here breaks one clause the way a real backend
breaks it, then asserts the corresponding contract method fails.

Every store below is a plausible mistake, not a strawman — a signed column for an unsigned hash, a
`>=` where the contract says `>`, a version advanced by a failed save.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest
from edax.aggregate import Envelope
from edax.backend.memory import (
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemoryProjectionStore,
    MemorySagaStore,
    MemoryTransactor,
)
from edax.command import Record
from edax.projection import Entity
from edax.testing import (
    EventStoreContract,
    IdempotencyStoreContract,
    ProjectionStoreContract,
    SagaStoreContract,
)
from edax.testing.faults import _Delegating


class InclusiveLoad(_Delegating):
    """`load(after=n)` returns `sequence >= n` instead of `> n`. The off-by-one that replays one
    event per load, so every projection double-counts its oldest unapplied event."""

    async def load(self, tx: Any, aggregate_id: str, after: int = 0) -> Any:
        rows = await self._inner.load(tx, aggregate_id, after=max(0, after - 1))
        return rows


class LeakyOutbox(_Delegating):
    """`unpublished` reads rows the transaction has not committed. A relay then publishes an event
    whose state change may still roll back."""

    def __init__(self, inner: Any, db: MemoryTransactor) -> None:
        super().__init__(inner)
        self._staged: list[Any] = []

    async def append(self, tx: Any, aggregate_id: str, expected: int, events: Any) -> None:
        await self._inner.append(tx, aggregate_id, expected, events)
        self._staged.extend(events)

    async def unpublished(self, limit: int = 100) -> Any:
        committed = await self._inner.unpublished(limit=limit)
        return list(committed) + [e for e in self._staged if e not in committed]


class VersionNotAdvanced(_Delegating):
    """`save` persists but leaves `entity.version` alone, so the caller's next save conflicts with
    the row it just wrote itself."""

    async def save(self, entity: Entity, tx: Any = None) -> None:
        before = entity.version
        await self._inner.save(entity, tx)
        entity.version = before


class VersionAdvancedOnConflict(_Delegating):
    """A refused save bumps the caller's version anyway. The caller reloads, retries, and conflicts
    forever against a version no row ever had."""

    async def save(self, entity: Entity, tx: Any = None) -> None:
        try:
            await self._inner.save(entity, tx)
        except Exception:
            entity.version += 1
            raise


class AlwaysWins(_Delegating):
    """`claim` reports a win for a key another caller already holds — a store that cannot tell "I
    won" from "someone else holds this". Two handlers then run for one key."""

    async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Optional[Record]:
        await self._inner.claim(key, payload_hash, tx)
        return None


class SignedHash(_Delegating):
    """The payload hash stored in a signed 64-bit column without a cast. Everything passes until a
    hash above 2^63 appears, and then a genuine duplicate reads as `PayloadMismatch`."""

    async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Optional[Record]:
        wrapped = payload_hash - 2**64 if payload_hash >= 2**63 else payload_hash
        prior = await self._inner.claim(key, wrapped, tx)
        return prior


class UnversionedDelete(_Delegating):
    """`delete` ignores the version it was given, dropping an instance another worker just
    advanced."""

    async def delete(self, id: str, version: int, tx: Any = None) -> None:
        instance = await self._inner.load(id, tx)
        if instance is not None:
            await self._inner.delete(id, instance.version, tx)


class DropsMetadata(_Delegating):
    """Persists the columns it recognised and quietly drops `metadata`. The correlation and causation
    ids go with it, so tracing dies at the first replay and nothing reports an error."""

    async def append(self, tx: Any, aggregate_id: str, expected: int, events: Any) -> None:
        stripped = []
        for e in events:
            stripped.append(
                Envelope(
                    id=e.id,
                    aggregate_id=e.aggregate_id,
                    event=e.event,
                    namespace=dict(e.namespace),
                    metadata={},
                    sequence=e.sequence,
                    timestamp=e.timestamp,
                )
            )
        await self._inner.append(tx, aggregate_id, expected, stripped)


class ShortcutsEmptyAppend(_Delegating):
    """Returns early for an empty batch instead of checking the version. A stale caller then believes
    it holds the current version, and its next save overwrites someone else's work."""

    async def append(self, tx: Any, aggregate_id: str, expected: int, events: Any) -> None:
        if not events:
            return
        await self._inner.append(tx, aggregate_id, expected, events)


class NoneResultReadsAsPending(_Delegating):
    """Cannot tell "completed with no value" from "still pending" — the classic mistake of storing
    the result column as NULL and inferring pending from it. A handler returning `None` then re-runs
    on every redelivery."""

    async def complete(self, key: str, record: Record, tx: Any = None) -> None:
        if record.result is None and record.error is None:
            return  # "nothing to store", so the pending claim stays
        await self._inner.complete(key, record, tx)


@pytest.fixture
def db() -> MemoryTransactor:
    return MemoryTransactor()


async def _fails(coro: Any) -> None:
    """The contract method must fail. Every verdict counts: an assertion it caught, a `pytest.raises`
    that did not fire, or the store raising where the clause said it must not.

    `pytest.fail.Exception` is listed explicitly because it derives from `BaseException`, not
    `Exception`, so a bare `except Exception` lets a caught clause escape and reads as a suite that
    failed to catch anything.
    """
    try:
        await coro
    except (Exception, pytest.fail.Exception):  # noqa: BLE001 - any failure is the verdict here
        return
    raise AssertionError("the contract passed a store that breaks its clause")


async def test_an_inclusive_load_is_caught(db: MemoryTransactor) -> None:
    store = InclusiveLoad(MemoryEventStore(db))
    await _fails(EventStoreContract().test_load_after_skips_what_the_caller_already_has(store))


async def test_an_outbox_leaking_uncommitted_rows_is_caught(db: MemoryTransactor) -> None:
    store = LeakyOutbox(MemoryEventStore(db), db)
    await _fails(EventStoreContract().test_the_outbox_never_shows_an_uncommitted_row(store))


async def test_a_version_that_does_not_advance_is_caught(db: MemoryTransactor) -> None:
    store = VersionNotAdvanced(MemoryProjectionStore(db))
    await _fails(ProjectionStoreContract().test_save_then_get_round_trips_and_advances_the_version(store))


async def test_a_version_advanced_by_a_failed_save_is_caught(db: MemoryTransactor) -> None:
    store = VersionAdvancedOnConflict(MemoryProjectionStore(db))
    await _fails(ProjectionStoreContract().test_a_refused_save_leaves_the_callers_version_alone(store))


async def test_a_ledger_that_wins_twice_is_caught(db: MemoryTransactor) -> None:
    store = AlwaysWins(MemoryIdempotencyStore(db))
    await _fails(IdempotencyStoreContract().test_a_second_claim_loses_and_reads_the_winners_record(store))


async def test_a_signed_payload_hash_column_is_caught(db: MemoryTransactor) -> None:
    store = SignedHash(MemoryIdempotencyStore(db))
    await _fails(IdempotencyStoreContract().test_the_record_keeps_the_payload_hash_it_was_given(store))


async def test_an_unversioned_saga_delete_is_caught(db: MemoryTransactor) -> None:
    store = UnversionedDelete(MemorySagaStore(db))
    await _fails(SagaStoreContract().test_delete_is_versioned_and_removes_the_instance(store))


async def test_dropped_metadata_is_caught(db: MemoryTransactor) -> None:
    store = DropsMetadata(MemoryEventStore(db))
    await _fails(EventStoreContract().test_every_envelope_field_survives_the_round_trip(store))


async def test_an_empty_append_that_skips_the_version_check_is_caught(db: MemoryTransactor) -> None:
    store = ShortcutsEmptyAppend(MemoryEventStore(db))
    await _fails(EventStoreContract().test_an_empty_append_still_checks_the_version(store))


async def test_a_none_result_left_pending_is_caught(db: MemoryTransactor) -> None:
    store = NoneResultReadsAsPending(MemoryIdempotencyStore(db))
    await _fails(IdempotencyStoreContract().test_a_none_result_replays_as_a_success(store))

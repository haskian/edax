"""Run the packaged conformance suites against the in-tree memory backends.

Two things at once. The memory stores are held to the clauses an adopter's store is held to, and the
packaged suite is proved to run — a conformance suite nobody executes is a promise, not a test.

`test_contract_discriminates.py` proves the other half: that these clauses fail a store that breaks
them.
"""

from __future__ import annotations

from typing import Any

import pytest
from conftest import SplitLedger
from edax.backend.memory import (
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemoryProjectionStore,
    MemorySagaStore,
    MemoryTransactor,
)
from edax.testing import (
    EventStoreContract,
    IdempotencyStoreContract,
    ProjectionStoreContract,
    SagaStoreContract,
)


@pytest.fixture
def db() -> MemoryTransactor:
    return MemoryTransactor()


class TestMemoryEventStore(EventStoreContract):
    supports_unpublished_count = True

    @pytest.fixture
    def event_store(self, db: MemoryTransactor) -> Any:
        return MemoryEventStore(db)


class TestMemoryProjectionStore(ProjectionStoreContract):
    @pytest.fixture
    def projection_store(self, db: MemoryTransactor) -> Any:
        return MemoryProjectionStore(db)

    @pytest.fixture
    def transactor(self, db: MemoryTransactor) -> Any:
        return db


class TestMemorySagaStore(SagaStoreContract):
    @pytest.fixture
    def saga_store(self, db: MemoryTransactor) -> Any:
        return MemorySagaStore(db)


class TestMemoryIdempotencyStore(IdempotencyStoreContract):
    @pytest.fixture
    def idempotency_store(self, db: MemoryTransactor) -> Any:
        return MemoryIdempotencyStore(db)

    @pytest.fixture
    def event_store(self, db: MemoryTransactor) -> Any:
        # The same transactor, so the rollback clause has one unit of work to roll back.
        return MemoryEventStore(db)


class TestSplitLedgerIdempotencyStore(IdempotencyStoreContract):
    """The Redis shape: a ledger of its own, with no ``event_store`` fixture on purpose. The
    rollback clause must skip, not error on the missing fixture."""

    joins_transaction = False

    @pytest.fixture
    def idempotency_store(self) -> Any:
        return SplitLedger()

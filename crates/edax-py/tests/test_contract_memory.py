"""Run the packaged conformance suites against the in-tree memory backends.

Two things at once. The memory stores are held to the clauses an adopter's store is held to, and the
packaged suite is proved to run — a conformance suite nobody executes is a promise, not a test.

`test_contract_discriminates.py` proves the other half: that these clauses fail a store that breaks
them.
"""

from __future__ import annotations

from typing import Any

import pytest
from edax.backend.memory import (
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemoryLedger,
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
    supports_kernel_queries = True

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

    @pytest.fixture
    def transactor(self, db: MemoryTransactor) -> Any:
        return db


class TestMemoryIdempotencyStore(IdempotencyStoreContract):
    # The memory store's claims lapse through its `expire` hook, so the takeover clauses run on it too.
    claims_expire = True

    async def expire_claim(self, idempotency_store: Any, key: str) -> None:
        assert idempotency_store.expire(key), f"{key} held nothing to lapse"

    @pytest.fixture
    def idempotency_store(self, db: MemoryTransactor) -> Any:
        return MemoryIdempotencyStore(db)

    @pytest.fixture
    def event_store(self, db: MemoryTransactor) -> Any:
        # The same transactor, so the rollback clause has one unit of work to roll back.
        return MemoryEventStore(db)


class TestMemoryLedger(IdempotencyStoreContract):
    """The Redis shape, shipped: a ledger of its own, with no ``event_store`` fixture on purpose. The store
    declares ``joins_transaction = False``, so the clauses that need a shared rollback skip rather than
    error on the missing fixture, and its claims lapse through ``expire`` the way Redis's TTL lapses them,
    so the takeover clauses hold it to the clauses `test_redis.py` holds Redis to."""

    claims_expire = True

    async def expire_claim(self, idempotency_store: Any, key: str) -> None:
        assert idempotency_store.expire(key), f"{key} held nothing to lapse"

    @pytest.fixture
    def idempotency_store(self) -> Any:
        return MemoryLedger()

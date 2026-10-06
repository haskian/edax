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
    MemoryCommandOutbox,
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemoryLedger,
    MemoryProjectionStore,
    MemorySagaStore,
    MemoryTransactor,
)
from edax.saga import SagaOutbox
from edax.testing import (
    CommandOutboxContract,
    EventOutboxContract,
    EventStoreContract,
    IdempotencyStoreContract,
    ProjectionStoreContract,
    SagaOutboxContract,
    SagaStoreContract,
)


@pytest.fixture
def db() -> MemoryTransactor:
    return MemoryTransactor()


class TestMemoryEventStore(EventStoreContract):
    @pytest.fixture
    def event_store(self, db: MemoryTransactor) -> Any:
        return MemoryEventStore(db)


class TestMemoryEventOutbox(EventOutboxContract):
    @pytest.fixture
    def event_store(self, db: MemoryTransactor) -> Any:
        return MemoryEventStore(db)

    @pytest.fixture
    def outbox(self, event_store: MemoryEventStore) -> Any:
        # The memory event store is both ports: it writes the rows and reads them back.
        return event_store


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

    @pytest.fixture
    def transactor(self, db: MemoryTransactor) -> Any:
        return db


class TestMemorySagaOutbox(SagaOutboxContract):
    @pytest.fixture
    def saga_store(self, db: MemoryTransactor) -> Any:
        return MemorySagaStore(db)

    @pytest.fixture
    def outbox(self, saga_store: MemorySagaStore) -> Any:
        return SagaOutbox(saga_store, "order")

    @pytest.fixture
    def transactor(self, db: MemoryTransactor) -> Any:
        return db


class TestMemoryCommandOutbox(CommandOutboxContract):
    @pytest.fixture
    def command_outbox(self, db: MemoryTransactor) -> Any:
        return MemoryCommandOutbox(db)

    @pytest.fixture
    def outbox(self, command_outbox: MemoryCommandOutbox) -> Any:
        return command_outbox

    @pytest.fixture
    def event_store(self, db: MemoryTransactor) -> Any:
        # The same transactor, so the outbox writes under the event store's unit of work.
        return MemoryEventStore(db)


class TestMemoryIdempotencyStore(IdempotencyStoreContract):
    # The memory store's claims lapse through its `expire` hook, so the takeover clauses run on it too.
    claims_expire = True
    stores_raw_records = True

    async def expire_claim(self, idempotency_store: Any, key: str) -> None:
        assert idempotency_store.expire(key), f"{key} held nothing to lapse"

    async def put_raw_record(self, idempotency_store: Any, key: str, raw: str) -> None:
        idempotency_store.put_raw(key, raw)

    @pytest.fixture
    def idempotency_store(self, db: MemoryTransactor) -> Any:
        return MemoryIdempotencyStore(db)

    @pytest.fixture
    def transactor(self, db: MemoryTransactor) -> Any:
        # The store's own transactor, so the rollback clauses have one unit of work to roll back.
        return db


class TestMemoryLedger(IdempotencyStoreContract):
    """The Redis shape, shipped: a ledger of its own, with no ``transactor`` fixture on purpose. The store
    declares ``joins_transaction = False``, so the clauses that need a shared rollback skip rather than
    error on the missing fixture, and its claims lapse through ``expire`` the way Redis's TTL lapses them,
    so the takeover clauses hold it to the clauses `test_redis.py` holds Redis to."""

    claims_expire = True
    stores_raw_records = True

    async def expire_claim(self, idempotency_store: Any, key: str) -> None:
        assert idempotency_store.expire(key), f"{key} held nothing to lapse"

    async def put_raw_record(self, idempotency_store: Any, key: str, raw: str) -> None:
        idempotency_store.put_raw(key, raw)

    @pytest.fixture
    def idempotency_store(self) -> Any:
        return MemoryLedger()

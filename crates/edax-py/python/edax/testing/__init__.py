"""Conformance suites for backends you write yourself.

``edax`` ships no durable storage: the event store, projection store, ledger and saga store are
seams, and their implementations are yours. Each is correctness-critical, none is reviewed by this
project, and the clauses that matter most are the ones no type checker can see — that ``append``
compare-and-sets *in the database*, that ``claim`` can tell "I won" from "someone else holds this".

Subclass a contract, give it a fixture, and the clauses run against your store::

    from edax.testing import EventStoreContract, IdempotencyStoreContract


    class TestMyEventStore(EventStoreContract):
        @pytest.fixture
        async def event_store(self):
            return MyEventStore(pool)


    class TestMyLedger(IdempotencyStoreContract):
        joins_transaction = False  # a ledger of its own, as Redis is

        @pytest.fixture
        async def idempotency_store(self):
            return MyLedger(conn)

Requires ``pytest`` and ``asyncio_mode = "auto"``; install with ``pip install edax[test]``. The
suites are the same clauses ``edax``'s own backends are held to, so passing them means your store
and the in-tree ones are interchangeable.

:mod:`edax.testing.faults` wraps any store to inject the failure that matters most — the write that
landed and lost its response.
"""

from __future__ import annotations

from .contracts import (
    EventStoreContract,
    IdempotencyStoreContract,
    ProjectionStoreContract,
    SagaStoreContract,
)
from .faults import LostResponse, Refusing

__all__ = [
    "EventStoreContract",
    "IdempotencyStoreContract",
    "LostResponse",
    "ProjectionStoreContract",
    "Refusing",
    "SagaStoreContract",
]

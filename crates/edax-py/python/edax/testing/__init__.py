"""Conformance suites for backends you write yourself.

``edax`` ships no durable storage: the event store, projection store, ledger and saga store are
seams, and their implementations are yours. This project does not review them, and no type checker
sees their hardest clauses: that ``append`` compare-and-sets *in the database*, and that ``claim`` can
tell "I won" from "someone else holds this".

Subclass a contract, give it a fixture, and the clauses run against your store::

    from edax.testing import EventStoreContract, IdempotencyStoreContract


    class TestMyEventStore(EventStoreContract):
        @pytest.fixture
        async def event_store(self):
            return MyEventStore(pool)


    class TestMyLedger(IdempotencyStoreContract):
        # The suite reads `joins_transaction` from the store, as the guard does: a ledger of its
        # own (Redis) declares none, so the clauses that need a shared rollback skip.
        @pytest.fixture
        async def idempotency_store(self):
            return MyLedger(conn)

A broker adapter subclasses :class:`BrokerContract` and supplies a ``broker_rig`` (a :class:`BrokerRig`):
per-key order through a nack, redelivery from the commit after a crash, consumer groups.

Requires ``pytest`` and ``asyncio_mode = "auto"``; install with ``pip install edax[test]``. The
suites are the same clauses ``edax``'s own backends are held to, so passing them means your store
and the in-tree ones are interchangeable.

:mod:`edax.testing.faults` wraps any store to inject a write that landed and lost its response.

:func:`event` builds a stored event by hand, to publish with no store behind it. :func:`drafted` and
:func:`recorded` move an event between its two shapes: the draft a repository hands ``append``, and the
message the store keeps for it. :func:`event_of` narrows a delivered message to the event a test published.
"""

from __future__ import annotations

from .contracts import (
    BrokerContract,
    BrokerRig,
    EventStoreContract,
    IdempotencyStoreContract,
    ProjectionStoreContract,
    SagaStoreContract,
    drafted,
    event,
    event_of,
    recorded,
)
from .faults import LostResponse, Refusing

__all__ = [
    "BrokerContract",
    "BrokerRig",
    "EventStoreContract",
    "IdempotencyStoreContract",
    "LostResponse",
    "ProjectionStoreContract",
    "Refusing",
    "SagaStoreContract",
    "drafted",
    "event",
    "event_of",
    "recorded",
]

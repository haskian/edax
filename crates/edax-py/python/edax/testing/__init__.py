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
        # own (Redis) declares none, so the clauses that need a shared rollback skip and it needs
        # no `transactor` fixture. A ledger that joins one supplies the transactor it joins.
        @pytest.fixture
        async def idempotency_store(self):
            return MyLedger(conn)

Every outbox's read side is held to one :class:`OutboxContract`: committed rows only, oldest commit
first, the limit, an idempotent mark and, for a :class:`~edax.outbox.CountedOutbox`, the backlog count. A
seam's outbox suite subclasses it: :class:`EventOutboxContract` supplies ``outbox`` plus the
``event_store`` that writes it, :class:`SagaOutboxContract` ``outbox`` plus the ``saga_store``, and
:class:`CommandOutboxContract` ``outbox`` plus the ``command_outbox`` write side and an ``event_store``
over the same database: commands commit and roll back with the events.

A broker adapter subclasses :class:`BrokerContract` and supplies a ``broker_rig`` (a :class:`BrokerRig`):
per-key order through a nack, redelivery from the commit after a crash, consumer groups.

The suites require ``pytest`` and ``asyncio_mode = "auto"``; install with ``pip install edax[test]``.
They load on first use, so the message builders below work without pytest. The suites are the same
clauses ``edax``'s own backends are held to, so passing them means your store and the in-tree ones are
interchangeable.

:mod:`edax.testing.faults` wraps any store to inject a fault: :class:`LostResponse` lands a write and
loses its response, :class:`Refusing` refuses one and writes nothing, :class:`BrokenCommit` fails a commit
either way, and :class:`Delegating` is the base to break a store your own way. :mod:`edax.testing.redis`
holds the Redis backend's own fault.

:func:`draft`, :func:`message` and :func:`command` build the rich messages the suites write: fresh ids, a
tenant, a non-ASCII JSON body, a lineage, a trace parent and the application's metadata. :func:`event` builds
a stored event with stable ids by hand, to publish with no store behind it, and :func:`delivery` wraps a
message in a delivery whose settlement does nothing, to submit to a dispatcher. :func:`drafted` and
:func:`recorded` move an event between its two shapes: the draft a repository hands ``append``, and the
message the store keeps for it. :func:`event_of` narrows a delivered message to the event a test published.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .faults import BrokenCommit, CommitFault, Delegating, LostResponse, Refusing
from .messages import command, delivery, draft, drafted, event, event_of, message, recorded

if TYPE_CHECKING:
    from .contracts import (
        BatchSinkContract,
        BrokerContract,
        BrokerRig,
        CommandOutboxContract,
        EventOutboxContract,
        EventStoreContract,
        IdempotencyStoreContract,
        OutboxContract,
        ProjectionStoreContract,
        SagaOutboxContract,
        SagaStoreContract,
        TransactionalBatchSinkContract,
    )

__all__ = [
    "BatchSinkContract",
    "TransactionalBatchSinkContract",
    "BrokenCommit",
    "BrokerContract",
    "BrokerRig",
    "CommandOutboxContract",
    "CommitFault",
    "Delegating",
    "EventOutboxContract",
    "EventStoreContract",
    "IdempotencyStoreContract",
    "LostResponse",
    "OutboxContract",
    "ProjectionStoreContract",
    "Refusing",
    "SagaOutboxContract",
    "SagaStoreContract",
    "command",
    "delivery",
    "draft",
    "drafted",
    "event",
    "event_of",
    "message",
    "recorded",
]

_CONTRACTS = frozenset(
    {
        "BatchSinkContract",
        "TransactionalBatchSinkContract",
        "BrokerContract",
        "BrokerRig",
        "CommandOutboxContract",
        "EventOutboxContract",
        "EventStoreContract",
        "IdempotencyStoreContract",
        "OutboxContract",
        "ProjectionStoreContract",
        "SagaOutboxContract",
        "SagaStoreContract",
    }
)


def __getattr__(name: str) -> object:
    """Load the contract suites on first use: they import pytest, which a test that only builds a message
    does not need."""
    if name in _CONTRACTS:
        from . import contracts

        return getattr(contracts, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

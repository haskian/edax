"""Executable conformance suites for the storage and broker seams.

Every backend an adopter writes rests on clauses no type and no review can check: whether ``append``
compare-and-sets in the database or reads then writes in Python, and whether ``claim`` can tell "I won"
from "someone else holds this". A test the adopter writes from the same reading of a clause as the
implementation encodes a misread clause in both. These suites state each clause once, for every
backend.

Subclass the contract for each seam you implement and supply the one fixture it asks for::

    from edax.testing import EventStoreContract


    class TestPostgresEventStore(EventStoreContract):
        @pytest.fixture
        async def event_store(self, pool):
            await reset(pool)
            return PostgresEventStore(pool)

Name the subclass ``Test...`` so pytest collects it; the ``*Contract`` bases are not collected
themselves. The suites are ``async def``, so they need ``asyncio_mode = "auto"`` in your pytest
configuration (the ``test`` extra installs ``pytest-asyncio``).

**Capabilities.** Backends differ in ways the contract allows, so a clause that cannot apply is
skipped rather than failed. Declare them as class attributes on your subclass. The defaults describe
a transactional SQL backend, the common case and the strictest. One flag, ``supports_transactions``, says
whether there is a unit of work at all; where it is on, the clauses that join, roll back, race in or
isolate one ask for one ``transactor`` fixture, resolved only when such a clause runs. The idempotency
suite reads the same answer from the store's own ``joins_transaction``.

**Ambient enlistment.** The SDK's guards and the saga runner call a store with no ``tx`` inside
:func:`~edax.unit_of_work.transaction`, so the store must join :func:`~edax.unit_of_work.current_tx` of its
own transactor. Each seam with an optional ``tx`` has a clause for that path, apart from the one that passes
``tx=`` itself: a store that honours an explicit handle and auto-commits without one passes every explicit
clause.

**Concurrency and isolation.** A clause that runs one call at a time cannot tell a compare-and-set in the
database from a read-then-write in Python. The race clauses run two units of work that each stage their
write before either commits, forced in that order, not left to timing: the second one's write runs in a
task of its own, so a store that blocks it on the first one's row lock does not deadlock the clause, which
waits a bounded moment for it and then commits the first. Exactly one of the two may land. The isolation
clauses check that a staged write is invisible to every other reader until it commits. Both need an async
driver: a driver that blocks the event loop on a row lock deadlocks any concurrent asyncio code, this kit
included.

**Payloads.** Every draft carries a time-ordered :func:`~edax.guid` id, a lineage of guids and a real JSON
body, as the SDK writes them, so a store with an ``id uuid`` column takes them as they come in production.
The store stamps ``recorded_at`` itself, so a ``timestamptz`` column holds the store's own clock.
"""

from __future__ import annotations

from .broker import BrokerContract, BrokerRig
from .command_outbox import CommandOutboxContract
from .event_store import EventOutboxContract, EventStoreContract
from .idempotency_store import IdempotencyStoreContract
from .outbox import OutboxContract
from .projection_store import ProjectionStoreContract
from .saga_store import SagaOutboxContract, SagaStoreContract

__all__ = [
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
]

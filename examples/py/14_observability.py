"""Step 14: see what the SDK does, with an Observer and the `timing` middleware.

edax ships no telemetry library. It calls plain hooks with plain values:

- an `Observer` hears what is known after the fact: how a claim ended, how a transaction ended,
  what a relay drain published. Subclass it and override the hooks you export;
- `timing(record, name)` is a middleware that hands `record` one row per handler call.

Pass the same observer wherever you build an object: `observer=observer`.

Run:  cd examples/py && uv run python 14_observability.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import ClassVar

from edax import (
    CommandBus,
    EventDraft,
    Observer,
    Relay,
    Tenancy,
    current_timestamp,
    idempotent,
    timing,
    transaction,
)
from edax.backend.memory import MemoryEventStore, MemoryIdempotencyStore, MemoryTransactor


class PrintingObserver(Observer):
    """Prints and keeps every signal. A real one feeds OpenTelemetry instead."""

    def __init__(self) -> None:
        self.signals: list[str] = []

    def emit(self, signal: str) -> None:
        print(f"  observer: {signal}")
        self.signals.append(signal)

    def claim(self, name: str, verdict: str) -> None:
        self.emit(f"claim {name} {verdict}")  # a dispatch lost the claim to an earlier one

    def settled(self, name: str, how: str, duration_ns: int) -> None:
        self.emit(f"settled {name} {how}")

    def transaction(self, outcome: str, duration_ns: int) -> None:
        self.emit(f"transaction {outcome}")

    def drain(
        self, published: int, dead_lettered: int, duration_ns: int, oldest_timestamp: int | None
    ) -> None:
        self.emit(f"drain published={published} dead_lettered={dead_lettered}")


@dataclass
class Deposit:
    account_id: str
    amount: int
    type: ClassVar[str] = "Deposit"


def record_latency(name: str, outcome: str, error_type: str | None, duration_ns: int) -> None:
    print(f"  timing:   {name} {outcome} in {duration_ns / 1e6:.3f} ms")


async def main() -> None:
    observer = PrintingObserver()
    db = MemoryTransactor()
    store = MemoryEventStore(db)

    # 1. timing() outermost, so it times every dispatch, the replayed ones too.
    ledger = MemoryIdempotencyStore(db)
    guard = idempotent(ledger, db, scope="accounts-api", observer=observer, tenancy=Tenancy.SILO)
    bus = CommandBus(timing(record_latency, lambda cmd: cmd.type), guard)

    async def deposit(cmd: Deposit) -> int:
        return cmd.amount  # a real handler saves through a Repository here

    bus.register("Deposit", deposit)

    # 2. A first dispatch, then a repeat of the same request.
    print("dispatch req-1:")
    await bus.dispatch(Deposit("acct-1", 100), idempotency_key="req-1")
    print("dispatch req-1 again:")
    await bus.dispatch(Deposit("acct-1", 100), idempotency_key="req-1")

    # 3. A unit of work you open yourself reports through `transaction(..., observer=)`.
    print("append an event:")
    draft = EventDraft(
        id="e1",
        aggregate_id="acct-1",
        sequence=1,
        name="Deposited",
        version=1,
        data=b"{}",
        correlation_id="e1",
        causation_id="e1",
        extensions={},
        metadata={},
    )
    async with transaction(store, observer=observer) as tx:
        await store.append(tx, "acct-1", 0, [draft])

    # 4. A relay drain reports what it published. `relay.last` keeps it for a health check.
    print("drain the outbox:")
    relay = Relay(store, lambda event: None, observer=observer)
    await relay.drain()
    assert relay.last is not None and relay.last.oldest_timestamp is not None
    lag_ms = (current_timestamp() - relay.last.oldest_timestamp) / 1e6
    print(f"  pull:     relay.last.published={relay.last.published}, outbox lag {lag_ms:.3f} ms")

    assert observer.signals == [
        "transaction committed",  # dispatch 1: the guard's unit of work
        "settled Deposit completed",  # dispatch 1: the claim it won
        "claim Deposit succeeded",  # dispatch 2: the repeat found the first outcome
        "transaction committed",  # dispatch 2
        "transaction committed",  # the unit of work opened by hand
        "drain published=1 dead_lettered=0",
    ], observer.signals
    print("\nok: every dispatch was timed; claims, transactions and the drain were observed")


if __name__ == "__main__":
    asyncio.run(main())

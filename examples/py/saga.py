"""Saga / process manager: react to committed events, drive a workflow, emit commands.

    events ─▶ KeyOrderedDispatcher ─▶ EventRouter ─▶ SagaRunner.on_event ─▶ SagaStore (state + outbox)
                                                       │ correlation_id -> load state
                                                       │ handle(state, event) -> (new_state, commands)
                                                       └ save state and commands in one versioned write
    SagaStore outbox ─▶ runner.relay(...) ─▶ CommandBus.dispatch

An order saga waits for Paid, fires Fulfill, completes. partition(aggregate_id) pins each order to one
worker, so Created lands before Paid without a lock. The runner never sends: it commits the new state and
the commands together, and the relay sends what committed. Completion leaves a tombstone, so the
redelivered Paid finds the order completed and is ignored; the bus is idempotent for the relay's re-sends.

Run:  maturin develop -m crates/edax-py/Cargo.toml  &&  python examples/py/saga.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, ClassVar

import edax
from edax.backend.memory import MemoryIdempotencyStore, MemorySagaStore
from edax.causation import CORRELATION_ID, propagate
from edax.codec import json_codec
from edax.command import CommandBus, idempotent
from edax.saga import Completed, SagaRunner


@dataclass
class Fulfill:
    order_id: str
    # The chain, carried across the saga hop: `handle` returns commands, not metadata, so these
    # reach the handler only as fields. Retry-stable — a redelivery hands the saga the same
    # envelope — so they do not change the hash. See docs/sdk/sagas.md.
    correlation_id: str
    causation_id: str
    type: ClassVar[str] = "Fulfill"  # routing only: a class attribute stays out of the hash


def fulfill_key(cmd: Any) -> str:
    """The idempotency key for a dispatched command. `Any` because one bus routes several command
    types and `idempotent` types its key function over the `Command` protocol, which has only `type`.

    Keys on the order alone because exactly one event type causes Fulfill. A saga where two
    different events can cause one intent must key on the cause too, or the second is a
    `PayloadMismatch` under the first one's record."""
    return f"fulfill:{cmd.order_id}"


class OrderSaga:
    def correlation_id(self, env: edax.EventEnvelope) -> str | None:
        return env.aggregate_id if env.event.name in ("Created", "Paid") else None

    def handle(self, state: Any, env: edax.EventEnvelope) -> tuple[Any, list]:
        if env.event.name == "Created":
            return "open", []
        if env.event.name == "Paid" and state is not None:
            chain = propagate(env.metadata, env.id)  # exactly the two keys, for the command to carry
            return Completed, [Fulfill(env.aggregate_id, **chain)]  # complete + emit
        return Completed, []  # Paid with no instance: nothing to complete (a redelivery never gets here)


def make_event(order_id: str, name: str) -> edax.EventEnvelope:
    return edax.EventEnvelope(
        id=f"{order_id}-{name}",
        aggregate_id=order_id,
        event=edax.Event(name, 1, b""),
        namespace={},
        # A real root stamps this with `ensure_ids()`; fixed here so the assertion below can name it.
        metadata={CORRELATION_ID: f"corr-{order_id}"},
        timestamp=0,
    )


STREAM = [
    make_event("order-1", "Created"),
    make_event("order-2", "Created"),
    make_event("order-1", "Paid"),
    make_event("order-1", "Paid"),  # redelivery
    make_event("order-2", "Paid"),
]


async def main() -> None:
    fulfilled: list[tuple[str, str, str]] = []
    bus = CommandBus(idempotent(MemoryIdempotencyStore(), key=fulfill_key))

    async def fulfill(c: Fulfill) -> None:
        # A real handler stamps these onto whatever it writes:
        # repo.save(order, metadata={"correlation_id": c.correlation_id, "causation_id": c.causation_id})
        fulfilled.append((c.order_id, c.correlation_id, c.causation_id))

    bus.register("Fulfill", fulfill)

    runner = SagaRunner(OrderSaga(), MemorySagaStore(), json_codec({"Fulfill": Fulfill}))
    router = edax.EventRouter()
    router.register("Created", runner.on_event)
    router.register("Paid", runner.on_event)

    dispatcher = edax.KeyOrderedDispatcher(4, router.dispatch)
    dispatcher.start()
    for env in STREAM:
        await dispatcher.submit(env)
    await dispatcher.stop()
    # The commands committed with each completion; the relay sends them through the bus.
    sent = await runner.relay(bus.dispatch).drain()
    assert sent == 2, sent

    print("fulfilled:", fulfilled)
    # Once each (cross-order is unordered), and the chain survived the hop: the flow id came from the
    # Paid event's metadata, the causation id is that event's own id.
    assert sorted(fulfilled) == [
        ("order-1", "corr-order-1", "order-1-Paid"),
        ("order-2", "corr-order-2", "order-2-Paid"),
    ], fulfilled
    print("\nok: saga fulfilled each order once (concurrent, redelivery tolerated), chain intact")


if __name__ == "__main__":
    asyncio.run(main())

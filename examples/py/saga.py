"""Saga / process manager: react to committed events, drive a workflow, emit commands.

    events ─▶ KeyOrderedDispatcher ─▶ EventRouter ─▶ SagaRunner.on_event ─▶ CommandBus.dispatch
                                                       │ correlation_id -> load state
                                                       │ handle(state, event) -> (new_state, commands)
                                                       └ dispatch commands, then save/delete state

An order saga waits for Paid, fires Fulfill, completes. partition(aggregate_id) pins each order to one
worker, so Created lands before Paid without a lock. The runner dispatches before it persists, so a
redelivered Paid is possible: the saga ignores it for a completed order and the bus is idempotent.

Run:  maturin develop -m crates/edax-py/Cargo.toml  &&  python examples/py/saga.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, ClassVar

import edax
from edax.backend.memory import MemoryIdempotencyStore, MemorySagaStore
from edax.command import CommandBus, idempotent
from edax.saga import Completed, SagaRunner


@dataclass
class Fulfill:
    order_id: str
    type: ClassVar[str] = "Fulfill"  # routing only: a class attribute stays out of the hash


def fulfill_key(cmd: Any) -> str:
    """The idempotency key for a dispatched command. `Any` because one bus routes several command
    types and `idempotent` types its key function over the `Command` protocol, which has only `type`."""
    return f"fulfill:{cmd.order_id}"


class OrderSaga:
    def correlation_id(self, env: edax.Envelope) -> str | None:
        return env.aggregate_id if env.event.name in ("Created", "Paid") else None

    def handle(self, state: Any, env: edax.Envelope) -> tuple[Any, list]:
        if env.event.name == "Created":
            return "open", []
        if env.event.name == "Paid" and state is not None:
            return Completed, [Fulfill(env.aggregate_id)]  # complete + emit
        return Completed, []  # Paid with no live instance (redelivery after completion)


def make_event(order_id: str, name: str) -> edax.Envelope:
    return edax.Envelope(
        id=f"{order_id}-{name}",
        aggregate_id=order_id,
        event=edax.Event(name, 1, b""),
        namespace={},
        metadata={},
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
    fulfilled: list[str] = []
    bus = CommandBus(idempotent(MemoryIdempotencyStore(), key=fulfill_key))

    async def fulfill(c: Fulfill) -> None:
        fulfilled.append(c.order_id)

    bus.register("Fulfill", fulfill)

    runner = SagaRunner(OrderSaga(), MemorySagaStore(), dispatch=bus.dispatch)
    router = edax.EventRouter()
    router.register("Created", runner.on_event)
    router.register("Paid", runner.on_event)

    dispatcher = edax.KeyOrderedDispatcher(4, router.dispatch)
    dispatcher.start()
    for env in STREAM:
        await dispatcher.submit(env)
    await dispatcher.stop()

    print("fulfilled:", fulfilled)
    assert sorted(fulfilled) == ["order-1", "order-2"], fulfilled  # once each; cross-order is unordered
    print("\nok: saga fulfilled each order once (concurrent, redelivery tolerated)")


if __name__ == "__main__":
    asyncio.run(main())

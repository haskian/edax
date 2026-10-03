"""Step 13: a saga reacts to events and sends commands, through its own outbox.

An order saga waits for `OrderPlaced`, then for `PaymentReceived`, then sends `ShipOrder` and
completes. The runner never sends a command itself. It saves the new state and the commands in one
write. `runner.relay(...)` then sends what was saved, as the event Relay does (step 7).

Run:  cd examples/py && uv run python 13_saga.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, ClassVar

from edax import (
    CommandBus,
    Completed,
    Event,
    EventEnvelope,
    SagaCommand,
    SagaRunner,
    ensure_ids,
    idempotent,
    json_codec,
    propagate,
)
from edax.backend.memory import MemoryIdempotencyStore, MemorySagaStore


@dataclass
class ShipOrder:
    order_id: str
    # The causation chain (step 12). A saga returns commands, not metadata, so it rides as fields.
    correlation_id: str
    causation_id: str
    type: ClassVar[str] = "ShipOrder"


class OrderSaga:
    def correlation_id(self, envelope: EventEnvelope) -> str | None:
        """Which saga instance an event belongs to: one per order."""
        return envelope.aggregate_id

    def handle(self, state: Any, envelope: EventEnvelope) -> tuple[Any, list[ShipOrder]]:
        """Fold one event into the state. Return the new state and the commands to send."""
        if envelope.event.name == "OrderPlaced":
            return "awaiting payment", []
        if envelope.event.name == "PaymentReceived":
            chain = propagate(envelope.metadata, envelope.id)
            return Completed, [ShipOrder(envelope.aggregate_id, **chain)]
        return state, []  # any other event changes nothing


def order_event(order_id: str, name: str) -> EventEnvelope:
    return EventEnvelope(
        id=f"{order_id}-{name}",
        aggregate_id=order_id,
        event=Event(name, 1, b"{}"),
        namespace={},
        metadata=ensure_ids(),
        timestamp=0,
    )


def ship_key(cmd: Any) -> str:
    return f"ship:{cmd.order_id}"


async def main() -> None:
    # 1. The command side: a bus that ships an order, once per order.
    shipped: list[ShipOrder] = []
    bus = CommandBus(idempotent(MemoryIdempotencyStore(), key=ship_key))

    async def ship(cmd: ShipOrder) -> None:
        shipped.append(cmd)

    bus.register("ShipOrder", ship)

    # 2. The saga runner: saga, state store, and a codec for the commands it saves.
    sagas = MemorySagaStore()
    runner = SagaRunner(OrderSaga(), sagas, json_codec({"ShipOrder": ShipOrder}))

    # 3. Feed it events. In a service, pass `runner.on_event` to an EventRouter instead.
    placed = order_event("order-1", "OrderPlaced")
    paid = order_event("order-1", "PaymentReceived")
    await runner.on_event(placed)
    instance = await sagas.load("order-1")
    assert instance is not None
    print(f"after OrderPlaced:     state {instance.state!r}")
    await runner.on_event(paid)
    await runner.on_event(paid)  # the broker delivered it twice
    print(f"after PaymentReceived: commands waiting {len(await sagas.unpublished())}")

    # 4. Send the saved commands. A command that fails for good goes to `on_dead`.
    dead: list[str] = []

    async def to_dead_letters(row: SagaCommand, error: BaseException) -> None:
        dead.append(row.id)

    sent = await runner.relay(bus.dispatch, on_dead=to_dead_letters).drain()
    print(f"relay sent {sent}: shipped {[cmd.order_id for cmd in shipped]}")
    print(f"ShipOrder was caused by {shipped[0].causation_id}")

    assert [cmd.order_id for cmd in shipped] == ["order-1"] and dead == [], (shipped, dead)
    assert shipped[0].causation_id == paid.id
    print("\nok: one order, one ShipOrder, even with the payment delivered twice")


if __name__ == "__main__":
    asyncio.run(main())

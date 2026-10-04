"""Step 13: a saga reacts to events and sends commands, through its own outbox.

An order saga waits for `OrderPlaced`, then for `PaymentReceived`, then sends `ShipOrder` and
completes. The runner never sends a command itself. It saves the new state and the commands in one
write. `runner.relay(bus)` then sends what was saved, as the event Relay does (step 7).

Each saved command carries its saga command id as its idempotency key. A relay that sends a row twice,
after a crash before the row was marked, is replayed by the bus's `idempotent` guard.

Run:  cd examples/py && uv run python 13_saga.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, ClassVar

from edax import (
    CommandBus,
    CommandMessage,
    Completed,
    EventMessage,
    Lineage,
    SagaRunner,
    current_lineage,
    idempotent,
    json_codec,
    originate,
    testing,
)
from edax.backend.memory import MemoryIdempotencyStore, MemorySagaStore


@dataclass
class ShipOrder:
    order_id: str
    type: ClassVar[str] = "ShipOrder"


class OrderSaga:
    name: ClassVar[str] = "order"

    def instance_id(self, event: EventMessage) -> str | None:
        """Which saga instance an event belongs to: one per order."""
        return event.aggregate_id

    def handle(self, state: Any, event: EventMessage) -> tuple[Any, list[ShipOrder]]:
        """Fold one event into the state. Return the new state and the commands to send."""
        if event.name == "OrderPlaced":
            return "awaiting payment", []
        if event.name == "PaymentReceived":
            return Completed, [ShipOrder(event.aggregate_id)]
        return state, []  # any other event changes nothing


def order_event(order_id: str, name: str, flow: Lineage) -> EventMessage:
    return testing.event(
        order_id,
        name,
        id=f"{order_id}-{name}",
        data=b"{}",
        correlation_id=flow.correlation_id,
        causation_id=flow.causation_id,
    )


async def main() -> None:
    # 1. The command side: a bus that ships an order, once per order. The handler notes the lineage
    #    it runs under: whatever it saves continues the order's flow (step 12).
    shipped: list[ShipOrder] = []
    lineages: list[Lineage | None] = []
    bus = CommandBus(idempotent(MemoryIdempotencyStore(), scope="shipping"))

    async def ship(cmd: ShipOrder) -> None:
        shipped.append(cmd)
        lineages.append(current_lineage())

    bus.register("ShipOrder", ship)

    # 2. The saga runner: saga, state store, and a codec for the commands it saves.
    sagas = MemorySagaStore()
    commands = json_codec({"ShipOrder": ShipOrder})
    runner = SagaRunner(OrderSaga(), sagas, commands)

    # 3. Feed it events of one order flow. In a service, pass `runner.on_event` to an EventRouter.
    flow = originate()
    placed = order_event("order-1", "OrderPlaced", flow)
    paid = order_event("order-1", "PaymentReceived", flow)
    await runner.on_event(placed)
    instance = await sagas.load("order-1")
    assert instance is not None
    print(f"after OrderPlaced:     state {instance.state!r}")
    await runner.on_event(paid)
    await runner.on_event(paid)  # the broker delivered it twice
    [row] = await sagas.unpublished()
    print(f"after PaymentReceived: one command waiting, {row.name} {row.id}, caused by {row.causation_id}")

    # 4. Send the saved commands. A command that fails for good goes to `on_dead`.
    dead: list[str] = []

    async def to_dead_letters(row: CommandMessage, error: BaseException) -> None:
        dead.append(row.id)

    sent = await runner.relay(bus, on_dead=to_dead_letters).drain()
    print(f"relay sent {sent}: shipped {[cmd.order_id for cmd in shipped]}")

    # 5. A relay that crashed before marking the row sends it again: the guard replays it.
    await bus.dispatch_message(row, commands)
    print(f"row sent again:        shipped {[cmd.order_id for cmd in shipped]} (replayed)")

    assert [cmd.order_id for cmd in shipped] == ["order-1"] and dead == [], (shipped, dead)
    assert (row.correlation_id, row.causation_id, row.aggregate_id) == (
        flow.correlation_id,
        paid.id,
        "order-1",
    )
    assert lineages == [Lineage(flow.correlation_id, row.id)], lineages
    print("\nok: one order, one ShipOrder, even with the payment delivered twice")


if __name__ == "__main__":
    asyncio.run(main())

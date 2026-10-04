"""Step 8: consume events from a broker, in order per account, in parallel across accounts.

A `KeyOrderedDispatcher` runs a pool of workers. The worker for an event is picked from its
aggregate id, so one account's events run one at a time, in order. Different accounts run in
parallel. An `EventRouter` hands each event to the handler for its name. A handler that returns acks
the delivery.

Run:  cd examples/py && uv run python 08_consume_events.py
"""

from __future__ import annotations

import asyncio
import contextlib
import json

from edax import EventMessage, EventRouter, KeyOrderedDispatcher, partition, testing
from edax.backend.memory import MemoryBroker

WORKERS = 3


def deposited(account_id: str, sequence: int, amount: int) -> EventMessage:
    data = json.dumps({"amount": amount}).encode()
    return testing.event(account_id, "Deposited", sequence, id=f"{account_id}-{sequence}", data=data)


async def main() -> None:
    # 1. Publish six events for three accounts. In a service the Relay (step 7) does this.
    broker = MemoryBroker()
    for sequence in (1, 2):
        for account_id in ("alice", "dave", "eve"):
            await broker.publish(deposited(account_id, sequence, amount=10 * sequence))

    # 2. Route each event to the handler for its name.
    seen: dict[str, list[int]] = {"alice": [], "dave": [], "eve": []}
    router = EventRouter()

    @router.on("Deposited")
    async def on_deposited(event: EventMessage) -> None:
        assert event.sequence is not None
        seen[event.aggregate_id].append(event.sequence)
        worker = partition(event.aggregate_id, WORKERS)
        print(f"  worker {worker}: {event.aggregate_id} #{event.sequence}")

    # 3. Start the workers, then let the broker feed them. Step 9 adds a dead-letter sink.
    dispatcher = KeyOrderedDispatcher(workers=WORKERS, handler=router.dispatch)
    dispatcher.start()
    consumer = asyncio.create_task(broker.run(dispatcher))

    # 4. Wait until every delivery is acked, then shut down.
    while not broker.empty():
        await asyncio.sleep(0.01)
    consumer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consumer
    await dispatcher.stop()

    print(f"per account: {seen}")
    assert seen == {"alice": [1, 2], "dave": [1, 2], "eve": [1, 2]}, seen
    print("\nok: every event handled and acked; each account's events stayed in order")


if __name__ == "__main__":
    asyncio.run(main())

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

from edax import Event, EventEnvelope, EventRouter, KeyOrderedDispatcher, partition
from edax.backend.memory import MemoryBroker

WORKERS = 3


def deposited(account_id: str, sequence: int, amount: int) -> EventEnvelope:
    return EventEnvelope(
        id=f"{account_id}-{sequence}",
        aggregate_id=account_id,
        event=Event("Deposited", 1, json.dumps({"amount": amount}).encode()),
        namespace={},
        metadata={},
        timestamp=0,
        sequence=sequence,
    )


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
    async def on_deposited(envelope: EventEnvelope) -> None:
        assert envelope.sequence is not None
        seen[envelope.aggregate_id].append(envelope.sequence)
        worker = partition(envelope.aggregate_id, WORKERS)
        print(f"  worker {worker}: {envelope.aggregate_id} #{envelope.sequence}")

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

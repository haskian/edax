"""Step 20: append events or replace complete read-model state in atomic batches.

Use append for an event archive; use upsert when each event contains the whole row. Command
handlers validate requests and produce events (steps 1 to 7). Ingestion projects those saved events.
Delta events such as AmountDeposited need the ordered reducer in step 21.

This example calls the sink directly with source coordinates. KafkaIngestion supplies this wiring
for Kafka. See docs/backends/ingestion.md for the runner and PostgreSQL setup. These memory stores
retain state and rejection records only while the process lives.

Run:  cd examples/py && uv run python 20_ingestion.py
"""

from __future__ import annotations

import asyncio
import json

from edax import EventMessage, testing
from edax.ingestion import BatchInput, IngestionTarget, MemoryIngestionSink, Rejection, RejectionOutbox
from edax.outbox import Relay
from edax.serializer import KernelSerializer


def order(sequence: int, status: str | None, *, event_id: str) -> EventMessage:
    value = None if status is None else {"order_id": "order-42", "status": status}
    return testing.event(
        "order-42", "OrderState", sequence, id=event_id, tenant="shop-eu", data=json.dumps(value).encode()
    )


def original(target: IngestionTarget, event: EventMessage, offset: int) -> BatchInput:
    """Keep the full event envelope and source bytes beside the mapped complete state."""
    wire = KernelSerializer().encode(event).decode()
    return target.input(
        topic="orders",
        partition=0,
        offset=offset,
        tenant=event.tenant,
        row_key=event.id if target.mode == "append" else event.aggregate_id,
        stream=event.aggregate_id,
        event_id=event.id,
        sequence=event.sequence,
        content=wire,
        value=json.loads(event.data),
        raw=json.dumps({"offset": offset, "event": wire}),
    )


async def main() -> None:
    paid = order(1, "paid", event_id="order-42-paid")

    # 1. Append identity is the event ID within its tenant. A new source offset can be a replay.
    archive = MemoryIngestionSink(IngestionTarget("append", "archive-v1", "orders-incarnation-1", "copy-v1"))
    conflict = order(1, "cancelled", event_id=paid.id)
    batch = [
        original(archive.target, paid, 0),
        original(archive.target, paid, 1),
        original(archive.target, conflict, 2),
    ]
    result = await archive.commit(batch)
    assert result.outcomes == ("accepted", "accepted", "rejected")
    saved = await archive.get("shop-eu", paid.id)
    assert saved is not None and saved["value"]["status"] == "paid"
    print(f"append: {result.outcomes}; the first event value stays paid")

    # 2. A Relay publishes committed rejections, then marks them. Production delivery must be durable.
    # The destination deduplicates by source if delivery succeeds but marking is interrupted.
    delivered: dict[str, str] = {}

    async def publish(rejection: Rejection) -> None:
        delivered.setdefault(rejection.source, rejection.error)
        assert rejection.raw == batch[2].raw

    assert list(await archive.pending()) == [batch[2].source]
    relay = Relay(RejectionOutbox(archive), publish)
    assert await relay.drain() == 1
    replay = await archive.commit(batch)
    assert replay.outcomes == result.outcomes
    assert replay.rejections[batch[2].source]["published"] is True
    assert await relay.drain() == 0 and len(delivered) == 1
    print("rejection: publication marked; replay keeps the rejection and does not republish")

    # 3. Upsert accepts a newer complete state and ignores an older sequence.
    view = MemoryIngestionSink(
        IngestionTarget("upsert", "orders-v1", "orders-incarnation-1", "order-state-v1")
    )
    shipped = order(2, "shipped", event_id="order-42-shipped")
    stale = original(view.target, paid, 1)
    assert (await view.commit([original(view.target, shipped, 0), stale])).outcomes == (
        "accepted",
        "accepted",
    )
    saved = await view.get("shop-eu", "order-42")
    assert saved is not None and saved["value"]["status"] == "shipped"

    # 4. None retains a deletion marker, sequence and stream owner. Replay cannot resurrect it.
    deleted = original(view.target, order(3, None, event_id="order-42-deleted"), 2)
    assert (await view.commit([deleted, stale, deleted])).outcomes == ("accepted",) * 3
    saved = await view.get("shop-eu", "order-42")
    assert saved is not None and saved["sequence"] == 3 and saved["value"] is None
    assert saved["stream"] == "order-42"
    print("upsert: shipped survived stale replay; deletion retained sequence 3")
    print("\nok: append replay, durable rejection decisions, stale upserts and deletion markers checked")


if __name__ == "__main__":
    asyncio.run(main())

"""Transactional outbox end to end on the in-memory broker: consume input -> handle -> emit via outbox.

    commands broker ─▶ poll ─▶ Delivery ─▶ KeyOrderedDispatcher ─▶ handler
                                            worker = partition(aggregate_id)   │
                                store.append(Deposited -> outbox, one tx) ◀────┘
                                ack input after the append
                                Relay.drain ─▶ events broker.publish

Same aggregate -> same worker, so the read-modify-write needs no lock. The input is acked only after
the outbox append (at-least-once). MemoryBroker is both Producer and Consumer; swap in
edax.backend.kafka over your own client for a real broker, the shape is identical.

Run (from examples/py):
  uv sync && uv run python outbox.py
"""

import asyncio
import json
from collections import defaultdict

import edax
from edax.backend.memory import MemoryBroker, MemoryEventStore

# acct-a and acct-b each get two deposits; per-aggregate order is what we verify.
REQUESTS = [("acct-a", 100), ("acct-b", 50), ("acct-a", 25), ("acct-b", 10)]


def _env(env_id: str, aggregate_id: str, name: str, seq: int, data: bytes) -> edax.Envelope:
    return edax.Envelope(
        id=env_id,
        aggregate_id=aggregate_id,
        event=edax.Event(name, 1, data),
        namespace={},
        metadata={},
        timestamp=0,
        sequence=seq,
    )


async def _poll(broker: MemoryBroker) -> edax.Delivery:
    """Next delivery. ``Consumer.poll`` returns ``None`` at end of stream; here every message was
    published a few lines earlier, so ``None`` would mean the broker dropped one."""
    delivery = await broker.poll()
    assert delivery is not None, "the broker had nothing to deliver"
    return delivery


async def main() -> None:
    store = MemoryEventStore()  # emitted domain events + outbox
    commands = MemoryBroker()  # input topic
    events = MemoryBroker()  # output topic

    # 1. publish input requests (plain producer path, no outbox)
    for i, (agg, amount) in enumerate(REQUESTS, 1):
        data = json.dumps({"amount": amount}).encode()
        await commands.publish(_env(f"{agg}-req-{i}", agg, "DepositRequested", i, data))
    print(f"published {len(REQUESTS)} requests")

    # 2. consume input, run the operation, append a domain event to the outbox
    balances: dict[str, int] = defaultdict(int)
    version: dict[str, int] = defaultdict(int)  # expected outbox version per aggregate

    async def handle(env: edax.Envelope) -> None:
        # same aggregate -> same worker -> serialized: no lock around this read-modify-write
        agg = env.aggregate_id
        balances[agg] += json.loads(env.event.data)["amount"]
        v = version[agg]
        data = json.dumps({"balance": balances[agg]}).encode()
        async with edax.transaction(store) as tx:
            await store.append(tx, agg, v, [_env(f"{agg}-ev-{v + 1}", agg, "Deposited", v + 1, data)])
        version[agg] = v + 1
        print(f"  handled {agg} -> balance {balances[agg]}")

    dispatcher = edax.KeyOrderedDispatcher(workers=4, handler=handle)
    dispatcher.start()
    for _ in REQUESTS:
        await dispatcher.submit(await _poll(commands))  # worker acks after the append
    await dispatcher.stop()

    # 3. drain the outbox to the events broker, then read it back to verify
    published = await edax.Relay(store, events.publish).drain()
    print(f"drained {published} domain events")
    assert published == len(REQUESTS), published

    seen: dict[str, list[int]] = defaultdict(list)
    for _ in range(published):
        d = await _poll(events)
        await d.ack()  # in-loop consumption: poll + ack directly, no dispatcher
        seen[d.envelope.aggregate_id].append(json.loads(d.envelope.event.data)["balance"])

    assert seen["acct-a"] == [100, 125], seen["acct-a"]
    assert seen["acct-b"] == [50, 60], seen["acct-b"]
    print("\nok: input consumed -> domain events emitted via the outbox, per-aggregate order kept")


if __name__ == "__main__":
    asyncio.run(main())

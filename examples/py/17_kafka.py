"""Step 17: publish and consume through Kafka.

The outbox Relay publishes with a `KafkaProducer`. A `KafkaConsumer` feeds a KeyOrderedDispatcher.
On Kafka a nack rewinds the whole partition, so `KafkaConsumer.run` refuses a dispatcher without
`on_dead`. A message that fails for good goes to a dead-letter topic through `KafkaDeadLetterSink`.

Set `EDAX_KAFKA_BOOTSTRAP` (for example `localhost:9092`) to use a real broker. Unset, the example
runs on `MemoryBroker`, which behaves like a Kafka topic.

Run:  cd examples/py && uv run python 17_kafka.py
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from edax import (
    DeadLetter,
    EventDraft,
    EventMessage,
    KeyOrderedDispatcher,
    Rejected,
    Relay,
    Undecodable,
    guid,
    json_codec,
    transaction,
)
from edax.backend.kafka import KafkaConsumer, KafkaDeadLetterSink
from edax.backend.memory import MemoryBroker, MemoryEventStore

ORDERS = [("order-1", 2), ("order-2", 0), ("order-3", 1)]  # order-2 is invalid: quantity 0


@dataclass
class OrderPlaced:
    quantity: int


# The writer's codec. The consumer decodes with the same one, which applies any upcast.
events = json_codec({"OrderPlaced": OrderPlaced})


class InvalidOrder(Rejected):
    """Terminal: the same order is invalid on every delivery."""


def order_placed(order_id: str, quantity: int) -> EventDraft:
    return EventDraft(
        id=f"{order_id}-placed",
        aggregate_id=order_id,  # the Kafka message key: one order stays on one partition
        sequence=1,
        name="OrderPlaced",
        version=1,
        data=events.marshal(OrderPlaced(quantity)).data,
        correlation_id=f"{order_id}-placed",
        causation_id=f"{order_id}-placed",
        extensions={},
        metadata={},
    )


async def run(
    publish: Callable[[EventMessage], Awaitable[None]],
    consumer: KafkaConsumer | MemoryBroker,
    sink: KafkaDeadLetterSink | None,
) -> None:
    # 1. Save the orders, then let the Relay publish them from the outbox.
    store = MemoryEventStore()
    async with transaction(store) as tx:
        for order_id, quantity in ORDERS:
            await store.append(tx, order_id, 0, [order_placed(order_id, quantity)])
    print(f"relay published {await Relay(store, publish).drain()} events")

    # 2. The handler, and a dead-letter route that records what it forwards.
    packed: list[str] = []
    dead: list[str] = []

    async def pack(event: EventMessage) -> None:
        order: OrderPlaced = events.unmarshal(event)
        if order.quantity < 1:
            raise InvalidOrder(f"{event.aggregate_id} has no items")
        packed.append(event.aggregate_id)
        print(f"  packed {event.aggregate_id}")

    async def to_dead_letters(letter: DeadLetter[EventMessage | Undecodable]) -> None:
        order = letter.message
        assert isinstance(order, EventMessage), "every record decoded"
        dead.append(order.aggregate_id)
        print(f"  dead-lettered {order.aggregate_id} at attempt {letter.attempt}: {letter.failure}")
        if sink is not None:
            await sink(letter)

    # 3. Consume until every order is packed or dead-lettered, then shut down.
    dispatcher = KeyOrderedDispatcher(workers=2, handler=pack, on_dead=to_dead_letters)
    dispatcher.start()
    task = asyncio.create_task(consumer.run(dispatcher))
    for _ in range(3000):  # up to 30 seconds
        if len(packed) + len(dead) == len(ORDERS):
            break
        await asyncio.sleep(0.01)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    await dispatcher.stop()

    assert (sorted(packed), dead) == (["order-1", "order-3"], ["order-2"]), (packed, dead)
    print("\nok: valid orders packed; the invalid one went to the dead-letter route")


async def run_on_kafka(bootstrap: str) -> None:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from edax.backend.kafka import KafkaProducer

    # Fresh topics per run. A real deployment creates its topics up front.
    topic = f"orders-{guid()}"
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    await admin.create_topics([NewTopic(topic, 1, 1), NewTopic(f"{topic}.dlq", 1, 1)])

    # enable_idempotence implies acks="all": the relay must not mark a row before every replica has it.
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, enable_idempotence=True)
    await producer.start()
    # No auto-commit: the adapter commits only what every handler has settled.
    client = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=f"{topic}-packing",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    consumer = KafkaConsumer(client)
    client.subscribe([topic], listener=consumer.rebalance_listener())
    await client.start()
    try:
        print(f"using Kafka at {bootstrap}, topic {topic}\n")
        await run(
            KafkaProducer(producer, topic).publish, consumer, KafkaDeadLetterSink(producer, f"{topic}.dlq")
        )
        await consumer.flush()  # commit what the workers settled before closing the client
    finally:
        await client.stop()
        await producer.stop()
        await admin.delete_topics([topic, f"{topic}.dlq"])
        await admin.close()


async def main() -> None:
    bootstrap = os.environ.get("EDAX_KAFKA_BOOTSTRAP")
    if bootstrap is None:
        print("EDAX_KAFKA_BOOTSTRAP is not set: using MemoryBroker, the in-memory stand-in\n")
        broker = MemoryBroker()
        await run(broker.publish, broker, sink=None)
        return
    await run_on_kafka(bootstrap)


if __name__ == "__main__":
    asyncio.run(main())

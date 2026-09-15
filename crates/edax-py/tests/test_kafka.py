"""Kafka adapter over a fake aiokafka client (no broker). Kafka-only semantics: the per-partition
commit watermark and the producer's key/topic; the seam contract itself is in
``test_broker_contract.py``."""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from typing import Any, Optional

import edax
import pytest
from conftest import FakeKafkaClient, KafkaMsg, TopicPartition, build_envelope, delivered
from edax.backend.kafka import KafkaConsumer, KafkaProducer
from edax.broker import UNDECODABLE


class _Serializer:
    """Decodes the fake client's payload (the message body doubles as the envelope id). The
    consumer never encodes, so `encode` exists only to complete the seam."""

    def encode(self, env: edax.EventEnvelope) -> bytes:
        return env.id.encode()

    def decode(self, wire: bytes) -> edax.EventEnvelope:
        return build_envelope("a", "Tick", 1, id=wire.decode())


async def _consumer(n: int) -> tuple[FakeKafkaClient, KafkaConsumer]:
    c = FakeKafkaClient()
    for i in range(n):
        await c.send_and_wait("t", str(i).encode())
    return c, KafkaConsumer(c, _Serializer())


async def test_out_of_order_ack_commits_watermark_only() -> None:
    c, k = await _consumer(3)
    d0, d1, d2 = [delivered(await k.poll()) for _ in range(3)]
    await d2.ack()
    await d1.ack()
    assert c.commits == [0]  # 0 still in flight: never commit past it
    await d0.ack()
    assert c.commits == [0, 3]


async def test_nack_keeps_offset_in_flight_until_redelivered() -> None:
    c, k = await _consumer(3)
    d0, d1, d2 = [delivered(await k.poll()) for _ in range(3)]
    await d1.ack()
    await d0.nack()
    await d2.ack()
    assert c.seeks == [0]
    assert max(c.commits) <= 0, "committed past nacked offset 0"
    redelivered = delivered(await k.poll())
    await redelivered.ack()
    assert c.commits[-1] == 3


async def test_second_nack_rewinds_to_the_lowest_unsettled_offset() -> None:
    # Two nacks out of order: seeking to the later offset would move the fetch position past the
    # earlier one, which would then never redeliver and would pin the watermark forever.
    c, k = await _consumer(3)
    d0, d1, d2 = [delivered(await k.poll()) for _ in range(3)]
    await d0.nack()
    await d2.nack()
    assert c.seeks == [0, 0]  # the second nack rewinds to 0, not to 2

    # Redelivery from 0 settles everything, so the watermark finally advances past the batch.
    for _ in range(3):
        await delivered(await k.poll()).ack()
    await d1.ack()
    assert c.commits[-1] == 3


class _MultiPartitionClient:
    """Two partitions of one topic in one fetch stream, recording commits and seeks per partition —
    what a single-partition fake cannot show."""

    def __init__(self, messages: list[KafkaMsg]) -> None:
        self._fetch: deque[KafkaMsg] = deque(messages)
        self.commits: list[tuple[Any, int]] = []
        self.seeks: list[tuple[Any, int]] = []

    async def getone(self) -> KafkaMsg:
        return self._fetch.popleft()

    async def commit(self, offsets: dict[Any, int]) -> None:
        self.commits.extend(offsets.items())

    def seek(self, tp: Any, offset: int) -> None:
        self.seeks.append((tp, offset))


async def test_watermark_is_tracked_per_partition() -> None:
    # Interleaved partitions: one partition's progress must not settle the other's, and each
    # partition's watermark must be committed against its own TopicPartition.
    c = _MultiPartitionClient(
        [
            KafkaMsg("t", 0, 0, b"p0-0"),
            KafkaMsg("t", 1, 0, b"p1-0"),
            KafkaMsg("t", 0, 1, b"p0-1"),
            KafkaMsg("t", 1, 1, b"p1-1"),
        ]
    )
    k = KafkaConsumer(c, _Serializer())
    p0_0, p1_0, p0_1, p1_1 = [delivered(await k.poll()) for _ in range(4)]
    tp0, tp1 = TopicPartition("t", 0), TopicPartition("t", 1)

    await p1_1.ack()  # partition 1 out of order: 0 is still in flight there
    assert c.commits == [(tp1, 0)]

    await p0_0.ack()  # partition 0 in order: 1 still in flight there
    assert c.commits == [(tp1, 0), (tp0, 1)]

    await p1_0.ack()  # partition 1 drained: one past its highest delivered
    assert c.commits[-1] == (tp1, 2)

    await p0_1.nack()
    assert c.seeks == [(tp0, 1)]  # only partition 0 rewinds
    assert (tp0, 2) not in c.commits, "committed past the nacked offset on partition 0"


class _SpyProducerClient:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []

    async def send_and_wait(self, topic: str, value: bytes, key: bytes) -> None:
        self.sent.append((topic, value, key))

    async def send(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("publish must await the broker ack (send_and_wait), not fire and forget")


@pytest.mark.parametrize(
    "key_fn, want_key",
    [
        (None, b"acct-1"),  # default: the aggregate id keys the partition, so per-key order holds
        (lambda env: env.namespace["tenant"], b"t1"),  # caller-supplied key
    ],
)
async def test_publish_sends_keyed_encoded_envelope(key_fn: Optional[Any], want_key: bytes) -> None:
    client, serializer = _SpyProducerClient(), edax.kernel()
    env = build_envelope("acct-1", "Deposited", 3, namespace={"tenant": "t1"})
    kwargs = {"key": key_fn} if key_fn is not None else {}
    await KafkaProducer(client, serializer, "events.account", **kwargs).publish(env)

    assert client.sent == [("events.account", serializer.encode(env), want_key)]


class _GatedCommitClient(FakeKafkaClient):
    """A client whose commits park on a per-watermark gate. A commit is a network round trip, and
    nothing makes two in-flight ones return in the order they were sent; the gates let the test
    pick that order instead of hoping for it."""

    def __init__(self) -> None:
        super().__init__()
        self.gates: dict[int, asyncio.Event] = defaultdict(asyncio.Event)

    async def commit(self, offsets: dict[Any, int]) -> None:
        (watermark,) = offsets.values()
        await self.gates[watermark].wait()
        self.commits.extend(offsets.values())


async def test_the_commit_guard_never_steps_backwards() -> None:
    """Two acks in flight at once, the higher watermark's commit returning first. The guard records
    the highest watermark committed, not the last one written, or it would let an already-committed
    watermark be sent again on every later ack."""
    c = _GatedCommitClient()
    for i in range(4):
        await c.send_and_wait("t", str(i).encode())
    k = KafkaConsumer(c, _Serializer())
    d0, d1, _d2, d3 = [delivered(await k.poll()) for _ in range(4)]
    tp = TopicPartition("t", 0)

    lower = asyncio.create_task(d0.ack())  # watermark 1: offsets 1..3 are still in flight
    await asyncio.sleep(0)  # let it compute its watermark and park on the gate
    higher = asyncio.create_task(d1.ack())  # watermark 2: offset 1 settled, 2 is now the lowest
    await asyncio.sleep(0)

    c.gates[2].set()
    await higher
    c.gates[1].set()
    await lower
    assert c.commits == [2, 1], "the two round trips really did return out of order"

    # Offset 2 is still in flight, so this ack computes watermark 2 again — already committed.
    c.gates[2].set()
    await d3.ack()
    assert c.commits == [2, 1], "re-committed a watermark the guard had already recorded"
    assert k._committed[tp] == 2


class _SchemaSerializer:
    """Decodes a numeric body. Anything else is a payload this consumer has no schema for: the
    poison message a producer rollout leaves on the topic."""

    def encode(self, env: edax.EventEnvelope) -> bytes:
        return env.id.encode()

    def decode(self, wire: bytes) -> edax.EventEnvelope:
        if not wire.isdigit():
            raise ValueError(f"no schema for {wire[:16]!r}")
        return build_envelope("acct-1", "Tick", 1, id=wire.decode())


POISON_BYTES = b'{"schema":"v3","balance_cents":4200}'


async def test_a_payload_that_will_not_decode_is_dead_lettered_and_the_watermark_advances() -> None:
    """The poison message: `decode` fails, so there is no envelope and no handler run. The offset
    is booked before the decode, so the loop can dead-letter the message and ack it — without that
    the watermark pins on the offset and the consumer replays the same failure after every restart.
    """
    c = FakeKafkaClient()
    for body in (b"0", POISON_BYTES, b"2"):
        await c.send_and_wait("t", body)

    handled: list[str] = []
    dead: list[tuple[str, bytes, str, str]] = []

    async def on_dead(delivery: edax.Delivery, err: BaseException) -> None:
        dead.append(
            (
                delivery.envelope.event.name,
                bytes(delivery.envelope.event.data),
                str(err),
                delivery.envelope.id,
            )
        )

    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=lambda env: handled.append(env.id), on_dead=on_dead
    )
    dispatcher.start()
    task = asyncio.create_task(KafkaConsumer(c, _SchemaSerializer()).run(dispatcher))
    for _ in range(200):  # bounded: a pinned watermark must fail the assertions, not hang the suite
        if c.settled():
            break
        await asyncio.sleep(0.005)
    task.cancel()
    await dispatcher.stop()

    assert handled == ["0", "2"], "the poison message must never reach a handler"
    assert len(dead) == 1
    name, payload, message, poison_id = dead[0]
    assert (name, payload) == (UNDECODABLE, POISON_BYTES)  # the raw bytes reach the dead-letter sink
    assert "offset 1 did not decode" in message
    assert poison_id == "t/0/1", "topic and partition qualify the offset"
    assert c.commits[-1] == 3, "the watermark never advanced past the poison message"


async def test_poison_ids_differ_across_partitions_at_one_offset() -> None:
    """The offset is per partition, so the bare offset is not an identity. Two poison messages at
    offset 0 on different partitions would share one envelope id, and a dead-letter queue keyed on it
    — the natural key, and the one `idempotent_by_id` uses — keeps the first and rejects the second
    as a `PayloadMismatch`. Silently: the first one wins.
    """
    c = _MultiPartitionClient([KafkaMsg("t", 0, 0, POISON_BYTES), KafkaMsg("t", 1, 0, POISON_BYTES)])
    k = KafkaConsumer(c, _SchemaSerializer())
    # The id is built in `poll`; the test above already pins that a poison delivery reaches `on_dead`
    # carrying it, so there is nothing a dispatcher and a consumer loop would add here.
    ids = [delivered(await k.poll()).envelope.id for _ in range(2)]

    assert ids == ["t/0/0", "t/1/0"], "one id per partition, not one shared offset"

"""Kafka adapter over the in-tree Kafka-shaped fake (no broker). Kafka-only semantics: the per-partition
commit watermark and the batched committer, the rewind a nack makes, the producer's key/topic/header/
partition and its durability probe, the serializer registry, the record middleware, the dead-letter
record, and the consumer loop under faults — a nack, a raising sink, a poison message, a killed consumer,
a rebalance, a broker that stops answering commits. The seam contract itself is in
``test_broker_contract.py``; the same faults against a real broker are in ``test_kafka_live.py``."""

from __future__ import annotations

import asyncio
import base64
import warnings
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

import edax
import pytest
from aiokafka import TopicPartition
from conftest import (
    FAKE_KAFKA_EPOCH_MS,
    FakeKafkaClient,
    FakeKafkaCluster,
    IdSerializer,
    KafkaMsg,
    Recording,
    SeqSerializer,
    SpyProducerClient,
    build_command,
    build_message,
    delivered,
    golden,
    kafka_consumer,
    publish_all,
    sequence_of,
    sink_ok,
)
from edax import _edax
from edax.backend.kafka import (
    CONTENT_TYPE,
    KafkaConsumer,
    KafkaDeadLetterSink,
    KafkaFetched,
    KafkaHeader,
    KafkaProducer,
    KafkaRecord,
    RecordMiddleware,
)
from edax.broker import UNDECODABLE, KeyFn, UndecodableDelivery

TP0 = TopicPartition("t", 0)


async def test_out_of_order_ack_commits_watermark_only() -> None:
    c, k = await kafka_consumer(3)
    d0, d1, d2 = [delivered(await k.poll()) for _ in range(3)]
    await d2.ack()
    await d1.ack()
    await k.flush()
    assert c.commits == [], "0 is still in flight: nothing moved past where the fetch started"
    await d0.ack()
    await k.flush()
    assert c.commits == [3]
    await k.flush()
    assert c.commits == [3], "nothing new is due, so no second round trip"


async def test_a_nack_rewinds_the_partition_and_retires_what_was_fetched() -> None:
    """The copies fetched after the nacked offset are dead: their tokens are retired, so the dispatcher
    skips them, and the seek brings them back behind it, in order."""
    c, k = await kafka_consumer(3)
    d0, d1, d2 = [delivered(await k.poll()) for _ in range(3)]
    await d1.ack()
    await d0.nack()
    assert c.seeks == [0]
    assert [d.live() for d in (d0, d1, d2)] == [False, False, False], "the rewind retired every token"
    await d2.ack()  # a stale ack: the delivery was retired
    await k.flush()
    assert c.commits == [], "committed past nacked offset 0"
    again = [delivered(await k.poll()) for _ in range(3)]
    assert [d.message.id for d in again] == ["0", "1", "2"], "redelivered in order, the acked 1 included"
    for d in again:
        assert d.live()
        await d.ack()
    await k.flush()
    assert c.commits == [3]


async def test_a_nack_after_a_rewind_seeks_nowhere() -> None:
    """The first nack already rewound to the lowest unsettled offset and retired every token, so a later
    nack of a delivery fetched before it is stale: it must not move the fetch position again."""
    c, k = await kafka_consumer(3)
    d0, _d1, d2 = [delivered(await k.poll()) for _ in range(3)]
    await d0.nack()
    await d2.nack()
    assert c.seeks == [0], "the stale nack sought"
    for _ in range(3):
        await delivered(await k.poll()).ack()
    await k.flush()
    assert c.commits == [3]


def _msg(topic: str, partition: int, offset: int, value: bytes) -> KafkaMsg:
    """A fetched record with no headers, the shape another writer on the topic leaves behind. The
    consumer falls back to its default serializer for it."""
    return KafkaMsg(topic, partition, offset, b"", value, [])


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
    # Interleaved partitions: one partition's progress must not settle the other's, each partition's
    # watermark is committed against its own TopicPartition, and a rewind stays in its partition.
    c = _MultiPartitionClient(
        [
            _msg("t", 0, 0, b"p0-0"),
            _msg("t", 1, 0, b"p1-0"),
            _msg("t", 0, 1, b"p0-1"),
            _msg("t", 1, 1, b"p1-1"),
        ]
    )
    k = KafkaConsumer(c, serializer=IdSerializer())
    p0_0, p1_0, p0_1, p1_1 = [delivered(await k.poll()) for _ in range(4)]
    tp0, tp1 = TopicPartition("t", 0), TopicPartition("t", 1)

    await p1_1.ack()  # partition 1 out of order: 0 is still in flight there
    await p0_0.ack()  # partition 0 in order: 1 still in flight there
    await k.flush()
    assert c.commits == [(tp0, 1)], "partition 1 has not moved past where it started"

    await p1_0.ack()  # partition 1 drained: one past its highest delivered
    await k.flush()
    assert c.commits[-1] == (tp1, 2)

    await p0_1.nack()
    assert c.seeks == [(tp0, 1)]  # only partition 0 rewinds
    await k.flush()
    assert (tp0, 2) not in c.commits, "committed past the nacked offset on partition 0"


async def test_a_stale_nack_seeks_nowhere() -> None:
    """A delivery settled once is stale: a late nack from it must not move the fetch position, or it
    would rewind a partition over offsets another delivery already owns."""
    c, k = await kafka_consumer(1)
    d = delivered(await k.poll())
    await d.ack()
    await d.nack()
    assert c.seeks == [], "the stale token sought"
    await k.flush()
    assert c.commits == [1]


@pytest.mark.parametrize(
    "key_fn, want_key",
    [
        (None, b"acct-1"),  # default: the aggregate id keys the partition, so per-key order holds
        (lambda env: env.metadata["tenant"], b"t1"),  # caller-supplied key
    ],
)
async def test_publish_sends_keyed_encoded_message(key_fn: KeyFn | None, want_key: bytes) -> None:
    client, serializer = SpyProducerClient(), edax.KernelSerializer()
    env = build_message("acct-1", "Deposited", 3, metadata={"tenant": "t1"})
    await KafkaProducer(client, "events.account", serializer=serializer, key=key_fn).publish(env)

    assert len(client.sent) == 1
    topic, value, key, partition, headers = client.sent[0]
    assert (topic, value, key) == ("events.account", serializer.encode(env), want_key)
    # The partition is the kernel's (Java's murmur2 over the key), never the client's partitioner.
    assert partition == _edax.kafka_partition_for(want_key, 12)
    # The one contract header: the media type a consumer routes on, under the kernel's name. The id,
    # event name and version travel in the value, which every reader decodes anyway.
    assert headers == [(CONTENT_TYPE, b"application/vnd.edax.event+json")]
    assert CONTENT_TYPE == "content-type"


class _KeyFromHex:
    """A middleware that replaces the record key with raw bytes, which a key function (text) cannot
    produce: placement must follow the key the broker stores, after every middleware ran."""

    def __init__(self, key: bytes) -> None:
        self.key = key

    def on_publish(self, record: KafkaRecord) -> KafkaRecord:
        return KafkaRecord(self.key, record.value, record.headers)

    def on_poll(self, record: KafkaRecord) -> KafkaRecord:
        return record


_PLACEMENT = [c for c in golden("kafka_partition.json") if c["error"] is None]


@pytest.mark.parametrize("case", _PLACEMENT, ids=lambda c: f"{c['name']}/{c['partitions']}")
async def test_a_python_producer_places_a_key_where_java_and_rust_do(case: dict[str, Any]) -> None:
    """Mixed-language producers on one topic: the Rust kernel computed every row, Kafka's own Java
    ``UtilsTest`` numbers back six of them, and this producer must send each key to the same partition.
    One disagreement splits an aggregate across partitions and loses its order silently."""
    key = bytes.fromhex(case["key_hex"])
    client = SpyProducerClient(case["partitions"])
    producer = KafkaProducer(client, "t", serializer=edax.KernelSerializer(), middleware=[_KeyFromHex(key)])
    await producer.publish(build_message("acct-1", "Deposited", 1))
    (_, _, sent_key, partition, _) = client.sent[0]
    assert (sent_key, partition) == (key, case["expected"])
    if case["java_murmur2"] is not None:
        assert partition == (case["java_murmur2"] & 0x7FFFFFFF) % case["partitions"], "Java's own number"


async def test_every_event_of_an_aggregate_lands_on_one_partition() -> None:
    """Per-key order needs one partition per aggregate across every publish, whatever the event."""
    client = SpyProducerClient(7)
    producer = KafkaProducer(client, "t")
    for seq in range(1, 6):
        for agg in ("acct-1", "acct-2", "acct-3"):
            await producer.publish(build_message(agg, "Deposited", seq))
    by_key: dict[bytes, set[int]] = defaultdict(set)
    for _, _, key, partition, _ in client.sent:
        by_key[key].add(partition)
    assert {k: len(v) for k, v in by_key.items()} == {b"acct-1": 1, b"acct-2": 1, b"acct-3": 1}


async def test_a_topic_reported_with_no_partitions_is_refused_and_sends_nothing() -> None:
    client = SpyProducerClient(0)
    with pytest.raises(edax.CodecError, match="at least one partition"):
        await KafkaProducer(client, "t").publish(build_message("acct-1", "Deposited", 1))
    assert client.sent == []


class _FrozenBroker(SpyProducerClient):
    """A broker that stopped answering, as an idempotent ``AIOKafkaProducer`` sees it: it never expires
    the batch, so the send waits forever."""

    async def send_and_wait(self, *args: Any, **kwargs: Any) -> None:
        await asyncio.Event().wait()


@pytest.mark.parametrize("side", ["producer", "dead_letter_sink"])
async def test_a_send_to_a_frozen_broker_times_out_as_transient(side: str) -> None:
    """Both senders bound the send, so a relay or a dispatcher stuck on a frozen broker gets a failure to
    retry instead of a hang. The failure is transient, because the next try may find the broker back."""
    client = _FrozenBroker(3)
    msg = build_message("acct-1", "Deposited", 1)
    if side == "producer":
        send = KafkaProducer(client, "orders", publish_timeout_s=0.01).publish(msg)
    else:
        fetched = KafkaFetched(
            "orders", 2, 41, b"acct-1", b'{"id":"x"}', ((CONTENT_TYPE, b"application/json"),)
        )
        delivery = edax.Delivery(msg, lambda: None, lambda: None, origin=fetched)
        sink = KafkaDeadLetterSink(client, "orders.dlq", publish_timeout_s=0.01)
        send = sink(delivery, edax.CodecError("bad"))
    with pytest.raises(TimeoutError, match=r"no ack within 0\.01s; the record may still land") as raised:
        await send
    assert not edax.errors.is_terminal(raised.value)


def test_two_serializers_for_one_media_type_are_refused() -> None:
    """One topic, one decoder per media type: a second claimant would decode typed records one way and
    header-less records another."""
    with pytest.raises(edax.AlreadyRegistered, match="application/x-test-id"):
        KafkaConsumer(FakeKafkaClient(), serializer=IdSerializer(), serializers=[IdSerializer()])
    with pytest.raises(edax.AlreadyRegistered):
        KafkaConsumer(
            FakeKafkaClient(), serializers=[edax.KernelSerializer()]
        )  # the default already claims json


class _GatedCommitClient(FakeKafkaClient):
    """A client whose commits park on a per-watermark gate. A commit is a network round trip, and
    nothing makes two in-flight ones return in the order they were sent; the gates let the test
    pick that order instead of hoping for it."""

    def __init__(self) -> None:
        super().__init__()
        self.gates: dict[int, asyncio.Event] = defaultdict(asyncio.Event)

    async def commit(self, offsets: dict[TopicPartition, int]) -> None:
        (watermark,) = offsets.values()
        await self.gates[watermark].wait()
        await super().commit(offsets)


async def test_the_commit_guard_never_steps_backwards() -> None:
    """Two flushes in flight at once — the committer's and a rebalance's — the higher watermark's
    commit returning first. The guard records the highest watermark committed, not the last one
    written, or the lower one would be due, and sent, again."""
    c = _GatedCommitClient()
    producer = KafkaProducer(c, "t", serializer=IdSerializer())
    for i in range(4):
        await producer.publish(build_message("a", "Tick", 1, id=str(i)))
    k = KafkaConsumer(c, serializer=IdSerializer())
    d0, d1, _d2, d3 = [delivered(await k.poll()) for _ in range(4)]

    await d0.ack()
    lower = asyncio.create_task(k.flush())  # watermark 1: offsets 1..3 are still in flight
    await asyncio.sleep(0)  # let it read what is due and park on the gate
    await d1.ack()
    higher = asyncio.create_task(k.flush())  # watermark 2: offset 1 settled, 2 is now the lowest
    await asyncio.sleep(0)

    c.gates[2].set()
    await higher
    c.gates[1].set()
    await lower
    assert c.commits == [2, 1], "the two round trips really did return out of order"
    assert k.stats()[TP0].committed == 2

    # Offset 2 is still in flight, so the watermark is 2 again — already committed, so nothing is due.
    await d3.ack()
    await k.flush()
    assert c.commits == [2, 1], "re-committed a watermark the guard had already recorded"


class _SchemaSerializer:
    """Decodes a numeric body. Anything else is a payload this consumer has no schema for: the
    poison message a producer rollout leaves on the topic. It refuses with a plain ``ValueError``,
    the way a third-party serializer raises its own error type rather than the SDK's."""

    media_type = "application/x-test-schema"
    kind: Literal["event"] = "event"

    def encode(self, env: edax.EventMessage | edax.CommandMessage) -> bytes:
        return env.id.encode()

    def decode(self, wire: bytes) -> edax.EventMessage:
        if not wire.isdigit():
            raise ValueError(f"no schema for {wire[:16]!r}")
        return build_message("acct-1", "Tick", 1, id=wire.decode())


POISON_BYTES = b'{"schema":"v3","balance_cents":4200}'


async def test_a_payload_that_will_not_decode_is_dead_lettered_and_the_watermark_advances() -> None:
    """The poison message: `decode` fails, so there is no message and no handler run. The offset
    is booked before the decode, so the loop can dead-letter the message and ack it — without that
    the watermark pins on the offset and the consumer replays the same failure after every restart.
    """
    c = FakeKafkaClient()
    producer = KafkaProducer(c, "t", serializer=_SchemaSerializer())
    await producer.publish(build_message("acct-1", "Tick", 1, id="0"))
    await c.send_and_wait("t", POISON_BYTES)  # another writer's bytes, no headers of ours
    await producer.publish(build_message("acct-1", "Tick", 1, id="2"))

    handled: list[str] = []
    dead: list[tuple[str, bytes, str, str]] = []

    async def on_dead(delivery: edax.Delivery, err: BaseException) -> None:
        dead.append(
            (
                delivery.message.name,
                bytes(delivery.message.data),
                str(err),
                delivery.message.id,
            )
        )

    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=lambda env: handled.append(env.id), on_dead=on_dead
    )
    dispatcher.start()
    consumer = KafkaConsumer(c, serializer=_SchemaSerializer(), commit_interval_s=0.001)
    task = asyncio.create_task(consumer.run(dispatcher))
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
    assert "t/0/1 did not decode" in message
    assert poison_id == "t/0/1", "topic and partition qualify the offset"
    assert c.commits[-1] == 3, "the watermark never advanced past the poison message"


@pytest.mark.parametrize(
    "branch, key, value, headers, want",
    [
        # A foreign header with a null value rides along: the record still decodes, and the null crosses
        # as empty bytes, the kernel record's rule, so a dead letter carries it too.
        pytest.param(
            "null_foreign_header", b"a", b"0", [("x-null", None)], ("ok", "0"), id="null_foreign_header"
        ),
        # A null content-type is an empty one: poison, never the default — the kernel's route.
        pytest.param(
            "null_content_type", b"a", b"0", [(CONTENT_TYPE, None)], ("poison", "''"), id="null_content_type"
        ),
        # A tombstone from another writer: no value at all is a payload that will not decode.
        pytest.param("null_value", b"a", None, [], ("poison", "t/0/0"), id="tombstone"),
        # A keyless record decodes by its value alone.
        pytest.param("null_key", None, b"0", [], ("ok", "0"), id="keyless"),
    ],
)
async def test_nullable_record_parts_cross_as_empty_bytes(
    branch: str, key: bytes | None, value: bytes | None, headers: list[Any], want: tuple[str, str]
) -> None:
    """Kafka's key, value and header values are nullable, and aiokafka hands a null over as ``None``. The
    kernel record has no nulls, so each crosses as empty bytes; before the shell did that, a null header
    raised after the offset was booked."""
    c = FakeKafkaClient()
    c.log.append(KafkaMsg("t", 0, 0, key, value, headers))
    delivery = delivered(await KafkaConsumer(c, serializer=_SchemaSerializer()).poll())
    kind, text = want
    fetched = delivery.origin
    assert isinstance(fetched, KafkaFetched), branch
    assert (fetched.key, fetched.value) == (key or b"", value or b""), branch
    assert all(v is not None for _, v in fetched.headers), branch
    if kind == "ok":
        assert not isinstance(delivery, UndecodableDelivery), branch
        assert delivery.message.id == text, branch
    else:
        assert isinstance(delivery, UndecodableDelivery), branch
        assert text in str(delivery.error), branch


async def test_poison_ids_differ_across_partitions_at_one_offset() -> None:
    """The offset is per partition, so the bare offset is not an identity. Two poison messages at
    offset 0 on different partitions would share one message id, and a dead-letter queue keyed on it
    — the natural key, and the one `idempotent_by_id` uses — keeps the first and rejects the second
    as a `PayloadMismatch`. Silently: the first one wins.
    """
    c = _MultiPartitionClient([_msg("t", 0, 0, POISON_BYTES), _msg("t", 1, 0, POISON_BYTES)])
    k = KafkaConsumer(c, serializer=_SchemaSerializer())
    # The id is built in `poll`; the test above already pins that a poison delivery reaches `on_dead`
    # carrying it, so there is nothing a dispatcher and a consumer loop would add here.
    ids = [delivered(await k.poll()).message.id for _ in range(2)]

    assert ids == ["t/0/0", "t/1/0"], "one id per partition, not one shared offset"


# =========================================================================
# serializer registry: which serializer a record's `content-type` routes to
# =========================================================================


class _UpperSerializer:
    """A second serializer on the same topic, under a media type of its own. Its decode tags the id,
    so the message names the serializer that produced it and a wrong route cannot pass unseen."""

    media_type = "application/x-test-upper"
    kind: Literal["event"] = "event"

    def encode(self, env: edax.EventMessage | edax.CommandMessage) -> bytes:
        return env.id.upper().encode()

    def decode(self, wire: bytes) -> edax.EventMessage:
        return build_message("a", "Tick", 1, id=f"upper:{wire.decode()}")


@pytest.mark.parametrize(
    "branch, headers, want_id, want_poison",
    [
        pytest.param(
            "no_content_type_uses_the_default",
            [],
            "evt-1",
            None,
            id="header_less_record_uses_the_default",
        ),
        pytest.param(
            "a_registered_media_type_routes_to_its_serializer",
            [("content-type", b"application/x-test-upper")],
            "upper:evt-1",
            None,
            id="registered_media_type_routes",
        ),
        pytest.param(
            "an_unclaimed_media_type_is_poison",
            [("content-type", b"application/x-protobuf")],
            None,
            "no serializer for content type 'application/x-protobuf'",
            id="unknown_media_type_is_poison",
        ),
        # An empty header must not fall back to the default serializer; the kernel refuses it.
        pytest.param(
            "an_empty_media_type_is_poison",
            [("content-type", b"")],
            None,
            "no serializer for content type ''",
            id="empty_media_type_is_poison",
        ),
        pytest.param(
            "an_upper_case_name_is_not_the_content_type",
            [("Content-Type", b"application/x-test-upper")],
            "evt-1",
            None,
            id="upper_case_name_uses_the_default",
        ),
        pytest.param(
            "parameters_are_not_stripped",
            [("content-type", b"application/x-test-upper; charset=utf-8")],
            None,
            "no serializer for content type 'application/x-test-upper; charset=utf-8'",
            id="parameterized_media_type_is_poison",
        ),
        pytest.param(
            "the_default_claims_its_own_media_type",
            [("content-type", b"application/x-test-id")],
            "evt-1",
            None,
            id="default_media_type_routes_to_the_default",
        ),
    ],
)
async def test_serializer_registry_branches(
    branch: str,
    headers: list[tuple[str, bytes]],
    want_id: str | None,
    want_poison: str | None,
) -> None:
    """One record, one header changed per case. A media type no serializer claims is never decoded
    with the default: that fallback is how a record in another format reaches a handler as a wrong
    message instead of the dead-letter sink."""
    client = FakeKafkaClient()
    client.log.append(KafkaMsg("t", 0, 0, b"a", b"evt-1", headers))
    consumer = KafkaConsumer(client, serializer=IdSerializer(), serializers=[_UpperSerializer()])

    delivery = delivered(await consumer.poll())

    if want_poison is None:
        assert not isinstance(delivery, UndecodableDelivery), branch
        assert delivery.message.id == want_id, branch
    else:
        assert isinstance(delivery, UndecodableDelivery), branch
        assert want_poison in str(delivery.error), branch
        assert delivery.message.id == "t/0/0", branch
    # Booked before the decode either way, so settling a poison record moves the watermark past it
    # instead of pinning the partition on bytes that will never decode.
    await delivery.ack()
    await consumer.flush()
    assert client.commits == [1], branch


# =========================================================================
# record middleware: publish in order, poll in reverse
# =========================================================================


def _with_content_type(headers: list[KafkaHeader], content_type: str) -> list[KafkaHeader]:
    """The same headers with ``content-type`` replaced — the swap a compressing middleware makes."""
    return [
        KafkaHeader(h.key, content_type.encode() if h.key == "content-type" else h.value) for h in headers
    ]


class _Tag:
    """Records the order it ran in and adds a header of its own on the way out, so one stack shows
    both directions at once."""

    def __init__(self, name: str, calls: list[str]) -> None:
        self._name = name
        self._calls = calls

    def on_publish(self, record: KafkaRecord) -> KafkaRecord:
        self._calls.append(f"publish:{self._name}")
        headers = [*record.headers, KafkaHeader(f"x-{self._name}", b"1")]
        return KafkaRecord(record.key, record.value, headers)

    def on_poll(self, record: KafkaRecord) -> KafkaRecord:
        self._calls.append(f"poll:{self._name}")
        return record


class _Boxing:
    """A compression stand-in: it rewrites the value and the ``content-type`` on the way out and
    restores both on the way in, so the consumer still routes to the serializer that wrote the bytes."""

    BOXED = "application/x-test-boxed"

    def __init__(self, inner_content_type: str, calls: list[str]) -> None:
        self._inner = inner_content_type
        self._calls = calls

    def on_publish(self, record: KafkaRecord) -> KafkaRecord:
        self._calls.append("publish:box")
        value = base64.b64encode(record.value)
        return KafkaRecord(record.key, value, _with_content_type(record.headers, self.BOXED))

    def on_poll(self, record: KafkaRecord) -> KafkaRecord:
        self._calls.append("poll:box")
        value = base64.b64decode(record.value)
        return KafkaRecord(record.key, value, _with_content_type(record.headers, self._inner))


class _Raising:
    """Publishes untouched and fails on the way back, the shape a decrypt hook takes when its key
    is wrong (terminal) or its key service is down (transient)."""

    def __init__(self, error: BaseException, calls: list[str]) -> None:
        self._error = error
        self._calls = calls

    def on_publish(self, record: KafkaRecord) -> KafkaRecord:
        self._calls.append("publish:raise")
        return record

    def on_poll(self, record: KafkaRecord) -> KafkaRecord:
        raise self._error


Stack = Callable[[list[str]], list[RecordMiddleware]]

ID_TYPE = IdSerializer.media_type


#: What the consumer made of the record coming back: the decoded message id, the text of the poison
#: error the delivery carries, or the exception type ``poll`` propagated instead of returning.
Want = tuple[str, Any]


@pytest.mark.parametrize(
    "branch, stack, want_calls, want_wire, want",
    [
        pytest.param(
            "publish_runs_in_order_and_poll_in_reverse",
            lambda calls: [_Tag("a", calls), _Tag("b", calls)],
            ["publish:a", "publish:b", "poll:b", "poll:a"],
            (ID_TYPE, b"evt-1", ["x-a", "x-b"]),
            ("ok", "evt-1"),
            id="order_is_onion",
        ),
        pytest.param(
            "a_rewritten_content_type_and_value_round_trip",
            lambda calls: [_Boxing(ID_TYPE, calls)],
            ["publish:box", "poll:box"],
            (_Boxing.BOXED, base64.b64encode(b"evt-1"), []),
            ("ok", "evt-1"),
            id="round_trip",
        ),
        pytest.param(
            "a_codec_error_on_poll_is_poison",
            lambda calls: [_Raising(edax.CodecError("cannot unbox"), calls)],
            ["publish:raise"],
            (ID_TYPE, b"evt-1", []),
            ("poison", "cannot unbox"),
            id="codec_error_is_poison",
        ),
        pytest.param(
            "any_other_error_on_poll_propagates",
            lambda calls: [_Raising(RuntimeError("key service unreachable"), calls)],
            ["publish:raise"],
            (ID_TYPE, b"evt-1", []),
            ("raises", RuntimeError),
            id="other_error_propagates",
        ),
    ],
)
async def test_record_middleware_branches(
    branch: str,
    stack: Stack,
    want_calls: list[str],
    want_wire: tuple[str, bytes, list[str]],
    want: Want,
) -> None:
    """One stack published and polled through the same fake, so each case pins what its middleware
    changed on the wire and what the consumer made of it coming back."""
    calls: list[str] = []
    middleware = stack(calls)
    client = FakeKafkaClient()
    await KafkaProducer(client, "t", serializer=IdSerializer(), middleware=middleware).publish(
        build_message("a", "Tick", 1, id="evt-1")
    )

    want_content_type, want_value, want_extra = want_wire
    sent = client.log[0]
    assert (dict(sent.headers)["content-type"].decode(), sent.value) == (
        want_content_type,
        want_value,
    ), branch
    assert [k for k, _ in sent.headers if k.startswith("x-")] == want_extra, branch

    consumer = KafkaConsumer(client, serializer=IdSerializer(), middleware=middleware)
    kind, expected = want
    if kind == "raises":
        with pytest.raises(expected):
            await consumer.poll()
        assert calls == want_calls, branch
        # Nacked, not dropped: the fetch position is back on the record so it redelivers, and the
        # offset stays in flight so no later ack commits past it.
        assert (client.seeks, client.cursor) == ([0], 0), branch
        assert consumer.stats()[TopicPartition("t", 0)].unsettled == 1, branch
        return

    delivery = delivered(await consumer.poll())
    assert calls == want_calls, branch
    if kind == "ok":
        assert not isinstance(delivery, UndecodableDelivery), branch
        assert delivery.message.id == expected, branch
    else:
        assert isinstance(delivery, UndecodableDelivery), branch
        assert expected in str(delivery.error), branch


# =========================================================================
# client configuration probes
# =========================================================================


@dataclass(frozen=True)
class ProbeCase:
    """One client configuration, and what building the adapter over it does."""

    side: str  # "producer" or "consumer"
    client: dict[str, Any]  # FakeKafkaClient keyword arguments; {} is aiokafka's safe shape
    raises: str | None = None  # a ValueError matching this
    warns: str | None = None  # a UserWarning matching this


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(ProbeCase("producer", {"acks": -1}), id="producer_acks_all_is_quiet"),
        pytest.param(ProbeCase("producer", {"acks": 1}, warns="acks=1"), id="producer_leader_ack_warns"),
        pytest.param(ProbeCase("producer", {"acks": 0}, warns="acks=0"), id="producer_no_ack_warns"),
        pytest.param(ProbeCase("consumer", {}), id="consumer_manual_commit_with_a_group_is_accepted"),
        pytest.param(
            ProbeCase("consumer", {"enable_auto_commit": True}, raises="enable_auto_commit=False"),
            id="consumer_auto_commit_is_refused",
        ),
        pytest.param(
            ProbeCase("consumer", {"group_id": None}, raises="group_id"),
            id="consumer_without_a_group_is_refused",
        ),
    ],
)
def test_client_configuration_probes(case: ProbeCase) -> None:
    """The settings that lose data or never commit are checked when the adapter is built, not discovered
    in production: auto-commit commits past in-flight work, no group fails every commit, and an ack from
    the leader alone loses an outbox event on failover."""
    client = FakeKafkaClient(**case.client)
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        if case.raises is not None:
            with pytest.raises(ValueError, match=case.raises):
                KafkaConsumer(client)
        elif case.side == "consumer":
            KafkaConsumer(client)
        else:
            KafkaProducer(client, "t")
            KafkaDeadLetterSink(client, "t.dlq")
    messages = [str(w.message) for w in seen]
    if case.warns is None:
        assert messages == []
    else:
        assert len(messages) == 2 and all(case.warns in m for m in messages), "producer and sink both warn"


def test_a_client_that_is_not_aiokafka_is_not_judged() -> None:
    """The probes read aiokafka's private attributes; a client without them passes."""
    KafkaProducer(SpyProducerClient(), "t")
    KafkaConsumer(_MultiPartitionClient([]))


# =========================================================================
# the consumer loop under faults, on the Kafka-shaped fake
# =========================================================================


async def _drive(
    consumer: KafkaConsumer,
    dispatcher: edax.KeyOrderedDispatcher,
    done: Callable[[], bool],
    *,
    limit: float = 3.0,
) -> asyncio.Task[None]:
    """Run ``consumer`` into ``dispatcher`` until ``done`` or ``limit`` seconds, then cancel the run and
    stop the dispatcher. Bounded, so a stall fails the test's assertions instead of hanging the suite.
    Returns the finished run task, so a test can read how it ended."""
    dispatcher.start()
    task = asyncio.create_task(consumer.run(dispatcher))
    loop = asyncio.get_running_loop()
    deadline = loop.time() + limit
    while not done() and not task.done() and loop.time() < deadline:
        await asyncio.sleep(0.002)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await dispatcher.stop(timeout=1)
    return task


async def test_a_nack_keeps_per_key_order_so_the_staleness_guard_loses_nothing(slept: list[int]) -> None:
    """Seq 1 fails once. If the worker then ran the seq 2 copy already queued, the guard would apply it
    and skip the redelivered seq 1 as stale for good. The rewind retires that copy, so the redelivered
    seq 1 runs first."""
    client = FakeKafkaClient()
    await publish_all(client, "t", SeqSerializer(), [build_message("a", "Tick", seq) for seq in (1, 2)])
    stored: int | None = None
    applied: list[int] = []
    runs: list[int] = []

    async def handler(env: edax.EventMessage) -> None:
        nonlocal stored
        runs.append(sequence_of(env))
        if env.sequence == 1 and runs.count(1) == 1:
            # Barrier: fail only once seq 2 is fetched and queued behind seq 1 on this worker, the
            # interleaving that lost seq 1. Waiting for it, not sleeping, makes it certain.
            while client.cursor < 2:
                await asyncio.sleep(0)
            raise edax.StoreError("projection db blip")
        if not edax.is_stale(stored, env.sequence):
            applied.append(sequence_of(env))
            stored = edax.advance(stored, env.sequence)

    observer = Recording()
    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler, on_dead=sink_ok, observer=observer)
    consumer = KafkaConsumer(client, serializer=SeqSerializer(), commit_interval_s=0.001)
    await _drive(consumer, dispatcher, client.settled)

    assert applied == [1, 2], "seq 1 was lost or applied after seq 2"
    assert runs == [1, 1, 2], "the stale seq 2 copy ran ahead of the redelivered seq 1"
    assert [d[1] for d in observer.deliveries] == ["nack", "stale", "ack", "ack"]
    assert slept == [0]
    assert client.settled()


async def test_a_raising_sink_nacks_and_gets_another_try(slept: list[int]) -> None:
    """The sink raises once on offset 3. Left unsettled, the delivery would wait for a redelivery Kafka
    never makes, and the partition's commits would stop at 3 for good. It nacks through the backoff
    path instead, the redelivery reaches the sink again, and the partition drains."""
    client = FakeKafkaClient()
    await publish_all(
        client, "t", IdSerializer(), [build_message(f"k{i}", "Tick", 1, id=str(i)) for i in range(10)]
    )
    sink_calls: list[str] = []
    seen: list[str] = []

    def handler(env: edax.EventMessage) -> None:
        seen.append(env.id)
        if env.id == "3":
            raise edax.CodecError("bad payload")  # terminal: dead-letters on the first attempt

    async def on_dead(delivery: edax.Delivery, err: BaseException) -> None:
        sink_calls.append(delivery.message.id)
        if len(sink_calls) == 1:
            raise ConnectionError("DLQ topic unavailable")

    observer = Recording()
    dispatcher = edax.KeyOrderedDispatcher(workers=2, handler=handler, on_dead=on_dead, observer=observer)
    consumer = KafkaConsumer(client, serializer=IdSerializer(), commit_interval_s=0.001)
    await _drive(consumer, dispatcher, client.settled)

    assert sink_calls == ["3", "3"], "the sink got exactly one more try"
    outcomes = [d[1] for d in observer.deliveries]
    assert outcomes.count("sink_failed") == 1 and outcomes.count("dead") == 1
    assert client.settled() and client.commits[-1] == 10, "the partition drained past the failed sink"
    assert slept[:1] == [0], "the sink failure waited one backoff step before the nack"
    assert seen.count("3") == 2 and all(seen.count(str(i)) >= 1 for i in range(10))


async def test_a_poison_message_with_a_failing_sink_backs_off_every_retry(slept: list[int]) -> None:
    """A poison nack with no backoff spins about 36,000 seek-and-fetch cycles a second. Every nack waits a
    growing step first, so the rate is the backoff schedule's."""
    client = FakeKafkaClient()
    await client.send_and_wait("t", POISON_BYTES, key=b"a")
    calls = 0

    async def on_dead(delivery: edax.Delivery, err: BaseException) -> None:
        nonlocal calls
        calls += 1
        raise ConnectionError("DLQ topic unavailable")

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=lambda env: None, on_dead=on_dead)
    consumer = KafkaConsumer(client, serializer=_SchemaSerializer())
    await _drive(consumer, dispatcher, lambda: calls >= 5)

    assert calls >= 5
    assert len(client.seeks) == len(slept) and len(slept) >= 4, "a nack without its backoff wait"
    assert slept == list(range(len(slept))), "the step grows with every failed try of one poison id"
    assert client.commits == []


@pytest.mark.parametrize(
    ("wiring", "on_dead", "raises"),
    [
        pytest.param("subscribed_with_a_listener", None, "on_dead", id="a_dispatcher_without_a_sink"),
        pytest.param(
            "subscribed_without_a_listener",
            sink_ok,
            "without a rebalance listener",
            id="a_client_subscribed_without_a_listener",
        ),
        pytest.param(
            "assigned_by_hand", sink_ok, None, id="a_client_assigned_by_hand_without_a_listener_runs"
        ),
    ],
)
async def test_run_refuses_only_a_wiring_that_stalls(
    wiring: str,
    on_dead: Callable[[edax.Delivery, BaseException], Awaitable[None]] | None,
    raises: str | None,
) -> None:
    """On Kafka a nack rewinds, so a message that fails for good with no sink stalls its partition
    forever. A client the group assigns partitions to, with no listener, keeps a revoked partition's
    bookkeeping, and the client then refuses every commit that names it. The run refuses both before
    it polls anything. A client that took its partitions by hand has no group to revoke them, so it
    runs without a listener."""
    cluster = FakeKafkaCluster()
    client = FakeKafkaClient(cluster)
    await publish_all(client, "t", IdSerializer(), [build_message("a", "Tick", 1, id="0")])
    consumer = KafkaConsumer(client, serializer=IdSerializer())
    if wiring == "assigned_by_hand":
        client.assign([TP0])
    else:
        listener = consumer.rebalance_listener() if wiring == "subscribed_with_a_listener" else None
        client.subscribe(["t"], listener=listener)
        await cluster.join(client)
    handled: list[str] = []
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=lambda env: handled.append(env.id), on_dead=on_dead
    )
    if raises is not None:
        with pytest.raises(ValueError, match=raises):
            await consumer.run(dispatcher)
        assert client.cursor == 0, "nothing was polled"
        return
    run = await _drive(consumer, dispatcher, lambda: handled == ["0"])
    assert handled == ["0"], wiring
    assert run.cancelled(), "the run was stopped by the test, not refused"


class _FlakyKeyService:
    """A decrypt middleware whose key service fails for the first ``failures`` polls, transiently."""

    def __init__(self, failures: int, error: Exception) -> None:
        self.failures = failures
        self.error = error

    def on_publish(self, record: KafkaRecord) -> KafkaRecord:
        return record

    def on_poll(self, record: KafkaRecord) -> KafkaRecord:
        if self.failures:
            self.failures -= 1
            raise self.error
        return record


async def test_run_survives_a_transient_middleware_failure(slept: list[int]) -> None:
    """One key-service blip must not end the run, or nothing polls again. The loop waits a backoff step
    per consecutive failure and polls again; the poll already sought back."""
    client = FakeKafkaClient()
    await publish_all(client, "t", IdSerializer(), [build_message("a", "Tick", 1, id="0")])
    handled: list[str] = []
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=lambda env: handled.append(env.id), on_dead=sink_ok
    )
    middleware = _FlakyKeyService(2, ConnectionError("key service unreachable"))
    consumer = KafkaConsumer(
        client, serializer=IdSerializer(), middleware=[middleware], commit_interval_s=0.001
    )
    task = await _drive(consumer, dispatcher, client.settled)

    assert handled == ["0"]
    assert task.cancelled(), "the run ended on its own instead of surviving the blip"
    assert slept == [0, 1], "one growing step per consecutive failed poll"
    assert client.seeks == [0, 0]


async def test_a_terminal_poll_failure_ends_the_run() -> None:
    """A failure that repeats on every poll is not retried: the run fails fast with it."""
    client = FakeKafkaClient()
    await publish_all(client, "t", IdSerializer(), [build_message("a", "Tick", 1, id="0")])
    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=lambda env: None, on_dead=sink_ok)
    middleware = _FlakyKeyService(1, edax.BadHandle("built on another thread"))
    consumer = KafkaConsumer(client, serializer=IdSerializer(), middleware=[middleware])
    task = await _drive(consumer, dispatcher, lambda: False, limit=0.2)
    assert isinstance(task.exception(), edax.BadHandle)


async def test_a_stopped_client_ends_the_run_and_commits_what_settled() -> None:
    """aiokafka raises ``ConsumerStoppedError`` from ``getone`` once stopped: the end of the stream. The
    run returns, and its final flush commits the work that settled."""
    client = FakeKafkaClient()
    await publish_all(
        client, "t", IdSerializer(), [build_message("a", "Tick", 1, id=str(i)) for i in range(3)]
    )
    handled: list[str] = []
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=lambda env: handled.append(env.id), on_dead=sink_ok
    )
    consumer = KafkaConsumer(client, serializer=IdSerializer(), commit_interval_s=60)
    dispatcher.start()
    task = asyncio.create_task(consumer.run(dispatcher))
    while len(handled) < 3:
        await asyncio.sleep(0.001)
    await client.stop()
    await asyncio.wait_for(task, 1)  # returns, not cancelled
    await dispatcher.stop()
    assert client.commits == [3], "the final flush committed once, the interval never fired"


async def test_commits_are_batched_off_the_ack_path() -> None:
    """A commit per ack would make 100 acks 100 serialized round trips inside the workers. An ack only
    settles; the committer commits what is due on its interval, in one call."""
    client = FakeKafkaClient()
    await publish_all(
        client, "t", IdSerializer(), [build_message(f"k{i % 7}", "Tick", 1, id=str(i)) for i in range(100)]
    )
    dispatcher = edax.KeyOrderedDispatcher(workers=4, handler=lambda env: None, on_dead=sink_ok)
    consumer = KafkaConsumer(client, serializer=IdSerializer(), commit_interval_s=0.01)
    await _drive(consumer, dispatcher, client.settled)
    assert client.settled()
    assert len(client.commits) <= 10, f"{len(client.commits)} commits for 100 acks"
    assert client.commits == sorted(client.commits), "a commit moved backwards"


async def test_a_commit_that_never_returns_hits_its_deadline() -> None:
    """aiokafka retries a commit while the coordinator is away, without end. A paused broker costs one
    deadline per flush: the flush raises, nothing is recorded, consumption goes on, and the
    watermark is due again once the broker is back."""
    client = FakeKafkaClient()
    await publish_all(
        client, "t", IdSerializer(), [build_message("a", "Tick", 1, id=str(i)) for i in range(3)]
    )
    consumer = KafkaConsumer(client, serializer=IdSerializer(), commit_timeout_s=0.05)
    for _ in range(3):
        await delivered(await consumer.poll()).ack()
    client.cluster.pause_commits()
    loop = asyncio.get_running_loop()
    started = loop.time()
    with pytest.raises(asyncio.TimeoutError):
        await consumer.flush()
    assert loop.time() - started < 1, "the deadline did not bound the commit"
    assert consumer.stats()[TP0].committed is None, "a commit that did not land was recorded"

    # The run keeps consuming while every commit times out, and its own stop is not held hostage.
    await publish_all(client, "t", IdSerializer(), [build_message("a", "Tick", 1, id="3")])
    handled: list[str] = []
    observer = Recording()
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=lambda env: handled.append(env.id), on_dead=sink_ok, observer=observer
    )
    run_consumer = KafkaConsumer(
        client, serializer=IdSerializer(), commit_interval_s=0.01, commit_timeout_s=0.05
    )
    started = loop.time()
    await _drive(run_consumer, dispatcher, lambda: handled == ["3"] and bool(observer.commit_failures))
    assert handled == ["3"], "the client's fetch position is past 0..2, so only the new record comes"
    assert loop.time() - started < 2, "a paused broker hung the run"
    assert client.commits == []
    # A consumer whose progress is not saved is a counter, not only a log line.
    assert observer.commit_failures and set(observer.commit_failures) == {asyncio.TimeoutError}

    client.cluster.resume_commits()
    await consumer.flush()
    assert client.commits == [3] and consumer.stats()[TP0].committed == 3


async def test_the_last_commit_after_run_ends_reaches_the_observer() -> None:
    """The interval never fires here, so the one commit attempted is the flush ``run`` does on its way
    out. Its failure is a counter too, not only a log line."""
    client = FakeKafkaClient()
    await publish_all(client, "t", IdSerializer(), [build_message("a", "Tick", 1, id="0")])
    client.cluster.pause_commits()
    handled: list[str] = []
    observer = Recording()
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=lambda env: handled.append(env.id), on_dead=sink_ok, observer=observer
    )
    consumer = KafkaConsumer(client, serializer=IdSerializer(), commit_interval_s=60, commit_timeout_s=0.05)
    await _drive(consumer, dispatcher, lambda: handled == ["0"])
    assert handled == ["0"]
    assert client.commits == []
    assert observer.commit_failures == [asyncio.TimeoutError]


async def test_a_killed_consumer_redelivers_from_the_commit_and_never_below_it(slept: list[int]) -> None:
    """A consumer dies mid-batch: it committed up to offset 10, handled more, and holds one in flight.
    Its replacement starts at the group's commit, so every offset from 10 runs again and none below it."""
    cluster = FakeKafkaCluster()
    first = FakeKafkaClient(cluster)
    first_consumer = KafkaConsumer(first, serializer=IdSerializer(), commit_interval_s=0.001)
    first.subscribe(["t"], listener=first_consumer.rebalance_listener())
    await cluster.join(first)
    await publish_all(
        first, "t", IdSerializer(), [build_message(f"k{i % 3}", "Tick", 1, id=str(i)) for i in range(20)]
    )

    handled: list[tuple[str, int]] = []
    stuck = asyncio.Event()

    async def first_handler(env: edax.EventMessage) -> None:
        handled.append(("first", int(env.id)))
        if env.id == "12":
            await stuck.wait()  # mid-batch: this one never finishes before the crash

    first_dispatcher = edax.KeyOrderedDispatcher(workers=3, handler=first_handler, on_dead=sink_ok)
    first_dispatcher.start()
    first_run = asyncio.create_task(first_consumer.run(first_dispatcher))
    while cluster.committed.get(TP0, 0) < 10:
        await asyncio.sleep(0.001)
    committed = cluster.committed[TP0]
    # The process dies: the broker drops the member, its fetches stop, and its commits are refused.
    await cluster.leave(first, crash=True)
    await asyncio.wait_for(first_run, 1)
    await first_dispatcher.stop(timeout=0.05)
    assert cluster.committed[TP0] == committed, "a dead member's commit landed"

    second = FakeKafkaClient(cluster)
    second_consumer = KafkaConsumer(second, serializer=IdSerializer(), commit_interval_s=0.001)
    second.subscribe(["t"], listener=second_consumer.rebalance_listener())
    await cluster.join(second)

    async def second_handler(env: edax.EventMessage) -> None:
        handled.append(("second", int(env.id)))

    second_dispatcher = edax.KeyOrderedDispatcher(workers=3, handler=second_handler, on_dead=sink_ok)
    await _drive(second_consumer, second_dispatcher, lambda: cluster.committed.get(TP0) == 20)

    redone = sorted(offset for who, offset in handled if who == "second")
    assert redone == list(range(committed, 20)), "the replacement skipped work or re-ran committed work"
    assert cluster.committed[TP0] == 20
    offsets = [offset for _, tp, offset in cluster.commit_log if tp == TP0]
    assert offsets == sorted(offsets), "a commit moved the group backwards"


async def test_a_rebalance_mid_batch_drains_then_hands_over_without_overlap(slept: list[int]) -> None:
    """A second member joins while the first is mid-batch. The listener drains the revoked work
    before the new owner starts, so no key is handled by two members at once, the group's commit never
    moves back, and every message is handled by someone."""
    cluster = FakeKafkaCluster(partitions=2)
    first = FakeKafkaClient(cluster)
    first_consumer = KafkaConsumer(first, serializer=SeqSerializer(), commit_interval_s=0.002)
    first.subscribe(["t"], listener=first_consumer.rebalance_listener(drain_timeout_s=2))
    await cluster.join(first)
    keys = ["a", "b", "c", "d", "e", "f"]
    await publish_all(
        first, "t", SeqSerializer(), [build_message(k, "Tick", seq) for seq in range(1, 9) for k in keys]
    )

    active: dict[str, str] = {}
    handled: dict[str, list[tuple[str, int]]] = defaultdict(list)
    overlaps: list[str] = []

    def handler_for(owner: str) -> Callable[[edax.EventMessage], Awaitable[None]]:
        async def handler(env: edax.EventMessage) -> None:
            key = env.aggregate_id
            if key in active and active[key] != owner:
                overlaps.append(f"{key}: {active[key]} and {owner}")
            active[key] = owner
            await asyncio.sleep(0.001)  # a real handler awaits I/O, so others interleave
            del active[key]
            handled[key].append((owner, sequence_of(env)))

        return handler

    first_dispatcher = edax.KeyOrderedDispatcher(workers=3, handler=handler_for("first"), on_dead=sink_ok)
    first_dispatcher.start()
    first_run = asyncio.create_task(first_consumer.run(first_dispatcher))
    while sum(len(v) for v in handled.values()) < 10:
        await asyncio.sleep(0.001)

    second = FakeKafkaClient(cluster)
    second_consumer = KafkaConsumer(second, serializer=SeqSerializer(), commit_interval_s=0.002)
    second.subscribe(["t"], listener=second_consumer.rebalance_listener(drain_timeout_s=2))
    await cluster.join(second)  # the first member's revoke runs and drains in here
    assert first_consumer.stats() == {} or all(tp in first.assignment for tp in first_consumer.stats()), (
        "the first member kept bookkeeping for a partition it lost"
    )
    await _drive(
        second_consumer,
        edax.KeyOrderedDispatcher(workers=3, handler=handler_for("second"), on_dead=sink_ok),
        lambda: all(
            cluster.committed.get(TopicPartition("t", p)) == len(cluster.logs[("t", p)]) for p in (0, 1)
        ),
    )
    first_run.cancel()
    await asyncio.gather(first_run, return_exceptions=True)
    await first_dispatcher.stop(timeout=1)

    assert overlaps == [], "a key was handled by two members at once"
    for key in keys:
        assert {seq for _, seq in handled[key]} == set(range(1, 9)), f"{key} lost an event"
    for tp in (TopicPartition("t", 0), TopicPartition("t", 1)):
        offsets = [offset for _, at, offset in cluster.commit_log if at == tp]
        assert offsets == sorted(offsets), f"the commit of {tp} moved backwards"


async def test_a_revoke_skips_the_queue_and_waits_only_for_running_handlers() -> None:
    """A revoke must neither drain a deep queue — that outlasts the rebalance — nor let the new owner start
    while a handler of this member still runs. It resets at once, so queued deliveries are skipped, and
    waits for the running handler; one stuck past the deadline is reported, and the revoke returns."""
    cluster = FakeKafkaCluster()
    first = FakeKafkaClient(cluster)
    consumer = KafkaConsumer(first, serializer=IdSerializer())
    first.subscribe(["t"], listener=consumer.rebalance_listener(drain_timeout_s=0.05))
    await cluster.join(first)
    await publish_all(
        first, "t", IdSerializer(), [build_message("a", "Tick", 1, id=str(i)) for i in range(5)]
    )
    release = asyncio.Event()
    handled: list[str] = []

    async def handler(env: edax.EventMessage) -> None:
        handled.append(env.id)
        if env.id == "0":
            await release.wait()  # running when the revoke comes

    observer = Recording()
    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler, on_dead=sink_ok, observer=observer)
    dispatcher.start()
    run = asyncio.create_task(consumer.run(dispatcher))
    while dispatcher.stats().queue_depth != [4] or dispatcher.stats().handling != 1:
        await asyncio.sleep(0.001)

    loop = asyncio.get_running_loop()
    started = loop.time()
    await first.listener.on_partitions_revoked({TP0})
    assert 0.05 <= loop.time() - started < 1, (
        "the revoke did not wait for, or did not give up on, the handler"
    )
    assert consumer.stats() == {}, "the revoked partition was not reset"
    assert observer.revoke_overruns == [1], (
        "an overrun lets two members run one key: a counter, not a log line"
    )
    release.set()
    await asyncio.sleep(0.01)
    assert handled == ["0"], "a queued delivery of the revoked partition ran"
    assert dispatcher.idle()
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)
    await dispatcher.stop()
    assert first.commits == [], "a stale ack committed something"

    # A handler that finishes inside the deadline releases the revoke at once.
    await cluster.rebalance()
    quick = KafkaConsumer(first, serializer=IdSerializer())
    first.subscribe(["t"], listener=quick.rebalance_listener(drain_timeout_s=5))
    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=lambda env: None, on_dead=sink_ok)
    dispatcher.start()
    run = asyncio.create_task(quick.run(dispatcher))
    while quick.stats().get(TP0) is None or quick.stats()[TP0].unsettled:
        await asyncio.sleep(0.001)
    started = loop.time()
    await first.listener.on_partitions_revoked({TP0})
    assert loop.time() - started < 1
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)
    await dispatcher.stop()
    assert first.commits == [5], "the revoke committed what had settled before it reset"


# =========================================================================
# dead-letter record
# =========================================================================


class _TraceHeader:
    """Adds a ``traceparent`` on the way out and leaves the record alone on the way in."""

    def on_publish(self, record: KafkaRecord) -> KafkaRecord:
        return KafkaRecord(
            record.key, record.value, [*record.headers, KafkaHeader("traceparent", b"00-ab-01")]
        )

    def on_poll(self, record: KafkaRecord) -> KafkaRecord:
        return record


@pytest.mark.parametrize("cause", ["poison", "terminal_handler"])
async def test_the_dead_letter_sink_publishes_the_record_as_fetched(cause: str) -> None:
    """The sink republishes the record as it was fetched (key, value and every header, before any
    middleware), plus where it came from and why, on the kernel's partition for its key."""
    cluster = FakeKafkaCluster(partitions=3)
    client = FakeKafkaClient(cluster)
    consumer = KafkaConsumer(client, serializer=_SchemaSerializer(), commit_interval_s=0.001)
    client.subscribe(["t"], listener=consumer.rebalance_listener())
    await cluster.join(client)
    producer = KafkaProducer(client, "t", serializer=_SchemaSerializer(), middleware=[_TraceHeader()])
    await producer.publish(build_message("acct-9", "Tick", 1, id="BAD" if cause == "poison" else "7"))

    def handler(env: edax.EventMessage) -> None:
        raise edax.CodecError("negative balance")  # terminal

    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=handler, on_dead=KafkaDeadLetterSink(client, "t.dlq")
    )
    source_tp = next(TopicPartition("t", p) for p in range(3) if cluster.logs[("t", p)])
    await _drive(consumer, dispatcher, lambda: any(cluster.logs[("t.dlq", p)] for p in range(3)))

    (original,) = cluster.logs[(source_tp.topic, source_tp.partition)]
    dead = [m for p in range(3) for m in cluster.logs[("t.dlq", p)]]
    assert len(dead) == 1
    (msg,) = dead
    assert (msg.key, msg.value) == (original.key, original.value), "key and value verbatim"
    assert msg.headers[: len(original.headers)] == original.headers, "every original header, in order"
    assert msg.partition == _edax.kafka_partition_for(original.key, 3), "the kernel's placement"
    source = dict(msg.headers[len(original.headers) :])
    assert {k: source[k] for k in ("edax-dlq-topic", "edax-dlq-partition", "edax-dlq-offset")} == {
        "edax-dlq-topic": b"t",
        "edax-dlq-partition": str(source_tp.partition).encode(),
        "edax-dlq-offset": b"0",
    }
    assert source["edax-dlq-error"].startswith(b"codec: ")


async def test_the_dead_letter_sink_refuses_a_delivery_with_no_kafka_record() -> None:
    sink = KafkaDeadLetterSink(SpyProducerClient(), "t.dlq")
    delivery = edax.Delivery(build_message("a", "Tick", 1), lambda: None, lambda: None)
    with pytest.raises(TypeError, match="no Kafka record"):
        await sink(delivery, edax.CodecError("bad"))


class _BrokenFetch(FakeKafkaClient):
    """A client whose fetch fails ``failures`` times, the way a fetch does while the broker is away."""

    def __init__(self, failures: int) -> None:
        super().__init__()
        self.failures = failures

    async def getone(self) -> KafkaMsg:
        if self.failures:
            self.failures -= 1
            raise ConnectionError("broker unreachable")
        return await super().getone()


async def test_a_failed_fetch_propagates_and_the_loop_backs_off(slept: list[int]) -> None:
    """A fetch that fails is no end of stream: ``poll`` raises it, and the loop waits a growing step and
    polls again. Only a stopped client ends the stream."""
    client = _BrokenFetch(2)
    await publish_all(client, "t", IdSerializer(), [build_message("a", "Tick", 1, id="0")])
    with pytest.raises(ConnectionError):
        await KafkaConsumer(client, serializer=IdSerializer()).poll()
    handled: list[str] = []
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=lambda env: handled.append(env.id), on_dead=sink_ok
    )
    await _drive(KafkaConsumer(client, serializer=IdSerializer()), dispatcher, lambda: handled == ["0"])
    assert handled == ["0"]
    assert slept == [0], "the second failure waited one step, then the fetch came through"


def test_a_client_without_aiokafka_errors_is_never_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    """The stopped-client check imports aiokafka's error lazily; a client of another library raises
    its own errors, none of which means end of stream."""
    import sys

    from edax.backend import kafka as kafka_module

    monkeypatch.setitem(sys.modules, "aiokafka.errors", None)
    assert kafka_module._consumer_stopped(ConnectionError("x")) is False


# =========================================================================
# commands on a topic: the record per kind, the consumer's registry per kind
# =========================================================================


async def test_a_command_travels_under_its_own_media_type_and_comes_back_whole() -> None:
    """The producer writes a command with the command serializer, keyed by the kernel's ordering key (its
    id when it names no aggregate). A consumer that registers the command serializer delivers the
    :class:`~edax.CommandMessage` as it was published, with the broker's timestamp on the fetched record."""
    client = FakeKafkaClient()
    command = build_command("Deposit", id="cmd-1", idempotency_key="req-1", metadata={"user": "u-7"})
    await KafkaProducer(client, "t").publish(command)
    [record] = client.log
    assert record.key == b"cmd-1", "no aggregate: the command's own id is the key"
    assert record.headers == [(CONTENT_TYPE, b"application/vnd.edax.command+json")]

    consumer = KafkaConsumer(client, serializers=[edax.KernelSerializer("json", "command")])
    delivery = delivered(await consumer.poll())
    assert not isinstance(delivery, UndecodableDelivery)
    assert delivery.message == command
    fetched = delivery.origin
    assert isinstance(fetched, KafkaFetched)
    assert (fetched.timestamp, fetched.timestamp_type) == (FAKE_KAFKA_EPOCH_MS, 0)


@pytest.mark.parametrize(
    "branch, serializer, serializers, publish, want",
    [
        pytest.param(
            "event_consumer",
            None,
            (),
            "command",
            "no serializer for content type 'application/vnd.edax.command+json'",
            id="an_event_consumer_refuses_a_command_record",
        ),
        pytest.param(
            "command_consumer",
            edax.KernelSerializer("json", "command"),
            (),
            "event",
            "no serializer for content type 'application/vnd.edax.event+json'",
            id="a_command_consumer_refuses_an_event_record",
        ),
        pytest.param(
            "command_consumer",
            edax.KernelSerializer("json", "command"),
            (),
            "header_less",
            "no readable content-type header",
            id="a_command_consumer_has_no_header_less_default",
        ),
    ],
)
async def test_a_record_of_a_kind_the_consumer_does_not_read_is_poison(
    branch: str, serializer: Any, serializers: tuple[Any, ...], publish: str, want: str
) -> None:
    client = FakeKafkaClient()
    if publish == "header_less":
        await client.send_and_wait("t", edax.KernelSerializer("json").encode(build_message("a", "Tick", 1)))
    else:
        message = build_command("Deposit") if publish == "command" else build_message("a", "Tick", 1)
        await KafkaProducer(client, "t").publish(message)
    consumer = KafkaConsumer(client, serializer=serializer, serializers=serializers)
    delivery = delivered(await consumer.poll())
    assert isinstance(delivery, UndecodableDelivery), branch
    assert isinstance(delivery.error, edax.CodecError) and delivery.error.terminal
    assert want in str(delivery.error)


@pytest.mark.parametrize(
    "role, kwargs",
    [
        ("serializer", {"serializer": edax.KernelSerializer("json", "command")}),
        ("command_serializer", {"command_serializer": edax.KernelSerializer("json")}),
    ],
)
def test_a_producer_refuses_a_serializer_of_the_wrong_kind(role: str, kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match=role):
        KafkaProducer(SpyProducerClient(), "t", **kwargs)


def test_a_fetched_record_reads_without_a_broker_timestamp() -> None:
    """A client that reports no timestamp leaves both fields unset, so a hand-built record still works."""
    fetched = KafkaFetched("t", 0, 7, b"k", b"v", ())
    assert (fetched.timestamp, fetched.timestamp_type) == (None, None)

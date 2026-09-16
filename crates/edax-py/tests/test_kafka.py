"""Kafka adapter over a fake aiokafka client (no broker). Kafka-only semantics: the per-partition
commit watermark, the producer's key/topic/headers, the serializer registry and the record
middleware; the seam contract itself is in ``test_broker_contract.py``."""

from __future__ import annotations

import asyncio
import base64
from collections import defaultdict, deque
from collections.abc import Sequence
from typing import Any, Callable, Optional

import edax
import pytest
from conftest import (
    FakeKafkaClient,
    IdSerializer,
    KafkaMsg,
    TopicPartition,
    build_envelope,
    delivered,
    kafka_consumer,
)
from edax.backend.kafka import KafkaConsumer, KafkaHeader, KafkaProducer, KafkaRecord, RecordMiddleware
from edax.broker import UNDECODABLE, KeyFn, UndecodableDelivery


async def test_out_of_order_ack_commits_watermark_only() -> None:
    c, k = await kafka_consumer(3)
    d0, d1, d2 = [delivered(await k.poll()) for _ in range(3)]
    await d2.ack()
    await d1.ack()
    assert c.commits == [0]  # 0 still in flight: never commit past it
    await d0.ack()
    assert c.commits == [0, 3]


async def test_nack_keeps_offset_in_flight_until_redelivered() -> None:
    c, k = await kafka_consumer(3)
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
    c, k = await kafka_consumer(3)
    d0, d1, d2 = [delivered(await k.poll()) for _ in range(3)]
    await d0.nack()
    await d2.nack()
    assert c.seeks == [0, 0]  # the second nack rewinds to 0, not to 2

    # Redelivery from 0 settles everything, so the watermark finally advances past the batch.
    for _ in range(3):
        await delivered(await k.poll()).ack()
    await d1.ack()
    assert c.commits[-1] == 3


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
    # Interleaved partitions: one partition's progress must not settle the other's, and each
    # partition's watermark must be committed against its own TopicPartition.
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
        self.sent: list[tuple[str, bytes, bytes, list[tuple[str, bytes]]]] = []

    async def send_and_wait(
        self, topic: str, value: bytes, key: bytes, headers: Sequence[tuple[str, bytes]]
    ) -> None:
        self.sent.append((topic, value, key, list(headers)))

    async def send(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("publish must await the broker ack (send_and_wait), not fire and forget")


@pytest.mark.parametrize(
    "key_fn, want_key",
    [
        (None, b"acct-1"),  # default: the aggregate id keys the partition, so per-key order holds
        (lambda env: env.namespace["tenant"], b"t1"),  # caller-supplied key
    ],
)
async def test_publish_sends_keyed_encoded_envelope(key_fn: Optional[KeyFn], want_key: bytes) -> None:
    client, serializer = _SpyProducerClient(), edax.kernel()
    env = build_envelope("acct-1", "Deposited", 3, namespace={"tenant": "t1"})
    await KafkaProducer(client, "events.account", serializer=serializer, key=key_fn).publish(env)

    assert len(client.sent) == 1
    topic, value, key, headers = client.sent[0]
    assert (topic, value, key) == ("events.account", serializer.encode(env), want_key)
    # The four contract headers, in the kernel's order: the media type a consumer routes on, then
    # the envelope id, event name and event version a router or a dead-letter sink reads without
    # decoding the value.
    assert headers == [
        ("content-type", b"application/vnd.edax.envelope+json"),
        ("edax-id", env.id.encode()),
        ("edax-event", b"Deposited"),
        ("edax-event-version", b"1"),
    ]


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
    producer = KafkaProducer(c, "t", serializer=IdSerializer())
    for i in range(4):
        await producer.publish(build_envelope("a", "Tick", 1, id=str(i)))
    k = KafkaConsumer(c, serializer=IdSerializer())
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
    assert k.stats()[tp].committed == 2


class _SchemaSerializer:
    """Decodes a numeric body. Anything else is a payload this consumer has no schema for: the
    poison message a producer rollout leaves on the topic. It refuses with a plain ``ValueError``,
    the way a third-party serializer raises its own error type rather than the SDK's."""

    media_type = "application/x-test-schema"

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
    producer = KafkaProducer(c, "t", serializer=_SchemaSerializer())
    await producer.publish(build_envelope("acct-1", "Tick", 1, id="0"))
    await c.send_and_wait("t", POISON_BYTES)  # another writer's bytes, no headers of ours
    await producer.publish(build_envelope("acct-1", "Tick", 1, id="2"))

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
    consumer = KafkaConsumer(c, serializer=_SchemaSerializer())
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


async def test_a_null_header_value_polls_as_empty_bytes() -> None:
    """Kafka header values are nullable and aiokafka hands a null one over as ``None``. The kernel
    header is bytes, so the shell coerces; before it did, the poll raised after booking the offset."""
    c = FakeKafkaClient()
    await KafkaProducer(c, "t", serializer=IdSerializer()).publish(build_envelope("a", "Tick", 1, id="0"))
    c.log[0] = c.log[0]._replace(headers=[*c.log[0].headers, ("x-null", None)])
    delivery = delivered(await KafkaConsumer(c, serializer=IdSerializer()).poll())
    assert not isinstance(delivery, UndecodableDelivery)
    assert delivery.envelope.id == "0"


async def test_poison_ids_differ_across_partitions_at_one_offset() -> None:
    """The offset is per partition, so the bare offset is not an identity. Two poison messages at
    offset 0 on different partitions would share one envelope id, and a dead-letter queue keyed on it
    — the natural key, and the one `idempotent_by_id` uses — keeps the first and rejects the second
    as a `PayloadMismatch`. Silently: the first one wins.
    """
    c = _MultiPartitionClient([_msg("t", 0, 0, POISON_BYTES), _msg("t", 1, 0, POISON_BYTES)])
    k = KafkaConsumer(c, serializer=_SchemaSerializer())
    # The id is built in `poll`; the test above already pins that a poison delivery reaches `on_dead`
    # carrying it, so there is nothing a dispatcher and a consumer loop would add here.
    ids = [delivered(await k.poll()).envelope.id for _ in range(2)]

    assert ids == ["t/0/0", "t/1/0"], "one id per partition, not one shared offset"


# =========================================================================
# serializer registry: which serializer a record's `content-type` routes to
# =========================================================================


class _UpperSerializer:
    """A second serializer on the same topic, under a media type of its own. Its decode tags the id,
    so the envelope names the serializer that produced it and a wrong route cannot pass unseen."""

    media_type = "application/x-test-upper"

    def encode(self, env: edax.EventEnvelope) -> bytes:
        return env.id.upper().encode()

    def decode(self, wire: bytes) -> edax.EventEnvelope:
        return build_envelope("a", "Tick", 1, id=f"upper:{wire.decode()}")


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
    ],
)
async def test_serializer_registry_branches(
    branch: str,
    headers: list[tuple[str, bytes]],
    want_id: Optional[str],
    want_poison: Optional[str],
) -> None:
    """One record, one header changed per case. A media type no serializer claims is never decoded
    with the default: that fallback is how a record in another format reaches a handler as a wrong
    envelope instead of the dead-letter sink."""
    client = FakeKafkaClient()
    client.log.append(KafkaMsg("t", 0, 0, b"a", b"evt-1", headers))
    consumer = KafkaConsumer(client, serializer=IdSerializer(), serializers=[_UpperSerializer()])

    delivery = delivered(await consumer.poll())

    if want_poison is None:
        assert not isinstance(delivery, UndecodableDelivery), branch
        assert delivery.envelope.id == want_id, branch
    else:
        assert isinstance(delivery, UndecodableDelivery), branch
        assert want_poison in str(delivery.error), branch
        assert delivery.envelope.id == "t/0/0", branch
    # Booked before the decode either way, so settling a poison record moves the watermark past it
    # instead of pinning the partition on bytes that will never decode.
    await delivery.ack()
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


#: What the consumer made of the record coming back: the decoded envelope id, the text of the poison
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
        build_envelope("a", "Tick", 1, id="evt-1")
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
        assert delivery.envelope.id == expected, branch
    else:
        assert isinstance(delivery, UndecodableDelivery), branch
        assert expected in str(delivery.error), branch

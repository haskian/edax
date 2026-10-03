import asyncio
import base64
from typing import Any

import edax
import pytest
from conftest import FakeKafkaClient, KafkaMsg, golden, sequence_of
from edax import _edax, causation
from edax.backend.kafka import KafkaConsumer
from edax.broker import UndecodableDelivery
from edax.causation import CAUSATION_ID, CORRELATION_ID, ensure_ids, originate, propagate


def test_golden_partition() -> None:
    for c in golden("partition.json"):
        assert edax.partition(c["id"], c["n"]) == c["expected"]


def test_golden_staleness() -> None:
    """`is_stale` and `advance` per pair, and the fold an ordered consumer runs over a redelivered
    stream: skip what is stale, advance after each apply. The first fold is the one the old
    `watermark = sequence` update got wrong: the unsequenced event erased the mark and the stale 3
    applied."""
    for c in golden("staleness.json"):
        if c["op"] == "pair":
            assert edax.is_stale(c["stored"], c["incoming"]) == c["stale"], c
            assert edax.advance(c["stored"], c["incoming"]) == c["advanced"], c
            continue
        watermark: int | None = None
        applied: list[int | None] = []
        for incoming in c["stream"]:
            if not edax.is_stale(watermark, incoming):
                applied.append(incoming)
                watermark = edax.advance(watermark, incoming)
        assert (applied, watermark) == (c["applied"], c["watermark"]), c["name"]


def test_golden_sequence_batch() -> None:
    """The dense-batch rule, one case per branch, through the public :func:`edax.check_sequence_batch` a
    durable store calls. Every refusal is the kernel's terminal ``codec:`` error, so a store whose append
    hits it fails the same way on every retry instead of reading as a transient store error."""
    for c in golden("sequence_batch.json"):
        if c["error"] is None:
            assert edax.check_sequence_batch(c["expected"], c["sequences"]) is None
        else:
            with pytest.raises(edax.CodecError) as caught:
                edax.check_sequence_batch(c["expected"], c["sequences"])
            assert _edax.tag_error("codec", str(caught.value)) == c["error"]
            assert edax.is_terminal(caught.value)


@pytest.mark.parametrize(
    "expected, sequences", [(-1, [0]), (0, [-1])], ids=["negative_version", "negative_sequence"]
)
def test_a_negative_version_or_sequence_is_a_codec_error(expected: int, sequences: list[int]) -> None:
    """Versions and sequences are unsigned 64-bit end to end, so a negative one never reaches the rule
    clamped or wrapped: it is refused at the boundary as the terminal ``CodecError``."""
    with pytest.raises(edax.CodecError):
        edax.check_sequence_batch(expected, sequences)


def test_golden_backoff() -> None:
    """`(policy, attempt, entropy) -> millis`, through the raw kernel call and through the SDK's
    :class:`~edax.Backoff`, which is what :class:`~edax.Pacing` waits on."""
    for c in golden("backoff.json"):
        policy = edax.Backoff(c["base_millis"], c["cap_millis"])
        got = edax.backoff_millis(c["base_millis"], c["cap_millis"], c["attempt"], c["entropy"])
        assert got == policy.delay_millis(c["attempt"], c["entropy"]) == c["expected_millis"], c
        assert got <= c["ceiling_millis"] <= c["cap_millis"], c


def _envelope_from_obj(o: dict[str, Any]) -> edax.EventEnvelope:
    ev = o["event"]
    return edax.EventEnvelope(
        id=o["id"],
        aggregate_id=o["aggregate_id"],
        event=edax.Event(ev["name"], ev["version"], base64.b64decode(ev["data"])),
        timestamp=o["timestamp"],
        sequence=o["sequence"],
        namespace=o["namespace"],
        metadata=o["metadata"],
    )


def test_golden_envelopes() -> None:
    for case in golden("envelopes.json"):
        env = _envelope_from_obj(case["envelope"])
        for fmt, want in (("json", case["json_hex"]), ("msgpack", case["msgpack_hex"])):
            ser = edax.KernelSerializer(fmt)
            wire = ser.encode(env)
            assert wire.hex() == want, (case["name"], fmt)
            # decode -> re-encode reproduces the same wire bytes
            assert ser.encode(ser.decode(wire)).hex() == want, (case["name"], fmt)


def test_golden_envelope_decode() -> None:
    """The envelope evolution rule, one wire value per case in both formats: an unknown field and a
    missing ``sequence`` read, a positional (array) envelope, a repeated field and trailing bytes are
    refused. A newer producer's field therefore never turns a message into poison for this reader."""
    for case in golden("envelope_decode.json"):
        for fmt in ("json", "msgpack"):
            wire = bytes.fromhex(case[f"{fmt}_hex"])
            got: dict[str, Any]
            try:
                got = {"envelope": _envelope_as_obj(edax.KernelSerializer(fmt).decode(wire))}
            except edax.CodecError as e:
                got = {"error_code": e.code}
            assert got == case["want"], (case["name"], fmt)


def _envelope_as_obj(env: edax.EventEnvelope) -> dict[str, Any]:
    """The golden's envelope shape out of a native envelope, so a case compares one value instead of
    seven fields. The native class has no ``__eq__``, so two envelopes never compare equal directly."""
    return {
        "id": env.id,
        "aggregate_id": env.aggregate_id,
        "event": {
            "name": env.event.name,
            "version": env.event.version,
            "data": base64.b64encode(bytes(env.event.data)).decode(),
        },
        "namespace": dict(env.namespace),
        "metadata": dict(env.metadata),
        "timestamp": env.timestamp,
        "sequence": env.sequence,
    }


def _headers_from_obj(headers: list[dict[str, str]]) -> list[Any]:
    return [_edax.KafkaHeader(h["key"], bytes.fromhex(h["value_hex"])) for h in headers]


def test_golden_kafka_record() -> None:
    """The record a producer sends, built the way :class:`~edax.backend.kafka.KafkaProducer` builds
    it: the serializer's bytes and media type into the kernel's record. A second SDK writing a
    different key or header set would split the topic in two."""
    for c in golden("kafka_record.json"):
        env = _envelope_from_obj(c["envelope"])
        serializer = edax.KernelSerializer(c["format"])
        record = _edax.kafka_record(env, serializer.encode(env), _edax.media_type(c["format"]), c["key"])
        assert record.key.hex() == c["key_hex"], c["name"]
        assert record.value.hex() == c["value_hex"], c["name"]
        got = [(h.key, h.value.hex()) for h in record.headers]
        assert got == [(h["key"], h["value_hex"]) for h in c["headers"]], c["name"]


def test_golden_kafka_decode() -> None:
    """The consumer side, through a real :class:`~edax.backend.kafka.KafkaConsumer` over a fake client, so
    the vector proves the adapter and not a copy of its rule: the kernel routes the record by its first
    ``content-type``, an absent one falls back to the default, and a media type no serializer claims —
    the empty one included — is poison instead of a guess. Every failure is a terminal ``CodecError``,
    so the consumer dead-letters the record instead of redelivering it forever."""
    for c in golden("kafka_decode.json"):
        default = c["default_format"]
        other = "msgpack" if default == "json" else "json"
        client = FakeKafkaClient()
        headers = [(h["key"], bytes.fromhex(h["value_hex"])) for h in c["headers"]]
        client.log.append(KafkaMsg("t", 0, 0, b"agg-1", bytes.fromhex(c["value_hex"]), headers))
        consumer = KafkaConsumer(
            client, serializer=edax.KernelSerializer(default), serializers=[edax.KernelSerializer(other)]
        )
        delivery = asyncio.run(consumer.poll())
        assert delivery is not None, c["name"]
        if "envelope" in c["want"]:
            assert not isinstance(delivery, UndecodableDelivery), c["name"]
            assert _envelope_as_obj(delivery.envelope) == c["want"]["envelope"], c["name"]
            continue
        assert isinstance(delivery, UndecodableDelivery), c["name"]
        assert isinstance(delivery.error, edax.CodecError) and delivery.error.terminal, c["name"]
        unknown = c["want"]["error"].removeprefix("codec: unknown format: ")
        if unknown != c["want"]["error"]:
            # The kernel refused the media type; the shell names the same one it found no serializer for.
            assert f"no serializer for content type {unknown!r}" in str(delivery.error), c["name"]


def test_golden_kafka_route() -> None:
    """The routing rule every shell runs, replayed through the binding: the default, an index into the
    host's own registry, or the unknown media type — an empty value included, which a null header value
    crosses as."""
    for c in golden("kafka_route.json"):
        route = _edax.kafka_route(_headers_from_obj(c["headers"]), c["registered"])
        got: dict[str, Any]
        if route.registered is not None:
            got = {"route": "registered", "index": route.registered}
        elif route.unknown is not None:
            got = {"route": "unknown", "media_type": route.unknown}
        else:
            got = {"route": "default"}
        assert got == c["want"], c["name"]


def test_golden_kafka_dead_letter() -> None:
    """The record a dead-letter sink publishes: key, value and headers verbatim plus the four
    ``edax-dlq-*`` headers, and the source its reader recovers — the last one, after two dead-letterings —
    or none when a coordinate is missing or unreadable."""
    for c in golden("kafka_dead_letter.json"):
        if "record" not in c:
            assert _edax.kafka_dead_letter_source(_headers_from_obj(c["headers"])) is None, c["name"]
            continue
        rec, want = c["record"], c["want"]
        record = _edax.KafkaRecord(
            bytes.fromhex(rec["key_hex"]), bytes.fromhex(rec["value_hex"]), _headers_from_obj(rec["headers"])
        )
        dead = _edax.kafka_dead_letter_record(record, c["topic"], c["partition"], c["offset"], c["error"])
        assert (dead.key.hex(), dead.value.hex()) == (want["key_hex"], want["value_hex"]), c["name"]
        assert [{"key": h.key, "value_hex": h.value.hex()} for h in dead.headers] == want["headers"], c[
            "name"
        ]
        source = _edax.kafka_dead_letter_source(dead.headers)
        assert source is not None, c["name"]
        got = {
            "topic": source.topic,
            "partition": source.partition,
            "offset": source.offset,
            "error": source.error,
        }
        assert got == want["source"], c["name"]


def test_golden_kafka_offsets() -> None:
    """The offset tracker replayed step by step: settlement is by token, so a stale token from a
    revoked or rewound partition decides nothing, a nack retires every token of its partition, and
    ``due`` names the commits. Every scenario ends with the per-partition stats it left."""
    for c in golden("kafka_offsets.json"):
        tracker = _edax.offset_tracker()  # fresh per scenario: tokens are per tracker
        for i, step in enumerate(c["steps"]):
            where = f"{c['name']} step {i} ({step['op']})"
            op, want = step["op"], step["want"]
            if op == "book":
                if "error" in want:
                    with pytest.raises(RuntimeError) as refused:
                        tracker.book(step["topic"], step["partition"], step["offset"])
                    assert str(refused.value) == want["error"], where
                else:
                    assert tracker.book(step["topic"], step["partition"], step["offset"]) == want["token"], (
                        where
                    )
            elif op == "ack":
                assert {"decision": "settled" if tracker.ack(step["token"]) else "stale"} == want, where
            elif op == "nack":
                seek = tracker.nack(step["token"]).seek
                got = {"decision": "stale"} if seek is None else {"decision": "seek", "offset": seek}
                assert got == want, where
            elif op == "is_live":
                assert {"live": tracker.is_live(step["token"])} == want, where
            elif op == "due":
                rows = [
                    {"topic": d.topic, "partition": d.partition, "offset": d.offset} for d in tracker.due()
                ]
                assert rows == want["rows"], where
            elif op == "committed":
                assert tracker.committed(step["topic"], step["partition"], step["offset"]) is None, where
            elif op == "reset":
                assert tracker.reset(step["topic"], step["partition"]) is None, where
            elif op == "stats":
                rows = [
                    {
                        "topic": r.topic,
                        "partition": r.partition,
                        "unsettled": r.unsettled,
                        "live": r.live,
                        "max_delivered": r.max_delivered,
                        "committed": r.committed,
                    }
                    for r in tracker.stats()
                ]
                assert rows == want["rows"], where
            else:
                raise AssertionError(f"{where}: unknown op")


def test_golden_redis_value() -> None:
    """The bytes under an idempotency key: ``1<owner><record>``, and the split back out of them. A
    second SDK that laid them out differently could not read this SDK's ledger."""
    for c in golden("redis_value.json"):
        name, op = c["name"], c["op"]
        if op == "claim":
            assert _edax.redis_claim_value(c["owner"], c["payload_hash"]) == c["value"].encode(), name
        elif op == "settled":
            assert _edax.redis_settled_value(c["claim"].encode(), c["record_json"]) == c["value"].encode(), (
                name
            )
        elif op == "split":
            raw, want = bytes.fromhex(c["raw_hex"]), c["want"]
            if "error" in want:
                with pytest.raises(edax.CodecError) as caught, edax.errors.mapped():
                    _edax.redis_split_value(raw)
                assert f"codec: {caught.value}" == want["error"], name
                continue
            split = _edax.redis_split_value(raw)
            assert (split.owner, split.record) == (want["owner"], want["record"]), name
        else:
            raise AssertionError(f"{name}: unknown op {op}")


def _redis_ledger_case(c: dict[str, Any]) -> Any:
    """One ``redis_ledger.json`` operation through the binding: its value, raising what the kernel raises."""
    op = c["op"]
    if op == "key":
        return _edax.redis_ledger_key(c["prefix"], c["key"]).hex()
    if op == "ttl":
        return _edax.redis_check_ttl(c["setting"], c["ttl_ms"])
    if op == "handle":
        return _edax.redis_check_claim(bytes.fromhex(c["handle_hex"]))
    if op == "outcome":
        reply = None if c["reply_hex"] is None else bytes.fromhex(c["reply_hex"])
        prior = _edax.redis_claim_outcome(c["claim"].encode(), reply)
        return {"won": True} if prior is None else {"prior": prior}
    raise AssertionError(f"{c['name']}: unknown op {op}")


def test_golden_redis_ledger() -> None:
    """The key, the refused settings and handles, the claim outcome and the reply verdicts: every
    decision a second SDK's Redis store must make the same way, read through the binding."""
    for c in golden("redis_ledger.json"):
        if c["op"] == "reply":
            assert _edax.redis_reply_error(c["reply"]) == c["error"], c["name"]
            continue
        try:
            got: dict[str, Any] = {"ok": _redis_ledger_case(c)}
        except RuntimeError as e:
            got = {"error": str(e)}
        assert got == c["want"], c["name"]


def test_golden_redis_scripts() -> None:
    """The four Lua bodies, verbatim: EVALSHA hashes the source, so a byte of drift in any one is a
    different script and a lost fence between SDKs."""
    bodies = {
        "claim": _edax.redis_claim_script(),
        "complete": _edax.redis_complete_script(),
        "release": _edax.redis_release_script(),
        "extend": _edax.redis_extend_script(),
    }
    assert [c["name"] for c in golden("redis_scripts.json")] == list(bodies)
    for c in golden("redis_scripts.json"):
        assert bodies[c["name"]] == c["body"], c["name"]


def test_guid_is_well_formed_v7() -> None:
    a, b = edax.guid(), edax.guid()
    assert a != b
    assert [len(p) for p in a.split("-")] == [8, 4, 4, 4, 12]
    assert a[14] == "7"  # version nibble
    assert a[19] in "89ab"  # RFC4122 variant


def test_current_timestamp_strictly_increasing() -> None:
    prev = 0
    for _ in range(10_000):
        t = edax.current_timestamp()
        assert t > prev  # never repeats or goes back
        prev = t


def test_golden_causation_originate() -> None:
    for c in golden("causation_originate.json"):
        assert originate(c["metadata"], c["fresh_id"]) == c["expected"], c


# Stands in for the guid `ensure_ids` mints; no real guid can equal it.
MINTED = "<minted>"


@pytest.mark.parametrize(
    "branch, metadata, want_correlation, want_causation",
    [
        ("no_metadata", None, MINTED, MINTED),
        ("empty_metadata", {}, MINTED, MINTED),
        ("other_keys_only", {"trace": "abc", "tenant": "t1"}, MINTED, MINTED),
        ("correlation_given", {"correlation_id": "corr-1"}, "corr-1", "corr-1"),
        ("causation_given", {"causation_id": "evt-0"}, MINTED, "evt-0"),
        ("both_given", {"correlation_id": "corr-1", "causation_id": "evt-0"}, "corr-1", "evt-0"),
        ("correlation_and_other_keys", {"correlation_id": "corr-1", "trace": "abc"}, "corr-1", "corr-1"),
    ],
)
def test_ensure_ids_fills_missing_and_preserves_given(
    branch: str,
    metadata: dict[str, str] | None,
    want_correlation: str,
    want_causation: str,
) -> None:
    given = dict(metadata or {})
    out = ensure_ids(metadata)

    correlation = out[CORRELATION_ID]
    if want_correlation == MINTED:
        assert len(correlation.split("-")) == 5, "a missing correlation id is minted as a guid"
    else:
        assert correlation == want_correlation, "a supplied id is never overwritten"
    # A wanted MINTED causation means it followed the correlation: a root event causes itself.
    want = correlation if want_causation == MINTED else want_causation
    assert out[CAUSATION_ID] == want

    extra = {k: v for k, v in given.items() if k not in (CORRELATION_ID, CAUSATION_ID)}
    assert {k: v for k, v in out.items() if k in extra} == extra, "unrelated keys pass through"
    assert metadata is None or metadata == given, "the caller's dict is not mutated"


@pytest.mark.parametrize(
    "name, wrapper",
    [
        ("propagate", causation.propagate),
        ("originate", causation.originate),
        ("ensure_ids", causation.ensure_ids),
    ],
)
def test_top_level_causation_names_are_the_wrappers(name: str, wrapper: object) -> None:
    """One public name per causation rule: the top level re-exports the `edax.causation` wrapper,
    never the raw kernel function, so both spellings apply the same defaulting rule."""
    assert name in edax.__all__, "the name is part of the public API"
    assert getattr(edax, name) is wrapper, "the top-level name is the causation wrapper itself"


def test_seeded_root_then_one_propagate_hop_is_a_connected_chain() -> None:
    # The root's correlation id is minted, so it differs from the root's event id — the hop has to
    # link two different ids and still keep one flow.
    root = ensure_ids({"trace": "abc"})
    child = propagate(root, "evt-root")

    assert root[CAUSATION_ID] == root[CORRELATION_ID], "the root causes itself"
    assert child[CORRELATION_ID] == root[CORRELATION_ID], "one flow across the hop"
    assert child[CAUSATION_ID] == "evt-root", "the child points back at the event that caused it"
    assert set(child) == {CORRELATION_ID, CAUSATION_ID}, "the parent's trace is not carried"


async def test_key_ordered_dispatch() -> None:
    workers, per_agg = 4, 50
    aggregates = ["agg-a", "agg-b", "agg-c", "agg-d", "agg-e", "agg-f"]
    received: dict[str, list[int]] = {a: [] for a in aggregates}

    async def handler(env: edax.EventEnvelope) -> None:
        await asyncio.sleep(0)  # force interleaving across workers
        received[env.aggregate_id].append(sequence_of(env))

    consumer = edax.KeyOrderedDispatcher(workers, handler)
    consumer.start()
    for seq in range(per_agg):
        for agg in aggregates:
            await consumer.submit(
                edax.EventEnvelope(
                    id=f"{agg}-{seq}",
                    aggregate_id=agg,
                    event=edax.Event("t", 1, b""),
                    namespace={},
                    metadata={},
                    timestamp=0,
                    sequence=seq,
                )
            )
    await consumer.stop()

    for agg in aggregates:
        got = received[agg]
        assert got == sorted(got), f"{agg} out of order: {got}"
        assert len(got) == per_agg

    used = {consumer._worker_index(a) for a in aggregates}
    assert len(used) >= 2, "aggregates should span multiple workers"

import asyncio
from typing import Any

import edax
import pytest
from conftest import (
    FakeKafkaClient,
    KafkaMsg,
    SpyProducerClient,
    build_command,
    build_message,
    golden,
    message_as_obj,
    message_from_obj,
    sequence_of,
)
from edax import _edax, causation
from edax.backend.kafka import KafkaConsumer, KafkaProducer
from edax.broker import UndecodableDelivery
from edax.causation import caused_by, current_lineage, originate, propagate, use_lineage


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


def test_golden_snapshot_due() -> None:
    """The snapshot interval :class:`~edax.store.Repository` asks the kernel about on every save."""
    for c in golden("snapshot_due.json"):
        assert _edax.snapshot_due(c["every"], c["initial"], c["current"]) is c["due"], c


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
            assert f"{caught.value.code}: {caught.value}" == c["error"]
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


def test_golden_tenancy() -> None:
    """`(mode, key, tenant) -> the key a seam is called with`, and the refusal where the mode wants a
    tenant and the call has none. Read through the SDK's own wrapper, so a composition that drifted from
    the kernel's would fail here rather than in a store."""
    for c in golden("tenancy.json"):
        mode = edax.Tenancy(c["mode"])
        assert (mode.requires_tenant, mode.composes_keys) == (c["requires_tenant"], c["composes_keys"]), c
        if c["error"] is None:
            assert edax.tenant_key(mode, c["key"], c["tenant"]) == c["expected"], c
            continue
        with pytest.raises(edax.errors.CodecError) as caught:
            edax.tenant_key(mode, c["key"], c["tenant"])
        assert str(caught.value) == c["error"].removeprefix("codec: "), c


def test_golden_envelopes() -> None:
    """Each message encodes to the vector's bytes in both formats, by its kind's serializer, and a decode
    re-encodes to the same bytes."""
    for case in golden("envelopes.json"):
        msg = message_from_obj(case["message"])
        assert message_as_obj(msg) == case["message"], case["name"]
        for fmt, want in (("json", case["json_hex"]), ("msgpack", case["msgpack_hex"])):
            ser = edax.KernelSerializer(fmt, case["kind"])
            wire = ser.encode(msg)
            assert wire.hex() == want, (case["name"], fmt)
            assert ser.decode(wire) == msg, (case["name"], fmt)


def test_a_serializer_refuses_the_other_kind() -> None:
    """An event serializer handed a command, or a command serializer handed an event, is the terminal
    ``CodecError``: the same message fails the same way on every retry."""
    event = build_message("agg-1", "Deposited", 1)
    command = build_command("Deposit")
    for kind, msg in (("event", command), ("command", event)):
        with pytest.raises(edax.CodecError) as refused:
            edax.KernelSerializer("json", kind).encode(msg)
        assert refused.value.terminal, kind


def test_golden_envelope_decode() -> None:
    """The message evolution rule, one wire value per case in both formats and of its kind: an unknown
    field and a missing optional field read, a positional (array) message, a repeated field, trailing
    bytes and a tenant outside the key rule are refused. A newer producer's field therefore never turns a
    message into poison for this reader, and a poisoned tenant never reaches a key."""
    for case in golden("envelope_decode.json"):
        for fmt in ("json", "msgpack"):
            wire = bytes.fromhex(case[f"{fmt}_hex"])
            got: dict[str, Any]
            try:
                got = {"message": message_as_obj(edax.KernelSerializer(fmt, case["kind"]).decode(wire))}
            except edax.CodecError as e:
                got = {"error_code": e.code}
            assert got == case["want"], (case["name"], fmt)


def _headers_from_obj(headers: list[dict[str, str]]) -> list[Any]:
    return [_edax.KafkaHeader(h["key"], bytes.fromhex(h["value_hex"])) for h in headers]


def test_golden_kafka_record() -> None:
    """The record a :class:`~edax.backend.kafka.KafkaProducer` sends, through a spy client: the
    serializer's bytes and media type in the kernel's record, keyed under the row's tenancy. A second SDK
    writing a different key or header set would split the topic in two."""
    for c in golden("kafka_record.json"):
        msg, key = message_from_obj(c["message"]), c["key"]
        client = SpyProducerClient()
        producer = KafkaProducer(
            client,
            "t",
            serializer=edax.KernelSerializer(c["format"]),
            command_serializer=edax.KernelSerializer(c["format"], "command"),
            key=None if key is None else lambda _, key=key: key,
            tenancy=edax.Tenancy(c["tenancy"]),
        )
        if c["error"] is not None:
            with pytest.raises(edax.CodecError) as refused:
                asyncio.run(producer.publish(msg))
            assert f"codec: {refused.value}" == c["error"], c["name"]
            assert client.sent == [], c["name"]
            continue
        asyncio.run(producer.publish(msg))
        [(_, value, sent_key, _, headers)] = client.sent
        assert (sent_key.hex(), value.hex()) == (c["key_hex"], c["value_hex"]), c["name"]
        assert [(k, v.hex()) for k, v in headers] == [(h["key"], h["value_hex"]) for h in c["headers"]], c[
            "name"
        ]


def test_golden_kafka_decode() -> None:
    """The consumer side, through a real :class:`~edax.backend.kafka.KafkaConsumer` over a fake client, so
    the vector proves the adapter and not a copy of its rule: the kernel routes the record by its first
    ``content-type``, an absent one falls back to the default, and a media type no serializer claims —
    the empty one included — is poison instead of a guess. A consumer of one kind registers that kind's
    serializers only, so a record of the other kind is poison too, and a command consumer has no
    header-less default. Every failure is a terminal ``CodecError``, so the consumer dead-letters the
    record instead of redelivering it forever."""
    for c in golden("kafka_decode.json"):
        kind = c["kind"]
        default = c["default_format"] or "json"
        other = "msgpack" if default == "json" else "json"
        client = FakeKafkaClient()
        headers = [(h["key"], bytes.fromhex(h["value_hex"])) for h in c["headers"]]
        value = bytes.fromhex(c["value_hex"])
        client.log.append(KafkaMsg("t", 0, 0, b"agg-1", value, headers))
        consumer = KafkaConsumer(
            client,
            serializer=edax.KernelSerializer(default, kind),
            serializers=[edax.KernelSerializer(other, kind)],
        )
        delivery = asyncio.run(consumer.poll())
        assert delivery is not None, c["name"]
        if "message" in c["want"]:
            assert not isinstance(delivery, UndecodableDelivery), c["name"]
            assert message_as_obj(delivery.message) == c["want"]["message"], c["name"]
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
    crosses as — and the refusal of a registry that names a media type twice."""
    for c in golden("kafka_route.json"):
        if "error" in c["want"]:
            with pytest.raises(edax.AlreadyRegistered) as refused, edax.errors.mapped():
                _edax.kafka_check_media_types(c["registered"])
            assert f"already_registered: {refused.value}" == c["want"]["error"], c["name"]
            continue
        _edax.kafka_check_media_types(c["registered"])
        route = _edax.kafka_route(_headers_from_obj(c["headers"]), c["registered"], c["default_kind"])
        got: dict[str, Any]
        if route.registered is not None:
            got = {"route": "registered", "index": route.registered}
        elif route.unknown is not None:
            got = {"route": "unknown", "media_type": route.unknown}
        elif route.headerless:
            got = {"route": "headerless"}
        else:
            got = {"route": "default"}
        assert got == c["want"], c["name"]


def test_golden_kafka_dead_letter() -> None:
    """The record a dead-letter sink publishes: key, value and headers verbatim plus the four
    ``edax-dlq-*`` headers. The rows for the kernel's reader of those headers replay in Rust only: no
    SDK reads a dead-letter record back."""
    for c in golden("kafka_dead_letter.json"):
        if "record" not in c:
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
    """The bytes under an idempotency key: ``1<owner><record>``, and the record read back out of them
    the way a claim reads the key's holder; a value the split refuses reads as a cached codec failure. A
    second SDK that laid them out differently could not read this SDK's ledger. A claim value with a fixed owner is a kernel test hook, so those rows replay in
    Rust only."""
    reader = _edax.redis_new_claim(0)  # a claim of this SDK's own, so every held value reads as a prior
    for c in golden("redis_value.json"):
        name, op = c["name"], c["op"]
        if op == "settled":
            assert _edax.redis_settled_value(c["claim"].encode(), c["record_json"]) == c["value"].encode(), (
                name
            )
        elif op == "split":
            raw, want = bytes.fromhex(c["raw_hex"]), c["want"]
            if "error" in want:
                # A value the split refuses still holds the key: the claimant reads the kernel's
                # unreadable record, a codec failure under its own hash naming the split's refusal.
                prior = _edax.redis_claim_outcome(reader, raw)
                assert prior is not None, name
                code, _, cause = want["error"].partition(": ")
                failed = edax.Record(0, error=f"unreadable idempotency record: {cause}", error_code=code)
                assert edax.command.decode_prior(prior, 0) == failed, name
                continue
            assert _edax.redis_claim_outcome(reader, raw) == want["record"], name
        elif op != "claim":
            raise AssertionError(f"{name}: unknown op {op}")


def _redis_ledger_case(c: dict[str, Any]) -> Any:
    """One ``redis_ledger.json`` operation through the binding: its value, raising what the kernel raises."""
    op = c["op"]
    if op == "key":
        return _edax.redis_ledger_key(c["prefix"], c["key"]).hex()
    if op == "config":
        return _edax.redis_check_config(
            c["prefix"], c["ttl_ms"], c["claim_ttl_ms"], c["op_timeout_ms"], c["heartbeat"]
        )
    if op == "handle":
        return _edax.redis_check_claim(bytes.fromhex(c["handle_hex"]))
    if op == "outcome":
        reply = None if c["reply_hex"] is None else bytes.fromhex(c["reply_hex"])
        prior = _edax.redis_claim_outcome(c["claim"].encode(), reply)
        return {"won": True} if prior is None else {"prior": prior}
    if op == "forget":
        held = None if c["held_hex"] is None else bytes.fromhex(c["held_hex"])
        return "delete" if _edax.redis_forget_outcome(c["key"], held) else "nothing"
    raise AssertionError(f"{c['name']}: unknown op {op}")


def test_golden_redis_ledger() -> None:
    """The key, the refused settings and handles, the claim outcome, the forget verdict and the reply
    verdicts: every
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
    """The five Lua bodies, verbatim: EVALSHA hashes the source, so a byte of drift in any one is a
    different script and a lost fence between SDKs."""
    scripts = _edax.redis_scripts()
    bodies = {
        "claim": scripts.claim,
        "complete": scripts.complete,
        "release": scripts.release,
        "forget": scripts.forget,
        "extend": scripts.extend,
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


def test_golden_causation() -> None:
    """A child's lineage from its parent's id and correlation, through the SDK wrapper over a real parent
    message, and through the raw kernel call."""
    for c in golden("causation.json"):
        parent = build_message(
            "agg-1", "Parent", 1, id=c["parent_id"], correlation_id=c["parent_correlation_id"]
        )
        want = edax.Lineage(**c["expected"])
        assert propagate(parent) == want, c
        assert _edax.propagate(c["parent_id"], c["parent_correlation_id"]) == want, c


def test_golden_causation_originate() -> None:
    """A root's lineage fills only what is missing; ``fresh_id`` stands in for the guid the SDK wrapper
    mints, so the kernel call is replayed with it."""
    for c in golden("causation_originate.json"):
        got = _edax.originate(c["fresh_id"], c["correlation_id"], c["causation_id"])
        assert got == edax.Lineage(**c["expected"]), c


@pytest.mark.parametrize(
    "branch, correlation_id, causation_id",
    [
        ("nothing_given", None, None),
        ("correlation_given", "corr-1", None),
        ("causation_given", None, "evt-0"),
        ("both_given", "corr-1", "evt-0"),
    ],
)
def test_originate_mints_only_what_is_missing(
    branch: str, correlation_id: str | None, causation_id: str | None
) -> None:
    """The wrapper mints a guid for a missing correlation and lets the kernel default the causation to the
    resulting correlation: a root causes itself. A supplied id is never overwritten."""
    got = originate(correlation_id, causation_id)
    if correlation_id is None:
        assert len(got.correlation_id.split("-")) == 5, f"{branch}: a missing correlation is a minted guid"
    else:
        assert got.correlation_id == correlation_id, branch
    assert got.causation_id == (causation_id or got.correlation_id), branch


@pytest.mark.parametrize(
    "name, wrapper",
    [
        ("propagate", causation.propagate),
        ("originate", causation.originate),
        ("caused_by", causation.caused_by),
        ("current_lineage", causation.current_lineage),
        ("use_lineage", causation.use_lineage),
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
    root_lineage = originate()
    root = build_message(
        "agg-1",
        "Root",
        1,
        id="evt-root",
        correlation_id=root_lineage.correlation_id,
        causation_id=root_lineage.causation_id,
    )
    child = propagate(root)

    assert root_lineage.causation_id == root_lineage.correlation_id, "the root causes itself"
    assert child.correlation_id == root_lineage.correlation_id, "one flow across the hop"
    assert child.causation_id == "evt-root", "the child points back at the event that caused it"


def test_the_ambient_lineage_is_scoped_to_its_block() -> None:
    """Outside any block there is none; ``caused_by`` sets the child lineage of its message, a nested
    ``use_lineage`` shadows it, and each exit restores what was there before."""
    parent = build_message("agg-1", "Parent", 1, id="evt-1", correlation_id="flow-1")
    assert current_lineage() is None
    with caused_by(parent) as outer:
        assert current_lineage() == outer == edax.Lineage("flow-1", "evt-1")
        with use_lineage(edax.Lineage("flow-2", "cmd-2")):
            assert current_lineage() == edax.Lineage("flow-2", "cmd-2")
        assert current_lineage() == outer
    assert current_lineage() is None


async def test_key_ordered_dispatch() -> None:
    workers, per_agg = 4, 50
    aggregates = ["agg-a", "agg-b", "agg-c", "agg-d", "agg-e", "agg-f"]
    received: dict[str, list[int]] = {a: [] for a in aggregates}

    async def handler(msg: edax.EventMessage) -> None:
        await asyncio.sleep(0)  # force interleaving across workers
        received[msg.aggregate_id].append(sequence_of(msg))

    consumer = edax.KeyOrderedDispatcher(workers, handler)
    consumer.start()
    for seq in range(per_agg):
        for agg in aggregates:
            await consumer.submit(build_message(agg, "t", seq, id=f"{agg}-{seq}"))
    await consumer.stop()

    for agg in aggregates:
        got = received[agg]
        assert got == sorted(got), f"{agg} out of order: {got}"
        assert len(got) == per_agg

    used = {consumer._worker_index(build_message(a, "t", 0)) for a in aggregates}
    assert len(used) >= 2, "aggregates should span multiple workers"

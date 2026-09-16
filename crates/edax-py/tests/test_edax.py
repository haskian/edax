import asyncio
import base64
import json
from pathlib import Path
from typing import Any, Optional

import edax
import pytest
from conftest import sequence_of
from edax import _edax, causation, errors
from edax.causation import CAUSATION_ID, CORRELATION_ID, ensure_ids, originate, propagate

GOLDEN = Path(__file__).resolve().parents[3] / "testdata" / "golden"


def load(name: str) -> list[dict[str, Any]]:
    return json.loads((GOLDEN / name).read_text())


def test_golden_partition() -> None:
    for c in load("partition.json"):
        assert edax.partition(c["id"], c["n"]) == c["expected"]


def test_golden_dedup() -> None:
    for c in load("dedup.json"):
        assert edax.should_apply(c["stored"], c["incoming"]) == c["expected"]


def test_golden_sequence_batch() -> None:
    """The dense-batch rule, one case per branch. The adapter's `append` calls this same kernel
    function, so a durable store cannot invent its own version of the rule."""
    for c in load("sequence_batch.json"):
        if c["error"] is None:
            assert _edax.check_sequence_batch(c["expected"], c["sequences"]) is None
        else:
            with pytest.raises(RuntimeError) as caught:
                _edax.check_sequence_batch(c["expected"], c["sequences"])
            assert str(caught.value) == c["error"]


def test_golden_backoff() -> None:
    for c in load("backoff.json"):
        assert edax.backoff_millis(c["attempt"]) == c["expected_millis"]


def _envelope_from_obj(o: dict[str, Any]) -> edax.EventEnvelope:
    ev = o["event"]
    return edax.Envelope(
        id=o["id"],
        aggregate_id=o["aggregate_id"],
        event=edax.Event(ev["name"], ev["version"], base64.b64decode(ev["data"])),
        timestamp=o["timestamp"],
        sequence=o["sequence"],
        namespace=o["namespace"],
        metadata=o["metadata"],
    )


def test_golden_envelopes() -> None:
    for case in load("envelopes.json"):
        env = _envelope_from_obj(case["envelope"])
        for fmt, want in (("json", case["json_hex"]), ("msgpack", case["msgpack_hex"])):
            ser = edax.kernel(fmt)
            wire = ser.encode(env)
            assert wire.hex() == want, fmt
            # decode -> re-encode reproduces the same wire bytes
            assert ser.encode(ser.decode(wire)).hex() == want, fmt


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
    """The record a producer sends: key rule, value bytes and the four contract headers. A second
    SDK writing a different key or header set would split the topic in two."""
    for c in load("kafka_record.json"):
        env = _envelope_from_obj(c["envelope"])
        record = _edax.encode_kafka_record(c["format"], env, c["key"])
        assert record.key.hex() == c["key_hex"], c["name"]
        assert record.value.hex() == c["value_hex"], c["name"]
        got = [(h.key, h.value.hex()) for h in record.headers]
        assert got == [(h["key"], h["value_hex"]) for h in c["headers"]], c["name"]


def test_golden_kafka_decode() -> None:
    """The consumer side: the first ``content-type`` picks the format, an absent one falls back to
    the default, and an unknown one is refused instead of guessed at."""
    for c in load("kafka_decode.json"):
        value, headers = bytes.fromhex(c["value_hex"]), _headers_from_obj(c["headers"])
        want = c["want"]
        if "envelope" in want:
            env = _edax.decode_kafka_record(c["default_format"], value, headers)
            assert _envelope_as_obj(env) == want["envelope"], c["name"]
            continue
        with pytest.raises(RuntimeError) as caught:
            _edax.decode_kafka_record(c["default_format"], value, headers)
        assert want["error"] in str(caught.value), c["name"]
        # The same failure through the SDK's mapper is the terminal CodecError, so the consumer
        # dead-letters the record instead of redelivering it as a transient fault forever.
        with pytest.raises(edax.CodecError), errors.mapped():
            _edax.decode_kafka_record(c["default_format"], value, headers)


def test_golden_kafka_offsets() -> None:
    """The offset tracker replayed step by step: settlement is by token, so a stale token from a
    revoked partition decides nothing. Every scenario ends with the per-partition stats it left."""
    for c in load("kafka_offsets.json"):
        tracker = _edax.offset_tracker()  # fresh per scenario: tokens are per tracker
        for i, step in enumerate(c["steps"]):
            where = f"{c['name']} step {i} ({step['op']})"
            op, want = step["op"], step["want"]
            if op == "book":
                assert tracker.book(step["topic"], step["partition"], step["offset"]) == want["token"], where
            elif op == "ack":
                decision = tracker.ack(step["token"])
                assert (decision.commit, decision.offset) == (want["commit"], want["offset"]), where
            elif op == "nack":
                decision = tracker.nack(step["token"])
                assert (decision.seek, decision.offset) == (want["seek"], want["offset"]), where
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
                        "max_delivered": r.max_delivered,
                        "committed": r.committed,
                    }
                    for r in tracker.stats().items
                ]
                assert rows == want["rows"], where
            else:
                raise AssertionError(f"{where}: unknown op")


def test_golden_redis_value() -> None:
    """The bytes under an idempotency key: ``<owner><record>``, and the split back out of them. A
    second SDK that laid them out differently could not read this SDK's ledger."""
    for c in load("redis_value.json"):
        name, op = c["name"], c["op"]
        if op == "claim":
            assert _edax.redis_claim_value(c["owner"], c["payload_hash"]) == c["value"], name
        elif op == "record":
            assert _edax.redis_record_value(c["owner"], c["record_json"]) == c["value"], name
        elif op == "split":
            raw, want = bytes.fromhex(c["raw_hex"]), c["want"]
            if "error" in want:
                with pytest.raises(RuntimeError) as caught:
                    _edax.redis_split_value(raw)
                assert want["error"] in str(caught.value), name
                continue
            split = _edax.redis_split_value(raw)
            assert (split.owner, split.record) == (want["owner"], want["record"]), name
        else:
            raise AssertionError(f"{name}: unknown op {op}")


def test_golden_redis_scripts() -> None:
    """The two Lua fence bodies, verbatim: EVALSHA hashes the source, so a byte of drift in either
    one is a different script and a lost fence between SDKs."""
    bodies = {
        "fenced_set": _edax.redis_fenced_set_script(),
        "fenced_del": _edax.redis_fenced_del_script(),
    }
    for c in load("redis_scripts.json"):
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
    for c in load("causation_originate.json"):
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
    metadata: Optional[dict[str, str]],
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

    async def handler(env: edax.Envelope) -> None:
        await asyncio.sleep(0)  # force interleaving across workers
        received[env.aggregate_id].append(sequence_of(env))

    consumer = edax.KeyOrderedDispatcher(workers, handler)
    consumer.start()
    for seq in range(per_agg):
        for agg in aggregates:
            await consumer.submit(
                edax.Envelope(
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

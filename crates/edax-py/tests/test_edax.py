import asyncio
import base64
import json
from pathlib import Path
from typing import Any, Optional

import edax
import pytest
from conftest import sequence_of
from edax import _edax, causation
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

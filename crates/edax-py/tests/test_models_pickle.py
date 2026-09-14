"""copy / deepcopy / pickle round-trip for the native model classes (copyreg-registered in aggregate.py).

The native Event/EventEnvelope are extension types with no __reduce__; without the registration these
would raise. Guards multiprocessing use (handing an envelope to another process) and the copy paths.
"""

import copy
import pickle
from typing import Any

import edax
import pytest


def _envelope(**overrides: Any) -> edax.EventEnvelope:
    fields: dict[str, Any] = {
        "id": "e1",
        "aggregate_id": "acct-1",
        "event": edax.Event("Deposited", 1, b"\x00\x01"),
        "namespace": {"tenant": "t1"},
        "metadata": {"trace": "x"},
        "timestamp": 42,
        "sequence": 3,
    }
    return edax.Envelope(**{**fields, **overrides})


# Every field the reducer has to carry, one case per shape it can take. `sequence=None` is the one
# optional field: an outbox row published before sequencing round-trips through the same reducer.
ENVELOPES: dict[str, edax.EventEnvelope] = {
    "full": _envelope(),
    "no_sequence": _envelope(sequence=None),
    "empty_dicts": _envelope(namespace={}, metadata={}),
    "empty_payload": _envelope(event=edax.Event("Deposited", 1, b"")),
    "non_ascii_metadata": _envelope(metadata={"trace": "déjà-vu"}, namespace={"region": "eu"}),
}


def _same(a: edax.EventEnvelope, b: edax.EventEnvelope) -> bool:
    return (
        a.id == b.id
        and a.aggregate_id == b.aggregate_id
        and a.timestamp == b.timestamp
        and a.sequence == b.sequence
        and a.namespace == b.namespace
        and a.metadata == b.metadata
        and a.event.name == b.event.name
        and a.event.version == b.event.version
        and a.event.data == b.event.data
    )


@pytest.mark.parametrize("shape", list(ENVELOPES))
def test_copy_and_deepcopy_round_trip(shape: str) -> None:
    v = ENVELOPES[shape]
    assert _same(v, copy.copy(v))
    assert _same(v, copy.deepcopy(v))


@pytest.mark.parametrize("shape", list(ENVELOPES))
def test_pickle_round_trips_every_protocol(shape: str) -> None:
    v = ENVELOPES[shape]
    for proto in range(pickle.HIGHEST_PROTOCOL + 1):
        assert _same(v, pickle.loads(pickle.dumps(v, proto))), proto


def test_event_pickles_standalone() -> None:
    e = edax.Event("X", 2, b"d")
    r = pickle.loads(pickle.dumps(e))
    assert (r.name, r.version, r.data) == ("X", 2, b"d")

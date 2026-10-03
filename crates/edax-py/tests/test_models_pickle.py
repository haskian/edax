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
    return edax.EventEnvelope(**{**fields, **overrides})


# Every field the reducer has to carry, one case per shape it can take. `sequence=None` is the one
# optional field: an outbox row published before sequencing round-trips through the same reducer.
ENVELOPES: dict[str, edax.EventEnvelope] = {
    "full": _envelope(),
    "no_sequence": _envelope(sequence=None),
    "empty_dicts": _envelope(namespace={}, metadata={}),
    "empty_payload": _envelope(event=edax.Event("Deposited", 1, b"")),
    "non_ascii_metadata": _envelope(metadata={"trace": "déjà-vu"}, namespace={"region": "eu"}),
}


@pytest.mark.parametrize("shape", list(ENVELOPES))
def test_copy_and_deepcopy_round_trip(shape: str) -> None:
    v = ENVELOPES[shape]
    assert v == copy.copy(v)
    assert v == copy.deepcopy(v)


@pytest.mark.parametrize("shape", list(ENVELOPES))
def test_pickle_round_trips_every_protocol(shape: str) -> None:
    v = ENVELOPES[shape]
    for proto in range(pickle.HIGHEST_PROTOCOL + 1):
        assert v == pickle.loads(pickle.dumps(v, proto)), proto


def test_event_pickles_standalone() -> None:
    e = edax.Event("X", 2, b"d")
    r = pickle.loads(pickle.dumps(e))
    assert (r.name, r.version, r.data) == ("X", 2, b"d")


# =========================================================================
# value semantics on the native models
# =========================================================================


def test_a_getters_fresh_copy_equals_itself() -> None:
    """Each getter builds a new native object; with identity equality ``env.event == env.event`` was False."""
    v = _envelope()
    assert v.event == v.event
    assert v == _envelope(), "two envelopes built from equal values are equal"
    assert hash(v) == hash(_envelope())
    assert len({_envelope(), _envelope()}) == 1, "a set of equal envelopes holds one"


@pytest.mark.parametrize(
    "field, value",
    [
        ("id", "e2"),
        ("aggregate_id", "acct-2"),
        ("event", edax.Event("Deposited", 2, b"\x00\x01")),
        ("namespace", {"tenant": "t2"}),
        ("metadata", {"trace": "y"}),
        ("timestamp", 43),
        ("sequence", None),
    ],
)
def test_one_differing_field_makes_two_envelopes_unequal(field: str, value: Any) -> None:
    assert _envelope() != _envelope(**{field: value}), field


@pytest.mark.parametrize(
    "event, other",
    [
        (edax.Event("Deposited", 1, b"x"), edax.Event("Withdrawn", 1, b"x")),
        (edax.Event("Deposited", 1, b"x"), edax.Event("Deposited", 2, b"x")),
        (edax.Event("Deposited", 1, b"x"), edax.Event("Deposited", 1, b"y")),
    ],
    ids=["name", "version", "data"],
)
def test_events_compare_by_every_field(event: edax.Event, other: edax.Event) -> None:
    assert event != other and event == edax.Event(event.name, event.version, event.data)


def test_a_model_never_equals_another_type() -> None:
    v = _envelope()
    assert v != "e1" and v != v.event and (v == object()) is False
    assert v.__eq__(object()) is NotImplemented


def test_a_saga_command_has_value_semantics_and_round_trips() -> None:
    cmd = edax.SagaCommand("order-7:2:0", "order-7", edax.Event("Fulfill", 1, b'{"order_id":"order-7"}'), 7)
    same = edax.SagaCommand("order-7:2:0", "order-7", edax.Event("Fulfill", 1, b'{"order_id":"order-7"}'), 7)
    assert cmd == same and hash(cmd) == hash(same) and pickle.loads(pickle.dumps(cmd)) == cmd
    assert repr(cmd).startswith("SagaCommand(id='order-7:2:0'")


@pytest.mark.parametrize("shape", list(ENVELOPES))
def test_a_round_trip_is_equal_to_the_original(shape: str) -> None:
    v = ENVELOPES[shape]
    assert pickle.loads(pickle.dumps(v)) == v and copy.deepcopy(v) == v


def test_the_repr_names_every_field_and_the_public_module() -> None:
    assert type(_envelope()).__module__ == "edax" and type(_envelope().event).__module__ == "edax"
    assert repr(_envelope()) == (
        "EventEnvelope(id='e1', aggregate_id='acct-1', event=Event(name='Deposited', version=1, "
        "data=b'\\x00\\x01'), namespace={'tenant': 't1'}, metadata={'trace': 'x'}, timestamp=42, sequence=3)"
    )


def test_the_dictionaries_come_back_in_one_order_in_every_process() -> None:
    """The binding's map iterates in a per-process hash order; a host writing ``json.dumps(env.metadata)``
    to a column got different bytes per process. The order is the kernel's wire order, by key, in every
    process whatever its hash seed."""
    import os
    import subprocess
    import sys

    code = (
        "import edax, json\n"
        "e = edax.EventEnvelope('e1', 'a', edax.Event('n', 1, b''), "
        "{k: k for k in 'hgfedcba'}, {k: k for k in 'zyxwvuts'}, 0)\n"
        "print(json.dumps([list(e.namespace), list(e.metadata)]))"
    )
    seen = {
        subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, "PYTHONHASHSEED": str(seed)},
        ).stdout
        for seed in range(4)
    }
    assert seen == {'[["a", "b", "c", "d", "e", "f", "g", "h"], ["s", "t", "u", "v", "w", "x", "y", "z"]]\n'}

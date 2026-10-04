"""copy / deepcopy / pickle round-trip for the native model classes (copyreg-registered in aggregate.py).

The native records are extension types with no __reduce__; without the registration these would raise.
Guards multiprocessing use (handing a message to another process) and the copy paths. The reducers build
by keyword, because the binding moves a constructor's optional parameters last: a positional rebuild
would hand ``sequence`` the value of ``name``.
"""

import copy
import json
import pickle
from typing import Any

import edax
import pytest
from conftest import build_command, build_message
from edax.aggregate import _replace
from edax.testing import drafted

#: The base fixtures: every field set, each to a value no other field shares.
MESSAGE = build_message(
    "acct-1",
    "Deposited",
    3,
    id="e1",
    data=b"\x00\x01",
    correlation_id="flow-1",
    causation_id="cmd-1",
    occurred_at=41,
    recorded_at=42,
    extensions={"traceparent": "00-a-b-01"},
    metadata={"trace": "x"},
)
DRAFT = drafted(MESSAGE)
COMMAND = build_command(
    "Fulfill",
    id="order-7:2:0",
    idempotency_key="order-7:2:0",
    aggregate_id="order-7",
    version=2,
    data=b'{"order_id":"order-7"}',
    issued_at=7,
    extensions={"traceparent": "00-a-b-01"},
    metadata={"user": "u-7"},
)
EVENT = edax.Event("Deposited", 1, b"\x00\x01")


# Every field a reducer has to carry, one case per shape it can take: each optional field set and unset.
MODELS: dict[str, Any] = {
    "message_full": MESSAGE,
    "message_no_sequence_no_occurred_at": _replace(MESSAGE, sequence=None, occurred_at=None),
    "message_empty_dicts": _replace(MESSAGE, extensions={}, metadata={}),
    "message_empty_payload": _replace(MESSAGE, data=b""),
    "message_non_ascii_metadata": _replace(MESSAGE, metadata={"trace": "déjà-vu"}),
    "draft_full": DRAFT,
    "draft_no_sequence_no_occurred_at": _replace(DRAFT, sequence=None, occurred_at=None),
    "command_full": COMMAND,
    "command_no_key_no_aggregate": _replace(COMMAND, idempotency_key=None, aggregate_id=None),
    "event": EVENT,
    "lineage": edax.Lineage("flow-1", "evt-1"),
}


@pytest.mark.parametrize("shape", list(MODELS))
def test_copy_and_deepcopy_round_trip(shape: str) -> None:
    v = MODELS[shape]
    assert v == copy.copy(v)
    assert v == copy.deepcopy(v)


@pytest.mark.parametrize("shape", list(MODELS))
def test_pickle_round_trips_every_protocol(shape: str) -> None:
    v = MODELS[shape]
    for proto in range(pickle.HIGHEST_PROTOCOL + 1):
        assert v == pickle.loads(pickle.dumps(v, proto)), proto


# =========================================================================
# value semantics on the native models
# =========================================================================


def test_a_getters_fresh_copy_equals_itself() -> None:
    """Each getter builds a new native object; with identity equality two equal messages were unequal."""
    rebuilt = _replace(MESSAGE)
    assert rebuilt == MESSAGE, "two messages built from equal values are equal"
    assert hash(MESSAGE) == hash(rebuilt)
    assert len({MESSAGE, rebuilt}) == 1, "a set of equal messages holds one"


# One row per field of each value class: changing that field alone makes the two unequal.
UNEQUAL: list[tuple[Any, str, Any]] = [
    (MESSAGE, "id", "e2"),
    (MESSAGE, "aggregate_id", "acct-2"),
    (MESSAGE, "sequence", None),
    (MESSAGE, "name", "Withdrawn"),
    (MESSAGE, "version", 2),
    (MESSAGE, "data", b"\x00"),
    (MESSAGE, "correlation_id", "flow-2"),
    (MESSAGE, "causation_id", "cmd-2"),
    (MESSAGE, "occurred_at", None),
    (MESSAGE, "recorded_at", 43),
    (MESSAGE, "extensions", {"traceparent": "00-c-d-01"}),
    (MESSAGE, "metadata", {"trace": "y"}),
    (COMMAND, "id", "order-7:2:1"),
    (COMMAND, "idempotency_key", None),
    (COMMAND, "aggregate_id", None),
    (COMMAND, "name", "Refund"),
    (COMMAND, "version", 1),
    (COMMAND, "data", b"{}"),
    (COMMAND, "correlation_id", "flow-2"),
    (COMMAND, "causation_id", "evt-2"),
    (COMMAND, "issued_at", 8),
    (COMMAND, "extensions", {}),
    (COMMAND, "metadata", {}),
    (EVENT, "name", "Withdrawn"),
    (EVENT, "version", 2),
    (EVENT, "data", b"\x00"),
]


@pytest.mark.parametrize(
    "model, field, value", UNEQUAL, ids=[f"{type(model).__name__}.{field}" for model, field, _ in UNEQUAL]
)
def test_one_differing_field_makes_two_models_unequal(model: Any, field: str, value: Any) -> None:
    assert _replace(model) == model != _replace(model, **{field: value}), field


def test_a_model_never_equals_another_type() -> None:
    """A draft and the message stored from it share every field the draft has, yet they are two types."""
    v = MESSAGE
    assert v != "e1" and v != DRAFT and (v == object()) is False
    assert v.__eq__(object()) is NotImplemented


def test_the_repr_names_every_field_and_the_public_module() -> None:
    for model in (MESSAGE, DRAFT, COMMAND, EVENT, edax.Lineage("a", "b")):
        assert type(model).__module__ == "edax", type(model)
    assert repr(MESSAGE) == (
        "EventMessage(id='e1', aggregate_id='acct-1', tenant=None, sequence=3, name='Deposited', version=1, "
        "data=b'\\x00\\x01', correlation_id='flow-1', causation_id='cmd-1', occurred_at=41, recorded_at=42, "
        "extensions={'traceparent': '00-a-b-01'}, metadata={'trace': 'x'})"
    )
    assert repr(edax.Lineage("flow-1", "evt-1")) == "Lineage(correlation_id='flow-1', causation_id='evt-1')"


def test_the_dictionaries_come_back_in_one_order_in_every_process() -> None:
    """The binding's map iterates in a per-process hash order; a host writing ``json.dumps(msg.metadata)``
    to a column got different bytes per process. The order is the kernel's wire order, by key, in every
    process whatever its hash seed."""
    import os
    import subprocess
    import sys

    code = (
        "import edax, json\n"
        "e = edax.EventMessage(id='e1', aggregate_id='a', name='n', version=1, data=b'', correlation_id='c', "
        "causation_id='c', recorded_at=0, extensions={k: k for k in 'hgfedcba'}, "
        "metadata={k: k for k in 'zyxwvuts'})\n"
        "c = edax.CommandMessage(id='c1', name='n', version=1, data=b'', correlation_id='c', "
        "causation_id='c', issued_at=0, extensions={k: k for k in 'hgfedcba'}, "
        "metadata={k: k for k in 'zyxwvuts'})\n"
        "print(json.dumps([list(e.extensions), list(e.metadata), list(c.extensions), list(c.metadata)]))"
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
    first, second = list("abcdefgh"), list("stuvwxyz")
    assert len(seen) == 1
    assert json.loads(seen.pop()) == [first, second, first, second]

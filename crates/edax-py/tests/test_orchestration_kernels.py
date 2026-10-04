"""The orchestration decisions both SDKs make, replayed from the kernels' golden vectors through the Python
SDK's own seams: the retry and dead-letter policy (through the dispatcher and the relay), the guard's
settle table, the saga step, the upcast plan through the codec, and the event codec's wire shape against
the bytes the Rust registry codec writes."""

from __future__ import annotations

import base64
from typing import Any

import edax
import msgspec
import pytest
from conftest import build_message, golden, message_as_obj, recording_pacing
from edax import _edax
from edax.backend.memory import MemoryEventStore
from edax.codec import json_codec, msgpack_codec
from edax.testing import drafted

# =========================================================================
# retries.json: the consumer's retry and dead-letter policy
# =========================================================================


def _verdict(verdict: Any) -> dict[str, int]:
    return {"dead": verdict.attempt} if verdict.dead else {"nack": verdict.step}


def test_golden_retries() -> None:
    for scenario in golden("retries.json"):
        policy = _edax.retries(scenario["max_attempts"], scenario["sink"])
        for op in scenario["ops"]:
            name = op["op"]
            where = f"{scenario['name']}: {op}"
            if name == "attempt":
                assert policy.attempt(op["id"], op["broker_attempt"]) == op["want"], where
            elif name == "failed":
                verdict = policy.failed(op["id"], op["broker_attempt"], op["code"])
                assert _verdict(verdict) == op["want"], where
            elif name == "succeeded":
                policy.succeeded(op["id"])
            elif name == "dead_lettered":
                policy.dead_lettered(op["id"])
            elif name == "sink_failed":
                assert policy.sink_failed(op["id"], op["attempt"]) == op["want"], where
            else:
                tracked = policy.tracked()
                assert {"attempts": tracked.attempts, "contended": tracked.contended} == op["want"], where


def _kind(code: str | None) -> str:
    """The failure kind the kernel reads off ``code``, read back through a fresh policy with a sink and no
    ceiling: a terminal failure dies at once, contention is tracked apart, a transient one is counted."""
    policy = _edax.retries(0, True)
    if policy.failed("evt-1", 0, code).dead:
        return "terminal"
    return "contended" if policy.tracked().contended else "transient"


def test_golden_retry_failure() -> None:
    """Every code's failure kind, through the policy's own entry point, and through the SDK's mapping from
    an exception to the code it carries: an edax error by its class, a host exception by its verdict."""
    rows = golden("retry_failure.json")
    assert {row["code"] for row in rows} == {None, *_edax.error_codes()}, "every code has a row"
    for row in rows:
        assert _kind(row["code"]) == row["failure"], row
        failure = (
            ValueError("a host bug") if row["code"] is None else edax.errors.from_wire(f"{row['code']}: boom")
        )
        assert _kind(edax.errors._carried_code(failure)) == row["failure"], row


class _HostRejection(Exception):
    terminal = True


@pytest.mark.parametrize(
    "error, want",
    [
        pytest.param(
            _HostRejection("insufficient funds"), "rejected", id="a_terminal_host_exception_is_rejected"
        ),
        pytest.param(ValueError("a host bug"), None, id="an_unclassified_exception_carries_no_code"),
        pytest.param(edax.InProgress("fulfill-7"), "in_progress", id="an_edax_error_carries_its_own_code"),
    ],
)
def test_the_code_a_failure_carries(error: Exception, want: str | None) -> None:
    assert edax.errors._carried_code(error) == want


def test_an_unknown_error_code_is_a_codec_error() -> None:
    with pytest.raises(edax.CodecError, match="unknown error code: poison"), edax.errors.mapped():
        _edax.retries(0, True).failed("evt-1", 0, "poison")


@pytest.mark.parametrize(
    "scenario",
    [
        s
        for s in golden("retries.json")
        if s["name"] == "a_failed_sink_keeps_the_attempt_and_the_redelivery_dies_again"
    ],
)
async def test_the_dispatcher_settles_as_the_golden_policy_answers(scenario: dict[str, Any]) -> None:
    """The dispatcher performs the kernel's answers: the same script, run as deliveries, settles the way
    the vector says (nack, then dead with a refused sink, then dead again)."""
    steps: list[float] = []

    async def record(seconds: float) -> None:
        steps.append(seconds)

    sink_calls: list[int] = []

    async def on_dead(delivery: edax.Delivery, error: BaseException) -> None:
        sink_calls.append(len(sink_calls))
        if len(sink_calls) == 1:
            raise edax.StoreError("dead-letter table unavailable")

    async def handler(env: edax.EventMessage) -> None:
        raise edax.StoreError("projection unavailable")

    dispatcher = edax.KeyOrderedDispatcher(
        1, handler, max_attempts=scenario["max_attempts"], on_dead=on_dead, pacing=edax.Pacing(sleep=record)
    )
    dispatcher.start()
    settled: list[str] = []
    env = build_message("acct-1", "Deposited", 1, id="evt-1")
    for _ in range(3):
        await dispatcher.submit(
            edax.Delivery(env, lambda: settled.append("ack"), lambda: settled.append("nack"))
        )
        await dispatcher._queues[0].join()
    await dispatcher.stop()
    # attempt 1 nacks; attempt 2 dies and the sink refuses (nack); attempt 3 dies and the sink takes it.
    assert settled == ["nack", "nack", "ack"]
    assert sink_calls == [0, 1]
    assert dispatcher.stats().attempts_tracked == 0


# =========================================================================
# The relay over an outbox: the same policy keyed by row id
# =========================================================================


async def _outbox_of(n: int) -> MemoryEventStore:
    store = MemoryEventStore()
    await store.append(
        None,
        "acct-r",
        0,
        [drafted(build_message("acct-r", "Deposited", seq, id=f"acct-r-{seq}")) for seq in range(1, n + 1)],
    )
    return store


TRANSIENT = edax.StoreError("broker unavailable")
TERMINAL = edax.CodecError("message too large")
CONTENDED = edax.InProgress("fulfill-7")


@pytest.mark.parametrize(
    "rows, sink, max_attempts, drains, want_results, want_left, want_dead, want_steps",
    [
        pytest.param([None] * 4, None, 0, 1, [4], [], [], [], id="every_row_published_and_marked"),
        pytest.param(
            [None, TRANSIENT, None, None],
            None,
            0,
            1,
            ["StoreError"],
            [2, 3, 4],
            [],
            [0],
            id="a_transient_failure_marks_the_published_prefix_and_waits",
        ),
        pytest.param(
            [TRANSIENT, None, None, None],
            None,
            0,
            3,
            ["StoreError"] * 3,
            [1, 2, 3, 4],
            [],
            [0, 1, 2],
            id="the_same_row_failing_again_waits_a_longer_step",
        ),
        pytest.param(
            [None, TERMINAL, None, None],
            None,
            0,
            2,
            ["CodecError", "CodecError"],
            [2, 3, 4],
            [],
            [0, 1],
            id="no_sink_a_poison_row_stays_and_its_wait_grows",
        ),
        pytest.param(
            [None, TERMINAL, None, None],
            "takes",
            0,
            1,
            [4],
            [],
            [2],
            [],
            id="a_sink_takes_a_poison_row_and_the_rows_behind_it_go_out",
        ),
        pytest.param(
            [TERMINAL, None, None, None],
            "raises",
            0,
            2,
            ["CodecError", "CodecError"],
            [1, 2, 3, 4],
            [1, 1],
            [0, 1],
            id="a_raising_sink_hands_the_row_back_for_the_next_drain",
        ),
        pytest.param(
            [TRANSIENT, None, None, None],
            "takes",
            2,
            2,
            ["StoreError", 4],
            [],
            [1],
            [0],
            id="max_attempts_dead_letters_a_row_that_keeps_failing",
        ),
        pytest.param(
            [CONTENDED, None, None, None],
            "takes",
            1,
            2,
            ["InProgress", "InProgress"],
            [1, 2, 3, 4],
            [],
            [0, 1],
            id="contention_is_handed_back_without_spending_an_attempt",
        ),
    ],
)
async def test_relay_drain_branches(
    rows: list[Exception | None],
    sink: str | None,
    max_attempts: int,
    drains: int,
    want_results: list[Any],
    want_left: list[int],
    want_dead: list[int],
    want_steps: list[int],
) -> None:
    store = await _outbox_of(4)
    pacing, waits = recording_pacing()
    dead: list[str] = []

    async def on_dead(row: edax.EventMessage, error: BaseException) -> None:
        dead.append(row.id)
        if sink == "raises":
            raise edax.StoreError("dead-letter table unavailable")

    async def publish(row: edax.EventMessage) -> None:
        failure = rows[(row.sequence or 1) - 1]
        if failure is not None:
            raise failure

    relay = edax.Relay(
        store,
        publish,
        on_dead=on_dead if sink else None,
        max_attempts=max_attempts,
        pacing=pacing,
    )
    results: list[Any] = []
    for _ in range(drains):
        try:
            results.append(await relay.drain())
        except Exception as e:  # noqa: BLE001 - the drain's raise is the subject
            results.append(type(e).__name__)
    assert results == want_results
    assert [e.id for e in await store.unpublished()] == [f"acct-r-{n}" for n in want_left]
    assert dead == [f"acct-r-{n}" for n in want_dead]
    ceiling = edax.Pacing(entropy=lambda: 2**64 - 1)
    assert waits == [ceiling.delay(step) for step in want_steps], "a handed-back row waits its growing step"
    if want_dead and sink == "takes":
        assert relay.last is not None and relay.last.dead_lettered == len(want_dead)


# =========================================================================
# idempotency_settle.json: the guard's failure settle table
# =========================================================================


def test_golden_idempotency_settle() -> None:
    for row in golden("idempotency_settle.json"):
        assert _edax.idempotency_settle(row["code"]) == row["cache"], row
        if row["code"] is None:
            failure: BaseException = ValueError("a host bug")
        else:
            failure = edax.errors.from_wire(f"{row['code']}: boom")
        assert edax.errors.code_of(failure) == row["cache"], row
    with pytest.raises(edax.CodecError, match="unknown error code"), edax.errors.mapped():
        _edax.idempotency_settle("timeout")


# =========================================================================
# saga.json: the saga step's write and the outbox command id
# =========================================================================


def test_golden_saga() -> None:
    for row in golden("saga.json"):
        if "admit" in row:
            admit, want = row["admit"], row["want"]
            env = build_message(admit["aggregate_id"], "OrderCreated", admit["sequence"], id=admit["id"])
            if "error" in want:
                with pytest.raises(edax.CodecError) as caught, edax.errors.mapped():
                    _edax.saga_admit(admit["completed"], admit["folded"], env)
                assert str(caught.value) == want["error"].removeprefix("codec: "), row
                continue
            got = _edax.saga_admit(admit["completed"], admit["folded"], env)
            assert got == want.get("folded"), row
            continue
        if "command_id" in row:
            assert (
                _edax.saga_command_id(row["instance_id"], row["version"], row["index"]) == row["command_id"]
            ), row
            continue
        if "saga_command" in row:
            step = row["saga_command"]
            cause = step["cause"]
            command = step["command"]
            payload = edax.Event(command["name"], command["version"], base64.b64decode(command["data"]))
            got = _edax.saga_command(
                step["saga"],
                step["instance_id"],
                step["version"],
                step["index"],
                payload,
                cause["id"],
                cause["correlation_id"],
                step["issued_at"],
            )
            assert message_as_obj(got) == row["want"], row
            continue
        loaded = row["loaded"]
        want = row["want"]
        if "error" in want:
            with pytest.raises(edax.CodecError) as caught, edax.errors.mapped():
                _edax.saga_write(row["completes"], row["has_commands"], loaded.get("version"))
            assert str(caught.value) == want["error"].removeprefix("codec: "), row
            continue
        write = _edax.saga_write(row["completes"], row["has_commands"], loaded.get("version"))
        got = (write.action, write.expected, write.version)
        assert got == (want["write"], want.get("expected", 0), want.get("version", 0)), row


# =========================================================================
# upcast.json through the codec: every unmarshal asks the kernel
# =========================================================================


class Probe(msgspec.Struct):
    n: int


def test_golden_upcast_through_the_codec() -> None:
    """Each plan row, replayed as an event at version `from` read by a codec at version `to`: a row whose
    `available` is null is a name with no upcaster, else an upcaster whose steps each add one to `n`. The
    codec decodes exactly when the kernel plans, refuses exactly when it refuses, and runs every planned
    step once."""
    for row in golden("upcast.json"):
        if row["from"] < -1000 or row["to"] > 1000:
            continue  # the i32 extremes are pinned on the raw call; a codec version that far is not a codec
        upcasters = None
        if row["available"] is not None:
            upcaster = edax.Upcaster()
            for step in sorted(set(row["available"])):
                upcaster.step(
                    step, lambda d: msgspec.json.encode(Probe(msgspec.json.decode(d, type=Probe).n + 1))
                )
            upcasters = {"Probe": upcaster}
        codec = json_codec({"Probe": Probe}, version=row["to"], upcasters=upcasters)
        event = edax.Event("Probe", row["from"], b'{"n":0}')
        if row["error"] is not None:
            with pytest.raises(edax.CodecError) as caught:
                codec.unmarshal(event)
            assert row["error"].removeprefix("codec: ") in str(caught.value), row
        else:
            assert codec.unmarshal(event) == Probe(len(row["plan"])), row


# =========================================================================
# event_codec.json: one wire shape for one event, whichever SDK wrote it
# =========================================================================


class Deposited(msgspec.Struct):
    account_id: str
    amount_cents: int
    currency: str


class Withdrawn(msgspec.Struct):
    account_id: str
    amount_cents: int
    memo: str | None


class Tagged(msgspec.Struct):
    tags: list[str]
    flags: list[bool]


class Frozen(msgspec.Struct):
    pass


EVENTS: dict[str, type] = {"Deposited": Deposited, "Withdrawn": Withdrawn, "Tagged": Tagged, "Frozen": Frozen}


@pytest.mark.parametrize("fmt", ["json", "msgpack"])
def test_golden_event_codec(fmt: str) -> None:
    """The Rust registry codec's bytes for each event: Python reads them back, and writes the same name
    and the same bytes for the same event, so a stream reads the same from either SDK."""
    codec = (json_codec if fmt == "json" else msgpack_codec)(EVENTS, version=1)
    for row in golden("event_codec.json"):
        wire = row["json"].encode() if fmt == "json" else bytes.fromhex(row["msgpack_hex"])
        event = codec.unmarshal(edax.Event(row["name"], row["version"], wire))
        assert type(event).__name__ == row["name"], row["case"]
        written = codec.marshal(event)
        assert (written.name, written.version, bytes(written.data)) == (row["name"], row["version"], wire), (
            row["case"]
        )

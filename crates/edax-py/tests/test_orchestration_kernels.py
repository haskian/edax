"""The orchestration decisions both SDKs make, replayed from the kernels' golden vectors through the Python
SDK's own seams: the retry and dead-letter policy (through the dispatcher), the in-process retry, the
relay's drain rule, the guard's settle table and its whole claim/run/settle scenario, the drafts a save
appends and the tenant of the commands it sends, the saga step, the upcast plan through the codec, and the
event codec's wire shape against the bytes the Rust registry codec writes."""

from __future__ import annotations

import asyncio
import base64
import logging
import sys
import types
from typing import Any

import edax
import msgspec
import pytest
from conftest import (
    CreateUser,
    build_command,
    build_message,
    golden,
    host_text,
    message_as_obj,
    message_from_obj,
    recording_pacing,
    wire_code,
)
from edax import _edax, _json
from edax._observer import wrap
from edax._settle import dead_letter
from edax._values import replace
from edax.backend.memory import MemoryEventStore, MemoryLedger
from edax.codec import json_codec, msgpack_codec
from edax.idempotency import Guard, decode_prior, guarded
from edax.outbox import _begin
from edax.testing import drafted

# =========================================================================
# retries.json: the consumer's retry and dead-letter policy
# =========================================================================


def _verdict(verdict: Any) -> dict[str, int | None]:
    """The verdict as the golden row writes it. A field its action does not use crosses as ``None``."""
    if verdict.action == "dead":
        assert verdict.step is None
        return {"dead": verdict.attempt}
    if verdict.action == "stop":
        assert (verdict.attempt, verdict.step) == (None, None)
        return {"stop": None}
    assert verdict.attempt is None
    return {"nack": verdict.step}


def test_golden_retries() -> None:
    for scenario in golden("retries.json"):
        policy = _edax.retries(scenario["max_attempts"], scenario["sink"])
        for op in scenario["ops"]:
            name = op["op"]
            where = f"{scenario['name']}: {op}"
            if name == "attempt":
                assert policy.attempt(op["id"], op["broker_attempt"], op["tenant"]) == op["want"], where
            elif name == "failed":
                verdict = policy.failed(op["id"], op["broker_attempt"], op["code"], op["tenant"])
                assert _verdict(verdict) == op["want"], where
            elif name == "succeeded":
                policy.succeeded(op["id"], op["tenant"])
            elif name == "sunk":
                sunk = policy.sunk(op["id"], op["attempt"], op["taken"], op["code"], op["tenant"])
                assert sunk == op["want"], where
            else:
                tracked = policy.tracked()
                assert {"attempts": tracked.attempts, "contended": tracked.contended} == op["want"], where


def _kind(code: str | None) -> str:
    """The failure kind the kernel reads off ``code``, read back through a fresh policy with a sink and no
    ceiling: a fatal failure stops, a terminal one dies at once, contention is tracked apart, a transient
    one is counted."""
    policy = _edax.retries(0, True)
    action = policy.failed("evt-1", 0, code).action
    if action == "stop":
        return "fatal"
    if action == "dead":
        return "terminal"
    return "contended" if policy.tracked().contended else "transient"


def test_golden_retry_failure() -> None:
    """Every code's failure kind, through the kernel's own answer, through the policy's entry point, and
    through the SDK's mapping from an exception to the code it carries: an edax error by its class, a host
    exception by its verdict. ``is_fatal`` agrees with the kind on every row."""
    rows = golden("retry_failure.json")
    assert {row["code"] for row in rows} == {None, *_edax.error_codes()}, "every code has a row"
    for row in rows:
        assert _edax.retry_failure(row["code"]) == row["failure"], row
        assert _kind(row["code"]) == row["failure"], row
        failure = (
            ValueError("a host bug") if row["code"] is None else edax.errors.from_wire(f"{row['code']}: boom")
        )
        assert _kind(edax.errors.code_of(failure)) == row["failure"], row
        assert edax.is_fatal(failure) == (row["failure"] == "fatal"), row


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

    async def on_dead(letter: edax.DeadLetter[Any]) -> None:
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
FATAL = edax.BadHandle("topic authorization failed")


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
        pytest.param(
            # A wiring fault fails every row alike: the sink never sees it, the ceiling never counts it,
            # and the drain raises at once for `run` to end on.
            [None, FATAL, None, None],
            "takes",
            1,
            2,
            ["BadHandle", "BadHandle"],
            [2, 3, 4],
            [],
            [],
            id="a_fatal_failure_hands_the_row_back_uncounted_and_raises_without_a_wait",
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

    async def on_dead(letter: edax.DeadLetter[Any]) -> None:
        # The relay's letter is the row, the failure its publish raised, and the kernel's attempt: one for
        # a terminal failure, the ceiling for a transient one, and one more per earlier refused try. A row
        # has no transport record.
        row = letter.message
        failure = rows[(row.sequence or 1) - 1]
        assert failure is not None and letter.failure is failure and letter.origin is None
        first = 1 if edax.is_terminal(failure) else max_attempts
        assert letter.attempt == first + dead.count(row.id)
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
    assert [e.id for e in await store.unpublished(100)] == [f"acct-r-{n}" for n in want_left]
    assert dead == [f"acct-r-{n}" for n in want_dead]
    ceiling = edax.Pacing(entropy=lambda: 2**64 - 1)
    assert waits == [ceiling.delay(step) for step in want_steps], "a handed-back row waits its growing step"
    if want_dead and sink == "takes":
        assert relay.last is not None and relay.last.dead_lettered == len(want_dead)


def _drain_rows(op: dict[str, Any]) -> list[Any]:
    """The rows a ``begin`` op of ``relay_drain.json`` names, as native messages."""
    if op["kind"] == "event":
        return [
            build_message(r["aggregate_id"], "Deposited", 1, id=r["id"], tenant=r["tenant"])
            for r in op["rows"]
        ]
    return [
        _with_tenant(build_command("IssueInvoice", id=r["id"], aggregate_id=r["aggregate_id"]), r["tenant"])
        for r in op["rows"]
    ]


def _with_tenant(command: edax.CommandMessage, tenant: str | None) -> edax.CommandMessage:
    replaced: edax.CommandMessage = replace(command, tenant=tenant)
    return replaced


def test_golden_relay_drain() -> None:
    """The drain rule's scripts through the native handle the relay holds: every answer as the kernel
    gave it."""
    for scenario in golden("relay_drain.json"):
        drain = _edax.drain(scenario["max_attempts"], scenario["sink"])
        for op in scenario["ops"]:
            name, where = op["op"], f"{scenario['name']}: {op}"
            if name == "begin":
                _begin(drain, _drain_rows(op))
            elif name == "admits":
                assert drain.admits(op["id"]) is op["want"], where
            elif name == "published":
                drain.published(op["id"])
            elif name == "failed":
                answer = drain.failed(op["id"], op["code"])
                assert {answer.action: answer.attempt} == op["want"], where
            elif name == "sunk":
                refused = drain.sunk(op["id"], op["attempt"], op["taken"], op["code"])
                assert (None if refused is None else refused.action) == op["want"], where
            elif name == "finish":
                d = drain.finish()
                got = {
                    "settled": d.settled,
                    "published": d.published,
                    "dead_lettered": d.dead_lettered,
                    "wait": d.wait_step,
                    "raise": d.raise_id,
                }
                assert got == op["want"], where
            elif name == "store_failed":
                assert drain.store_failed() == op["want"], where
            elif name == "store_answered":
                drain.store_answered()
            else:
                tracked = drain.tracked()
                assert {"attempts": tracked.attempts, "contended": tracked.contended} == op["want"], where


@pytest.mark.parametrize(
    "fails, want_raised, want_sent, want_left",
    [
        pytest.param(
            {"acct-A": TERMINAL},
            "CodecError",
            ["acct-A-1", "acct-B-1", "acct-B-2"],
            ["acct-A-1", "acct-A-2"],
            id="a_poison_row_holds_back_its_aggregate_and_the_other_is_published",
        ),
        pytest.param(
            {"acct-A": TRANSIENT},
            "StoreError",
            ["acct-A-1", "acct-B-1", "acct-B-2"],
            ["acct-A-1", "acct-A-2"],
            id="one_transient_failure_holds_back_its_aggregate_only",
        ),
        pytest.param(
            {"acct-A": TRANSIENT, "acct-B": TRANSIENT},
            "StoreError",
            ["acct-A-1", "acct-B-1"],
            ["acct-A-1", "acct-B-1", "acct-A-2", "acct-B-2"],
            id="a_transient_failure_on_both_aggregates_stops_the_drain_at_the_second",
        ),
    ],
)
async def test_a_failing_key_holds_back_only_its_own_rows(
    fails: dict[str, Exception], want_raised: str, want_sent: list[str], want_left: list[str]
) -> None:
    """Order is promised per aggregate, so one aggregate's failing row no longer stalls the others, and two
    transient failures in a row read as an outage and stop the drain."""
    store = MemoryEventStore()
    for seq in (1, 2):
        for agg in ("acct-A", "acct-B"):
            await store.append(
                None, agg, seq - 1, [drafted(build_message(agg, "Deposited", seq, id=f"{agg}-{seq}"))]
            )
    sent: list[str] = []

    async def publish(row: edax.EventMessage) -> None:
        sent.append(row.id)
        if (failure := fails.get(row.aggregate_id)) is not None:
            raise failure

    relay = edax.Relay(store, publish, pacing=recording_pacing()[0])
    with pytest.raises(edax.EdaxError) as raised:
        await relay.drain()
    assert type(raised.value).__name__ == want_raised
    assert sent == want_sent
    assert [e.id for e in await store.unpublished(100)] == want_left


# pyo3 raises a Rust panic as `pyo3_runtime.PanicException`, a BaseException; one class per extension.
PanicException = type("PanicException", (BaseException,), {"__module__": "pyo3_runtime"})


@pytest.mark.parametrize(
    "sink_raises, want, want_tracked",
    [
        pytest.param(None, "taken", 0, id="a_sink_that_returns_takes_the_row_and_the_policy_forgets_it"),
        pytest.param(
            edax.StoreError("dead-letter table unavailable"),
            "nack",
            1,
            id="a_raising_sink_hands_the_row_back",
        ),
        pytest.param(
            PanicException("dead-letter producer panicked"),
            "nack",
            1,
            id="a_panicking_native_sink_hands_the_row_back_as_the_rust_sdk_does",
        ),
        pytest.param(
            edax.BadHandle("dead-letter topic authorization failed"),
            "stop",
            0,
            id="a_fatal_sink_failure_stops_and_keeps_no_attempt",
        ),
    ],
)
async def test_dead_letter_sink_step_branches(
    sink_raises: BaseException | None, want: str, want_tracked: int
) -> None:
    """The one sink step the relay and the dispatcher share, reported to the kernel as they report it. A
    sink failure, a native panic included, keeps the attempt and answers a backoff step, so the next try
    reaches the sink again; a fatal one answers no step, so the owner stops."""
    policy = _edax.retries(0, True)
    row = build_message("acct-r-1", "Deposited", 1, id="acct-r-1")
    verdict = policy.failed(row.id, 0, "codec")
    assert verdict.action == "dead" and verdict.attempt is not None
    dead = edax.DeadLetter(row, verdict.attempt, edax.CodecError("no schema for v3"))
    took: list[edax.DeadLetter[edax.EventMessage]] = []

    async def sink(letter: edax.DeadLetter[edax.EventMessage]) -> None:
        if sink_raises is not None:
            raise sink_raises
        took.append(letter)

    refusal, step = await dead_letter(logging.getLogger("edax.test"), sink, dead, row.id, policy.sunk)
    assert refusal is sink_raises
    got = "taken" if refusal is None else "nack" if step is not None else "stop"
    assert got == want
    assert took == ([dead] if refusal is None else [])
    assert policy.tracked().attempts == want_tracked


async def test_a_dead_letter_with_no_sink_is_a_native_module_out_of_step() -> None:
    """The kernel declares a message dead only to a policy built with a sink, so a dead verdict with none
    wired is a native module out of step with the SDK: the terminal CodecError, not a ``None`` called."""
    policy = _edax.retries(0, True)
    row = build_message("acct-r-1", "Deposited", 1, id="acct-r-1")
    dead = edax.DeadLetter(row, 1, edax.CodecError("no schema for v3"))
    with pytest.raises(edax.CodecError, match="dead-letter sink"):
        await dead_letter(logging.getLogger("edax.test"), None, dead, row.id, policy.sunk)


@pytest.mark.parametrize(
    "branch",
    [
        pytest.param("own_cancel", id="a_cancel_of_the_running_task_propagates_with_the_attempt_unspent"),
        pytest.param(
            "foreign_cancel",
            id="a_cancel_from_a_future_someone_else_cancelled_is_a_sink_failure",
            marks=pytest.mark.skipif(
                sys.version_info < (3, 11), reason="Task.cancelling, which tells the two apart, is 3.11+"
            ),
        ),
    ],
)
async def test_a_cancelled_sink(branch: str) -> None:
    """A cancel of the task running the sink is not a sink failure: it propagates, so the owner being stopped
    hands the row back. A ``CancelledError`` the sink re-raises from a future someone else cancelled is: it
    is refused like any failure and handed back after a backoff step. Propagated, it would end a dispatcher's
    worker and strand every key routed to it."""
    policy = _edax.retries(0, True)
    row = build_message("acct-r-1", "Deposited", 1, id="acct-r-1")
    dead = edax.DeadLetter(row, 1, edax.CodecError("no schema for v3"))
    started = asyncio.Event()

    async def sink(letter: edax.DeadLetter[edax.EventMessage]) -> None:
        started.set()
        if branch == "own_cancel":
            await asyncio.Event().wait()
        cancelled: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        cancelled.cancel()
        await cancelled

    task = asyncio.create_task(dead_letter(logging.getLogger("edax.test"), sink, dead, row.id, policy.sunk))
    await started.wait()
    if branch == "own_cancel":
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert policy.tracked().attempts == 0, branch
        return
    refusal, step = await task
    assert isinstance(refusal, asyncio.CancelledError) and step is not None, branch
    assert policy.tracked().attempts == 1, branch


# =========================================================================
# idempotency_settle.json and idempotency_guard.json: the guard's settle table and its scenarios
# =========================================================================


def test_golden_idempotency_settle() -> None:
    """The failed record a won claim caches for each code, or a release, through the kernel's answer and
    through the SDK's mapping from an exception to the code it carries."""
    for row in golden("idempotency_settle.json"):
        if row["code"] is None:
            failure: BaseException = ValueError(row["message"])
        else:
            failure = edax.errors.from_wire(f"{row['code']}: {row['message']}")
        cached = _edax.idempotency_settle_failure(
            row["payload_hash"], str(failure), edax.errors.code_of(failure), row["reason"]
        )
        got = (
            "release"
            if cached is None
            else {"cache": edax.idempotency.encode_record(decode_prior_of(cached))}
        )
        assert got == row["want"], row
    with pytest.raises(edax.CodecError, match="unknown error code"), edax.errors.mapped():
        _edax.idempotency_settle_failure(1, "boom", "timeout")


def decode_prior_of(record: _edax.IdempotencyRecord) -> edax.idempotency.Record:
    """A kernel-decoded record as the SDK's :class:`~edax.idempotency.Record`."""
    return edax.idempotency._record_of(record)


async def test_golden_idempotency_guard() -> None:
    """Each scenario through this SDK's guard over its memory ledger, as the Rust SDK replays it: what one
    dispatch returns or raises, whether the handler ran, and what the key holds after it."""
    for row in golden("idempotency_guard.json"):
        name, key, payload_hash, run = row["name"], row["key"], row["payload_hash"], row["run"]
        ledger = MemoryLedger()
        if row["prior"] is not None:
            ledger.put_raw(key, row["prior"])
        runs: list[None] = []

        async def handler(run: dict[str, Any] | None = run, runs: list[None] = runs) -> Any:
            runs.append(None)
            assert run is not None, "a loser never runs the handler"
            if "result" in run:
                return run["result"]
            failure = run["error"]
            if failure["code"] is None:
                raise RuntimeError(failure["message"])
            if failure["reason"] is not None:
                refused = types.new_class("Refused", (edax.Rejected,), {"reason": failure["reason"]})
                raise refused(failure["message"])
            raise edax.errors.from_wire(f"{failure['code']}: {failure['message']}")

        guard = Guard(ledger, "IssueInvoice", key, payload_hash, wrap(None))
        try:
            got: dict[str, Any] = {"result": await guarded(guard, handler, None)}
        except Exception as e:  # noqa: BLE001 - the raise is the subject
            got = {"error": f"{e.code}: {e}" if isinstance(e, edax.EdaxError) else str(e)}
            if row["prior"] is not None:
                assert edax.reason_of(e) == row["want_reason"], f"{name}: the replayed reason"
        assert got == row["want"], name
        assert len(runs) == (run is not None), f"{name}: handler runs"
        # What the key holds: a fresh claim wins once the claim was released, else reads the record.
        held = await ledger.claim(key, payload_hash)
        want = None if row["want_record"] is None else decode_prior(row["want_record"], payload_hash)
        assert (None if isinstance(held, edax.idempotency.Won) else held) == want, f"{name}: the record held"


# =========================================================================
# retry_step.json, draft_batch.json, command_owner.json
# =========================================================================


def test_golden_retry_step() -> None:
    for row in golden("retry_step.json"):
        if "error" in row:
            with pytest.raises(edax.BadHandle) as caught, edax.errors.mapped():
                _edax.retry_step(row["attempts"], row["failed"], row["code"])
            assert f"{caught.value.code}: {caught.value}" == row["error"], row["name"]
        else:
            assert _edax.retry_step(row["attempts"], row["failed"], row["code"]) == row["want"], row["name"]


@pytest.mark.parametrize(
    "row",
    [r for r in golden("retry_step.json") if "error" not in r and r["code"] is not None],
    ids=lambda r: r["name"],
)
async def test_retry_runs_as_the_golden_step_answers(row: dict[str, Any]) -> None:
    """The middleware performs the kernel's answer: a handler that has failed ``failed`` times fails again
    with ``code``, and the middleware either waits the golden step and runs it once more, or raises."""
    pacing, waits = recording_pacing()
    calls: list[None] = []

    async def step(msg: edax.CommandMessage, cmd: Any) -> str:
        calls.append(None)
        if len(calls) <= row["failed"] + 1:
            raise edax.errors.from_wire(f"{'store' if len(calls) <= row['failed'] else row['code']}: boom")
        return "done"

    retried = edax.retry(row["attempts"], pacing=pacing)(step)
    msg = edax.command_message(CreateUser("a@b.c"))
    if row["want"] is None:
        with pytest.raises(edax.EdaxError):
            await retried(msg, CreateUser("a@b.c"))
    else:
        assert await retried(msg, CreateUser("a@b.c")) == "done"
    ceiling = edax.Pacing(entropy=lambda: 2**64 - 1)
    want_steps = [*range(row["failed"]), *([] if row["want"] is None else [row["want"]])]
    assert waits == [ceiling.delay(step) for step in want_steps], row["name"]


def test_golden_draft_batch() -> None:
    for row in golden("draft_batch.json"):
        payloads = [
            edax.Payload(p["name"], p["version"], bytes.fromhex(p["data_hex"])) for p in row["payloads"]
        ]
        lineage = edax.Lineage(row["lineage"]["correlation_id"], row["lineage"]["causation_id"])
        args = (row["aggregate_id"], row["initial"], row["snapshot_every"], payloads, row["ids"], lineage)
        rest = (row["extensions"], row["metadata"], row["tenant"], row["occurred_at"])
        if "error" in row:
            with pytest.raises(edax.EdaxError) as caught, edax.errors.mapped():
                _edax.draft_batch(*args, *rest)
            assert f"{caught.value.code}: {caught.value}" == row["error"], row["name"]
            continue
        batch = _edax.draft_batch(*args, *rest)
        drafts = [
            {
                "id": d.id,
                "aggregate_id": d.aggregate_id,
                "tenant": d.tenant,
                "sequence": d.sequence,
                "name": d.name,
                "version": d.version,
                "data_hex": bytes(d.data).hex(),
                "correlation_id": d.correlation_id,
                "causation_id": d.causation_id,
                "occurred_at": d.occurred_at,
                "extensions": d.extensions,
                "metadata": d.metadata,
            }
            for d in batch.drafts
        ]
        got = {"drafts": drafts, "version": batch.version, "snapshot": batch.snapshot}
        assert got == row["want"], row["name"]


def test_golden_command_owner() -> None:
    for row in golden("command_owner.json"):
        command = _with_tenant(build_command("IssueInvoice", id=row["id"]), row["command_tenant"])
        if "error" in row:
            with pytest.raises(edax.CodecError) as caught, edax.errors.mapped():
                _edax.own_command(command, row["tenant"])
            assert f"{caught.value.code}: {caught.value}" == row["error"], row["name"]
        else:
            assert _edax.own_command(command, row["tenant"]).tenant == row["want"], row["name"]


# =========================================================================
# saga.json: the saga step's write and the outbox command id
# =========================================================================


def test_golden_saga() -> None:
    for row in golden("saga.json"):
        if "loaded_row" in row:
            # The flat row both saga calls read. The rows it loads are the `loaded` states the write rows
            # replay; the row no store writes is refused by both calls alike.
            loaded, want = row["loaded_row"], row["want"]
            if "error" in want:
                completed, version = loaded["completed"], loaded["version"]
                env = build_message("order-7", "OrderCreated", 1, id="evt-1")
                with pytest.raises(edax.CodecError) as written, edax.errors.mapped():
                    _edax.saga_write(False, True, completed, version)
                with pytest.raises(edax.CodecError) as admitted, edax.errors.mapped():
                    _edax.saga_admit("shipping", completed, "", env, version)
                for caught in (written, admitted):
                    assert str(caught.value) == want["error"].removeprefix("codec: "), row
            continue
        if "admit" in row:
            admit, want = row["admit"], row["want"]
            # Only a failure row names its event and its cause: the kernel reads them of no other.
            name = admit.get("name", "OrderCreated")
            env = build_message(
                admit["aggregate_id"],
                name,
                admit["sequence"],
                id=admit["id"],
                causation_id=admit.get("causation_id"),
            )
            state = admit["loaded"]
            args = (admit["saga"], state["state"] == "completed", admit["folded"], env, state.get("version"))
            if "error" in want:
                with pytest.raises(edax.CodecError) as caught, edax.errors.mapped():
                    _edax.saga_admit(*args)
                assert str(caught.value) == want["error"].removeprefix("codec: "), row
                continue
            got = _edax.saga_admit(*args)
            assert (got.action, got.folded) == (want["action"], want.get("folded")), row
            continue
        if "saga_command_failed" in row:
            failure, want = row["saga_command_failed"], row["want"]
            command = message_from_obj(failure["command"])
            assert isinstance(command, edax.CommandMessage), row
            args = (command, failure["error"], failure["recorded_at"], failure["code"])
            if "error" in want:
                with pytest.raises(edax.CodecError) as caught, edax.errors.mapped():
                    _edax.saga_command_failed(*args)
                assert str(caught.value) == want["error"].removeprefix("codec: "), row
                continue
            assert message_as_obj(_edax.saga_command_failed(*args)) == want, row
            continue
        if "command_id" in row:
            assert (
                _edax.saga_command_id(row["saga"], row["instance_id"], row["version"], row["index"])
                == row["command_id"]
            ), row
            continue
        if "saga_command" in row:
            step = row["saga_command"]
            cause = step["cause"]
            command = step["command"]
            payload = edax.Payload(command["name"], command["version"], base64.b64decode(command["data"]))
            got = _edax.saga_command(
                step["saga"],
                step["instance_id"],
                step["version"],
                step["index"],
                payload,
                cause["id"],
                cause["correlation_id"],
                step["issued_at"],
                cause["tenant"],
                cause.get("subject"),
            )
            assert message_as_obj(got) == row["want"], row
            continue
        loaded = row["loaded"]
        want = row["want"]
        step = (row["completes"], row["has_commands"], loaded["state"] == "completed", loaded.get("version"))
        if "error" in want:
            with pytest.raises(edax.CodecError) as caught, edax.errors.mapped():
                _edax.saga_write(*step)
            assert str(caught.value) == want["error"].removeprefix("codec: "), row
            continue
        write = _edax.saga_write(*step)
        got = (write.action, write.expected, write.version)
        assert got == (want["action"], want.get("expected"), want.get("version")), row


# =========================================================================
# upcast.json through the codec: every unmarshal asks the kernel
# =========================================================================


class Probe(msgspec.Struct):
    n: int


def test_golden_upcast_through_the_codec() -> None:
    """Each row's `available` as a codec: null is a name with no upcaster, else an upcaster whose steps
    each add one to `n`. The codec is refused exactly when `target` is, and writes `target`. A row whose
    `to` is that target is replayed as an event at version `from`: the codec decodes exactly when the
    kernel plans, refuses exactly when it refuses, and runs every planned step once."""
    for row in golden("upcast.json"):
        upcasters = None
        if row["available"] is not None:
            upcaster = edax.Upcaster()
            for step in sorted(set(row["available"])):
                upcaster.step(
                    step, lambda d: msgspec.json.encode(Probe(msgspec.json.decode(d, type=Probe).n + 1))
                )
            upcasters = {"Probe": upcaster}
        if row["target_error"] is not None:
            with pytest.raises(edax.CodecError) as refused:
                json_codec({"Probe": Probe}, upcasters=upcasters)
            assert row["target_error"].removeprefix("codec: ") in str(refused.value), row
            continue
        codec = json_codec({"Probe": Probe}, upcasters=upcasters)
        assert codec.marshal(Probe(0)).version == row["target"], row
        if row["to"] != row["target"]:
            continue  # this plan's `to` is no version this name's codec writes
        event = edax.Payload("Probe", row["from"], b'{"n":0}')
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


class Address(msgspec.Struct):
    city: str
    zip: str


class Relocated(msgspec.Struct):
    account_id: str
    address: Address


class Priced(msgspec.Struct):
    account_id: str
    amount: edax.Decimal
    at: edax.Instant


class Frozen(msgspec.Struct):
    pass


EVENTS: dict[str, type] = {
    "Deposited": Deposited,
    "Withdrawn": Withdrawn,
    "Tagged": Tagged,
    "Relocated": Relocated,
    "Priced": Priced,
    "Renamed": str,
    "Frozen": Frozen,
}


@pytest.mark.parametrize("fmt", ["json", "msgpack"])
def test_golden_event_codec(fmt: str) -> None:
    """The Rust registry codec's bytes for each event. Python builds the event from the row's fields with
    its own host types, writes the same name and the same bytes, and reads them back to the same event; and
    it refuses the foreign bytes Rust refuses. So a stream reads the same from either SDK."""
    codec = (json_codec if fmt == "json" else msgpack_codec)(EVENTS)
    for row in golden("event_codec.json"):
        wire = row["json"].encode() if fmt == "json" else bytes.fromhex(row["msgpack_hex"])
        stored = edax.Payload(row["name"], row["version"], wire)
        if row["error"] is not None:
            with pytest.raises(edax.CodecError) as refused:
                codec.unmarshal(stored)
            assert refused.value.code == wire_code(row["error"]), row["case"]
            continue
        # As the Rust suite reads the row: the markers' text, read into the host types by the codec's rule.
        built = msgspec.convert(host_text(row["fields"]), EVENTS[row["name"]], dec_hook=_json.decode_hook)
        written = codec.marshal(built)
        assert (written.name, written.version, bytes(written.data)) == (row["name"], row["version"], wire), (
            row["case"]
        )
        assert codec.unmarshal(stored) == built, row["case"]

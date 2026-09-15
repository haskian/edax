"""Dead-letter routing in :class:`~edax.dispatcher.KeyOrderedDispatcher`: bounded redelivery, the
terminal short-circuit, and the ``on_dead`` hook.

Drives an in-memory broker into the dispatcher pool one delivery at a time, waiting for the worker to
settle each (ack or nack, hence redelivery) before the next poll, so the sequence is deterministic.
Nacks are counted via a wrapper on ``Delivery.on_nack``; a second :class:`InMemoryBroker` stands in as
the dead-letter queue.
"""

import asyncio
from dataclasses import dataclass
from typing import Any, Optional

import edax
import pytest
from conftest import build_envelope, delivered, sequence_of
from edax._async import maybe_await
from edax.backend.memory import InMemoryBroker


@pytest.fixture
def slept(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record the dispatcher's backoff waits instead of paying them. A nack sleeps one step of the
    kernel's schedule, so a test walking three redeliveries would otherwise spend 700 ms proving
    arithmetic."""
    waits: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        waits.append(seconds)

    monkeypatch.setattr(edax.dispatcher, "_sleep", fake_sleep)
    return waits


async def _drive(broker: InMemoryBroker, dispatcher: edax.KeyOrderedDispatcher, polls: int) -> int:
    """Poll ``polls`` times; wrap each delivery's ``on_nack`` to count it; submit and wait for the
    worker to settle before the next poll. Returns the nack count. (Tests use one worker, so the lone
    queue is ``_queues[_worker_index(...)]``.)"""
    nacks = 0

    def counting(nack: edax.dispatcher.AckFn) -> edax.dispatcher.AckFn:
        async def wrapped() -> None:
            nonlocal nacks
            nacks += 1
            await maybe_await(nack())  # AckFn may be sync or async

        return wrapped

    for _ in range(polls):
        delivery = delivered(await broker.poll())
        delivery.on_nack = counting(delivery.on_nack)
        idx = dispatcher._worker_index(delivery.envelope.aggregate_id)
        await dispatcher.submit(delivery)
        await dispatcher._queues[idx].join()  # settled (acked/nacked) before we poll again
    return nacks


async def test_transient_dead_letters_after_max_attempts(slept: list[float]) -> None:
    broker, dlq = InMemoryBroker(), InMemoryBroker()
    await broker.publish(build_envelope("acct-1", "Boom", 1))

    async def handler(env: edax.EventEnvelope) -> None:
        raise ValueError("transient")

    async def on_dead(d: edax.Delivery, err: BaseException) -> None:
        await dlq.publish(d.envelope)

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler, max_attempts=3, on_dead=on_dead)
    dispatcher.start()
    nacks = await _drive(broker, dispatcher, 3)
    await dispatcher.stop()

    assert nacks == 2  # nacked on attempts 1 and 2, dead-lettered on 3
    # Each nack waits one step; the dead-letter on attempt 3 waits none.
    assert slept == [edax.backoff_millis(i) / 1000 for i in range(2)]
    assert broker.empty()  # acked (parked in the DLQ), not requeued
    dead = delivered(await dlq.poll())
    assert dead.envelope.aggregate_id == "acct-1"
    await dead.ack()  # settle it: an unacked poll is still outstanding, as on a real broker
    assert dlq.empty()


async def test_a_failing_ack_is_logged_as_a_settlement_problem_not_a_handler_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The handler succeeded, so "event handling failed" names the wrong layer and sends an operator
    to debug handlers instead of commits — a rebalance or a broker outage produces one such line per
    delivery. The swallow itself is deliberate (the broker redelivers and an idempotent handler
    absorbs it), and `on_dead` must not see it: the DLQ would hold a successfully handled event.
    """
    dead: list[str] = []

    async def handler(env: edax.EventEnvelope) -> None:
        return None

    async def failing_ack() -> None:
        raise edax.StoreError("the partition was revoked")

    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=handler, on_dead=lambda d, e: dead.append(d.envelope.id)
    )
    dispatcher.start()
    env = build_envelope("acct-1", "Tick", 1)
    with caplog.at_level("WARNING", logger="edax.dispatcher"):
        await dispatcher.submit(edax.Delivery(env, failing_ack, lambda: None))
        await dispatcher._queues[dispatcher._worker_index("acct-1")].join()
    await dispatcher.stop()

    records = [r for r in caplog.records if r.name == "edax.dispatcher"]
    assert len(records) == 1, "one line, from the settlement site rather than the worker catch-all"
    assert "after a successful handler" in records[0].getMessage()
    assert dead == [], "a handled event must never reach the dead-letter queue"
    # The bookkeeping below the ack still ran, which the worker's catch-all would have skipped.
    assert env.id not in dispatcher._attempts and env.id not in dispatcher._contended


async def test_on_dead_is_readable_through_the_public_accessor() -> None:
    """`broker.py` must settle a poison message through the same sink a terminal handler failure
    takes, and an out-of-tree `Consumer` needs a supported way to reach it."""

    async def sink(delivery: edax.Delivery, err: BaseException) -> None:
        return None

    async def handler(env: edax.EventEnvelope) -> None:
        return None

    assert edax.KeyOrderedDispatcher(workers=1, handler=handler, on_dead=sink).on_dead is sink
    assert edax.KeyOrderedDispatcher(workers=1, handler=handler).on_dead is None


async def test_terminal_dead_letters_on_first_failure(slept: list[float]) -> None:
    broker, dlq = InMemoryBroker(), InMemoryBroker()
    await broker.publish(build_envelope("acct-1", "Boom", 1))

    async def handler(env: edax.EventEnvelope) -> None:
        raise edax.CodecError("terminal")  # CodecError.terminal is True

    async def on_dead(d: edax.Delivery, err: BaseException) -> None:
        await dlq.publish(d.envelope)

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler, max_attempts=3, on_dead=on_dead)
    dispatcher.start()
    nacks = await _drive(broker, dispatcher, 1)
    await dispatcher.stop()

    assert nacks == 0  # terminal short-circuits before max_attempts, no nack
    assert slept == [], "a dead-letter is not a redelivery, so nothing waits"
    assert broker.empty()
    assert not dlq.empty()


async def test_default_nacks_forever_and_never_dead_letters(slept: list[float]) -> None:
    broker, dlq = InMemoryBroker(), InMemoryBroker()
    await broker.publish(build_envelope("acct-1", "Boom", 1))

    async def handler(env: edax.EventEnvelope) -> None:
        raise ValueError("transient")

    async def on_dead(d: edax.Delivery, err: BaseException) -> None:  # never reached: max_attempts=0
        await dlq.publish(d.envelope)

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler, max_attempts=0, on_dead=on_dead)
    dispatcher.start()
    nacks = await _drive(broker, dispatcher, 3)
    await dispatcher.stop()

    assert nacks == 3  # nacked every time (behaviour unchanged)
    # The count is kept even with no ceiling, so the wait grows across redeliveries instead of
    # repeating a flat first step.
    assert slept == [edax.backoff_millis(i) / 1000 for i in range(3)]
    assert not broker.empty()  # still requeued, never acked
    assert dlq.empty()  # on_dead never fired


# =========================================================================
# Worker settlement branch table
# =========================================================================


@dataclass
class SettleCase:
    handler_error: Optional[BaseException]
    sync_handler: bool
    bare_envelope: bool  # submit the envelope itself, not a Delivery
    ack_raises: bool
    on_dead_raises: bool
    settled: list[str]  # settlement calls for the first message, in order
    dead: int  # on_dead invocations for the first message


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(SettleCase(None, True, False, False, False, ["ack"], 0), id="sync_handler_is_acked"),
        pytest.param(SettleCase(None, False, True, False, False, [], 0), id="bare_envelope_is_never_settled"),
        pytest.param(
            SettleCase(None, False, False, True, False, ["ack"], 0),
            id="ack_raising_is_logged_and_the_worker_continues",
        ),
        pytest.param(
            SettleCase(edax.CodecError("terminal"), False, False, False, True, [], 1),
            id="on_dead_raising_leaves_the_message_unsettled",
        ),
    ],
)
async def test_worker_settlement_branches(case: SettleCase) -> None:
    handled: list[str] = []
    settled: dict[str, list[str]] = {}
    dead: list[str] = []

    async def async_handler(env: edax.EventEnvelope) -> None:
        handled.append(env.id)
        if case.handler_error is not None and env.sequence == 1:  # only the first delivery fails
            raise case.handler_error

    def sync_handler(env: edax.EventEnvelope) -> None:
        handled.append(env.id)

    async def on_dead(d: edax.Delivery, err: BaseException) -> None:
        dead.append(d.envelope.id)
        if case.on_dead_raises:
            raise edax.StoreError("dead-letter sink unavailable")

    def delivery(env: edax.EventEnvelope, ack_raises: bool) -> edax.Delivery:
        calls = settled.setdefault(env.id, [])

        async def ack() -> None:
            calls.append("ack")
            if ack_raises:
                raise edax.StoreError("broker commit failed")

        async def nack() -> None:
            calls.append("nack")

        return edax.Delivery(env, ack, nack)

    dispatcher = edax.KeyOrderedDispatcher(
        workers=1,
        handler=sync_handler if case.sync_handler else async_handler,
        max_attempts=3,
        on_dead=on_dead,
    )
    dispatcher.start()
    first = build_envelope("acct-1", "Deposited", 1, data=b'{"amount": 10}')
    if case.bare_envelope:
        await dispatcher.submit(first)
    else:
        await dispatcher.submit(delivery(first, case.ack_raises))
    # A healthy second delivery on the same key proves the worker survived whatever the first did.
    second = build_envelope("acct-1", "Deposited", 2, data=b'{"amount": 5}')
    await dispatcher.submit(delivery(second, False))
    await dispatcher.stop()

    assert handled == [first.id, second.id]
    assert settled.get(first.id, []) == case.settled
    assert settled[second.id] == ["ack"]
    assert dead.count(first.id) == case.dead


# =========================================================================
# Lifecycle branch table
# =========================================================================


@dataclass
class LifecycleCase:
    workers: int
    start_before_submit: bool
    start_twice: bool
    stops: int


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(LifecycleCase(2, True, True, 1), id="a_second_start_raises"),
        pytest.param(LifecycleCase(1, True, False, 2), id="a_second_stop_is_a_no_op"),
        pytest.param(LifecycleCase(1, False, False, 1), id="events_submitted_before_start_are_drained"),
        pytest.param(LifecycleCase(0, True, False, 1), id="zero_workers_floors_to_one_worker"),
    ],
)
async def test_dispatcher_lifecycle_branches(case: LifecycleCase) -> None:
    handled: list[int] = []
    dispatcher = edax.KeyOrderedDispatcher(
        workers=case.workers, handler=lambda env: handled.append(sequence_of(env))
    )
    if case.start_before_submit:
        dispatcher.start()
    if case.start_twice:
        # A second worker on one queue would consume it concurrently and break per-key order.
        with pytest.raises(RuntimeError, match="already started"):
            dispatcher.start()
    for n in (1, 2, 3):
        await dispatcher.submit(build_envelope("acct-1", "Deposited", n))
    if not case.start_before_submit:
        dispatcher.start()
    for _ in range(case.stops):
        await asyncio.wait_for(dispatcher.stop(), 5)

    assert handled == [1, 2, 3]  # one aggregate: FIFO, whatever the worker count
    # A repeated stop must not leave a _STOP nobody takes: that would hang the next `join()`.
    assert [q.qsize() for q in dispatcher._queues] == [0] * len(dispatcher._queues)


async def test_different_aggregates_progress_on_different_workers() -> None:
    # Per-aggregate FIFO alone would also hold if every event landed on one worker. This pins the
    # other half of the promise: the kernel's `partition` fans distinct ids out, so one parked
    # aggregate does not block another.
    assert edax.partition("acct-1", 2) != edax.partition("acct-2", 2)
    parked, other_done = asyncio.Event(), asyncio.Event()

    async def handler(env: edax.EventEnvelope) -> None:
        if env.aggregate_id == "acct-1":
            await parked.wait()
        else:
            other_done.set()

    dispatcher = edax.KeyOrderedDispatcher(workers=2, handler=handler)
    dispatcher.start()
    await dispatcher.submit(build_envelope("acct-1", "Deposited", 1))  # parks its worker
    await dispatcher.submit(build_envelope("acct-2", "Deposited", 1))
    await asyncio.wait_for(other_done.wait(), 5)  # ran while acct-1 is still parked
    parked.set()
    await asyncio.wait_for(dispatcher.stop(), 5)


async def test_max_queue_applies_backpressure_to_submit() -> None:
    release = asyncio.Event()

    async def blocking_handler(env: edax.EventEnvelope) -> None:
        await release.wait()

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=blocking_handler, max_queue=1)
    dispatcher.start()
    await dispatcher.submit(build_envelope("acct-1", "Deposited", 1))  # taken by the worker, blocked
    await dispatcher.submit(build_envelope("acct-1", "Deposited", 2))  # fills the queue
    third = asyncio.ensure_future(dispatcher.submit(build_envelope("acct-1", "Deposited", 3)))
    await asyncio.sleep(0.01)
    assert not third.done()  # submit waits for room rather than growing the queue unbounded
    release.set()
    await asyncio.wait_for(third, 1)
    await dispatcher.stop()


@dataclass(frozen=True)
class QueueCase:
    """One ``max_queue`` wiring and the ``maxsize`` every worker queue must end up with."""

    kwargs: dict[str, Any]
    maxsize: int


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(QueueCase({}, 1024), id="default_is_bounded"),
        pytest.param(QueueCase({"max_queue": 0}, 0), id="zero_opts_back_in_to_unbounded"),
        pytest.param(QueueCase({"max_queue": 4}, 4), id="explicit_bound_is_used"),
    ],
)
async def test_max_queue_default_bounds_every_worker_queue(case: QueueCase) -> None:
    """A fast broker with a slow handler must not grow memory without limit, so the queue is
    bounded unless the caller asks for unbounded with an explicit ``0``."""

    async def handler(env: edax.EventEnvelope) -> None:  # pragma: no cover - never dispatched
        raise AssertionError("no delivery is submitted")

    dispatcher = edax.KeyOrderedDispatcher(workers=3, handler=handler, **case.kwargs)
    assert [q.maxsize for q in dispatcher._queues] == [case.maxsize] * 3

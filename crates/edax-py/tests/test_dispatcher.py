"""Dead-letter routing in :class:`~edax.dispatcher.KeyOrderedDispatcher`: bounded redelivery, the
terminal short-circuit, and the ``on_dead`` hook.

Drives an in-memory broker into the dispatcher pool one delivery at a time, waiting for the worker to
settle each (ack or nack, hence redelivery) before the next poll, so the sequence is deterministic.
Nacks are counted via a wrapper on ``Delivery.on_nack``; a second :class:`MemoryBroker` stands in as
the dead-letter queue.
"""

import asyncio
import sys
from dataclasses import dataclass, field
from typing import Any
from unittest import mock

import edax
import pytest
from conftest import Recording, assert_logged, build_envelope, delivered, recorded_backoff, sequence_of
from edax._async import maybe_await
from edax.backend.memory import MemoryBroker
from hypothesis import given, settings
from hypothesis import strategies as st


class NativePanic(BaseException):
    """What PyO3 raises for a panic: a ``BaseException``, which an ``except Exception`` misses."""


async def _drive(broker: MemoryBroker, dispatcher: edax.KeyOrderedDispatcher, polls: int) -> int:
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


async def test_transient_dead_letters_after_max_attempts(slept: list[int]) -> None:
    broker, dlq = MemoryBroker(), MemoryBroker()
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
    assert slept == [0, 1], "each nack waits its own step of the schedule"
    assert broker.empty()  # acked (parked in the DLQ), not requeued
    dead = delivered(await dlq.poll())
    assert dead.envelope.aggregate_id == "acct-1"
    await dead.ack()  # settle it: an unacked poll is still outstanding, as on a real broker
    assert dlq.empty()


async def test_on_dead_is_readable_through_the_public_accessor() -> None:
    """`broker.py` must settle a poison message through the same sink a terminal handler failure
    takes, and an out-of-tree `Consumer` needs a supported way to reach it."""

    async def sink(delivery: edax.Delivery, err: BaseException) -> None:
        return None

    async def handler(env: edax.EventEnvelope) -> None:
        return None

    assert edax.KeyOrderedDispatcher(workers=1, handler=handler, on_dead=sink).on_dead is sink
    assert edax.KeyOrderedDispatcher(workers=1, handler=handler).on_dead is None


async def test_terminal_dead_letters_on_first_failure(slept: list[int]) -> None:
    broker, dlq = MemoryBroker(), MemoryBroker()
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


async def test_default_nacks_forever_and_never_dead_letters(slept: list[int]) -> None:
    broker, dlq = MemoryBroker(), MemoryBroker()
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
    assert slept == [0, 1, 2], "each nack waits its own step of the schedule"
    assert not broker.empty()  # still requeued, never acked
    assert dlq.empty()  # on_dead never fired


# =========================================================================
# Worker settlement branch table
# =========================================================================


@dataclass
class SettleCase:
    handler_error: BaseException | None
    sync_handler: bool
    bare_envelope: bool  # submit the envelope itself, not a Delivery
    ack_raises: bool
    on_dead_raises: bool
    settled: list[str]  # settlement calls for the first message, in order
    dead: int  # on_dead invocations for the first message
    # The `edax.dispatcher` lines this branch must log, in order, each as a fragment of the message.
    # Which layer the message names is the point: an operator sent to debug handlers by a failed
    # commit, or to debug commits by a failed handler, reads the wrong code for as long as the outage
    # lasts.
    want_log: list[str] = field(default_factory=list)
    # A nacked message keeps its attempt count until it comes back, so its backoff grows.
    attempt_tracked: bool = False


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(SettleCase(None, True, False, False, False, ["ack"], 0), id="sync_handler_is_acked"),
        pytest.param(SettleCase(None, False, True, False, False, [], 0), id="bare_envelope_is_never_settled"),
        pytest.param(
            SettleCase(None, False, False, True, False, ["ack"], 0, want_log=["after a successful handler"]),
            id="ack_raising_is_logged_as_a_settlement_problem_and_the_worker_continues",
        ),
        pytest.param(
            SettleCase(
                edax.CodecError("terminal"),
                False,
                False,
                False,
                True,
                ["nack"],
                1,
                # The dead-letter line names the sink, and the next one says the sink failed and the
                # delivery goes back for another try: an operator debugs the dead-letter route.
                want_log=["dead-lettering", "dead-letter sink failed"],
                attempt_tracked=True,
            ),
            # 05#F2: left unsettled, a log-based broker would never bring it back while the consumer lives.
            id="on_dead_raising_nacks_the_message_for_another_try",
        ),
        pytest.param(
            SettleCase(
                asyncio.CancelledError(), False, False, False, False, ["nack"], 0, attempt_tracked=True
            ),
            # A handler awaiting a future someone else cancelled: the delivery failed, the worker did not.
            id="a_cancelled_error_from_the_handler_nacks_and_the_worker_survives",
            marks=pytest.mark.skipif(
                sys.version_info < (3, 11),
                reason="Task.cancelling() is 3.11+: on 3.10 every CancelledError counts as the worker's own",
            ),
        ),
        pytest.param(
            SettleCase(
                NativePanic("unsendable handle used from another thread"),
                False,
                False,
                False,
                False,
                ["nack"],
                0,
                attempt_tracked=True,
            ),
            # Never terminal: nothing says a BaseException repeats, so it is retried, not dead-lettered.
            id="a_base_exception_from_the_handler_nacks_and_the_worker_survives",
        ),
    ],
)
async def test_worker_settlement_branches(
    case: SettleCase, caplog: pytest.LogCaptureFixture, slept: list[int]
) -> None:
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
    with caplog.at_level("WARNING", logger="edax.dispatcher"):
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
    assert_logged(caplog, "edax.dispatcher", case.want_log)
    # Whatever the branch did, the per-envelope bookkeeping below it ran: a raising ack swallowed in
    # the worker's catch-all instead would leave both maps holding the id forever.
    # `second` succeeded, so whatever the policy still tracks is `first`'s.
    stats = dispatcher.stats()
    assert stats.attempts_tracked == int(case.attempt_tracked)
    assert stats.contended_tracked == 0


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


# =========================================================================
# Shutdown and worker health
# =========================================================================


class Settlements:
    """Deliveries whose acks and nacks are recorded per envelope id; a nack can be made to fail."""

    def __init__(self, nack_raises: bool = False) -> None:
        self.calls: dict[str, list[str]] = {}
        self.nack_raises = nack_raises

    def delivery(self, sequence: int) -> edax.Delivery:
        env = build_envelope("acct-1", "Deposited", sequence, data=b'{"amount": 10}')
        calls = self.calls.setdefault(env.id, [])

        async def ack() -> None:
            calls.append("ack")

        async def nack() -> None:
            calls.append("nack")
            if self.nack_raises:
                raise edax.StoreError("broker connection lost during shutdown")

        return edax.Delivery(env, ack, nack)


@dataclass
class StopCase:
    stop_with_deadline: bool  # stop(timeout=...) rather than a host's wait_for around stop()
    nack_raises: bool
    want_settlement_failures: int


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(StopCase(True, False, 0), id="a_deadline_cancels_the_worker_and_nacks_what_it_held"),
        pytest.param(StopCase(False, False, 0), id="a_cancelled_stop_still_reaps_the_workers"),
        pytest.param(StopCase(True, True, 2), id="a_nack_failing_on_shutdown_is_reported_not_raised"),
    ],
)
async def test_stop_past_its_deadline_leaves_no_worker_and_nothing_unsettled(case: StopCase) -> None:
    """Before the fix, a deadline on ``stop()`` orphaned the workers: ``stats().running`` read
    ``False`` while they ran on, and a restart put a second worker on each queue, so two handlers ran
    one aggregate at once. The held delivery and the queued one were never settled."""
    entered = asyncio.Event()
    concurrent = 0
    peak = 0

    async def handler(env: edax.EventEnvelope) -> None:
        nonlocal concurrent, peak
        concurrent += 1
        peak = max(peak, concurrent)
        try:
            entered.set()
            await asyncio.Event().wait()  # stuck: only a cancel gets the worker out
        finally:
            concurrent -= 1

    observer = mock.Mock(spec=edax.Observer)
    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler, observer=observer)
    settlements = Settlements(case.nack_raises)
    dispatcher.start()
    held, queued = settlements.delivery(1), settlements.delivery(2)
    await dispatcher.submit(held)
    await dispatcher.submit(queued)  # same key: waits behind the stuck one
    await entered.wait()
    workers = list(dispatcher._tasks)

    if case.stop_with_deadline:
        await dispatcher.stop(timeout=0.01)
    else:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(dispatcher.stop(), 0.01)

    assert all(task.done() for task in workers), "no worker outlives stop()"
    assert dispatcher.stats().running is False and dispatcher.stats().workers_alive == 0
    assert settlements.calls == {held.envelope.id: ["nack"], queued.envelope.id: ["nack"]}
    assert observer.settlement_failed.call_count == case.want_settlement_failures
    with pytest.raises(RuntimeError, match="dispatcher is stopped"):
        await dispatcher.submit(settlements.delivery(3))

    # A restart runs one worker per queue again, never two on one aggregate.
    dispatcher.start()
    entered.clear()
    await dispatcher.submit(settlements.delivery(4))
    await entered.wait()
    assert len(dispatcher._tasks) == 1 and peak == 1
    await dispatcher.stop(timeout=0.01)


async def test_a_dead_worker_is_reported_and_skipped_by_stop(caplog: pytest.LogCaptureFixture) -> None:
    """A worker that ends while the dispatcher runs strands every key routed to it. Before the fix,
    ``running`` still read ``True`` and ``stop()`` blocked on a sentinel the dead worker never took."""

    async def handler(env: edax.EventEnvelope) -> None:
        pass

    dispatcher = edax.KeyOrderedDispatcher(workers=2, handler=handler, max_queue=1)
    dispatcher.start()
    await asyncio.sleep(0)  # both workers park on their queues
    with caplog.at_level("ERROR", logger="edax.dispatcher"):
        dispatcher._tasks[0].cancel()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
    stats = dispatcher.stats()
    assert (stats.running, stats.workers_alive) == (False, 1)
    assert_logged(caplog, "edax.dispatcher", ["a worker ended while running"])
    await asyncio.wait_for(dispatcher.stop(), 1)
    assert dispatcher.stats().workers_alive == 0


async def test_a_worker_killed_by_a_settlement_reports_its_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An ack that raises a ``BaseException`` ends the worker with it; the log carries the cause."""

    async def ack() -> None:
        raise NativePanic("unsendable handle used from another thread")

    async def nack() -> None:  # pragma: no cover - the handler succeeds
        pass

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=lambda env: None)
    dispatcher.start()
    with caplog.at_level("ERROR", logger="edax.dispatcher"):
        await dispatcher.submit(edax.Delivery(build_envelope("acct-1", "Deposited", 1), ack, nack))
        await asyncio.wait_for(asyncio.gather(*dispatcher._tasks, return_exceptions=True), 1)
        await asyncio.sleep(0)
    records = [r for r in caplog.records if r.name == "edax.dispatcher"]
    assert len(records) == 1 and records[0].exc_info is not None
    assert isinstance(records[0].exc_info[1], NativePanic)
    assert dispatcher.stats().running is False
    await dispatcher.stop()


async def test_keyboard_interrupt_and_system_exit_are_never_swallowed() -> None:
    """The process is being stopped, not the delivery failing: neither is nacked as a failure."""
    for raised in (KeyboardInterrupt(), SystemExit(3)):

        async def handler(env: edax.EventEnvelope, raised: BaseException = raised) -> None:
            raise raised

        settlements = Settlements()
        dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler)
        delivery = settlements.delivery(1)
        with pytest.raises(type(raised)):
            await dispatcher._settle(delivery)
        assert settlements.calls[delivery.envelope.id] == [], f"{type(raised).__name__} settled nothing"


# =========================================================================
# Property: whatever the handlers do, stop() settles every delivery exactly once
# =========================================================================

#: What a handler does with one delivery: return, fail transiently, raise a native-panic-like
#: `BaseException`, raise `CancelledError` from a future someone else cancelled, or never return.
BEHAVIOURS = ("ok", "transient", "panic", "cancelled", "blocks")


async def _settle_everything(workers: int, plan: list[tuple[int, str]], deadline: float) -> None:
    calls: dict[str, list[str]] = {}
    running: dict[str, int] = {}
    peak = 0
    behaviour_of: dict[str, str] = {}

    async def handler(env: edax.EventEnvelope) -> None:
        nonlocal peak
        running[env.aggregate_id] = running.get(env.aggregate_id, 0) + 1
        peak = max(peak, running[env.aggregate_id])
        try:
            behaviour = behaviour_of[env.id]
            if behaviour == "transient":
                raise edax.StoreError("downstream reset")
            if behaviour == "panic":
                raise NativePanic("unsendable handle used from another thread")
            if behaviour == "cancelled":
                raise asyncio.CancelledError()
            if behaviour == "blocks":
                await asyncio.Event().wait()
        finally:
            running[env.aggregate_id] -= 1

    dispatcher = edax.KeyOrderedDispatcher(workers=workers, handler=handler)
    dispatcher.start()
    for n, (aggregate, behaviour) in enumerate(plan, start=1):
        env = build_envelope(f"acct-{aggregate}", "Deposited", n)
        behaviour_of[env.id] = behaviour
        settled = calls.setdefault(env.id, [])

        async def ack(settled: list[str] = settled) -> None:
            settled.append("ack")

        async def nack(settled: list[str] = settled) -> None:
            settled.append("nack")

        await dispatcher.submit(edax.Delivery(env, ack, nack))
    await dispatcher.stop(timeout=deadline)

    assert all(len(settled) == 1 for settled in calls.values()), calls
    # A handler that returned is acked, unless it was still queued behind a stuck one at the
    # deadline; every other behaviour is nacked, by the dispatcher or by `stop`.
    for env_id, settled in calls.items():
        if behaviour_of[env_id] != "ok":
            assert settled == ["nack"], (env_id, behaviour_of[env_id], settled)
    assert peak <= 1, "two handlers ran one aggregate at once"
    assert dispatcher.stats().workers_alive == 0


@settings(max_examples=150, deadline=None)
@given(
    workers=st.integers(1, 3),
    plan=st.lists(st.tuples(st.integers(0, 2), st.sampled_from(BEHAVIOURS)), min_size=1, max_size=12),
)
def test_stop_settles_every_submitted_delivery_exactly_once(
    workers: int, plan: list[tuple[int, str]]
) -> None:
    """A delivery a handler finished is acked or nacked by its outcome; one a stuck handler held,
    or one still queued behind it, is nacked by ``stop``. None is settled twice, none is left
    unsettled, and no aggregate ever runs on two handlers at once."""
    with recorded_backoff():
        asyncio.run(_settle_everything(workers, plan, deadline=0.005))


# =========================================================================
# liveness: a delivery the transport retired is neither run nor settled (05#F1)
# =========================================================================


@dataclass
class LiveCase:
    dies: str | None  # None, "before" the worker takes it, or "during" the handler
    error: BaseException | None
    with_on_dead: bool
    runs: int
    settled: list[str]
    outcome: tuple[str, int, str | None]
    sink_calls: int = 0


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(LiveCase(None, None, True, 1, ["ack"], ("ack", 1, None)), id="live_and_handled_acks"),
        pytest.param(
            LiveCase("before", None, True, 0, [], ("stale", 1, None)), id="dead_before_the_worker_is_skipped"
        ),
        pytest.param(
            LiveCase("during", None, True, 1, [], ("stale", 1, None)),
            id="dead_mid_handler_success_is_not_acked",
        ),
        pytest.param(
            LiveCase("during", edax.StoreError("db"), True, 1, [], ("stale", 1, "store")),
            id="dead_mid_handler_failure_is_not_nacked",
        ),
        # The redelivered copy decides: dead-lettering this one too would put the message in the DLQ twice.
        pytest.param(
            LiveCase("during", edax.CodecError("bad"), True, 1, [], ("stale", 1, "codec")),
            id="dead_mid_handler_terminal_is_not_dead_lettered",
        ),
    ],
)
async def test_liveness_branches(case: LiveCase, slept: list[int]) -> None:
    alive = case.dies != "before"
    runs = 0
    settled: list[str] = []
    sink_calls = 0

    async def handler(env: edax.EventEnvelope) -> None:
        nonlocal runs, alive
        runs += 1
        if case.dies == "during":
            alive = False  # another key's nack rewound this partition while the handler ran
        if case.error is not None:
            raise case.error

    async def on_dead(d: edax.Delivery, err: BaseException) -> None:
        nonlocal sink_calls
        sink_calls += 1

    observer = Recording()
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=handler, on_dead=on_dead if case.with_on_dead else None, observer=observer
    )
    dispatcher.start()
    env = build_envelope("acct-1", "Deposited", 1)
    delivery = edax.Delivery(
        env, lambda: settled.append("ack"), lambda: settled.append("nack"), live=lambda: alive
    )
    await dispatcher.submit(delivery)
    await dispatcher.stop()
    assert (runs, settled, sink_calls) == (case.runs, case.settled, case.sink_calls)
    assert [(d[1], d[2], d[4]) for d in observer.deliveries] == [case.outcome]
    assert slept == [], "a retired delivery waits no backoff"
    assert dispatcher.stats().attempts_tracked == 0


# =========================================================================
# poison settlement (05#F2, 05#F3) and cancellation at every await
# =========================================================================


@dataclass
class PoisonCase:
    on_dead: str | None  # None (no sink), "ok", or "raises"
    attempts: int  # poison deliveries of the one id, in a row
    settled: list[str]
    outcomes: list[str]
    slept: list[int]


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(PoisonCase("ok", 1, ["ack"], ["dead"], []), id="a_sink_takes_it_then_it_is_acked"),
        # 05#F3: before, the no-sink nack was immediate and the consumer spun on the broker.
        pytest.param(
            PoisonCase(None, 3, ["nack", "nack", "nack"], ["nack", "nack", "nack"], [0, 1, 2]),
            id="no_sink_nacks_after_a_growing_backoff",
        ),
        # 05#F2: before, a raising sink left it unsettled for a redelivery Kafka never makes.
        pytest.param(
            PoisonCase("raises", 2, ["nack", "nack"], ["sink_failed", "sink_failed"], [0, 1]),
            id="a_raising_sink_nacks_after_a_growing_backoff",
        ),
    ],
)
async def test_settle_poison_branches(case: PoisonCase, slept: list[int]) -> None:
    settled: list[str] = []
    reported: list[str] = []

    async def on_dead(d: edax.Delivery, err: BaseException) -> None:
        if case.on_dead == "raises":
            raise ConnectionError("DLQ topic unavailable")

    class Seen(edax.Observer):
        def undecodable(self, message_id: str, error: BaseException) -> None:
            reported.append(message_id)

    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=lambda env: None, on_dead=on_dead if case.on_dead else None, observer=Seen()
    )
    outcomes = []
    for _ in range(case.attempts):
        poison = edax.broker.UndecodableDelivery(
            b"\xff",
            "t/0/7",
            lambda: settled.append("ack"),
            lambda: settled.append("nack"),
            edax.CodecError("x"),
        )
        outcomes.append(await dispatcher.settle_poison(poison, poison.error))
    assert (settled, outcomes, slept) == (case.settled, case.outcomes, case.slept)
    assert reported == ["t/0/7"] * case.attempts, "every poisoned delivery is reported once"


@pytest.mark.parametrize(
    "where, want",
    [
        pytest.param("backoff", ["nack"], id="in_the_backoff_wait"),
        pytest.param("sink", ["nack"], id="in_the_sink"),
        # The ack already settled it: there is nothing left to hand back.
        pytest.param("ack", ["ack"], id="in_its_ack"),
        # A bare envelope settles nothing, so a cancel mid-handler nacks nothing either.
        pytest.param("fire_and_forget", [], id="in_a_fire_and_forget_handler"),
    ],
)
async def test_a_worker_cancelled_while_settling_hands_the_delivery_back(where: str, want: list[str]) -> None:
    """Stopping the dispatcher while a worker waits out a nack's backoff, or while the sink runs, must not
    strand the delivery: it is nacked at once, then the cancel goes on. A delivery that settled, or a bare
    envelope, is not nacked."""
    parked = asyncio.Event()
    settled: list[str] = []

    async def park(*_: object) -> None:
        parked.set()
        await asyncio.Event().wait()

    async def handler(env: edax.EventEnvelope) -> None:
        if where == "fire_and_forget":
            await park()
        if where in ("sink", "backoff"):
            raise edax.CodecError("bad") if where == "sink" else edax.StoreError("db")

    async def ack() -> None:
        settled.append("ack")
        await park()

    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=handler, on_dead=park, pacing=edax.Pacing(sleep=park)
    )
    dispatcher.start()
    env = build_envelope("acct-1", "Deposited", 1)
    await dispatcher.submit(
        env if where == "fire_and_forget" else edax.Delivery(env, ack, lambda: settled.append("nack"))
    )
    await asyncio.wait_for(parked.wait(), 1)
    await dispatcher.stop(timeout=0.01)
    assert settled == want


@pytest.mark.parametrize(
    "where, want",
    [
        pytest.param("backoff", ["nack"], id="in_the_backoff_wait"),
        pytest.param("sink", ["nack"], id="in_the_sink"),
        # The sink took it and the ack already settled it: nothing is left to hand back.
        pytest.param("ack", ["ack"], id="in_its_ack"),
    ],
)
async def test_a_consumer_cancelled_while_settling_poison_hands_it_back(where: str, want: list[str]) -> None:
    """The consumer loop settles a poison message itself, not a worker: a cancel there hands the message
    back too, or the transport's watermark pins on it. One that already settled is not nacked."""
    parked = asyncio.Event()
    settled: list[str] = []

    async def park(*_: object) -> None:
        parked.set()
        await asyncio.Event().wait()

    async def sink_ok(*_: object) -> None:
        pass

    async def ack() -> None:
        settled.append("ack")
        await park()

    on_dead = {"backoff": None, "sink": park, "ack": sink_ok}[where]
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=lambda env: None, on_dead=on_dead, pacing=edax.Pacing(sleep=park)
    )
    poison = edax.broker.UndecodableDelivery(
        b"\xff", "t/0/7", ack, lambda: settled.append("nack"), edax.CodecError("x")
    )
    task = asyncio.create_task(dispatcher.settle_poison(poison, poison.error))
    await asyncio.wait_for(parked.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert settled == want


async def test_the_dispatcher_lends_its_pacing_to_the_consumer_loop() -> None:
    pacing = edax.Pacing()
    assert edax.KeyOrderedDispatcher(workers=1, handler=lambda env: None, pacing=pacing).pacing is pacing


@pytest.mark.parametrize("workers", [0, -1], ids=["zero", "negative"])
def test_a_dispatcher_refuses_fewer_than_one_worker(workers: int) -> None:
    """09#F12: a count below one used to become one worker silently, hiding a wiring typo."""
    with pytest.raises(ValueError, match=f"at least one worker, got {workers}"):
        edax.KeyOrderedDispatcher(workers=workers, handler=lambda env: None)

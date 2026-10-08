"""Dead-letter routing in :class:`~edax.dispatcher.KeyOrderedDispatcher`: bounded redelivery, the
terminal short-circuit, and the ``on_dead`` hook.

Drives an in-memory broker into the dispatcher pool one delivery at a time, waiting for the worker to
settle each (ack or nack, hence redelivery) before the next poll, so the sequence is deterministic.
Nacks are counted via a wrapper on ``Delivery.on_nack``; a second :class:`MemoryBroker` stands in as
the dead-letter queue.
"""

import asyncio
import contextlib
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from unittest import mock

import edax
import pytest
from conftest import (
    Recording,
    assert_logged,
    build_command,
    build_message,
    delivered,
    handling,
    recorded_backoff,
    sequence_of,
    sink_ok,
)
from edax._async import maybe_await
from edax.backend.memory import MemoryBroker, MemoryEventStore
from edax.testing import delivery as unsettled
from hypothesis import given, settings
from hypothesis import strategies as st


class NativePanic(BaseException):
    """What PyO3 raises for a panic: a ``BaseException``, which an ``except Exception`` misses."""


async def _drive(broker: MemoryBroker, dispatcher: edax.KeyOrderedDispatcher, polls: int) -> int:
    """Poll ``polls`` times; wrap each delivery's ``on_nack`` to count it; submit and wait for the
    worker to settle before the next poll. Returns the nack count. (Tests use one worker, so the lone
    queue is ``_queues[_worker_index(...)]``.)"""
    nacks = 0

    def counting(nack: edax.delivery.AckFn) -> edax.delivery.AckFn:
        async def wrapped() -> None:
            nonlocal nacks
            nacks += 1
            await maybe_await(nack())  # AckFn may be sync or async

        return wrapped

    for _ in range(polls):
        delivery = delivered(await broker.poll())
        delivery.on_nack = counting(delivery.on_nack)
        idx = dispatcher._worker_index(delivery.message)
        await dispatcher.submit(delivery)
        await dispatcher._queues[idx].join()  # settled (acked/nacked) before we poll again
    return nacks


async def test_transient_dead_letters_after_max_attempts(slept: list[int]) -> None:
    broker, dlq = MemoryBroker(), MemoryBroker()
    await broker.publish(build_message("acct-1", "Boom", 1))

    async def handler(env: edax.EventMessage) -> None:
        raise ValueError("transient")

    async def on_dead(letter: edax.DeadLetter[Any]) -> None:
        await dlq.publish(letter.message)

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler, max_attempts=3, on_dead=on_dead)
    dispatcher.start()
    nacks = await _drive(broker, dispatcher, 3)
    await dispatcher.stop()

    assert nacks == 2  # nacked on attempts 1 and 2, dead-lettered on 3
    # Each nack waits one step; the dead-letter on attempt 3 waits none.
    assert slept == [0, 1], "each nack waits its own step of the schedule"
    assert broker.empty()  # acked (parked in the DLQ), not requeued
    dead = delivered(await dlq.poll())
    assert dead.message.aggregate_id == "acct-1"
    await dead.ack()  # settle it: an unacked poll is still outstanding, as on a real broker
    assert dlq.empty()


async def test_on_dead_is_readable_through_the_public_accessor() -> None:
    """`broker.py` must settle a poison message through the same sink a terminal handler failure
    takes, and an out-of-tree `Consumer` needs a supported way to reach it."""

    async def sink(letter: edax.DeadLetter[Any]) -> None:
        return None

    async def handler(env: edax.EventMessage) -> None:
        return None

    assert edax.KeyOrderedDispatcher(workers=1, handler=handler, on_dead=sink).on_dead is sink
    assert edax.KeyOrderedDispatcher(workers=1, handler=handler).on_dead is None


async def test_terminal_dead_letters_on_first_failure(slept: list[int]) -> None:
    broker, dlq = MemoryBroker(), MemoryBroker()
    await broker.publish(build_message("acct-1", "Boom", 1))

    async def handler(env: edax.EventMessage) -> None:
        raise edax.CodecError("terminal")  # CodecError.terminal is True

    async def on_dead(letter: edax.DeadLetter[Any]) -> None:
        await dlq.publish(letter.message)

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler, max_attempts=3, on_dead=on_dead)
    dispatcher.start()
    nacks = await _drive(broker, dispatcher, 1)
    await dispatcher.stop()

    assert nacks == 0  # terminal short-circuits before max_attempts, no nack
    assert slept == [], "a dead-letter is not a redelivery, so nothing waits"
    assert broker.empty()
    assert not dlq.empty()


async def test_one_id_of_two_tenants_keeps_two_retry_budgets(slept: list[int]) -> None:
    """Bridge tenancy: two tenants' saga stores mint the same command id, and one shared dispatcher
    serves both. The second tenant's first failure is its own first attempt, not the first tenant's
    second, and the kernel counts each tenant's attempts until its own ceiling."""
    settled: list[str] = []
    dead: list[tuple[str | None, int]] = []

    async def handler(cmd: edax.CommandMessage) -> None:
        raise edax.StoreError("shipping API down")

    async def on_dead(letter: edax.DeadLetter[Any]) -> None:
        dead.append((letter.message.tenant, letter.attempt))

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler, max_attempts=2, on_dead=on_dead)
    dispatcher.start()
    for tenant in ("acme", "globex", "acme"):
        cmd = build_command("Ship", id="shipping-cmd-1", aggregate_id="order-7", tenant=tenant)
        await dispatcher.submit(
            edax.Delivery(
                cmd, lambda t=tenant: settled.append(f"{t}:ack"), lambda t=tenant: settled.append(f"{t}:nack")
            )
        )
        await dispatcher._queues[dispatcher._worker_index(cmd)].join()
    await dispatcher.stop()

    assert settled == ["acme:nack", "globex:nack", "acme:ack"]
    assert dead == [("acme", 2)]
    assert slept == [0, 0], "each tenant's first nack waits the first step"


@pytest.mark.parametrize(
    "handler_error, sink_error, want_dead, want_raised",
    [
        pytest.param(
            edax.BadHandle("topic authorization failed"),
            None,
            [],
            "topic authorization failed",
            id="a_fatal_handler_failure_is_never_dead_lettered",
        ),
        pytest.param(
            edax.CodecError("bad bytes"),
            edax.BadHandle("dead-letter topic authorization failed"),
            ["1"],
            "dead-letter topic authorization failed",
            id="a_fatal_sink_failure_stops_on_the_sinks_error",
        ),
    ],
)
async def test_a_fatal_failure_stops_the_dispatcher_and_ends_the_consumer_loop(
    slept: list[int],
    handler_error: Exception,
    sink_error: Exception | None,
    want_dead: list[str],
    want_raised: str,
) -> None:
    """A wiring fault (a refused credential) fails every message alike, whether the handler or the
    dead-letter sink meets it. The delivery is handed back uncounted and unwaited, the message queued behind
    it goes back unrun, and the consumer loop ends on the failure instead of draining the topic into the
    dead-letter queue, or spinning a dead message against a sink that can never take it. The messages stay
    on the broker for the consumer that runs once the wiring is fixed."""
    broker = MemoryBroker()
    for seq in (1, 2):
        await broker.publish(build_message("acct-1", "Deposited", seq))
    handled: list[int] = []
    dead: list[str] = []

    async def handler(env: edax.EventMessage) -> None:
        handled.append(sequence_of(env))
        raise handler_error

    async def on_dead(letter: edax.DeadLetter[Any]) -> None:
        dead.append(str(sequence_of(letter.message)))
        if sink_error is not None:
            raise sink_error

    observer = Recording()
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=handler, max_attempts=1, on_dead=on_dead, observer=observer
    )
    dispatcher.start()
    with pytest.raises(edax.BadHandle, match=want_raised):
        await asyncio.wait_for(broker.run(dispatcher), 5)
    await dispatcher.stop()

    assert handled == [1], "the first failure stopped it: nothing else ran"
    assert dead == want_dead
    assert slept == [], "no backoff step: the dispatcher is stopping"
    # The nack rewinds the partition, so a message polled behind the first is retired, never run.
    outcomes = [outcome for _, outcome, *_ in observer.deliveries]
    assert outcomes[0] == "stopped" and set(outcomes) <= {"stopped", "retired"}, outcomes
    with pytest.raises(edax.BadHandle, match=want_raised):
        await dispatcher.submit(unsettled(build_message("acct-2", "Deposited", 1)))
    assert not broker.empty(), "both messages wait for the fixed consumer"


async def test_default_nacks_forever_and_never_dead_letters(slept: list[int]) -> None:
    broker, dlq = MemoryBroker(), MemoryBroker()
    await broker.publish(build_message("acct-1", "Boom", 1))

    async def handler(env: edax.EventMessage) -> None:
        raise ValueError("transient")

    async def on_dead(letter: edax.DeadLetter[Any]) -> None:  # never reached: max_attempts=0
        await dlq.publish(letter.message)

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
    ack_raises: bool
    sink_error: BaseException | None  # what on_dead raises, or None when it takes the message
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
        # `Handler` is async, as a router's handlers are; an untyped host's sync one still settles.
        pytest.param(SettleCase(None, True, False, None, ["ack"], 0), id="sync_handler_is_acked"),
        pytest.param(
            SettleCase(
                None, False, True, None, ["ack"], 0, want_log=["failed; the broker will redeliver it"]
            ),
            id="ack_raising_is_logged_as_a_settlement_problem_and_the_worker_continues",
        ),
        pytest.param(
            SettleCase(
                edax.CodecError("terminal"),
                False,
                False,
                edax.StoreError("dead-letter sink unavailable"),
                ["nack"],
                1,
                # The dead-letter line names the sink, and the next one says the sink failed and the
                # delivery goes back for another try: an operator debugs the dead-letter route.
                want_log=["dead-lettering", "dead-letter sink failed"],
                attempt_tracked=True,
            ),
            # Left unsettled, a log-based broker would never bring it back while the consumer lives.
            id="on_dead_raising_nacks_the_message_for_another_try",
        ),
        pytest.param(
            SettleCase(
                edax.CodecError("terminal"),
                False,
                False,
                asyncio.CancelledError(),
                ["nack"],
                1,
                want_log=["dead-lettering", "dead-letter sink failed"],
                attempt_tracked=True,
            ),
            # A sink awaiting a future someone else cancelled refused the message, as a handler would have
            # failed it. Read as this worker stopping, it would end the worker and strand its keys.
            id="a_cancelled_error_from_on_dead_nacks_and_the_worker_survives",
            marks=pytest.mark.skipif(
                sys.version_info < (3, 11),
                reason="Task.cancelling() is 3.11+: on 3.10 every CancelledError counts as the worker's own",
            ),
        ),
        pytest.param(
            SettleCase(asyncio.CancelledError(), False, False, None, ["nack"], 0, attempt_tracked=True),
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
                None,
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

    async def async_handler(env: edax.EventMessage) -> None:
        handled.append(env.id)
        if case.handler_error is not None and env.sequence == 1:  # only the first delivery fails
            raise case.handler_error

    def sync_handler(env: edax.EventMessage) -> None:
        handled.append(env.id)

    async def on_dead(letter: edax.DeadLetter[Any]) -> None:
        dead.append(letter.message.id)
        if case.sink_error is not None:
            raise case.sink_error

    def delivery(env: edax.EventMessage, ack_raises: bool) -> edax.Delivery:
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
        handler=sync_handler if case.sync_handler else async_handler,  # ty: ignore[invalid-argument-type]
        max_attempts=3,
        on_dead=on_dead,
    )
    dispatcher.start()
    first = build_message("acct-1", "Deposited", 1, data=b'{"amount": 10}')
    with caplog.at_level("WARNING", logger="edax.dispatcher"):
        await dispatcher.submit(delivery(first, case.ack_raises))
        # A healthy second delivery on the same key proves the worker survived whatever the first did.
        second = build_message("acct-1", "Deposited", 2, data=b'{"amount": 5}')
        await dispatcher.submit(delivery(second, False))
        await dispatcher.stop()

    assert handled == [first.id, second.id]
    assert settled.get(first.id, []) == case.settled
    assert settled[second.id] == ["ack"]
    assert dead.count(first.id) == case.dead
    assert_logged(caplog, "edax.dispatcher", case.want_log)
    # Whatever the branch did, the per-message bookkeeping below it ran: a raising ack swallowed in
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
        workers=case.workers, handler=handling(lambda env: handled.append(sequence_of(env)))
    )
    if case.start_before_submit:
        dispatcher.start()
    if case.start_twice:
        # A second worker on one queue would consume it concurrently and break per-key order.
        with pytest.raises(RuntimeError, match="already started"):
            dispatcher.start()
    for n in (1, 2, 3):
        await dispatcher.submit(unsettled(build_message("acct-1", "Deposited", n)))
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

    async def handler(env: edax.EventMessage) -> None:
        if env.aggregate_id == "acct-1":
            await parked.wait()
        else:
            other_done.set()

    dispatcher = edax.KeyOrderedDispatcher(workers=2, handler=handler)
    dispatcher.start()
    await dispatcher.submit(unsettled(build_message("acct-1", "Deposited", 1)))  # parks its worker
    await dispatcher.submit(unsettled(build_message("acct-2", "Deposited", 1)))
    await asyncio.wait_for(other_done.wait(), 5)  # ran while acct-1 is still parked
    parked.set()
    await asyncio.wait_for(dispatcher.stop(), 5)


async def test_max_queue_applies_backpressure_to_submit() -> None:
    release = asyncio.Event()

    async def blocking_handler(env: edax.EventMessage) -> None:
        await release.wait()

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=blocking_handler, max_queue=1)
    dispatcher.start()
    await dispatcher.submit(
        unsettled(build_message("acct-1", "Deposited", 1))
    )  # taken by the worker, blocked
    await dispatcher.submit(unsettled(build_message("acct-1", "Deposited", 2)))  # fills the queue
    third = asyncio.ensure_future(dispatcher.submit(unsettled(build_message("acct-1", "Deposited", 3))))
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

    async def handler(env: edax.EventMessage) -> None:  # pragma: no cover - never dispatched
        raise AssertionError("no delivery is submitted")

    dispatcher = edax.KeyOrderedDispatcher(workers=3, handler=handler, **case.kwargs)
    assert [q.maxsize for q in dispatcher._queues] == [case.maxsize] * 3


# =========================================================================
# Shutdown and worker health
# =========================================================================


class Settlements:
    """Deliveries whose acks and nacks are recorded per message id; a nack can be made to fail."""

    def __init__(self, nack_raises: bool = False) -> None:
        self.calls: dict[str, list[str]] = {}
        self.nack_raises = nack_raises

    def delivery(self, sequence: int) -> edax.Delivery:
        env = build_message("acct-1", "Deposited", sequence, data=b'{"amount": 10}')
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
    #: (outcome, error_type) the observer hears for the held delivery, then the queued one.
    want_reported: list[tuple[str, str | None]]
    handler_fails: bool = False  # the held one's handler raises, and the worker waits out its backoff


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            StopCase(True, False, 0, [("stopped", None), ("stopped", None)]),
            id="a_deadline_cancels_the_worker_and_nacks_what_it_held",
        ),
        pytest.param(
            StopCase(False, False, 0, [("stopped", None), ("stopped", None)]),
            id="a_cancelled_stop_still_reaps_the_workers",
        ),
        pytest.param(
            StopCase(True, True, 2, [("unsettled", None), ("unsettled", None)]),
            id="a_nack_failing_on_shutdown_is_reported_not_raised",
        ),
        pytest.param(
            StopCase(True, False, 0, [("stopped", "store"), ("stopped", None)], handler_fails=True),
            id="a_delivery_stopped_in_its_backoff_wait_reports_stopped_with_its_cause",
        ),
    ],
)
async def test_stop_past_its_deadline_leaves_no_worker_and_nothing_unsettled(
    case: StopCase, caplog: pytest.LogCaptureFixture
) -> None:
    """A deadline on ``stop()`` must not orphan the workers. An orphan runs on while ``stats().running``
    reads ``False``, a restart puts a second worker on its queue, and two handlers run one aggregate at
    once. The held delivery and the queued one would never be settled."""
    entered = asyncio.Event()
    concurrent = 0
    peak = 0

    async def handler(env: edax.EventMessage) -> None:
        nonlocal concurrent, peak
        concurrent += 1
        peak = max(peak, concurrent)
        try:
            if case.handler_fails and env.sequence == 1:
                raise edax.StoreError("downstream reset")
            entered.set()
            await asyncio.Event().wait()  # stuck: only a cancel gets the worker out
        finally:
            concurrent -= 1

    async def backoff(seconds: float) -> None:
        entered.set()
        await asyncio.Event().wait()  # stuck in the backoff wait: only a cancel gets the worker out

    observer = mock.Mock(spec=edax.Observer)
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=handler, observer=observer, pacing=edax.Pacing(sleep=backoff)
    )
    settlements = Settlements(case.nack_raises)
    dispatcher.start()
    held, queued = settlements.delivery(1), settlements.delivery(2)
    await dispatcher.submit(held)
    await dispatcher.submit(queued)  # same key: waits behind the stuck one
    await entered.wait()
    workers = list(dispatcher._tasks)

    if case.stop_with_deadline:
        with caplog.at_level("WARNING", logger="edax.dispatcher"):
            await dispatcher.stop(timeout=0.01)
        # `wait` cancels nothing at the deadline, so the count is the worker still running.
        assert_logged(caplog, "edax.dispatcher", ["stop() passed its 0.01s deadline; cancelling 1 worker(s)"])
    else:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(dispatcher.stop(), 0.01)

    assert all(task.done() for task in workers), "no worker outlives stop()"
    assert dispatcher.stats().running is False and dispatcher.stats().workers_alive == 0
    assert settlements.calls == {held.message.id: ["nack"], queued.message.id: ["nack"]}
    assert observer.settlement_failed.call_count == case.want_settlement_failures
    # Every delivery `stop` hands back is reported once, held or queued, by the arm that settled it.
    reported = [(c.args[1], c.args[3], c.args[4], c.args[6]) for c in observer.delivery.call_args_list]
    assert reported == [
        (d.message.id, outcome, 1, cause)
        for d, (outcome, cause) in zip([held, queued], case.want_reported, strict=True)
    ]
    assert observer.delivery.call_args_list[1].args[5] == 0, "the queued one never ran"
    observer.delivery.reset_mock()
    with pytest.raises(edax.DispatcherStopped, match="dispatcher is stopped"):
        await dispatcher.submit(settlements.delivery(3))

    # A restart runs one worker per queue again, never two on one aggregate.
    dispatcher.start()
    entered.clear()
    await dispatcher.submit(settlements.delivery(4))
    await entered.wait()
    assert len(dispatcher._tasks) == 1 and peak == 1
    await dispatcher.stop(timeout=0.01)


@pytest.mark.parametrize("stop_with_deadline", [True, False], ids=["deadline", "cancelled_stop"])
@pytest.mark.parametrize("nack_yields", [False, True], ids=["sync_nack", "async_nack_that_yields"])
async def test_stop_settles_a_submit_blocked_on_a_full_queue(
    stop_with_deadline: bool, nack_yields: bool
) -> None:
    """One worker holds the first delivery, the second fills the queue, and a third submit waits for room.
    ``stop()`` frees that room as it hands the queue back, so the third lands after the drain began: it must
    be handed back too, or it sits in a queue no worker reads. A synchronous nack (Kafka's ``seek``) never
    yields, so nothing but ``stop()`` itself can wait for it."""
    entered = asyncio.Event()

    async def handler(env: edax.EventMessage) -> None:
        entered.set()
        await asyncio.Event().wait()  # stuck: only a cancel gets the worker out

    settled: dict[int, list[str]] = {}

    def delivery(sequence: int) -> edax.Delivery:
        calls = settled.setdefault(sequence, [])

        async def ack() -> None:  # pragma: no cover - every handler is stuck
            calls.append("ack")

        async def async_nack() -> None:
            await asyncio.sleep(0)
            calls.append("nack")

        def sync_nack() -> None:
            calls.append("nack")

        message = build_message("acct-1", "Deposited", sequence, data=b'{"amount": 10}')
        return edax.Delivery(message, ack, async_nack if nack_yields else sync_nack)

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler, max_queue=1)
    dispatcher.start()
    await dispatcher.submit(delivery(1))
    await entered.wait()
    await dispatcher.submit(delivery(2))
    blocked = asyncio.create_task(dispatcher.submit(delivery(3)))
    await asyncio.sleep(0)  # the third submit passes the stop check and waits on the full queue
    if stop_with_deadline:
        await dispatcher.stop(timeout=0.01)
    else:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(dispatcher.stop(), 0.01)
    assert blocked.done() and blocked.exception() is None, "the admitted submit landed"
    assert settled == {1: ["nack"], 2: ["nack"], 3: ["nack"]}
    assert dispatcher.stats().queue_depth == [0]


@pytest.mark.parametrize("nack_yields", [False, True], ids=["sync_nack", "async_nack_that_yields"])
async def test_stop_before_start_hands_back_every_submitted_delivery(nack_yields: bool) -> None:
    """A submit before ``start()`` waits in its queue, and a startup abandoned with ``stop()`` starts no
    worker to take it. ``stop()`` must hand back the queued delivery and the submits still waiting for room,
    or they stay unsettled until the consumer restarts."""
    settled: list[int] = []

    def delivery(sequence: int) -> edax.Delivery:
        async def async_nack() -> None:
            await asyncio.sleep(0)
            settled.append(sequence)

        message = build_message("acct-1", "Deposited", sequence, data=b'{"amount": 10}')
        return edax.Delivery(
            message, lambda: None, async_nack if nack_yields else lambda: settled.append(sequence)
        )

    observer = mock.Mock(spec=edax.Observer)
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=handling(lambda env: None), max_queue=1, observer=observer
    )
    await dispatcher.submit(delivery(1))
    blocked = [asyncio.create_task(dispatcher.submit(delivery(n))) for n in (2, 3)]
    await asyncio.sleep(0)  # both pass the stop check and wait on the full queue
    await asyncio.wait_for(dispatcher.stop(), 1)
    assert all(task.done() and task.exception() is None for task in blocked), "every admitted submit landed"
    assert settled == [1, 2, 3]
    assert [c.args[3] for c in observer.delivery.call_args_list] == ["stopped"] * 3
    assert dispatcher.stats().queue_depth == [0]


async def test_a_consumer_loop_hands_back_a_delivery_the_stopped_dispatcher_refuses() -> None:
    """``stop()`` under a running consumer loop: the next poll's delivery is refused by ``submit``. The loop
    nacks it and ends, rather than raising out with the delivery booked and never settled."""
    nacked: list[str] = []

    class OnePoll:
        async def poll(self) -> edax.broker.Polled:
            message = build_message("acct-1", "Deposited", 1, data=b'{"amount": 10}')
            return edax.Delivery(message, lambda: None, lambda: nacked.append(message.id))

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handling(lambda env: None))
    dispatcher.start()
    await dispatcher.stop()
    await asyncio.wait_for(edax.broker.consume(OnePoll(), dispatcher), 1)
    assert len(nacked) == 1


async def test_a_dead_worker_is_reported_and_skipped_by_stop(caplog: pytest.LogCaptureFixture) -> None:
    """A worker that ends while the dispatcher runs strands every key routed to it. ``running``
    must read ``False``, and ``stop()`` must not block on a sentinel the dead worker never takes."""

    async def handler(env: edax.EventMessage) -> None:
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

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handling(lambda env: None))
    dispatcher.start()
    with caplog.at_level("ERROR", logger="edax.dispatcher"):
        await dispatcher.submit(edax.Delivery(build_message("acct-1", "Deposited", 1), ack, nack))
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

        async def handler(env: edax.EventMessage, raised: BaseException = raised) -> None:
            raise raised

        settlements = Settlements()
        dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler)
        delivery = settlements.delivery(1)
        with pytest.raises(type(raised)):
            await dispatcher._settle(delivery)
        assert settlements.calls[delivery.message.id] == [], f"{type(raised).__name__} settled nothing"


# =========================================================================
# Property: whatever the handlers do, stop() settles every delivery exactly once
# =========================================================================

#: What a handler does with one delivery: return, fail transiently, raise a native-panic-like
#: `BaseException`, raise `CancelledError` from a future someone else cancelled, or never return.
BEHAVIOURS = ("ok", "transient", "panic", "cancelled", "blocks")


async def _settle_everything(
    workers: int, plan: list[tuple[int, str]], deadline: float, max_queue: int
) -> None:
    calls: dict[str, list[str]] = {}
    running: dict[str, int] = {}
    peak = 0
    behaviour_of: dict[str, str] = {}

    async def handler(env: edax.EventMessage) -> None:
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

    dispatcher = edax.KeyOrderedDispatcher(workers=workers, handler=handler, max_queue=max_queue)
    dispatcher.start()
    submits: list[asyncio.Task[None]] = []
    for n, (aggregate, behaviour) in enumerate(plan, start=1):
        env = build_message(f"acct-{aggregate}", "Deposited", n)
        behaviour_of[env.id] = behaviour
        settled = calls.setdefault(env.id, [])

        async def ack(settled: list[str] = settled) -> None:
            settled.append("ack")

        async def nack(settled: list[str] = settled) -> None:
            settled.append("nack")

        # Concurrent submits, in order: with a short queue some wait in `put` when `stop()` begins.
        submits.append(asyncio.create_task(dispatcher.submit(edax.Delivery(env, ack, nack))))
    await asyncio.sleep(0)  # every submit passes the stop check before `stop()` begins
    await dispatcher.stop(timeout=deadline)
    assert all(submit.done() for submit in submits), "stop() returned before an admitted submit landed"
    await asyncio.gather(*submits)

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
    max_queue=st.sampled_from([1, 1024]),
)
def test_stop_settles_every_submitted_delivery_exactly_once(
    workers: int, plan: list[tuple[int, str]], max_queue: int
) -> None:
    """A delivery a handler finished is acked or nacked by its outcome; one a stuck handler held,
    or one still queued behind it, is nacked by ``stop``. None is settled twice, none is left
    unsettled, and no aggregate ever runs on two handlers at once."""
    with recorded_backoff():
        asyncio.run(_settle_everything(workers, plan, deadline=0.005, max_queue=max_queue))


# =========================================================================
# liveness: a delivery the transport retired is neither run nor settled
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
            LiveCase("before", None, True, 0, [], ("retired", 1, None)),
            id="dead_before_the_worker_is_skipped",
        ),
        pytest.param(
            LiveCase("during", None, True, 1, [], ("retired", 1, None)),
            id="dead_mid_handler_success_is_not_acked",
        ),
        pytest.param(
            LiveCase("during", edax.StoreError("db"), True, 1, [], ("retired", 1, "store")),
            id="dead_mid_handler_failure_is_not_nacked",
        ),
        # The redelivered copy decides: dead-lettering this one too would put the message in the DLQ twice.
        pytest.param(
            LiveCase("during", edax.CodecError("bad"), True, 1, [], ("retired", 1, "codec")),
            id="dead_mid_handler_terminal_is_not_dead_lettered",
        ),
    ],
)
async def test_liveness_branches(case: LiveCase, slept: list[int]) -> None:
    alive = case.dies != "before"
    runs = 0
    settled: list[str] = []
    sink_calls = 0

    async def handler(env: edax.EventMessage) -> None:
        nonlocal runs, alive
        runs += 1
        if case.dies == "during":
            alive = False  # another key's nack rewound this partition while the handler ran
        if case.error is not None:
            raise case.error

    async def on_dead(letter: edax.DeadLetter[Any]) -> None:
        nonlocal sink_calls
        sink_calls += 1

    observer = Recording()
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=handler, on_dead=on_dead if case.with_on_dead else None, observer=observer
    )
    dispatcher.start()
    env = build_message("acct-1", "Deposited", 1)
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
# poison settlement and cancellation at every await
# =========================================================================


@dataclass
class PoisonCase:
    on_dead: str | None  # None (no sink), "ok", or "raises"
    attempts: int  # poison deliveries of the one id, in a row
    settled: list[str]
    outcomes: list[str]
    slept: list[int]
    ack_fails: bool = False
    #: Messages whose failed ack reached `Observer.settlement_failed`.
    settlement_failed: list[str] = field(default_factory=list)
    #: What the decoder raised.
    error: Exception = field(default_factory=lambda: edax.CodecError("unknown schema v3"))


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(PoisonCase("ok", 1, ["ack"], ["dead"], []), id="a_sink_takes_it_then_it_is_acked"),
        # An immediate no-sink nack would spin the consumer on the broker.
        pytest.param(
            PoisonCase(None, 3, ["nack", "nack", "nack"], ["nack", "nack", "nack"], [0, 1, 2]),
            id="no_sink_nacks_after_a_growing_backoff",
        ),
        # Left unsettled after a raising sink, it would wait for a redelivery Kafka never makes.
        pytest.param(
            PoisonCase("raises", 2, ["nack", "nack"], ["sink_failed", "sink_failed"], [0, 1]),
            id="a_raising_sink_nacks_after_a_growing_backoff",
        ),
        # The sink took it, then the commit failed: reported as a settlement failure, never raised out.
        pytest.param(
            PoisonCase("ok", 1, ["ack"], ["unsettled"], [], ack_fails=True, settlement_failed=["t/0/7"]),
            id="a_failed_ack_after_the_sink_is_reported_unsettled",
        ),
        # The kernel reads a poison as a codec failure whatever the decoder raised, so a decoder's
        # InProgress is a counted nack, labelled as one, never as contention.
        pytest.param(
            PoisonCase(
                None, 2, ["nack", "nack"], ["nack", "nack"], [0, 1], error=edax.InProgress("schema-k")
            ),
            id="a_poison_raised_as_contention_is_labelled_as_the_kernel_reads_it",
        ),
    ],
)
async def test_settle_poison_branches(case: PoisonCase, slept: list[int]) -> None:
    settled: list[str] = []
    reported: list[str] = []
    settlement_failed: list[str] = []
    letters: list[edax.DeadLetter[Any]] = []

    async def on_dead(letter: edax.DeadLetter[Any]) -> None:
        letters.append(letter)
        if case.on_dead == "raises":
            raise ConnectionError("DLQ topic unavailable")

    def ack() -> None:
        settled.append("ack")
        if case.ack_fails:
            raise ConnectionError("commit lost")

    class Seen(edax.Observer):
        def undecodable(self, message_id: str, error: BaseException) -> None:
            reported.append(message_id)

        def settlement_failed(self, name: str, event_id: str, error: BaseException) -> None:
            settlement_failed.append(event_id)

    dispatcher = edax.KeyOrderedDispatcher(
        workers=1,
        handler=handling(lambda env: None),
        on_dead=on_dead if case.on_dead else None,
        observer=Seen(),
    )
    outcomes = []
    for _ in range(case.attempts):
        poison = edax.Undecodable("t/0/7", b"\xff", case.error, ack, lambda: settled.append("nack"), "rec")
        outcomes.append(await dispatcher.settle_poison(poison))
    assert (settled, outcomes, slept) == (case.settled, case.outcomes, case.slept)
    assert settlement_failed == case.settlement_failed
    assert reported == ["t/0/7"] * case.attempts, "every poisoned delivery is reported once"
    # The sink gets the poison itself, never a stand-in message, with its record and the kernel's attempt:
    # a terminal failure dies on its first, and each redelivery after a failed sink counts one more.
    for attempt, letter in enumerate(letters, start=1):
        assert isinstance(letter.message, edax.Undecodable) and letter.message.payload == b"\xff"
        assert (letter.attempt, letter.failure, letter.origin) == (attempt, case.error, "rec")


@pytest.mark.parametrize(
    "branch, poisons, want_raised, want_sink_calls",
    [
        # The first poison meets the refusal in the sink; the second is never shown to the broken sink.
        pytest.param(
            "fatal_sink", 2, edax.BadHandle, 1, id="a_fatal_sink_refusal_stops_every_poison_after_it"
        ),
        pytest.param(
            "stopped", 1, edax.DispatcherStopped, 0, id="a_stopped_dispatcher_hands_the_poison_back"
        ),
    ],
)
async def test_settle_poison_refuses_once_the_dispatcher_stopped(
    slept: list[int],
    branch: str,
    poisons: int,
    want_raised: type[Exception],
    want_sink_calls: int,
) -> None:
    """A stopped dispatcher settles a poison as `submit` refuses a delivery: it hands it back, unwaited, and
    raises why it stopped, so the consumer loop ends instead of feeding the sink. Every poison is nacked by
    `settle_poison` itself, so the caller has nothing left to hand back."""
    settled: list[str] = []
    sink_calls: list[str] = []

    async def on_dead(letter: edax.DeadLetter[Any]) -> None:
        sink_calls.append(letter.message.id)
        raise edax.BadHandle("DLQ producer: SASL authentication failed")

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handling(lambda env: None), on_dead=on_dead)
    if branch == "stopped":
        dispatcher.start()
        await dispatcher.stop()
    for _ in range(poisons):
        poison = edax.Undecodable(
            "t/0/7",
            b"\xff",
            edax.CodecError("unknown schema v3"),
            lambda: settled.append("ack"),
            lambda: settled.append("nack"),
        )
        with pytest.raises(want_raised):
            await dispatcher.settle_poison(poison)
    assert settled == ["nack"] * poisons, branch
    assert len(sink_calls) == want_sink_calls, branch
    assert slept == [], f"{branch}: a stopping dispatcher waits no backoff step"


@pytest.mark.parametrize(
    "where, want",
    [
        pytest.param("backoff", ["nack"], id="in_the_backoff_wait"),
        pytest.param("sink", ["nack"], id="in_the_sink"),
        # The ack already settled it: there is nothing left to hand back.
        pytest.param("ack", ["ack"], id="in_its_ack"),
        pytest.param("handler", ["nack"], id="in_its_handler"),
    ],
)
async def test_a_worker_cancelled_while_settling_hands_the_delivery_back(where: str, want: list[str]) -> None:
    """Stopping the dispatcher while a worker runs the handler, waits out a nack's backoff, or runs the sink
    must not strand the delivery: it is nacked at once, then the cancel goes on. A delivery that settled is
    not nacked."""
    parked = asyncio.Event()
    settled: list[str] = []

    async def park(*_: object) -> None:
        parked.set()
        await asyncio.Event().wait()

    async def handler(env: edax.EventMessage) -> None:
        if where == "handler":
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
    env = build_message("acct-1", "Deposited", 1)
    await dispatcher.submit(edax.Delivery(env, ack, lambda: settled.append("nack")))
    await asyncio.wait_for(parked.wait(), 1)
    await dispatcher.stop(timeout=0.01)
    assert settled == want


@pytest.mark.parametrize("settle", ["ack", "nack"])
@pytest.mark.parametrize("reply_fails", [False, True], ids=["reply_arrives", "reply_fails"])
async def test_a_settlement_cancelled_before_its_effect_still_lands(
    settle: str, reply_fails: bool, slept: list[int], caplog: pytest.LogCaptureFixture
) -> None:
    """A transport's async ack or nack may await a round trip before its effect lands. A stop that cancels
    the worker there must not cut the call short: the delivery already reads settled, so nothing hands it
    back, and the broker would hold it unsettled until the consumer restarts."""
    broker = MemoryBroker()
    await broker.publish(build_message("acct-1", "Deposited", 1))
    delivery = delivered(await broker.poll())
    entered, answered = asyncio.Event(), asyncio.Event()
    effects: list[str] = []
    failure = edax.StoreError("settlement reply lost")

    def in_flight(name: str, effect: edax.delivery.AckFn) -> edax.delivery.AckFn:
        async def call() -> None:
            entered.set()
            await answered.wait()  # the round trip
            effects.append(name)
            await maybe_await(effect())
            if reply_fails:
                raise failure

        return call

    delivery.on_ack, delivery.on_nack = in_flight("ack", delivery.on_ack), in_flight("nack", delivery.on_nack)

    async def handler(env: edax.EventMessage) -> None:
        if settle == "nack":
            raise edax.StoreError("db")

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler)
    dispatcher.start()
    await dispatcher.submit(delivery)
    await asyncio.wait_for(entered.wait(), 1)
    await dispatcher.stop(timeout=0.01)
    answered.set()
    for _ in range(3):
        await asyncio.sleep(0)
    assert effects == [settle], "the call ran to its end, and nothing settled it twice"
    # The ack committed the message. The nack made it fetchable again.
    assert broker.empty() is (settle == "ack")
    errors = [record for record in caplog.records if record.name == "edax.delivery"]
    assert len(errors) == int(reply_fails), "a detached failure must be reported once"
    assert not any("Task exception was never retrieved" in record.message for record in caplog.records)
    if reply_fails:
        assert errors[0].message == "edax delivery: settlement failed after cancellation"
        assert errors[0].exc_info is not None and errors[0].exc_info[1] is failure


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

    async def ack() -> None:
        settled.append("ack")
        await park()

    on_dead = {"backoff": None, "sink": park, "ack": sink_ok}[where]
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=handling(lambda env: None), on_dead=on_dead, pacing=edax.Pacing(sleep=park)
    )
    poison = edax.Undecodable("t/0/7", b"\xff", edax.CodecError("x"), ack, lambda: settled.append("nack"))
    task = asyncio.create_task(dispatcher.settle_poison(poison))
    await asyncio.wait_for(parked.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert settled == want


@pytest.mark.parametrize(
    "branch, handler_s, timeout_s, want",
    [
        pytest.param("none_running", None, 0.0, 0, id="no_handler_running_answers_at_once"),
        pytest.param("ends_in_time", 0.01, 5.0, 0, id="a_handler_that_ends_wakes_the_wait"),
        pytest.param("outlives_it", 5.0, 0.01, 1, id="a_handler_past_the_deadline_is_counted"),
    ],
)
async def test_wait_handlers_branches(
    branch: str, handler_s: float | None, timeout_s: float, want: int
) -> None:
    """A rebalance waits for the running handlers before another member takes their keys. The wait is
    woken by the last handler that ends, so it returns well before its deadline, and at the deadline it
    says how many still run."""
    release = asyncio.Event()

    async def handler(env: edax.EventMessage) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(release.wait(), handler_s)

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler)
    dispatcher.start()
    if handler_s is not None:
        await dispatcher.submit(unsettled(build_message("acct-1", "Deposited", 1)))
        while not dispatcher.stats().handling:
            await asyncio.sleep(0)
    loop = asyncio.get_running_loop()
    started = loop.time()
    assert await dispatcher.wait_handlers(timeout_s) == want, branch
    assert loop.time() - started < 1, f"{branch}: waited out a deadline no handler held it to"
    release.set()
    await dispatcher.stop()


async def test_wait_handlers_outlasts_a_handler_that_starts_as_the_last_one_ends() -> None:
    """The last handler ends and wakes the wait; a handler of a kept partition starts on another worker
    before the wait resumes. The wait goes on until that one ends too: a count read at the wake would
    report a revoke overrun long before the deadline, though no revoked handler still runs."""
    release = asyncio.Event()
    later_ends = asyncio.Event()

    async def handler(env: edax.EventMessage) -> None:
        await (release if env.aggregate_id == "acct-1" else later_ends).wait()

    dispatcher = edax.KeyOrderedDispatcher(workers=2, handler=handler)
    dispatcher.start()
    other = next(
        f"acct-{n}" for n in range(2, 100) if edax.partition(f"acct-{n}", 2) != edax.partition("acct-1", 2)
    )
    await dispatcher.submit(unsettled(build_message("acct-1", "Deposited", 1)))
    while not dispatcher.stats().handling:
        await asyncio.sleep(0)
    waiter = asyncio.ensure_future(dispatcher.wait_handlers(5.0))
    await asyncio.sleep(0)  # the wait parks on the running handler
    release.set()  # that handler ends in the next tick and wakes the wait
    await dispatcher.submit(unsettled(build_message(other, "Deposited", 1)))  # its successor starts in it
    await asyncio.sleep(0.01)
    assert not waiter.done(), "a handler still runs: the wait is not over"
    later_ends.set()
    assert await asyncio.wait_for(waiter, 1) == 0
    await dispatcher.stop()


async def test_the_dispatcher_lends_its_pacing_to_the_consumer_loop() -> None:
    pacing = edax.Pacing()
    assert (
        edax.KeyOrderedDispatcher(workers=1, handler=handling(lambda env: None), pacing=pacing).pacing
        is pacing
    )


@pytest.mark.parametrize(
    "workers, want",
    [
        (0, "at least one worker, got 0"),
        (-1, "at least one worker, got -1"),
        (2.0, "workers must be an int, got 2.0"),
    ],
    ids=["zero", "negative", "not_an_int"],
)
def test_a_dispatcher_refuses_fewer_than_one_worker(workers: Any, want: str) -> None:
    """A count below one is a wiring typo; clamping it to one worker would hide it. The terminal BadHandle,
    as the Rust SDK's ``build`` refuses zero workers."""
    with pytest.raises(edax.BadHandle, match=want):
        edax.KeyOrderedDispatcher(workers=workers, handler=handling(lambda env: None))


@pytest.mark.parametrize(
    "max_attempts, want",
    [
        pytest.param(-1, edax.BadHandle, id="negative"),
        pytest.param(2**32, edax.BadHandle, id="past_u32"),
        pytest.param("3", edax.BadHandle, id="not_an_int"),
        pytest.param(True, edax.BadHandle, id="a_bool"),
        pytest.param(0, None, id="zero_is_no_ceiling"),
        pytest.param(2**32 - 1, None, id="the_u32_bound"),
    ],
)
def test_max_attempts_outside_u32_is_refused_at_wiring(
    max_attempts: Any, want: type[Exception] | None
) -> None:
    """Built lazily, an out-of-range policy failed the first delivery and left it neither acked nor
    nacked, pinning a log partition; the dispatcher and the relay refuse it when they are built, as the
    terminal wiring fault every other setting refusal is."""
    wirings: list[Callable[[], object]] = [
        lambda: edax.KeyOrderedDispatcher(
            workers=1, handler=handling(lambda env: None), max_attempts=max_attempts
        ),
        lambda: edax.Relay(MemoryEventStore(), lambda row: None, max_attempts=max_attempts),
    ]
    for wire in wirings:
        if want is None:
            wire()
        else:
            with pytest.raises(want, match="max_attempts"):
                wire()


@pytest.mark.parametrize(
    "attempt",
    [
        pytest.param(-1, id="negative"),
        pytest.param(2**32, id="past_u32"),
        pytest.param("1", id="not_an_int"),
    ],
)
async def test_a_delivery_attempt_the_kernel_cannot_carry_is_refused_at_submit(attempt: Any) -> None:
    """The broker's count crosses into the kernel as an unsigned 32-bit integer. A worker that took one
    outside it failed before any settlement, so the delivery was neither acked nor nacked and pinned its log
    partition. ``submit`` refuses it with the fatal BadHandle instead, which the consumer loop hands back
    and stops on, and nothing is queued."""
    ran: list[str] = []
    dispatcher = edax.KeyOrderedDispatcher(1, handling(lambda msg: ran.append(msg.id)))
    delivery = unsettled(build_message("acct-1", "Deposited", 1, id="evt-1"))
    delivery.attempt = attempt
    dispatcher.start()
    with pytest.raises(edax.BadHandle, match="attempt"):
        await dispatcher.submit(delivery)
    await dispatcher.stop()
    assert (ran, dispatcher.stats().queue_depth) == ([], [0])


async def test_a_handler_runs_under_the_lineage_of_its_message() -> None:
    """A command delivered from a broker is handled under its own child lineage, so the events its handler
    saves name it as their cause. The worker starts with no lineage of its starter's."""
    seen: list[edax.Lineage | None] = []
    dispatcher = edax.KeyOrderedDispatcher(1, handling(lambda msg: seen.append(edax.current_lineage())))
    command = build_command("Deposit", id="cmd-7", correlation_id="flow-7")
    with edax.use_lineage(edax.Lineage("starter-flow", "starter")):
        dispatcher.start()
    await dispatcher.submit(unsettled(command))
    await dispatcher.stop()
    assert seen == [edax.Lineage("flow-7", "cmd-7")]


async def test_submit_refuses_a_bare_message() -> None:
    """A bare message has nothing to settle it, so a handler failure would reach no ack, nack or sink: the
    wiring mistake is refused before it is queued."""
    handled: list[str] = []
    dispatcher = edax.KeyOrderedDispatcher(1, handling(lambda msg: handled.append(msg.id)))
    dispatcher.start()
    message: Any = build_message("acct-1", "Deposited", 1)
    with pytest.raises(edax.BadHandle, match="submit takes a Delivery, got EventMessage"):
        await dispatcher.submit(message)
    await dispatcher.stop()
    assert handled == [] and dispatcher.idle()

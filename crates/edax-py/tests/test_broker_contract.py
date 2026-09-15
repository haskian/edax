"""Broker seam contract (:class:`edax.broker.Producer` / ``Consumer``) over every in-tree adapter:
in-memory, Kafka (fake aiokafka client). Each test drives
``Relay(store, publish).drain()`` then ``consumer.poll()`` and asserts only what the contract
promises — per-key order, nack redelivers, ack settles. The fakes prove adapter glue, not broker
semantics (rebalance reordering, key-shared quirks stay out of tree). Kafka's watermark-only commit
is Kafka-specific and lives in ``test_kafka.py``.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Callable, Optional

import edax
import pytest
from conftest import FakeKafkaClient, agg_id, build_envelope, delivered, sequence_of
from edax.backend.kafka import KafkaConsumer, KafkaProducer
from edax.backend.memory import InMemoryBroker, MemoryEventStore
from edax.broker import UNDECODABLE, UndecodableDelivery, _ConsumerLoop
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st


@dataclass
class Rig:
    publish: Callable[[edax.EventEnvelope], Awaitable[None]]
    consumer: Any
    settled: Callable[[], bool]  # nothing left to (re)deliver


def _memory() -> Rig:
    b = InMemoryBroker()
    return Rig(b.publish, b, b.empty)  # ack is a no-op here: poll already took the message


def _kafka() -> Rig:
    c, s = FakeKafkaClient(), edax.kernel()
    return Rig(KafkaProducer(c, s, "t").publish, KafkaConsumer(c, s), c.settled)


# A factory, not an instance: hypothesis re-runs the test body per example on one fixture value,
# and its property below must stay a sync test (hypothesis cannot drive `async def`).
@pytest.fixture(params=[_memory, _kafka], ids=["memory", "kafka"])
def make_rig(request: Any) -> Callable[[], Rig]:
    return request.param


async def _append(store: MemoryEventStore, versions: dict[int, int], agg: int, n: int) -> list[str]:
    v = versions.get(agg, 0)
    envs = [build_envelope(agg_id(agg), "Tick", v + i + 1) for i in range(n)]
    async with edax.with_tx(store) as tx:
        await store.append(tx, agg_id(agg), v, envs)
    versions[agg] = v + n
    return [e.id for e in envs]


async def _poll(rig: Rig, n: int) -> list[edax.Delivery]:
    # timeout: a broken nack/publish leaves poll blocking forever; fail instead of hanging
    return [delivered(await asyncio.wait_for(rig.consumer.poll(), 1)) for _ in range(n)]


async def test_round_trip_preserves_per_key_order(make_rig: Callable[[], Rig]) -> None:
    rig, store, versions = make_rig(), MemoryEventStore(), {}
    want = {agg: await _append(store, versions, agg, 2) for agg in (0, 1)}
    assert await edax.Relay(store, rig.publish).drain() == 4
    seen: dict[str, list[str]] = defaultdict(list)
    for d in await _poll(rig, 4):
        seen[d.envelope.aggregate_id].append(d.envelope.id)
        await d.ack()
    assert seen == {agg_id(a): ids for a, ids in want.items()}


async def test_nack_redelivers(make_rig: Callable[[], Rig]) -> None:
    rig, store = make_rig(), MemoryEventStore()
    (id_,) = await _append(store, {}, 0, 1)
    await edax.Relay(store, rig.publish).drain()
    (first,) = await _poll(rig, 1)
    assert first.attempt == 0  # nothing has been redelivered yet
    await first.nack()
    (again,) = await _poll(rig, 1)
    assert again.envelope.id == id_ == first.envelope.id
    # attempt carries the broker's own redelivery count where it has one, else 0 and the
    # dispatcher counts in-process; a wrong value dead-letters early or never. Neither in-tree
    # adapter reads one from its client.
    assert again.attempt == 0
    await again.ack()
    assert rig.settled()


async def test_ack_out_of_order_settles_everything(make_rig: Callable[[], Rig]) -> None:
    rig, store, versions = make_rig(), MemoryEventStore(), {}
    for agg in (0, 1):
        await _append(store, versions, agg, 2)
    await edax.Relay(store, rig.publish).drain()
    deliveries = await _poll(rig, 4)
    for d in reversed(deliveries):  # the dispatcher acks in worker order, not fetch order
        await d.ack()
    assert rig.settled()


class _ScriptedConsumer(_ConsumerLoop):
    """A consumer whose ``poll`` replays a script: a :class:`edax.Delivery`, ``None`` (end of
    stream), or ``"block"`` (park until cancelled)."""

    def __init__(self, script: list[Any]) -> None:
        self._script = list(script)

    async def poll(self) -> Any:
        item = self._script.pop(0)
        if item == "block":
            await asyncio.Event().wait()
        return item


# A payload a schema change made unreadable: the poison message `poll` cannot turn into an event.
POISON_BYTES = b'{"schema":"v3","balance_cents":4200}'


@dataclass
class RunCase:
    script: list  # per poll: "d" (a delivery), "poison", None, or "block"
    cancel: bool
    handled: list  # envelope sequences the dispatcher handled, in order
    settled: list  # "ack" / "nack" per delivery, in settlement order
    returns: bool  # run() returned (end of stream) rather than raising CancelledError
    dead: list  # event names the dead-letter hook saw
    on_dead: bool = True  # wire the dispatcher's dead-letter hook
    sink_raises: bool = False  # the dead-letter sink is unreachable, the usual reason one fails
    max_queue: int = 0  # 0 = unbounded; a small queue makes `submit` block
    start: bool = True  # an unstarted dispatcher drains nothing, so its queue stays full


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(RunCase([None], False, [], [], True, []), id="end_of_stream_submits_nothing"),
        pytest.param(
            RunCase(["d", "d", None], False, [1, 2], ["ack", "ack"], True, []),
            id="submits_each_delivery_in_order_until_none",
        ),
        pytest.param(
            RunCase(["d", "block"], True, [1], ["ack"], False, []),
            id="cancelled_while_polling_stops_the_loop",
        ),
        # Queue of one that nobody drains: the second submit parks, so the cancel lands with the
        # delivery polled but unsettled — the case that used to drop it.
        pytest.param(
            RunCase(["d", "d"], True, [], ["nack"], False, [], max_queue=1, start=False),
            id="cancelled_between_poll_and_submit_nacks_the_delivery",
        ),
        pytest.param(
            RunCase(["poison", None], False, [], ["ack"], True, [UNDECODABLE]),
            id="undecodable_is_dead_lettered_then_acked",
        ),
        pytest.param(
            RunCase(["poison", None], False, [], ["nack"], True, [], on_dead=False),
            id="undecodable_without_a_hook_is_nacked_never_dropped",
        ),
        # A DLQ outage must not stop the consumer. The poison stays unsettled, exactly as the
        # dispatcher route leaves it, so the broker redelivers and the sink gets another try.
        pytest.param(
            RunCase(["poison", "d", None], False, [1], ["ack"], True, [UNDECODABLE], sink_raises=True),
            id="undecodable_with_a_raising_sink_is_left_unsettled_and_the_loop_continues",
        ),
    ],
)
async def test_consumer_loop_run_branches(case: RunCase) -> None:
    handled: list[int] = []
    settled: list[str] = []
    dead: list[str] = []
    envs = [build_envelope(agg_id(0), "Tick", n) for n in range(1, 3)]

    def scripted(step: Any) -> Any:
        if step == "poison":
            return UndecodableDelivery(
                POISON_BYTES,
                "msg-poison",
                lambda: settled.append("ack"),
                lambda: settled.append("nack"),
                edax.CodecError("unknown schema v3"),
            )
        if step != "d":
            return step
        env = envs.pop(0)
        return edax.Delivery(env, lambda: settled.append("ack"), lambda: settled.append("nack"))

    async def on_dead(delivery: edax.Delivery, err: BaseException) -> None:
        assert isinstance(err, edax.CodecError) and err.terminal
        assert bytes(delivery.envelope.event.data) == POISON_BYTES, "the raw payload must survive"
        # Recorded before the raise: `dead` is what the sink saw, so it doubles as the call count and
        # the raising row still proves the sink was reached exactly once.
        dead.append(delivery.envelope.event.name)
        if case.sink_raises:
            raise edax.StoreError("dead-letter sink unavailable")

    dispatcher = edax.KeyOrderedDispatcher(
        workers=2,
        handler=lambda env: handled.append(sequence_of(env)),
        max_queue=case.max_queue,
        on_dead=on_dead if case.on_dead else None,
    )
    if case.start:
        dispatcher.start()
    task = asyncio.create_task(_ScriptedConsumer([scripted(s) for s in case.script]).run(dispatcher))
    if case.cancel:
        await asyncio.sleep(0.01)  # let the loop park (in poll, or in a submit that cannot proceed)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        await asyncio.wait_for(task, 1)  # a run that never ends means a broken end-of-stream branch
    await dispatcher.stop()

    assert handled == case.handled
    assert settled == case.settled  # every polled delivery reached settlement
    assert dead == case.dead
    assert task.done() and (not task.cancelled()) is case.returns


step = st.tuples(st.integers(0, 2), st.integers(1, 3), st.integers(1, 4))  # (agg, n events, drain limit)


# the fixture is a factory, so a shared fixture value across examples is exactly what we want
@settings(max_examples=100, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.lists(step, min_size=1, max_size=10))
def test_drain_history_reaches_consumer_in_key_order(
    make_rig: Callable[[], Rig], history: list[tuple[int, int, int]]
) -> None:
    async def run() -> None:
        rig, store, versions = make_rig(), MemoryEventStore(), {}
        relay = edax.Relay(store, rig.publish)
        want: dict[int, list[str]] = defaultdict(list)
        published = 0
        for agg, n, limit in history:
            want[agg].extend(await _append(store, versions, agg, n))
            published += await relay.drain(limit)
        while await relay.drain():
            pass
        total = sum(versions.values())
        seen: dict[str, list[str]] = defaultdict(list)
        for d in await _poll(rig, total):
            seen[d.envelope.aggregate_id].append(d.envelope.id)
            await d.ack()
        assert seen == {agg_id(a): ids for a, ids in want.items()}
        assert rig.settled()

    asyncio.run(run())


async def test_in_memory_broker_reports_an_unsettled_delivery() -> None:
    """A polled-but-unsettled delivery keeps the broker non-empty, as Kafka reports it.

    Without this the in-memory rig flatters a dispatcher that drops a delivery — it would pass here
    and lose the message against a durable broker, which is the one bug the rig exists to catch.
    """
    broker = InMemoryBroker()
    await broker.publish(build_envelope(agg_id(0), "Deposited", 1))
    assert not broker.empty()

    delivery = delivered(await broker.poll())
    assert not broker.empty(), "polled but not settled: still outstanding"

    await delivery.ack()
    assert broker.empty()


async def test_the_same_id_in_flight_twice_needs_two_settlements() -> None:
    """The outbox republishes an envelope after a crash between `publish` and `mark_published`, so
    one id is legitimately in flight twice. Tracking ids in a set would collapse the two into one
    entry and report drained while a delivery is still outstanding."""
    broker = InMemoryBroker()
    env = build_envelope(agg_id(0), "Deposited", 1)
    await broker.publish(env)
    await broker.publish(env)  # at-least-once: the same envelope, republished

    first, second = delivered(await broker.poll()), delivered(await broker.poll())
    assert first.envelope.id == second.envelope.id, "the rig really is holding one id twice"

    await first.ack()
    assert not broker.empty(), "one copy settled, the other is still outstanding"
    await second.ack()
    assert broker.empty()


async def test_settling_a_delivery_twice_does_not_lose_an_outstanding_one() -> None:
    """A double ack from a buggy caller must not drive the count below what is really in flight,
    or the broker would report drained while another delivery is outstanding."""
    broker = InMemoryBroker()
    for seq in (1, 2):
        await broker.publish(build_envelope(agg_id(0), "Deposited", seq))
    first, second = delivered(await broker.poll()), delivered(await broker.poll())

    await first.ack()
    await first.ack()  # the bug
    assert not broker.empty(), "the second delivery is still unsettled"

    await second.ack()
    assert broker.empty()


async def test_in_memory_broker_nack_settles_and_requeues() -> None:
    broker = InMemoryBroker()
    await broker.publish(build_envelope(agg_id(0), "Deposited", 1))
    delivery = delivered(await broker.poll())
    await delivery.nack()
    # Settled as far as the poll is concerned, but back on the queue — so still not empty.
    assert not broker.empty()
    assert delivered(await broker.poll()).envelope.aggregate_id == agg_id(0)


@pytest.mark.parametrize("wake", ["publish", "nack"], ids=["a_publish_wakes_it", "a_nack_wakes_it"])
async def test_in_memory_broker_poll_parks_until_a_message_arrives(wake: str) -> None:
    """The waiting is the one part Python owns over the native machine: a poll that finds nothing
    must park, and both ways a message can appear (a publish, a nack's requeue) must wake it."""
    broker = InMemoryBroker()
    env = build_envelope(agg_id(0), "Deposited", 1)
    held: Optional[edax.Delivery] = None
    if wake == "nack":
        await broker.publish(env)
        held = delivered(await broker.poll())  # taken, so the queue is empty again while it is in flight

    parked = asyncio.create_task(broker.poll())
    await asyncio.sleep(0.01)  # let the poll peek an empty queue and park
    assert not parked.done(), "nothing queued: the poll must wait, not return"

    if held is None:
        await broker.publish(env)
    else:
        await held.nack()
    # timeout: a lost wakeup leaves the poll parked forever; fail instead of hanging
    delivery = delivered(await asyncio.wait_for(parked, 1))
    assert delivery.envelope.id == env.id
    await delivery.ack()
    assert broker.empty()


async def test_in_memory_broker_ack_after_nack_does_not_drop_the_redelivery() -> None:
    """At-least-once: a stale ack of an already-nacked delivery must settle nothing, or the
    requeued copy would vanish before anyone handled it."""
    broker = InMemoryBroker()
    await broker.publish(build_envelope(agg_id(0), "Deposited", 1))
    first = delivered(await broker.poll())
    await first.nack()
    await first.ack()  # the bug: the delivery was already settled by the nack
    assert not broker.empty(), "the redelivery is still queued"
    again = delivered(await asyncio.wait_for(broker.poll(), 1))
    assert again.envelope.id == first.envelope.id
    await again.ack()
    assert broker.empty()


@dataclass(frozen=True)
class DoubleSettleCase:
    """One settle operation, applied twice to the same delivery."""

    settle: str  # "ack" or "nack"


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(DoubleSettleCase("ack"), id="double_ack_warns"),
        pytest.param(DoubleSettleCase("nack"), id="double_nack_warns"),
    ],
)
async def test_memory_broker_warns_when_the_native_refuses_a_second_settlement(
    case: DoubleSettleCase, caplog: pytest.LogCaptureFixture
) -> None:
    """The native machine refuses a second settlement of one token and says so with ``False``. The
    refusal is a caller bug, so it is logged rather than dropped."""
    broker = InMemoryBroker()
    await broker.publish(build_envelope(agg_id(0), "Tick", 1))
    delivery = await broker.poll()
    assert delivery is not None
    settle = getattr(delivery, case.settle)

    with caplog.at_level(logging.WARNING, logger="edax.backend.memory"):
        await settle()
        assert caplog.records == [], "the first settlement is accepted, so it says nothing"
        await settle()

    assert len(caplog.records) == 1
    assert f"{case.settle} of already-settled delivery" in caplog.records[0].getMessage()

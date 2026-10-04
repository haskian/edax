"""Broker seam contract (:class:`edax.broker.Producer` / ``Consumer``) over every in-tree adapter: the
in-memory broker and Kafka over its Kafka-shaped fake, each through :class:`edax.testing.BrokerContract`
— per-key order, a nack that keeps the key's order, redelivery from the commit after a crash, consumer
groups. Then the consumer loop's own branches, and the memory broker's settlement accounting. Kafka's
watermark and commit specifics live in ``test_kafka.py``.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import edax
import pytest
from aiokafka import TopicPartition
from conftest import (
    FakeKafkaClient,
    FakeKafkaCluster,
    agg_id,
    build_message,
    delivered,
    event_of,
    sequence_of,
)
from edax.aggregate import _replace
from edax.backend.kafka import KafkaConsumer, KafkaProducer
from edax.backend.memory import MemoryBroker, MemoryEventStore
from edax.broker import UNDECODABLE, Consumer, UndecodableDelivery, _ConsumerLoop
from edax.testing import BrokerContract, drafted
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st


class MemoryRig:
    """The in-memory broker: a group is a view of one log, and a crash is the broker's own hook."""

    def __init__(self, partitions: int = 1) -> None:
        self.broker = MemoryBroker(partitions)
        self.groups: dict[str, MemoryBroker] = {}

    async def publish(self, env: edax.EventMessage | edax.CommandMessage) -> None:
        await self.broker.publish(env)

    async def consumer(self, group: str) -> Consumer:
        return self.groups.setdefault(group, self.broker.group(group))

    async def commit(self, group: str) -> None:
        """The memory broker commits on every ack that moves the watermark."""

    async def crash(self, group: str) -> Consumer:
        view = self.groups.setdefault(group, self.broker.group(group))
        view.crash()
        return view

    def drained(self, group: str) -> bool:
        return self.groups.setdefault(group, self.broker.group(group)).empty()


class KafkaRig:
    """Kafka over the fake: a group is a cluster of its own over the shared logs, its consumer one member
    with a :class:`KafkaConsumer`, a commit a flush, and a crash a member the broker drops with no commit,
    replaced by a new member that resumes from the group's commit."""

    def __init__(self, partitions: int = 1) -> None:
        self.broker = FakeKafkaCluster(partitions)
        self.producer = KafkaProducer(FakeKafkaClient(), "t", serializer=edax.KernelSerializer())
        self.producer._client = FakeKafkaClient(self.broker.group())  # publishes into the shared logs
        self.clusters: dict[str, FakeKafkaCluster] = {}
        self.members: dict[str, tuple[FakeKafkaClient, KafkaConsumer]] = {}

    async def publish(self, env: edax.EventMessage | edax.CommandMessage) -> None:
        await self.producer.publish(env)

    async def _join(self, group: str) -> KafkaConsumer:
        cluster = self.clusters.setdefault(group, self.broker.group())
        client = FakeKafkaClient(cluster)
        consumer = KafkaConsumer(
            client, serializer=edax.KernelSerializer(), serializers=[edax.KernelSerializer("json", "command")]
        )
        client.subscribe(["t"], listener=consumer.rebalance_listener(drain_timeout_s=0))
        await cluster.join(client)
        self.members[group] = (client, consumer)
        return consumer

    async def consumer(self, group: str) -> Consumer:
        if group in self.members:
            return self.members[group][1]
        return await self._join(group)

    async def commit(self, group: str) -> None:
        await self.members[group][1].flush()

    async def crash(self, group: str) -> Consumer:
        client, _ = self.members.pop(group)
        await self.clusters[group].leave(client, crash=True)
        return await self._join(group)

    def drained(self, group: str) -> bool:
        cluster = self.clusters.setdefault(group, self.broker.group())
        return all(
            cluster.committed.get(TopicPartition("t", p), 0) == len(self.broker.logs[("t", p)])
            for p in range(self.broker.partitions)
        )


class TestMemoryBroker(BrokerContract):
    @pytest.fixture(params=[1, 3], ids=["one_partition", "three_partitions"])
    def broker_rig(self, request: Any) -> MemoryRig:
        return MemoryRig(request.param)


class TestKafkaBroker(BrokerContract):
    @pytest.fixture(params=[1, 3], ids=["one_partition", "three_partitions"])
    def broker_rig(self, request: Any) -> KafkaRig:
        return KafkaRig(request.param)


# A factory, not an instance: hypothesis re-runs the test body per example on one fixture value,
# and its property below must stay a sync test (hypothesis cannot drive `async def`).
@pytest.fixture(params=[MemoryRig, KafkaRig], ids=["memory", "kafka"])
def make_rig(request: Any) -> Callable[[], MemoryRig | KafkaRig]:
    return request.param


async def _append(store: MemoryEventStore, versions: dict[int, int], agg: int, n: int) -> list[str]:
    v = versions.get(agg, 0)
    envs = [drafted(build_message(agg_id(agg), "Tick", v + i + 1)) for i in range(n)]
    async with edax.transaction(store) as tx:
        await store.append(tx, agg_id(agg), v, envs)
    versions[agg] = v + n
    return [e.id for e in envs]


class _ScriptedConsumer(_ConsumerLoop):
    """A consumer whose ``poll`` replays a script: a :class:`edax.Delivery`, ``None`` (end of
    stream), or ``"block"`` (park until cancelled)."""

    def __init__(self, script: list[Any]) -> None:
        self._script = list(script)

    async def poll(self) -> Any:
        item = self._script.pop(0)
        if item == "block":
            await asyncio.Event().wait()
        if isinstance(item, Exception):
            raise item
        return item


# A payload a schema change made unreadable: the poison message `poll` cannot turn into an event.
POISON_BYTES = b'{"schema":"v3","balance_cents":4200}'


@dataclass
class RunCase:
    script: list  # per poll: "d" (a delivery), "poison", None, or "block"
    cancel: bool
    handled: list  # event sequences the dispatcher handled, in order
    settled: list  # "ack" / "nack" per delivery, in settlement order
    returns: bool  # run() returned (end of stream) rather than raising CancelledError
    dead: list  # event names the dead-letter hook saw
    on_dead: bool = True  # wire the dispatcher's dead-letter hook
    sink_raises: bool = False  # the dead-letter sink is unreachable, the usual reason one fails
    max_queue: int = 0  # 0 = unbounded; a small queue makes `submit` block
    start: bool = True  # an unstarted dispatcher drains nothing, so its queue stays full
    slept: list = field(default_factory=list)  # backoff steps waited, in order
    raises: type[BaseException] | None = None  # run() raised this instead of returning or being cancelled
    nack_raises: bool = False  # the poison's nack raises, as a seek on a lost partition does


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
        # delivery polled but unsettled, and it must be nacked rather than dropped.
        pytest.param(
            RunCase(["d", "d"], True, [], ["nack"], False, [], max_queue=1, start=False),
            id="cancelled_between_poll_and_submit_nacks_the_delivery",
        ),
        pytest.param(
            RunCase(["poison", None], False, [], ["ack"], True, [UNDECODABLE]),
            id="undecodable_is_dead_lettered_then_acked",
        ),
        pytest.param(
            RunCase(["poison", None], False, [], ["nack"], True, [], on_dead=False, slept=[0]),
            id="undecodable_without_a_hook_is_nacked_after_a_backoff_step_never_dropped",
        ),
        # A DLQ outage must not stop the consumer, and must not leave the poison unsettled for a
        # redelivery no log-based broker makes. It nacks after a backoff step, so the sink gets another
        # try, and the loop goes on.
        pytest.param(
            RunCase(
                ["poison", "d", None],
                False,
                [1],
                ["nack", "ack"],
                True,
                [UNDECODABLE],
                sink_raises=True,
                slept=[0],
            ),
            id="undecodable_with_a_raising_sink_is_nacked_and_the_loop_continues",
        ),
        # A transient poll failure (a middleware's key service) waits a growing step and polls again.
        pytest.param(
            RunCase(["fail", "fail", "d", None], False, [1], ["ack"], True, [], slept=[0, 1]),
            id="a_transient_poll_failure_backs_off_and_the_loop_continues",
        ),
        # A failure that repeats on every poll ends the loop at once rather than spinning on it.
        pytest.param(
            RunCase(["terminal"], False, [], [], False, [], raises=edax.BadHandle),
            id="a_terminal_poll_failure_ends_the_loop",
        ),
        # A nack that raises while the poison settles is the transport's fault: logged, and the loop goes on.
        pytest.param(
            RunCase(
                ["poison", "d", None],
                False,
                [1],
                ["nack", "ack"],
                True,
                [],
                on_dead=False,
                nack_raises=True,
                slept=[0],
            ),
            id="a_poison_nack_that_raises_is_logged_and_the_loop_continues",
        ),
    ],
)
async def test_consumer_loop_run_branches(case: RunCase, slept: list[int]) -> None:
    handled: list[int] = []
    settled: list[str] = []
    dead: list[str] = []
    envs = [build_message(agg_id(0), "Tick", n) for n in range(1, 3)]

    def poison_nack() -> None:
        settled.append("nack")
        if case.nack_raises:
            raise edax.StoreError("seek on a partition this member lost")

    def scripted(step: Any) -> Any:
        if step == "poison":
            return UndecodableDelivery(
                POISON_BYTES,
                "msg-poison",
                lambda: settled.append("ack"),
                poison_nack,
                edax.CodecError("unknown schema v3"),
            )
        if step == "fail":
            return ConnectionError("key service unreachable")
        if step == "terminal":
            return edax.BadHandle("built on another thread")
        if step != "d":
            return step
        env = envs.pop(0)
        return edax.Delivery(env, lambda: settled.append("ack"), lambda: settled.append("nack"))

    async def on_dead(delivery: edax.Delivery, err: BaseException) -> None:
        assert isinstance(err, edax.CodecError) and err.terminal
        assert bytes(delivery.message.data) == POISON_BYTES, "the raw payload must survive"
        # Recorded before the raise: `dead` is what the sink saw, so it doubles as the call count and
        # the raising row still proves the sink was reached exactly once.
        dead.append(delivery.message.name)
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
    elif (raises := case.raises) is not None:
        with pytest.raises(raises):
            await asyncio.wait_for(task, 1)
    else:
        await asyncio.wait_for(task, 1)  # a run that never ends means a broken end-of-stream branch
    await dispatcher.stop()

    assert handled == case.handled
    assert settled == case.settled  # every polled delivery reached settlement
    assert dead == case.dead
    assert slept == case.slept
    assert task.done() and (not task.cancelled() and task.exception() is None) is case.returns


step = st.tuples(st.integers(0, 2), st.integers(1, 3), st.integers(1, 4))  # (agg, n events, drain limit)


# the fixture is a factory, so one fixture value shared across examples is intended
@settings(max_examples=100, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.lists(step, min_size=1, max_size=10))
def test_drain_history_reaches_consumer_in_key_order(
    make_rig: Callable[[], MemoryRig | KafkaRig], history: list[tuple[int, int, int]]
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
        consumer = await rig.consumer("drain")
        for _ in range(total):
            # timeout: a broken nack/publish leaves poll blocking forever; fail instead of hanging
            d = delivered(await asyncio.wait_for(consumer.poll(), 1))
            seen[event_of(d.message).aggregate_id].append(d.message.id)
            await d.ack()
        assert seen == {agg_id(a): ids for a, ids in want.items()}
        await rig.commit("drain")
        assert rig.drained("drain")

    asyncio.run(run())


async def test_in_memory_broker_reports_an_unsettled_delivery() -> None:
    """A polled-but-unsettled delivery keeps the broker non-empty, as Kafka reports it.

    Without this the in-memory rig flatters a dispatcher that drops a delivery — it would pass here
    and lose the message against a durable broker, which is the one bug the rig exists to catch.
    """
    broker = MemoryBroker()
    await broker.publish(build_message(agg_id(0), "Deposited", 1))
    assert not broker.empty()

    delivery = delivered(await broker.poll())
    assert not broker.empty(), "polled but not settled: still outstanding"

    await delivery.ack()
    assert broker.empty()


async def test_the_same_id_in_flight_twice_needs_two_settlements() -> None:
    """The outbox republishes a message after a crash between `publish` and `mark_published`, so
    one id is legitimately in flight twice. Tracking ids in a set would collapse the two into one
    entry and report drained while a delivery is still outstanding."""
    broker = MemoryBroker()
    env = build_message(agg_id(0), "Deposited", 1)
    await broker.publish(env)
    await broker.publish(env)  # at-least-once: the same message, republished

    first, second = delivered(await broker.poll()), delivered(await broker.poll())
    assert first.message.id == second.message.id, "the rig really is holding one id twice"

    await first.ack()
    assert not broker.empty(), "one copy settled, the other is still outstanding"
    await second.ack()
    assert broker.empty()


async def test_settling_a_delivery_twice_does_not_lose_an_outstanding_one() -> None:
    """A double ack from a buggy caller must not drive the count below what is really in flight,
    or the broker would report drained while another delivery is outstanding."""
    broker = MemoryBroker()
    for seq in (1, 2):
        await broker.publish(build_message(agg_id(0), "Deposited", seq))
    first, second = delivered(await broker.poll()), delivered(await broker.poll())

    await first.ack()
    await first.ack()  # the bug
    assert not broker.empty(), "the second delivery is still unsettled"

    await second.ack()
    assert broker.empty()


async def test_in_memory_broker_nack_settles_and_requeues() -> None:
    broker = MemoryBroker()
    await broker.publish(build_message(agg_id(0), "Deposited", 1))
    delivery = delivered(await broker.poll())
    await delivery.nack()
    # Settled as far as the poll is concerned, but back on the queue — so still not empty.
    assert not broker.empty()
    assert delivered(await broker.poll()).message.aggregate_id == agg_id(0)


@pytest.mark.parametrize(
    "wake", ["publish", "nack", "crash"], ids=["a_publish_wakes_it", "a_nack_wakes_it", "a_crash_wakes_it"]
)
async def test_in_memory_broker_poll_parks_until_a_message_arrives(wake: str) -> None:
    """The waiting is the one part Python owns over the native machine: a poll that finds nothing
    must park, and both ways a message can appear (a publish, a nack's requeue) must wake it."""
    broker = MemoryBroker()
    env = build_message(agg_id(0), "Deposited", 1)
    held: edax.Delivery | None = None
    if wake != "publish":
        await broker.publish(env)
        held = delivered(await broker.poll())  # taken, so nothing is fetchable while it is in flight

    parked = asyncio.create_task(broker.poll())
    await asyncio.sleep(0.01)  # let the poll peek an empty queue and park
    assert not parked.done(), "nothing queued: the poll must wait, not return"

    if held is None:
        await broker.publish(env)
    elif wake == "nack":
        await held.nack()
    else:
        broker.crash()
    # timeout: a lost wakeup leaves the poll parked forever; fail instead of hanging
    delivery = delivered(await asyncio.wait_for(parked, 1))
    assert delivery.message.id == env.id
    await delivery.ack()
    assert broker.empty()


async def test_in_memory_broker_ack_after_nack_does_not_drop_the_redelivery() -> None:
    """At-least-once: a stale ack of an already-nacked delivery must settle nothing, or the
    requeued copy would vanish before anyone handled it."""
    broker = MemoryBroker()
    await broker.publish(build_message(agg_id(0), "Deposited", 1))
    first = delivered(await broker.poll())
    await first.nack()
    await first.ack()  # the bug: the delivery was already settled by the nack
    assert not broker.empty(), "the redelivery is still queued"
    again = delivered(await asyncio.wait_for(broker.poll(), 1))
    assert again.message.id == first.message.id
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
    broker = MemoryBroker()
    await broker.publish(build_message(agg_id(0), "Tick", 1))
    delivery = await broker.poll()
    assert delivery is not None
    settle = getattr(delivery, case.settle)

    with caplog.at_level(logging.WARNING, logger="edax.backend.memory"):
        await settle()
        assert caplog.records == [], "the first settlement is accepted, so it says nothing"
        await settle()

    assert len(caplog.records) == 1
    assert f"{case.settle} of a settled or retired delivery" in caplog.records[0].getMessage()


async def test_a_publish_wakes_a_poller_of_every_group() -> None:
    """A message is a message for every group, so a publish through one view wakes a poller parked on
    another."""
    broker = MemoryBroker()
    saga = broker.group("saga")
    parked = asyncio.create_task(saga.poll())
    await asyncio.sleep(0.01)
    assert not parked.done()
    await broker.publish(build_message(agg_id(0), "Deposited", 1))
    delivery = delivered(await asyncio.wait_for(parked, 1))
    await delivery.ack()
    assert saga.empty() and not broker.empty(), "each group settles on its own"


def test_a_memory_broker_needs_a_partition() -> None:
    with pytest.raises(edax.CodecError, match="at least one partition"):
        MemoryBroker(0)


async def test_the_memory_broker_reproduces_kafkas_seek_back() -> None:
    """Nack a-1 after a-2 was polled and acked. A queue requeues a-1 alone, behind a-2; Kafka seeks back
    and redelivers a-1 *and* the acked a-2. The memory broker does what Kafka does, so a handler that
    cannot take the duplicate fails here first."""
    broker = MemoryBroker()
    for seq in (1, 2):
        await broker.publish(build_message("a", "Deposited", seq, id=f"a-{seq}"))
    a1, a2 = delivered(await broker.poll()), delivered(await broker.poll())
    await a2.ack()
    await a1.nack()
    seen = [delivered(await asyncio.wait_for(broker.poll(), 1)).message.id for _ in range(2)]
    assert seen == ["a-1", "a-2"]


# =========================================================================
# the broker contract discriminates: each plausible broken broker fails its clause
# =========================================================================


class QueueRig(MemoryRig):
    """The old memory broker: a nack requeues the delivery at the tail and retires nothing, so a later
    event of the key, handed out before the nack, still runs ahead of it."""

    def __init__(self) -> None:
        super().__init__()
        self.queue: list[edax.EventMessage] = []
        self.unsettled = 0

    async def publish(self, env: edax.EventMessage | edax.CommandMessage) -> None:
        self.queue.append(event_of(env))

    async def consumer(self, group: str) -> Consumer:
        rig = self

        class Queue(_ConsumerLoop):
            async def poll(self) -> edax.Delivery | None:
                while not rig.queue:
                    await asyncio.sleep(0.001)
                env = rig.queue.pop(0)
                rig.unsettled += 1

                def ack() -> None:
                    rig.unsettled -= 1

                def nack() -> None:
                    rig.unsettled -= 1
                    rig.queue.append(env)

                return edax.Delivery(env, ack, nack)

        return Queue()

    def drained(self, group: str) -> bool:
        return not self.queue and not self.unsettled


class AmnesiacCrashRig(MemoryRig):
    """A crash that resumes from the start of the log instead of the group's commit: committed work runs
    again."""

    async def crash(self, group: str) -> Consumer:
        view = self.groups.pop(group)
        view.crash()
        fresh = self.broker.group(f"{group}-reborn")
        self.groups[group] = fresh
        return fresh


class GhostRig(MemoryRig):
    """A broker that, once caught up, hands out the first message again: a delivery after everything was
    settled."""

    async def consumer(self, group: str) -> Consumer:
        inner = await super().consumer(group)
        seen: list[edax.Delivery] = []

        class Ghost(_ConsumerLoop):
            async def poll(self) -> edax.Delivery | None:
                try:
                    delivery = await asyncio.wait_for(inner.poll(), 0.01)
                except asyncio.TimeoutError:
                    return seen[0]
                if delivery is not None:
                    seen.append(delivery)
                return delivery

        return Ghost()


class DropsTheTenantRig(MemoryRig):
    """A broker adapter whose wire schema has no tenant field: every message arrives tenant-less, so a
    pooled consumer can key and claim nothing it receives."""

    async def publish(self, env: edax.EventMessage | edax.CommandMessage) -> None:
        await super().publish(_replace(env, tenant=None))


class SharedGroupRig(MemoryRig):
    """Every group is the one consumer: a projection and a saga compete for each message."""

    async def consumer(self, group: str) -> Consumer:
        return await super().consumer("shared")

    def drained(self, group: str) -> bool:
        return super().drained("shared")


@pytest.mark.parametrize(
    "rig, clause",
    [
        pytest.param(
            QueueRig, "test_a_nack_never_reorders_its_key", id="a_requeue_at_the_tail_reorders_the_key"
        ),
        pytest.param(
            AmnesiacCrashRig, "test_a_crash_redelivers_from_the_commit", id="a_crash_that_forgets_the_commit"
        ),
        pytest.param(GhostRig, "test_a_nack_never_reorders_its_key", id="a_ghost_delivery_after_settlement"),
        pytest.param(
            SharedGroupRig, "test_every_consumer_group_reads_every_message", id="groups_that_compete"
        ),
        pytest.param(
            DropsTheTenantRig, "test_publish_then_poll_keeps_each_keys_order", id="an_event_loses_its_tenant"
        ),
        pytest.param(
            DropsTheTenantRig,
            "test_a_command_is_delivered_whole_and_keyed_by_its_id",
            id="a_command_loses_its_tenant",
        ),
    ],
)
async def test_the_broker_contract_fails_a_broken_broker(rig: Callable[[], Any], clause: str) -> None:
    with pytest.raises(AssertionError):
        await getattr(BrokerContract(), clause)(rig())

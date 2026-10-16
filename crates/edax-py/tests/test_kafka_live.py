"""The Kafka adapter against a live broker, for the faults only a real broker, a real group coordinator and a
real process have: a nack that seeks a real fetcher back, a consumer process killed mid-batch, a second
member joining mid-batch, a raising dead-letter sink, a poison record, a broker frozen during a commit, and
a broker restarted during publishes. Set ``EDAX_KAFKA_BOOTSTRAP`` (``localhost:59192``) to run them; the
pause and restart tests also need ``EDAX_KAFKA_CONTAINER``, the Docker container to freeze or restart.
Without them the module is skipped: ``test_kafka.py`` covers the same decisions on the Kafka-shaped fake."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import textwrap
import time
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import edax
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from conftest import SeqSerializer, build_message, handling, publish_all, sequence_of, sink_ok
from edax.backend.kafka import KafkaConsumer, KafkaProducer
from edax.sealing import Cipher

BOOTSTRAP: str | None = os.environ.get("EDAX_KAFKA_BOOTSTRAP")
CONTAINER: str | None = os.environ.get("EDAX_KAFKA_CONTAINER")

if BOOTSTRAP is None:
    pytest.skip("EDAX_KAFKA_BOOTSTRAP is not set: no live Kafka", allow_module_level=True)

SERVERS: str = BOOTSTRAP or ""


def _needs_container() -> None:
    if CONTAINER is None:
        raise pytest.skip.Exception("EDAX_KAFKA_CONTAINER is not set: cannot pause or restart the broker")


def _docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True, timeout=120)


@pytest.fixture
async def topic() -> AsyncIterator[Callable[[int], Awaitable[str]]]:
    """A factory of fresh topics with a given partition count, deleted afterwards."""
    admin = AIOKafkaAdminClient(bootstrap_servers=SERVERS)
    await admin.start()
    made: list[str] = []

    async def make(partitions: int) -> str:
        name = f"edax-live-{uuid.uuid4().hex[:10]}"
        await admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])
        made.append(name)
        return name

    try:
        yield make
    finally:
        try:
            await admin.delete_topics(made)
        finally:
            await admin.close()


@pytest.fixture
async def producer() -> AsyncIterator[AIOKafkaProducer]:
    client = AIOKafkaProducer(bootstrap_servers=SERVERS, enable_idempotence=True)
    await client.start()
    try:
        yield client
    finally:
        await client.stop()


def _consumer_client(topic: str, group: str) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        bootstrap_servers=SERVERS,
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        session_timeout_ms=6000,
        heartbeat_interval_ms=1000,
    )


async def _group_offsets(group: str, topic: str, partitions: int) -> dict[int, int]:
    admin = AIOKafkaAdminClient(bootstrap_servers=SERVERS)
    await admin.start()
    try:
        offsets = await admin.list_consumer_group_offsets(group)
    finally:
        await admin.close()
    return {
        tp.partition: meta.offset
        for tp, meta in offsets.items()
        if tp.topic == topic and tp.partition < partitions
    }


async def _wait(done: Callable[[], bool], limit: float, what: str) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + limit
    while not done():
        if loop.time() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.02)


class RecordingConsumer(AIOKafkaConsumer):
    """An aiokafka consumer that records every commit that landed, with the time it landed."""

    def __init__(self, topic: str, group: str) -> None:
        super().__init__(
            bootstrap_servers=SERVERS,
            group_id=group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            session_timeout_ms=6000,
            heartbeat_interval_ms=1000,
        )
        self.commits: list[tuple[float, TopicPartition, int]] = []

    async def commit(self, offsets: Any = None) -> None:
        await super().commit(offsets)
        self.commits.extend((time.monotonic(), tp, offset) for tp, offset in (offsets or {}).items())


class Member:
    """One consumer process in a group: an aiokafka client, the edax adapter with its rebalance listener, a
    dispatcher, and the running loop."""

    def __init__(
        self,
        topic: str,
        group: str,
        handler: Callable[[edax.EventMessage], Any],
        *,
        on_dead: Any = None,
        serializer: Any = None,
        commit_timeout_s: float = 10.0,
    ) -> None:
        self.client = RecordingConsumer(topic, group)
        self.commits = self.client.commits
        self.consumer = KafkaConsumer(
            self.client,
            serializer=serializer or SeqSerializer(),
            commit_interval_s=0.05,
            commit_timeout_s=commit_timeout_s,
        )
        self.client.subscribe([topic], listener=self.consumer.rebalance_listener(drain_timeout_s=5))
        self.dispatcher = edax.KeyOrderedDispatcher(workers=4, handler=handler, on_dead=on_dead or sink_ok)
        self.task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        await self.client.start()
        self.dispatcher.start()
        self.task = asyncio.create_task(self.consumer.run(self.dispatcher))

    async def stop(self) -> None:
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.dispatcher.stop(timeout=5)
        await self.consumer.flush()
        await self.client.stop()


async def test_a_nack_on_a_real_fetcher_keeps_the_key_in_order(
    topic: Any, producer: AIOKafkaProducer
) -> None:
    """Live: seq 1 fails once while seq 2 and 3 are already fetched. The seek-back redelivers them
    behind seq 1, the copies fetched before it are skipped, and the staleness guard loses nothing."""
    name = await topic(1)
    await publish_all(producer, name, SeqSerializer(), [build_message("a", "Tick", seq) for seq in (1, 2, 3)])
    stored: int | None = None
    applied: list[int] = []
    runs: list[int] = []
    member_box: list[Member] = []

    async def handler(env: edax.EventMessage) -> None:
        nonlocal stored
        runs.append(sequence_of(env))
        if sequence_of(env) == 1 and runs.count(1) == 1:
            # Fail only once seq 3 is fetched, so the later copies are queued behind seq 1.
            consumer = member_box[0].consumer
            while not any(row.max_delivered == 2 for row in consumer.stats().values()):
                await asyncio.sleep(0.005)
            raise edax.StoreError("projection db blip")
        if not edax.is_stale(stored, env.sequence):
            applied.append(sequence_of(env))
            stored = edax.advance(stored, env.sequence)

    member = Member(name, f"g-{uuid.uuid4().hex[:8]}", handler)
    member_box.append(member)
    await member.start()
    try:
        await _wait(lambda: applied == [1, 2, 3], 30, "every event applied")
    finally:
        await member.stop()
    assert applied == [1, 2, 3]
    assert runs[:2] == [1, 1], f"a later event ran between the failure and the redelivery: {runs}"
    assert [s for s in runs if s == 2] == [2], "seq 2 ran twice: a stale copy was not skipped"


async def test_a_consumer_killed_mid_batch_redelivers_from_the_commit(
    topic: Any, producer: AIOKafkaProducer
) -> None:
    """A consumer process is killed with SIGKILL mid-batch, with work committed, work handled but not yet
    committed, and work in flight. Its replacement starts from the group's commit: every offset from the
    commit runs again, and none below it."""
    name, group = await topic(1), f"g-{uuid.uuid4().hex[:8]}"
    await publish_all(
        producer, name, SeqSerializer(), [build_message(f"k{i % 4}", "Tick", i + 1) for i in range(40)]
    )
    script = textwrap.dedent(
        f"""
        import asyncio, sys
        from typing import Any
        sys.path.insert(0, {os.path.dirname(__file__)!r})
        import edax
        from aiokafka import AIOKafkaConsumer
        from edax.backend.kafka import KafkaConsumer
        from test_kafka_live import SeqSerializer

        async def main():
            client = AIOKafkaConsumer(bootstrap_servers={SERVERS!r}, group_id={group!r},
                enable_auto_commit=False, auto_offset_reset="earliest")
            consumer = KafkaConsumer(client, serializer=SeqSerializer(), commit_interval_s=0.05)
            client.subscribe([{name!r}], listener=consumer.rebalance_listener())
            await client.start()
            async def handler(env):
                print(env.sequence, flush=True)
                if env.sequence >= 25:
                    await asyncio.Event().wait()  # in flight when the process dies
            async def sink(letter: edax.DeadLetter[Any]) -> None: pass
            dispatcher = edax.KeyOrderedDispatcher(workers=4, handler=handler, on_dead=sink)
            dispatcher.start()
            await consumer.run(dispatcher)

        asyncio.run(main())
        """
    )
    env = {**os.environ, "EDAX_KAFKA_BOOTSTRAP": SERVERS}
    proc = subprocess.Popen([sys.executable, "-c", script], stdout=subprocess.PIPE, text=True, env=env)
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            offsets = await _group_offsets(group, name, 1)
            if offsets.get(0, 0) >= 20:
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("the first consumer never committed past 20")
    finally:
        proc.send_signal(signal.SIGKILL)
        out, _ = proc.communicate(timeout=10)
    first_handled = {int(line) for line in out.split()}
    committed = (await _group_offsets(group, name, 1))[0]
    assert committed <= 25, "committed past a delivery still in flight"

    handled: list[int] = []
    member = Member(name, group, lambda e: handled.append(sequence_of(e)))
    await member.start()
    try:
        await _wait(lambda: len(set(handled)) >= 40 - committed, 60, "the replacement to finish")
    finally:
        await member.stop()
    # The partition is in publish order, so offset o carries sequence o + 1.
    assert min(handled) == committed + 1, "the replacement did not start at the group's commit"
    assert set(handled) == set(range(committed + 1, 41)), "an offset from the commit on was lost"
    assert first_handled | set(handled) == set(range(1, 41))
    assert (await _group_offsets(group, name, 1))[0] == 40


async def test_a_member_joining_mid_batch_drains_the_revoked_work_first(
    topic: Any, producer: AIOKafkaProducer
) -> None:
    """Live: a second member joins while the first is mid-batch. The first one's listener drains its
    in-flight work and commits before the partitions move, so no key is handled by two members at once, no
    commit moves a partition back, and every message is handled."""
    name, group = await topic(4), f"g-{uuid.uuid4().hex[:8]}"
    keys = [f"k{i}" for i in range(8)]
    await publish_all(
        producer,
        name,
        SeqSerializer(),
        [build_message(k, "Tick", seq) for seq in range(1, 61) for k in keys],
    )
    active: dict[str, str] = {}
    overlaps: list[str] = []
    handled: dict[str, set[int]] = defaultdict(set)

    def handler_for(owner: str) -> Callable[[edax.EventMessage], Awaitable[None]]:
        async def handler(env: edax.EventMessage) -> None:
            key = env.aggregate_id
            if key in active and active[key] != owner:
                overlaps.append(f"{key}: {active[key]} and {owner}")
            active[key] = owner
            await asyncio.sleep(0.05)  # slow enough that the handover lands mid-batch, with work in flight
            if active.get(key) == owner:
                del active[key]
            handled[key].add(sequence_of(env))

        return handler

    first = Member(name, group, handler_for("first"))
    second = Member(name, group, handler_for("second"))
    await first.start()
    try:
        await _wait(
            lambda: sum(len(v) for v in handled.values()) >= 40, 60, "the first member to be mid-batch"
        )
        await second.start()
        await _wait(lambda: all(len(handled[k]) == 60 for k in keys), 120, "every message handled")
    finally:
        await second.stop()
        await first.stop()
    assert overlaps == [], f"a key was handled by two members at once: {overlaps[:5]}"
    commits = sorted(first.commits + second.commits)
    for p in range(4):
        tp = TopicPartition(name, p)
        sequence = [offset for _, at, offset in commits if at == tp]
        assert sequence == sorted(sequence), f"the commit of {tp} moved backwards: {sequence}"
    final = await _group_offsets(group, name, 4)
    assert sum(final.values()) == 8 * 60, "the group did not commit past every message"
    assert first.commits and second.commits, "both members committed: the partitions really moved"


async def test_a_raising_sink_gets_another_try_and_the_partition_drains(
    topic: Any, producer: AIOKafkaProducer
) -> None:
    """Live: the sink raises once. The delivery is nacked, sought back, dead-lettered on the next try,
    and the partition's commit reaches its end."""
    name, group = await topic(1), f"g-{uuid.uuid4().hex[:8]}"
    await publish_all(
        producer, name, SeqSerializer(), [build_message("a", "Tick", seq) for seq in range(1, 11)]
    )
    sink_calls: list[int] = []

    def handler(env: edax.EventMessage) -> None:
        if sequence_of(env) == 4:
            raise edax.CodecError("negative balance")

    async def on_dead(letter: edax.DeadLetter[Any]) -> None:
        sink_calls.append(sequence_of(letter.message))
        if len(sink_calls) == 1:
            raise ConnectionError("DLQ topic unavailable")

    member = Member(name, group, handler, on_dead=on_dead)
    await member.start()
    try:
        await _wait(lambda: any(o == 10 for _, _, o in member.commits), 30, "the partition to drain")
    finally:
        await member.stop()
    assert sink_calls == [4, 4]
    assert (await _group_offsets(group, name, 1))[0] == 10


async def test_a_poison_record_backs_off_and_a_sinkless_run_is_refused(
    topic: Any, producer: AIOKafkaProducer
) -> None:
    """Live: with a sink that keeps failing, a poison record is retried at the backoff schedule's rate, a
    handful a second rather than tens of thousands, and a run with no sink is refused outright."""
    name, group = await topic(1), f"g-{uuid.uuid4().hex[:8]}"
    await producer.send_and_wait(
        name, b"\xff\xfe not a message", key=b"a", headers=[("content-type", b"application/x-nobody")]
    )
    tries: list[float] = []

    async def on_dead(letter: edax.DeadLetter[Any]) -> None:
        tries.append(time.monotonic())
        raise ConnectionError("DLQ topic unavailable")

    member = Member(name, group, lambda env: None, on_dead=on_dead)
    await member.start()
    try:
        await _wait(lambda: len(tries) >= 1, 30, "the first poison try")
        await asyncio.sleep(2)
    finally:
        await member.stop()
    window = [t for t in tries if t - tries[0] <= 2]
    assert len(window) <= 20, f"{len(window)} poison tries in 2 s: the backoff did not hold"

    client = _consumer_client(name, group)
    sinkless = KafkaConsumer(client, serializer=SeqSerializer())
    with pytest.raises(edax.BadHandle, match="dead-letter sink"):
        await sinkless.run(edax.KeyOrderedDispatcher(workers=1, handler=handling(lambda env: None)))


async def test_a_frozen_broker_costs_a_commit_its_deadline_not_the_run(
    topic: Any, producer: AIOKafkaProducer
) -> None:
    """Live: the broker is frozen (``docker pause``) while commits are due. Each flush gives up at its
    deadline instead of waiting on aiokafka's own retry loop, the run stops on time, and once the broker
    is back the same watermark commits."""
    _needs_container()
    name, group = await topic(1), f"g-{uuid.uuid4().hex[:8]}"
    await publish_all(
        producer, name, SeqSerializer(), [build_message("a", "Tick", seq) for seq in range(1, 6)]
    )
    handled: list[int] = []
    member = Member(name, group, lambda e: handled.append(sequence_of(e)), commit_timeout_s=1.0)
    await member.start()
    await _wait(lambda: len(handled) == 5, 30, "every message handled")
    await _wait(lambda: any(o == 5 for _, _, o in member.commits), 10, "the first commit")
    await publish_all(producer, name, SeqSerializer(), [build_message("a", "Tick", 6)])
    await _wait(lambda: len(handled) == 6, 30, "the sixth message")
    _docker("pause", CONTAINER or "")
    try:
        started = time.monotonic()
        with pytest.raises(edax.StoreError, match="kafka commit: no reply within"):
            await member.consumer.flush()
        assert time.monotonic() - started < 5, "the commit deadline did not hold"
        stop_started = time.monotonic()
        run = member.task
        assert run is not None
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)
        assert time.monotonic() - stop_started < 5, "a frozen broker held the run's stop hostage"
    finally:
        _docker("unpause", CONTAINER or "")
    await _wait_for_flush(member.consumer, 60)
    await member.dispatcher.stop(timeout=5)
    await member.client.stop()
    assert (await _group_offsets(group, name, 1))[0] == 6


async def _start_when_up(start: Callable[[], Awaitable[None]], limit: float = 60) -> None:
    """Start a client once the broker answers again: a restarted container takes a few seconds."""
    deadline = time.monotonic() + limit
    while True:
        try:
            await start()
            return
        except Exception:
            if time.monotonic() > deadline:
                raise
            await asyncio.sleep(0.5)


async def _wait_for_flush(consumer: KafkaConsumer, limit: float) -> None:
    """Flush until the broker answers: right after an unpause the coordinator may still be settling."""
    deadline = time.monotonic() + limit
    while True:
        try:
            await consumer.flush()
            return
        except Exception:
            if time.monotonic() > deadline:
                raise
            await asyncio.sleep(0.5)


async def test_a_broker_restart_during_publishes_loses_nothing_acked(topic: Any) -> None:
    """Live: the broker restarts while the outbox publishes. With ``enable_idempotence=True`` (so
    ``acks="all"``) every publish that returned is on the log afterwards; a publish that raised is retried,
    as the relay retries a batch it could not mark."""
    _needs_container()
    name = await topic(1)
    client = AIOKafkaProducer(
        bootstrap_servers=SERVERS, enable_idempotence=True, request_timeout_ms=5000, retry_backoff_ms=200
    )
    await client.start()
    edax_producer = KafkaProducer(client, name, serializer=SeqSerializer())
    acked: list[int] = []
    restarted = False
    try:
        for seq in range(1, 61):
            if seq == 20 and not restarted:
                restarted = True
                restart = asyncio.create_task(
                    asyncio.to_thread(_docker, "restart", "-t", "5", CONTAINER or "")
                )
            for _ in range(100):
                try:
                    await edax_producer.publish(build_message("a", "Tick", seq))
                    acked.append(seq)
                    break
                except Exception:  # noqa: BLE001 - the broker is restarting; the relay retries too
                    await asyncio.sleep(0.5)
            else:
                raise AssertionError(f"publish {seq} never succeeded")
        await restart
    finally:
        await client.stop()

    seen: set[int] = set()
    reader = AIOKafkaConsumer(
        name, bootstrap_servers=SERVERS, auto_offset_reset="earliest", enable_auto_commit=False
    )
    await _start_when_up(reader.start)
    try:
        deadline = time.monotonic() + 60
        while not set(acked) <= seen and time.monotonic() < deadline:
            batch = await reader.getmany(timeout_ms=1000)
            for records in batch.values():
                seen.update(sequence_of(SeqSerializer().decode(r.value or b"")) for r in records)
    finally:
        await reader.stop()
    assert acked == list(range(1, 61))
    assert set(acked) <= seen, f"acked publishes missing after the restart: {sorted(set(acked) - seen)}"


@pytest.mark.parametrize("cipher", ["aes-256-gcm", "xchacha20-poly1305"])
async def test_protected_record_and_dead_letter_never_persist_opened_payload(
    topic: Callable[[int], Awaitable[str]], producer: AIOKafkaProducer, cipher: Cipher
) -> None:
    from edax import _native
    from edax._values import replace
    from edax.backend.kafka import KafkaDeadLetterSink, KafkaFetched
    from edax.testing import drafted

    name, dead_name = await topic(1), await topic(1)
    secret = b"customer-private-address@example.test"
    keys = edax.MemoryKeys()
    sealing = edax.Sealing(keys, key_namespace="live-test", cipher=cipher)
    raw = replace(build_message("opaque-aggregate", "Protected", 1, data=secret), subject="opaque-subject")
    encrypted = _native.record_event(await sealing.seal_event(drafted(raw)), raw.recorded_at)
    consumer = AIOKafkaConsumer(
        bootstrap_servers=SERVERS, enable_auto_commit=False, auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        consumer.assign([TopicPartition(name, 0), TopicPartition(dead_name, 0)])
        await KafkaProducer(producer, name).publish(encrypted)
        fetched = await asyncio.wait_for(consumer.getone(TopicPartition(name, 0)), 10)
        assert isinstance(fetched.value, bytes) and isinstance(fetched.key, bytes)
        assert secret not in fetched.value
        decoded = edax.KernelSerializer().decode(fetched.value)
        assert isinstance(decoded, edax.EventMessage)
        assert await sealing.open_event(decoded) == secret
        origin = KafkaFetched(
            name, fetched.partition, fetched.offset, fetched.key, fetched.value, tuple(fetched.headers)
        )
        await KafkaDeadLetterSink(producer, dead_name)(
            edax.DeadLetter(decoded, 1, edax.Rejected(secret.decode()), origin)
        )
        dead = await asyncio.wait_for(consumer.getone(TopicPartition(dead_name, 0)), 10)
        assert dead.value == fetched.value
        assert dict(dead.headers)["edax-dlq-error"] == b"rejected"
        assert all(secret not in value for _, value in dead.headers)
    finally:
        await consumer.stop()

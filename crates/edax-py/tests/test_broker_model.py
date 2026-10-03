"""A model test of the in-tree brokers under random faults (hypothesis): the memory broker and the Kafka
adapter over its Kafka-shaped fake, each driven by a key-ordered consumer whose handlers start and finish
separately, through publishes, polls, successes, transient failures, commits and crashes in any order.

Invariants: a handler processes event ``s`` of a key only after ``s - 1`` (per-key order through every
nack and crash); a delivery the broker retired is never run; once the faults stop, every message is
processed and the group commits past all of it — so no commit passed a message that was not processed,
or the crash after it would have skipped that message. The Rust twin over the kernel tracker and the
memory machine is ``crates/edax-kafka/tests/model.rs`` and ``crates/edax-memory/tests/broker_model.rs``.
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

import edax
import pytest
from conftest import build_envelope, sequence_of
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st
from test_broker_contract import KafkaRig, MemoryRig

KEYS = 2

op = st.one_of(
    st.tuples(st.just("publish"), st.integers(0, KEYS - 1)),
    st.tuples(st.just("poll"), st.just(0)),
    st.tuples(st.just("start"), st.integers(0, KEYS - 1)),
    st.tuples(st.just("finish"), st.integers(0, KEYS - 1), st.booleans()),
    st.tuples(st.just("commit"), st.just(0)),
    st.tuples(st.just("crash"), st.just(0)),
)


async def _try_poll(consumer: Any) -> edax.Delivery | None:
    """A poll that gives up when nothing is fetchable. Both brokers answer a fetchable message within a
    couple of loop turns, and a parked poll is safe to cancel: nothing was booked."""
    task = asyncio.ensure_future(consumer.poll())
    for _ in range(5):
        if task.done():
            return task.result()
        await asyncio.sleep(0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    return None


class Model:
    def __init__(self, rig: Any) -> None:
        self.rig = rig
        self.consumer: Any = None
        self.queues: list[deque[edax.Delivery]] = [deque() for _ in range(KEYS)]
        self.running: list[edax.Delivery | None] = [None] * KEYS
        self.processed: dict[str, int] = {}
        self.published = [0] * KEYS

    async def setup(self) -> None:
        self.consumer = await self.rig.consumer("g")

    async def publish(self, key: int) -> None:
        self.published[key] += 1
        await self.rig.publish(build_envelope(f"k{key}", "Tick", self.published[key]))

    async def poll(self) -> bool:
        delivery = await _try_poll(self.consumer)
        if delivery is None:
            return False
        self.queues[int(delivery.envelope.aggregate_id[1:])].append(delivery)
        return True

    def start(self, key: int) -> None:
        if self.running[key] is not None:
            return
        while self.queues[key]:
            delivery = self.queues[key].popleft()
            if delivery.live():
                self.running[key] = delivery
                return

    async def finish(self, key: int, ok: bool) -> None:
        delivery, self.running[key] = self.running[key], None
        if delivery is None:
            return
        if not ok:
            await delivery.nack()
            return
        env, seq = delivery.envelope, sequence_of(delivery.envelope)
        last = self.processed.get(env.aggregate_id, 0)
        assert seq <= last + 1, f"{env.aggregate_id} processed {seq} after only {last}"
        self.processed[env.aggregate_id] = max(last, seq)
        await delivery.ack()

    async def crash(self) -> None:
        self.consumer = await self.rig.crash("g")
        self.queues = [deque() for _ in range(KEYS)]
        self.running = [None] * KEYS

    async def drain(self) -> None:
        for _ in range(1000):
            while await self.poll():
                pass
            for key in range(KEYS):
                self.start(key)
                await self.finish(key, True)
            await self.rig.commit("g")
            if self.rig.drained("g") and not any(self.queues):
                return
        raise AssertionError("the consumer never drained")


async def _run(rig: Any, ops: list[tuple[Any, ...]]) -> None:
    model = Model(rig)
    await model.setup()
    for step in ops:
        kind = step[0]
        if kind == "publish":
            await model.publish(step[1])
        elif kind == "poll":
            await model.poll()
        elif kind == "start":
            model.start(step[1])
        elif kind == "finish":
            await model.finish(step[1], step[2])
        elif kind == "commit":
            await rig.commit("g")
        else:
            await model.crash()
    await model.drain()
    want = {f"k{k}": n for k, n in enumerate(model.published) if n}
    assert model.processed == want, "a message was lost"


@pytest.fixture(params=["memory", "kafka"])
def rig_kind(request: Any) -> str:
    return request.param


#: Spelled out so every run checks it: seq 2 is queued behind seq 1, seq 1 fails, and the seq 2 copy
#: must not run before the redelivered seq 1.
NACK_BEHIND_A_QUEUED_COPY = [
    ("publish", 0),
    ("publish", 0),
    ("poll", 0),
    ("poll", 0),
    ("start", 0),
    ("finish", 0, False),
    ("start", 0),
    ("finish", 0, True),
]
#: A crash with one acked and one held: the held one comes back, the acked one may.
CRASH = [
    ("publish", 0),
    ("publish", 1),
    ("poll", 0),
    ("poll", 0),
    ("start", 0),
    ("finish", 0, True),
    ("crash", 0),
]


@settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(partitions=st.integers(1, 3), ops=st.lists(op, min_size=8, max_size=80))
@example(partitions=1, ops=NACK_BEHIND_A_QUEUED_COPY)
@example(partitions=2, ops=CRASH)
def test_every_message_is_processed_once_in_key_order_whatever_the_faults(
    rig_kind: str, partitions: int, ops: list[tuple[Any, ...]]
) -> None:
    rig = MemoryRig(partitions) if rig_kind == "memory" else KafkaRig(partitions)
    asyncio.run(_run(rig, ops))

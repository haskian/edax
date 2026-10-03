"""Shared test fixtures and the in-tree fault rigs.

The fault wrappers themselves ship in :mod:`edax.testing.faults`, where an adopter's suite gets the same
ones: :class:`~edax.testing.faults.LostResponse` lands a write and loses its reply (``"commit"`` loses a whole
unit of work's), :class:`~edax.testing.faults.Refusing` refuses one, :class:`~edax.testing.faults.BrokenCommit`
fails a commit either way, :class:`~edax.testing.faults.LostReply` loses one Redis script's reply. The
``Flaky*`` factories here are those wrappers over the memory stores.
"""

import asyncio
import json
import os
import types
from collections import defaultdict, namedtuple
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest import mock

import aiokafka
import aiokafka.errors
import edax
import fakeredis
import pytest
import redis.asyncio
import redis.exceptions
from aiokafka import TopicPartition
from edax.backend.kafka import KafkaConsumer, KafkaProducer
from edax.backend.memory import (
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemoryProjectionStore,
    MemorySagaStore,
    MemoryTransactor,
)
from edax.testing.faults import LostResponse, Refusing

GOLDEN_DIR: Path = Path(__file__).resolve().parents[3] / "testdata" / "golden"
_GOLDEN_READ: set[str] = set()


def golden(name: str) -> list[dict[str, Any]]:
    """The cases of ``testdata/golden/<name>``: the one reader every suite uses. Each name read is
    recorded, so a full run can prove that every vector the kernels write has a Python reader."""
    _GOLDEN_READ.add(name)
    cases: list[dict[str, Any]] = json.loads((GOLDEN_DIR / name).read_text())
    return cases


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Fail a full, green run that left a golden vector unread: a vector no Python test replays
    proves nothing across languages. A partial run (a ``-k``, a ``-m``, one file) skips the check,
    because it never meant to read every vector."""
    if exitstatus != 0 or session.config.option.keyword or session.config.option.markexpr:
        return
    test_files = {path.name for path in Path(__file__).parent.glob("test_*.py")}
    if not test_files <= {item.path.name for item in session.items}:
        return
    unread = sorted({path.name for path in GOLDEN_DIR.glob("*.json")} - _GOLDEN_READ)
    if unread:
        reporter = session.config.pluginmanager.get_plugin("terminalreporter")
        if reporter is not None:
            reporter.write_line(f"golden vectors no Python test read: {', '.join(unread)}", red=True)
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def delivered(delivery: edax.Delivery | None) -> edax.Delivery:
    """Narrow a ``Consumer.poll`` result. ``poll`` returns ``None`` at end of stream; every caller
    here has just published, so ``None`` means the broker lost the message — a failed assertion
    naming that, not an ``AttributeError`` three lines later."""
    assert delivery is not None, "poll returned None: the broker had nothing to deliver"
    return delivery


def sequence_of(env: edax.EventEnvelope) -> int:
    """An envelope's ``sequence``. Optional on the wire (an unsequenced envelope is legal), but
    every envelope these tests build or store carries one, so a ``None`` here is a store defect."""
    assert env.sequence is not None, f"envelope {env.id} reached the handler without a sequence"
    return env.sequence


def assert_logged(caplog: pytest.LogCaptureFixture, logger_name: str, fragments: Sequence[str]) -> None:
    """``logger_name`` emitted exactly one record per fragment, in order, each containing its fragment."""
    logged = [r.getMessage() for r in caplog.records if r.name == logger_name]
    assert len(logged) == len(fragments), logged
    assert all(want in got for want, got in zip(fragments, logged, strict=True)), logged


@dataclass
class CreateUser:
    email: str
    # Unannotated, so it is a class attribute (out of `vars()` and out of the hashed payload) that
    # still satisfies the `Command` protocol, whose `type` is a read-only property. `ClassVar[str]`
    # is the declared form and satisfies it too; this case covers the bare attribute.
    type = "user.create"


def build_envelope(
    aggregate_id: str,
    name: str,
    sequence: int | None,
    *,
    id: str | None = None,
    version: int = 1,
    data: bytes = b"",
    namespace: dict[str, str] | None = None,
    metadata: dict[str, str] | None = None,
    timestamp: int = 0,
) -> edax.EventEnvelope:
    """A native :class:`edax.EventEnvelope` with sensible defaults; ``id`` defaults to a stable
    ``{aggregate_id}-{name}-{sequence}``."""
    return edax.EventEnvelope(
        id=id or f"{aggregate_id}-{name}-{sequence}",
        aggregate_id=aggregate_id,
        event=edax.Event(name, version, data),
        namespace=namespace or {},
        metadata=metadata or {},
        timestamp=timestamp,
        sequence=sequence,
    )


# =========================================================================
# Fault injection: the shipped wrappers over the memory stores
# =========================================================================


def FlakyEventStore() -> Any:
    """A memory event store whose next armed call (``.arm("append")``, ``.arm("commit")``) loses its reply."""
    return LostResponse(MemoryEventStore())


def FlakyProjectionStore() -> Any:
    return LostResponse(MemoryProjectionStore())


def FlakySagaStore() -> Any:
    return LostResponse(MemorySagaStore())


def FlakyIdempotency(inner: Any) -> Any:
    """Any ``IdempotencyStore`` (memory or Redis) losing a reply identically: the fault fires after the inner
    store has committed."""
    return LostResponse(inner)


def RefusingLedger(db: MemoryTransactor | None = None) -> Any:
    """A joining ledger whose compensating writes are refused, counted in ``refusals``. A joining ledger
    refuses because the handler's own failed statement has already aborted the transaction they share."""
    return Refusing(MemoryIdempotencyStore(db), "complete", "release")


# =========================================================================
# Redis: a Lua-capable fake for the fast suite, and a live server when one is named
# =========================================================================

TTL_MS = 60_000

#: A live Redis to run the Redis suites against as well, as a URL (``redis://localhost:56380/0``). Unset, the
#: suites run against fakeredis only.
REDIS_URL: str | None = os.environ.get("EDAX_REDIS_URL")


@pytest.fixture(params=["fakeredis", "live"] if REDIS_URL else ["fakeredis"])
async def redis_client(request: pytest.FixtureRequest) -> AsyncIterator[Any]:
    """A ``redis.asyncio`` client. ``fakeredis`` runs the scripts in an embedded Lua interpreter (lupa), so
    every script the store sends runs the kernel's own bytes: the fake never re-implements the fence. With
    ``EDAX_REDIS_URL`` set the same tests also run against that server. Tests share no keys, so the live
    database is never flushed."""
    if request.param == "fakeredis":
        client: Any = fakeredis.FakeAsyncRedis()
    else:
        assert REDIS_URL is not None, "the live parameter exists only with a URL"
        client = redis.asyncio.Redis.from_url(REDIS_URL)
    try:
        yield client
    finally:
        await client.aclose()


# =========================================================================
# Model-test helpers: envelope comparison, expectation wrapping, a Counter aggregate
# =========================================================================


@contextmanager
def recorded_backoff() -> Iterator[list[int]]:
    """Record every backoff wait as the 0-based step it was asked for, instead of paying it.

    Every nack and every retry waits one step of the kernel's jittered schedule, so a test walking three
    redeliveries would otherwise spend real time proving arithmetic, and a Hypothesis run spends minutes.
    Patches :meth:`edax.Pacing.wait`, so a dispatcher or a ``retry`` built anywhere — inside a consumer
    included — records too; what one wait costs is ``tests/test_pacing.py``'s business. A context manager
    rather than only a fixture: Hypothesis rejects a function-scoped fixture on a generated test.
    """
    steps: list[int] = []

    async def record(self: edax.Pacing, attempt: int) -> None:
        steps.append(attempt)

    with mock.patch.object(edax.Pacing, "wait", record):
        yield steps


@pytest.fixture
def slept() -> Iterator[list[int]]:
    """:func:`recorded_backoff` as a fixture, for the tests that do not generate their own cases."""
    with recorded_backoff() as waits:
        yield waits


def expect(exc: type[BaseException] | None, match: str | None = None) -> AbstractContextManager[Any]:
    """``with expect(StoreError):`` or ``with expect(None):``, so a replay makes one seam call
    whatever the model says the outcome is, instead of one ``pytest.raises`` block per branch."""
    return pytest.raises(exc, match=match) if exc is not None else nullcontext()


def fields(env: edax.EventEnvelope) -> tuple[Any, ...]:
    """Every field of a native envelope as a tuple, so a failed comparison names the field that differs
    (the envelope itself has value equality, see ``aggregate.py``)."""
    return (
        env.id,
        env.aggregate_id,
        env.event.name,
        env.event.version,
        bytes(env.event.data),
        env.sequence,
        dict(env.namespace),
        dict(env.metadata),
        env.timestamp,
    )


def same(got: Sequence[edax.EventEnvelope], want: Sequence[edax.EventEnvelope]) -> None:
    """Full envelope equality, not just ids: namespace/metadata/timestamp fidelity."""
    assert [fields(e) for e in got] == [fields(e) for e in want]


def stamp(variant: int) -> tuple[dict[str, str], dict[str, str], int]:
    """Envelope decoration varied per append, so the comparison covers the fields a store could
    silently drop rather than just ids and sequences."""
    if variant % 3 == 0:
        return {}, {}, 0
    if variant % 3 == 1:
        return {"tenant": "t1"}, {"trace": "abc"}, 1000
    # non-ASCII: the field has to survive the binding round-trip byte for byte
    return {"tenant": "t2", "region": "eu"}, {"trace": "déjà-vu"}, 2000


def agg_id(agg: int) -> str:
    return f"agg-{agg}"


@dataclass
class Tick:
    v: int


class Counter(edax.Aggregate):
    """The smallest aggregate: ``n`` is the sum of every ``Tick.v`` applied."""

    def __init__(self, id: str, initial: int = 0) -> None:
        super().__init__(id, initial)
        self.n = 0

    def transition(self, event: Any) -> None:
        self.n += event.v


class TickEvents:
    def marshal(self, event: Any) -> edax.Event:
        return edax.Event("Tick", 1, str(event.v).encode())

    def unmarshal(self, event: edax.Event) -> Any:
        return Tick(int(bytes(event.data).decode()))


class CounterCodec:
    type = "Counter"

    def load(self, id: str, version: int, data: bytes | None) -> Counter:
        c = Counter(id, version)
        if data is not None:
            c.n = int(data.decode())
        return c

    def dump(self, agg: Counter) -> bytes:
        return str(agg.n).encode()


def counter_repo(store: Any, ids: Iterator[int]) -> edax.Repository[Counter]:
    """A Repository over ``store`` with a deterministic id sequence and clock. The codec fixes the
    aggregate type, so ``repo.load(...)`` is a :class:`Counter`."""
    return edax.Repository(store, TickEvents(), CounterCodec(), new_id=lambda: f"e{next(ids)}", now=lambda: 0)


# =========================================================================
# Broker fakes: producer + consumer client in one object, no server
# =========================================================================

KafkaMsg = namedtuple("KafkaMsg", "topic partition offset key value headers")


class FakeKafkaCluster:
    """What a Kafka broker holds for one consumer group: topics of partitioned, append-only logs and the
    group's committed offsets. :class:`FakeKafkaClient` members attach to it. It models the behaviour the
    adapter must survive, which a queue-shaped fake hides:

    * nothing redelivers unless a member seeks back, a member joins or leaves (a rebalance), or a member
      restarts from the commits;
    * a rebalance is eager, the way aiokafka's is: every member's listener hears ``on_partitions_revoked``
      for all it held and is awaited before the new assignment, and a member fetches nothing meanwhile;
    * a newly assigned partition is fetched from the group's commit, and a commit overwrites — it can move
      backwards, so a regression is visible in :attr:`commit_log`;
    * ``pause_commits`` parks every commit until ``resume_commits``: a broker that stopped answering."""

    def __init__(self, partitions: int = 1, *, _peers: list["FakeKafkaCluster"] | None = None) -> None:
        self.partitions = partitions
        # Every group of one broker shares the logs; each group has its own commits and members.
        self._peers: list[FakeKafkaCluster] = _peers if _peers is not None else []
        self._peers.append(self)
        self.logs: dict[tuple[str, int], list[KafkaMsg]] = (
            self._peers[0].logs if len(self._peers) > 1 else defaultdict(list)
        )
        self.committed: dict[TopicPartition, int] = {}
        #: Every commit the group accepted, in order: ``(member, TopicPartition, offset)``.
        self.commit_log: list[tuple[int, TopicPartition, int]] = []
        self.members: list[FakeKafkaClient] = []
        self._commits_open = asyncio.Event()
        self._commits_open.set()

    def append(self, topic: str, partition: int, key: bytes | None, value: bytes | None, headers: Any) -> int:
        log = self.logs[(topic, partition)]
        log.append(KafkaMsg(topic, partition, len(log), key, value, list(headers)))
        self.wake()
        return len(log) - 1

    def topic_partitions(self) -> list[TopicPartition]:
        topics = sorted({topic for topic, _ in self.logs} | {"t"})
        return [TopicPartition(t, p) for t in topics for p in range(self.partitions)]

    def group(self) -> "FakeKafkaCluster":
        """Another consumer group of the same broker: the same logs, commits and members of its own."""
        return FakeKafkaCluster(self.partitions, _peers=self._peers)

    def wake(self) -> None:
        for peer in self._peers:
            for member in peer.members:
                member._changed.set()

    def pause_commits(self) -> None:
        self._commits_open.clear()

    def resume_commits(self) -> None:
        self._commits_open.set()

    async def rebalance(self) -> None:
        """Revoke everything from every member (awaiting each listener), then spread the partitions over
        the members in join order and announce the new assignment."""
        for member in self.members:
            member._rebalancing = True
        # The revoked partitions are still held while the listener runs, so it can commit them.
        for member in self.members:
            if member.listener is not None:
                await member.listener.on_partitions_revoked(set(member.assignment))
            member.assignment = set()
        tps = self.topic_partitions()
        for i, member in enumerate(self.members):
            member.assignment = {tp for j, tp in enumerate(tps) if j % len(self.members) == i}
            member.position = {tp: self.committed.get(tp, 0) for tp in member.assignment}
        for member in self.members:
            if member.listener is not None:
                await member.listener.on_partitions_assigned(set(member.assignment))
            member._rebalancing = False
        self.wake()

    async def join(self, member: "FakeKafkaClient") -> None:
        self.members.append(member)
        await self.rebalance()

    async def leave(self, member: "FakeKafkaClient", *, crash: bool = False) -> None:
        """A member leaves. A clean leave hears its revoke; a crashed one hears nothing and keeps nothing."""
        self.members.remove(member)
        if crash:
            member.assignment = set()
            member._stopped = True
            member._changed.set()
        else:
            if member.listener is not None:
                await member.listener.on_partitions_revoked(set(member.assignment))
            member.assignment = set()
        await self.rebalance()


class FakeKafkaClient:
    """One ``AIOKafkaConsumer`` member and an ``AIOKafkaProducer`` in one object, over a
    :class:`FakeKafkaCluster` (its own one-partition cluster unless given one). A client built without a
    cluster owns every partition at once, with no group protocol, which is what most tests want.

    ``getone`` reads the assigned partitions round robin, parks when caught up, and wakes on an append, a
    seek, a rebalance or a stop. ``seek`` and ``commit`` refuse a partition this member does not hold, as
    aiokafka does. ``stop`` makes ``getone`` raise ``ConsumerStoppedError``. The single-partition views
    (``log``, ``cursor``, ``commits``, ``seeks``) read topic ``t``, partition 0."""

    def __init__(
        self,
        cluster: FakeKafkaCluster | None = None,
        *,
        group_id: str | None = "g",
        enable_auto_commit: bool = False,
        acks: int = -1,
    ) -> None:
        standalone = cluster is None
        self.cluster = cluster if cluster is not None else FakeKafkaCluster()
        # The private attributes the adapter probes, under aiokafka's own names.
        self._group_id = group_id
        self._enable_auto_commit = enable_auto_commit
        self._sender = types.SimpleNamespace(_acks=acks)
        self.listener: Any = None
        self.assignment: set[TopicPartition] = set()
        self.position: dict[TopicPartition, int] = {}
        self.seek_log: list[tuple[TopicPartition, int]] = []
        self._standalone = standalone
        self._rebalancing = False
        self._stopped = False
        self._changed = asyncio.Event()
        self._next = 0
        if standalone:
            self.cluster.members.append(self)

    # --- the single-partition views most tests read ---------------------------------------------

    @property
    def log(self) -> list[KafkaMsg]:
        return self.cluster.logs[("t", 0)]

    @property
    def cursor(self) -> int:
        return self._position(TopicPartition("t", 0))

    @property
    def commits(self) -> list[int]:
        return [offset for _, tp, offset in self.cluster.commit_log if tp == TopicPartition("t", 0)]

    @property
    def seeks(self) -> list[int]:
        return [offset for tp, offset in self.seek_log if tp == TopicPartition("t", 0)]

    def settled(self) -> bool:
        """Committed watermark is past the whole log: nothing would redeliver on restart."""
        return self.cluster.committed.get(TopicPartition("t", 0)) == len(self.log)

    # --- the consumer -----------------------------------------------------------------------------

    def subscribe(self, topics: Sequence[str], listener: Any = None) -> None:
        self.listener = listener

    def _held(self) -> list[TopicPartition]:
        return self.cluster.topic_partitions() if self._standalone else sorted(self.assignment)

    def _position(self, tp: TopicPartition) -> int:
        return self.position.setdefault(tp, self.cluster.committed.get(tp, 0))

    def _refuse_unassigned(self, tp: TopicPartition) -> None:
        if not self._standalone and tp not in self.assignment:
            raise aiokafka.errors.IllegalStateError(f"No current assignment for partition {tp}")

    async def getone(self) -> KafkaMsg:
        # A real fetch suspends for a round trip; without this yield a consumer that seeks and polls in a
        # loop would never hand the event loop back, which hides a hot loop instead of showing it.
        await asyncio.sleep(0)
        while True:
            self._changed.clear()
            if self._stopped:
                raise aiokafka.errors.ConsumerStoppedError()
            held = [] if self._rebalancing else self._held()
            for step in range(len(held)):
                tp = held[(self._next + step) % len(held)]
                log, at = self.cluster.logs[(tp.topic, tp.partition)], self._position(tp)
                if at < len(log):
                    self.position[tp] = at + 1
                    self._next = (self._next + step + 1) % len(held)
                    return log[at]
            await self._changed.wait()

    def seek(self, tp: TopicPartition, offset: int) -> None:
        self._refuse_unassigned(tp)
        self.seek_log.append((tp, offset))
        self.position[tp] = offset
        self._changed.set()

    async def commit(self, offsets: dict[TopicPartition, int]) -> None:
        if self._group_id is None:
            raise aiokafka.errors.IllegalOperation("Requires group_id")
        for tp in offsets:
            self._refuse_unassigned(tp)
        await self.cluster._commits_open.wait()
        for tp, offset in offsets.items():
            self.cluster.committed[tp] = offset
            self.cluster.commit_log.append((id(self), tp, offset))

    async def stop(self) -> None:
        self._stopped = True
        self._changed.set()

    # --- the producer -----------------------------------------------------------------------------

    async def partitions_for(self, topic: str) -> set[int]:
        return set(range(self.cluster.partitions))

    async def send_and_wait(
        self,
        topic: str,
        value: bytes | None,
        key: bytes | None = b"",
        partition: int | None = None,
        headers: Sequence[tuple[str, bytes | None]] = (),
    ) -> None:
        part = 0 if partition is None else partition
        assert 0 <= part < self.cluster.partitions, f"no partition {part}"
        self.cluster.append(topic, part, key, value, headers)


class IdSerializer:
    """A custom serializer whose wire form is the envelope id, under a media type of its own, so a
    record it wrote routes back to it through the consumer's registry."""

    media_type = "application/x-test-id"

    def encode(self, env: edax.EventEnvelope) -> bytes:
        return env.id.encode()

    def decode(self, wire: bytes) -> edax.EventEnvelope:
        return build_envelope("a", "Tick", 1, id=wire.decode())


async def kafka_consumer(n: int) -> tuple[FakeKafkaClient, KafkaConsumer]:
    """A fake client holding ``n`` messages (bodies ``"0"``..) and a :class:`KafkaConsumer` over it.
    Published through a real :class:`KafkaProducer`, so every record carries the kernel's key and
    headers rather than a bare value the consumer would have to guess at."""
    c = FakeKafkaClient()
    producer = KafkaProducer(c, "t", serializer=IdSerializer())
    for i in range(n):
        await producer.publish(build_envelope("a", "Tick", 1, id=str(i)))
    return c, KafkaConsumer(c, serializer=IdSerializer())

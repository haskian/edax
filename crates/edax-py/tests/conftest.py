"""Shared test fixtures and fault-injection fakes.

The ``Flaky*`` stores model one specific real failure: a write **lands** and then its response is
**lost** (a dropped ack, a killed connection between commit and reply). ``arm_lost_response`` names
the seam method to lose (``"append"``, ``"complete"``, ...); the next call to that method forwards
to the real store, commits, and *then* raises :class:`edax.StoreError`. It fires once, then clears.
``"commit"`` loses the response of a whole unit of work: the transaction commits and the caller
sees the error afterwards, which is the only place a *transactional* write can land and still fail.
"""

import asyncio
import sys
import types
from collections import namedtuple
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import AbstractContextManager, asynccontextmanager, contextmanager, nullcontext
from dataclasses import dataclass
from typing import Any, Optional
from unittest import mock

import edax
import pytest
from edax.backend.memory import MemoryEventStore, MemoryProjectionStore, MemorySagaStore, MemoryTx
from edax.command import Record
from edax.projection import Entity
from edax.store import Tx


def delivered(delivery: Optional[edax.Delivery]) -> edax.Delivery:
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


def build_envelope(
    aggregate_id: str,
    name: str,
    sequence: Optional[int],
    *,
    id: Optional[str] = None,
    version: int = 1,
    data: bytes = b"",
    namespace: Optional[dict[str, str]] = None,
    metadata: Optional[dict[str, str]] = None,
    timestamp: int = 0,
) -> edax.EventEnvelope:
    """A native :class:`edax.EventEnvelope` with sensible defaults; ``id`` defaults to a stable
    ``{aggregate_id}-{name}-{sequence}``."""
    return edax.Envelope(
        id=id or f"{aggregate_id}-{name}-{sequence}",
        aggregate_id=aggregate_id,
        event=edax.Event(name, version, data),
        namespace=namespace or {},
        metadata=metadata or {},
        timestamp=timestamp,
        sequence=sequence,
    )


# =========================================================================
# Fault injection: land the write, then lose the response
# =========================================================================


class _LostResponse:
    """Mixin: arm one seam method to land its write and then raise ``StoreError`` (a lost response)."""

    arm_lost_response: Optional[str] = None

    def _trip(self, op: str) -> None:
        if self.arm_lost_response == op:
            self.arm_lost_response = None  # one-shot
            raise edax.StoreError("response lost after write")


class FlakyEventStore(_LostResponse, MemoryEventStore):
    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[MemoryTx]:
        async with super().transaction() as tx:
            yield tx
        self._trip("commit")  # committed; the response is lost on the way back

    async def append(
        self, tx: Tx, aggregate_id: str, expected_version: int, events: Sequence[edax.EventEnvelope]
    ) -> None:
        await super().append(tx, aggregate_id, expected_version, events)
        self._trip("append")

    async def mark_published(self, ids: Sequence[str]) -> None:
        await super().mark_published(ids)
        self._trip("mark_published")


class FlakyProjectionStore(_LostResponse, MemoryProjectionStore):
    async def save(self, entity: Entity, tx: Optional[Tx] = None) -> None:
        before = entity.version
        await super().save(entity, tx)  # advances entity.version in place
        if self.arm_lost_response == "save":
            entity.version = before  # the caller never saw the new version
        self._trip("save")


class FlakyIdempotency(_LostResponse):
    """Seam wrapper over *any* ``IdempotencyStore`` (memory or redis), so both backends lose a
    response identically: the trip fires after the inner store has committed."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    async def claim(self, key: str, payload_hash: int, tx: Optional[Tx] = None) -> Optional[Record]:
        prior = await self._inner.claim(key, payload_hash, tx)
        self._trip("claim")
        return prior

    async def complete(self, key: str, record: Record, tx: Optional[Tx] = None) -> None:
        await self._inner.complete(key, record, tx)
        self._trip("complete")

    async def release(self, key: str, tx: Optional[Tx] = None) -> None:
        await self._inner.release(key, tx)
        self._trip("release")


class BrokenCommitTransactor:
    """A :class:`~edax.store.Transactor` whose commit fails, in the two ways that differ.

    ``"nothing_landed"`` rolls the unit of work back and raises: the classic failed commit.
    ``"lost_response"`` commits and *then* raises — the same shape ``FlakyEventStore("commit")``
    models. Both are needed and they assert opposite things: the first lets the redelivery re-run
    once, the second releases a claim over work that **did** land, so the redelivery runs it twice.
    That second case is at-least-once, and it is the irreducible price of a ledger that cannot join
    the transaction."""

    def __init__(self, inner: Any, arm: Optional[str] = "nothing_landed") -> None:
        self._inner = inner
        self.arm = arm  # settable per unit of work, so one rig can drive a whole history

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Any]:
        arm = self.arm
        if arm == "nothing_landed":
            async with self._inner.transaction() as tx:
                yield tx
                # Raised inside the inner block, so the inner transactor rolls back before this
                # propagates: the caller sees a failed commit over which nothing landed.
                raise edax.StoreError("commit failed; nothing landed")
        elif arm == "lost_response":
            async with self._inner.transaction() as tx:
                yield tx
            raise edax.StoreError("commit landed; the response was lost")
        else:
            async with self._inner.transaction() as tx:
                yield tx


class SplitLedger:
    """An ``IdempotencyStore`` on a database of its own: it accepts ``tx`` and ignores it, exactly
    as :class:`~edax.backend.redis.RedisIdempotencyStore` does, because Redis cannot enlist in
    another store's transaction. ``joins_transaction`` stays at its ``False`` default, so the guard
    compensates by hand instead of relying on a rollback."""

    def __init__(self) -> None:
        from edax.backend.memory import MemoryIdempotencyStore, MemoryTransactor

        self._db = MemoryTransactor()
        self._inner = MemoryIdempotencyStore(self._db)

    async def claim(self, key: str, payload_hash: int, tx: Optional[Tx] = None) -> Optional[Record]:
        return await self._inner.claim(key, payload_hash, tx=self._db.auto_commit())

    async def complete(self, key: str, record: Record, tx: Optional[Tx] = None) -> None:
        await self._inner.complete(key, record, tx=self._db.auto_commit())

    async def release(self, key: str, tx: Optional[Tx] = None) -> None:
        await self._inner.release(key, tx=self._db.auto_commit())


class FlakySagaStore(_LostResponse, MemorySagaStore):
    async def save(self, id: str, state: Any, version: int, tx: Optional[Tx] = None) -> int:
        v = await super().save(id, state, version, tx)
        self._trip("save")
        return v

    async def delete(self, id: str, version: int, tx: Optional[Tx] = None) -> None:
        await super().delete(id, version, tx)
        self._trip("delete")


# =========================================================================
# In-memory Redis stand-in (shared by test_redis and the P2 redis param)
# =========================================================================

TTL_MS = 60_000


class FakeRedis:
    """In-memory stand-in for Redis: enough to exercise claim/replay, plus the two faults a real
    server has and a dict cannot fake by itself, a transport error and key expiry. ``now_ms`` is
    the clock TTLs are measured against, so a test advances time instead of sleeping."""

    def __init__(self) -> None:
        self.map: dict[str, tuple[bytes, int]] = {}  # key -> (value, absolute expiry in ms)
        self.fail = False  # every command raises a transport error
        self.vanish_once = False  # the next SET NX loses but leaves no key (TTL/DEL race)
        self.now_ms = 0
        self.set_nx_calls = 0

    def _live(self, key: str) -> Optional[bytes]:
        """The stored value if the key exists and has not expired: Redis's own view of a key."""
        entry = self.map.get(key)
        return entry[0] if entry is not None and entry[1] > self.now_ms else None

    def _check(self) -> None:
        if self.fail:
            raise RuntimeError("connection reset")

    async def get(self, key: str) -> Optional[bytes]:
        self._check()
        return self._live(key)

    async def set_px(self, key: str, value: bytes, ttl_ms: int) -> None:
        self._check()
        self.map[key] = (value, self.now_ms + ttl_ms)

    async def set_nx_px(self, key: str, value: bytes, ttl_ms: int) -> bool:
        self._check()
        self.set_nx_calls += 1
        if self.vanish_once:
            self.vanish_once = False
            return False  # lose the claim, but the key is gone by the time the caller GETs it
        if self._live(key) is not None:
            return False
        self.map[key] = (value, self.now_ms + ttl_ms)
        return True

    async def delete(self, key: str) -> None:
        self._check()
        self.map.pop(key, None)

    # The fence. Compare-and-write in one step, as the Lua in `AsyncRedisClient` does: a read
    # followed by a write here would hide the race these exist to close.
    async def set_px_if_prefix(self, key: str, prefix: bytes, value: bytes, ttl_ms: int) -> bool:
        self._check()
        current = self._live(key)
        if current is None or not current.startswith(prefix):
            return False
        self.map[key] = (value, self.now_ms + ttl_ms)
        return True

    async def delete_if_prefix(self, key: str, prefix: bytes) -> bool:
        self._check()
        current = self._live(key)
        if current is None or not current.startswith(prefix):
            return False
        self.map.pop(key, None)
        return True


# =========================================================================
# Model-test helpers: envelope comparison, expectation wrapping, a Counter aggregate
# =========================================================================


@contextmanager
def recorded_backoff() -> Iterator[list[float]]:
    """Record the dispatcher's backoff waits instead of paying them, yielding the seconds asked for.

    Every nack waits one step of the kernel's schedule, so a test walking three redeliveries would
    otherwise spend 700 ms proving arithmetic, and a Hypothesis run spends minutes. A context manager
    rather than only a fixture: Hypothesis rejects a function-scoped fixture on a generated test.
    """
    waits: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        waits.append(seconds)

    with mock.patch.object(edax.dispatcher, "_sleep", fake_sleep):
        yield waits


@pytest.fixture
def slept() -> Iterator[list[float]]:
    """:func:`recorded_backoff` as a fixture, for the tests that do not generate their own cases."""
    with recorded_backoff() as waits:
        yield waits


def expect(exc: Optional[type[BaseException]], match: Optional[str] = None) -> AbstractContextManager[Any]:
    """``with expect(StoreError):`` or ``with expect(None):``, so a replay makes one seam call
    whatever the model says the outcome is, instead of one ``pytest.raises`` block per branch."""
    return pytest.raises(exc, match=match) if exc is not None else nullcontext()


def fields(env: edax.EventEnvelope) -> tuple[Any, ...]:
    """Every field of a native envelope as a tuple. The native class has no ``__eq__`` (a type slot
    the binding can't add, see ``aggregate.py``), so equality is compared field by field."""
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

    def load(self, id: str, version: int, data: Optional[bytes]) -> Counter:
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

# The Kafka adapter imports TopicPartition lazily inside poll(); no broker package is installed here.
TopicPartition = namedtuple("TopicPartition", "topic partition")
_aiokafka = types.ModuleType("aiokafka")  # a real module: sys.modules holds modules, not namespaces
_aiokafka.__dict__["TopicPartition"] = TopicPartition  # a module's __dict__ is its namespace
sys.modules.setdefault("aiokafka", _aiokafka)

KafkaMsg = namedtuple("KafkaMsg", "topic partition offset value")


class FakeKafkaClient:
    """One partition of one topic: an append-only log with a fetch cursor. ``send_and_wait`` appends
    at the next offset, ``getone`` reads at the cursor (blocks at the end, like a real consumer),
    ``seek`` moves the cursor, ``commit`` records the watermark."""

    def __init__(self) -> None:
        self.log: list[KafkaMsg] = []
        self.cursor = 0
        self.commits: list[int] = []
        self.seeks: list[int] = []

    async def send_and_wait(self, topic: str, value: bytes, key: bytes = b"") -> None:
        self.log.append(KafkaMsg(topic, 0, len(self.log), value))

    async def getone(self) -> KafkaMsg:
        if self.cursor == len(self.log):
            await asyncio.Event().wait()  # end of log: block forever, tests never poll past it
        msg = self.log[self.cursor]
        self.cursor += 1
        return msg

    async def commit(self, offsets: dict[Any, int]) -> None:
        self.commits.extend(offsets.values())

    def seek(self, tp: Any, offset: int) -> None:
        self.seeks.append(offset)
        self.cursor = offset

    def settled(self) -> bool:
        """Committed watermark is past the whole log: nothing would redeliver on restart."""
        return bool(self.commits) and self.commits[-1] == len(self.log)

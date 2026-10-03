"""Model-based tests for the Python SDK — the mirror of ``crates/edax/tests/model.rs``.

Each property replays a random operation history against the shipped in-memory stores and compares
every observable against a trivial reference model (a ``list``/``dict`` that obviously has the
intended semantics). Hypothesis shrinks a failure to a minimal history and prints it.

Each replay is split three ways: a pure ``*Model`` (the oracle, no store calls), a ``*Replay`` with
one method per op that makes **one** seam call under :func:`conftest.expect` and then checks the
store against the model, and a ``finish`` for end-of-history invariants. Fault flags on an op only
change what the model says the outcome is; they never add a second code path through the seam.

Hypothesis cannot drive ``async def`` tests, so every property is a sync test whose body calls
``asyncio.run`` on the replay coroutine. The native stores are ``unsendable``; everything here stays
on the pytest thread, so that holds.

Reproduce a reported failure with the printed example, or re-run with
``--hypothesis-seed=<seed>``; falsifying examples are cached under ``.hypothesis/`` (gitignored).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import edax
import fakeredis
import pytest
from conftest import (
    NO_WAIT,
    TTL_MS,
    Counter,
    Tick,
    agg_id,
    build_envelope,
    counter_repo,
    expect,
    recorded_backoff,
    same,
    sequence_of,
    stamp,
)
from edax import command as C
from edax.aggregate import WireFormat
from edax.backend.memory import MemoryEventStore, MemoryIdempotencyStore, MemoryProjectionStore
from edax.backend.redis import AsyncRedisClient, RedisIdempotencyStore
from edax.testing.faults import LostResponse
from hypothesis import given, settings
from hypothesis import strategies as st

# Native calls plus an `asyncio.run` per example blow past hypothesis' 200 ms default deadline on a
# cold run; the properties are deterministic, so the deadline buys nothing here.
MODEL = settings(max_examples=200, deadline=None)


# "Lost response" faults are rare on purpose: histories stay mostly clean so the happy paths still
# get exercised. `lost` (bool) is true ~1 in 5; `lost_kind` loses the claim (1) or the outcome (2).
lost_flag = st.integers(0, 4).map(lambda n: n == 0)
lost_kind = st.sampled_from([0, 0, 0, 0, 1, 2])


# =========================================================================
# P1 — event store + outbox relay
# =========================================================================


@dataclass
class Append:
    agg: int
    delta: int
    n: int
    ns: int
    lost: bool  # the append lands, then its response is lost


@dataclass
class Load:
    agg: int
    after: int


@dataclass
class Drain:
    limit: int
    fail_at: int | None
    lost_mark: bool  # mark_published lands, then its response is lost


StoreOp = Append | Load | Drain

store_op = st.one_of(
    st.builds(Append, st.integers(0, 2), st.integers(-2, 2), st.integers(0, 3), st.integers(0, 2), lost_flag),
    st.builds(Load, st.integers(0, 2), st.integers(0, 8)),
    st.builds(Drain, st.integers(0, 6), st.none() | st.integers(0, 5), lost_flag),
)


@dataclass
class OutboxModel:
    """Per-aggregate streams plus a FIFO outbox drained by prefixes. Pure: no store calls."""

    events: dict[int, list[edax.EventEnvelope]] = field(default_factory=dict)
    outbox: list[edax.EventEnvelope] = field(default_factory=list)
    published: list[str] = field(default_factory=list)
    failed_drains: int = 0
    next_id: int = 0

    def version(self, agg: int) -> int:
        return len(self.events.get(agg, []))

    def stream(self, agg: int, after: int = 0) -> list[edax.EventEnvelope]:
        return [e for e in self.events.get(agg, []) if (e.sequence or 0) > after]

    def envelopes(self, op: Append, expected: int) -> list[edax.EventEnvelope]:
        namespace, metadata, timestamp = stamp(op.ns)
        envs: list[edax.EventEnvelope] = []
        for i in range(op.n):
            self.next_id += 1
            envs.append(
                build_envelope(
                    agg_id(op.agg),
                    "Ticked",
                    expected + 1 + i,
                    id=f"e{self.next_id}",
                    data=bytes([op.agg, i]),
                    namespace=namespace,
                    metadata=metadata,
                    timestamp=timestamp,
                )
            )
        return envs

    def appended(self, agg: int, envs: list[edax.EventEnvelope]) -> None:
        self.events.setdefault(agg, []).extend(envs)
        self.outbox.extend(envs)

    def drained(self, n: int) -> None:
        self.outbox = self.outbox[n:]

    @property
    def all_ids(self) -> list[str]:
        # Ids come from a monotonic counter, so sorting by it *is* global append order.
        return sorted((e.id for es in self.events.values() for e in es), key=lambda i: int(i[1:]))


class Sink:
    """Publisher fake: records what it saw, fails at index ``fail_at``."""

    def __init__(self) -> None:
        self.reset()

    def reset(self, fail_at: int | None = None) -> None:
        self.seen: list[edax.EventEnvelope] = []
        self.fail_at = fail_at

    async def publish(self, env: edax.EventEnvelope) -> None:
        self.seen.append(env)
        if self.fail_at == len(self.seen) - 1:
            raise edax.StoreError("publisher unavailable")


class StoreReplay:
    def __init__(self, store: Any) -> None:  # a LostResponse over the memory event store
        self.store = store
        self.model = OutboxModel()
        self.sink = Sink()
        self.relay = edax.Relay(store, self.sink.publish, pacing=NO_WAIT)
        self.tx: Any = None

    async def step(self, op: StoreOp) -> None:
        # An if-chain, not a `{type: method}` table: each branch narrows the union to the one
        # variant its method takes, so a method wired to the wrong op is a type error here.
        if isinstance(op, Append):
            await self.append(op)
        elif isinstance(op, Load):
            await self.load(op)
        else:
            await self.drain(op)

    async def append(self, op: Append) -> None:
        stored = self.model.version(op.agg)
        # Signed delta off the model's own version, so conflicts and clean appends are both common
        # (a uniform `expected` would almost always conflict).
        expected = max(0, stored + op.delta)
        envs = self.model.envelopes(op, expected)
        clean = expected == stored
        if op.lost:
            self.store.arm("append")
        # Clean: the write lands (the caller only sees a StoreError if its response is lost).
        # Conflict: rejected before anything is written, whatever the arm says.
        exc = (edax.StoreError if op.lost else None) if clean else edax.ConcurrencyError
        with expect(exc):
            await self.store.append(self.tx, agg_id(op.agg), expected, envs)
        if clean:
            self.model.appended(op.agg, envs)
        elif op.lost:
            # The trip never fired, so the arm survives: a lost write is never mistaken for a conflict.
            assert self.store.armed == "append"
            self.store.arm(None)
        await self.load(Load(op.agg, 0))
        stream = self.model.stream(op.agg)
        assert [e.sequence for e in stream] == list(range(1, len(stream) + 1))

    async def load(self, op: Load) -> None:
        got = await self.store.load(self.tx, agg_id(op.agg), op.after)
        same(got, self.model.stream(op.agg, op.after))

    async def drain(self, op: Drain) -> None:
        batch = self.model.outbox[: op.limit]
        failed = op.fail_at is not None and op.fail_at < len(batch)
        lost = op.lost_mark and bool(batch) and not failed
        self.sink.reset(op.fail_at)
        if lost:
            self.store.arm("mark_published")
        with expect(edax.StoreError if failed or lost else None):
            n = await self.relay.drain(op.limit)
        if failed:
            # The published prefix is marked, and the failed row and everything behind it stay for the
            # next drain (at-least-once, never a drop, never out of order).
            assert op.fail_at is not None
            self.model.failed_drains += 1
            same(self.sink.seen, batch[: op.fail_at + 1])
            self.model.drained(op.fail_at)
        else:
            # Marked — even when the mark's response was lost — so the batch never republishes.
            self.model.drained(len(batch))
            same(self.sink.seen, batch)
            if not lost:
                assert n == len(batch)
        same(await self.store.unpublished(1000), self.model.outbox)
        self.model.published.extend(e.id for e in self.sink.seen)

    async def finish(self) -> None:
        # Drain what's left with an infallible publisher: every appended event must have been
        # published at least once, and at most once more per failed drain.
        self.sink.reset()
        await self.relay.drain(1000)
        self.model.published.extend(e.id for e in self.sink.seen)
        assert await self.store.unpublished(1000) == []
        # The outbox is a FIFO drained by prefixes, so first publication follows append order.
        first_seen = list(dict.fromkeys(self.model.published))
        assert first_seen == self.model.all_ids
        for id_ in first_seen:
            assert self.model.published.count(id_) <= 1 + self.model.failed_drains


async def _replay_store(history: list[StoreOp]) -> None:
    # No open unit of work: the model describes committed state, and an outbox drain reads only
    # committed rows, so every op auto-commits (`tx=None` outside any `transaction`).
    r = StoreReplay(LostResponse(MemoryEventStore()))
    for op in history:
        await r.step(op)
    await r.finish()


@MODEL
@given(history=st.lists(store_op, min_size=1, max_size=24))
def test_store_matches_model(history: list[StoreOp]) -> None:
    asyncio.run(_replay_store(history))


# =========================================================================
# P2 — command bus: routing, idempotency claim, replay, pending
# =========================================================================

_TYPES = {0: "A", 1: "B", 2: "Z"}  # "Z" is never registered


def _bus_key(cmd: Any) -> str:
    """Idempotency key off the command's class attribute. ``Any`` because ``idempotent`` types its
    key function over the ``Command`` protocol, which carries only ``type``."""
    return str(cmd.key)


def make_cmd(kind: int, key: int, payload: int, outcome: int) -> Any:
    """A command whose only *instance* attribute is ``p``, so the kernel hashes the payload alone;
    routing, key and outcome ride as class attributes (the SDK hashes ``vars(cmd)``)."""
    # A class per call: class attributes stay out of `vars(cmd)`, so out of the hashed payload.
    cls = type("Cmd", (), {"type": _TYPES[kind], "kind": kind, "key": key, "outcome": outcome})
    cmd: Any = cls()  # built at runtime: its attributes are not statically known
    cmd.p = payload
    return cmd


@dataclass
class Dispatch:
    kind: int
    key: int
    payload: int
    outcome: int  # 0 ok, 1 transient, 2 terminal
    lost: int  # 0 none, 1 claim lands then loses its response, 2 outcome does


@dataclass
class SeedPending:
    key: int
    payload: int


@dataclass
class Release:
    """Release the claim a ``SeedPending`` took on ``key``, by its handle, if that claim still holds it."""

    key: int


@dataclass
class Expire:
    """Every record's TTL runs out, pending claims included: a Redis TTL, or memory's ``expire`` hook."""


BusOp = Dispatch | SeedPending | Release | Expire

bus_op = st.one_of(
    st.builds(
        Dispatch,
        st.integers(0, 2),
        st.integers(0, 2),
        st.integers(0, 2),
        st.integers(0, 2),
        lost_kind,
    ),
    st.builds(SeedPending, st.integers(0, 2), st.integers(0, 2)),
    st.builds(Release, st.integers(0, 2)),
    st.builds(Expire),
)


def _idempotency_backend(backend: str) -> tuple[Any, Callable[[], Awaitable[None]]]:
    """A flaky idempotency store plus how to make every key's TTL run out: ``FLUSHALL`` on the fakeredis
    client, memory's ``expire`` hook per key. Built inside the replay's event loop, which the fake client
    binds to. fakeredis runs the kernel's own Lua; memory runs the kernel's own fence."""
    if backend == "memory":
        memory = MemoryIdempotencyStore()

        async def lapse_memory() -> None:
            for key in range(3):
                memory.expire(str(key))

        return LostResponse(memory), lapse_memory
    client = fakeredis.FakeAsyncRedis()

    async def lapse_redis() -> None:
        await client.flushall()

    # A short claim TTL bounds how long a dead worker blocks a key, a long record TTL is the dedup
    # window. `Expire` stands for both running out; claim lapses mid-handler are the two-worker
    # property's (`test_redis.py`).
    redis = RedisIdempotencyStore(
        AsyncRedisClient(client), prefix="idem:", ttl_ms=TTL_MS, claim_ttl_ms=TTL_MS // 10
    )
    return LostResponse(redis), lapse_redis


class Slot(Enum):
    PENDING = "pending"


# model[key] = (payload hash, slot); slot is Slot.PENDING, ("done", value) or ("failed", message)
Record = tuple[int, Any]
KEEP = object()  # Expectation.slot: leave model[key] as it is


@dataclass
class Expectation:
    """What the model says one dispatch does: what it raises or returns, how many times the handler
    runs, which seam (if any) loses its response, and what the key's record is afterwards."""

    raises: type[BaseException] | None = None
    match: str | None = None
    value: Any = None
    runs: int = 0
    arm: str | None = None
    slot: Any = KEEP  # KEEP, None (record removed) or a Record


def decide(prior: Record | None, op: Dispatch, hash_: int) -> Expectation:
    """The P2 oracle: pure, no awaits. Both stores fence their settle writes on the claim's handle (memory
    and Redis alike): a release deletes nothing once the claim's own ``complete`` has landed."""
    if op.kind == 2:
        # Routing precedes the middleware chain: no claim, no handler, no record.
        return Expectation(raises=C.HandlerNotFound)
    if op.lost == 1:
        # The claim lands (pending on a free key, unchanged on a taken one), then loses its response
        # before the middleware can decide — the handler never runs. A free key stays blocked until
        # Release / TTL Expire.
        slot = KEEP if prior is not None else (hash_, Slot.PENDING)
        return Expectation(raises=edax.StoreError, arm="claim", slot=slot)
    if prior is None:
        # Free key: this dispatch wins the claim and runs the handler exactly once.
        value = op.payload + 10 * op.kind
        done: Record = (hash_, ("done", value))
        failed: Record = (hash_, ("failed", "terminal"))
        if op.lost == 2:
            # The outcome write lands, then loses its response, and the middleware answers with a
            # release. A released claim is gone either way. A `complete` that landed stays: the release
            # carries the claim's handle and the key now holds the settled record, so a redelivery
            # replays it.
            #
            # What the *caller* sees differs by path. After a failed handler the handler's own error
            # survives, because a compensation the store refuses must not replace the verdict `retry`
            # and the dispatcher branch on.
            if op.outcome == 1:
                return Expectation(raises=RuntimeError, match="transient", arm="release", runs=1, slot=None)
            if op.outcome == 2:
                return Expectation(raises=edax.CodecError, arm="complete", runs=1, slot=failed)
            # Success: the store's failure is the only news there is, and the caller must not report
            # a success it never heard back about.
            return Expectation(raises=edax.StoreError, arm="complete", runs=1, slot=done)
        return (
            Expectation(value=value, runs=1, slot=done),
            Expectation(raises=RuntimeError, match="transient", runs=1, slot=None),  # released
            Expectation(raises=edax.CodecError, runs=1, slot=failed),  # cached
        )[op.outcome]
    # Taken key: the kernel decides, and the handler never runs.
    prior_hash, slot = prior
    if prior_hash != hash_:
        return Expectation(raises=edax.PayloadMismatch)
    if slot is Slot.PENDING:
        return Expectation(raises=edax.InProgress)
    if slot[0] == "done":
        return Expectation(value=slot[1])
    # The cached failure replays as a terminal error, so a dispatcher dead-letters it.
    return Expectation(raises=C.CachedFailure, match=slot[1])


class BusReplay:
    def __init__(self, store: Any, lapse: Callable[[], Awaitable[None]]) -> None:
        self.store = store
        self.lapse = lapse
        self.calls = [0, 0]
        self.model: dict[int, Record] = {}
        # The handle of each pending claim a SeedPending took, so a Release can name it.
        self.seeded: dict[int, bytes] = {}
        self.bus = C.CommandBus(C.idempotent(store, _bus_key))
        self.bus.register("A", self.handler)
        self.bus.register("B", self.handler)

    async def handler(self, cmd: Any) -> int:
        self.calls[cmd.kind] += 1
        if cmd.outcome == 0:
            return cmd.p + 10 * cmd.kind
        if cmd.outcome == 1:
            raise RuntimeError("transient")  # not terminal: the claim is released
        raise edax.CodecError("terminal")  # terminal: the failure is cached

    async def step(self, op: BusOp) -> None:
        if isinstance(op, Dispatch):
            await self.dispatch(op)
        elif isinstance(op, SeedPending):
            await self.seed_pending(op)
        elif isinstance(op, Release):
            await self.release(op)
        else:
            await self.expire(op)

    async def dispatch(self, op: Dispatch) -> None:
        cmd = make_cmd(op.kind, op.key, op.payload, op.outcome)
        hash_ = C._payload_hash(cmd)
        prior = self.model.get(op.key)
        e = decide(prior, op, hash_)

        want_calls = list(self.calls)
        if e.runs:
            want_calls[op.kind] += e.runs
        if e.arm:
            self.store.arm(e.arm)
        with expect(e.raises, e.match):
            assert await self.bus.dispatch(cmd) == e.value
        assert self.calls == want_calls
        if e.slot is None:
            self.model.pop(op.key, None)
        elif e.slot is not KEEP:
            self.model[op.key] = e.slot

    async def seed_pending(self, op: SeedPending) -> None:
        # An in-flight winner: claim takes the key if free and leaves any existing record alone.
        cmd = make_cmd(0, op.key, op.payload, 0)
        hash_ = C._payload_hash(cmd)
        claimed = await self.store.claim(str(op.key), hash_)
        if isinstance(claimed, C.Won):
            assert op.key not in self.model
            self.model[op.key] = (hash_, Slot.PENDING)
            self.seeded[op.key] = claimed.handle
        else:
            assert self.model[op.key][0] == claimed.payload_hash

    async def release(self, op: Release) -> None:
        handle = self.seeded.pop(op.key, None)
        if handle is None:
            return  # no claim of the test's own to release: a guard only ever releases its own
        await self.store.release(str(op.key), handle)
        self.model.pop(op.key, None)

    async def expire(self, op: Expire) -> None:
        await self.lapse()  # every TTL runs out
        self.model.clear()
        self.seeded.clear()

    async def finish(self) -> None:
        # Every modeled record is readable back through the seam with the same hash and state.
        for key, (hash_, slot) in self.model.items():
            record = await self.store.claim(str(key), hash_)
            assert not isinstance(record, C.Won), f"key {key} should still hold a record"
            assert record.payload_hash == hash_
            if slot is Slot.PENDING:
                assert record.pending
            elif slot[0] == "done":
                assert record.result == slot[1]
            else:
                assert record.error == slot[1]


async def _replay_bus(history: list[BusOp], backend: str) -> None:
    store, lapse = _idempotency_backend(backend)
    r = BusReplay(store, lapse)
    for op in history:
        await r.step(op)
    await r.finish()


@pytest.mark.parametrize("backend", ["memory", "redis"])
@MODEL
@given(history=st.lists(bus_op, min_size=1, max_size=24))
def test_idempotent_bus_matches_model(backend: str, history: list[BusOp]) -> None:
    asyncio.run(_replay_bus(history, backend))


# =========================================================================
# P3 — dispatcher: per-aggregate FIFO, settlement, dead-letter routing
# =========================================================================


@dataclass
class Submit:
    agg: int
    fail: int  # 0 ok, 1 transient, 2 terminal
    attempt: int  # broker-supplied redelivery count


async def _replay_dispatcher(workers: int, max_attempts: int, with_dead: bool, submits: list[Submit]) -> None:
    handled: list[tuple[str, int]] = []
    acked: list[str] = []
    nacked: list[str] = []
    dead: list[str] = []

    async def handler(env: edax.EventEnvelope) -> None:
        await asyncio.sleep(0)  # yield, so workers actually interleave
        handled.append((env.aggregate_id, sequence_of(env)))
        if env.event.name == "Transient":
            raise ValueError("transient")
        if env.event.name == "Terminal":
            raise edax.CodecError("terminal")

    async def on_dead(delivery: edax.Delivery, err: BaseException) -> None:
        dead.append(delivery.envelope.id)

    dispatcher = edax.KeyOrderedDispatcher(
        workers=workers,
        handler=handler,
        max_attempts=max_attempts,
        on_dead=on_dead if with_dead else None,
    )
    dispatcher.start()

    seq: dict[int, int] = {}
    want_ack: list[str] = []
    want_nack: list[str] = []
    want_dead: list[str] = []
    for op in submits:
        n = seq[op.agg] = seq.get(op.agg, 0) + 1
        name = ("Tick", "Transient", "Terminal")[op.fail]
        env = build_envelope(agg_id(op.agg), name, n, id=f"{agg_id(op.agg)}-{n}")
        if op.fail == 0:
            want_ack.append(env.id)
        else:
            # attempt = max(broker count, in-process counter) + 1; each id is delivered once here,
            # so the in-process counter is always 0.
            attempt = op.attempt + 1
            is_dead = op.fail == 2 or (max_attempts and attempt >= max_attempts)
            if is_dead and with_dead:
                want_dead.append(env.id)
                want_ack.append(env.id)  # dead-lettered messages are acked, not nacked
            else:
                want_nack.append(env.id)
        await dispatcher.submit(
            edax.Delivery(
                env,
                lambda i=env.id: acked.append(i),
                lambda i=env.id: nacked.append(i),
                op.attempt,
            )
        )
    await dispatcher.stop()

    assert len(handled) == len(submits)
    for agg, count in seq.items():
        seqs = [s for a, s in handled if a == agg_id(agg)]
        # Same aggregate -> same worker -> submitted order, whatever the worker count.
        assert seqs == list(range(1, count + 1))
    assert sorted(acked) == sorted(want_ack)
    assert sorted(nacked) == sorted(want_nack)
    assert sorted(dead) == sorted(want_dead)


@MODEL
@given(
    workers=st.integers(1, 4),
    max_attempts=st.integers(0, 3),
    with_dead=st.booleans(),
    submits=st.lists(
        st.builds(Submit, st.integers(0, 3), st.integers(0, 2), st.integers(0, 3)),
        min_size=1,
        max_size=32,
    ),
)
def test_dispatcher_keeps_per_aggregate_fifo(
    workers: int, max_attempts: int, with_dead: bool, submits: list[Submit]
) -> None:
    # The property under test is settlement order, not timing, and every nack waits one step of the
    # kernel's backoff schedule. Paid for real across a hundred examples that is minutes of sleeping;
    # `test_dispatcher.py` asserts the schedule itself.
    with recorded_backoff():
        asyncio.run(_replay_dispatcher(workers, max_attempts, with_dead, submits))


# =========================================================================
# P4 — Repository: save/load against a pure fold
# =========================================================================


@dataclass
class Apply:
    agg: int
    ticks: list[int]
    ns: int
    stale: bool
    lost: bool  # the save's append lands, then its response is lost


@dataclass
class Reload:
    agg: int


RepoOp = Apply | Reload

repo_op = st.one_of(
    st.builds(
        Apply,
        st.integers(0, 2),
        st.lists(st.integers(0, 3), max_size=3),
        st.integers(0, 2),
        st.booleans(),
        lost_flag,
    ),
    st.builds(Reload, st.integers(0, 2)),
)


@dataclass
class FoldModel:
    """The fold input per aggregate (version = len) and the snapshot version at the last
    successful save. Pure."""

    ticks: dict[int, list[int]] = field(default_factory=dict)
    snapped: dict[int, int] = field(default_factory=dict)

    def total(self, agg: int) -> int:
        return sum(self.ticks.get(agg, []))

    def version(self, agg: int) -> int:
        return len(self.ticks.get(agg, []))


class RepoReplay:
    def __init__(self, store: LostResponse) -> None:
        self.store = store
        self.ids = iter(range(1, 1_000_000))
        self.repo = counter_repo(store, self.ids)
        self.model = FoldModel()
        self.live: dict[int, Counter] = {}
        self.tx: Any = None

    async def step(self, op: RepoOp) -> None:
        if isinstance(op, Apply):
            await self.apply(op)
        else:
            await self.reload(op)

    async def instance(self, agg: int) -> Counter:
        if agg not in self.live:
            self.live[agg] = await self.repo.load(agg_id(agg))
        return self.live[agg]

    async def apply(self, op: Apply) -> None:
        ticks = self.model.ticks.setdefault(op.agg, [])
        version = len(ticks)
        namespace, metadata, _ = stamp(op.ns)
        if op.lost and op.ticks:
            await self._lost_save(op, namespace, metadata)
            return

        # A stale save only conflicts if the live save actually bumps the version. Loaded *before*
        # the live instance saves, so it holds the old version.
        stale = op.stale and bool(op.ticks) and version > 0
        stale_agg = await self.repo.load(agg_id(op.agg)) if stale else None

        instance = await self.instance(op.agg)
        instance.apply(*[Tick(t) for t in op.ticks])
        envs = await self.repo.save(instance, namespace=namespace, metadata=metadata)
        assert [e.sequence for e in envs] == list(range(version + 1, version + len(op.ticks) + 1))
        assert all(dict(e.namespace) == namespace and dict(e.metadata) == metadata for e in envs)
        assert instance.pending == []
        assert instance.version.initial == version + len(op.ticks)
        assert instance.version.updates == 0
        ticks.extend(op.ticks)

        # Stamping survives the store, not just the returned envelopes.
        stored = await self.store.load(self.tx, agg_id(op.agg), version)
        assert all(dict(e.namespace) == namespace and dict(e.metadata) == metadata for e in stored)

        if stale_agg is not None:
            await self._stale_save(stale_agg, op, namespace, metadata)
        self.model.snapped[op.agg] = version + len(op.ticks)

    async def _lost_save(self, op: Apply, namespace: dict[str, str], metadata: dict[str, str]) -> None:
        # save appends and snapshots in one unit of work, and the response of its commit is lost:
        # events and snapshot both land, the caller sees a StoreError, and the instance is left
        # untouched for it to retry. (A response lost *inside* the unit of work would roll it back
        # instead, landing nothing: that is the transaction contract, pinned in test_transactor.)
        instance = await self.instance(op.agg)
        instance.apply(*[Tick(t) for t in op.ticks])
        self.store.arm("commit")
        with expect(edax.StoreError):
            await self.repo.save(instance, namespace=namespace, metadata=metadata)
        assert len(instance.pending) == len(op.ticks)
        assert instance.version.updates == len(op.ticks)
        self.model.ticks[op.agg].extend(op.ticks)
        self.model.snapped[op.agg] = len(self.model.ticks[op.agg])

        # Blind retry with the same instance: the events landed, so OCC conflicts.
        with expect(edax.ConcurrencyError):
            await self.repo.save(instance, namespace=namespace, metadata=metadata)
        assert len(instance.pending) == len(op.ticks)  # still untouched

        # Reload heals: replay past the stale (or absent) snapshot reaches the state.
        await self.reload(Reload(op.agg))

    async def _stale_save(
        self, stale_agg: Counter, op: Apply, namespace: dict[str, str], metadata: dict[str, str]
    ) -> None:
        stale_agg.apply(*[Tick(t) for t in op.ticks])
        with expect(edax.ConcurrencyError):
            await self.repo.save(stale_agg, namespace=namespace, metadata=metadata)
        # The failed save leaves the aggregate untouched so the caller can retry.
        assert len(stale_agg.pending) == len(op.ticks)
        assert stale_agg.version.updates == len(op.ticks)

    async def reload(self, op: Reload) -> None:
        want, version = self.model.total(op.agg), self.model.version(op.agg)
        loaded = await self.repo.load(agg_id(op.agg))
        assert loaded.n == want
        assert loaded.version.initial == version

        # The snapshot path must agree with a pure replay: rebuild the same stream in a store that
        # has no snapshot at all and load again.
        fresh = MemoryEventStore()
        events = await self.store.load(self.tx, agg_id(op.agg), 0)
        async with fresh.transaction() as ftx:
            await fresh.append(ftx, agg_id(op.agg), 0, events)
            replayed = await counter_repo(fresh, self.ids).load(agg_id(op.agg), tx=ftx)
        assert replayed.n == want
        assert replayed.version.initial == version

        self.live[op.agg] = loaded

    async def finish(self) -> None:
        # Every save that landed snapshotted in the same unit of work, so the snapshot tracks the
        # stream exactly: it never leads it, and a lost commit response no longer leaves it behind.
        for agg, ticks in self.model.ticks.items():
            snap = await self.store.load_snapshot(self.tx, agg_id(agg))
            if not ticks:
                assert snap is None, "no save carried a version, so none snapshotted"
                continue
            assert snap is not None and snap.version == self.model.snapped[agg] == len(ticks)


async def _replay_repo(history: list[RepoOp]) -> None:
    # No open unit of work, as in `_replay_store`: each save is the repository's own transaction.
    r = RepoReplay(LostResponse(MemoryEventStore()))
    for op in history:
        await r.step(op)
    await r.finish()


@MODEL
@given(history=st.lists(repo_op, min_size=1, max_size=16))
def test_repository_matches_fold(history: list[RepoOp]) -> None:
    asyncio.run(_replay_repo(history))


# =========================================================================
# P5 — kernel envelope serializer round-trip (the binding boundary)
# =========================================================================

text = st.text(max_size=24)
str_map = st.dictionaries(text, text, max_size=3)


@MODEL
@given(
    fmt=st.sampled_from(["json", "msgpack"]),
    id_=text,
    aggregate_id=text,
    name=text,
    version=st.integers(-(2**31), 2**31 - 1),
    data=st.binary(max_size=64),
    namespace=str_map,
    metadata=str_map,
    timestamp=st.integers(-(2**63), 2**63 - 1),
    sequence=st.none() | st.integers(0, 2**63 - 1),
)
def test_kernel_serializer_roundtrip(
    fmt: WireFormat,
    id_: str,
    aggregate_id: str,
    name: str,
    version: int,
    data: bytes,
    namespace: dict[str, str],
    metadata: dict[str, str],
    timestamp: int,
    sequence: int | None,
) -> None:
    """Pins the binding boundary in both formats, including non-ASCII text and arbitrary bytes — the
    ``café`` class of bug, where a host-side encoding choice diverges from the kernel's."""
    env = edax.EventEnvelope(
        id=id_,
        aggregate_id=aggregate_id,
        event=edax.Event(name, version, data),
        namespace=namespace,
        metadata=metadata,
        timestamp=timestamp,
        sequence=sequence,
    )
    codec = edax.KernelSerializer(fmt)
    assert codec.decode(codec.encode(env)) == env


@given(fmt=st.sampled_from(["json", "msgpack"]), wire=st.binary(min_size=1, max_size=32))
@settings(max_examples=100, deadline=None)
def test_kernel_serializer_rejects_garbage(fmt: WireFormat, wire: bytes) -> None:
    # A wire that will not decode fails the same way on every redelivery: the kernel tags it `codec:`
    # and the serializer maps it to the terminal CodecError, so a consumer dead-letters the poison
    # message instead of treating it as a transient RuntimeError.
    try:
        edax.KernelSerializer(fmt).decode(wire)
    except edax.CodecError as e:
        assert e.terminal is True
        assert isinstance(e.__cause__, RuntimeError)


# =========================================================================
# P6 — projection store: OCC + paging
# =========================================================================


@dataclass
class Save:
    id: int
    delta: int
    n: int
    lost: bool  # the save lands, then its response is lost


@dataclass
class Get:
    id: int


@dataclass
class Page:
    offset: int
    limit: int | None
    desc: bool


ProjOp = Save | Get | Page

proj_op = st.one_of(
    st.builds(Save, st.integers(0, 5), st.integers(-2, 2), st.integers(0, 9), lost_flag),
    st.builds(Get, st.integers(0, 5)),
    st.builds(Page, st.integers(0, 5), st.none() | st.integers(0, 5), st.booleans()),
)


class ProjectionReplay:
    def __init__(self, store: LostResponse) -> None:
        self.store = store
        self.model: dict[int, tuple[int, int]] = {}  # id -> (version, n)

    async def step(self, op: ProjOp) -> None:
        if isinstance(op, Save):
            await self.save(op)
        elif isinstance(op, Get):
            await self.get(op)
        else:
            await self.page(op)

    async def save(self, op: Save) -> None:
        stored = self.model.get(op.id, (0, 0))[0]
        expected = max(0, stored + op.delta)
        entity = edax.Entity(f"u{op.id}", expected, {"n": op.n})
        clean = expected == stored
        lost = clean and op.lost
        if lost:
            self.store.arm("save")
        exc = (edax.StoreError if lost else None) if clean else edax.ConcurrencyError
        with expect(exc):
            await self.store.save(entity)
        if clean:
            self.model[op.id] = (stored + 1, op.n)
        # Clean: advanced in place. Lost: the row advanced but the caller's entity.version is rolled
        # back to what it saw (the OCC bump was lost with the response). Conflict: unchanged.
        assert entity.version == (stored + 1 if clean and not lost else expected)
        if lost:
            # Blind retry with the same stale entity: OCC turns it into a conflict, not a dup write.
            with expect(edax.ConcurrencyError):
                await self.store.save(entity)
            assert entity.version == expected

    async def get(self, op: Get) -> None:
        got = await self.store.get(f"u{op.id}")
        if op.id in self.model:
            version, n = self.model[op.id]
            assert got is not None and got.version == version
            assert got.document["n"] == n
        else:
            assert got is None

    async def page(self, op: Page) -> None:
        page = await self.store.get_many(
            sort=[{"path": "id", "desc": op.desc}], offset=op.offset, limit=op.limit
        )
        ids = [f"u{i}" for i in sorted(self.model, reverse=op.desc)]
        want = ids[op.offset :] if op.limit is None else ids[op.offset : op.offset + op.limit]
        assert [d["id"] for d in page.rows] == want


async def _replay_projection(history: list[ProjOp]) -> None:
    r = ProjectionReplay(LostResponse(MemoryProjectionStore()))
    for op in history:
        await r.step(op)


@MODEL
@given(history=st.lists(proj_op, min_size=1, max_size=24))
def test_projection_matches_model(history: list[ProjOp]) -> None:
    asyncio.run(_replay_projection(history))

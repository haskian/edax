"""The memory ledger of its own rehearses what a Redis ledger does, behaviour for behaviour.

Each scenario runs over :class:`~edax.backend.memory.MemoryLedger` and over the Redis store (the kernel's own
Lua on fakeredis, and a live server when ``EDAX_REDIS_URL`` names one), through the guard, and asserts the
same outcome for both: the split ordering (a terminal failure cached, a redelivery replaying it), a claim
lapsing mid-handler and taken over, the stale owner's late ``complete`` refused by the fence and its release
deleting nothing. The joining memory store is the contrast: it is the other ordering, and the scenarios that
differ say so. The contract clauses themselves run over both in ``test_contract_memory.py`` and
``test_redis.py``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, ClassVar

import edax
import pytest
from conftest import TTL_MS, claimed, fresh_prefix, ledger_key
from edax import _edax
from edax.backend.memory import MemoryIdempotencyStore, MemoryLedger, MemoryTransactor
from edax.backend.redis import AsyncRedisClient, RedisIdempotencyStore
from edax.command import CommandBus, Record, Won, idempotent
from hypothesis import given, settings
from hypothesis import strategies as st


@dataclass
class Debit:
    account_id: str
    amount: int
    request_id: str
    type: ClassVar[str] = "Debit"


class InsufficientFunds(Exception):
    """The host's deterministic rejection: terminal, so a split ledger caches it."""

    terminal: ClassVar[bool] = True


@dataclass
class Rig:
    """A ledger under test and how to lapse one of its keys, as a claim TTL running out would."""

    name: str
    store: Any
    lapse: Callable[[str], Awaitable[None]]


@pytest.fixture(params=["memory_ledger", "redis"])
async def rig(request: pytest.FixtureRequest, redis_client: Any) -> AsyncIterator[Rig]:
    if request.param == "memory_ledger":
        ledger = MemoryLedger()

        async def lapse_memory(key: str) -> None:
            assert ledger.expire(key), f"{key} held nothing to lapse"

        yield Rig("memory_ledger", ledger, lapse_memory)
        return
    prefix = fresh_prefix()
    store = RedisIdempotencyStore(AsyncRedisClient(redis_client), prefix, ttl_ms=TTL_MS, claim_ttl_ms=TTL_MS)

    async def lapse_redis(key: str) -> None:
        assert await redis_client.delete(ledger_key(prefix, key)) == 1, f"{key} held nothing to lapse"

    yield Rig("redis", store, lapse_redis)


def debit(request_id: str = "req-1") -> Debit:
    return Debit("acct-1", 500, request_id)


async def test_the_memory_ledger_declares_the_split_ordering() -> None:
    assert MemoryLedger.joins_transaction is False
    assert MemoryIdempotencyStore.joins_transaction is True


async def test_a_terminal_failure_is_cached_and_replayed_on_redelivery(rig: Rig) -> None:
    """The split ordering caches a deterministic rejection, so a redelivery raises the cached failure and
    the handler never runs again: identical on memory and on Redis."""
    runs = 0

    async def handler(cmd: Debit) -> None:
        nonlocal runs
        runs += 1
        raise InsufficientFunds("balance 100, debit 500")

    bus = CommandBus(idempotent(rig.store, unit_of_work=MemoryTransactor(), scope="test"))
    bus.register("Debit", handler)
    with pytest.raises(InsufficientFunds):
        await bus.dispatch(debit(), idempotency_key="req-1")
    with pytest.raises(edax.CachedFailure, match="balance 100, debit 500") as replay:
        await bus.dispatch(debit(), idempotency_key="req-1")
    assert replay.value.original_code == "rejected"
    assert runs == 1, rig.name


async def test_a_terminal_failure_on_the_joining_store_is_not_cached() -> None:
    """The contrast: the joining store's terminal failure rolls back with the work, so the redelivery re-runs.
    This is why a test must run on the ordering it deploys."""
    runs = 0
    db = MemoryTransactor()

    async def handler(cmd: Debit) -> None:
        nonlocal runs
        runs += 1
        raise InsufficientFunds("balance 100, debit 500")

    bus = CommandBus(idempotent(MemoryIdempotencyStore(db), unit_of_work=db, scope="test"))
    bus.register("Debit", handler)
    for _ in range(2):
        with pytest.raises(InsufficientFunds):
            await bus.dispatch(debit(), idempotency_key="req-1")
    assert runs == 2


@pytest.mark.parametrize("finish_order", ["stale_owner_last", "stale_owner_first"])
async def test_a_claim_lapsed_mid_handler_is_taken_over_and_its_stale_owner_settles_nothing(
    rig: Rig, finish_order: str
) -> None:
    """Worker A wins the claim; its claim lapses while its handler runs; worker B redelivers, takes the key
    over and runs. Whichever finishes first, only B's outcome lands: A's late ``complete`` is refused by
    the fence (A's caller sees ``StoreError``) and the release the guard answers it with deletes nothing.
    Both handlers ran: the at-least-once a lapsing claim costs. Barriers force the interleaving."""
    a_running, a_may_finish, a_finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    ran: list[str] = []

    async def handler(cmd: Debit) -> dict[str, Any]:
        worker = "A" if not ran else "B"
        ran.append(worker)
        if worker == "A":
            a_running.set()
            await a_may_finish.wait()
        return {"debited_by": worker}

    bus = CommandBus(idempotent(rig.store, scope="test"))
    bus.register("Debit", handler)

    async def worker_a() -> Any:
        try:
            return await bus.dispatch(debit(), idempotency_key="req-1")
        finally:
            a_finished.set()

    async def worker_b() -> Any:
        await a_running.wait()
        await rig.lapse(claimed("req-1"))  # A's claim TTL runs out mid-handler
        if finish_order == "stale_owner_first":
            a_may_finish.set()
            await a_finished.wait()  # A settles over a key nobody holds now
        result = await bus.dispatch(debit(), idempotency_key="req-1")
        a_may_finish.set()  # stale_owner_last: A finishes only after B settled
        return result

    a_result, b_result = await asyncio.gather(worker_a(), worker_b(), return_exceptions=True)
    assert isinstance(a_result, edax.StoreError), f"{rig.name}: the stale owner's complete was fenced"
    assert b_result == {"debited_by": "B"}, f"{rig.name}: the worker that took the key over settled it"
    assert ran == ["A", "B"], rig.name
    replay = await rig.store.claim(claimed("req-1"), 0)
    assert not isinstance(replay, Won) and replay.result == {"debited_by": "B"}, (
        f"{rig.name}: B's outcome alone"
    )


async def test_a_lost_complete_reply_keeps_the_record_and_the_redelivery_replays_it(rig: Rig) -> None:
    """The complete lands, its reply is lost, the guard releases with the same handle: the fence keeps the
    settled record, so the redelivery replays it rather than running the handler again."""
    from edax.testing.faults import LostResponse

    store: Any = LostResponse(rig.store)
    runs = 0

    async def handler(cmd: Debit) -> dict[str, int]:
        nonlocal runs
        runs += 1
        return {"balance": 600}

    bus = CommandBus(idempotent(store, scope="test"))
    bus.register("Debit", handler)
    store.arm("complete")
    with pytest.raises(edax.StoreError):
        await bus.dispatch(debit(), idempotency_key="req-1")
    assert await bus.dispatch(debit(), idempotency_key="req-1") == {"balance": 600}
    assert runs == 1, rig.name


# =========================================================================
# Two workers, one key, on the memory ledger: the Redis property's invariants, over the seam
# =========================================================================


@dataclass(frozen=True)
class Step:
    worker: int
    op: str  # claim, run, complete, release, lapse
    lost: bool  # the call lands and its reply is lost


steps = st.lists(
    st.builds(
        Step,
        st.integers(0, 1),
        st.sampled_from(["claim", "run", "complete", "release", "lapse"]),
        st.booleans(),
    ),
    min_size=1,
    max_size=30,
)


async def _two_workers(history: list[Step]) -> None:
    """After every step: at most one worker's outcome ever lands; a settled record is never deleted or
    replaced; at most one worker holds the live claim. ``lapse`` is the claim TTL running out on a pending
    claim. A peek reads the key through a rolled-back unit of work, so it changes nothing."""
    from edax.testing.faults import LostResponse

    ledger = MemoryLedger()
    peek_store = MemoryIdempotencyStore(ledger.transactor)
    held: dict[int, bytes] = {}
    ran: set[int] = set()
    landed: set[str] = set()

    async def peek() -> Any:
        try:
            async with ledger.transactor.begin() as tx:
                seen = await peek_store.claim("k", 7, tx=tx)
                raise _Peeked(seen)
        except _Peeked as p:
            return p.seen

    async def observe() -> None:
        seen = await peek()
        if isinstance(seen, Record) and not seen.pending:
            landed.add(str(seen.result))
        assert len(landed) <= 1, f"two outcomes landed: {landed}"
        if landed:
            assert isinstance(seen, Record) and str(seen.result) in landed, (
                f"the record was deleted: {seen!r}"
            )

    for s in history:
        w = s.worker
        store = LostResponse(ledger)
        if s.lost and s.op in ("claim", "complete", "release"):
            store.arm(s.op)
        try:
            if s.op == "claim" and w not in held:
                claimed = await store.claim("k", 7)
                if isinstance(claimed, Won):
                    held[w] = claimed.handle
            elif s.op == "run" and w in held:
                ran.add(w)
            elif s.op == "complete" and w in ran:
                handle = held.pop(w)
                ran.discard(w)
                try:
                    await store.complete("k", handle, Record(7, result=f"worker {w}"))
                finally:
                    await observe()
                    await ledger.release("k", handle)  # the guard's compensation, whatever complete did
            elif s.op == "release" and w in held:
                ran.discard(w)
                await store.release("k", held.pop(w))
            elif s.op == "lapse":
                seen = await peek()
                if isinstance(seen, Record) and seen.pending:
                    ledger.expire("k")
        except edax.StoreError:
            pass  # a lost reply or a refused complete: the property is about what the ledger holds
        await observe()


class _Peeked(Exception):
    def __init__(self, seen: Any) -> None:
        self.seen = seen


@settings(max_examples=300, deadline=None)
@given(history=steps)
def test_two_workers_on_one_memory_key_never_land_two_outcomes(history: list[Step]) -> None:
    asyncio.run(_two_workers(history))


def test_the_memory_fence_is_the_kernels() -> None:
    """The handle is opaque bytes from the native store, never a value the SDK mints."""
    claimed = _edax.memory_transactor().claim(_edax.no_tx(), "k", 7)
    assert claimed.won and isinstance(claimed.handle, bytes) and len(claimed.handle) > 0

"""The shipped fault wrappers, :mod:`edax.testing.faults`, each branch proved over a real store.

A fault wrapper that silently never fires makes a suite pass that tests nothing, so every wrapper is held
here to firing exactly once where it is armed, landing (or not) what it says, and refusing to be armed on a
method the store does not have.
"""

from __future__ import annotations

from dataclasses import dataclass

import edax
import fakeredis
import pytest
from edax.backend.memory import (
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemoryLedger,
    MemoryProjectionStore,
    MemoryTransactor,
)
from edax.backend.redis import CLAIM_SCRIPT, RELEASE_SCRIPT, AsyncRedisClient, RedisIdempotencyStore
from edax.command import Record, Won
from edax.projection import Entity
from edax.testing.contracts import draft
from edax.testing.faults import COMMIT, BrokenCommit, CommitFault, LostReply, LostResponse, Refusing

# =========================================================================
# LostResponse
# =========================================================================


@dataclass(frozen=True)
class LostCase:
    name: str
    #: The method armed, or None for a disarmed wrapper.
    arm: str | None
    #: Whether the call raises the lost response.
    lost: bool


@pytest.mark.parametrize(
    "case",
    [
        LostCase("an_armed_append_lands_then_loses_its_reply", "append", lost=True),
        LostCase("a_commit_lands_then_loses_its_reply", COMMIT, lost=True),
        LostCase("another_armed_method_leaves_this_call_alone", "mark_published", lost=False),
        LostCase("disarmed_nothing_is_lost", None, lost=False),
    ],
    ids=lambda c: c.name,
)
async def test_a_lost_response_lands_the_write_and_fires_once(case: LostCase) -> None:
    store = LostResponse(MemoryEventStore())
    store.arm(case.arm)
    env = draft("acct-1", 1)
    raised: BaseException | None = None
    try:
        if case.arm == COMMIT:
            async with store.transaction() as tx:
                await store.append(tx, "acct-1", 0, [env])
        else:
            # Outside a unit of work, so the statement that loses its reply is the one that commits.
            await store.append(store.transactor.auto_commit(), "acct-1", 0, [env])
    except edax.StoreError as e:
        raised = e
    assert (raised is not None) is case.lost, case.name
    # Landed whatever the caller heard: the write is what makes a lost reply dangerous.
    assert [e.id for e in await store.load(store.transactor.auto_commit(), "acct-1")] == [env.id]
    if case.lost:
        assert store.armed is None, "one shot"
        async with store.transaction() as tx:  # the retry meets a healthy store
            await store.append(tx, "acct-1", 1, [draft("acct-1", 2)])
    else:
        assert store.armed == case.arm, "an unfired fault stays armed"


async def test_a_lost_save_leaves_the_callers_version_where_it_was() -> None:
    """The caller never heard the new version, so the in-place bump of a delivered reply does not happen."""
    store = LostResponse(MemoryProjectionStore())
    entity = Entity("acct-1", 0, {"balance": 100})
    store.arm("save")
    with pytest.raises(edax.StoreError, match="save: the write landed"):
        await store.save(entity)
    assert entity.version == 0, "the reply that would have advanced it was lost"
    stored = await store.get("acct-1")
    assert stored is not None and stored.version == 1, "and the row landed at version 1"
    await store.save(Entity("acct-2", 0, {"balance": 1}))  # disarmed: an ordinary save advances in place


@pytest.mark.parametrize("method", ["unpublished_counts", "claim"], ids=["a_typo", "another_seams_method"])
def test_arming_a_method_the_store_lacks_is_refused(method: str) -> None:
    with pytest.raises(AttributeError, match=f"no method {method!r}"):
        LostResponse(MemoryEventStore()).arm(method)


def test_arming_commit_needs_a_store_that_opens_units_of_work() -> None:
    with pytest.raises(AttributeError, match="no method 'transaction'"):
        LostResponse(MemoryIdempotencyStore()).arm(COMMIT)


def test_a_lost_response_passes_attributes_and_sync_methods_through() -> None:
    inner = MemoryIdempotencyStore()
    store = LostResponse(inner)
    assert store.transactor is inner.transactor, "a property"
    assert store.joins_transaction is True, "a class attribute the guard branches on"
    assert store.expire("absent") is False, "a sync method, called through untouched"


# =========================================================================
# Refusing
# =========================================================================


async def test_refusing_refuses_the_named_methods_and_lands_nothing() -> None:
    ledger = MemoryIdempotencyStore()
    store = Refusing(ledger, "complete", "release")
    won = await store.claim("req-1", 7)  # an unnamed method reaches the store
    assert isinstance(won, Won)
    with pytest.raises(edax.StoreError, match="complete: refused"):
        await store.complete("req-1", won.handle, Record(7, result={"balance": 100}))
    with pytest.raises(edax.StoreError, match="release: refused"):
        await store.release("req-1", won.handle)
    assert store.refusals == 2
    held = await ledger.claim("req-1", 7)
    assert not isinstance(held, Won) and held.pending, "neither refused write landed"
    assert store.joins_transaction is True, "attributes pass through"


@pytest.mark.parametrize(
    "methods, error",
    [((), ValueError), (("complete", "settle"), AttributeError)],
    ids=["no_method_named", "a_method_the_store_lacks"],
)
def test_refusing_is_refused_a_wiring_that_would_never_fire(
    methods: tuple[str, ...], error: type[Exception]
) -> None:
    with pytest.raises(error):
        Refusing(MemoryIdempotencyStore(), *methods)


# =========================================================================
# BrokenCommit
# =========================================================================


@pytest.mark.parametrize(
    "arm, raises, landed",
    [("nothing_landed", True, False), ("lost_response", True, True), (None, False, True)],
    ids=["nothing_landed_rolls_back_and_raises", "lost_response_commits_then_raises", "unarmed_commits"],
)
async def test_a_broken_commit_fails_the_way_it_is_armed(
    arm: CommitFault, raises: bool, landed: bool
) -> None:
    db = MemoryTransactor()
    events = MemoryEventStore(db)
    unit = BrokenCommit(db, arm)
    assert unit.transactor is db, "stores over the database find the units of work it opens"
    env = draft("acct-1", 1)
    raised: BaseException | None = None
    try:
        async with edax.transaction(unit):
            await events.append(None, "acct-1", 0, [env])  # joins the ambient tx of `db`
    except edax.StoreError as e:
        raised = e
    assert (raised is not None) is raises
    assert len(await events.load(db.auto_commit(), "acct-1")) == (1 if landed else 0)
    assert db.open_count() == 0, "settled either way"


# =========================================================================
# LostReply
# =========================================================================


async def test_a_lost_reply_runs_the_script_and_loses_its_reply_once() -> None:
    client = fakeredis.FakeAsyncRedis()
    conn = LostReply(AsyncRedisClient(client), CLAIM_SCRIPT, resend=False)
    store = RedisIdempotencyStore(conn, "idem:")
    with pytest.raises(edax.StoreError, match="connection reset"):
        await store.claim("req-1", 7)
    assert conn.armed is False, "one shot"
    held = await store.claim("req-1", 7)
    assert not isinstance(held, Won) and held.pending, "the claim landed before its reply was lost"
    await client.aclose()


async def test_a_lost_reply_with_the_clients_resend_returns_the_second_reply() -> None:
    client = fakeredis.FakeAsyncRedis()
    conn = LostReply(AsyncRedisClient(client), CLAIM_SCRIPT, resend=True)
    store = RedisIdempotencyStore(conn, "idem:")
    won = await store.claim("req-1", 7)
    assert isinstance(won, Won), "the re-sent claim reads its own claim back: a win, not a stranger's hold"
    await client.aclose()


async def test_a_lost_reply_leaves_every_other_script_alone() -> None:
    client = fakeredis.FakeAsyncRedis()
    conn = LostReply(AsyncRedisClient(client), RELEASE_SCRIPT, resend=False)
    store = RedisIdempotencyStore(conn, "idem:")
    won = await store.claim("req-1", 7)  # the claim script: not the armed one
    assert isinstance(won, Won) and conn.armed is True
    await store.complete("req-1", won.handle, Record(7, result={"balance": 100}))
    await client.aclose()


async def test_the_memory_ledger_is_a_ledger_of_its_own() -> None:
    """The wrappers compose with the shipped split ledger as with any store."""
    ledger = LostResponse(MemoryLedger())
    ledger.arm("claim")
    with pytest.raises(edax.StoreError):
        await ledger.claim("req-1", 7)
    held = await ledger.claim("req-1", 7)
    assert not isinstance(held, Won) and held.pending

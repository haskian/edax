"""RedisIdempotencyStore against a Redis that runs the kernel's Lua: fakeredis (an embedded Lua interpreter)
always, and a live server as well when ``EDAX_REDIS_URL`` names one. Nothing here re-implements a script.
Each test takes a ledger under a prefix of its own, so a live database is shared safely. Timing faults that
need a real server (a TCP proxy that drops a reply, a paused container) are in ``test_redis_live.py``; the
claim/complete/release contract runs under P2 in ``test_model.py`` too."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

import edax
import fakeredis
import pytest
import redis.asyncio
import redis.exceptions
from conftest import TTL_MS, Charge, claimed, fresh_prefix, golden, ledger_key
from edax import _edax
from edax.backend.redis import (
    CLAIM_SCRIPT,
    COMPLETE_SCRIPT,
    EXTEND_SCRIPT,
    FORGET_SCRIPT,
    RELEASE_SCRIPT,
    AsyncRedisClient,
    RedisIdempotencyStore,
)
from edax.command import CommandBus, Record, Won, decode_record, idempotent
from edax.testing import IdempotencyStoreContract
from edax.testing.faults import LostReply
from hypothesis import given, settings
from hypothesis import strategies as st

# A real dispatch outcome, not a stand-in: the record the middleware caches for a completed command.
HASH = 11258580006047770801
RECORD = Record(HASH, result={"account_id": "acct-1", "balance_cents": 4200})
#: Short enough that a PTTL read tells a claim from a record, long enough that nothing lapses by itself.
CLAIM_TTL_MS = 6_000
SCRIPTS = {
    "claim": CLAIM_SCRIPT,
    "complete": COMPLETE_SCRIPT,
    "release": RELEASE_SCRIPT,
    "extend": EXTEND_SCRIPT,
    "forget": FORGET_SCRIPT,
}


def _held_record(value: bytes) -> Record:
    """The record a ledger value holds, read the way a claim reads the key's holder."""
    prior = _edax.redis_claim_outcome(_edax.redis_new_claim(0), value)
    assert prior is not None, "a fresh claim never equals a held value"
    return decode_record(prior)


@dataclass
class Ledger:
    """One test's ledger: a client, a prefix of its own, and direct reads of what Redis holds."""

    client: Any
    prefix: str

    def store(self, conn: Any | None = None, **settings: Any) -> RedisIdempotencyStore:
        settings.setdefault("claim_ttl_ms", CLAIM_TTL_MS)
        return RedisIdempotencyStore(conn or self.conn(), self.prefix, ttl_ms=TTL_MS, **settings)

    def conn(self) -> AsyncRedisClient:
        return AsyncRedisClient(self.client)

    def rkey(self, key: str) -> bytes:
        return ledger_key(self.prefix, key)

    async def lapse(self, key: str) -> None:
        """What the claim TTL does when it runs out, without waiting for it."""
        await self.client.delete(self.rkey(key))

    async def held(self, key: str) -> bytes | None:
        return await self.client.get(self.rkey(key))

    async def pttl(self, key: str) -> int:
        return await self.client.pttl(self.rkey(key))


@pytest.fixture
def ledger(redis_client: Any) -> Ledger:
    return Ledger(redis_client, fresh_prefix())


def near(ttl_ms: int, want_ms: int) -> bool:
    """A PTTL read just after a write: at most the TTL written, and not much less."""
    return want_ms - 2_000 < ttl_ms <= want_ms


# =========================================================================
# The kernel's vectors, replayed against a server that runs the Lua
# =========================================================================


async def test_golden_redis_fence(ledger: Ledger) -> None:
    """``redis_fence.json``: what each script does to a key, row by row. The rows are the kernel's
    specification; this runs the kernel's own script bytes against them, so a typo in either script, or
    a fake that disagreed with Redis, fails here."""
    conn = ledger.conn()
    for c in golden("redis_fence.json"):
        name, key = c["name"], c["name"]
        if c["initial"] is not None:
            await ledger.client.set(ledger.rkey(key), c["initial"].encode(), px=c["initial_ttl_ms"])
        args = [a.encode() if isinstance(a, str) else a for a in c["args"]]
        reply = await conn.eval(SCRIPTS[c["script"]], [ledger.rkey(key)], args)
        assert (reply.decode() if isinstance(reply, bytes) else reply) == c["want_reply"], name
        held = await ledger.held(key)
        assert (held.decode() if held is not None else None) == c["want_value"], name
        if c["want_ttl_ms"] is not None:
            assert near(await ledger.pttl(key), c["want_ttl_ms"]), name


# =========================================================================
# Construction: a misconfigured store fails when built, not on every dispatch
# =========================================================================


@pytest.mark.parametrize(
    "branch, settings, match",
    [
        ("empty_prefix", {"prefix": ""}, "prefix is empty"),
        ("zero_ttl", {"ttl_ms": 0}, "ttl_ms is 0 ms"),
        ("negative_claim_ttl", {"claim_ttl_ms": -5}, "claim_ttl_ms is -5 ms"),
        ("float_ttl", {"ttl_ms": 2.5}, "ttl_ms must be a whole number"),
        ("bool_ttl", {"ttl_ms": True}, "ttl_ms must be a whole number"),
        ("ttl_past_the_largest_exact_integer", {"ttl_ms": 2**53}, "from 1 to 9007199254740991"),
        ("zero_deadline", {"op_timeout_ms": 0}, "a deadline must be positive"),
        ("float_deadline", {"op_timeout_ms": 1.5}, "op_timeout_ms must be a whole number"),
        ("heartbeat_on_a_claim_ttl_too_short_to_divide", {"heartbeat": True, "claim_ttl_ms": 2}, "needs 3"),
    ],
)
def test_a_misconfigured_store_is_refused_when_built(
    branch: str, settings: dict[str, Any], match: str
) -> None:
    """Every refused setting fails identically on every write, so it is the terminal ``BadHandle`` at
    construction. Accepted, a zero or float TTL would fail every ``complete`` on the server as a
    transient ``StoreError``, and the handler would re-run on every redelivery."""
    kwargs: dict[str, Any] = {"prefix": "idem:", **settings}
    prefix = kwargs.pop("prefix")
    with pytest.raises(edax.BadHandle, match=match) as caught:
        RedisIdempotencyStore(AsyncRedisClient(fakeredis.FakeAsyncRedis()), prefix, **kwargs)
    assert caught.value.terminal is True, branch


@pytest.mark.parametrize(
    "client", [fakeredis.FakeRedis(), redis.Redis()], ids=["fakeredis_sync", "redis_py_sync"]
)
def test_a_sync_client_is_refused_when_wrapped(client: Any) -> None:
    """A sync client's calls return values, not awaitables, so every call would fail the same way:
    terminal, and at wiring time."""
    with pytest.raises(edax.BadHandle, match=r"needs a redis\.asyncio client"):
        AsyncRedisClient(client)


# =========================================================================
# claim / complete / release / extend, one branch per case
# =========================================================================


def settled(handle: bytes, record: Record = RECORD) -> bytes:
    return _edax.redis_settled_value(handle, edax.command.encode_record(record))


async def test_claim_branches(ledger: Ledger) -> None:
    s = ledger.store()
    other = _edax.redis_new_claim(HASH)

    won = await s.claim("free", HASH)
    assert isinstance(won, Won) and await ledger.held("free") == won.handle, "the handle is the value written"
    assert near(await ledger.pttl("free"), CLAIM_TTL_MS), "a claim lives for the claim TTL"
    assert _held_record(won.handle).pending

    await ledger.client.set(ledger.rkey("held"), other, px=TTL_MS)
    assert await s.claim("held", HASH) == Record(HASH, pending=True), "another's claim reads as pending"
    assert await ledger.held("held") == other, "and is left alone"

    await ledger.client.set(ledger.rkey("settled"), settled(other), px=TTL_MS)
    assert await s.claim("settled", HASH) == RECORD, "a settled key replays its record"


OWNER = b"0" * 32


@pytest.mark.parametrize(
    "branch, stored, want_hash, cause",
    [
        (
            "unversioned_value",
            b"{not json",
            42,
            "stored value has layout version '{'; this kernel reads version '1'",
        ),
        ("too_short_for_a_token", b"1abc", 42, "stored value has no 32-character owner token"),
        (
            "not_utf8",
            b"1" + OWNER + b"\xff",
            42,
            "stored value is not UTF-8: invalid utf-8 sequence of 1 bytes from index 33",
        ),
        ("unreadable_record", b"1" + OWNER + b"{not json", 42, "key must be a string at line 1 column 2"),
        (
            "torn_record_keeps_no_hash",
            b"1" + OWNER + b'{"payload_hash":11258580006047770801,"res',
            42,
            "EOF while parsing a string at line 1 column 41",
        ),
        (
            "wrong_type_keeps_the_stored_hash",
            b"1" + OWNER + b'{"payload_hash":11258580006047770801,"pending":"settling"}',
            HASH,
            'invalid type: string "settling", expected a boolean',
        ),
    ],
)
async def test_claim_of_an_unreadable_value_replays_a_cached_codec_failure(
    ledger: Ledger, branch: str, stored: bytes, want_hash: int, cause: str
) -> None:
    """A stored value no reader can decode, at either layer (the frame or the record half), still holds its
    key, and nobody can settle it. The claim reads the kernel's terminal codec failure, as every SDK does,
    and leaves the value alone; the guard replays it as ``CachedFailure`` on every redelivery, so the
    handler never runs and the dispatcher dead-letters it. The failure keeps the stored hash where it
    reads, else the claimant's."""
    key = claimed("k1")  # the ledger key the guard below claims
    await ledger.client.set(ledger.rkey(key), stored, px=TTL_MS)
    failed = Record(want_hash, error=f"unreadable idempotency record: {cause}", error_code="codec")
    assert await ledger.store().claim(key, 42) == failed, branch
    assert await ledger.held(key) == stored, f"{branch}: the claim wrote nothing"

    runs = 0

    async def handler(cmd: Charge) -> None:
        nonlocal runs
        runs += 1

    bus = CommandBus(idempotent(ledger.store(), scope="test"))
    bus.register("Charge", handler)
    # `Charge("k1")` hashes to neither 42 nor HASH: a stored hash that reads makes it a reused key.
    replay = edax.PayloadMismatch if want_hash == HASH else edax.CachedFailure
    for attempt in ("first delivery", "redelivery"):
        with pytest.raises(replay) as caught:
            await bus.dispatch(Charge("k1"), idempotency_key="k1")
        error = caught.value
        assert error.terminal, f"{branch}: {attempt}"
        if isinstance(error, edax.CachedFailure):
            assert error.original_code == "codec", f"{branch}: {attempt}"
    assert runs == 0, f"{branch}: the handler never ran"


class ChangedBeforeTheDelete:
    """A connection over which the key changes between the forget's read and its delete, once per value in
    ``values``, as when the record lapses and another worker claims the key in that gap."""

    def __init__(self, inner: AsyncRedisClient, client: Any, rkey: bytes, values: list[bytes]) -> None:
        self._inner, self._client, self._rkey = inner, client, rkey
        self._values = list(values)

    async def eval(self, script: str, keys: Any, args: Any) -> Any:
        if self._values and script == FORGET_SCRIPT:
            await self._client.set(self._rkey, self._values.pop(0), px=TTL_MS)
        return await self._inner.eval(script, keys, args)

    async def get(self, key: bytes) -> Any:
        return await self._inner.get(key)


@pytest.mark.parametrize(
    "branch, changed_to, raises, match, want_held",
    [
        ("a_new_settled_record_is_read_again_and_deleted", "settled", None, None, None),
        ("a_new_claim_is_read_again_and_refused", "claim", edax.InProgress, None, "claim"),
        (
            "a_key_that_keeps_changing_raises_store_error",
            "settled_on_every_read",
            edax.StoreError,
            "changed on every one of 3 reads",
            "last",
        ),
    ],
)
async def test_a_forget_whose_key_changed_reads_it_again(
    ledger: Ledger,
    branch: str,
    changed_to: str,
    raises: type[Exception] | None,
    match: str | None,
    want_held: str | None,
) -> None:
    """The delete runs only while the key holds the value the forget read. A key that changed in between
    is read again, and the kernel decides on the new value. A key that changes before every delete wins
    no round, so the forget gives up with a transient error and leaves the key as it found it."""
    s = ledger.store()
    won = await s.claim("k", HASH)
    assert isinstance(won, Won)
    await s.complete("k", won.handle, RECORD)
    claim = _edax.redis_new_claim(HASH)
    values = {
        "settled": [settled(_edax.redis_new_claim(HASH))],
        "claim": [claim],
        # A fresh handle per value, so no read ever matches the value the next delete meets.
        "settled_on_every_read": [settled(_edax.redis_new_claim(HASH)) for _ in range(3)],
    }[changed_to]
    store = ledger.store(ChangedBeforeTheDelete(ledger.conn(), ledger.client, ledger.rkey("k"), values))
    if raises is None:
        await store.forget("k")
    else:
        with pytest.raises(raises, match=match):
            await store.forget("k")
    want = {None: None, "claim": claim, "last": values[-1]}[want_held]
    assert await ledger.held("k") == want, branch


async def test_complete_branches(ledger: Ledger) -> None:
    s = ledger.store()
    won = await s.claim("own", HASH)
    assert isinstance(won, Won)
    await s.complete("own", won.handle, RECORD)
    assert await ledger.held("own") == settled(won.handle)
    assert near(await ledger.pttl("own"), TTL_MS), "a record lives for the record TTL"
    await s.complete("own", won.handle, RECORD)  # a re-sent complete settles on the same value
    assert await s.claim("own", HASH) == RECORD

    lapsed = await s.claim("lapsed", HASH)
    assert isinstance(lapsed, Won)
    await ledger.lapse("lapsed")
    with pytest.raises(
        edax.StoreError, match="redis complete 'lapsed': the claim expired and was taken over"
    ):
        await s.complete("lapsed", lapsed.handle, RECORD)
    assert await ledger.held("lapsed") is None, "no ghost key written over a lapsed claim"


async def test_complete_of_an_unencodable_result_is_terminal_and_writes_nothing(ledger: Ledger) -> None:
    """A result the record codec cannot serialize fails identically on every redelivery, so it must stay
    the terminal ``CodecError``, raised before any I/O."""
    s = ledger.store()
    won = await s.claim("order-7", HASH)
    assert isinstance(won, Won)
    with pytest.raises(edax.CodecError, match="not JSON-encodable") as caught:
        await s.complete("order-7", won.handle, Record(HASH, result=object()))
    assert caught.value.terminal is True
    assert await ledger.held("order-7") == won.handle, "nothing was written"


def foreign_handles(own: bytes) -> list[tuple[str, bytes]]:
    """Handles no claim of this store returned: each must be refused before any I/O."""
    return [
        ("empty", b""),
        ("truncated_to_a_prefix", own[:3]),
        ("bare_owner_token", own[1:33]),
        ("uppercase_owner_token", own[:1] + own[1:33].upper() + own[33:]),
        ("a_settled_value", settled(own)),
    ]


@pytest.mark.parametrize("op", ["complete", "release", "extend"])
async def test_a_handle_the_kernel_never_minted_is_refused_before_any_io(ledger: Ledger, op: str) -> None:
    """A fence that compares only a prefix of the caller's width lets an empty token re-create an expired
    key and a one-character token overwrite a live claim. A malformed handle is a terminal ``CodecError``
    and the key is untouched."""
    s = ledger.store()
    won = await s.claim("order-7", HASH)
    assert isinstance(won, Won)
    calls: dict[str, Callable[[bytes], Awaitable[Any]]] = {
        "complete": lambda h: s.complete("order-7", h, RECORD),
        "release": lambda h: s.release("order-7", h),
        "extend": lambda h: s.extend("order-7", h),
    }
    for name, handle in foreign_handles(won.handle):
        with pytest.raises(edax.CodecError, match=r"claim handle is not a claim|owner token") as caught:
            await calls[op](handle)
        assert caught.value.terminal is True, name
        assert await ledger.held("order-7") == won.handle, f"{name}: the key was touched"


async def test_release_branches(ledger: Ledger) -> None:
    s = ledger.store()
    won = await s.claim("own", HASH)
    assert isinstance(won, Won)
    await s.release("own", won.handle)
    assert await ledger.held("own") is None
    await s.release("own", won.handle)  # releasing an absent key is a no-op
    await s.release("never-claimed", won.handle)

    # The complete landed, its reply was lost, and the guard releases. The record stays.
    done = await s.claim("done", HASH)
    assert isinstance(done, Won)
    await s.complete("done", done.handle, RECORD)
    await s.release("done", done.handle)
    assert await s.claim("done", HASH) == RECORD, "a release never deletes a settled record"


async def test_extend_branches(ledger: Ledger) -> None:
    s = ledger.store()
    won = await s.claim("own", HASH)
    assert isinstance(won, Won)
    await ledger.client.pexpire(ledger.rkey("own"), 500)
    assert await s.extend("own", won.handle) is True
    assert near(await ledger.pttl("own"), CLAIM_TTL_MS), "the lease is back to the claim TTL"

    await s.complete("own", won.handle, RECORD)
    assert await s.extend("own", won.handle) is False, "a settled record is no lease to extend"
    assert near(await ledger.pttl("own"), TTL_MS), "and keeps its record TTL"
    assert await s.extend("never-claimed", won.handle) is False
    assert await ledger.held("never-claimed") is None, "no key created"


# =========================================================================
# two claims on one key in one store instance
# =========================================================================


# The takeover itself (a lapsed claim's complete refused, its release deleting nothing, a third claim
# losing to the live one) is `IdempotencyStoreContract.test_a_lapsed_claim_settles_nothing_for_its_old_owner`,
# which `TestRedisIdempotencyStore` runs. Extending is Redis's alone.


async def test_a_stale_owner_cannot_extend_the_new_holders_lease(ledger: Ledger) -> None:
    s = ledger.store()
    a = await s.claim("k", HASH)
    assert isinstance(a, Won)
    await ledger.lapse("k")
    b = await s.claim("k", HASH)
    assert isinstance(b, Won)
    await ledger.client.pexpire(ledger.rkey("k"), 900)
    assert await s.extend("k", a.handle) is False, "the stale owner learns its lease is gone"
    assert await ledger.pttl("k") <= 900, "and the new holder's TTL was not touched"


@pytest.mark.parametrize("first_to_finish", ["stale", "live"])
async def test_two_dispatches_on_one_bus_settle_only_the_live_claim(
    ledger: Ledger, first_to_finish: str
) -> None:
    """Two claims on one key, through the guard. Two dispatches of one request share one bus and one
    store. A's handler outlives its claim, and B takes the key over while A still runs. Whichever finishes
    first, A's complete is refused: exactly one dispatch reports success and the ledger holds B's result.
    Without the fence, A finishing first would complete over B's live claim and B would then write over
    it, so both would report success. Both handlers ran, which is the documented at-least-once window a
    lapse opens; the fence makes the ledger say so. Barriers order every step."""
    store = ledger.store()
    a_lapsed, b_started, a_finished, b_finished = (asyncio.Event() for _ in range(4))
    runs: list[str] = []

    async def handler(cmd: Charge) -> str:
        runs.append("A" if not runs else "B")
        me = runs[-1]
        if me == "A":
            await ledger.lapse(claimed(cmd.request_id))  # A's claim runs out mid-handler
            a_lapsed.set()
            await (b_started if first_to_finish == "stale" else b_finished).wait()
        else:
            b_started.set()
            if first_to_finish == "stale":
                await a_finished.wait()
        return f"charged by {me}"

    bus = CommandBus(idempotent(store, scope="test"))
    bus.register(Charge.type, handler)

    first = asyncio.create_task(bus.dispatch(Charge("req-1"), idempotency_key="req-1"))
    await a_lapsed.wait()
    second = asyncio.create_task(bus.dispatch(Charge("req-1"), idempotency_key="req-1"))
    if first_to_finish == "stale":
        with pytest.raises(edax.StoreError, match="taken over"):
            await first
        a_finished.set()
        assert await second == "charged by B"
    else:
        assert await second == "charged by B"
        b_finished.set()
        with pytest.raises(edax.StoreError, match="taken over"):
            await first

    assert runs == ["A", "B"]
    assert await bus.dispatch(Charge("req-1"), idempotency_key="req-1") == "charged by B", (
        "the redelivery replays B's outcome"
    )
    assert runs == ["A", "B"], "and runs no handler"


async def test_an_acknowledged_claim_lost_in_a_failover_is_one_more_at_least_once_window(
    ledger: Ledger,
) -> None:
    """Redis replicates asynchronously, so a claim the primary acknowledged can be missing on the promoted
    replica (``docs/backends/redis.md``, Durability). The key is then free and a second worker wins it:
    both handlers run. The ledger stays consistent: the first worker's complete is refused, and the
    second's outcome is the one recorded. Deleting the key models the promotion."""
    a_store, b_store = ledger.store(), ledger.store()
    a = await a_store.claim("k", HASH)
    assert isinstance(a, Won)
    await ledger.lapse("k")  # the replica that took over never saw the claim
    b = await b_store.claim("k", HASH)
    assert isinstance(b, Won)
    await b_store.complete("k", b.handle, Record(HASH, result="B"))
    with pytest.raises(edax.StoreError, match="taken over"):
        await a_store.complete("k", a.handle, Record(HASH, result="A"))
    assert await a_store.claim("k", HASH) == Record(HASH, result="B")


# =========================================================================
# Lost replies, deadlines, error replies
# =========================================================================


async def test_a_claim_whose_reply_was_lost_and_resent_is_won(ledger: Ledger) -> None:
    """redis-py re-sends a command whose reply was lost. The re-sent claim finds the claim its first send
    wrote; the kernel reads its own value back as won, so the handler runs once. A store that read its own
    claim as another's would raise ``InProgress`` on every redelivery until the claim TTL ran out."""
    conn = LostReply(ledger.conn(), CLAIM_SCRIPT, resend=True)
    runs: list[str] = []

    async def handler(cmd: Charge) -> str:
        runs.append(cmd.request_id)
        return "charged"

    bus = CommandBus(idempotent(ledger.store(conn), scope="test"))
    bus.register(Charge.type, handler)
    assert await bus.dispatch(Charge("req-1"), idempotency_key="req-1") == "charged"
    assert runs == ["req-1"] and not conn.armed


async def test_a_claim_whose_reply_was_lost_for_good_is_held_for_the_claim_ttl_only(ledger: Ledger) -> None:
    """With no re-send the claim raises, the guard cannot know whether it landed, and compensates nothing:
    no handle came back to release with. The key stays pending for the claim TTL, not the record TTL."""
    conn = LostReply(ledger.conn(), CLAIM_SCRIPT, resend=False)
    store = ledger.store(conn)
    with pytest.raises(edax.StoreError, match="redis claim 'req-1': connection reset") as caught:
        await store.claim("req-1", HASH)
    assert caught.value.terminal is False
    assert near(await ledger.pttl("req-1"), CLAIM_TTL_MS)
    assert await store.claim("req-1", HASH) == Record(HASH, pending=True), "a redelivery sees InProgress"


@pytest.mark.parametrize("resend", [False, True], ids=["reply_lost", "reply_lost_then_resent"])
async def test_a_complete_whose_reply_was_lost_keeps_the_record(ledger: Ledger, resend: bool) -> None:
    """The complete lands and its reply is lost. Without a re-send the dispatch fails and the guard
    releases, and that release must not delete the record of committed work: the redelivery replays it
    and the handler ran once. A release that matched the token in front of the record would delete it,
    and the redelivery would charge twice."""
    conn = LostReply(ledger.conn(), COMPLETE_SCRIPT, resend=resend)
    runs: list[str] = []

    async def handler(cmd: Charge) -> str:
        runs.append(cmd.request_id)
        return "charged"

    bus = CommandBus(idempotent(ledger.store(conn), scope="test"))
    bus.register(Charge.type, handler)
    if resend:
        assert await bus.dispatch(Charge("req-1"), idempotency_key="req-1") == "charged"
    else:
        with pytest.raises(edax.StoreError, match=re.escape(f"redis complete {claimed('req-1')!r}")):
            await bus.dispatch(Charge("req-1"), idempotency_key="req-1")
    assert await bus.dispatch(Charge("req-1"), idempotency_key="req-1") == "charged", (
        "the redelivery replays the record"
    )
    assert runs == ["req-1"], "the handler ran once"


async def test_a_release_whose_reply_was_lost_still_frees_the_key(ledger: Ledger) -> None:
    """A transient failure releases the claim; the release lands and its reply is lost. The handler's own
    error is the verdict, and the key is free for the redelivery."""
    conn = LostReply(ledger.conn(), RELEASE_SCRIPT, resend=False)
    attempts: list[int] = []

    async def handler(cmd: Charge) -> str:
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("downstream timed out")
        return "charged"

    bus = CommandBus(idempotent(ledger.store(conn), scope="test"))
    bus.register(Charge.type, handler)
    with pytest.raises(RuntimeError, match="downstream timed out"):
        await bus.dispatch(Charge("req-1"), idempotency_key="req-1")
    assert await ledger.held(claimed("req-1")) is None
    assert await bus.dispatch(Charge("req-1"), idempotency_key="req-1") == "charged"


class _Silent:
    """A connection that never answers: a server that hangs, or a network that drops every packet."""

    async def eval(self, script: str, keys: Any, args: Any) -> Any:
        await asyncio.Event().wait()

    async def get(self, key: bytes) -> Any:
        await asyncio.Event().wait()


async def test_a_call_with_no_reply_fails_at_the_deadline() -> None:
    """The deadline bounds each call, client retries included, so a hung server costs a dispatch
    ``op_timeout_ms`` and not redis-py's ten retries of five seconds."""
    store = RedisIdempotencyStore(_Silent(), "idem:", op_timeout_ms=20)
    with pytest.raises(edax.StoreError, match="redis claim 'k': no reply within 20 ms") as caught:
        await store.claim("k", HASH)
    assert caught.value.terminal is False


class _ScriptRaises:
    """A ``redis.asyncio`` client whose every script raises ``error``, as redis-py builds it."""

    def __init__(self, error: BaseException) -> None:
        self._error = error

    async def execute_command(self, *args: Any, **options: Any) -> Any:  # what marks an async client
        raise AssertionError("unused")

    def register_script(self, source: str) -> Callable[..., Awaitable[Any]]:
        async def run(keys: list[bytes], args: list[Any]) -> Any:
            raise self._error

        return run


@pytest.mark.parametrize(
    "branch, error, want, terminal",
    [
        # Built as redis-py's parser builds them: a mapped prefix is stripped and kept in status_code.
        ("invalid_expire_time", redis.exceptions.ResponseError("invalid expire time", status_code="ERR"), edax.BadHandle, True),
        ("no_permission", redis.exceptions.NoPermissionError("no permissions", status_code="NOPERM"), edax.BadHandle, True),
        ("wrong_password", redis.exceptions.AuthenticationError("invalid password", status_code="WRONGPASS"), edax.BadHandle, True),
        ("wrong_type", redis.exceptions.ResponseError("WRONGTYPE Operation against a key"), edax.CodecError, True),
        ("read_only_replica_in_a_failover", redis.exceptions.ReadOnlyError("read only replica", status_code="READONLY"), edax.StoreError, False),
        ("loading", redis.exceptions.BusyLoadingError("loading", status_code="LOADING"), edax.StoreError, False),
        ("connection_reset", redis.exceptions.ConnectionError("reset by peer"), edax.StoreError, False),
        ("client_timeout", redis.exceptions.TimeoutError("timed out"), edax.StoreError, False),
        ("value_not_utf8_under_decode_responses", UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad"), edax.CodecError, True),
    ],
)  # fmt: skip
async def test_a_client_failure_carries_the_kernels_verdict(
    branch: str, error: BaseException, want: type[edax.EdaxError], terminal: bool
) -> None:
    """A deterministic failure (``WRONGTYPE``, a refused command or credential, a value the client cannot
    decode) fails the same way on every retry, so it is terminal; a passing state (a replica during
    failover, a connection reset) is transient. The kernel's ``reply_error`` reads the reply's prefix. Were
    every failure a transient ``StoreError``, the handler would re-run forever."""
    store = RedisIdempotencyStore(AsyncRedisClient(cast(Any, _ScriptRaises(error))), "idem:")
    with pytest.raises(want, match="redis claim 'order-7'") as caught:
        await store.claim("order-7", HASH)
    assert caught.value.terminal is terminal, branch
    assert caught.value.__cause__ is not None


async def test_a_hash_under_the_key_is_a_terminal_wrongtype(ledger: Ledger) -> None:
    """The same verdict from a real server: a hash where the ledger expects a string."""
    await ledger.client.hset(ledger.rkey("k"), "field", "value")
    with pytest.raises(edax.CodecError, match="WRONGTYPE") as caught:
        await ledger.store().claim("k", HASH)
    assert caught.value.terminal is True


async def test_a_client_that_decodes_replies_reads_the_same_ledger() -> None:
    """``decode_responses=True`` answers ``str``; the store reads it back as the kernel's bytes, so a prior
    record, an own re-sent claim and a value that is not UTF-8 get the same verdicts as with bytes."""
    server = fakeredis.FakeServer()
    raw = fakeredis.FakeAsyncRedis(server=server)
    text = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
    store = RedisIdempotencyStore(AsyncRedisClient(text), "idem:")
    won = await store.claim("k", HASH)
    assert isinstance(won, Won)
    await store.complete("k", won.handle, RECORD)
    assert await store.claim("k", HASH) == RECORD

    resent = RedisIdempotencyStore(LostReply(AsyncRedisClient(text), CLAIM_SCRIPT, resend=True), "idem:")
    assert isinstance(await resent.claim("fresh", HASH), Won)

    await raw.set(b"idem:binary", b"1" + b"\xff" * 40)
    with pytest.raises(edax.CodecError, match="not UTF-8") as caught:
        await store.claim("binary", HASH)
    assert caught.value.terminal is True


async def test_each_script_is_registered_once_and_sent_by_digest(ledger: Ledger) -> None:
    """Every script is registered on first use and reused, so a dispatch sends a digest, not the source,
    and every SDK's digest is the same because the body is the kernel's."""
    registered: list[str] = []
    client = ledger.client
    original = client.register_script

    def spy(source: str) -> Any:
        registered.append(source)
        return original(source)

    client.register_script = spy
    store = ledger.store()
    for n in range(3):
        won = await store.claim(f"k{n}", HASH)
        assert isinstance(won, Won)
        await store.complete(f"k{n}", won.handle, RECORD)
    assert registered == [CLAIM_SCRIPT, COMPLETE_SCRIPT]


# =========================================================================
# Lease renewal
# =========================================================================


class Beats:
    """The heartbeat's sleep, stepped by the test: the renewal parks here, and :meth:`beat` lets it run
    one renewal and waits until it parks again or ends. No wall clock is involved."""

    def __init__(self) -> None:
        self.asked: list[float] = []
        self._go = asyncio.Event()
        self._parked = asyncio.Event()

    async def sleep(self, seconds: float) -> None:
        self.asked.append(seconds)
        self._parked.set()
        await self._go.wait()
        self._go.clear()

    async def beat(self, task: asyncio.Task[None]) -> None:
        await self._parked.wait()
        self._parked.clear()
        self._go.set()
        parked = asyncio.ensure_future(self._parked.wait())
        await asyncio.wait({parked, task}, return_when=asyncio.FIRST_COMPLETED)
        parked.cancel()


def heartbeat_store(ledger: Ledger, conn: Any | None = None) -> tuple[RedisIdempotencyStore, Beats]:
    beats = Beats()
    return ledger.store(conn, heartbeat=True, sleep=beats.sleep), beats


async def test_a_heartbeat_renews_the_claim_until_it_settles(ledger: Ledger) -> None:
    store, beats = heartbeat_store(ledger)
    won = await store.claim("k", HASH)
    assert isinstance(won, Won)
    task = store._renewals[won.handle]
    for _ in range(2):
        await ledger.client.pexpire(ledger.rkey("k"), 500)
        await beats.beat(task)
        assert near(await ledger.pttl("k"), CLAIM_TTL_MS), "renewed to the claim TTL"
    assert beats.asked == [CLAIM_TTL_MS / 3000] * 3, "every third of the claim TTL"
    await store.complete("k", won.handle, RECORD)
    await asyncio.sleep(0)
    assert task.cancelled() and store._renewals == {}, "settling stops the renewal"


@pytest.mark.parametrize("settle", ["release", "refused_complete"])
async def test_every_settlement_stops_the_heartbeat(ledger: Ledger, settle: str) -> None:
    """A renewal that outlived its settlement would hold a key nobody settles: every settle path stops it,
    a refused one included."""
    store, _ = heartbeat_store(ledger)
    won = await store.claim("k", HASH)
    assert isinstance(won, Won)
    task = store._renewals[won.handle]
    if settle == "release":
        await store.release("k", won.handle)
    else:
        await ledger.lapse("k")
        with pytest.raises(edax.StoreError, match="taken over"):
            await store.complete("k", won.handle, RECORD)
    await asyncio.sleep(0)
    assert task.cancelled() and store._renewals == {}


async def test_a_heartbeat_that_finds_its_claim_taken_over_stops(
    ledger: Ledger, caplog: pytest.LogCaptureFixture
) -> None:
    """The renewal is fenced like every write: once another worker holds the key, the old owner's beat is
    refused, it says so, and it never touches the new holder's lease."""
    store, beats = heartbeat_store(ledger)
    won = await store.claim("k", HASH)
    assert isinstance(won, Won)
    task = store._renewals[won.handle]
    await ledger.lapse("k")
    taker = await ledger.store().claim("k", HASH)
    assert isinstance(taker, Won)
    await ledger.client.pexpire(ledger.rkey("k"), 900)
    with caplog.at_level(logging.WARNING, logger="edax.backend.redis"):
        await beats.beat(task)
    assert task.done() and not task.cancelled()
    assert "was lost before it settled" in caplog.text
    assert await ledger.pttl("k") <= 900, "the new holder's lease was not extended"


class _ExtendFails:
    """A connection whose extend fails with ``error`` once, then works."""

    def __init__(self, inner: AsyncRedisClient, error: BaseException) -> None:
        self._inner = inner
        self._error: BaseException | None = error

    async def eval(self, script: str, keys: Any, args: Any) -> Any:
        if script == EXTEND_SCRIPT and self._error is not None:
            error, self._error = self._error, None
            raise error
        return await self._inner.eval(script, keys, args)


@pytest.mark.parametrize(
    "branch, error, keeps_beating",
    [
        ("transient_failure_retries_next_beat", redis.exceptions.ConnectionError("reset"), True),
        ("terminal_failure_stops", edax.BadHandle("redis replied NOPERM"), False),
    ],
)
async def test_a_failed_beat(ledger: Ledger, branch: str, error: BaseException, keeps_beating: bool) -> None:
    store, beats = heartbeat_store(ledger, _ExtendFails(ledger.conn(), error))
    won = await store.claim("k", HASH)
    assert isinstance(won, Won)
    task = store._renewals[won.handle]
    await beats.beat(task)
    assert task.done() is not keeps_beating, branch
    if keeps_beating:
        await ledger.client.pexpire(ledger.rkey("k"), 500)
        await beats.beat(task)
        assert near(await ledger.pttl("k"), CLAIM_TTL_MS), "the next beat renewed"
    await store.release("k", won.handle)


# =========================================================================
# Two workers, one key: a property over claim, run, settle, extend, lapse and lost replies
# =========================================================================


@dataclass(frozen=True)
class Step:
    worker: int
    op: str  # claim, run, complete, release, extend, lapse
    fault: str  # none, lost, resent


steps = st.lists(
    st.builds(
        Step,
        st.integers(0, 1),
        st.sampled_from(["claim", "run", "complete", "release", "extend", "lapse"]),
        st.sampled_from(["none", "none", "lost", "resent"]),
    ),
    min_size=1,
    max_size=30,
)


async def _two_workers(history: list[Step]) -> None:
    """Replays ``history`` against fakeredis and checks the ledger's invariants after every step:
    - a settled record, once written, is never deleted or overwritten (no record TTL runs here);
    - at most one worker's outcome ever lands, so at most one side effect is recorded as the outcome;
    - at most one worker's handle is the live claim.
    ``lapse`` is the claim TTL running out: it removes a pending claim, never a record."""
    client = fakeredis.FakeAsyncRedis()
    ledger = Ledger(client, "idem:")
    held: dict[int, bytes] = {}  # worker -> the handle of the claim it believes it holds
    ran: set[int] = set()  # workers whose handler ran under their current claim
    landed: set[bytes] = set()  # every settled value the key ever held
    record: bytes | None = None
    script_of = {
        "claim": CLAIM_SCRIPT,
        "complete": COMPLETE_SCRIPT,
        "release": RELEASE_SCRIPT,
        "extend": EXTEND_SCRIPT,
    }

    async def observe() -> bytes | None:
        """Read the key and check every invariant against what it holds now."""
        nonlocal record
        value = await ledger.held("k")
        if value is not None and not _held_record(value).pending:
            landed.add(value)
            record = record or value
        assert len(landed) <= 1, f"two outcomes landed: {landed}"
        assert record is None or value == record, f"the settled record was deleted or replaced: {value!r}"
        assert sum(1 for h in held.values() if h == value) <= 1, "two workers hold the live claim"
        return value

    for s in history:
        w = s.worker
        conn: Any = ledger.conn()
        if s.fault != "none" and s.op in script_of:
            conn = LostReply(conn, script_of[s.op], resend=s.fault == "resent")
        store = ledger.store(conn)
        try:
            if s.op == "claim" and w not in held:
                claimed = await store.claim("k", HASH)
                if isinstance(claimed, Won):
                    held[w] = claimed.handle
            elif s.op == "run" and w in held:
                ran.add(w)
            elif s.op == "complete" and w in ran:
                handle = held.pop(w)
                ran.discard(w)
                try:
                    await store.complete("k", handle, Record(HASH, result=f"worker {w}"))
                finally:
                    # Checked between the two writes, then the guard's compensation runs whatever the
                    # complete did: a release that deleted a landed record must not hide behind it.
                    await observe()
                    await ledger.store().release("k", handle)
            elif s.op == "release" and w in held:
                ran.discard(w)
                await store.release("k", held.pop(w))
            elif s.op == "extend" and w in held:
                await store.extend("k", held[w])
            elif s.op == "lapse":
                value = await ledger.held("k")
                if value is not None and _held_record(value).pending:
                    await ledger.lapse("k")
        except edax.StoreError:
            pass  # a lost reply or a refused complete: the property is about what the ledger holds
        await observe()


@settings(max_examples=300, deadline=None)
@given(history=steps)
def test_two_workers_on_one_key_never_land_two_outcomes(history: list[Step]) -> None:
    asyncio.run(_two_workers(history))


# =========================================================================
# The conformance suite, with the takeover and concurrency clauses, against real Lua
# =========================================================================


class TestRedisIdempotencyStore(IdempotencyStoreContract):
    claims_expire = True
    stores_raw_records = True

    @pytest.fixture
    def idempotency_store(self, ledger: Ledger) -> Any:
        return ledger.store()

    async def expire_claim(self, idempotency_store: Any, key: str) -> None:
        await idempotency_store._conn._client.delete(idempotency_store._key(key))

    async def put_raw_record(self, idempotency_store: Any, key: str, raw: str) -> None:
        # The record half of a well-framed value: what a torn or foreign record write leaves behind.
        value = b"1" + OWNER + raw.encode()
        await idempotency_store._conn._client.set(idempotency_store._key(key), value, px=TTL_MS)

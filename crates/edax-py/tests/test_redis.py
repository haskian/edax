"""RedisIdempotencyStore over an in-memory fake, no server. Redis-only behaviour; the
claim/complete/release contract runs under P2 in ``test_model.py``."""

from __future__ import annotations

from collections.abc import Awaitable
from typing import Any, Callable, Optional

import edax
import pytest
from conftest import TTL_MS, FakeRedis
from edax.backend.redis import (
    _CLAIM_ATTEMPTS,
    _DEL_IF_PREFIX,
    _SET_IF_PREFIX,
    AsyncRedisClient,
    RedisIdempotencyStore,
)
from edax.command import Record, decode_record


def store(conn: Optional[FakeRedis] = None) -> RedisIdempotencyStore:
    return RedisIdempotencyStore(conn or FakeRedis(), prefix="idem:", ttl_ms=TTL_MS)


# A real dispatch outcome, not a stand-in: the record the middleware caches for a completed command.
RECORD = Record(11258580006047770801, result={"account_id": "acct-1", "balance_cents": 4200})
# The pending record the winner of a claim leaves behind, exactly as it sits in Redis: the
# claimant's fence token, then the kernel's record JSON.
OWNER = "0" * 32
RECORD_JSON = b'{"payload_hash":11258580006047770801,"result":null,"error":null,"pending":true}'
RECORD_BYTES = OWNER.encode() + RECORD_JSON


Call = Callable[[RedisIdempotencyStore], Awaitable[Any]]


@pytest.mark.parametrize(
    "seam_method, call, holds_claim",
    [
        ("claim", lambda s: s.claim("order-7", RECORD.payload_hash), False),
        # `complete` and `release` are fenced: without a claim of its own the store has nothing to
        # settle and makes no call at all, so the claim has to be taken before the fault is armed.
        ("complete", lambda s: s.complete("order-7", RECORD), True),
        ("release", lambda s: s.release("order-7"), True),
    ],
)
async def test_client_failure_maps_to_store_error(seam_method: str, call: Call, holds_claim: bool) -> None:
    """Every seam method types a client failure as the contract's transient ``StoreError``, naming
    the operation and the key and chaining the client's exception. An untyped error would read as
    "no record" to the middleware, or be misclassified as terminal."""
    conn = FakeRedis()
    s = store(conn)
    if holds_claim:
        await s.claim("order-7", RECORD.payload_hash)
    conn.fail = True
    with pytest.raises(edax.StoreError, match=f"redis {seam_method} 'order-7'") as caught:
        await call(s)
    assert caught.value.terminal is False  # transient: the claim is released and retried
    assert isinstance(caught.value.__cause__, RuntimeError)
    assert "connection reset" in str(caught.value.__cause__)


async def test_claim_retries_when_key_vanishes_between_setnx_and_get() -> None:
    # SET NX says the key exists, but the TTL (or a concurrent release) drops it before the GET. The
    # claim loop must retry rather than treat a missing record as a prior one.
    conn = FakeRedis()
    s = store(conn)
    conn.vanish_once = True
    assert await s.claim("k1", 42) is None  # won on the retry
    assert conn.set_nx_calls == 2  # lost once, retried, won


async def test_claim_gives_up_when_the_key_keeps_vanishing() -> None:
    """The SET NX -> GET retry is bounded: a key that never materializes fails fast instead of
    spinning the dispatch forever."""

    class AlwaysVanishes(FakeRedis):
        async def set_nx_px(self, key: str, value: bytes, ttl_ms: int) -> bool:
            self.set_nx_calls += 1
            return False  # lost, yet the GET that follows finds nothing

    conn = AlwaysVanishes()
    with pytest.raises(edax.StoreError, match="record vanished"):
        await store(conn).claim("k1", 42)
    assert conn.set_nx_calls == _CLAIM_ATTEMPTS


class _LostClaim(FakeRedis):
    """A client that always loses the SET NX and answers the follow-up GET with ``reply``, so the
    test drives the adapter's decoding of a prior record rather than a fake server's storage."""

    def __init__(self, reply: Any) -> None:
        super().__init__()
        self._reply = reply

    async def set_nx_px(self, key: str, value: bytes, ttl_ms: int) -> bool:
        self.set_nx_calls += 1
        return False

    async def get(self, key: str) -> Any:
        return self._reply


@pytest.mark.parametrize(
    "branch, reply",
    [
        ("bytes_reply", RECORD_BYTES),  # a default redis-py client
        ("bytearray_reply", bytearray(RECORD_BYTES)),
        # A client built with decode_responses=True answers with str; treating it as bytes would
        # raise AttributeError inside the claim and surface as a bogus CodecError.
        ("str_reply", RECORD_BYTES.decode()),
    ],
)
async def test_claim_decodes_the_prior_record_from_any_client_reply(branch: str, reply: Any) -> None:
    prior = await store(_LostClaim(reply)).claim("order-7", RECORD.payload_hash)
    assert prior == Record(RECORD.payload_hash, pending=True)


async def test_complete_of_an_unencodable_result_is_terminal() -> None:
    """A result the record codec cannot serialize fails identically on every redelivery, so it must
    stay the terminal ``CodecError``. Typing it as the transient ``StoreError`` (the fate of anything
    raised inside the client guard) would nack the same command until the broker gave up."""
    conn = FakeRedis()
    with pytest.raises(edax.CodecError, match="not JSON-encodable") as caught:
        await store(conn).complete("order-7", Record(RECORD.payload_hash, result=object()))
    assert caught.value.terminal is True
    assert conn.map == {}  # nothing was written: the encode failed before the SET


async def test_claim_of_an_unreadable_record_is_terminal() -> None:
    """A stored record that will not decode fails the same way on every retry, so it is a terminal
    ``CodecError``; retrying or replaying it would loop until the TTL."""
    conn = FakeRedis()
    conn.map["idem:k1"] = (b"{not json", TTL_MS)
    with pytest.raises(edax.CodecError, match="unreadable") as caught:
        await store(conn).claim("k1", 42)
    assert caught.value.terminal is True


class _RegisteredScript:
    """What ``register_script`` hands back: a callable that sends EVALSHA with ``keys`` / ``args``
    and only falls back to the source the server already holds."""

    def __init__(self, calls: list[tuple[str, tuple, dict]], source: str, returns: Any) -> None:
        self._calls = calls
        self._source = source
        self._returns = returns

    async def __call__(self, keys: list[str], args: list[Any]) -> Any:
        self._calls.append(("evalsha", (self._source,), {"keys": keys, "args": args}))
        return self._returns


class _SpyRedisPy:
    """Records the redis-py calls the adapter makes, so the test checks the adapter's translation
    (``px=``, ``nx=True``, the NX ``None``, the registered scripts) rather than a fake server."""

    def __init__(
        self, set_returns: Any = True, get_returns: Optional[bytes] = None, script_returns: Any = 1
    ) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []
        self.registered: list[str] = []
        self._set_returns = set_returns
        self._get_returns = get_returns
        self._script_returns = script_returns

    def register_script(self, source: str) -> _RegisteredScript:
        self.registered.append(source)
        return _RegisteredScript(self.calls, source, self._script_returns)

    async def get(self, key: str) -> Optional[bytes]:
        self.calls.append(("get", (key,), {}))
        return self._get_returns

    async def set(self, key: str, value: bytes, **kwargs: Any) -> Any:
        self.calls.append(("set", (key, value), kwargs))
        return self._set_returns

    async def delete(self, key: str) -> int:
        self.calls.append(("delete", (key,), {}))
        return 1


@pytest.mark.parametrize(
    "branch, client, call, want_calls, want",
    [
        (
            "get_returns_the_stored_bytes",
            _SpyRedisPy(get_returns=RECORD_BYTES),
            lambda c: c.get("idem:order-7"),
            [("get", ("idem:order-7",), {})],
            RECORD_BYTES,
        ),
        (
            "set_px_sets_the_ttl_without_nx",
            _SpyRedisPy(),
            lambda c: c.set_px("idem:order-7", RECORD_BYTES, TTL_MS),
            [("set", ("idem:order-7", RECORD_BYTES), {"px": TTL_MS})],
            None,
        ),
        (
            "set_nx_px_won_is_true",
            _SpyRedisPy(set_returns=True),
            lambda c: c.set_nx_px("idem:order-7", RECORD_BYTES, TTL_MS),
            [("set", ("idem:order-7", RECORD_BYTES), {"px": TTL_MS, "nx": True})],
            True,
        ),
        # redis-py reports a lost NX as None, not False; the adapter must not read it as "won".
        (
            "set_nx_px_lost_none_is_false",
            _SpyRedisPy(set_returns=None),
            lambda c: c.set_nx_px("idem:order-7", RECORD_BYTES, TTL_MS),
            [("set", ("idem:order-7", RECORD_BYTES), {"px": TTL_MS, "nx": True})],
            False,
        ),
        (
            "delete_removes_the_key",
            _SpyRedisPy(),
            lambda c: c.delete("idem:order-7"),
            [("delete", ("idem:order-7",), {})],
            None,
        ),
        (
            "set_px_if_prefix_calls_the_registered_script",
            _SpyRedisPy(script_returns=1),
            lambda c: c.set_px_if_prefix("idem:order-7", OWNER.encode(), RECORD_BYTES, TTL_MS),
            [
                (
                    "evalsha",
                    (_SET_IF_PREFIX,),
                    {"keys": ["idem:order-7"], "args": [OWNER.encode(), RECORD_BYTES, TTL_MS]},
                )
            ],
            True,
        ),
        # The fence reports a lost claim as 0, which the adapter must read as False, not truthy.
        (
            "delete_if_prefix_lost_the_fence_is_false",
            _SpyRedisPy(script_returns=0),
            lambda c: c.delete_if_prefix("idem:order-7", OWNER.encode()),
            [("evalsha", (_DEL_IF_PREFIX,), {"keys": ["idem:order-7"], "args": [OWNER.encode()]})],
            False,
        ),
    ],
)
async def test_async_redis_client_translates_each_seam_call(
    branch: str,
    client: _SpyRedisPy,
    call: Callable[[AsyncRedisClient], Awaitable[Any]],
    want_calls: list,
    want: Any,
) -> None:
    got = await call(AsyncRedisClient(client))
    assert got == want and got.__class__ is want.__class__
    assert client.calls == want_calls


async def test_claim_after_ttl_expiry_wins_again() -> None:
    # The TTL bounds how long an outcome is replayable: past it the key is gone and the same command
    # wins a fresh claim and re-runs. At-least-once by construction: size the TTL past the longest
    # redelivery window (see the IdempotencyStore contract).
    conn = FakeRedis()
    s = store(conn)
    assert await s.claim("k1", 42) is None
    await s.complete("k1", Record(42, result=7))
    assert await s.claim("k1", 42) is not None  # replayable inside the window

    conn.now_ms = TTL_MS + 1  # the record expired
    assert await s.claim("k1", 42) is None, "an expired key is claimable again"

    # completing after the re-claim writes a record with a fresh TTL, replayable once more
    await s.complete("k1", Record(42, result=8))
    again = await s.claim("k1", 42)
    assert again is not None and again.result == 8


async def test_a_pending_claim_expires_on_the_claim_ttl_not_the_record_ttl() -> None:
    """The two TTLs bound different things. A pending claim blocks its key only until
    ``claim_ttl_ms``, so a worker that dies mid-handler cannot block it for the record's whole
    retention — which is what one shared 24-hour value used to do."""
    conn = FakeRedis()
    s = RedisIdempotencyStore(conn, prefix="idem:", ttl_ms=TTL_MS, claim_ttl_ms=TTL_MS // 10)

    assert await s.claim("order-7", RECORD.payload_hash) is None  # won, and then the worker dies
    conn.now_ms += TTL_MS // 10 + 1  # past the claim TTL, far short of the record TTL

    other = RedisIdempotencyStore(conn, prefix="idem:", ttl_ms=TTL_MS, claim_ttl_ms=TTL_MS // 10)
    assert await other.claim("order-7", RECORD.payload_hash) is None, "the key is free again"


async def test_a_completed_record_outlives_the_claim_ttl() -> None:
    """The converse: once the outcome is recorded it must survive for the dedup window, or a
    redelivery inside that window re-runs the handler instead of replaying."""
    conn = FakeRedis()
    s = RedisIdempotencyStore(conn, prefix="idem:", ttl_ms=TTL_MS, claim_ttl_ms=TTL_MS // 10)

    await s.claim("order-7", RECORD.payload_hash)
    await s.complete("order-7", RECORD)
    conn.now_ms += TTL_MS // 10 + 1  # past the claim TTL

    prior = await RedisIdempotencyStore(conn, prefix="idem:", ttl_ms=TTL_MS).claim(
        "order-7", RECORD.payload_hash
    )
    assert prior == RECORD, "the terminal record still replays"


async def test_a_worker_that_lost_its_claim_cannot_complete_over_the_new_holder() -> None:
    """The fence. A claim expires mid-handler, another worker takes the key, and the first worker
    finally finishes. Its ``complete`` must not overwrite the new holder's claim — the outcome it
    carries belongs to a run nobody is waiting for any more — and it must say so rather than
    reporting a success it did not record."""
    conn = FakeRedis()
    slow = RedisIdempotencyStore(conn, prefix="idem:", ttl_ms=TTL_MS, claim_ttl_ms=TTL_MS // 10)
    fast = RedisIdempotencyStore(conn, prefix="idem:", ttl_ms=TTL_MS, claim_ttl_ms=TTL_MS // 10)

    assert await slow.claim("order-7", RECORD.payload_hash) is None
    conn.now_ms += TTL_MS // 10 + 1  # the slow worker's claim lapses while it is still working
    assert await fast.claim("order-7", RECORD.payload_hash) is None, "the second worker takes over"

    with pytest.raises(edax.StoreError, match="taken over"):
        await slow.complete("order-7", RECORD)

    held = await conn.get("idem:order-7")
    assert held is not None and decode_record(held.decode()[32:]).pending, "still the new holder's"

    # And the new holder's own completion still works.
    await fast.complete("order-7", RECORD)
    assert await RedisIdempotencyStore(conn, prefix="idem:").claim("order-7", RECORD.payload_hash) == RECORD


async def test_a_worker_that_lost_its_claim_cannot_release_the_new_holders() -> None:
    """The same fence on the delete path, which is the one that silently loses work: an unfenced
    ``DEL`` here frees a key another worker is actively holding, so two handlers run for it."""
    conn = FakeRedis()
    slow = RedisIdempotencyStore(conn, prefix="idem:", ttl_ms=TTL_MS, claim_ttl_ms=TTL_MS // 10)
    fast = RedisIdempotencyStore(conn, prefix="idem:", ttl_ms=TTL_MS, claim_ttl_ms=TTL_MS // 10)

    await slow.claim("order-7", RECORD.payload_hash)
    conn.now_ms += TTL_MS // 10 + 1
    await fast.claim("order-7", RECORD.payload_hash)

    await slow.release("order-7")  # the lapsed worker gives up
    prior = await RedisIdempotencyStore(conn, prefix="idem:").claim("order-7", RECORD.payload_hash)
    assert prior is not None and prior.pending, "the new holder's claim survived the stale release"


async def test_the_fence_scripts_are_registered_once_not_shipped_per_call() -> None:
    """Both Lua bodies are registered at construction, so each settled claim sends a hash instead
    of the script source. Shipping the source on every ``eval`` costs a few hundred bytes per
    dispatch on the hot path."""
    client = _SpyRedisPy(script_returns=1)
    adapter = AsyncRedisClient(client)
    assert client.registered == [_SET_IF_PREFIX, _DEL_IF_PREFIX]

    for _ in range(3):
        await adapter.set_px_if_prefix("idem:order-7", OWNER.encode(), RECORD_BYTES, TTL_MS)
        await adapter.delete_if_prefix("idem:order-7", OWNER.encode())
    assert client.registered == [_SET_IF_PREFIX, _DEL_IF_PREFIX], "re-registered on a later call"
    assert len(client.calls) == 6


async def test_a_failed_release_keeps_the_fence_for_the_retry() -> None:
    """The delete is what the fence protects, so the token may only be dropped once the delete has
    landed. Dropping it first leaves a failed ``DEL`` with the claim still in Redis and this caller
    no longer holding its token — and the retry then issues an unfenced ``DEL`` that frees whatever
    claim replaced it."""
    conn = FakeRedis()
    slow = RedisIdempotencyStore(conn, prefix="idem:", ttl_ms=TTL_MS, claim_ttl_ms=TTL_MS // 10)
    fast = RedisIdempotencyStore(conn, prefix="idem:", ttl_ms=TTL_MS, claim_ttl_ms=TTL_MS // 10)

    await slow.claim("order-7", RECORD.payload_hash)
    conn.fail = True
    with pytest.raises(edax.StoreError, match="redis release 'order-7'"):
        await slow.release("order-7")
    conn.fail = False

    # The claim lapses and another worker takes it over before the release is retried.
    conn.now_ms += TTL_MS // 10 + 1
    await fast.claim("order-7", RECORD.payload_hash)
    await slow.release("order-7")

    prior = await RedisIdempotencyStore(conn, prefix="idem:").claim("order-7", RECORD.payload_hash)
    assert prior is not None and prior.pending, "the retried release deleted the new holder's claim"

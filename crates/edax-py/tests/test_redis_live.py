"""RedisIdempotencyStore against a live Redis, for the faults only a real server and a real network have: a
reply dropped on the wire and re-sent by redis-py's own retry, a lease that real time runs out, and a server
frozen mid-handler. Set ``EDAX_REDIS_URL`` (``redis://localhost:56380/0``) to run them; the pause tests also
need ``EDAX_REDIS_CONTAINER``, the Docker container to freeze. Without them the module is skipped: the fast
suite (``test_redis.py``) covers the same decisions on fakeredis, which runs the same Lua."""

from __future__ import annotations

import asyncio
import hashlib
import os
import subprocess
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import Any
from urllib.parse import urlparse

import edax
import pytest
import redis.asyncio
from conftest import REDIS_URL, Charge, claimed, fresh_prefix, ledger_key
from edax.backend.redis import CLAIM_SCRIPT, COMPLETE_SCRIPT, AsyncRedisClient, RedisIdempotencyStore
from edax.command import CommandBus, Record, Won, idempotent

if REDIS_URL is None:
    pytest.skip("EDAX_REDIS_URL is not set: no live Redis", allow_module_level=True)

URL: str = REDIS_URL or ""  # set: the module is skipped without it
CONTAINER: str | None = os.environ.get("EDAX_REDIS_CONTAINER")
HASH = 11258580006047770801


@pytest.fixture
async def direct() -> AsyncIterator[Any]:
    client = redis.asyncio.Redis.from_url(URL)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def prefix() -> str:
    return fresh_prefix()


class DroppingProxy:
    """A TCP proxy in front of Redis that, once armed, forwards the next command carrying ``marker`` and then
    drops that command's reply and cuts the connection, as a network blip does after the server has
    applied the write. redis-py's default retry reconnects through the proxy and sends the command again."""

    def __init__(self, upstream: tuple[str, int]) -> None:
        self._upstream = upstream
        self.marker: bytes | None = None
        self.dropped = 0

    async def _handle(self, client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter) -> None:
        up_r, up_w = await asyncio.open_connection(*self._upstream)
        drop_next_reply = asyncio.Event()

        async def to_server() -> None:
            try:
                while data := await client_r.read(65536):
                    if self.marker is not None and self.marker in data:
                        self.marker = None
                        drop_next_reply.set()
                    up_w.write(data)
                    await up_w.drain()
            except ConnectionError:
                pass
            finally:
                up_w.close()

        async def to_client() -> None:
            try:
                while data := await up_r.read(65536):
                    if drop_next_reply.is_set():
                        self.dropped += 1
                        return  # the reply never reaches the client
                    client_w.write(data)
                    await client_w.drain()
            except ConnectionError:
                pass
            finally:
                client_w.close()

        await asyncio.gather(to_server(), to_client(), return_exceptions=True)

    @asynccontextmanager
    async def serving(self) -> AsyncIterator[int]:
        server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        try:
            yield server.sockets[0].getsockname()[1]
        finally:
            server.close()


def upstream() -> tuple[str, int]:
    url = urlparse(URL)
    return url.hostname or "localhost", url.port or 6379


@pytest.mark.parametrize("script", ["claim", "complete"])
async def test_a_reply_dropped_on_the_wire_is_resent_by_redis_py_and_runs_the_handler_once(
    direct: Any, prefix: str, script: str
) -> None:
    """A lost reply to the claim or to the complete, on a real wire through a dropping proxy. The server
    applies the script, the reply is lost, and redis-py's default retry sends the same ``EVALSHA`` again.
    The re-sent claim reads its own value back as won; the re-sent complete finds its own settled value.
    Either way the dispatch succeeds once and the handler runs once. A re-sent claim read as another
    worker's would raise ``InProgress`` with no handler run, for the whole claim TTL."""
    body = {"claim": CLAIM_SCRIPT, "complete": COMPLETE_SCRIPT}[script]
    await direct.script_load(body)  # so the dropped reply is the script's, not a NOSCRIPT round
    proxy = DroppingProxy(upstream())
    runs: list[str] = []

    async def handler(cmd: Charge) -> str:
        runs.append(cmd.request_id)
        return "charged"

    async with proxy.serving() as port:
        via_proxy = redis.asyncio.Redis(host="127.0.0.1", port=port)
        store = RedisIdempotencyStore(AsyncRedisClient(via_proxy), prefix)
        bus = CommandBus(idempotent(store, scope="test"))
        bus.register(Charge.type, handler)
        proxy.marker = hashlib.sha1(body.encode()).hexdigest().encode()
        assert await bus.dispatch(Charge("req-1"), idempotency_key="req-1") == "charged"
        assert proxy.dropped == 1, "the proxy dropped one reply"
        assert await bus.dispatch(Charge("req-1"), idempotency_key="req-1") == "charged", (
            "the redelivery replays"
        )
        await via_proxy.aclose()
    assert runs == ["req-1"]


async def test_a_heartbeat_holds_a_claim_past_its_ttl_and_without_it_the_key_is_taken_over(
    direct: Any, prefix: str
) -> None:
    """Lease renewal in real time. A handler runs four claim TTLs while a second worker keeps trying the key. With
    the heartbeat the claim stays live and the first worker's outcome lands; without it the claim lapses,
    the second worker takes over, and the first worker's complete is refused."""
    for heartbeat in (True, False):
        key = f"req-{heartbeat}"
        holder = RedisIdempotencyStore(
            AsyncRedisClient(direct), prefix, claim_ttl_ms=300, op_timeout_ms=90, heartbeat=heartbeat
        )
        rival = RedisIdempotencyStore(AsyncRedisClient(direct), prefix, claim_ttl_ms=300)
        won = await holder.claim(key, HASH)
        assert isinstance(won, Won)
        taken: bytes | None = None
        for _ in range(12):  # 1.2 s of rival claims, every 100 ms
            await asyncio.sleep(0.1)
            claimed = await rival.claim(key, HASH)
            if isinstance(claimed, Won):
                taken = claimed.handle
                break
        if heartbeat:
            assert taken is None, "the renewed claim was never free"
            await holder.complete(key, won.handle, Record(HASH, result="holder"))
            assert await rival.claim(key, HASH) == Record(HASH, result="holder")
        else:
            assert taken is not None, "the unrenewed claim lapsed and was taken over"
            with pytest.raises(edax.StoreError, match="taken over"):
                await holder.complete(key, won.handle, Record(HASH, result="holder"))
            assert await direct.get(ledger_key(prefix, key)) == taken


@contextmanager
def frozen(container: str) -> Iterator[None]:
    subprocess.run(["docker", "pause", container], check=True, capture_output=True)
    try:
        yield
    finally:
        subprocess.run(["docker", "unpause", container], check=True, capture_output=True)


@pytest.mark.skipif(CONTAINER is None, reason="EDAX_REDIS_CONTAINER is not set: no container to pause")
async def test_a_server_frozen_past_the_claim_ttl_refuses_the_late_complete(direct: Any, prefix: str) -> None:
    """The server is frozen mid-handler for longer than the claim TTL. Redis checks expiry against its
    wall clock, so when it resumes the claim has expired: the worker's complete is refused, the guard's
    release deletes nothing, and the key is free for the redelivery, which runs the handler again
    (at-least-once, as documented)."""
    assert CONTAINER is not None
    container = CONTAINER
    store = RedisIdempotencyStore(AsyncRedisClient(direct), prefix, claim_ttl_ms=800, op_timeout_ms=10_000)
    runs: list[str] = []

    async def handler(cmd: Charge) -> str:
        runs.append(cmd.request_id)
        if len(runs) == 1:
            with frozen(container):
                await asyncio.sleep(1.5)
        return "charged"

    bus = CommandBus(idempotent(store, scope="test"))
    bus.register(Charge.type, handler)
    with pytest.raises(edax.StoreError, match="taken over"):
        await bus.dispatch(Charge("req-1"), idempotency_key="req-1")
    assert await direct.get(ledger_key(prefix, claimed("req-1"))) is None
    assert await bus.dispatch(Charge("req-1"), idempotency_key="req-1") == "charged"
    assert runs == ["req-1", "req-1"]


@pytest.mark.skipif(CONTAINER is None, reason="EDAX_REDIS_CONTAINER is not set: no container to pause")
async def test_settlement_sent_into_a_frozen_server_never_loses_a_landed_record(
    direct: Any, prefix: str
) -> None:
    """The handler finishes while the server is frozen. The complete misses its deadline, and so does the
    guard's release; both commands still sit in the server's socket buffers and run when it resumes, in
    an order nobody controls. Whatever that order, the key ends free or holding this worker's outcome:
    never a deleted record whose work the caller was told failed, and never a stuck claim."""
    assert CONTAINER is not None
    store = RedisIdempotencyStore(AsyncRedisClient(direct), prefix, claim_ttl_ms=10_000, op_timeout_ms=300)
    won = await store.claim("req-1", HASH)
    assert isinstance(won, Won)
    with frozen(CONTAINER):
        with pytest.raises(edax.StoreError, match="no reply within 300 ms"):
            await store.complete("req-1", won.handle, Record(HASH, result="charged"))
        with pytest.raises(edax.StoreError, match="no reply within 300 ms"):
            await store.release("req-1", won.handle)
    await asyncio.sleep(0.3)  # the buffered commands run once the server resumes
    held = await direct.get(ledger_key(prefix, "req-1"))
    settled = edax._edax.redis_settled_value(
        won.handle, edax.command.encode_record(Record(HASH, result="charged"))
    )
    assert held in (None, settled), f"the key holds neither nothing nor the outcome: {held!r}"
    replay = await store.claim("req-1", HASH)
    assert isinstance(replay, Won) or replay == Record(HASH, result="charged")

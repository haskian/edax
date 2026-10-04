"""Redis-backed idempotency store: a cross-process :class:`~edax.command.IdempotencyStore` over a one-method
async ``RedisConn`` seam that runs the kernel's Lua. The kernel owns every decision: the key, the stored
value, the scripts, what a reply means. This adds the I/O, the TTLs and a per-call deadline.
:class:`AsyncRedisClient` adapts a caller-owned ``redis.asyncio`` client, so edax pulls no redis dependency.
See ``docs/backends/redis.md``."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, cast

from .. import _edax, errors
from ..command import Claimed, Record, Won, decode_prior, encode_record
from ..errors import BadHandle, CodecError, EdaxError, StoreError
from ..store import Tx

if TYPE_CHECKING:
    from redis.asyncio import Redis, RedisCluster

logger = logging.getLogger("edax.backend.redis")

# The kernel's scripts, byte for byte, so every SDK's EVALSHA names the same digest.
_SCRIPTS = _edax.redis_scripts()
CLAIM_SCRIPT: str = _SCRIPTS.claim
COMPLETE_SCRIPT: str = _SCRIPTS.complete
RELEASE_SCRIPT: str = _SCRIPTS.release
EXTEND_SCRIPT: str = _SCRIPTS.extend
FORGET_SCRIPT: str = _SCRIPTS.forget

# The script each store operation runs, by the operation's name.
_Script = Literal["claim", "complete", "release", "extend", "forget"]
_SCRIPT_OF: dict[_Script, str] = {
    "claim": CLAIM_SCRIPT,
    "complete": COMPLETE_SCRIPT,
    "release": RELEASE_SCRIPT,
    "extend": EXTEND_SCRIPT,
    "forget": FORGET_SCRIPT,
}
#: What the store sends: a kernel script, or the plain read a forget starts with.
_Op = Literal[_Script, "get"]

# How many times a forget reads the key again after the key changed under it: the kernel's.
_FORGET_READS: int = _edax.redis_forget_reads()

# How many times per claim TTL the heartbeat renews a held claim: the kernel's, as its check is.
_HEARTBEATS_PER_TTL: int = _edax.redis_heartbeats_per_ttl()

#: A script argument: a value's bytes, or a TTL in milliseconds.
ScriptArg = bytes | int


class RedisConn(Protocol):
    """The two Redis operations the store needs. ``eval`` runs a kernel script: ``script`` is one of the
    ones above, sent as given (``EVALSHA`` of it, the source once on ``NOSCRIPT``), never rewritten, so
    every connection runs the kernel's bytes and none re-derives the fence. The reply is the script's:
    ``None``, bytes (or ``str`` from a client that decodes replies), or an integer. ``get`` is a plain
    ``GET`` of one key, which only :meth:`RedisIdempotencyStore.forget` sends.

    Raise :class:`~edax.errors.EdaxError` for a failure you can classify (:class:`AsyncRedisClient` asks
    the kernel's ``redis_reply_error`` about a Redis error reply); anything else the store types as transient
    :class:`~edax.errors.StoreError`."""

    async def eval(self, script: str, keys: Sequence[bytes], args: Sequence[ScriptArg]) -> Any: ...
    async def get(self, key: bytes) -> Any: ...


class RedisIdempotencyStore:
    """A cross-process ``IdempotencyStore`` backed by Redis: one key per idempotency key, so a redelivery
    within the TTL replays the cached outcome. ``tx`` is accepted and ignored, since Redis cannot join the
    caller's unit of work, so :attr:`joins_transaction` is ``False`` and the guard compensates by hand:
    at-least-once over an idempotent handler, never effectively-once.

    ``claim`` returns a :class:`~edax.command.Won` whose handle is the exact value the claim wrote.
    ``complete``, ``release`` and :meth:`extend` write only while the key still holds that value, so a
    worker whose claim lapsed and was taken over settles nothing, and a release never deletes a settled
    record. That fences the ledger only: a handler's own side effects need a version check of their own.

    ``ttl_ms`` is the dedup window for a terminal record and must outlive the longest redelivery window.
    ``claim_ttl_ms`` bounds how long a pending claim blocks its key when its worker dies. Without
    ``heartbeat`` it must exceed the longest handler run; with it, the store renews each claim every
    ``claim_ttl_ms / 3`` while it is held. ``op_timeout_ms`` is the deadline on each Redis call, client
    retries included. A misconfigured store raises terminal :class:`~edax.errors.BadHandle` when built. A
    client or server failure raises transient :class:`~edax.errors.StoreError`, a deterministic one
    (``WRONGTYPE``, a refused command) terminal. A stored value no reader can decode is the kernel's
    terminal ``codec`` failure, which the guard replays as :class:`~edax.errors.CachedFailure`. See
    ``docs/backends/redis.md``."""

    # Redis cannot enlist in another database's transaction, so the guard must compensate by hand.
    joins_transaction: ClassVar[bool] = False

    def __init__(
        self,
        conn: RedisConn,
        prefix: str,
        ttl_ms: int = 86_400_000,
        claim_ttl_ms: int = 300_000,
        *,
        op_timeout_ms: int = 5_000,
        heartbeat: bool = False,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """``prefix`` is required and has no default: it is part of the ledger's identity, not a
        cosmetic namespace. Two services sharing one Redis under one prefix share a key space, and a
        request id both use is one claim. Name the service and the environment,
        ``"billing:prod:idem:"``. A second SDK's store must use the same prefix to share the ledger.
        ``sleep`` is how the heartbeat waits between renewals; a test steps it instead of sleeping."""
        for name, value in (
            ("ttl_ms", ttl_ms),
            ("claim_ttl_ms", claim_ttl_ms),
            ("op_timeout_ms", op_timeout_ms),
        ):
            _check_whole_ms(name, value)
        with errors.mapped():
            _edax.redis_check_config(prefix, ttl_ms, claim_ttl_ms, op_timeout_ms, heartbeat)
        self._conn = conn
        self._prefix = prefix
        self._ttl_ms = ttl_ms
        self._claim_ttl_ms = claim_ttl_ms
        self._op_timeout_s = op_timeout_ms / 1000
        self._heartbeat = heartbeat
        # Keyed by claim handle, never by ledger key: two claims on one key in one process are two
        # handles, so one can never stop or replace the other's renewal.
        self._renewals: dict[bytes, asyncio.Task[None]] = {}
        self._sleep = sleep

    def _key(self, key: str) -> bytes:
        with errors.mapped():
            return _edax.redis_ledger_key(self._prefix, key)

    async def _eval(self, op: _Script, key: str, args: Sequence[ScriptArg]) -> Any:
        """``op``'s script on ``key``'s ledger key, under the store's deadline."""
        return await self._send(op, key, lambda rkey: self._conn.eval(_SCRIPT_OF[op], [rkey], args))

    async def _send(self, op: _Op, key: str, call: Callable[[bytes], Awaitable[Any]]) -> Any:
        """``call`` on ``key``'s ledger key, under the store's deadline. A failure the connection
        classified keeps its class; any other is the seam contract's transient ``StoreError``. Each names
        the operation and the key."""
        try:
            return await asyncio.wait_for(call(self._key(key)), self._op_timeout_s)
        except EdaxError as e:
            raise type(e)(f"redis {op} {key!r}: {e}") from e
        except asyncio.TimeoutError as e:
            raise StoreError(f"redis {op} {key!r}: no reply within {self._op_timeout_s * 1000:.0f} ms") from e
        except Exception as e:
            raise StoreError(f"redis {op} {key!r}: {e}") from e

    async def claim(self, key: str, payload_hash: int, tx: Tx | None = None) -> Claimed:
        """Atomically claim ``key`` with a pending record under ``claim_ttl_ms``. :class:`~edax.command.Won`
        if this caller holds the claim, else the existing :class:`~edax.command.Record`."""
        with errors.mapped():
            # Building the value is the kernel's, not the client's: a hash it refuses is a terminal
            # CodecError, never the transient StoreError an I/O failure gets.
            claim = _edax.redis_new_claim(payload_hash)
        reply = await self._eval("claim", key, [claim, self._claim_ttl_ms])
        with errors.mapped():
            # A reply equal to this claim is its own first send, landed with its reply lost: won. A value
            # the kernel cannot read comes back as its unreadable record, a terminal codec failure.
            prior = _edax.redis_claim_outcome(claim, _reply_bytes(reply))
        if prior is not None:
            return decode_prior(prior, payload_hash)  # replayed as cached, never re-run
        if self._heartbeat:
            self._renewals[claim] = asyncio.create_task(self._renew(key, claim))
        return Won(claim)

    async def complete(self, key: str, handle: bytes, record: Record, tx: Tx | None = None) -> None:
        """Write the terminal record for ``key`` under ``ttl_ms``, replacing the claim ``handle`` names and
        only that one. A claim already taken over is reported, not overwritten: another worker owns it."""
        try:
            # Encoded and framed before any I/O: an unencodable result or a handle the kernel never
            # minted is a terminal CodecError, and a transient verdict would redeliver it forever.
            body = encode_record(record)
            with errors.mapped():
                value = _edax.redis_settled_value(handle, body)
            written = await self._eval("complete", key, [handle, value, self._ttl_ms])
        finally:
            self._stop_renewing(handle)
        if not written:
            # Loud on purpose: the outcome was not recorded, so the caller must not report success.
            raise StoreError(
                f"redis complete {key!r}: the claim expired and was taken over, "
                "so the outcome was not recorded"
            )

    async def release(self, key: str, handle: bytes, tx: Tx | None = None) -> None:
        """Delete ``key`` while it still holds the claim ``handle`` names. A release that arrives after the
        claim lapsed, or after its own ``complete`` landed with the reply lost, deletes nothing."""
        try:
            with errors.mapped():
                _edax.redis_check_claim(handle)
            await self._eval("release", key, [handle])
        finally:
            self._stop_renewing(handle)

    async def extend(self, key: str, handle: bytes) -> bool:
        """Reset the claim's TTL to ``claim_ttl_ms`` while the key still holds the claim ``handle`` names.
        ``False`` means the lease is gone: the claim lapsed, or another worker took the key over. Renewal
        narrows the window in which a paused worker loses its claim; it does not close it, which only a
        check by the downstream resource can do (``docs/backends/redis.md``)."""
        with errors.mapped():
            _edax.redis_check_claim(handle)
        return bool(await self._eval("extend", key, [handle, self._claim_ttl_ms]))

    async def forget(self, key: str, tx: Tx | None = None) -> None:
        """Delete the settled record ``key`` holds, so the next claim wins and a re-driven command runs
        again. The store reads the value, and the kernel decides from it: a pending claim raises
        :class:`~edax.errors.InProgress`, a free key is a no-op, and anything else is deleted, a value no
        reader can decode included. The delete runs only while the key still holds the value read. When the
        key changed in between (the record lapsed and a new claim took it), the store reads it again and
        the kernel decides again. A key that keeps changing raises transient
        :class:`~edax.errors.StoreError` after a few rounds."""
        for _ in range(_FORGET_READS):
            held = _reply_bytes(await self._send("get", key, self._conn.get))
            with errors.mapped():
                deletes = _edax.redis_forget_outcome(key, held)
            if not deletes:
                return
            if await self._eval("forget", key, [cast(bytes, held)]):
                return
        raise StoreError(f"redis forget {key!r}: the key changed on every one of {_FORGET_READS} reads")

    async def _renew(self, key: str, handle: bytes) -> None:
        """The heartbeat: extend the claim every third of its TTL until it is settled or refused. A
        transient failure is retried on the next beat, since one missed beat still leaves a third of the
        lease; a terminal one would fail every beat, so it stops the renewal."""
        while True:
            await self._sleep(self._claim_ttl_ms / _HEARTBEATS_PER_TTL / 1000)
            try:
                renewed = await self.extend(key, handle)
            except EdaxError as e:
                logger.warning("edax redis: renewing the claim on %r failed: %s", key, e)
                if errors.is_terminal(e):
                    return
                continue
            if not renewed:
                logger.warning("edax redis: the claim on %r was lost before it settled; renewal stopped", key)
                return

    def _stop_renewing(self, handle: bytes) -> None:
        task = self._renewals.pop(handle, None)
        if task is not None:
            task.cancel()


class AsyncRedisClient:
    """A ``RedisConn`` over a caller-owned ``redis.asyncio.Redis`` or ``RedisCluster`` (redis-py 8 or later).
    Each kernel script is registered once, so a call sends its digest, not its source. A Redis error reply
    is classified by the kernel (``WRONGTYPE`` terminal, ``READONLY`` transient); a connection failure or a
    timeout is left to the store, which types it transient. redis-py re-sends a command whose reply was
    lost; every kernel script answers the re-send as it answered the first."""

    def __init__(self, client: Redis | RedisCluster) -> None:
        if not inspect.iscoroutinefunction(getattr(client, "execute_command", None)):
            # A sync client's calls return values, not awaitables: every call would fail the same way.
            raise BadHandle(f"AsyncRedisClient needs a redis.asyncio client, got {type(client).__name__}")
        from redis.exceptions import AuthenticationError, ResponseError

        self._client = client
        # AuthenticationError is a ConnectionError in redis-py, but a wrong password fails every time.
        self._replies: tuple[type[Exception], ...] = (ResponseError, AuthenticationError)
        self._scripts: dict[str, Any] = {}

    async def eval(self, script: str, keys: Sequence[bytes], args: Sequence[ScriptArg]) -> Any:
        registered = self._scripts.get(script)
        if registered is None:
            registered = self._scripts[script] = self._client.register_script(script)
        return await self._classified(registered(keys=list(keys), args=list(args)))

    async def get(self, key: bytes) -> Any:
        return await self._classified(self._client.get(key))

    async def _classified(self, reply: Awaitable[Any]) -> Any:
        try:
            return await reply
        except self._replies as e:
            raise errors.from_wire(_edax.redis_reply_error(_reply_text(e))) from e
        except UnicodeDecodeError as e:
            # A client built with `decode_responses=True` cannot decode a value that is not UTF-8.
            raise CodecError(f"redis reply is not UTF-8: {e}") from e


def _check_whole_ms(name: str, value: Any) -> None:
    """Refuse a ``value`` that is not the integer the kernel checks. A float or a bool is a configuration
    mistake, refused here because the binding would convert a bool silently."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise BadHandle(f"{name} must be a whole number of milliseconds, got {value!r}")


def _reply_bytes(reply: Any) -> bytes | None:
    """A script reply as the bytes the kernel reads. A client opened with ``decode_responses=True`` answers
    ``str``; everything this store writes is UTF-8, so encoding it back is lossless."""
    if reply is None or isinstance(reply, bytes):
        return reply
    return reply.encode() if isinstance(reply, str) else bytes(reply)


def _reply_text(e: Exception) -> str:
    """The Redis error reply, prefix included. redis-py strips the prefix it maps to an exception class
    and keeps it in ``status_code``; an unmapped prefix stays in the message."""
    status = getattr(e, "status_code", None)
    return f"{status} {e}" if status else str(e)

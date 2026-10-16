"""Redis-backed idempotency store: a cross-process :class:`~edax.idempotency.IdempotencyStore` over a
one-method async ``RedisConn`` seam that runs the kernel's Lua. The kernel owns every decision: the key, the
stored value, the scripts, what a reply means. This adds the I/O, the TTLs and a per-call deadline.
:class:`AsyncRedisClient` adapts a caller-owned ``redis.asyncio`` client, so edax pulls no redis dependency.
See ``docs/backends/redis.md``."""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, cast, get_args

from .. import _native, errors
from .._deadline import within
from .._observer import wrap
from .._units import own_write
from .._wiring import wiring_int
from ..errors import BadHandle, CodecError, EdaxError, StoreError
from ..idempotency import Claimed, Record, Won, decode_claim, encode_record
from ..observe import Observer
from ..unit_of_work import Tx

if TYPE_CHECKING:
    from redis.asyncio import Redis, RedisCluster

logger = logging.getLogger("edax.backend.redis")

# The script each store operation runs, by the operation's name: the kernel's, byte for byte, so every
# SDK's EVALSHA names the same digest. Read off the kernel's struct by field name, so this is the one table.
_Script = Literal["claim", "complete", "release", "extend", "forget"]
_SCRIPT_OF: dict[_Script, str] = {name: getattr(_native.redis_scripts(), name) for name in get_args(_Script)}
CLAIM_SCRIPT: str = _SCRIPT_OF["claim"]
COMPLETE_SCRIPT: str = _SCRIPT_OF["complete"]
RELEASE_SCRIPT: str = _SCRIPT_OF["release"]
EXTEND_SCRIPT: str = _SCRIPT_OF["extend"]
FORGET_SCRIPT: str = _SCRIPT_OF["forget"]
#: What the store sends: a kernel script, or the plain read a forget starts with.
_Op = Literal[_Script, "get"]

# How many times a forget reads the key again after the key changed under it: the kernel's.
_FORGET_READS: int = _native.redis_forget_reads()
# The settings a store takes when its host overrides none: the kernel's, so two SDKs on one ledger left at
# their defaults agree on its windows.
_DEFAULTS = _native.redis_defaults()

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

    ``claim`` returns a :class:`~edax.idempotency.Won` whose handle is the exact value the claim wrote.
    ``complete``, ``release`` and :meth:`extend` write only while the key still holds that value, so a
    worker whose claim lapsed and was taken over settles nothing, and a release never deletes a settled
    record. That fences the ledger only: a handler's own side effects need a version check of their own.

    ``ttl_ms`` is the dedup window for a terminal record and must outlive the longest redelivery window.
    ``claim_ttl_ms`` bounds how long a pending claim blocks its key when its worker dies. Without
    ``heartbeat`` it must exceed the longest handler run; with it, the store renews each claim every
    ``claim_ttl_ms / 3`` while it is held. ``op_timeout_ms`` is the deadline on each Redis call, client
    retries included. With ``heartbeat`` it must be under ``claim_ttl_ms / 3``, so a renewal that stalls
    for the whole deadline still leaves the next one time to land. A misconfigured store raises terminal
    :class:`~edax.errors.BadHandle` when built. A client or server failure raises transient
    :class:`~edax.errors.StoreError`, a deterministic one (``WRONGTYPE``, a refused command) terminal. A
    stored value no reader can decode is the kernel's terminal ``codec`` failure, which the guard replays
    as :class:`~edax.errors.CachedFailure`. See ``docs/backends/redis.md``."""

    # Redis cannot enlist in another database's transaction, so the guard must compensate by hand.
    joins_transaction: ClassVar[bool] = False

    def __init__(
        self,
        conn: RedisConn,
        prefix: str,
        ttl_ms: int = _DEFAULTS.ttl_ms,
        claim_ttl_ms: int = _DEFAULTS.claim_ttl_ms,
        *,
        op_timeout_ms: int = _DEFAULTS.op_timeout_ms,
        heartbeat: bool = False,
        observer: Observer | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """``prefix`` is required and has no default: it is part of the ledger's identity, not a
        cosmetic namespace. Two services sharing one Redis under one prefix share a key space, and a
        request id both use is one claim. Name the service and the environment,
        ``"billing:prod:idem:"``. A second SDK's store must use the same prefix to share the ledger.
        ``observer`` receives :meth:`~edax.observe.Observer.lease_lost` when the heartbeat stops renewing
        a claim it was meant to hold; ``None`` is the no-op observer.
        ``sleep`` and ``clock`` are how the heartbeat waits and tells time. ``clock`` defaults to the
        running event loop's monotonic ``time()``. A test steps both instead of sleeping."""
        # The binding would convert a bool silently, and a float is no whole number of milliseconds.
        for name, value in (
            ("ttl_ms", ttl_ms),
            ("claim_ttl_ms", claim_ttl_ms),
            ("op_timeout_ms", op_timeout_ms),
        ):
            wiring_int(value, name)
        _native.redis_check_config(prefix, ttl_ms, claim_ttl_ms, op_timeout_ms, heartbeat)
        self._conn = conn
        self._prefix = prefix
        self._ttl_ms = ttl_ms
        self._claim_ttl_ms = claim_ttl_ms
        self._op_timeout_s = op_timeout_ms / 1000
        self._heartbeat = heartbeat
        self._observer = wrap(observer)
        # Keyed by claim handle, never by ledger key: two claims on one key in one process are two
        # handles, so one can never stop or replace the other's renewal.
        self._renewals: dict[bytes, asyncio.Task[None]] = {}
        self._sleep = sleep
        self._clock = clock

    def _key(self, key: str) -> bytes:
        return _native.redis_ledger_key(self._prefix, key)

    async def _eval(self, op: _Script, key: str, args: Sequence[ScriptArg]) -> Any:
        """``op``'s script on ``key``'s ledger key, under the store's deadline."""
        return await self._send(op, key, lambda rkey: self._conn.eval(_SCRIPT_OF[op], [rkey], args))

    async def _send(self, op: _Op, key: str, call: Callable[[bytes], Awaitable[Any]]) -> Any:
        """``call`` on ``key``'s ledger key, under the store's deadline. A failure the connection
        classified is re-raised as it is; any other, a missed deadline included, is the seam contract's
        transient ``StoreError``. Each names the operation and the key."""
        what = f"redis {op} {key!r}"

        async def sent() -> Any:
            try:
                return await call(self._key(key))
            except EdaxError as e:
                # Re-raised, never rebuilt: a rebuilt error loses what its class carries (a cached
                # failure's original code), and a subclass may not take a message alone.
                _add_note(e, what)
                raise
            except Exception as e:
                raise StoreError(f"{what}: {e}") from e

        return await within(sent(), self._op_timeout_s, what)

    async def claim(self, key: str, payload_hash: int, tx: Tx | None = None) -> Claimed:
        """Atomically claim ``key`` with a pending record under ``claim_ttl_ms``.
        :class:`~edax.idempotency.Won` if this caller holds the claim, else the existing
        :class:`~edax.idempotency.Record`."""
        # Building the value is the kernel's, not the client's: a hash it refuses is a terminal
        # CodecError, never the transient StoreError an I/O failure gets.
        claim = _native.redis_new_claim(payload_hash)
        # Read before the send: the claim's lease runs from when it lands, never earlier.
        sent_at = self._now()
        async with own_write():
            reply = await self._eval("claim", key, [claim, self._claim_ttl_ms])
        # A reply equal to this claim is its own first send, landed with its reply lost: won. A value
        # the kernel cannot read comes back as its unreadable record, a terminal codec failure.
        claimed = decode_claim(_native.redis_claim_outcome(claim, _reply_bytes(reply)), payload_hash)
        if isinstance(claimed, Won) and self._heartbeat:
            self._renewals[claimed.handle] = asyncio.create_task(self._renew(key, claimed.handle, sent_at))
        return claimed

    async def complete(self, key: str, handle: bytes, record: Record, tx: Tx | None = None) -> None:
        """Write the terminal record for ``key`` under ``ttl_ms``, replacing the claim ``handle`` names and
        only that one. A claim already taken over is reported, not overwritten: another worker owns it."""
        # Encoded and framed before any I/O: an unencodable result or a handle the kernel never minted is
        # a terminal CodecError, and a transient verdict would redeliver it forever. A pending record is
        # the kernel's BadHandle, refused before the renewal stops, so the claim stays settleable.
        body = encode_record(record)
        value = _native.redis_settled_value(handle, body)
        # Stopped before the call, not after it: a beat while the reply is late would find the settled
        # value and report the claim lost.
        self._stop_renewing(handle)
        async with own_write():
            written = await self._eval("complete", key, [handle, value, self._ttl_ms])
        # Loud on purpose: a refused write recorded no outcome, so the caller must not report success.
        _native.redis_complete_outcome(key, bool(written))

    async def release(self, key: str, handle: bytes, tx: Tx | None = None) -> None:
        """Delete ``key`` while it still holds the claim ``handle`` names. A release that arrives after the
        claim lapsed, or after its own ``complete`` landed with the reply lost, deletes nothing."""
        self._stop_renewing(handle)  # before the call, as in `complete`
        _native.redis_check_claim(handle)
        await self._eval("release", key, [handle])

    async def extend(self, key: str, handle: bytes) -> bool:
        """Reset the claim's TTL to ``claim_ttl_ms`` while the key still holds the claim ``handle`` names.
        ``False`` means the lease is gone: the claim lapsed, or another worker took the key over. Renewal
        narrows the window in which a paused worker loses its claim; it does not close it, which only a
        check by the downstream resource can do (``docs/backends/redis.md``)."""
        _native.redis_check_claim(handle)
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
            deletes = _native.redis_forget_outcome(key, held)
            if not deletes:
                return
            if await self._eval("forget", key, [cast(bytes, held)]):
                return
        raise errors.from_wire(_native.redis_forget_exhausted(key))

    def _now(self) -> float:
        return self._clock() if self._clock is not None else asyncio.get_running_loop().time()

    async def _renew(self, key: str, handle: bytes, sent_at: float) -> None:
        """The heartbeat: extend the claim every third of its TTL until it is settled or refused. Beat
        ``k`` is sent at ``sent_at + k * claim_ttl / 3``, ``sent_at`` being when the claim was sent, so a
        slow reply never delays the next beat. A beat sent at ``S`` that lands holds the lease to at least
        ``S + claim_ttl``. A transient failure is retried on the next beat, sent at ``S + 2 * claim_ttl / 3``:
        the kernel keeps ``op_timeout_ms`` under a third of the TTL, so it lands before the lease lapses. A
        terminal failure would fail every beat, so it stops the renewal. The kernel times each beat; the
        send time is floored to a millisecond, so a beat is never late."""
        sent_at_ms = math.floor(sent_at * 1000)
        beat = 0
        while True:
            beat += 1
            due = _native.redis_heartbeat_due_ms(sent_at_ms, self._claim_ttl_ms, beat) / 1000
            await self._sleep(max(0.0, due - self._now()))
            try:
                renewed = await self.extend(key, handle)
            except EdaxError as e:
                logger.warning("edax redis: renewing the claim on %r failed: %s", key, e)
                if errors.is_terminal(e):
                    # The lease now runs out on its own while the handler may still be running.
                    self._observer.lease_lost(key, e)
                    return
                continue
            if not renewed:
                logger.warning("edax redis: the claim on %r was lost before it settled; renewal stopped", key)
                self._observer.lease_lost(key, None)
                return

    def _stop_renewing(self, handle: bytes) -> None:
        task = self._renewals.pop(handle, None)
        if task is not None:
            task.cancel()


def _add_note(error: BaseException, note: str) -> None:
    """Name the call that raised ``error`` in its traceback, on a Python that has notes (3.11 and later)."""
    add_note = getattr(error, "add_note", None)
    if add_note is not None:
        add_note(note)


class AsyncRedisClient:
    """A ``RedisConn`` over a caller-owned ``redis.asyncio.Redis`` or ``RedisCluster`` (redis-py 8 or later).
    Each kernel script is registered once, so a call sends its digest, not its source. A Redis error reply
    is classified by the kernel (``WRONGTYPE`` terminal, ``READONLY`` transient); a connection failure or a
    timeout is left to the store, which types it transient. redis-py 8 re-sends a command whose reply was
    lost by default: its ``Retry`` re-sends on a ``ConnectionError`` or a ``TimeoutError``, with jittered
    exponential backoff, up to 10 times on ``Redis`` and on ``RedisCluster``. Every kernel script answers
    the re-send as it answered the first, so a client built with another ``retry=``, or none, stays
    correct: the store's own deadline and the caller's retry cover it."""

    def __init__(self, client: Redis | RedisCluster) -> None:
        if not inspect.iscoroutinefunction(getattr(client, "execute_command", None)):
            # A sync client's calls return values, not awaitables: every call would fail the same way.
            raise BadHandle(f"AsyncRedisClient needs a redis.asyncio client, got {type(client).__name__}")
        from redis.exceptions import AuthenticationError, ResponseError

        self._client = client
        # AuthenticationError is a ConnectionError in redis-py, but a wrong password fails every time.
        self._replies: tuple[type[Exception], ...] = (ResponseError, AuthenticationError)
        # Each registered script is called with its keys and args and answers a reply to await.
        self._scripts: dict[str, Callable[..., Awaitable[Any]]] = {}

    async def eval(self, script: str, keys: Sequence[bytes], args: Sequence[ScriptArg]) -> Any:
        registered = self._scripts.get(script)
        if registered is None:
            registered = self._scripts[script] = self._client.register_script(script)
        return await self._classified(registered(keys=list(keys), args=list(args)))

    async def get(self, key: bytes) -> Any:
        # Raw bytes even from a client built with `decode_responses=True`: a forget must read a value that
        # is not UTF-8 to delete it, and its compare-and-delete sends the same bytes back.
        return await self._classified(self._client.execute_command("GET", key, NEVER_DECODE=[]))

    async def _classified(self, reply: Awaitable[Any]) -> Any:
        try:
            return await reply
        except self._replies as e:
            raise errors.from_wire(_native.redis_reply_error(_reply_text(e))) from e
        except UnicodeDecodeError as e:
            # A client built with `decode_responses=True` cannot decode a value that is not UTF-8.
            raise CodecError(f"redis reply is not UTF-8: {e}") from e


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

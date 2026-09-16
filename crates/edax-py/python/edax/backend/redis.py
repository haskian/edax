"""Redis-backed idempotency store: a cross-process :class:`~edax.command.IdempotencyStore` over a small async
``RedisConn`` seam. The kernel owns the verdict; this adds the I/O only. :class:`AsyncRedisClient` adapts a
caller-owned ``redis.asyncio.Redis``, so edax pulls no redis dependency. See ``docs/backends/redis.md``."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, ClassVar, Optional, Protocol

from .. import _edax, errors
from ..command import Record, decode_record, encode_record
from ..errors import StoreError
from ..store import Tx

# Bounds the SET NX -> GET retry below. A loop with no ceiling would spin instead of failing.
_CLAIM_ATTEMPTS = 3

# The stored value is `<owner><record>`: a fixed-width hex fence token, then the kernel's record JSON. That
# layout, the token's width and the two Lua fence scripts are the `edax-redis` kernel's, pinned by its
# golden vectors, so a second SDK's Redis store shares the ledger by calling the same functions. One key, so
# one TTL governs both halves; a sibling key would let a claimant win the record, lose the owner, and hold a
# claim it could never settle.


@contextmanager
def _as_store_error(op: str, key: str) -> Iterator[None]:
    """Client failures are backend failures, which the seam contract types as ``StoreError``."""
    try:
        yield
    except Exception as e:
        raise StoreError(f"redis {op} {key!r}: {e}") from e


class RedisConn(Protocol):
    """The Redis operations the store needs. ``set_nx_px`` is the atomic claim (``SET key value NX PX ttl``);
    ``get`` / ``set_px`` / ``delete`` are plain. The two ``*_if_prefix`` operations are the fence and must be
    atomic, one ``EVAL`` on Redis. Both return ``False`` when the key is absent or carries a different
    prefix, which is how a caller learns it lost its claim."""

    async def get(self, key: str) -> Optional[bytes]: ...
    async def set_px(self, key: str, value: bytes, ttl_ms: int) -> None: ...
    async def set_nx_px(self, key: str, value: bytes, ttl_ms: int) -> bool: ...
    async def delete(self, key: str) -> None: ...
    async def set_px_if_prefix(self, key: str, prefix: bytes, value: bytes, ttl_ms: int) -> bool: ...
    async def delete_if_prefix(self, key: str, prefix: bytes) -> bool: ...


class RedisIdempotencyStore:
    """A cross-process ``IdempotencyStore`` backed by Redis: records stored as JSON under ``{prefix}{key}``,
    so a redelivery within the TTL replays the cached outcome. ``tx`` is accepted and ignored, since Redis
    cannot join the caller's unit of work, so :attr:`joins_transaction` is ``False`` and the guard
    compensates by hand: at-least-once over an idempotent handler, never effectively-once. A client failure
    raises transient :class:`~edax.errors.StoreError`, an undecodable record terminal
    :class:`~edax.errors.CodecError`. ``ttl_ms`` is the dedup window for a terminal record and must outlive
    the longest redelivery window; ``claim_ttl_ms`` bounds how long a pending claim blocks its key when its
    worker dies and must exceed the longest handler run. ``complete`` and ``release`` write only if the key
    still carries this caller's owner token, which fences the ledger only: a handler's own side effects need
    a version check of their own. See ``docs/backends/redis.md``."""

    # Redis cannot enlist in another database's transaction, so the guard must compensate by hand.
    joins_transaction: ClassVar[bool] = False

    def __init__(
        self,
        conn: RedisConn,
        prefix: str,
        ttl_ms: int = 86_400_000,
        claim_ttl_ms: int = 300_000,
    ) -> None:
        """``prefix`` is required and has no default: it is part of the ledger's identity, not a
        cosmetic namespace. Two services sharing one Redis under one prefix share a key space, and a
        request id both use is one claim. Name the service and the environment,
        ``"billing:prod:idem:"``. A second SDK's store must use the same prefix to share the ledger."""
        self._conn = conn
        self._prefix = prefix
        self._ttl_ms = ttl_ms
        self._claim_ttl_ms = claim_ttl_ms
        # Fence tokens for the claims this instance holds. A key absent here was never won by this caller,
        # so it has nothing to settle and must not write.
        self._owned: dict[str, str] = {}

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    async def claim(self, key: str, payload_hash: int, tx: Optional[Tx] = None) -> Optional[Record]:
        """Atomically claim ``key`` with a pending record under ``claim_ttl_ms``. ``None`` if this caller
        won, else the existing :class:`~edax.command.Record`."""
        rkey = self._key(key)
        owner = _edax.redis_owner()
        pending = _edax.redis_claim_value(owner, payload_hash).encode()
        for _ in range(_CLAIM_ATTEMPTS):
            with _as_store_error("claim", key):
                if await self._conn.set_nx_px(rkey, pending, self._claim_ttl_ms):
                    self._owned[key] = owner
                    return None
                raw = await self._conn.get(rkey)
            if raw is not None:
                # A client opened with `decode_responses=True` answers text; the kernel splits bytes.
                stored = raw.encode() if isinstance(raw, str) else bytes(raw)
                with errors.mapped():
                    # A value shorter than the owner token, or not UTF-8, is a terminal CodecError:
                    # the same verdict an unreadable record gets, not a retried transient failure.
                    record_json = _edax.redis_split_value(stored).record
                # `decode_record` types an unreadable record as a terminal CodecError.
                return decode_record(record_json)
            # TTL expiry or a release dropped the winner's record between SET NX and GET. Nothing to
            # replay, so retry the claim rather than report "no prior".
        raise StoreError(f"redis claim {key!r}: record vanished on {_CLAIM_ATTEMPTS} attempts")

    async def complete(self, key: str, record: Record, tx: Optional[Tx] = None) -> None:
        """Write the terminal record for ``key`` under ``ttl_ms``, replacing this caller's pending claim and
        only that one. A claim already taken over is reported, not overwritten: another worker owns it."""
        # Encode first: a record that will not serialize is a terminal CodecError, and a transient verdict
        # would redeliver the same unencodable result forever.
        body = encode_record(record)
        owner = self._owned.get(key)
        if owner is None:
            # No claim of this caller's to fence against: a direct write, not a guarded dispatch.
            with _as_store_error("complete", key):
                value = _edax.redis_record_value(_edax.redis_owner(), body).encode()
                await self._conn.set_px(self._key(key), value, self._ttl_ms)
            return
        with _as_store_error("complete", key):
            fenced = await self._conn.set_px_if_prefix(
                self._key(key),
                owner.encode(),
                _edax.redis_record_value(owner, body).encode(),
                self._ttl_ms,
            )
        if not fenced:
            # Loud on purpose: the outcome was not recorded, so the caller must not report success.
            # The token is kept: the guard answers a refused `complete` with `release`, which must stay
            # fenced or its plain DEL would delete the claim that replaced this one. `release` drops the
            # token once its fenced DEL has run, a no-op against the new holder's prefix.
            raise StoreError(
                f"redis complete {key!r}: the claim expired and was taken over, "
                "so the outcome was not recorded"
            )
        self._owned.pop(key, None)

    async def release(self, key: str, tx: Optional[Tx] = None) -> None:
        """Delete the key, releasing a claim. Fenced while this caller holds the claim, so a release
        arriving after ``claim_ttl_ms`` expired cannot remove the claim that replaced it. Holding none, it
        is the seam's plain unconditional ``DEL``."""
        owner = self._owned.get(key)
        with _as_store_error("release", key):
            if owner is None:
                await self._conn.delete(self._key(key))
                return
            await self._conn.delete_if_prefix(self._key(key), owner.encode())
        # Dropped only once the DEL landed. Popping first would leave a failed DEL with the claim still in
        # Redis and this caller without its token, so the retry would issue an unfenced DEL.
        self._owned.pop(key, None)


class AsyncRedisClient:
    """A ``RedisConn`` over a caller-owned ``redis.asyncio.Redis``. The per-call timeout is the client's, not
    this adapter's: set ``socket_timeout`` and ``socket_connect_timeout`` on it, or a dispatch blocks on the
    claim for as long as the socket hangs."""

    def __init__(self, client: Any) -> None:
        self._client = client
        # Registered once: EVALSHA per call, source only when the server reports NOSCRIPT. Both bodies
        # are the kernel's, so every SDK's EVALSHA hashes to the same script.
        self._set_if_prefix = client.register_script(_edax.redis_fenced_set_script())
        self._del_if_prefix = client.register_script(_edax.redis_fenced_del_script())

    async def get(self, key: str) -> Optional[bytes]:
        return await self._client.get(key)

    async def set_px(self, key: str, value: bytes, ttl_ms: int) -> None:
        await self._client.set(key, value, px=ttl_ms)

    async def set_nx_px(self, key: str, value: bytes, ttl_ms: int) -> bool:
        # redis-py returns True when set, None when NX failed.
        return bool(await self._client.set(key, value, px=ttl_ms, nx=True))

    async def delete(self, key: str) -> None:
        await self._client.delete(key)

    async def set_px_if_prefix(self, key: str, prefix: bytes, value: bytes, ttl_ms: int) -> bool:
        return bool(await self._set_if_prefix(keys=[key], args=[prefix, value, ttl_ms]))

    async def delete_if_prefix(self, key: str, prefix: bytes) -> bool:
        return bool(await self._del_if_prefix(keys=[key], args=[prefix]))

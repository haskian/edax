"""Fault injection for the Redis backend, :class:`~edax.backend.redis.RedisIdempotencyStore`."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ..backend.redis import RedisConn, ScriptArg

__all__ = ["LostReply"]


class LostReply:
    """A Redis connection wrapper (the ``eval`` seam of :class:`~edax.backend.redis.RedisIdempotencyStore`)
    that loses the reply to the next ``eval`` of ``script``, once: the script runs on the server, then the
    caller gets a :class:`ConnectionError`. ``resend`` models redis-py's default retry, which sends the same
    command again on a new connection and returns that reply instead."""

    def __init__(self, inner: RedisConn, script: str, resend: bool) -> None:
        self._inner = inner
        self._script = script
        self._resend = resend
        #: Whether the fault is still to fire.
        self.armed = True

    async def eval(self, script: str, keys: Sequence[bytes], args: Sequence[ScriptArg]) -> Any:
        reply = await self._inner.eval(script, keys, args)
        if self.armed and script == self._script:
            self.armed = False
            if self._resend:
                return await self._inner.eval(script, keys, args)
            raise ConnectionError("connection reset by peer while reading the reply")
        return reply

    async def get(self, key: bytes) -> Any:
        """A read loses nothing, so it passes through."""
        return await self._inner.get(key)

"""Fault injection over an arbitrary backend.

The in-tree fixtures subclass the concrete memory stores. A packaged suite cannot: it is handed an
adopter's store and has no idea what class it is. Everything here therefore *delegates* — it wraps
the object it was given and forwards every call — so the same fault lands on Postgres, Redis or a
hand-rolled store without any of them knowing.

The fault that matters most is the lost response: the write reached the database and committed, and
the reply did not come back. A caller cannot tell that from a write that never landed, which is why
every seam in ``edax`` is specified to be safe under a retry. A suite that only tests clean failures
never exercises the half of the contract that costs money.
"""

from __future__ import annotations

from typing import Any, Optional

from ..errors import StoreError

__all__ = ["LostResponse", "Refusing"]


class _Delegating:
    """Forward every attribute the wrapper does not define to the wrapped object, so an unknown or
    optional seam method (``get_many``, a backend's own helper) still reaches the real store."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class LostResponse(_Delegating):
    """Let a call land, then raise as though its response was lost in transit.

    Arm with the method name; the *next* call to it commits and then raises ``StoreError``. One shot,
    so the retry that follows sees a healthy store and meets the state the lost call actually wrote.

    ``store = LostResponse(real); store.arm("append")`` then asserts what a redelivery does about an
    append that already succeeded — the window every at-least-once guarantee is written against.
    """

    def __init__(self, inner: Any) -> None:
        super().__init__(inner)
        self.armed: Optional[str] = None

    def arm(self, method: str) -> None:
        self.armed = method

    async def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        result = await getattr(self._inner, method)(*args, **kwargs)
        if self.armed == method:
            self.armed = None
            raise StoreError(f"{method}: the write landed and the response was lost")
        return result

    async def append(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("append", *args, **kwargs)

    async def save(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("save", *args, **kwargs)

    async def save_snapshot(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("save_snapshot", *args, **kwargs)

    async def mark_published(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("mark_published", *args, **kwargs)

    async def claim(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("claim", *args, **kwargs)

    async def complete(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("complete", *args, **kwargs)

    async def release(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("release", *args, **kwargs)

    async def delete(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("delete", *args, **kwargs)


class Refusing(_Delegating):
    """Refuse a named method outright, without letting it land.

    The other half of the pair: ``LostResponse`` writes and then fails, this one fails and writes
    nothing. A contract clause that says "raises and writes nothing" needs this one to be provable.
    """

    def __init__(self, inner: Any, method: str) -> None:
        super().__init__(inner)
        self._method = method
        self.refusals = 0

    def __getattr__(self, name: str) -> Any:
        if name == self._method:

            async def refuse(*args: Any, **kwargs: Any) -> Any:
                self.refusals += 1
                raise StoreError(f"{name}: refused")

            return refuse
        return getattr(self._inner, name)

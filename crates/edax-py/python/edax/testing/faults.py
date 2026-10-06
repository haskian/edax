"""Fault injection over an arbitrary backend.

A packaged suite is handed an adopter's store and has no idea what class it is, so everything here
*delegates*: it wraps the object it was given and forwards every call, and the same fault lands on Postgres,
Redis or a hand-rolled store without any of them knowing.

The main fault is the lost response: the write reached the database and committed, and the reply did not
come back. A caller cannot tell that from a write that never landed, so every seam in ``edax`` is specified
to be safe under a retry. A suite that tests only clean failures never runs that half of the contract.

- :class:`LostResponse` lands a call, then raises as though its reply was lost; ``"commit"`` loses the reply
  of a whole unit of work, the only place a transactional write can land and still fail.
- :class:`Refusing` refuses named calls outright and writes nothing.
- :class:`BrokenCommit` is a transactor whose commit fails, either with nothing landed or after it landed.
- :class:`Delegating` is the base of each: subclass it to break one method of a store your own way.

:class:`~edax.testing.redis.LostReply` loses the reply to one Redis script: a fault of one backend, so it
lives in a module of its own.
"""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, Literal

from ..errors import StoreError

__all__ = ["BrokenCommit", "CommitFault", "Delegating", "LostResponse", "Refusing"]

#: The seam method name that stands for a unit of work's commit, which no store method is.
COMMIT = "commit"


class Delegating:
    """Forward every attribute the wrapper does not define to the wrapped object, so an unknown or
    optional seam method (``get_many``, a backend's own helper, a ``transactor`` property) still reaches the
    real store. A subclass overrides the one method it breaks and reaches the store through ``_inner``."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def _check(self, method: str) -> None:
        """Refuse to arm a name the wrapped store has no method for: the fault would silently never fire."""
        if not callable(getattr(self._inner, method, None)):
            raise AttributeError(
                f"{type(self._inner).__name__} has no method {method!r} to inject a fault into"
            )


class LostResponse(Delegating):
    """Let a call land, then raise as though its response was lost in transit.

    Arm with the method name; the *next* call to it lands and then raises :class:`~edax.errors.StoreError`.
    One shot, so the retry that follows sees a healthy store and meets the state the lost call actually
    wrote. Arm :data:`COMMIT` to lose the reply of the next unit of work instead: ``begin()`` commits,
    then raises.

    ``store = LostResponse(real); store.arm("append")`` then asserts what a redelivery does about an append
    that already succeeded."""

    def __init__(self, inner: Any) -> None:
        super().__init__(inner)
        #: The armed method, or ``None``.
        self.armed: str | None = None

    def arm(self, method: str | None) -> None:
        """Lose the reply of the next call to ``method`` (``None`` disarms). A name the store has no method
        for raises :class:`AttributeError` instead of never firing."""
        if method is not None:
            self._check("begin" if method == COMMIT else method)
        self.armed = method

    def _trip(self, method: str) -> None:
        if self.armed == method:
            self.armed = None
            raise StoreError(f"{method}: the write landed and the response was lost")

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._inner, name)
        if name == "begin":
            return self._begin(target)
        if not inspect.iscoroutinefunction(target):
            return target

        async def call(*args: Any, **kwargs: Any) -> Any:
            result = await target(*args, **kwargs)
            self._trip(name)
            return result

        return call

    def _begin(self, begin: Callable[[], AbstractAsyncContextManager[Any]]) -> Callable[[], Any]:
        @asynccontextmanager
        async def unit_of_work() -> AsyncIterator[Any]:
            async with begin() as tx:
                yield tx
            self._trip(COMMIT)  # committed; the response is lost on the way back

        return unit_of_work


class Refusing(Delegating):
    """Refuse the named methods outright, without letting them land, and count each refusal.

    The other half of the pair: :class:`LostResponse` writes and then fails, this one fails and writes
    nothing. A contract clause that says "raises and writes nothing" needs this one to be provable, and so
    does a guard whose compensation the store refuses (``Refusing(ledger, "complete", "release")``)."""

    def __init__(self, inner: Any, *methods: str) -> None:
        super().__init__(inner)
        if not methods:
            raise ValueError("name at least one method to refuse")
        for method in methods:
            self._check(method)
        self._methods = frozenset(methods)
        #: How many calls were refused.
        self.refusals = 0

    def __getattr__(self, name: str) -> Any:
        if name in self._methods:

            async def refuse(*args: Any, **kwargs: Any) -> Any:
                self.refusals += 1
                raise StoreError(f"{name}: refused")

            return refuse
        return getattr(self._inner, name)


#: How a :class:`BrokenCommit` fails a commit: ``"nothing_landed"`` rolls back and raises, ``"lost_response"``
#: commits and then raises, ``None`` commits cleanly.
CommitFault = Literal["nothing_landed", "lost_response"] | None


class BrokenCommit:
    """A :class:`~edax.unit_of_work.Transactor` whose commit fails, in the two ways that differ.

    ``"nothing_landed"`` rolls the unit of work back and raises: a plain failed commit.
    ``"lost_response"`` commits and *then* raises. They assert opposite things: the first lets a redelivery
    re-run once, the second releases a claim over work that *did* land, so a redelivery runs it twice on a
    ledger of its own. ``arm`` is settable per unit of work, so one rig can drive a whole history. Its
    ``transactor`` is the wrapped one, so stores over that database find the units of work this opens."""

    def __init__(self, inner: Any, arm: CommitFault = "nothing_landed") -> None:
        self._inner = inner
        self.arm: CommitFault = arm

    @property
    def transactor(self) -> Any:
        """The wrapped transactor: the database the units of work belong to."""
        return self._inner

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[Any]:
        arm = self.arm
        if arm == "nothing_landed":
            async with self._inner.begin() as tx:
                yield tx
                # Raised inside the inner block, so the inner transactor rolls back before this
                # propagates: the caller sees a failed commit over which nothing landed.
                raise StoreError("commit failed; nothing landed")
        async with self._inner.begin() as tx:
            yield tx
        if arm == "lost_response":
            raise StoreError("commit landed; the response was lost")

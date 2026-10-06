"""Native handles bound to the thread that built them."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Generic, TypeVar

from . import _native
from .errors import BadHandle

T = TypeVar("T")

_this_thread = threading.local()


def _thread_token() -> object:
    """A marker for the calling thread that no other thread ever holds. ``threading.get_ident()`` is not
    one: Python recycles an ident once its thread exits, so a new thread could pass for a dead owner."""
    token: object | None = getattr(_this_thread, "token", None)
    if token is None:
        token = _this_thread.token = object()
    return token


class ThreadBound(Generic[T]):
    """A native ``_edax`` handle and the thread that built it.

    :meth:`get` hands out the handle behind :func:`~edax._native.handle`, so each method call's failure is
    mapped to :mod:`edax.errors` once, here, for every holder.

    The binding generator marks every native handle ``unsendable``, so a call from another thread
    panics inside PyO3. The panic surfaces as ``PanicException``, a ``BaseException`` that no
    ``except Exception`` arm in the SDK or the host sees. :meth:`get` raises the terminal
    :class:`~edax.errors.BadHandle` instead, before the native call is made. The limit goes once the
    generator can drop ``unsendable`` for these types, which are ``Send + Sync`` in Rust."""

    __slots__ = ("_build", "_handle", "_owner", "_what")
    _build: Callable[[], T]
    _handle: T
    _owner: object | None  # `None` until a lazy handle is built
    _what: str

    def __init__(self, handle: T, what: str) -> None:
        self._handle = _native.handle(handle)
        self._owner = _thread_token()
        self._what = what

    @classmethod
    def lazy(cls, build: Callable[[], T], what: str) -> ThreadBound[T]:
        """A handle ``build`` makes on the first :meth:`get`, bound to that thread: for an object built on
        one thread and run on another thread's event loop."""
        bound = cls.__new__(cls)
        bound._build, bound._owner, bound._what = build, None, what
        return bound

    def get(self) -> T:
        """The handle, on the thread that built it; :class:`~edax.errors.BadHandle` on any other."""
        if self._owner is None:
            self._handle, self._owner = _native.handle(self._build()), _thread_token()
        if _thread_token() is not self._owner:
            raise BadHandle(
                f"{self._what} was built on another thread, and its native state is bound to that "
                "thread: build it on the thread (and event loop) that uses it"
            )
        return self._handle

    def peek(self) -> T | None:
        """The handle as :meth:`get` returns it, or ``None`` while a lazy one is unbuilt. For a read that
        must not build the handle, so a caller on another thread never takes it from its user."""
        return None if self._owner is None else self.get()

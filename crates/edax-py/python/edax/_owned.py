"""Native handles bound to the thread that built them."""

from __future__ import annotations

import threading
from typing import Generic, TypeVar

from .errors import BadHandle

T = TypeVar("T")


class ThreadBound(Generic[T]):
    """A native ``_edax`` handle and the thread that built it.

    The binding generator marks every native handle ``unsendable``, so a call from another thread
    panics inside PyO3. The panic surfaces as ``PanicException``, a ``BaseException`` that no
    ``except Exception`` arm in the SDK or the host sees. :meth:`get` raises the terminal
    :class:`~edax.errors.BadHandle` instead, before the native call is made. The limit goes once the
    generator can drop ``unsendable`` for these types, which are ``Send + Sync`` in Rust."""

    __slots__ = ("_handle", "_owner", "_what")

    def __init__(self, handle: T, what: str) -> None:
        self._handle = handle
        self._owner = threading.get_ident()
        self._what = what

    def get(self) -> T:
        """The handle, on the thread that built it; :class:`~edax.errors.BadHandle` on any other."""
        if threading.get_ident() != self._owner:
            raise BadHandle(
                f"{self._what} was built on another thread, and its native state is bound to that "
                "thread: build it on the thread (and event loop) that uses it"
            )
        return self._handle

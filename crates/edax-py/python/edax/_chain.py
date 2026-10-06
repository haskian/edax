"""The handler registry the command bus and the event router share: handlers by name, each wrapped in the
middleware chain at register time."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Sequence
from typing import Any, Generic, TypeVar

from .errors import AlreadyRegistered

H = TypeVar("H", bound=Callable[..., Any])


def require_async(handler: Callable[..., Any], what: str) -> None:
    """Refuse, with :class:`TypeError`, a ``handler`` that is not a coroutine function (or an object whose
    ``__call__`` is one). Refused at wiring time, because a sync handler would block the event loop, and
    one that returned an awaitable by hand would hide that call from the check."""
    call = getattr(handler, "__call__", None)  # noqa: B004 - the bound method is what we inspect
    if not inspect.iscoroutinefunction(handler) and not inspect.iscoroutinefunction(call):
        raise TypeError(f"{what} must be a coroutine function, got {handler.__class__.__name__}")


def compose(middleware: Sequence[Callable[[H], H]], handler: H) -> H:
    """Wrap ``handler`` in ``middleware``, the first one outermost."""
    for mw in reversed(middleware):
        handler = mw(handler)
    return handler


class Registry(Generic[H]):
    """Handlers by name, each wrapped in ``middleware`` (the first one outermost) when it is registered. The
    chain is fixed at construction, so no handler is registered before a middleware that should wrap it."""

    def __init__(self, middleware: Sequence[Callable[[H], H]]) -> None:
        self._middleware = tuple(middleware)
        self._handlers: dict[str, H] = {}

    def register(self, name: str, handler: H) -> None:
        """Install ``handler`` under ``name``, wrapped in the chain. A second handler for one name raises the
        terminal :class:`~edax.errors.AlreadyRegistered`, naming it, as the Rust registry does."""
        if name in self._handlers:
            raise AlreadyRegistered(name)
        self._handlers[name] = self.wrap(handler)

    def wrap(self, handler: H) -> H:
        """``handler`` wrapped in the chain without registering it, for a fallback handler."""
        return compose(self._middleware, handler)

    def get(self, name: str) -> H | None:
        """The wrapped handler registered under ``name``, or ``None``."""
        return self._handlers.get(name)

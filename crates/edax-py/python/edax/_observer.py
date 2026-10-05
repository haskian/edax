"""How the orchestration objects hold a host :class:`~edax.observe.Observer`: resolved once from an
``observer=`` keyword, and guarded so a hook that raises is logged, never raised into the work."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Final

from ._hooks import safely
from .observe import Observer


class _Guarded(Observer):
    """A host :class:`Observer` behind :func:`~edax._hooks.safely`: the orchestration objects call this and
    never see the host's exceptions. Every public hook of :class:`Observer` is wrapped below, so a hook added
    there is guarded here without a second edit."""

    def __init__(self, inner: Observer) -> None:
        self._inner = inner


def _guard(hook: str) -> Callable[..., None]:
    def guarded(self: _Guarded, *args: Any) -> None:
        safely(getattr(self._inner, hook), *args)

    guarded.__name__ = guarded.__qualname__ = hook
    return guarded


for _hook in [name for name, value in vars(Observer).items() if not name.startswith("_") and callable(value)]:
    setattr(_Guarded, _hook, _guard(_hook))


# One no-op for every unobserved call site: `transaction` resolves its observer per call, so a fresh
# instance there would be an allocation on every unit of work.
_NOOP: Final[Observer] = Observer()


def wrap(observer: Observer | None) -> Observer:
    """Resolve an ``observer=`` keyword: ``None`` is the no-op :class:`Observer`, and a host observer
    is wrapped so its failures are logged, never raised."""
    return _NOOP if observer is None else _Guarded(observer)

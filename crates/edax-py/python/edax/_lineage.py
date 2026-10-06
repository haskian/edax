"""The ambient :class:`~edax.message.Lineage` behind :mod:`edax.causation`, and the two SDK-internal moves on
it: stamping a message produced now, and leaving every lineage when a task starts."""

from __future__ import annotations

import contextvars
from collections.abc import Callable

from . import _native
from .message import Lineage

# Each `use_lineage` sets a new value and resets it on exit, so a task created inside the block keeps
# the lineage it was created under, and nothing outside the block sees it.
AMBIENT: contextvars.ContextVar[Lineage | None] = contextvars.ContextVar("edax_lineage", default=None)


def stamp(
    lineage: Lineage | None,
    extensions: dict[str, str] | None,
    metadata: dict[str, str] | None,
    new_id: Callable[[], str] = _native.guid,
) -> tuple[Lineage, dict[str, str], dict[str, str]]:
    """What a message produced now carries: ``lineage`` when there is one, else the ambient
    :func:`~edax.causation.current_lineage`, else a new flow the kernel's ``originate`` roots at
    ``new_id()``; and its own copies of ``extensions`` and ``metadata`` (``None`` reads as empty). An
    extension key outside the kernel's rule raises the terminal :class:`~edax.errors.CodecError` before a
    new flow takes an id."""
    ext = dict(extensions or {})
    meta = dict(metadata or {})
    if ext:
        _native.check_extensions(ext)
    resolved = lineage if lineage is not None else AMBIENT.get()
    if resolved is None:
        resolved = _native.originate(new_id())
    return resolved, ext, meta


def leave_every_lineage() -> None:
    """Start this task with no ambient lineage. A long-lived task started inside a handler would otherwise
    stamp every message it produces with that one handler's lineage."""
    AMBIENT.set(None)

"""Causation and correlation: where a message sits in its flow. Every event and command carries a
``correlation_id`` constant across one flow and the ``causation_id`` of the message that caused it. The
rules are the kernel's. See ``docs/kernels/event.md``.

The SDK also keeps an ambient :class:`~edax.aggregate.Lineage`: the one a message produced right now must
get. The router, the dispatcher and the saga runner set it to :func:`propagate` of the message they
handle, so a :meth:`~edax.store.Repository.save` inside a handler continues the flow with no argument."""

from __future__ import annotations

import contextvars
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager

from . import _edax, errors
from .aggregate import CommandMessage, EventMessage, Lineage

__all__ = [
    "Lineage",
    "caused_by",
    "current_lineage",
    "originate",
    "propagate",
    "use_lineage",
]

# Each `use_lineage` sets a new value and resets it on exit, so a task created inside the block keeps
# the lineage it was created under, and nothing outside the block sees it.
_ambient: contextvars.ContextVar[Lineage | None] = contextvars.ContextVar("edax_lineage", default=None)


def propagate(parent: EventMessage | CommandMessage) -> Lineage:
    """The lineage of a child caused by ``parent``: ``causation_id`` is the parent's id, ``correlation_id``
    the parent's own. Pinned by ``testdata/golden/causation.json``."""
    with errors.mapped():
        return _edax.propagate(parent.id, parent.correlation_id)


def originate(correlation_id: str | None = None, causation_id: str | None = None) -> Lineage:
    """The lineage of a message no message caused, such as one an inbound request starts. Fills only what
    is missing: ``correlation_id`` defaults to a fresh :func:`~edax.guid`, ``causation_id`` to the resulting
    ``correlation_id``. Pinned by ``testdata/golden/causation_originate.json``."""
    # The kernel owns the defaulting rule, the host the entropy, which keeps the rule pure and pinnable.
    with errors.mapped():
        return _edax.originate(_edax.guid(), correlation_id, causation_id)


def current_lineage() -> Lineage | None:
    """The ambient lineage a message produced now must get, or ``None`` outside any handled message."""
    return _ambient.get()


def _stamp(
    lineage: Lineage | None,
    extensions: dict[str, str] | None,
    metadata: dict[str, str] | None,
    new_id: Callable[[], str] = _edax.guid,
) -> tuple[Lineage, dict[str, str], dict[str, str]]:
    """What a message produced now carries: ``lineage`` when there is one, else the ambient
    :func:`current_lineage`, else a new flow the kernel's ``originate`` roots at ``new_id()``; and its own
    copies of ``extensions`` and ``metadata`` (``None`` reads as empty). An extension key outside the
    kernel's rule raises the terminal :class:`~edax.errors.CodecError` before a new flow takes an id."""
    ext = dict(extensions or {})
    meta = dict(metadata or {})
    if ext:
        with errors.mapped():
            _edax.check_extensions(ext)
    resolved = lineage if lineage is not None else _ambient.get()
    if resolved is None:
        with errors.mapped():
            resolved = _edax.originate(new_id())
    return resolved, ext, meta


@contextmanager
def use_lineage(lineage: Lineage) -> Iterator[Lineage]:
    """Make ``lineage`` the ambient one for the block, so a message produced inside it continues that flow."""
    token = _ambient.set(lineage)
    try:
        yield lineage
    finally:
        _ambient.reset(token)


def caused_by(msg: EventMessage | CommandMessage) -> AbstractContextManager[Lineage]:
    """Run the block as a handler of ``msg``: the ambient lineage is :func:`propagate` of it."""
    return use_lineage(propagate(msg))


def _leave_every_lineage() -> None:
    """Start this task with no ambient lineage. A long-lived task started inside a handler would otherwise
    stamp every message it produces with that one handler's lineage."""
    _ambient.set(None)

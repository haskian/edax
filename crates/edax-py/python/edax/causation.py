"""Causation and correlation: where a message sits in its flow. Every event and command carries a
``correlation_id`` constant across one flow and the ``causation_id`` of the message that caused it. The
rules are the kernel's. See ``docs/kernels/event.md``.

The SDK also keeps an ambient :class:`~edax.message.Lineage`: the one a message produced right now must
get. The router, the dispatcher and the saga runner set it to :func:`propagate` of the message they
handle, so a :meth:`~edax.repository.Repository.save` inside a handler continues the flow with no argument."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager

from . import _native
from ._lineage import AMBIENT
from .message import CommandMessage, EventMessage, Lineage

__all__ = [
    "Lineage",
    "caused_by",
    "current_lineage",
    "originate",
    "propagate",
    "use_lineage",
]


def propagate(parent: EventMessage | CommandMessage) -> Lineage:
    """The lineage of a child caused by ``parent``: ``causation_id`` is the parent's id, ``correlation_id``
    the parent's own. Pinned by ``testdata/golden/causation.json``."""
    return _native.propagate(parent.id, parent.correlation_id)


def originate(correlation_id: str | None = None, causation_id: str | None = None) -> Lineage:
    """The lineage of a message no message caused, such as one an inbound request starts. Fills only what
    is missing: ``correlation_id`` defaults to a fresh :func:`~edax.guid`, ``causation_id`` to the resulting
    ``correlation_id``. Pinned by ``testdata/golden/causation_originate.json``."""
    # The kernel owns the defaulting rule, the host the entropy, which keeps the rule pure and pinnable.
    return _native.originate(_native.guid(), correlation_id, causation_id)


def current_lineage() -> Lineage | None:
    """The ambient lineage a message produced now must get, or ``None`` outside any handled message."""
    return AMBIENT.get()


@contextmanager
def use_lineage(lineage: Lineage) -> Iterator[Lineage]:
    """Make ``lineage`` the ambient one for the block, so a message produced inside it continues that flow."""
    token = AMBIENT.set(lineage)
    try:
        yield lineage
    finally:
        AMBIENT.reset(token)


def caused_by(msg: EventMessage | CommandMessage) -> AbstractContextManager[Lineage]:
    """Run the block as a handler of ``msg``: the ambient lineage is :func:`propagate` of it."""
    return use_lineage(propagate(msg))

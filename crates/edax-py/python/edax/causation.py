"""Causation and correlation id propagation: two metadata keys linking events into a traceable graph, so the
envelope wire format stays untouched. The rules are the kernel's. See ``docs/kernels/event.md``."""

from __future__ import annotations

from . import _edax

CORRELATION_ID = "correlation_id"
CAUSATION_ID = "causation_id"


def propagate(parent_metadata: dict[str, str], parent_event_id: str) -> dict[str, str]:
    """Chain metadata for a child caused by ``parent_event_id``: ``causation_id`` is that id,
    ``correlation_id`` the parent's if present, else that id. Merge into the child's metadata."""
    return _edax.propagate(dict(parent_metadata), parent_event_id)


def originate(metadata: dict[str, str], fresh_id: str) -> dict[str, str]:
    """The two chain keys for a root event, given a fresh id. Fills only what is missing:
    ``correlation_id`` defaults to ``fresh_id``, ``causation_id`` to the resulting ``correlation_id``."""
    return _edax.originate(dict(metadata), fresh_id)


def ensure_ids(metadata: dict[str, str] | None = None) -> dict[str, str]:
    """Stamp a root event's metadata, minting the ids not supplied: ``correlation_id`` defaults to a fresh
    :func:`~edax.guid`, ``causation_id`` to the resulting ``correlation_id``. Returns a merged copy without
    mutating the caller's dict. For a caused event use :func:`propagate`."""
    out = dict(metadata or {})
    # The kernel owns the defaulting rule, the host the entropy, which keeps the rule pure and pinnable.
    out.update(originate(out, _edax.guid()))
    return out

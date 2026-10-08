"""The kernel's standalone rules, public, typed and documented. ``edax._native``, which every SDK call into
the kernel goes through, maps each failure to :mod:`edax.errors`."""

from __future__ import annotations

from . import _native

__all__ = [
    "advance",
    "canonical_json",
    "current_timestamp",
    "guid",
    "is_stale",
    "partition",
    "payload_hash",
    "payload_hash_json",
]


def payload_hash(payload: bytes) -> int:
    """A stable 64-bit hash (FNV-1a) of ``payload``, byte for byte. A command's guard hashes canonical JSON
    with the command name instead."""
    return _native.payload_hash(payload)


def payload_hash_json(payload: bytes) -> int:
    """The hash of ``payload``'s canonical JSON (RFC 8785), so two spellings of one value hash alike. A
    payload that is not JSON, or an integer outside ±(2^53-1), raises :class:`~edax.errors.CodecError`."""
    return _native.payload_hash_json(payload)


def canonical_json(payload: bytes) -> str:
    """The exact text :func:`payload_hash_json` hashes, for a host that checks bytes and not only hashes.
    Raises :class:`~edax.errors.CodecError` where :func:`payload_hash_json` does."""
    return _native.canonical_json(payload)


def partition(aggregate_id: str, n: int) -> int:
    """The local worker, of ``n`` (at least 1), that runs ``aggregate_id``'s events: local fan-out only, never
    broker placement. A negative ``n`` raises :class:`~edax.errors.CodecError`."""
    return _native.partition(aggregate_id, n)


def is_stale(stored: int | None = None, incoming: int | None = None) -> bool:
    """Whether a message at sequence ``incoming`` is older than the watermark ``stored``. Ordering, not
    dedup: ``is_stale(5, 5)`` is ``False``, because an ordinal cannot tell a redelivery from a re-send.
    :func:`~edax.idempotent_by_id` is the dedup. See ``docs/kernels/event.md``."""
    return _native.is_stale(stored is not None, stored or 0, incoming is not None, incoming or 0)


def advance(stored: int | None = None, incoming: int | None = None) -> int | None:
    """The watermark after a message at ``incoming`` applies: the higher sequence, so an unsequenced message
    never erases it. Pair it with :func:`is_stale`."""
    return _native.advance(stored is not None, stored or 0, incoming is not None, incoming or 0)


def guid() -> str:
    """A fresh time-ordered, globally unique id (UUID v7)."""
    return _native.guid()


def current_timestamp() -> int:
    """The kernel clock: nanoseconds since the Unix epoch."""
    return _native.current_timestamp()

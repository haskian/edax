"""Settings checked at wiring time: a value the kernel cannot carry fails before any message reaches it, as
the terminal :class:`~edax.errors.BadHandle`, since the same setting would fail every message alike."""

from __future__ import annotations

from typing import TypeGuard

from .errors import BadHandle

#: The largest value the kernel's ``u32`` arguments take: a schema version, an attempt count.
U32_MAX = 2**32 - 1
#: The largest value the kernel's ``u64`` arguments take: a backoff duration in milliseconds.
U64_MAX = 2**64 - 1
#: The largest value the kernel's ``i64`` arguments take: a Kafka timeout in milliseconds.
I64_MAX = 2**63 - 1


def is_int(value: object) -> TypeGuard[int]:
    """Whether ``value`` is an ``int`` and not a ``bool``: a bool is an int to Python, and the binding would
    convert it silently."""
    return isinstance(value, int) and not isinstance(value, bool)


def wiring_int(value: object, what: str) -> int:
    """``value`` if it is an ``int``; any other value raises the terminal :class:`BadHandle` at wiring time,
    since the same setting would fail every message it reaches. ``what`` names the setting, for the
    message."""
    if not is_int(value):
        raise BadHandle(f"{what} must be an int, got {value!r}")
    return value


def wiring_str(value: object, what: str) -> str:
    """``value`` if it is a ``str``; any other value raises the terminal :class:`BadHandle` at wiring time,
    as :func:`wiring_int` does."""
    if not isinstance(value, str):
        raise BadHandle(f"{what} must be a str, got {type(value).__name__}")
    return value


def wiring_u32(value: object, what: str) -> int:
    """``value`` if the kernel can carry it as an unsigned 32-bit integer; any other value raises the terminal
    :class:`BadHandle` at wiring time, as :func:`wiring_int` does."""
    return _within(value, U32_MAX, "an unsigned 32-bit integer", what)


def wiring_u64(value: object, what: str) -> int:
    """``value`` if the kernel can carry it as an unsigned 64-bit integer; any other value raises the terminal
    :class:`BadHandle` at wiring time, as :func:`wiring_int` does."""
    return _within(value, U64_MAX, "an unsigned 64-bit integer", what)


def _within(value: object, largest: int, kind: str, what: str) -> int:
    checked = wiring_int(value, what)
    if not 0 <= checked <= largest:
        raise BadHandle(f"{what} must be {kind}, got {checked}")
    return checked

"""The host-value types: a decimal and an instant that hold the kernel's canonical text.

Declare a payload field as :class:`Decimal` or :class:`Instant`, the twins of Rust's ``edax::Decimal`` /
``edax::Instant`` and Go's ``edax.Decimal`` / ``edax.Instant``. The codecs then read the field from the text
the wire carries, through the kernel, before anything rounds it: ``9.50`` and ``+9.5`` read as ``9.5``, while
``1_000``, `` 9.5 `` and a fraction finer than a microsecond are refused, as the other SDKs refuse them. A
``decimal.Decimal`` or ``datetime.datetime`` field cannot promise that, because msgspec converts the text
first, so :func:`~edax.codec.json_codec` and :func:`~edax.codec.msgpack_codec` refuse to register one.

Each value is built from text, which the kernel canonicalizes (``canonical_decimal``,
``canonical_timestamp``), so a value that exists is always canonical. Equal values have one text, so equality
and the hash compare the text. A :class:`Decimal` orders by value and an :class:`Instant` by its fixed-width
text, which is instant order. Neither does arithmetic: convert to ``decimal.Decimal`` or ``datetime`` and
back. Every refusal is the kernel's terminal :class:`~edax.errors.CodecError`. Pinned by
``testdata/golden/host_values.json``."""

from __future__ import annotations

import datetime as _dt
import decimal as _decimal
import functools
from typing import Any, NoReturn, final

from . import _native
from .errors import CodecError

__all__ = ["Decimal", "Instant"]


def _require_text(value: object, built_by: str) -> str:
    """``value``, the text a host value is built from. Anything else is a programming error, so a
    :class:`TypeError` naming ``built_by``, the conversion that takes it."""
    if not isinstance(value, str):
        raise TypeError(f"a host value is built from text, not {type(value).__name__}: use {built_by}")
    return value


class _Frozen:
    """Refuses every attribute write after ``__init__``, so a value object never changes."""

    __slots__ = ()

    def __setattr__(self, name: str, value: object) -> NoReturn:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __delattr__(self, name: str) -> NoReturn:
        raise AttributeError(f"{type(self).__name__} is immutable")


@final
@functools.total_ordering
class Decimal(_Frozen):
    """An exact decimal number in the kernel's canonical text: ``Decimal("9.50")`` is ``9.5``. Written as
    that text, a JSON or MessagePack string. Two decimals are equal when their values are, and they order by
    value, not by text. ``NaN``, an infinity or more than 1000 digits is a
    :class:`~edax.errors.CodecError`."""

    __slots__ = ("_text", "_value")
    _text: str
    _value: _decimal.Decimal

    def __init__(self, text: str) -> None:
        canonical = _native.canonical_decimal(_require_text(text, "Decimal.from_decimal()"))
        object.__setattr__(self, "_text", canonical)
        # Read once, so an order compares values without parsing. Constructing from text is exact at any
        # context precision.
        object.__setattr__(self, "_value", _decimal.Decimal(canonical))

    @classmethod
    def from_decimal(cls, value: _decimal.Decimal) -> Decimal:
        """``value`` as a :class:`Decimal`, for a value computed with ``decimal.Decimal`` arithmetic."""
        if not isinstance(value, _decimal.Decimal):
            raise TypeError(f"from_decimal takes a decimal.Decimal, not {type(value).__name__}")
        return cls(str(value))

    def to_decimal(self) -> _decimal.Decimal:
        """The exact ``decimal.Decimal`` for arithmetic."""
        return self._value

    def __str__(self) -> str:
        return self._text

    def __repr__(self) -> str:
        return f"edax.Decimal({self._text!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Decimal):
            return NotImplemented
        return self._text == other._text

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Decimal):
            return NotImplemented
        return self._value < other._value

    def __hash__(self) -> int:
        return hash(self._text)

    def __reduce__(self) -> tuple[type[Decimal], tuple[str]]:
        return (Decimal, (self._text,))


@final
@functools.total_ordering
class Instant(_Frozen):
    """An instant in the kernel's fixed-width UTC text: ``Instant("2024-01-01T01:30:00+01:00")`` is
    ``2024-01-01T00:30:00.000000Z``. Written as that text, a JSON or MessagePack string, so text order is
    instant order. Text with no offset, a fraction finer than a microsecond, or a UTC year outside
    ``0000``-``9999`` is a :class:`~edax.errors.CodecError`."""

    __slots__ = ("_text",)
    _text: str

    def __init__(self, text: str) -> None:
        object.__setattr__(
            self, "_text", _native.canonical_timestamp(_require_text(text, "Instant.from_datetime()"))
        )

    @classmethod
    def from_datetime(cls, value: _dt.datetime) -> Instant:
        """``value`` as an :class:`Instant`. A naive ``datetime`` is a :class:`~edax.errors.CodecError`: its
        instant is unknown, and guessing UTC would misplace it silently."""
        if not isinstance(value, _dt.datetime):
            raise TypeError(f"from_datetime takes a datetime.datetime, not {type(value).__name__}")
        return cls(value.isoformat())

    def to_datetime(self) -> _dt.datetime:
        """The aware UTC ``datetime`` for arithmetic. Year ``0000``, which the kernel reads and Python's
        ``datetime`` cannot hold, is a :class:`~edax.errors.CodecError`."""
        try:
            # Python 3.10 reads no `Z` suffix.
            return _dt.datetime.fromisoformat(self._text[:-1] + "+00:00")
        except ValueError as e:
            raise CodecError(f"{self._text} has no Python datetime: {e}") from e

    def __str__(self) -> str:
        return self._text

    def __repr__(self) -> str:
        return f"edax.Instant({self._text!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Instant):
            return NotImplemented
        return self._text == other._text

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Instant):
            return NotImplemented
        return self._text < other._text

    def __hash__(self) -> int:
        return hash(self._text)

    def __reduce__(self) -> tuple[type[Instant], tuple[str]]:
        return (Instant, (self._text,))


#: The host-value types, which the SDK's encoder writes as their text and its decoders read from it.
HOST_TYPES: tuple[type[Any], ...] = (Decimal, Instant)
#: The values the encoder writes as decimal text: a filter comparand of one sets the decimal collation.
DECIMAL_TYPES: tuple[type[Any], ...] = (Decimal, _decimal.Decimal)

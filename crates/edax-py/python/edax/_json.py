"""The SDK's one encoder from host values to the JSON text the kernel reads, and back.

Every path that hands the kernel a host value goes through it: a filter, a query, a cursor, a projection
document, a saga state, a cached command result and the command hash. One encoder means a value that hashes
in a command also matches in a filter. It uses msgspec, so dataclasses, Structs, ``UUID``, ``date`` and
``Enum`` all encode, deterministically.

Two host types get the form every SDK writes, so they compare by value and hash alike across languages
(``testdata/golden/host_values.json`` pins both):

- A ``Decimal`` becomes the kernel's canonical decimal text, a JSON string: ``Decimal("9.50")`` and
  ``Decimal("9.5")`` are both ``"9.5"``, at any precision, and no double is involved. Equality and the hash
  see one value. A range filter or a sort orders decimal text by value only under the decimal collation,
  which :mod:`edax.filter` sets for a ``Decimal`` comparand, and ``sort_key(..., decimal=True)`` for a sort.
  ``NaN``, an infinity and a value past 1000 digits are refused.
- A ``datetime`` becomes the kernel's fixed-width UTC text (``2024-01-01T00:30:00.000000Z``), so string order
  is instant order: ``+01:00`` is converted, and a whole second gets ``.000000``. A naive ``datetime`` is
  refused: its instant is unknown, and guessing UTC would misplace it silently.

Every failure is deterministic, so it is a terminal :class:`~edax.errors.CodecError`, never a bare
``TypeError`` a retry or a dispatcher would read as transient.

msgspec writes ``NaN`` and the infinities as ``null``: JSON has no form for them, and the kernel would refuse
the bare words. Normalize such values before they reach a document or a filter if ``null`` is not what they
mean."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import msgspec

from . import _edax, errors
from .errors import CodecError

# msgspec passes these through `to_builtins` unchanged, so the walk below writes them in the shared form.
_OWN_FORM = (datetime, Decimal)


def encode(value: Any) -> str:
    """``value`` as JSON text. A value msgspec cannot encode is a :class:`~edax.errors.CodecError`."""
    return encode_bytes(value).decode()


def encode_bytes(value: Any) -> bytes:
    """``value`` as UTF-8 JSON bytes, for a kernel call that takes bytes."""
    try:
        return msgspec.json.encode(_shared_form(msgspec.to_builtins(value, builtin_types=_OWN_FORM)))
    # UnicodeEncodeError (a lone surrogate) is a ValueError; a self-containing value recurses.
    except (TypeError, ValueError, OverflowError, RecursionError) as e:
        raise CodecError(f"value is not JSON-encodable: {e}") from e


def decode(text: str | bytes) -> Any:
    """The value behind JSON text. Text that is not JSON is a :class:`~edax.errors.CodecError`."""
    try:
        return msgspec.json.decode(text)
    except msgspec.DecodeError as e:
        raise CodecError(f"value is not valid JSON: {e}") from e


def _shared_form(value: Any) -> Any:
    """``value``, the builtins ``to_builtins`` made, with every ``datetime`` and ``Decimal`` in its shared
    form, dictionary keys included."""
    if isinstance(value, dict):
        return {_shared_form(key): _shared_form(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_shared_form(item) for item in value]
    if isinstance(value, datetime):
        with errors.mapped():  # the kernel refuses a naive value: its text has no offset
            return _edax.canonical_timestamp(value.isoformat())
    if isinstance(value, Decimal):
        with errors.mapped():
            return _edax.canonical_decimal(str(value))
    return value

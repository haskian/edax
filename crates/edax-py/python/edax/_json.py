"""The SDK's one encoder from host values to the JSON text the kernel reads, and back.

It uses msgspec, the encoder the command hash already uses, so a value that hashes in a command also
encodes in a filter, a query, a projection document and a saga state: ``datetime``, ``UUID``, ``Decimal``,
dataclasses and Structs all encode, deterministically. Encoding is not ordering. msgspec writes a
``Decimal`` as a string, so a range filter or a sort compares it as text: ``Decimal("9.5")`` sorts after
``Decimal("10")``, and ``Decimal("9.50")`` does not equal ``Decimal("9.5")``. It writes a ``datetime``
without a zero fraction and with its own offset, so ``...00Z`` sorts after ``...00.500000Z``. Store
amounts as integer minor units, and timestamps as epoch integers or in the kernel's fixed-width UTC form
(``docs/kernels/filter.md``). Every failure is deterministic, so it is a terminal
:class:`~edax.errors.CodecError`, never a bare ``TypeError`` a retry or a dispatcher would read as transient.

msgspec writes ``NaN`` and the infinities as ``null``: JSON has no form for them, and the kernel would refuse
the bare words. Normalize such values before they reach a document or a filter if ``null`` is not what they
mean."""

from __future__ import annotations

from typing import Any

import msgspec

from .errors import CodecError


def encode(value: Any) -> str:
    """``value`` as JSON text. A value msgspec cannot encode is a :class:`~edax.errors.CodecError`."""
    return encode_bytes(value).decode()


def encode_bytes(value: Any) -> bytes:
    """``value`` as UTF-8 JSON bytes, for a kernel call that takes bytes."""
    try:
        return msgspec.json.encode(value)
    # UnicodeEncodeError (a lone surrogate) is a ValueError; a self-containing value recurses.
    except (TypeError, ValueError, OverflowError, RecursionError) as e:
        raise CodecError(f"value is not JSON-encodable: {e}") from e


def decode(text: str | bytes) -> Any:
    """The value behind JSON text. Text that is not JSON is a :class:`~edax.errors.CodecError`."""
    try:
        return msgspec.json.decode(text)
    except msgspec.DecodeError as e:
        raise CodecError(f"value is not valid JSON: {e}") from e

"""The SDK's one encoder from host values to the JSON text the kernel reads, and back.

It uses msgspec, the encoder the command hash already uses, so a value that hashes in a command also
works in a filter, a query, a projection document and a saga state: ``datetime``, ``UUID``, ``Decimal``,
dataclasses and Structs all encode, deterministically. Every failure is deterministic, so it is a terminal
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

"""The SDK's one encoder from host values to the JSON text the kernel reads, and back.

Every path that hands the kernel a host value goes through it: a filter, a query, a cursor, a projection
document, a saga state, a cached command result and the command hash. The event codecs encode their payloads
with it too, in JSON and in MessagePack (:func:`encode_msgpack`). One encoder means a value that hashes in a
command also matches in a filter and reads the same in an event. It uses msgspec, so dataclasses, Structs,
``UUID``, ``date`` and ``Enum`` all encode, deterministically. A type whose declared fields hold no
``datetime``, ``Decimal`` or dataclass is written by msgspec as it is, with no walk, decided once per type.

The host types :class:`edax.Decimal` and :class:`edax.Instant` (:mod:`edax.host_value`) are written as
their canonical text, and the typed decoders (:func:`decode_json`, :func:`decode_msgpack`) read them from the
text the wire carries, through the kernel. A plain ``Decimal`` or ``datetime`` in a value an encoder writes
gets the same text through the kernel, so the two compare by value and hash alike across languages
(``testdata/golden/host_values.json`` pins both):

- A decimal becomes the kernel's canonical decimal text, a JSON string: ``Decimal("9.50")`` and
  ``Decimal("9.5")`` are both ``"9.5"``, at any precision, and no double is involved. Equality and the hash
  see one value. A range filter or a sort orders decimal text by value only under the decimal collation.
  :mod:`edax.filter` sets it for a decimal comparand, and ``sort_key(..., decimal=True)`` for a sort.
  ``NaN``, an infinity and a value past 1000 digits are refused.
- An instant becomes the kernel's fixed-width UTC text (``2024-01-01T00:30:00.000000Z``), so string order is
  instant order: ``+01:00`` is converted, and a whole second gets ``.000000``. A naive ``datetime`` is
  refused: its instant is unknown, and guessing UTC would misplace it silently.

Every failure is deterministic, so it is a terminal :class:`~edax.errors.CodecError`, never a bare
``TypeError`` a retry or a dispatcher would read as transient.

A ``float`` ``NaN`` or infinity is written as ``null``: JSON has no form for them, and the kernel would refuse
the bare words. The Rust SDK's ``serde_json`` writes the same ``null``, so a hash, a filter and a document
agree across SDKs (``host_values.json`` pins it). A field typed as a plain ``float`` cannot read that ``null``
back, so an event carrying one fails to decode on every SDK: normalize such values before they reach a
payload, a document or a filter if ``null`` is not what they mean. MessagePack keeps them as they are."""

from __future__ import annotations

import weakref
from collections.abc import Callable
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, TypeVar

import msgspec

from . import _native
from .errors import CodecError
from .host_value import HOST_TYPES, Instant

T = TypeVar("T")

# msgspec passes these through `to_builtins` unchanged, so the walk below writes them in the shared form. A
# `Raw` passes through too, for the encoder to write as it is: it holds text an encoder already wrote.
_OWN_FORM = (datetime, Decimal, msgspec.Raw)
# MessagePack has a native byte string, so a binary payload keeps bytes as they are; JSON writes base64. Its
# extension type is MessagePack's own.
_OWN_FORM_BINARY = (*_OWN_FORM, bytes, bytearray, memoryview, msgspec.msgpack.Ext)


def encode(value: Any) -> str:
    """``value`` as JSON text. A value msgspec cannot encode is a :class:`~edax.errors.CodecError`."""
    return encode_bytes(value).decode()


def call(native: Callable[..., T], *values: Any) -> T:
    """``native``, an ``edax._native`` function, called with each host value in ``values`` as JSON text. A
    value that will not encode is a :class:`~edax.errors.CodecError` before the native call runs."""
    return native(*[encode(value) for value in values])


#: What an encoder raises for a value it cannot write. UnicodeEncodeError (a lone surrogate) is a ValueError;
#: a self-containing value recurses.
_ENCODE_ERRORS = (TypeError, ValueError, OverflowError, RecursionError)


def encode_bytes(value: Any) -> bytes:
    """``value`` as UTF-8 JSON bytes, for a kernel call that takes bytes."""
    try:
        return _JSON_ENCODER.encode(value if _plan(type(value)).direct else _builtins(value, _OWN_FORM))
    except _ENCODE_ERRORS as e:
        raise CodecError(f"value is not JSON-encodable: {e}") from e


def encode_msgpack(value: Any) -> bytes:
    """``value`` as MessagePack, with every host value in the shared form JSON writes and bytes kept native:
    what the MessagePack event codec writes. A value msgspec cannot encode is a
    :class:`~edax.errors.CodecError`."""
    try:
        return _MSGPACK_ENCODER.encode(
            value if _plan(type(value)).direct else _builtins(value, _OWN_FORM_BINARY)
        )
    except _ENCODE_ERRORS as e:
        raise CodecError(f"value is not encodable: {e}") from e


def decode_json(data: bytes, type_: type[T]) -> T:
    """``data``, JSON, read as ``type_``, each host value read from its wire text by :func:`decode_hook`."""
    return msgspec.json.decode(data, type=type_, dec_hook=decode_hook)


def decode_msgpack(data: bytes, type_: type[T]) -> T:
    """``data``, MessagePack, read as ``type_``, each host value read from its wire text by
    :func:`decode_hook`."""
    return msgspec.msgpack.decode(data, type=type_, dec_hook=decode_hook)


def _encode_hook(value: Any) -> str:
    """A host value's canonical text, what msgspec writes for it. Any other type is one msgspec cannot
    encode."""
    if isinstance(value, HOST_TYPES):
        return str(value)
    raise TypeError(f"Encoding objects of type {type(value).__name__} is unsupported")


_JSON_ENCODER = msgspec.json.Encoder(enc_hook=_encode_hook)
_MSGPACK_ENCODER = msgspec.msgpack.Encoder(enc_hook=_encode_hook)


def decode_hook(type_: type, value: Any) -> Any:
    """``value``, what msgspec found on the wire for a field of ``type_`` it cannot build itself, as
    ``type_``. A host value is read from its text by the kernel, so a refusal is the kernel's, and an
    instance passes as it is. A MessagePack timestamp, which an earlier release wrote for a ``datetime``,
    reads as an :class:`~edax.Instant`: msgspec has already rounded it to a microsecond, so a finer one is
    not refused as the text form is. Anything else is a ``TypeError``, which msgspec reports with the
    field's path."""
    if type_ in HOST_TYPES:
        if isinstance(value, type_):
            return value
        try:
            if isinstance(value, str):
                return type_(value)
            if type_ is Instant and isinstance(value, datetime):
                return Instant.from_datetime(value)
        except CodecError as e:
            raise ValueError(str(e)) from e  # msgspec adds the path to a ValueError, not to a CodecError
    raise TypeError(f"Expected `{type_.__name__}`, got `{type(value).__name__}`")


def _builtins(value: Any, own_form: tuple[type, ...]) -> Any:
    return _shared_form(
        msgspec.to_builtins(_materialized(value), builtin_types=own_form, enc_hook=_encode_hook)
    )


@dataclass(frozen=True)
class _Plan:
    """What a value of one type needs before an encoder writes it in the shared form, read off its declared
    field types once: msgspec itself trusts them to encode and decode."""

    #: It may hold a plain ``datetime`` or ``Decimal``, rewritten to its shared form on encode.
    host_values: bool
    #: It may hold a dataclass, whose defaulted fields must be read through ``dataclasses.fields()``.
    dataclasses: bool

    @property
    def direct(self) -> bool:
        """Whether an encoder writes the value as it is, with no walk."""
        return not (self.host_values or self.dataclasses)


#: The plan of each type met so far. Weak, so a class nobody holds any more drops its plan.
_PLANS: weakref.WeakKeyDictionary[type, _Plan] = weakref.WeakKeyDictionary()


def _plan(type_: type) -> _Plan:
    plan = _PLANS.get(type_)
    if plan is None:
        plan = _PLANS[type_] = _plan_of(type_)
    return plan


def _plan_of(type_: type) -> _Plan:
    try:
        nodes = type_nodes(msgspec.inspect.type_info(type_))
    except Exception:  # noqa: BLE001 - a type msgspec cannot inspect gets the full walk
        return _Plan(host_values=True, dataclasses=True)
    kinds = {type(node) for node in nodes}
    # `Any` and a type msgspec does not know may hold anything at run time. A host type is known: the encoder
    # writes it through its hook.
    opaque = msgspec.inspect.AnyType in kinds or any(
        isinstance(node, msgspec.inspect.CustomType) and node.cls not in HOST_TYPES for node in nodes
    )
    host_values = bool(kinds & {msgspec.inspect.DateTimeType, msgspec.inspect.DecimalType})
    return _Plan(opaque or host_values, opaque or msgspec.inspect.DataclassType in kinds)


def type_nodes(root: msgspec.inspect.Type) -> list[msgspec.inspect.Type]:
    """Every node in ``root``'s type tree, once. A recursive type points back at a node already seen."""
    nodes: list[msgspec.inspect.Type] = []
    seen: set[int] = set()
    stack = [root]
    while stack:
        node = stack.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        nodes.append(node)
        for name in node.__struct_fields__:
            value = getattr(node, name)
            for child in value if isinstance(value, tuple) else (value,):
                child_type = child.type if isinstance(child, msgspec.inspect.Field) else child
                if isinstance(child_type, msgspec.inspect.Type):
                    stack.append(child_type)
    return nodes


def decode(text: str | bytes) -> Any:
    """The value behind JSON text. Text that is not JSON is a :class:`~edax.errors.CodecError`."""
    try:
        return msgspec.json.decode(text)
    except msgspec.DecodeError as e:
        raise CodecError(f"value is not valid JSON: {e}") from e


def _shared_form(value: Any) -> Any:
    """``value``, the builtins ``to_builtins`` made, with every plain ``datetime`` and ``Decimal`` in the
    shared form its host type writes, dictionary keys included."""
    if isinstance(value, dict):
        return {_shared_form(key): _shared_form(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_shared_form(item) for item in value]
    if isinstance(value, datetime):
        return _native.canonical_timestamp(value.isoformat())
    if isinstance(value, Decimal):
        return _native.canonical_decimal(str(value))
    return value


# Distinguishes a field with no value at all from one whose value is None.
_NO_VALUE = object()


def _materialized(value: Any) -> Any:
    """``value`` with every dataclass in it read through ``dataclasses.fields()``.

    ``msgspec.convert`` / ``decode`` leave a defaulted field out of the instance ``__dict__`` that msgspec's
    encoder reads, so two equal values would hash, match and store two ways. Reading each declared field
    with ``getattr`` sees the class-level default. Returns ``value`` itself when nothing changed."""
    if is_dataclass(value) and not isinstance(value, type):
        data: dict[str, Any] = {}
        for f in fields(value):
            present = getattr(value, f.name, _NO_VALUE)
            # A field with no value (init=False, never assigned, or `UNSET`) is omitted, as msgspec omits it.
            # Kept as a dict value, `UNSET` would be refused: msgspec omits it only from a field.
            if present is not _NO_VALUE and present is not msgspec.UNSET:
                data[f.name] = _materialized(present)
        return data
    if isinstance(value, msgspec.Struct):
        if not _plan(type(value)).dataclasses:
            return value  # no field of this type can hold a dataclass
        changes = _changed({f.name: getattr(value, f.name) for f in msgspec.structs.fields(value)})
        return msgspec.structs.replace(value, **changes) if changes else value
    if isinstance(value, (list, tuple)):
        items = [_materialized(item) for item in value]
        return items if any(new is not old for new, old in zip(items, value, strict=True)) else value
    if isinstance(value, dict):
        changes = _changed(value)
        return {**value, **changes} if changes else value
    return value


def _changed(entries: dict[Any, Any]) -> dict[Any, Any]:
    changes: dict[Any, Any] = {}
    for key, old in entries.items():
        new = _materialized(old)
        if new is not old:
            changes[key] = new
    return changes

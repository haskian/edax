"""Ready-made codecs: marshal a domain event or command to bytes by name, and back.

A :class:`~edax.aggregate.PayloadCodec` is the boundary between your domain objects and the
``name``/``version``/``bytes`` a message carries. These two cover the common case, a fixed set of
types each serialized whole (``json_codec({"Deposited": Deposited})``). A snapshot (``AggregateCodec``)
stays hand-written: only you know which fields are state and which are derived. Every failure here raises
:class:`~edax.errors.CodecError`, which is terminal, so the caller dead-letters instead of redelivering;
an upcast step's own edax error keeps its class."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import msgspec

from . import _json
from .errors import AlreadyRegistered, CodecError, code_of
from .message import Marshaled, Payload
from .upcast import Upcaster, target, upcast

__all__ = [
    "RegistryCodec",
    "json_codec",
    "msgpack_codec",
]


@contextmanager
def _as_codec_error(what: str) -> Iterator[None]:
    """Re-raise a marshal/unmarshal failure as the terminal :class:`~edax.errors.CodecError`, named by
    ``what``. An untyped ``KeyError``/``ValueError`` would be classed transient and redelivered forever. A
    failure that carries another taxonomy code keeps it, as the Rust SDK's upcast step returns its own
    error: an upcast step whose lookup service is down raises a transient error, and retyping it terminal
    would dead-letter a payload a retry could read."""
    try:
        yield
    except Exception as e:
        if code_of(e) not in (None, CodecError.code):
            raise
        raise CodecError(f"{what}: {e}") from e


class RegistryCodec:
    """Maps event names to types and serializes each whole. Build via :func:`json_codec` /
    :func:`msgpack_codec` rather than directly.

    Each name has its own schema version: 1, or one past the highest step of its
    :class:`~edax.upcast.Upcaster`. When one payload changes, give that name an upcaster (or one more
    step); every other name keeps its version."""

    def __init__(
        self,
        events: dict[str, type],
        encode: Callable[[Any], bytes],
        decode: Callable[[bytes, type], Any],
        upcasters: dict[str, Upcaster] | None = None,
    ) -> None:
        self._encode = encode
        self._decode = decode
        self._by_name = dict(events)
        # Inverting name -> type drops a collision silently, and `marshal` would then persist the
        # surviving name for both types. A wrong event name in the store is permanent.
        self._by_type: dict[type, str] = {}
        for name, type_ in events.items():
            first = self._by_type.setdefault(type_, name)
            if first != name:
                raise AlreadyRegistered(
                    f"{type_.__name__} is registered under two event names: {first!r} and {name!r}"
                )
        for name, type_ in events.items():
            _check_registered(name, type_)
        # Without an upcaster an older payload decodes as it is, fine only while the shape is unchanged;
        # a newer writer's payload is refused either way.
        self._upcasters = dict(upcasters or {})
        # An upcaster keyed by an unregistered name never runs, so the stale payload it was written
        # for decodes as-is and reaches the handler with the old shape. A startup typo, not a
        # runtime failure, so it fails here rather than corrupting one event type forever.
        dangling = sorted(self._upcasters.keys() - self._by_name.keys())
        if dangling:
            raise CodecError(f"upcasters registered for unknown event names: {dangling}")
        # The upcaster is a name's only version source, so the two cannot disagree, and a hole in its
        # chain fails here rather than on the first stored payload at the missing version.
        self._versions: dict[str, int] = {}
        for name in self._by_name:
            with _as_codec_error(f"the upcaster for {name!r}"):
                self._versions[name] = target(self._upcasters.get(name))

    def type_of(self, name: str) -> type | None:
        """The class this codec reads ``name`` as, or ``None`` for a name it does not read: what a
        :class:`~edax.command.CommandBus` checks a handler's command class against."""
        return self._by_name.get(name)

    def marshal(self, event: Any) -> Payload:
        name = self._by_type.get(type(event))
        if name is None:
            raise CodecError(f"no event name registered for {type(event).__name__}")
        with _as_codec_error(f"encoding {name}"):
            return Payload(name, self._versions[name], self._encode(event))

    def unmarshal(self, event: Marshaled) -> Any:
        """The domain object behind ``event``: a :class:`~edax.message.Payload`, an
        :class:`~edax.message.EventMessage` or a :class:`~edax.message.CommandMessage`."""
        type_ = self._by_name.get(event.name)
        if type_ is None:
            raise CodecError(f"no type registered for event {event.name!r}")
        # On every unmarshal, with or without an upcaster for the name: the kernel decides that a payload
        # is current, older (raised, or read as it is with no upcaster), or from a *newer* writer than
        # this codec (an error, never a silent decode under the older schema).
        version = self._versions[event.name]
        with _as_codec_error(f"upcasting {event.name} v{event.version}->v{version}"):
            data = upcast(self._upcasters.get(event.name), bytes(event.data), from_=event.version, to=version)
        with _as_codec_error(f"decoding {event.name} v{event.version}"):
            return self._decode(data, type_)


#: What a plain host-library type declares, and the host type that reads the wire text in its place.
_PLAIN_HOST_VALUES: dict[type[msgspec.inspect.Type], str] = {
    msgspec.inspect.DateTimeType: "datetime.datetime (declare edax.Instant)",
    msgspec.inspect.DecimalType: "decimal.Decimal (declare edax.Decimal)",
}


def _check_registered(name: str, type_: type) -> None:
    """Refuse a type no other SDK reads alike, at any depth, when the codec is built rather than on the first
    payload. An ``array_like`` Struct is written and read as an array, while every SDK reads a struct from a
    map only. A ``datetime`` or ``Decimal`` field is converted by msgspec before the kernel sees its text, so
    a fraction finer than a microsecond is rounded and ``1_000`` reads as a number, where the other SDKs
    refuse both."""
    with _as_codec_error(f"the type registered for {name!r}"):
        nodes = _json.type_nodes(msgspec.inspect.type_info(type_))
    positional = sorted(
        node.cls.__name__
        for node in nodes
        if isinstance(node, msgspec.inspect.StructType) and node.array_like
    )
    if positional:
        raise CodecError(f"{name!r} holds array_like structs {positional}: a struct is a map only")
    plain = sorted({_PLAIN_HOST_VALUES[type(node)] for node in nodes if type(node) in _PLAIN_HOST_VALUES})
    if plain:
        raise CodecError(f"{name!r} holds {', '.join(plain)}: only a host type reads the wire text exactly")


def json_codec(events: dict[str, type], *, upcasters: dict[str, Upcaster] | None = None) -> RegistryCodec:
    """A PayloadCodec that serializes payloads as JSON (via msgspec), through the SDK's one encoder. An
    :class:`~edax.Decimal` or :class:`~edax.Instant` field is written as its canonical text and read from the
    text the payload carries, through the kernel, as the Rust and Go SDKs read it: ``"9.50"`` reads as
    ``9.5``, and ``"1_000"``, a number or a fraction finer than a microsecond is refused. A struct reads from
    a map only, at every depth. A name writes version 1, or one past the highest step of its upcaster in
    ``upcasters``. The upcaster raises that name's older stored payloads on load (see
    :class:`~edax.upcast.Upcaster`). A hole in a chain, or a type that holds an ``array_like`` Struct, a
    ``datetime`` or a ``decimal.Decimal`` at any depth, raises :class:`~edax.errors.CodecError` here, not on
    the first event."""
    return RegistryCodec(events, _json.encode_bytes, _json.decode_json, upcasters)


def msgpack_codec(events: dict[str, type], *, upcasters: dict[str, Upcaster] | None = None) -> RegistryCodec:
    """A PayloadCodec that serializes payloads as MessagePack (via msgspec), smaller and binary. Host values,
    versions, ``upcasters`` and the refusals at registration work as in :func:`json_codec`: an
    :class:`~edax.Instant` is the same UTC text, not a MessagePack timestamp, so the Rust SDK reads it. A
    MessagePack timestamp an earlier release stored still reads as an :class:`~edax.Instant`, rounded to the
    microsecond by msgspec before the kernel sees it. Bytes stay a native byte string."""
    return RegistryCodec(events, _json.encode_msgpack, _json.decode_msgpack, upcasters)

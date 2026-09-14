"""Ready-made event codecs: marshal a domain event to bytes by name, and back.

An :class:`~edax.store.EventCodec` is the boundary between your domain event objects and the
``name``/``version``/``bytes`` the store persists. These two cover the common case, a fixed set of event
types each serialized whole (``json_codec({"Deposited": Deposited})``). A snapshot (``AggregateCodec``)
stays hand-written: only you know which fields are state and which are derived. Every failure here raises
:class:`~edax.errors.CodecError`, which is terminal, so the caller dead-letters instead of redelivering."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Callable, Optional

import msgspec

from .aggregate import Event
from .errors import AlreadyRegistered, CodecError
from .upcast import Upcaster


@contextmanager
def _as_codec_error(what: str) -> Iterator[None]:
    """Re-raise a marshal/unmarshal failure as the terminal :class:`~edax.errors.CodecError`. An
    untyped ``KeyError``/``ValueError`` would be classed transient and redelivered forever."""
    try:
        yield
    except Exception as e:
        raise CodecError(f"{what}: {e}") from e


class RegistryEventCodec:
    """Maps event names to types and serializes each whole. Build via :func:`json_codec` /
    :func:`msgpack_codec` rather than directly."""

    def __init__(
        self,
        events: dict[str, type],
        version: int,
        encode: Callable[[Any], bytes],
        decode: Callable[[bytes, type], Any],
        upcasters: Optional[dict[str, Upcaster]] = None,
    ) -> None:
        # One version for the whole registry; split registries if one event must bump alone.
        self._version = version
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
        # Without an upcaster an older payload decodes as-is, fine only while the shape is unchanged.
        self._upcasters = dict(upcasters or {})
        # An upcaster keyed by an unregistered name never runs, so the stale payload it was written
        # for decodes as-is and reaches the handler with the old shape. A startup typo, not a
        # runtime failure, so it fails here rather than corrupting one event type forever.
        dangling = sorted(self._upcasters.keys() - self._by_name.keys())
        if dangling:
            raise CodecError(f"upcasters registered for unknown event names: {dangling}")

    def marshal(self, event: Any) -> Event:
        name = self._by_type.get(type(event))
        if name is None:
            raise CodecError(f"no event name registered for {type(event).__name__}")
        with _as_codec_error(f"encoding {name}"):
            return Event(name, self._version, self._encode(event))

    def unmarshal(self, event: Event) -> Any:
        type_ = self._by_name.get(event.name)
        if type_ is None:
            raise CodecError(f"no type registered for event {event.name!r}")
        data = bytes(event.data)
        upcaster = self._upcasters.get(event.name)
        if upcaster is not None:
            # No host-side version comparison: the kernel decides that a payload is current (empty
            # plan) or from a *newer* writer than this codec (an error, not a silent decode).
            with _as_codec_error(f"upcasting {event.name} v{event.version}->v{self._version}"):
                data = upcaster.to(data, from_=event.version, to=self._version)
        with _as_codec_error(f"decoding {event.name} v{event.version}"):
            return self._decode(data, type_)


def json_codec(
    events: dict[str, type], *, version: int = 1, upcasters: Optional[dict[str, Upcaster]] = None
) -> RegistryEventCodec:
    """An EventCodec that serializes payloads as JSON (via msgspec). ``upcasters`` raise older stored
    payloads to ``version`` on load (see :class:`~edax.upcast.Upcaster`)."""
    return RegistryEventCodec(
        events, version, msgspec.json.encode, lambda b, t: msgspec.json.decode(b, type=t), upcasters
    )


def msgpack_codec(
    events: dict[str, type], *, version: int = 1, upcasters: Optional[dict[str, Upcaster]] = None
) -> RegistryEventCodec:
    """An EventCodec that serializes payloads as MessagePack (via msgspec) — smaller, binary.
    ``upcasters`` raise older stored payloads to ``version`` on load."""
    return RegistryEventCodec(
        events, version, msgspec.msgpack.encode, lambda b, t: msgspec.msgpack.decode(b, type=t), upcasters
    )

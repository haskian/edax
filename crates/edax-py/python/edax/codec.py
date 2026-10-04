"""Ready-made event codecs: marshal a domain event to bytes by name, and back.

An :class:`~edax.store.EventCodec` is the boundary between your domain event objects and the
``name``/``version``/``bytes`` the store persists. These two cover the common case, a fixed set of event
types each serialized whole (``json_codec({"Deposited": Deposited})``). A snapshot (``AggregateCodec``)
stays hand-written: only you know which fields are state and which are derived. Every failure here raises
:class:`~edax.errors.CodecError`, which is terminal, so the caller dead-letters instead of redelivering."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import msgspec

from .aggregate import Event, Payload
from .errors import AlreadyRegistered, CodecError
from .upcast import Upcaster, target, upcast

__all__ = [
    "RegistryEventCodec",
    "json_codec",
    "msgpack_codec",
]


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

    def marshal(self, event: Any) -> Event:
        name = self._by_type.get(type(event))
        if name is None:
            raise CodecError(f"no event name registered for {type(event).__name__}")
        with _as_codec_error(f"encoding {name}"):
            return Event(name, self._versions[name], self._encode(event))

    def unmarshal(self, event: Payload) -> Any:
        """The domain object behind ``event``: an :class:`~edax.aggregate.Event`, an
        :class:`~edax.aggregate.EventMessage` or a :class:`~edax.aggregate.CommandMessage`."""
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


def json_codec(
    events: dict[str, type], *, upcasters: dict[str, Upcaster] | None = None
) -> RegistryEventCodec:
    """An EventCodec that serializes payloads as JSON (via msgspec). A name writes version 1, or one past
    the highest step of its upcaster in ``upcasters``. The upcaster raises that name's older stored
    payloads on load (see :class:`~edax.upcast.Upcaster`). A hole in a chain raises
    :class:`~edax.errors.CodecError` here, not on the first event."""
    return RegistryEventCodec(
        events, msgspec.json.encode, lambda b, t: msgspec.json.decode(b, type=t), upcasters
    )


def msgpack_codec(
    events: dict[str, type], *, upcasters: dict[str, Upcaster] | None = None
) -> RegistryEventCodec:
    """An EventCodec that serializes payloads as MessagePack (via msgspec), smaller and binary. Versions and
    ``upcasters`` work as in :func:`json_codec`."""
    return RegistryEventCodec(
        events, msgspec.msgpack.encode, lambda b, t: msgspec.msgpack.decode(b, type=t), upcasters
    )

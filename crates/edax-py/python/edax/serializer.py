"""Wire serializers: a :class:`Serializer` turns one kind of message to and from bytes, and
:class:`KernelSerializer` is the kernel's format."""

from __future__ import annotations

from typing import Protocol

from . import _native, errors
from .message import CommandMessage, EventMessage, MessageKind, WireFormat

__all__ = [
    "KernelSerializer",
    "Serializer",
]


class Serializer(Protocol):
    """Turns one kind of message to and from wire bytes. Supply your own for a format the lib doesn't ship.

    ``kind`` is ``"event"`` or ``"command"``: :meth:`decode` returns an :class:`EventMessage` for the first
    and a :class:`CommandMessage` for the second. Name a custom format
    ``application/vnd.<you>.<kind>+<syntax>`` and register the serializer under that same media type in
    every SDK that reads the topic; the media type is matched as one exact string, with no parameters.
    ``docs/kernels/kafka.md`` has the vocabulary."""

    # Read-only, so a plain class attribute and a computed property both satisfy it.
    @property
    def media_type(self) -> str: ...
    @property
    def kind(self) -> MessageKind: ...

    # Positional-only, so an implementation may name the parameters after its own format.
    def encode(self, msg: EventMessage | CommandMessage, /) -> bytes: ...
    def decode(self, wire: bytes, /) -> EventMessage | CommandMessage: ...


class KernelSerializer:
    """The kernel's wire format for one kind of message: ``fmt`` is ``"json"`` (default) or ``"msgpack"``,
    ``kind`` is ``"event"`` (default) or ``"command"``."""

    def __init__(self, fmt: WireFormat = "json", kind: MessageKind = "event") -> None:
        self._fmt = fmt
        self._kind: MessageKind = kind
        # Resolved once, at wiring time: a format or a kind the kernel does not know fails here, where the
        # traceback names the wiring, instead of on the first record published.
        self._media_type: str = _native.media_type(fmt, kind)

    @property
    def media_type(self) -> str:
        """The kernel's media type for this format and kind, ``application/vnd.edax.{kind}+{fmt}``."""
        return self._media_type

    @property
    def kind(self) -> MessageKind:
        """The kind of message this serializer encodes and decodes."""
        return self._kind

    def encode(self, msg: EventMessage | CommandMessage) -> bytes:
        # An extension key outside the kernel's rule is the terminal CodecError.
        if self._kind == "event" and isinstance(msg, EventMessage):
            return _native.encode_event(self._fmt, msg)
        if self._kind == "command" and isinstance(msg, CommandMessage):
            return _native.encode_command(self._fmt, msg)
        # The same message fails the same way on every retry, so the refusal is terminal.
        raise errors.CodecError(f"a {self._kind} serializer cannot encode a {type(msg).__name__}")

    def decode(self, wire: bytes) -> EventMessage | CommandMessage:
        # The kernel's `codec:` refusal is the terminal CodecError, so a poison message dead-letters.
        if self._kind == "event":
            return _native.decode_event(self._fmt, wire)
        return _native.decode_command(self._fmt, wire)

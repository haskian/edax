"""Shared serializer/aggregate helpers plus the default native-backed models."""

from __future__ import annotations

import copyreg
from dataclasses import dataclass
from typing import Any, Literal, Protocol, get_args

from . import _edax, errors

# The native binding classes are the models: an envelope crosses the binding as itself, with no
# parallel dataclass or field-by-field marshaling. Field types live in `_edax.pyi`.
Event = _edax.Event
EventEnvelope = _edax.EventEnvelope

#: The names the kernel selects a format by. A `Literal` so a type checker catches a typo; it cannot be
#: read from the kernel, so the import checks every name against it instead.
WireFormat = Literal["json", "msgpack"]


def _check_every_wire_format_is_the_kernels() -> None:
    """Fail the import if a name in :data:`WireFormat` is one the kernel does not resolve."""
    for name in get_args(WireFormat):
        with errors.mapped():
            _edax.media_type(name)


_check_every_wire_format_is_the_kernels()


# The binding emits each class with no value semantics: `==` is identity, so a getter's fresh copy is
# not equal to itself, and `namespace` / `metadata` come back in a per-process hash order. The classes
# are heap types, so the SDK gives them value semantics here, once, at import: equality and hashing by
# value, a repr naming the fields, the public module, sorted dictionaries (the kernel's own order on the
# wire), and copy/pickle reducers. The classes still cannot be subclassed; compose instead.
_ENVELOPE_FIELDS = ("id", "aggregate_id", "event", "namespace", "metadata", "timestamp", "sequence")
_EVENT_FIELDS = ("name", "version", "data")
_SAGA_COMMAND_FIELDS = ("id", "correlation_id", "command", "timestamp")


def _sorted_dict(getter: Any) -> property:
    """A property returning the native getter's dictionary sorted by key, so its order is the same in every
    process: by code point, which is the order of the kernel's UTF-8 keys on the wire."""

    def read(self: Any) -> dict[str, str]:
        return dict(sorted(getter.__get__(self, type(self)).items()))

    return property(read, doc=getter.__doc__)


def _value(fields: tuple[str, ...], obj: Any) -> tuple[Any, ...]:
    """``obj`` as the tuple of its fields, recursively: what equality and the hash compare."""
    out: list[Any] = []
    for name in fields:
        v = getattr(obj, name)
        if isinstance(v, dict):
            v = tuple(v.items())  # already sorted by the property above
        elif isinstance(v, _edax.Event):
            v = _value(_EVENT_FIELDS, v)
        out.append(v)
    return tuple(out)


def _give_value_semantics(cls: Any, fields: tuple[str, ...]) -> None:
    for name in ("namespace", "metadata"):
        if name in fields:
            setattr(cls, name, _sorted_dict(cls.__dict__[name]))

    def eq(self: Any, other: object) -> object:  # a bool, or NotImplemented for another type
        if type(other) is not type(self):
            return NotImplemented
        same: bool = _value(fields, self) == _value(fields, other)
        return same

    def hash_(self: Any) -> int:
        return hash(_value(fields, self))

    def repr_(self: Any) -> str:
        shown = ", ".join(f"{name}={getattr(self, name)!r}" for name in fields)
        return f"{cls.__name__}({shown})"

    cls.__eq__ = eq
    cls.__hash__ = hash_
    cls.__repr__ = repr_
    cls.__module__ = "edax"
    copyreg.dispatch_table[cls] = lambda v: (
        cls,
        tuple(getattr(v, name) for name in _constructor_order(fields)),
    )


def _constructor_order(fields: tuple[str, ...]) -> tuple[str, ...]:
    """The binding's constructor puts an optional parameter last, so ``sequence`` trails the envelope's."""
    return tuple(f for f in fields if f != "sequence") + (("sequence",) if "sequence" in fields else ())


_give_value_semantics(_edax.Event, _EVENT_FIELDS)
_give_value_semantics(_edax.EventEnvelope, _ENVELOPE_FIELDS)
_give_value_semantics(_edax.SagaCommand, _SAGA_COMMAND_FIELDS)


class Serializer(Protocol):
    """Turns envelopes to and from wire bytes. Supply your own for a format the lib doesn't ship.

    Name a custom format ``application/vnd.<you>.<format>+<syntax>`` and register the serializer
    under that same media type in every SDK that reads the topic; the media type is matched as one
    exact string, with no parameters. ``docs/kernels/kafka.md`` has the vocabulary."""

    # Read-only, so a plain class attribute and a computed property both satisfy it.
    @property
    def media_type(self) -> str: ...

    def encode(self, env: EventEnvelope) -> bytes: ...
    def decode(self, wire: bytes) -> EventEnvelope: ...


class KernelSerializer:
    """The kernel's envelope wire format: ``"json"`` (default) or ``"msgpack"``."""

    def __init__(self, fmt: WireFormat = "json") -> None:
        self._fmt = fmt
        # Resolved once, at wiring time: a format the kernel does not know fails here, where the
        # traceback names the wiring, instead of on the first record published.
        with errors.mapped():
            self._media_type: str = _edax.media_type(fmt)

    @property
    def media_type(self) -> str:
        """The kernel's media type for this format, ``application/vnd.edax.envelope+{fmt}``."""
        return self._media_type

    def encode(self, env: EventEnvelope) -> bytes:
        with errors.mapped():
            return _edax.encode_envelope(self._fmt, env)

    def decode(self, wire: bytes) -> EventEnvelope:
        # mapped() turns the kernel's `codec:` tag into the terminal CodecError, so a poison
        # message dead-letters instead of reading as a transient RuntimeError.
        with errors.mapped():
            return _edax.decode_envelope(self._fmt, wire)


@dataclass
class Version:
    """An aggregate's version: the one it was loaded at, plus the updates applied since."""

    initial: int
    updates: int = 0

    @property
    def current(self) -> int:
        return self.initial + self.updates

    def increment(self) -> Version:
        return Version(self.initial, self.updates + 1)


class Aggregate:
    """Base for an event-sourced aggregate. ``version`` is where it stands: ``version.initial`` is what it was
    loaded at, ``version.current`` counts the events applied since."""

    def __init__(self, id: str, initial: int = 0) -> None:
        self.id = id
        self.version = Version(initial)
        self.pending: list[Any] = []

    def transition(self, event: Any) -> None:  # pragma: no cover - overridden
        raise NotImplementedError

    def apply(self, *events: Any) -> None:
        for e in events:
            self.transition(e)
            self.version = self.version.increment()
            self.pending.append(e)

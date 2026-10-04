"""Shared serializer/aggregate helpers plus the default native-backed models."""

from __future__ import annotations

import copyreg
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypeVar, get_args

from . import _edax, errors

# The native binding classes are the models: a message crosses the binding as itself, with no parallel
# dataclass or field-by-field marshaling. Field types live in `_edax.pyi`.
#: A codec's output: the event's name, schema version and payload bytes, before an aggregate owns it.
Event = _edax.Event
#: What :class:`~edax.store.Repository` hands :meth:`~edax.store.EventStore.append`: every field of an
#: event but the time the store records it at.
EventDraft = _edax.EventDraft
#: A stored event, as ``append`` returns it and as every reader downstream sees it.
EventMessage = _edax.EventMessage
#: A command on the wire, as a saga's outbox holds it and a broker carries it.
CommandMessage = _edax.CommandMessage
#: Where a message sits in its flow: the ``correlation_id`` constant across the flow, and the
#: ``causation_id`` of the message that caused it. See :mod:`edax.causation`.
Lineage = _edax.Lineage

#: The names the kernel selects a format by. A `Literal` so a type checker catches a typo; it cannot be
#: read from the kernel, so the import checks every name against it instead.
WireFormat = Literal["json", "msgpack"]
#: The kinds of message the kernel encodes. The import checks every name against the kernel too.
MessageKind = Literal["event", "command"]


def _check_every_wire_name_is_the_kernels() -> None:
    """Fail the import if a name in :data:`WireFormat` or :data:`MessageKind` is one the kernel does not
    resolve."""
    for fmt in get_args(WireFormat):
        for kind in get_args(MessageKind):
            with errors.mapped():
                _edax.media_type(fmt, kind)


_check_every_wire_name_is_the_kernels()


class Payload(Protocol):
    """What an :class:`~edax.store.EventCodec` reads back: a name, a schema version and the payload bytes.
    An :class:`Event`, an :class:`EventMessage` and a :class:`CommandMessage` all have them."""

    @property
    def name(self) -> str: ...
    @property
    def version(self) -> int: ...
    @property
    def data(self) -> bytes: ...


# The binding emits each class with no value semantics: `==` is identity, so a getter's fresh copy is
# not equal to itself, and `extensions` / `metadata` come back in a per-process hash order. The classes
# are heap types, so the SDK gives them value semantics here, once, at import: equality and hashing by
# value, a repr naming the fields, the public module, sorted dictionaries (the kernel's own order on the
# wire), and copy/pickle reducers. The classes still cannot be subclassed; compose instead.
_EVENT_FIELDS = ("name", "version", "data")
_LINEAGE_FIELDS = ("correlation_id", "causation_id")
_DRAFT_FIELDS = (
    "id",
    "aggregate_id",
    "tenant",
    "sequence",
    "name",
    "version",
    "data",
    "correlation_id",
    "causation_id",
    "occurred_at",
    "extensions",
    "metadata",
)
_EVENT_MESSAGE_FIELDS = (
    "id",
    "aggregate_id",
    "tenant",
    "sequence",
    "name",
    "version",
    "data",
    "correlation_id",
    "causation_id",
    "occurred_at",
    "recorded_at",
    "extensions",
    "metadata",
)
_COMMAND_MESSAGE_FIELDS = (
    "id",
    "idempotency_key",
    "aggregate_id",
    "tenant",
    "name",
    "version",
    "data",
    "correlation_id",
    "causation_id",
    "issued_at",
    "extensions",
    "metadata",
)


def _sorted_dict(getter: Any) -> property:
    """A property returning the native getter's dictionary sorted by key, so its order is the same in every
    process: by code point, which is the order of the kernel's UTF-8 keys on the wire."""

    def read(self: Any) -> dict[str, str]:
        return dict(sorted(getter.__get__(self, type(self)).items()))

    return property(read, doc=getter.__doc__)


def _value(fields: tuple[str, ...], obj: Any) -> tuple[Any, ...]:
    """``obj`` as the tuple of its fields: what equality and the hash compare."""
    out: list[Any] = []
    for name in fields:
        v = getattr(obj, name)
        if isinstance(v, dict):
            v = tuple(v.items())  # already sorted by the property above
        out.append(v)
    return tuple(out)


def _rebuild(cls: type, fields: dict[str, Any]) -> Any:
    """Build ``cls`` from its fields by keyword. The binding orders a constructor's optional parameters
    last, so a positional rebuild would hand a field to the wrong parameter."""
    return cls(**fields)


# Each value class's fields, by class: what `_replace` copies and what the pickle reducer rebuilds from.
_FIELDS: dict[type, tuple[str, ...]] = {}

M = TypeVar("M")


def _fields_of(obj: Any) -> dict[str, Any]:
    """Every field of a value class instance, by name."""
    return {name: getattr(obj, name) for name in _FIELDS[type(obj)]}


def _replace(msg: M, **changes: Any) -> M:
    """``msg`` with ``changes`` applied and every other field kept. A name that is not a field of the class
    raises :class:`TypeError`, as its constructor does."""
    rebuilt: M = _rebuild(type(msg), _fields_of(msg) | changes)
    return rebuilt


def _give_value_semantics(cls: Any, fields: tuple[str, ...]) -> None:
    _FIELDS[cls] = fields
    for name in ("extensions", "metadata"):
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
    copyreg.dispatch_table[cls] = lambda v: (_rebuild, (cls, _fields_of(v)))


_give_value_semantics(_edax.Event, _EVENT_FIELDS)
_give_value_semantics(_edax.Lineage, _LINEAGE_FIELDS)
_give_value_semantics(_edax.EventDraft, _DRAFT_FIELDS)
_give_value_semantics(_edax.EventMessage, _EVENT_MESSAGE_FIELDS)
_give_value_semantics(_edax.CommandMessage, _COMMAND_MESSAGE_FIELDS)


def message_kind(msg: object) -> MessageKind:
    """The kind of ``msg``: ``"event"`` for an :class:`EventMessage`, ``"command"`` for a
    :class:`CommandMessage`. Anything else raises :class:`TypeError`."""
    if isinstance(msg, EventMessage):
        return "event"
    if isinstance(msg, CommandMessage):
        return "command"
    raise TypeError(f"expected an EventMessage or a CommandMessage, not {msg!r}")


def ordering_key(msg: EventMessage | CommandMessage) -> str:
    """The key that orders ``msg`` against others: its aggregate, or its own id when it targets none. The
    Kafka record key and a key-ordered dispatcher's worker start here. Pinned by
    ``testdata/golden/ordering_key.json``."""
    with errors.mapped():
        return _edax.ordering_key(msg.id, msg.aggregate_id)


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
        with errors.mapped():
            self._media_type: str = _edax.media_type(fmt, kind)

    @property
    def media_type(self) -> str:
        """The kernel's media type for this format and kind, ``application/vnd.edax.{kind}+{fmt}``."""
        return self._media_type

    @property
    def kind(self) -> MessageKind:
        """The kind of message this serializer encodes and decodes."""
        return self._kind

    def encode(self, msg: EventMessage | CommandMessage) -> bytes:
        with errors.mapped():  # an extension key outside the kernel's rule is the terminal CodecError
            if self._kind == "event" and isinstance(msg, EventMessage):
                return _edax.encode_event(self._fmt, msg)
            if self._kind == "command" and isinstance(msg, CommandMessage):
                return _edax.encode_command(self._fmt, msg)
        # The same message fails the same way on every retry, so the refusal is terminal.
        raise errors.CodecError(f"a {self._kind} serializer cannot encode a {type(msg).__name__}")

    def decode(self, wire: bytes) -> EventMessage | CommandMessage:
        # mapped() turns the kernel's `codec:` tag into the terminal CodecError, so a poison
        # message dead-letters instead of reading as a transient RuntimeError.
        with errors.mapped():
            if self._kind == "event":
                return _edax.decode_event(self._fmt, wire)
            return _edax.decode_command(self._fmt, wire)


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
        #: The tenant this instance belongs to, set by the :class:`~edax.store.Repository` that
        #: loaded it and stamped onto every event and command it saves. ``None`` outside a multi-tenant
        #: deployment. A host never assigns it: the repository fills it from the call that loaded the
        #: aggregate, which is what keeps a load and the save that follows on one tenant. So under
        #: ``POOL`` or ``BRIDGE`` a new aggregate starts from ``repo.load(id, tenant=...)``, which returns
        #: it empty at version 0 with the tenant set; one built with this constructor has none, and its
        #: save raises :class:`~edax.errors.CodecError`.
        self.tenant: str | None = None

    def transition(self, event: Any) -> None:  # pragma: no cover - overridden
        raise NotImplementedError

    def apply(self, *events: Any) -> None:
        for e in events:
            self.transition(e)
            self.version = self.version.increment()
            self.pending.append(e)

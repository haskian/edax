"""The messages: the native-backed event and command models, their value semantics, and the kernel's
ordering key. Every other module builds on these."""

from __future__ import annotations

from typing import Literal, Protocol, get_args

from . import _names, _native
from ._values import give_value_semantics

__all__ = [
    "CommandMessage",
    "EventDraft",
    "EventMessage",
    "Lineage",
    "Marshaled",
    "MessageKind",
    "Payload",
    "WireFormat",
    "message_kind",
    "ordering_key",
]

# The native binding classes are the models: a message crosses the binding as itself, with no parallel
# dataclass or field-by-field marshaling. Field types live in `_native.pyi`.
#: A codec's output: an event's or a command's name, schema version and payload bytes. An aggregate holds its
#: pending events in this shape, and a saga hands the commands it sends over in it.
Payload = _native.Payload
#: What :class:`~edax.repository.Repository` hands :meth:`~edax.store.EventStore.append`: every field of an
#: event but the time the store records it at.
EventDraft = _native.EventDraft
#: A stored event, as ``append`` returns it and as every reader downstream sees it.
EventMessage = _native.EventMessage
#: A command on the wire, as a saga's outbox holds it and a broker carries it.
CommandMessage = _native.CommandMessage
#: Where a message sits in its flow: the ``correlation_id`` constant across the flow, and the
#: ``causation_id`` of the message that caused it. See :mod:`edax.causation`.
Lineage = _native.Lineage

#: The names the kernel selects a format by. A `Literal` so a type checker catches a typo; it cannot be
#: read from the kernel, so the import checks every name against it instead.
WireFormat = Literal["json", "msgpack"]
#: The kinds of message the kernel encodes. The import checks every name against the kernel too.
MessageKind = Literal["event", "command"]


_names.check(get_args(WireFormat), _native.wire_formats(), "wire formats")
_names.check(get_args(MessageKind), _native.message_kinds(), "message kinds")


class Marshaled(Protocol):
    """What a :class:`~edax.aggregate.PayloadCodec` reads back: a name, a schema version and the payload
    bytes. A :class:`Payload`, an :class:`EventMessage` and a :class:`CommandMessage` all have them."""

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
give_value_semantics(_native.Payload)
give_value_semantics(_native.Lineage)
give_value_semantics(_native.EventDraft)
give_value_semantics(_native.EventMessage)
give_value_semantics(_native.CommandMessage)


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
    return _native.ordering_key(msg.id, msg.aggregate_id)

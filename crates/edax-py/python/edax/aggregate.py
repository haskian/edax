"""Shared serializer/aggregate helpers plus the default native-backed models."""

from __future__ import annotations

import copyreg
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from . import _edax, errors

# The native binding classes are the models: an envelope crosses the binding as itself, with no
# parallel dataclass or field-by-field marshaling. Field types live in `_edax.pyi`.
Event = _edax.Event
EventEnvelope = _edax.EventEnvelope
Envelope = EventEnvelope  # the alias callers construct with: `edax.Envelope(...)`

WireFormat = Literal["json", "msgpack"]


# The native classes have no __reduce__, so copy/pickle need these copyreg reducers (e.g. to hand
# an envelope to a ProcessPoolExecutor). Value `==`, repr and hash are type slots Python cannot
# add to a native class; those need the binding to emit `#[pyclass(frozen, eq, hash, str)]`.
def _rebuild_event(name: str, version: int, data: bytes) -> Event:
    return _edax.Event(name, version, data)


def _rebuild_envelope(
    id: str,
    aggregate_id: str,
    event: Event,
    namespace: dict[str, str],
    metadata: dict[str, str],
    timestamp: int,
    sequence: int | None,
) -> EventEnvelope:
    return _edax.EventEnvelope(id, aggregate_id, event, namespace, metadata, timestamp, sequence)


copyreg.dispatch_table[_edax.Event] = lambda e: (_rebuild_event, (e.name, e.version, e.data))
copyreg.dispatch_table[_edax.EventEnvelope] = lambda v: (
    _rebuild_envelope,
    (v.id, v.aggregate_id, v.event, v.namespace, v.metadata, v.timestamp, v.sequence),
)


class Serializer(Protocol):
    """Turns envelopes to and from wire bytes. Supply your own for a format the lib doesn't ship."""

    def encode(self, env: EventEnvelope) -> bytes: ...
    def decode(self, wire: bytes) -> EventEnvelope: ...


class KernelSerializer:
    """The kernel's envelope wire format: ``"json"`` (default) or ``"msgpack"``."""

    def __init__(self, fmt: WireFormat = "json") -> None:
        self._fmt = fmt

    def encode(self, env: EventEnvelope) -> bytes:
        with errors.mapped():
            return _edax.encode_envelope(
                self._fmt,
                env.id,
                env.aggregate_id,
                env.event.name,
                env.event.version,
                env.event.data,
                env.namespace,
                env.metadata,
                env.timestamp,
                env.sequence,
            )

    def decode(self, wire: bytes) -> EventEnvelope:
        # mapped() turns the kernel's `codec:` tag into the terminal CodecError, so a poison
        # message dead-letters instead of reading as a transient RuntimeError.
        with errors.mapped():
            return _edax.decode_envelope(self._fmt, wire)


def kernel(fmt: WireFormat = "json") -> KernelSerializer:
    """The default serializer over the kernel wire format."""
    return KernelSerializer(fmt)


@dataclass
class Version:
    """An aggregate's version: initial (from storage) plus updates applied now."""

    initial: int
    updates: int = 0

    @property
    def current(self) -> int:
        return self.initial + self.updates

    def increment(self) -> Version:
        return Version(self.initial, self.updates + 1)


class Aggregate:
    """Base for an event-sourced aggregate."""

    def __init__(self, id: str, initial: int = 0) -> None:
        self.id = id
        self.sequence = Version(initial)
        self.pending: list[Any] = []

    def transition(self, event: Any) -> None:  # pragma: no cover - overridden
        raise NotImplementedError

    def apply(self, *events: Any) -> None:
        for e in events:
            self.transition(e)
            self.sequence = self.sequence.increment()
            self.pending.append(e)

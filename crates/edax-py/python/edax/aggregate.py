"""The event-sourced aggregate and the two codecs a :class:`~edax.repository.Repository` needs: one for its
events, one for its snapshot."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

from .message import Marshaled, Payload

__all__ = [
    "Aggregate",
    "AggregateCodec",
    "CommandCodec",
    "EventCodec",
    "PayloadCodec",
    "Version",
    "stand_at",
]


@dataclass(frozen=True)
class Version:
    """An aggregate's version: the one it was loaded at, plus the updates applied since. Derived from the
    aggregate, never set on it."""

    initial: int
    updates: int = 0

    @property
    def current(self) -> int:
        return self.initial + self.updates


class Aggregate:
    """Base for an event-sourced aggregate. ``version`` is where it stands: ``version.initial`` is what it was
    loaded at, ``version.current`` counts the events applied since. Both follow from the events
    :meth:`apply` recorded, so the version a save writes and the events it writes cannot disagree."""

    def __init__(self, id: str, initial: int = 0) -> None:
        self.id = id
        self._initial = initial
        self._pending: list[Any] = []
        #: The tenant this instance belongs to, set by the :class:`~edax.repository.Repository` that
        #: loaded it and stamped onto every event and command it saves. ``None`` outside a multi-tenant
        #: deployment. A host never assigns it: the repository fills it from the call that loaded the
        #: aggregate, which is what keeps a load and the save that follows on one tenant. So under
        #: ``POOL`` or ``BRIDGE`` a new aggregate starts from ``repo.load(id, tenant=...)``, which returns
        #: it empty at version 0 with the tenant set. One built with this constructor has none, and its
        #: save raises :class:`~edax.errors.CodecError`.
        self.tenant: str | None = None

    @property
    def version(self) -> Version:
        return Version(self._initial, len(self._pending))

    @property
    def pending(self) -> tuple[Any, ...]:
        """The events :meth:`apply` recorded since the last load or save, in order. Read-only: only
        :meth:`apply` adds one, so the version always counts them."""
        return tuple(self._pending)

    def transition(self, event: Any) -> None:  # pragma: no cover - overridden
        raise NotImplementedError

    def apply(self, *events: Any) -> None:
        for e in events:
            self.transition(e)
            self._pending.append(e)


def stand_at(agg: Aggregate, version: int, *, tenant: str | None) -> None:
    """Leave ``agg`` at stored ``version`` of ``tenant``, with nothing pending: what a
    :class:`~edax.repository.Repository` load or save does, and the one way its version and tenant change.
    A host never calls it: a load names the tenant, and a save keeps it."""
    agg._initial = version
    agg._pending.clear()
    agg.tenant = tenant


# Invariant: the aggregate is returned by `load` and consumed by `dump`, so neither variance is sound.
A = TypeVar("A", bound=Aggregate)


class PayloadCodec(Protocol):
    """Bridges a domain value, an event or a command, and its marshaled :class:`~edax.message.Payload`
    (name/version/bytes). ``unmarshal`` reads any :class:`~edax.message.Marshaled`: a ``Payload``, a stored
    :class:`~edax.message.EventMessage`, or a :class:`~edax.message.CommandMessage`. Parameters are
    positional-only, so an implementation may name them after its own type."""

    def marshal(self, value: Any, /) -> Payload: ...
    def unmarshal(self, payload: Marshaled, /) -> Any: ...


#: :class:`PayloadCodec` in its event role, as a :class:`~edax.repository.Repository` takes it.
EventCodec = PayloadCodec


class CommandCodec(PayloadCodec, Protocol):
    """:class:`PayloadCodec` in its command role, as a :class:`~edax.command.CommandBus` takes it. It also
    says which class it reads each command name as, so the bus refuses at wiring a handler for a command its
    codec could never decode, or would decode as another class. :class:`~edax.codec.RegistryCodec` is
    one."""

    def type_of(self, name: str, /) -> type | None: ...


class AggregateCodec(Protocol[A]):
    """Bridges an aggregate and its snapshot bytes. Generic in the aggregate, so
    :class:`~edax.repository.Repository` hands your own class back with no downcast. Parameters are
    positional-only, so an implementation may name them after its own aggregate: a structural match compares
    parameter *names* otherwise, and ``dump(self, account)`` would not satisfy ``dump(self, agg)``."""

    type: str

    def load(self, id: str, version: int, data: bytes | None, /) -> A:
        """Rebuild an aggregate at ``version``; ``data is None`` means none stored yet (empty at v0)."""
        ...

    def dump(self, agg: A, /) -> bytes: ...

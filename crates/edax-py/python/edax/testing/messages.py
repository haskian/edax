"""Messages built by hand for tests.

:func:`draft`, :func:`message` and :func:`command` are the rich bases a clause starts from: fresh time-ordered
ids, a tenant, a JSON body with a non-ASCII memo, a lineage, a trace parent and the application's metadata,
as the SDK writes them in production. A case rebuilds one with only the field under test changed.
:func:`event` is a stored event with stable ids, to publish with no store behind it, and :func:`drafted` and
:func:`recorded` move an event between its two shapes: the draft a repository hands ``append``, and the
message the store keeps. :func:`saga_command` is a command a saga step stages, as the kernel builds it."""

from __future__ import annotations

from .. import _native
from .._values import FIELDS
from ..delivery import Delivery, M
from ..message import CommandMessage, EventDraft, EventMessage, Payload

__all__ = [
    "command",
    "delivery",
    "draft",
    "drafted",
    "event",
    "event_of",
    "message",
    "recorded",
    "saga_command",
]

#: The W3C trace parent every rich base carries in its ``extensions``.
_TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

#: When the producer says a drafted fact happened, in unix nanoseconds: before any store records it.
_OCCURRED_AT = 1_714_564_799_000_000_000

#: The application's context on every rich base. Non-ASCII, because a latin-1 column passes every test
#: written in English.
_METADATA = {"channel": "web", "note": "déjà-vu"}


def draft(
    aggregate_id: str, sequence: int | None, *, name: str = "Deposited", tenant: str | None = "acme"
) -> EventDraft:
    """One event draft as a repository of a multi-tenant deployment writes it: a fresh time-ordered id, the
    tenant, a JSON body, the lineage of a new flow, the producer's own time, a trace parent and the
    application's metadata."""
    lineage = _native.originate(_native.guid())
    body = f'{{"account_id":"{aggregate_id}","amount":100,"currency":"EUR","memo":"déjà-vu"}}'
    return EventDraft(
        id=_native.guid(),
        aggregate_id=aggregate_id,
        tenant=tenant,
        sequence=sequence,
        name=name,
        version=1,
        data=body.encode(),
        correlation_id=lineage.correlation_id,
        causation_id=lineage.causation_id,
        occurred_at=_OCCURRED_AT,
        extensions={"traceparent": _TRACEPARENT},
        metadata=dict(_METADATA),
    )


def message(aggregate_id: str, sequence: int | None, *, name: str = "Deposited") -> EventMessage:
    """One stored event as a relay publishes it: a :func:`draft` recorded at the current nanosecond."""
    return recorded(draft(aggregate_id, sequence, name=name), _native.current_timestamp())


def command(aggregate_id: str | None, *, tenant: str | None = "acme") -> CommandMessage:
    """One command as :func:`~edax.command.command_message` builds it: a fresh id, a client request key, the
    target aggregate (``None`` for none), the tenant, a JSON body, the lineage of the flow that decided it,
    a trace parent and the application's metadata."""
    return CommandMessage(
        id=_native.guid(),
        idempotency_key=f"req-{_native.guid()}",
        aggregate_id=aggregate_id,
        tenant=tenant,
        name="Ship",
        version=1,
        data=f'{{"order_id":"{aggregate_id}","memo":"déjà-vu"}}'.encode(),
        correlation_id=_native.guid(),
        causation_id=_native.guid(),
        issued_at=_native.current_timestamp(),
        extensions={"traceparent": _TRACEPARENT},
        metadata=dict(_METADATA),
    )


def saga_command(saga: str, instance_id: str, version: int, index: int) -> CommandMessage:
    """The ``index``-th command a step of ``saga`` sends when it commits ``version`` of ``instance_id``: the
    kernel's row, with a real ``Fulfill`` payload, caused by the ``OrderPaid`` event
    ``evt-<instance_id>-<version>`` of the flow ``flow-<instance_id>``, in that event's tenant."""
    payload = Payload("Fulfill", 1, f'{{"order_id":"{instance_id}"}}'.encode())
    return _native.saga_command(
        saga,
        instance_id,
        version,
        index,
        payload,
        f"evt-{instance_id}-{version}",
        f"flow-{instance_id}",
        1_714_564_800_000_000_001,
        "acme",
    )


def event(
    aggregate_id: str,
    name: str,
    sequence: int | None = None,
    *,
    id: str | None = None,
    version: int = 1,
    data: bytes = b"",
    correlation_id: str | None = None,
    causation_id: str | None = None,
    occurred_at: int | None = None,
    extensions: dict[str, str] | None = None,
    metadata: dict[str, str] | None = None,
    recorded_at: int = 0,
    tenant: str | None = None,
) -> EventMessage:
    """A stored event built by hand, for a test or an example that publishes one with no store behind it.

    ``id`` defaults to a stable ``{aggregate_id}-{name}-{sequence}``. The lineage is the kernel's
    :func:`~edax.originate`: a root's by default, whose correlation id is the event's own id and whose
    causation id is the correlation id. ``extensions`` and ``metadata`` default to empty, and
    ``recorded_at`` to 0."""
    message_id = f"{aggregate_id}-{name}-{sequence}" if id is None else id
    lineage = _native.originate(message_id, correlation_id, causation_id)
    drafted_event = EventDraft(
        id=message_id,
        aggregate_id=aggregate_id,
        tenant=tenant,
        sequence=sequence,
        name=name,
        version=version,
        data=data,
        correlation_id=lineage.correlation_id,
        causation_id=lineage.causation_id,
        occurred_at=occurred_at,
        extensions=extensions or {},
        metadata=metadata or {},
    )
    return recorded(drafted_event, recorded_at)


def drafted(msg: EventMessage) -> EventDraft:
    """The draft ``msg`` was stored from: every field but ``recorded_at``."""
    return EventDraft(**{name: getattr(msg, name) for name in FIELDS[EventDraft]})


def recorded(draft: EventDraft, recorded_at: int) -> EventMessage:
    """The message a store keeps for ``draft``: every field as drafted, recorded at ``recorded_at``
    nanoseconds."""
    return _native.record_event(draft, recorded_at)


def _settles_nothing() -> None:
    """A settlement nobody observes."""


def delivery(msg: M) -> Delivery[M]:
    """A delivery of ``msg`` whose ack and nack do nothing: what a test submits to a
    :class:`~edax.dispatcher.KeyOrderedDispatcher` when it watches the handler, not the settlement."""
    return Delivery(msg, _settles_nothing, _settles_nothing)


def event_of(msg: EventMessage | CommandMessage) -> EventMessage:
    """Narrow a delivered message to the event that was published, failing the test on a command."""
    assert isinstance(msg, EventMessage), f"{msg.id} came back as a command; it was published as an event"
    return msg

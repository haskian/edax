"""What a transport hands a consumer, and what a dead-letter sink takes: a :class:`Delivery` (a decoded
message with the handles that settle it), an :class:`Undecodable` (a poison record no handler may see), and
the :class:`DeadLetter` a dispatcher and a relay alike hand their sink."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Final, Generic, TypeVar

from ._async import MaybeAwaitable
from .message import CommandMessage, EventMessage

__all__ = [
    "UNDECODABLE",
    "AckFn",
    "DeadLetter",
    "DeadLetterSink",
    "Delivery",
    "LiveFn",
    "Undecodable",
    "always_live",
]


AckFn = Callable[[], MaybeAwaitable[None]]
LiveFn = Callable[[], bool]
#: The message kind a delivery carries and a dispatcher's handler takes: an event, a command, or either
#: when one transport carries both.
M = TypeVar("M", bound=EventMessage | CommandMessage)
#: What a :class:`DeadLetter` carries: a dispatcher's message or :class:`Undecodable`, or a relay's row.
T = TypeVar("T")

#: The name an observer reports an :class:`Undecodable` under: it has no message, so no message name.
UNDECODABLE: Final[str] = "edax.undecodable"


def always_live() -> bool:
    """The liveness of a delivery whose transport never retires one it handed out, such as a queue that
    requeues a nacked delivery rather than rewinding: the default ``live`` of a :class:`Delivery`."""
    return True


# Settle callbacks a cancel no longer awaits. The loop keeps only weak references to tasks, so these hold
# each one until it ends.
_SETTLING: set[asyncio.Future[None]] = set()
logger = logging.getLogger(__name__)


def _log_detached_failure(running: asyncio.Future[None]) -> None:
    if not running.cancelled() and (error := running.exception()) is not None:
        logger.error("edax delivery: settlement failed after cancellation", exc_info=error)


async def _run_to_end(outcome: MaybeAwaitable[None]) -> None:
    """Await a settle callback's ``outcome`` so that a cancel of the caller does not cut it short. The
    cancel still reaches the caller at once. Cut short before its transport effect, an ack or a nack
    would leave the item marked settled with nothing done, and nothing would hand it back."""
    if not inspect.isawaitable(outcome):
        return
    running = asyncio.ensure_future(outcome)
    _SETTLING.add(running)
    running.add_done_callback(_SETTLING.discard)
    try:
        await asyncio.shield(running)
    except asyncio.CancelledError:
        # Nobody awaits the callback now. Consume and report its eventual failure ourselves.
        running.add_done_callback(_log_detached_failure)
        raise


class _Settles:
    """The settle handles a polled item carries: ``ack`` once it is done with, ``nack`` to have it
    redelivered. ``settled`` records that either was called, so a stopped worker hands back only what is
    left. A cancel during the call does not stop the callback: it runs to its end."""

    on_ack: AckFn
    on_nack: AckFn
    settled: bool

    async def ack(self) -> None:
        self.settled = True
        await _run_to_end(self.on_ack())

    async def nack(self) -> None:
        self.settled = True
        await _run_to_end(self.on_nack())


@dataclass
class Delivery(_Settles, Generic[M]):
    """A broker delivery: the message, an :class:`~edax.message.EventMessage` or a
    :class:`~edax.message.CommandMessage` (``Delivery[EventMessage]``, ``Delivery[CommandMessage]``), plus
    the handles that settle it. The transport handle (a Kafka
    offset, an SQS receipt) is per-delivery state, not stable across redelivery, so the adapter captures it
    in the ``on_ack`` / ``on_nack`` closures instead of on the message. A
    :class:`~edax.dispatcher.KeyOrderedDispatcher` acks on success and nacks if the handler raises.

    ``live`` tells whether the transport still counts on this delivery. A log-based transport (Kafka, the
    in-memory broker) retires every delivery it handed out from a partition a nack rewinds or a rebalance
    takes away, and fetches those messages again. A worker skips a delivery that is no longer live: no
    handler run and no settlement, so a later event of a key never runs ahead of the redelivered earlier
    one. ``origin`` is the transport's own record of the message, for a dead-letter sink that republishes
    it (a Kafka consumer sets a :class:`~edax.backend.kafka.KafkaFetched`); ``None`` when there is none."""

    message: M
    on_ack: AckFn
    on_nack: AckFn
    attempt: int = 0  # the broker's own redelivery count if it keeps one, else 0; see max_attempts
    live: LiveFn = always_live
    origin: Any = None
    #: Whether :meth:`ack` or :meth:`nack` has been called: a worker stopped before either hands it back.
    settled: bool = field(default=False, init=False)

    # The dispatcher reads a delivery and a poison record through these, with no switch on which it holds.
    @property
    def id(self) -> str:
        """The message's id: what the retry policy counts attempts by, inside :attr:`tenant`."""
        return self.message.id

    @property
    def tenant(self) -> str | None:
        """The message's tenant: two tenants' stores may mint the same id, so the retry policy counts
        each tenant's message apart."""
        return self.message.tenant

    @property
    def name(self) -> str:
        """The message's name, which an observer reports it under."""
        return self.message.name

    @property
    def dead_message(self) -> M:
        """What a dead-letter sink gets as :attr:`DeadLetter.message`: the message."""
        return self.message


@dataclass
class Undecodable(_Settles):
    """A polled record whose payload would not decode: a poison message. It carries no message, so no
    handler can see it. A consumer loop settles it with
    :meth:`~edax.dispatcher.KeyOrderedDispatcher.settle_poison`, the way a terminal handler failure settles:
    the dead-letter sink gets it as :attr:`DeadLetter.message`.

    ``id`` is the transport's own id for the record (a Kafka ``topic/partition/offset``): every other field
    lives inside the payload that would not decode. ``payload`` is the raw bytes, ``error`` why they would
    not decode, and ``origin`` the transport's record, as on :class:`Delivery`."""

    id: str
    payload: bytes
    error: BaseException
    on_ack: AckFn
    on_nack: AckFn
    origin: Any = None
    settled: bool = field(default=False, init=False)

    @property
    def attempt(self) -> int:
        """``0``: a poison record has no broker count, and dies on its first, terminal, attempt."""
        return 0

    @property
    def tenant(self) -> None:
        """``None``: the tenant lives inside the payload that would not decode."""
        return None

    @property
    def name(self) -> str:
        """:data:`UNDECODABLE`: the record has no message, so no message name."""
        return UNDECODABLE

    @property
    def dead_message(self) -> Undecodable:
        """What a dead-letter sink gets as :attr:`DeadLetter.message`: the poison record itself."""
        return self


@dataclass(frozen=True)
class DeadLetter(Generic[T]):
    """What a dead-letter sink takes, from a :class:`~edax.dispatcher.KeyOrderedDispatcher` and a
    :class:`~edax.outbox.Relay` alike: the Rust SDK's ``DeadLetter``.

    ``message`` is what died: a dispatcher's message or :class:`Undecodable` poison, or a relay's outbox row.
    ``attempt`` is the failed attempt that killed it, as the kernel counts it: ``1`` for a terminal failure
    on its first try, at least ``max_attempts`` for an exhausted transient one, and more when a sink failed on
    an earlier try. ``failure`` is what killed it, named as the Rust field is. ``origin`` is the transport's
    record of a delivered message (:attr:`Delivery.origin`), for a sink that republishes it; ``None`` for a
    relay's row."""

    message: T
    attempt: int
    failure: BaseException
    origin: Any = None


#: A dead-letter sink: takes the :class:`DeadLetter`, and returns once it is somewhere a human will look. A
#: sink that raises hands the message back for another try. It runs at least once per dead message, so it
#: deduplicates by the message's id.
DeadLetterSink = Callable[[DeadLetter[T]], MaybeAwaitable[None]]

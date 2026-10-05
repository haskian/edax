"""What a :class:`~edax.outbox.Relay` and a :class:`~edax.dispatcher.KeyOrderedDispatcher` share when a
message fails: the kernel's retry and dead-letter policy (the relay's inside the kernel's drain rule), its
closed sets of answers, and the dead-letter sink step."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, TypeVar, get_args

from . import _names, _native
from ._async import MaybeAwaitable, maybe_await
from ._owned import ThreadBound
from ._wiring import wiring_u32
from .delivery import DeadLetter, DeadLetterSink
from .errors import RetryFailure, code_of

#: What the kernel's retry policy tells a host to do with a failed message: hand it back after a backoff
#: step, dead-letter it, or stop on a fatal failure.
RetryAction = Literal["nack", "dead", "stop"]
RETRY_ACTIONS: tuple[RetryAction, ...] = get_args(RetryAction)
_names.check(RETRY_ACTIONS, _native.retry_actions(), "retry actions")
RETRY_FAILURES: tuple[RetryFailure, ...] = get_args(RetryFailure)
_names.check(RETRY_FAILURES, _native.retry_failures(), "retry failure kinds")
#: What the kernel's drain rule tells a relay to do with a row whose publish failed: leave it and its key's
#: later rows for the next drain, dead-letter it, or stop on a fatal failure.
DrainAction = Literal["hand_back", "dead_letter", "stop"]
DRAIN_ACTIONS: tuple[DrainAction, ...] = get_args(DrainAction)
_names.check(DRAIN_ACTIONS, _native.drain_actions(), "drain actions")

#: What a dead message is: a dispatcher's message or poison record, or a relay's row.
T = TypeVar("T")
#: The kernel's answer to a sink's outcome: a dispatcher policy's backoff step, or a drain's row answer.
Answer = TypeVar("Answer")
#: A native policy handle: a dispatcher's retry policy or a relay's drain rule.
Policy = TypeVar("Policy")


@dataclass(frozen=True)
class Nack:
    """Hand the message back after the backoff ``step``. ``contended`` when the failure was contention for an
    idempotency claim, which counts no attempt."""

    step: int
    contended: bool


@dataclass(frozen=True)
class HandBack:
    """Leave the row, and its key's later rows, for the next drain."""


@dataclass(frozen=True)
class Dead:
    """Dead-letter the message: ``attempt`` is the failed attempt that killed it."""

    attempt: int


@dataclass(frozen=True)
class Stop:
    """Stop on a fatal failure: every message after it would fail alike."""


#: A dispatcher policy's answer to a failed message, decoded once from the kernel's flat struct.
RetryVerdict = Nack | Dead | Stop
#: A drain's answer to a failed row, decoded the same way.
RowVerdict = HandBack | Dead | Stop


def retry_verdict(verdict: _native.RetryVerdict) -> RetryVerdict:
    """The kernel's retry ``verdict`` as one typed answer. Pinned by ``testdata/golden/retry_step.json``."""
    action = _names.named(verdict.action, RETRY_ACTIONS, "retry action")
    match action:
        case "nack":
            failure = _names.named(verdict.failure, RETRY_FAILURES, "retry failure")
            return Nack(_names.given(verdict.step, "nack's backoff step"), failure == "contended")
        case "dead":
            return Dead(_names.given(verdict.attempt, "dead attempt"))
        case "stop":
            return Stop()
        case _:
            _names.unreachable(action)


def row_verdict(answer: _native.DrainRowFailed) -> RowVerdict:
    """The kernel's drain ``answer`` for a failed row as one typed answer. Pinned by
    ``testdata/golden/relay_drain.json``."""
    action = _names.named(answer.action, DRAIN_ACTIONS, "drain action")
    match action:
        case "hand_back":
            return HandBack()
        case "dead_letter":
            return Dead(_names.given(answer.attempt, "dead row's attempt"))
        case "stop":
            return Stop()
        case _:
            _names.unreachable(action)


def retry_policy(max_attempts: int, sink: bool, what: str) -> ThreadBound[_native.Retries]:
    """The kernel's retry and dead-letter policy of a :class:`~edax.dispatcher.KeyOrderedDispatcher`."""
    return _policy(_native.retries, max_attempts, sink, what)


def drain_policy(max_attempts: int, sink: bool) -> ThreadBound[_native.Drain]:
    """The kernel's drain rule of a :class:`~edax.outbox.Relay`."""
    return _policy(_native.drain, max_attempts, sink, "Relay")


def _policy(
    build: Callable[[int, bool], Policy], max_attempts: int, sink: bool, what: str
) -> ThreadBound[Policy]:
    """A policy ``build`` makes on first use, on the loop's thread: the native handle is bound to the thread
    that builds it, and its owner may be built on another. A ``max_attempts`` the kernel cannot carry (not an
    int, negative or too large) raises the terminal :class:`~edax.errors.BadHandle` here, at wiring time.
    Built lazily, it would fail the first message and leave it unsettled, which pins a log partition."""
    wiring_u32(max_attempts, f"{what} max_attempts")
    return ThreadBound.lazy(lambda: build(max_attempts, sink), what)


def is_work_failure(error: BaseException) -> bool:
    """Whether ``error``, raised by a handler or a dead-letter sink, fails that work, as against stopping the
    process or the task running it. ``KeyboardInterrupt``, ``SystemExit`` and a cancel of the running task
    itself propagate. Anything else fails the work: an ``Exception``, a native ``PanicException``, and a
    ``CancelledError`` from a future someone else cancelled. Let any of those end a dispatcher's worker
    and it strands every key routed to it."""
    if isinstance(error, (KeyboardInterrupt, SystemExit)):
        return False
    return not (isinstance(error, asyncio.CancelledError) and _task_cancelled())


def _task_cancelled() -> bool:
    """Whether the running task itself has been asked to cancel, as against awaiting a future someone else
    cancelled. ``Task.cancelling`` exists from 3.11; on 3.10 every ``CancelledError`` counts as the task's
    own, so a dispatcher's worker can still end that way there, and
    :attr:`~edax.dispatcher.DispatcherStats.running` reports it."""
    task = asyncio.current_task()
    cancelling = getattr(task, "cancelling", None)
    return cancelling is None or cancelling() > 0


async def dead_letter(
    log: logging.Logger,
    on_dead: DeadLetterSink[T] | None,
    dead: DeadLetter[T],
    row_id: str,
    report: Callable[[str, int, bool, str | None], Answer],
) -> tuple[BaseException | None, Answer]:
    """Hand ``dead``, which the kernel declared dead at ``dead.attempt``, to ``on_dead``, then report how the
    sink took it to the kernel's ``report`` (a dispatcher policy's or a drain's ``sunk``). Returns what the
    sink raised, ``None`` once it took the message, and the kernel's answer: it keeps the attempt so the next
    try reaches the sink again, or stops on a fatal refusal. The one sink step of the
    :class:`~edax.outbox.Relay` and the :class:`~edax.dispatcher.KeyOrderedDispatcher`; each keeps only its
    own reaction to the answer.

    The kernel declares a message dead only to a policy built with a sink, so a missing ``on_dead`` is the
    terminal :class:`~edax.errors.CodecError` of a native module out of step with this SDK."""
    take = _names.given(on_dead, "dead-letter sink")
    refusal = await _refusal(log, row_id, dead.attempt, lambda: take(dead))
    # A refusal that is no Exception (a native panic) carries no code, so it is transient.
    answer = report(row_id, dead.attempt, refusal is None, None if refusal is None else code_of(refusal))
    return refusal, answer


async def _refusal(
    log: logging.Logger, row_id: str, attempt: int, deliver: Callable[[], MaybeAwaitable[None]]
) -> BaseException | None:
    """Run ``deliver``: ``None`` once the sink took the message, else what the sink raised. What counts as a
    sink failure is :func:`is_work_failure`, as for a handler: a sink that panics in a native extension
    refuses, as the Rust SDK's sink step catches the panic, while a cancel of this task propagates."""
    log.warning("edax: dead-lettering %s after %d attempt(s)", row_id, attempt)
    try:
        await maybe_await(deliver())
    except BaseException as e:
        if not is_work_failure(e):
            raise
        log.exception("edax: the dead-letter sink failed for %s", row_id)
        return e
    return None

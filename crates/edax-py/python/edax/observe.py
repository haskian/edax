"""The telemetry seam: what the SDK reports, in plain values, for a host to export as it likes.

edax ships no exporter and imports no telemetry library. Two seams cover the signals, split by
where each is measurable:

- **Traces and handler latency** belong in the middleware that already wraps every handler
  (:func:`timing` for the numbers; a host tracer for spans). A completion callback cannot start a
  span the handler's own calls nest under, so this is not the :class:`Observer`'s job.
- **Metrics of settlement, claims, transactions, drains and poison messages** belong in the
  :class:`Observer`, which the orchestration objects call after the fact with a ``duration_ns``.

The vocabulary below is the shareable part: OpenTelemetry (OTel) messaging metric names, the unit,
the histogram buckets and the outcome strings. The kernel already owns the classification that
must agree across SDKs (:attr:`~edax.errors.EdaxError.code`, the idempotency verdicts,
terminality); this module only names the labels. When the Rust SDK grows a second emitter the
table moves verbatim to a data-only golden file.

**Cardinality rule.** A metric adapter must label only by ``name`` / ``outcome`` / ``verdict`` /
``how`` / ``error_type``. ``name`` is the command type or the event name a handler is registered
under: the closed set the bus and the router know, so it is safe. Never label by ``key``,
``event_id``, ``message_id`` or ``aggregate_id``: per-message ids are unbounded and belong on spans
and logs only. The ``*_id`` arguments the :class:`Observer` receives exist for the log line, not the
label set."""

from __future__ import annotations

import logging
from collections.abc import Awaitable
from time import perf_counter_ns
from typing import Any, Callable, Final, Optional

from ._async import MaybeAwaitable, maybe_await

logger = logging.getLogger("edax.observe")

# --- vocabulary --------------------------------------------------------------------------------

#: OTel messaging metric names the signals map onto (semconv ``messaging.*``, Development stability).
METRIC_PROCESS_DURATION: Final[str] = "messaging.process.duration"  # `Observer.delivery` / `settled`
METRIC_OPERATION_DURATION: Final[str] = "messaging.client.operation.duration"  # `Observer.drain`
METRIC_SENT_MESSAGES: Final[str] = "messaging.client.sent.messages"  # `Observer.drain` published
METRIC_CONSUMED_MESSAGES: Final[str] = "messaging.client.consumed.messages"  # `Observer.delivery`

#: Durations cross as nanoseconds; OTel histograms take seconds, so an adapter divides by 1e9.
DURATION_UNIT: Final[str] = "s"
#: The OTel explicit bucket boundaries for ``messaging.*.duration``, in seconds.
DURATION_BUCKETS: Final[tuple[float, ...]] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1,
    2.5,
    5,
    7.5,
    10,
)

#: ``Observer.claim`` verdicts. A guarded dispatch asked the ledger for the claim on its key and found
#: a prior record, so it lost. The kernel judged that record; the string is its verdict, verbatim:
#:
#: - ``succeeded``: the prior run completed. The caller gets the cached result; the handler does not run.
#: - ``in_progress``: another worker holds the claim right now. The caller gets ``InProgress`` (transient).
#: - ``payload_mismatch``: same key, different body. The caller gets ``PayloadMismatch`` (terminal).
#: - ``failed``: the prior run failed terminally and that failure is the record. The caller gets
#:   ``CachedFailure``.
#:
#: The dispatch that found no record, the winner, is not a claim verdict: it reports ``settled``.
CLAIM_VERDICTS: Final[tuple[str, ...]] = ("succeeded", "in_progress", "payload_mismatch", "failed")
#: ``Observer.settled``: how the winner's claim ended, which decides what the next dispatch of the
#: same key will find:
#:
#: - ``completed``: the handler returned and the ledger holds its result. A replay finds ``succeeded``.
#: - ``released``: the handler failed transiently, or the unit of work rolled back. The record is
#:   gone; the next dispatch runs the handler again.
#: - ``cached``: the handler failed terminally and the failure is the record. A replay finds ``failed``.
#: - ``refused``: the ledger would not take the compensating write (``complete`` or ``release``
#:   raised). The record is stale until the ledger's retention drops it; a replay may find
#:   ``in_progress`` until then. Worth an alert.
SETTLEMENTS: Final[tuple[str, ...]] = ("completed", "released", "cached", "refused")
#: ``Observer.delivery`` outcomes, one per arm of the dispatcher's settlement:
#:
#: - ``ack``: the handler returned and the delivery was acked. The broker forgets it.
#: - ``nack``: the handler raised and the delivery was nacked after one backoff step. Either a
#:   transient error below the ``max_attempts`` ceiling, or any error with no ``on_dead`` wired. The
#:   broker redelivers.
#: - ``dead``: ``on_dead`` took the delivery and it was acked. A terminal error on any attempt, or a
#:   transient one at the ceiling.
#: - ``in_progress``: the handler raised ``InProgress``, so another worker holds the claim. Nacked
#:   after one backoff step; no attempt spent.
#: - ``unsettled``: the settlement itself raised (``on_dead``, the ack or the nack). Neither acked nor
#:   nacked; the broker redelivers when its session times out.
DELIVERY_OUTCOMES: Final[tuple[str, ...]] = ("ack", "nack", "dead", "in_progress", "unsettled")
#: ``Observer.transaction`` outcomes: how a ``transaction`` unit of work ended.
#:
#: - ``committed``: the body returned and the commit landed.
#: - ``rolled_back``: the body raised or was cancelled, or the commit itself failed. Nothing landed.
TRANSACTIONS: Final[tuple[str, ...]] = ("committed", "rolled_back")
#: ``timing`` outcomes: ``ok`` when the handler returned, ``error`` when it raised.
HANDLER_OUTCOMES: Final[tuple[str, ...]] = ("ok", "error")


def error_type(e: BaseException) -> str:
    """The low-cardinality ``error.type`` label for ``e``: the kernel's wire code for an edax error,
    else the qualified class name (the OTel recommendation for a host exception)."""
    return getattr(e, "code", None) or f"{type(e).__module__}.{type(e).__qualname__}"


# --- the seam ----------------------------------------------------------------------------------


class Observer:
    """Receives the SDK's metric events. Every method is a no-op; subclass and override what you
    export. Plain values only: the exception is passed for the log line, and a metric adapter labels
    it with :func:`error_type`, never with the message.

    Wire an instance explicitly (``observer=``) on :class:`~edax.dispatcher.KeyOrderedDispatcher`,
    :class:`~edax.store.Relay`, :class:`~edax.store.Repository`, :func:`~edax.store.transaction`,
    :func:`~edax.command.idempotent` and :func:`~edax.router.idempotent_by_id`; there is no default
    global. Every call is guarded by the SDK: an observer that raises is logged and never fails a
    dispatch, a drain, a transaction or a settlement. Metric labels follow the cardinality rule in
    the module docstring."""

    def claim(self, name: str, verdict: str) -> None:
        """A guarded dispatch **lost** the idempotency claim: the ledger already held a record for its
        key. Fires once per losing dispatch, from :func:`~edax.command.idempotent` and
        :func:`~edax.router.idempotent_by_id`, before the caller gets the cached result or the error.

        ``name`` is the command type (``cmd.type``) or the event name (``env.event.name``) the guarded
        handler serves, so every claim signal breaks down per handler. ``verdict`` is the kernel's
        judgement of the prior record, one of :data:`CLAIM_VERDICTS`. Read
        ``succeeded`` as the duplicate-request rate, ``in_progress`` as contention between workers,
        ``payload_mismatch`` as a client reusing a key for a different request, and ``failed`` as
        clients retrying a request that can never succeed.

        Not a count of dispatches: the winner reports through :meth:`settled`, never here."""

    def settled(self, name: str, how: str, duration_ns: int) -> None:
        """A guarded dispatch **won** the claim, ran, and its claim reached a terminal state. Fires once
        per winning dispatch, after the ledger write that ends the claim, whether that write succeeded
        or not.

        ``name`` is the command type or the event name, as on :meth:`claim`.
        ``how`` is one of :data:`SETTLEMENTS`: what the ledger now holds, and so what the next dispatch
        of the same key will find. ``duration_ns`` is how long the claim was held, from winning it to
        the settlement, handler and ledger writes included. Read ``released`` as the transient failure
        rate, ``cached`` as the terminal failure rate, and any ``refused`` as ledger trouble worth an
        alert.

        Not handler latency: that is :func:`timing` on the bus. Not a delivery: a guarded event handler
        reports here **and** through :meth:`delivery`, one for the claim and one for the broker."""

    def delivery(
        self, name: str, outcome: str, attempt: int, duration_ns: int, error_type: Optional[str]
    ) -> None:
        """A :class:`~edax.dispatcher.KeyOrderedDispatcher` worker ran the handler for one broker
        delivery and settled it. Fires exactly once per :class:`~edax.dispatcher.Delivery`, after the
        ack, the nack or the dead-letter, even if that settlement raised.

        ``name`` is the event name of the delivered envelope, ``env.event.name``, so every delivery
        signal breaks down per event handler.
        ``outcome`` is one of :data:`DELIVERY_OUTCOMES`: which settlement arm ran. ``attempt`` is the
        attempt this delivery was, from ``1``, so a nack at the ceiling is told from a first nack;
        bucket it in a histogram, do not label by it. ``duration_ns`` is the handler alone, never the
        backoff wait, so a latency histogram over it is not polluted by the retry schedule.
        ``error_type`` is :func:`error_type` of what the handler raised, ``None`` when it returned. It
        splits nacks by cause and tells a ``dead`` on a terminal error from a ``dead`` at the ceiling.

        Not fired for a bare envelope submitted without a delivery: nothing settles it. Not the
        settlement's own failure: that is :meth:`settlement_failed`."""

    def settlement_failed(self, name: str, event_id: str, error: BaseException) -> None:
        """The handler ran, and then the ack or the nack itself raised. The delivery is now neither
        acked nor nacked; the broker redelivers it when its session times out. Fires at most once per
        delivery, before :meth:`delivery` reports that same delivery as ``unsettled``.

        ``name`` is the event name, as on :meth:`delivery`.
        ``event_id`` is for the log line, never a label. ``error`` is the transport's exception; label
        by :func:`error_type` of it. Read a non-zero rate as broker or commit trouble, not handler
        trouble: the work itself finished.

        Not fired when ``on_dead`` raises: that is a sink failure, reported as ``unsettled`` alone."""

    def transaction(self, outcome: str, duration_ns: int) -> None:
        """A unit of work opened through :func:`~edax.store.transaction` ended. Fires once per unit of work
        the SDK opened: a ``transaction`` you called with an observer, a :class:`~edax.store.Repository`
        that opened its own, or an idempotency guard given a ``unit_of_work``.

        ``outcome`` is one of :data:`TRANSACTIONS`: ``committed`` when the body returned and the commit
        landed, ``rolled_back`` when the body raised, was cancelled, or the commit itself failed.
        ``duration_ns`` is begin to end, commit included: the time the connection was held.

        Not fired for a call that joined an ambient transaction; the ``transaction`` that opened it reports
        once for all participants."""

    def drain(self, published: int, duration_ns: int, oldest_timestamp: Optional[int]) -> None:
        """A :class:`~edax.store.Relay` drained one outbox batch to completion: every row published and
        the batch marked. Fires once per completed drain, an empty batch included.

        ``published`` is the row count, ``0`` for an empty outbox. ``duration_ns`` is read to mark.
        ``oldest_timestamp`` is the ``timestamp`` of the oldest row in the batch, Unix nanoseconds from
        the kernel clock unless the envelopes were stamped by another; ``None`` for an empty batch.
        Export it as a gauge and compute the lag at query time, reader's clock minus the value: a
        batch that stays old is a relay that cannot keep up.

        Not fired for a drain that raised: that is :meth:`drain_failed`."""

    def drain_failed(self, error: BaseException) -> None:
        """A :class:`~edax.store.Relay` drain raised before it marked the batch: ``unpublished``,
        ``publish`` or ``mark_published`` failed. Nothing was marked, so the same rows come back on the
        next drain and delivery stays at-least-once. Fires once per failed drain, before the error
        re-raises to the caller.

        Label by :func:`error_type` of ``error``. Read a sustained rate as a relay that is stuck, and
        pair it with the ``oldest_timestamp`` gauge going stale."""

    def undecodable(self, message_id: str, error: BaseException) -> None:
        """A consumer polled a message whose payload the serializer rejected: a poison message. No
        handler sees it; the consumer loop routes it to ``on_dead`` and acks it, or nacks it when no
        sink is wired. Fires once per poisoned message, from the loop, for any ``Consumer``.

        ``message_id`` is the transport's own id (a Kafka ``topic/partition/offset``), for the log
        line only. ``error`` is the decode failure; label by :func:`error_type` of it. Read any count
        as a producer publishing a shape this consumer cannot read: a contract problem, not a retry
        problem.

        Not a :meth:`delivery`: the message never reached the dispatcher."""


def _safely(call: Callable[..., None], *args: Any) -> None:
    """Call a host telemetry hook; log and swallow whatever it raises. The one guard behind every
    observer call and the :func:`timing` recorder, so a broken exporter never fails the work it
    measures."""
    try:
        call(*args)
    except Exception:
        name = getattr(call, "__qualname__", repr(call))
        logger.exception("edax observe: %s raised; the failure is ignored", name)


class _Guarded(Observer):
    """A host :class:`Observer` behind :func:`_safely`: the orchestration objects call this and never
    see the host's exceptions."""

    def __init__(self, inner: Observer) -> None:
        self._inner = inner

    def claim(self, name: str, verdict: str) -> None:
        _safely(self._inner.claim, name, verdict)

    def settled(self, name: str, how: str, duration_ns: int) -> None:
        _safely(self._inner.settled, name, how, duration_ns)

    def delivery(
        self, name: str, outcome: str, attempt: int, duration_ns: int, error_type: Optional[str]
    ) -> None:
        _safely(self._inner.delivery, name, outcome, attempt, duration_ns, error_type)

    def settlement_failed(self, name: str, event_id: str, error: BaseException) -> None:
        _safely(self._inner.settlement_failed, name, event_id, error)

    def transaction(self, outcome: str, duration_ns: int) -> None:
        _safely(self._inner.transaction, outcome, duration_ns)

    def drain(self, published: int, duration_ns: int, oldest_timestamp: Optional[int]) -> None:
        _safely(self._inner.drain, published, duration_ns, oldest_timestamp)

    def drain_failed(self, error: BaseException) -> None:
        _safely(self._inner.drain_failed, error)

    def undecodable(self, message_id: str, error: BaseException) -> None:
        _safely(self._inner.undecodable, message_id, error)


# One no-op for every unobserved call site: `transaction` resolves its observer per call, so a fresh
# instance there would be an allocation on every unit of work.
_NOOP: Final[Observer] = Observer()


def _wrap(observer: Optional[Observer]) -> Observer:
    """Resolve an ``observer=`` keyword: ``None`` is the no-op :class:`Observer`, and a host observer
    is wrapped so its failures are logged, never raised."""
    return _NOOP if observer is None else _Guarded(observer)


# --- handler timing ----------------------------------------------------------------------------

#: What ``timing`` records: ``(name, outcome, error_type, duration_ns)``. ``outcome`` is ``"ok"`` with
#: ``error_type=None``, or ``"error"`` with the :func:`error_type` of the raised exception.
TimingRecorder = Callable[[str, str, Optional[str], int], None]

_Wrapped = Callable[[Any], MaybeAwaitable[Any]]
_Timed = Callable[[Any], Awaitable[Any]]


def timing(record: TimingRecorder, name: Callable[[Any], str]) -> Callable[[_Wrapped], _Timed]:
    """A middleware that times the handler it wraps and hands ``record`` one row per call:
    ``(name(x), "ok", None, duration_ns)`` on success, ``(name(x), "error", error_type(e),
    duration_ns)`` when the handler raises, which it then re-raises. ``record`` is guarded like an
    observer call. Structurally a :data:`~edax.command.Middleware` and an
    :data:`~edax.router.EventMiddleware`; only ``name`` differs: ``lambda c: c.type`` for commands,
    ``lambda env: env.event.name`` for events. This is the one place command latency is measured.
    A host tracer wraps the same seam to start the span the handler's own calls nest under."""

    def mw(next_: _Wrapped) -> _Timed:
        async def handler(x: Any) -> Any:
            started = perf_counter_ns()
            try:
                result = await maybe_await(next_(x))
            except Exception as e:
                _safely(record, name(x), "error", error_type(e), perf_counter_ns() - started)
                raise
            _safely(record, name(x), "ok", None, perf_counter_ns() - started)
            return result

        return handler

    return mw

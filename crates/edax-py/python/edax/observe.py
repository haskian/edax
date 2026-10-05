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
``how`` / ``error_type``. On the command bus ``name`` is the command type a handler is registered
under: the closed set the bus knows. On the consume path it is the ``msg.name`` the producer wrote,
read before any routing, so an unhandled name reaches the observer too. An adapter maps every name
its router does not register to one fixed value. Never label by ``key``, ``event_id``,
``message_id``, ``correlation_id`` or ``aggregate_id``: per-message ids are unbounded and belong on
spans and logs only. The ``*_id`` arguments the :class:`Observer` receives exist for the log line,
not the label set."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from time import perf_counter_ns
from typing import Any, Final, Literal, get_args

from ._hooks import safely
from .errors import EdaxError

__all__ = [
    "CLAIM_VERDICTS",
    "ClaimVerdict",
    "DELIVERY_OUTCOMES",
    "DURATION_BUCKETS",
    "DURATION_UNIT",
    "DeliveryOutcome",
    "HANDLER_OUTCOMES",
    "HandlerOutcome",
    "METRIC_CONSUMED_MESSAGES",
    "METRIC_OPERATION_DURATION",
    "METRIC_PROCESS_DURATION",
    "METRIC_SENT_MESSAGES",
    "Observer",
    "SAGA_SKIPS",
    "SETTLEMENTS",
    "SagaSkip",
    "Settlement",
    "TRANSACTIONS",
    "TimingRecorder",
    "TransactionOutcome",
    "error_type",
    "timing",
]

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
ClaimVerdict = Literal["succeeded", "in_progress", "payload_mismatch", "failed"]
CLAIM_VERDICTS: Final[tuple[ClaimVerdict, ...]] = get_args(ClaimVerdict)
#: ``Observer.settled``: how the winner's claim ended, which decides what the next dispatch of the
#: same key will find:
#:
#: - ``completed``: the handler returned and the ledger holds its result. A replay finds ``succeeded``.
#: - ``released``: the handler failed transiently, the unit of work rolled back, or ``complete``
#:   raised and the fallback ``release`` landed. The record is gone; the next dispatch runs the
#:   handler again.
#: - ``cached``: the handler failed terminally and the failure is the record. A replay finds ``failed``.
#: - ``refused``: the ledger would not take the write that ends the claim: ``release`` raised, after a
#:   failed ``complete`` or on its own. The record is stale until the ledger's retention drops it; a
#:   replay may find ``in_progress`` until then. Worth an alert.
Settlement = Literal["completed", "released", "cached", "refused"]
SETTLEMENTS: Final[tuple[Settlement, ...]] = get_args(Settlement)
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
#: - ``sink_failed``: ``on_dead`` raised, so the delivery was nacked after one backoff step; the
#:   redelivery gives the sink another try. Worth an alert: the dead-letter route is down.
#: - ``retired``: the transport retired the delivery (a nack rewound its partition, or a rebalance took
#:   the partition away) before or while the handler ran. Not settled; the transport fetches the
#:   message again. ``attempt`` is the one this delivery would have been, ``duration_ns`` is ``0`` when
#:   the handler never ran.
#: - ``stopped``: :meth:`~edax.dispatcher.KeyOrderedDispatcher.stop` cancelled the worker before the
#:   delivery settled (in the handler, the sink or a backoff wait), or the delivery was still queued; or
#:   a fatal failure (:func:`~edax.errors.is_fatal`) stopped the dispatcher, on this delivery or before it
#:   ran. Nacked at once and never counted; the broker redelivers. ``error_type`` is set when the handler
#:   had already raised.
#: - ``unsettled``: the ack or the nack itself raised, or a stop cut it short. Neither is known to have
#:   landed; on a log-based broker the message comes back only after a rebalance or a restart.
DeliveryOutcome = Literal[
    "ack", "nack", "dead", "in_progress", "sink_failed", "retired", "stopped", "unsettled"
]
DELIVERY_OUTCOMES: Final[tuple[DeliveryOutcome, ...]] = get_args(DeliveryOutcome)
#: ``Observer.transaction`` outcomes: how a ``transaction`` unit of work ended.
#:
#: - ``committed``: the body returned and the commit landed.
#: - ``rolled_back``: the body raised or was cancelled, or the commit itself failed. Nothing landed.
TransactionOutcome = Literal["committed", "rolled_back"]
TRANSACTIONS: Final[tuple[TransactionOutcome, ...]] = get_args(TransactionOutcome)
#: ``timing`` outcomes: ``ok`` when the handler returned, ``error`` when it raised.
HandlerOutcome = Literal["ok", "error"]
HANDLER_OUTCOMES: Final[tuple[HandlerOutcome, ...]] = get_args(HandlerOutcome)
#: ``Observer.saga_skipped`` reasons: why a :class:`~edax.saga.SagaRunner` did not fold an event. The
#: kernel's ``saga_admit`` answer, verbatim:
#:
#: - ``completed``: the instance is a tombstone. A late or redelivered event, normal after completion.
#: - ``repeat``: the instance folded this very event id. A redelivery, or a replay after a crash.
#: - ``below_mark``: the event's sequence is below the highest one the instance folded from its
#:   aggregate. A replay after a crash looks like this, but so does a dead-lettered event re-driven after a
#:   later one folded, which is then never folded. Worth a look when it follows a re-drive.
SagaSkip = Literal["completed", "repeat", "below_mark"]
SAGA_SKIPS: Final[tuple[SagaSkip, ...]] = get_args(SagaSkip)


def error_type(e: BaseException) -> str:
    """The low-cardinality ``error.type`` label for ``e``: the kernel's wire code for an edax error,
    else the qualified class name (the OTel recommendation for a host exception). A host exception's
    own ``code`` is never read: it may be an int, or a method whose text holds the error details."""
    if isinstance(e, EdaxError) and e.code is not None:
        return e.code
    return f"{type(e).__module__}.{type(e).__qualname__}"


# --- the seam ----------------------------------------------------------------------------------


class Observer:
    """Receives the SDK's metric events. Every method is a no-op; subclass and override what you
    export. Plain values only: the exception is passed for the log line, and a metric adapter labels
    it with :func:`error_type`, never with the message.

    Wire an instance explicitly (``observer=``) on :class:`~edax.dispatcher.KeyOrderedDispatcher`,
    :class:`~edax.outbox.Relay`, :class:`~edax.repository.Repository`, :func:`~edax.unit_of_work.transaction`,
    :func:`~edax.command.idempotent`, :func:`~edax.router.idempotent_by_id`,
    :class:`~edax.saga.SagaRunner` and :class:`~edax.backend.redis.RedisIdempotencyStore`; there is no
    default global. Every call is guarded by the SDK: an observer that raises is logged and never fails a
    dispatch, a drain, a transaction or a settlement. Metric labels follow the cardinality rule in
    the module docstring."""

    def claim(self, name: str, verdict: ClaimVerdict) -> None:
        """A guarded dispatch lost the idempotency claim: the ledger already held a record for its
        key. Fires once per losing dispatch, from :func:`~edax.command.idempotent` and
        :func:`~edax.router.idempotent_by_id`, before the caller gets the cached result or the error.

        ``name`` is the command type (``cmd.type``) or the event name (``msg.name``) the guarded
        handler serves, so every claim signal breaks down per handler. ``verdict`` is the kernel's
        judgement of the prior record, one of :data:`CLAIM_VERDICTS`. Read
        ``succeeded`` as the duplicate-request rate, ``in_progress`` as contention between workers,
        ``payload_mismatch`` as a client reusing a key for a different request, and ``failed`` as
        clients retrying a request that can never succeed.

        Not a count of dispatches: the winner reports through :meth:`settled`, never here."""

    def settled(self, name: str, how: Settlement, duration_ns: int) -> None:
        """A guarded dispatch won the claim, ran, and its claim reached a terminal state. Fires once
        per winning dispatch, after the ledger write that ends the claim, whether that write succeeded
        or not. On a shared ledger that write is the commit, so ``completed`` is
        reported only once the commit has landed, and a failed commit reports ``released``.

        ``name`` is the command type or the event name, as on :meth:`claim`.
        ``how`` is one of :data:`SETTLEMENTS`: what the ledger now holds, and so what the next dispatch
        of the same key will find. ``duration_ns`` is how long the claim was held, from winning it to
        the settlement, handler and ledger writes included. Read ``released`` as the transient failure
        rate, ``cached`` as the terminal failure rate, and any ``refused`` as ledger trouble worth an
        alert.

        Not handler latency: that is :func:`timing` on the bus. Not a delivery: a guarded event handler
        reports both here and through :meth:`delivery`, one for the claim and one for the broker."""

    def delivery(
        self,
        name: str,
        message_id: str,
        correlation_id: str,
        outcome: DeliveryOutcome,
        attempt: int,
        duration_ns: int,
        error_type: str | None,
    ) -> None:
        """A :class:`~edax.dispatcher.KeyOrderedDispatcher` worker ran the handler for one broker
        delivery and settled it. Fires exactly once per :class:`~edax.delivery.Delivery`, after the
        ack, the nack or the dead-letter, even if that settlement raised. A delivery that
        :meth:`~edax.dispatcher.KeyOrderedDispatcher.stop` or a fatal failure hands back reports ``stopped``.

        ``name`` is the name of the delivered message, ``msg.name``, so every delivery signal breaks down
        per handler; on the consume path it is whatever the producer wrote (see the cardinality rule).
        ``message_id`` and ``correlation_id`` are the message's own, for the log line that ties a ``nack``
        or a ``dead`` to its trace; never labels.
        ``outcome`` is one of :data:`DELIVERY_OUTCOMES`: which settlement arm ran. ``attempt`` is the
        attempt this delivery was, from ``1``, so a nack at the ceiling is told from a first nack;
        bucket it in a histogram, do not label by it. ``duration_ns`` is the handler alone, never the
        backoff wait, so a latency histogram over it is not polluted by the retry schedule.
        ``error_type`` is :func:`error_type` of what the handler raised, ``None`` when it returned. It
        splits nacks by cause and tells a ``dead`` on a terminal error from a ``dead`` at the ceiling.

        Not fired for a bare message submitted without a delivery: nothing settles it. Not the
        settlement's own failure: that is :meth:`settlement_failed`."""

    def settlement_failed(self, name: str, event_id: str, error: BaseException) -> None:
        """The handler ran, or the dead-letter sink took a poison message, and then the ack or the nack
        itself raised. The delivery is now neither acked nor nacked; a log-based broker brings it back only
        after a rebalance or a restart. Fires at most once per delivery, before :meth:`delivery` reports
        that same delivery as ``unsettled``, or before
        :meth:`~edax.dispatcher.KeyOrderedDispatcher.settle_poison` returns it.

        ``name`` is the event name, as on :meth:`delivery`.
        ``event_id`` is for the log line, never a label. ``error`` is the transport's exception; label
        by :func:`error_type` of it. Read a non-zero rate as broker or commit trouble, not handler
        trouble: the work itself finished.

        Not fired when ``on_dead`` raises: that is a sink failure, reported as ``sink_failed``."""

    def transaction(self, outcome: TransactionOutcome, duration_ns: int) -> None:
        """A unit of work opened through :func:`~edax.unit_of_work.transaction` ended. Fires once per unit of
        work the SDK opened: a ``transaction`` you called with an observer, a
        :class:`~edax.repository.Repository` that opened its own, or an idempotency guard given a
        ``unit_of_work``.

        ``outcome`` is one of :data:`TRANSACTIONS`: ``committed`` when the body returned and the commit
        landed, ``rolled_back`` when the body raised, was cancelled, or the commit itself failed.
        ``duration_ns`` is begin to end, commit included: the time the connection was held.

        Not fired for a call that joined an ambient transaction; the ``transaction`` that opened it reports
        once for all participants."""

    def drain(
        self, published: int, dead_lettered: int, duration_ns: int, oldest_timestamp: int | None
    ) -> None:
        """A :class:`~edax.outbox.Relay` drained one outbox batch to completion: every row published (or
        dead-lettered) and the batch marked. Fires once per completed drain, an empty batch included.

        ``published`` is the count of rows published, ``0`` for an empty outbox. ``dead_lettered`` is
        the count of rows handed to ``on_dead`` instead. ``duration_ns`` is read to mark.
        ``oldest_timestamp`` is when the oldest row in the batch was written, its ``recorded_at`` (an event)
        or ``issued_at`` (a command), Unix nanoseconds from the clock of the store or saga runner that
        stamped it; ``None`` for an empty batch.
        Export it as a gauge and compute the lag at query time, reader's clock minus the value: a
        batch that stays old is a relay that cannot keep up.

        Not fired for a drain that raised: that is :meth:`drain_failed`."""

    def drain_failed(self, error: BaseException, published: int, dead_lettered: int) -> None:
        """A :class:`~edax.outbox.Relay` drain raised: ``unpublished``, ``publish`` or ``mark_published``
        failed. The rows the drain settled are marked; the failing row and the rows behind it of its own
        key come back on the next drain, so delivery stays at-least-once. Fires once per failed drain, before
        the backoff wait and before the error re-raises to the caller.

        ``published`` and ``dead_lettered`` count what the drain did before it failed, so the sent and
        dead-letter counters lose no row. Both are ``0`` when ``unpublished`` failed. When
        ``mark_published`` failed they count rows the next drain sends again (at-least-once).
        Label by :func:`error_type` of ``error``. Read a sustained rate as a relay that is stuck, and
        pair it with the ``oldest_timestamp`` gauge going stale."""

    def saga_skipped(self, name: str, reason: SagaSkip, event_id: str) -> None:
        """A :class:`~edax.saga.SagaRunner` routed an event to an instance and did not fold it: no fold,
        no write, no command. Fires once per skipped event, before ``on_event`` returns.

        ``name`` is the saga's name, a closed set. ``reason`` is one of :data:`SAGA_SKIPS`, the kernel's
        answer. ``event_id`` is for the log line, never a label. Read ``repeat`` as the redelivery rate,
        and a ``below_mark`` after a dead-lettered event was re-driven as that event lost: the instance
        moved past it.

        Not fired for an event the saga's ``instance_id`` ignores: no instance was asked."""

    def undecodable(self, message_id: str, error: BaseException) -> None:
        """A consumer polled a message whose payload the serializer rejected: a poison message. No
        handler sees it; the consumer loop routes it to ``on_dead`` and acks it, or nacks it after a
        backoff step when no sink is wired or the sink raises. Fires once per poisoned delivery, from the
        loop, for any ``Consumer``.

        ``message_id`` is the transport's own id (a Kafka ``topic/partition/offset``), for the log
        line only. ``error`` is the decode failure; label by :func:`error_type` of it. Read any count
        as a producer publishing a shape this consumer cannot read: a contract problem, not a retry
        problem.

        Not a :meth:`delivery`: the message never reached the dispatcher."""

    def poll_failed(self, error: BaseException) -> None:
        """A consumer loop's ``poll`` raised. A fatal error (:func:`~edax.errors.is_fatal`) ends the loop
        and re-raises to the caller; any other waits one backoff step, which grows while the polls keep
        failing, and polls again. Fires once per failed poll, from the loop, for any ``Consumer``, through
        the observer of the dispatcher it feeds.

        Label by :func:`error_type` of ``error``. Read a sustained rate as a broker, or a record
        middleware's dependency, that is down: nothing is consumed meanwhile."""

    def commit_failed(self, error: BaseException) -> None:
        """A :class:`~edax.backend.kafka.KafkaConsumer` offset commit raised or passed its deadline,
        on the interval or before a revoke. The watermarks stay due, and the next interval tries again.
        Fires once per failed commit while the consumer runs.

        Label by :func:`error_type` of ``error``. Read a sustained rate as a consumer whose progress is
        not saved: a restart or a rebalance redelivers everything since the last commit that landed."""

    def lease_lost(self, key: str, error: BaseException | None) -> None:
        """A :class:`~edax.backend.redis.RedisIdempotencyStore` heartbeat stopped renewing a claim before
        it settled. The handler under that claim may still be running, and once the claim lapses another
        worker can take the key and run it too: at-least-once, for that key. Fires once per stopped
        renewal.

        ``key`` is the idempotency key, for the log line only, never a label. ``error`` is ``None`` when
        the claim was already lost (it lapsed, and another worker may hold it), else the terminal failure
        that stopped the renewal; label by :func:`error_type` of it. Any count is worth an alert: a
        handler that outlives its claim breaks the guard's at-most-one-run promise for its key."""

    def revoke_overrun(self, handling: int) -> None:
        """A :class:`~edax.backend.kafka.KafkaConsumer` rebalance revoke passed its drain deadline with
        handlers still running. The new owner of the revoked partitions may run one of their keys
        while this member still does: per-key order is broken for that window. Fires once per
        overrunning revoke.

        ``handling`` is how many handlers of the dispatcher were still running. Any count is worth an
        alert: raise the drain deadline above the slowest handler."""


# --- handler timing ----------------------------------------------------------------------------

#: What ``timing`` records: ``(name, outcome, error_type, duration_ns)``. ``outcome`` is ``"ok"`` with
#: ``error_type=None``, or ``"error"`` with the :func:`error_type` of the raised exception.
TimingRecorder = Callable[[str, HandlerOutcome, str | None, int], None]

# A step takes the message first: an event handler the event alone, a command step the message and the
# command (`edax.command.Next`).
_Step = Callable[..., Awaitable[Any]]


def timing(record: TimingRecorder, name: Callable[[Any], str]) -> Callable[[_Step], _Step]:
    """A middleware that times the step it wraps and hands ``record`` one row per call:
    ``(name(msg), "ok", None, duration_ns)`` on success, ``(name(msg), "error", error_type(e),
    duration_ns)`` when the step raises, which it then re-raises. ``record`` is guarded like an
    observer call. Structurally a :data:`~edax.command.Middleware` and an
    :data:`~edax.router.EventMiddleware`: both hand the message first, so ``lambda msg: msg.name`` names a
    command and an event alike. This is the one place command latency is measured.
    A host tracer wraps the same seam to start the span the handler's own calls nest under."""

    def mw(next_: _Step) -> _Step:
        async def step(msg: Any, *rest: Any) -> Any:
            started = perf_counter_ns()
            try:
                result = await next_(msg, *rest)
            except Exception as e:
                safely(record, name(msg), "error", error_type(e), perf_counter_ns() - started)
                raise
            safely(record, name(msg), "ok", None, perf_counter_ns() - started)
            return result

        return step

    return mw

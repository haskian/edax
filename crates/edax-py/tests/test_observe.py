"""The telemetry seam: what each orchestration object reports to an :class:`edax.Observer`, one case
per branch of the code that emits; the pull surface (``stats()`` / ``idle()`` / ``Relay.last`` /
``KafkaConsumer.stats()``); and the ``timing`` middleware. A recording observer stands in for a
host's exporter, so every case asserts the emitted arguments, not that something was called."""

from __future__ import annotations

import asyncio
import gc
import threading
import urllib.error
import warnings
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from email.message import Message
from typing import Any

import edax
import pytest
from conftest import (
    NO_WAIT,
    SCOPE,
    CounterCodec,
    CreateUser,
    FakeKafkaClient,
    Recording,
    Tick,
    TickEvents,
    TopicPartition,
    assert_logged,
    build_message,
    claimed,
    delivered,
    kafka_consumer,
)
from edax._async import maybe_await
from edax.aggregate import _replace
from edax.backend.kafka import KafkaConsumer
from edax.backend.memory import (
    MemoryCommandOutbox,
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemoryTransactor,
)
from edax.broker import UndecodableDelivery, _ConsumerLoop
from edax.observe import _wrap, timing
from edax.store import transaction
from edax.testing import drafted
from edax.testing.faults import BrokenCommit, Refusing


class _RpcError(Exception):
    """Shaped like ``grpc.RpcError``: ``code`` is a method, and its bound text holds the details."""

    def __init__(self, details: str) -> None:
        super().__init__(details)
        self.details = details

    def code(self) -> str:
        return "NOT_FOUND"


def _boom(*args: object) -> None:
    raise RuntimeError("exporter down")


class Raising(edax.Observer):
    """A broken exporter: every hook raises. The SDK must log it and carry on. Explicit overrides,
    not aliases: an alias with a different signature is an invalid override to the type checker."""

    def claim(self, name: str, verdict: str) -> None:
        _boom()

    def settled(self, name: str, how: str, duration_ns: int) -> None:
        _boom()

    def delivery(
        self,
        name: str,
        message_id: str,
        correlation_id: str,
        outcome: str,
        attempt: int,
        duration_ns: int,
        error_type: str | None,
    ) -> None:
        _boom()

    def settlement_failed(self, name: str, event_id: str, error: BaseException) -> None:
        _boom()

    def transaction(self, outcome: str, duration_ns: int) -> None:
        _boom()

    def drain(
        self, published: int, dead_lettered: int, duration_ns: int, oldest_timestamp: int | None
    ) -> None:
        _boom()

    def drain_failed(self, error: BaseException, published: int, dead_lettered: int) -> None:
        _boom()

    def undecodable(self, message_id: str, error: BaseException) -> None:
        _boom()

    def poll_failed(self, error: BaseException) -> None:
        _boom()

    def commit_failed(self, error: BaseException) -> None:
        _boom()

    def revoke_overrun(self, handling: int) -> None:
        _boom()

    def saga_skipped(self, name: str, reason: str, event_id: str) -> None:
        _boom()


DEPOSITED = b'{"account_id":"acct-1","amount_cents":4200,"currency":"USD"}'


# =========================================================================
# claim / settled: one case per branch of `_guarded`
# =========================================================================


@dataclass
class ClaimCase:
    errors: list[BaseException | None]  # per handler run; None = return normally
    payloads: list[str]  # one dispatch per entry; the email is the hashed payload
    raises: list[type[BaseException] | None]  # per dispatch
    want_claims: list[str]
    want_settled: list[str]
    held: bool = False  # another dispatch holds the claim before the first one
    refuses: tuple[str, ...] = ()  # the ledger writes it refuses, whatever is asked
    one_commit: bool = False  # `unit_of_work` given and the ledger joins it
    broken_commit: bool = False  # the one-commit unit of work fails at its commit, so nothing lands
    want_transactions: list[str] = field(default_factory=list)  # one per unit of work the guard opened


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            ClaimCase([None], ["a@b.c"], [None], [], ["completed"]),
            id="won_is_not_a_claim_verdict_and_settles_completed",
        ),
        pytest.param(
            ClaimCase([None], ["a@b.c", "a@b.c"], [None, None], ["succeeded"], ["completed"]),
            id="replay_reports_the_kernel_verdict_succeeded",
        ),
        pytest.param(
            ClaimCase([], ["a@b.c"], [edax.InProgress], ["in_progress"], [], held=True),
            id="in_progress",
        ),
        pytest.param(
            ClaimCase(
                [None], ["a@b.c", "x@y.z"], [None, edax.PayloadMismatch], ["payload_mismatch"], ["completed"]
            ),
            id="payload_mismatch",
        ),
        pytest.param(
            ClaimCase(
                [edax.CodecError("bad")],
                ["a@b.c", "a@b.c"],
                [edax.CodecError, edax.CachedFailure],
                ["failed"],
                ["cached"],
            ),
            id="terminal_failure_settles_cached_and_replays_as_the_kernel_verdict_failed",
        ),
        pytest.param(
            ClaimCase([RuntimeError("blip")], ["a@b.c"], [RuntimeError], [], ["released"]),
            id="transient_failure_settles_released",
        ),
        pytest.param(
            ClaimCase(
                [RuntimeError("blip")],
                ["a@b.c"],
                [RuntimeError],
                [],
                ["refused"],
                refuses=("complete", "release"),
            ),
            id="release_refused_after_a_failed_handler_settles_refused",
        ),
        pytest.param(
            ClaimCase([None], ["a@b.c"], [edax.StoreError], [], ["refused"], refuses=("complete", "release")),
            id="complete_and_release_refused_after_a_successful_handler_settles_refused",
        ),
        pytest.param(
            # The fallback release landed, so the ledger is clean: the repeat runs the handler again.
            ClaimCase(
                [None, None],
                ["a@b.c", "a@b.c"],
                [edax.StoreError, edax.StoreError],
                [],
                ["released", "released"],
                refuses=("complete",),
            ),
            id="complete_refused_after_a_successful_handler_but_released_settles_released",
        ),
        pytest.param(
            ClaimCase(
                [edax.CodecError("bad"), edax.CodecError("bad")],
                ["a@b.c", "a@b.c"],
                [edax.CodecError, edax.CodecError],
                [],
                ["released", "released"],
                refuses=("complete",),
            ),
            id="caching_a_terminal_failure_refused_but_released_settles_released",
        ),
        pytest.param(
            ClaimCase(
                [None], ["a@b.c"], [None], [], ["completed"], one_commit=True, want_transactions=["committed"]
            ),
            id="one_commit_path_settles_completed_and_commits",
        ),
        pytest.param(
            ClaimCase(
                [RuntimeError("blip")],
                ["a@b.c"],
                [RuntimeError],
                [],
                ["released"],
                one_commit=True,
                want_transactions=["rolled_back"],
            ),
            id="one_commit_path_rollback_settles_released_and_rolls_back",
        ),
        pytest.param(
            ClaimCase(
                [None],
                ["a@b.c", "a@b.c"],
                [None, None],
                ["succeeded"],
                ["completed"],
                one_commit=True,
                want_transactions=["committed", "committed"],
            ),
            id="one_commit_path_reports_a_lost_claim_inside_a_committed_unit",
        ),
        pytest.param(
            ClaimCase(
                [None],
                ["a@b.c"],
                [edax.StoreError],
                [],
                ["released"],
                one_commit=True,
                broken_commit=True,
                want_transactions=["rolled_back"],
            ),
            # The commit is the write that ends the claim: a failed one rolls the claim back, so a
            # `completed` reported before it would send an operator looking for a replay that re-runs.
            id="one_commit_path_failed_commit_settles_released",
        ),
    ],
)
async def test_idempotent_reports_claim_verdicts_and_settlements(case: ClaimCase) -> None:
    observer = Recording()
    # With no `unit_of_work` the guard takes the around-the-work path and compensates by hand, so a
    # refusing ledger reaches the `refused` settlement there.
    ledger: Any = (
        Refusing(MemoryIdempotencyStore(), *case.refuses) if case.refuses else MemoryIdempotencyStore()
    )
    unit_of_work: Any = ledger.transactor if case.one_commit else None
    if case.broken_commit:
        unit_of_work = BrokenCommit(ledger.transactor, "nothing_landed")
    calls = 0

    async def handler(cmd: CreateUser) -> str:
        nonlocal calls
        error = case.errors[calls]
        calls += 1
        await asyncio.sleep(0.01)  # so the settled duration visibly covers the handler
        if error is not None:
            raise error
        return f"user:{cmd.email}"

    bus = edax.CommandBus(edax.idempotent(ledger, unit_of_work, observer=observer, scope="test"))
    bus.register("user.create", handler)
    if case.held:
        assert isinstance(
            await ledger.claim(
                claimed("req-1"), edax.command._payload_hash(CreateUser("a@b.c"), CreateUser.type)
            ),
            edax.Won,
        )

    for email, raises in zip(case.payloads, case.raises, strict=True):
        if raises is None:
            assert await bus.dispatch(CreateUser(email), idempotency_key="req-1") == f"user:{email}"
        else:
            with pytest.raises(raises):
                await bus.dispatch(CreateUser(email), idempotency_key="req-1")

    assert calls == len(case.errors)
    assert observer.claims == [("user.create", verdict) for verdict in case.want_claims]
    assert [(name, how) for name, how, _ in observer.settlements] == [
        ("user.create", how) for how in case.want_settled
    ]
    # The claim was held for at least the handler's sleep, so the histogram sees the handler.
    assert all(duration_ns >= 5_000_000 for _, _, duration_ns in observer.settlements)
    assert [outcome for outcome, _ in observer.transactions] == case.want_transactions


async def test_idempotent_by_id_threads_the_observer_through_the_same_guard() -> None:
    observer = Recording()
    guarded = edax.idempotent_by_id(MemoryIdempotencyStore(), observer=observer, scope=SCOPE)(
        lambda env: None
    )
    env = build_message("acct-1", "Deposited", 3, data=DEPOSITED)

    await maybe_await(guarded(env))
    await maybe_await(guarded(env))  # a redelivery: skipped silently, but the observer still hears it
    assert observer.claims == [("Deposited", "succeeded")], "named after the event, not the command"
    assert [(name, how) for name, how, _ in observer.settlements] == [("Deposited", "completed")]


# =========================================================================
# delivery: one case per arm of `KeyOrderedDispatcher._ack_nack_or_dead`
# =========================================================================


TRANSIENT = "builtins.RuntimeError"  # `error_type` of the host exception the cases raise


@dataclass
class DeliveryCase:
    errors: list[BaseException | None]  # per submitted delivery of the one event id
    want: list[tuple[str, int, str | None]]  # (outcome, attempt, error_type) per delivery, in order
    max_attempts: int = 0
    with_on_dead: bool = False
    on_dead_raises: bool = False
    ack_raises: bool = False
    nack_raises: bool = False
    want_failures: int = 0  # `settlement_failed` rows
    want_log: list[str] = field(default_factory=list)  # `edax.dispatcher` fragments, in order


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(DeliveryCase([None], [("ack", 1, None)]), id="ack_has_no_error_type"),
        pytest.param(
            DeliveryCase(
                [RuntimeError("t")] * 3,
                [("nack", 1, TRANSIENT), ("nack", 2, TRANSIENT), ("nack", 3, TRANSIENT)],
                max_attempts=3,
            ),
            id="nack_attempt_distinguishes_the_ceiling_from_the_first_when_no_sink_is_wired",
        ),
        pytest.param(
            DeliveryCase(
                [RuntimeError("t")] * 2,
                [("nack", 1, TRANSIENT), ("dead", 2, TRANSIENT)],
                max_attempts=2,
                with_on_dead=True,
                want_log=["dead-lettering"],
            ),
            id="dead_at_the_ceiling_keeps_the_transient_error_type",
        ),
        pytest.param(
            DeliveryCase(
                [edax.CodecError("terminal")],
                [("dead", 1, "codec")],
                with_on_dead=True,
                want_log=["dead-lettering"],
            ),
            id="dead_on_a_terminal_error_carries_the_kernel_code",
        ),
        pytest.param(
            DeliveryCase([edax.InProgress("k")], [("in_progress", 1, "in_progress")]), id="in_progress"
        ),
        pytest.param(
            DeliveryCase(
                [edax.CodecError("terminal")],
                [("sink_failed", 1, "codec")],
                with_on_dead=True,
                on_dead_raises=True,
                want_log=["dead-lettering", "dead-letter sink failed"],
            ),
            id="sink_failed_when_on_dead_raises_and_the_delivery_is_nacked",
        ),
        pytest.param(
            DeliveryCase(
                [edax.CodecError("terminal")],
                [("unsettled", 1, "codec")],
                with_on_dead=True,
                on_dead_raises=True,
                nack_raises=True,
                want_failures=1,
                want_log=["dead-lettering", "dead-letter sink failed", "settling a delivery failed"],
            ),
            id="unsettled_when_on_dead_raises_and_then_the_nack_raises",
        ),
        pytest.param(
            DeliveryCase(
                [RuntimeError("t")],
                [("unsettled", 1, TRANSIENT)],
                nack_raises=True,
                want_failures=1,
                want_log=["settling a delivery failed"],
            ),
            id="unsettled_and_settlement_failed_when_nack_raises",
        ),
        pytest.param(
            DeliveryCase(
                [None],
                [("unsettled", 1, None)],
                ack_raises=True,
                want_failures=1,
                want_log=["after a successful handler"],
            ),
            id="unsettled_after_a_successful_handler_has_no_error_type",
        ),
    ],
)
async def test_dispatcher_reports_every_settlement_arm(
    case: DeliveryCase, slept: list[int], caplog: pytest.LogCaptureFixture
) -> None:
    observer = Recording()
    calls = 0

    async def handler(env: edax.EventMessage) -> None:
        nonlocal calls
        error = case.errors[calls]
        calls += 1
        if error is not None:
            raise error

    async def on_dead(d: edax.Delivery, err: BaseException) -> None:
        if case.on_dead_raises:
            raise edax.StoreError("sink unavailable")

    async def ack() -> None:
        if case.ack_raises:
            raise edax.StoreError("commit failed")

    async def nack() -> None:
        if case.nack_raises:
            raise edax.StoreError("seek failed")

    dispatcher = edax.KeyOrderedDispatcher(
        workers=1,
        handler=handler,
        max_attempts=case.max_attempts,
        on_dead=on_dead if case.with_on_dead else None,
        observer=observer,
    )
    dispatcher.start()
    env = build_message("acct-1", "Deposited", 1, data=DEPOSITED)
    with caplog.at_level("WARNING", logger="edax.dispatcher"):
        for _ in case.errors:
            await dispatcher.submit(edax.Delivery(env, ack, nack))  # a fresh delivery of the same id
            await dispatcher._queues[0].join()
        await dispatcher.stop()

    assert calls == len(case.errors)
    assert [(outcome, attempt, cause) for _, outcome, attempt, _, cause in observer.deliveries] == case.want
    assert all(name == "Deposited" and duration_ns >= 0 for name, _, _, duration_ns, _ in observer.deliveries)
    assert observer.delivered_ids == [(env.id, env.correlation_id)] * len(case.want), "for the log line"
    assert observer.settlement_failures == [("Deposited", env.id, edax.StoreError)] * case.want_failures
    assert_logged(caplog, "edax.dispatcher", case.want_log)


async def test_delivery_duration_is_the_handler_only_not_the_backoff() -> None:
    """A latency histogram over ``delivery`` must not absorb the kernel's backoff wait, so the
    duration is taken before any settlement arm sleeps."""
    observer = Recording()
    backoff_s = 0.05

    async def slow_sleep(seconds: float) -> None:
        await asyncio.sleep(backoff_s)

    async def handler(env: edax.EventMessage) -> None:
        raise RuntimeError("transient")

    pacing = edax.Pacing(sleep=slow_sleep)
    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler, observer=observer, pacing=pacing)
    dispatcher.start()
    await dispatcher.submit(
        edax.Delivery(build_message("acct-1", "Deposited", 1), lambda: None, lambda: None)
    )
    await dispatcher._queues[0].join()
    await dispatcher.stop()

    [(_, outcome, _, duration_ns, _)] = observer.deliveries
    assert outcome == "nack"
    assert duration_ns < backoff_s * 1e9 / 2, "the backoff sleep leaked into the handler duration"


# =========================================================================
# undecodable: reported by the consumer loop, so every adapter's poison message counts
# =========================================================================


class _Poisoned(_ConsumerLoop):
    """A consumer whose stream is one poison message, then end of stream."""

    def __init__(self, poison: UndecodableDelivery) -> None:
        self._items: list[edax.Delivery | None] = [poison, None]

    async def poll(self) -> edax.Delivery | None:
        return self._items.pop(0)


async def _kafka_poison() -> tuple[FakeKafkaClient, UndecodableDelivery]:
    client = FakeKafkaClient()
    await client.send_and_wait("t", b"not a message")
    poison = delivered(await KafkaConsumer(client, serializer=edax.KernelSerializer()).poll())
    assert isinstance(poison, UndecodableDelivery)
    return client, poison


async def test_the_consumer_loop_reports_an_undecodable_message_by_its_partition_id() -> None:
    observer = Recording()
    client, poison = await _kafka_poison()
    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=lambda env: None, observer=observer)

    await _Poisoned(poison).run(dispatcher)

    assert observer.undecodables == [("t/0/0", edax.CodecError)]
    assert client.seeks == [0], "no sink wired: the poison message was nacked, never dropped"


# =========================================================================
# a raising observer never fails the work it measures
# =========================================================================


async def _dispatch_with(observer: edax.Observer) -> None:
    acked: list[str] = []
    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=lambda env: None, observer=observer)
    dispatcher.start()
    env = build_message("acct-1", "Deposited", 1)
    await dispatcher.submit(edax.Delivery(env, lambda: acked.append(env.id), lambda: None))
    await dispatcher.stop()
    assert acked == [env.id], "the delivery was not acked"


async def _drain_with(observer: edax.Observer) -> None:
    store = MemoryEventStore(now=lambda: 1_000)
    async with transaction(store) as tx:
        await store.append(tx, "acct-1", 0, [drafted(build_message("acct-1", "Deposited", 1))])
    relay = edax.Relay(store, lambda env: None, observer=observer)
    assert await relay.drain() == 1
    assert relay.last is not None and (relay.last.published, relay.last.oldest_timestamp) == (1, 1_000)


async def _guard_with(observer: edax.Observer) -> None:
    bus = edax.CommandBus(edax.idempotent(MemoryIdempotencyStore(), observer=observer, scope="test"))

    async def handler(cmd: CreateUser) -> str:
        return "ok"

    bus.register("user.create", handler)
    assert await bus.dispatch(CreateUser("a@b.c"), idempotency_key="req-1") == "ok"
    assert (
        await bus.dispatch(CreateUser("a@b.c"), idempotency_key="req-1") == "ok"
    )  # the replay: `claim` raises too


async def _consume_with(observer: edax.Observer) -> None:
    client, poison = await _kafka_poison()
    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=lambda env: None, observer=observer)
    await _Poisoned(poison).run(dispatcher)
    assert client.seeks == [0], "the poison message was not settled"


async def _failing_drain_with(observer: edax.Observer) -> None:
    store = MemoryEventStore()
    async with transaction(store) as tx:
        await store.append(tx, "acct-1", 0, [drafted(build_message("acct-1", "Deposited", 1))])

    def publish(env: edax.EventMessage) -> None:
        raise edax.StoreError("broker down")

    with pytest.raises(edax.StoreError):  # the drain's own error, not the observer's
        await edax.Relay(store, publish, observer=observer).drain()


async def _commit_with(observer: edax.Observer) -> None:
    store = MemoryEventStore()
    async with transaction(store, observer=observer) as tx:
        await store.append(tx, "acct-1", 0, [drafted(build_message("acct-1", "Deposited", 1))])
    assert await store.unpublished() != [], "the unit of work did not commit"


@pytest.mark.parametrize(
    "work, hooks",
    [
        pytest.param(_dispatch_with, ["delivery"], id="dispatch_still_acks"),
        pytest.param(_drain_with, ["drain"], id="drain_still_marks"),
        pytest.param(_failing_drain_with, ["drain_failed"], id="failing_drain_still_raises_its_own_error"),
        pytest.param(_commit_with, ["transaction"], id="unit_of_work_still_commits"),
        pytest.param(_guard_with, ["settled", "claim"], id="guard_still_settles_and_replays"),
        pytest.param(_consume_with, ["undecodable"], id="loop_still_settles_the_poison_message"),
    ],
)
async def test_a_raising_observer_is_logged_and_the_work_completes(
    work: Callable[[edax.Observer], Awaitable[None]], hooks: list[str], caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level("ERROR", logger="edax.observe"):
        await work(Raising())
    logged = [r for r in caplog.records if r.name == "edax.observe"]
    assert [r.getMessage() for r in logged] == [
        f"edax observe: Raising.{hook} raised; the failure is ignored" for hook in hooks
    ]
    assert all(r.exc_info is not None and isinstance(r.exc_info[1], RuntimeError) for r in logged)


_HOOK_ARGS: dict[str, tuple[Any, ...]] = {
    "claim": ("Deposit", "succeeded"),
    "settled": ("Deposit", "completed", 1_200),
    "delivery": ("Deposited", "evt-1", "corr-1", "nack", 2, 3_400, "store"),
    "settlement_failed": ("Deposited", "evt-1", RuntimeError("commit lost")),
    "transaction": ("committed", 5_600),
    "drain": (3, 1, 7_800, 1_000),
    "drain_failed": (RuntimeError("broker down"), 2, 1),
    "undecodable": ("orders/0/17", ValueError("not msgpack")),
    "poll_failed": (ConnectionError("broker unreachable"),),
    "commit_failed": (TimeoutError(),),
    "revoke_overrun": (2,),
    "saga_skipped": ("order", "repeat", "evt-1"),
}


def test_every_observer_hook_is_guarded_and_forwards_its_arguments(caplog: pytest.LogCaptureFixture) -> None:
    """The guard wraps every public hook of ``Observer`` by reading the class, so the table above must
    name each one: a hook added to ``Observer`` fails here until it has a row, and is guarded already."""
    hooks = sorted(n for n, v in vars(edax.Observer).items() if not n.startswith("_") and callable(v))
    assert hooks == sorted(_HOOK_ARGS)

    seen: list[tuple[str, tuple[Any, ...]]] = []

    class Forwarding(edax.Observer):
        def __getattribute__(self, name: str) -> Any:
            if name in _HOOK_ARGS:
                return lambda *args: seen.append((name, args))
            return super().__getattribute__(name)

    guarded = _wrap(Forwarding())
    raising = _wrap(Raising())
    with caplog.at_level("ERROR", logger="edax.observe"):
        for hook, args in _HOOK_ARGS.items():
            getattr(guarded, hook)(*args)
            getattr(raising, hook)(*args)  # never raises
    assert seen == list(_HOOK_ARGS.items())
    assert [r.getMessage() for r in caplog.records if r.name == "edax.observe"] == [
        f"edax observe: Raising.{hook} raised; the failure is ignored" for hook in _HOOK_ARGS
    ]


# =========================================================================
# drain, and the pull surface
# =========================================================================


async def _event_rows(timestamps: list[int]) -> edax.Outbox[edax.EventMessage]:
    """An event store holding one committed event per timestamp, each recorded at it."""
    clock = iter(timestamps)
    store = MemoryEventStore(now=lambda: next(clock))
    async with transaction(store) as tx:
        for i in range(1, len(timestamps) + 1):
            await store.append(tx, "acct-1", i - 1, [drafted(build_message("acct-1", "Deposited", i))])
    return store


async def _command_rows(timestamps: list[int]) -> edax.Outbox[edax.CommandMessage]:
    """A command outbox holding one committed command per timestamp, each issued at it."""
    outbox = MemoryCommandOutbox()
    commands = [
        _replace(edax.command_message(CreateUser(f"user-{t}@example.com")), issued_at=t) for t in timestamps
    ]
    async with transaction(outbox.transactor) as tx:
        await outbox.enqueue(tx, commands)
    return outbox


@pytest.mark.parametrize(
    "rows, timestamps, want",
    [
        pytest.param(_event_rows, [], (0, None), id="empty_batch_has_no_oldest"),
        pytest.param(_event_rows, [3_000, 1_000, 4_000], (3, 1_000), id="oldest_is_the_lowest_recorded_at"),
        pytest.param(
            _command_rows,
            [3_000, 1_000, 4_000],
            (3, 1_000),
            id="oldest_is_the_lowest_issued_at_of_a_command_row",
        ),
    ],
)
async def test_relay_reports_and_keeps_the_last_drain(
    rows: Callable[[list[int]], Awaitable[edax.Outbox[Any]]],
    timestamps: list[int],
    want: tuple[int, int | None],
) -> None:
    observer = Recording()
    relay = edax.Relay(await rows(timestamps), lambda env: None, observer=observer)
    assert relay.last is None

    published = await relay.drain()

    assert published == want[0]
    assert relay.last is not None
    assert (relay.last.published, relay.last.oldest_timestamp) == want
    assert relay.last.duration_ns > 0
    assert observer.drains == [(relay.last.published, 0, relay.last.duration_ns, relay.last.oldest_timestamp)]


@dataclass
class DrainCase:
    rows: int  # committed outbox rows, sequences 1..rows
    failures: dict[int, Exception]  # the sequence of a row whose publish raises, and what it raises
    refuse: str | None  # the outbox method that is refused, if any
    want_sent: int  # rows the transport took
    want_drains: list[tuple[int, int]]  # (published, dead_lettered) per completed drain
    want_failures: list[tuple[type[BaseException], int, int]]  # (error, published, dead_lettered)


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            DrainCase(3, {2: edax.CodecError("cannot encode")}, None, 2, [(2, 1)], []),
            id="completed_drain_reports_its_dead_letters",
        ),
        pytest.param(
            DrainCase(
                5,
                {2: edax.CodecError("cannot encode"), 4: RuntimeError("broker down")},
                None,
                2,
                [],
                [(RuntimeError, 2, 1)],
            ),
            id="row_failure_reports_the_rows_settled_before_it",
        ),
        pytest.param(
            DrainCase(3, {}, "mark_published", 3, [], [(edax.StoreError, 3, 0)]),
            id="mark_failure_reports_the_rows_already_sent",
        ),
        pytest.param(
            DrainCase(3, {}, "unpublished", 0, [], [(edax.StoreError, 0, 0)]),
            id="read_failure_reports_nothing_done",
        ),
    ],
)
async def test_relay_reports_what_each_drain_did(case: DrainCase) -> None:
    """A drain that fails part way has already sent and dead-lettered rows: ``drain_failed`` carries
    those counts, so a counter fed from ``drain`` and ``drain_failed`` loses no row."""
    observer = Recording()
    store = MemoryEventStore()
    async with transaction(store) as tx:
        drafts = [drafted(build_message("acct-1", "Deposited", i)) for i in range(1, case.rows + 1)]
        await store.append(tx, "acct-1", 0, drafts)
    sent: list[str] = []

    def publish(env: edax.EventMessage) -> None:
        if env.sequence in case.failures:
            raise case.failures[env.sequence]
        sent.append(env.id)

    outbox: Any = Refusing(store, case.refuse) if case.refuse else store
    relay = edax.Relay(outbox, publish, observer=observer, on_dead=lambda row, e: None, pacing=NO_WAIT)
    if case.want_failures:
        with pytest.raises(case.want_failures[0][0]):
            await relay.drain()
        assert relay.last is None, "a drain that raised is not a completed drain"
    else:
        await relay.drain()
    assert len(sent) == case.want_sent
    assert [(published, dead) for published, dead, _, _ in observer.drains] == case.want_drains
    assert observer.drain_failures == case.want_failures


# =========================================================================
# transaction: one case per way a unit of work ends
# =========================================================================


@pytest.mark.parametrize(
    "error, want",
    [
        pytest.param(None, "committed", id="clean_exit_commits"),
        pytest.param(RuntimeError("body"), "rolled_back", id="a_raising_body_rolls_back"),
    ],
)
async def test_transaction_reports_how_the_unit_of_work_ended(error: BaseException | None, want: str) -> None:
    observer = Recording()
    store = MemoryEventStore()

    async def unit() -> None:
        async with transaction(store, observer=observer) as tx:
            await store.append(tx, "acct-1", 0, [drafted(build_message("acct-1", "Deposited", 1))])
            if error is not None:
                raise error

    if error is None:
        await unit()
    else:
        with pytest.raises(type(error)):
            await unit()

    [(outcome, duration_ns)] = observer.transactions
    assert (outcome, duration_ns >= 0) == (want, True)


async def test_repository_reports_only_the_units_of_work_it_opens_itself() -> None:
    observer = Recording()
    store = MemoryEventStore()
    repo = edax.Repository(store, TickEvents(), CounterCodec(), observer=observer)

    counter = await repo.load("c1")
    counter.apply(Tick(1))
    await repo.save(counter)  # no ambient tx: the repository opens and commits its own, once per call
    assert [outcome for outcome, _ in observer.transactions] == ["committed", "committed"]

    async with transaction(store):
        counter = await repo.load("c1")
        counter.apply(Tick(1))
        await repo.save(counter)  # joined the caller's unit of work, which is the caller's to report
    assert len(observer.transactions) == 2


async def test_dispatcher_stats_and_idle_track_a_delivery_from_submit_to_settlement() -> None:
    release = asyncio.Event()

    async def handler(env: edax.EventMessage) -> None:
        await release.wait()

    dispatcher = edax.KeyOrderedDispatcher(workers=2, handler=handler)
    assert dispatcher.stats() == edax.DispatcherStats([0, 0], 0, 0, 0, False, 0, 0)
    assert dispatcher.idle()

    dispatcher.start()
    first = build_message("acct-1", "Deposited", 1)
    await dispatcher.submit(edax.Delivery(first, lambda: None, lambda: None))
    await asyncio.sleep(0)  # the worker takes it and parks in the handler
    await dispatcher.submit(build_message("acct-1", "Deposited", 2))  # same key: queued behind it
    stats = dispatcher.stats()
    assert (stats.in_flight, sum(stats.queue_depth), stats.running, stats.workers_alive) == (1, 1, True, 2)
    assert not dispatcher.idle(), "an item a worker holds is not idle, even with the queue drained"

    release.set()
    await dispatcher.stop()
    assert dispatcher.stats() == edax.DispatcherStats([0, 0], 0, 0, 0, False, 0, 0)
    assert dispatcher.idle()


async def test_dispatcher_stats_count_the_failure_maps(slept: list[int]) -> None:
    async def handler(env: edax.EventMessage) -> None:
        raise RuntimeError("transient")

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler)
    dispatcher.start()
    await dispatcher.submit(
        edax.Delivery(build_message("acct-1", "Deposited", 1), lambda: None, lambda: None)
    )
    await dispatcher.stop()
    stats = dispatcher.stats()
    assert (stats.attempts_tracked, stats.contended_tracked, stats.in_flight) == (1, 0, 0)
    assert dispatcher.idle(), "a nacked message is the broker's again, not in flight here"


async def test_dispatcher_stats_read_from_a_metrics_thread_never_bind_the_native_policy() -> None:
    """An observable gauge's callback runs on the exporter's thread. Read there before the first
    delivery, ``stats()`` once built the lazy native retry policy on that thread, and every delivery
    after failed with ``BadHandle``: never handled, settled or observed."""
    observer = Recording()
    handled: list[str] = []
    acked: list[str] = []
    dispatcher = edax.KeyOrderedDispatcher(
        workers=1, handler=lambda env: handled.append(env.id), observer=observer
    )

    def read_on_a_metrics_thread() -> edax.DispatcherStats:
        read: list[edax.DispatcherStats] = []
        thread = threading.Thread(target=lambda: read.append(dispatcher.stats()))
        thread.start()
        thread.join()
        return read[0]

    assert read_on_a_metrics_thread().attempts_tracked == 0
    dispatcher.start()
    env = build_message("acct-1", "Deposited", 1)
    await dispatcher.submit(edax.Delivery(env, lambda: acked.append(env.id), lambda: None))
    await dispatcher._queues[0].join()

    assert (handled, acked) == ([env.id], [env.id])
    assert [outcome for _, outcome, _, _, _ in observer.deliveries] == ["ack"]
    assert read_on_a_metrics_thread().in_flight == 0, "a read after the first delivery works too"
    await dispatcher.stop()


def _stats(consumer: KafkaConsumer) -> dict[Any, tuple[int, int, int | None, int | None]]:
    """``(unsettled, live, max_delivered, committed)`` per partition. ``stats()`` returns the kernel's own
    ``PartitionStats`` rows, which are native objects with no ``__eq__``, so the fields compare."""
    return {
        tp: (row.unsettled, row.live, row.max_delivered, row.committed)
        for tp, row in consumer.stats().items()
    }


async def test_kafka_consumer_stats_track_inflight_across_ack_and_nack() -> None:
    _, consumer = await kafka_consumer(2)
    tp = TopicPartition("t", 0)
    assert _stats(consumer) == {}
    d0, d1 = [delivered(await consumer.poll()) for _ in range(2)]
    assert _stats(consumer) == {tp: (2, 2, 1, None)}

    await d1.ack()  # out of order: 0 is still in flight, so nothing is due
    await consumer.flush()
    assert _stats(consumer) == {tp: (1, 1, 1, None)}
    await d0.nack()  # stays in flight until its redelivery settles, and is no longer live
    assert _stats(consumer) == {tp: (1, 0, 1, None)}
    await d0.ack()  # stale: the rewind retired it
    assert _stats(consumer) == {tp: (1, 0, 1, None)}
    for _ in range(2):  # the redelivery of 0, and of the acked 1
        await delivered(await consumer.poll()).ack()
    await consumer.flush()
    assert _stats(consumer) == {tp: (0, 0, 1, 2)}

    consumer.reset(tp)
    assert _stats(consumer) == {}
    consumer.reset(tp)  # idempotent: a listener may revoke a partition it never saw


# =========================================================================
# timing middleware and the `error.type` rule
# =========================================================================


@pytest.mark.parametrize(
    "error, want",
    [
        pytest.param(edax.CodecError("x"), "codec", id="edax_error_is_its_kernel_code"),
        pytest.param(ValueError("x"), "builtins.ValueError", id="host_exception_is_its_qualified_name"),
        pytest.param(
            urllib.error.HTTPError(
                "https://api.example.com/v1/users", 503, "Service Unavailable", Message(), None
            ),
            "urllib.error.HTTPError",
            id="host_exception_with_an_int_code_is_its_qualified_name",
        ),
        pytest.param(
            _RpcError("user alice@example.com not found"),
            "test_observe._RpcError",
            id="host_exception_with_a_code_method_is_its_qualified_name",
        ),
        pytest.param(edax.EdaxError("x"), "edax.errors.EdaxError", id="edax_base_without_a_code_is_its_name"),
    ],
)
def test_error_type(error: BaseException, want: str) -> None:
    """Only an edax error's ``code`` is read: a host's ``code`` may be an int or a method whose text
    holds the error details, either one an unbounded label."""
    assert edax.error_type(error) == want


@dataclass
class TimingCase:
    bus: str  # "command" | "event"
    error: BaseException | None
    want: tuple[str, str, str | None]  # (name, outcome, error_type)
    record_raises: bool = False


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(TimingCase("command", None, ("user.create", "ok", None)), id="command_ok"),
        pytest.param(
            TimingCase("command", edax.CodecError("bad"), ("user.create", "error", "codec")),
            id="command_error",
        ),
        pytest.param(
            TimingCase("event", ValueError("bad"), ("Deposited", "error", "builtins.ValueError")),
            id="event_error_from_a_sync_handler",
        ),
        pytest.param(
            TimingCase("command", None, ("user.create", "ok", None), record_raises=True),
            id="a_raising_recorder_is_logged_and_the_result_still_returns",
        ),
    ],
)
async def test_timing_records_the_handler_outcome(case: TimingCase, caplog: pytest.LogCaptureFixture) -> None:
    rows: list[tuple[str, str, str | None, int]] = []

    def record(name: str, outcome: str, error_type: str | None, duration_ns: int) -> None:
        rows.append((name, outcome, error_type, duration_ns))
        if case.record_raises:
            raise RuntimeError("exporter down")

    def run(x: Any) -> str:
        if case.error is not None:
            raise case.error
        return "done"

    async def dispatch() -> Any:
        if case.bus == "command":
            bus = edax.CommandBus(timing(record, lambda cmd: cmd.type))

            async def handler(cmd: CreateUser) -> str:
                return run(cmd)

            bus.register("user.create", handler)
            return await bus.dispatch(CreateUser("a@b.c"))
        router = edax.EventRouter(timing(record, lambda env: env.name))

        def event_handler(env: edax.EventMessage) -> None:  # sync: the middleware awaits either shape
            run(env)

        router.register("Deposited", event_handler)
        return await router.dispatch(build_message("acct-1", "Deposited", 1, data=DEPOSITED))

    with caplog.at_level("ERROR", logger="edax.observe"):
        if case.error is None:
            assert await dispatch() == "done"
        else:
            with pytest.raises(type(case.error)):
                await dispatch()

    [(name, outcome, error_type, duration_ns)] = rows
    assert (name, outcome, error_type) == case.want
    assert duration_ns >= 0
    assert_logged(
        caplog,
        "edax.observe",
        [f"edax observe: {record.__qualname__} raised; the failure is ignored"] if case.record_raises else [],
    )


class AsyncHooks(edax.Observer):
    """A host that wrote ``async def`` hooks, as every other host callback in the SDK may be. Each call
    returns a coroutine nobody awaits."""

    def __init__(self) -> None:
        self.ran = 0

    async def transaction(self, outcome: str, duration_ns: int) -> None:  # type: ignore[override]  # ty: ignore[invalid-method-override]
        self.ran += 1


class ReturnsAFuture(edax.Observer):
    """A hook that hands back some other awaitable: logged the same way, and left alone."""

    def transaction(self, outcome: str, duration_ns: int) -> Any:
        return asyncio.get_running_loop().create_future()


@pytest.mark.parametrize("observer", [AsyncHooks, ReturnsAFuture], ids=["async_def_hook", "other_awaitable"])
async def test_an_async_hook_is_reported_and_never_left_dangling(
    observer: type[edax.Observer], caplog: pytest.LogCaptureFixture
) -> None:
    """An ``async def`` hook returns a coroutine nobody awaits, so its metric would drop with only a
    ``RuntimeWarning``. The guard closes the coroutine, logs an error naming the hook, and the work goes on."""
    hooks = observer()
    db = MemoryTransactor()
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # a "never awaited" warning fails the test
        async with edax.transaction(db, observer=hooks):
            pass
        gc.collect()
    assert "returned an awaitable, which is never awaited" in caplog.text
    assert observer.__name__ + ".transaction" in caplog.text
    assert getattr(hooks, "ran", 0) == 0, "nothing ran the hook's body"

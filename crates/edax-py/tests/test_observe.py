"""The telemetry seam: what each orchestration object reports to an :class:`edax.Observer`, one case
per branch of the code that emits; the pull surface (``stats()`` / ``idle()`` / ``Relay.last`` /
``KafkaConsumer.stats()``); and the ``timing`` middleware. A recording observer stands in for a
host's exporter, so every case asserts the emitted arguments, not that something was called."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from unittest import mock

import edax
import pytest
from conftest import (
    CounterCodec,
    CreateUser,
    FakeKafkaClient,
    RefusingLedger,
    Tick,
    TickEvents,
    TopicPartition,
    assert_logged,
    build_envelope,
    delivered,
    kafka_consumer,
)
from edax._async import maybe_await
from edax.backend.kafka import KafkaConsumer
from edax.backend.memory import MemoryEventStore, MemoryIdempotencyStore
from edax.broker import UndecodableDelivery, _ConsumerLoop
from edax.observe import CLAIM_VERDICTS, DELIVERY_OUTCOMES, SETTLEMENTS, TRANSACTIONS, timing
from edax.store import transaction


class Recording(edax.Observer):
    """Every hook appends its arguments, so a case asserts the exact rows a branch emitted. Each
    outcome string is checked against the published vocabulary, so an emitter cannot drift from it."""

    def __init__(self) -> None:
        self.claims: list[tuple[str, str]] = []  # (name, verdict)
        self.settlements: list[tuple[str, str, int]] = []  # (name, how, duration_ns)
        self.deliveries: list[tuple[str, str, int, int, Optional[str]]] = []
        self.settlement_failures: list[tuple[str, str, type[BaseException]]] = []
        self.transactions: list[tuple[str, int]] = []
        self.drains: list[tuple[int, int, Optional[int]]] = []
        self.drain_failures: list[type[BaseException]] = []
        self.undecodables: list[tuple[str, type[BaseException]]] = []

    def claim(self, name: str, verdict: str) -> None:
        assert verdict in CLAIM_VERDICTS, verdict
        self.claims.append((name, verdict))

    def settled(self, name: str, how: str, duration_ns: int) -> None:
        assert how in SETTLEMENTS, how
        self.settlements.append((name, how, duration_ns))

    def delivery(
        self, name: str, outcome: str, attempt: int, duration_ns: int, error_type: Optional[str]
    ) -> None:
        assert outcome in DELIVERY_OUTCOMES, outcome
        self.deliveries.append((name, outcome, attempt, duration_ns, error_type))

    def settlement_failed(self, name: str, event_id: str, error: BaseException) -> None:
        self.settlement_failures.append((name, event_id, type(error)))

    def transaction(self, outcome: str, duration_ns: int) -> None:
        assert outcome in TRANSACTIONS, outcome
        self.transactions.append((outcome, duration_ns))

    def drain(self, published: int, duration_ns: int, oldest_timestamp: Optional[int]) -> None:
        self.drains.append((published, duration_ns, oldest_timestamp))

    def drain_failed(self, error: BaseException) -> None:
        self.drain_failures.append(type(error))

    def undecodable(self, message_id: str, error: BaseException) -> None:
        self.undecodables.append((message_id, type(error)))


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
        self, name: str, outcome: str, attempt: int, duration_ns: int, error_type: Optional[str]
    ) -> None:
        _boom()

    def settlement_failed(self, name: str, event_id: str, error: BaseException) -> None:
        _boom()

    def transaction(self, outcome: str, duration_ns: int) -> None:
        _boom()

    def drain(self, published: int, duration_ns: int, oldest_timestamp: Optional[int]) -> None:
        _boom()

    def drain_failed(self, error: BaseException) -> None:
        _boom()

    def undecodable(self, message_id: str, error: BaseException) -> None:
        _boom()


DEPOSITED = b'{"account_id":"acct-1","amount_cents":4200,"currency":"USD"}'


# =========================================================================
# claim / settled: one case per branch of `_guarded`
# =========================================================================


@dataclass
class ClaimCase:
    errors: list[Optional[BaseException]]  # per handler run; None = return normally
    payloads: list[str]  # one dispatch per entry; the email is the hashed payload
    raises: list[Optional[type[BaseException]]]  # per dispatch
    want_claims: list[str]
    want_settled: list[str]
    held: bool = False  # another dispatch holds the claim before the first one
    refusing: bool = False  # the ledger refuses every compensating write
    one_commit: bool = False  # `unit_of_work` given and the ledger joins it
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
            ClaimCase([RuntimeError("blip")], ["a@b.c"], [RuntimeError], [], ["refused"], refusing=True),
            id="release_refused_after_a_failed_handler_settles_refused",
        ),
        pytest.param(
            ClaimCase([None], ["a@b.c"], [edax.StoreError], [], ["refused"], refusing=True),
            id="complete_refused_after_a_successful_handler_settles_refused",
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
    ],
)
async def test_idempotent_reports_claim_verdicts_and_settlements(case: ClaimCase) -> None:
    observer = Recording()
    # With no `unit_of_work` the guard takes the around-the-work path and compensates by hand, so a
    # refusing ledger reaches the `refused` settlement there.
    ledger = RefusingLedger() if case.refusing else MemoryIdempotencyStore()
    unit_of_work = ledger.transactor if case.one_commit else None
    calls = 0

    async def handler(cmd: CreateUser) -> str:
        nonlocal calls
        error = case.errors[calls]
        calls += 1
        await asyncio.sleep(0.01)  # so the settled duration visibly covers the handler
        if error is not None:
            raise error
        return f"user:{cmd.email}"

    bus = edax.CommandBus(edax.idempotent(ledger, lambda cmd: "req-1", unit_of_work, observer=observer))
    bus.register("user.create", handler)
    if case.held:
        assert await ledger.claim("req-1", edax.command._payload_hash(CreateUser("a@b.c"))) is None

    for email, raises in zip(case.payloads, case.raises):
        if raises is None:
            assert await bus.dispatch(CreateUser(email)) == f"user:{email}"
        else:
            with pytest.raises(raises):
                await bus.dispatch(CreateUser(email))

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
    guarded = edax.idempotent_by_id(MemoryIdempotencyStore(), observer=observer)(lambda env: None)
    env = build_envelope("acct-1", "Deposited", 3, data=DEPOSITED)

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
    errors: list[Optional[BaseException]]  # per submitted delivery of the one event id
    want: list[tuple[str, int, Optional[str]]]  # (outcome, attempt, error_type) per delivery, in order
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
                [("unsettled", 1, "codec")],
                with_on_dead=True,
                on_dead_raises=True,
                want_log=["dead-lettering", "settling a delivery failed"],
            ),
            id="unsettled_when_on_dead_raises",
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
    case: DeliveryCase, slept: list[float], caplog: pytest.LogCaptureFixture
) -> None:
    observer = Recording()
    calls = 0

    async def handler(env: edax.EventEnvelope) -> None:
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
    env = build_envelope("acct-1", "Deposited", 1, data=DEPOSITED)
    with caplog.at_level("WARNING", logger="edax.dispatcher"):
        for _ in case.errors:
            await dispatcher.submit(edax.Delivery(env, ack, nack))  # a fresh delivery of the same id
            await dispatcher._queues[0].join()
        await dispatcher.stop()

    assert calls == len(case.errors)
    assert [(outcome, attempt, cause) for _, outcome, attempt, _, cause in observer.deliveries] == case.want
    assert all(name == "Deposited" and duration_ns >= 0 for name, _, _, duration_ns, _ in observer.deliveries)
    assert observer.settlement_failures == [("Deposited", env.id, edax.StoreError)] * case.want_failures
    assert_logged(caplog, "edax.dispatcher", case.want_log)


async def test_delivery_duration_is_the_handler_only_not_the_backoff() -> None:
    """A latency histogram over ``delivery`` must not absorb the kernel's backoff wait, so the
    duration is taken before any settlement arm sleeps."""
    observer = Recording()
    backoff_s = 0.05

    async def slow_sleep(seconds: float) -> None:
        await asyncio.sleep(backoff_s)

    async def handler(env: edax.EventEnvelope) -> None:
        raise RuntimeError("transient")

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler, observer=observer)
    dispatcher.start()
    with mock.patch.object(edax.dispatcher, "_sleep", slow_sleep):
        await dispatcher.submit(
            edax.Delivery(build_envelope("acct-1", "Deposited", 1), lambda: None, lambda: None)
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
        self._items: list[Optional[edax.Delivery]] = [poison, None]

    async def poll(self) -> Optional[edax.Delivery]:
        return self._items.pop(0)


async def _kafka_poison() -> tuple[FakeKafkaClient, UndecodableDelivery]:
    client = FakeKafkaClient()
    await client.send_and_wait("t", b"not an envelope")
    poison = delivered(await KafkaConsumer(client, serializer=edax.kernel()).poll())
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
    env = build_envelope("acct-1", "Deposited", 1)
    await dispatcher.submit(edax.Delivery(env, lambda: acked.append(env.id), lambda: None))
    await dispatcher.stop()
    assert acked == [env.id], "the delivery was not acked"


async def _drain_with(observer: edax.Observer) -> None:
    store = MemoryEventStore()
    async with transaction(store) as tx:
        await store.append(tx, "acct-1", 0, [build_envelope("acct-1", "Deposited", 1, timestamp=1_000)])
    relay = edax.Relay(store, lambda env: None, observer=observer)
    assert await relay.drain() == 1
    assert relay.last is not None and (relay.last.published, relay.last.oldest_timestamp) == (1, 1_000)


async def _guard_with(observer: edax.Observer) -> None:
    bus = edax.CommandBus(edax.idempotent(MemoryIdempotencyStore(), lambda cmd: "req-1", observer=observer))

    async def handler(cmd: CreateUser) -> str:
        return "ok"

    bus.register("user.create", handler)
    assert await bus.dispatch(CreateUser("a@b.c")) == "ok"
    assert await bus.dispatch(CreateUser("a@b.c")) == "ok"  # the replay: `claim` raises too


async def _consume_with(observer: edax.Observer) -> None:
    client, poison = await _kafka_poison()
    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=lambda env: None, observer=observer)
    await _Poisoned(poison).run(dispatcher)
    assert client.seeks == [0], "the poison message was not settled"


async def _failing_drain_with(observer: edax.Observer) -> None:
    store = MemoryEventStore()
    async with transaction(store) as tx:
        await store.append(tx, "acct-1", 0, [build_envelope("acct-1", "Deposited", 1)])

    def publish(env: edax.EventEnvelope) -> None:
        raise edax.StoreError("broker down")

    with pytest.raises(edax.StoreError):  # the drain's own error, not the observer's
        await edax.Relay(store, publish, observer=observer).drain()


async def _commit_with(observer: edax.Observer) -> None:
    store = MemoryEventStore()
    async with transaction(store, observer=observer) as tx:
        await store.append(tx, "acct-1", 0, [build_envelope("acct-1", "Deposited", 1)])
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


# =========================================================================
# drain, and the pull surface
# =========================================================================


@pytest.mark.parametrize(
    "timestamps, want",
    [
        pytest.param([], (0, None), id="empty_batch_has_no_oldest"),
        pytest.param([3_000, 1_000, 4_000], (3, 1_000), id="oldest_is_the_lowest_timestamp"),
    ],
)
async def test_relay_reports_and_keeps_the_last_drain(
    timestamps: list[int], want: tuple[int, Optional[int]]
) -> None:
    observer = Recording()
    store = MemoryEventStore()
    async with transaction(store) as tx:
        for i, ts in enumerate(timestamps, start=1):
            await store.append(tx, "acct-1", i - 1, [build_envelope("acct-1", "Deposited", i, timestamp=ts)])
    relay = edax.Relay(store, lambda env: None, observer=observer)
    assert relay.last is None

    published = await relay.drain()

    assert published == want[0]
    assert relay.last is not None
    assert (relay.last.published, relay.last.oldest_timestamp) == want
    assert relay.last.duration_ns > 0
    assert observer.drains == [(relay.last.published, relay.last.duration_ns, relay.last.oldest_timestamp)]


async def test_a_raising_publish_reports_drain_failed_and_leaves_last_untouched() -> None:
    observer = Recording()
    store = MemoryEventStore()
    async with transaction(store) as tx:
        await store.append(tx, "acct-1", 0, [build_envelope("acct-1", "Deposited", 1)])

    def publish(env: edax.EventEnvelope) -> None:
        raise edax.StoreError("broker down")

    relay = edax.Relay(store, publish, observer=observer)
    with pytest.raises(edax.StoreError):
        await relay.drain()
    assert relay.last is None, "a drain that did not mark the batch is not a completed drain"
    assert (observer.drains, observer.drain_failures) == ([], [edax.StoreError])


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
async def test_transaction_reports_how_the_unit_of_work_ended(
    error: Optional[BaseException], want: str
) -> None:
    observer = Recording()
    store = MemoryEventStore()

    async def unit() -> None:
        async with transaction(store, observer=observer) as tx:
            await store.append(tx, "acct-1", 0, [build_envelope("acct-1", "Deposited", 1)])
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

    async def handler(env: edax.EventEnvelope) -> None:
        await release.wait()

    dispatcher = edax.KeyOrderedDispatcher(workers=2, handler=handler)
    assert dispatcher.stats() == edax.DispatcherStats([0, 0], 0, 0, 0, False)
    assert dispatcher.idle()

    dispatcher.start()
    first = build_envelope("acct-1", "Deposited", 1)
    await dispatcher.submit(edax.Delivery(first, lambda: None, lambda: None))
    await asyncio.sleep(0)  # the worker takes it and parks in the handler
    await dispatcher.submit(build_envelope("acct-1", "Deposited", 2))  # same key: queued behind it
    stats = dispatcher.stats()
    assert (stats.in_flight, sum(stats.queue_depth), stats.running) == (1, 1, True)
    assert not dispatcher.idle(), "an item a worker holds is not idle, even with the queue drained"

    release.set()
    await dispatcher.stop()
    assert dispatcher.stats() == edax.DispatcherStats([0, 0], 0, 0, 0, False)
    assert dispatcher.idle()


async def test_dispatcher_stats_count_the_failure_maps(slept: list[float]) -> None:
    async def handler(env: edax.EventEnvelope) -> None:
        raise RuntimeError("transient")

    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=handler)
    dispatcher.start()
    await dispatcher.submit(
        edax.Delivery(build_envelope("acct-1", "Deposited", 1), lambda: None, lambda: None)
    )
    await dispatcher.stop()
    stats = dispatcher.stats()
    assert (stats.attempts_tracked, stats.contended_tracked, stats.in_flight) == (1, 0, 0)
    assert dispatcher.idle(), "a nacked message is the broker's again, not in flight here"


def _stats(consumer: KafkaConsumer) -> dict[Any, tuple[int, int, int]]:
    """``(unsettled, max_delivered, committed)`` per partition. ``stats()`` returns the kernel's own
    ``PartitionStats`` rows, which are native objects with no ``__eq__``, so the fields compare."""
    return {tp: (row.unsettled, row.max_delivered, row.committed) for tp, row in consumer.stats().items()}


async def test_kafka_consumer_stats_track_inflight_across_ack_and_nack() -> None:
    _, consumer = await kafka_consumer(2)
    tp = TopicPartition("t", 0)
    assert _stats(consumer) == {}
    d0, d1 = [delivered(await consumer.poll()) for _ in range(2)]
    assert _stats(consumer) == {tp: (2, 1, -1)}

    await d1.ack()  # out of order: 0 still in flight, so the watermark commits at 0
    assert _stats(consumer) == {tp: (1, 1, 0)}
    await d0.nack()  # stays in flight until its redelivery settles
    assert _stats(consumer) == {tp: (1, 1, 0)}
    await d0.ack()
    assert _stats(consumer) == {tp: (0, 1, 2)}

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
    ],
)
def test_error_type(error: BaseException, want: str) -> None:
    assert edax.error_type(error) == want


@dataclass
class TimingCase:
    bus: str  # "command" | "event"
    error: Optional[BaseException]
    want: tuple[str, str, Optional[str]]  # (name, outcome, error_type)
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
    rows: list[tuple[str, str, Optional[str], int]] = []

    def record(name: str, outcome: str, error_type: Optional[str], duration_ns: int) -> None:
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
        router = edax.EventRouter(timing(record, lambda env: env.event.name))

        def event_handler(env: edax.EventEnvelope) -> None:  # sync: the middleware awaits either shape
            run(env)

        router.register("Deposited", event_handler)
        return await router.dispatch(build_envelope("acct-1", "Deposited", 1, data=DEPOSITED))

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

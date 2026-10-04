"""Saga seam exercised by an order process manager: the runner, the saga outbox, the tombstone, and the
relay that sends the commands. The faults are the ones a saga meets in production: two workers folding one
instance at once, a crash on either side of the commit, a relay that dies after it sent and before it
marked, a lost response, and late or redelivered events after completion."""

import asyncio
import copy
import json
import pickle
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, ClassVar

import edax
import pytest
from conftest import NO_WAIT, Recording, build_message
from edax import CommandMessage
from edax.backend.memory import MemoryIdempotencyStore, MemorySagaStore, MemoryTransactor
from edax.command import Handler
from edax.saga import Completed, SagaRunner
from edax.store import Tx, current_tx
from edax.testing.faults import LostResponse
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st


def env(aggregate_id: str, name: str, order_id: str = "order-7") -> edax.EventMessage:
    """An event of `aggregate_id` about `order_id`: the order and its payment are two aggregates with one
    correlation, so two workers can fold one instance at once."""
    return build_message(
        aggregate_id,
        name,
        1,
        id=f"{aggregate_id}-{name}",
        data=b'{"total": 1200}',
        metadata={"order": order_id},
    )


@dataclass
class Fulfill:
    order_id: str
    type: ClassVar[str] = "Fulfill"  # a class attribute: the hash takes it as the name, not a field


@dataclass
class Refund:
    order_id: str
    type: ClassVar[str] = "Refund"


COMMANDS = edax.json_codec({"Fulfill": Fulfill, "Refund": Refund})


class OrderSaga:
    """`OrderCreated` opens the instance; `PaymentReceived` fulfills it and `OrderCancelled` refunds it,
    both completing it. `Noted` keeps it running. The instance is the order, whatever the aggregate."""

    name: ClassVar[str] = "order"

    def __init__(self) -> None:
        self.folds = 0

    def instance_id(self, env: edax.EventMessage, /) -> str | None:
        return env.metadata.get("order")

    def handle(self, state: Any, env: edax.EventMessage, /) -> tuple[Any, list[Any]]:
        self.folds += 1
        order_id = env.metadata["order"]
        name = env.name
        if state is None:
            return ({"status": "open"}, []) if name == "OrderCreated" else (Completed, [])
        if name == "PaymentReceived":
            return Completed, [Fulfill(order_id)]
        if name == "OrderCancelled":
            return Completed, [Refund(order_id)]
        return {"status": "open", "noted": name}, []


#: The kernel's marks once the instance folded `OrderCreated` of `order-7`.
CREATED = '{"order-7":{"sequence":1,"id":"order-7-OrderCreated"}}'


def runner(store: edax.SagaStore, saga: OrderSaga | None = None) -> SagaRunner:
    return SagaRunner(saga or OrderSaga(), store, COMMANDS, now=lambda: 1_714_564_800_000_000_000)


async def sent_by_relay(run: SagaRunner, store: edax.SagaStore) -> list[Any]:
    """Drain every committed command through the runner's relay, recording the decoded commands."""
    sent: list[Any] = []

    async def record(cmd: Any) -> None:
        sent.append(cmd)

    bus = edax.CommandBus()
    for name in ("Fulfill", "Refund"):
        bus.register(name, record)
    relay = run.relay(bus, pacing=NO_WAIT)
    for _ in range(50):  # bounded, so a relay that never drains fails instead of hanging
        if not await relay.drain():
            break
    assert await store.unpublished() == []
    return sent


async def test_a_step_persists_its_state_and_commands_and_a_completed_instance_takes_no_event() -> None:
    @dataclass
    class Step:
        event: str
        want: edax.SagaInstance | None
        want_outbox: list[str]

    open_at = lambda v, noted=None: edax.SagaInstance(  # noqa: E731
        {"status": "open"} if noted is None else {"status": "open", "noted": noted},
        v,
        folded=f'{{"order-7":{{"sequence":1,"id":"order-7-{noted or "OrderCreated"}"}}}}',
    )
    tombstone = edax.SagaInstance(None, 3, completed=True)
    steps = [
        Step("Stray", None, []),  # nothing loaded, nothing kept, no command: no write at all
        Step("OrderCreated", open_at(1), []),
        Step("Noted", open_at(2, "Noted"), []),
        Step("PaymentReceived", tombstone, ["order-7:3:0"]),
        # A redelivered payment, a late cancel and a fresh create all find the tombstone: no fold, no
        # write, no command. Completion is final.
        Step("PaymentReceived", tombstone, ["order-7:3:0"]),
        Step("OrderCancelled", tombstone, ["order-7:3:0"]),
        Step("OrderCreated", tombstone, ["order-7:3:0"]),
    ]
    store = MemorySagaStore()
    saga = OrderSaga()
    run = runner(store, saga)
    for step in steps:
        await run.on_event(env("order-7", step.event))
        assert await store.load("order-7") == step.want, step.event
        assert [c.id for c in await store.unpublished()] == step.want_outbox, step.event
    assert saga.folds == 4, "a completed instance's events are not even folded"
    row = (await store.unpublished())[0]
    paid = env("order-7", "PaymentReceived")
    assert (row.aggregate_id, row.name, bytes(row.data), row.issued_at) == (
        "order-7",
        "Fulfill",
        b'{"order_id":"order-7"}',
        1_714_564_800_000_000_000,
    ), "the instance is the command's aggregate"
    assert (row.correlation_id, row.causation_id, row.idempotency_key) == (
        paid.correlation_id,
        paid.id,
        f"5:order:{row.id}",
    ), "the command is a child of the event that decided it, keyed by the saga and its own id"
    assert await sent_by_relay(run, store) == [Fulfill("order-7")]


async def test_saga_state_is_stored_as_a_copy() -> None:
    # State crosses the native store as JSON, so a caller mutating the object it saved cannot change
    # what a later load reads: the stored instance is what was committed, not an alias.
    store = MemorySagaStore()
    state = {"status": "awaiting_payment", "items": ["sku-1"]}
    await store.save("o1", state, 0, "", [])
    state["items"].append("sku-2")
    loaded = await store.load("o1")
    assert loaded is not None and loaded.state == {"status": "awaiting_payment", "items": ["sku-1"]}


# =========================================================================
# Races: two workers on one instance, forced into one interleaving
# =========================================================================


class RacingStore(MemorySagaStore):
    """The real store, with a hook run once between a worker's load and its write: the interleaving the
    version exists for, forced deterministically instead of with tasks and sleeps."""

    def __init__(self, transactor: MemoryTransactor) -> None:
        super().__init__(transactor)
        self.after_load: Callable[[], Awaitable[None]] | None = None

    async def load(self, id: str, tx: Tx | None = None) -> edax.SagaInstance | None:
        loaded = await super().load(id, tx)
        hook, self.after_load = self.after_load, None
        if hook is not None:
            await hook()
        return loaded


@pytest.mark.parametrize(
    "loser, winner, want",
    [
        # Payment and cancellation race. The cancel commits a Refund first; the
        # payment fold's Fulfill must never go out.
        pytest.param(
            "PaymentReceived", "OrderCancelled", [Refund("order-7")], id="cancel_wins_no_fulfill_escapes"
        ),
        pytest.param(
            "OrderCancelled", "PaymentReceived", [Fulfill("order-7")], id="payment_wins_no_refund_escapes"
        ),
    ],
)
async def test_the_fold_that_loses_the_version_race_sends_nothing(
    loser: str, winner: str, want: list[Any]
) -> None:
    db = MemoryTransactor()
    store = RacingStore(db)
    await runner(store).on_event(env("order-7", "OrderCreated"))

    async def the_other_worker_commits_first() -> None:
        await runner(MemorySagaStore(db)).on_event(env("order-7", winner))

    store.after_load = the_other_worker_commits_first
    loser_runner = runner(store)
    with pytest.raises(edax.ConcurrencyError):
        await loser_runner.on_event(env("payment-7", loser))
    assert not edax.is_terminal(edax.ConcurrencyError("x")), "the conflict is redelivered, not dead-lettered"
    await loser_runner.on_event(env("payment-7", loser))  # the redelivery finds the tombstone
    assert await store.load("order-7") == edax.SagaInstance(None, 2, completed=True)
    assert await sent_by_relay(loser_runner, store) == want


# =========================================================================
# Crashes on either side of the commit, and lost responses
# =========================================================================


class FailingSagaStore(MemorySagaStore):
    """The real store, whose writes fail while ``fail_write`` is set: the store went away mid-step."""

    fail_write = False

    async def save(
        self,
        id: str,
        state: Any,
        version: int,
        folded: str,
        commands: Sequence[CommandMessage],
        tx: Tx | None = None,
    ) -> int:
        if self.fail_write:
            raise edax.StoreError("saga store unavailable")
        return await super().save(id, state, version, folded, commands, tx)

    async def complete(
        self, id: str, version: int, commands: Sequence[CommandMessage], tx: Tx | None = None
    ) -> int:
        if self.fail_write:
            raise edax.StoreError("saga store unavailable")
        return await super().complete(id, version, commands, tx)


@pytest.mark.parametrize("how", ["store_refuses_the_write", "unit_of_work_rolls_back"])
async def test_a_step_whose_write_does_not_land_sends_nothing(how: str) -> None:
    # Crash before the commit: the commands are part of the write, so nothing is left to send, and the
    # redelivery decides again from the state that did land.
    db = MemoryTransactor()
    store = FailingSagaStore(db)
    run = runner(store)
    await run.on_event(env("order-7", "OrderCreated"))
    store.fail_write = how == "store_refuses_the_write"
    with pytest.raises((edax.StoreError, RuntimeError)):
        async with edax.transaction(db):
            await run.on_event(env("payment-7", "PaymentReceived"))
            raise RuntimeError("the unit of work failed after the saga step")
    assert await store.load("order-7") == edax.SagaInstance({"status": "open"}, 1, folded=CREATED)
    assert await store.unpublished() == [], "nothing to send"

    store.fail_write = False
    await run.on_event(env("payment-7", "PaymentReceived"))
    assert await sent_by_relay(run, store) == [Fulfill("order-7")]


class CrashingMark(MemorySagaStore):
    """The outbox, whose ``mark_published`` fails while ``crash`` is set: the relay died after it sent."""

    crash = False

    async def mark_published(self, ids: Sequence[str]) -> None:
        if self.crash:
            raise edax.StoreError("relay crashed before the mark")
        await super().mark_published(ids)


async def test_a_committed_command_survives_a_crash_and_a_resend_is_deduplicated() -> None:
    # Crash after the commit, before the send: the command waits in the outbox and the next relay sends
    # it. Crash after the send, before the mark: the relay sends it again (at-least-once), and the guard
    # replays the row's idempotency key, its saga command id, so the handler runs once (effectively-once).
    store = CrashingMark()
    run = runner(store)
    await run.on_event(env("order-7", "OrderCreated"))
    await run.on_event(env("payment-7", "PaymentReceived"))
    # The process dies here: nothing was sent, and the row is still waiting.
    assert [c.id for c in await store.unpublished()] == ["order-7:2:0"]

    handled: list[Any] = []
    sent: list[str | None] = []

    def recording(next_: Handler) -> Handler:
        """Outside the guard: sees every send, the replayed one too."""

        async def handler(cmd: Any) -> Any:
            msg = edax.current_command()
            sent.append(None if msg is None else msg.idempotency_key)
            return await next_(cmd)

        return handler

    async def fulfill(cmd: Fulfill) -> None:
        handled.append(cmd)

    bus = edax.CommandBus(recording, edax.idempotent(MemoryIdempotencyStore(), scope="fulfilment"))
    bus.register("Fulfill", fulfill)
    relay = run.relay(bus, pacing=NO_WAIT)
    store.crash = True
    with pytest.raises(edax.StoreError, match="before the mark"):
        await relay.drain()
    store.crash = False
    assert await relay.drain() == 1, "re-sent after the crash"
    assert await relay.drain() == 0, "and marked"
    assert sent == ["5:order:order-7:2:0"] * 2, "sent twice, under the row's saga-scoped key"
    assert handled == [Fulfill("order-7")], "applied once"


@pytest.mark.parametrize("lost", ["save", "complete"])
async def test_a_lost_write_response_converges_on_redelivery(lost: str) -> None:
    # The write lands, then the runner sees a StoreError as if it hadn't. The redelivery reads what
    # landed, so it never repeats the step: a lost `complete` finds the tombstone, a lost `save` folds the
    # advanced state. Either way the command committed once and is sent once.
    store: Any = LostResponse(MemorySagaStore())
    run = runner(store)
    if lost == "save":
        store.arm("save")
        with pytest.raises(edax.StoreError):
            await run.on_event(env("order-7", "OrderCreated"))
        assert await store.load("order-7") == edax.SagaInstance({"status": "open"}, 1, folded=CREATED), (
            "the save landed"
        )
        await run.on_event(env("order-7", "Noted"))
    else:
        await run.on_event(env("order-7", "OrderCreated"))
    store.arm("complete")
    with pytest.raises(edax.StoreError):
        await run.on_event(env("payment-7", "PaymentReceived"))
    await run.on_event(env("payment-7", "PaymentReceived"))
    instance = await store.load("order-7")
    assert instance is not None and instance.completed
    assert await sent_by_relay(run, store) == [Fulfill("order-7")]


# =========================================================================
# Redelivery after the step committed: a lost ack, a crash before the offset commit
# =========================================================================


@dataclass
class Reserve:
    sku: str
    order_id: str
    type: ClassVar[str] = "Reserve"


RESERVE = edax.json_codec({"Reserve": Reserve})


class ReserveSaga:
    """Every event it folds asks the warehouse to reserve stock, so a fold run twice shows as a second
    ``Reserve``, a workflow step that must not repeat."""

    name: ClassVar[str] = "reserve"

    def instance_id(self, env: edax.EventMessage, /) -> str | None:
        return "order-7"

    def handle(self, state: Any, env: edax.EventMessage, /) -> tuple[Any, list[Any]]:
        return (state or 0) + 1, [Reserve("sku-1", env.id)]


def at(aggregate_id: str, sequence: int | None, id: str, name: str = "OrderCreated") -> edax.EventMessage:
    return build_message(aggregate_id, name, sequence, id=id, data=b'{"total": 1200}', metadata={"order": "order-7"})


def failed(id: str) -> edax.EventMessage:
    """The unsequenced failure a dead-letter sink folds back when one of the instance's commands dies."""
    return at("order-7", None, id, edax.SAGA_COMMAND_FAILED)


@pytest.mark.parametrize(
    "deliveries, want",
    [
        pytest.param(
            [at("order-7", 1, "o-1"), at("order-7", 1, "o-1")],
            ["o-1"],
            id="created_redelivered_after_ack_loss",
        ),
        pytest.param(
            [
                at("order-7", 1, "o-1"),
                at("order-7", 2, "o-2"),
                at("order-7", 1, "o-1"),
                at("order-7", 2, "o-2"),
            ],
            ["o-1", "o-2"],
            id="a_crash_replays_every_event_above_the_commit",
        ),
        pytest.param(
            [
                at("order-7", 1, "o-1"),
                at("payment-7", 1, "p-1"),
                at("order-7", 1, "o-1"),
                at("payment-7", 1, "p-1"),
            ],
            ["o-1", "p-1"],
            id="another_aggregates_event_between_them",
        ),
        pytest.param(
            [at("order-7", None, "u-1"), at("order-7", None, "u-1")], ["u-1"], id="an_unsequenced_event_again"
        ),
        pytest.param(
            # With no sequence there is no order to compare: only the last event is known.
            [at("order-7", None, "u-1"), at("order-7", None, "u-2"), at("order-7", None, "u-1")],
            ["u-1", "u-2", "u-1"],
            id="an_older_unsequenced_event_is_folded_again",
        ),
        pytest.param(
            # A relay that lost its mark replays the first dead row after the second died: the instance
            # keeps every failure it folded, so neither compensates twice.
            [failed("order-7:1:0:failed"), failed("order-7:1:1:failed"), failed("order-7:1:0:failed")],
            ["order-7:1:0:failed", "order-7:1:1:failed"],
            id="two_failures_then_the_first_replayed",
        ),
        pytest.param(
            [
                at("order-7", 2, "o-2"),
                at("order-7", None, "u-1"),
                at("order-7", 2, "o-2"),
                at("order-7", None, "u-1"),
            ],
            ["o-2", "u-1"],
            id="a_crash_replays_one_aggregates_sequenced_and_unsequenced_events",
        ),
        pytest.param(
            # The limit of the sequence mark: once a later event folded, a re-driven dead letter is below it.
            [at("order-7", 3, "o-3"), at("order-7", 5, "o-5"), at("order-7", 4, "o-4")],
            ["o-3", "o-5"],
            id="a_re_driven_dead_letter_below_the_mark_is_not_folded",
        ),
    ],
)
async def test_an_event_redelivered_after_its_step_committed_is_not_folded_again(
    deliveries: list[edax.EventMessage], want: list[str]
) -> None:
    """The step committed (state and ``Reserve`` in one write), then the ack was lost or the consumer crashed
    before its offset commit, so the broker hands the same messages over again. Folding one again would
    commit its ``Reserve`` a second time under the next version's command id, which a consumer deduplicating
    by command id runs twice."""
    store = MemorySagaStore()
    run = SagaRunner(ReserveSaga(), store, RESERVE)
    for delivery in deliveries:
        await run.on_event(delivery)
    sent = [run.command(row) for row in await store.unpublished()]
    assert [r.order_id for r in sent] == want, "one Reserve per event folded"
    instance = await store.load("order-7")
    assert instance is not None and (instance.state, instance.version) == (len(want), len(want)), (
        "one write per fold"
    )


async def test_a_redelivery_through_the_dispatcher_sends_reserve_once() -> None:
    """The same fault end to end: the runner behind a guarded relay and the real dispatcher, the ack of the
    committed step lost, the broker redelivering. The warehouse gets one ``Reserve``."""
    store = MemorySagaStore()
    run = SagaRunner(ReserveSaga(), store, RESERVE)
    acks: list[str] = []
    dispatcher = edax.KeyOrderedDispatcher(1, run.on_event, pacing=NO_WAIT)
    dispatcher.start()
    created = at("order-7", 1, "o-1")
    for lost_ack in (True, False):
        await dispatcher.submit(
            edax.Delivery(created, lambda lost=lost_ack: None if lost else acks.append("ack"), lambda: None)
        )
        await dispatcher._queues[0].join()
    await dispatcher.stop()
    reserved: list[Reserve] = []

    async def warehouse(cmd: Reserve) -> None:
        reserved.append(cmd)

    bus = edax.CommandBus()
    bus.register("Reserve", warehouse)
    await run.relay(bus, pacing=NO_WAIT).drain()
    assert reserved == [Reserve("sku-1", "o-1")], "the redelivered step sent nothing"
    assert acks == ["ack"], "the redelivery itself settled cleanly"


@pytest.mark.parametrize(
    "deliveries, preload_completed, want_skips",
    [
        pytest.param([at("order-7", 1, "o-1")], False, [], id="a_fold_reports_nothing"),
        pytest.param(
            [at("order-7", 1, "o-1"), at("order-7", 1, "o-1")],
            False,
            [("reserve", "repeat", "o-1")],
            id="a_repeat_by_its_id",
        ),
        pytest.param(
            [at("order-7", 3, "o-3"), at("order-7", 5, "o-5"), at("order-7", 4, "o-4")],
            False,
            [("reserve", "below_mark", "o-4")],
            id="below_the_mark_is_reported_apart_from_a_repeat",
        ),
        pytest.param(
            [at("order-7", 1, "o-1")], True, [("reserve", "completed", "o-1")], id="a_tombstone_folds_nothing"
        ),
    ],
)
async def test_every_event_an_instance_does_not_fold_is_reported_with_the_kernels_reason(
    deliveries: list[edax.EventMessage], preload_completed: bool, want_skips: list[tuple[str, str, str]]
) -> None:
    """A skip writes nothing and returns normally, so the observer is the only place it shows: a
    ``below_mark`` after a re-drive is an event the instance will never fold."""
    store = MemorySagaStore()
    if preload_completed:
        await store.complete("order-7", 0, [])
    observer = Recording()
    run = SagaRunner(ReserveSaga(), store, RESERVE, observer=observer)
    for delivery in deliveries:
        await run.on_event(delivery)
    assert observer.saga_skips == want_skips


class ShippingSaga:
    """``Shipped`` sends ``Fulfill`` and waits for its outcome. A failed ``Fulfill`` comes back as a
    :data:`~edax.saga.SAGA_COMMAND_FAILED` event, and the saga compensates with a ``Refund``."""

    name: ClassVar[str] = "shipping"

    def __init__(self) -> None:
        self.failures: list[edax.SagaCommandFailed] = []

    def instance_id(self, env: edax.EventMessage, /) -> str | None:
        return env.metadata.get("order")

    def handle(self, state: Any, env: edax.EventMessage, /) -> tuple[Any, list[Any]]:
        if env.name == edax.SAGA_COMMAND_FAILED:
            self.failures.append(json.loads(env.data))
            return Completed, [Refund(env.aggregate_id)]
        return {"status": "shipping"}, [Fulfill(env.metadata["order"])]


@pytest.mark.parametrize(
    "failure, max_attempts, want_code, want_error",
    [
        pytest.param(edax.Rejected("out of stock"), 0, "rejected", "out of stock", id="a_rejected_command"),
        pytest.param(
            RuntimeError("warehouse API timed out"),
            1,
            None,
            "warehouse API timed out",
            id="a_transient_failure_at_the_ceiling_carries_no_code",
        ),
    ],
)
async def test_a_dead_command_folds_back_into_its_instance_as_a_failure(
    failure: Exception, max_attempts: int, want_code: str | None, want_error: str
) -> None:
    """A rejected command saves no event, so without the sink the instance would wait forever. The sink
    folds the failure into the instance that sent the command, and the saga compensates."""
    store = MemorySagaStore()
    saga = ShippingSaga()
    observer = Recording()
    run = SagaRunner(saga, store, COMMANDS, now=lambda: 1_714_564_800_000_000_000, observer=observer)
    await run.on_event(env("order-7", "Shipped"))
    refunds: list[Refund] = []

    async def fulfill(cmd: Fulfill) -> None:
        raise failure

    async def refund(cmd: Refund) -> None:
        refunds.append(cmd)

    bus = edax.CommandBus()
    bus.register("Fulfill", fulfill)
    bus.register("Refund", refund)
    [shipped] = await store.unpublished()
    relay = run.relay(bus, on_dead=run.command_failed, max_attempts=max_attempts, pacing=NO_WAIT)
    assert await relay.drain() == 1, "the dead Fulfill is settled"
    assert saga.failures == [
        {"command_id": "order-7:1:0", "command": "Fulfill", "code": want_code, "error": want_error}
    ]
    [compensation] = await store.unpublished()
    assert (compensation.id, compensation.causation_id) == ("order-7:2:0", "order-7:1:0:failed")
    instance = await store.load("order-7")
    assert instance is not None and (instance.completed, instance.version) == (True, 2)
    assert await relay.drain() == 1 and refunds == [Refund("order-7")], "the Refund is sent"

    # The sink runs again for the same row (its mark was lost): the completed instance folds nothing.
    await run.command_failed(shipped, failure)
    assert len(saga.failures) == 1 and await store.unpublished() == []
    assert observer.saga_skips == [("shipping", "completed", "order-7:1:0:failed")]


# =========================================================================
# Runner branch table
# =========================================================================


class RecordingSagaStore:
    """A SagaStore fake that records each write and the ``tx`` it ran under, so a test can prove the runner
    threaded it. Resolving the ambient :func:`current_tx` is the store's job, as the ``SagaStore`` contract
    requires, so the fake does it too. The instances live in a :class:`MemorySagaStore` that auto-commits:
    the recorded handles are stand-ins from a fake transactor, which a real memory store would refuse."""

    def __init__(self) -> None:
        self._inner = MemorySagaStore()
        #: The fake database whose ambient tx the recorded writes resolve.
        self.transactor = _OneTx()
        # Explicit, so the inner store never joins the fake's ambient tx, which it would refuse.
        self._none = self._inner.transactor.auto_commit()
        self.writes: list[tuple[str, Any, list[str]]] = []

    async def load(self, id: str, tx: Tx | None = None) -> edax.SagaInstance | None:
        return await self._inner.load(id, tx=self._none)

    async def save(
        self,
        id: str,
        state: Any,
        version: int,
        folded: str,
        commands: Sequence[CommandMessage],
        tx: Tx | None = None,
    ) -> int:
        self.writes.append(
            ("save", tx if tx is not None else current_tx(self.transactor), [c.id for c in commands])
        )
        return await self._inner.save(id, state, version, folded, commands, tx=self._none)

    async def complete(
        self, id: str, version: int, commands: Sequence[CommandMessage], tx: Tx | None = None
    ) -> int:
        self.writes.append(
            ("complete", tx if tx is not None else current_tx(self.transactor), [c.id for c in commands])
        )
        return await self._inner.complete(id, version, commands, tx=self._none)

    async def unpublished(self, limit: int = 100) -> list[CommandMessage]:
        return await self._inner.unpublished(limit)

    async def mark_published(self, ids: Sequence[str]) -> None:
        await self._inner.mark_published(ids)


class NoneStateSaga:
    """Keeps ``None`` as its state: legitimate, and distinct from ``Completed``."""

    name: ClassVar[str] = "none-state"

    def instance_id(self, e: edax.EventMessage, /) -> str | None:
        return e.metadata.get("order")

    def handle(self, state: Any, e: edax.EventMessage, /) -> tuple[Any, list[Any]]:
        return None, []


class _OneTx:
    @asynccontextmanager
    async def begin(self) -> AsyncIterator[str]:
        yield "ambient-tx"


@dataclass
class RunnerCase:
    saga: Any
    event: str
    preload: str | None  # None: no instance; "open": an open one at v1; "completed": a tombstone at v2
    tx: Any = None  # explicit tx passed to on_event
    ambient: bool = False  # run inside transaction(_OneTx())
    want_writes: list[tuple[str, Any, list[str]]] | None = None
    want_state: Any = None
    order: str | None = "order-7"  # the instance the event names; None names none


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            RunnerCase(NoneStateSaga(), "Noted", "open", want_writes=[("save", None, [])], want_state=None),
            id="none_state_is_saved_not_completed",
        ),
        pytest.param(
            RunnerCase(OrderSaga(), "Noted", "open", want_writes=[("save", None, [])], want_state="noted"),
            id="a_fold_that_keeps_state_saves_it",
        ),
        pytest.param(
            RunnerCase(
                OrderSaga(),
                "PaymentReceived",
                "open",
                want_writes=[("complete", None, ["order-7:2:0"])],
                want_state=Completed,
            ),
            id="completion_writes_the_tombstone_with_its_commands",
        ),
        pytest.param(
            RunnerCase(
                OrderSaga(),
                "PaymentReceived",
                "open",
                tx="explicit-tx",
                ambient=True,
                want_writes=[("complete", "explicit-tx", ["order-7:2:0"])],
                want_state=Completed,
            ),
            id="explicit_tx_wins_over_ambient",
        ),
        pytest.param(
            RunnerCase(
                OrderSaga(),
                "PaymentReceived",
                "open",
                ambient=True,
                want_writes=[("complete", "ambient-tx", ["order-7:2:0"])],
                want_state=Completed,
            ),
            id="ambient_tx_is_joined",
        ),
        pytest.param(
            RunnerCase(OrderSaga(), "PaymentReceived", None, want_writes=[], want_state=None),
            id="completing_with_no_instance_and_no_command_writes_nothing",
        ),
        pytest.param(
            RunnerCase(
                OrderSaga(), "OrderCreated", None, want_writes=[("save", None, [])], want_state="open"
            ),
            id="a_first_event_creates_the_instance_at_version_zero",
        ),
        pytest.param(
            RunnerCase(OrderSaga(), "OrderCancelled", "completed", want_writes=[], want_state=Completed),
            id="a_completed_instance_takes_no_write",
        ),
        pytest.param(
            RunnerCase(OrderSaga(), "Unrelated", None, want_writes=[], want_state=None, order=None),
            id="an_event_with_no_instance_is_ignored",
        ),
    ],
)
async def test_runner_branches(case: RunnerCase) -> None:
    store = RecordingSagaStore()
    if case.preload is not None:
        await store.save("order-7", {"status": "open"}, 0, "", [])
    if case.preload == "completed":
        await store.complete("order-7", 1, [])
    store.writes.clear()
    run = SagaRunner(case.saga, store, COMMANDS)
    event = env("order-7", case.event)
    if case.order is None:
        event = build_message("order-7", case.event, 1, metadata={})
    if case.ambient:
        async with edax.transaction(store.transactor):
            await run.on_event(event, tx=case.tx)
    else:
        await run.on_event(event, tx=case.tx)

    assert store.writes == case.want_writes
    instance = await store.load("order-7")
    if case.want_state is Completed:
        assert instance is not None and instance.completed
    elif case.want_state is None and case.preload is None:
        assert instance is None
    elif case.want_state == "noted":
        assert instance is not None and instance.state == {"status": "open", "noted": "Noted"}
    elif case.want_state == "open":
        assert instance is not None and instance.state == {"status": "open"}
    else:
        assert instance is not None and instance.state is None and not instance.completed


async def test_the_runners_relay_keeps_the_commands_verdict() -> None:
    # The relay is where the command runs, so it is where its verdict lands: a terminal failure
    # dead-letters the row and the rows behind it go out; a transient one hands the row back.
    store = MemorySagaStore()
    run = runner(store)
    for order in ("order-1", "order-2"):
        await run.on_event(env(order, "OrderCreated", order))
        await run.on_event(env(order, "PaymentReceived", order))
    sent: list[Any] = []
    dead: list[str] = []

    async def fulfill(cmd: Fulfill) -> None:
        if cmd.order_id == "order-1":
            raise edax.Rejected("order already shipped")
        sent.append(cmd)

    async def on_dead(row: CommandMessage, error: BaseException) -> None:
        dead.append(row.id)

    bus = edax.CommandBus()
    bus.register("Fulfill", fulfill)
    assert await run.relay(bus, on_dead=on_dead, pacing=NO_WAIT).drain() == 2
    assert (dead, sent) == (["order-1:2:0"], [Fulfill("order-2")])
    assert await store.unpublished() == []


async def test_the_relay_dispatches_each_command_as_a_child_of_its_row() -> None:
    """A command handler that saves events continues the saga's flow: the relay dispatches under the
    lineage of the row, so the events it saves carry the flow's correlation and the command's id."""
    store = MemorySagaStore()
    run = runner(store)
    await run.on_event(env("order-7", "OrderCreated"))
    await run.on_event(env("order-7", "PaymentReceived"))
    (row,) = await store.unpublished()
    seen: list[edax.Lineage | None] = []

    async def fulfill(cmd: Fulfill) -> None:
        seen.append(edax.current_lineage())

    bus = edax.CommandBus()
    bus.register("Fulfill", fulfill)
    await run.relay(bus, pacing=NO_WAIT).drain()
    assert seen == [edax.Lineage(row.correlation_id, row.id)]
    assert edax.current_lineage() is None, "the lineage ends with the dispatch"


def test_completed_sentinel_identity_survives_copy_and_pickle() -> None:
    # A saga's state may travel through copy/pickle (a snapshot, a worker pool); the runner's
    # `new_state is Completed` check must still hold afterwards.
    assert copy.copy(Completed) is Completed
    assert copy.deepcopy(Completed) is Completed
    assert pickle.loads(pickle.dumps(Completed)) is Completed


# =========================================================================
# Model: two workers, stale reads, crashes and a lossy relay
# =========================================================================


@dataclass
class Did:
    step: int
    event: str
    type: ClassVar[str] = "Did"


class CountSaga:
    """Every event emits a command naming the step it made; `Finish` completes the instance."""

    name: ClassVar[str] = "count"

    def instance_id(self, env: edax.EventMessage, /) -> str | None:
        return "flow-1"

    def handle(self, state: Any, env: edax.EventMessage, /) -> tuple[Any, list[Any]]:
        step = (state or 0) + 1
        did = Did(step, env.id)
        return (Completed if env.name == "Finish" else step), [did]


class StaleRead(MemorySagaStore):
    """Answers ``load`` with what one worker read earlier, so its write races whatever committed since."""

    def __init__(self, transactor: MemoryTransactor, loaded: edax.SagaInstance | None) -> None:
        super().__init__(transactor)
        self._loaded = loaded

    async def load(self, id: str, tx: Tx | None = None) -> edax.SagaInstance | None:
        return self._loaded


OPS = st.one_of(
    st.tuples(st.just("load"), st.integers(0, 1)),
    st.tuples(st.just("write"), st.integers(0, 1), st.booleans()),
    st.tuples(st.just("drain"), st.one_of(st.none(), st.integers(0, 2)), st.booleans()),
    st.tuples(st.just("redeliver"), st.integers(0, 3)),
)


@settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(st.lists(OPS, max_size=40))
def test_the_saga_outbox_sends_each_committed_command_once_and_nothing_else(
    ops: list[tuple[Any, ...]],
) -> None:
    asyncio.run(_replay_saga(ops))


async def _replay_saga(ops: list[tuple[Any, ...]]) -> None:
    """After any history of stale reads, crashed commits, failed publishes, lost marks and redeliveries of
    committed events: nothing is sent that no committed step decided, every committed command is sent, a
    re-send happens only after a lost mark, nothing commits after completion, no event's step commits twice,
    and every committed fold read the state the previous commit left."""
    db = MemoryTransactor()
    store = CrashingMark(db)
    codec = edax.json_codec({"Did": Did})
    # Each event from an aggregate of its own: two workers fold two of them at once, as they may only for
    # events of different aggregates (a dispatcher serializes one aggregate's events).
    pending = [
        build_message(f"agg-{i}", name, 1, id=f"evt-{i}") for i, name in enumerate(["Step", "Step", "Finish"])
    ]
    pending.append(build_message("agg-late", "Step", 1, id="evt-late"))
    landed_events: list[edax.EventMessage] = []
    taken: list[tuple[edax.EventMessage, edax.SagaInstance | None] | None] = [None, None]
    committed: set[str] = set()
    sends: dict[str, int] = {}
    resends_allowed: dict[str, int] = {}
    steps: list[int] = []
    folded: list[str] = []

    async def unsent() -> set[str]:
        return {c.id for c in await store.unpublished(1000)}

    async def load(w: int) -> None:
        if taken[w] is None and pending:
            taken[w] = (pending.pop(0), await MemorySagaStore(db).load("flow-1"))

    async def write(w: int, crash: bool) -> None:
        held = taken[w]
        if held is None:
            return
        event, loaded = held
        taken[w] = None
        was_completed = (await MemorySagaStore(db).load("flow-1") or edax.SagaInstance(None)).completed
        before = await unsent()
        run = SagaRunner(CountSaga(), StaleRead(db, loaded), codec)
        try:
            async with edax.transaction(db):
                await run.on_event(event)
                if crash:
                    raise RuntimeError("crashed before the commit")
        except (edax.ConcurrencyError, RuntimeError):
            pending.insert(0, event)  # redelivered
            return
        landed = await unsent() - before
        assert not (was_completed and landed), "a commit landed after the instance completed"
        if landed:
            landed_events.append(event)
        for row in await store.unpublished(1000):
            if row.id in landed:
                did = codec.unmarshal(row)
                steps.append(did.step)
                folded.append(did.event)
        committed.update(landed)

    async def drain(fail_at: int | None, crash_mark: bool) -> None:
        index = 0
        sent: list[str] = []

        async def publish(row: CommandMessage) -> None:
            nonlocal index
            at, index = index, index + 1
            if at == fail_at:
                raise edax.StoreError("broker unavailable")
            sent.append(row.id)

        store.crash = crash_mark
        try:
            await edax.Relay(store, publish, pacing=NO_WAIT).drain(10)
        except edax.StoreError:
            pass
        finally:
            store.crash = False
        for row_id in sent:
            assert row_id in committed, f"sent {row_id}, which no committed step decided"
            sends[row_id] = sends.get(row_id, 0) + 1
            assert sends[row_id] <= 1 + resends_allowed.get(row_id, 0), (
                f"{row_id} re-sent without a lost mark"
            )
            if crash_mark:
                resends_allowed[row_id] = resends_allowed.get(row_id, 0) + 1

    def redeliver(start: int) -> None:
        """The broker hands over again every committed event from the ``start``-th on, in order: one lost
        ack when it names the last, a crash above the committed offset when it names an earlier one."""
        pending[:0] = landed_events[start:]

    for op in ops:
        if op[0] == "load":
            await load(op[1])
        elif op[0] == "write":
            await write(op[1], op[2])
        elif op[0] == "redeliver":
            redeliver(op[1])
        else:
            await drain(op[1], op[2])

    # Settle: redeliver what is pending with fresh reads, then drain with no faults.
    for w in (0, 1):
        held = taken[w]
        if held is not None:
            pending.insert(0, held[0])
            taken[w] = None
    # Bounded, so a store or runner that never converges fails here instead of hanging the suite.
    for _ in range(50):
        if not pending:
            break
        await load(0)
        await write(0, False)
    assert not pending, "the pending events never settled"
    for _ in range(50):
        if not await unsent():
            break
        await drain(None, False)

    final = await MemorySagaStore(db).load("flow-1")
    assert final is not None and final.completed, "every event was folded once the races settled"
    assert set(sends) == committed, "every committed command sent, nothing else"
    assert steps == list(range(1, len(steps) + 1)), "a stale fold landed"
    # One command per committed transition, and no event's step committed twice. An event that reached
    # the instance after `Finish` committed nothing, so `Finish` is always the last step.
    assert len(set(folded)) == len(folded) == len(committed), "an event's step committed twice"
    assert folded[-1] == "evt-2", "a step committed after completion"


async def test_a_crash_replay_of_committed_steps_is_one_of_the_histories() -> None:
    """Two steps commit, then the broker replays both (the offset commit was lost) and the last one once
    more: pinned so a shrink never loses it."""
    await _replay_saga(
        [
            ("load", 0),
            ("write", 0, False),
            ("load", 0),
            ("write", 0, False),
            ("redeliver", 0),
            ("redeliver", 1),
            ("load", 0),
            ("write", 0, False),
            ("load", 1),
            ("write", 1, False),
            ("load", 0),
            ("write", 0, False),
        ]
    )

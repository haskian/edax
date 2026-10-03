"""Saga seam exercised by an order process manager: the runner, the saga outbox, the tombstone, and the
relay that sends the commands. The faults are the ones a saga meets in production: two workers folding one
instance at once, a crash on either side of the commit, a relay that dies after it sent and before it
marked, a lost response, and late or redelivered events after completion."""

import asyncio
import copy
import pickle
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, ClassVar

import edax
import pytest
from conftest import FlakySagaStore, build_envelope
from edax.backend.memory import MemoryIdempotencyStore, MemorySagaStore, MemoryTransactor
from edax.saga import Completed, SagaCommand, SagaRunner
from edax.store import Tx, current_tx
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st


async def _no_sleep(seconds: float) -> None:
    pass


NO_WAIT = edax.Pacing(sleep=_no_sleep)


def env(aggregate_id: str, name: str, order_id: str = "order-7") -> edax.EventEnvelope:
    """An event of `aggregate_id` about `order_id`: the order and its payment are two aggregates with one
    correlation, so two workers can fold one instance at once."""
    return build_envelope(
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
    type: ClassVar[str] = "Fulfill"  # routing only: a class attribute stays out of the hash


@dataclass
class Refund:
    order_id: str
    type: ClassVar[str] = "Refund"


COMMANDS = edax.json_codec({"Fulfill": Fulfill, "Refund": Refund})


class OrderSaga:
    """`OrderCreated` opens the instance; `PaymentReceived` fulfills it and `OrderCancelled` refunds it,
    both completing it. `Noted` keeps it running. The correlation is the order, whatever the aggregate."""

    def __init__(self) -> None:
        self.folds = 0

    def correlation_id(self, env: edax.EventEnvelope, /) -> str | None:
        return env.metadata.get("order")

    def handle(self, state: Any, env: edax.EventEnvelope, /) -> tuple[Any, list[Any]]:
        self.folds += 1
        order_id = env.metadata["order"]
        name = env.event.name
        if state is None:
            return ({"status": "open"}, []) if name == "OrderCreated" else (Completed, [])
        if name == "PaymentReceived":
            return Completed, [Fulfill(order_id)]
        if name == "OrderCancelled":
            return Completed, [Refund(order_id)]
        return {"status": "open", "noted": name}, []


def runner(store: edax.SagaStore, saga: OrderSaga | None = None) -> SagaRunner:
    return SagaRunner(saga or OrderSaga(), store, COMMANDS, now=lambda: 1_714_564_800_000_000_000)


async def sent_by_relay(run: SagaRunner, store: edax.SagaStore) -> list[Any]:
    """Drain every committed command through the runner's relay, recording the decoded commands."""
    sent: list[Any] = []
    relay = run.relay(sent.append, pacing=NO_WAIT)
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
        {"status": "open"} if noted is None else {"status": "open", "noted": noted}, v
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
    assert (row.correlation_id, row.command.name, bytes(row.command.data), row.timestamp) == (
        "order-7",
        "Fulfill",
        b'{"order_id":"order-7"}',
        1_714_564_800_000_000_000,
    )
    assert await sent_by_relay(run, store) == [Fulfill("order-7")]


async def test_saga_state_is_stored_as_a_copy() -> None:
    # State crosses the native store as JSON, so a caller mutating the object it saved cannot change
    # what a later load reads: the stored instance is what was committed, not an alias.
    store = MemorySagaStore()
    state = {"status": "awaiting_payment", "items": ["sku-1"]}
    await store.save("o1", state, 0, [])
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
        # The reviewer's E4: payment and cancellation race. The cancel commits a Refund first; the
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
        self, id: str, state: Any, version: int, commands: Sequence[SagaCommand], tx: Tx | None = None
    ) -> int:
        if self.fail_write:
            raise edax.StoreError("saga store unavailable")
        return await super().save(id, state, version, commands, tx)

    async def complete(
        self, id: str, version: int, commands: Sequence[SagaCommand], tx: Tx | None = None
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
    assert await store.load("order-7") == edax.SagaInstance({"status": "open"}, 1)
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
    # keyed by the command's id runs the handler once (effectively-once).
    store = CrashingMark()
    run = runner(store)
    await run.on_event(env("order-7", "OrderCreated"))
    await run.on_event(env("payment-7", "PaymentReceived"))
    # The process dies here: nothing was sent, and the row is still waiting.
    assert [c.id for c in await store.unpublished()] == ["order-7:2:0"]

    handled: list[Any] = []
    row_id: list[str] = []

    async def fulfill(cmd: Fulfill) -> None:
        handled.append(cmd)

    bus = edax.CommandBus(edax.idempotent(MemoryIdempotencyStore(), lambda cmd: row_id[-1]))
    bus.register("Fulfill", fulfill)

    async def publish(row: SagaCommand) -> None:
        row_id.append(row.id)
        await bus.dispatch(run.command(row))

    relay = edax.Relay(store, publish, pacing=NO_WAIT)
    store.crash = True
    with pytest.raises(edax.StoreError, match="before the mark"):
        await relay.drain()
    store.crash = False
    assert await relay.drain() == 1, "re-sent after the crash"
    assert await relay.drain() == 0, "and marked"
    assert row_id == ["order-7:2:0", "order-7:2:0"], "sent twice"
    assert handled == [Fulfill("order-7")], "applied once"


@pytest.mark.parametrize("lost", ["save", "complete"])
async def test_a_lost_write_response_converges_on_redelivery(lost: str) -> None:
    # The write lands, then the runner sees a StoreError as if it hadn't. The redelivery reads what
    # landed, so it never repeats the step: a lost `complete` finds the tombstone, a lost `save` folds the
    # advanced state. Either way the command committed once and is sent once.
    store = FlakySagaStore()
    run = runner(store)
    if lost == "save":
        store.arm("save")
        with pytest.raises(edax.StoreError):
            await run.on_event(env("order-7", "OrderCreated"))
        assert await store.load("order-7") == edax.SagaInstance({"status": "open"}, 1), "the save landed"
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
        self, id: str, state: Any, version: int, commands: Sequence[SagaCommand], tx: Tx | None = None
    ) -> int:
        self.writes.append(
            ("save", tx if tx is not None else current_tx(self.transactor), [c.id for c in commands])
        )
        return await self._inner.save(id, state, version, commands, tx=self._none)

    async def complete(
        self, id: str, version: int, commands: Sequence[SagaCommand], tx: Tx | None = None
    ) -> int:
        self.writes.append(
            ("complete", tx if tx is not None else current_tx(self.transactor), [c.id for c in commands])
        )
        return await self._inner.complete(id, version, commands, tx=self._none)

    async def unpublished(self, limit: int = 100) -> list[SagaCommand]:
        return await self._inner.unpublished(limit)

    async def mark_published(self, ids: Sequence[str]) -> None:
        await self._inner.mark_published(ids)


class NoneStateSaga:
    """Keeps ``None`` as its state: legitimate, and distinct from ``Completed``."""

    def correlation_id(self, e: edax.EventEnvelope, /) -> str | None:
        return e.metadata.get("order")

    def handle(self, state: Any, e: edax.EventEnvelope, /) -> tuple[Any, list[Any]]:
        return None, []


class _OneTx:
    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[str]:
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
    order: str | None = "order-7"  # the correlation the event names; None names none


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
            id="an_event_with_no_correlation_is_ignored",
        ),
    ],
)
async def test_runner_branches(case: RunnerCase) -> None:
    store = RecordingSagaStore()
    if case.preload is not None:
        await store.save("order-7", {"status": "open"}, 0, [])
    if case.preload == "completed":
        await store.complete("order-7", 1, [])
    store.writes.clear()
    run = SagaRunner(case.saga, store, COMMANDS)
    event = env("order-7", case.event)
    if case.order is None:
        event = build_envelope("order-7", case.event, 1, metadata={})
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
    # The relay is where the command runs now, so it is where its verdict lands: a terminal failure
    # dead-letters the row and the rows behind it go out; a transient one hands the row back.
    store = MemorySagaStore()
    run = runner(store)
    for order in ("order-1", "order-2"):
        await run.on_event(env(order, "OrderCreated", order))
        await run.on_event(env(order, "PaymentReceived", order))
    sent: list[Any] = []
    dead: list[str] = []

    async def dispatch(cmd: Fulfill) -> None:
        if cmd.order_id == "order-1":
            raise edax.Rejected("order already shipped")
        sent.append(cmd)

    async def on_dead(row: SagaCommand, error: BaseException) -> None:
        dead.append(row.id)

    assert await run.relay(dispatch, on_dead=on_dead, pacing=NO_WAIT).drain() == 2
    assert (dead, sent) == (["order-1:2:0"], [Fulfill("order-2")])
    assert await store.unpublished() == []


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

    def correlation_id(self, env: edax.EventEnvelope, /) -> str | None:
        return "flow-1"

    def handle(self, state: Any, env: edax.EventEnvelope, /) -> tuple[Any, list[Any]]:
        step = (state or 0) + 1
        did = Did(step, env.id)
        return (Completed if env.event.name == "Finish" else step), [did]


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
)


@settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(st.lists(OPS, max_size=40))
def test_the_saga_outbox_sends_each_committed_command_once_and_nothing_else(
    ops: list[tuple[Any, ...]],
) -> None:
    asyncio.run(_replay_saga(ops))


async def _replay_saga(ops: list[tuple[Any, ...]]) -> None:
    """After any history of stale reads, crashed commits, failed publishes and lost marks: nothing is sent
    that no committed step decided, every committed command is sent, a re-send happens only after a lost
    mark, nothing commits after completion, and every committed fold read the state the previous commit
    left."""
    db = MemoryTransactor()
    store = CrashingMark(db)
    codec = edax.json_codec({"Did": Did})
    pending = [
        build_envelope("flow-1", name, None, id=f"evt-{i}")
        for i, name in enumerate(["Step", "Step", "Finish"])
    ]
    pending.append(build_envelope("flow-1", "Step", None, id="evt-late"))
    taken: list[tuple[edax.EventEnvelope, edax.SagaInstance | None] | None] = [None, None]
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
        for row in await store.unpublished(1000):
            if row.id in landed:
                did = codec.unmarshal(row.command)
                steps.append(did.step)
                folded.append(did.event)
        committed.update(landed)

    async def drain(fail_at: int | None, crash_mark: bool) -> None:
        index = 0
        sent: list[str] = []

        async def publish(row: SagaCommand) -> None:
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

    for op in ops:
        if op[0] == "load":
            await load(op[1])
        elif op[0] == "write":
            await write(op[1], op[2])
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

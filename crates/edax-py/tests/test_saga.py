"""Saga seam exercised by a tiny order-shipping process manager."""

import asyncio
import copy
import pickle
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Optional

import edax
import pytest
from conftest import FlakySagaStore, build_envelope
from edax.backend.memory import MemorySagaStore
from edax.saga import Completed, SagaRunner
from edax.store import Tx, current_tx


def env(aggregate_id: str, name: str) -> edax.EventEnvelope:
    return build_envelope(aggregate_id, name, 1, data=b'{"total": 1200}', metadata={"trace": "t-1"})


@dataclass
class ShipOrder:
    order_id: str
    type: str = "ShipOrder"


class OrderSaga:
    """Created -> (wait for payment) -> on PaymentReceived emit ShipOrder and complete."""

    # The parameter is named `env` because the `Saga` protocol names it that and it is not
    # positional-only, so a keyword call has to reach it.
    def correlation_id(self, env: edax.EventEnvelope) -> Optional[str]:
        if env.event.name in ("OrderCreated", "PaymentReceived"):
            return env.aggregate_id
        return None  # unrelated event: ignore

    def handle(self, state: Any, env: edax.EventEnvelope) -> tuple[Any, list[Any]]:
        if env.event.name == "OrderCreated":
            return {"status": "awaiting_payment"}, []
        if env.event.name == "PaymentReceived":
            if state is None:  # no live instance (e.g. redelivery after completion): complete, no-op
                return Completed, []
            return Completed, [ShipOrder(env.aggregate_id)]  # complete + emit
        return state, []


async def instance(store: edax.SagaStore, correlation_id: str) -> edax.SagaInstance:
    """The stored instance, or fail naming the correlation. These assertions are about *what* was
    saved, so "nothing was saved" has to read as its own failure."""
    loaded = await store.load(correlation_id)
    assert loaded is not None, f"no saga instance stored for {correlation_id!r}"
    return loaded


async def test_saga_lifecycle_and_redelivery() -> None:
    store = MemorySagaStore()
    dispatched: list[ShipOrder] = []
    runner = SagaRunner(OrderSaga(), store, dispatch=dispatched.append)

    await runner.on_event(env("o1", "SomethingElse"))  # no correlation -> ignored
    assert await store.load("o1") is None and dispatched == []

    await runner.on_event(env("o1", "OrderCreated"))  # creates instance
    assert (await instance(store, "o1")).state == {"status": "awaiting_payment"}
    assert dispatched == []

    await runner.on_event(env("o1", "PaymentReceived"))  # emits command, completes
    assert await store.load("o1") is None  # instance removed
    assert dispatched == [ShipOrder("o1")]

    await runner.on_event(env("o1", "PaymentReceived"))  # redelivery after completion: safe no-op
    assert len(dispatched) == 1


async def test_saga_version_advances_and_stale_write_conflicts() -> None:
    store = MemorySagaStore()
    assert await store.save("o1", {"n": 1}, 0) == 1  # first write at v0 -> v1
    assert (await instance(store, "o1")).version == 1
    assert await store.save("o1", {"n": 2}, 1) == 2  # at the loaded version -> v2
    with pytest.raises(edax.ConcurrencyError):
        await store.save("o1", {"n": 3}, 1)  # stale version -> conflict
    with pytest.raises(edax.ConcurrencyError):
        await store.delete("o1", 1)  # stale version -> conflict
    assert (await instance(store, "o1")).version == 2  # a refused delete keeps the instance
    await store.delete("o1", 2)  # at the current version -> removed
    assert await store.load("o1") is None
    await store.delete("o1", 2)  # already gone: idempotent, whatever version it carries
    await store.delete("never-saved", 7)
    assert await store.load("o1") is None and await store.load("never-saved") is None


async def test_saga_state_is_stored_as_a_copy() -> None:
    # State crosses the native store as JSON, so a caller mutating the object it saved cannot
    # change what a later load reads: the stored instance is what was committed, not an alias.
    store = MemorySagaStore()
    state = {"status": "awaiting_payment", "items": ["sku-1"]}
    await store.save("o1", state, 0)
    state["items"].append("sku-2")
    assert (await instance(store, "o1")).state == {"status": "awaiting_payment", "items": ["sku-1"]}


class FailingSagaStore(MemorySagaStore):
    """Fault injection: when ``fail_write`` is set, every write (save *and* delete) fails, a store
    that dies after the runner has already dispatched its commands."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_write = False

    async def save(self, id: str, state: Any, version: int, tx: Optional[Tx] = None) -> int:
        if self.fail_write:
            raise edax.StoreError("saga store unavailable")
        return await super().save(id, state, version, tx)

    async def delete(self, id: str, version: int, tx: Optional[Tx] = None) -> None:
        if self.fail_write:
            raise edax.StoreError("saga store unavailable")
        await super().delete(id, version, tx)


async def test_saga_partial_failure_redelivers_at_least_once() -> None:
    # The runner dispatches before it persists, so a failure on either side leaves the instance alive
    # and the event redeliverable: at-least-once, with duplicate commands the idempotent bus absorbs.
    store = FailingSagaStore()
    dispatched: list[ShipOrder] = []

    def failing_dispatch(cmd: ShipOrder) -> None:
        dispatched.append(cmd)
        raise edax.StoreError("command bus unavailable")

    ok = SagaRunner(OrderSaga(), store, dispatch=dispatched.append)
    failing = SagaRunner(OrderSaga(), store, dispatch=failing_dispatch)
    payment = env("o1", "PaymentReceived")

    await ok.on_event(env("o1", "OrderCreated"))
    assert (await instance(store, "o1")).state == {"status": "awaiting_payment"}

    # Dispatch fails: the instance must survive so the redelivery can re-emit the command.
    with pytest.raises(edax.StoreError):
        await failing.on_event(payment)
    assert len(dispatched) == 1
    assert (await instance(store, "o1")).state == {"status": "awaiting_payment"}

    # Redelivery: the command dispatches, but persisting the completion fails. The instance is still
    # open, so the event redelivers again and the command is emitted a second time.
    store.fail_write = True
    with pytest.raises(edax.StoreError):
        await ok.on_event(payment)
    assert len(dispatched) == 2
    assert (await instance(store, "o1")).state == {"status": "awaiting_payment"}

    # Healthy redelivery: completes and removes the instance (third dispatch of the same command).
    store.fail_write = False
    await ok.on_event(payment)
    assert len(dispatched) == 3
    assert await store.load("o1") is None

    # A fourth redelivery finds no instance: no command, no error.
    await ok.on_event(payment)
    assert len(dispatched) == 3


async def test_saga_lost_save_response_converges_on_redelivery() -> None:
    # A "lost response" is worse than the plain failure above: the write *lands*, then the runner sees
    # a StoreError as if it hadn't. Redelivery reads the landed state, so it is not a replay of the old
    # step: the saga folds the already-advanced state and does not repeat the side effect.

    # Lost delete: completion persists (instance removed), but on_event still raises. The redelivery
    # loads None and completes as a no-op, so the ShipOrder command is dispatched exactly once.
    store = FlakySagaStore()
    dispatched: list[ShipOrder] = []
    runner = SagaRunner(OrderSaga(), store, dispatch=dispatched.append)
    await runner.on_event(env("o1", "OrderCreated"))
    store.arm_lost_response = "delete"
    with pytest.raises(edax.StoreError):
        await runner.on_event(env("o1", "PaymentReceived"))
    assert len(dispatched) == 1
    assert await store.load("o1") is None  # delete landed despite the lost response
    await runner.on_event(env("o1", "PaymentReceived"))
    assert len(dispatched) == 1  # redelivery finds no instance: no duplicate command

    # Lost save: the state write lands at v1, then on_event raises. Redelivery loads the *new* state
    # (not the pre-save None), folds OrderCreated again idempotently, and re-saves at v2: no rewind,
    # no duplicate side effect. The workflow then completes normally, dispatching once.
    store = FlakySagaStore()
    dispatched = []
    runner = SagaRunner(OrderSaga(), store, dispatch=dispatched.append)
    store.arm_lost_response = "save"
    with pytest.raises(edax.StoreError):
        await runner.on_event(env("o2", "OrderCreated"))
    assert (await instance(store, "o2")).state == {"status": "awaiting_payment"}  # save landed at v1
    assert dispatched == []
    await runner.on_event(env("o2", "OrderCreated"))
    assert (await instance(store, "o2")).version == 2  # redelivery advanced the same state, did not rewind
    await runner.on_event(env("o2", "PaymentReceived"))
    assert dispatched == [ShipOrder("o2")]
    assert await store.load("o2") is None


# =========================================================================
# Runner branch table
# =========================================================================


class RecordingSagaStore:
    """A SagaStore fake that records the ``tx`` each write ran under, so a test can prove the
    runner threaded it. Resolving the ambient :func:`current_tx` is the store's job, as the
    ``SagaStore`` contract requires, so the fake does it too. The instances live in a
    :class:`MemorySagaStore` that auto-commits: the recorded handles are stand-ins from a fake
    transactor, which a real memory store would refuse."""

    def __init__(self) -> None:
        self._inner = MemorySagaStore()
        # Explicit, so the inner store never joins the fake's ambient tx, which it would refuse.
        self._none = self._inner.transactor.auto_commit()
        self.txs: list[Any] = []

    async def load(self, id: str, tx: Optional[Tx] = None) -> Optional[edax.SagaInstance]:
        return await self._inner.load(id, tx=self._none)

    async def save(self, id: str, state: Any, version: int, tx: Optional[Tx] = None) -> int:
        self.txs.append(tx if tx is not None else current_tx())
        return await self._inner.save(id, state, version, tx=self._none)

    async def delete(self, id: str, version: int, tx: Optional[Tx] = None) -> None:
        self.txs.append(tx if tx is not None else current_tx())
        await self._inner.delete(id, version, tx=self._none)


class NoneStateSaga:
    """Keeps ``None`` as its state: legitimate, and distinct from ``Completed``."""

    def correlation_id(self, e: edax.EventEnvelope) -> Optional[str]:
        return e.aggregate_id

    def handle(self, state: Any, e: edax.EventEnvelope) -> tuple[Any, list[Any]]:
        return None, []


class _OneTx:
    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[str]:
        yield "ambient-tx"


@dataclass
class RunnerCase:
    saga: Any
    tx: Any  # explicit tx passed to on_event
    ambient: bool  # run inside with_tx(_OneTx())
    async_dispatch: bool
    racer: bool  # the dispatch callback writes the same instance first
    raises: Optional[type[BaseException]]
    state_after: Any  # the store's state after the event, or Completed if removed
    txs: list[Any]
    preload: bool = True  # whether an open instance exists before the event


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            RunnerCase(NoneStateSaga(), None, False, False, False, None, None, [None]),
            id="none_state_is_saved_not_completed",
        ),
        pytest.param(
            RunnerCase(OrderSaga(), None, False, True, False, None, Completed, [None]),
            id="async_dispatch_is_awaited_before_persist",
        ),
        pytest.param(
            RunnerCase(OrderSaga(), "explicit-tx", True, False, False, None, Completed, ["explicit-tx"]),
            id="explicit_tx_wins_over_ambient",
        ),
        pytest.param(
            RunnerCase(OrderSaga(), None, True, False, False, None, Completed, ["ambient-tx"]),
            id="ambient_tx_is_joined",
        ),
        pytest.param(
            RunnerCase(
                OrderSaga(),
                None,
                False,
                False,
                True,
                edax.ConcurrencyError,
                {"status": "racer"},
                [None, None],
            ),
            id="racing_writer_between_load_and_save_conflicts",
        ),
        pytest.param(
            RunnerCase(OrderSaga(), None, False, False, False, None, Completed, [], preload=False),
            id="completing_with_no_instance_loaded_writes_nothing",
        ),
    ],
)
async def test_runner_branches(case: RunnerCase) -> None:
    store = RecordingSagaStore()
    if case.preload:
        await store.save("o1", {"status": "awaiting_payment"}, 0)  # an open instance at v1
    store.txs.clear()
    dispatched: list[Any] = []

    async def async_dispatch(cmd: Any) -> None:
        await asyncio.sleep(0)
        dispatched.append(cmd)

    def sync_dispatch(cmd: Any) -> None:
        dispatched.append(cmd)

    async def racing_dispatch(cmd: Any) -> None:
        dispatched.append(cmd)
        await store.save("o1", {"status": "racer"}, 1)  # a cross-aggregate writer got there first

    dispatch = racing_dispatch if case.racer else async_dispatch if case.async_dispatch else sync_dispatch
    runner = SagaRunner(case.saga, store, dispatch=dispatch)
    event = env("o1", "PaymentReceived")

    async def run_event() -> None:
        if case.raises is None:
            await runner.on_event(event, tx=case.tx)
        else:
            with pytest.raises(case.raises):
                await runner.on_event(event, tx=case.tx)

    if case.ambient:
        async with edax.with_tx(_OneTx()):
            await run_event()
    else:
        await run_event()

    instance = await store.load("o1")
    if case.state_after is Completed:
        assert instance is None
    else:
        assert instance is not None and instance.state == case.state_after
    # An empty `txs` is the assertion for the no-instance case: completing a correlation that was
    # never open must not write, because a versioned delete is a write a concurrent creator of the
    # same correlation would conflict with.
    assert store.txs == case.txs
    # OrderSaga emits ShipOrder on PaymentReceived, but only with a live instance to complete.
    assert len(dispatched) == (1 if isinstance(case.saga, OrderSaga) and case.preload else 0)


def test_completed_sentinel_identity_survives_copy_and_pickle() -> None:
    # A saga's state may travel through copy/pickle (a snapshot, a worker pool); the runner's
    # `new_state is Completed` check must still hold afterwards.
    assert copy.copy(Completed) is Completed
    assert copy.deepcopy(Completed) is Completed
    assert pickle.loads(pickle.dumps(Completed)) is Completed

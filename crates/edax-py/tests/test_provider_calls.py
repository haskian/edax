"""Healthy overlapping provider operations wait without sharing results or cancellation."""

from __future__ import annotations

import asyncio

import pytest
from edax.errors import KeyUnavailable
from edax.projection import MemoryProjectionStateStore
from edax.sealing import Cipher, KeyScope, MemoryKeys, Sealing, _ProviderCall, _ProviderCalls
from test_projection_at_rest import committed
from test_projection_state import target


async def test_concurrent_protected_state_reads_each_open_fresh() -> None:
    class Keys(MemoryKeys):
        reads = 0

        async def resolve(self, scope: KeyScope, cipher: Cipher, key_id: str) -> bytes:
            self.reads += 1
            await asyncio.sleep(0.005)
            return await super().resolve(scope, cipher, key_id)

    keys = Keys()
    store = MemoryProjectionStateStore(target(), sealing=Sealing(keys, "accounts"))
    await committed(store, "person-7", 1)
    states = await asyncio.gather(*(store.get("bank-eu", "account-42", "person-7") for _ in range(4)))
    assert all(state is not None and state["private"]["credits"] == 1 for state in states)
    assert keys.reads == 4


@pytest.mark.parametrize("operation", ["status", "reserve"])
async def test_overlapping_calls_do_not_share_results(operation: str) -> None:
    calls = _ProviderCalls()
    entered, release = asyncio.Event(), asyncio.Event()
    invoked = 0

    async def provider() -> int:
        nonlocal invoked
        invoked += 1
        number = invoked
        if number == 1:
            entered.set()
            await release.wait()
        return number

    first = asyncio.create_task(calls.call((operation,), provider, 1))
    await entered.wait()
    second = asyncio.create_task(calls.call((operation,), provider, 1))
    await asyncio.sleep(0)
    release.set()
    assert await asyncio.gather(first, second) == [1, 2]


@pytest.mark.parametrize("ending", ["cancel", "timeout"])
async def test_waiter_does_not_cancel_active_provider(ending: str) -> None:
    calls = _ProviderCalls()
    entered, release = asyncio.Event(), asyncio.Event()

    async def provider() -> str:
        entered.set()
        await release.wait()
        return "active"

    first = asyncio.create_task(calls.call(("status",), provider, 1))
    await entered.wait()
    waiter = asyncio.create_task(calls.call(("status",), provider, 0.01 if ending == "timeout" else 1))
    await asyncio.sleep(0)
    if ending == "cancel":
        waiter.cancel()
    with pytest.raises(asyncio.CancelledError if ending == "cancel" else KeyUnavailable):
        await waiter
    assert not first.done()
    release.set()
    assert await first == "active"


async def test_waiting_spends_the_callers_provider_budget() -> None:
    calls = _ProviderCalls()
    entered = asyncio.Event()
    invocations = 0

    async def provider() -> int:
        nonlocal invocations
        invocations += 1
        entered.set()
        await asyncio.sleep(0.06)
        return invocations

    first = asyncio.create_task(calls.call(("status",), provider, 1))
    await entered.wait()
    with pytest.raises(KeyUnavailable, match="deadline"):
        await calls.call(("status",), provider, 0.1)
    assert await first == 1
    assert invocations == 2


async def test_waiter_refuses_call_abandoned_by_cancelled_owner() -> None:
    calls = _ProviderCalls()
    entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    invocations = 0

    async def provider() -> None:
        nonlocal invocations
        invocations += 1
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()

    first = asyncio.create_task(calls.call(("reserve",), provider, None))
    await entered.wait()
    waiter = asyncio.create_task(calls.call(("reserve",), provider, None))
    await asyncio.sleep(0)
    try:
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await cancelled.wait()
        with pytest.raises(KeyUnavailable, match="unresolved"):
            await asyncio.wait_for(waiter, 0.2)
        assert invocations == 1
    finally:
        release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)


async def test_abandoned_call_keeps_its_capacity_until_actual_completion() -> None:
    calls = _ProviderCalls()
    calls._limit = 1
    release = asyncio.Event()

    async def stubborn() -> str:
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()
        return "late reservation"

    async def fresh() -> str:
        return "fresh reservation"

    try:
        with pytest.raises(KeyUnavailable, match="deadline"):
            await calls.call(("reserve", "first"), stubborn, 0.01)
        with pytest.raises(KeyUnavailable, match="too many"):
            await calls.call(("reserve", "second"), fresh, 1)
        with pytest.raises(KeyUnavailable, match="unresolved"):
            await calls.call(("reserve", "first"), fresh, 1)
    finally:
        release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
    assert await calls.call(("reserve", "first"), fresh, 1) == "fresh reservation"


@pytest.mark.parametrize("budget", [0, -1])
@pytest.mark.parametrize("already_running", [False, True])
async def test_expired_budget_creates_no_provider_or_waiter_coroutine(
    budget: float, already_running: bool, recwarn: pytest.WarningsRecorder
) -> None:
    import gc

    calls = _ProviderCalls()
    entered, release = asyncio.Event(), asyncio.Event()
    invoked = 0

    async def provider() -> None:
        nonlocal invoked
        invoked += 1
        entered.set()
        await release.wait()

    first = asyncio.create_task(calls.call(("status",), provider, 1)) if already_running else None
    if first is not None:
        await entered.wait()
    try:
        with pytest.raises(KeyUnavailable, match="deadline"):
            await calls.call(("status",), provider, budget)
        gc.collect()
        assert invoked == int(already_running)
        assert not [warning for warning in recwarn if issubclass(warning.category, RuntimeWarning)]
    finally:
        release.set()
        if first is not None:
            await first


async def test_cancelled_waiter_cannot_reserve_when_previous_call_finishes_concurrently() -> None:
    calls = _ProviderCalls()
    previous: asyncio.Future[str] = asyncio.get_running_loop().create_future()
    owner: asyncio.Task[str] | None = None
    reservations = 0

    class Released(asyncio.Event):
        async def wait(self) -> bool:
            previous.set_result("previous reservation")
            assert owner is not None
            owner.cancel()
            return True

    calls._pending[("reserve",)] = _ProviderCall(previous, released=Released())

    async def reserve() -> str:
        nonlocal reservations
        reservations += 1
        return "new reservation"

    owner = asyncio.create_task(calls.call(("reserve",), reserve, 1))
    with pytest.raises(asyncio.CancelledError):
        await owner
    assert reservations == 0

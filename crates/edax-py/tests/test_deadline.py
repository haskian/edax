"""Backend deadlines preserve cancellation, including completion races on Python 3.10."""

from __future__ import annotations

import asyncio

import pytest
from edax._deadline import within
from edax.errors import StoreError


@pytest.mark.parametrize("fails", [False, True])
async def test_cancellation_wins_when_the_call_finishes_at_the_same_time(fails: bool) -> None:
    owner: asyncio.Task[int]

    async def call() -> int:
        owner.cancel()
        if fails:
            raise ValueError("completed with failure")
        return 42

    owner = asyncio.create_task(within(call(), 1, "backend call"))
    with pytest.raises(asyncio.CancelledError):
        await owner


@pytest.mark.parametrize("cancelled", [False, True])
async def test_timeout_and_cancellation_wait_for_call_cleanup(cancelled: bool) -> None:
    started, cleaned = asyncio.Event(), asyncio.Event()

    async def call() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            cleaned.set()

    owner = asyncio.create_task(within(call(), 1 if cancelled else 0.01, "backend call", after="retry later"))
    await started.wait()
    if cancelled:
        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
    else:
        with pytest.raises(StoreError, match="backend call: no reply within 10 ms; retry later"):
            await owner
    assert cleaned.is_set()


async def test_completed_call_keeps_its_result_or_failure() -> None:
    async def success() -> int:
        return 42

    async def failure() -> int:
        raise ValueError("client refusal")

    assert await within(success(), 1, "backend call") == 42
    with pytest.raises(ValueError, match="client refusal"):
        await within(failure(), 1, "backend call")


async def test_client_timeout_uses_the_same_store_error() -> None:
    async def call() -> None:
        raise asyncio.TimeoutError("client deadline")

    with pytest.raises(StoreError, match="backend call: no reply within 1000 ms"):
        await within(call(), 1, "backend call")


@pytest.mark.parametrize("fails", [False, True])
async def test_timeout_keeps_the_result_of_a_call_that_handles_cancellation(fails: bool) -> None:
    async def call() -> int:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if fails:
                raise ValueError("client cleanup failed") from None
            return 42
        raise AssertionError("the test never releases the call")

    if fails:
        with pytest.raises(ValueError, match="client cleanup failed"):
            await within(call(), 0.01, "backend call")
    else:
        assert await within(call(), 0.01, "backend call") == 42


async def test_caller_cancellation_wins_during_timeout_cleanup() -> None:
    cleaning = asyncio.Event()

    async def call() -> int:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleaning.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return 42
        raise AssertionError("the test never releases the call")

    owner = asyncio.create_task(within(call(), 0.01, "backend call"))
    await cleaning.wait()
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner


@pytest.mark.parametrize("timeout_s", [0, -1])
async def test_expired_deadline_never_starts_work_but_accepts_an_existing_result(timeout_s: float) -> None:
    started = False

    async def call() -> None:
        nonlocal started
        started = True

    with pytest.raises(StoreError):
        await within(call(), timeout_s, "backend call")
    assert not started
    completed: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    completed.set_result(42)
    assert await within(completed, timeout_s, "backend call") == 42

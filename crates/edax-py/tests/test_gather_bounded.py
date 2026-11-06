"""``gather_bounded``: results in order, at most ``limit`` calls at once, and the first failure in order raised
only after every call ended."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest
from edax._async import gather_bounded


@pytest.mark.parametrize(
    "failing, limit, want",
    [
        pytest.param(set(), 2, [0, 1, 2, 3, 4], id="every_result_in_order_within_the_limit"),
        pytest.param({3, 1}, 2, "failed 1", id="the_first_failure_in_order_is_raised_after_all_end"),
        pytest.param(set(), 8, [0, 1, 2, 3, 4], id="a_limit_above_the_calls_runs_them_all_at_once"),
    ],
)
async def test_gather_bounded(failing: set[int], limit: int, want: list[int] | str) -> None:
    running = peak = 0
    ended: list[int] = []

    def call(n: int) -> Callable[[], Awaitable[int]]:
        async def run() -> int:
            nonlocal running, peak
            running += 1
            peak = max(peak, running)
            await asyncio.sleep(0)
            running -= 1
            ended.append(n)
            if n in failing:
                raise ValueError(f"failed {n}")
            return n

        return run

    calls = [call(n) for n in range(5)]
    if isinstance(want, str):
        with pytest.raises(ValueError, match=want):
            await gather_bounded(calls, limit)
    else:
        assert await gather_bounded(calls, limit) == want
    assert sorted(ended) == [0, 1, 2, 3, 4], "every call ended"
    assert peak == min(limit, 5)

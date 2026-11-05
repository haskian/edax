"""Accept a host callback that is either sync or async."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Iterable
from typing import TypeAlias, TypeVar

T = TypeVar("T")

MaybeAwaitable: TypeAlias = T | Awaitable[T]


async def maybe_await(value: MaybeAwaitable[T]) -> T:
    """Return ``value``, awaiting it first if a callback returned an awaitable."""
    if inspect.isawaitable(value):
        return await value
    return value


async def gather_bounded(calls: Iterable[Callable[[], Awaitable[T]]], limit: int) -> list[T]:
    """Each call's result, in order, with at most ``limit`` running at once. Every call ends before the first
    failure in order is raised, so none is left running unobserved."""
    running = asyncio.Semaphore(limit)

    async def bounded(call: Callable[[], Awaitable[T]]) -> T:
        async with running:
            return await call()

    ended = await asyncio.gather(*(bounded(call) for call in calls), return_exceptions=True)
    results: list[T] = []
    for result in ended:
        if isinstance(result, BaseException):
            raise result
        results.append(result)
    return results

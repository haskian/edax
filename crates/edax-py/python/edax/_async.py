"""Accept a host callback that is either sync or async."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable
from typing import TypeVar, Union

T = TypeVar("T")

MaybeAwaitable = Union[T, Awaitable[T]]


async def maybe_await(value: MaybeAwaitable[T]) -> T:
    """Return ``value``, awaiting it first if a callback returned an awaitable."""
    if inspect.isawaitable(value):
        return await value
    return value

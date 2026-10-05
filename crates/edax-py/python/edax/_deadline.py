"""The one deadline every backend call runs under, and the one error it raises when the call misses it."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TypeVar

from .errors import StoreError

T = TypeVar("T")


async def within(call: Awaitable[T], timeout_s: float, what: str, *, after: str = "") -> T:
    """``call``'s result, or the transient :class:`~edax.errors.StoreError` once ``timeout_s`` passes, the
    call cancelled. ``what`` names the call in the message, and ``after`` says what a missed deadline leaves
    behind. One error type for every backend, so a host catches a timeout the same way on each, and on
    every Python version: ``asyncio.TimeoutError`` is the builtin ``TimeoutError`` only from 3.11."""
    try:
        return await asyncio.wait_for(call, timeout_s)
    except asyncio.TimeoutError as e:
        note = f"; {after}" if after else ""
        raise StoreError(f"{what}: no reply within {timeout_s * 1000:g} ms{note}") from e

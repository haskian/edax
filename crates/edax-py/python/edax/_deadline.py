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
    task = asyncio.ensure_future(call)
    try:
        # On Python 3.10, wait_for can swallow the caller's cancellation when the call finishes
        # concurrently. wait leaves cancellation intact; this helper owns cancelling the call.
        if timeout_s <= 0 and not task.done():
            raise asyncio.TimeoutError
        done, _ = await asyncio.wait({task}, timeout=timeout_s)
        if not done:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if task.cancelled():
                raise asyncio.TimeoutError
            # As with wait_for, a client that handles cancellation keeps its result or failure.
        return task.result()
    except asyncio.TimeoutError as e:
        note = f"; {after}" if after else ""
        raise StoreError(f"{what}: no reply within {timeout_s * 1000:g} ms{note}") from e
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        elif not task.cancelled():
            # A failure concurrent with caller cancellation must still be retrieved.
            task.exception()

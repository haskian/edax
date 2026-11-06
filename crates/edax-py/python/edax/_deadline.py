"""The one deadline every backend call runs under, and the one error it raises when the call misses it."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable
from typing import TypeVar

from .errors import StoreError

T = TypeVar("T")


async def within(call: Awaitable[T], timeout_s: float, what: str, *, after: str = "") -> T:
    """``call``'s result, or the transient :class:`~edax.errors.StoreError` once ``timeout_s`` passes, the
    call cancelled. ``what`` names the call in the message, and ``after`` says what a missed deadline leaves
    behind. One error type for every backend, so a host catches a timeout the same way on each, and on
    every Python version: ``asyncio.TimeoutError`` is the builtin ``TimeoutError`` only from 3.11."""
    if sys.version_info < (3, 11) or timeout_s <= 0:
        return await _within_task(call, timeout_s, what, after)
    # Awaited in place: a task per backend call costs more than a local round trip. A call that handles
    # its cancellation keeps its result or failure, as with wait_for, unless the caller was cancelled too:
    # its cancel request outlives the timeout's own, and wins.
    owner = asyncio.current_task()
    if owner is None:  # asyncio.timeout needs a task
        return await _within_task(call, timeout_s, what, after)
    pending = owner.cancelling()
    try:
        async with asyncio.timeout(timeout_s):
            result = await call
    except BaseException as e:
        if owner.cancelling() > pending and not isinstance(e, asyncio.CancelledError):
            raise asyncio.CancelledError from e
        if isinstance(e, TimeoutError):
            raise _missed(timeout_s, what, after) from e
        raise
    if owner.cancelling() > pending:
        raise asyncio.CancelledError
    return result


async def _within_task(call: Awaitable[T], timeout_s: float, what: str, after: str) -> T:
    """:func:`within` in its own task, for Python 3.10, which has no ``asyncio.timeout``, and for a deadline
    already passed, where the call must never start."""
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
        raise _missed(timeout_s, what, after) from e
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        elif not task.cancelled():
            # A failure concurrent with caller cancellation must still be retrieved.
            task.exception()


def _missed(timeout_s: float, what: str, after: str) -> StoreError:
    note = f"; {after}" if after else ""
    return StoreError(f"{what}: no reply within {timeout_s * 1000:g} ms{note}")

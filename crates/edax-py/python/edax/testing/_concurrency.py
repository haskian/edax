"""How the race, hold and isolation clauses run two units of work against each other, and how long they
wait."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Any, Literal, TypeVar

from .. import _native
from ..errors import ConcurrencyError
from ..unit_of_work import Tx

#: How long a clause waits for a delivery the contract says must come, or for a raced unit of work to
#: finish; also how long it waits to be sure a delivery that must not come does not.
POLL_S = 1.0


QUIET_S = 0.05


#: How long a race clause waits for the second unit of work's write before it decides the write is blocked
#: on the first one's row lock and commits the first. A non-blocking store answers at once, so the clause
#: never waits this long for one; only a store that parks the second writer does.
BLOCKED_S = 0.2


Outcome = Literal["landed", "conflict"]

T = TypeVar("T")


async def until_answered(answered: asyncio.Event, task: asyncio.Task[Any]) -> None:
    """Wait until ``task``, the other unit of work, sets ``answered`` or ends, or for :data:`BLOCKED_S` when
    the store parks it on a row lock: the clause then goes on, so the parked write cannot deadlock it."""
    waiting = asyncio.create_task(answered.wait())
    await asyncio.wait({waiting, task}, timeout=BLOCKED_S, return_when=asyncio.FIRST_COMPLETED)
    waiting.cancel()


async def race(
    unit_of_work: Callable[[], AbstractAsyncContextManager[Tx]],
    first: Callable[[Tx], Awaitable[object]],
    second: Callable[[Tx], Awaitable[object]],
) -> tuple[Outcome, Outcome]:
    """Run two units of work that each stage their write before either commits, and report how each ended.

    The order is forced: the first stages its write, then the second stages its own (in a task of its own,
    so a store that blocks it on the first one's row lock does not deadlock this coroutine), then the first
    commits, then the second does. A second write the store parks is given :data:`BLOCKED_S` to answer
    before the first commits anyway. :class:`~edax.errors.ConcurrencyError` from a write or a commit is a
    ``conflict``; any other error fails the clause."""
    second_staged = asyncio.Event()
    second_may_commit = asyncio.Event()

    async def run_second() -> Outcome:
        try:
            async with unit_of_work() as tx:
                await second(tx)
                second_staged.set()
                await second_may_commit.wait()
        except ConcurrencyError:
            return "conflict"
        finally:
            second_staged.set()
        return "landed"

    racer: asyncio.Task[Outcome] | None = None
    try:
        async with unit_of_work() as tx:
            await first(tx)
            racer = asyncio.create_task(run_second())
            await until_answered(second_staged, racer)
    except ConcurrencyError:
        first_outcome: Outcome = "conflict"
    else:
        first_outcome = "landed"
    second_may_commit.set()
    assert racer is not None, "the first unit of work failed before the second one started"
    return first_outcome, await asyncio.wait_for(racer, POLL_S)


class RolledBack(Exception):
    """A holder's way out of :func:`while_held`: a rollback, never a commit."""


#: What :func:`while_held` reports for the other unit of work when the store refused it.
REFUSED: Literal["refused"] = "refused"


async def while_held(
    unit_of_work: Callable[[], AbstractAsyncContextManager[Tx]],
    hold: Callable[[Tx], Awaitable[object]],
    other: Callable[[Tx], Awaitable[T]],
    refused: type[BaseException] | tuple[type[BaseException], ...] = (),
) -> tuple[T | Literal["refused"], bool, bool]:
    """Stage ``hold`` in one unit of work and keep it open; meanwhile run ``other`` in another. Return what
    ``other`` returned, or :data:`REFUSED` if it raised ``refused``; whether the holder was already rolling
    back when ``other`` itself returned; and whether the holder had ended once ``other``'s unit of work
    ended.

    The two flags differ for a store that answers at once and defers its check to the commit: its
    ``other`` returns while the holder is open, and its commit then waits for the holder to end. A clause
    about what ``other`` returns reads the first flag, a clause about what lands reads the second.

    ``other`` runs in a task of its own, so a store that blocks it on the holder's row lock does not
    deadlock this coroutine. The holder stays open until ``other`` answered, or for :data:`BLOCKED_S` if the
    store parks it, and then always rolls back. Once it has, ``other`` gets :data:`POLL_S` to answer."""
    holder_open, holder_leaving, holder_ended, other_done = (asyncio.Event() for _ in range(4))

    async def contend() -> tuple[T | Literal["refused"], bool, bool]:
        await holder_open.wait()
        got: T | Literal["refused"]
        answered_after_holder = False
        try:
            async with unit_of_work() as tx:
                try:
                    got = await other(tx)
                finally:
                    answered_after_holder = holder_leaving.is_set()
        except refused:
            got = REFUSED
        finally:
            other_done.set()
        return got, answered_after_holder, holder_ended.is_set()

    contender = asyncio.create_task(contend())
    try:
        async with unit_of_work() as tx:
            await hold(tx)
            holder_open.set()
            await until_answered(other_done, contender)
            # Set before the rollback, not after it: a store that blocks `other` on the holder's row lock
            # may answer it before the holder's own rollback call returns.
            holder_leaving.set()
            raise RolledBack
    except RolledBack:
        pass
    except BaseException:
        contender.cancel()
        raise
    finally:
        holder_ended.set()
    return await asyncio.wait_for(contender, POLL_S)


async def staged_then_read(
    unit_of_work: Callable[[], AbstractAsyncContextManager[Tx]],
    write: Callable[[Tx], Awaitable[object]],
    read: Callable[[Tx], Awaitable[T]],
) -> T | Literal["refused"]:
    """Stage ``write`` in one unit of work and, while it is open, ``read`` in another; return what the
    reader saw. A store whose reader blocks on the writer's lock answers once the writer rolls back."""
    seen, _, _ = await while_held(unit_of_work, write, read)
    return seen


def unique_id(prefix: str) -> str:
    """A fresh id per test, so a suite run against a durable store needs no truncation between tests, and
    two clauses racing on one store never share a key."""
    return f"{prefix}-{_native.guid()}"

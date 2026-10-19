"""The ambient units of work behind :mod:`edax.unit_of_work`: one open tx per database in this task, and the
SDK-internal moves on it, opening a unit of work over an already-guarded observer, asking whether any is
open, enlisting a store's call in one, and leaving every one when a task starts."""

from __future__ import annotations

import asyncio
import contextvars
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager, nullcontext
from dataclasses import dataclass, field
from time import perf_counter_ns
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, TypeVar

from .errors import BadHandle
from .observe import Observer, TransactionOutcome
from .sealing import (
    KeyScope,
    PendingFence,
    TransactionFence,
    detach_sealing_context,
    pending_fences,
    sealing_context,
)

if TYPE_CHECKING:
    from .unit_of_work import Transactor, TxT

#: A backend's own transaction handle type.
Handle = TypeVar("Handle")

# One open tx per database, keyed by the identity of the transactor that owns it (an `id`, since a
# transactor need not be hashable; the open `transaction` holds the object, so the id cannot be reused
# meanwhile). One slot for every database would hand a store of one database the tx of another as soon as
# two nest, which writes through the wrong connection. Each `transaction` sets a new mapping, never mutates.
AMBIENT: contextvars.ContextVar[Mapping[int, Any]] = contextvars.ContextVar(
    "edax_ambient_tx", default=MappingProxyType({})
)


@dataclass(eq=False)
class _Unit:
    """One open unit of work. ``resources`` outlive every participant and are released only after
    commit/rollback; ``task`` opened it; ``fenced`` holds the (provider, scope) pairs admitted."""

    resources: AsyncExitStack
    task: asyncio.Task[Any] | None
    fenced: set[tuple[int, KeyScope]] = field(default_factory=set)


# The open units of work in this task, keyed by database as AMBIENT is.
RESOURCES: contextvars.ContextVar[Mapping[int, _Unit]] = contextvars.ContextVar(
    "edax_transaction_resources", default=MappingProxyType({})
)


def _fences_in(keys: TransactionFence, database: int) -> bool:
    return id(database_of(keys.transactor)) == database


async def _admit(database: int, unit: _Unit, pending: Sequence[PendingFence], tx: object | None) -> None:
    """Fence ``pending`` for ``unit``: inside ``tx``, once it has begun, on the provider's database; with the
    provider's ``guard`` on its resources on any other database, since a lock taken in one database cannot
    hold back a commit in another. A task that did not open ``unit`` takes the guard too: a transaction's
    connection serves the task that opened it, one call at a time. Each provider fences all its scopes in
    one call, so it takes their locks in one order."""
    owned = unit.task is asyncio.current_task()
    batches: dict[int, tuple[TransactionFence, dict[KeyScope, None]]] = {}
    for fence in pending:
        admitted = (id(fence.keys), fence.scope)
        if admitted in unit.fenced:
            continue
        if not owned or not _fences_in(fence.keys, database):
            await unit.resources.enter_async_context(fence.keys.guard(fence.scope))
            unit.fenced.add(admitted)
        elif tx is not None:
            batches.setdefault(id(fence.keys), (fence.keys, {}))[1][fence.scope] = None
    for keys, scopes in batches.values():
        await keys.fence(tx, list(scopes))
        unit.fenced.update((id(keys), scope) for scope in scopes)


@asynccontextmanager
async def _begun(
    transactor: Transactor[TxT],
    database: int,
    unit: _Unit,
    opened: AbstractAsyncContextManager[TxT] | None = None,
) -> AsyncIterator[TxT]:
    """Begin ``transactor``'s transaction for ``unit`` with this task's pending fences: a guard on another
    database is held from before ``begin``, as an eager write guard is; a fence on the provider's database
    goes inside, before the caller's first statement."""
    pending = pending_fences()
    if pending:
        await _admit(database, unit, pending, None)
    async with transactor.begin() if opened is None else opened as tx:
        if pending:
            await _admit(database, unit, pending, tx)
        yield tx


@asynccontextmanager
async def _fenced(fence: PendingFence) -> AsyncIterator[None]:
    """Enter ``fence`` and fence every unit of work already open in this task now. A transaction that opens
    while it is pending takes it as it begins: a unit of work's, or a store's own (:func:`own_transaction`,
    :func:`own_write`)."""
    for database, unit in RESOURCES.get().items():
        await _admit(database, unit, [fence], AMBIENT.get().get(database))
    async with fence:
        yield


@asynccontextmanager
async def _own_transaction(
    transactor: Transactor[TxT], opened: AbstractAsyncContextManager[TxT]
) -> AsyncIterator[TxT]:
    async with AsyncExitStack() as resources:
        unit = _Unit(resources, asyncio.current_task())
        async with _begun(transactor, id(database_of(transactor)), unit, opened) as tx:
            yield tx


def own_transaction(
    transactor: Transactor[TxT], *, opened: AbstractAsyncContextManager[TxT] | None = None
) -> AbstractAsyncContextManager[TxT]:
    """A store's own short transaction, outside any unit of work: it takes this task's pending fences as a
    unit of work does, so a write a handler makes through a store alone is fenced too. ``opened`` lets an
    adapter retain its own commit/rollback evidence while using the same fence admission."""
    opened = transactor.begin() if opened is None else opened
    return _own_transaction(transactor, opened) if pending_fences() else opened


@asynccontextmanager
async def _guards(pending: Sequence[PendingFence]) -> AsyncIterator[None]:
    async with AsyncExitStack() as guards:
        for fence in pending:
            await guards.enter_async_context(fence.keys.guard(fence.scope))
        yield


def own_write() -> AbstractAsyncContextManager[None]:
    """A store write that lands on its own, with no transaction to fence in: hold each pending fence's guard
    through it."""
    pending = pending_fences()
    return _guards(pending) if pending else nullcontext()


@asynccontextmanager
async def _retained(unit: _Unit | None, guard: AbstractAsyncContextManager[None]) -> AsyncIterator[None]:
    """Hold ``guard`` through the commit of ``unit``, else around the block; a pending fence is admitted by
    every transaction it meets instead."""
    if isinstance(guard, PendingFence):
        async with _fenced(guard):
            yield
    elif unit is not None:
        await unit.resources.enter_async_context(guard)
        yield
    else:
        async with guard:
            yield


@asynccontextmanager
async def transaction_guard(
    transactor: object, guard: AbstractAsyncContextManager[None], tx: object | None = None
) -> AsyncIterator[None]:
    """Hold ``guard`` around a write through ``transactor``'s stores, through the commit of the unit of work
    that holds it."""
    database = id(database_of(transactor))
    unit = RESOURCES.get().get(database)
    if unit is None and tx is not None:
        # A borrowed transaction no unit of work holds: only a fence inside it lasts until it ends.
        if not isinstance(guard, PendingFence) or not _fences_in(guard.keys, database):
            raise BadHandle(
                "protected writes with an explicit transaction require transaction() to retain the scope "
                "fence through commit, unless the key provider fences inside that transaction"
            )
        await guard.keys.fence(tx, [guard.scope])
    async with _retained(unit, guard):
        yield


def database_of(transactor: object) -> object:
    """The transactor that owns ``transactor``'s units of work: its ``transactor`` attribute, else itself
    when it has none or it is ``None``. The ambient is keyed by this, so a store and the transactor it
    delegates to share one key. One hop only, as the :class:`~edax.unit_of_work.Transactor` *Identity*
    clause says: the attribute names the owner itself."""
    owner = getattr(transactor, "transactor", None)
    return transactor if owner is None else owner


def ambient_tx(transactor: object) -> Any:
    """The tx of the open unit of work over ``transactor``'s database in this task, else ``None``."""
    return AMBIENT.get().get(id(database_of(transactor)))


def enlisted(transactor: object, tx: object | None, handle: type[Handle], kind: str) -> Handle | None:
    """The handle a store of ``transactor`` runs a call under: ``tx``, else the ambient unit of work of its
    database, else ``None`` for a call outside any. A ``tx`` that is not a ``handle`` of this very transactor,
    whose class ``kind`` names, raises :class:`~edax.errors.BadHandle`: a store cannot enlist in a unit of
    work its database knows nothing about, and going on would silently drop the atomicity the caller asked
    for. ``BadHandle``, not ``StoreError``: the same handle fails the same way on every retry, so a transient
    verdict would have a host redeliver a wiring mistake forever."""
    if tx is None:
        tx = ambient_tx(transactor)
    if tx is None:
        return None
    if isinstance(tx, handle) and getattr(tx, "transactor", None) is transactor:
        return tx
    raise BadHandle(
        f"tx {tx!r} is not a transaction of this {kind}: build every store that must commit together over "
        f"one {kind}"
    )


def in_unit_of_work() -> bool:
    """Whether any :func:`~edax.unit_of_work.transaction` is open in this task, over any database."""
    return bool(AMBIENT.get())


def leave_every_unit_of_work() -> None:
    """Start this task with no ambient tx. A task copies its creator's context, so a long-lived task started
    inside :func:`~edax.unit_of_work.transaction` would otherwise keep using that tx from a second task."""
    AMBIENT.set({})
    RESOURCES.set({})
    detach_sealing_context()


@asynccontextmanager
async def open_transaction(transactor: Transactor[TxT], observe: Observer) -> AsyncIterator[TxT]:
    """:func:`~edax.unit_of_work.transaction` over an already-guarded observer, for the orchestration objects
    that hold one."""
    database = id(database_of(transactor))
    open_now = AMBIENT.get()
    if database in open_now:
        # The Transactor contract says the SDK never nests. A second unit of work on the same database
        # commits on its own clean exit, and the outer rollback cannot take those writes back. BadHandle,
        # not StoreError: the wiring mistake is fatal, so a dispatcher stops on it and hands the message
        # back uncounted, and a guard releases its claim, so the message runs once the wiring is fixed.
        # The check is by transactor identity: two transactors over one pool that do not name a shared
        # `transactor` are two objects, so this catches composition of service functions, not every way
        # to reach one database twice.
        raise BadHandle(
            "transaction() was opened while this transactor already has one open: the inner unit of work "
            "would commit independently of the outer one. Pass the ambient tx down, or open the "
            "transaction around both callers, not in each"
        )
    started = perf_counter_ns()
    outcome: TransactionOutcome = "rolled_back"
    try:
        async with AsyncExitStack() as resources:
            resources.enter_context(sealing_context())
            unit = _Unit(resources, asyncio.current_task())
            resource_token = RESOURCES.set({**RESOURCES.get(), database: unit})
            try:
                async with _begun(transactor, database, unit) as tx:
                    token = AMBIENT.set({**open_now, database: tx})
                    try:
                        yield tx
                    finally:
                        AMBIENT.reset(token)
                outcome = "committed"
            finally:
                RESOURCES.reset(resource_token)
    finally:
        observe.transaction(outcome, perf_counter_ns() - started)


@asynccontextmanager
async def current_transaction_guard(guard: AbstractAsyncContextManager[None]) -> AsyncIterator[None]:
    """Retain a dispatch guard through the outermost managed transaction, if any."""
    async with _retained(next(iter(RESOURCES.get().values()), None), guard):
        yield

"""The ambient units of work behind :mod:`edax.unit_of_work`: one open tx per database in this task, and the
SDK-internal moves on it, opening a unit of work over an already-guarded observer, asking whether any is
open, enlisting a store's call in one, and leaving every one when a task starts."""

from __future__ import annotations

import contextvars
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from time import perf_counter_ns
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, TypeVar

from .errors import BadHandle
from .observe import Observer, TransactionOutcome

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
        async with transactor.begin() as tx:
            token = AMBIENT.set({**open_now, database: tx})
            try:
                yield tx
            finally:
                AMBIENT.reset(token)
        outcome = "committed"
    finally:
        observe.transaction(outcome, perf_counter_ns() - started)

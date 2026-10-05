"""The unit of work: a :class:`Transactor` owns the transaction boundary, :func:`transaction` opens one,
and :func:`current_tx` is the ambient tx every participant store of that database joins."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol, TypeVar

from ._observer import wrap
from ._units import AMBIENT, database_of, open_transaction
from .observe import Observer

__all__ = ["Transactor", "Tx", "TxT", "TxT_co", "TxT_contra", "current_tx", "transaction"]


#: An opaque backend transaction handle (a connection, a transaction) where nothing ties it to a port: the
#: ambient :func:`current_tx`, which one task may hold for several databases, and a seam's own fakes.
Tx = Any

#: The handle type one backend's ports share. Each port is generic in it, so a checker proves that the
#: stores enlisted in one unit of work take the handle its :class:`Transactor` yields.
TxT = TypeVar("TxT")
#: :data:`TxT` as a port that only yields the handle declares it: a :class:`Transactor`.
TxT_co = TypeVar("TxT_co", covariant=True)
#: :data:`TxT` as a port that only takes the handle declares it: every participant store.
TxT_contra = TypeVar("TxT_contra", contravariant=True)


class Transactor(Protocol[TxT_co]):
    """Owns the transaction boundary: begin a unit of work, commit it, or roll it back. The stores that take
    a ``tx`` (:class:`~edax.store.EventStore`, :class:`~edax.projection.ProjectionStore`,
    :class:`~edax.idempotency.IdempotencyStore`) are pure participants, so any number of them commit together.
    Every one is generic in the handle type ``TxT``, as this is. What a backend must guarantee:

    - ``begin()`` returns an async context manager: a clean exit commits, an exception rolls back and
      re-raises, and nothing written through the yielded ``tx`` is visible elsewhere before commit.
    - ``begin()`` is the backend's hook, which :func:`transaction` calls. A host opens every unit of work
      with :func:`transaction`. A raw ``begin()`` publishes no :func:`current_tx`, so a participant given no
      tx inside it would commit on its own, and the nesting guard below would not see it.
    - The yielded ``tx`` is one backend handle every participant store accepts, atomic as a set.
    - A participant handed the same ``tx`` reads its own uncommitted writes.
    - The SDK never nests: stores join the ambient :func:`current_tx` of their own transactor rather than
      open a second unit of work, so no savepoints are needed. A second :func:`transaction` on the same
      transactor raises :class:`~edax.errors.BadHandle` rather than opening one; a transactor over another
      database nests, and each store still sees only its own database's tx.
    - *Identity.* The ambient tx is keyed by transactor. An object whose ``begin()`` only delegates to
      another transactor (an :class:`~edax.store.EventStore` over a shared pool, say) names that transactor as
      its ``transactor`` attribute, so ``transaction(store)`` and ``transaction(store.transactor)`` open the
      same unit of work and every store over the pool finds it. The attribute names the owner itself: the
      SDK follows it one hop, never a chain.
    - The handle is used from one asyncio task at a time; no thread safety required.

    :class:`~edax.backend.memory.MemoryTransactor` implements it, so a test can assert a raise left
    nothing behind."""

    def begin(self) -> AbstractAsyncContextManager[TxT_co]:
        """The backend's unit of work: commit on clean exit, roll back if the body raises. Open it through
        :func:`transaction`, never directly."""
        ...


def current_tx(transactor: object) -> Tx | None:
    """The tx of the open :func:`transaction` over ``transactor``'s database, else ``None``. A store handed
    ``tx=None`` reads this with its own transactor, so it never sees another database's tx."""
    return AMBIENT.get().get(id(database_of(transactor)))


def transaction(
    transactor: Transactor[TxT], *, observer: Observer | None = None
) -> AbstractAsyncContextManager[TxT]:
    """Run a unit of work over ``transactor``: yield the tx, commit on clean exit, roll back if the body
    raises. The tx is published to :func:`current_tx` for ``transactor``'s database for the duration, so
    participants of that database given no explicit tx join it. A task created inside the block keeps the
    tx after the block exits, which the :class:`Transactor` contract forbids: start long-lived tasks outside
    a unit of work.

    Opening this while the same transactor already has a unit of work open raises
    :class:`~edax.errors.BadHandle`: the inner one would commit on its own clean exit and the outer
    rollback could not take it back. Two service functions that each open a unit of work compose by
    passing the tx down, or by moving the :func:`transaction` out to their caller. A second transactor,
    over another database, nests: each database's stores join that database's tx, and neither commit is
    atomic with the other's.

    ``observer`` receives one :meth:`~edax.observe.Observer.transaction` per unit of work, ``committed`` or
    ``rolled_back``, with its duration; ``None`` is the no-op observer."""
    return open_transaction(transactor, wrap(observer))

"""Read-model persistence seam. The event-sourced side (:mod:`edax.store`) replays events to rebuild an
aggregate; a read model is the other shape, a current document saved with an optimistic version and queried
directly. A backend serves :meth:`ProjectionStore.get_many` by pushing the kernel's filter, sort and
pagination down to native SQL, so two services page it identically. See ``docs/sdk/projections.md``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from .errors import ConcurrencyError
from .filter import Filter
from .query import Page, SortKey
from .store import Tx

__all__ = ["ConcurrencyError", "Entity", "ProjectionStore"]


@dataclass
class Entity:
    """A read model at a point in time: the queryable ``document`` and the ``version`` it was loaded at
    (``0`` if never saved). A save persists at ``version + 1``, then advances ``version`` in place."""

    id: str
    version: int
    document: Any


class ProjectionStore(Protocol):
    """Stores read-model documents keyed by id, with optimistic concurrency and kernel-defined querying: the
    read side's analogue of :class:`~edax.store.EventStore`. Pass the ``tx`` from
    :func:`~edax.store.transaction` to commit a projection write with the events that produced it. What a
    backend must guarantee:

    - Nothing in the SDK calls this store, only your handlers, so implement what you use.
    - *Optimistic concurrency.* ``save`` writes at ``version + 1`` only if the stored version still equals
      ``entity.version`` (``0`` means not yet stored), else it raises :class:`~edax.errors.ConcurrencyError`
      and writes nothing. Enforce it in the database. On success set ``entity.version`` to the new one.
    - *Enlistment.* ``get`` and ``save`` run inside ``tx``, or :func:`~edax.store.current_tx`, else
      auto-commit.
    - *Reads.* ``get`` returns the current committed row plus the ``tx``'s own writes, or ``None``.
    - *Queries (optional).* A conforming ``get_many`` pages identically to the kernel: the filter operators'
      semantics, the decimal collation, the fixed total order over JSON types with stable multi-key sort,
      keyset resume strictly after the cursor tuple, and a :class:`~edax.query.Page` whose ``next`` is minted
      with :func:`~edax.query.encode_cursor` from the last row when more rows follow. Read ``after`` with
      :func:`~edax.query.decode_cursor`, which refuses a cursor minted under another sort. Documents need a
      unique ``"id"`` field. A backend may instead treat the spec as its own dialect, and then cursors are not
      portable across services.
    - *Errors.* :class:`~edax.errors.ConcurrencyError` only for a version mismatch, else ``StoreError``.
    """

    async def get(self, id: str, tx: Tx | None = None) -> Entity | None:
        """The current entity, or ``None`` if absent."""
        ...

    async def save(self, entity: Entity, tx: Tx | None = None) -> None:
        """Persist at ``entity.version + 1`` if the stored version still equals ``entity.version``, else
        raise :class:`~edax.errors.ConcurrencyError`. On success ``entity.version`` advances."""
        ...

    async def get_many(
        self,
        *,
        filter: Filter | None = None,
        sort: list[SortKey] | None = None,
        offset: int = 0,
        limit: int | None = None,
        after: str | None = None,
        tx: Tx | None = None,
    ) -> Page:
        """The page selected by filter -> keyset(``after``) -> sort -> offset -> limit, with the cursor
        that resumes after it."""
        ...

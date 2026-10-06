"""Read-model persistence seam. The event-sourced side (:mod:`edax.store`) replays events to rebuild an
aggregate; a read model is the other shape, a current document saved with an optimistic version and queried
directly: a versioned document store, not a projector. It records no folded events, so a handler skips a
redelivered one with :func:`~edax.router.idempotent_by_id`. A :class:`ProjectionQuery` backend serves
:meth:`~ProjectionQuery.get_many` by pushing the kernel's filter, sort and pagination down to native SQL, so
two services page it identically. See ``docs/sdk/projections.md``."""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from . import _json, _native
from .query import Document, Page, Query
from .unit_of_work import TxT_contra

__all__ = [
    "Entity",
    "ProjectionQuery",
    "ProjectionStore",
    "check_document",
    "keyed_document",
]


def check_document(id: str, document: Any) -> None:
    """Refuse ``document`` as the stored document of projection ``id`` exactly as the kernel's stores do:
    one that is not a JSON object (the query reads each row's key from its ``"id"`` field, so a scalar or a
    list could be saved and never found), or one holding an integer wider than 64 bits (stored as the
    nearest float, it would come back changed). Raises the terminal :class:`~edax.errors.CodecError`. Call
    it in your :meth:`ProjectionStore.save` before the write. Pinned by
    ``testdata/golden/projection_document.json``."""
    _json.call(functools.partial(_native.check_projection_document, id), document)


def keyed_document(id: str, document: Any) -> dict[str, Any]:
    """``document``, the stored document of projection ``id``, as :meth:`ProjectionStore.get` and
    :meth:`ProjectionQuery.get_many` return it: with the row key as its ``"id"`` field, overwriting any the
    document carries. The query sorts its keyset tiebreak on that field and mints every cursor from it. A
    document :func:`check_document` refuses raises the same terminal :class:`~edax.errors.CodecError`. Call
    it in your ``get``, or inject the key in your ``get_many`` query. Pinned by
    ``testdata/golden/projection_document.json``."""
    keyed: Document = _json.decode(
        _json.call(functools.partial(_native.keyed_projection_document, id), document)
    )
    return keyed


@dataclass(frozen=True)
class Entity:
    """A read model at a point in time: the queryable ``document`` and the ``version`` it was loaded at
    (``0`` if never saved). A value: :meth:`ProjectionStore.save` persists it at ``version + 1`` and returns
    that new version, leaving this one as it was."""

    id: str
    version: int
    document: Document


class ProjectionStore(Protocol[TxT_contra]):
    """Stores read-model documents keyed by id, with optimistic concurrency: the read side's analogue of
    :class:`~edax.store.EventStore`. A store that also pages them by the kernel's query is a
    :class:`ProjectionQuery`. Pass the ``tx`` from :func:`~edax.unit_of_work.transaction` to commit a
    projection write with the events that produced it. What a backend must guarantee:

    - Nothing in the SDK calls this store, only your handlers, so implement what you use.
    - *Documents.* ``save`` refuses a document :func:`check_document` refuses, with
      :class:`~edax.errors.CodecError`, and writes nothing.
    - *Optimistic concurrency.* ``save`` writes at ``version + 1`` only if the stored version still equals
      ``entity.version`` (``0`` means not yet stored), and returns that new version; else it raises
      :class:`~edax.errors.ConcurrencyError` and writes nothing. Enforce it in the database.
    - *Enlistment.* ``get`` and ``save`` run inside ``tx``, or :func:`~edax.unit_of_work.current_tx`, else
      auto-commit.
    - *Reads.* ``get`` returns the current committed row plus the ``tx``'s own writes, or ``None``. Its
      document carries the row key as ``"id"``, overwriting any the saved document held
      (:func:`keyed_document`).
    - *Errors.* :class:`~edax.errors.ConcurrencyError` only for a version mismatch,
      :class:`~edax.errors.CodecError` for a refused document, else ``StoreError``.

    ``ProjectionStoreContract`` in :mod:`edax.testing` holds a backend to every clause.
    """

    async def get(self, id: str, tx: TxT_contra | None = None) -> Entity | None:
        """The current entity, or ``None`` if absent."""
        ...

    async def save(self, entity: Entity, tx: TxT_contra | None = None) -> int:
        """Persist at ``entity.version + 1`` if the stored version still equals ``entity.version``, and return
        that new version; else raise :class:`~edax.errors.ConcurrencyError`."""
        ...


@runtime_checkable
class ProjectionQuery(Protocol[TxT_contra]):
    """The query side of a :class:`ProjectionStore`, which a store implements beside it to page its
    documents by the kernel's query, so two services page them identically. What a backend must guarantee:

    - *Paging.* ``get_many`` pages identically to the kernel: the filter operators' semantics, the decimal
      collation, the fixed total order over JSON types with stable multi-key sort, keyset resume strictly
      after the cursor tuple, and a :class:`~edax.query.Page` whose ``next`` is minted with
      :func:`~edax.query.encode_cursor` from the last row when more rows follow. Read ``after`` with
      :func:`~edax.query.decode_cursor`, which refuses a cursor minted under another sort.
    - *Rows.* Each row is a document as ``get`` reads it, its row key as ``"id"`` (:func:`keyed_document`).
    - *Specs.* :meth:`~edax.query.Query.to_json` gives the kernel's spec. ``get_many`` refuses a spec
      :func:`~edax.query.check_query` refuses, with :class:`~edax.errors.CodecError`, and reads nothing.

    A backend that treats the spec as its own dialect is no ``ProjectionQuery``: its cursors are not
    portable across services. ``ProjectionStoreContract`` runs the paging clauses for a store that is one.
    """

    async def get_many(self, query: Query = Query(), *, tx: TxT_contra | None = None) -> Page:
        """The page ``query`` selects (filter -> keyset(``after``) -> sort -> offset -> limit), with the
        cursor that resumes after it."""
        ...

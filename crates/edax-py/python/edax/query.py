"""Read-side query: order and paginate a set of documents with kernel-decided semantics.

The total value order, sort stability, and cursor encoding all come from the kernel, so two
services paging the same read model agree on row order and cursor boundaries. Backends translate
the same query spec into their native ORDER BY / LIMIT / keyset clauses.
"""

from __future__ import annotations

import json
from typing import Any, Optional

from . import _edax, errors
from .filter import Filter

SortKey = dict[str, Any]  # one ordering key: {"path": ..., "desc": ...}
Document = dict[str, Any]  # a read-model document, as it crosses the JSON boundary
QuerySpec = dict[str, Any]  # the kernel query spec: filter / sort / offset / limit / after


def sort_key(path: str, desc: bool = False) -> SortKey:
    """One ordering key (ascending unless ``desc``)."""
    return {"path": path, "desc": desc}


def build_spec(
    *,
    filter: Optional[Filter] = None,
    sort: Optional[list[SortKey]] = None,
    offset: int = 0,
    limit: Optional[int] = None,
    after: Optional[str] = None,
) -> QuerySpec:
    """Assemble the kernel query spec with only the set fields. Shared by :func:`query` and
    :meth:`~edax.projection.ProjectionStore.get_many` so the two build it one way."""
    spec: QuerySpec = {}
    if filter is not None:
        spec["filter"] = filter
    if sort:
        spec["sort"] = sort
    if offset:
        spec["offset"] = offset
    if limit is not None:
        spec["limit"] = limit
    if after:
        spec["after"] = after
    return spec


def query(
    documents: list[Document],
    *,
    filter: Optional[Filter] = None,
    sort: Optional[list[SortKey]] = None,
    offset: int = 0,
    limit: Optional[int] = None,
    after: Optional[str] = None,
) -> list[Document]:
    """Return the page of ``documents`` selected by filter -> keyset(after) -> sort -> offset -> limit.

    ``after`` is a keyset cursor from a prior page (see :func:`encode_cursor`) and requires ``sort``.
    """
    spec = build_spec(filter=filter, sort=sort, offset=offset, limit=limit, after=after)
    with errors.mapped():
        return json.loads(_edax.query(json.dumps(spec), json.dumps(documents)))


def encode_cursor(document: Document, sort: list[SortKey]) -> str:
    """Mint the keyset cursor for the last document of a page under ``sort``. Portable across bindings."""
    with errors.mapped():
        return _edax.encode_cursor(json.dumps(document), json.dumps(sort))


def decode_cursor(cursor: str, sort: list[SortKey]) -> list[Any]:
    """Read a keyset cursor back into the value tuple it carries, one value per effective sort key.

    The inverse of :func:`encode_cursor`, and what a backend binds into its own keyset ``WHERE``
    clause instead of re-deriving the base64/JSON wire format from prose. Pass the query's declared
    ``sort``; the ``id`` tiebreak is applied here as it is everywhere else, so the tuple lines up with
    :func:`effective_sort`.

    A tuple that does not match the sort's arity is padded with ``None`` or truncated rather than
    rejected, because that is what the reference applier compares it as. A cursor minted under a
    different sort therefore reads the same way here and there.
    """
    with errors.mapped():
        return json.loads(_edax.decode_cursor(cursor, json.dumps(sort)))


def effective_sort(sort: list[SortKey]) -> list[SortKey]:
    """The keys a page is actually ordered by: ``sort`` with the unique ``id`` tiebreak appended.

    A backend that orders by the declared keys alone disagrees with the kernel at every tie, and
    keyset paging then drops or repeats tied rows. Empty in, empty out: an unsorted query keeps input
    order and takes no cursor.
    """
    with errors.mapped():
        return json.loads(_edax.effective_sort(json.dumps(sort)))

"""Read-side query: order and paginate a set of documents with kernel-decided semantics.

The total value order, sort stability, and cursor encoding all come from the kernel, so two
services paging the same read model agree on row order and cursor boundaries. Backends translate
the same query spec into their native ORDER BY / LIMIT / keyset clauses.

A query answers with a :class:`Page`: the rows and ``next``, the cursor the kernel minted from the last
row under the query's own sort. Pass ``next`` back as ``after`` with the same sort to read the next page;
a cursor replayed under another sort is a :class:`~edax.errors.CodecError`, never a silently wrong page.
The spec is read strictly: an unknown field, a ``None`` comparand, a path with an empty segment or an
integer wider than 64 bits is a :class:`~edax.errors.CodecError` too.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict

from . import _edax, _json, errors
from .filter import Filter


class _SortPath(TypedDict):
    path: str


class SortKey(_SortPath, total=False):
    """One ordering key: the dotted ``path`` to sort by, ascending unless ``desc``. A typed dictionary, so a
    checker catches a misspelt field (``{"dsc": True}``) that the kernel would refuse at run time."""

    desc: bool


Document = dict[str, Any]  # a read-model document, as it crosses the JSON boundary
QuerySpec = dict[str, Any]  # the kernel query spec: filter / sort / offset / limit / after


@dataclass(frozen=True)
class Page:
    """One page of a query. ``rows`` are the documents in the query's order. ``next`` is the cursor that
    resumes after them: pass it back as ``after`` with the same ``sort``. It is ``None`` when no row
    follows the page, and always for an unsorted query, which has no keyset and pages by ``offset``."""

    rows: list[Document]
    next: str | None

    @classmethod
    def from_kernel(cls, text: str) -> Page:
        """The kernel's ``{"rows": [...], "next": cursor|null}`` as a page."""
        page = _json.decode(text)
        return cls(rows=page["rows"], next=page["next"])


def sort_key(path: str, desc: bool = False) -> SortKey:
    """One ordering key (ascending unless ``desc``)."""
    return {"path": path, "desc": desc}


def build_spec(
    *,
    filter: Filter | None = None,
    sort: list[SortKey] | None = None,
    offset: int = 0,
    limit: int | None = None,
    after: str | None = None,
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
    if after is not None:
        spec["after"] = after
    return spec


def query(
    documents: list[Document],
    *,
    filter: Filter | None = None,
    sort: list[SortKey] | None = None,
    offset: int = 0,
    limit: int | None = None,
    after: str | None = None,
) -> Page:
    """Return the :class:`Page` of ``documents`` selected by filter -> keyset(after) -> sort -> offset ->
    limit. ``after`` is a prior page's ``next`` under the same ``sort``."""
    spec = build_spec(filter=filter, sort=sort, offset=offset, limit=limit, after=after)
    with errors.mapped():
        return Page.from_kernel(_edax.query(_json.encode(spec), _json.encode(documents)))


def encode_cursor(document: Document, sort: list[SortKey]) -> str:
    """Mint the keyset cursor that resumes after ``document`` under ``sort``. Portable across bindings.

    :func:`query` already returns it as ``Page.next``; this is for a backend that pages natively and mints
    its own. The document needs a string or number ``"id"``, the kernel's tiebreak, else a
    :class:`~edax.errors.CodecError`: rows tied on every other key would tie on it too, and paging past
    them would drop the rest of the tie."""
    with errors.mapped():
        return _edax.encode_cursor(_json.encode(document), _json.encode(sort))


def decode_cursor(cursor: str, sort: list[SortKey]) -> list[Any]:
    """Read a keyset cursor back into the value tuple it carries, one value per effective sort key.

    The inverse of :func:`encode_cursor`, and what a backend binds into its own keyset ``WHERE``
    clause instead of re-deriving the wire format from prose. Pass the query's declared ``sort``; the
    ``id`` tiebreak is applied here as it is everywhere else, so the tuple lines up with
    :func:`effective_sort`. A cursor minted under any other sort is a :class:`~edax.errors.CodecError`.
    """
    with errors.mapped():
        values: list[Any] = _json.decode(_edax.decode_cursor(cursor, _json.encode(sort)))
    return values


def effective_sort(sort: list[SortKey]) -> list[SortKey]:
    """The keys a page is actually ordered by: ``sort`` with the unique ``id`` tiebreak appended.

    A backend that orders by the declared keys alone disagrees with the kernel at every tie, and
    keyset paging then drops or repeats tied rows. Empty in, empty out: an unsorted query keeps input
    order and takes no cursor.
    """
    with errors.mapped():
        keys: list[SortKey] = _json.decode(_edax.effective_sort(_json.encode(sort)))
    return keys

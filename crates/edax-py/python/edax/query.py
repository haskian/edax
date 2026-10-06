"""Read-side query: order and paginate a set of documents with kernel-decided semantics.

The total value order, sort stability, and cursor encoding all come from the kernel, so two
services paging the same read model agree on row order and cursor boundaries. Backends translate
the same query spec into their native ORDER BY / LIMIT / keyset clauses.

A :class:`Query` names what to read, and the answer is a :class:`Page`: the rows and ``next``, the cursor
the kernel minted from the last row under the query's own sort. Pass ``next`` back as ``after`` with the
same sort to read the next page;
a cursor replayed under another sort is a :class:`~edax.errors.CodecError`, never a silently wrong page.
The spec is read strictly: an unknown field, a ``None`` comparand, a path with an empty segment or an
integer wider than 64 bits is a :class:`~edax.errors.CodecError` too.
"""

from __future__ import annotations

import functools
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypedDict

from . import _json, _native
from .filter import Filter

__all__ = [
    "Document",
    "Page",
    "Query",
    "SortKey",
    "check_query",
    "decode_cursor",
    "effective_sort",
    "encode_cursor",
    "query",
    "sort_key",
]


class _SortPath(TypedDict):
    path: str


class SortKey(_SortPath, total=False):
    """One ordering key: the dotted ``path`` to sort by, ascending unless ``desc``. A typed dictionary, so a
    checker catches a misspelt field (``{"dsc": True}``) that the kernel would refuse at run time.
    ``collation: "decimal"`` orders decimal text by exact value, as a ``Decimal`` encodes."""

    desc: bool
    collation: Literal["decimal"]


Document = dict[str, Any]  # a read-model document, as it crosses the JSON boundary


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


def sort_key(path: str, desc: bool = False, *, decimal: bool = False) -> SortKey:
    """One ordering key (ascending unless ``desc``). Set ``decimal`` for a field that holds ``Decimal``
    values: their text then sorts by exact value (``"9.5"`` before ``"10"``), and other text after it."""
    key: SortKey = {"path": path, "desc": desc}
    if decimal:
        key["collation"] = "decimal"
    return key


@dataclass(frozen=True)
class Query:
    """What a read selects: filter -> keyset(``after``) -> sort -> offset -> limit, the kernel's one query
    shape (``edax_cqrs::Query``). The default selects every document in input order. ``after`` is a prior
    page's ``next`` under the same ``sort``; ``limit=0`` is an empty page, not "no limit"."""

    filter: Filter | None = None
    sort: Sequence[SortKey] = ()
    offset: int = 0
    limit: int | None = None
    after: str | None = None

    def to_json(self) -> str:
        """The kernel query spec as JSON text: :meth:`spec`, encoded."""
        return _json.encode(self.spec())

    def spec(self) -> dict[str, Any]:
        """The kernel query spec, with only the set fields. Rust's ``Query::default()`` serializes to ``{}``
        too, so a backend gets one document from every binding. ``limit`` and ``after`` are tested against
        ``None``, not truthiness: an empty ``after`` (from ``?after=``) reaches the kernel, which refuses it
        as it does from every binding."""
        spec: dict[str, Any] = {}
        if self.filter is not None:
            spec["filter"] = self.filter
        if self.sort:
            spec["sort"] = list(self.sort)
        if self.offset:
            spec["offset"] = self.offset
        if self.limit is not None:
            spec["limit"] = self.limit
        if self.after is not None:
            spec["after"] = self.after
        return spec


def query(documents: list[Document], query: Query = Query()) -> Page:
    """Return the :class:`Page` of ``documents`` that ``query`` selects."""
    return Page.from_kernel(_json.call(_native.query, query.spec(), documents))


def check_query(query: Query) -> None:
    """Refuse the spec of ``query`` as :func:`query` refuses it, with :class:`~edax.errors.CodecError`: an
    unknown field, a ``None`` comparand, a path with an empty segment, an integer wider than 64 bits, or an
    ``after`` cursor on an unsorted query. The cursor itself is read by :func:`decode_cursor`. For a backend
    that answers the query itself: run it in your :meth:`~edax.projection.ProjectionQuery.get_many` before
    you read."""
    _json.call(_native.check_query, query.spec())


def encode_cursor(document: Document, sort: list[SortKey]) -> str:
    """Mint the keyset cursor that resumes after ``document`` under ``sort``. Portable across bindings.

    :func:`query` already returns it as ``Page.next``; this is for a backend that pages natively and mints
    its own. The document needs a string or number ``"id"``, the kernel's tiebreak, else a
    :class:`~edax.errors.CodecError`: rows tied on every other key would tie on it too, and paging past
    them would drop the rest of the tie."""
    return _json.call(_native.encode_cursor, document, sort)


def decode_cursor(cursor: str, sort: list[SortKey]) -> list[Any]:
    """Read a keyset cursor back into the value tuple it carries, one value per effective sort key.

    The inverse of :func:`encode_cursor`, and what a backend binds into its own keyset ``WHERE``
    clause instead of re-deriving the wire format from prose. Pass the query's declared ``sort``; the
    ``id`` tiebreak is applied here as it is everywhere else, so the tuple lines up with
    :func:`effective_sort`. A cursor minted under any other sort is a :class:`~edax.errors.CodecError`.
    """
    values: list[Any] = _json.decode(_json.call(functools.partial(_native.decode_cursor, cursor), sort))
    return values


def effective_sort(sort: list[SortKey]) -> list[SortKey]:
    """The keys a page is actually ordered by: ``sort`` with the unique ``id`` tiebreak appended.

    A backend that orders by the declared keys alone disagrees with the kernel at every tie, and
    keyset paging then drops or repeats tied rows. Empty in, empty out: an unsorted query keeps input
    order and takes no cursor.
    """
    keys: list[SortKey] = _json.decode(_json.call(_native.effective_sort, sort))
    return keys

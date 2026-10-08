"""Backend-neutral query filter. Build with the constructors, evaluate with :func:`matches`.

A filter is a plain dict in canonical form; the match decision is made by the kernel, so semantics
are identical across bindings.

Two-valued semantics: a path predicate matches only when the path is present, non-null, and the
comparison holds; a missing or null path makes that leaf false. So ``ne(p, x)`` does not match a
document missing ``p``, while ``not_(eq(p, x))`` does.

A decimal comparand, an :class:`edax.Decimal` or a ``decimal.Decimal``, compares by exact value. ``eq``,
``ne``, ``gt``, ``gte``, ``lt``, ``lte`` and ``in_`` set the kernel's decimal collation for it, so
``gt("price", Decimal("10"))`` reads the document's decimal text as a number: ``"9.5"`` is below it, and
``"9.50"`` equals ``Decimal("9.5")``. A document value that is not decimal text then compares as a
mismatched type does. Every member of a collated ``in_`` must be a decimal or decimal text, else the kernel
refuses the filter. ``contains`` and ``starts_with`` compare
the text.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypeAlias, TypedDict

from . import _json, _native
from .host_value import DECIMAL_TYPES

if TYPE_CHECKING:  # `query` imports this module for `Filter`
    from .query import Document

__all__ = [
    "AndFilter",
    "CompareFilter",
    "Comparison",
    "Containment",
    "ContainsFilter",
    "ExistsFilter",
    "Filter",
    "InFilter",
    "Membership",
    "NotFilter",
    "Operator",
    "OrFilter",
    "Presence",
    "and_",
    "contains",
    "eq",
    "exists",
    "gt",
    "gte",
    "in_",
    "lt",
    "lte",
    "matches",
    "ne",
    "not_",
    "or_",
    "starts_with",
]

#: A comparison's operator, as the kernel's filter grammar spells it.
Operator = Literal["eq", "ne", "gt", "gte", "lt", "lte", "starts_with"]


class _Collated(TypedDict, total=False):
    #: Set for a decimal comparand: decimal text then compares by exact value.
    collation: Literal["decimal"]


class Comparison(_Collated):
    path: str
    op: Operator
    value: Any


class Membership(_Collated):
    path: str
    values: list[Any]


class Containment(TypedDict):
    path: str
    value: Any


class Presence(TypedDict):
    path: str
    exists: bool


# One node of the kernel's canonical filter form each, keyed by its kind. The keys `in`, `and`, `or` and
# `not` are Python keywords, so those nodes take the functional form.
class CompareFilter(TypedDict):
    compare: Comparison


InFilter = TypedDict("InFilter", {"in": Membership})


class ContainsFilter(TypedDict):
    contains: Containment


class ExistsFilter(TypedDict):
    exists: Presence


AndFilter = TypedDict("AndFilter", {"and": list["Filter"]})
OrFilter = TypedDict("OrFilter", {"or": list["Filter"]})
NotFilter = TypedDict("NotFilter", {"not": "Filter"})

#: One filter node in the kernel's canonical form.
Filter: TypeAlias = (
    CompareFilter | InFilter | ContainsFilter | ExistsFilter | AndFilter | OrFilter | NotFilter
)


def _collation(*values: Any) -> _Collated:
    # A decimal encodes as decimal text, which orders by value only under the decimal collation.
    return {"collation": "decimal"} if any(isinstance(value, DECIMAL_TYPES) for value in values) else {}


def _compare(path: str, op: Operator, value: Any) -> Filter:
    return {"compare": {"path": path, "op": op, "value": value, **_collation(value)}}


def eq(path: str, value: Any) -> Filter:
    return _compare(path, "eq", value)


def ne(path: str, value: Any) -> Filter:
    return _compare(path, "ne", value)


def gt(path: str, value: Any) -> Filter:
    return _compare(path, "gt", value)


def gte(path: str, value: Any) -> Filter:
    return _compare(path, "gte", value)


def lt(path: str, value: Any) -> Filter:
    return _compare(path, "lt", value)


def lte(path: str, value: Any) -> Filter:
    return _compare(path, "lte", value)


def starts_with(path: str, prefix: str) -> Filter:
    return {"compare": {"path": path, "op": "starts_with", "value": prefix}}


def in_(path: str, *values: Any) -> Filter:
    return {"in": {"path": path, "values": list(values), **_collation(*values)}}


def contains(path: str, value: Any) -> Filter:
    return {"contains": {"path": path, "value": value}}


def exists(path: str, present: bool = True) -> Filter:
    # The wire key stays `exists`: it is the kernel's filter grammar, not ours to rename.
    return {"exists": {"path": path, "exists": present}}


def and_(*filters: Filter) -> Filter:
    return {"and": list(filters)}


def or_(*filters: Filter) -> Filter:
    return {"or": list(filters)}


def not_(filter: Filter) -> Filter:
    return {"not": filter}


def matches(filter: Filter, document: Document) -> bool:
    """Return whether ``document`` satisfies ``filter``."""
    return _json.call(_native.eval_filter, filter, document)

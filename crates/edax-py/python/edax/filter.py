"""Backend-neutral query filter. Build with the constructors, evaluate with :func:`matches`.

A filter is a plain dict in canonical form; the match decision is made by the kernel, so semantics
are identical across bindings.

Two-valued semantics: a path predicate matches only when the path is present, non-null, and the
comparison holds; a missing or null path makes that leaf false. So ``ne(p, x)`` does not match a
document missing ``p``, while ``not_(eq(p, x))`` does.
"""

from __future__ import annotations

import json
from typing import Any

from . import _edax, errors

Filter = dict[str, Any]  # one filter node in the kernel's canonical form


def _compare(path: str, op: str, value: Any) -> Filter:
    return {"compare": {"path": path, "op": op, "value": value}}


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
    return _compare(path, "starts_with", prefix)


def in_(path: str, *values: Any) -> Filter:
    return {"in": {"path": path, "values": list(values)}}


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


def matches(filter: Filter, document: Any) -> bool:
    """Return whether ``document`` satisfies ``filter``."""
    with errors.mapped():
        return _edax.eval_filter(json.dumps(filter), json.dumps(document))

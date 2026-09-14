"""ProjectionStore seam exercised by the shipped MemoryProjectionStore (get_many() defers to the query
kernel; no durable store ships)."""

from dataclasses import dataclass
from typing import Any, Optional

import pytest
from edax import filter as f
from edax import query as q
from edax.backend.memory import MemoryProjectionStore
from edax.errors import CodecError
from edax.projection import ConcurrencyError, Entity


async def test_save_versions_then_get() -> None:
    store = MemoryProjectionStore()
    e = Entity("u1", 0, {"id": "u1", "name": "ada", "age": 36})
    await store.save(e)
    assert e.version == 1  # advanced in place
    got = await store.get("u1")
    assert got is not None
    assert got.version == 1 and got.document["name"] == "ada"


async def test_stale_version_raises() -> None:
    store = MemoryProjectionStore()
    e = Entity("u1", 0, {"id": "u1", "n": 1})
    await store.save(e)  # now at version 1

    stale = await store.get("u1")
    assert stale is not None
    stale.document["n"] = 2
    await store.save(stale)  # store -> version 2

    e.document["n"] = 3
    with pytest.raises(ConcurrencyError):
        await store.save(e)  # e.version is still 1, store is at 2


# One realistic read model; every paging case below queries this same set. Documents are JSON:
# the value type is heterogeneous by design, so it is `Any` rather than `str | int`.
USERS: list[dict[str, Any]] = [
    {"id": "u0", "name": "ada", "age": 36},
    {"id": "u1", "name": "bob", "age": 41},
    {"id": "u2", "name": "cal", "age": 36},
    {"id": "u3", "name": "dot", "age": 29},
]


async def _seeded() -> MemoryProjectionStore:
    store = MemoryProjectionStore()
    for doc in USERS:
        await store.save(Entity(doc["id"], 0, dict(doc)))
    return store


@dataclass
class PageCase:
    kwargs: dict[str, Any]
    names: list[str]
    cursor_after: Optional[str] = None  # take the cursor of this name's row and resume from it


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(PageCase({}, ["ada", "bob", "cal", "dot"]), id="no_spec_returns_every_document"),
        pytest.param(
            PageCase({"sort": []}, ["ada", "bob", "cal", "dot"]), id="an_empty_sort_list_is_no_sort"
        ),
        pytest.param(
            PageCase({"filter": f.gte("age", 36), "sort": [q.sort_key("name")], "limit": 2}, ["ada", "bob"]),
            id="filter_then_sort_then_limit",
        ),
        pytest.param(
            PageCase({"filter": f.gte("age", 36), "sort": [q.sort_key("name")], "offset": 2}, ["cal"]),
            id="offset_skips_within_the_filtered_set",
        ),
        pytest.param(
            PageCase({"sort": [q.sort_key("name")], "limit": 0}, []),
            id="limit_zero_is_an_empty_page_not_an_absent_limit",
        ),
        pytest.param(
            PageCase({"sort": [q.sort_key("name")], "offset": 9}, []),
            id="offset_past_the_end_is_an_empty_page",
        ),
        pytest.param(
            PageCase({"sort": [q.sort_key("name", desc=True)]}, ["dot", "cal", "bob", "ada"]),
            id="descending_sort_reverses_the_order",
        ),
        pytest.param(
            PageCase({"sort": [q.sort_key("name")]}, ["cal", "dot"], cursor_after="bob"),
            id="keyset_after_resumes_strictly_after_the_cursor_row",
        ),
    ],
)
async def test_get_many_paging_branches(case: PageCase) -> None:
    # The kernel owns filter semantics, the total value order and the cursor encoding; this pins
    # that the seam passes each field through rather than paging on its own.
    store = await _seeded()
    kwargs = dict(case.kwargs)
    if case.cursor_after is not None:
        row = next(d for d in USERS if d["name"] == case.cursor_after)
        kwargs["after"] = q.encode_cursor(row, kwargs["sort"])
    assert [d["name"] for d in await store.get_many(**kwargs)] == case.names


@pytest.mark.parametrize(
    "branch, kwargs",
    [
        ("malformed_cursor", {"sort": [q.sort_key("name")], "after": "not-valid-base64!!"}),
        ("unknown_filter_op", {"filter": {"compare": {"path": "age", "op": "no_such_op", "value": 1}}}),
    ],
)
async def test_get_many_rejects_a_bad_spec_as_terminal(branch: str, kwargs: dict[str, Any]) -> None:
    # The kernel tags a malformed spec `codec:`. Left as a raw RuntimeError it would read as transient
    # and a handler paging with a corrupt cursor would be redelivered forever.
    store = MemoryProjectionStore()
    await store.save(Entity("u0", 0, {"id": "u0", "name": "ada", "age": 36}))
    with pytest.raises(CodecError) as caught:
        await store.get_many(**kwargs)
    assert caught.value.terminal is True

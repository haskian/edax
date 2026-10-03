"""ProjectionStore seam exercised by the shipped MemoryProjectionStore (get_many() defers to the query
kernel; no durable store ships)."""

from dataclasses import dataclass
from typing import Any

import pytest
from edax import filter as f
from edax import query as q
from edax.backend.memory import MemoryProjectionStore
from edax.errors import CodecError
from edax.projection import ConcurrencyError, Entity
from hypothesis import given, settings
from hypothesis import strategies as st


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
    cursor_after: str | None = None  # take the cursor of this name's row and resume from it
    next_after: str | None = None  # the row whose cursor the page's `next` must be, if any


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(PageCase({}, ["ada", "bob", "cal", "dot"]), id="no_spec_returns_every_document"),
        pytest.param(
            PageCase({"sort": []}, ["ada", "bob", "cal", "dot"]), id="an_empty_sort_list_is_no_sort"
        ),
        pytest.param(
            PageCase({"limit": 2}, ["ada", "bob"]), id="an_unsorted_page_mints_no_cursor_and_pages_by_offset"
        ),
        pytest.param(
            PageCase(
                {"filter": f.gte("age", 36), "sort": [q.sort_key("name")], "limit": 2},
                ["ada", "bob"],
                next_after="bob",
            ),
            id="filter_then_sort_then_limit_mints_next",
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
        pytest.param(
            PageCase({"sort": [q.sort_key("name")], "limit": 2}, ["cal", "dot"], cursor_after="bob"),
            id="a_page_ending_at_the_last_row_mints_no_next",
        ),
    ],
)
async def test_get_many_paging_branches(case: PageCase) -> None:
    # The kernel owns filter semantics, the total value order and the cursor encoding; this pins
    # that the seam passes each field through and hands the kernel's `next` back rather than paging on
    # its own.
    store = await _seeded()
    kwargs = dict(case.kwargs)
    if case.cursor_after is not None:
        row = next(d for d in USERS if d["name"] == case.cursor_after)
        kwargs["after"] = q.encode_cursor(row, kwargs["sort"])
    page = await store.get_many(**kwargs)
    assert [d["name"] for d in page.rows] == case.names
    want_next = None
    if case.next_after is not None:
        want_next = q.encode_cursor(next(d for d in USERS if d["name"] == case.next_after), kwargs["sort"])
    assert page.next == want_next


@pytest.mark.parametrize(
    "branch, kwargs",
    [
        ("malformed_cursor", {"sort": [q.sort_key("name")], "after": "not-valid-base64!!"}),
        ("unknown_filter_op", {"filter": {"compare": {"path": "age", "op": "no_such_op", "value": 1}}}),
        ("cursor_under_another_sort", {"sort": [q.sort_key("age")], "after": "<name cursor>"}),
        (
            "cursor_under_another_direction",
            {"sort": [q.sort_key("name", desc=True)], "after": "<name cursor>"},
        ),
        ("cursor_without_a_sort", {"after": "<name cursor>"}),
        ("a_typo_in_a_sort_key", {"sort": [{"path": "name", "dsc": True}]}),
        ("a_none_comparand", {"filter": f.eq("age", None)}),
        ("an_empty_path_segment", {"filter": f.exists("owner..name")}),
        ("an_integer_wider_than_64_bits", {"filter": f.eq("age", 2**70)}),
    ],
)
async def test_get_many_rejects_a_bad_spec_as_terminal(branch: str, kwargs: dict[str, Any]) -> None:
    # The kernel tags a malformed spec `codec:`. Left as a raw RuntimeError it would read as transient
    # and a handler paging with a corrupt cursor would be redelivered forever. A cursor replayed under
    # another sort would page garbage if it were read at all.
    store = MemoryProjectionStore()
    await store.save(Entity("u0", 0, {"id": "u0", "name": "ada", "age": 36}))
    if kwargs.get("after") == "<name cursor>":
        kwargs = {**kwargs, "after": q.encode_cursor(USERS[0], [q.sort_key("name")])}
    with pytest.raises(CodecError) as caught:
        await store.get_many(**kwargs)
    assert caught.value.terminal is True, branch


async def test_paging_reads_every_row_once_while_other_transactions_commit_between_pages() -> None:
    """A reader pages by ``next`` while another writer commits inserts between its pages, and a third
    leaves one uncommitted. Every row present throughout arrives exactly once, in order; a committed row
    after the cursor arrives too, one before it never does, and the open transaction's row stays unseen."""
    store = await _seeded()
    sort = [q.sort_key("age"), q.sort_key("name")]
    seen: list[str] = []
    page = await store.get_many(sort=sort, limit=2)
    seen.extend(d["name"] for d in page.rows)
    async with store.transactor.transaction():
        await store.save(Entity("u4", 0, {"id": "u4", "name": "eve", "age": 37}))  # after the cursor
        await store.save(Entity("u5", 0, {"id": "u5", "name": "abe", "age": 20}))  # before it: never seen
    reader = store.transactor.auto_commit()
    with pytest.raises(_RollBack):
        async with store.transactor.transaction() as pending:
            await store.save(Entity("u6", 0, {"id": "u6", "name": "fay", "age": 50}), tx=pending)
            for _ in range(len(USERS) + 1):  # bounded: a cursor that repeats a row must fail, not hang
                if page.next is None:
                    break
                page = await store.get_many(sort=sort, limit=2, after=page.next, tx=reader)
                seen.extend(d["name"] for d in page.rows)
            raise _RollBack
    assert seen == ["dot", "ada", "cal", "eve", "bob"]


class _RollBack(Exception):
    """Ends the open writer's transaction without committing it."""


@settings(max_examples=150, deadline=None)
@given(
    initial=st.lists(st.one_of(st.none(), st.integers(0, 3)), max_size=10),
    sort=st.sampled_from(
        [[q.sort_key("k")], [q.sort_key("k", desc=True)], [q.sort_key("k"), q.sort_key("id", desc=True)]]
    ),
    limit=st.integers(1, 3),
    changes=st.lists(
        st.lists(
            st.one_of(
                st.tuples(st.just("insert"), st.one_of(st.none(), st.integers(0, 3))),
                st.tuples(st.just("delete"), st.integers(0, 63)),
            ),
            max_size=3,
        ),
        max_size=30,
    ),
)
def test_keyset_paging_never_skips_or_repeats_under_concurrent_writes(
    initial: list[int | None],
    sort: list[q.SortKey],
    limit: int,
    changes: list[list[tuple[str, int | None]]],
) -> None:
    """The Python twin of the kernel's proptest, through the kernel's own ``query``: other writers insert
    and delete rows between pages (sort keys never change, the stable-sort premise). No row is returned
    twice, and every row present for the whole walk is returned exactly once."""

    def row(i: int, k: int | None) -> dict[str, Any]:
        return {"id": f"r{i:03d}"} if k is None else {"id": f"r{i:03d}", "k": k}

    table = [row(i, k) for i, k in enumerate(initial)]
    next_id = len(table)
    survivors = {r["id"] for r in table}
    seen: list[str] = []
    rounds = iter(changes)
    # Every page holds a row no earlier page held, so the walk ends within this many pages; a cursor
    # that resumes at or before its own row would loop forever.
    budget = len(table) + sum(len(c) for c in changes) + 1
    page = q.query(table, sort=sort, limit=limit)
    while True:
        assert budget > 0, f"paging never reached the last page: {seen}"
        budget -= 1
        seen.extend(r["id"] for r in page.rows)
        if page.next is None:
            break
        for op, arg in next(rounds, []):
            if op == "insert":
                table.append(row(next_id, arg))
                next_id += 1
            elif table:
                gone = table.pop((arg or 0) % len(table))
                survivors.discard(gone["id"])
        page = q.query(table, sort=sort, limit=limit, after=page.next)
    assert len(seen) == len(set(seen)), f"a row was returned twice: {seen}"
    assert survivors <= set(seen), f"present throughout yet skipped: {survivors - set(seen)}"


@pytest.mark.parametrize(
    "other_sort",
    [[q.sort_key("k")], [q.sort_key("id", desc=True)], [q.sort_key("k"), q.sort_key("id", desc=True)]],
    ids=["another_key", "the_tiebreak_reversed", "one_key_more"],
)
def test_a_cursor_replayed_under_another_sort_is_refused(other_sort: list[q.SortKey]) -> None:
    rows = [{"id": "a", "k": 1}, {"id": "b", "k": 2}]
    page = q.query(rows, sort=[q.sort_key("id")], limit=1)
    assert page.next is not None
    with pytest.raises(CodecError):
        q.query(rows, sort=other_sort, after=page.next)

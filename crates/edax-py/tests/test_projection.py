"""ProjectionStore seam exercised by the shipped MemoryProjectionStore (get_many() defers to the query
kernel; no durable store ships)."""

import json
from dataclasses import dataclass
from typing import Any

import pytest
from conftest import golden
from edax import _edax
from edax import filter as f
from edax import query as q
from edax.backend.memory import MemoryProjectionStore
from edax.errors import CodecError
from edax.projection import Entity, check_document, keyed_document
from hypothesis import given, settings
from hypothesis import strategies as st

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
    page = await store.get_many(q.Query(**kwargs))
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
        # As from `?after=`: the kernel refuses it, so the SDK must not read it as "no cursor".
        ("an_empty_cursor", {"sort": [q.sort_key("name")], "after": ""}),
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
        await store.get_many(q.Query(**kwargs))
    assert caught.value.terminal is True, branch


@pytest.mark.parametrize(
    "kwargs, refused",
    [
        pytest.param({"sort": [q.sort_key("name")], "limit": 2}, False, id="a_valid_spec_passes"),
        pytest.param({"sort": [{"path": "name", "dsc": True}]}, True, id="a_typo_in_a_sort_key"),
        pytest.param({"filter": f.eq("age", None)}, True, id="a_none_comparand"),
        pytest.param({"filter": f.exists("owner..name")}, True, id="an_empty_path_segment"),
        pytest.param({"filter": f.eq("age", 2**70)}, True, id="an_integer_wider_than_64_bits"),
        pytest.param({"after": "x"}, True, id="a_cursor_without_a_sort"),
        # The cursor is read against the sort when the query runs, by `decode_cursor`.
        pytest.param({"sort": [q.sort_key("name")], "after": "!!"}, False, id="the_cursor_is_not_read"),
    ],
)
def test_check_query_refuses_the_specs_the_kernel_refuses(kwargs: dict[str, Any], refused: bool) -> None:
    spec = q.Query(**kwargs)
    if not refused:
        assert q.check_query(spec) is None
        return
    with pytest.raises(CodecError) as caught:
        q.check_query(spec)
    assert caught.value.terminal is True


async def test_paging_reads_every_row_once_while_other_transactions_commit_between_pages() -> None:
    """A reader pages by ``next`` while another writer commits inserts between its pages, and a third
    leaves one uncommitted. Every row present throughout arrives exactly once, in order; a committed row
    after the cursor arrives too, one before it never does, and the open transaction's row stays unseen."""
    store = await _seeded()
    sort = [q.sort_key("age"), q.sort_key("name")]
    seen: list[str] = []
    page = await store.get_many(q.Query(sort=sort, limit=2))
    seen.extend(d["name"] for d in page.rows)
    async with store.transactor.begin():
        await store.save(Entity("u4", 0, {"id": "u4", "name": "eve", "age": 37}))  # after the cursor
        await store.save(Entity("u5", 0, {"id": "u5", "name": "abe", "age": 20}))  # before it: never seen
    reader = store.transactor.auto_commit()
    with pytest.raises(_RollBack):
        async with store.transactor.begin() as pending:
            await store.save(Entity("u6", 0, {"id": "u6", "name": "fay", "age": 50}), tx=pending)
            for _ in range(len(USERS) + 1):  # bounded: a cursor that repeats a row must fail, not hang
                if page.next is None:
                    break
                page = await store.get_many(q.Query(sort=sort, limit=2, after=page.next), tx=reader)
                seen.extend(d["name"] for d in page.rows)
            raise _RollBack
    assert seen == ["dot", "ada", "cal", "eve", "bob"]


class _RollBack(Exception):
    """Ends the open writer's transaction without committing it."""


# A sort value: a small integer, so rows tie often, or a whole-number float past 2^53, which the cursor
# carries as integer digits that are not its exact value.
_SORT_VALUE = st.one_of(st.integers(0, 3), st.sampled_from([5.808820077685227e16, 1.1108287563226614e18]))
_SORT = [q.sort_key("n")]


@settings(max_examples=300, deadline=None)
@given(
    value=st.one_of(
        st.floats(allow_nan=False, allow_infinity=False),
        st.integers(2**53, 2**70).map(float),
        st.integers(-(2**53 - 1), 2**53 - 1),
    )
)
def test_a_sort_value_round_trips_through_the_cursor(value: int | float) -> None:
    """Every finite float, and every integer the cursor accepts, decodes as the value it was minted from,
    so the next page resumes exactly at its row."""
    decoded = q.decode_cursor(q.encode_cursor({"id": "a", "n": value}, _SORT), _SORT)
    assert decoded == [value, "a"]


@pytest.mark.parametrize(
    "tiebreak",
    [None, ["t1", 0], {"tenant": "t1", "n": 0}],
    ids=["null_id", "array_id", "object_id"],
)
def test_a_row_whose_id_is_not_a_string_or_a_number_mints_no_cursor(tiebreak: Any) -> None:
    # Arrays and objects compare by type alone, so these rows tie on the tiebreak as null ids would,
    # and resuming after the first would drop the second.
    rows = [{"id": tiebreak, "k": 1}, {"id": tiebreak, "k": 1}]
    with pytest.raises(CodecError) as caught:
        q.query(rows, q.Query(sort=[q.sort_key("k")], limit=1))
    assert "string or number `id`" in str(caught.value)


_WIDE_DOCUMENT: dict[str, Any] = {"id": "a", "name": "ada", "n": 2**64 + 1}


@pytest.mark.parametrize(
    "boundary",
    ["save", "query", "matches", "encode_cursor"],
)
async def test_a_document_integer_wider_than_64_bits_is_refused(boundary: str) -> None:
    """The filter and the sort would read the integer as the nearest float, and ``get``, ``get_many`` and
    ``query`` would return it rounded. So every boundary that takes a document refuses it, as the spec
    refuses one, and the save stores nothing."""
    store = MemoryProjectionStore()
    entity = Entity("a", 0, dict(_WIDE_DOCUMENT))
    with pytest.raises(CodecError) as caught:
        if boundary == "save":
            await store.save(entity)
        elif boundary == "query":
            q.query([_WIDE_DOCUMENT])
        elif boundary == "matches":
            f.matches(f.eq("name", "ada"), _WIDE_DOCUMENT)
        else:
            q.encode_cursor(_WIDE_DOCUMENT, [q.sort_key("n")])
    assert caught.value.terminal is True
    assert "outside the 64-bit range" in str(caught.value)
    assert await store.get("a") is None


@settings(max_examples=150, deadline=None)
@given(
    initial=st.lists(st.one_of(st.none(), _SORT_VALUE), max_size=10),
    sort=st.sampled_from(
        [[q.sort_key("k")], [q.sort_key("k", desc=True)], [q.sort_key("k"), q.sort_key("id", desc=True)]]
    ),
    limit=st.integers(1, 3),
    changes=st.lists(
        st.lists(
            st.one_of(
                st.tuples(st.just("insert"), st.one_of(st.none(), _SORT_VALUE)),
                st.tuples(st.just("delete"), st.integers(0, 63)),
            ),
            max_size=3,
        ),
        max_size=30,
    ),
)
def test_keyset_paging_never_skips_or_repeats_under_concurrent_writes(
    initial: list[int | float | None],
    sort: list[q.SortKey],
    limit: int,
    changes: list[list[tuple[str, int | float | None]]],
) -> None:
    """The Python twin of the kernel's proptest, through the kernel's own ``query``: other writers insert
    and delete rows between pages (sort keys never change, the stable-sort premise). No row is returned
    twice, and every row present for the whole walk is returned exactly once."""

    def row(i: int, k: int | float | None) -> dict[str, Any]:
        return {"id": f"r{i:03d}"} if k is None else {"id": f"r{i:03d}", "k": k}

    table = [row(i, k) for i, k in enumerate(initial)]
    next_id = len(table)
    survivors = {r["id"] for r in table}
    seen: list[str] = []
    rounds = iter(changes)
    # Every page holds a row no earlier page held, so the walk ends within this many pages; a cursor
    # that resumes at or before its own row would loop forever.
    budget = len(table) + sum(len(c) for c in changes) + 1
    page = q.query(table, q.Query(sort=sort, limit=limit))
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
                gone = table.pop(int(arg or 0) % len(table))
                survivors.discard(gone["id"])
        page = q.query(table, q.Query(sort=sort, limit=limit, after=page.next))
    assert len(seen) == len(set(seen)), f"a row was returned twice: {seen}"
    assert survivors <= set(seen), f"present throughout yet skipped: {survivors - set(seen)}"


@pytest.mark.parametrize(
    "other_sort",
    [[q.sort_key("k")], [q.sort_key("id", desc=True)], [q.sort_key("k"), q.sort_key("id", desc=True)]],
    ids=["another_key", "the_tiebreak_reversed", "one_key_more"],
)
def test_a_cursor_replayed_under_another_sort_is_refused(other_sort: list[q.SortKey]) -> None:
    rows = [{"id": "a", "k": 1}, {"id": "b", "k": 2}]
    page = q.query(rows, q.Query(sort=[q.sort_key("id")], limit=1))
    assert page.next is not None
    with pytest.raises(CodecError):
        q.query(rows, q.Query(sort=other_sort, after=page.next))


async def test_golden_projection_document() -> None:
    """The kernel's document rule, read through every Python path to it: the check a host's own store
    calls, the keyed read it calls in ``get``, and the memory store's save and both its reads. A row the
    rule refuses saves nothing."""
    for c in golden("projection_document.json"):
        name, want = c["name"], c["want"]
        try:
            _edax.check_projection_document(c["id"], c["json"])
            keyed = json.loads(_edax.keyed_projection_document(c["id"], c["json"]))
            got: dict[str, Any] = {"ok": json.loads(c["json"]), "keyed": keyed}
        except RuntimeError as e:
            got = {"error": str(e)}
        assert got == want, name

        store = MemoryProjectionStore()
        try:
            document = json.loads(c["json"])
        except json.JSONDecodeError:
            continue  # text that is not JSON has no Python value to save
        entity = Entity(c["id"], 0, document)
        if "ok" in want:
            check_document(c["id"], document)
            assert keyed_document(c["id"], document) == want["keyed"], name
            await store.save(entity)
            stored = await store.get(c["id"])
            assert stored is not None and stored.document == want["keyed"], name
            assert (await store.get_many()).rows == [want["keyed"]], name
            continue
        for refuses in (check_document, keyed_document):
            with pytest.raises(CodecError) as caught:
                refuses(c["id"], document)
            assert f"{caught.value.code}: {caught.value}" == want["error"], name
        with pytest.raises(CodecError):
            await store.save(entity)
        assert await store.get(c["id"]) is None, name

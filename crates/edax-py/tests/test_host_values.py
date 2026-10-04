"""``Decimal`` and ``datetime`` values: the shared form the SDK's one encoder writes them in, and what that
buys. ``host_values.json`` pins the JSON text per value. The tables below show a filter, a sort, a cursor and
the command hash comparing them by value, and every refusal as the terminal ``CodecError`` at the call a host
makes."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, ClassVar

import edax
import pytest
from conftest import golden
from edax import _json
from edax import command as C
from edax import filter as F
from edax import query as Q
from edax.backend.memory import MemoryIdempotencyStore, MemoryProjectionStore
from edax.projection import Entity

UTC = dt.timezone.utc
PLUS_ONE = dt.timezone(dt.timedelta(hours=1))
MINUS_FIVE = dt.timezone(dt.timedelta(hours=-5))
EPOCH = dt.datetime(1970, 1, 1, tzinfo=UTC)
MICROSECOND = dt.timedelta(microseconds=1)


def _host_value(kind: str, text: str) -> Decimal | dt.datetime | dt.date:
    """The host value a golden row names by its ``kind`` and ``input`` text."""
    if kind == "decimal":
        return Decimal(text)
    if kind == "datetime":
        # Python 3.10 reads no `Z` suffix; the kernel reads both spellings of UTC.
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    assert kind == "date", kind
    return dt.date.fromisoformat(text)


@pytest.mark.parametrize("case", golden("host_values.json"), ids=lambda case: case["name"])
def test_golden_host_values(case: dict[str, Any]) -> None:
    value = _host_value(case["kind"], case["input"])
    if case["error"] is not None:
        # As a value and as a dict key: the encoder walks keys too. A signalling NaN cannot hash, so it
        # is never a key.
        holders: list[dict[Any, Any]] = [{"nested": [value]}]
        if not (isinstance(value, Decimal) and value.is_snan()):
            holders.append({value: "key"})
        for holder in holders:
            with pytest.raises(edax.CodecError) as caught:
                _json.encode(holder)
            assert caught.value.code == case["error"], holder
        return
    assert _json.encode(value) == case["json"]
    assert _json.encode({value: 1}) == f"{{{case['json']}:1}}", "a dict key takes the same form"
    if case["epoch_micros"] is not None:
        assert isinstance(value, dt.datetime)
        assert (value - EPOCH) // MICROSECOND == case["epoch_micros"], "the row's input is its pinned instant"


# One priced, timestamped read-model row; each case overrides only the field it probes. `at` is
# 2024-01-01T00:00:00Z, written with a +01:00 offset.
INVOICE: dict[str, Any] = {
    "id": "inv-1",
    "price": Decimal("9.50"),
    "at": dt.datetime(2024, 1, 1, 1, 0, tzinfo=PLUS_ONE),
}
WIDE = Decimal("12345678901234567890.12")


@pytest.mark.parametrize(
    "branch, built, override, want",
    [
        ("decimal_gt_by_value", F.gt("price", Decimal("10")), {}, False),
        ("decimal_lt_by_value", F.lt("price", Decimal("10")), {}, True),
        ("decimal_eq_across_trailing_zeros", F.eq("price", Decimal("9.5")), {}, True),
        ("decimal_ne_across_trailing_zeros", F.ne("price", Decimal("9.5")), {}, False),
        ("decimal_gte_at_the_boundary", F.gte("price", Decimal("9.500")), {}, True),
        ("decimal_lte_below", F.lte("price", Decimal("9.49")), {}, False),
        ("decimal_in_by_value", F.in_("price", Decimal("1"), Decimal("9.5")), {}, True),
        ("decimal_in_misses", F.in_("price", Decimal("9.51")), {}, False),
        ("decimal_negative", F.lt("price", Decimal("-9.5")), {"price": Decimal("-10")}, True),
        ("decimal_past_17_digits_is_exact", F.gt("price", Decimal("12345678901234567890.11")), {"price": WIDE}, True),
        ("decimal_against_other_text_is_unordered", F.lt("price", Decimal("10")), {"price": "n/a"}, False),
        ("decimal_ne_against_other_text", F.ne("price", Decimal("10")), {"price": "n/a"}, True),
        # A str comparand sets no collation, so the text compares by code point: "9.5" > "10".
        ("a_str_comparand_compares_text", F.lt("price", "10"), {}, False),
        ("datetime_eq_across_offsets", F.eq("at", dt.datetime(2024, 1, 1, tzinfo=UTC)), {}, True),
        ("datetime_gt_by_instant_not_wall_time", F.gt("at", dt.datetime(2023, 12, 31, 19, 30, tzinfo=MINUS_FIVE)), {}, False),
        ("datetime_lt_by_instant_not_wall_time", F.lt("at", dt.datetime(2023, 12, 31, 19, 30, tzinfo=MINUS_FIVE)), {}, True),
        ("datetime_whole_second_before_a_later_fraction", F.lt("at", dt.datetime(2024, 1, 1, 0, 0, 0, 500000, tzinfo=UTC)), {}, True),
        ("datetime_microsecond_after_the_whole_second", F.gt("at", dt.datetime(2024, 1, 1, tzinfo=UTC)), {"at": dt.datetime(2024, 1, 1, 0, 0, 0, 1, tzinfo=UTC)}, True),
        ("datetime_in_by_instant", F.in_("at", dt.datetime(2023, 12, 31, 19, 0, tzinfo=MINUS_FIVE)), {}, True),
    ],
)  # fmt: skip
def test_filter_compares_host_values_by_value(
    branch: str, built: F.Filter, override: dict[str, Any], want: bool
) -> None:
    assert F.matches(built, {**INVOICE, **override}) is want, branch


@pytest.mark.parametrize(
    "branch, built, collated",
    [
        ("a_decimal_comparand_sets_the_collation", F.gt("price", Decimal("10")), True),
        ("a_decimal_member_sets_the_collation", F.in_("price", "1", Decimal("2")), True),
        ("a_str_comparand_sets_none", F.gt("price", "10"), False),
        ("a_datetime_comparand_sets_none", F.gt("at", dt.datetime(2024, 1, 1, tzinfo=UTC)), False),
        ("starts_with_sets_none", F.starts_with("price", "9"), False),
    ],
)
def test_constructor_sets_the_decimal_collation(branch: str, built: F.Filter, collated: bool) -> None:
    (node,) = built.values()
    assert (node.get("collation") == "decimal") is collated, branch


@pytest.mark.parametrize(
    "branch, built, document",
    [
        ("a_naive_datetime_comparand", F.gt("at", dt.datetime(2024, 1, 1)), INVOICE),
        ("a_naive_datetime_in_the_document", F.exists("at"), {**INVOICE, "at": dt.datetime(2024, 1, 1)}),
        ("a_nan_decimal_comparand", F.eq("price", Decimal("NaN")), INVOICE),
        ("an_infinite_decimal_in_the_document", F.exists("price"), {**INVOICE, "price": Decimal("-Infinity")}),
        ("a_decimal_past_the_digit_bound", F.lt("price", Decimal("1E+1000")), INVOICE),
        # A collated `in_` reads every member as decimal text; an int is not text.
        ("a_collated_in_with_an_int_member", F.in_("price", Decimal("9.5"), 10), INVOICE),
    ],
)  # fmt: skip
def test_filter_refusals_are_codec_errors(branch: str, built: F.Filter, document: dict[str, Any]) -> None:
    with pytest.raises(edax.CodecError) as caught:
        F.matches(built, document)
    assert caught.value.terminal, branch


# Prices that sort wrongly as text: a tie spelled two ways, a negative, a value past 17 digits. The
# timestamps carry three offsets, so wall-time order differs from instant order.
LEDGER: list[dict[str, Any]] = [
    {"id": "a", "price": Decimal("10"), "at": dt.datetime(2024, 1, 1, 1, 0, tzinfo=PLUS_ONE)},  # 00:00Z
    {"id": "b", "price": Decimal("9.50"), "at": dt.datetime(2023, 12, 31, 19, 0, 0, 1, tzinfo=MINUS_FIVE)},  # 00:00:00.000001Z
    {"id": "c", "price": Decimal("9.5"), "at": dt.datetime(2023, 12, 31, 23, 30, tzinfo=UTC)},
    {"id": "d", "price": Decimal("-2.25"), "at": dt.datetime(2024, 1, 1, 0, 0, 0, 500000, tzinfo=UTC)},
    {"id": "e", "price": WIDE, "at": dt.datetime(2024, 1, 1, 0, 30, tzinfo=PLUS_ONE)},  # 23:30Z, ties c
]  # fmt: skip


@pytest.mark.parametrize(
    "branch, sort, want",
    [
        ("decimal_ascending_by_value", [Q.sort_key("price", decimal=True)], ["d", "b", "c", "a", "e"]),
        ("decimal_descending_by_value", [Q.sort_key("price", desc=True, decimal=True)], ["e", "a", "b", "c", "d"]),
        # Without the collation the decimal text sorts by code point: "-2.25" < "10" < "1234..." < "9.5".
        ("decimal_without_the_collation_sorts_text", [Q.sort_key("price")], ["d", "a", "e", "b", "c"]),
        ("datetime_ascending_by_instant", [Q.sort_key("at")], ["c", "e", "a", "b", "d"]),
        ("datetime_descending_by_instant", [Q.sort_key("at", desc=True)], ["d", "b", "a", "c", "e"]),
    ],
)  # fmt: skip
async def test_sort_and_cursor_paging_order_host_values_by_value(
    branch: str, sort: list[Q.SortKey], want: list[str]
) -> None:
    store = MemoryProjectionStore()
    for row in LEDGER:
        await store.save(Entity(row["id"], 0, row))
    whole = await store.get_many(sort=sort)
    assert [row["id"] for row in whole.rows] == want, branch
    # Two rows a page, each resumed from the previous page's cursor: no row dropped or repeated.
    walked: list[str] = []
    after: str | None = None
    for _ in range(len(LEDGER)):
        page = await store.get_many(sort=sort, limit=2, after=after)
        walked.extend(row["id"] for row in page.rows)
        if page.next is None:
            break
        after = page.next
    assert walked == want, branch


def test_a_cursor_carries_the_decimal_text_exactly() -> None:
    sort = [Q.sort_key("price", decimal=True)]
    cursor = Q.encode_cursor({"id": "e", "price": WIDE}, sort)
    assert Q.decode_cursor(cursor, sort) == ["12345678901234567890.12", "e"]
    with pytest.raises(edax.CodecError, match="another sort"):
        Q.decode_cursor(cursor, [Q.sort_key("price")])


@dataclass
class Charge:
    amount: Decimal
    at: dt.datetime
    type: ClassVar[str] = "charge"


AT = dt.datetime(2024, 1, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    "branch, first, second, same",
    [
        ("decimal_trailing_zeros_hash_alike", Charge(Decimal("9.5"), AT), Charge(Decimal("9.50"), AT), True),
        ("decimal_exponent_hashes_as_its_plain_form", Charge(Decimal("1E+2"), AT), Charge(Decimal("100"), AT), True),
        ("decimal_other_value_hashes_apart", Charge(Decimal("9.5"), AT), Charge(Decimal("9.51"), AT), False),
        ("datetime_one_instant_in_two_offsets_hashes_alike", Charge(Decimal("1"), AT), Charge(Decimal("1"), AT.astimezone(MINUS_FIVE)), True),
        ("datetime_other_instant_hashes_apart", Charge(Decimal("1"), AT), Charge(Decimal("1"), AT + MICROSECOND), False),
    ],
)  # fmt: skip
async def test_command_hash_sees_host_values_by_value(
    branch: str, first: Charge, second: Charge, same: bool
) -> None:
    assert (C._payload_hash(first, Charge.type) == C._payload_hash(second, Charge.type)) is same, branch
    if not same:
        return
    # Through the guard a host wires: the second spelling replays the first outcome, the handler runs once.
    runs: list[Charge] = []

    async def handler(cmd: Charge) -> str:
        runs.append(cmd)
        return "charged"

    bus = C.CommandBus(C.idempotent(MemoryIdempotencyStore(), scope="test"))
    bus.register(Charge.type, handler)
    assert await bus.dispatch(first, idempotency_key="req-1") == "charged"
    assert await bus.dispatch(second, idempotency_key="req-1") == "charged"
    assert runs == [first], branch


@pytest.mark.parametrize(
    "branch, cmd",
    [
        ("a_naive_datetime", Charge(Decimal("1"), dt.datetime(2024, 1, 1))),
        ("a_nan_decimal", Charge(Decimal("NaN"), AT)),
        ("a_decimal_past_the_digit_bound", Charge(Decimal("1E-1000"), AT)),
    ],
)
async def test_command_refusals_are_codec_errors_before_the_handler(branch: str, cmd: Charge) -> None:
    runs: list[Charge] = []

    async def handler(cmd: Charge) -> None:
        runs.append(cmd)

    bus = C.CommandBus(C.idempotent(MemoryIdempotencyStore(), scope="test"))
    bus.register(Charge.type, handler)
    with pytest.raises(edax.CodecError) as caught:
        await bus.dispatch(cmd, idempotency_key="req-1")
    assert caught.value.terminal and runs == [], branch

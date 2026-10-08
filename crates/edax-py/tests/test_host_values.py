"""``Decimal`` and ``datetime`` values: the shared form the SDK's one encoder writes them in, and what that
buys. ``host_values.json`` pins the JSON text per value. The tables below show a filter, a sort, a cursor and
the command hash comparing them by value, and every refusal as the terminal ``CodecError`` at the call a host
makes."""

from __future__ import annotations

import copy
import datetime as dt
import pickle
import re
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, ClassVar, cast

import edax
import msgspec
import pytest
from conftest import golden, wire_of
from edax import _json
from edax import command as C
from edax import filter as F
from edax import query as Q
from edax.backend.memory import MemoryIdempotencyStore, MemoryProjectionStore
from edax.codec import RegistryCodec, json_codec, msgpack_codec
from edax.projection import Entity

UTC = dt.timezone.utc
PLUS_ONE = dt.timezone(dt.timedelta(hours=1))
MINUS_FIVE = dt.timezone(dt.timedelta(hours=-5))
EPOCH = dt.datetime(1970, 1, 1, tzinfo=UTC)
MICROSECOND = dt.timedelta(microseconds=1)


def _host_value(kind: str, text: str) -> Decimal | dt.datetime | dt.date | float:
    """The host value a golden row names by its ``kind`` and ``input`` text."""
    if kind == "decimal":
        return Decimal(text)
    if kind == "float":
        return float(text)
    if kind == "datetime":
        # Python 3.10 reads no `Z` suffix; the kernel reads both spellings of UTC.
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    assert kind == "date", kind
    return dt.date.fromisoformat(text)


_HOST_TYPE_OF: dict[str, type[edax.Decimal] | type[edax.Instant]] = {
    "decimal": edax.Decimal,
    "datetime": edax.Instant,
}


@pytest.mark.parametrize(
    "case",
    [case for case in golden("host_values.json") if case["kind"] in _HOST_TYPE_OF],
    ids=lambda case: case["name"],
)
@pytest.mark.parametrize("fmt", ["json", "msgpack"])
def test_golden_host_values_read_from_the_wire_text(fmt: str, case: dict[str, Any]) -> None:
    """Each decimal and instant row read as its host type from the row's own text, as a payload carries it:
    the vector's text, or the kernel's refusal, in both formats. The host type's constructor gives the same
    answer, and writes the same text."""
    host_type = _HOST_TYPE_OF[case["kind"]]
    decode = _json.decode_json if fmt == "json" else _json.decode_msgpack
    wire = _ENCODE[fmt](case["input"])
    if case["error"] is not None:
        with pytest.raises(edax.CodecError) as built:
            host_type(case["input"])
        assert wire_of(built.value) == case["error"]
        with pytest.raises(msgspec.ValidationError, match=re.escape(str(built.value))):
            decode(wire, host_type)
        return
    read = decode(wire, host_type)
    assert read == host_type(case["input"]) and _json.encode(read) == case["json"]


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
            assert wire_of(caught.value) == case["error"], holder
        return
    assert _json.encode(value) == case["json"]
    if case["kind"] == "float":
        return  # a JSON object key is text, so a float key has no form of its own to pin
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
        ("host_decimal_lt_by_value", F.lt("price", edax.Decimal("10")), {}, True),
        ("host_decimal_document_value", F.eq("price", Decimal("9.5")), {"price": edax.Decimal("9.50")}, True),
        ("host_instant_eq_across_offsets", F.eq("at", edax.Instant("2024-01-01T00:00:00Z")), {}, True),
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
        ("a_host_decimal_comparand_sets_the_collation", F.gt("price", edax.Decimal("10")), True),
        ("a_host_instant_comparand_sets_none", F.gt("at", edax.Instant("2024-01-01T00:00:00Z")), False),
        ("a_str_comparand_sets_none", F.gt("price", "10"), False),
        ("a_datetime_comparand_sets_none", F.gt("at", dt.datetime(2024, 1, 1, tzinfo=UTC)), False),
        ("starts_with_sets_none", F.starts_with("price", "9"), False),
    ],
)
def test_constructor_sets_the_decimal_collation(branch: str, built: F.Filter, collated: bool) -> None:
    (node,) = cast("dict[str, Any]", built).values()
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
    whole = await store.get_many(Q.Query(sort=sort))
    assert [row["id"] for row in whole.rows] == want, branch
    # Two rows a page, each resumed from the previous page's cursor: no row dropped or repeated.
    walked: list[str] = []
    after: str | None = None
    for _ in range(len(LEDGER)):
        page = await store.get_many(Q.Query(sort=sort, limit=2, after=after))
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

    bus = C.CommandBus(C.idempotent(MemoryIdempotencyStore(), scope="test", tenancy=edax.Tenancy.SILO))
    bus.register(Charge, handler)
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

    bus = C.CommandBus(C.idempotent(MemoryIdempotencyStore(), scope="test", tenancy=edax.Tenancy.SILO))
    bus.register(Charge, handler)
    with pytest.raises(edax.CodecError) as caught:
        await bus.dispatch(cmd, idempotency_key="req-1")
    assert caught.value.terminal and runs == [], branch


class Settled(msgspec.Struct):
    """An event with both host values and raw bytes."""

    amount: edax.Decimal
    at: edax.Instant
    receipt: bytes


SETTLED = Settled(edax.Decimal("9.50"), edax.Instant("2024-01-01T01:30:00+01:00"), b"\x00\xff")
_CODECS: dict[str, Callable[..., RegistryCodec]] = {"json": json_codec, "msgpack": msgpack_codec}
_ENCODE: dict[str, Callable[[Any], bytes]] = {"json": msgspec.json.encode, "msgpack": msgspec.msgpack.encode}


@pytest.mark.parametrize("fmt", ["json", "msgpack"])
def test_event_codecs_write_host_values_in_the_shared_form(fmt: str) -> None:
    """The event codecs write what every other path writes: the decimal and instant texts the Rust SDK's
    ``edax::Decimal`` / ``edax::Instant`` write, so a stream reads alike from both SDKs. MessagePack keeps
    bytes native and writes the instant as text, never as a MessagePack timestamp."""
    codec = _CODECS[fmt]({"Settled": Settled})
    written = codec.marshal(SETTLED)
    payload = (msgspec.json.decode if fmt == "json" else msgspec.msgpack.decode)(bytes(written.data))
    receipt = "AP8=" if fmt == "json" else b"\x00\xff"
    assert payload == {"amount": "9.5", "at": "2024-01-01T00:30:00.000000Z", "receipt": receipt}
    assert codec.unmarshal(written) == SETTLED, "read back as the same value"


@pytest.mark.parametrize(
    "branch, fmt, data",
    [
        ("json_decimal_with_trailing_zeros", "json", b'{"amount":"9.50","at":"2024-01-01T01:30:00+01:00","receipt":"AP8="}'),
        # What an earlier release wrote for a `datetime` field: a MessagePack timestamp, not text.
        ("msgpack_timestamp_extension", "msgpack", msgspec.msgpack.encode({"amount": "9.50", "at": dt.datetime(2024, 1, 1, 0, 30, tzinfo=UTC), "receipt": b"\x00\xff"})),
    ],
)  # fmt: skip
def test_event_codecs_read_payloads_written_before_the_shared_form(
    branch: str, fmt: str, data: bytes
) -> None:
    """A payload stored before the codecs wrote the shared form still reads, as the same value."""
    codec = _CODECS[fmt]({"Settled": Settled})
    assert codec.unmarshal(edax.Payload("Settled", 1, data)) == SETTLED, branch


class Ledger(msgspec.Struct):
    """A payload with each place a host value is read from: a field, an optional field, a map key and a
    field of a struct nested in a list."""

    amount: edax.Decimal
    at: edax.Instant | None = None
    by_price: dict[edax.Decimal, int] = {}
    lines: list[Settled] = []


#: A valid payload; each case below overrides the one field it probes.
LEDGER_WIRE: dict[str, Any] = {"amount": "9.5", "at": "2024-01-01T00:30:00.000000Z"}
LEDGER_READ = Ledger(edax.Decimal("9.5"), edax.Instant("2024-01-01T00:30:00.000000Z"))


@pytest.mark.parametrize("fmt", ["json", "msgpack"])
@pytest.mark.parametrize(
    "branch, override, want",
    [
        ("canonical_text_is_kept", {}, LEDGER_READ),
        ("a_plus_sign_is_read", {"amount": "+9.5"}, LEDGER_READ),
        ("trailing_zeros_are_read", {"amount": "9.50"}, LEDGER_READ),
        ("an_offset_converts_to_utc", {"at": "2024-01-01T01:30:00+01:00"}, LEDGER_READ),
        ("a_nine_digit_whole_microsecond_is_read", {"at": "2024-01-01T00:30:00.000001000Z"}, msgspec.structs.replace(LEDGER_READ, at=edax.Instant("2024-01-01T00:30:00.000001Z"))),
        ("null_for_an_optional_field_is_none", {"at": None}, msgspec.structs.replace(LEDGER_READ, at=None)),
        ("a_map_key_is_read_from_its_text", {"by_price": {"9.50": 1}}, msgspec.structs.replace(LEDGER_READ, by_price={edax.Decimal("9.5"): 1})),
        ("a_finer_fraction_is_refused", {"at": "2024-01-01T00:30:00.0000001Z"}, "finer than a microsecond"),
        ("a_fraction_that_would_round_up_a_second_is_refused", {"at": "2024-01-01T00:30:00.9999999Z"}, "finer than a microsecond"),
        ("an_underscore_is_refused", {"amount": "1_000"}, "is not decimal text"),
        ("surrounding_whitespace_is_refused", {"amount": " 9.5 "}, "is not decimal text"),
        ("a_number_is_refused", {"amount": 9.5}, "Expected `Decimal`, got `float`"),
        ("an_integer_instant_is_refused", {"at": 1704069000}, "Expected `Instant`, got `int`"),
        ("null_for_a_required_field_is_refused", {"amount": None}, "Expected `Decimal`, got `NoneType`"),
        ("a_refused_map_key", {"by_price": {"1_000": 1}}, "is not decimal text"),
        ("a_refused_value_in_a_nested_struct", {"lines": [{"amount": "NaN", "at": "2024-01-01T00:30:00Z", "receipt": ""}]}, "is not decimal text"),
    ],
)  # fmt: skip
def test_event_codecs_read_host_values_from_the_wire_text(
    fmt: str, branch: str, override: dict[str, Any], want: Ledger | str
) -> None:
    """Each host value is read from the text the payload carries, through the kernel, before anything rounds
    it: the spellings the kernel reads are read, and what it refuses is the terminal ``CodecError``, as in the
    Rust and Go SDKs. A ``datetime`` / ``Decimal`` field read every refused row here as a value."""
    codec = _CODECS[fmt]({"Ledger": Ledger})
    stored = edax.Payload("Ledger", 1, _ENCODE[fmt]({**LEDGER_WIRE, **override}))
    if isinstance(want, str):
        with pytest.raises(edax.CodecError, match=re.escape(want)) as caught:
            codec.unmarshal(stored)
        assert caught.value.terminal, branch
        return
    assert codec.unmarshal(stored) == want, branch


class _Line(msgspec.Struct):
    at: dt.datetime


@pytest.mark.parametrize("fmt", ["json", "msgpack"])
@pytest.mark.parametrize(
    "branch, registered, refused",
    [
        ("host_types_register", Ledger, None),
        ("a_datetime_field_is_refused", _Line, "datetime.datetime (declare edax.Instant)"),
        ("a_decimal_in_a_dict_is_refused", dict[str, Decimal], "decimal.Decimal (declare edax.Decimal)"),
        ("a_datetime_in_a_nested_struct_is_refused", list[_Line], "datetime.datetime (declare edax.Instant)"),
    ],
)  # fmt: skip
def test_event_codecs_refuse_a_plain_host_value_at_registration(
    fmt: str, branch: str, registered: Any, refused: str | None
) -> None:
    """msgspec converts a ``datetime`` or ``decimal.Decimal`` field before the kernel sees its text, so the
    codec could not refuse what the other SDKs refuse. Such a type is refused when the codec is built, at any
    depth."""
    if refused is None:
        assert _CODECS[fmt]({"Event": registered}).type_of("Event") is registered, branch
        return
    with pytest.raises(edax.CodecError, match=re.escape(refused)) as caught:
        _CODECS[fmt]({"Event": registered})
    assert caught.value.terminal, branch


@dataclass
class Renamed:
    """An event with an optional field msgspec leaves out while it holds ``UNSET``."""

    id: str
    note: str | msgspec.UnsetType = msgspec.UNSET


class Deferred(msgspec.Struct):
    """An event that defers part of its body: ``body`` holds the bytes as the decoder found them."""

    id: str
    body: msgspec.Raw


class Tagged(msgspec.Struct):
    """An event carrying a MessagePack extension value."""

    id: str
    tag: msgspec.msgpack.Ext


def _raw_body(fmt: str) -> msgspec.Raw:
    """The deferred body ``{"x": 1}`` as raw bytes in ``fmt``'s own encoding."""
    return msgspec.Raw(msgspec.json.encode({"x": 1}) if fmt == "json" else msgspec.msgpack.encode({"x": 1}))


@pytest.mark.parametrize("fmt", ["json", "msgpack"])
@pytest.mark.parametrize(
    "branch, event, written",
    [
        ("an_unset_field_is_left_out", lambda fmt: Renamed("a"), {"id": "a"}),
        ("a_set_optional_field_is_written", lambda fmt: Renamed("a", "hi"), {"id": "a", "note": "hi"}),
        ("a_raw_field_is_written_as_it_is", lambda fmt: Deferred("a", _raw_body(fmt)), {"id": "a", "body": {"x": 1}}),
    ],
    ids=lambda row: row if isinstance(row, str) else "",
)  # fmt: skip
def test_event_codecs_pass_msgspec_markers_through_as_msgspec_writes_them(
    fmt: str, branch: str, event: Callable[[str], Any], written: dict[str, Any]
) -> None:
    """``UNSET`` and ``Raw`` are msgspec's own: msgspec leaves an ``UNSET`` field out and writes a ``Raw``
    verbatim. The codecs keep both, on write and on the read check, so such an event marshals, and one
    another writer stored reads back."""
    codec = _CODECS[fmt]({"Renamed": Renamed, "Deferred": Deferred})
    value = event(fmt)
    marshaled = codec.marshal(value)
    loads = msgspec.json.decode if fmt == "json" else msgspec.msgpack.decode
    assert loads(bytes(marshaled.data)) == written, branch
    assert codec.unmarshal(marshaled) == value, branch


def test_the_msgpack_codec_passes_an_extension_value_through() -> None:
    """A MessagePack extension value is the format's own: written as it is and read back as it was."""
    codec = msgpack_codec({"Tagged": Tagged})
    event = Tagged("a", msgspec.msgpack.Ext(7, b"\x01\x02"))
    assert codec.unmarshal(codec.marshal(event)) == event


def test_an_unset_field_is_left_out_of_every_encoded_value() -> None:
    """The one encoder writes a cached command result, a document and the command hash: an ``UNSET`` field
    is left out there too, as msgspec leaves it out."""
    assert _json.encode({"result": Renamed("a")}) == '{"result":{"id":"a"}}'


@dataclass
class OrderLine:
    """A read-model value with a plain default: the field ``msgspec.convert`` leaves out of ``__dict__``."""

    sku: str
    quantity: int
    currency: str = "EUR"


class OrderView(msgspec.Struct):
    order_id: str
    line: OrderLine


LINE_FIELDS: dict[str, Any] = {"sku": "sku-42", "quantity": 3}


@pytest.mark.parametrize(
    "branch, document, filter",
    [
        ("at_the_root", lambda line: line, F.eq("currency", "EUR")),
        ("in_a_dict", lambda line: {"order_id": "ord-7", "line": line}, F.eq("line.currency", "EUR")),
        ("in_a_struct", lambda line: OrderView("ord-7", line), F.eq("line.currency", "EUR")),
        (
            "in_a_list",
            lambda line: {"order_id": "ord-7", "lines": [line]},
            F.contains("lines", {**LINE_FIELDS, "currency": "EUR"}),
        ),
    ],
)
async def test_a_defaulted_field_encodes_matches_and_stores_however_the_value_was_built(
    branch: str, document: Callable[[OrderLine], Any], filter: F.Filter
) -> None:
    """Two equal values, one built by the constructor and one by ``msgspec.convert``, are one document: the
    same JSON text, the same filter verdict and the same stored row."""
    built = OrderLine("sku-42", 3)
    converted = msgspec.convert(LINE_FIELDS, OrderLine)
    assert built == converted and "currency" not in vars(converted), "the gap the encoder must fill"
    documents = [document(built), document(converted)]
    assert _json.encode(documents[0]) == _json.encode(documents[1]), branch
    assert [F.matches(filter, d) for d in documents] == [True, True], branch
    stored: list[list[Any]] = []
    for d in documents:
        store = MemoryProjectionStore()
        await store.save(Entity("view-1", 0, d))
        stored.append((await store.get_many(Q.Query(filter=filter))).rows)
    assert len(stored[0]) == 1 and stored[0] == stored[1], branch


class _NotJson:
    """A host value msgspec cannot encode."""


@pytest.mark.parametrize(
    "branch, value, native_raises, want",
    [
        pytest.param("ok", {"price": Decimal("9.50")}, None, '{"price":"9.5"}', id="the_native_call_gets_json_text"),
        pytest.param(
            "native_refuses",
            {"price": 1},
            RuntimeError("codec: no such operator"),
            "no such operator",
            id="a_native_refusal_passes_through_for_the_native_module_to_retype",
        ),
        pytest.param(
            "host_value_refused",
            {"price": _NotJson()},
            None,
            "value is not JSON-encodable",
            id="a_host_value_that_will_not_encode_never_reaches_the_native_call",
        ),
    ],
)  # fmt: skip
def test_json_call_branches(branch: str, value: Any, native_raises: Exception | None, want: str) -> None:
    """``_json.call`` encodes every host value before the native call, so an encoding error keeps its own
    message and the native function never runs on it. It retypes nothing: ``edax._native`` maps the native
    call's own failure."""
    calls: list[str] = []

    def native(text: str) -> str:
        calls.append(text)
        if native_raises is not None:
            raise native_raises
        return text

    if branch == "ok":
        assert _json.call(native, value) == want
        assert calls == [want]
        return
    with pytest.raises(RuntimeError if branch == "native_refuses" else edax.CodecError, match=want):
        _json.call(native, value)
    assert len(calls) == (1 if native_raises is not None else 0), branch


class _Plain(msgspec.Struct):
    sku: str
    quantity: int
    tags: tuple[str, ...] = ()


class _Timed(msgspec.Struct):
    sku: str
    at: dt.datetime


class _Stamped(msgspec.Struct):
    sku: str
    at: edax.Instant
    price: dict[edax.Decimal, int]


class _Loose(msgspec.Struct):
    sku: str
    price: Any


@dataclass
class _Line:
    sku: str
    currency: str = "EUR"


@pytest.mark.parametrize(
    "value, direct, want",
    [
        pytest.param(_Plain("sku-42", 3), True, b'{"sku":"sku-42","quantity":3,"tags":[]}', id="a_type_with_no_host_value_is_written_as_it_is"),
        pytest.param(_Timed("sku-42", dt.datetime(2024, 1, 1, 0, 30, tzinfo=dt.timezone.utc)), False, b'{"sku":"sku-42","at":"2024-01-01T00:30:00.000000Z"}', id="a_declared_datetime_is_walked_to_its_shared_form"),
        pytest.param(_Stamped("sku-42", edax.Instant("2024-01-01T01:30:00+01:00"), {edax.Decimal("9.50"): 1}), True, b'{"sku":"sku-42","at":"2024-01-01T00:30:00.000000Z","price":{"9.5":1}}', id="a_host_type_is_written_as_its_text_with_no_walk"),
        pytest.param(_Loose("sku-42", Decimal("9.50")), False, b'{"sku":"sku-42","price":"9.5"}', id="an_any_field_may_hold_a_decimal_so_it_is_walked"),
        pytest.param(_Line("sku-42"), False, b'{"sku":"sku-42","currency":"EUR"}', id="a_dataclass_is_walked_for_its_defaulted_fields"),
        pytest.param({"price": Decimal("9.50")}, False, b'{"price":"9.5"}', id="a_dict_holds_anything_so_it_is_walked"),
    ],
)  # fmt: skip
def test_the_encode_plan_is_decided_per_type(value: Any, direct: bool, want: bytes) -> None:
    """Whether a value needs the shared-form walk is read once off its type's declared fields. A type that
    holds no ``datetime``, ``Decimal`` or dataclass is written by msgspec as it is, and its bytes are exactly
    the walk's; a type that may hold one, an ``Any`` field included, is walked."""
    assert _json._plan(type(value)).direct is direct
    assert _json.encode_bytes(value) == want == msgspec.json.encode(_json._builtins(value, _json._OWN_FORM))


@pytest.mark.parametrize(
    "branch, build, want",
    [
        ("decimal_canonical_text_is_kept", lambda: edax.Decimal("9.5"), "9.5"),
        ("decimal_another_spelling_is_canonicalized", lambda: edax.Decimal("+0.95E+1"), "9.5"),
        ("decimal_refused_text_is_a_codec_error", lambda: edax.Decimal("1_000"), edax.CodecError),
        ("decimal_from_text_only", lambda: edax.Decimal(cast(Any, 9.5)), TypeError),
        ("decimal_from_decimal", lambda: edax.Decimal.from_decimal(Decimal("1E+2")), "100"),
        ("decimal_from_a_nan_decimal_is_refused", lambda: edax.Decimal.from_decimal(Decimal("NaN")), edax.CodecError),
        ("decimal_from_decimal_takes_a_decimal_only", lambda: edax.Decimal.from_decimal(cast(Any, "9.5")), TypeError),
        ("decimal_to_decimal_is_exact", lambda: edax.Decimal(str(WIDE)).to_decimal(), WIDE),
        ("instant_canonical_text_is_kept", lambda: edax.Instant("2024-01-01T00:30:00.000000Z"), "2024-01-01T00:30:00.000000Z"),
        ("instant_an_offset_converts_to_utc", lambda: edax.Instant("2024-01-01T01:30:00+01:00"), "2024-01-01T00:30:00.000000Z"),
        ("instant_refused_text_is_a_codec_error", lambda: edax.Instant("2024-01-01T00:30:00"), edax.CodecError),
        ("instant_from_text_only", lambda: edax.Instant(cast(Any, AT)), TypeError),
        ("instant_from_datetime", lambda: edax.Instant.from_datetime(AT.astimezone(MINUS_FIVE)), "2024-01-01T00:00:00.000000Z"),
        ("instant_from_a_naive_datetime_is_refused", lambda: edax.Instant.from_datetime(dt.datetime(2024, 1, 1)), edax.CodecError),
        ("instant_from_datetime_takes_a_datetime_only", lambda: edax.Instant.from_datetime(cast(Any, dt.date(2024, 1, 1))), TypeError),
        ("instant_to_datetime_is_aware_utc", lambda: edax.Instant("2024-01-01T01:30:00+01:00").to_datetime(), dt.datetime(2024, 1, 1, 0, 30, tzinfo=UTC)),
        # The kernel reads year 0000, which a Python datetime cannot hold.
        ("instant_year_zero_has_no_datetime", lambda: edax.Instant("0001-01-01T00:30:00+01:00").to_datetime(), edax.CodecError),
    ],
)  # fmt: skip
def test_host_type_construction_and_conversion(branch: str, build: Callable[[], Any], want: Any) -> None:
    """A host value is built from text by the kernel, or converted from and to the Python type that does the
    arithmetic. Refused text is the kernel's terminal ``CodecError``; a value that is not text is a
    ``TypeError``, a programming error."""
    if isinstance(want, type) and issubclass(want, Exception):
        with pytest.raises(want):
            build()
        return
    got = build()
    assert (str(got) if isinstance(want, str) else got) == want, branch
    if isinstance(got, dt.datetime):
        assert got.tzinfo is not None, branch


@pytest.mark.parametrize(
    "branch, low, high",
    [
        # Text order is the other way round: "10" < "9.5".
        ("decimal_orders_by_value_not_text", edax.Decimal("9.5"), edax.Decimal("10")),
        ("decimal_negative_below_positive", edax.Decimal("-10"), edax.Decimal("-9.5")),
        ("instant_orders_by_instant_not_wall_time", edax.Instant("2024-01-01T01:00:00+01:00"), edax.Instant("2023-12-31T19:30:00-05:00")),
    ],
)  # fmt: skip
def test_host_types_order(branch: str, low: Any, high: Any) -> None:
    assert low < high and high > low and low <= high and not high <= low, branch
    assert sorted([high, low]) == [low, high], branch


@pytest.mark.parametrize(
    "branch, value, same, other",
    [
        ("decimal", edax.Decimal("9.50"), edax.Decimal("9.5"), edax.Decimal("9.51")),
        ("instant", edax.Instant("2024-01-01T01:30:00+01:00"), edax.Instant("2024-01-01T00:30:00Z"), edax.Instant("2024-01-01T00:30:00.000001Z")),
    ],
)  # fmt: skip
def test_host_types_are_immutable_value_objects(branch: str, value: Any, same: Any, other: Any) -> None:
    """Equal by value, hashed alike, never equal to their text or to the Python type, unchangeable, and
    rebuilt through the kernel by copy and pickle."""
    assert value == same and hash(value) == hash(same) and value != other, branch
    assert value != str(value) and len({value, same, other}) == 2, branch
    assert value != (value.to_decimal() if isinstance(value, edax.Decimal) else value.to_datetime()), branch
    with pytest.raises(AttributeError):
        value._text = "0"
    with pytest.raises(AttributeError):
        del value._text
    for copied in (copy.copy(value), copy.deepcopy(value), pickle.loads(pickle.dumps(value))):
        assert copied == value and type(copied) is type(value), branch
    assert repr(value) == f"edax.{type(value).__name__}({str(value)!r})", branch

"""The thin SDK wrappers that sit straight on the kernel — query, upcast, causation,
router — plus the two host-side pieces on the same path: the event codec registry and the error
taxonomy every layer branches on."""

import asyncio
import json
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, get_args

import edax
import msgspec
import pytest
from conftest import Tick, build_message, golden, wire_of
from edax import _edax, _names, _native, upcast
from edax import query as Q
from edax._owned import ThreadBound
from edax.codec import json_codec, msgpack_codec
from edax.errors import _BY_CODE, from_wire
from edax.observe import CLAIM_VERDICTS


def _golden_page(case: dict[str, Any]) -> Q.Page:
    """A golden query through the SDK's :class:`~edax.query.Query`, or as raw spec text when it holds a key
    no field spells (a typo the kernel must refuse)."""
    spec = case["query"]
    if not set(spec) <= {"filter", "sort", "offset", "limit", "after"}:
        with edax.errors.mapped():
            return Q.Page.from_kernel(_edax.query(json.dumps(spec), json.dumps(case["documents"])))
    return Q.query(case["documents"], Q.Query(**spec))


def test_golden_query() -> None:
    """The page and its ``next`` cursor, byte for byte, and every refusal as the terminal
    ``CodecError``: a cursor under another sort or without one, a ``next`` from a row with no id, a typo."""
    for c in golden("query.json"):
        if c["error"] is not None:
            with pytest.raises(edax.CodecError) as caught:
                _golden_page(c)
            assert wire_of(caught.value) == c["error"], c["name"]
            continue
        page = _golden_page(c)
        assert {"rows": page.rows, "next": page.next} == c["expected_page"], c["name"]
        if page.next is not None:
            # A backend that pages natively mints the same cursor from the same last row.
            assert Q.encode_cursor(page.rows[-1], c["query"]["sort"]) == page.next, c["name"]


def test_golden_cursor() -> None:
    """The decode half of the cursor contract, the sort a page is really ordered by, and the refusals.

    A backend binds `decode_cursor`'s tuple straight into its own keyset `WHERE` clause and orders by
    `effective_sort`, so a binding that disagrees on either pages differently from the kernel while
    every cursor still looks well formed. A cursor read under a sort it was not minted under is refused
    with the kernel's own text.
    """
    for c in golden("cursor.json"):
        sort = c["sort"]
        assert Q.effective_sort(sort) == c["expected_effective_sort"], c
        if c["error"] is not None:
            with pytest.raises(edax.CodecError) as caught:
                if c["expected_cursor"] is not None:
                    Q.decode_cursor(c["expected_cursor"], sort)
                else:
                    Q.encode_cursor(c["document"], sort)
            assert f"{caught.value.code}: {caught.value}" == c["error"], c["name"]
            continue
        assert Q.decode_cursor(c["expected_cursor"], sort) == c["expected_tuple"], c
        # Round trip: the cursor minted here is the one the vector pins.
        assert Q.encode_cursor(c["document"], sort) == c["expected_cursor"], c


def test_query_json_matches_the_golden_query_shape() -> None:
    """Python builds the query spec itself, so the golden is the only thing holding it to the kernel's one
    ``Query`` shape. Rebuilding each golden spec from its own fields must reproduce it exactly: a renamed
    key, an extra key, or a field emitted when it was not set all fail here."""
    seen: set[str] = set()
    for c in golden("query.json"):
        spec = c["query"]
        if c["name"] == "refused_unknown_field":
            continue  # a typo the kernel refuses, which no constructor here can build
        seen |= set(spec)
        assert json.loads(Q.Query(**spec).to_json()) == spec, c
    assert seen == {"filter", "sort", "offset", "limit", "after"}, "the golden must set every field"


def test_golden_upcast() -> None:
    for c in golden("upcast.json"):
        if c["error"] is not None:
            # The kernel's own wire form, `codec:` included: the raw call carries the code, and the
            # pinned message names the step, so a binding that refused for another reason fails here.
            with pytest.raises(RuntimeError) as caught:
                _edax.upcast_plan(c["from"], c["to"], c["available"])
            assert str(caught.value) == c["error"], c
            with pytest.raises(edax.CodecError) as mapped, edax.errors.mapped():
                _edax.upcast_plan(c["from"], c["to"], c["available"])
            assert f"{mapped.value.code}: {mapped.value}" == c["error"], c
        else:
            assert _edax.upcast_plan(c["from"], c["to"], c["available"]) == c["plan"], c
        # The same steps decide the version a codec writes for the name.
        if c["target_error"] is not None:
            with pytest.raises(edax.CodecError) as mapped, edax.errors.mapped():
                _edax.upcast_target(c["available"])
            assert f"{mapped.value.code}: {mapped.value}" == c["target_error"], c
        else:
            assert _edax.upcast_target(c["available"]) == c["target"], c


@pytest.mark.parametrize(
    "branch, version, want",
    [
        ("negative", -1, edax.BadHandle),
        ("past_u32", 2**32, edax.BadHandle),
        ("not_an_int", "1", edax.BadHandle),
        ("a_bool", True, edax.BadHandle),
    ],
)
def test_a_schema_version_outside_u32_is_refused_at_wiring(
    branch: str, version: Any, want: type[Exception]
) -> None:
    """A schema version is an unsigned 32-bit integer on the wire. A step outside it would fail every
    event it raises, so the registration refuses it, not the first unmarshal as a terminal CodecError."""
    with pytest.raises(want):
        upcast.Upcaster().step(version, lambda d: d)


def test_the_u32_bounds_are_accepted() -> None:
    upcast.Upcaster().step(0, lambda d: d).step(2**32 - 1, lambda d: d)
    last = {"Opened": upcast.Upcaster().step(2**32 - 2, lambda d: d)}
    assert edax.json_codec({"Opened": dict}, upcasters=last).marshal({}).version == 2**32 - 1


def test_a_second_transform_for_one_step_is_refused() -> None:
    # Keeping either one silently would run a migration nobody chose.
    upcaster = upcast.Upcaster().step(1, lambda d: d + b"a")
    with pytest.raises(edax.AlreadyRegistered, match="upcast step 1 is registered twice") as caught:
        upcaster.step(1, lambda d: d + b"b")
    assert caught.value.terminal is True
    assert upcaster.to(b"x", from_=1, to=2) == b"xa", "the first transform is kept"


def _two_step_upcaster() -> upcast.Upcaster:
    """v1 -> v2 -> v3 for one event type; `step` is chainable, so registration reads as the chain."""
    return upcast.Upcaster().step(1, lambda d: d + b"1").step(2, lambda d: d + b"2")


@pytest.mark.parametrize(
    "branch, from_, to, want, want_error",
    [
        ("every_step_runs_in_kernel_planned_order", 1, 3, b"x12", None),
        ("one_step", 2, 3, b"x2", None),
        # Already current: the kernel plans nothing, so the payload must come back untouched rather
        # than have the v2 step re-applied.
        ("already_current_applies_nothing", 3, 3, b"x", None),
        # Both failures are deterministic, so they must surface as *terminal* CodecErrors: the
        # dispatcher dead-letters on the first one instead of nacking the same event forever.
        ("missing_step_is_terminal", 1, 4, None, "missing upcast step 3"),
        ("downward_is_terminal", 3, 1, None, "cannot upcast downward"),
    ],
)
def test_upcaster_to_branches(
    branch: str, from_: int, to: int, want: bytes | None, want_error: str | None
) -> None:
    upcaster = _two_step_upcaster()
    if want_error is not None:
        with pytest.raises(edax.CodecError, match=want_error) as caught:
            upcaster.to(b"x", from_=from_, to=to)
        assert caught.value.terminal is True
        return
    assert upcaster.to(b"x", from_=from_, to=to) == want


# =========================================================================
# errors: the native "<code>: <message>" string -> the typed taxonomy
# =========================================================================


@pytest.mark.parametrize(
    "branch, message, want_type, want_message, want_terminal",
    [
        (
            "concurrency_prefix",
            "concurrency: expected 3, stored 4",
            edax.ConcurrencyError,
            "expected 3, stored 4",
            False,
        ),
        (
            "codec_prefix",
            "codec: invalid type: string, expected i64",
            edax.CodecError,
            "invalid type: string, expected i64",
            True,
        ),
        (
            "store_prefix",
            "store: connection to the event store was reset",
            edax.StoreError,
            "connection to the event store was reset",
            False,
        ),
        # An unknown code must not be guessed at: the whole message is kept under StoreError, so a
        # future kernel code is never silently read as a concurrency conflict.
        (
            "unknown_prefix",
            "timeout: deadline exceeded after 5s",
            edax.StoreError,
            "timeout: deadline exceeded after 5s",
            False,
        ),
        ("no_separator", "connection reset by peer", edax.StoreError, "connection reset by peer", False),
        # The separator is ": ", not ":" — a bare colon is part of the message, not a code.
        ("colon_without_space", "codec:no space", edax.StoreError, "codec:no space", False),
        ("empty_message", "", edax.StoreError, "", False),
    ],
)
def test_from_wire_maps_the_native_code(
    branch: str, message: str, want_type: type, want_message: str, want_terminal: bool
) -> None:
    err = from_wire(message)
    assert type(err) is want_type
    assert str(err) == want_message
    assert err.terminal is want_terminal


ERROR_WIRE: list[dict[str, Any]] = golden("error_wire.json")


@pytest.mark.parametrize("case", ERROR_WIRE, ids=lambda c: c["name"])
def test_split_error_and_the_verdict_are_the_kernels(case: dict[str, Any]) -> None:
    """Every row of `error_wire.json`, the negative ones included: `from_wire` splits through the
    kernel, so the ": " separator and the class of a string with no code the kernel knows have one
    owner, read here rather than chosen."""
    split = _edax.split_error(case["wire"])
    assert (split.code, split.message, split.terminal) == (case["code"], case["message"], case["terminal"])
    err = from_wire(case["wire"])
    assert (err.code, str(err)) == (case["code"], case["message"])
    assert edax.errors.is_terminal(err) is case["terminal"], "the class carries the kernel's verdict"


def test_the_unclassified_default_is_the_kernels() -> None:
    """A host exception carries no verdict: `is_terminal` falls back to the kernel's default, the
    verdict of the class an unknown code maps to."""
    assert edax.errors.UNCLASSIFIED_IS_TERMINAL is _edax.error_verdict(None) is False
    assert edax.errors.is_terminal(ValueError("a host bug")) is edax.errors.UNCLASSIFIED_IS_TERMINAL
    assert edax.StoreError.terminal is edax.errors.UNCLASSIFIED_IS_TERMINAL


def test_the_import_fails_when_a_kernel_code_has_no_class(monkeypatch: pytest.MonkeyPatch) -> None:
    """A kernel code with no class would leave `from_wire` with nothing to raise."""
    codes = SimpleNamespace(error_codes=lambda: [*_edax.error_codes(), "quota_exceeded"])
    monkeypatch.setattr(edax.errors, "_edax", codes)
    with pytest.raises(ImportError, match="quota_exceeded"):
        edax.errors._check_every_kernel_code_has_a_class()


@pytest.mark.parametrize(
    ("ours", "kernel"),
    [
        pytest.param(tuple(mode.value for mode in edax.Tenancy), _edax.tenancy_modes, id="tenancy_modes"),
        pytest.param(
            edax.idempotency._RECORD_STATES, _edax.idempotency_states, id="idempotency_record_states"
        ),
        pytest.param(CLAIM_VERDICTS, _edax.idempotency_outcomes, id="claim_verdicts"),
        pytest.param(edax.saga._SAGA_WRITES, _edax.saga_write_actions, id="saga_write_actions"),
        pytest.param(edax.saga._SAGA_ADMITS, _edax.saga_admit_actions, id="saga_admit_actions"),
        pytest.param(get_args(edax.message.WireFormat), _edax.wire_formats, id="wire_formats"),
        pytest.param(get_args(edax.message.MessageKind), _edax.message_kinds, id="message_kinds"),
    ],
)
def test_every_closed_set_is_the_kernels(ours: tuple[str, ...], kernel: Callable[[], list[str]]) -> None:
    """The SDK spells each closed set as a ``Literal`` a type checker sees, and the import checks it against
    the kernel's list: equal passes, and a name the kernel adds fails the import."""
    assert sorted(ours) == sorted(kernel())
    _names.check(ours, kernel(), "names")
    with pytest.raises(ImportError, match="the kernel names"):
        _names.check(ours, [*kernel(), "added"], "names")


@pytest.mark.parametrize(
    ("value", "want"),
    [
        pytest.param("save", "save", id="a_known_name_is_typed_as_itself"),
        pytest.param("bogus", edax.CodecError, id="an_unknown_name_is_a_codec_error"),
    ],
)
def test_named_narrows_a_kernel_name_or_refuses_it(value: str, want: str | type[Exception]) -> None:
    names = ("nothing", "save", "complete")
    if isinstance(want, str):
        assert _names.named(value, names, "saga write action") == want
        return
    with pytest.raises(want, match="unknown saga write action 'bogus'"):
        _names.named(value, names, "saga write action")


# The kernel's error taxonomy: one row per code with the verdict every SDK persists on. The Rust
# SDK maps its error enums to the kernel's `ErrorCode` exhaustively, so a new code fails to compile
# there; this SDK reads the same table through `_edax` and refuses to import with a code unmapped.
# These tests pin that the classes, the kernel, and the committed golden agree row by row, so a code
# changing sides anywhere fails in one obvious place instead of flipping a dead-letter into an
# infinite redelivery or a released claim into a poisoned key.
ERROR_TAXONOMY: list[dict[str, Any]] = golden("error_taxonomy.json")


def _verdict_id(case: dict[str, Any]) -> str:
    return f"{case['code']}_is_{'terminal' if case['terminal'] else 'transient'}"


def test_error_classes_are_the_kernel_code_set() -> None:
    kernel_codes = list(_edax.error_codes())
    assert [c["code"] for c in ERROR_TAXONOMY] == kernel_codes, (
        "the kernel's error codes drifted from testdata/golden/error_taxonomy.json: regenerate with "
        "EDAX_REGEN=1 mise exec -- cargo test -p edax-core --test golden"
    )
    assert set(_BY_CODE) == set(kernel_codes)


@pytest.mark.parametrize("case", ERROR_TAXONOMY, ids=_verdict_id)
def test_each_code_keeps_the_kernel_verdict(case: dict[str, Any]) -> None:
    """One case per code: the class it maps to carries the golden's verdict, and the golden's wire
    form parses back to that class with the code stripped off the message."""
    cls = _BY_CODE[case["code"]]
    assert cls.code == case["code"]
    assert cls.terminal is case["terminal"]
    assert _edax.error_verdict(case["code"]) is case["terminal"]
    err = from_wire(case["wire"])
    assert type(err) is cls
    assert str(err) == case["message"]
    assert edax.errors.is_terminal(err) is case["terminal"]
    # A fatal code stops a consumer and is never cached: the kernel's one predicate, read by both.
    assert edax.errors.is_fatal(err) is case["fatal"]


def test_a_host_subclass_inherits_its_parents_code_and_verdict() -> None:
    """A host may specialise a class without naming a code; it keeps the parent's kind and verdict
    and does not enter the code table, so the parent stays what a native code maps to."""

    class LedgerCodecError(edax.CodecError):
        pass

    assert (LedgerCodecError.code, LedgerCodecError.terminal) == ("codec", True)
    assert _BY_CODE["codec"] is edax.CodecError


def test_a_class_with_a_code_the_kernel_does_not_know_cannot_be_defined() -> None:
    """The verdict comes from the kernel, so a class naming a code the kernel never pinned has no
    verdict to carry: defining it raises instead of defaulting."""
    with pytest.raises(RuntimeError, match="codec: unknown error code: timeout"):

        class TimeoutError_(edax.EdaxError, code="timeout"):
            pass


def test_an_unmapped_code_stays_transient_and_keeps_the_code_in_the_message() -> None:
    """The default the invariant above exists to keep unreachable. Transient is deliberate: a
    terminal default would cache a failed idempotency record under the key, so re-driving the same
    command replays `CachedFailure` forever — a poisoned key needing manual repair. A transient
    default only redelivers, which is visible and recoverable once the code is mapped. The code must
    survive into the message so an operator can see which one went unmapped."""
    err = from_wire("throttled: quota exhausted, retry after 30s")
    assert type(err) is edax.StoreError
    assert edax.errors.is_terminal(err) is False
    assert str(err) == "throttled: quota exhausted, retry after 30s"


# =========================================================================
# codec: the event registry marshals by type and unmarshals by name
# =========================================================================


class Deposited(msgspec.Struct):
    account_id: str
    amount_cents: int
    currency: str
    occurred_at: str


class Withdrawn(msgspec.Struct):
    account_id: str
    amount_cents: int
    currency: str
    occurred_at: str


class Noted(msgspec.Struct):
    account_id: str
    note: Any


class Line(msgspec.Struct, array_like=True):
    """A struct msgspec writes as an array, not a map."""

    sku: str


class Shipped(msgspec.Struct):
    order_id: str
    lines: list[Line]


REGISTRY: dict[str, type] = {"Deposited": Deposited, "Withdrawn": Withdrawn, "Noted": Noted}
DEPOSITED = Deposited("acct-1", 4200, "EUR", "2024-05-01T12:00:00Z")
WITHDRAWN = Withdrawn("acct-1", 999, "GBP", "2024-05-02T09:30:00Z")

# v2 added `currency`; the v1 upcast step defaults it, so a stored v1 payload still decodes.
DEPOSITED_V1 = b'{"account_id":"acct-1","amount_cents":4200,"occurred_at":"2024-05-01T12:00:00Z"}'
DEPOSITED_V2 = (
    b'{"account_id":"acct-1","amount_cents":4200,"currency":"USD","occurred_at":"2024-05-01T12:00:00Z"}'
)
WITHDRAWN_V2 = (
    b'{"account_id":"acct-1","amount_cents":999,"currency":"GBP","occurred_at":"2024-05-02T09:30:00Z"}'
)


def _default_currency(data: bytes) -> bytes:
    d = json.loads(data)
    d["currency"] = "EUR"
    return json.dumps(d).encode()


CURRENCY_STEP = {"Deposited": upcast.Upcaster().step(1, _default_currency)}
GAPPED_STEP = {"Withdrawn": upcast.Upcaster().step(2, _default_currency)}  # no v1 -> v2 step


@pytest.mark.parametrize(
    "branch, factory, event, want_version, want_error",
    [
        # Deposited has a one-step upcaster, so it writes v2; Withdrawn has none, so it writes v1.
        ("registered_type_with_an_upcaster_json", json_codec, DEPOSITED, 2, None),
        ("registered_type_without_an_upcaster_msgpack", msgpack_codec, WITHDRAWN, 1, None),
        # Terminal, not a bare KeyError: retrying an unregistered type just repeats it.
        ("unregistered_type", json_codec, Tick(7), None, "no event name registered for Tick"),
        ("unencodable_payload", json_codec, Noted("acct-1", object()), None, "encoding Noted"),
    ],
)
def test_marshal_branches(
    branch: str, factory: Callable[..., Any], event: Any, want_version: int | None, want_error: str | None
) -> None:
    codec = factory(REGISTRY, upcasters=CURRENCY_STEP)
    if want_error is not None:
        with pytest.raises(edax.CodecError, match=want_error) as caught:
            codec.marshal(event)
        assert caught.value.terminal is True
        return
    marshaled = codec.marshal(event)
    assert (marshaled.name, marshaled.version) == (type(event).__name__, want_version)
    assert codec.unmarshal(marshaled) == event  # the bytes are the event, whole


@pytest.mark.parametrize(
    "branch, events, upcasters, want_error",
    [
        ("distinct_types", REGISTRY, CURRENCY_STEP, None),
        # Every SDK reads a struct from a map only, so an array_like one would store bytes only Python reads.
        (
            "a_positional_struct",
            {**REGISTRY, "Line": Line},
            None,
            r"'Line' holds array_like structs \['Line'\]",
        ),
        (
            "a_nested_positional_struct",
            {**REGISTRY, "Shipped": Shipped},
            None,
            r"'Shipped' holds .* \['Line'\]",
        ),
        # Two names for one type: inverting the map would keep only the last, and every Deposited
        # would then be stored under "Credited". The store keeps that wrong name forever.
        (
            "one_type_under_two_names",
            {"Deposited": Deposited, "Credited": Deposited},
            None,
            "Deposited is registered under two event names: 'Deposited' and 'Credited'",
        ),
        # A typo'd upcaster key never fires, so the v1 payload it was written for decodes as-is and
        # reaches the handler missing `currency`. Silent, and only on the old rows.
        (
            "upcaster_for_an_unregistered_name",
            REGISTRY,
            {"Deposted": upcast.Upcaster().step(1, _default_currency)},
            r"unknown event names: \['Deposted'\]",
        ),
        # A hole in the chain: a payload stored at v2 could never be raised, so the codec is refused
        # at startup rather than on the first v2 row.
        (
            "upcaster_with_a_hole",
            REGISTRY,
            {"Deposited": upcast.Upcaster().step(1, _default_currency).step(3, _default_currency)},
            "the upcaster for 'Deposited': upcast steps skip v2",
        ),
        # The last step's output would have no schema version.
        (
            "upcaster_step_at_the_u32_ceiling",
            REGISTRY,
            {"Deposited": upcast.Upcaster().step(2**32 - 1, _default_currency)},
            "upcast step 4294967295 raises past the largest schema version",
        ),
    ],
)
def test_registry_is_validated_at_construction(
    branch: str,
    events: dict[str, type],
    upcasters: dict[str, Any] | None,
    want_error: str | None,
) -> None:
    if want_error is not None:
        with pytest.raises(edax.EdaxError, match=want_error) as caught:
            json_codec(events, upcasters=upcasters)
        assert caught.value.terminal is True, "a wiring mistake repeats on every retry"
        return
    codec = json_codec(events, upcasters=upcasters)
    assert codec.marshal(DEPOSITED).name == "Deposited"


@pytest.mark.parametrize(
    "branch, upcasters, name, version, data, want, want_error",
    [
        # Registered for Deposited but the payload is already current: the kernel plans no steps, so
        # `currency` stays the stored USD rather than the step's EUR.
        (
            "current_version_plans_no_steps",
            CURRENCY_STEP,
            "Deposited",
            2,
            DEPOSITED_V2,
            Deposited("acct-1", 4200, "USD", "2024-05-01T12:00:00Z"),
            None,
        ),
        (
            "older_version_is_upcast",
            CURRENCY_STEP,
            "Deposited",
            1,
            DEPOSITED_V1,
            Deposited("acct-1", 4200, "EUR", "2024-05-01T12:00:00Z"),
            None,
        ),
        # No upcaster for this name, so it is at v1: an older payload is decoded as-is, which only works
        # while the shape has not changed.
        ("older_version_without_upcaster", {}, "Withdrawn", 0, WITHDRAWN_V2, WITHDRAWN, None),
        (
            "unknown_event_name",
            CURRENCY_STEP,
            "AccountClosed",
            2,
            DEPOSITED_V2,
            None,
            "no type registered for event 'AccountClosed'",
        ),
        ("corrupt_payload", CURRENCY_STEP, "Deposited", 2, b'{"account_id":', None, "decoding Deposited v2"),
        (
            "missing_upcast_step",
            GAPPED_STEP,
            "Withdrawn",
            1,
            WITHDRAWN_V2,
            None,
            "upcasting Withdrawn v1->v3",
        ),
        # Written by a newer deployment than this codec knows: the kernel rejects the backwards plan
        # rather than letting an unknown shape decode as if it were current.
        (
            "newer_version_than_the_codec",
            CURRENCY_STEP,
            "Deposited",
            3,
            DEPOSITED_V2,
            None,
            "upcasting Deposited v3->v2",
        ),
        # The same newer writer for a name with no upcaster: still the kernel's refusal, never a silent
        # decode that drops the fields this reader does not know.
        (
            "newer_version_without_upcaster",
            {},
            "Withdrawn",
            5,
            WITHDRAWN_V2,
            None,
            "cannot upcast downward: from 5 > to 1",
        ),
    ],
)
def test_unmarshal_branches(
    branch: str,
    upcasters: dict,
    name: str,
    version: int,
    data: bytes,
    want: Any,
    want_error: str | None,
) -> None:
    codec = json_codec(REGISTRY, upcasters=upcasters)
    event = edax.Payload(name, version, data)
    if want_error is not None:
        with pytest.raises(edax.CodecError, match=want_error) as caught:
            codec.unmarshal(event)
        assert caught.value.terminal is True  # a poison event dead-letters, it never loops
        return
    assert codec.unmarshal(event) == want


def _raising_step(error: Exception) -> dict[str, upcast.Upcaster]:
    """Deposited's v1 -> v2 step failing with ``error``: a step that looks up the currency it adds."""

    def step(data: bytes) -> bytes:
        raise error

    return {"Deposited": upcast.Upcaster().step(1, step)}


@pytest.mark.parametrize(
    "branch, error, want_type, want_terminal",
    [
        # Its lookup service is down: a retry can read the payload, so the step's transient error stays.
        ("a_step_raising_a_transient_edax_error_keeps_it", edax.StoreError("rates unreachable"), edax.StoreError, False),
        ("a_step_raising_a_terminal_edax_error_keeps_it", edax.Rejected("no rate for XTS"), edax.Rejected, True),
        # An untyped failure says nothing about a retry: the codec's terminal error, named by the upcast.
        ("a_step_raising_an_untyped_error_is_a_codec_error", KeyError("currency"), edax.CodecError, True),
    ],
)  # fmt: skip
def test_an_upcast_step_failure_keeps_its_code(
    branch: str, error: Exception, want_type: type[Exception], want_terminal: bool
) -> None:
    """As the Rust SDK's upcast step returns its own error: only an untyped failure becomes a codec one."""
    codec = json_codec(REGISTRY, upcasters=_raising_step(error))
    with pytest.raises(want_type) as caught:
        codec.unmarshal(edax.Payload("Deposited", 1, DEPOSITED_V1))
    assert type(caught.value) is want_type, branch
    assert edax.is_terminal(caught.value) is want_terminal, branch
    if want_type is edax.CodecError:
        assert "upcasting Deposited v1->v2" in str(caught.value), branch
    else:
        assert caught.value is error, "the step's own error, not a rebuilt one"


def _withdrawal_currency(data: bytes) -> bytes:
    """Withdrawn's own v1 -> v2 step: it gained `currency` later than Deposited did."""
    d = json.loads(data)
    d["currency"] = "GBP"
    return json.dumps(d).encode()


# Deposited changed shape at v2. Later only Withdrawn changes, so it gets a step of its own.
BUMPED = {**CURRENCY_STEP, "Withdrawn": upcast.Upcaster().step(1, _withdrawal_currency)}
WITHDRAWN_V1 = b'{"account_id":"acct-1","amount_cents":999,"occurred_at":"2024-05-02T09:30:00Z"}'


@pytest.mark.parametrize(
    "branch, name, version, data, want",
    [
        ("unchanged_name_reads_its_v2_row", "Deposited", 2, DEPOSITED_V2, Deposited("acct-1", 4200, "USD", "2024-05-01T12:00:00Z")),
        ("unchanged_name_still_raises_its_v1_row", "Deposited", 1, DEPOSITED_V1, DEPOSITED),
        ("bumped_name_raises_its_v1_row", "Withdrawn", 1, WITHDRAWN_V1, WITHDRAWN),
        ("bumped_name_reads_its_v2_row", "Withdrawn", 2, WITHDRAWN_V2, WITHDRAWN),
        ("name_without_an_upcaster_reads_its_v1_row", "Noted", 1, b'{"account_id":"acct-1","note":"hi"}', Noted("acct-1", "hi")),
    ],
)  # fmt: skip
def test_bumping_one_name_moves_no_other(
    branch: str, name: str, version: int, data: bytes, want: Any
) -> None:
    """Each name has its own schema version. Under one registry version, bumping Withdrawn to v3 made
    Deposited's upcaster miss step 2, and its v1 and v2 rows failed as terminal CodecErrors."""
    codec = json_codec(REGISTRY, upcasters=BUMPED)
    assert [codec.marshal(e).version for e in (DEPOSITED, WITHDRAWN, Noted("acct-1", "hi"))] == [2, 2, 1]
    assert codec.unmarshal(edax.Payload(name, version, data)) == want


# =========================================================================
# query.Query.to_json: only the set fields reach the kernel
# =========================================================================

FILTER = edax.filter.and_(edax.filter.eq("status", "settled"), edax.filter.gt("balance_cents", 1000))
SORT = [Q.sort_key("balance_cents", desc=True), Q.sort_key("account_id")]
CURSOR = "W3sicGF0aCI6ImJhbGFuY2VfY2VudHMifV0="


@pytest.mark.parametrize(
    "branch, kwargs, want",
    [
        # The "no constraints" spec. Rust's `Query::default()` serializes to `{}` too; a binding that
        # emitted `{"offset": 0}` here would hand a backend a document the other never sends.
        ("nothing_set", {}, {}),
        ("filter_set", {"filter": FILTER}, {"filter": FILTER}),
        ("sort_set", {"sort": SORT}, {"sort": SORT}),
        ("sort_empty_is_omitted", {"sort": []}, {}),
        ("offset_set", {"offset": 20}, {"offset": 20}),
        ("offset_zero_is_omitted", {"offset": 0}, {}),
        ("limit_set", {"limit": 50}, {"limit": 50}),
        # limit is tested against None, not truthiness: limit=0 is "an empty page", not "no limit".
        ("limit_zero_is_kept", {"limit": 0}, {"limit": 0}),
        ("after_set", {"sort": SORT, "after": CURSOR}, {"sort": SORT, "after": CURSOR}),
        # after is tested against None, not truthiness: "" (from `?after=`) reaches the kernel, which
        # refuses it as it refuses it from every other binding.
        ("after_empty_is_kept", {"after": ""}, {"after": ""}),
        (
            "every_field_set",
            {"filter": FILTER, "sort": SORT, "offset": 20, "limit": 50, "after": CURSOR},
            {"filter": FILTER, "sort": SORT, "offset": 20, "limit": 50, "after": CURSOR},
        ),
    ],
)
def test_query_json_branches(branch: str, kwargs: dict[str, Any], want: dict[str, Any]) -> None:
    assert json.loads(Q.Query(**kwargs).to_json()) == want, branch


# =========================================================================
# router: dispatch by event name
# =========================================================================


def _router_case(kind: str, seen: list[edax.EventMessage]) -> edax.EventRouter:
    async def handle(env: edax.EventMessage) -> None:
        await asyncio.sleep(0)
        seen.append(env)

    router = edax.EventRouter(on_unhandled=handle if kind == "fallback" else None)
    if kind == "register":
        router.register("Deposited", handle)
    elif kind == "decorator":
        router.on("Deposited")(handle)
    return router


@pytest.mark.parametrize(
    "branch, kind, event_name, want_names",
    [
        ("registered_handler_is_awaited", "register", "Deposited", ["Deposited"]),
        ("decorator_registers", "decorator", "Deposited", ["Deposited"]),
        ("unhandled_falls_back", "fallback", "AccountClosed", ["AccountClosed"]),
        ("unhandled_without_fallback_is_dropped", "none", "AccountClosed", []),
    ],
)
def test_router_dispatch_branches(branch: str, kind: str, event_name: str, want_names: list[str]) -> None:
    seen: list[edax.EventMessage] = []
    router = _router_case(kind, seen)
    env = build_message("acct-1", event_name, 3, data=DEPOSITED_V2, metadata={"trace": "abc"})
    asyncio.run(router.dispatch(env))
    assert [e.name for e in seen] == want_names
    assert all(e.id == env.id for e in seen)  # the handler gets the message, not a copy


async def _ignore(env: edax.EventMessage) -> None:
    """An event handler with nothing to do."""


class _AsyncCall:
    """A handler object whose ``__call__`` is a coroutine function: accepted like a coroutine function."""

    async def __call__(self, env: edax.EventMessage) -> None:
        return None


@pytest.mark.parametrize(
    "where, handler, want",
    [
        pytest.param("register", _ignore, None, id="a_coroutine_function_is_accepted"),
        pytest.param("register", _AsyncCall(), None, id="an_object_with_an_async_call_is_accepted"),
        pytest.param(
            "register", lambda env: None, "event handler for 'Deposited'", id="a_sync_handler_is_refused"
        ),
        pytest.param(
            "decorator", print, "event handler for 'Deposited'", id="a_sync_decorated_handler_is_refused"
        ),
        pytest.param("on_unhandled", lambda env: None, "on_unhandled", id="a_sync_fallback_is_refused"),
    ],
)
def test_the_router_takes_only_async_handlers(where: str, handler: Any, want: str | None) -> None:
    """As the command bus refuses a sync handler, so does the router: at wiring time, not at the first
    event, and before a sync call could block the event loop."""

    def wire() -> None:
        if where == "on_unhandled":
            edax.EventRouter(on_unhandled=handler)
        elif where == "decorator":
            edax.EventRouter().on("Deposited")(handler)
        else:
            edax.EventRouter().register("Deposited", handler)

    if want is None:
        wire()
        return
    with pytest.raises(TypeError, match=f"^{want} must be a coroutine function"):
        wire()


def test_register_rejects_a_second_handler_for_one_name() -> None:
    router = edax.EventRouter()
    router.register("Deposited", _ignore)
    # The same wiring mistake as CommandBus.register, so the same typed terminal error: a startup
    # misconfiguration must never be retried.
    with pytest.raises(edax.AlreadyRegistered, match=r"^Deposited$"):
        router.register("Deposited", _ignore)


# =========================================================================
# is_terminal / mapped: the single terminal-vs-transient authority
# =========================================================================


class _DeclaredTerminal(Exception):
    """A host's own exception that declares itself terminal, with a code the kernel does not know."""

    terminal = True
    code = "order_already_shipped"


@pytest.mark.parametrize(
    "branch, error, want_terminal",
    [
        # Deterministic failures: a redelivery repeats them, so retry and the dispatcher must stop.
        ("codec_error", edax.CodecError("bad payload"), True),
        ("payload_mismatch", edax.PayloadMismatch("k1"), True),
        ("cached_failure", edax.CachedFailure("insufficient funds"), True),
        ("handler_not_found", edax.HandlerNotFound("Withdraw"), True),
        ("already_registered", edax.AlreadyRegistered("Withdraw"), True),
        ("rejected", edax.Rejected("insufficient funds"), True),
        ("host_exception_declaring_terminal", _DeclaredTerminal("order already shipped"), True),
        # Transient: worth another attempt.
        ("concurrency", edax.ConcurrencyError("expected 3, stored 4"), False),
        ("store", edax.StoreError("connection reset"), False),
        ("in_progress", edax.InProgress("k1"), False),
        # Host code carries no `terminal` attribute; transient is the safe default under at-least-once.
        ("plain_exception", ValueError("domain rule"), False),
        ("base_exception", KeyboardInterrupt(), False),
    ],
)
def test_is_terminal_classifies_every_error(branch: str, error: BaseException, want_terminal: bool) -> None:
    assert edax.is_terminal(error) is want_terminal


@pytest.mark.parametrize(
    "branch, error, want",
    [
        ("transient_edax_error_keeps_its_code", edax.StoreError("connection reset"), "store"),
        ("host_error_is_unclassified", ValueError("a bug"), None),
        ("edax_error_keeps_its_code", edax.CodecError("bad payload"), "codec"),
        ("cached_failure_keeps_its_own_code", edax.CachedFailure("codec: bad"), "cached_failure"),
        ("rejected_is_rejected", edax.Rejected("insufficient funds"), "rejected"),
        # A host's own `code` attribute is not a kernel code, so a declared terminal carries `rejected`.
        ("host_terminal_with_a_foreign_code", _DeclaredTerminal("shipped"), "rejected"),
        ("edax_subclass_inherits_the_code", type("LedgerCodec", (edax.CodecError,), {})("x"), "codec"),
    ],
)
def test_code_of_is_the_code_a_failure_carries(branch: str, error: BaseException, want: Any) -> None:
    """The carried code, as Rust's `command::code_of`; what a claim caches is the kernel's settle of it."""
    assert edax.errors.code_of(error) == want


@pytest.mark.parametrize(
    "branch, raised, want_type, want_terminal",
    [
        # The kernel tags deterministic failures `codec:`, so they map to a terminal CodecError.
        ("tagged_codec", RuntimeError("codec: invalid cursor"), edax.CodecError, True),
        ("tagged_concurrency", RuntimeError("concurrency: expected 3"), edax.ConcurrencyError, False),
        # No code: kept whole under StoreError rather than guessed at.
        ("untagged", RuntimeError("something went wrong"), edax.StoreError, False),
        # An argument the binding could not convert fails the same way on every retry.
        ("overflow", OverflowError("can't convert negative int to unsigned"), edax.CodecError, True),
        ("wrong_type", TypeError("'int' object cannot be converted to 'PyString'"), edax.CodecError, True),
        (
            "lone_surrogate",
            UnicodeEncodeError("utf-8", "\ud800", 0, 1, "surrogates not allowed"),
            edax.CodecError,
            True,
        ),
    ],
)
def test_mapped_types_native_failures(
    branch: str, raised: Exception, want_type: type[edax.EdaxError], want_terminal: bool
) -> None:
    with pytest.raises(want_type) as excinfo, edax.errors.mapped():
        raise raised
    assert excinfo.value.terminal is want_terminal
    assert excinfo.value.__cause__ is raised  # the native error stays attached


@pytest.mark.parametrize(
    "branch, call",
    [
        # Two kernel errors that would read as transient if they crossed untagged.
        ("sequence_batch_gap", lambda: _edax.check_sequence_batch(0, [2])),
        ("unknown_error_code", lambda: _edax.error_verdict("timeout")),
        # Integer range at the boundary.
        ("negative_u64_sequence", lambda: _edax.check_sequence_batch(0, [-1])),
        ("i32_overflow_version", lambda: _edax.upcast_plan(1, 2**31, [1])),
        ("negative_backoff_attempt", lambda: _edax.backoff_millis(100, 30_000, -1, 0)),
        ("negative_stored_hash", lambda: _edax.idempotency_replay(1, "req-1", -5, False)),
        ("recorded_at_past_i64", lambda: build_message("a", "X", None, recorded_at=2**63)),
        ("negative_u32_version", lambda: build_message("a", "X", None, version=-1)),
        # Types.
        ("str_as_bytes", lambda: _edax.payload_hash("text")),  # ty: ignore[invalid-argument-type]
        ("non_str_metadata_value", lambda: build_message("a", "X", None, metadata={"n": 3})),  # ty: ignore[invalid-argument-type]
        # A lone surrogate is a legal Python `str`.
        ("lone_surrogate_key", lambda: _edax.partition("\ud800", 8)),
        ("lone_surrogate_wire", lambda: _edax.split_error("\ud800")),
        # Kernel refusals of a value's content.
        ("canonical_json_refuses_a_lone_surrogate_escape", lambda: _edax.canonical_json(b'{"s":"\\ud800"}')),
        ("event_rule_takes_a_message", lambda: _edax.event_idempotency("evt-1", "silo", "test")),  # ty: ignore[invalid-argument-type]
    ],
)
def test_every_deterministic_boundary_failure_is_terminal(branch: str, call: Callable[[], Any]) -> None:
    """Row by row: whatever the native call refuses (a kernel `codec:` error, or an argument the binding
    cannot convert) reaches the host as the terminal CodecError inside
    `mapped()`, so retry, the guard and the dispatcher stop instead of looping on it."""
    with pytest.raises(edax.CodecError) as caught, edax.errors.mapped():
        call()
    assert edax.is_terminal(caught.value)


def test_mapped_leaves_non_runtime_errors_alone() -> None:
    # Only the native boundary speaks the "<code>: <message>" protocol; host errors pass through
    # untouched so a domain exception is never relabelled as a store failure.
    with pytest.raises(ValueError), edax.errors.mapped():
        raise ValueError("codec: this is a domain error, not a native one")


ACCOUNT_ROW = {"account_id": "acct-1", "status": "settled", "balance_cents": 4200}
# Typed `Any`: an operator no `Filter` names, as an untyped caller can still send it.
BAD_OP: Any = {"compare": {"path": "status", "op": "no_such_op", "value": "settled"}}


@pytest.mark.parametrize(
    "branch, call",
    [
        (
            "query_rejects_a_malformed_cursor",
            lambda: Q.query([ACCOUNT_ROW], Q.Query(sort=SORT, after="not-valid-base64!!")),
        ),
        ("query_rejects_an_unknown_filter_op", lambda: Q.query([ACCOUNT_ROW], Q.Query(filter=BAD_OP))),
        ("matches_rejects_an_unknown_filter_op", lambda: edax.filter.matches(BAD_OP, ACCOUNT_ROW)),
        (
            "encode_cursor_rejects_a_malformed_sort_key",
            # A type checker refuses this too; the row pins the run-time refusal.
            lambda: Q.encode_cursor(ACCOUNT_ROW, [{"desc": True}]),  # type: ignore[typeddict-item]  # ty: ignore[missing-typed-dict-key, invalid-argument-type]
        ),
    ],
)
def test_kernel_wrappers_raise_typed_terminal_errors(branch: str, call: Callable[[], Any]) -> None:
    # Every one of these failures is deterministic: the same spec fails the same way on redelivery.
    # Untyped they would read as transient RuntimeErrors and be redelivered forever.
    with pytest.raises(edax.CodecError) as caught:
        call()
    assert caught.value.terminal is True


class _FailingNative:
    """A stand-in native handle whose one method raises what it is built with."""

    def __init__(self, raised: BaseException) -> None:
        self._raised = raised

    def call(self) -> None:
        raise self._raised


@pytest.mark.parametrize(
    "call, want",
    [
        pytest.param(lambda: ThreadBound(_FailingNative(RuntimeError("codec: bad payload")), "Native").get().call(), edax.CodecError, id="a_handle_method_refusal_is_typed_by_its_kernel_code"),
        pytest.param(lambda: ThreadBound(_FailingNative(RuntimeError("bad_handle: refused")), "Native").get().call(), edax.BadHandle, id="a_fatal_code_stays_fatal"),
        pytest.param(lambda: ThreadBound(_FailingNative(RuntimeError("no code at all")), "Native").get().call(), edax.StoreError, id="a_refusal_with_no_code_is_the_transient_store_error"),
        pytest.param(lambda: ThreadBound.lazy(_edax.offset_tracker, "KafkaConsumer").get().revoke("t", 2**40), edax.CodecError, id="an_argument_a_handle_cannot_convert_is_a_codec_error"),
        pytest.param(lambda: _native.partition("acct-1", -1), edax.CodecError, id="an_argument_a_function_cannot_convert_is_a_codec_error"),
    ],
)  # fmt: skip
def test_every_native_call_is_mapped_once_at_its_access_point(
    call: Callable[[], object], want: type[edax.EdaxError]
) -> None:
    """A native handle reached through ``ThreadBound`` and a function reached through ``edax._native`` map
    their failures to ``edax.errors`` with no wrapper at the call site, so no call is left raising a bare
    ``RuntimeError`` that reads as transient."""
    with pytest.raises(want):
        call()

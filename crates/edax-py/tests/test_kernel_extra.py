"""The thin SDK wrappers that sit straight on the kernel — query, upcast, causation,
router — plus the two host-side pieces on the same path: the event codec registry and the error
taxonomy every layer branches on."""

import asyncio
import json
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, Literal

import edax
import msgspec
import pytest
from conftest import Tick, build_message, golden
from edax import _edax, upcast
from edax import query as Q
from edax.codec import json_codec, msgpack_codec
from edax.errors import _BY_CODE, from_wire


def _golden_page(case: dict[str, Any]) -> Q.Page:
    """A golden query through the SDK's keyword API, or as raw spec text when it holds a key no
    keyword spells (a typo the kernel must refuse)."""
    spec = case["query"]
    if not set(spec) <= {"filter", "sort", "offset", "limit", "after"}:
        with edax.errors.mapped():
            return Q.Page.from_kernel(_edax.query(json.dumps(spec), json.dumps(case["documents"])))
    return Q.query(
        case["documents"],
        filter=spec.get("filter"),
        sort=spec.get("sort"),
        offset=spec.get("offset", 0),
        limit=spec.get("limit"),
        after=spec.get("after"),
    )


def test_golden_query() -> None:
    """The page and its ``next`` cursor, byte for byte, and every refusal as the terminal
    ``CodecError``: a cursor under another sort or without one, a ``next`` from a row with no id, a typo."""
    for c in golden("query.json"):
        if c["error_code"] is not None:
            with pytest.raises(edax.CodecError) as caught:
                _golden_page(c)
            assert caught.value.code == c["error_code"], c["name"]
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


@pytest.mark.parametrize(
    "branch, kwargs, want",
    [
        # The "no constraints" spec. Rust's `Query::default()` serializes to `{}` too; a binding
        # that emitted `{"offset": 0}` here would hand a backend a document the other never sends.
        ("no_arguments_emit_nothing", {}, {}),
        ("an_explicit_zero_offset_is_still_nothing", {"offset": 0}, {}),
        ("a_nonzero_offset_is_emitted", {"offset": 3}, {"offset": 3}),
        ("an_empty_sort_is_omitted", {"sort": []}, {}),
    ],
)
def test_build_spec_omits_what_the_caller_did_not_set(
    branch: str, kwargs: dict[str, Any], want: dict[str, Any]
) -> None:
    assert Q.build_spec(**kwargs) == want


def test_build_spec_matches_the_golden_query_shape() -> None:
    """Python builds the query spec as a dict, so the golden is the only thing holding it to the
    kernel's one ``Query`` shape. Rebuilding each golden spec from its own fields must reproduce it
    exactly: a renamed key, an extra key, or a field emitted when it was not set all fail here."""
    seen: set[str] = set()
    for c in golden("query.json"):
        spec = c["query"]
        if c["name"] == "refused_unknown_field":
            continue  # a typo the kernel refuses, which no constructor here can build
        seen |= set(spec)
        rebuilt = Q.build_spec(
            filter=spec.get("filter"),
            sort=spec.get("sort"),
            offset=spec.get("offset", 0),
            limit=spec.get("limit"),
            after=spec.get("after"),
        )
        assert rebuilt == spec, c
    assert seen == {"filter", "sort", "offset", "limit", "after"}, "the golden must set every field"


def test_golden_upcast() -> None:
    for c in golden("upcast.json"):
        if c["error"] is not None:
            # The kernel's own wire form, `codec:` included: the raw call carries the code, and the
            # pinned message names the step, so a binding that refused for another reason fails here.
            with pytest.raises(RuntimeError) as caught:
                edax.upcast_plan(c["from"], c["to"], c["available"])
            assert str(caught.value) == c["error"], c
            with pytest.raises(edax.CodecError) as mapped, edax.errors.mapped():
                edax.upcast_plan(c["from"], c["to"], c["available"])
            assert f"{mapped.value.code}: {mapped.value}" == c["error"], c
        else:
            assert edax.upcast_plan(c["from"], c["to"], c["available"]) == c["plan"], c


@pytest.mark.parametrize(
    "branch, version, want",
    [
        ("negative", -1, ValueError),
        ("past_u32", 2**32, ValueError),
        ("not_an_int", "1", TypeError),
        ("a_bool", True, TypeError),
    ],
)
def test_a_schema_version_outside_u32_is_refused_at_wiring(
    branch: str, version: Any, want: type[Exception]
) -> None:
    """A schema version is an unsigned 32-bit integer on the wire. A version outside it would fail every
    event it stamps, so the registration refuses it, not the first marshal as a terminal CodecError."""
    with pytest.raises(want):
        upcast.Upcaster().step(version, lambda d: d)
    with pytest.raises(want):
        edax.json_codec({"Opened": dict}, version=version)
    with pytest.raises(want):
        edax.msgpack_codec({"Opened": dict}, version=version)


def test_the_u32_bounds_are_accepted() -> None:
    upcast.Upcaster().step(0, lambda d: d).step(2**32 - 1, lambda d: d)
    assert edax.json_codec({"Opened": dict}, version=2**32 - 1).marshal({}).version == 2**32 - 1


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
    assert _edax.error_verdict(case["wire"]) is case["terminal"]
    err = from_wire(case["wire"])
    assert (err.code, str(err)) == (case["code"], case["message"])
    assert edax.errors.is_terminal(err) is case["terminal"], "the class carries the kernel's verdict"


def test_the_unclassified_default_is_the_kernels() -> None:
    """A host exception carries no verdict: `is_terminal` falls back to the kernel's default, the
    verdict of the class an unknown code maps to."""
    assert edax.errors.UNCLASSIFIED_IS_TERMINAL is _edax.error_verdict("") is False
    assert edax.errors.is_terminal(ValueError("a host bug")) is edax.errors.UNCLASSIFIED_IS_TERMINAL
    assert edax.StoreError.terminal is edax.errors.UNCLASSIFIED_IS_TERMINAL


def test_the_import_fails_when_a_kernel_code_has_no_class(monkeypatch: pytest.MonkeyPatch) -> None:
    """A kernel code with no class would leave `from_wire` with nothing to raise."""
    codes = SimpleNamespace(error_codes=lambda: [*_edax.error_codes(), "quota_exceeded"])
    monkeypatch.setattr(edax.errors, "_edax", codes)
    with pytest.raises(ImportError, match="quota_exceeded"):
        edax.errors._check_every_kernel_code_has_a_class()


def test_the_import_fails_on_a_wire_format_the_kernel_does_not_know(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(edax.aggregate, "WireFormat", Literal["json", "protobuf"])
    with pytest.raises(edax.CodecError, match="unknown format: protobuf"):
        edax.aggregate._check_every_wire_name_is_the_kernels()


def test_the_import_fails_on_a_message_kind_the_kernel_does_not_know(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(edax.aggregate, "MessageKind", Literal["event", "query"])
    with pytest.raises(edax.CodecError, match="unknown message kind: query"):
        edax.aggregate._check_every_wire_name_is_the_kernels()


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
    assert _edax.error_code_is_terminal(case["code"]) is case["terminal"]
    err = from_wire(case["wire"])
    assert type(err) is cls
    assert str(err) == case["message"]
    assert edax.errors.is_terminal(err) is case["terminal"]


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
    "branch, factory, event, want_error",
    [
        ("registered_type_json", json_codec, DEPOSITED, None),
        ("registered_type_msgpack", msgpack_codec, WITHDRAWN, None),
        # Terminal, not a bare KeyError: retrying an unregistered type just repeats it.
        ("unregistered_type", json_codec, Tick(7), "no event name registered for Tick"),
        ("unencodable_payload", json_codec, Noted("acct-1", object()), "encoding Noted"),
    ],
)
def test_marshal_branches(
    branch: str, factory: Callable[..., Any], event: Any, want_error: str | None
) -> None:
    codec = factory(REGISTRY, version=3)
    if want_error is not None:
        with pytest.raises(edax.CodecError, match=want_error) as caught:
            codec.marshal(event)
        assert caught.value.terminal is True
        return
    marshaled = codec.marshal(event)
    assert (marshaled.name, marshaled.version) == (type(event).__name__, 3)
    assert codec.unmarshal(marshaled) == event  # the bytes are the event, whole


@pytest.mark.parametrize(
    "branch, events, upcasters, want_error",
    [
        ("distinct_types", REGISTRY, CURRENCY_STEP, None),
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
            json_codec(events, version=3, upcasters=upcasters)
        assert caught.value.terminal is True, "a wiring mistake repeats on every retry"
        return
    codec = json_codec(events, version=3, upcasters=upcasters)
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
        # No upcaster for this name: an older payload is decoded as-is, which only works while the
        # shape has not changed.
        ("older_version_without_upcaster", {}, "Withdrawn", 1, WITHDRAWN_V2, WITHDRAWN, None),
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
            "upcasting Withdrawn v1->v2",
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
            "cannot upcast downward: from 5 > to 2",
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
    codec = json_codec(REGISTRY, version=2, upcasters=upcasters)
    event = edax.Event(name, version, data)
    if want_error is not None:
        with pytest.raises(edax.CodecError, match=want_error) as caught:
            codec.unmarshal(event)
        assert caught.value.terminal is True  # a poison event dead-letters, it never loops
        return
    assert codec.unmarshal(event) == want


# =========================================================================
# query.build_spec: only the set fields reach the kernel
# =========================================================================

FILTER = edax.filter.and_(edax.filter.eq("status", "settled"), edax.filter.gt("balance_cents", 1000))
SORT = [Q.sort_key("balance_cents", desc=True), Q.sort_key("account_id")]
CURSOR = "W3sicGF0aCI6ImJhbGFuY2VfY2VudHMifV0="


@pytest.mark.parametrize(
    "branch, kwargs, want",
    [
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
        ("after_empty_is_omitted", {"after": ""}, {}),
        (
            "every_field_set",
            {"filter": FILTER, "sort": SORT, "offset": 20, "limit": 50, "after": CURSOR},
            {"filter": FILTER, "sort": SORT, "offset": 20, "limit": 50, "after": CURSOR},
        ),
    ],
)
def test_build_spec_branches(branch: str, kwargs: dict, want: dict) -> None:
    assert Q.build_spec(**kwargs) == want


# =========================================================================
# router: dispatch by event name
# =========================================================================


def _router_case(kind: str, seen: list) -> edax.EventRouter:
    router = edax.EventRouter(on_unhandled=seen.append if kind == "fallback" else None)
    if kind == "sync":
        router.register("Deposited", seen.append)
    elif kind == "async":

        async def handle(env: edax.EventMessage) -> None:
            await asyncio.sleep(0)
            seen.append(env)

        router.register("Deposited", handle)
    elif kind == "decorator":
        router.on("Deposited")(seen.append)
    return router


@pytest.mark.parametrize(
    "branch, kind, event_name, want_names",
    [
        ("sync_handler", "sync", "Deposited", ["Deposited"]),
        ("async_handler_is_awaited", "async", "Deposited", ["Deposited"]),
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


def test_register_rejects_a_second_handler_for_one_name() -> None:
    router = edax.EventRouter()
    router.register("Deposited", lambda env: None)
    # The same wiring mistake as CommandBus.register, so the same typed terminal error: a startup
    # misconfiguration must never be retried.
    with pytest.raises(edax.AlreadyRegistered, match="already registered for 'Deposited'"):
        router.register("Deposited", lambda env: None)


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
        ("transient_is_never_cached", edax.StoreError("connection reset"), None),
        ("host_error_is_unclassified", ValueError("a bug"), None),
        ("edax_error_keeps_its_code", edax.CodecError("bad payload"), "codec"),
        ("cached_failure_keeps_its_own_code", edax.CachedFailure("codec: bad"), "cached_failure"),
        ("rejected_is_rejected", edax.Rejected("insufficient funds"), "rejected"),
        # A host's own `code` attribute is not a kernel code, so the cache names it a rejection.
        ("host_terminal_with_a_foreign_code", _DeclaredTerminal("shipped"), "rejected"),
        ("edax_subclass_inherits_the_code", type("LedgerCodec", (edax.CodecError,), {})("x"), "codec"),
    ],
)
def test_code_of_is_what_a_terminal_failure_caches(branch: str, error: BaseException, want: Any) -> None:
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
        ("unknown_error_code", lambda: _edax.error_code_is_terminal("timeout")),
        # Integer range at the boundary.
        ("negative_u64_sequence", lambda: _edax.check_sequence_batch(0, [-1])),
        ("i32_overflow_version", lambda: _edax.upcast_plan(1, 2**31, [1])),
        ("negative_backoff_attempt", lambda: _edax.backoff_millis(100, 30_000, -1, 0)),
        ("negative_stored_hash", lambda: _edax.idempotency_outcome(1, -5, False)),
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
BAD_OP = {"compare": {"path": "status", "op": "no_such_op", "value": "settled"}}


@pytest.mark.parametrize(
    "branch, call",
    [
        (
            "query_rejects_a_malformed_cursor",
            lambda: Q.query([ACCOUNT_ROW], sort=SORT, after="not-valid-base64!!"),
        ),
        ("query_rejects_an_unknown_filter_op", lambda: Q.query([ACCOUNT_ROW], filter=BAD_OP)),
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

import asyncio
import contextlib
import datetime
import json
import re
import uuid
from dataclasses import dataclass, field, is_dataclass
from typing import Any, ClassVar

import edax
import msgspec
import pytest
from conftest import NO_WAIT, CreateUser, golden, recording_pacing
from edax import command as C
from edax.backend.memory import MemoryIdempotencyStore, MemoryProjectionStore, MemoryTransactor
from edax.command import Claimed, Record, Won
from edax.projection import Entity
from edax.store import Tx


def test_golden_payload_hash() -> None:
    for c in golden("payload_hash.json"):
        assert edax.payload_hash(c["input"].encode()) == c["expected"]


def test_golden_idempotency_record() -> None:
    # The record's wire shape, reading order and replay verdict are the kernel's; the SDK must read
    # the stored bytes to the same fields, re-emit the same bytes, and reach the same verdict. The
    # `null` result case pins that a completed `null` reads as a success, never as pending.
    for c in golden("idempotency_record.json"):
        if "decode_error" in c:
            # A stored value no reader may guess at: the strict reader refuses it with the kernel's text...
            with pytest.raises(edax.CodecError) as caught:
                C.decode_record(c["stored"])
            assert f"{caught.value.code}: {caught.value}" == c["decode_error"], c["name"]
        else:
            assert C.decode_record(c["stored"]) == C.decode_prior(c["stored"], c["current"]), c["name"]
        # ...and a claimant reads it as the kernel's terminal codec failure, as every SDK does.
        record = C.decode_prior(c["stored"], c["current"])
        want = Record(
            c["payload_hash"], c.get("result"), c.get("error"), c["state"] == "pending", c.get("error_code")
        )
        assert record == want, c["name"]
        assert C.encode_record(record) == c["encoded"], c["name"]
        assert C._outcome(record, c["current"]) == c["outcome"], c["name"]
        if c["outcome"] == "failed":
            # The replay is the kernel's text in both SDKs: Rust renders it whole, Python keeps the code
            # apart, and the original failure's code survives inside the message.
            with pytest.raises(edax.CachedFailure) as caught:
                C._replay("Deposit", "k", c["current"], record, edax.observe._wrap(None), None)
            assert f"{caught.value.code}: {caught.value}" == c["replay"], c["name"]
            assert caught.value.original_code == c["error_code"], c["name"]


async def test_none_result_is_replayed_as_a_success() -> None:
    # A handler that returns None (the common case) must replay as None, not as "no result" or as
    # a failure; the golden vector pins this for both SDKs.
    store = MemoryIdempotencyStore()
    bus = C.CommandBus(C.idempotent(store, lambda cmd: "req-1"))
    calls = 0

    async def handler(cmd: CreateUser) -> None:
        nonlocal calls
        calls += 1

    bus.register("user.create", handler)
    assert await bus.dispatch(CreateUser("a@b.c")) is None
    assert await bus.dispatch(CreateUser("a@b.c")) is None
    assert calls == 1


@pytest.mark.parametrize(
    "branch, result, want, want_type",
    [
        ("past_2_53_is_still_an_exact_int", 2**53 + 1, 9007199254740993, int),
        ("u64_max_is_exact", 2**64 - 1, 18446744073709551615, int),
        ("2_64_becomes_a_float_equal_in_value", 2**64, 1.8446744073709552e19, float),
        ("below_i64_min_becomes_a_float_lossy", -(2**63) - 1, -9.223372036854776e18, float),
    ],
)
async def test_cached_result_integers_are_exact_only_within_64_bits(
    branch: str, result: int, want: Any, want_type: type
) -> None:
    # The bound in the IdempotencyStore contract: a result integer outside the i64/u64 range comes
    # back from a JSON-backed store as a float. Pinned so the edge is known, not discovered.
    store = MemoryIdempotencyStore()
    won = await store.claim("req-1", 1)
    assert isinstance(won, Won)
    await store.complete("req-1", won.handle, Record(payload_hash=1, result=result))
    prior = await store.claim("req-1", 1)
    assert not isinstance(prior, Won)
    assert prior.result == want and type(prior.result) is want_type, branch
    assert (prior.result == result) is (branch != "below_i64_min_becomes_a_float_lossy")


def test_golden_canonical_payload_hash() -> None:
    # payload_hash_json canonicalizes in the kernel, so Python agrees with the Rust-generated golden
    # even for non-ASCII (the case a Python-side json.dumps(ensure_ascii=True) would get wrong).
    for c in golden("canonical_payload_hash.json"):
        if c["error"] is not None:
            with pytest.raises(RuntimeError) as caught:
                edax.payload_hash_json(c["input"].encode())
            assert str(caught.value) == c["error"], c["input"]
            continue
        assert edax.payload_hash_json(c["input"].encode()) == c["expected"], c["input"]


@dataclass
class GoldenShipping:
    city: str
    zip: str | None


@dataclass
class GoldenPlaceOrder:
    """The golden command as the idiomatic Python type. Fields are declared in an order that is
    neither the vector's nor alphabetical: the hash must not depend on declaration order."""

    request_id: str
    weight_kg: float
    total_cents: int
    tags: list[str]
    shipping: GoldenShipping
    reference: int
    order_id: str
    note: str | None
    customer: str
    currency: str
    type: ClassVar[str] = "order.place"


class GoldenShippingStruct(msgspec.Struct):
    city: str
    zip: str | None


class GoldenPlaceOrderStruct(msgspec.Struct):
    request_id: str
    weight_kg: float
    total_cents: int
    tags: list[str]
    shipping: GoldenShippingStruct
    reference: int
    order_id: str
    note: str | None
    customer: str
    currency: str
    type: ClassVar[str] = "order.place"


class GoldenPlainPlaceOrder:
    """No dataclass, no Struct: the instance attributes are the fields (``shipping`` stays a dict)."""

    type = "order.place"

    def __init__(self, **fields: Any) -> None:
        self.__dict__.update(fields)


@dataclass
class GoldenPlaceOrderWithTypeField:
    """The shape the rule rules out: ``type`` as an instance field is hashed here and has no
    counterpart in a Rust command, whose type name is ``command_type()``."""

    request_id: str
    weight_kg: float
    total_cents: int
    tags: list[str]
    shipping: GoldenShipping
    reference: int
    order_id: str
    note: str | None
    customer: str
    currency: str
    type: str = "order.place"


def _golden_command(command_type: str, fields: dict[str, Any], shape: str) -> Any:
    """Rebuild a golden case as a Python command of the given shape."""
    if command_type != "order.place":
        raise ValueError(f"no Python command type for {command_type!r}")
    if shape == "dataclass":
        return msgspec.convert(fields, GoldenPlaceOrder)
    if shape == "struct":
        return msgspec.convert(fields, GoldenPlaceOrderStruct)
    if shape == "plain":
        return GoldenPlainPlaceOrder(**fields)
    raise ValueError(f"unknown command shape {shape!r}")


@pytest.mark.parametrize("shape", ["dataclass", "struct", "plain"])
async def test_golden_command_hash_holds_through_the_middleware(shape: str) -> None:
    # testdata/golden/command_hash.json asserted through the SDK layer: every case is rebuilt as a
    # Python command, dispatched through the real `idempotent` middleware, and the hash the store
    # received must be the vector's. The Rust suite does the same from a serde struct, so this is
    # the proof that both SDKs bind one key to one number for one command. All three Python shapes
    # run so the plain-object fallback is held to the same rule as a dataclass.
    cases = golden("command_hash.json")
    assert cases, "the vector has cases"

    runs: list[str] = []

    async def handler(cmd: Any) -> None:
        runs.append(cmd.request_id)

    for case in cases:
        cmd = _golden_command(case["type"], case["command"], shape)
        store = MemoryIdempotencyStore()
        bus = C.CommandBus(C.idempotent(store, lambda c: c.request_id))
        bus.register(case["type"], handler)
        runs.clear()
        if case["error"] is not None:
            # A refusal row: the kernel's terminal `codec` error before any claim or handler run. The
            # row pins the code and the rule; the byte offset is the encoder's, whose field order is
            # its own.
            rule = case["error"].rsplit(" is outside ", 1)[1]
            with pytest.raises(edax.CodecError, match=re.escape(rule)) as refused:
                await bus.dispatch(cmd)
            assert refused.value.terminal, (shape, case["name"])
            assert runs == [], (shape, case["name"])
            assert isinstance(await store.claim(cmd.request_id, 0), Won), (shape, case["name"])
            continue
        await bus.dispatch(cmd)
        assert runs == [cmd.request_id], (shape, case["name"])
        # The hash the middleware bound to the key is what a Rust service would compare its own
        # against; a claim with any hash returns the winner's record.
        prior = await store.claim(cmd.request_id, 0)
        assert not isinstance(prior, Won), (shape, case["name"])
        assert prior.payload_hash == case["expected"], (shape, case["name"])


def test_golden_command_hash_rejects_the_old_per_sdk_rules() -> None:
    # The two conventions the vector replaces. Neither may reach it, or the gap is not closed:
    # a Rust `payload()` that returned the business body without the key field, and a Python
    # command carrying `type` as an instance field.
    case = golden("command_hash.json")[0]
    assert case["name"] == "whole_command_including_the_idempotency_key_field"

    body_only = dict(case["command"])
    del body_only["request_id"]
    assert edax.payload_hash_json(msgspec.json.encode(body_only)) != case["expected"]

    # Built through the constructor, as a service writes `Deposit(...)`. A converted instance shows
    # the same divergence, because defaulted fields are read from the class.
    fields = dict(case["command"])
    fields["shipping"] = GoldenShipping(**fields["shipping"])
    with_type_field = GoldenPlaceOrderWithTypeField(**fields)
    assert C._payload_hash(with_type_field) != case["expected"]


@dataclass
class DefaultedShipping:
    city: str
    zip: str | None = None


@dataclass
class DefaultedPlaceOrder:
    """Every field but the key has a default, one per type the golden vector covers, so a wire
    mapping that carries only ``request_id``, ``shipping`` and ``lines`` decodes to the same
    logical command the constructor builds. Nested dataclasses sit directly and inside a list."""

    request_id: str
    shipping: DefaultedShipping
    lines: list[DefaultedShipping]
    order_id: str = "ord-7f3c"
    total_cents: int = 4200
    reference: int = 9007199254740992
    weight_kg: float = 2.5
    note: str | None = None
    customer: str = "café ☕ 🦀"
    tags: list[str] = field(default_factory=lambda: ["eu", "paid"])
    type: ClassVar[str] = "order.place"


class DefaultedShippingStruct(msgspec.Struct):
    city: str
    zip: str | None = None


class DefaultedPlaceOrderStruct(msgspec.Struct):
    """The Struct shape of :class:`DefaultedPlaceOrder`; ``shipping`` stays a *dataclass*, so a
    dataclass nested in a Struct is covered, and ``lines`` is a list of Structs."""

    request_id: str
    shipping: DefaultedShipping
    lines: list[DefaultedShippingStruct]
    order_id: str = "ord-7f3c"
    total_cents: int = 4200
    reference: int = 9007199254740992
    weight_kg: float = 2.5
    note: str | None = None
    customer: str = "café ☕ 🦀"
    tags: list[str] = msgspec.field(default_factory=lambda: ["eu", "paid"])
    type: ClassVar[str] = "order.place"


# The mapping a queue carries: only the fields without a default, defaults left implicit.
DEFAULTED_WIRE: dict[str, Any] = {
    "request_id": "req-7f3c-1",
    "shipping": {"city": "NYC"},
    "lines": [{"city": "LA"}],
}

# The logical value of that command, every default materialized: what the hash must cover.
DEFAULTED_ORDER: dict[str, Any] = {
    "request_id": "req-7f3c-1",
    "shipping": {"city": "NYC", "zip": None},
    "lines": [{"city": "LA", "zip": None}],
    "order_id": "ord-7f3c",
    "total_cents": 4200,
    "reference": 9007199254740992,
    "weight_kg": 2.5,
    "note": None,
    "customer": "café ☕ 🦀",
    "tags": ["eu", "paid"],
}


def _same_command(a: Any, b: Any) -> bool:
    """The two are one logical command. A plain object has no ``__eq__``, so compare its fields."""
    if is_dataclass(a) or isinstance(a, msgspec.Struct):
        return a == b
    return vars(a) == vars(b)


@dataclass
class ConstructionCase:
    """One logical command built two ways: ``built`` by its constructor, ``decoded`` by msgspec."""

    built: Any
    decoded: Any


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            ConstructionCase(
                DefaultedPlaceOrder("req-7f3c-1", DefaultedShipping("NYC"), [DefaultedShipping("LA")]),
                msgspec.convert(DEFAULTED_WIRE, DefaultedPlaceOrder),
            ),
            id="dataclass_via_convert",
        ),
        pytest.param(
            ConstructionCase(
                DefaultedPlaceOrder("req-7f3c-1", DefaultedShipping("NYC"), [DefaultedShipping("LA")]),
                msgspec.json.decode(msgspec.json.encode(DEFAULTED_WIRE), type=DefaultedPlaceOrder),
            ),
            id="dataclass_via_decode",
        ),
        pytest.param(
            ConstructionCase(
                DefaultedPlaceOrderStruct(
                    "req-7f3c-1", DefaultedShipping("NYC"), [DefaultedShippingStruct("LA")]
                ),
                msgspec.convert(DEFAULTED_WIRE, DefaultedPlaceOrderStruct),
            ),
            id="struct_holding_a_dataclass_via_convert",
        ),
        pytest.param(
            ConstructionCase(
                GoldenPlainPlaceOrder(**{**DEFAULTED_ORDER, "shipping": DefaultedShipping("NYC")}),
                GoldenPlainPlaceOrder(
                    **{**DEFAULTED_ORDER, "shipping": msgspec.convert({"city": "NYC"}, DefaultedShipping)}
                ),
            ),
            id="plain_object_holding_a_dataclass_via_convert",
        ),
    ],
)
def test_hash_is_independent_of_how_the_command_was_built(case: ConstructionCase) -> None:
    # A service that builds `Deposit(...)` and one that decodes the same command off a queue must
    # bind one key to one number, or the second reports PayloadMismatch on a genuine duplicate.
    # msgspec's dataclass encoder reads `__dict__`, and `convert` / `decode` leave a defaulted
    # field out of it, so the two would hash differently. Both must equal the hash of the fully
    # materialized value, not merely each other: two encodings that both dropped the defaults
    # would agree and still be wrong.
    assert _same_command(case.built, case.decoded)
    expected = edax.payload_hash_json(msgspec.json.encode(DEFAULTED_ORDER))
    assert C._payload_hash(case.built) == expected
    assert C._payload_hash(case.decoded) == expected


async def test_bus_idempotency() -> None:
    store = MemoryIdempotencyStore()
    bus = C.CommandBus(C.idempotent(store, lambda cmd: "req-1"), C.retry(3))

    calls = 0

    async def handler(cmd: CreateUser) -> str:
        nonlocal calls
        calls += 1
        return f"user:{cmd.email}"

    bus.register("user.create", handler)

    assert await bus.dispatch(CreateUser("a@b.c")) == "user:a@b.c"
    # Same key + same payload -> replayed, handler not called again.
    assert await bus.dispatch(CreateUser("a@b.c")) == "user:a@b.c"
    assert calls == 1
    # Same key + different payload -> mismatch.
    with pytest.raises(C.PayloadMismatch):
        await bus.dispatch(CreateUser("x@y.z"))


async def test_bus_handler_not_found() -> None:
    with pytest.raises(C.HandlerNotFound):
        await C.CommandBus().dispatch(CreateUser("a@b.c"))


async def _async_handler(cmd: Any) -> str:
    return "ran"


def _sync_handler(cmd: Any) -> str:  # pragma: no cover - never dispatched
    return "ran"


class AsyncCallable:
    """A callable object whose ``__call__`` is a coroutine function: a valid handler."""

    async def __call__(self, cmd: Any) -> str:
        return "ran"


class SyncCallable:
    """A callable object whose ``__call__`` is not: rejected like any other sync handler."""

    def __call__(self, cmd: Any) -> str:  # pragma: no cover - never dispatched
        return "ran"


@dataclass(frozen=True)
class HandlerTypeCase:
    """One handler shape and the type name ``register`` must name when it refuses it."""

    handler: Any
    rejected_as: str | None


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(HandlerTypeCase(_async_handler, None), id="coroutine_function_is_accepted"),
        pytest.param(HandlerTypeCase(AsyncCallable(), None), id="async_callable_object_is_accepted"),
        pytest.param(HandlerTypeCase(_sync_handler, "function"), id="sync_function_is_rejected"),
        pytest.param(HandlerTypeCase(SyncCallable(), "SyncCallable"), id="sync_callable_object_is_rejected"),
        pytest.param(
            HandlerTypeCase(lambda cmd: _async_handler(cmd), "function"),
            id="sync_lambda_returning_a_coroutine_is_rejected",
        ),
    ],
)
async def test_register_rejects_a_handler_that_is_not_a_coroutine_function(case: HandlerTypeCase) -> None:
    """The wiring mistake is caught at register time, not on the first dispatch far from it."""
    bus = C.CommandBus()
    if case.rejected_as is None:
        bus.register("user.create", case.handler)
        assert await bus.dispatch(CreateUser("a@b.c")) == "ran"
        return
    with pytest.raises(TypeError) as caught:
        bus.register("user.create", case.handler)
    assert "user.create" in str(caught.value) and case.rejected_as in str(caught.value)
    assert bus._handlers == {}, "a refused handler must not be registered"


async def test_transient_failure_releases_claim_so_a_retry_reruns() -> None:
    # A non-terminal (transient) handler failure must NOT poison the key: the claim is released so a
    # fresh dispatch of the same key runs the handler again rather than replaying the error forever.
    store = MemoryIdempotencyStore()
    bus = C.CommandBus(C.idempotent(store, lambda cmd: "req-1"))  # no retry: observe one run at a time
    calls = 0

    async def handler(cmd: CreateUser) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient downstream blip")  # not terminal
        return f"user:{cmd.email}"

    bus.register("user.create", handler)

    with pytest.raises(RuntimeError):
        await bus.dispatch(CreateUser("a@b.c"))
    # Claim released -> the redelivery re-runs and succeeds (would replay the error if it were cached).
    assert await bus.dispatch(CreateUser("a@b.c")) == "user:a@b.c"
    assert calls == 2


async def test_terminal_failure_is_cached_and_replayed() -> None:
    store = MemoryIdempotencyStore()
    bus = C.CommandBus(C.idempotent(store, lambda cmd: "req-1"))
    calls = 0

    async def handler(cmd: CreateUser) -> str:
        nonlocal calls
        calls += 1
        raise edax.CodecError("bad payload")  # terminal -> cached

    bus.register("user.create", handler)

    with pytest.raises(edax.CodecError):
        await bus.dispatch(CreateUser("a@b.c"))
    # Terminal error cached: the redelivery replays it without re-running the handler, and the replay
    # is itself terminal so a dispatcher dead-letters it instead of redelivering forever.
    with pytest.raises(C.CachedFailure, match="bad payload") as info:
        await bus.dispatch(CreateUser("a@b.c"))
    assert info.value.terminal
    assert calls == 1


async def test_pending_claim_raises_in_progress_not_none() -> None:
    # A concurrent dispatch that loses the claim while the winner is still in flight must not replay a
    # bogus None success — it raises InProgress so the caller retries/redelivers.
    store = MemoryIdempotencyStore()
    cmd = CreateUser("a@b.c")
    hash_ = C._payload_hash(cmd)
    assert isinstance(
        await store.claim("req-1", hash_), Won
    )  # simulate the in-flight winner (pending record)

    async def must_not_run(c: Any) -> None:
        raise AssertionError("handler must not run")

    bus = C.CommandBus(C.idempotent(store, lambda c: "req-1"))
    bus.register("user.create", must_not_run)
    with pytest.raises(edax.InProgress):
        await bus.dispatch(cmd)


# One realistic command payload; every case below carries the same fields in a different shape.
ORDER = {
    "order_id": "ord-7f3c",
    "customer_id": "cust-88",
    "total_cents": 4200,
    "currency": "USD",
}


@dataclass
class PlaceOrder:
    order_id: str
    customer_id: str
    total_cents: int
    currency: str
    type: ClassVar[str] = "order.place"


class PlaceOrderStruct(msgspec.Struct):
    order_id: str
    customer_id: str
    total_cents: int
    currency: str
    type: ClassVar[str] = "order.place"


class PlainPlaceOrder:
    """No dataclass, no Struct: serialized from ``vars()``. ``type`` is a *class* attribute, so it
    stays out of the hashed payload."""

    type = "order.place"

    def __init__(self, **fields: Any) -> None:
        self.__dict__.update(fields)


@dataclass
class PlaceOrderWithScalars:
    """Fields ``json.dumps`` would reject; msgspec encodes them deterministically."""

    id: uuid.UUID
    at: datetime.datetime
    type: ClassVar[str] = "order.place"


class SlottedPlaceOrder:
    """``__slots__`` and no ``__dict__``, so ``vars()`` raises."""

    __slots__ = ("order_id",)
    type = "order.place"

    def __init__(self, order_id: str) -> None:
        self.order_id = order_id


def _cyclic_order() -> PlainPlaceOrder:
    cmd = PlainPlaceOrder(**ORDER)
    # Written through __dict__ because that *is* the payload: the instance dict holds itself, so
    # msgspec recurses until it gives up.
    cmd.__dict__["parent"] = cmd.__dict__
    return cmd


@dataclass
class ComputedOrder:
    """``total_cents`` is ``init=False`` and never assigned: a field with no value at all."""

    order_id: str
    total_cents: int = field(init=False)
    type: ClassVar[str] = "order.place"


@dataclass
class PayloadCase:
    cmd: Any
    encoded: dict[str, Any] | None  # the parsed payload, for an encodable command
    # An SDK error, not any exception: the case below asserts `terminal` on what it caught.
    raises: type[edax.EdaxError] | None = None


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(PayloadCase(PlaceOrder(**ORDER), ORDER), id="dataclass_encodes_its_fields"),
        pytest.param(
            PayloadCase(PlaceOrderStruct(**ORDER), ORDER), id="msgspec_struct_encodes_without_a_dict"
        ),
        pytest.param(
            PayloadCase(PlainPlaceOrder(**ORDER), ORDER),
            id="plain_object_encodes_instance_attrs_only",
        ),
        pytest.param(
            PayloadCase(
                PlaceOrderWithScalars(uuid.UUID(int=1), datetime.datetime(2026, 1, 2, 3, 4, 5)),
                {"id": "00000000-0000-0000-0000-000000000001", "at": "2026-01-02T03:04:05"},
            ),
            id="uuid_and_datetime_encode_instead_of_raising_typeerror",
        ),
        pytest.param(
            PayloadCase(msgspec.convert(DEFAULTED_WIRE, DefaultedPlaceOrder), DEFAULTED_ORDER),
            id="converted_dataclass_materializes_its_defaults_at_every_depth",
        ),
        pytest.param(
            PayloadCase(
                PlainPlaceOrder(**ORDER, pair=(msgspec.convert({"city": "NYC"}, DefaultedShipping),)),
                {**ORDER, "pair": [{"city": "NYC", "zip": None}]},
            ),
            id="dataclass_inside_a_tuple_attribute",
        ),
        pytest.param(
            PayloadCase(
                PlainPlaceOrder(
                    **ORDER, by_name={"home": msgspec.convert({"city": "NYC"}, DefaultedShipping)}
                ),
                {**ORDER, "by_name": {"home": {"city": "NYC", "zip": None}}},
            ),
            id="dataclass_inside_a_dict_attribute",
        ),
        pytest.param(
            PayloadCase(msgspec.convert(DEFAULTED_WIRE, DefaultedPlaceOrderStruct), DEFAULTED_ORDER),
            id="struct_holding_a_converted_dataclass_is_rebuilt_with_its_defaults",
        ),
        pytest.param(
            PayloadCase(ComputedOrder("ord-7f3c"), {"order_id": "ord-7f3c"}),
            id="init_false_field_without_a_value_is_omitted_as_msgspec_omits_it",
        ),
        pytest.param(
            PayloadCase(SlottedPlaceOrder("ord-7f3c"), None, edax.CodecError),
            id="slotted_command_has_no_dict_and_is_terminal",
        ),
        pytest.param(
            PayloadCase(_cyclic_order(), None, edax.CodecError),
            id="reference_cycle_is_terminal_not_a_bare_recursionerror",
        ),
    ],
)
def test_payload_json_branches(case: PayloadCase) -> None:
    # _payload_json only serializes — canonicalization is the kernel's job in payload_hash_json —
    # so assert the parsed content, not exact bytes. Every failure here is a pure function of the
    # command, so it must surface as the terminal CodecError; a bare TypeError / RecursionError
    # would read as transient and be redelivered forever.
    if case.raises is not None:
        with pytest.raises(case.raises, match="not JSON-encodable") as caught:
            C._payload_json(case.cmd)
        assert caught.value.terminal is True
        assert caught.value.__cause__ is not None  # the underlying encoder error is preserved
        return
    payload = C._payload_json(case.cmd)
    assert json.loads(payload) == case.encoded
    assert edax.payload_hash_json(payload) == edax.payload_hash_json(payload)  # stable, no raise


POST_INITS: list[str] = []


class PostInitOrder(msgspec.Struct):
    """A Struct whose ``__post_init__`` is observable, so a needless rebuild would show."""

    order_id: str
    tags: list[str]
    type: ClassVar[str] = "order.place"

    def __post_init__(self) -> None:
        POST_INITS.append(self.order_id)


@dataclass
class UntouchedCase:
    cmd: Any
    attr: str | None  # the value that must come back as the same object; None = the command


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            UntouchedCase(PostInitOrder("ord-7f3c", ["eu"]), None),
            id="struct_without_dataclasses_is_not_rebuilt",
        ),
        pytest.param(
            UntouchedCase(DefaultedPlaceOrder("req-7f3c-1", DefaultedShipping("NYC"), []), "tags"),
            id="list_of_scalars_in_a_dataclass",
        ),
        pytest.param(
            UntouchedCase(PlainPlaceOrder(**ORDER, meta={"channel": "web"}), "meta"),
            id="dict_of_scalars_in_a_plain_object",
        ),
    ],
)
def test_payload_data_leaves_values_without_dataclasses_untouched(case: UntouchedCase) -> None:
    # Materializing walks the value for dataclasses but must hand back what it did not change: the
    # common case copies nothing, and a Struct is not rebuilt, since `msgspec.structs.replace`
    # re-runs `__post_init__` and hashing must not have that side effect.
    post_inits = len(POST_INITS)
    data = C._payload_data(case.cmd)
    original = case.cmd if case.attr is None else getattr(case.cmd, case.attr)
    got = data if case.attr is None else data[case.attr]
    assert got is original
    assert len(POST_INITS) == post_inits


class CountingIdempotencyStore(MemoryIdempotencyStore):
    """Counts claims/releases so the middleware nesting is observable: ``idempotent`` outside,
    ``retry`` inside means every attempt runs under **one** claim."""

    def __init__(self) -> None:
        super().__init__()
        self.claims = 0
        self.releases = 0

    async def claim(self, key: str, payload_hash: int, tx: Tx | None = None) -> Claimed:
        self.claims += 1
        return await super().claim(key, payload_hash, tx)

    async def release(self, key: str, handle: bytes, tx: Tx | None = None) -> None:
        self.releases += 1
        await super().release(key, handle, tx)


class BrokenCompleteStore(CountingIdempotencyStore):
    """Recording the outcome fails on its own (an unserializable result, a store hiccup)."""

    async def complete(self, key: str, handle: bytes, record: Record, tx: Tx | None = None) -> None:
        raise edax.StoreError("cannot record outcome")


async def test_retry_inside_idempotent_shares_one_claim() -> None:
    store = CountingIdempotencyStore()
    # Order matters: idempotent first (outermost), retry inside it.
    pacing, slept = recording_pacing()
    bus = C.CommandBus(C.idempotent(store, lambda cmd: "req-1"), C.retry(2, pacing=pacing))
    calls = 0

    async def handler(cmd: CreateUser) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient downstream blip")  # not terminal
        return f"user:{cmd.email}"

    bus.register("user.create", handler)

    assert await bus.dispatch(CreateUser("a@b.c")) == "user:a@b.c"
    assert calls == 2
    assert store.claims == 1  # both attempts ran under one claim
    assert store.releases == 0  # never released — the retry succeeded under the same claim
    assert slept == [edax.DEFAULT_BACKOFF.delay_millis(0, 2**64 - 1) / 1000]  # one backoff waited

    # The claim completed, so a redelivery replays the cached result without another attempt.
    assert await bus.dispatch(CreateUser("a@b.c")) == "user:a@b.c"
    assert calls == 2
    assert store.claims == 2


# =========================================================================
# Branch tables
# =========================================================================


def test_register_duplicate_type_raises() -> None:
    bus = C.CommandBus()

    async def handler(cmd: CreateUser) -> str:
        return f"user:{cmd.email}"

    bus.register("user.create", handler)
    with pytest.raises(C.AlreadyRegistered, match=r"user\.create"):
        bus.register("user.create", handler)


class DeclaredTerminal(Exception):
    """A host's own exception that declares itself terminal, the attribute every consumer reads."""

    terminal = True


@dataclass
class RetryCase:
    attempts: int
    errors: list[BaseException | None]  # per handler call; None = return normally
    raises: type[BaseException] | None
    calls: int
    sleeps: int  # backoff sleeps taken: one per failed attempt that is followed by another


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(RetryCase(3, [None], None, 1, 0), id="first_attempt_succeeds"),
        pytest.param(RetryCase(3, [RuntimeError("blip"), None], None, 2, 1), id="transient_then_success"),
        pytest.param(
            RetryCase(3, [RuntimeError("1"), RuntimeError("2"), RuntimeError("3")], RuntimeError, 3, 2),
            id="transient_exhausts_attempts_raises_last",
        ),
        pytest.param(
            RetryCase(3, [edax.CodecError("bad")], edax.CodecError, 1, 0), id="terminal_short_circuits"
        ),
        pytest.param(
            RetryCase(3, [edax.Rejected("insufficient funds")], edax.Rejected, 1, 0),
            id="host_rejection_short_circuits",
        ),
        pytest.param(
            RetryCase(3, [DeclaredTerminal("order already shipped")], DeclaredTerminal, 1, 0),
            id="host_exception_declaring_terminal_short_circuits",
        ),
        pytest.param(
            RetryCase(3, [asyncio.CancelledError()], asyncio.CancelledError, 1, 0),
            id="cancellation_is_not_retried",
        ),
        pytest.param(
            RetryCase(1, [RuntimeError("blip")], RuntimeError, 1, 0), id="one_attempt_never_retries"
        ),
    ],
)
async def test_retry_branches(case: RetryCase) -> None:
    draw = 0x9E3779B97F4A7C15
    pacing, slept = recording_pacing(draw)
    bus = C.CommandBus(C.retry(case.attempts, pacing=pacing))
    calls = 0

    async def handler(cmd: CreateUser) -> str:
        nonlocal calls
        error = case.errors[calls]
        calls += 1
        if error is not None:
            raise error
        return f"user:{cmd.email}"

    bus.register("user.create", handler)

    if case.raises is None:
        assert await bus.dispatch(CreateUser("a@b.c")) == "user:a@b.c"
    else:
        with pytest.raises(case.raises) as info:
            await bus.dispatch(CreateUser("a@b.c"))
        assert info.value is case.errors[calls - 1]  # the last failure, not an earlier one
    assert calls == case.calls
    # The kernel's jittered schedule: failed attempt i waits step i before attempt i + 1.
    assert slept == [edax.DEFAULT_BACKOFF.delay_millis(i, draw) / 1000 for i in range(case.sleeps)]


@dataclass
class Deposit:
    account_id: str
    amount: int
    request_id: str
    type: ClassVar[str] = "Deposit"


@dataclass
class PlacementCase:
    # "inside_the_guard": idempotent(unit_of_work=db) first, retry inside it. "under_the_host": the
    # host opens transaction(db) around the dispatch. "outside_the_guard": the one wiring to use.
    wiring: str
    raises: type[BaseException] | None
    runs: int  # handler runs, the rolled-back attempt included
    balance: int  # committed after the dispatch


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            PlacementCase("inside_the_guard", edax.BadHandle, 0, 0), id="retry_inside_the_guard_is_refused"
        ),
        pytest.param(
            PlacementCase("under_the_host", edax.BadHandle, 0, 0),
            id="retry_under_a_host_transaction_is_refused",
        ),
        pytest.param(
            PlacementCase("outside_the_guard", None, 2, 100), id="retry_outside_the_guard_reruns_whole_units"
        ),
    ],
)
async def test_retry_refuses_to_run_inside_an_open_transaction(case: PlacementCase) -> None:
    """A retry re-runs a whole unit of work, never a handler inside one. Before the refusal, the two
    refused wirings committed a balance of 200 for one deposit of 100: the second attempt re-read the
    first one's staged save and applied the deposit on top of it, and one commit landed both."""

    db = MemoryTransactor()
    accounts = MemoryProjectionStore(db)
    ledger = MemoryIdempotencyStore(db)
    runs = 0

    async def deposit(cmd: Deposit) -> int:
        nonlocal runs
        runs += 1
        account = await accounts.get(cmd.account_id) or Entity(cmd.account_id, 0, {"balance": 0})
        account.document = {"balance": account.document["balance"] + cmd.amount}
        await accounts.save(account)  # joins the open unit of work
        if runs == 1:
            raise ConnectionError("notification service reset after the save")  # transient
        return account.document["balance"]

    def key(cmd: Deposit) -> str:
        return cmd.request_id

    if case.wiring == "inside_the_guard":
        bus = C.CommandBus(C.idempotent(ledger, key, unit_of_work=db), C.retry(3, pacing=NO_WAIT))
    elif case.wiring == "under_the_host":
        bus = C.CommandBus(C.retry(3, pacing=NO_WAIT), C.idempotent(ledger, key))
    else:
        bus = C.CommandBus(C.retry(3, pacing=NO_WAIT), C.idempotent(ledger, key, unit_of_work=db))
    bus.register("Deposit", deposit)
    command = Deposit("acct-1", 100, "req-1")

    async def dispatch() -> int:
        if case.wiring == "under_the_host":
            async with edax.transaction(db):
                return await bus.dispatch(command)
        return await bus.dispatch(command)

    if case.raises is None:
        assert await dispatch() == case.balance
        assert await dispatch() == case.balance, "a redelivery replays the committed outcome"
    else:
        with pytest.raises(case.raises, match="retry was dispatched inside an open transaction"):
            await dispatch()
    assert runs == case.runs
    committed = await accounts.get("acct-1", tx=db.auto_commit())
    assert (committed.document["balance"] if committed else 0) == case.balance
    assert db.open_count() == 0, "every unit of work ended"


@dataclass
class Unencodable:
    """A command whose payload the JSON encoder rejects — deterministic, so it must be terminal."""

    blob: object
    type = "user.create"


@dataclass
class IdemCase:
    key: str | None  # the key fn's return for every command
    cmd: Any
    errors: list[BaseException | None]  # per handler call; None = return normally
    store: type[CountingIdempotencyStore]
    raises: type[BaseException] | None
    calls: int  # handler runs after two dispatches of the same command
    claims: int
    releases: int


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            IdemCase(None, CreateUser("a@b.c"), [None, None], CountingIdempotencyStore, None, 2, 0, 0),
            id="key_none_bypasses_the_store",
        ),
        pytest.param(
            IdemCase("req-1", Unencodable(object()), [], CountingIdempotencyStore, edax.CodecError, 0, 0, 0),
            id="unencodable_payload_is_terminal_before_any_claim",
        ),
        # A claim nobody completes would raise InProgress on every redelivery until it expires, so a
        # failing complete releases it and the redelivery re-runs the handler. Which error the caller
        # sees differs by path: after a success the store's failure is the only news there is, but
        # after a failed handler the handler's own verdict is what `retry` and the dispatcher branch on,
        # so a refused compensation must not replace it.
        pytest.param(
            IdemCase(
                "req-1", CreateUser("a@b.c"), [None, None], BrokenCompleteStore, edax.StoreError, 2, 2, 2
            ),
            id="success_with_failing_complete_releases_the_claim",
        ),
        pytest.param(
            IdemCase(
                "req-1",
                CreateUser("a@b.c"),
                [edax.CodecError("bad"), edax.CodecError("bad")],
                BrokenCompleteStore,
                edax.CodecError,
                2,
                2,
                2,
            ),
            id="terminal_failure_with_failing_complete_shows_the_handlers_verdict",
        ),
    ],
)
async def test_idempotent_branches(case: IdemCase) -> None:
    store = case.store()
    bus = C.CommandBus(C.idempotent(store, lambda cmd: case.key))
    calls = 0

    async def handler(cmd: Any) -> str:
        nonlocal calls
        error = case.errors[calls]
        calls += 1
        if error is not None:
            raise error
        return "ok"

    bus.register("user.create", handler)

    for _ in range(2):  # a dispatch and its redelivery
        if case.raises is None:
            assert await bus.dispatch(case.cmd) == "ok"
        else:
            with pytest.raises(case.raises):
                await bus.dispatch(case.cmd)
    assert calls == case.calls
    assert (store.claims, store.releases) == (case.claims, case.releases)


@dataclass
class CancelCase:
    cancel_in: str  # which awaited seam step the cancellation lands in: "handler" or "complete"
    releases: int
    handler_ran: bool


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(CancelCase("handler", 1, True), id="cancelled_handler_releases_the_claim"),
        pytest.param(CancelCase("complete", 1, True), id="cancelled_complete_releases_the_claim"),
    ],
)
async def test_cancellation_never_strands_a_pending_claim(case: CancelCase) -> None:
    # A wait_for timeout or a shutdown cancels the dispatch mid-flight. The claim is pending and no
    # other task can ever complete it, so without a release every later dispatch of the key would
    # raise InProgress until the record expires (never, for the memory store).
    class CancellableStore(CountingIdempotencyStore):
        async def complete(self, key: str, handle: bytes, record: Record, tx: Tx | None = None) -> None:
            if case.cancel_in == "complete":
                raise asyncio.CancelledError()
            await super().complete(key, handle, record, tx)

    store = CancellableStore()
    bus = C.CommandBus(C.idempotent(store, lambda cmd: "req-1"))
    started = asyncio.Event()
    calls = 0

    async def handler(cmd: CreateUser) -> str:
        nonlocal calls
        calls += 1
        started.set()
        if case.cancel_in == "handler":
            await asyncio.Event().wait()  # parked until cancelled
        return f"user:{cmd.email}"

    bus.register("user.create", handler)

    task = asyncio.create_task(bus.dispatch(CreateUser("a@b.c")))
    await started.wait()
    if case.cancel_in == "handler":
        task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (calls > 0) is case.handler_ran
    assert store.releases == case.releases
    # The key is free again: a redelivery runs the handler rather than hitting a stranded claim.
    assert isinstance(await store.claim("req-1", 1), Won)


async def test_concurrent_dispatches_of_one_key_run_the_handler_once() -> None:
    # The atomic-claim promise inside one process: of N concurrent dispatches of a key, exactly one
    # wins and runs the handler; the losers see the pending record and raise InProgress rather than a
    # bogus None; once the winner completes, a later dispatch replays its result.
    store = MemoryIdempotencyStore()
    bus = C.CommandBus(C.idempotent(store, lambda cmd: "req-1"))
    release = asyncio.Event()
    calls = 0

    async def handler(cmd: CreateUser) -> str:
        nonlocal calls
        calls += 1
        await release.wait()
        return f"user:{cmd.email}"

    bus.register("user.create", handler)
    cmd = CreateUser("a@b.c")
    racers = [asyncio.create_task(bus.dispatch(cmd)) for _ in range(5)]
    # The native claim resolves on another thread, so one loop turn does not put every racer past its
    # claim. Release the winner only once the four losers have finished, or a late racer finds the
    # done record and replays the result instead of losing.
    while sum(t.done() for t in racers) < 4:
        await asyncio.sleep(0)
    release.set()
    outcomes = await asyncio.gather(*racers, return_exceptions=True)

    assert calls == 1
    assert outcomes.count("user:a@b.c") == 1
    assert sum(isinstance(o, edax.InProgress) for o in outcomes) == 4
    assert await bus.dispatch(cmd) == "user:a@b.c"  # replayed from the winner's record
    assert calls == 1


async def test_unserializable_result_is_terminal_and_releases_the_claim() -> None:
    # A handler result the record encoder rejects fails identically on every re-run. Untyped it would
    # be transient: the claim released, the message redelivered, the handler re-run — forever.
    store = CountingIdempotencyStore()
    bus = C.CommandBus(C.idempotent(store, lambda cmd: "req-1"))

    async def handler(cmd: CreateUser) -> object:
        return object()

    bus.register("user.create", handler)
    with pytest.raises(edax.CodecError, match="not JSON-encodable") as caught:
        await bus.dispatch(CreateUser("a@b.c"))
    assert caught.value.terminal is True
    assert store.releases == 1  # not left pending either


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param("{not json", id="malformed_json"),
        pytest.param('{"result": 1}', id="missing_payload_hash"),
        pytest.param("[1, 2]", id="not_an_object"),
    ],
)
def test_decode_record_rejects_unreadable_json_as_terminal(raw: str) -> None:
    with pytest.raises(
        edax.CodecError, match=r"not a JSON object|missing field|line 1 column|not valid JSON"
    ) as caught:
        C.decode_record(raw)
    assert caught.value.terminal is True


@pytest.mark.parametrize(
    "raw, want",
    [
        pytest.param(
            '{"payload_hash": 18446744073709551615, "result": {"id": 7}, "error": null}',
            (2**64 - 1, {"id": 7}, None, False),
            id="record_without_pending_field_reads_as_completed",
        ),
        pytest.param(
            '{"payload_hash": 1, "result": null, "error": null, "pending": true}',
            (1, None, None, True),
            id="pending_claim_is_not_a_none_success",
        ),
        pytest.param(
            '{"payload_hash": 1, "result": null, "error": "bad payload", "pending": false}',
            (1, None, "bad payload", False),
            id="cached_failure",
        ),
    ],
)
def test_record_decodes_cross_sdk_json(raw: str, want: tuple[int, Any, str | None, bool]) -> None:
    record = C.decode_record(raw)
    assert (record.payload_hash, record.result, record.error, record.pending) == want
    # The 64-bit hash survives the round trip losslessly (no float).
    assert C.decode_record(C.encode_record(record)).payload_hash == want[0]


# A real 64-bit payload hash above 2**63, and what a *signed* BIGINT column reads it back as. The
# IdempotencyStore contract allows a BIGINT; half of all hashes come back negative from a signed one.
BIG_HASH = 11_234_062_831_410_934_249
SIGNED_READBACK = BIG_HASH - 2**64


@dataclass
class HashCase:
    payload_hash: int
    terminal_error: bool


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(HashCase(0, False), id="zero_is_the_lower_boundary"),
        pytest.param(HashCase(BIG_HASH, False), id="past_2_63_is_still_in_range"),
        pytest.param(HashCase(2**64 - 1, False), id="u64_max_is_the_upper_boundary"),
        pytest.param(HashCase(SIGNED_READBACK, True), id="signed_bigint_readback_is_negative"),
        pytest.param(HashCase(2**64, True), id="above_u64_max"),
    ],
)
def test_stored_payload_hash_outside_64_bits_is_terminal(case: HashCase) -> None:
    # A hash a backend stored lossily is a deterministic store defect: every read of that key fails
    # the same way. Untyped it surfaces as a bare OverflowError, which is_terminal reads as
    # transient — so the dispatcher would nack and the broker redeliver it forever.
    record = Record(case.payload_hash, result={"user_id": "u-1"})
    if not case.terminal_error:
        assert C._outcome(record, case.payload_hash) == "succeeded"
        assert C.decode_record(C.encode_record(record)).payload_hash == case.payload_hash
        return
    for entry, call in (
        ("_outcome", lambda: C._outcome(record, 1)),
        ("encode", lambda: C.encode_record(record)),
    ):
        with pytest.raises(edax.CodecError, match="could not cross into the kernel") as caught:
            call()
        assert caught.value.terminal is True, entry


class SlowReleaseStore(MemoryIdempotencyStore):
    """A ledger whose ``release`` is a real round trip: it parks until the test opens the gate, so a
    cancel can land while the compensation is in flight."""

    def __init__(self) -> None:
        super().__init__()
        self.gate = asyncio.Event()
        self.released: list[str] = []

    async def release(self, key: str, handle: bytes, tx: Tx | None = None) -> None:
        await self.gate.wait()
        self.released.append(key)
        await super().release(key, handle, tx)


class SlowReleaseBrokenComplete(SlowReleaseStore):
    """...and recording the outcome fails, so the guard compensates from the success path instead
    of the cancellation path."""

    async def complete(self, key: str, handle: bytes, record: Record, tx: Tx | None = None) -> None:
        raise edax.StoreError("cannot record outcome")


@dataclass
class CancelCase:
    store: type[SlowReleaseStore]
    # How the guard reaches its release: "blocks" waits for the first cancel, "returns" succeeds and
    # `complete` fails, "fails" raises a transient error.
    handler: str


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(CancelCase(SlowReleaseStore, "blocks"), id="cancelled_handler_releases_the_claim"),
        pytest.param(
            CancelCase(SlowReleaseBrokenComplete, "returns"), id="failing_complete_releases_the_claim"
        ),
        pytest.param(CancelCase(SlowReleaseStore, "fails"), id="transient_failure_releases_the_claim"),
    ],
)
async def test_a_second_cancel_does_not_abandon_the_claim(case: CancelCase) -> None:
    """Shutdown cancels a dispatch, and the shutdown's own deadline cancels it again while the
    guard is releasing. Unshielded, a cancel landing in the release kills it, and the claim sits
    pending for the store's whole retention, raising ``InProgress`` on every redelivery of that key.
    The transient row was the unshielded one: a host deadline during that release abandoned the claim."""
    store = case.store()
    running = asyncio.Event()

    async def handler(cmd: CreateUser) -> str:
        running.set()
        if case.handler == "blocks":
            await asyncio.Event().wait()  # never returns; only a cancel gets us out
        if case.handler == "fails":
            raise ConnectionError("downstream call reset")
        return f"user:{cmd.email}"

    bus = C.CommandBus(C.idempotent(store, lambda cmd: "req-1"))
    bus.register("user.create", handler)
    task = asyncio.create_task(bus.dispatch(CreateUser("a@b.c")))
    await running.wait()

    task.cancel()  # first cancel: the guard enters its compensating release
    await asyncio.sleep(0.01)
    assert not store.released, "the release is in flight, parked on the gate"

    task.cancel()  # second cancel, landing inside the release
    store.gate.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.01)  # the shielded release outlives the task that started it

    assert store.released == ["req-1"]
    assert isinstance(await store.claim("req-1", 1), Won), "the key is free: the next dispatch can claim it"


def test_a_result_the_kernel_accepted_but_python_cannot_decode_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``decode_record`` owns the whole record, the host's ``result`` included. A kernel that hands
    back a ``result_json`` this SDK cannot parse — a newer writer, a wider number format — must
    still read as a terminal ``CodecError``, or the same stored record is a terminal fault through
    one backend and a bare ``msgspec`` error through another."""

    class _Decoded:
        state = "done"
        payload_hash = 11258580006047770801
        result_json = '{"balance_cents": 42,}'  # trailing comma: valid to no JSON parser
        error = ""

    monkeypatch.setattr(C._edax, "decode_idempotency_record", lambda raw: _Decoded())
    with pytest.raises(
        edax.CodecError, match=r"not a JSON object|missing field|line 1 column|not valid JSON"
    ) as caught:
        C.decode_record('{"payload_hash":11258580006047770801,"result":{},"error":null}')
    assert caught.value.terminal is True


async def test_a_cancel_at_the_claims_first_suspension_point_never_strands_the_key() -> None:
    """An ``async`` native store call would spawn the claim on a runtime thread at call time, and a
    cancel landing on its ``await`` would stop only the waiter. The claim would land unseen, and every
    redelivery would raise ``InProgress`` for the process lifetime. Synchronous native calls have no such
    window: the cancel lands before the claim or after the dispatch, never between them."""
    store = MemoryIdempotencyStore()
    runs: list[str] = []

    async def handler(cmd: CreateUser) -> str:
        runs.append(cmd.email)
        return f"user:{cmd.email}"

    bus = C.CommandBus(C.idempotent(store, lambda cmd: "req-1"))
    bus.register("user.create", handler)
    task = asyncio.create_task(bus.dispatch(CreateUser("a@b.c")))
    await asyncio.sleep(0)  # the dispatch runs up to its first suspension point
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task  # the cancel landed before the claim, or after the whole dispatch
    for _ in range(3):
        assert await bus.dispatch(CreateUser("a@b.c")) == "user:a@b.c", "replays or runs, never InProgress"
    assert runs == ["a@b.c"], "the handler ran exactly once"


@pytest.mark.parametrize("attempts", [0, -3], ids=["zero", "negative"])
def test_retry_refuses_fewer_than_one_attempt(attempts: int) -> None:
    """A count below one is a wiring typo; clamping it to one attempt would hide it."""
    with pytest.raises(ValueError, match=f"at least one attempt, got {attempts}"):
        C.retry(attempts)


async def test_the_bus_registers_by_decorator() -> None:
    """The command bus has the event router's decorator form, and the bare handler comes back."""
    bus = C.CommandBus()

    @bus.on("user.create")
    async def create(cmd: CreateUser) -> str:
        return cmd.email

    assert await bus.dispatch(CreateUser("ada@example.com")) == "ada@example.com"
    assert await create(CreateUser("grace@example.com")) == "grace@example.com", "the bare handler"
    with pytest.raises(edax.AlreadyRegistered):
        bus.on("user.create")(create)

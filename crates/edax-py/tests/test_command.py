import asyncio
import datetime
import json
import time
import uuid
from dataclasses import dataclass, field, is_dataclass
from pathlib import Path
from typing import Any, ClassVar, Optional

import edax
import msgspec
import pytest
from edax import command as C
from edax.backend.memory import MemoryIdempotencyStore
from edax.command import Record
from edax.store import Tx

GOLDEN = Path(__file__).resolve().parents[3] / "testdata" / "golden"


def test_golden_payload_hash() -> None:
    for c in json.loads((GOLDEN / "payload_hash.json").read_text()):
        assert edax.payload_hash(c["input"].encode()) == c["expected"]


def test_golden_idempotency() -> None:
    for c in json.loads((GOLDEN / "idempotency.json").read_text()):
        assert edax.idempotency_decision(prior=c["prior"], current=c["current"]) == c["expected"]


def test_golden_idempotency_record() -> None:
    # The record's wire shape, reading order and replay verdict are the kernel's; the SDK must read
    # the stored bytes to the same fields, re-emit the same bytes, and reach the same verdict. The
    # `null` result case is the one that used to diverge from the Rust SDK.
    for c in json.loads((GOLDEN / "idempotency_record.json").read_text()):
        record = C.decode_record(c["stored"])
        want = Record(c["payload_hash"], c.get("result"), c.get("error"), c["state"] == "pending")
        assert record == want, c["name"]
        assert C.encode_record(record) == c["encoded"], c["name"]
        assert C._outcome(record, c["current"]) == c["outcome"], c["name"]


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
    await store.complete("req-1", Record(payload_hash=1, result=result))
    prior = await store.claim("req-1", 1)
    assert prior is not None
    assert prior.result == want and type(prior.result) is want_type, branch
    assert (prior.result == result) is (branch != "below_i64_min_becomes_a_float_lossy")


def test_golden_canonical_payload_hash() -> None:
    # payload_hash_json canonicalizes in the kernel, so Python agrees with the Rust-generated golden
    # even for non-ASCII (the case a Python-side json.dumps(ensure_ascii=True) would get wrong).
    for c in json.loads((GOLDEN / "canonical_payload_hash.json").read_text()):
        assert edax.payload_hash_json(c["input"].encode()) == c["expected"], c["input"]


@dataclass
class GoldenShipping:
    city: str
    zip: Optional[str]


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
    note: Optional[str]
    customer: str
    currency: str
    type: ClassVar[str] = "order.place"


class GoldenShippingStruct(msgspec.Struct):
    city: str
    zip: Optional[str]


class GoldenPlaceOrderStruct(msgspec.Struct):
    request_id: str
    weight_kg: float
    total_cents: int
    tags: list[str]
    shipping: GoldenShippingStruct
    reference: int
    order_id: str
    note: Optional[str]
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
    note: Optional[str]
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
    cases = json.loads((GOLDEN / "command_hash.json").read_text())
    assert cases, "the vector has cases"

    async def handler(cmd: Any) -> None:
        return None

    for case in cases:
        cmd = _golden_command(case["type"], case["command"], shape)
        store = MemoryIdempotencyStore()
        bus = C.CommandBus(C.idempotent(store, lambda c: c.request_id))
        bus.register(case["type"], handler)
        await bus.dispatch(cmd)
        # The hash the middleware bound to the key is what a Rust service would compare its own
        # against; a claim with any hash returns the winner's record.
        prior = await store.claim(cmd.request_id, 0)
        assert prior is not None, (shape, case["name"])
        assert prior.payload_hash == case["expected"], (shape, case["name"])


def test_golden_command_hash_rejects_the_old_per_sdk_rules() -> None:
    # The two conventions the vector replaces. Neither may reach it, or the gap is not closed:
    # a Rust `payload()` that returned the business body without the key field, and a Python
    # command carrying `type` as an instance field.
    case = json.loads((GOLDEN / "command_hash.json").read_text())[0]
    assert case["name"] == "whole_command_including_the_idempotency_key_field"

    body_only = dict(case["command"])
    del body_only["request_id"]
    assert edax.payload_hash_json(msgspec.json.encode(body_only)) != case["expected"]

    # Built through the constructor, as a service writes `Deposit(...)`. A converted instance would
    # show the same divergence now that defaulted fields are read from the class; the constructor
    # is simply the shape that carried `type` before that was true.
    fields = dict(case["command"])
    fields["shipping"] = GoldenShipping(**fields["shipping"])
    with_type_field = GoldenPlaceOrderWithTypeField(**fields)
    assert C._payload_hash(with_type_field) != case["expected"]


@dataclass
class DefaultedShipping:
    city: str
    zip: Optional[str] = None


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
    reference: int = 9223372036854775809
    weight_kg: float = 2.5
    note: Optional[str] = None
    customer: str = "café ☕ 🦀"
    tags: list[str] = field(default_factory=lambda: ["eu", "paid"])
    type: ClassVar[str] = "order.place"


class DefaultedShippingStruct(msgspec.Struct):
    city: str
    zip: Optional[str] = None


class DefaultedPlaceOrderStruct(msgspec.Struct):
    """The Struct shape of :class:`DefaultedPlaceOrder`; ``shipping`` stays a *dataclass*, so a
    dataclass nested in a Struct is covered, and ``lines`` is a list of Structs."""

    request_id: str
    shipping: DefaultedShipping
    lines: list[DefaultedShippingStruct]
    order_id: str = "ord-7f3c"
    total_cents: int = 4200
    reference: int = 9223372036854775809
    weight_kg: float = 2.5
    note: Optional[str] = None
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
    "reference": 9223372036854775809,
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
    # field out of it, so the two used to hash differently. Both must equal the hash of the fully
    # materialized value, not merely each other: two encodings that both dropped the defaults
    # would agree and still be wrong.
    assert _same_command(case.built, case.decoded)
    expected = edax.payload_hash_json(msgspec.json.encode(DEFAULTED_ORDER))
    assert C._payload_hash(case.built) == expected
    assert C._payload_hash(case.decoded) == expected


@dataclass
class CreateUser:
    email: str
    # Unannotated, so it is a class attribute (out of `vars()` and out of the hashed payload) that
    # still satisfies the `Command` protocol, whose `type` is a read-only property. `ClassVar[str]`
    # is the declared form and satisfies it too; this case covers the bare attribute.
    type = "user.create"


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
    rejected_as: Optional[str]


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
    hash_ = edax.payload_hash_json(C._payload_json(cmd))
    assert await store.claim("req-1", hash_) is None  # simulate the in-flight winner (pending record)

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
    encoded: Optional[dict[str, Any]]  # the parsed payload, for an encodable command
    # An SDK error, not any exception: the case below asserts `terminal` on what it caught.
    raises: Optional[type[edax.EdaxError]] = None


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
    attr: Optional[str]  # the value that must come back as the same object; None = the command


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

    async def claim(self, key: str, payload_hash: int, tx: Optional[Tx] = None) -> Optional[Record]:
        self.claims += 1
        return await super().claim(key, payload_hash, tx)

    async def release(self, key: str, tx: Optional[Tx] = None) -> None:
        self.releases += 1
        await super().release(key, tx)


class BrokenCompleteStore(CountingIdempotencyStore):
    """Recording the outcome fails on its own (an unserializable result, a store hiccup)."""

    async def complete(self, key: str, record: Record, tx: Optional[Tx] = None) -> None:
        raise edax.StoreError("cannot record outcome")


async def test_retry_inside_idempotent_shares_one_claim() -> None:
    store = CountingIdempotencyStore()
    # Order matters: idempotent first (outermost), retry inside it.
    bus = C.CommandBus(C.idempotent(store, lambda cmd: "req-1"), C.retry(2))
    calls = 0

    async def handler(cmd: CreateUser) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient downstream blip")  # not terminal
        return f"user:{cmd.email}"

    bus.register("user.create", handler)

    started = time.monotonic()
    assert await bus.dispatch(CreateUser("a@b.c")) == "user:a@b.c"
    assert calls == 2
    assert store.claims == 1  # both attempts ran under one claim
    assert store.releases == 0  # never released — the retry succeeded under the same claim
    assert time.monotonic() - started >= edax.backoff_millis(0) / 1000  # one backoff slept

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


@dataclass
class RetryCase:
    attempts: int
    errors: list[Optional[BaseException]]  # per handler call; None = return normally
    raises: Optional[type[BaseException]]
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
            RetryCase(3, [asyncio.CancelledError()], asyncio.CancelledError, 1, 0),
            id="cancellation_is_not_retried",
        ),
        pytest.param(RetryCase(0, [RuntimeError("blip")], RuntimeError, 1, 0), id="zero_attempts_runs_once"),
    ],
)
async def test_retry_branches(case: RetryCase, monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(C.asyncio, "sleep", fake_sleep)
    bus = C.CommandBus(C.retry(case.attempts))
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
    # The kernel's schedule: failed attempt i sleeps backoff_millis(i) before attempt i + 1.
    assert slept == [edax.backoff_millis(i) / 1000 for i in range(case.sleeps)]


@dataclass
class Unencodable:
    """A command whose payload the JSON encoder rejects — deterministic, so it must be terminal."""

    blob: object
    type = "user.create"


@dataclass
class IdemCase:
    key: Optional[str]  # the key fn's return for every command
    cmd: Any
    errors: list[Optional[BaseException]]  # per handler call; None = return normally
    store: type[CountingIdempotencyStore]
    raises: Optional[type[BaseException]]
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
        async def complete(self, key: str, record: Record, tx: Optional[Tx] = None) -> None:
            if case.cancel_in == "complete":
                raise asyncio.CancelledError()
            await super().complete(key, record, tx)

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
    assert await store.claim("req-1", 1) is None


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
    await asyncio.sleep(0)  # every racer reaches its claim before the winner is released
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
    with pytest.raises(edax.CodecError, match="unreadable") as caught:
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
def test_record_decodes_cross_sdk_json(raw: str, want: tuple[int, Any, Optional[str], bool]) -> None:
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
        with pytest.raises(edax.CodecError, match="64-bit range") as caught:
            call()
        assert caught.value.terminal is True, entry


class SlowReleaseStore(MemoryIdempotencyStore):
    """A ledger whose ``release`` is a real round trip: it parks until the test opens the gate, so a
    cancel can land while the compensation is in flight."""

    def __init__(self) -> None:
        super().__init__()
        self.gate = asyncio.Event()
        self.released: list[str] = []

    async def release(self, key: str, tx: Optional[Tx] = None) -> None:
        await self.gate.wait()
        self.released.append(key)
        await super().release(key, tx)


class SlowReleaseBrokenComplete(SlowReleaseStore):
    """...and recording the outcome fails, so the guard compensates from the success path instead
    of the cancellation path."""

    async def complete(self, key: str, record: Record, tx: Optional[Tx] = None) -> None:
        raise edax.StoreError("cannot record outcome")


@dataclass
class CancelCase:
    store: type[SlowReleaseStore]
    handler_blocks: bool  # False: the handler returns and `complete` fails instead


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(CancelCase(SlowReleaseStore, True), id="cancelled_handler_releases_the_claim"),
        pytest.param(CancelCase(SlowReleaseBrokenComplete, False), id="failing_complete_releases_the_claim"),
    ],
)
async def test_a_second_cancel_does_not_abandon_the_claim(case: CancelCase) -> None:
    """Shutdown cancels a dispatch, and the shutdown's own deadline cancels it again while the
    guard is releasing. Unshielded, the second cancel kills the release and the claim sits pending
    for the store's whole retention, raising ``InProgress`` on every redelivery of that key."""
    store = case.store()
    running = asyncio.Event()

    async def handler(cmd: CreateUser) -> str:
        running.set()
        if case.handler_blocks:
            await asyncio.Event().wait()  # never returns; only a cancel gets us out
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
    assert await store.claim("req-1", 1) is None, "the key is free: the next dispatch can claim it"


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
    with pytest.raises(edax.CodecError, match="unreadable") as caught:
        C.decode_record('{"payload_hash":11258580006047770801,"result":{},"error":null}')
    assert caught.value.terminal is True

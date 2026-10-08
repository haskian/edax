"""A cached success replays as the handler's declared return type, not as the JSON it was stored as.

The first dispatch returns the handler's own object. A replay that handed back a ``dict`` would let caller
code such as ``result.invoice_id`` work in tests and fail only on the path idempotency exists for. Each
branch of the replay-type choice is a row below, and the crash-and-redeliver rows run over every ledger
ordering: the joining memory store, the memory ledger of its own, and Redis (the kernel's Lua on fakeredis).
"""

from __future__ import annotations

import datetime
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, ClassVar

import edax
import msgspec
import pytest
from conftest import TTL_MS, Recording
from edax.backend.memory import MemoryIdempotencyStore, MemoryLedger, MemoryTransactor
from edax.backend.redis import AsyncRedisClient, RedisIdempotencyStore
from edax.command import CommandBus, idempotent


@dataclass
class IssueInvoice:
    order_id: str
    total_cents: int
    request_id: str
    type: ClassVar[str] = "IssueInvoice"


@dataclass
class Invoice:
    invoice_id: uuid.UUID
    order_id: str
    total: Decimal
    issued_at: datetime.datetime


class InvoiceStruct(msgspec.Struct):
    invoice_id: str
    total_cents: int


ISSUED_AT = datetime.datetime(2026, 10, 3, 9, 30, tzinfo=datetime.timezone.utc)
# The invoice as the SDK's encoder writes it: canonical decimal text, so `50.00` replays as `50`, and the
# fixed-width UTC timestamp.
INVOICE_JSON: dict[str, Any] = {
    "invoice_id": "01a10285-7d7c-71a1-8fb8-3caf0732bb67",
    "order_id": "order-7",
    "total": "50",
    "issued_at": "2026-10-03T09:30:00.000000Z",
}


def command() -> IssueInvoice:
    return IssueInvoice("order-7", 5000, "req-7")


def invoice() -> Invoice:
    return Invoice(uuid.UUID("01a10285-7d7c-71a1-8fb8-3caf0732bb67"), "order-7", Decimal("50.00"), ISSUED_AT)


async def issue(cmd: IssueInvoice) -> Invoice:
    return invoice()


async def issue_struct(cmd: IssueInvoice) -> InvoiceStruct:
    return InvoiceStruct("inv-7", cmd.total_cents)


async def issue_optional(cmd: IssueInvoice) -> Invoice | None:
    return None


async def issue_untyped(cmd):  # type: ignore[no-untyped-def]
    return invoice()


class Receipt(msgspec.Struct):
    total: edax.Decimal
    issued_at: edax.Instant


RECEIPT = Receipt(edax.Decimal("50.00"), edax.Instant("2026-10-03T10:30:00+01:00"))


async def issue_receipt(cmd: IssueInvoice) -> Receipt:
    return RECEIPT


async def issue_total(cmd: IssueInvoice) -> edax.Decimal:
    return RECEIPT.total


async def issue_object(cmd: IssueInvoice) -> object:
    return {"invoice_id": "inv-7"}


async def issue_any(cmd: IssueInvoice) -> Any:
    return {"invoice_id": "inv-7"}


async def issue_forward_ref(cmd: IssueInvoice) -> NotDefinedAnywhere:  # type: ignore[name-defined]  # noqa: F821  # ty: ignore[unresolved-reference]
    return {"invoice_id": "inv-7"}


@dataclass(frozen=True)
class Case:
    name: str
    handler: Callable[..., Any]
    result_type: Any
    #: What the replay must be, compared with ``==``, and its type.
    want: Any
    want_type: type


CASES = [
    Case("inferred_from_the_handlers_annotation", issue, None, invoice(), Invoice),
    Case("explicit_result_type_wins", issue_untyped, Invoice, invoice(), Invoice),
    Case("a_struct_annotation", issue_struct, None, InvoiceStruct("inv-7", 5000), InvoiceStruct),
    Case("an_optional_annotation_replays_none", issue_optional, None, None, type(None)),
    Case("host_type_fields_replay_from_their_text", issue_receipt, None, RECEIPT, Receipt),
    Case("a_host_type_annotation", issue_total, None, edax.Decimal("50"), edax.Decimal),
    Case(
        "no_annotation_replays_plain_json",
        issue_untyped,
        None,
        INVOICE_JSON,
        dict,
    ),
    Case("an_object_annotation_replays_plain_json", issue_object, None, {"invoice_id": "inv-7"}, dict),
    Case("an_any_annotation_replays_plain_json", issue_any, None, {"invoice_id": "inv-7"}, dict),
    Case("an_explicit_any_opts_out", issue, Any, INVOICE_JSON, dict),
    Case(
        "an_unresolvable_annotation_replays_plain_json",
        issue_forward_ref,
        None,
        {"invoice_id": "inv-7"},
        dict,
    ),
]


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
async def test_a_replay_is_the_type_the_handler_declares(case: Case) -> None:
    ledger = MemoryIdempotencyStore()
    bus = CommandBus(
        idempotent(ledger, result_type=case.result_type, scope="test", tenancy=edax.Tenancy.SILO)
    )
    bus.register(IssueInvoice, case.handler)
    first = await bus.dispatch(command(), idempotency_key="req-7")
    replay = await bus.dispatch(command(), idempotency_key="req-7")
    assert type(replay) is case.want_type, case.name
    assert replay == case.want, case.name
    if case.want_type is not dict:
        assert replay == first, "the replay equals what the first dispatch returned"


class Opaque:
    """A class msgspec has no conversion for: neither a dataclass nor a Struct nor a JSON type."""


async def issue_opaque(cmd: IssueInvoice) -> Opaque:
    return Opaque()


@pytest.mark.parametrize(
    "handler, result_type",
    [(issue_opaque, None), (issue, Opaque)],
    ids=["an_inferred_type_msgspec_cannot_convert", "an_explicit_type_msgspec_cannot_convert"],
)
def test_a_type_with_no_conversion_is_refused_at_wiring(handler: Any, result_type: Any) -> None:
    bus = CommandBus(
        idempotent(MemoryIdempotencyStore(), result_type=result_type, scope="test", tenancy=edax.Tenancy.SILO)
    )
    with pytest.raises(TypeError, match="cannot be replayed as"):
        bus.register(IssueInvoice, handler)


async def test_a_cached_result_that_does_not_convert_replays_as_a_cached_codec_failure() -> None:
    """A record written by another SDK, or under an older result type, that no longer converts is an
    unreadable record, as the Rust SDK's typed store reads it: the kernel's terminal codec failure, replayed
    as ``CachedFailure`` on every redelivery, so the handler never runs and a dispatcher dead-letters it.
    The observer hears ``failed``, the verdict the replay carries."""
    ledger = MemoryIdempotencyStore()
    writer = CommandBus(idempotent(ledger, result_type=Any, scope="test", tenancy=edax.Tenancy.SILO))

    async def old_shape(cmd: IssueInvoice) -> dict[str, str]:
        return {"invoice": "inv-7"}

    writer.register(IssueInvoice, old_shape)
    await writer.dispatch(command(), idempotency_key="req-7")
    observer = Recording()
    reader = CommandBus(idempotent(ledger, observer=observer, scope="test", tenancy=edax.Tenancy.SILO))
    reader.register(IssueInvoice, issue)
    for attempt in ("first delivery", "redelivery"):
        with pytest.raises(
            edax.CachedFailure, match="codec: unreadable idempotency record: cached result"
        ) as caught:
            await reader.dispatch(command(), idempotency_key="req-7")
        assert caught.value.terminal is True and caught.value.original_code == "codec", attempt
        assert "does not convert to" in str(caught.value), attempt
    assert observer.claims == [("IssueInvoice", "failed")] * 2


@pytest.fixture(params=["joining_memory_store", "memory_ledger", "redis"])
def ledger_factory(request: pytest.FixtureRequest, redis_client: Any) -> Callable[[], tuple[Any, Any]]:
    """A ledger and the unit of work its guard opens, for each ordering; called once per process, so every
    process after the first is a new bus over the same ledger."""
    if request.param == "joining_memory_store":
        db = MemoryTransactor()
        store = MemoryIdempotencyStore(db)
        return lambda: (store, db)
    if request.param == "memory_ledger":
        ledger = MemoryLedger()
        unit = MemoryTransactor()
        return lambda: (ledger, unit)
    prefix = f"idem:{uuid.uuid4().hex}:"
    redis_store = RedisIdempotencyStore(AsyncRedisClient(redis_client), prefix, ttl_ms=TTL_MS)
    return lambda: (redis_store, None)


async def test_a_redelivery_after_a_crash_replays_the_declared_type(
    ledger_factory: Callable[[], tuple[Any, Any]],
) -> None:
    """The process that ran the handler dies after the record settled; the broker redelivers to a fresh
    process, whose bus has never seen the command. It must hand its caller an ``Invoice``, not a ``dict``,
    and run nothing."""
    runs = 0

    async def handler(cmd: IssueInvoice) -> Invoice:
        nonlocal runs
        runs += 1
        return invoice()

    def process() -> CommandBus:
        store, unit = ledger_factory()
        bus = CommandBus(idempotent(store, unit_of_work=unit, scope="test", tenancy=edax.Tenancy.SILO))
        bus.register(IssueInvoice, handler)
        return bus

    first = await process().dispatch(command(), idempotency_key="req-7")
    redelivered = await process().dispatch(
        command(), idempotency_key="req-7"
    )  # the first process crashed after settling
    assert isinstance(redelivered, Invoice) and redelivered == first == invoice()
    assert redelivered.issued_at == ISSUED_AT and redelivered.total == Decimal("50.00")
    assert runs == 1

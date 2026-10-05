"""The command message a dispatch runs: how it is built, what the handler sees, how a guard claims its key,
how a received message is run again, how a record is forgotten, and the optional command outbox."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, ClassVar

import edax
import msgspec
import pytest
from conftest import CounterCodec, CreateUser, Tick, TickEvents, claimed, counter_repo
from edax import (
    CommandMessage,
    Lineage,
    _edax,
    command_message,
    current_command,
    forget,
    idempotent,
    idempotent_by_id,
    json_codec,
    msgpack_codec,
)
from edax._async import maybe_await
from edax._values import replace
from edax.backend.memory import (
    MemoryCommandOutbox,
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemoryTransactor,
)
from edax.idempotency import Done
from edax.outbox import Relay
from edax.unit_of_work import transaction


@dataclass
class PlaceOrder:
    """A realistic command: the payload a client sends, nested, with its own schema version."""

    order_id: str
    customer_id: str
    lines: list[dict[str, Any]] = field(default_factory=list)
    type: ClassVar[str] = "order.place"
    version: ClassVar[int] = 2


ORDER = PlaceOrder("ord-7f3c", "cust-88", [{"sku": "sku-1", "quantity": 2, "price_cents": 2100}])
ORDER_JSON = (
    b'{"order_id":"ord-7f3c","customer_id":"cust-88",'
    b'"lines":[{"sku":"sku-1","quantity":2,"price_cents":2100}]}'
)
FLOW = Lineage("01a10285-7d7c-71a1-8fb8-3caf0732bb67", "01a10285-7d7c-71a1-8fb8-3caf0732bb68")
# PlaceOrder is at v2 with its v1 shape: the identity step is the bump.
ORDERS = json_codec(
    {"order.place": PlaceOrder}, upcasters={"order.place": edax.Upcaster().step(1, lambda d: d)}
)


@dataclass
class BadVersion:
    order_id: str
    type: ClassVar[str] = "order.place"
    version: ClassVar[int] = -1


@dataclass
class ShipV1:
    """The command as a version-1 producer still sends it: no carrier yet."""

    order_id: str
    type: ClassVar[str] = "order.ship"


@dataclass
class Ship:
    """The current, version-2 command. It declares no ``version``: its codec owns the schema version."""

    order_id: str
    carrier: str
    type: ClassVar[str] = "order.ship"


SHIP = Ship("ord-7f3c", "ups")


def _carrier_unknown(data: bytes) -> bytes:
    """The v1 -> v2 step: a v1 order named no carrier. Run on v2 data, it would overwrite the carrier."""
    return msgspec.json.encode({**msgspec.json.decode(data), "carrier": "unknown"})


SHIPMENTS = json_codec(
    {"order.ship": Ship}, upcasters={"order.ship": edax.Upcaster().step(1, _carrier_unknown)}
)


# =========================================================================
# command_message: the one builder dispatch and a publisher share
# =========================================================================


@dataclass(frozen=True)
class BuildCase:
    name: str
    cmd: Any
    kwargs: dict[str, Any]
    ambient: Lineage | None
    want: dict[str, Any]  # fields the message must carry; "flow" is "new" for a fresh root lineage
    raises: str | None = None


BUILD_CASES = [
    BuildCase(
        "defaults_take_the_type_the_class_version_and_a_new_flow",
        ORDER,
        {},
        None,
        {"name": "order.place", "version": 2, "data": ORDER_JSON, "flow": "new", "idempotency_key": None},
    ),
    BuildCase(
        "a_command_without_a_version_is_version_1",
        CreateUser("ada@example.com"),
        {},
        None,
        {"name": "user.create", "version": 1, "data": b'{"email":"ada@example.com"}', "flow": "new"},
    ),
    BuildCase(
        "the_ambient_lineage_is_used_when_none_is_given",
        ORDER,
        {},
        FLOW,
        {"correlation_id": FLOW.correlation_id, "causation_id": FLOW.causation_id},
    ),
    BuildCase(
        "an_explicit_lineage_wins_over_the_ambient_one",
        ORDER,
        {"lineage": Lineage("corr-explicit", "cause-explicit")},
        FLOW,
        {"correlation_id": "corr-explicit", "causation_id": "cause-explicit"},
    ),
    BuildCase(
        "keys_extensions_and_metadata_are_carried",
        ORDER,
        {
            "idempotency_key": "req-7",
            "aggregate_id": "ord-7f3c",
            "extensions": {"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
            "metadata": {"tenant": "acme"},
        },
        None,
        {
            "idempotency_key": "req-7",
            "aggregate_id": "ord-7f3c",
            "extensions": {"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
            "metadata": {"tenant": "acme"},
        },
    ),
    BuildCase(
        "an_extension_key_outside_the_kernel_rule_is_refused",
        ORDER,
        {"extensions": {"Trace Parent": "x"}},
        None,
        {},
        raises="extension key",
    ),
    BuildCase(
        "a_version_outside_u32_is_refused",
        BadVersion("ord-1"),
        {},
        None,
        {},
        raises="argument could not cross into the kernel",
    ),
    BuildCase(
        "a_codec_that_names_the_command_otherwise_is_refused",
        ORDER,
        {"codec": json_codec({"order.placed": PlaceOrder})},
        None,
        {},
        raises="a message name must be its type",
    ),
]


@pytest.mark.parametrize("case", BUILD_CASES, ids=lambda c: c.name)
def test_command_message_branches(case: BuildCase) -> None:
    ambient = case.ambient

    def build() -> CommandMessage:
        if ambient is None:
            return command_message(case.cmd, **case.kwargs)
        with edax.use_lineage(ambient):
            return command_message(case.cmd, **case.kwargs)

    if case.raises is not None:
        with pytest.raises(edax.CodecError, match=case.raises) as caught:
            build()
        assert caught.value.terminal, case.name
        return
    msg = build()
    for name, want in case.want.items():
        if name == "flow":
            assert msg.causation_id == msg.correlation_id != "", "a root's causation is its correlation"
            continue
        assert getattr(msg, name) == want, (case.name, name)
    assert msg.id and msg.issued_at > 0, case.name
    assert _edax.command_hash_json(msg.name, msg.data) == edax.command._payload_hash(
        case.cmd, case.cmd.type
    ), "the message data is the bytes the guard hashes"


# =========================================================================
# dispatch: the current command and the lineage of what the handler saves
# =========================================================================


async def test_the_handler_sees_its_message_and_the_events_it_saves_are_its_children() -> None:
    store = MemoryEventStore()
    repo = counter_repo(store, iter(range(1, 100)))
    seen: list[CommandMessage | None] = []

    async def place(cmd: PlaceOrder) -> list[edax.EventMessage]:
        seen.append(current_command())
        counter = await repo.load(cmd.order_id)
        counter.apply(Tick(2))
        return await repo.save(counter)

    bus = edax.CommandBus()
    bus.register(PlaceOrder, place)
    events = await bus.dispatch(ORDER, idempotency_key="req-7", aggregate_id="ord-7f3c", lineage=FLOW)

    (msg,) = seen
    assert msg is not None and msg.idempotency_key == "req-7" and msg.aggregate_id == "ord-7f3c"
    assert (msg.correlation_id, msg.causation_id) == (FLOW.correlation_id, FLOW.causation_id)
    assert [(e.correlation_id, e.causation_id) for e in events] == [(FLOW.correlation_id, msg.id)]
    assert current_command() is None and edax.current_lineage() is None, "both end with the dispatch"


# =========================================================================
# idempotent: the scope, the key, the hash
# =========================================================================


async def test_an_ingress_and_a_consumer_guard_on_one_ledger_both_run() -> None:
    """One request id reaches the API and then a worker. The two guards share a ledger but not a key,
    so neither replays the other's outcome; each replays its own."""
    ledger = MemoryIdempotencyStore()
    runs: list[str] = []

    def bus_for(stage: str) -> edax.CommandBus:
        async def place(cmd: PlaceOrder) -> str:
            runs.append(stage)
            return f"{stage}:{cmd.order_id}"

        bus = edax.CommandBus(idempotent(ledger, scope=stage, tenancy=edax.Tenancy.SILO))
        bus.register(PlaceOrder, place)
        return bus

    api, worker = bus_for("api"), bus_for("worker")
    for _ in range(2):
        assert await api.dispatch(ORDER, idempotency_key="req-7") == "api:ord-7f3c"
        assert await worker.dispatch(ORDER, idempotency_key="req-7") == "worker:ord-7f3c"
    assert runs == ["api", "worker"], "each stage ran once and replayed its own record"
    for stage in ("api", "worker"):
        record = await ledger.claim(claimed("req-7", stage), 0)
        assert isinstance(record, edax.Record) and record.state == Done(f"{stage}:ord-7f3c")


@pytest.mark.parametrize(
    "guard, match",
    [
        pytest.param(idempotent, r"idempotent\(scope=\.\.\.\), which names the guard,", id="command_guard"),
        pytest.param(
            idempotent_by_id,
            r"idempotent_by_id\(scope=\.\.\.\), which names the consumer,",
            id="event_guard",
        ),
    ],
)
def test_a_scope_that_is_not_a_str_is_refused_at_wiring(guard: Callable[..., Any], match: str) -> None:
    not_a_str: Any = None
    with pytest.raises(edax.BadHandle, match=match):
        guard(MemoryIdempotencyStore(), scope=not_a_str, tenancy=edax.Tenancy.SILO)


# =========================================================================
# dispatch_message: a received message, run again
# =========================================================================


@pytest.mark.parametrize(
    "key, want_key",
    [(None, "id"), ("req-7", "req-7")],
    ids=["no_key_is_claimed_under_the_message_id", "a_key_is_kept"],
)
async def test_a_redelivered_message_runs_its_handler_once(key: str | None, want_key: str) -> None:
    runs: list[CommandMessage | None] = []

    async def place(cmd: PlaceOrder) -> str:
        runs.append(current_command())
        return cmd.order_id

    ledger = MemoryIdempotencyStore()
    bus = edax.CommandBus(idempotent(ledger, scope="orders-worker", tenancy=edax.Tenancy.SILO), codec=ORDERS)
    bus.register(PlaceOrder, place)
    msg = command_message(ORDER, codec=ORDERS, idempotency_key=key, lineage=FLOW)
    assert await bus.dispatch_message(msg) == "ord-7f3c"
    assert await bus.dispatch_message(msg) == "ord-7f3c", "the redelivery replays"

    (seen,) = runs
    assert seen == msg, "the handler sees the message as received, its key included"
    record = await ledger.claim(claimed(msg.id if want_key == "id" else want_key, "orders-worker"), 0)
    assert isinstance(record, edax.Record) and record.state == Done("ord-7f3c"), "claimed under that key"


@pytest.mark.parametrize(
    "name, data, raises",
    [
        ("order.cancel", ORDER_JSON, edax.HandlerNotFound),
        ("order.place", b'{"order_id":', edax.CodecError),
    ],
    ids=["an_unknown_name_has_no_handler", "data_the_codec_cannot_read_is_terminal"],
)
async def test_a_message_the_bus_cannot_run_is_refused(
    name: str, data: bytes, raises: type[Exception]
) -> None:
    async def place(cmd: PlaceOrder) -> None:
        raise AssertionError("never runs")

    bus = edax.CommandBus(codec=ORDERS)
    bus.register(PlaceOrder, place)
    msg = replace(command_message(ORDER), name=name, version=2, data=data)
    with pytest.raises(raises):
        await bus.dispatch_message(msg)


@pytest.mark.parametrize(
    "codec, version",
    [(SHIPMENTS, 2), (msgpack_codec({"order.ship": Ship}), 1)],
    ids=["a_v2_codec_stamps_its_version_so_nothing_is_upcast", "a_msgpack_codec_encodes_the_data"],
)
async def test_a_message_built_with_the_consumers_codec_decodes_as_sent(
    codec: edax.CommandCodec, version: int
) -> None:
    seen: list[Ship] = []

    async def ship(cmd: Ship) -> None:
        seen.append(cmd)

    bus = edax.CommandBus(codec=codec)
    bus.register(Ship, ship)
    msg = command_message(SHIP, codec=codec)
    assert (msg.name, msg.version, msg.data) == ("order.ship", version, codec.marshal(SHIP).data)
    await bus.dispatch_message(msg)
    assert seen == [SHIP], "the handler got the command as sent, carrier included"


async def _ship(cmd: Ship) -> str:
    return cmd.carrier


@pytest.mark.parametrize(
    "codec, wire, want",
    [
        pytest.param(
            ORDERS, lambda bus: bus.register(Ship, _ship), "reads no 'order.ship' command", id="register"
        ),
        # The codec still maps the name to the class a v1 producer sent: the handler would get a `ShipV1`,
        # and `dispatch(Ship(...))` would fail to marshal on every call.
        pytest.param(
            json_codec({"order.ship": ShipV1}),
            lambda bus: bus.register(Ship, _ship),
            "reads 'order.ship' as ShipV1, not Ship",
            id="register_a_class_the_codec_reads_the_name_as_another",
        ),
        pytest.param(
            None,
            lambda bus: bus.dispatch_message(command_message(SHIP, codec=SHIPMENTS)),
            "build it with CommandBus\\(codec=",
            id="dispatch_message_with_no_codec",
        ),
    ],
)
async def test_a_bus_that_could_never_decode_a_command_is_refused_at_wiring(
    codec: edax.CommandCodec | None, wire: Callable[[edax.CommandBus], object], want: str
) -> None:
    """The bus's codec is the one map from a command's name to its class. A handler for a command it does not
    name, or names as another class, and a received message on a bus with none, would fail every message;
    each is the terminal BadHandle at wiring instead."""
    bus = edax.CommandBus(codec=codec)
    with pytest.raises(edax.BadHandle, match=want):
        await maybe_await(wire(bus))


async def test_dispatch_builds_its_message_with_the_buss_codec() -> None:
    """A bus with a codec builds the message :meth:`dispatch` runs as that codec writes it, so middleware and
    the handler see the message a broker would have delivered: the codec's schema version, not 1."""
    seen: list[CommandMessage | None] = []

    async def ship(cmd: Ship) -> str:
        seen.append(current_command())
        return cmd.carrier

    bus = edax.CommandBus(codec=SHIPMENTS)
    bus.register(Ship, ship)
    assert await bus.dispatch(SHIP) == "ups"
    (msg,) = seen
    assert msg is not None and (msg.name, msg.version, msg.data) == (
        "order.ship",
        2,
        SHIPMENTS.marshal(SHIP).data,
    )


async def test_an_upcast_message_hashes_as_its_current_version() -> None:
    """A v1 message, upcast on receipt, and the equal v2 command under the same key are one request: the
    guard hashes the value the handler receives, after the upcast."""
    upcast = Ship("ord-7f3c", "unknown")
    runs: list[Ship] = []

    async def ship(cmd: Ship) -> str:
        runs.append(cmd)
        return cmd.carrier

    bus = edax.CommandBus(
        idempotent(MemoryIdempotencyStore(), scope="orders-worker", tenancy=edax.Tenancy.SILO),
        codec=SHIPMENTS,
    )
    bus.register(Ship, ship)
    v1 = command_message(ShipV1("ord-7f3c"), idempotency_key="ship-ord-7f3c")
    assert v1.version == 1
    assert await bus.dispatch_message(v1) == "unknown"
    assert await bus.dispatch(upcast, idempotency_key="ship-ord-7f3c") == "unknown", "the v2 dispatch replays"
    assert runs == [upcast], "the handler ran once"


# =========================================================================
# forget: re-drive a dead-lettered command
# =========================================================================


async def test_forget_lets_a_cached_failure_run_again() -> None:
    ledger = MemoryIdempotencyStore()
    runs = 0

    async def place(cmd: PlaceOrder) -> str:
        nonlocal runs
        runs += 1
        if runs == 1:
            raise edax.Rejected("the warehouse is closed")
        return "placed"

    bus = edax.CommandBus(idempotent(ledger, scope="orders-worker", tenancy=edax.Tenancy.SILO))
    bus.register(PlaceOrder, place)
    with pytest.raises(edax.Rejected):
        await bus.dispatch(ORDER, idempotency_key="req-7")
    with pytest.raises(edax.CachedFailure):
        await bus.dispatch(ORDER, idempotency_key="req-7")
    assert runs == 1, "the cached failure replays without a run"

    await forget(ledger, "orders-worker", "req-7", tenancy=edax.Tenancy.SILO)
    assert await bus.dispatch(ORDER, idempotency_key="req-7") == "placed"
    assert runs == 2, "the re-driven command ran again"


# =========================================================================
# The command outbox: commands commit with the events that caused them
# =========================================================================


@dataclass
class Rig:
    db: MemoryTransactor
    events: MemoryEventStore
    commands: MemoryCommandOutbox
    repo: edax.Repository[Any, Any]


def rig() -> Rig:
    db = MemoryTransactor()
    events = MemoryEventStore(db)
    commands = MemoryCommandOutbox(db)
    repo = edax.Repository(events, TickEvents(), CounterCodec(), commands=commands)
    return Rig(db, events, commands, repo)


@pytest.mark.parametrize("fails", [False, True], ids=["commit_lands_both", "rollback_drops_both"])
async def test_saved_commands_commit_or_roll_back_with_the_events(fails: bool) -> None:
    r = rig()
    counter = await r.repo.load("ord-7f3c")
    counter.apply(Tick(2))
    ship = command_message(ORDER, idempotency_key="ship-ord-7f3c", aggregate_id="ord-7f3c")
    try:
        async with transaction(r.db):
            await r.repo.save(counter, commands=[ship])
            if fails:
                raise RuntimeError("a later write in the same unit of work failed")
    except RuntimeError:
        assert fails

    want = 0 if fails else 1
    assert (len(await r.events.unpublished(100)), await r.commands.unpublished_count()) == (want, want)
    if fails:
        return
    published: list[CommandMessage] = []
    relay = Relay(r.commands, published.append)
    assert await relay.drain() == 1
    assert published == [ship]
    assert await r.commands.unpublished_count() == 0, "the relay marked the row"


async def test_save_with_commands_needs_an_outbox() -> None:
    store = MemoryEventStore()
    repo = counter_repo(store, iter(range(1, 100)))
    counter = await repo.load("ord-7f3c")
    counter.apply(Tick(2))
    with pytest.raises(TypeError, match="needs a Repository built with commands="):
        await repo.save(counter, commands=[command_message(ORDER)])
    assert await store.unpublished(100) == [] and counter.pending, "nothing was written"


async def test_an_outbox_over_another_database_is_refused() -> None:
    outbox = MemoryCommandOutbox()
    async with MemoryTransactor().begin() as foreign:
        with pytest.raises(edax.BadHandle):
            await outbox.enqueue(foreign, [command_message(ORDER)])
    assert await outbox.unpublished_count() == 0

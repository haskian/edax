"""Tenancy end to end: the kernel's key rule, and every seam that keys by it.

The kernel's own table is `testdata/golden/tenancy.json`, read by `test_edax.py`. What is proved here
is the wiring: that a pool composes every key it touches, that a tenant cannot be forgotten where the
mode requires one, and that two tenants sharing one store, one ledger, one saga store and one topic
never read each other's rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import edax
import pytest
from conftest import SpyProducerClient
from edax import Aggregate, CommandBus, Repository, Tenancy, Version, json_codec, tenant_key
from edax.backend.kafka import KafkaProducer
from edax.backend.memory import (
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemorySagaStore,
    MemoryTransactor,
)
from edax.command import idempotent
from edax.errors import CodecError
from edax.saga import SagaRunner
from edax.testing import event

TENANTS = ("acme", "globex")


@dataclass
class Deposited:
    amount: int


@dataclass
class Fulfill:
    order_id: str


class Account(Aggregate):
    def __init__(self, id: str, initial: int = 0) -> None:
        super().__init__(id, initial)
        self.balance = 0

    def transition(self, event: Any) -> None:
        if isinstance(event, Deposited):
            self.balance += event.amount


class AccountCodec:
    type = "Account"

    def load(self, id: str, version: int, data: bytes | None) -> Account:
        account = Account(id, version)
        if data is not None:
            account.balance = int(data.decode())
        return account

    def dump(self, account: Account) -> bytes:
        return str(account.balance).encode()


def repository(mode: Tenancy, store: MemoryEventStore, *, snapshot_every: int = 1) -> Repository[Account]:
    events = json_codec({"Deposited": Deposited})
    return Repository(store, events, AccountCodec(), tenancy=mode, snapshot_every=snapshot_every)


# ---- the mode itself -----------------------------------------------------


@pytest.mark.parametrize(
    "mode, requires_tenant, composes_keys",
    [
        (Tenancy.SILO, False, False),  # one deployment per tenant: nothing to say, nothing to compose
        (Tenancy.POOL, True, True),  # everything shared: a key without its tenant reads the pool
        (Tenancy.BRIDGE, True, False),  # a store per tenant: the tenant routes, the key stays bare
    ],
)
def test_a_mode_reports_the_kernels_rules(mode: Tenancy, requires_tenant: bool, composes_keys: bool) -> None:
    """The two rules come from the kernel, so the SDK cannot drift from the golden by restating them."""
    assert (mode.requires_tenant, mode.composes_keys) == (requires_tenant, composes_keys)
    assert mode.value == str(mode.value), "the value is the name the kernel reads"


@pytest.mark.parametrize(
    "name, mode, tenant, expected",
    [
        ("pool composes", Tenancy.POOL, "acme", "4:acme:order-1"),
        ("bridge leaves the key bare", Tenancy.BRIDGE, "acme", "order-1"),
        ("silo takes no tenant", Tenancy.SILO, None, "order-1"),
        ("silo ignores one it is given", Tenancy.SILO, "acme", "order-1"),
    ],
)
def test_tenant_key_composes_only_where_the_mode_says(
    name: str, mode: Tenancy, tenant: str | None, expected: str
) -> None:
    assert tenant_key(mode, "order-1", tenant) == expected, name


@pytest.mark.parametrize(
    "name, mode, tenant",
    [
        ("a pool cannot key without a tenant", Tenancy.POOL, None),
        ("a bridge cannot route without one", Tenancy.BRIDGE, None),
        ("the empty string is not a tenant", Tenancy.POOL, ""),
        ("over the length limit", Tenancy.POOL, "a" * (edax.TENANT_MAX + 1)),
    ],
)
def test_a_bad_tenant_is_terminal(name: str, mode: Tenancy, tenant: str | None) -> None:
    """Terminal, not transient: the same call fails the same way on every retry, so a guard caches it
    rather than releasing a claim for a key it will never accept."""
    with pytest.raises(CodecError):
        tenant_key(mode, "order-1", tenant)


@pytest.mark.parametrize(
    "name, tenant, raises",
    [
        ("a plain name", "acme", False),
        ("the separator is allowed: the key rule is injective anyway", "a:b", False),
        ("at the limit", "a" * edax.TENANT_MAX, False),
        ("empty", "", True),
        ("over the limit", "a" * (edax.TENANT_MAX + 1), True),
    ],
)
def test_check_tenant_bounds_the_id_but_not_its_charset(name: str, tenant: str, raises: bool) -> None:
    if raises:
        with pytest.raises(CodecError):
            edax.check_tenant(tenant)
    else:
        edax.check_tenant(tenant)


def test_two_tenants_never_share_a_composed_key() -> None:
    """The collision a bare separator would allow: tenant `a:b` with key `c`, against tenant `a` with
    key `b:c`. The kernel's length prefix is what keeps them apart, and this is the one case a host
    cannot be asked to remember."""
    assert tenant_key(Tenancy.POOL, "c", "a:b") != tenant_key(Tenancy.POOL, "b:c", "a")


# ---- the write side ------------------------------------------------------


async def test_a_pool_keeps_two_tenants_streams_apart() -> None:
    """One store, one aggregate id, two tenants: two streams, each at its own version. A repository
    that keyed by the bare id would fold both tenants' events into one aggregate."""
    repo = repository(Tenancy.POOL, MemoryEventStore())
    for tenant, amount in zip(TENANTS, (10, 70), strict=True):
        account = await repo.load("order-1", tenant=tenant)
        account.apply(Deposited(amount))
        await repo.save(account)

    for tenant, amount in zip(TENANTS, (10, 70), strict=True):
        account = await repo.load("order-1", tenant=tenant)
        assert (account.balance, account.version) == (amount, Version(1)), tenant
        assert account.tenant == tenant, "the aggregate carries the tenant it was loaded for"


async def test_a_pool_stamps_the_tenant_on_every_event_it_saves() -> None:
    """The stamp is what lets a relay route the row and a consumer attribute the fact, so it is the
    repository's job and not the caller's."""
    repo = repository(Tenancy.POOL, MemoryEventStore())
    account = await repo.load("order-1", tenant="acme")
    account.apply(Deposited(10), Deposited(5))
    stored = await repo.save(account)
    assert [msg.tenant for msg in stored] == ["acme", "acme"]
    assert {msg.aggregate_id for msg in stored} == {"order-1"}, "the id stays the host's own, bare"


async def test_a_pool_keys_the_snapshot_by_tenant_too() -> None:
    """A snapshot is a cache of one stream. Keyed by the bare id it would hand one tenant's state to
    another, and no replay afterwards could notice: the version would agree."""
    store = MemoryEventStore()
    repo = repository(Tenancy.POOL, store, snapshot_every=1)
    acme = await repo.load("order-1", tenant="acme")
    acme.apply(Deposited(99))
    await repo.save(acme)

    globex = await repo.load("order-1", tenant="globex")
    assert (globex.balance, globex.version) == (0, Version(0)), "no snapshot of another tenant"


@pytest.mark.parametrize(
    "name, mode, tenant, fails",
    [
        ("a pool refuses a load with no tenant", Tenancy.POOL, None, True),
        ("a bridge refuses one too", Tenancy.BRIDGE, None, True),
        ("a silo expects none", Tenancy.SILO, None, False),
        ("a pool with a tenant is fine", Tenancy.POOL, "acme", False),
    ],
)
async def test_a_mode_that_requires_a_tenant_refuses_a_load_without_one(
    name: str, mode: Tenancy, tenant: str | None, fails: bool
) -> None:
    repo = repository(mode, MemoryEventStore())
    if fails:
        with pytest.raises(CodecError):
            await repo.load("order-1", tenant=tenant)
    else:
        assert (await repo.load("order-1", tenant=tenant)).version == Version(0), name


async def test_a_save_cannot_name_a_tenant_the_load_did_not() -> None:
    """The tenant travels on the aggregate, so there is no second argument to get wrong. A save after a
    silo load into a pool repository is the one mismatch left, and it fails before any write."""
    store = MemoryEventStore()
    account = await repository(Tenancy.SILO, store).load("order-1")
    account.apply(Deposited(10))
    with pytest.raises(CodecError):
        await repository(Tenancy.POOL, store).save(account)
    async with store.begin() as tx:
        assert await store.load(tx, "order-1") == [], "and wrote nothing"


async def test_a_bridge_leaves_the_keys_bare() -> None:
    """A bridge gives each tenant its own store, so the key carries no tenant. Two tenants pointed at
    one store therefore share a stream: that is the model working as documented, not a leak."""
    store = MemoryEventStore()
    repo = repository(Tenancy.BRIDGE, store)
    acme = await repo.load("order-1", tenant="acme")
    acme.apply(Deposited(10))
    stored = await repo.save(acme)
    assert stored[0].tenant == "acme", "the tenant still travels, for routing and for the audit trail"

    globex = await repo.load("order-1", tenant="globex")
    assert globex.balance == 10, "one store, one stream: a bridge separates by store, not by key"


# ---- the command side ----------------------------------------------------


@dataclass
class OpenAccount:
    owner: str

    type = "OpenAccount"


@pytest.mark.parametrize(
    "name, mode, want_runs",
    [
        ("a pool separates two tenants' claims", Tenancy.POOL, 2),
        ("a silo reads one request id once", Tenancy.SILO, 1),
    ],
)
async def test_an_idempotency_claim_is_keyed_by_tenant_in_a_pool(
    name: str, mode: Tenancy, want_runs: int
) -> None:
    """Two tenants, one shared ledger, the same client request id and the same payload: only the tenant
    differs. In a pool each tenant claims its own row and the handler runs twice; without the
    composition the second tenant replays the first's cached result, which is the leak."""
    runs: list[str | None] = []
    bus = CommandBus(idempotent(MemoryIdempotencyStore(), scope="api", tenancy=mode))

    @bus.on("OpenAccount")
    async def open_account(cmd: OpenAccount) -> str:
        runs.append(cmd.owner)
        return f"account:{cmd.owner}"

    results = [
        await bus.dispatch(OpenAccount("ada"), idempotency_key="req-1", tenant=tenant) for tenant in TENANTS
    ]
    assert results == ["account:ada", "account:ada"], "the same request, so the same answer"
    assert len(runs) == want_runs, name


# ---- sagas ---------------------------------------------------------------


class Shipping:
    """A saga whose instance id carries the tenant, which is how one saga store serves a pool: the
    correlation is the host's to derive, so the composition belongs here and not in the runner."""

    name = "shipping"

    def __init__(self, mode: Tenancy = Tenancy.SILO) -> None:
        self._mode = mode

    def instance_id(self, msg: Any) -> str | None:
        return tenant_key(self._mode, msg.aggregate_id, msg.tenant)

    def handle(self, state: str | None, msg: Any) -> tuple[Any, list[Fulfill]]:
        return ("seen" if state is None else f"{state}+seen"), [Fulfill(msg.aggregate_id)]


async def test_a_pool_saga_folds_one_instance_per_tenant() -> None:
    """Two tenants' events share a correlation id. Keyed bare they would fold into one instance, and
    one tenant's state would decide the other's commands."""
    store = MemorySagaStore(MemoryTransactor())
    runner = SagaRunner(Shipping(Tenancy.POOL), store, json_codec({"Fulfill": Fulfill}))
    for tenant in TENANTS:
        await runner.on_event(event("order-7", "OrderPaid", 1, id=f"{tenant}-evt-1", tenant=tenant))

    for tenant in TENANTS:
        instance = await store.load(tenant_key(Tenancy.POOL, "order-7", tenant))
        assert instance is not None and instance.state == "seen", tenant
    assert await store.load("order-7") is None, "nothing under the bare correlation id"


async def test_a_saga_command_inherits_the_folded_events_tenant() -> None:
    """The command a step emits must claim and key inside the tenant of the fact that caused it, so
    the whole flow stays in one tenant."""
    store = MemorySagaStore(MemoryTransactor())
    runner = SagaRunner(Shipping(Tenancy.POOL), store, json_codec({"Fulfill": Fulfill}))
    await runner.on_event(event("order-7", "OrderPaid", 1, tenant="acme"))
    rows = await store.unpublished(10)
    assert [row.tenant for row in rows] == ["acme"], "the command inherits the folded event's tenant"
    instance = tenant_key(Tenancy.POOL, "order-7", "acme")
    assert [row.aggregate_id for row in rows] == [instance], "targeted and ordered by the instance"


# ---- the broker ----------------------------------------------------------


@pytest.mark.parametrize(
    "name, mode, key_fn, want_key",
    [
        ("a pool keys by the composed key", Tenancy.POOL, None, b"4:acme:order-1"),
        ("a silo keys by the ordering key", Tenancy.SILO, None, b"order-1"),
        ("a bridge keys bare: its topic is the tenant's", Tenancy.BRIDGE, None, b"order-1"),
        ("an explicit key wins outright", Tenancy.POOL, lambda msg: msg.tenant or "", b"acme"),
    ],
)
async def test_the_record_key_follows_the_producers_tenancy(
    name: str, mode: Tenancy, key_fn: Any, want_key: bytes
) -> None:
    """Two tenants' same-named aggregates must not share a key, or they share a partition and a
    key-ordered consumer serializes them together."""
    client = SpyProducerClient()
    producer = KafkaProducer(client, "events", key=key_fn, tenancy=mode)
    await producer.publish(event("order-1", "Deposited", 1, tenant="acme"))
    assert client.sent[0][2] == want_key, name


# ---- the wire ------------------------------------------------------------


@pytest.mark.parametrize(
    "name, tenant, raises",
    [
        ("a tenant round trips as a typed field", "acme", False),
        ("none is the single-tenant default", None, False),
        ("an over-long tenant is refused", "a" * (edax.TENANT_MAX + 1), True),
    ],
)
def test_the_codec_holds_a_tenant_to_the_kernels_rule(name: str, tenant: str | None, raises: bool) -> None:
    """Checked on encode and on decode both: the wire is a trust boundary, so a tenant minted by
    another service is held to the rule before any key is composed from it."""
    serializer = edax.KernelSerializer()
    msg = event("order-1", "Deposited", 1, tenant=tenant)
    if raises:
        with pytest.raises(CodecError):
            serializer.encode(msg)
        return
    decoded = serializer.decode(serializer.encode(msg))
    assert decoded.tenant == tenant, name

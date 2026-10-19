"""A subject's projection state and intent payloads at rest, in the memory and Postgres stores: one case
per branch of sealing them."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, replace
from typing import Any, Literal
from uuid import uuid4

import pytest
from edax import _json, _native
from edax.backend.postgres import PgConnection, PostgresTransactor
from edax.backend.projection_postgres import PostgresProjectionStateStore, reference_projection_state_ddl
from edax.errors import BadHandle, Shredded
from edax.projection import (
    MemoryProjectionStateStore,
    ProjectionEvent,
    ProjectionInput,
    ProjectionIntent,
    ProjectionResult,
    ProjectionState,
)
from edax.sealing import Cipher, KeyReservation, KeyScope, MemoryKeys, Sealing
from test_projection_state import event, initial_cutover, proof, target

URL = os.environ.get("EDAX_POSTGRES_URL")
EMAIL = "ana@example.com"


class CountingKeys(MemoryKeys):
    """The reference provider, recording each reservation's subject and count."""

    def __init__(self) -> None:
        super().__init__()
        self.reserved: list[tuple[str | None, int]] = []

    async def reserve(self, scope: KeyScope, cipher: Cipher, count: int = 1) -> KeyReservation:
        self.reserved.append((scope.subject, count))
        return await super().reserve(scope, cipher, count)


Store = MemoryProjectionStateStore | PostgresProjectionStateStore


@pytest.fixture(params=["memory", "postgres"])
async def backend(request: pytest.FixtureRequest) -> AsyncIterator[Callable[..., Store]]:
    if request.param == "memory":
        yield lambda t, sealing: MemoryProjectionStateStore(t, sealing=sealing)
        return
    if URL is None:
        pytest.skip("EDAX_POSTGRES_URL is not set: no live Postgres")
    asyncpg = pytest.importorskip("asyncpg")
    pool = await asyncpg.create_pool(URL, min_size=1, max_size=3)
    schema = f"projection_at_rest_{uuid4().hex}"
    await pool.execute(f'CREATE SCHEMA "{schema}"')
    try:
        await pool.execute(reference_projection_state_ddl(schema))
        tx = PostgresTransactor(pool, schema=schema)
        yield lambda t, sealing: PostgresProjectionStateStore(tx, t, schema=schema, sealing=sealing)
    finally:
        await pool.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


async def raw_state(store: Store, key: str) -> Any:
    """The state record exactly as the store holds it, nothing opened."""
    if isinstance(store, MemoryProjectionStateStore):
        return _json.decode(store._database.get().read(_json.encode([key])))["records"].get(key)

    async def read(conn: PgConnection) -> Any:
        rows = await store._call(conn.fetch(store._sql["read"], _json.encode([key])))
        return _json.decode(rows[0]["value"]) if rows else None

    return await store._owned(read)


async def raw_intents(store: Store) -> list[Any]:
    """The pending intent records exactly as the store holds them."""
    t = store.target
    if isinstance(store, MemoryProjectionStateStore):
        return list(_json.decode(store._database.get().unpublished(t.manifest_key, t.generation, 100)))

    async def read(conn: PgConnection) -> list[Any]:
        rows = await store._call(conn.fetch(store._sql["pending"], t.manifest_key, t.generation, "100"))
        return [_json.decode(row["value"]) for row in rows]

    return await store._owned(read)


def owned(sequence: int, subject: str | None) -> ProjectionEvent:
    """A credit of account-42, owned by ``subject`` as the runner sets it from a sealed envelope."""
    return replace(event(sequence, f"credit-{sequence}"), subject=subject)


def statement(state: ProjectionState | None, value: ProjectionEvent) -> ProjectionResult:
    """The account holder's email is personal; the balance is public."""
    balance = (state.public["balance_minor"] if state else 0) + value.payload["amount_minor"]
    private = {"email": EMAIL, "credits": (state.private["credits"] if state else 0) + 1}
    receipt = ProjectionIntent("receipt", "account-notifications", {"email": EMAIL, "balance_minor": balance})
    return ProjectionResult(private=private, public={"balance_minor": balance}, intents=[receipt])


async def committed(store: Store, subject: str | None, *sequences: int) -> None:
    """Commit one batch of ``subject``'s credits at ``sequences`` and activate delivery."""
    await store.initialize()
    lease = await store.acquire(0, "worker-a", proof())
    lease = await store.recover(lease)
    inputs = [ProjectionInput(lease.next_offset + i, owned(n, subject)) for i, n in enumerate(sequences)]
    await store.commit(lease, inputs, proof(), statement)


@dataclass(frozen=True)
class Case:
    name: str
    at_rest: Literal["sealed", "plaintext"] = "sealed"
    subject: str | None = "person-7"
    #: Whether the stored ``private`` state is sealed; a subject's intent payloads always are.
    state_sealed: bool = True
    #: The batch's reservations: one per subject, counting every part it seals.
    reserved: tuple[tuple[str | None, int], ...] = (("person-7", 4),)


CASES = [
    Case("sealed_state_and_intents_share_one_reservation"),
    Case("state_without_subject_stays_plaintext", subject=None, state_sealed=False, reserved=()),
    Case(
        "plaintext_target_still_seals_intent_payloads",
        at_rest="plaintext",
        state_sealed=False,
        reserved=(("person-7", 2),),
    ),
]


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
async def test_state_and_intents_at_rest(backend: Callable[..., Store], case: Case) -> None:
    keys = CountingKeys()
    sealing = Sealing(keys, "accounts")
    t = replace(target(), at_rest=case.at_rest)
    store = backend(t, sealing)
    await committed(store, case.subject, 1, 2)
    assert tuple(keys.reserved) == case.reserved
    raw = await raw_state(store, t.state_key("bank-eu", case.subject, "account-42"))
    assert raw["public"] == {"balance_minor": 2500}
    if case.state_sealed:
        envelope = _native.decode_protected_state(_json.encode(raw["private"])).value
        assert envelope is not None and (envelope.tenant, envelope.subject) == ("bank-eu", case.subject)
    else:
        assert raw["private"] == {"email": EMAIL, "credits": 2}
    pending = await raw_intents(store)
    assert len(pending) == 0  # a building generation delivers nothing
    await store.activate(initial_cutover(t))
    pending = await raw_intents(store)
    assert [intent["subject"] for intent in pending] == [case.subject, case.subject]
    assert (EMAIL in _json.encode(pending)) is (case.subject is None)
    state = await store.get("bank-eu", "account-42", case.subject)
    assert state is not None and state["sequence"] == 2
    assert (state["private"], state["public"]) == ({"email": EMAIL, "credits": 2}, {"balance_minor": 2500})
    delivered = await store.unpublished(100)
    assert [intent.payload for intent in delivered] == [
        {"email": EMAIL, "balance_minor": 1250},
        {"email": EMAIL, "balance_minor": 2500},
    ]


async def test_reducer_reads_the_sealed_state_a_previous_batch_left(backend: Callable[..., Store]) -> None:
    sealing = Sealing(MemoryKeys(), "accounts")
    store = backend(target(), sealing)
    await committed(store, "person-7", 1)
    await committed(store, "person-7", 2)
    state = await store.get("bank-eu", "account-42", "person-7")
    assert state is not None and state["private"] == {"email": EMAIL, "credits": 2}


async def test_store_without_sealing_writes_nothing(backend: Callable[..., Store]) -> None:
    store = backend(target(), None)
    with pytest.raises(BadHandle, match="sealing="):
        await committed(store, "person-7", 1)
    assert await store.get("bank-eu", "account-42", "person-7") is None


async def test_destroyed_key_refuses_the_read_and_drops_its_intents(backend: Callable[..., Store]) -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "accounts")
    store = backend(target(), sealing)
    await committed(store, "person-7", 1)
    await store.activate(initial_cutover(store.target))
    await keys.destroy(sealing.scope("bank-eu", "person-7"))
    with pytest.raises(Shredded):
        await store.get("bank-eu", "account-42", "person-7")
    assert await store.unpublished(100) == []
    assert await raw_intents(store) == []

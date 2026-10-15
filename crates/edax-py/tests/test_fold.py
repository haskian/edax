"""Ordered deltas, transactional rollback, recovery and effect cutovers across both stores."""

from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from typing import Any
from uuid import uuid4

import pytest
from conftest import golden
from edax import _edax, _json
from edax.errors import BadHandle
from edax.fold import (
    FoldCutover,
    FoldEvent,
    FoldInput,
    FoldIntent,
    FoldOutbox,
    FoldResult,
    FoldStore,
    FoldTarget,
    MemoryFoldStore,
    SourceProof,
)


def target(generation: str = "balance-v1") -> FoldTarget:
    return FoldTarget(
        generation,
        "cluster-42/topic-81",
        "no-rollback-1",
        "account-events",
        "balance-reducer-1",
        "envelope-1",
        "account-key-1",
        "receipt-effect-1",
        {0: 0, 1: 0},
    )


def proof(beginning: int = 0, end: int = 100) -> SourceProof:
    return SourceProof("cluster-42/topic-81", "no-rollback-1", beginning, end)


def event(sequence: int = 1, event_id: str = "credit-1", *, amount: int = 1250) -> FoldEvent:
    return FoldEvent(
        "bank-eu",
        "account-42",
        event_id,
        sequence,
        "CreditPosted",
        "1",
        {"amount_minor": amount, "currency": "EUR"},
        {"correlation_id": "payment-99"},
    )


def reducer(state: Any, value: FoldEvent) -> FoldResult:
    balance = (state or {}).get("balance_minor", 0) + value.payload["amount_minor"]
    return FoldResult(
        {"balance_minor": balance, "currency": "EUR"},
        [FoldIntent("receipt", "account-notifications", {"account": value.aggregate})],
    )


def initial_cutover(t: FoldTarget) -> FoldCutover:
    return FoldCutover(t.source_identity, t.source_history, t.topic, t.starts)


@pytest.fixture(params=["memory", "postgres"])
async def stores(request: Any) -> Any:
    if request.param == "memory":
        yield lambda t: MemoryFoldStore(t)
        return
    url = os.environ.get("EDAX_POSTGRES_URL")
    if url is None:
        if os.environ.get("EDAX_REQUIRE_POSTGRES") == "1":
            pytest.fail("EDAX_POSTGRES_URL required")
        pytest.skip("EDAX_POSTGRES_URL not set")
    import asyncpg
    from edax.backend.fold_postgres import PostgresFoldStore, reference_fold_ddl
    from edax.backend.postgres import PostgresTransactor

    pool = await asyncpg.create_pool(url, min_size=1, max_size=4)
    schema = f"fold_{uuid4().hex}"
    await pool.execute(f'CREATE SCHEMA "{schema}"')
    try:
        await pool.execute(reference_fold_ddl(schema))
        tx = PostgresTransactor(pool, schema=schema)
        yield lambda t: PostgresFoldStore(tx, t, schema=schema)
    finally:
        await pool.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


async def test_deltas_duplicate_new_offset_and_uncertain_commit(stores: Any) -> None:
    store: FoldStore = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker-a", proof())
    first = await store.commit(lease, [FoldInput(3, event())], proof(), reducer)
    recovered = await store.recover(lease)  # lost commit response
    assert recovered == first and first.next_offset == 4
    recovered = await store.commit(recovered, [FoldInput(8, event())], proof(), reducer)
    assert recovered.next_offset == 9
    state = await store.get("bank-eu", "account-42")
    assert state and state["value"]["balance_minor"] == 1250
    assert not await store.pending()  # rebuilding suppresses delivery
    await store.activate(initial_cutover(store.target))
    assert len(await store.pending()) == 1


async def test_poison_second_delta_rolls_back_entire_batch(stores: Any) -> None:
    store: FoldStore = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker-a", proof())
    with pytest.raises(Exception, match="sequence gap"):
        await store.commit(
            lease, [FoldInput(0, event()), FoldInput(1, event(3, "credit-3"))], proof(), reducer
        )
    assert (await store.recover(lease)).next_offset == 0
    assert await store.get("bank-eu", "account-42") is None
    await store.activate(initial_cutover(store.target))
    assert not await store.pending()


@pytest.mark.parametrize(
    "changed",
    [
        replace(event(), event_id="another-id"),
        replace(event(), sequence=2),
        replace(event(), aggregate="account-43"),
        replace(event(), payload={"amount_minor": 1300, "currency": "EUR"}),
        replace(event(), metadata={"correlation_id": "different-payment"}),
    ],
)
async def test_every_reducer_visible_identity_conflict_stops(stores: Any, changed: FoldEvent) -> None:
    store: FoldStore = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker-a", proof())
    lease = await store.commit(lease, [FoldInput(0, event())], proof(), reducer)
    with pytest.raises(Exception, match="conflicting fold identity"):
        await store.commit(lease, [FoldInput(1, changed)], proof(), reducer)
    assert (await store.recover(lease)).next_offset == 1


async def test_noop_advances_sequence_and_null_state_is_real(stores: Any) -> None:
    store: FoldStore = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker", proof())
    lease = await store.commit(
        lease,
        [FoldInput(0, event()), FoldInput(1, event(2, "noop"))],
        proof(),
        lambda state, value: FoldResult(None),
    )
    state = await store.get("bank-eu", "account-42")
    assert state == {"partition": 0, "sequence": 2, "value": None}
    assert lease.next_offset == 2


async def test_rebuild_cutoff_and_restore_repeat_stable_effect_id(stores: Any) -> None:
    old: FoldStore = stores(target())
    new: FoldStore = stores(target("balance-v2"))
    for store in (old, new):
        await store.initialize()
    await old.activate(initial_cutover(old.target))
    old_lease = await old.acquire(0, "old", proof())
    await old.commit(old_lease, [FoldInput(0, event())], proof(), reducer)
    delivered: set[str] = set()
    calls: list[str] = []

    async def destination(intent: Any) -> None:
        calls.append(intent.id)
        delivered.add(intent.id)

    intent = (await old.pending())[0]
    await destination(intent)  # crash before publication marker
    assert await FoldOutbox(old, destination).drain() == 1
    assert calls == [intent.id, intent.id] and len(delivered) == 1
    cutover = await old.freeze()
    assert cutover.cutoffs == {0: 1, 1: 0}
    new_lease = await new.acquire(0, "new", proof())
    new_lease = await new.commit(
        new_lease, [FoldInput(0, event()), FoldInput(1, event(2, "credit-2"))], proof(), reducer
    )
    assert not await new.pending()
    await new.activate(cutover)
    pending = await new.pending()
    assert len(pending) == 1 and pending[0].offset == 1
    with pytest.raises(Exception, match="immutable"):
        await new.activate(initial_cutover(new.target))
    with pytest.raises(Exception, match="frozen"):
        await old.acquire(0, "obsolete", proof())
    assert new_lease.next_offset == 2


async def test_source_loss_and_obsolete_assignment_rejected_before_reducer(stores: Any) -> None:
    store: FoldStore = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker", proof())

    def forbidden(state: Any, value: FoldEvent) -> FoldResult:
        pytest.fail("reducer ran without source continuity")

    with pytest.raises(Exception, match="history is missing"):
        await store.commit(lease, [FoldInput(4, event())], proof(beginning=1), forbidden)
    with pytest.raises(BadHandle, match="obsolete"):
        await store.commit(lease, [FoldInput(0, event())], proof(), forbidden, is_current=lambda: False)
    assert (await store.recover(lease)).next_offset == 0


async def test_takeover_and_stale_release(stores: Any) -> None:
    store: FoldStore = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "a", proof(), lease_ms=1)
    await asyncio.sleep(0.005)
    newer = await store.acquire(0, "b", proof())
    assert newer.epoch > lease.epoch
    with pytest.raises(Exception, match="stale fold"):
        await store.commit(lease, [FoldInput(0, event())], proof(), reducer)
    await store.release(lease)
    assert await store.recover(newer) == newer


async def test_changed_source_cannot_activate_old_cutover(stores: Any) -> None:
    store: FoldStore = stores(target())
    await store.initialize()
    with pytest.raises(Exception, match="another source history"):
        await store.activate(replace(initial_cutover(store.target), source_identity="recreated-topic"))
    assert not await store.pending()


def test_fold_golden_across_python_binding() -> None:
    for case in golden("fold.json"):
        args = (_json.encode(case["request"]), _json.encode(case["records"]), case["now"])
        if case["error"] is not None:
            with pytest.raises(RuntimeError) as error:
                _edax.fold_transition(*args)
            assert str(error.value) == case["error"], case["name"]
        else:
            assert _json.decode(_edax.fold_transition(*args)) == case["want"], case["name"]


async def test_foreign_generation_lease_is_rejected(stores: Any) -> None:
    old, new = stores(target()), stores(target("balance-v2"))
    for store in (old, new):
        await store.initialize()
    old_lease = await old.acquire(0, "same-owner", proof())
    new_lease = await new.acquire(0, "same-owner", proof())
    with pytest.raises(Exception, match="stale fold"):
        await new.commit(old_lease, [FoldInput(0, event())], proof(), reducer)
    await new.release(old_lease)
    assert await new.recover(new_lease) == new_lease
    assert await new.get("bank-eu", "account-42") is None


async def test_canonical_reducer_input_and_state_match_across_rebuilds(stores: Any) -> None:
    states = []
    for generation, payload in [
        ("balance-v1", {"z": 1, "number": 1.0}),
        ("balance-v2", {"number": 1, "z": 1}),
    ]:
        store = stores(target(generation))
        await store.initialize()
        lease = await store.acquire(0, "worker", proof())

        def inspect(state: Any, value: FoldEvent) -> FoldResult:
            return FoldResult(
                {
                    "first_key": next(iter(value.payload)),
                    "number_type": type(value.payload["number"]).__name__,
                    "zero": -0.0,
                }
            )

        await store.commit(lease, [FoldInput(0, replace(event(), payload=payload))], proof(), inspect)
        states.append(await store.get("bank-eu", "account-42"))
    assert states[0] == states[1]
    assert states[0]["value"] == {"first_key": "number", "number_type": "int", "zero": 0}


async def test_expired_current_assignment_refreshes_fence(stores: Any) -> None:
    store = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker", proof(), lease_ms=1)
    await asyncio.sleep(0.005)
    refreshed = await store.refresh(lease, proof())
    assert refreshed.epoch > lease.epoch
    await store.commit(refreshed, [FoldInput(0, event())], proof(), reducer)
    assert (await store.get("bank-eu", "account-42"))["sequence"] == 1


async def test_lost_acquire_response_reuses_same_logical_acquisition(stores: Any) -> None:
    store = stores(target())
    await store.initialize()
    first = await store.acquire(0, "unique-assignment-owner", proof())
    replay = await store.acquire(0, "unique-assignment-owner", proof())
    assert first == replay


async def test_inactive_generation_cannot_supply_an_effect_cutover(stores: Any) -> None:
    store = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker", proof())
    await store.commit(lease, [FoldInput(0, event())], proof(), reducer)
    with pytest.raises(Exception, match="inactive fold"):
        await store.freeze()
    await store.activate(initial_cutover(store.target))
    assert len(await store.pending()) == 1


@pytest.mark.parametrize("timeout", [float("inf"), float("nan"), -1, 0, True])
def test_outbox_requires_finite_positive_deadline(timeout: float) -> None:
    async def deliver(intent: Any) -> None:
        pass

    with pytest.raises(BadHandle):
        FoldOutbox(MemoryFoldStore(target()), deliver, timeout_s=timeout)


async def test_outbox_timeout_preserves_pending_intent(stores: Any) -> None:
    store = stores(target())
    await store.initialize()
    await store.activate(initial_cutover(store.target))
    lease = await store.acquire(0, "worker", proof())
    await store.commit(lease, [FoldInput(0, event())], proof(), reducer)

    async def deliver(intent: Any) -> None:
        await asyncio.Event().wait()

    with pytest.raises(asyncio.TimeoutError):
        await FoldOutbox(store, deliver, timeout_s=0.001).drain()
    assert len(await store.pending()) == 1


@pytest.mark.parametrize("where", ["envelope", "state", "intent"])
async def test_normalized_unsafe_integer_is_refused_before_persistence(stores: Any, where: str) -> None:
    store = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker", proof())
    value = replace(event(), payload={"amount": 1e18}) if where == "envelope" else event()

    def unsafe(state: Any, value: FoldEvent) -> FoldResult:
        return FoldResult(
            {"amount": 1e18} if where == "state" else {"amount": "1000000000000000000"},
            [FoldIntent("receipt", "notifications", {"amount": 1e18})] if where == "intent" else [],
        )

    with pytest.raises(Exception, match="integer"):
        await store.commit(lease, [FoldInput(0, value)], proof(), unsafe)
    assert await store.get("bank-eu", "account-42") is None
    assert (await store.recover(lease)).next_offset == 0

"""Ordered deltas, transactional rollback, recovery and effect cutovers across both stores."""

from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from typing import Any
from uuid import uuid4

import pytest
from conftest import NO_WAIT, golden
from edax import _edax, _json
from edax.errors import BadHandle, CodecError, StoreError
from edax.outbox import Relay
from edax.projection import (
    MemoryProjectionStateStore,
    ProjectionCutover,
    ProjectionEvent,
    ProjectionInput,
    ProjectionIntent,
    ProjectionResult,
    ProjectionStateStore,
    ProjectionTarget,
    SourceProof,
)


def target(generation: str = "balance-v1") -> ProjectionTarget:
    return ProjectionTarget(
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


def event(sequence: int = 1, event_id: str = "credit-1", *, amount: int = 1250) -> ProjectionEvent:
    return ProjectionEvent(
        "bank-eu",
        "account-42",
        event_id,
        sequence,
        "CreditPosted",
        "1",
        {"amount_minor": amount, "currency": "EUR"},
        {"correlation_id": "payment-99"},
    )


def reducer(state: Any, value: ProjectionEvent) -> ProjectionResult:
    balance = (state or {}).get("balance_minor", 0) + value.payload["amount_minor"]
    return ProjectionResult(
        {"balance_minor": balance, "currency": "EUR"},
        [ProjectionIntent("receipt", "account-notifications", {"account": value.aggregate})],
    )


def initial_cutover(t: ProjectionTarget) -> ProjectionCutover:
    return ProjectionCutover(t.source_identity, t.source_history, t.topic, t.starts)


@pytest.fixture(params=["memory", "postgres"])
async def stores(request: Any) -> Any:
    if request.param == "memory":
        yield lambda t: MemoryProjectionStateStore(t)
        return
    url = os.environ.get("EDAX_POSTGRES_URL")
    if url is None:
        if os.environ.get("EDAX_REQUIRE_POSTGRES") == "1":
            pytest.fail("EDAX_POSTGRES_URL required")
        pytest.skip("EDAX_POSTGRES_URL not set")
    import asyncpg
    from edax.backend.postgres import PostgresTransactor
    from edax.backend.projection_postgres import PostgresProjectionStateStore, reference_projection_state_ddl

    pool = await asyncpg.create_pool(url, min_size=1, max_size=4)
    schema = f"projection_{uuid4().hex}"
    await pool.execute(f'CREATE SCHEMA "{schema}"')
    try:
        await pool.execute(reference_projection_state_ddl(schema))
        tx = PostgresTransactor(pool, schema=schema)
        yield lambda t: PostgresProjectionStateStore(tx, t, schema=schema)
    finally:
        await pool.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


async def test_deltas_duplicate_new_offset_and_uncertain_commit(stores: Any) -> None:
    store: ProjectionStateStore = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker-a", proof())
    first = await store.commit(lease, [ProjectionInput(3, event())], proof(), reducer)
    recovered = await store.recover(lease)  # lost commit response
    assert recovered == first and first.next_offset == 4
    recovered = await store.commit(recovered, [ProjectionInput(8, event())], proof(), reducer)
    assert recovered.next_offset == 9
    state = await store.get("bank-eu", "account-42")
    assert state and state["value"]["balance_minor"] == 1250
    assert not await store.unpublished(100)  # rebuilding suppresses delivery
    await store.activate(initial_cutover(store.target))
    assert len(await store.unpublished(100)) == 1


async def test_poison_second_delta_rolls_back_entire_batch(stores: Any) -> None:
    store: ProjectionStateStore = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker-a", proof())
    with pytest.raises(Exception, match="sequence gap"):
        await store.commit(
            lease, [ProjectionInput(0, event()), ProjectionInput(1, event(3, "credit-3"))], proof(), reducer
        )
    assert (await store.recover(lease)).next_offset == 0
    assert await store.get("bank-eu", "account-42") is None
    await store.activate(initial_cutover(store.target))
    assert not await store.unpublished(100)


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
async def test_every_reducer_visible_identity_conflict_stops(stores: Any, changed: ProjectionEvent) -> None:
    store: ProjectionStateStore = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker-a", proof())
    lease = await store.commit(lease, [ProjectionInput(0, event())], proof(), reducer)
    with pytest.raises(Exception, match="conflicting projection event identity"):
        await store.commit(lease, [ProjectionInput(1, changed)], proof(), reducer)
    assert (await store.recover(lease)).next_offset == 1


async def test_noop_advances_sequence_and_null_state_is_real(stores: Any) -> None:
    store: ProjectionStateStore = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker", proof())
    lease = await store.commit(
        lease,
        [ProjectionInput(0, event()), ProjectionInput(1, event(2, "noop"))],
        proof(),
        lambda state, value: ProjectionResult(None),
    )
    state = await store.get("bank-eu", "account-42")
    assert state == {"partition": 0, "sequence": 2, "value": None}
    assert lease.next_offset == 2


async def test_rebuild_cutoff_and_restore_repeat_stable_effect_id(stores: Any) -> None:
    old: ProjectionStateStore = stores(target())
    new: ProjectionStateStore = stores(target("balance-v2"))
    for store in (old, new):
        await store.initialize()
    await old.activate(initial_cutover(old.target))
    old_lease = await old.acquire(0, "old", proof())
    await old.commit(old_lease, [ProjectionInput(0, event())], proof(), reducer)
    delivered: set[str] = set()
    calls: list[str] = []

    async def destination(intent: Any) -> None:
        calls.append(intent.id)
        delivered.add(intent.id)

    intent = (await old.unpublished(100))[0]
    await destination(intent)  # crash before publication marker
    assert await Relay(old, destination).drain() == 1
    assert calls == [intent.id, intent.id] and len(delivered) == 1
    cutover = await old.freeze()
    assert cutover.cutoffs == {0: 1, 1: 0}
    new_lease = await new.acquire(0, "new", proof())
    new_lease = await new.commit(
        new_lease, [ProjectionInput(0, event()), ProjectionInput(1, event(2, "credit-2"))], proof(), reducer
    )
    assert not await new.unpublished(100)
    await new.activate(cutover)
    pending = await new.unpublished(100)
    assert len(pending) == 1 and pending[0].offset == 1
    with pytest.raises(Exception, match="immutable"):
        await new.activate(initial_cutover(new.target))
    with pytest.raises(Exception, match="frozen"):
        await old.acquire(0, "obsolete", proof())
    assert new_lease.next_offset == 2


async def test_source_loss_and_obsolete_assignment_rejected_before_reducer(stores: Any) -> None:
    store: ProjectionStateStore = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker", proof())

    def forbidden(state: Any, value: ProjectionEvent) -> ProjectionResult:
        pytest.fail("reducer ran without source continuity")

    with pytest.raises(Exception, match="history is missing"):
        await store.commit(lease, [ProjectionInput(4, event())], proof(beginning=1), forbidden)
    with pytest.raises(BadHandle, match="obsolete"):
        await store.commit(lease, [ProjectionInput(0, event())], proof(), forbidden, is_current=lambda: False)
    assert (await store.recover(lease)).next_offset == 0


async def test_takeover_and_stale_release(stores: Any) -> None:
    store: ProjectionStateStore = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "a", proof(), lease_ms=1)
    await asyncio.sleep(0.005)
    newer = await store.acquire(0, "b", proof())
    assert newer.epoch > lease.epoch
    with pytest.raises(Exception, match="stale projection"):
        await store.commit(lease, [ProjectionInput(0, event())], proof(), reducer)
    await store.release(lease)
    assert await store.recover(newer) == newer


async def test_changed_source_cannot_activate_old_cutover(stores: Any) -> None:
    store: ProjectionStateStore = stores(target())
    await store.initialize()
    with pytest.raises(Exception, match="another source history"):
        await store.activate(replace(initial_cutover(store.target), source_identity="recreated-topic"))
    assert not await store.unpublished(100)


def test_projection_state_golden_across_python_binding() -> None:
    for case in golden("projection_state.json"):
        args = (_json.encode(case["request"]), _json.encode(case["records"]), case["now"])
        if case["error"] is not None:
            with pytest.raises(RuntimeError) as error:
                _edax.projection_transition(*args)
            assert str(error.value) == case["error"], case["name"]
        else:
            assert _json.decode(_edax.projection_transition(*args)) == case["want"], case["name"]


async def test_foreign_generation_lease_is_rejected(stores: Any) -> None:
    old, new = stores(target()), stores(target("balance-v2"))
    for store in (old, new):
        await store.initialize()
    old_lease = await old.acquire(0, "same-owner", proof())
    new_lease = await new.acquire(0, "same-owner", proof())
    with pytest.raises(Exception, match="stale projection"):
        await new.commit(old_lease, [ProjectionInput(0, event())], proof(), reducer)
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

        def inspect(state: Any, value: ProjectionEvent) -> ProjectionResult:
            return ProjectionResult(
                {
                    "first_key": next(iter(value.payload)),
                    "number_type": type(value.payload["number"]).__name__,
                    "zero": -0.0,
                }
            )

        await store.commit(lease, [ProjectionInput(0, replace(event(), payload=payload))], proof(), inspect)
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
    await store.commit(refreshed, [ProjectionInput(0, event())], proof(), reducer)
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
    await store.commit(lease, [ProjectionInput(0, event())], proof(), reducer)
    with pytest.raises(Exception, match="inactive projection"):
        await store.freeze()
    await store.activate(initial_cutover(store.target))
    assert len(await store.unpublished(100)) == 1


def account_event(aggregate: str, sequence: int) -> ProjectionEvent:
    return replace(event(sequence, f"{aggregate}-{sequence}"), aggregate=aggregate)


async def test_relay_delivers_intents_in_commit_order_and_isolates_a_refused_one(stores: Any) -> None:
    """One refused intent goes to the dead-letter sink; the other aggregates' intents still go out, in the
    source order across partitions."""
    store = stores(target())
    await store.initialize()
    await store.activate(initial_cutover(store.target))
    first, second = await store.acquire(0, "worker", proof()), await store.acquire(1, "worker", proof())
    first = await store.commit(first, [ProjectionInput(0, account_event("account-a", 1))], proof(), reducer)
    await store.commit(second, [ProjectionInput(0, account_event("account-b", 1))], proof(), reducer)
    await store.commit(first, [ProjectionInput(1, account_event("account-c", 1))], proof(), reducer)
    pending = await store.unpublished(100)
    # Source order: offset first, then partition, then the intent's place in its event.
    assert [(i.aggregate_id, i.partition, i.offset) for i in pending] == [
        ("account-a", 0, 0),
        ("account-b", 1, 0),
        ("account-c", 0, 1),
    ]
    delivered: list[str] = []
    dead: list[str] = []

    async def destination(intent: Any) -> None:
        if intent.aggregate_id == "account-a":
            raise CodecError("destination refuses this receipt")
        delivered.append(intent.aggregate_id)

    relay = Relay(store, destination, on_dead=lambda letter: dead.append(letter.message.aggregate_id))
    assert await relay.drain() == 3
    assert (delivered, dead) == (["account-b", "account-c"], ["account-a"])
    assert await store.unpublished(100) == []


async def test_relay_holds_back_only_the_failing_aggregate(stores: Any) -> None:
    store = stores(target())
    await store.initialize()
    await store.activate(initial_cutover(store.target))
    lease = await store.acquire(0, "worker", proof())
    inputs = [
        ProjectionInput(0, account_event("account-a", 1)),
        ProjectionInput(1, account_event("account-a", 2)),
    ]
    inputs.append(ProjectionInput(2, account_event("account-b", 1)))
    await store.commit(lease, inputs, proof(), reducer)
    delivered: list[tuple[str, int]] = []

    async def destination(intent: Any) -> None:
        if intent.offset == 0:
            raise OSError("destination briefly down")
        delivered.append((intent.aggregate_id, intent.offset))

    with pytest.raises(OSError):
        await Relay(store, destination, pacing=NO_WAIT).drain()
    assert delivered == [("account-b", 2)]
    assert [i.offset for i in await store.unpublished(100)] == [0, 1]


async def test_mark_published_ignores_an_unknown_intent(stores: Any) -> None:
    store = stores(target())
    await store.initialize()
    await store.mark_published(["no-such-intent"])
    assert await store.unpublished(100) == []


@pytest.mark.parametrize("where", ["envelope", "state", "intent"])
async def test_normalized_unsafe_integer_is_refused_before_persistence(stores: Any, where: str) -> None:
    store = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker", proof())
    value = replace(event(), payload={"amount": 1e18}) if where == "envelope" else event()

    def unsafe(state: Any, value: ProjectionEvent) -> ProjectionResult:
        return ProjectionResult(
            {"amount": 1e18} if where == "state" else {"amount": "1000000000000000000"},
            [ProjectionIntent("receipt", "notifications", {"amount": 1e18})] if where == "intent" else [],
        )

    with pytest.raises(Exception, match="integer"):
        await store.commit(lease, [ProjectionInput(0, value)], proof(), unsafe)
    assert await store.get("bank-eu", "account-42") is None
    assert (await store.recover(lease)).next_offset == 0


async def test_prune_removes_only_intents_a_cutover_made_ineligible(stores: Any) -> None:
    store = stores(target())
    await store.initialize()
    assert await store.prune(100) == 0  # building: nothing is ineligible for good yet
    lease = await store.acquire(0, "worker", proof())
    await store.commit(
        lease, [ProjectionInput(0, event()), ProjectionInput(1, event(2, "credit-2"))], proof(), reducer
    )
    await store.activate(replace(initial_cutover(store.target), cutoffs={0: 1, 1: 0}))
    assert await store.prune(1) == 1
    assert await store.prune(100) == 0
    assert [i.offset for i in await store.unpublished(100)] == [1]


async def test_mark_published_leaves_an_intent_its_generation_cannot_deliver(stores: Any) -> None:
    old, new = stores(target()), stores(target("balance-v2"))
    for store in (old, new):
        await store.initialize()
    await old.activate(initial_cutover(old.target))
    await old.commit(await old.acquire(0, "old", proof()), [ProjectionInput(0, event())], proof(), reducer)
    [intent] = await old.unpublished(100)
    await new.commit(await new.acquire(0, "new", proof()), [ProjectionInput(0, event())], proof(), reducer)
    await new.mark_published([intent.id])  # the new generation is still building
    await new.activate(initial_cutover(new.target))
    assert [i.id for i in await new.unpublished(100)] == [intent.id]


async def test_a_short_page_takes_each_partitions_oldest_first(stores: Any) -> None:
    store = stores(target())
    await store.initialize()
    await store.activate(initial_cutover(store.target))
    first, second = await store.acquire(0, "worker", proof()), await store.acquire(1, "worker", proof())
    # Partition 0 is far ahead in offsets; partition 1 must not wait behind it, nor it behind 1.
    await store.commit(first, [ProjectionInput(50, account_event("account-a", 1))], proof(), reducer)
    second = await store.commit(second, [ProjectionInput(0, account_event("account-b", 1))], proof(), reducer)
    await store.commit(second, [ProjectionInput(1, account_event("account-c", 1))], proof(), reducer)
    # Each partition's oldest first, by rank: offsets of two partitions say nothing about age.
    assert [i.aggregate_id for i in await store.unpublished(2)] == ["account-a", "account-b"]


async def test_changed_contract_is_refused(stores: Any) -> None:
    store = stores(target())
    await store.initialize()
    changed = stores(replace(target(), reducer_id="balance-reducer-2"))
    if isinstance(store, MemoryProjectionStateStore):
        changed._database = store._database  # memory stores share nothing unless given the same database
    with pytest.raises(BadHandle, match="changed projection contract"):
        await changed.initialize()
    with pytest.raises(BadHandle, match="changed projection contract"):
        await changed.acquire(0, "worker", proof())


async def test_activation_waits_for_the_rebuild_to_reach_its_cutoff(stores: Any) -> None:
    store = stores(target())
    await store.initialize()
    with pytest.raises(BadHandle, match="not reached cutoff"):
        await store.activate(replace(initial_cutover(store.target), cutoffs={0: 1, 1: 0}))
    assert not await store.unpublished(100)


async def test_commit_refuses_a_cursor_the_store_moved(stores: Any) -> None:
    store = stores(target())
    await store.initialize()
    lease = await store.acquire(0, "worker", proof())
    ahead = replace(lease, next_offset=5)  # as if a failover lost the commit that moved it to 5
    with pytest.raises(StoreError, match="cursor moved"):
        await store.commit(ahead, [ProjectionInput(5, event())], proof(), reducer)
    assert (await store.recover(lease)).next_offset == 0
    assert await store.get("bank-eu", "account-42") is None

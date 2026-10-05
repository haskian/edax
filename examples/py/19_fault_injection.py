"""Step 19: rehearse a lost reply with `edax.testing.faults`.

In a lost reply the write landed, and the reply never came back. The caller cannot tell it from a
write that failed, so it sends again. `BrokenCommit(db, "lost_response")` makes a
commit land and then raise, so a test can prove the handler survives it.

The outer guard's ledger lives in its own database (as Redis does). It sees the failed commit and
frees the claim, so the redelivery gets past it. A second `idempotent_by_id` guard absorbs that run:
its ledger lives in the read model's database, so its record committed with the deposit, and it skips
the deposit it already applied. No hand-rolled list of applied ids is needed, and none grows without
bound.

Run:  cd examples/py && uv run python 19_fault_injection.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace

from edax import Entity, EventMessage, EventRouter, StoreError, Tenancy, idempotent_by_id, json_codec, testing
from edax.backend.memory import MemoryIdempotencyStore, MemoryLedger, MemoryProjectionStore, MemoryTransactor
from edax.router import EventHandler
from edax.testing.faults import BrokenCommit


@dataclass
class Deposited:
    amount: int


events = json_codec({"Deposited": Deposited})


def deposited(event_id: str, amount: int) -> EventMessage:
    return testing.event("acct-1", "Deposited", id=event_id, data=events.marshal(Deposited(amount)).data)


async def main() -> None:
    # 1. The read model's database, behind a commit we can break on purpose.
    db = MemoryTransactor()
    balances = MemoryProjectionStore(db)
    unit = BrokenCommit(db, arm="lost_response")  # the next commit lands, then raises
    deliveries: list[str] = []
    applied: list[str] = []

    def count_deliveries(next_: EventHandler) -> EventHandler:
        async def handler(event: EventMessage) -> None:
            deliveries.append(event.id)
            ran = next_(event)
            if ran is not None:  # a handler may be sync or async
                await ran

        return handler

    # 2. Two guards, first outermost. The outer one opens the unit of work over `unit`, with a ledger of its
    #    own. The inner one joins that unit of work, so its record commits with the deposit.
    router = EventRouter(
        idempotent_by_id(MemoryLedger(), unit_of_work=unit, scope="balances", tenancy=Tenancy.SILO),
        count_deliveries,
        idempotent_by_id(MemoryIdempotencyStore(db), scope="balances-fold", tenancy=Tenancy.SILO),
    )

    @router.on("Deposited")
    async def add_deposit(event: EventMessage) -> None:
        applied.append(event.id)
        row = await balances.get(event.aggregate_id) or Entity(event.aggregate_id, 0, {"balance": 0})
        deposit: Deposited = events.unmarshal(event)
        balance = row.document["balance"] + deposit.amount
        await balances.save(replace(row, document={"balance": balance}))

    # 3. The first delivery: the deposit lands, but the caller sees an error.
    try:
        await router.dispatch(deposited("e1", 100))
        raise AssertionError("expected StoreError")
    except StoreError as e:
        print(f"delivery 1: {e}")

    # 4. The broker redelivers. The commit works now, and the outer claim is free again.
    unit.arm = None
    await router.dispatch(deposited("e1", 100))
    print("delivery 2: ok")

    row = await balances.get("acct-1")
    assert row is not None
    print(
        f"passed the outer guard {len(deliveries)}x; applied {len(applied)}x; balance {row.document['balance']}"
    )
    assert deliveries == ["e1", "e1"], "a ledger of its own lets the redelivery past the outer guard"
    assert applied == ["e1"], "the inner guard's record committed with the deposit, so it skips the rerun"
    assert row.document["balance"] == 100
    print("\nok: the lost reply caused a second run, and the inner guard absorbed it")


if __name__ == "__main__":
    asyncio.run(main())

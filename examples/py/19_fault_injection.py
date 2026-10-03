"""Step 19: rehearse a lost reply with `edax.testing.faults`.

In a lost reply the write landed, and the reply never came back. The caller cannot tell it from a
write that failed, so it sends again. `BrokenCommit(db, "lost_response")` makes a
commit land and then raise, so a test can prove the handler survives it.

Here the ledger lives in its own database (as Redis does). The guard sees the failed commit and
frees the claim, so the redelivery runs the handler a second time. The handler is safe only
because it remembers which deposits it already applied.

Run:  cd examples/py && uv run python 19_fault_injection.py
"""

from __future__ import annotations

import asyncio
import json

from edax import Entity, Event, EventEnvelope, EventRouter, StoreError, idempotent_by_id
from edax.backend.memory import MemoryLedger, MemoryProjectionStore, MemoryTransactor
from edax.testing.faults import BrokenCommit


def deposited(event_id: str, amount: int) -> EventEnvelope:
    return EventEnvelope(
        id=event_id,
        aggregate_id="acct-1",
        event=Event("Deposited", 1, json.dumps({"amount": amount}).encode()),
        namespace={},
        metadata={},
        timestamp=0,
    )


async def main() -> None:
    # 1. The read model's database, behind a commit we can break on purpose.
    db = MemoryTransactor()
    balances = MemoryProjectionStore(db)
    unit = BrokenCommit(db, arm="lost_response")  # the next commit lands, then raises
    router = EventRouter(idempotent_by_id(MemoryLedger(), unit_of_work=unit))
    runs: list[str] = []

    # 2. An idempotent handler: it skips a deposit it has already applied.
    @router.on("Deposited")
    async def add_deposit(envelope: EventEnvelope) -> None:
        runs.append(envelope.id)
        row = await balances.get(envelope.aggregate_id)
        if row is None:
            row = Entity(envelope.aggregate_id, 0, {"balance": 0, "applied": []})
        if envelope.id in row.document["applied"]:
            print(f"  {envelope.id} already applied: nothing to do")
            return
        row.document["balance"] += json.loads(envelope.event.data)["amount"]
        row.document["applied"].append(envelope.id)
        await balances.save(row)

    # 3. The first delivery: the deposit lands, but the caller sees an error.
    try:
        await router.dispatch(deposited("e1", 100))
        raise AssertionError("expected StoreError")
    except StoreError as e:
        print(f"delivery 1: {e}")

    # 4. The broker redelivers. The commit works now, and the claim is free again.
    unit.arm = None
    await router.dispatch(deposited("e1", 100))
    print("delivery 2: ok")

    row = await balances.get("acct-1")
    assert row is not None
    print(f"handler ran {len(runs)}x; balance {row.document['balance']}")
    assert runs == ["e1", "e1"], "a ledger of its own lets the redelivery run the handler again"
    assert row.document["balance"] == 100, "and the idempotent handler applied the deposit once"
    print("\nok: the lost reply caused a second run, and the handler absorbed it")


if __name__ == "__main__":
    asyncio.run(main())

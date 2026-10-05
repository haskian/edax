"""Step 10: a redelivered event must not update the read model twice.

A broker delivers at least once, so a handler sees some events twice. `idempotent_by_id` claims each
event id before the handler runs, and skips an id it has seen. Its `scope` names this consumer, so
another consumer guarding over the same ledger keeps its own record of the same events.

The ledger and the read model share one database here. With `unit_of_work=db` the guard opens the
transaction: the claim and the read-model write commit together, or roll back together.

The handler reads the payload with the codec the writer used, `codec.unmarshal(event)`. That applies any
upcast, and refuses a payload from a newer writer, rather than reading raw JSON.

Run:  cd examples/py && uv run python 10_idempotent_events.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace

from edax import Entity, EventMessage, EventRouter, Tenancy, idempotent_by_id, json_codec, testing
from edax.backend.memory import MemoryIdempotencyStore, MemoryProjectionStore, MemoryTransactor


@dataclass
class Deposited:
    amount: int


# The writer's codec. The consumer decodes with the same one.
events = json_codec({"Deposited": Deposited})


def deposited(event_id: str, amount: int) -> EventMessage:
    # `id` is the key the guard claims: stable across redeliveries.
    return testing.event("acct-1", "Deposited", id=event_id, data=events.marshal(Deposited(amount)).data)


async def main() -> None:
    # 1. One database for the ledger and the read model.
    db = MemoryTransactor()
    balances = MemoryProjectionStore(db)
    router = EventRouter(
        idempotent_by_id(MemoryIdempotencyStore(db), unit_of_work=db, scope="balances", tenancy=Tenancy.SILO)
    )
    runs: list[str] = []
    audit_log_down = True  # the first run of e2 fails after its write

    # 2. The handler adds each deposit to the account's balance document.
    @router.on("Deposited")
    async def add_deposit(event: EventMessage) -> None:
        nonlocal audit_log_down
        runs.append(event.id)
        row = await balances.get(event.aggregate_id) or Entity(event.aggregate_id, 0, {"balance": 0})
        deposit: Deposited = events.unmarshal(event)
        balance = row.document["balance"] + deposit.amount
        await balances.save(replace(row, document={"balance": balance}))  # joins the guard's transaction
        if event.id == "e2" and audit_log_down:
            audit_log_down = False
            raise ConnectionError("audit log unreachable")  # rolls back the save and the claim

    async def balance() -> int:
        row = await balances.get("acct-1")
        return 0 if row is None else row.document["balance"]

    # 3. e1 arrives, then the broker delivers it again (an ack was lost).
    await router.dispatch(deposited("e1", 100))
    await router.dispatch(deposited("e1", 100))
    print(f"e1 twice:          handler ran {runs.count('e1')}x, balance {await balance()}")

    # 4. e2 fails after its write: the write and the claim roll back together.
    try:
        await router.dispatch(deposited("e2", 50))
    except ConnectionError:
        print(f"e2 failed:         balance {await balance()} (the write rolled back)")

    # 5. The redelivered e2 runs again, because its claim was released.
    await router.dispatch(deposited("e2", 50))
    print(f"e2 redelivered:    handler ran {runs.count('e2')}x, balance {await balance()}")

    assert runs == ["e1", "e2", "e2"], runs
    assert await balance() == 150
    print("\nok: each deposit counted exactly once in the read model")


if __name__ == "__main__":
    asyncio.run(main())

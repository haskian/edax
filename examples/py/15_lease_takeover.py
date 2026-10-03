"""Step 15: rehearse a Redis lease takeover in memory, with `MemoryLedger` and `expire`.

A Redis ledger lives in its own database, and its claims expire. If a worker runs longer than the
claim's time to live, a second worker can take the key over. `MemoryLedger` behaves the same way
without Redis, and `expire(key)` makes a claim run out on demand.

The ledger fences each claim: the first worker's late result is refused, so the second worker's
outcome stands. The fence guards the ledger only. Both workers ran the handler, so the charge
itself needs its own duplicate check downstream.

Run:  cd examples/py && uv run python 15_lease_takeover.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, ClassVar

from edax import CommandBus, InProgress, StoreError, idempotent
from edax.backend.memory import MemoryLedger


@dataclass
class ChargeCard:
    payment_id: str
    amount: int
    type: ClassVar[str] = "ChargeCard"


def payment_id_of(cmd: Any) -> str:
    return cmd.payment_id


async def main() -> None:
    ledger = MemoryLedger()  # its own database, like Redis: joins no transaction
    bus = CommandBus(idempotent(ledger, key=payment_id_of))
    runs: list[int] = []
    worker_a_may_finish = asyncio.Event()

    async def charge(cmd: ChargeCard) -> str:
        run = len(runs) + 1
        runs.append(run)
        if run == 1:
            await worker_a_may_finish.wait()  # worker A is slow
        return f"charged by run {run}"

    bus.register("ChargeCard", charge)
    command = ChargeCard("pay-1", 40)

    # 1. Worker A claims pay-1 and starts charging.
    worker_a = asyncio.create_task(bus.dispatch(command))
    while not runs:
        await asyncio.sleep(0)
    print("worker A: claimed pay-1, still charging")

    # 2. Worker B gets the same message while A holds the claim.
    try:
        await bus.dispatch(command)
        raise AssertionError("expected InProgress")
    except InProgress:
        print("worker B: InProgress, A holds the claim")

    # 3. A's claim runs out, as a Redis claim TTL would. B takes over and finishes.
    ledger.expire("pay-1")
    result_b = await bus.dispatch(command)
    print(f"worker B: took over -> {result_b}")

    # 4. A finally finishes. The fence refuses its late result.
    worker_a_may_finish.set()
    try:
        await worker_a
        raise AssertionError("expected StoreError")
    except StoreError:
        print("worker A: its late completion was refused (StoreError)")

    # 5. A later redelivery replays B's outcome.
    replay = await bus.dispatch(command)
    print(f"redelivery: {replay}")

    assert (result_b, replay, runs) == ("charged by run 2", "charged by run 2", [1, 2])
    print("\nok: B's outcome stood; note the handler ran twice, so the charge needs its own check")


if __name__ == "__main__":
    asyncio.run(main())

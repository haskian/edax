"""Step 6: transient errors are retried, terminal errors are not.

A transient error may pass on the next try: a timeout, a dropped connection. A terminal error fails
the same way every time: a business rule says no. Raise `Rejected`, or a subclass, for those.

`retry` re-runs the whole unit of work. The failed attempt rolls back, so nothing it wrote survives.
`Pacing` sets the wait between attempts.

Run:  cd examples/py && uv run python 06_errors_and_retry.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import ClassVar

from edax import Backoff, CommandBus, Entity, Pacing, Rejected, Tenancy, idempotent, is_terminal, retry
from edax.backend.memory import MemoryIdempotencyStore, MemoryProjectionStore, MemoryTransactor


class InsufficientFunds(Rejected):
    """Terminal: running the handler again cannot change the answer."""


@dataclass
class Withdraw:
    account_id: str
    amount: int
    type: ClassVar[str] = "Withdraw"


async def main() -> None:
    # 1. An accounts table and the ledger, in one database. acct-1 opens with 100.
    db = MemoryTransactor()
    accounts = MemoryProjectionStore(db)
    await accounts.save(Entity("acct-1", 0, {"balance": 100}))
    attempts: dict[int, int] = {}  # handler runs, by amount
    receipt_printer_down = True  # the first receipt fails, after the balance was written

    async def withdraw(cmd: Withdraw) -> int:
        nonlocal receipt_printer_down
        attempts[cmd.amount] = attempts.get(cmd.amount, 0) + 1
        account = await accounts.get(cmd.account_id)
        assert account is not None
        if account.document["balance"] < cmd.amount:
            raise InsufficientFunds(f"balance {account.document['balance']} < {cmd.amount}")
        account.document["balance"] -= cmd.amount
        await accounts.save(account)  # joins the guard's transaction
        if receipt_printer_down:
            receipt_printer_down = False
            raise ConnectionError("receipt printer unreachable")  # transient
        return account.document["balance"]

    # 2. Short waits so the example runs fast. The default backoff starts at 100 ms.
    fast = Pacing(backoff=Backoff(base_millis=1, cap_millis=10))
    ledger = MemoryIdempotencyStore(db)
    bus = CommandBus(
        retry(3, pacing=fast), idempotent(ledger, db, scope="accounts-api", tenancy=Tenancy.SILO)
    )
    bus.register("Withdraw", withdraw)

    # 3. Transient: attempt 1 writes, fails, and rolls back. Attempt 2 starts from 100 again.
    balance = await bus.dispatch(Withdraw("acct-1", 30), idempotency_key="req-1")
    print(f"withdraw 30:  balance {balance} after {attempts[30]} attempts (not 40)")

    # 4. Terminal: InsufficientFunds comes out at once, with no second attempt.
    try:
        await bus.dispatch(Withdraw("acct-1", 500), idempotency_key="req-2")
        raise AssertionError("expected InsufficientFunds")
    except InsufficientFunds as e:
        print(f"withdraw 500: InsufficientFunds ({e}) after {attempts[500]} attempt")

    # 5. The kernel decides which is which. A plain exception counts as transient.
    print(f"is_terminal: InsufficientFunds={is_terminal(InsufficientFunds())}", end=", ")
    print(f"ConnectionError={is_terminal(ConnectionError())}")

    assert (balance, attempts) == (70, {30: 2, 500: 1}), (balance, attempts)
    print("\nok: the transient failure was retried from a clean state; the terminal one was not retried")


if __name__ == "__main__":
    asyncio.run(main())

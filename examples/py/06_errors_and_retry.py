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

from edax import (
    Aggregate,
    Backoff,
    CommandBus,
    Pacing,
    Rejected,
    Repository,
    Tenancy,
    idempotent,
    is_terminal,
    json_codec,
    retry,
)
from edax.backend.memory import MemoryEventStore, MemoryIdempotencyStore, MemoryTransactor


class InsufficientFunds(Rejected):
    """Terminal: running the handler again cannot change the answer."""


@dataclass
class Deposited:
    amount: int


@dataclass
class Withdrawn:
    amount: int


class Account(Aggregate):
    def __init__(self, id: str, initial: int = 0) -> None:
        super().__init__(id, initial)
        self.balance = 0

    def withdraw(self, amount: int) -> None:
        # The rule runs on state the account's own events built, never on a read model.
        if self.balance < amount:
            raise InsufficientFunds(f"balance {self.balance} < {amount}")
        self.apply(Withdrawn(amount))

    def transition(self, event: object) -> None:
        if isinstance(event, Deposited):
            self.balance += event.amount
        elif isinstance(event, Withdrawn):
            self.balance -= event.amount


class AccountCodec:
    type = "Account"

    def load(self, id: str, version: int, data: bytes | None) -> Account:
        account = Account(id, version)
        if data is not None:
            account.balance = int(data.decode())
        return account

    def dump(self, account: Account) -> bytes:
        return str(account.balance).encode()


@dataclass
class Withdraw:
    account_id: str
    amount: int
    type: ClassVar[str] = "Withdraw"


async def main() -> None:
    # 1. The event store and the ledger, in one database. acct-1 opens with 100.
    db = MemoryTransactor()
    events = json_codec({"Deposited": Deposited, "Withdrawn": Withdrawn})
    accounts = Repository(MemoryEventStore(db), events, AccountCodec(), snapshot_every=0)
    opened = await accounts.load("acct-1")
    opened.apply(Deposited(100))
    await accounts.save(opened)
    attempts: dict[int, int] = {}  # handler runs, by amount
    receipt_printer_down = True  # the first receipt fails, after the withdrawal was saved

    async def withdraw(cmd: Withdraw) -> int:
        nonlocal receipt_printer_down
        attempts[cmd.amount] = attempts.get(cmd.amount, 0) + 1
        account = await accounts.load(cmd.account_id)
        account.withdraw(cmd.amount)
        await accounts.save(account)  # joins the guard's transaction
        if receipt_printer_down:
            receipt_printer_down = False
            raise ConnectionError("receipt printer unreachable")  # transient
        return account.balance

    # 2. Short waits so the example runs fast. The default backoff starts at 100 ms.
    fast = Pacing(backoff=Backoff(base_millis=1, cap_millis=10))
    ledger = MemoryIdempotencyStore(db)
    bus = CommandBus(
        retry(3, pacing=fast), idempotent(ledger, db, scope="accounts-api", tenancy=Tenancy.SILO)
    )
    bus.register(Withdraw, withdraw)

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

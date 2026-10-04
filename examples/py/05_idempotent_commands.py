"""Step 5: apply a command once, even when the client sends it twice.

The client stamps each request with a request id, and `dispatch` passes it as `idempotency_key`.
The `idempotent` guard claims that key under its `scope`. A repeat replays the first outcome without
running the handler. The same key with a different body is refused with `PayloadMismatch`.

`unit_of_work=db` makes the guard open the transaction. The claim and the events then commit
together. Both stores are built over one MemoryTransactor, so they share one database.

Run:  cd examples/py && uv run python 05_idempotent_commands.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import ClassVar

from edax import Aggregate, CommandBus, PayloadMismatch, Repository, Tenancy, idempotent, json_codec, retry
from edax.backend.memory import MemoryEventStore, MemoryIdempotencyStore, MemoryTransactor


@dataclass
class Deposited:
    amount: int


class Account(Aggregate):
    def __init__(self, id: str, initial: int = 0) -> None:
        super().__init__(id, initial)
        self.balance = 0

    def transition(self, event: object) -> None:
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


@dataclass
class Deposit:
    account_id: str
    amount: int
    type: ClassVar[str] = "Deposit"


async def main() -> None:
    # 1. One database: the event store and the idempotency store commit together.
    db = MemoryTransactor()
    store = MemoryEventStore(db)
    repo = Repository(store, json_codec({"Deposited": Deposited}), AccountCodec())

    async def deposit(cmd: Deposit) -> int:
        account = await repo.load(cmd.account_id)
        account.apply(Deposited(cmd.amount))
        await repo.save(account)  # joins the guard's transaction
        return account.balance

    # 2. The recommended wiring: retry outermost, the guard owns the unit of work.
    #    Step 6 shows what retry adds. Open no transaction around a dispatch.
    #    The scope names this guard, so another service sharing the ledger never replays it.
    bus = CommandBus(
        retry(3), idempotent(MemoryIdempotencyStore(db), db, scope="accounts-api", tenancy=Tenancy.SILO)
    )
    bus.register("Deposit", deposit)

    # 3. The first request deposits.
    first = await bus.dispatch(Deposit("acct-1", 100), idempotency_key="req-A")
    print(f"req-A        -> balance {first}")

    # 4. The client timed out and sent req-A again: replayed, no second deposit.
    again = await bus.dispatch(Deposit("acct-1", 100), idempotency_key="req-A")
    print(f"req-A again  -> balance {again} (replayed)")

    # 5. The same key with another amount is a client bug, not a repeat.
    try:
        await bus.dispatch(Deposit("acct-1", 999), idempotency_key="req-A")
        raise AssertionError("expected PayloadMismatch")
    except PayloadMismatch:
        print("req-A, 999   -> PayloadMismatch")

    account = await repo.load("acct-1")
    print(f"account: balance {account.balance} after {account.version.current} event(s)")
    assert (first, again, account.balance, account.version.current) == (100, 100, 100, 1)
    print("\nok: three dispatches of req-A, one deposit")


if __name__ == "__main__":
    asyncio.run(main())

"""Step 2: keep an account as a stream of events, and save and load it with a Repository.

An aggregate never stores its state. It stores the events that changed it. `transition` folds one
event into state. Loading replays every event in order.

Run:  cd examples/py && uv run python 02_aggregate.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from edax import Aggregate, Repository, json_codec
from edax.backend.memory import MemoryEventStore


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

    def transition(self, event: object) -> None:
        # Apply one event. No checks here: the event has already happened.
        if isinstance(event, Deposited):
            self.balance += event.amount
        elif isinstance(event, Withdrawn):
            self.balance -= event.amount


class AccountCodec:
    """Builds an empty Account, and reads and writes its snapshot (step 3 covers snapshots)."""

    type = "Account"

    def load(self, id: str, version: int, data: bytes | None) -> Account:
        account = Account(id, version)
        if data is not None:
            account.balance = int(data.decode())
        return account

    def dump(self, account: Account) -> bytes:
        return str(account.balance).encode()


async def main() -> None:
    # 1. Wire a repository: an event store, a codec per event type, and the aggregate codec.
    store = MemoryEventStore()
    events = json_codec({"Deposited": Deposited, "Withdrawn": Withdrawn})
    repo = Repository(store, events, AccountCodec(), snapshot_every=0)  # 0: no snapshots yet

    # 2. Create a new account: empty at version 0. Nothing is read or written yet.
    account = repo.create("acct-1")
    print(f"new account: balance {account.balance}, version {account.version.current}")

    # 3. Apply events. They change the state now and wait in `pending` until the save.
    account.apply(Deposited(100), Withdrawn(30))
    print(f"applied:     balance {account.balance}, pending {account.pending}")

    # 4. Save appends the pending events to the stream.
    saved = await repo.save(account)
    for event in saved:
        print(f"  stored #{event.sequence} {event.name} {event.data.decode()}")

    # 5. Load again: the repository replays the stream into a fresh Account.
    loaded = await repo.load("acct-1")
    print(f"loaded:      balance {loaded.balance}, version {loaded.version.current}")

    assert [e.sequence for e in saved] == [1, 2]
    assert (loaded.balance, loaded.version.current) == (70, 2)
    print("\nok: the account was saved as two events and rebuilt by replaying them")


if __name__ == "__main__":
    asyncio.run(main())

"""Step 12: link events into a chain with correlation and causation ids.

Two metadata keys follow a flow across services:

- `correlation_id` is the same for every event of one flow;
- `causation_id` is the id of the event that directly caused this one.

`ensure_ids()` starts a chain for a root event. `propagate(parent.metadata, parent.id)` continues it
for an event the parent caused. Pass the result to `Repository.save(metadata=...)`.

Run:  cd examples/py && uv run python 12_causation.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from edax import Aggregate, EventEnvelope, Repository, ensure_ids, json_codec, propagate
from edax.backend.memory import MemoryEventStore
from edax.causation import CAUSATION_ID, CORRELATION_ID


@dataclass
class AccountOpened:
    owner: str


@dataclass
class BonusPaid:
    amount: int


class Account(Aggregate):
    def __init__(self, id: str, initial: int = 0) -> None:
        super().__init__(id, initial)
        self.balance = 0

    def transition(self, event: object) -> None:
        if isinstance(event, BonusPaid):
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


def show(label: str, envelope: EventEnvelope) -> None:
    chain = envelope.metadata
    print(f"{label:<14} id={envelope.id}")
    print(f"{'':<14} correlation={chain[CORRELATION_ID]} causation={chain[CAUSATION_ID]}")


async def main() -> None:
    events = json_codec({"AccountOpened": AccountOpened, "BonusPaid": BonusPaid})
    repo = Repository(MemoryEventStore(), events, AccountCodec())

    # 1. A request opens an account. It starts the flow, so it gets fresh chain ids.
    account = await repo.load("acct-1")
    account.apply(AccountOpened("Ann"))
    saved = await repo.save(account, metadata=ensure_ids())
    opened = saved[0]
    show("AccountOpened", opened)

    # 2. A welcome-bonus handler reacts to that event. Its event continues the chain.
    account = await repo.load("acct-1")
    account.apply(BonusPaid(10))
    saved = await repo.save(account, metadata=propagate(opened.metadata, opened.id))
    bonus = saved[0]
    show("BonusPaid", bonus)

    # 3. One flow id for both; the bonus names the event that caused it.
    assert bonus.metadata[CORRELATION_ID] == opened.metadata[CORRELATION_ID]
    assert bonus.metadata[CAUSATION_ID] == opened.id
    assert opened.metadata[CAUSATION_ID] == opened.metadata[CORRELATION_ID], "a root causes itself"
    print("\nok: both events share one correlation id; the bonus points at its cause")


if __name__ == "__main__":
    asyncio.run(main())

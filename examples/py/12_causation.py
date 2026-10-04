"""Step 12: link events into a chain with correlation and causation ids.

Every message carries two ids that follow a flow across services:

- `correlation_id` is the same for every message of one flow;
- `causation_id` is the id of the message that directly caused this one.

A save no message caused starts a new flow. `originate()` builds that lineage from the ids an inbound
request already carries. Inside a handler, `EventRouter`, the dispatcher and the saga runner set the
ambient lineage to `propagate(msg)`, so a save there continues the flow with no argument. `caused_by(msg)`
does the same by hand.

Run:  cd examples/py && uv run python 12_causation.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from edax import Aggregate, EventMessage, EventRouter, Repository, json_codec, originate
from edax.backend.memory import MemoryEventStore


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


def show(label: str, event: EventMessage) -> None:
    print(f"{label:<14} id={event.id}")
    print(f"{'':<14} correlation={event.correlation_id} causation={event.causation_id}")


async def main() -> None:
    events = json_codec({"AccountOpened": AccountOpened, "BonusPaid": BonusPaid})
    repo = Repository(MemoryEventStore(), events, AccountCodec())

    # 1. A request opens an account. It carries a request id, which becomes the flow's correlation id.
    account = await repo.load("acct-1")
    account.apply(AccountOpened("Ann"))
    saved = await repo.save(account, lineage=originate(correlation_id="req-42"))
    opened = saved[0]
    show("AccountOpened", opened)

    # 2. A welcome-bonus handler reacts to that event. The router runs it under the event's lineage,
    #    so its save continues the flow with no argument.
    bonuses: list[EventMessage] = []
    router = EventRouter()

    @router.on("AccountOpened")
    async def pay_bonus(event: EventMessage) -> None:
        account = await repo.load(event.aggregate_id)
        account.apply(BonusPaid(10))
        bonuses.extend(await repo.save(account))

    await router.dispatch(opened)
    [bonus] = bonuses
    show("BonusPaid", bonus)

    # 3. One flow id for both; the bonus names the event that caused it.
    assert bonus.correlation_id == opened.correlation_id == "req-42"
    assert bonus.causation_id == opened.id
    assert opened.causation_id == opened.correlation_id, "a root causes itself"
    print("\nok: both events share one correlation id; the bonus points at its cause")


if __name__ == "__main__":
    asyncio.run(main())

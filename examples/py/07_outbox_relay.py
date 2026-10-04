"""Step 7: publish saved events with the transactional outbox and a Relay.

`Repository.save` writes each event and an outbox row in one commit. So an event is never saved
without its row, and never published without being saved. A `Relay` reads the outbox and publishes
each row. A row stays until a publish succeeds, so delivery is at-least-once.

Run:  cd examples/py && uv run python 07_outbox_relay.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from edax import Aggregate, Backoff, EventMessage, Pacing, Relay, Repository, json_codec
from edax.backend.memory import MemoryEventStore


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


def broker_down(event: EventMessage) -> None:
    raise ConnectionError("broker unreachable")


async def main() -> None:
    store = MemoryEventStore()
    repo = Repository(store, json_codec({"Deposited": Deposited}), AccountCodec())

    # 1. Save two events. The same commit writes two outbox rows.
    account = await repo.load("acct-1")
    account.apply(Deposited(100), Deposited(50))
    await repo.save(account)
    print(f"saved 2 events; outbox holds {await store.unpublished_count()} rows")

    # 2. The broker is down: the drain raises and every row stays.
    fast = Pacing(backoff=Backoff(base_millis=1, cap_millis=10))  # short wait before the raise
    try:
        await Relay(store, broker_down, pacing=fast).drain()
        raise AssertionError("expected ConnectionError")
    except ConnectionError as e:
        print(f"drain failed ({e}); outbox holds {await store.unpublished_count()} rows")

    # 3. The broker is back: the drain publishes both rows, oldest first.
    published: list[str] = []

    def publish(event: EventMessage) -> None:
        published.append(f"{event.name} #{event.sequence}")

    relay = Relay(store, publish)
    sent = await relay.drain()
    print(f"drain published {sent}: {published}")

    # 4. Nothing is left to send. A service runs `relay.run()` as a task instead.
    print(f"next drain published {await relay.drain()}")

    assert published == ["Deposited #1", "Deposited #2"], published
    assert await store.unpublished_count() == 0
    print("\nok: the rows waited out the outage, then went out once, in order")


if __name__ == "__main__":
    asyncio.run(main())

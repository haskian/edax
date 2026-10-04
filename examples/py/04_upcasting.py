"""Step 4: read events an older version of the service stored, with an Upcaster.

Version 1 of `Deposited` had no currency. Version 2 adds one. The stored bytes never change. An
upcaster rewrites an old payload on load, one version step at a time.

Run:  cd examples/py && uv run python 04_upcasting.py
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

from edax import Aggregate, EventDraft, Repository, Upcaster, json_codec, transaction
from edax.backend.memory import MemoryEventStore


@dataclass
class Deposited:  # version 2: today's shape
    amount: int
    currency: str


class Account(Aggregate):
    def __init__(self, id: str, initial: int = 0) -> None:
        super().__init__(id, initial)
        self.balances: dict[str, int] = {}

    def transition(self, event: object) -> None:
        if isinstance(event, Deposited):
            self.balances[event.currency] = self.balances.get(event.currency, 0) + event.amount


class AccountCodec:
    type = "Account"

    def load(self, id: str, version: int, data: bytes | None) -> Account:
        return Account(id, version)  # no snapshots in this example, so `data` is always None

    def dump(self, account: Account) -> bytes:
        return json.dumps(account.balances).encode()


def add_currency(data: bytes) -> bytes:
    """Upgrade a version 1 payload to version 2. Every version 1 deposit was in euros."""
    payload = json.loads(data)
    payload["currency"] = "EUR"
    return json.dumps(payload).encode()


async def main() -> None:
    store = MemoryEventStore()

    # 1. The old service stored a version 1 event: no currency in the payload.
    draft = EventDraft(
        id="e1",
        aggregate_id="acct-1",
        sequence=1,
        name="Deposited",
        version=1,
        data=b'{"amount": 100}',
        correlation_id="e1",
        causation_id="e1",
        extensions={},
        metadata={},
    )
    async with transaction(store) as tx:
        [old] = await store.append(tx, "acct-1", 0, [draft])
    print(f"stored by v1: {old.name} v{old.version} {old.data.decode()}")

    # 2. Today's codec writes version 2 and knows how to raise version 1 to it.
    upcasters = {"Deposited": Upcaster().step(1, add_currency)}
    events = json_codec({"Deposited": Deposited}, version=2, upcasters=upcasters)
    repo = Repository(store, events, AccountCodec(), snapshot_every=0)  # 0: every load replays

    # 3. Load: the old event is upcast on the way in.
    account = await repo.load("acct-1")
    print(f"loaded:       balances {account.balances}")

    # 4. A new event is written as version 2.
    account.apply(Deposited(50, "USD"))
    saved = await repo.save(account)
    new = saved[0]
    print(f"stored by v2: {new.name} v{new.version} {new.data.decode()}")

    assert account.balances == {"EUR": 100, "USD": 50}, account.balances
    assert (old.version, new.version) == (1, 2)
    print("\nok: the version 1 event was read as version 2; the stored bytes never changed")


if __name__ == "__main__":
    asyncio.run(main())

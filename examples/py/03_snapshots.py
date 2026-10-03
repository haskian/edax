"""Step 3: snapshots, so a load does not replay the whole stream.

`snapshot_every=3` writes a snapshot each time the version crosses a multiple of 3. A load starts
from the latest snapshot and replays only the events after it. A snapshot is a cache: deleting it
only makes the next load slower.

Run:  cd examples/py && uv run python 03_snapshots.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from edax import Aggregate, Repository, json_codec
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
    """`dump` writes the snapshot. `load` reads it back, or builds an empty Account for `None`."""

    type = "Account"

    def load(self, id: str, version: int, data: bytes | None) -> Account:
        account = Account(id, version)
        if data is not None:
            account.balance = int(data.decode())
        return account

    def dump(self, account: Account) -> bytes:
        return str(account.balance).encode()


async def main() -> None:
    # 1. Snapshot every 3 versions.
    store = MemoryEventStore()
    repo = Repository(store, json_codec({"Deposited": Deposited}), AccountCodec(), snapshot_every=3)

    # 2. Seven deposits, one save each. Watch the snapshot version move.
    for _ in range(7):
        account = await repo.load("acct-1")
        account.apply(Deposited(10))
        await repo.save(account)
        snapshot = await store.load_snapshot(None, "acct-1")  # None: read what has committed
        latest = "none yet" if snapshot is None else f"version {snapshot.version}"
        print(f"saved version {account.version.current}: latest snapshot {latest}")

    # 3. Load: the snapshot at version 6, then the one event after it.
    snapshot = await store.load_snapshot(None, "acct-1")
    loaded = await repo.load("acct-1")
    assert snapshot is not None
    replayed = loaded.version.current - snapshot.version
    print(f"load: snapshot at version {snapshot.version} + {replayed} event replayed")
    print(f"      -> balance {loaded.balance} at version {loaded.version.current}")

    assert (snapshot.version, replayed, loaded.balance) == (6, 1, 70)
    print("\nok: the load started from the snapshot and replayed only the tail")


if __name__ == "__main__":
    asyncio.run(main())

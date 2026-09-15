"""Command handling (CQRS write side) with a transactional outbox.

    submit ─▶ CommandBus(idempotent, retry) ─▶ handler ─▶ Repository.save ─┬─ events + snapshot
                                                                          └─ outbox rows ─▶ Relay.drain ─▶ publish

request_id is the idempotency key: stable across redeliveries, so the bus replays instead of applying
twice; reuse with a different payload is rejected. transaction wraps the dispatch so the idempotency
record, events, snapshot and outbox rows commit (or roll back) together: both stores are built over
one MemoryTransactor, the unit of work every participant enlists in.

Run:  maturin develop -m crates/edax-py/Cargo.toml  &&  python examples/py/command_handling.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from itertools import count
from typing import Any, ClassVar

from edax import Aggregate, Relay, Repository, json_codec, transaction
from edax.backend.memory import MemoryEventStore, MemoryIdempotencyStore, MemoryTransactor
from edax.command import CommandBus, PayloadMismatch, idempotent, retry


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
    """Snapshot codec. It fixes the repository's aggregate type: `Repository(..., AccountCodec())`
    is a `Repository[Account]`, so `load` hands back an `Account`."""

    type = "Account"

    def load(self, id: str, version: int, data: bytes | None) -> Account:
        a = Account(id, version)
        if data is not None:
            a.balance = int(data.decode())
        return a

    def dump(self, agg: Account) -> bytes:
        return str(agg.balance).encode()


@dataclass
class Deposit:
    account_id: str
    amount: int
    request_id: str  # idempotency key; part of the hashed identity
    type: ClassVar[str] = "Deposit"  # routing only: a class attribute stays out of the hash


def request_id_of(cmd: Any) -> str:
    """The idempotency key for a dispatched command. `Any` because one bus routes several command
    types and `idempotent` types its key function over the `Command` protocol, which has only `type`."""
    return cmd.request_id


async def main() -> None:
    db = MemoryTransactor()  # one database: the event store and the idempotency store commit together
    store = MemoryEventStore(db)
    ids = count(1)
    repo = Repository(
        store,
        json_codec({"Deposited": Deposited}),
        AccountCodec(),
        new_id=lambda: f"e{next(ids)}",  # deterministic for the asserts
        now=lambda: 0,
    )

    async def deposit(cmd: Deposit) -> int:
        acct = await repo.load(cmd.account_id)  # snapshot + replay
        acct.apply(Deposited(cmd.amount))
        await repo.save(acct)  # joins the transaction opened by submit via current_tx()
        return acct.balance

    # idempotency outermost (one claim per request), retry inside it (retries share the claim).
    bus = CommandBus(idempotent(MemoryIdempotencyStore(db), key=request_id_of), retry(3))
    bus.register("Deposit", deposit)

    async def submit(request_id: str, account_id: str, amount: int) -> int:
        async with transaction(db):  # claim, events, snapshot, outbox: one commit, or one rollback
            return await bus.dispatch(Deposit(account_id, amount, request_id))

    b1 = await submit("req-A", "acct-1", 100)
    b2 = await submit("req-A", "acct-1", 100)  # same key + body -> replay
    try:
        await submit("req-A", "acct-1", 999)  # same key, different body -> rejected
        raise AssertionError("expected PayloadMismatch")
    except PayloadMismatch:
        pass
    b3 = await submit("req-B", "acct-1", 50)
    print(f"req-A -> {b1}, req-A retry -> {b2}, req-B -> {b3}")
    assert (b1, b2, b3) == (100, 100, 150), (b1, b2, b3)

    # Outbox holds exactly the two committed events; the replay and the clash wrote nothing.
    published: list[str] = []
    relay = Relay(store, publish=lambda env: published.append(env.id))
    n = await relay.drain()
    print(f"drained {published}")
    assert published == ["e1", "e2"], published
    assert n == 2 and await relay.drain() == 0

    print("\nok: idempotent command applied once, outbox drained")


if __name__ == "__main__":
    asyncio.run(main())

"""Step 16: keep the idempotency ledger in Redis, shared by every process of a service.

The guard is the same as in step 5; only the store changes. Redis cannot join your database's
transaction, so this is a ledger of its own: at-least-once, with a dedup window of `ttl_ms`. A ledger
of its own also caches a terminal failure, so a repeat raises `CachedFailure` without running the
handler.

Set `EDAX_REDIS_URL` (for example `redis://localhost:6379/0`) to use a real Redis. Unset, the example
runs on `MemoryLedger`, which behaves the same way in memory.

Run:  cd examples/py && uv run python 16_redis_ledger.py
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import ClassVar
from urllib.parse import urlsplit

from edax import CachedFailure, CommandBus, IdempotencyStore, Rejected, Tenancy, guid, idempotent
from edax.backend.memory import MemoryLedger


class CardDeclined(Rejected):
    """Terminal: the bank will decline the same charge again."""


@dataclass
class ChargeCard:
    payment_id: str
    card: str
    amount: int
    type: ClassVar[str] = "ChargeCard"


async def charge_twice(ledger: IdempotencyStore) -> None:
    runs: list[str] = []

    async def charge(cmd: ChargeCard) -> str:
        runs.append(cmd.payment_id)
        if cmd.card == "blocked":
            raise CardDeclined(f"card {cmd.card} declined")
        return f"charged {cmd.amount}"

    bus = CommandBus(idempotent(ledger, scope="payments", tenancy=Tenancy.SILO))
    bus.register(ChargeCard, charge)

    # 1. A charge, then the same message again: replayed from the ledger.
    first = await bus.dispatch(ChargeCard("pay-1", "visa", 40), idempotency_key="pay-1")
    again = await bus.dispatch(ChargeCard("pay-1", "visa", 40), idempotency_key="pay-1")
    print(f"pay-1: {first}, then again: {again} (replayed)")

    # 2. A declined charge is terminal, so the ledger caches the failure.
    raised: list[str] = []
    for attempt in (1, 2):
        try:
            await bus.dispatch(ChargeCard("pay-2", "blocked", 99), idempotency_key="pay-2")
        except (CardDeclined, CachedFailure) as e:
            raised.append(type(e).__name__)
            print(f"pay-2 dispatch {attempt}: {type(e).__name__}: {e}")

    assert (first, again) == ("charged 40", "charged 40")
    assert raised == ["CardDeclined", "CachedFailure"], raised
    assert runs == ["pay-1", "pay-2"], "each payment ran once; the repeats came from the ledger"
    print("\nok: the ledger replayed a success and a cached failure without running the handler")


async def main() -> None:
    url = os.environ.get("EDAX_REDIS_URL")
    if url is None:
        print("EDAX_REDIS_URL is not set: using MemoryLedger, the in-memory stand-in\n")
        await charge_twice(MemoryLedger())
        return

    from edax.backend.redis import AsyncRedisClient, RedisIdempotencyStore
    from redis.asyncio import Redis

    client = Redis.from_url(url)
    try:
        # The prefix names the service and environment. A fresh one per run keeps runs apart.
        ledger = RedisIdempotencyStore(AsyncRedisClient(client), prefix=f"examples:{guid()}:", ttl_ms=60_000)
        # The URL may carry a password, so only its host is printed.
        print(f"using Redis at {urlsplit(url).hostname}\n")
        await charge_twice(ledger)
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())

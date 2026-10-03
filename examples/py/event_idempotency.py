"""Event-handler idempotency, in its two modes — where the ledger lives decides the guarantee.

    Delivery ─▶ idempotent_by_id(ledger) ─▶ handler ─▶ ProjectionStore.save

The guard claims `env.id` before the handler runs and writes a terminal record after, so a broker
redelivery skips a handler that already ran. What that is worth depends on whether the claim and the
handler's write commit together.

Pass `unit_of_work=` and the guard opens the transaction itself, in the right place. That is what
lets a *failed commit* release the claim: a host that opens the transaction around the dispatch
commits after the guard has already returned, so the guard never learns the commit failed.

**Shared unit of work.** Ledger and projection store are built over one MemoryTransactor, so the
claim and the projection are one commit and a rollback drops both. Effectively-once. This is the
Idempotent Consumer pattern with a dedup table — the *lite* inbox. It is not the full transactional
Inbox, which stores the message body and acks the broker before processing; here the body is never
stored, so the handler cannot be decoupled from the broker.

**Ledger of its own.** A Redis ledger beside your aggregate database: two independent commits, which no
ordering can make atomic. The guard still collapses redeliveries and still releases the claim when
the commit fails, but a commit that lands and loses only its *reply* releases a claim over work that
survived, so the redelivery applies it twice. At-least-once with a dedup window, never
effectively-once. Keep the handler's own write idempotent — `EventStore.append` and
`ProjectionStore.save` already are, through their compare-and-set on version.

Run:  maturin develop -m crates/edax-py/Cargo.toml  &&  python examples/py/event_idempotency.py
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import edax
from edax import Entity
from edax.backend.memory import MemoryIdempotencyStore, MemoryProjectionStore, MemoryTransactor
from edax.command import Claimed, Record
from edax.router import idempotent_by_id

ACCOUNT = "acct-1"


def deposit(amount: int, seq: int) -> edax.EventEnvelope:
    """One Deposited event. `id` is what the guard claims, so it must be stable across redeliveries."""
    return edax.EventEnvelope(
        id=f"{ACCOUNT}-deposit-{seq}",
        aggregate_id=ACCOUNT,
        event=edax.Event("Deposited", 1, str(amount).encode()),
        namespace={},
        metadata={},
        timestamp=0,
    )


def deposit_handler(
    projections: MemoryProjectionStore, calls: list[str]
) -> Callable[[edax.EventEnvelope], Awaitable[None]]:
    """A handler with one side effect: it adds the deposit to the account's read model. `calls`
    records every invocation, so an assert can tell a skipped redelivery from a repeated one."""

    async def handle(env: edax.EventEnvelope) -> None:
        calls.append(env.id)
        entity = await projections.get(env.aggregate_id)
        if entity is None:
            entity = Entity(env.aggregate_id, 0, {"id": env.aggregate_id, "balance": 0})
        amount = int(env.event.data)
        entity.document["balance"] = entity.document["balance"] + abs(amount)
        await projections.save(entity)  # joins the guard's unit of work via current_tx(db)
        if amount < 0:  # a handler that writes and *then* fails, so the rollback has work to undo
            raise RuntimeError("the handler failed after writing")

    return handle


async def balance(projections: MemoryProjectionStore) -> int:
    entity = await projections.get(ACCOUNT)
    return 0 if entity is None else int(entity.document["balance"])


class CommitLosesItsReply:
    """A Transactor whose commit lands and then raises, as a connection dropped between the commit
    and its acknowledgement does. The caller cannot tell it from a commit that never happened —
    which is the whole reason two independent commits can only be at-least-once."""

    def __init__(self, inner: MemoryTransactor) -> None:
        self._inner = inner
        self.armed = False

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Any]:
        async with self._inner.transaction() as tx:
            yield tx
        if self.armed:
            self.armed = False
            raise edax.StoreError("the commit landed; its reply did not")


class LedgerOfItsOwn:
    """An :class:`~edax.command.IdempotencyStore` that does not join the caller's unit of work.

    `RedisIdempotencyStore` accepts `tx` and ignores it, because Redis cannot enlist in another
    database's transaction. Modelled here by forcing the auto-commit handle on every call, so the claim lands on
    its own database the moment it is taken, whatever transaction the caller has open."""

    def __init__(self) -> None:
        self._db = MemoryTransactor()
        self._inner = MemoryIdempotencyStore(self._db)

    async def claim(self, key: str, payload_hash: int, tx: Any | None = None) -> Claimed:
        return await self._inner.claim(key, payload_hash, tx=self._db.auto_commit())

    async def complete(self, key: str, handle: bytes, record: Record, tx: Any | None = None) -> None:
        await self._inner.complete(key, handle, record, tx=self._db.auto_commit())

    async def release(self, key: str, handle: bytes, tx: Any | None = None) -> None:
        await self._inner.release(key, handle, tx=self._db.auto_commit())


async def shared_unit_of_work() -> None:
    """The lite inbox: claim and projection commit together, so a rollback frees the key."""
    db = MemoryTransactor()  # one database for both participants
    projections = MemoryProjectionStore(db)
    calls: list[str] = []
    guard = idempotent_by_id(MemoryIdempotencyStore(db), unit_of_work=db)
    router = edax.EventRouter(guard)
    router.register("Deposited", deposit_handler(projections, calls))

    async def deliver(env: edax.EventEnvelope) -> None:
        await router.dispatch(env)  # the guard opens the unit of work; the handler joins it

    first, failing = deposit(100, 1), deposit(-50, 2)

    await deliver(first)
    await deliver(first)  # redelivery: the record is `done`, so the handler is skipped
    assert calls == [first.id], calls
    assert await balance(projections) == 100, await balance(projections)

    # The handler writes and then fails. The rollback drops the projection *and* the claim, because
    # both were staged in the one transaction.
    with contextlib.suppress(RuntimeError):
        await deliver(failing)
    assert calls == [first.id, failing.id], calls
    assert await balance(projections) == 100, "the handler's write rolled back with its claim"

    with contextlib.suppress(RuntimeError):
        await deliver(failing)  # the key is free, so the redelivery really re-runs
    assert calls == [first.id, failing.id, failing.id], calls
    assert await balance(projections) == 100, "and rolls back again: nothing accumulates"

    print(f"shared unit of work: handler ran {len(calls)}x, balance {await balance(projections)}")
    print("  committed event skipped on redelivery; a rolled-back one left no trace at all")


async def ledger_of_its_own() -> None:
    """Two stores, two commits: dedup still works, but the second commit has no guard around it."""
    work = MemoryTransactor()  # the aggregate/projection side
    projections = MemoryProjectionStore(work)
    calls: list[str] = []
    unit = CommitLosesItsReply(work)
    router = edax.EventRouter(idempotent_by_id(LedgerOfItsOwn(), unit_of_work=unit))
    router.register("Deposited", deposit_handler(projections, calls))

    async def deliver(env: edax.EventEnvelope) -> None:
        await router.dispatch(env)

    first, second = deposit(100, 1), deposit(50, 2)

    await deliver(first)
    await deliver(first)  # dedup works the same: one claim, one run
    assert calls == [first.id], calls
    assert await balance(projections) == 100, await balance(projections)

    # The window two independent commits cannot close. The work commits; only the reply is lost, so
    # the guard sees a failure and releases the claim — over a deposit that did land.
    unit.armed = True
    with contextlib.suppress(edax.StoreError):
        await deliver(second)
    assert calls == [first.id, second.id], calls
    assert await balance(projections) == 150, "the deposit landed, whatever the caller was told"

    await deliver(second)  # the claim was released, so the redelivery applies it a second time
    assert calls == [first.id, second.id, second.id], calls
    assert await balance(projections) == 200, "applied twice: at-least-once, not once"

    print(f"ledger of its own:   handler ran {len(calls)}x, balance {await balance(projections)}")
    print("  a commit whose reply was lost released its claim, so the redelivery applied it again")


async def main() -> None:
    await shared_unit_of_work()
    await ledger_of_its_own()
    print("\nok: one unit of work gives effectively-once; a separate ledger gives a dedup window")


if __name__ == "__main__":
    asyncio.run(main())

"""Event handling: broker deliveries fan out to handlers, acked on success / nacked on failure.

    broker ─▶ Delivery(envelope, ack, nack)             ─▶ KeyOrderedDispatcher ─▶ EventRouter(idempotent_by_id) ─▶ handler
                                       worker = partition(aggregate_id, workers)   │ ok    -> ack()
                                                                                   └ raise -> nack()

Same aggregate id -> same worker -> handlers run in order; different ids run concurrently. The
Delivery wraps the persisted envelope plus the broker's ack/nack handles; the worker settles it by
outcome.

A nacked delivery comes back, so the stream below replays two of them. `idempotent_by_id` claims
`env.id`, which is what tells the two redeliveries apart: the one that succeeded replays its record
and the handler is skipped, while the one that failed released its claim and runs again. The ledger
here is a store of its own, so this is dedup, not atomicity — see `event_idempotency.py` for what a
shared unit of work adds.

Run:  maturin develop -m crates/edax-py/Cargo.toml  &&  python examples/py/event_handling.py
"""

import asyncio

import edax
from edax import Delivery
from edax.backend.memory import MemoryIdempotencyStore
from edax.router import idempotent_by_id


def make_event(aggregate_id: str, name: str) -> edax.Envelope:
    return edax.Envelope(
        id=f"{aggregate_id}-{name}",
        aggregate_id=aggregate_id,
        event=edax.Event(name, 1, b""),
        namespace={},
        metadata={},
        timestamp=0,
    )


STREAM = [
    make_event("acct-alice", "Opened"),
    make_event("acct-bob", "Opened"),
    make_event("acct-alice", "Deposited"),
    make_event("acct-carol", "Opened"),
    make_event("acct-bob", "Deposited"),  # handler raises -> nacked
    make_event("acct-alice", "Withdrawn"),
    make_event("acct-carol", "Deposited"),
]
# What the broker brings back: the delivery that failed, and one that already succeeded.
REDELIVERED = [4, 2]


async def main() -> None:
    log: list[str] = []
    calls: list[str] = []  # every handler invocation, so a skipped redelivery is visible
    acked: list[bytes] = []
    nacked: list[bytes] = []

    router = edax.EventRouter(idempotent_by_id(MemoryIdempotencyStore()))

    @router.on("Opened")
    async def opened(env: edax.Envelope) -> None:
        calls.append(env.id)
        log.append(f"{env.aggregate_id}: opened")

    @router.on("Deposited")
    async def deposited(env: edax.Envelope) -> None:
        calls.append(env.id)
        if env.aggregate_id == "acct-bob":
            raise RuntimeError("projection store unavailable")  # transient -> claim released, nack
        log.append(f"{env.aggregate_id}: deposited")

    @router.on("Withdrawn")
    async def withdrawn(env: edax.Envelope) -> None:
        calls.append(env.id)
        log.append(f"{env.aggregate_id}: withdrawn")

    bus = edax.KeyOrderedDispatcher(4, router.dispatch)
    bus.start()
    for i in list(range(len(STREAM))) + REDELIVERED:
        env, mid = STREAM[i], f"msg-{i}".encode()
        # default arg binds mid per iteration so the closures don't all capture the last one
        await bus.submit(Delivery(env, lambda m=mid: acked.append(m), lambda m=mid: nacked.append(m)))
    await bus.stop()  # drain

    print("handled:", log)
    print(f"acked {sorted(acked)}\nnacked {nacked}")

    alice = [line for line in log if line.startswith("acct-alice")]
    assert alice == ["acct-alice: opened", "acct-alice: deposited", "acct-alice: withdrawn"], alice
    # msg-2 succeeded the first time: the redelivery replays the record without running the handler.
    assert acked.count(b"msg-2") == 2, acked
    assert calls.count("acct-alice-Deposited") == 1, calls
    # msg-4 failed transiently, so its claim was released and the redelivery runs — and fails again.
    assert nacked == [b"msg-4", b"msg-4"], nacked
    assert calls.count("acct-bob-Deposited") == 2, calls
    print("\nok: acct-alice handled in order; the succeeded redelivery skipped, the failed one re-run")


if __name__ == "__main__":
    asyncio.run(main())

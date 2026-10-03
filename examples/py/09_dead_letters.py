"""Step 9: a delivery that keeps failing goes to a dead-letter sink instead of blocking the stream.

A handler that raises nacks its delivery, and the broker sends it again. `on_dead` bounds that:

- a terminal error (a `Rejected`) is dead-lettered on the first attempt;
- a transient error is retried, and dead-lettered at the `max_attempts`-th failed attempt.

A dead-lettered delivery is acked, so the stream moves on.

Run:  cd examples/py && uv run python 09_dead_letters.py
"""

from __future__ import annotations

import asyncio
import contextlib

from edax import Backoff, Delivery, Event, EventEnvelope, KeyOrderedDispatcher, Pacing, Rejected
from edax.backend.memory import MemoryBroker


class AccountClosed(Rejected):
    """Terminal: the account stays closed however often the event comes back."""


def event(account_id: str, name: str) -> EventEnvelope:
    return EventEnvelope(
        id=f"{account_id}-{name}",
        aggregate_id=account_id,
        event=Event(name, 1, b"{}"),
        namespace={},
        metadata={},
        timestamp=0,
        sequence=1,
    )


async def main() -> None:
    # 1. Four events: one fine, one rejected, one that fails twice, one that always fails.
    broker = MemoryBroker()
    for account_id in ("alice", "bob", "carol", "dave"):
        await broker.publish(event(account_id, "Deposited"))

    attempts: dict[str, int] = {}

    async def handle(envelope: EventEnvelope) -> None:
        account_id = envelope.aggregate_id
        attempts[account_id] = attempts.get(account_id, 0) + 1
        print(f"  {account_id} attempt {attempts[account_id]}")
        if account_id == "bob":
            raise AccountClosed("bob closed his account")
        if account_id == "carol" and attempts["carol"] < 3:
            raise ConnectionError("ledger timeout")  # transient: passes on the third try
        if account_id == "dave":
            raise ConnectionError("ledger timeout")  # transient, but it never passes

    # 2. The dead-letter sink. A real one writes to a dead-letter topic or table.
    dead: list[str] = []

    async def to_dead_letters(delivery: Delivery, error: BaseException) -> None:
        dead.append(f"{delivery.envelope.aggregate_id}: {type(error).__name__}")

    # 3. One worker keeps the story in order. Short waits so the example runs fast.
    dispatcher = KeyOrderedDispatcher(
        workers=1,
        handler=handle,
        max_attempts=3,
        on_dead=to_dead_letters,
        pacing=Pacing(backoff=Backoff(base_millis=1, cap_millis=10)),
    )
    dispatcher.start()
    consumer = asyncio.create_task(broker.run(dispatcher))
    while not broker.empty():
        await asyncio.sleep(0.01)
    consumer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consumer
    await dispatcher.stop()

    print(f"dead letters: {dead}")
    assert attempts == {"alice": 1, "bob": 1, "carol": 3, "dave": 3}, attempts
    assert dead == ["bob: AccountClosed", "dave: ConnectionError"], dead
    print("\nok: the rejection died at once, the flaky event passed, the broken one died at attempt 3")


if __name__ == "__main__":
    asyncio.run(main())

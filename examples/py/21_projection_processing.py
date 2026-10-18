"""Step 21: fold ordered delta events into state and publish saved effect intents.

A stateful projection reads prior state and applies each logical event once. Use it for balances, counters or
other delta projections. Command handlers decide whether a requested deposit is allowed and save
its event. This reducer consumes that event; external effects go through its outbox, which a Relay drains.

MemoryProjectionStateStore is a volatile reference. See docs/sdk/projection-processing.md for PostgreSQL, Kafka source
verification and generation rebuilds. SourceProof below describes this example's fixed input log;
a production adapter must independently verify the source identity, history and retained bounds.

Run:  cd examples/py && uv run python 21_projection_processing.py
"""

from __future__ import annotations

import asyncio
from typing import Any

from edax.errors import CodecError
from edax.outbox import Relay
from edax.projection import (
    EffectIntent,
    MemoryProjectionStateStore,
    ProjectionCutover,
    ProjectionEvent,
    ProjectionInput,
    ProjectionIntent,
    ProjectionResult,
    ProjectionTarget,
    SourceProof,
)


def deposit(sequence: int, amount_minor: int) -> ProjectionEvent:
    return ProjectionEvent(
        tenant="bank-eu",
        aggregate="account-42",
        event_id=f"deposit-{sequence}",
        sequence=sequence,
        event_type="AmountDeposited",
        schema_version="1",
        payload={"amount_minor": amount_minor, "currency": "EUR"},
        metadata={"correlation_id": f"payment-{sequence}"},
    )


def balance(state: Any, event: ProjectionEvent) -> ProjectionResult:
    """Pure, deterministic calculation; the store commits state and intents together."""
    previous = 0 if state is None else state["balance_minor"]
    total = previous + event.payload["amount_minor"]
    return ProjectionResult(
        {"balance_minor": total, "currency": "EUR"},
        [
            ProjectionIntent(
                "receipt", "account-receipts", {"account": event.aggregate, "balance_minor": total}
            )
        ],
    )


async def main() -> None:
    target = ProjectionTarget(
        generation="balances-v1",
        source_identity="account-log-incarnation-1",
        source_history="account-history-1",
        topic="account-events",
        reducer_id="balance-v1",
        envelope_id="account-envelope-v1",
        keying_id="account-key-v1",
        effect_namespace="deposit-receipts-v1",
        starts={0: 0},
    )
    store = MemoryProjectionStateStore(target)
    await store.initialize()
    proof = SourceProof(target.source_identity, target.source_history, beginning=0, end=4)
    lease = await store.acquire(0, "example-worker", proof)

    # 1. Duplicate delivery at a new offset advances the cursor without adding the amount twice.
    first = deposit(1, 1250)
    log = [
        ProjectionInput(0, first),
        ProjectionInput(1, first),
        ProjectionInput(2, deposit(2, 500)),
        ProjectionInput(3, deposit(3, 700)),
    ]
    lease = await store.commit(lease, log[:2], proof, balance)
    saved = await store.get("bank-eu", "account-42")
    assert saved is not None and saved["value"]["balance_minor"] == 1250
    assert lease.next_offset == 2
    print("replay: two source offsets, one deposit, balance 1250")

    # 2. Skipping sequence 2 is refused. The failed batch changes neither state nor cursor.
    try:
        await store.commit(lease, [log[3]], proof, balance)
        raise AssertionError("expected a sequence gap")
    except CodecError as error:
        assert "sequence gap" in str(error)
    assert (await store.recover(lease)).next_offset == 2
    assert await store.get("bank-eu", "account-42") == saved
    lease = await store.commit(lease, [log[2]], proof, balance)
    saved = await store.get("bank-eu", "account-42")
    assert saved is not None and saved["value"]["balance_minor"] == 1750
    print("sequence: gap refused; sequence 2 applied, balance 1750")

    # 3. A new generation starts building with delivery disabled. For this first deployment, every
    # source offset is eligible. A replacement uses the old generation's freeze() cutover instead.
    assert not await store.unpublished(100)
    await store.activate(
        ProjectionCutover(target.source_identity, target.source_history, target.topic, target.starts)
    )
    assert len(await store.unpublished(100)) == 2

    # 4. Deduplicate by intent ID at the destination. Simulate delivery followed by a lost marker.
    receipts: dict[str, Any] = {}
    attempts: list[str] = []

    async def deliver(intent: EffectIntent) -> None:
        attempts.append(intent.id)
        receipts.setdefault(intent.id, intent.payload)

    pending = await store.unpublished(100)
    await deliver(pending[0])
    relay = Relay(store, deliver)
    assert await relay.drain() == 2
    assert len(attempts) == 3 and len(receipts) == 2
    assert await relay.drain() == 0
    await store.release(lease)
    print("outbox: three delivery attempts, two receipts after destination deduplication")
    print("\nok: ordered deltas, replay, sequence rollback, activation and outbox intents checked")


if __name__ == "__main__":
    asyncio.run(main())

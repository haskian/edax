"""Generic fold stores preserve transaction causes without importing a database adapter."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

import pytest
from edax.backend.kafka_fold import KafkaFold
from edax.backend.postgres import PostgresTransactionFailure
from edax.errors import BadHandle, CodecError, InProgress, StoreError, TransactionFailure
from edax.fold import FoldSession, FoldStore
from edax.pacing import Backoff, Pacing
from test_fold import reducer, target
from test_kafka_fold import TP, Client, Source, decode

T = TypeVar("T")


class FailingStore(FoldStore):
    def __init__(self, cause: Exception) -> None:
        super().__init__(target())
        self.cause = cause
        self.calls = 0

    async def _transaction(self, call: Callable[[FoldSession], Awaitable[T]]) -> T:
        self.calls += 1
        raise TransactionFailure(self.cause, rolled_back=True, commit_started=False)


@pytest.mark.parametrize(
    ("cause", "stops", "attempts"),
    [
        pytest.param(BadHandle("incompatible fold table"), True, 0, id="fatal"),
        pytest.param(CodecError("invalid stored account state"), True, 0, id="terminal"),
        pytest.param(InProgress("account partition is owned"), False, 0, id="contention"),
        pytest.param(StoreError("database unavailable"), False, 1, id="transient"),
    ],
)
async def test_generic_transaction_cause_controls_partition_recovery(
    cause: Exception, stops: bool, attempts: int
) -> None:
    store = FailingStore(cause)
    runner = KafkaFold(
        Client(),
        store,
        Source(),
        decode,
        reducer,
        pacing=Pacing(backoff=Backoff(1000, 1000), entropy=lambda: 999),
    )
    await runner.rebalance_listener().on_partitions_assigned({TP})
    task = asyncio.create_task(runner.run())
    assigned = runner._parts[TP]

    async def classified() -> None:
        while assigned.failure is None and assigned.ready_at == 0:
            await asyncio.sleep(0)

    try:
        await asyncio.wait_for(classified(), 1)
        assert not task.done()
        assert store.calls == 1
        assert assigned.attempts == attempts
        assert (assigned.failure is cause) is stops
        assert (TP in runner.partition_failures) is stops
        assert assigned.lease is None
        assert runner.retained_records == runner.retained_bytes == 0
    finally:
        await runner.stop()
        await runner.wait_closed()


def test_postgres_transaction_failure_preserves_public_evidence_contract() -> None:
    cause = StoreError("commit reply lost")
    error = PostgresTransactionFailure(cause, rolled_back=True, commit_started=True)
    assert isinstance(error, TransactionFailure)
    assert isinstance(error, StoreError)
    assert error.cause is cause
    assert error.rolled_back and error.commit_started
    assert str(error) == str(cause)

"""Recovery budgets and shutdown remain finite across replay and task completion."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest
from edax import _native
from edax.backend.kafka_ingest import KafkaIngestion
from edax.ingestion import BatchCommitError, BatchInput, BatchResult
from edax.pacing import Pacing
from test_kafka_ingest import (
    TP,
    BatchClient,
    EventIds,
    RecordingSink,
    events,
    mapper,
    no_dead,
    records,
    settings,
    until,
)


@pytest.mark.parametrize("parent_retry", [False, True], ids=["split", "retry_then_split"])
async def test_repeated_isolation_after_eviction_preserves_child_retry_budget(
    monkeypatch: pytest.MonkeyPatch,
    parent_retry: bool,
) -> None:
    monkeypatch.setattr(_native, "backoff_millis", lambda *args: 10_000)

    class FailedRejectionSink(RecordingSink):
        rejected_attempts: int = 0
        parent_attempts: int = 0
        retrying_parent: bool = False

        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            if parent_retry and len(inputs) > 1:
                self.parent_attempts += 1
                if self.parent_attempts % 2 == 1:
                    self.retrying_parent = True
                    raise BatchCommitError("retry", OSError("temporary parent transaction failure"))
            if any(item.event_id == "p0-0" and item.failure is None for item in inputs):
                raise BatchCommitError("isolate", ValueError("row constraint"))
            if any(item.failure is not None for item in inputs):
                self.retrying_parent = False
                self.rejected_attempts += 1
                raise BatchCommitError("retry", OSError("rejection outbox unavailable"))
            return await super().commit(inputs)

    messages = events(2)
    client = BatchClient(records(messages))
    sink = FailedRejectionSink()

    async def backoff(delay: float) -> None:
        if not sink.retrying_parent:
            await asyncio.Event().wait()

    runner = KafkaIngestion(
        client,
        sink,
        mapper,
        no_dead,
        settings=settings(max_records=2, max_attempts=3 if parent_retry else 2),
        serializer=EventIds(messages),
        pacing=Pacing(sleep=backoff),
    )
    await runner._assign({TP})
    part = runner._parts[TP]
    try:
        for cycle in range(2):
            for record in client.logs[TP]:
                runner._admit(TP, part, record)
            client.positions[TP] = 2
            runner._launch(TP, part)
            await until(lambda: part.backing_off or runner._failure is not None)
            if cycle == 0:
                assert runner._evict_retry()
                await until(lambda: part.task is None)
                part.ready_at = 0
        assert isinstance(runner._failure, BatchCommitError)
        assert sink.rejected_attempts == 2
        assert client.commits == []
        assert await sink.pending() == {}
    finally:
        for worker in tuple(runner._workers):
            worker.cancel()
        await runner.wait_closed()


async def test_new_isolation_resets_parent_retry_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_native, "backoff_millis", lambda *args: 0)

    class RecoveringSink(RecordingSink):
        def __init__(self) -> None:
            super().__init__()
            self.attempts: dict[tuple[str, ...], int] = {}

        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            batch = tuple(item.event_id for item in inputs)
            self.attempts[batch] = attempt = self.attempts.get(batch, 0) + 1
            if attempt == 1:
                raise BatchCommitError("retry", OSError("temporary connection loss"))
            if len(inputs) > 1:
                raise BatchCommitError("isolate", ValueError("confirmed row constraint"))
            return await super().commit(inputs)

    messages = events(2)
    client = BatchClient(records(messages))
    sink = RecoveringSink()
    runner = KafkaIngestion(
        client,
        sink,
        mapper,
        no_dead,
        settings=settings(max_records=2, max_attempts=2),
        serializer=EventIds(messages),
    )
    await runner._assign({TP})
    part = runner._parts[TP]
    for record in client.logs[TP]:
        runner._admit(TP, part, record)
    runner._launch(TP, part)
    await runner.wait_closed()
    assert runner._failure is None
    assert sink.attempts == {("p0-0", "p0-1"): 2, ("p0-0",): 2, ("p0-1",): 2}
    assert client.commits[-1] == {TP: 2}
    assert part.attempts == part.retry_batches == {}


async def test_wait_closed_runs_queued_completion_callbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = KafkaIngestion(BatchClient([]), RecordingSink(), mapper, no_dead, settings=settings())
    task = asyncio.create_task(asyncio.sleep(0))
    await task
    runner._workers.add(task)
    task.add_done_callback(runner._workers.discard)
    original_gather = asyncio.gather
    calls = 0

    def bounded_gather(*tasks: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        # A regression must fail instead of blocking the event loop and the test timeout forever.
        assert calls < 3, "wait_closed spins on finished tasks before running their callbacks"
        return original_gather(*tasks, **kwargs)

    monkeypatch.setattr(asyncio, "gather", bounded_gather)
    await runner.wait_closed()
    assert not runner._workers

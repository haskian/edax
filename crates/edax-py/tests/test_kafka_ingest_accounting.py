"""Serialized admission and actual data lifetime across failed ingestion operations."""

from __future__ import annotations

import asyncio
import gc
import weakref
from collections.abc import Sequence
from typing import Any

from edax import _json
from edax.delivery import DeadLetter
from edax.ingestion import BatchCommitError, BatchInput, BatchResult
from test_kafka_ingest import (
    BatchClient,
    EventIds,
    KafkaIngestion,
    RecordingSink,
    events,
    mapper,
    no_dead,
    records,
    running,
    settings,
    until,
)


async def test_isolated_rejection_stays_within_reserved_bytes() -> None:
    release = asyncio.Event()
    entered = asyncio.Event()
    measured: list[int] = []
    dead: list[DeadLetter[Any]] = []

    class Sink(RecordingSink):
        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            if inputs[0].failure is None:
                raise BatchCommitError("isolate", ValueError("bad row: " + "x" * 200_000))
            measured.append(len(_json.encode(inputs).encode()))
            entered.set()
            await release.wait()
            return await super().commit(inputs)

    async def publish(letter: DeadLetter[Any]) -> None:
        dead.append(letter)

    messages = events(1)
    client = BatchClient(records(messages))
    runner = KafkaIngestion(
        client, Sink(), mapper, publish, settings=settings(max_records=1), serializer=EventIds(messages)
    )
    async with running(runner):
        try:
            await asyncio.wait_for(entered.wait(), 1)
            assert measured[0] <= runner.retained_bytes <= runner.settings.retained_bytes
        finally:
            release.set()
        await until(lambda: bool(client.commits))
    assert len(dead) == 1
    assert "ingestion sink refused this original" in str(dead[0].failure)


async def test_failed_runner_releases_batch_references_after_wait_closed() -> None:
    seen: list[weakref.ReferenceType[BatchInput]] = []

    class Sink(RecordingSink):
        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            seen.extend(weakref.ref(item) for item in inputs)
            raise BatchCommitError("fatal", ValueError("invalid schema"))

    messages = events(1)
    runner = KafkaIngestion(
        BatchClient(records(messages)),
        Sink(),
        mapper,
        no_dead,
        settings=settings(max_records=1),
        serializer=EventIds(messages),
    )
    async with running(runner) as task:
        await until(task.done)
    del task
    gc.collect()
    assert runner.retained_records == runner.retained_bytes == 0
    assert seen and all(ref() is None for ref in seen)

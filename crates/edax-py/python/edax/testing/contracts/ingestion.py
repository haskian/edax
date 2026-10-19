"""Ingestion sink clauses. Supply a fresh ``ingestion_sink`` in each append/upsert mode.

Transactional subclasses also supply its ``transactor``. Run against real storage so the race
clauses test locks and uniqueness, not a mocked response. Targets should allow plaintext fixture
values (subjectless rows here); sealing/erasure integration needs provider-specific tests too.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

import pytest

from ...errors import BadHandle, ConcurrencyError, TransactionFailure
from ...ingestion import BatchInput, BatchSink, MappedRow, StagedBatchResult, TransactionalBatchSink
from ...unit_of_work import Transactor, transaction
from .._concurrency import staged_then_read, unique_id


def _input(sink: BatchSink, *, event_id: str | None = None, sequence: int = 1) -> BatchInput:
    event_id = event_id or unique_id("reading")
    row_key = event_id if sink.target.mode == "append" else "device-42-latest"
    return sink.target.input_from_source(
        unique_id("source"),
        event_id=event_id,
        stream="device-42",
        sequence=sequence,
        row=MappedRow(row_key, public={"pulse_bpm": 72, "sensor": "wearable"}),
        content={"reading_id": event_id, "pulse_bpm": 72},
        raw=f"device-42:{event_id}:{sequence}",
        tenant="clinic-a",
    )


async def _commit(sink: BatchSink, original: BatchInput) -> tuple[str, ...]:
    # Missing-key uniqueness may abort one racer. Retry only a proven rollback/concurrency failure.
    for attempt in range(3):
        try:
            return (await sink.commit([original])).outcomes
        except TransactionFailure as error:
            if (
                not error.rolled_back
                or error.commit_started
                or not isinstance(error.cause, ConcurrencyError)
                or attempt == 2
            ):
                raise
        except ConcurrencyError:
            if attempt == 2:
                raise
    raise AssertionError("bounded retry did not return")


class BatchSinkContract:
    """Durable decisions, isolation of identities, full-state ordering and rejection publication."""

    async def test_empty_batch_has_no_outcomes(self, ingestion_sink: BatchSink) -> None:
        result = await ingestion_sink.commit([])
        assert result.outcomes == () and result.rejections == {}

    async def test_replay_and_conflict_keep_first_state(self, ingestion_sink: BatchSink) -> None:
        item = _input(ingestion_sink)
        assert (await ingestion_sink.commit([item, item])).outcomes == ("accepted", "accepted")
        assert (await ingestion_sink.commit([item])).outcomes == ("accepted",)
        changed = replace(item, source=_input(ingestion_sink).source, public={"pulse_bpm": 90})
        assert (await ingestion_sink.commit([changed])).outcomes == ("rejected",)
        key = item.event_id if ingestion_sink.target.mode == "append" else "device-42-latest"
        row = await ingestion_sink.get("clinic-a", key)
        assert row is not None and row["public"] == item.public

    async def test_rejection_survives_replay_and_publication(self, ingestion_sink: BatchSink) -> None:
        item = replace(_input(ingestion_sink), failure="codec: invalid pulse reading")
        assert (await ingestion_sink.commit([item])).outcomes == ("rejected",)
        pending = await ingestion_sink.pending()
        assert pending[item.source]["raw"] == item.raw
        with pytest.raises(BadHandle):
            await ingestion_sink.published({item.source: "0" * 64})
        assert item.source in await ingestion_sink.pending()
        assert (await ingestion_sink.commit([replace(item, failure=None)])).outcomes == ("rejected",)
        await ingestion_sink.published({item.source: pending[item.source]["fingerprint"]})
        assert item.source not in await ingestion_sink.pending()
        replay = await ingestion_sink.commit([item])
        assert replay.rejections[item.source]["published"] is True
        with pytest.raises(BadHandle):
            await ingestion_sink.commit([replace(item, raw_fingerprint="f" * 64)])

    async def test_mixed_batch_retains_each_original_outcome(self, ingestion_sink: BatchSink) -> None:
        first = _input(ingestion_sink)
        refused = replace(_input(ingestion_sink, sequence=2), failure="codec: invalid pulse reading")
        result = await ingestion_sink.commit([first, refused])
        assert result.outcomes == ("accepted", "rejected")
        assert set(result.rejections) == {refused.source}

    async def test_concurrent_conflicting_writers_cannot_both_win(self, ingestion_sink: BatchSink) -> None:
        await ingestion_sink.commit([])  # exercise row races after manifest initialization
        first = _input(ingestion_sink)
        second = replace(first, source=_input(ingestion_sink).source, public={"pulse_bpm": 90})
        results = await asyncio.gather(_commit(ingestion_sink, first), _commit(ingestion_sink, second))
        assert sorted(results) == [("accepted",), ("rejected",)]

    async def test_upsert_keeps_owner_and_tombstone(self, ingestion_sink: BatchSink) -> None:
        if ingestion_sink.target.mode != "upsert":
            return  # this clause has no sequence contract in append mode
        first = _input(ingestion_sink)
        deleted = replace(_input(ingestion_sink, sequence=3), public=None)
        assert (await ingestion_sink.commit([first, deleted])).outcomes == ("accepted", "accepted")
        stale = _input(ingestion_sink, sequence=2)
        competitor = replace(_input(ingestion_sink, sequence=4), stream="device-99")
        assert (await ingestion_sink.commit([stale, competitor])).outcomes == ("accepted", "rejected")
        row = await ingestion_sink.get("clinic-a", "device-42-latest")
        assert row is not None and row["sequence"] == 3 and row["public"] is None
        assert row["stream"] == "device-42"


class TransactionalBatchSinkContract(BatchSinkContract):
    """Staging is provisional and must join the same live transaction as other writes."""

    async def test_stage_requires_a_transaction(self, ingestion_sink: TransactionalBatchSink) -> None:
        with pytest.raises(BadHandle):
            await ingestion_sink.stage([_input(ingestion_sink)])

    async def test_stage_rolls_back_state_and_rejections(
        self, ingestion_sink: TransactionalBatchSink, transactor: Transactor[Any]
    ) -> None:
        item = _input(ingestion_sink)
        refused = replace(_input(ingestion_sink), failure="codec: invalid pulse reading")
        with pytest.raises(RuntimeError, match="handler failed"):
            async with transaction(transactor):
                result = await ingestion_sink.stage([item, refused])
                assert isinstance(result, StagedBatchResult)
                assert result.outcomes == ("accepted", "rejected")
                raise RuntimeError("handler failed")
        key = item.event_id if ingestion_sink.target.mode == "append" else "device-42-latest"
        assert await ingestion_sink.get("clinic-a", key) is None
        assert await ingestion_sink.pending() == {}
        async with transaction(transactor) as tx:
            assert (await ingestion_sink.stage([item], tx=tx)).outcomes == ("accepted",)
            assert await ingestion_sink.get("clinic-a", key, tx=tx) is not None
        assert await ingestion_sink.get("clinic-a", key) is not None

    async def test_stage_is_invisible_until_commit(
        self, ingestion_sink: TransactionalBatchSink, transactor: Transactor[Any]
    ) -> None:
        await ingestion_sink.commit([])
        item = _input(ingestion_sink)
        key = item.event_id if ingestion_sink.target.mode == "append" else "device-42-latest"
        before = await staged_then_read(
            transactor.begin,
            lambda tx: ingestion_sink.stage([item], tx=tx),
            lambda tx: ingestion_sink.get("clinic-a", key, tx=tx),
        )
        assert before is None
        assert await ingestion_sink.get("clinic-a", key) is None
        async with transaction(transactor):
            await ingestion_sink.stage([item])
        assert await ingestion_sink.get("clinic-a", key) is not None

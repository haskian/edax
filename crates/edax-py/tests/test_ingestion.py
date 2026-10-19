from __future__ import annotations

import asyncio
import gc
from collections.abc import Mapping
from dataclasses import replace

import pytest
from conftest import NO_WAIT
from edax.batch import BatchSettings
from edax.errors import BadHandle
from edax.ingestion import (
    BatchInput,
    IngestionTarget,
    MemoryIngestionSink,
    Rejection,
    RejectionOutbox,
)
from edax.outbox import Relay


def original(
    target: IngestionTarget,
    offset: int = 0,
    sequence: int | None = 1,
    status: str = "open",
    *,
    stream: str = "order-42",
    failure: str | None = None,
) -> BatchInput:
    event_id = f"event-{sequence}"
    return target.input(
        "orders",
        0,
        offset,
        "tenant-7",
        event_id if target.mode == "append" else "order-42",
        stream,
        event_id,
        sequence,
        {"type": "order.state", "status": status, "sequence": sequence},
        {"order_id": "order-42", "status": status},
        f'{{"offset":{offset},"status":"{status}"}}',
        failure,
    )


@pytest.fixture
def target() -> IngestionTarget:
    return IngestionTarget("upsert", "orders-v1", "cluster-a:orders-v1", "full-order-v1")


async def test_rejected_original_cannot_claim_owner_on_replay(target: IngestionTarget) -> None:
    sink = MemoryIngestionSink(target)
    invalid = original(target, failure="codec: mapper refused event")
    rejected = await sink.commit([invalid])
    assert rejected.outcomes == ("rejected",)
    assert await sink.get("tenant-7", "order-42") is None

    valid = original(target, 1, 2, "paid", stream="new-owner")
    replay = await sink.commit([replace(invalid, failure=None), valid])
    assert replay.outcomes == ("rejected", "accepted")
    assert replay.rejections == rejected.rejections
    stored = await sink.get("tenant-7", "order-42")
    assert stored is not None
    assert stored["stream"] == "new-owner"
    assert stored["value"] == valid.value


async def test_tombstone_keeps_sequence_and_owner(target: IngestionTarget) -> None:
    sink = MemoryIngestionSink(target)
    deleted = replace(original(target, 3, 3, "deleted"), value=None)
    assert (await sink.commit([deleted])).outcomes == ("accepted",)
    result = await sink.commit(
        [
            original(target, 2, 2, "paid"),
            deleted,
            original(target, 4, 4, "open", stream="different-order"),
        ]
    )
    assert result.outcomes == ("accepted", "accepted", "rejected")
    stored = await sink.get("tenant-7", "order-42")
    assert stored is not None
    assert stored["sequence"] == 3
    assert stored["stream"] == "order-42"
    assert stored["value"] is None


async def test_append_replays_and_conflicts_are_atomic() -> None:
    target = IngestionTarget("append", "events-v1", "cluster-a:orders-v1", "event-copy-v1")
    sink = MemoryIngestionSink(target)
    first = original(target)
    duplicate = original(target, offset=1)
    conflict = original(target, offset=2, status="cancelled")
    result = await sink.commit([first, duplicate, conflict])
    assert result.outcomes == ("accepted", "accepted", "rejected")
    stored = await sink.get("tenant-7", first.event_id)
    assert stored is not None
    assert stored["value"] == first.value
    assert (await sink.commit([first, duplicate, conflict])).outcomes == result.outcomes
    assert list(await sink.pending()) == [conflict.source]


async def test_rejection_publish_failure_leaves_original_pending(target: IngestionTarget) -> None:
    sink = MemoryIngestionSink(target)
    rejected = original(target, failure="codec: unknown event media type")
    await sink.commit([rejected])
    calls: list[str] = []

    async def publish(row: Rejection) -> None:
        calls.append(row.source)
        assert row.raw == rejected.raw
        if len(calls) == 1:
            raise OSError("broker unavailable")

    relay = Relay(RejectionOutbox(sink), publish, pacing=NO_WAIT)
    with pytest.raises(OSError, match="broker unavailable"):
        await relay.drain()
    assert list(await sink.pending()) == [rejected.source]
    assert await relay.drain() == 1
    assert await sink.pending() == {}
    replay = await sink.commit([rejected])
    assert replay.rejections[rejected.source]["published"] is True
    assert calls == [rejected.source, rejected.source]


async def test_publish_mark_failure_allows_duplicate_delivery(target: IngestionTarget) -> None:
    class FailingMarkSink(MemoryIngestionSink):
        fail = True

        async def published(self, confirmed: Mapping[str, str]) -> None:
            if self.fail:
                self.fail = False
                raise OSError("mark acknowledgement lost")
            await super().published(confirmed)

    sink = FailingMarkSink(target)
    rejected = original(target, failure="rejected: invalid order")
    await sink.commit([rejected])
    delivered: list[str] = []

    async def publish(row: Rejection) -> None:
        delivered.append(row.source)

    relay = Relay(RejectionOutbox(sink), publish, pacing=NO_WAIT)
    with pytest.raises(OSError, match="mark acknowledgement lost"):
        await relay.drain()
    assert await relay.drain() == 1
    assert delivered == [rejected.source, rejected.source]
    assert await sink.pending() == {}


@pytest.mark.parametrize("case", ["unknown_source", "changed_fingerprint"])
async def test_published_refuses_unknown_or_changed_rejection(target: IngestionTarget, case: str) -> None:
    sink = MemoryIngestionSink(target)
    rejected = original(target, failure="codec: invalid order")
    await sink.commit([rejected])
    pending = await sink.pending()
    source = original(target, offset=1).source if case == "unknown_source" else rejected.source
    fingerprint = pending[rejected.source]["fingerprint"] if case == "unknown_source" else "changed"
    expected = "unknown ingestion rejection" if case == "unknown_source" else "fingerprint"
    with pytest.raises(BadHandle, match=expected):
        await sink.published({source: fingerprint})
    assert await sink.pending() == pending
    assert pending[rejected.source]["published"] is False


async def test_sink_refuses_other_target_before_writing(target: IngestionTarget) -> None:
    sink = MemoryIngestionSink(target)
    wrong = original(replace(target, generation="other-generation"))
    with pytest.raises(BadHandle):
        await sink.commit([original(target), wrong])
    assert await sink.get("tenant-7", "order-42") is None


async def test_memory_handle_refuses_other_thread(target: IngestionTarget) -> None:
    # Collect prior async-test cycles on their native handles' owner thread.
    gc.collect()
    sink = MemoryIngestionSink(target)

    def other_thread() -> str:
        try:
            asyncio.run(sink.get("tenant-7", "order-42"))
        except BadHandle as error:
            return str(error)
        raise AssertionError("native handle crossed its owner thread")

    assert "another thread" in await asyncio.to_thread(other_thread)


def test_settings_and_target_validation() -> None:
    defaults = BatchSettings()
    assert defaults.max_records == 500
    assert defaults.retained_records == 2_000
    assert defaults.retained_bytes == 16_777_216
    with pytest.raises(BadHandle):
        BatchSettings(retained_records=500)
    with pytest.raises(BadHandle):
        IngestionTarget("upsert", "", "source-v1", "mapper-v1")


def test_append_input_requires_event_identity_as_row_key() -> None:
    target = IngestionTarget("append", "events-v1", "source-v1", "mapper-v1")
    good = original(target)
    with pytest.raises(BadHandle):
        target.validate([replace(good, key=target.key(["append", "tenant-7", "different-event"]))])


async def test_rejection_outbox_ignores_ids_that_name_no_rejection(target: IngestionTarget) -> None:
    sink = MemoryIngestionSink(target)
    await sink.commit([original(target, failure="codec: invalid order")])
    outbox = RejectionOutbox(sink)
    await outbox.mark_published(["not-json", "42", '["only-one"]'])
    assert len(await outbox.unpublished(100)) == 1


@pytest.mark.parametrize("case", ["unknown_source", "changed_fingerprint"])
async def test_rejection_outbox_preserves_strict_publication_checks(
    target: IngestionTarget, case: str
) -> None:
    sink = MemoryIngestionSink(target)
    rejected = original(target, failure="codec: invalid order")
    await sink.commit([rejected])
    outbox = RejectionOutbox(sink)
    pending = await outbox.unpublished(100)
    changed = (
        replace(pending[0], source=original(target, offset=1).source)
        if case == "unknown_source"
        else replace(pending[0], fingerprint="changed")
    )
    with pytest.raises(BadHandle):
        await outbox.mark_published([changed.id])
    assert await outbox.unpublished(100) == pending
    await outbox.mark_published([pending[0].id])
    await outbox.mark_published([pending[0].id])
    assert await outbox.unpublished(100) == []

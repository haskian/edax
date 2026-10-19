"""Direct sources preserve identity without a Kafka coordinate or an edax wire envelope."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest
from edax.errors import BadHandle
from edax.ingestion import BatchInput, IngestionTarget, MappedRow, MemoryIngestionSink
from edax.sealing import KeyScope, MemoryKeys, Sealing
from edax.testing import BatchSinkContract


class NonReentrantKeys(MemoryKeys):
    """A provider need not support recursive acquisition of its storage fence."""

    def __init__(self) -> None:
        super().__init__()
        self.lock = asyncio.Lock()
        self.acquisitions = 0
        self.waiting = asyncio.Event()

    @asynccontextmanager
    async def guard(self, scope: KeyScope) -> AsyncIterator[None]:
        self.waiting.set()
        async with self.lock, super().guard(scope):
            self.acquisitions += 1
            task = asyncio.current_task()
            try:
                yield
            finally:
                assert asyncio.current_task() is task, "provider exit must stay with its owner"


def reading(target: IngestionTarget) -> BatchInput:
    return target.input_from_source(
        "device-42/session-9/reading-1",
        event_id="reading-1",
        stream="device-42",
        row=MappedRow("reading-1", private={"pulse_bpm": 72}, public={"sensor": "wearable"}),
        content={"reading_id": "reading-1", "pulse_bpm": 72},
        raw='{"reading_id":"reading-1","pulse_bpm":72}',
        tenant="clinic-a",
        subject="patient-42",
        sequence=1,
    )


async def test_direct_sealed_replay_uses_plaintext_fingerprint() -> None:
    target = IngestionTarget("append", "pulse-v1", "bridge-v1", "pulse-reading-v1")
    sink = MemoryIngestionSink(target, sealing=Sealing(MemoryKeys(), "clinic-keys"))
    original = reading(target)
    assert original.source != target.source_key("device-42/session-9/reading-1", 0, 0)
    assert (await sink.commit([original])).outcomes == ("accepted",)
    assert (await sink.commit([original])).outcomes == ("accepted",)
    stored = await sink.get("clinic-a", "reading-1", "patient-42")
    assert stored is not None and stored["private"] == {"pulse_bpm": 72}
    assert (await sink.commit([replace(original, private={"pulse_bpm": 73})])).outcomes == ("rejected",)


@pytest.mark.parametrize("replay", [False, True])
async def test_failed_direct_original_is_rejected_after_subject_destruction(replay: bool) -> None:
    target = IngestionTarget("append", "pulse-v1", "bridge-v1", "pulse-reading-v1")
    keys = MemoryKeys()
    sealing = Sealing(keys, "clinic-keys")
    sink = MemoryIngestionSink(target, sealing=sealing)
    original = replace(reading(target), failure="codec: invalid reading")
    if replay:
        assert (await sink.commit([original])).outcomes == ("rejected",)
    scope = sealing.scope("clinic-a", "patient-42")
    await keys.revoke(scope)
    await keys.destroy(scope)

    assert (await sink.commit([original])).outcomes == ("rejected",)
    assert (await sink.pending())[original.source]["error"] == "codec: invalid reading"


@pytest.mark.parametrize("source_id", ["", "reading\x00id"])
def test_direct_source_refuses_invalid_identity(source_id: str) -> None:
    target = IngestionTarget("append", "pulse-v1", "bridge-v1", "pulse-reading-v1")
    with pytest.raises(BadHandle):
        target.input_from_source(
            source_id, event_id="pulse-1", stream="device-42", row=MappedRow("pulse-1"), content={}, raw="{}"
        )


async def test_custom_plan_keeps_kernel_replay_and_manifest_rules() -> None:
    from edax.ingestion import plan_ingestion, prepare_ingestion

    target = IngestionTarget("append", "pulse-v1", "bridge-v1", "pulse-reading-v1", at_rest="plaintext")
    target.check_binding(None)
    with pytest.raises(BadHandle):
        target.check_binding(
            {
                "mode": "upsert",
                "generation": target.generation,
                "source_incarnation": target.source_incarnation,
                "mapper_id": target.mapper_id,
                "at_rest": target.at_rest,
            }
        )
    original = reading(target)
    async with prepare_ingestion(target, None, [original]) as prepared:
        plan = plan_ingestion(target, prepared, {}, {})
    assert plan.outcomes == ("accepted",)
    assert plan.writes[original.key]["private"] == {"pulse_bpm": 72}
    replay = plan_ingestion(target, [original], plan.writes, {})
    assert replay.outcomes == ("accepted",) and replay.writes == {}
    changed = replace(original, private={"pulse_bpm": 73})
    conflict = plan_ingestion(target, [changed], plan.writes, {})
    assert conflict.outcomes == ("rejected",) and conflict.writes == {}
    assert conflict.rejections[original.source]["raw"] == original.raw


class TestMemoryIngestionContract(BatchSinkContract):
    @pytest.fixture(params=["append", "upsert"])
    def ingestion_sink(self, request: pytest.FixtureRequest) -> MemoryIngestionSink:
        return MemoryIngestionSink(IngestionTarget(request.param, "pulse-v1", "bridge-v1", "pulse-v1"))


@pytest.mark.parametrize("value", [2**53, 2**80])
async def test_direct_sealed_invalid_numbers_are_durable_rejections(value: int) -> None:
    target = IngestionTarget("append", "pulse-v1", "bridge-v1", "pulse-v1")
    sink = MemoryIngestionSink(target, sealing=Sealing(MemoryKeys(), "clinic-keys"))
    item = replace(reading(target), private={"pulse_bpm": value})
    assert (await sink.commit([item])).outcomes == ("rejected",)
    assert item.source in await sink.pending()
    assert await sink.get("clinic-a", "reading-1", "patient-42") is None


async def test_preparation_refuses_transaction_without_its_owner() -> None:
    from edax.ingestion import prepare_ingestion

    target = IngestionTarget("append", "pulse-v1", "bridge-v1", "pulse-v1")
    with pytest.raises(BadHandle, match="transactor"):
        async with prepare_ingestion(target, None, [], tx=object()):
            pytest.fail("an explicit transaction must not lose its fence lifetime")


async def test_owned_commit_reuses_its_enclosing_scope_guard() -> None:
    keys = NonReentrantKeys()
    sealing = Sealing(keys, "clinic-keys")
    target = IngestionTarget("append", "pulse-v1", "bridge-v1", "pulse-v1")
    sink = MemoryIngestionSink(target, sealing=sealing)

    async def write() -> None:
        async with sealing.fenced({("clinic-a", "patient-42")}):
            assert (await sink.commit([reading(target)])).outcomes == ("accepted",)
            assert keys.lock.locked()

    await asyncio.wait_for(write(), 1)
    assert keys.acquisitions == 1 and not keys.lock.locked()


@pytest.mark.parametrize("start_after_release", [False, True], ids=["live-parent", "expired-parent"])
async def test_unjoined_child_cannot_borrow_parent_guard(start_after_release: bool) -> None:
    keys = NonReentrantKeys()
    sealing = Sealing(keys, "clinic-keys")
    target = IngestionTarget("append", "pulse-v1", "bridge-v1", "pulse-v1")
    sink = MemoryIngestionSink(target, sealing=sealing)
    start = asyncio.Event()

    async def child() -> None:
        await start.wait()
        assert (await sink.commit([reading(target)])).outcomes == ("accepted",)

    async with sealing.fenced({("clinic-a", "patient-42")}):
        writing = asyncio.create_task(child())
        keys.waiting.clear()
        if not start_after_release:
            start.set()
            await asyncio.wait_for(keys.waiting.wait(), 1)
            assert not writing.done(), "an independent child must acquire its own fence"
    start.set()
    await asyncio.wait_for(writing, 1)
    assert keys.acquisitions == 2 and not keys.lock.locked()

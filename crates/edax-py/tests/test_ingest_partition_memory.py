"""Memory reference coverage for partition admission and pre-sealing validation."""

from __future__ import annotations

import json

import pytest
from edax.ingestion import BatchInput, IngestionTarget, MappedRow, MemoryIngestionSink


def record(sink: MemoryIngestionSink, identity: str, *, subject: str | None = None) -> BatchInput:
    content = {"reading_id": identity, "device_id": "device-42", "pulse_bpm": 72}
    return sink.target.input_from_source(
        f"device-42/{identity}",
        event_id=identity,
        stream="device-42",
        row=MappedRow(identity, private=content),
        content=content,
        raw=json.dumps(content),
        tenant="clinic-a",
        subject=subject,
    )


@pytest.mark.parametrize(
    "identity",
    [
        "malformed",
        "00000000-0001-4000-8000-000000000001",
        "00000000-0001-7000-0000-000000000001",
    ],
    ids=["malformed", "wrong-version", "wrong-variant"],
)
async def test_invalid_uuid_on_sealed_subject_rejects_without_provider(identity: str) -> None:
    sink = MemoryIngestionSink(
        IngestionTarget("append", "pulse-v1", "device-42-v1", "pulse-v1", partition="event_id_uuid7")
    )
    original = record(sink, identity, subject="patient-42")
    first = await sink.commit([original])
    assert first.outcomes == ("rejected",)
    assert list(await sink.pending()) == [original.source]
    assert (await sink.commit([original])).rejections == first.rejections


async def test_memory_floor_preserves_equal_boundary_and_expires_older_replays() -> None:
    sink = MemoryIngestionSink(
        IngestionTarget("append", "pulse-v1", "device-42-v1", "pulse-v1", partition="event_id_uuid7")
    )
    original = record(sink, "00000000-0001-7000-8000-000000000001")
    assert await sink.advance_horizon(1000) == 1000
    assert (await sink.commit([original])).outcomes == ("accepted",)
    stored = await sink.get("clinic-a", original.event_id)
    assert stored is not None and stored["private"]["pulse_bpm"] == 72
    assert await sink.advance_horizon(1001) == 1001
    assert await sink.advance_horizon(0) == 1001
    assert (await sink.commit([original])).outcomes == ("expired",)
    assert await sink.get("clinic-a", original.event_id) == stored
    assert await sink.pending() == {}

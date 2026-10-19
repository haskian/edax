"""Expired originals settle the Kafka watermark without publishing a rejection."""

from typing import Any

import pytest
from conftest import build_message
from edax.backend.kafka_ingest import KafkaIngestion
from edax.delivery import DeadLetter
from edax.errors import KeyUnavailable, Rejected
from edax.ingestion import IngestionTarget, MappedRow, MemoryIngestionSink
from edax.message import EventMessage
from edax.sealing import KeyScope, KeyStatus, MemoryKeys
from test_kafka_ingest import TP, BatchClient, EventIds, no_dead, records, running, settings, until
from test_kafka_ingest_sealing import sealed


async def test_expired_batch_advances_source_watermark_without_dead_letter() -> None:
    event = build_message(
        "session-7",
        "PulseMeasured",
        1,
        id="00000000-0001-7000-8000-000000000001",
        data=b'{"heart_rate":72}',
        tenant="clinic",
    )
    target = IngestionTarget(
        "append", "pulse-v1", "sensor-a", "pulse-v1", at_rest="plaintext", partition="event_id_uuid7"
    )
    sink = MemoryIngestionSink(target)
    await sink.advance_horizon(2000)
    client = BatchClient(records([event]))
    runner = KafkaIngestion(
        client,
        sink,
        lambda message: MappedRow(message.id, {"heart_rate": 72}),
        no_dead,
        settings=settings(),
        serializer=EventIds([event]),
    )
    async with running(runner):
        await until(lambda: bool(client.commits) and client.commits[-1].get(TP) == 1)
    assert await sink.get("clinic", event.id) is None
    assert await sink.pending() == {}


@pytest.mark.parametrize("destroyed", [False, True], ids=["unavailable", "destroyed"])
async def test_invalid_sealed_event_rejects_before_provider_access(destroyed: bool) -> None:
    class ObservedKeys(MemoryKeys):
        unavailable = False
        reads = 0

        async def status(self, scope: KeyScope) -> KeyStatus:
            self.reads += 1
            if self.unavailable:
                raise KeyUnavailable("provider is unavailable")
            return await super().status(scope)

    keys = ObservedKeys()
    sealing, messages = await sealed(keys)
    assert messages[0].id == "e1" and messages[0].encryption is not None
    if destroyed:
        scope = sealing.scope("acme", "person-42")
        await keys.revoke(scope)
        await keys.destroy(scope)
    else:
        keys.unavailable = True
    keys.reads = 0
    sink = MemoryIngestionSink(
        IngestionTarget("append", "pulse-v1", "sensor-a", "pulse-v1", partition="event_id_uuid7")
    )
    client = BatchClient(records(messages))
    letters: list[DeadLetter[Any]] = []
    mapped: list[str] = []

    def mapper(event: EventMessage) -> MappedRow:
        mapped.append(event.id)
        return MappedRow(event.id, {"email": "ana@example.com"})

    async def publish(letter: DeadLetter[Any]) -> None:
        pending = await sink.pending()
        assert len(pending) == 1 and not next(iter(pending.values()))["published"]
        assert client.commits == []
        letters.append(letter)

    runner = KafkaIngestion(
        client, sink, mapper, publish, settings=settings(), serializer=EventIds(messages), sealing=sealing
    )
    async with running(runner):
        await until(lambda: client.commits == [{TP: 1}])
    assert keys.reads == 0 and mapped == []
    assert len(letters) == 1 and isinstance(letters[0].failure, Rejected)
    assert await sink.pending() == {}
    assert await sink.get("acme", messages[0].id, "person-42") is None

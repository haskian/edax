"""Durable ingestion provenance survives the shipped Kafka dead-letter publication path."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import replace

import pytest
from conftest import SpyProducerClient
from edax.backend.kafka import KafkaDeadLetterSink
from edax.backend.kafka_ingest import KafkaIngestion
from edax.errors import CodecError
from edax.ingestion import MappedRow, MemoryIngestionSink
from edax.message import EventMessage
from test_kafka_ingest import BatchClient, EventIds, events, records, settings, target


@pytest.mark.parametrize("representation", ["full", "oversized", "destination_stub"])
async def test_rejection_publication_keeps_full_source_identity(representation: str) -> None:
    class Producer(SpyProducerClient):
        async def send_and_wait(
            self, topic: str, value: bytes, key: bytes, partition: int, headers: Sequence[tuple[str, bytes]]
        ) -> None:
            if representation == "destination_stub" and value:
                raise CodecError("dead-letter destination refuses the full value")
            await super().send_and_wait(topic, value, key, partition, headers)

    def reject(event: EventMessage) -> MappedRow:
        raise CodecError("order state cannot be projected")

    messages = events(1)
    original = records(messages)[0]._replace(
        headers=[
            ("trace-id", b"order-trace-42"),
            ("edax-ingest-source", b"forged-source"),
            ("edax-ingest-sha256", b"forged-digest"),
            ("edax-ingest-oversized", b"true"),
        ]
    )
    if representation == "oversized":
        original = original._replace(value=b"order-state" * 4096)
    producer = Producer()
    source_keys: list[str] = []
    digests: list[str] = []
    for incarnation in ("original", "replacement"):
        sink = MemoryIngestionSink(
            replace(target(), generation=f"orders-{incarnation}", source_incarnation=f"cluster-{incarnation}")
        )
        runner = KafkaIngestion(
            BatchClient([]),
            sink,
            reject,
            KafkaDeadLetterSink(producer, "orders.dlq"),
            settings=settings(),
            serializer=EventIds(messages),
        )
        item = runner._prepare(original)
        source_keys.append(item.source)
        digests.append(json.loads(item.raw)["sha256"])
        assert (await sink.commit([item])).outcomes == ("rejected",)
        assert await runner.publisher.drain() == 1
        assert await runner.publisher.drain() == 0
        assert (await sink.commit([item])).rejections[item.source]["published"] is True

    assert source_keys[0] != source_keys[1]
    assert digests[0] == digests[1]
    assert len(producer.sent) == 2
    for sent, source, digest in zip(producer.sent, source_keys, digests, strict=True):
        topic, value, key, _, headers = sent
        last = dict(headers)
        assert topic == "orders.dlq"
        assert last["edax-ingest-source"] == source.encode()
        assert last["edax-ingest-sha256"] == digest.encode()
        assert last["edax-ingest-oversized"] == (b"true" if representation == "oversized" else b"false")
        assert last["edax-dlq-topic"] == b"orders"
        assert last["edax-dlq-partition"] == b"0"
        assert last["edax-dlq-offset"] == b"0"
        assert value == (original.value if representation == "full" else b"")
        if representation != "oversized":
            assert key == original.key
            assert headers[: len(original.headers)] == original.headers
        if representation == "destination_stub":
            assert last["edax-dlq-dropped"] == str(len(original.value)).encode()

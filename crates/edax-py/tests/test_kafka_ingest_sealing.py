"""Sealed events through the Kafka ingestion runner: one case per key state and failure branch."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest
from conftest import build_message
from edax import _native
from edax._values import replace
from edax.backend.kafka_ingest import KafkaIngestion
from edax.delivery import DeadLetter
from edax.errors import CodecError, KeyMissing, Rejected
from edax.ingestion import BatchInput, BatchResult, MappedRow, MemoryIngestionSink
from edax.message import EventMessage
from edax.sealing import MemoryKeys, Sealing
from edax.testing import drafted
from test_kafka_ingest import TP, BatchClient, EventIds, records, running, settings, target, until

SECRET = b'{"email":"ana@example.com","status":"paid"}'


async def sealed(keys: MemoryKeys, count: int = 1) -> tuple[Sealing, list[EventMessage]]:
    sealing = Sealing(keys, "orders")
    messages = []
    for sequence in range(1, count + 1):
        plain = replace(
            build_message("order-42", "OrderPlaced", sequence, id=f"e{sequence}", data=SECRET, tenant="acme"),
            subject="person-42",
        )
        messages.append(_native.record_event(await sealing.seal_event(drafted(plain)), plain.recorded_at))
    return sealing, messages


def private_row(event: EventMessage) -> MappedRow:
    return MappedRow(event.aggregate_id, private={"data": event.data.decode()}, public={"status": "paid"})


async def dead_letters(letters: list[DeadLetter[Any]]) -> Any:
    async def publish(letter: DeadLetter[Any]) -> None:
        letters.append(letter)

    return publish


def runner(
    sealing: Sealing, messages: Sequence[EventMessage], sink: MemoryIngestionSink, mapper: Any, on_dead: Any
) -> tuple[KafkaIngestion, BatchClient]:
    client = BatchClient(records(messages))
    run = KafkaIngestion(
        client,
        sink,
        mapper,
        on_dead,
        settings=settings(max_records=1),
        serializer=EventIds(messages),
        sealing=sealing,
    )
    return run, client


async def test_sealed_record_maps_its_opened_payload_with_a_keyed_fingerprint() -> None:
    keys = MemoryKeys()
    sealing, messages = await sealed(keys)
    seen: list[bytes] = []

    def mapper(event: EventMessage) -> MappedRow:
        seen.append(event.data)
        return private_row(event)

    sink = MemoryIngestionSink(target())
    run, client = runner(sealing, messages, sink, mapper, await dead_letters([]))
    async with running(run):
        await until(lambda: client.commits == [{TP: 1}])
    assert seen == [SECRET]
    row = await sink.get("acme", "order-42", "person-42")
    assert row is not None and row["public"] == {"status": "paid"}
    assert len(row["fingerprint"]) == 64 and int(row["fingerprint"], 16) >= 0


@pytest.mark.parametrize("destroyed", [True, False], ids=["destroyed", "revoking"])
async def test_erased_subject_commits_nothing_and_revocation_waits(destroyed: bool) -> None:
    keys = MemoryKeys()
    sealing, messages = await sealed(keys)
    scope = sealing.scope("acme", "person-42")
    await keys.revoke(scope)
    if destroyed:
        await keys.destroy(scope)
    sink = MemoryIngestionSink(target())
    run, client = runner(sealing, messages, sink, private_row, await dead_letters([]))
    async with running(run) as task:
        if not destroyed:
            await asyncio.sleep(0.05)
            # Revocation awaiting destruction is contention: no commit, no failure, no dead letter.
            assert client.commits == [] and not task.done()
            await keys.destroy(scope)
        await until(lambda: client.commits == [{TP: 1}])
    assert await sink.get("acme", "order-42", "person-42") is None
    assert await sink.pending() == {}


async def test_unknown_key_stops_the_runner() -> None:
    _, messages = await sealed(MemoryKeys())
    sink = MemoryIngestionSink(target())
    run, client = runner(Sealing(MemoryKeys(), "orders"), messages, sink, private_row, await dead_letters([]))
    async with running(run) as task:
        with pytest.raises(KeyMissing):
            await asyncio.wait_for(asyncio.shield(task), 1)
    assert client.commits == []


async def test_sealed_mapper_failure_keeps_only_its_code() -> None:
    keys = MemoryKeys()
    sealing, messages = await sealed(keys)

    def refuse(event: EventMessage) -> MappedRow:
        raise CodecError(f"bad email in {event.data.decode()}")

    letters: list[DeadLetter[Any]] = []
    sink = MemoryIngestionSink(target())
    run, client = runner(sealing, messages, sink, refuse, await dead_letters(letters))
    async with running(run):
        await until(lambda: client.commits == [{TP: 1}])
    assert len(letters) == 1
    assert str(letters[0].failure) == "sealed record refused"
    assert b"ana@example.com" not in repr(letters[0].failure).encode()


async def test_erasure_racing_the_commit_turns_the_batch_erased() -> None:
    keys = MemoryKeys()
    sealing, messages = await sealed(keys)
    scope = sealing.scope("acme", "person-42")

    class ErasedDuringCommit(MemoryIngestionSink):
        def __init__(self) -> None:
            super().__init__(target())
            self.attempts: list[tuple[bool, ...]] = []

        async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
            self.attempts.append(tuple(item.erased for item in inputs))
            if not any(item.erased for item in inputs) and await keys.status(scope) != "destroyed":
                # The fence refuses a write while cleanup of its scope is under way.
                raise Rejected("protected scope has been revoked")
            return await super().commit(inputs)

    sink = ErasedDuringCommit()
    run, client = runner(sealing, messages, sink, private_row, await dead_letters([]))
    async with running(run):
        await until(lambda: bool(sink.attempts))
        await keys.revoke(scope)
        await keys.destroy(scope)
        await until(lambda: client.commits == [{TP: 1}])
    # Retries before cleanup finishes still hold plaintext; once the key is gone the batch is erased.
    assert sink.attempts[0] == (False,) and sink.attempts[-1] == (True,)
    assert await sink.get("acme", "order-42", "person-42") is None

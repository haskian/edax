"""Sealed events through the Kafka ingestion runner: one case per key state and failure branch."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest
from conftest import build_message
from edax import _json, _native
from edax._values import replace
from edax.backend.kafka_ingest import KafkaIngestion
from edax.delivery import DeadLetter
from edax.errors import CodecError, KeyMissing, Rejected
from edax.ingestion import BatchInput, BatchResult, MappedRow, MemoryIngestionSink
from edax.message import EventMessage
from edax.pacing import Pacing
from edax.sealing import MemoryKeys, Sealing
from edax.serializer import KernelSerializer
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
    # The row binds the HMAC under the subject's fingerprint key, never the unkeyed hash.
    content = KernelSerializer().encode(messages[0]).decode()
    state = _json.encode(
        {"content": content, "private": {"data": SECRET.decode()}, "public": {"status": "paid"}}
    )
    key = await sealing.fingerprint_key("acme", "person-42")
    assert row["fingerprint"] == _native.ingest_keyed_fingerprint(key, state)


@pytest.mark.parametrize("allow_plaintext", [False, True], ids=["refused", "allowed"])
async def test_plaintext_event_under_sealing(allow_plaintext: bool) -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "orders", allow_plaintext=allow_plaintext)
    plain = replace(
        build_message("order-42", "OrderPlaced", 1, id="e1", data=SECRET, tenant="acme"), subject="person-42"
    )
    letters: list[DeadLetter[Any]] = []
    sink = MemoryIngestionSink(target())
    run, client = runner(sealing, [plain], sink, private_row, await dead_letters(letters))
    async with running(run):
        await until(lambda: client.commits == [{TP: 1}])
    row = await sink.get("acme", "order-42", "person-42")
    if allow_plaintext:
        assert letters == [] and row is not None and row["private"] == {"data": SECRET.decode()}
    else:
        # A plaintext event is no sealed record: its refusal keeps the full policy error.
        assert row is None and len(letters) == 1 and "plaintext" in str(letters[0].failure)


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


async def test_revocation_while_the_key_is_read_returns_the_record_to_the_new_owner() -> None:
    entered, release = asyncio.Event(), asyncio.Event()

    class SlowKeys(MemoryKeys):
        async def resolve(self, scope: Any, cipher: Any, key_id: str) -> bytes:
            entered.set()
            await release.wait()
            return await super().resolve(scope, cipher, key_id)

    keys = SlowKeys()
    sealing, messages = await sealed(keys)
    sink = MemoryIngestionSink(target())
    run, client = runner(sealing, messages, sink, private_row, await dead_letters([]))
    await run._assign({TP})
    part = run._parts[TP]
    admitting = asyncio.create_task(run._admit(TP, part, client.logs[TP][0]))
    await entered.wait()
    await run.rebalance_listener().on_partitions_revoked({TP})
    release.set()
    assert await admitting is False
    assert part.items == [] and run.retained_records == 0


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


async def test_revocation_waits_back_off_and_reset_on_progress() -> None:
    keys = MemoryKeys()
    sealing, messages = await sealed(keys)
    scope = sealing.scope("acme", "person-42")
    await keys.revoke(scope)
    steps: list[int] = []

    class Recorded(Pacing):
        def delay(self, step: int) -> float:
            steps.append(step)
            return 0.001

    sink = MemoryIngestionSink(target())
    run, client = runner(sealing, messages, sink, private_row, await dead_letters([]))
    run._pacing = Recorded()
    async with running(run):
        await until(lambda: len(steps) >= 3)
        await keys.destroy(scope)
        await until(lambda: client.commits == [{TP: 1}])
    # Each wait waits longer; progress resets the count.
    assert steps[:3] == [0, 1, 2]
    assert run._parts[TP].contended == 0

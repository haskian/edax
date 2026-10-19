"""Fetch/rebalance/retention interleavings for database-authoritative projection checkpoints."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from aiokafka import TopicPartition
from conftest import build_message
from edax import _json
from edax.backend.kafka_projection import KafkaProjection
from edax.errors import BadHandle, CodecError
from edax.message import EventMessage
from edax.projection import MemoryProjectionStateStore, ProjectionEvent
from edax.serializer import KernelSerializer
from test_projection_state import event, proof, reducer, target

TP = TopicPartition("account-events", 0)


class Client:
    def __init__(self) -> None:
        self.assigned = {TP}
        self.cursor = 0
        self._auto_offset_reset = "none"
        self.records = [
            SimpleNamespace(
                topic=TP.topic,
                partition=0,
                offset=i,
                key=b"account-42",
                value=wire(event(i + 1, f"credit-{i + 1}")),
                headers=[],
                timestamp=0,
                timestamp_type=0,
            )
            for i in range(3)
        ]
        self.seeks: list[int] = []
        self.before_return: Any = None

    def assignment(self) -> set[Any]:
        return self.assigned

    def seek(self, tp: Any, offset: int) -> None:
        self.cursor = offset
        self.seeks.append(offset)

    async def getmany(self, *partitions: Any, **kwargs: Any) -> dict[Any, Any]:
        result = [r for r in self.records if r.offset >= self.cursor][:1]
        if result:
            self.cursor = result[-1].offset + 1
        if self.before_return:
            callback, self.before_return = self.before_return, None
            await callback()
        return {TP: result}


class Source:
    def __init__(self) -> None:
        self.beginning = 0
        self.identity = proof().identity
        self.history = proof().history

    async def verify(self, tp: Any) -> Any:
        return replace(proof(self.beginning), identity=self.identity, history=self.history)


def wire(event: ProjectionEvent) -> bytes:
    """``event`` as the edax record a producer publishes."""
    message = build_message(
        event.aggregate,
        event.event_type,
        event.sequence,
        id=event.event_id,
        version=int(event.schema_version),
        data=_json.encode(event.payload).encode(),
        metadata=dict(event.metadata),
        tenant=event.tenant,
    )
    return KernelSerializer().encode(message)


def decode(message: EventMessage) -> ProjectionEvent:
    return ProjectionEvent(
        message.tenant,
        message.aggregate_id,
        message.id,
        message.sequence or 0,
        message.name,
        str(message.version),
        _json.decode(message.data),
        dict(message.metadata),
    )


async def runner() -> tuple[KafkaProjection, Client, Source, MemoryProjectionStateStore]:
    client, source, store = Client(), Source(), MemoryProjectionStateStore(target())
    await store.initialize()
    run = KafkaProjection(client, store, source, decode, reducer)
    await run.rebalance_listener().on_partitions_assigned({TP})
    return run, client, source, store


async def test_committed_database_cursor_overrides_client_position() -> None:
    run, client, _, store = await runner()
    client.cursor = 99
    await run._partition(TP, run._parts[TP])
    assert client.seeks == [0]
    observed_lease = run._parts[TP].lease
    assert observed_lease is not None
    assert observed_lease.next_offset == 3
    state = await store.get("bank-eu", "account-42")
    assert state and state["public"]["balance_minor"] == 3750
    assert run.retained_records == run.retained_bytes == 0


async def test_retention_during_fetch_stops_even_when_whole_aggregate_missing() -> None:
    run, client, source, store = await runner()

    async def truncate() -> None:
        source.beginning = 1

    client.before_return = truncate
    with pytest.raises(Exception, match="history is missing"):
        await run._partition(TP, run._parts[TP])
    assert await store.get("bank-eu", "account-42") is None
    observed_lease = run._parts[TP].lease
    assert observed_lease is not None
    assert observed_lease.next_offset == 0


async def test_late_fetch_after_reassignment_never_reaches_reducer() -> None:
    run, client, _, store = await runner()
    prior = run._parts[TP]

    async def reassign() -> None:
        listener = run.rebalance_listener()
        await listener.on_partitions_revoked({TP})
        await listener.on_partitions_assigned({TP})

    client.before_return = reassign
    await run._partition(TP, prior)
    assert await store.get("bank-eu", "account-42") is None
    assert run._parts[TP] is not prior
    assert client.seeks == [0]


async def test_snapshot_decoder_output_before_callback_reuses_dictionary() -> None:
    run, _, _, store = await runner()
    shared: dict[str, Any] = {"amount_minor": 0, "currency": "EUR"}

    def mutate(message: Any) -> ProjectionEvent:
        shared["amount_minor"] = message.sequence
        return replace(event(message.sequence, message.id), payload=shared)

    run.decode = mutate
    await run._partition(TP, run._parts[TP])
    saved = await store.get("bank-eu", "account-42")
    assert saved is not None
    assert saved["public"]["balance_minor"] == 6


async def test_topic_incarnation_change_stops_before_seek() -> None:
    run, client, source, _ = await runner()
    source.identity = "same-name-new-topic-uuid"
    with pytest.raises(Exception, match="continuity is unproven"):
        await run._partition(TP, run._parts[TP])
    assert not client.seeks


async def test_foreign_only_fetch_fails_without_advancing_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, client, _, store = await runner()

    async def foreign(*args: Any, **kwargs: Any) -> dict[Any, Any]:
        return {TopicPartition(TP.topic, 1): client.records[:1]}

    monkeypatch.setattr(client, "getmany", foreign)
    with pytest.raises(BadHandle, match="requested partition"):
        await run._partition(TP, run._parts[TP])
    lease = run._parts[TP].lease
    assert lease is not None and lease.next_offset == 0
    assert await store.get("bank-eu", "account-42") is None
    assert run.retained_records == run.retained_bytes == 0
    await run.wait_closed()


async def test_cancellation_suppression_retains_data_until_actual_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, _, _, store = await runner()
    entered, released = asyncio.Event(), asyncio.Event()
    original_commit = store.commit

    async def stubborn(*args: Any, **kwargs: Any) -> Any:
        entered.set()
        try:
            await released.wait()
        except asyncio.CancelledError:
            await released.wait()
        return await original_commit(*args, **kwargs)

    monkeypatch.setattr(store, "commit", stubborn)
    task = asyncio.create_task(run._partition(TP, run._parts[TP]))
    await entered.wait()
    run._stopped.set()
    task.cancel()
    await asyncio.sleep(0)
    assert run.retained_records == 3
    released.set()
    await asyncio.gather(task, return_exceptions=True)
    await run.wait_closed()
    assert await store.get("bank-eu", "account-42") is None
    assert run.retained_records == run.retained_bytes == 0


@pytest.mark.parametrize("case", ["undecodable_bytes", "command_message"])
async def test_records_that_are_no_event_stop_before_any_commit(case: str) -> None:
    from edax.message import CommandMessage
    from edax.testing import command

    run, client, _, store = await runner()
    if case == "undecodable_bytes":
        client.records[0].value = b"not an edax record"
    else:
        message: CommandMessage = command("account-42")
        client.records[0].value = KernelSerializer("json", kind="command").encode(message)
    with pytest.raises(CodecError):
        await run._partition(TP, run._parts[TP])
    lease = run._parts[TP].lease
    assert lease is not None and (await store.recover(lease)).next_offset == 0

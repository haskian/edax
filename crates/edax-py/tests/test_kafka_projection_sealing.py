"""Sealed events through the Kafka projection runner: one case per key state and failure branch."""

from __future__ import annotations

import dataclasses
from types import SimpleNamespace
from typing import Any

import pytest
from conftest import build_message
from edax import _json, _native
from edax._values import replace
from edax.backend.kafka_projection import KafkaProjection
from edax.errors import BadHandle, KeyMissing, KeyRevoking, Rejected
from edax.message import EventMessage
from edax.projection import (
    MemoryProjectionStateStore,
    ProjectionEvent,
    ProjectionInput,
    ProjectionLease,
    ProjectionResult,
    Reducer,
    SourceProof,
)
from edax.sealing import MemoryKeys, Sealing
from edax.serializer import KernelSerializer
from edax.testing import drafted
from test_kafka_projection import TP, Client, Source, decode
from test_projection_state import target

PAYLOAD = {"amount_minor": 1250, "currency": "EUR", "email": "ana@example.com"}


async def sealed_records(keys: MemoryKeys, count: int = 1) -> tuple[Sealing, list[Any]]:
    sealing = Sealing(keys, "accounts")
    records = []
    for sequence in range(1, count + 1):
        plain = replace(
            build_message(
                "account-42",
                "CreditPosted",
                sequence,
                id=f"credit-{sequence}",
                data=_json.encode(PAYLOAD).encode(),
                tenant="bank-eu",
            ),
            subject="person-42",
        )
        message = _native.record_event(await sealing.seal_event(drafted(plain)), plain.recorded_at)
        records.append(
            SimpleNamespace(
                topic=TP.topic,
                partition=0,
                offset=sequence - 1,
                key=b"account-42",
                value=KernelSerializer().encode(message),
                headers=[],
                timestamp=0,
                timestamp_type=0,
            )
        )
    return sealing, records


def balance(seen: list[Any]) -> Reducer:
    def reduce(state: Any, event: ProjectionEvent) -> ProjectionResult:
        seen.append(event.payload)
        return ProjectionResult({"balance_minor": (state or {}).get("balance_minor", 0) + 1250})

    return reduce


async def projection(
    sealing: Sealing, records: list[Any], reducer: Reducer, store: MemoryProjectionStateStore | None = None
) -> tuple[KafkaProjection, MemoryProjectionStateStore]:
    store = store or MemoryProjectionStateStore(target())
    await store.initialize()
    client = Client()
    client.records = records
    run = KafkaProjection(client, store, Source(), decode, reducer, sealing=sealing)
    await run.rebalance_listener().on_partitions_assigned({TP})
    return run, store


async def test_sealed_event_reduces_its_opened_payload_under_its_subject() -> None:
    sealing, records = await sealed_records(MemoryKeys())
    seen: list[Any] = []
    run, store = await projection(sealing, records, balance(seen))
    await run._partition(TP, run._parts[TP])
    assert seen == [PAYLOAD]
    saved = await store.get("bank-eu", "account-42", "person-42")
    assert saved is not None and saved["value"] == {"balance_minor": 1250}
    assert await store.get("bank-eu", "account-42") is None, "state sits under its subject"


async def test_destroyed_key_moves_past_the_event_without_state() -> None:
    keys = MemoryKeys()
    sealing, records = await sealed_records(keys, 2)
    scope = sealing.scope("bank-eu", "person-42")
    await keys.revoke(scope)
    await keys.destroy(scope)
    seen: list[Any] = []
    run, store = await projection(sealing, records, balance(seen))
    await run._partition(TP, run._parts[TP])
    lease = run._parts[TP].lease
    assert lease is not None and lease.next_offset == 2
    assert seen == []
    assert await store.get("bank-eu", "account-42", "person-42") is None


@pytest.mark.parametrize(
    ("case", "error"),
    [("revoking", KeyRevoking), ("unknown_key", KeyMissing), ("identity_changed", BadHandle)],
)
async def test_key_and_identity_failures_stop_before_any_commit(case: str, error: type[Exception]) -> None:
    keys = MemoryKeys()
    sealing, records = await sealed_records(keys)
    decoder = decode
    if case == "revoking":
        await keys.revoke(sealing.scope("bank-eu", "person-42"))
    elif case == "unknown_key":
        sealing = Sealing(MemoryKeys(), "accounts")
    else:

        def decoder(message: EventMessage) -> ProjectionEvent:
            return dataclasses.replace(decode(message), event_id="another-id")

    seen: list[Any] = []
    run, store = await projection(sealing, records, balance(seen))
    run.decode = decoder
    with pytest.raises(error):
        await run._partition(TP, run._parts[TP])
    assert seen == []
    lease = run._parts[TP].lease
    assert lease is not None and (await store.recover(lease)).next_offset == 0


async def test_erasure_racing_the_commit_restages_the_batch_erased() -> None:
    class RacedKeys(MemoryKeys):
        """Cleanup starts between the status read and the fenced write: the status turns revoking."""

        raced = False

        async def status(self, scope: Any) -> Any:
            return "revoking" if self.raced else await super().status(scope)

    keys = RacedKeys()
    sealing, records = await sealed_records(keys)
    scope = sealing.scope("bank-eu", "person-42")

    class RefusedDuringCleanup(MemoryProjectionStateStore):
        def __init__(self) -> None:
            super().__init__(target())
            self.attempts: list[tuple[bool, ...]] = []

        async def commit(
            self,
            lease: ProjectionLease,
            inputs: Any,
            proof: SourceProof,
            reducer: Reducer,
            **options: Any,
        ) -> ProjectionLease:
            items: list[ProjectionInput] = list(inputs)
            self.attempts.append(tuple(item.erased for item in items))
            if not any(item.erased for item in items):
                # The fence refuses the write that lost the race with cleanup.
                keys.raced = True
                raise Rejected("protected scope has been revoked")
            return await super().commit(lease, inputs, proof, reducer, **options)

    seen: list[Any] = []
    run, store = await projection(sealing, records, balance(seen), RefusedDuringCleanup())
    with pytest.raises(KeyRevoking):
        await run._partition(TP, run._parts[TP])
    keys.raced = False
    await keys.revoke(scope)
    await keys.destroy(scope)
    await run._partition(TP, run._parts[TP])
    assert store.attempts == [(False,), (True,)]
    lease = run._parts[TP].lease
    assert lease is not None and lease.next_offset == 1
    assert await store.get("bank-eu", "account-42", "person-42") is None

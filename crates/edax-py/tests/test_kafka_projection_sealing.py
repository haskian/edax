"""Sealed events through the Kafka projection runner: one case per key state and failure branch."""

from __future__ import annotations

import asyncio
import dataclasses
from types import SimpleNamespace
from typing import Any, cast

import pytest
from conftest import build_message
from edax import _json, _native
from edax._values import replace
from edax.backend.kafka_projection import KafkaProjection
from edax.errors import BadHandle, CodecError, KeyMissing, KeyRevoking, Rejected, Shredded, StoreError
from edax.message import EventMessage
from edax.projection import (
    MemoryProjectionStateStore,
    ProjectionEvent,
    ProjectionInput,
    ProjectionIntent,
    ProjectionLease,
    ProjectionResult,
    ProjectionState,
    Reducer,
    SourceProof,
)
from edax.sealing import MemoryKeys, Sealing
from edax.serializer import KernelSerializer
from edax.testing import drafted
from test_kafka_projection import TP, Client, Source, decode
from test_projection_state import initial_cutover, target

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
        return ProjectionResult(
            public={"balance_minor": (state.public if state else {}).get("balance_minor", 0) + 1250}
        )

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
    assert saved is not None and saved["public"] == {"balance_minor": 1250}
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
    refused = RefusedDuringCleanup()
    run, store = await projection(sealing, records, balance(seen), refused)
    with pytest.raises(KeyRevoking):
        await run._partition(TP, run._parts[TP])
    keys.raced = False
    await keys.revoke(scope)
    await keys.destroy(scope)
    await run._partition(TP, run._parts[TP])
    assert refused.attempts == [(False,), (True,)]
    lease = run._parts[TP].lease
    assert lease is not None and lease.next_offset == 1
    assert await store.get("bank-eu", "account-42", "person-42") is None


async def test_unsealed_replay_of_a_sealed_event_conflicts_with_its_keyed_receipt() -> None:
    sealing, records = await sealed_records(MemoryKeys())
    run, store = await projection(sealing, records, balance([]))
    await run._partition(TP, run._parts[TP])
    # The same event id and sequence published unsealed: its plaintext hash cannot match the keyed receipt.
    plain = replace(
        build_message(
            "account-42",
            "CreditPosted",
            1,
            id="credit-1",
            data=_json.encode(PAYLOAD).encode(),
            tenant="bank-eu",
        ),
        subject="person-42",
    )
    lease = run._parts[TP].lease
    assert lease is not None
    replay = ProjectionInput(lease.next_offset, dataclasses.replace(decode(plain), subject="person-42"))
    with pytest.raises(CodecError, match="conflicting projection event identity"):
        await store.commit(lease, [replay], await Source().verify(TP), balance([]))


class ErasingSource(Source):
    """A source whose second proof, the one taken before the commit, runs ``erase`` first."""

    def __init__(self, erase: Any) -> None:
        super().__init__()
        self.erase = erase
        self.proofs = 0

    async def verify(self, tp: Any) -> Any:
        self.proofs += 1
        if self.proofs == 2:
            await self.erase()
        return await super().verify(tp)


@pytest.mark.parametrize("destroyed", [False, True], ids=["revoking", "destroyed"])
async def test_subject_erased_after_staging_never_commits_its_state(destroyed: bool) -> None:
    keys = MemoryKeys()
    sealing, records = await sealed_records(keys)
    scope = sealing.scope("bank-eu", "person-42")

    async def erase() -> None:
        await keys.revoke(scope)
        if destroyed:
            await keys.destroy(scope)

    seen: list[Any] = []
    run, store = await projection(sealing, records, balance(seen))
    run.source = ErasingSource(erase)
    with pytest.raises(KeyRevoking):
        await run._partition(TP, run._parts[TP])
    lease = run._parts[TP].lease
    assert lease is not None and (await store.recover(lease)).next_offset == 0
    if destroyed:
        # The next pass stages the batch again and finds it erased.
        await keys.status(scope)
        run._parts[TP].seek_needed = True
        await run._partition(TP, run._parts[TP])
        lease = run._parts[TP].lease
        assert lease is not None and lease.next_offset == 1
        assert await store.get("bank-eu", "account-42", "person-42") is None


async def test_a_store_failure_with_every_subject_active_keeps_its_own_error() -> None:
    sealing, records = await sealed_records(MemoryKeys())

    class Failing(MemoryProjectionStateStore):
        async def commit(self, *args: Any, **options: Any) -> ProjectionLease:
            raise StoreError("database unreachable")

    run, _ = await projection(sealing, records, balance([]), Failing(target()))
    with pytest.raises(StoreError, match="database unreachable") as caught:
        await run._partition(TP, run._parts[TP])
    assert not isinstance(caught.value, KeyRevoking)


async def test_a_commit_past_its_deadline_holds_the_write_fence_until_it_ends() -> None:
    keys = MemoryKeys()
    sealing, records = await sealed_records(keys)
    scope = sealing.scope("bank-eu", "person-42")
    release = asyncio.Event()

    class Slow(MemoryProjectionStateStore):
        async def commit(self, *args: Any, **options: Any) -> ProjectionLease:
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    continue
            return await super().commit(*args, **options)

    store = Slow(target())
    await store.initialize()
    client = Client()
    client.records = records
    run = KafkaProjection(
        client, store, Source(), decode, balance([]), sealing=sealing, operation_timeout_s=0.02, lease_ms=1000
    )
    await run.rebalance_listener().on_partitions_assigned({TP})
    partition = asyncio.create_task(run._partition(TP, run._parts[TP]))
    await asyncio.sleep(0.05)
    revoking = asyncio.create_task(keys.revoke(scope))
    await asyncio.sleep(0.05)
    # The commit outlived its deadline but may still land, so erasure waits behind its write fence.
    assert not revoking.done() and not partition.done()
    release.set()
    await asyncio.wait_for(revoking, 1)
    # Once the commit ended, erasure ran; the subject is no longer active, so the runner waits.
    with pytest.raises(KeyRevoking):
        await asyncio.wait_for(partition, 1)


async def test_erasure_mid_stream_through_a_sealing_store() -> None:
    """The whole path: the runner opens, the store seals the subject's state and receipt, an erasure
    destroys the key, and the partition moves on without the subject's data."""
    keys = MemoryKeys()
    sealing, records = await sealed_records(keys, 3)

    def statement(state: ProjectionState | None, event: ProjectionEvent) -> ProjectionResult:
        balance = (state.public["balance_minor"] if state else 0) + event.payload["amount_minor"]
        receipt = ProjectionIntent("receipt", "statements", {"email": event.payload["email"]})
        return ProjectionResult(
            private={"email": event.payload["email"]}, public={"balance_minor": balance}, intents=[receipt]
        )

    store = MemoryProjectionStateStore(target(), sealing=sealing)
    run, _ = await projection(sealing, records[:2], statement, store)
    await run._partition(TP, run._parts[TP])
    await store.activate(initial_cutover(store.target))
    saved = await store.get("bank-eu", "account-42", "person-42")
    assert saved is not None and saved["private"] == {"email": PAYLOAD["email"]}
    assert [intent.payload for intent in await store.unpublished(10)] == [{"email": PAYLOAD["email"]}] * 2
    await keys.revoke(sealing.scope("bank-eu", "person-42"))
    await keys.destroy(sealing.scope("bank-eu", "person-42"))
    cast(Client, run.client).records = records
    await run._partition(TP, run._parts[TP])
    lease = run._parts[TP].lease
    assert lease is not None and lease.next_offset == 3, "the erased event still advances the cursor"
    with pytest.raises(Shredded):
        await store.get("bank-eu", "account-42", "person-42")
    assert await store.unpublished(10) == [], "a destroyed subject's receipts are dropped, not delivered"

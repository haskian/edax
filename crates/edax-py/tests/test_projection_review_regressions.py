"""Review regressions: effect ordering/lifecycle, bounded provider work and original-token checks."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

import pytest
from edax import _json, _native
from edax.errors import BadHandle, CodecError, KeyUnavailable, StoreError
from edax.outbox import Relay
from edax.projection import ProjectionInput, ProjectionIntent, ProjectionResult
from edax.sealing import MemoryKeys, Sealing, provider_deadline
from test_projection_at_rest import Store, owned, raw_intents, statement
from test_projection_at_rest import backend as backend
from test_projection_state import event, initial_cutover, proof, target


class ControlledKeys(MemoryKeys):
    def __init__(self) -> None:
        super().__init__()
        self.fail_next = False
        self.hang = False
        self.calls = 0
        self.release = asyncio.Event()

    async def resolve(self, scope: Any, cipher: Any, key_id: str) -> bytes:
        self.calls += 1
        if self.fail_next:
            self.fail_next = False
            raise KeyUnavailable("one failed read")
        if self.hang:
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                await self.release.wait()
        return await super().resolve(scope, cipher, key_id)


async def setup(store: Store, inputs: list[ProjectionInput], reducer: Any = statement) -> None:
    await store.initialize()
    boundary = replace(proof(), end=10_000)
    lease = await store.acquire(0, "review-worker", boundary)
    await store.commit(lease, inputs, boundary, reducer)
    await store.activate(initial_cutover(store.target))


@pytest.mark.parametrize("null_second", [False, True])
async def test_unavailable_head_holds_back_same_aggregate(backend: Any, null_second: bool) -> None:
    keys = ControlledKeys()
    store = backend(target(), Sealing(keys, "accounts"))

    def reduce(_state: Any, value: Any) -> ProjectionResult:
        return ProjectionResult(
            intents=[
                ProjectionIntent(
                    "notify",
                    "mail",
                    None if null_second and value.sequence == 2 else {"sequence": value.sequence},
                )
            ]
        )

    await setup(store, [ProjectionInput(i, owned(i + 1, "person-7")) for i in range(2)], reduce)
    keys.fail_next = True
    published: list[int] = []

    async def publish(row: Any) -> None:
        published.append(row.offset)

    relay = Relay(store, publish)
    assert await relay.drain(2) == 0
    assert await relay.drain(2) == 2
    assert published == [0, 1]


@pytest.mark.parametrize("page", [1, 100])
async def test_blocked_prefix_yields_to_healthy_aggregate(backend: Any, page: int) -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "accounts")
    store = backend(target(), sealing)
    blocked_count = max(2, page)
    inputs = [
        ProjectionInput(i, replace(owned(i + 1, "blocked"), aggregate="account-a", event_id=f"blocked-{i}"))
        for i in range(blocked_count)
    ]
    inputs.append(
        ProjectionInput(
            blocked_count, replace(owned(1, "healthy"), aggregate="account-z", event_id="healthy-0")
        )
    )
    await setup(store, inputs)
    await keys.revoke(sealing.scope("bank-eu", "blocked"))
    published: list[str] = []

    async def publish(row: Any) -> None:
        published.append(row.aggregate_id)

    relay = Relay(store, publish)
    for _ in range(3):
        await relay.drain(page)
    assert published == ["account-z"]
    # Deferred head was not acknowledged: after a later confirmed destruction its effects are removed.
    await keys.destroy(sealing.scope("bank-eu", "blocked"))
    for _ in range(3):
        await relay.drain(page)
    assert await store.unpublished(100) == []


@pytest.mark.parametrize("subject", ["person-7", None])
@pytest.mark.parametrize("destroyed", [False, True])
async def test_null_effect_obeys_subject_and_tenant_lifecycle(
    backend: Any, subject: str | None, destroyed: bool
) -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "accounts")
    store = backend(target(), sealing)
    await setup(
        store,
        [ProjectionInput(0, owned(1, subject))],
        lambda *_: ProjectionResult(intents=[ProjectionIntent("refresh", "cache", None)]),
    )
    scope = sealing.scope("bank-eu", subject)
    await keys.revoke(scope)
    if destroyed:
        await keys.destroy(scope)
    assert await store.unpublished(100) == []


async def test_cancel_suppressing_provider_is_bounded_and_not_duplicated() -> None:
    keys = ControlledKeys()
    sealing = Sealing(keys, "accounts")
    scope = sealing.scope("bank-eu", "person-7")
    reservation = await keys.reserve(scope, sealing.cipher)
    keys.hang = True
    try:
        for _ in range(4):
            view = sealing.for_batch(0.01)
            with pytest.raises(KeyUnavailable):
                await asyncio.wait_for(view.keys.resolve(scope, sealing.cipher, reservation.key_id), 0.2)
        assert keys.calls == 1
        assert len(sealing._calls._pending) == 1
    finally:
        keys.release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
    assert not sealing._calls._pending
    keys.hang = False
    assert (
        await sealing.for_batch(0.01).keys.resolve(scope, sealing.cipher, reservation.key_id)
        == reservation.key
    )


async def test_enclosing_budget_classifies_key_timeout_first() -> None:
    keys = ControlledKeys()
    sealing = Sealing(keys, "accounts")
    scope = sealing.scope("bank-eu", "person-7")
    reservation = await keys.reserve(scope, sealing.cipher)
    keys.hang = True
    try:
        with provider_deadline(0.02), pytest.raises(KeyUnavailable):
            await asyncio.wait_for(
                sealing.for_batch(10).keys.resolve(scope, sealing.cipher, reservation.key_id), 0.02
            )
    finally:
        keys.release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)


@pytest.mark.parametrize("field", ["payload", "metadata"])
def test_sealed_event_checks_original_wide_integer(field: str) -> None:
    value = _json.decode(_json.encode(event(1, "credit-1")))
    value[field] = {"amount": 10**21 + 1}
    envelope = {
        key: value[key]
        for key in ("tenant", "aggregate", "event_id", "sequence", "event_type", "schema_version")
    }
    envelope["subject"] = "person-7"
    text = _json.encode(value)
    with pytest.raises(CodecError, match="integer"):
        _native.projection_sealed_input(0, _json.encode(envelope), text)
    with pytest.raises(CodecError, match="integer"):
        _native.projection_keyed_fingerprint(bytes(32), text)


def test_projection_abi_contract_validation_and_single_wire_prefix() -> None:
    value = _json.decode(target()._contract)
    value["source_identity"] = ""
    with pytest.raises(BadHandle):
        _native.projection_intent_key(_json.encode(value), "intent")
    envelope = {
        "tenant": None,
        "subject": "person-7",
        "aggregate": "a",
        "event_id": "e",
        "sequence": 1,
        "event_type": "T",
        "schema_version": "1",
    }
    with pytest.raises(CodecError) as error:
        _native.projection_sealed_input(0, _json.encode(envelope), "{")
    assert "codec:" not in str(error.value)


async def test_commit_cursor_comparison_is_kernel_owned(backend: Any) -> None:
    store = backend(target(), None)
    await store.initialize()
    lease = await store.acquire(0, "review-worker", proof())
    stale = replace(lease, next_offset=1)
    with pytest.raises(StoreError, match="cursor moved"):
        await store._execute(op="refresh", lease=stale, lease_ms=30_000, proof=proof(), expected_offset=1)
    assert (await store.recover(lease)).next_offset == 0


async def test_direct_store_get_has_a_provider_deadline(backend: Any) -> None:
    keys = ControlledKeys()
    store = backend(target(), Sealing(keys, "accounts"))
    store.key_timeout_s = 0.01
    await setup(store, [ProjectionInput(0, owned(1, "person-7"))])
    keys.hang = True
    try:
        with pytest.raises(KeyUnavailable):
            await asyncio.wait_for(store.get("bank-eu", "account-42", "person-7"), 0.2)
    finally:
        keys.release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)


async def test_legacy_unsealed_null_effect_still_obeys_scope(backend: Any) -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "accounts")
    store = backend(target(), sealing)
    await setup(
        store,
        [ProjectionInput(0, owned(1, "person-7"))],
        lambda *_: ProjectionResult(intents=[ProjectionIntent("refresh", "cache", None)]),
    )
    record = (await raw_intents(store))[0]
    record["payload"] = None  # record shape written by the previous version
    assert (await store._delivered([record]))[0].payload is None
    scope = sealing.scope("bank-eu", "person-7")
    await keys.revoke(scope)
    assert await store._delivered([record]) == []
    await keys.destroy(scope)
    assert await store._delivered([record]) == []
    assert await store.unpublished(100) == []


async def test_destroyed_legacy_subject_precedes_plaintext_policy() -> None:
    from conftest import build_message
    from edax._values import replace as replace_message

    keys = MemoryKeys()
    sealing = Sealing(keys, "accounts", allow_plaintext=False)
    message = replace_message(build_message("account-42", "Credit", 1, tenant="bank-eu"), subject="person-7")
    with pytest.raises(CodecError, match="plaintext"):
        await sealing.open_event_for_consumer(message)
    scope = sealing.scope(message.tenant, message.subject)
    await keys.revoke(scope)
    await keys.destroy(scope)
    assert await sealing.open_event_for_consumer(message) is None


async def test_host_reducer_failure_is_identical_and_rolls_back(backend: Any) -> None:
    store = backend(target(), None)
    await store.initialize()
    lease = await store.acquire(0, "review-worker", proof())
    original = ValueError("host business input refused")

    def fail(*_: Any) -> ProjectionResult:
        raise original

    with pytest.raises(ValueError) as failed:
        await store.commit(lease, [ProjectionInput(0, event(1, "credit-1"))], proof(), fail)
    assert failed.value is original
    assert (await store.recover(lease)).next_offset == 0
    assert await store.get("bank-eu", "account-42") is None

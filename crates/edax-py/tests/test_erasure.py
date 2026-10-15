"""Crash recovery and participant fencing over the process-local erasure reference store."""

from __future__ import annotations

import asyncio
import json

import edax
import pytest
from edax import _native
from edax.erasure import (
    ErasureAction,
    ErasureCoordinator,
    ErasureOperation,
    MemoryErasureStore,
    StoredErasure,
)
from edax.errors import KeyRevoking, KeyUnavailable, Shredded
from edax.sealing import KeyScope, KeyStatus, MemoryKeys, Sealing


class CheckpointStore(MemoryErasureStore):
    """Lose one reply after the operation's stage has actually landed."""

    def __init__(self, fail_after: str) -> None:
        super().__init__()
        self.fail_after: str | None = fail_after

    def trip(self, action: str) -> None:
        if self.fail_after == action:
            self.fail_after = None
            raise edax.StoreError("checkpoint reply lost")

    async def create(self, candidate: ErasureOperation) -> StoredErasure:
        stored = await super().create(candidate)
        self.trip("create")
        return stored

    async def advance(
        self, previous: StoredErasure, action: ErasureAction, participant: str | None = None
    ) -> StoredErasure:
        stored = await super().advance(previous, action, participant)
        self.trip(action)
        return stored


@pytest.mark.parametrize(
    "checkpoint,stage",
    [
        ("create", "persisted"),
        ("revoke", "revoked"),
        ("notify", "notified"),
        ("acknowledge", "notified"),
        ("destroy", "destruction_confirmed"),
        ("complete", "complete"),
    ],
)
async def test_new_coordinator_recovers_each_persisted_stage_without_losing_notification(
    checkpoint: str, stage: str
) -> None:
    keys = MemoryKeys()
    scope = Sealing(keys, "profiles").scope("tenant-7", "subject-7")
    reservation = await keys.reserve(scope, "aes-256-gcm")
    fingerprint = await keys.fingerprint(scope)
    store = CheckpointStore(checkpoint)
    cleaned: list[KeyScope] = []

    async def cleanup(got: KeyScope) -> None:
        assert got == scope and await keys.status(got) == "revoking"
        assert len(await store.unpublished()) == 1, "notification must land before participant cleanup"
        cleaned.append(got)

    coordinator = ErasureCoordinator(keys, store, {"projection": cleanup})
    with pytest.raises(edax.StoreError, match="checkpoint reply lost"):
        await coordinator.erase(scope, "request-7")
    saved = await store.get(scope, "request-7")
    assert saved is not None and saved.operation.stage == stage
    operation_id = saved.operation.operation_id
    # Restart orchestration, retaining its storage/provider ports. The memory ports themselves
    # intentionally do not claim persistence across process death.
    restarted = ErasureCoordinator(keys, store, {"projection": cleanup})
    complete = await restarted.resume(scope, "request-7")
    assert complete.stage == "complete" and complete.operation_id == operation_id
    assert cleaned == [scope]
    assert await store.pending() == []
    notices = await store.unpublished()
    assert len(notices) == 1 and notices[0].operation_id == operation_id
    with pytest.raises(Shredded):
        await keys.resolve(scope, "aes-256-gcm", reservation.key_id)
    with pytest.raises(Shredded):
        await keys.resolve_fingerprint(scope, fingerprint.key_id)
    assert (await restarted.erase(scope, "request-7")) == complete
    assert await store.unpublished() == notices
    await store.mark_published(operation_id)
    await store.mark_published(operation_id)
    assert await store.unpublished() == []


async def test_offline_required_participant_keeps_keys_revoked_but_not_destroyed() -> None:
    keys = MemoryKeys()
    scope = Sealing(keys, "profiles").scope(None, "subject-7")
    reservation = await keys.reserve(scope, "xchacha20-poly1305")
    store = MemoryErasureStore()
    cleaned: list[str] = []

    async def online(got: KeyScope) -> None:
        cleaned.append("online")

    coordinator = ErasureCoordinator(keys, store, {"online": online, "offline": None})
    waiting = await coordinator.erase(scope, "request-7")
    assert waiting.stage == "notified"
    assert waiting.acknowledgements == {"online"}
    assert waiting.required_participants == {"online", "offline"}
    assert await keys.status(scope) == "revoking"
    with pytest.raises(KeyRevoking):
        await keys.resolve(scope, "xchacha20-poly1305", reservation.key_id)
    # A restarted worker's smaller configuration must not silently drop the frozen participant.
    restarted = ErasureCoordinator(keys, store, {"online": online})
    assert (await restarted.resume(scope, "request-7")).stage == "notified"
    assert cleaned == ["online"]
    with pytest.raises(edax.BadHandle, match="unknown erasure participant"):
        await restarted.acknowledge(scope, "request-7", "unregistered")
    await restarted.acknowledge(scope, "request-7", "offline")
    await restarted.acknowledge(scope, "request-7", "offline")
    assert (await restarted.resume(scope, "request-7")).stage == "complete"
    assert await keys.status(scope) == "destroyed"


class DelayedKeys(MemoryKeys):
    def __init__(self) -> None:
        super().__init__()
        self.confirm_revoke = False
        self.confirm_destroy = False
        self.destroy_calls = 0

    async def revoke(self, scope: KeyScope) -> KeyStatus:
        return await super().revoke(scope) if self.confirm_revoke else "active"

    async def destroy(self, scope: KeyScope) -> KeyStatus:
        self.destroy_calls += 1
        return await super().destroy(scope) if self.confirm_destroy else "revoking"


async def test_requested_provider_effect_is_not_a_confirmed_erasure_stage() -> None:
    keys = DelayedKeys()
    scope = Sealing(keys, "profiles").scope(None, "subject-7")
    await keys.reserve(scope, "aes-256-gcm")
    store = MemoryErasureStore()
    coordinator = ErasureCoordinator(keys, store, {})
    assert (await coordinator.erase(scope, "request-7")).stage == "persisted"
    assert await store.unpublished() == [] and keys.destroy_calls == 0
    keys.confirm_revoke = True
    assert (await coordinator.resume(scope, "request-7")).stage == "notified"
    assert keys.destroy_calls == 1 and await keys.status(scope) == "revoking"
    assert len(await store.pending()) == 1
    keys.confirm_destroy = True
    assert (await coordinator.resume(scope, "request-7")).stage == "complete"
    assert keys.destroy_calls == 2 and await keys.status(scope) == "destroyed"


async def test_failed_callback_leaves_no_acknowledgement_or_private_exception_text() -> None:
    keys = MemoryKeys()
    scope = Sealing(keys, "profiles").scope(None, "subject-7")
    await keys.reserve(scope, "aes-256-gcm")
    store = MemoryErasureStore()
    calls: list[str] = []

    async def first(got: KeyScope) -> None:
        calls.append("first")

    async def broken(got: KeyScope) -> None:
        calls.append("broken")
        raise RuntimeError("private-customer@example.test")

    coordinator = ErasureCoordinator(keys, store, {"a-first": first, "b-broken": broken})
    with pytest.raises(RuntimeError, match="private-customer"):
        await coordinator.erase(scope, "request-7")
    stored = await store.get(scope, "request-7")
    assert stored is not None and stored.operation.stage == "notified"
    assert stored.operation.acknowledgements == {"a-first"}
    assert "private-customer" not in stored.to_json()
    assert all("private-customer" not in row.record for row in await store.unpublished())
    assert await keys.status(scope) == "revoking"

    async def repaired(got: KeyScope) -> None:
        calls.append("repaired")

    restarted = ErasureCoordinator(keys, store, {"a-first": first, "b-broken": repaired})
    assert (await restarted.resume(scope, "request-7")).stage == "complete"
    assert calls == ["first", "broken", "repaired"]


async def test_lost_provider_reply_is_retried_without_rolling_back_the_revocation() -> None:
    class LostRevoke(MemoryKeys):
        fail = True

        async def revoke(self, scope: KeyScope) -> KeyStatus:
            result = await super().revoke(scope)
            if self.fail:
                self.fail = False
                raise KeyUnavailable("provider response lost")
            return result

    keys = LostRevoke()
    scope = Sealing(keys, "profiles").scope(None, "subject-7")
    store = MemoryErasureStore()
    coordinator = ErasureCoordinator(keys, store, {})
    with pytest.raises(KeyUnavailable):
        await coordinator.erase(scope, "request-7")
    operation = await coordinator.status(scope, "request-7")
    assert operation is not None and operation.stage == "persisted"
    assert await keys.status(scope) == "revoking"
    assert (await coordinator.resume(scope, "request-7")).stage == "complete"


async def test_request_deduplication_freezes_participants_and_keeps_scopes_separate() -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles")
    scope = sealing.scope("tenant-7", "subject-7")
    store = MemoryErasureStore()
    coordinator = ErasureCoordinator(keys, store, {"projection": None, "search": None})
    first, second = await asyncio.gather(
        coordinator.erase(scope, "request-7"), coordinator.erase(scope, "request-7")
    )
    assert first.operation_id == second.operation_id
    assert len(await store.pending()) == 1 and len(await store.unpublished()) == 1
    reordered = ErasureCoordinator(keys, store, {"search": None, "projection": None})
    assert (await reordered.erase(scope, "request-7")).operation_id == first.operation_id
    for participants in ({"projection": None}, {"projection": None, "search": None, "archive": None}):
        with pytest.raises(edax.Rejected):
            await ErasureCoordinator(keys, store, participants).erase(scope, "request-7")
    other = await coordinator.erase(sealing.scope("tenant-8", "subject-7"), "request-7")
    assert other.operation_id != first.operation_id
    assert len(await store.pending()) == 2


async def test_tenant_erasure_control_notification_survives_all_payload_keys() -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles")
    tenant = sealing.scope("tenant-7", None)
    scopes = [sealing.scope("tenant-7", subject) for subject in ("subject-1", "subject-2")]
    reservations = [await keys.reserve(scope, "aes-256-gcm") for scope in scopes]
    other = sealing.scope("tenant-8", "subject-1")
    unaffected = await keys.reserve(other, "aes-256-gcm")
    store = MemoryErasureStore()
    coordinator = ErasureCoordinator(keys, store, {})
    operation = await coordinator.erase(tenant, "request-7")
    assert operation.stage == "complete"
    for scope, reservation in zip(scopes, reservations, strict=True):
        with pytest.raises(Shredded):
            await keys.resolve(scope, "aes-256-gcm", reservation.key_id)
    with pytest.raises(Shredded):
        await keys.reserve(sealing.scope("tenant-7", "future-subject"), "xchacha20-poly1305")
    assert await keys.resolve(other, "aes-256-gcm", unaffected.key_id) == bytes(unaffected.key)
    notifications = await store.unpublished()
    assert len(notifications) == 1
    notice = json.loads(notifications[0].record)
    assert notice == {
        "type": "edax.erasure.requested",
        "operation_id": operation.operation_id,
        "scope": {"key_namespace": "profiles", "tenant": "tenant-7", "subject": None},
    }
    # Publication is a control-path operation: it needs no key reservation from the erased tenant.
    await store.mark_published(operation.operation_id)
    assert await store.unpublished() == []


async def test_store_rejects_stale_update_and_destruction_before_cleanup() -> None:
    from edax import _native

    keys = MemoryKeys()
    scope = Sealing(keys, "profiles").scope(None, "subject-7")
    store = MemoryErasureStore()
    initial = await store.create(
        ErasureOperation(_native.erasure_create(scope, "operation-7", "request-7", ["offline"]))
    )
    with pytest.raises(edax.BadHandle):
        await store.advance(initial, "acknowledge", "offline")
    revoked = await store.advance(initial, "revoke")
    with pytest.raises(edax.ConcurrencyError):
        await store.advance(initial, "notify")
    notified = await store.advance(revoked, "notify")
    with pytest.raises(edax.BadHandle):
        await store.advance(notified, "destroy")
    assert (await store.get(scope, "request-7")) == notified
    assert len(await store.unpublished()) == 1


async def test_failed_cleanup_does_not_starve_other_participants() -> None:
    keys, store = MemoryKeys(), MemoryErasureStore()
    scope = Sealing(keys, "profiles").scope("tenant", "person")
    calls: list[str] = []

    async def failing(_: KeyScope) -> None:
        calls.append("a")
        raise edax.StoreError("participant unavailable")

    async def healthy(_: KeyScope) -> None:
        calls.append("b")

    coordinator = ErasureCoordinator(keys, store, {"a": failing, "b": healthy})
    for _ in range(2):
        with pytest.raises(edax.StoreError):
            await coordinator.erase(scope, "request")
    assert calls == ["a", "b", "a"]
    saved = await store.get(scope, "request")
    assert saved is not None and saved.operation.acknowledgements == {"b"}
    assert await keys.status(scope) == "revoking"


async def test_registered_cleanup_cannot_be_silently_excluded() -> None:
    with pytest.raises(edax.BadHandle):
        ErasureCoordinator(MemoryKeys(), MemoryErasureStore(), {"database": None}, required_participants=[])


async def test_pending_cursor_can_reach_later_operations_and_wrap() -> None:
    store = MemoryErasureStore()
    scope = Sealing(MemoryKeys(), "profiles").scope("tenant", "person")
    for id in ["blocked", "later"]:
        await store.create(ErasureOperation(_native.erasure_create(scope, id, id, [])))
    first = await store.pending(1)
    second = await store.pending(1, after=first[0].operation.operation_id)
    assert second[0].operation.operation_id == "later"
    assert await store.pending(1, after="later") == []
    await store.create(ErasureOperation(_native.erasure_create(scope, "a-new", "new", [])))
    assert (await store.pending(1))[0].operation.operation_id == "a-new"

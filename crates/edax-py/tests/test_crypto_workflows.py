"""Protected workflows retain ciphertext at rest and fence erasure through commit."""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any, ClassVar

import edax
import pytest
from conftest import NO_WAIT, CreateUser, build_message
from edax import _native
from edax._async import maybe_await
from edax._values import replace
from edax.backend.memory import (
    MemoryCommandOutbox,
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemorySagaStore,
)
from edax.erasure import ErasureCoordinator, MemoryErasureStore
from edax.errors import KeyMissing, KeyRevoking, KeyUnauthorized, Shredded
from edax.idempotency import Done, Record, encode_record
from edax.saga import ProtectedSagaState
from edax.sealing import Cipher, KeyScope, MemoryKeys, Sealing
from edax.testing.faults import LostResponse
from edax.unit_of_work import Tx, transaction

EMAIL = "private-customer@example.test"
SUBJECT = "customer-7"
EVENTS = edax.json_codec({"EmailChanged": CreateUser})
COMMANDS = edax.json_codec({CreateUser.type: CreateUser})


class Profile(edax.Aggregate):
    def __init__(self, id: str, initial: int = 0) -> None:
        super().__init__(id, initial)
        self.email = ""

    def transition(self, event: CreateUser) -> None:
        self.email = event.email


class Profiles:
    type = "Profile"

    def load(self, id: str, version: int, data: bytes | None) -> Profile:
        profile = Profile(id, version)
        profile.email = "" if data is None else json.loads(data)["email"]
        return profile

    def dump(self, profile: Profile) -> bytes:
        return json.dumps({"email": profile.email}).encode()


def repository(store: Any, sealing: Sealing | None, **options: Any) -> edax.Repository[Profile, Any]:
    return edax.Repository(store, EVENTS, Profiles(), sealing=sealing, **options)


def private_bytes_absent(data: bytes) -> None:
    assert EMAIL.encode() not in data
    assert base64.b64encode(EMAIL.encode()) not in data


async def event(sealing: Sealing, *, subject: str = SUBJECT, id: str = "profile-1") -> edax.EventMessage:
    store = MemoryEventStore()
    repo = repository(store, sealing)
    profile = await repo.load(id, subject=subject)
    profile.apply(CreateUser(EMAIL))
    return (await repo.save(profile))[0]


@pytest.mark.parametrize("cipher", ["aes-256-gcm", "xchacha20-poly1305"])
@pytest.mark.parametrize("snapshot_every", [0, 1])
@pytest.mark.parametrize("origin", ["load", "create"])
async def test_repository_encrypts_events_snapshots_outboxes_and_both_wire_formats(
    cipher: Cipher, snapshot_every: int, origin: str
) -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles", cipher=cipher)
    store = MemoryEventStore()
    outbox = MemoryCommandOutbox(store.transactor)
    repo = repository(store, sealing, commands=outbox, snapshot_every=snapshot_every)
    if origin == "create":
        profile = repo.create("profile-1")
        profile.subject = SUBJECT
    else:
        profile = await repo.load("profile-1", subject=SUBJECT)
    profile.apply(CreateUser(EMAIL))
    command = edax.command_message(CreateUser(EMAIL), codec=COMMANDS)
    rows = await repo.save(profile, commands=[command])
    assert await store.load(None, profile.id) == rows
    assert await store.unpublished(100) == rows
    sent = await outbox.unpublished(100)
    assert len(sent) == 1
    for row in [*rows, *sent]:
        assert row.subject == SUBJECT and row.encryption is not None
        private_bytes_absent(bytes(row.data))
        for wire_format in ("json", "msgpack"):
            if isinstance(row, edax.EventMessage):
                wire = bytes(_native.encode_event(wire_format, row))
                assert _native.decode_event(wire_format, wire) == row
            else:
                wire = bytes(_native.encode_command(wire_format, row))
                assert _native.decode_command(wire_format, wire) == row
            private_bytes_absent(wire)
    snapshot = await store.load_snapshot(None, profile.id)
    if snapshot_every:
        assert snapshot is not None and snapshot.encryption is not None
        assert snapshot.subject == SUBJECT
        private_bytes_absent(snapshot.data)
    else:
        assert snapshot is None
    loaded = await repo.load(profile.id, subject=SUBJECT)
    assert loaded.email == EMAIL and loaded.version.initial == 1 and loaded.subject == SUBJECT
    await keys.revoke(sealing.scope(None, SUBJECT))
    await keys.destroy(sealing.scope(None, SUBJECT))
    with pytest.raises(Shredded):
        await repo.load(profile.id)
    with pytest.raises(Shredded):
        await sealing.open_command(sent[0])


async def test_committed_repository_owner_cannot_change_or_be_cleared() -> None:
    store = MemoryEventStore()
    repo = repository(store, Sealing(MemoryKeys(), "profiles"))
    profile = await repo.load("profile-1", subject=SUBJECT)
    profile.apply(CreateUser(EMAIL))
    original = await repo.save(profile)
    with pytest.raises(KeyUnauthorized):
        await repo.load(profile.id, subject="different-customer")
    for owner in (None, "different-customer"):
        loaded = await repo.load(profile.id)
        loaded.subject = owner
        loaded.apply(CreateUser("other@example.test"))
        with pytest.raises(edax.CodecError):
            await repo.save(loaded)
    assert await store.load(None, profile.id) == original


@pytest.mark.parametrize("snapshot_every", [0, 1])
async def test_plaintext_repository_requires_explicit_migration_opt_in(snapshot_every: int) -> None:
    store = MemoryEventStore()
    legacy = repository(store, None, snapshot_every=snapshot_every)
    profile = await legacy.load("profile-1")
    profile.apply(CreateUser(EMAIL))
    await legacy.save(profile)
    keys = MemoryKeys()
    with pytest.raises(edax.CodecError):
        await repository(store, Sealing(keys, "profiles")).load(profile.id)
    migrated = await repository(store, Sealing(keys, "profiles", allow_plaintext=True)).load(profile.id)
    assert migrated.email == EMAIL


class RecordedWrites(MemoryEventStore):
    def __init__(self) -> None:
        super().__init__()
        self.attempts: list[list[edax.EventDraft]] = []
        self.snapshots: list[edax.Snapshot] = []
        self.fail_commit = False

    async def append(
        self, tx: Tx, aggregate_id: str, expected_version: int, drafts: Sequence[edax.EventDraft]
    ) -> list[edax.EventMessage]:
        self.attempts.append(list(drafts))
        return await super().append(tx, aggregate_id, expected_version, drafts)

    async def save_snapshot(self, tx: Tx, snapshot: edax.Snapshot) -> None:
        self.snapshots.append(snapshot)
        await super().save_snapshot(tx, snapshot)

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[Any]:
        async with super().begin() as tx:
            yield tx
            if self.fail_commit:
                self.fail_commit = False
                raise edax.StoreError("commit did not land")


@pytest.mark.parametrize("landed", [False, True], ids=["rolled-back", "reply-lost-after-commit"])
async def test_prepared_save_reuses_ciphertext_and_does_not_duplicate_commands(landed: bool) -> None:
    real = RecordedWrites()
    fault = LostResponse(real)
    outbox = MemoryCommandOutbox(real.transactor)
    repo = repository(fault, Sealing(MemoryKeys(), "profiles"), commands=outbox)
    profile = await repo.load("profile-1", subject=SUBJECT)
    profile.apply(CreateUser(EMAIL))
    command = edax.command_message(CreateUser(EMAIL), codec=COMMANDS)
    if landed:
        fault.arm("commit")
    else:
        real.fail_commit = True
    with pytest.raises(edax.StoreError):
        await repo.save(profile, commands=[command])
    assert len(await real.load(None, profile.id)) == int(landed)
    assert len(await outbox.unpublished(100)) == int(landed)
    with pytest.raises(edax.BadHandle):
        profile.apply(CreateUser("mutated@example.test"))
    stored = await repo.save(profile, commands=[command])
    assert len(stored) == 1 and profile.version.initial == 1 and not profile.pending
    assert await real.load(None, profile.id) == stored
    assert len(await outbox.unpublished(100)) == 1
    assert len(real.attempts) == (1 if landed else 2)
    assert all(attempt == real.attempts[0] for attempt in real.attempts)
    assert all(snapshot == real.snapshots[0] for snapshot in real.snapshots)
    assert await repo.save(profile) == []


async def test_router_authenticates_before_handler_and_only_skips_confirmed_destruction() -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles")
    raw = await event(sealing)
    seen: list[edax.EventMessage] = []
    router = edax.EventRouter(sealing=sealing)

    async def handle(msg: edax.EventMessage) -> None:
        seen.append(msg)

    router.register(raw.name, handle)
    await router.dispatch(raw)
    assert len(seen) == 1 and EMAIL.encode() in bytes(seen[0].data)
    assert seen[0].encryption is None
    with pytest.raises(edax.CodecError):
        await router.dispatch(replace(raw, data=bytes(raw.data)[:-1] + bytes([raw.data[-1] ^ 1])))
    with pytest.raises(edax.CodecError):
        await router.dispatch(replace(raw, encryption=None, data=b"{}"))
    unknown = edax.EventRouter(sealing=Sealing(MemoryKeys(), "profiles"))
    unknown.register(raw.name, handle)
    with pytest.raises(KeyMissing):
        await unknown.dispatch(raw)
    await keys.revoke(sealing.scope(None, SUBJECT))
    with pytest.raises(KeyRevoking):
        await router.dispatch(raw)
    await keys.destroy(sealing.scope(None, SUBJECT))
    await router.dispatch(raw)
    assert len(seen) == 1


async def test_handler_shredded_error_is_never_treated_as_a_consumer_skip() -> None:
    class Confirming(MemoryKeys):
        """Reports the scope destroyed once the handler runs: even the provider's word skips nothing here."""

        destroyed = False

        async def status(self, scope: KeyScope) -> Any:
            return "destroyed" if self.destroyed else await super().status(scope)

    keys = Confirming()
    sealing = Sealing(keys, "profiles")
    raw = await event(sealing)
    router = edax.EventRouter(sealing=sealing)

    async def fail(msg: edax.EventMessage) -> None:
        keys.destroyed = True
        raise Shredded("handler failure")

    router.register(raw.name, fail)
    with pytest.raises(Shredded, match="handler failure"):
        await router.dispatch(raw)


async def test_protected_event_failure_ledger_does_not_retain_decrypted_error_text() -> None:
    sealing = Sealing(MemoryKeys(), "profiles")
    raw = await event(sealing)
    ledger = MemoryIdempotencyStore()
    router = edax.EventRouter(
        edax.idempotent_by_id(ledger, scope="projection", tenancy=edax.Tenancy.SILO),
        sealing=sealing,
    )
    handled = 0

    async def fail(msg: edax.EventMessage) -> None:
        nonlocal handled
        handled += 1
        raise edax.Rejected(json.loads(bytes(msg.data))["email"])

    router.register(raw.name, fail)
    with pytest.raises(edax.Rejected):
        await router.dispatch(raw)
    rule = _native.event_idempotency(raw, "silo", "projection")
    stored = await ledger.claim(rule.key, rule.payload_hash)
    assert isinstance(stored, Record)
    private_bytes_absent(encode_record(stored).encode())
    with pytest.raises(edax.CachedFailure) as caught:
        await router.dispatch(raw)
    assert caught.value.original_code == "rejected"
    assert EMAIL not in str(caught.value)
    assert handled == 1


async def test_dispatcher_started_in_transaction_drops_inherited_scope_fence_resources() -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles")
    store = MemoryEventStore()
    repo = repository(store, sealing)
    settled: list[str] = []
    finished = asyncio.Event()

    def settle(outcome: str) -> None:
        settled.append(outcome)
        finished.set()

    async def handle(msg: edax.EventMessage) -> None:
        profile = await repo.load("profile-1", subject=SUBJECT)
        profile.apply(CreateUser(EMAIL))
        await repo.save(profile)

    dispatcher = edax.KeyOrderedDispatcher(1, handle, pacing=NO_WAIT)
    async with transaction(store):
        dispatcher.start()
    try:
        await dispatcher.submit(
            edax.Delivery(
                build_message("source-1", "Changed", 1),
                lambda: settle("ack"),
                lambda: settle("nack"),
            )
        )
        await asyncio.wait_for(finished.wait(), 2)
        assert settled == ["ack"]
        assert len(await store.load(None, "profile-1")) == 1
        # A closed parent exit stack cannot own the worker's fence. This used to time out
        # forever after the worker saved successfully into its own transaction.
        assert await asyncio.wait_for(keys.revoke(sealing.scope(None, SUBJECT)), 1) == "revoking"
    finally:
        await dispatcher.stop()


async def test_bus_replay_stores_only_protected_result_and_refuses_changed_intent() -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles")
    ledger = MemoryIdempotencyStore()
    bus = edax.CommandBus(
        edax.idempotent(ledger, scope="api", tenancy=edax.Tenancy.SILO), codec=COMMANDS, sealing=sealing
    )
    messages: list[edax.CommandMessage] = []

    async def handle(cmd: CreateUser) -> dict[str, str]:
        current = edax.current_command()
        assert current is not None
        messages.append(current)
        return {"email": cmd.email}

    bus.register(CreateUser, handle)
    for _ in range(2):
        assert await bus.dispatch(CreateUser(EMAIL), idempotency_key="request-7", subject=SUBJECT) == {
            "email": EMAIL
        }
    assert len(messages) == 1 and messages[0].encryption is not None
    private_bytes_absent(bytes(messages[0].data))
    claim = edax.claim_key("api", "request-7", tenancy=edax.Tenancy.SILO)
    stored = await ledger.claim(_native.protected_claim_key(sealing.scope(None, SUBJECT), claim), 0)
    assert isinstance(stored, Record) and isinstance(stored.state, Done)
    private_bytes_absent(encode_record(stored).encode())
    assert stored.payload_hash == 0
    with pytest.raises(edax.PayloadMismatch):
        await bus.dispatch(CreateUser("different@example.test"), idempotency_key="request-7", subject=SUBJECT)
    await keys.revoke(sealing.scope(None, SUBJECT))
    await keys.destroy(sealing.scope(None, SUBJECT))
    with pytest.raises(Shredded):
        await bus.dispatch_message(messages[0])
    assert await bus.deliver_message(messages[0]) is None
    assert len(messages) == 1


@pytest.mark.parametrize("allow_plaintext", [False, True])
async def test_bus_plaintext_migration_is_explicit(allow_plaintext: bool) -> None:
    sealing = Sealing(MemoryKeys(), "profiles", allow_plaintext=allow_plaintext)
    bus = edax.CommandBus(codec=COMMANDS, sealing=sealing)
    handled: list[CreateUser] = []

    async def handle(cmd: CreateUser) -> None:
        handled.append(cmd)

    bus.register(CreateUser, handle)
    legacy = edax.command_message(CreateUser(EMAIL), codec=COMMANDS, subject=SUBJECT)
    if allow_plaintext:
        await bus.dispatch_message(legacy)
        assert handled == [CreateUser(EMAIL)]
    else:
        with pytest.raises(edax.CodecError):
            await bus.dispatch_message(legacy)
        assert handled == []


class ProfileSaga:
    name: ClassVar[str] = "profile-notification"

    def __init__(self) -> None:
        self.folds = 0

    def instance_id(self, msg: edax.EventMessage) -> str:
        return "workflow-7"

    def handle(self, state: Any, msg: edax.EventMessage) -> tuple[Any, list[CreateUser]]:
        self.folds += 1
        email = json.loads(bytes(msg.data))["email"]
        if state is not None:
            assert state == {"email": EMAIL}
        return {"email": email}, [CreateUser(email)]


async def test_router_saga_bus_roundtrip_keeps_state_and_outbox_encrypted_and_owner_fixed() -> None:
    sealing = Sealing(MemoryKeys(), "profiles")
    store = MemorySagaStore()
    saga = ProfileSaga()
    runner = edax.SagaRunner(saga, store, COMMANDS, sealing=sealing)
    router = edax.EventRouter(sealing=sealing)
    router.register("EmailChanged", runner.on_event)
    raw = await event(sealing)
    await router.dispatch(raw)
    await router.dispatch(raw)
    assert saga.folds == 1
    saved = await store.load(saga.name, "workflow-7")
    assert isinstance(saved, edax.RunningInstance) and isinstance(saved.state, ProtectedSagaState)
    private_bytes_absent(bytes(saved.state.data))
    assert saved.state.subject == SUBJECT
    outbox = await store.unpublished(saga.name, 100)
    assert len(outbox) == 1 and outbox[0].encryption is not None and outbox[0].subject == SUBJECT
    private_bytes_absent(bytes(outbox[0].data))
    received: list[CreateUser] = []

    async def handle(cmd: CreateUser) -> None:
        received.append(cmd)

    bus = edax.CommandBus(codec=COMMANDS, sealing=sealing)
    bus.register(CreateUser, handle)
    await maybe_await(runner.publish_to(bus)(outbox[0]))
    assert received == [CreateUser(EMAIL)]
    other = await event(sealing, subject="different-customer", id="other-profile")
    with pytest.raises(edax.CodecError):
        await router.dispatch(other)
    assert saga.folds == 1
    assert await store.load(saga.name, "workflow-7") == saved
    # A fresh event of the same owner must decrypt and fold the existing protected state.
    await router.dispatch(await event(sealing, id="another-profile"))
    assert saga.folds == 2


async def test_saga_relay_settles_destroyed_commands_without_a_failure_sink() -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles")
    runner = edax.SagaRunner(ProfileSaga(), MemorySagaStore(), COMMANDS, sealing=sealing)
    await runner.on_event(await event(sealing))
    bus = edax.CommandBus(codec=COMMANDS, sealing=sealing)

    async def handle(cmd: CreateUser) -> None:
        pytest.fail("a destroyed command must never reach its handler")

    bus.register(CreateUser, handle)
    await keys.revoke(sealing.scope(None, SUBJECT))
    await keys.destroy(sealing.scope(None, SUBJECT))
    relay = edax.Relay(runner.outbox(), runner.publish_to(bus), pacing=NO_WAIT)
    assert await relay.drain() == 1
    assert await runner.outbox().unpublished(100) == []
    assert relay.last is not None and relay.last.dead_lettered == 0


@pytest.mark.parametrize("allow_plaintext", [False, True])
async def test_saga_plaintext_state_requires_migration_opt_in(allow_plaintext: bool) -> None:
    sealing = Sealing(MemoryKeys(), "profiles", allow_plaintext=allow_plaintext)
    store = MemorySagaStore()
    saga = ProfileSaga()
    await store.save(saga.name, "workflow-7", {"email": EMAIL}, 0, "", [])
    runner = edax.SagaRunner(saga, store, COMMANDS, sealing=sealing)
    raw = await event(sealing)
    if allow_plaintext:
        await runner.on_event(raw)
        saved = await store.load(saga.name, "workflow-7")
        assert isinstance(saved, edax.RunningInstance)
        assert isinstance(saved.state, ProtectedSagaState)
        private_bytes_absent(bytes(saved.state.data))
        assert saga.folds == 1
    else:
        with pytest.raises(edax.CodecError):
            await runner.on_event(raw)
        assert saga.folds == 0


@pytest.mark.parametrize("cancelled", [False, True], ids=["committed", "cancelled-and-rolled-back"])
async def test_ambient_transaction_holds_writer_fence_until_commit_before_cleanup(cancelled: bool) -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles")
    store = MemoryEventStore()
    repo = repository(store, sealing)
    staged = asyncio.Event()
    allow_commit = asyncio.Event()
    revoking = asyncio.Event()
    cleaned: list[int] = []

    async def writer() -> None:
        async with transaction(store):
            profile = await repo.load("profile-1", subject=SUBJECT)
            profile.apply(CreateUser(EMAIL))
            await repo.save(profile)
            staged.set()
            await allow_commit.wait()

    async def cleanup(scope: KeyScope) -> None:
        assert scope == sealing.scope(None, SUBJECT)
        assert await keys.status(scope) == "revoking"
        cleaned.append(len(await store.load(None, "profile-1")))

    coordinator = ErasureCoordinator(keys, MemoryErasureStore(), {"projection": cleanup})

    async def erase() -> Any:
        revoking.set()
        return await coordinator.erase(sealing.scope(None, SUBJECT), "erase-7")

    writer_task = asyncio.create_task(writer())
    erase_task: asyncio.Task[Any] | None = None
    try:
        await asyncio.wait_for(staged.wait(), 2)
        assert await store.load(None, "profile-1") == []
        erase_task = asyncio.create_task(erase())
        await asyncio.wait_for(revoking.wait(), 2)
        await asyncio.sleep(0)
        assert not erase_task.done() and cleaned == []
        assert await keys.status(sealing.scope(None, SUBJECT)) == "active"
        if cancelled:
            writer_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(writer_task, 2)
        else:
            allow_commit.set()
            await asyncio.wait_for(writer_task, 2)
        assert (await asyncio.wait_for(erase_task, 2)).stage == "complete"
        assert cleaned == [0 if cancelled else 1], "cleanup must see the final transaction outcome"
        with pytest.raises(Shredded):
            await keys.reserve(sealing.scope(None, SUBJECT), "aes-256-gcm")
        if not cancelled:
            with pytest.raises(Shredded):
                await repo.load("profile-1")
    finally:
        allow_commit.set()
        for task in (writer_task, erase_task):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (writer_task, erase_task) if task is not None), return_exceptions=True
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"metadata": {"forged": "value"}},
        {"extensions": {"traceparent": "forged"}},
        {"correlation_id": "another-flow"},
        {"name": "UnregisteredForgedName"},
        {"version": 99},
    ],
)
async def test_event_dedup_replay_authenticates_context_before_returning_cached_success(
    changes: dict[str, Any],
) -> None:
    sealing = Sealing(MemoryKeys(), "profiles")
    raw = await event(sealing)
    ledger = MemoryIdempotencyStore()
    router = edax.EventRouter(
        edax.idempotent_by_id(ledger, scope="projection", tenancy=edax.Tenancy.SILO), sealing=sealing
    )
    seen: list[bytes] = []

    async def handle(message: edax.EventMessage) -> None:
        seen.append(bytes(message.data))

    router.register(raw.name, handle)
    await router.dispatch(raw)
    await router.dispatch(raw)
    assert len(seen) == 1
    # Same ID and ciphertext used to reach the successful ledger record before authentication.
    with pytest.raises(edax.CodecError):
        await router.dispatch(replace(raw, **changes))
    await router.dispatch(raw)
    # Store-assigned recorded_at is intentionally outside the producer-authenticated context.
    await router.dispatch(replace(raw, recorded_at=raw.recorded_at + 1))
    assert len(seen) == 1


async def test_completed_saga_keeps_authenticated_owner_and_no_application_state() -> None:
    class CompletingSaga(ProfileSaga):
        def handle(self, state: Any, msg: edax.EventMessage) -> tuple[Any, list[CreateUser]]:
            if state is None:
                return super().handle(state, msg)
            self.folds += 1
            assert state == {"email": EMAIL}
            return edax.Completed, []

    sealing = Sealing(MemoryKeys(), "profiles")
    store = MemorySagaStore()
    saga = CompletingSaga()
    runner = edax.SagaRunner(saga, store, COMMANDS, sealing=sealing)
    await runner.on_event(await event(sealing))
    completed_by = await event(sealing, id="completion-source")
    await runner.on_event(completed_by)
    completed = await store.load(saga.name, "workflow-7")
    assert isinstance(completed, edax.CompletedInstance)
    protected = completed.protected
    assert protected is not None and protected.completed and protected.subject == SUBJECT
    private_bytes_absent(bytes(protected.data))
    opened = await sealing.open_state(
        "saga",
        "workflow-7",
        saga.name,
        completed.version,
        edax.SealedValue(encryption=protected.encryption, data=protected.data),
        protected.tenant,
        protected.subject,
        completed.folded,
        True,
    )
    assert json.loads(opened) is None
    await runner.on_event(completed_by)
    await runner.on_event(await event(sealing, id="late-source"))
    assert saga.folds == 2
    with pytest.raises(edax.CodecError):
        await runner.on_event(await event(sealing, subject="different-owner", id="different-source"))
    assert saga.folds == 2
    assert await store.load(saga.name, "workflow-7") == completed
    with pytest.raises(edax.ConcurrencyError):
        await store.save(
            saga.name, "workflow-7", {"email": "forged"}, completed.version, completed.folded, []
        )


@pytest.mark.parametrize("allow_plaintext", [False, True])
async def test_completed_legacy_saga_requires_explicit_migration_policy(allow_plaintext: bool) -> None:
    sealing = Sealing(MemoryKeys(), "profiles", allow_plaintext=allow_plaintext)
    store = MemorySagaStore()
    saga = ProfileSaga()
    await store.complete(saga.name, "workflow-7", 0, [])
    runner = edax.SagaRunner(saga, store, COMMANDS, sealing=sealing)
    raw = await event(sealing)
    if allow_plaintext:
        await runner.on_event(raw)
    else:
        with pytest.raises(edax.CodecError):
            await runner.on_event(raw)
    assert saga.folds == 0


async def test_legacy_plaintext_restore_cannot_bypass_permanent_revocation() -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles", allow_plaintext=True)
    raw = replace(build_message("opaque", "Legacy", 1, data=EMAIL.encode()), subject=SUBJECT)
    command = edax.command_message(CreateUser(EMAIL), subject=SUBJECT)
    assert await sealing.open_event(raw) == EMAIL.encode()
    assert await sealing.open_command(command)
    await keys.revoke(sealing.scope(None, SUBJECT))
    await keys.destroy(sealing.scope(None, SUBJECT))
    with pytest.raises(Shredded):
        await sealing.open_event(raw)
    with pytest.raises(Shredded):
        await sealing.open_command(command)
    assert await sealing.open_event_for_consumer(raw) is None
    assert await sealing.open_command_for_consumer(command) is None


async def test_unconfigured_readers_reject_ciphertext_before_payload_decoding() -> None:
    sealing = Sealing(MemoryKeys(), "profiles")
    raw = await event(sealing)
    with pytest.raises(edax.BadHandle):
        await edax.EventRouter().dispatch(raw)
    store = MemoryEventStore()
    protected = repository(store, sealing)
    profile = await protected.load("opaque", subject=SUBJECT)
    profile.apply(CreateUser(EMAIL))
    await protected.save(profile)
    with pytest.raises(edax.BadHandle):
        await repository(store, None).load("opaque")
    command = await sealing.seal_command(edax.command_message(CreateUser(EMAIL), subject=SUBJECT))
    bus = edax.CommandBus(codec=COMMANDS)

    async def never(cmd: CreateUser) -> None:
        pytest.fail("unconfigured handler must not receive protected payloads")

    bus.register(CreateUser, never)
    with pytest.raises(edax.BadHandle):
        await bus.dispatch_message(command)


@pytest.mark.parametrize("snapshot_every", [0, 1])
async def test_legacy_repository_rejects_a_different_persisted_tenant(snapshot_every: int) -> None:
    store = MemoryEventStore()
    old = repository(store, None, snapshot_every=snapshot_every)
    profile = await old.load("profile", tenant="revoked-tenant", subject=SUBJECT)
    profile.apply(CreateUser(EMAIL))
    await old.save(profile)
    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles", allow_plaintext=True)
    await keys.revoke(sealing.scope("revoked-tenant", None))
    await keys.destroy(sealing.scope("revoked-tenant", None))
    with pytest.raises(edax.CodecError):
        await repository(store, sealing).load("profile", tenant="different-tenant")


async def test_unsealed_replay_restores_subject_without_a_snapshot() -> None:
    store = MemoryEventStore()
    repo = repository(store, None, snapshot_every=0)
    profile = await repo.load("profile", subject=SUBJECT)
    profile.apply(CreateUser(EMAIL))
    await repo.save(profile)
    restored = await repo.load("profile")
    assert restored.subject == SUBJECT
    restored.apply(CreateUser("changed@example.test"))
    rows = await repo.save(restored)
    assert rows[0].subject == SUBJECT


async def test_borrowed_transaction_cannot_release_sealing_guard_before_commit() -> None:
    store = MemoryEventStore()
    repo = repository(store, Sealing(MemoryKeys(), "profiles"))
    profile = await repo.load("profile", subject=SUBJECT)
    profile.apply(CreateUser(EMAIL))
    async with store.begin() as tx:
        with pytest.raises(edax.BadHandle):
            await repo.save(profile, tx=tx)
        assert await store.load(tx, "profile") == []


async def test_structured_child_writes_share_guard_until_all_finish() -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles")
    store = MemoryEventStore()
    repo = repository(store, sealing)
    profiles = [await repo.load(id, subject=SUBJECT) for id in ("profile-a", "profile-b")]
    for profile in profiles:
        profile.apply(CreateUser(EMAIL))
    async with sealing.write_guard(None, SUBJECT):
        await asyncio.wait_for(asyncio.gather(*(repo.save(profile) for profile in profiles)), 1)
    for profile in profiles:
        assert (await store.load(None, profile.id))[0].encryption is not None
    await asyncio.wait_for(keys.revoke(sealing.scope(None, SUBJECT)), 1)


async def test_received_command_and_event_each_resolve_payload_once() -> None:
    class CountingKeys(MemoryKeys):
        resolutions = 0

        async def resolve(self, scope: KeyScope, cipher: Cipher, key_id: str) -> bytes:
            self.resolutions += 1
            return await super().resolve(scope, cipher, key_id)

    keys = CountingKeys()
    sealing = Sealing(keys, "profiles")
    bus = edax.CommandBus(codec=COMMANDS, sealing=sealing)
    seen: list[str] = []

    async def handle(command: CreateUser) -> None:
        seen.append(command.email)

    bus.register(CreateUser, handle)
    command = await sealing.seal_command(
        edax.command_message(CreateUser(EMAIL), codec=COMMANDS, subject=SUBJECT)
    )
    await bus.deliver_message(command)
    assert keys.resolutions == 1 and seen == [EMAIL]
    raw = await event(sealing)
    router = edax.EventRouter(sealing=sealing)

    async def receive(message: edax.EventMessage) -> None:
        assert json.loads(bytes(message.data))["email"] == EMAIL

    router.register(raw.name, receive)
    before = keys.resolutions
    await router.dispatch(raw)
    assert keys.resolutions == before + 1


async def test_router_refuses_middleware_substitution_after_authentication() -> None:
    sealing = Sealing(MemoryKeys(), "profiles")
    raw = await event(sealing)
    called = False

    def tamper(next_: Any) -> Any:
        async def run(message: edax.EventMessage) -> None:
            await next_(replace(message, metadata={"altered": "context"}))

        return run

    router = edax.EventRouter(tamper, sealing=sealing)

    async def handle(_: edax.EventMessage) -> None:
        nonlocal called
        called = True

    router.register(raw.name, handle)
    with pytest.raises(edax.CodecError):
        await router.dispatch(raw)
    assert not called


@pytest.mark.parametrize("managed", [False, True], ids=["standalone", "transaction"])
async def test_expired_guard_lineage_does_not_admit_independent_tasks(managed: bool) -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles")
    scope = sealing.scope(None, SUBJECT)
    async with sealing.write_guard(None, SUBJECT):
        pass
    entered = asyncio.Event()
    finish = asyncio.Event()
    revoking = asyncio.Event()

    async def writer() -> None:
        async with AsyncExitStack() as resources:
            if managed:
                await resources.enter_async_context(transaction(MemoryEventStore()))
            await resources.enter_async_context(sealing.write_guard(None, SUBJECT))
            entered.set()
            await finish.wait()

    async def eraser() -> str:
        await entered.wait()
        revoking.set()
        return await keys.revoke(scope)

    writing = asyncio.create_task(writer())
    erasing = asyncio.create_task(eraser())
    try:
        await asyncio.wait_for(revoking.wait(), 1)
        assert not erasing.done(), "independent erasure must wait for the active writer"
        finish.set()
        await asyncio.wait_for(writing, 1)
        assert await asyncio.wait_for(erasing, 1) == "revoking"
    finally:
        finish.set()
        await asyncio.gather(writing, erasing, return_exceptions=True)


@pytest.mark.parametrize("cancel_first", [False, True], ids=["both-write", "first-cancelled"])
async def test_transaction_children_share_a_contended_guard_acquisition(cancel_first: bool) -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles")
    store = MemoryEventStore()
    repo = repository(store, sealing)
    profiles = [await repo.load(id, subject=SUBJECT) for id in ("profile-a", "profile-b")]
    for profile in profiles:
        profile.apply(CreateUser(EMAIL))
    blocking = asyncio.Event()
    unblock = asyncio.Event()

    async def independent_writer() -> None:
        async with sealing.write_guard(None, SUBJECT):
            blocking.set()
            await unblock.wait()

    blocker = asyncio.create_task(independent_writer())
    await asyncio.wait_for(blocking.wait(), 1)
    try:
        async with transaction(store):
            first = asyncio.create_task(repo.save(profiles[0]))
            second = asyncio.create_task(repo.save(profiles[1]))
            await asyncio.sleep(0)
            assert not first.done() and not second.done()
            if cancel_first:
                first.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await first
            unblock.set()
            pending = [second] if cancel_first else [first, second]
            await asyncio.wait_for(asyncio.gather(*pending), 1)
        assert len(await store.load(None, "profile-a")) == int(not cancel_first)
        assert len(await store.load(None, "profile-b")) == 1
        assert await asyncio.wait_for(keys.revoke(sealing.scope(None, SUBJECT)), 1) == "revoking"
    finally:
        unblock.set()
        await blocker


@pytest.mark.parametrize("changes", [{"metadata": {"forged": "context"}}, {"subject": "forged-owner"}])
@pytest.mark.parametrize("mode", ["no-guard", "no-key", "claim", "replay"])
async def test_command_rejects_middleware_substitution_before_handlers_and_guards(
    changes: dict[str, Any], mode: str
) -> None:
    class CountingLedger(MemoryIdempotencyStore):
        claims = 0

        async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Any:
            self.claims += 1
            return await super().claim(key, payload_hash, tx)

    keys = MemoryKeys()
    sealing = Sealing(keys, "profiles")
    ledger = CountingLedger()
    tampering = False
    handled: list[str] = []
    downstream: list[str] = []

    def tamper(next_: Any) -> Any:
        async def run(msg: edax.CommandMessage, cmd: CreateUser) -> Any:
            return await next_(replace(msg, **changes) if tampering else msg, cmd)

        return run

    def observe_downstream(next_: Any) -> Any:
        async def run(msg: edax.CommandMessage, cmd: CreateUser) -> Any:
            downstream.append(msg.id)
            return await next_(msg, cmd)

        return run

    middleware = [tamper]
    if mode != "no-guard":
        middleware.extend(
            [edax.idempotent(ledger, scope="api", tenancy=edax.Tenancy.SILO), observe_downstream]
        )
    bus = edax.CommandBus(*middleware, codec=COMMANDS, sealing=sealing)

    async def handle(command: CreateUser) -> str:
        handled.append(command.email)
        return command.email

    bus.register(CreateUser, handle)
    raw = await sealing.seal_command(edax.command_message(CreateUser(EMAIL), codec=COMMANDS, subject=SUBJECT))
    if mode == "replay":
        assert await bus.dispatch_message(raw) == EMAIL
    before_claims, before_handled, before_downstream = ledger.claims, len(handled), len(downstream)
    tampering = True
    with pytest.raises(edax.CodecError, match="authenticated command envelope"):
        if mode == "no-key":
            await bus.dispatch(CreateUser(EMAIL), subject=SUBJECT)
        else:
            await bus.dispatch_message(raw)
    assert ledger.claims == before_claims
    assert len(handled) == before_handled
    assert len(downstream) == before_downstream
    assert await keys.status(sealing.scope(None, "forged-owner")) == "unknown"


async def test_current_sealed_command_returns_a_copy_of_the_original_envelope() -> None:
    sealing = Sealing(MemoryKeys(), "profiles")
    raw = await sealing.seal_command(edax.command_message(CreateUser(EMAIL), codec=COMMANDS, subject=SUBJECT))
    observed: list[edax.CommandMessage] = []

    def inspect_current(next_: Any) -> Any:
        async def run(msg: edax.CommandMessage, cmd: CreateUser) -> Any:
            current = edax.current_command()
            assert current is not None and current == raw and current is not msg
            observed.append(current)
            return await next_(msg, cmd)

        return run

    bus = edax.CommandBus(inspect_current, codec=COMMANDS, sealing=sealing)

    async def handle(command: CreateUser) -> None:
        current = edax.current_command()
        assert current is not None and current == raw and current is not observed[0]
        observed.append(current)

    bus.register(CreateUser, handle)
    await bus.dispatch_message(raw)
    assert len(observed) == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("subject", "forged-owner"),
        ("data", b'{"email":"forged@example.test"}'),
        ("metadata", {"forged": "yes"}),
    ],
)
async def test_opened_event_cannot_be_mutated_before_a_nested_saga(field: str, value: Any) -> None:
    sealing = Sealing(MemoryKeys(), "profiles")
    store = MemorySagaStore()
    saga = ProfileSaga()
    runner = edax.SagaRunner(saga, store, COMMANDS, sealing=sealing)
    router = edax.EventRouter(sealing=sealing)

    async def mutate(message: edax.EventMessage) -> None:
        setattr(message, field, value)
        await runner.on_event(message)

    router.register("EmailChanged", mutate)
    with pytest.raises(AttributeError):
        await router.dispatch(await event(sealing))
    assert saga.folds == 0
    assert await store.load(saga.name, "workflow-7") is None
    assert await store.unpublished(saga.name, 100) == []


@pytest.mark.parametrize("cipher", ["aes-256-gcm", "xchacha20-poly1305"])
async def test_sealed_saga_command_failure_keeps_owner_and_safe_details_without_repeating(
    cipher: Cipher,
) -> None:
    class CompensatingSaga(ProfileSaga):
        def __init__(self) -> None:
            super().__init__()
            self.failures: list[dict[str, Any]] = []

        def handle(self, state: Any, msg: edax.EventMessage) -> tuple[Any, list[CreateUser]]:
            if msg.name != edax.SAGA_COMMAND_FAILED:
                return super().handle(state, msg)
            self.folds += 1
            self.failures.append(json.loads(bytes(msg.data)))
            assert msg.tenant == "acme" and msg.subject == SUBJECT
            assert state == {"email": EMAIL}
            return state, [CreateUser(EMAIL)]

    sealing = Sealing(MemoryKeys(), "profiles", cipher=cipher)
    repo = repository(MemoryEventStore(), sealing)
    profile = await repo.load("profile-1", tenant="acme", subject=SUBJECT)
    profile.apply(CreateUser(EMAIL))
    raw = (await repo.save(profile))[0]
    store = MemorySagaStore()
    saga = CompensatingSaga()
    runner = edax.SagaRunner(saga, store, COMMANDS, sealing=sealing)
    await runner.on_event(raw)
    original = (await store.unpublished(saga.name, 100))[0]
    dead = edax.DeadLetter(original, 1, edax.Rejected(EMAIL))
    await runner.command_failed(dead)
    saved = await store.load(saga.name, "workflow-7")
    assert isinstance(saved, edax.RunningInstance)
    assert isinstance(saved.state, ProtectedSagaState)
    assert saved.version == 2 and saved.state.tenant == "acme" and saved.state.subject == SUBJECT
    private_bytes_absent(bytes(saved.state.data))
    assert saga.failures == [
        {"command_id": original.id, "command": original.name, "code": "rejected", "error": ""}
    ]
    commands = await store.unpublished(saga.name, 100)
    assert len(commands) == 2
    for command in commands:
        assert command.tenant == "acme" and command.subject == SUBJECT
        assert command.encryption is not None and command.encryption.cipher == cipher
        private_bytes_absent(bytes(_native.encode_command("json", command)))
        assert json.loads(await sealing.open_command(command)) == {"email": EMAIL}
    await runner.command_failed(dead)
    assert saga.folds == 2 and len(saga.failures) == 1
    assert await store.load(saga.name, "workflow-7") == saved
    assert await store.unpublished(saga.name, 100) == commands

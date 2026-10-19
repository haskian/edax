"""``Sealing(write_fence="transaction")``: the key fence taken inside each transaction that writes."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast

import edax
import pytest
from conftest import CreateUser
from edax import _native
from edax._units import leave_every_unit_of_work, transaction_guard
from edax.backend.memory import (
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemoryProjectionStore,
    MemorySagaStore,
    MemoryTransactor,
)
from edax.errors import BadHandle, KeyRevoking, Shredded
from edax.idempotency import Won
from edax.projection import Entity
from edax.sealing import KeyScope, MemoryKeys, Sealing, WriteFence, pending_fences
from edax.unit_of_work import transaction
from test_brokerless_ingestion import NonReentrantKeys
from test_crypto_workflows import COMMANDS, EMAIL, SUBJECT, Profile, event, repository


class FencingKeys(MemoryKeys):
    """The reference provider, fencing inside transactions of ``transactor`` and recording each call.
    ``erase_at_fence`` destroys each scope just before the fence reads it: an erasure that won the race."""

    def __init__(self, transactor: object) -> None:
        super().__init__()
        self.transactor = transactor
        self.fences: list[tuple[Any, list[KeyScope]]] = []
        self.erase_at_fence = False

    async def fence(self, tx: Any, scopes: Sequence[KeyScope]) -> None:
        self.fences.append((tx, list(scopes)))
        for scope in scopes:
            if self.erase_at_fence:
                await self.revoke(scope)
                await self.destroy(scope)
            _native.check_key_write_status(await self.status(scope))


def rig(*, same_database: bool = True) -> tuple[MemoryEventStore, FencingKeys, Sealing]:
    store = MemoryEventStore()
    keys = FencingKeys(store.transactor if same_database else MemoryTransactor())
    return store, keys, Sealing(keys, "profiles", write_fence="transaction")


async def changed(repo: edax.Repository[Profile, Any], id: str = "profile-1") -> Profile:
    profile = await repo.load(id, subject=SUBJECT)
    profile.apply(CreateUser(EMAIL))
    return profile


@pytest.mark.parametrize(
    ("keys", "write_fence", "refused"),
    [
        (MemoryKeys(), "guard", False),
        (FencingKeys(MemoryTransactor()), "transaction", False),
        (MemoryKeys(), "transaction", True),
        (FencingKeys(MemoryTransactor()), "eager", True),
    ],
    ids=["guard", "transaction", "provider-without-fence", "unknown-mode"],
)
def test_transaction_mode_needs_a_fencing_provider(keys: MemoryKeys, write_fence: str, refused: bool) -> None:
    mode = cast(WriteFence, write_fence)
    if refused:
        with pytest.raises(BadHandle):
            Sealing(keys, "profiles", write_fence=mode)
    else:
        assert Sealing(keys, "profiles", write_fence=mode).write_fence == write_fence


async def test_command_whose_handler_saves_twice_fences_once_inside_its_transaction() -> None:
    store, keys, sealing = rig()
    repo = repository(store, sealing)
    bus = edax.CommandBus(codec=COMMANDS, sealing=sealing)
    opened: list[Any] = []

    async def handle(command: CreateUser) -> None:
        async with transaction(store) as tx:
            opened.append(tx)
            for id in ("profile-a", "profile-b"):
                await repo.save(await changed(repo, id))
            # No eager guard holds the provider's lock: another scope's erasure goes ahead at once.
            other = asyncio.get_running_loop().create_task(keys.revoke(sealing.scope(None, "other")))
            assert await asyncio.wait_for(other, 1) == "revoking"

    bus.register(CreateUser, handle)
    await bus.dispatch(CreateUser(EMAIL), subject=SUBJECT)
    assert keys.fences == [(opened[0], [sealing.scope(None, SUBJECT)])]
    assert len(await store.load(None, "profile-a")) == len(await store.load(None, "profile-b")) == 1


async def test_consumer_that_writes_nothing_takes_no_fence() -> None:
    _, keys, sealing = rig()
    raw = await event(sealing)  # saved on another database: no fence call
    seen: list[bytes] = []
    router = edax.EventRouter(sealing=sealing)

    async def push(msg: edax.EventMessage) -> None:
        seen.append(bytes(msg.data))

    router.register(raw.name, push)
    await router.dispatch(raw)
    assert seen and EMAIL.encode() in seen[0]
    assert keys.fences == []


async def test_save_outside_a_unit_of_work_fences_the_transaction_it_opens() -> None:
    store, keys, sealing = rig()
    repo = repository(store, sealing)
    await repo.save(await changed(repo))
    assert [scopes for _, scopes in keys.fences] == [[sealing.scope(None, SUBJECT)]]
    assert keys.fences[0][0] is not None


async def test_scopes_pending_together_are_fenced_in_one_call() -> None:
    store, keys, sealing = rig()
    async with (
        sealing.write_guard(None, "b"),
        sealing.write_guard(None, "a"),
        sealing.write_guard(None, "b"),
        transaction(store) as tx,
    ):
        pass
    assert keys.fences == [(tx, [sealing.scope(None, "b"), sealing.scope(None, "a")])]
    assert pending_fences() == ()


async def test_transaction_on_another_database_holds_the_eager_guard_through_commit() -> None:
    store, keys, sealing = rig(same_database=False)
    entered = asyncio.Event()
    commit = asyncio.Event()

    async def writer() -> None:
        async with sealing.write_guard(None, SUBJECT), transaction(store):
            entered.set()
            await commit.wait()

    writing = asyncio.create_task(writer())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        erasing = asyncio.create_task(keys.revoke(sealing.scope(None, SUBJECT)))
        await asyncio.sleep(0.01)
        assert not erasing.done(), "a lock in the provider's database cannot fence another database's commit"
        commit.set()
        await asyncio.wait_for(writing, 1)
        assert await asyncio.wait_for(erasing, 1) == "revoking"
        assert keys.fences == []
    finally:
        commit.set()
        await asyncio.gather(writing, return_exceptions=True)


@pytest.mark.parametrize(
    ("same_database", "child_task", "fenced_in_place"),
    [(True, False, True), (False, False, False), (True, True, False)],
    ids=["provider-database", "another-database", "task-that-did-not-open-it"],
)
async def test_dispatch_inside_an_open_unit_fences_it_at_once(
    same_database: bool, child_task: bool, fenced_in_place: bool
) -> None:
    store, keys, sealing = rig(same_database=same_database)
    scope = sealing.scope(None, "late")
    bus = edax.CommandBus(codec=COMMANDS, sealing=sealing)

    async def handle(_: CreateUser) -> None:
        pass

    bus.register(CreateUser, handle)
    dispatch = bus.dispatch(CreateUser(EMAIL), subject="late")

    async def erase() -> str:
        leave_every_unit_of_work()  # an independent erasure, not part of the write
        return await keys.revoke(scope)

    async with transaction(store) as tx:
        await (asyncio.create_task(dispatch) if child_task else dispatch)
        erasing = asyncio.create_task(erase())
        await asyncio.sleep(0.01)
        # In place: the lock lives in tx. Otherwise the provider's guard holds on the unit until it ends.
        assert erasing.done() is fenced_in_place
    assert await asyncio.wait_for(erasing, 1) == "revoking"
    assert keys.fences == ([(tx, [scope])] if fenced_in_place else [])


@pytest.mark.parametrize("same_database", [True, False], ids=["provider-database", "another-database"])
async def test_borrowed_transaction_is_fenced_in_place_only_on_the_provider_database(
    same_database: bool,
) -> None:
    store, keys, sealing = rig(same_database=same_database)
    repo = repository(store, sealing)
    profile = await changed(repo)
    async with store.begin() as tx:
        if same_database:
            await repo.save(profile, tx=tx)
            assert keys.fences == [(tx, [sealing.scope(None, SUBJECT)])]
        else:
            with pytest.raises(BadHandle):
                await repo.save(profile, tx=tx)
            assert keys.fences == []
    assert len(await store.load(None, profile.id)) == int(same_database)


class Notify:
    """Sends one command per event, and raises its own Shredded from the folds of the messages it names."""

    name = "notify"

    def __init__(self, raise_on: frozenset[str] = frozenset()) -> None:
        self.raise_on = raise_on

    def instance_id(self, msg: edax.EventMessage) -> str:
        return "workflow-7"

    def handle(self, state: Any, msg: edax.EventMessage) -> tuple[Any, list[CreateUser]]:
        if msg.name in self.raise_on:
            raise Shredded("a fold failure about another scope")
        return {"notified": True}, [CreateUser(EMAIL)]


class Erasing(MemorySagaStore):
    """Once ``armed``, destroys the scope as the fold loads: an erasure that lands after the runner's fence."""

    def __init__(self, transactor: MemoryTransactor, keys: FencingKeys, scope: KeyScope) -> None:
        super().__init__(transactor)
        self.keys, self.scope, self.armed = keys, scope, False

    async def load(self, *args: Any, **kwargs: Any) -> Any:
        if self.armed:
            await self.keys.revoke(self.scope)
            await self.keys.destroy(self.scope)
        return await super().load(*args, **kwargs)


@pytest.mark.parametrize(
    ("consumer", "skips"),
    [
        ("router", True),
        ("command-delivery", True),
        ("command-dispatch", False),
        ("saga-event", True),
        ("saga-command-failed", True),
    ],
)
@pytest.mark.parametrize("erased", [True, False], ids=["scope-destroyed", "scope-active"])
async def test_consumer_skips_a_refused_fence_only_when_the_provider_confirms_destruction(
    consumer: str, skips: bool, erased: bool
) -> None:
    store, keys, sealing = rig()
    scope = sealing.scope(None, SUBJECT)
    raw = await event(sealing)
    sagas = Erasing(store.transactor, keys, scope)
    failing = {"saga-event": raw.name, "saga-command-failed": edax.SAGA_COMMAND_FAILED}.get(consumer)
    runner = edax.SagaRunner(
        Notify(frozenset() if erased or failing is None else frozenset({failing})),
        sagas,
        COMMANDS,
        sealing=sealing,
    )
    keys.erase_at_fence = erased and failing is None
    opened: list[Any] = []

    async def handler(_: object) -> None:
        # Erased: the fence refuses as the transaction opens. Active: the handler's own Shredded.
        async with transaction(store) as tx:
            opened.append(tx)
            raise Shredded("a handler failure about another scope")

    router = edax.EventRouter(sealing=sealing)
    router.register(raw.name, handler)
    bus = edax.CommandBus(codec=COMMANDS, sealing=sealing)
    bus.register(CreateUser, handler)
    command = await sealing.seal_command(
        edax.command_message(CreateUser(EMAIL), codec=COMMANDS, subject=SUBJECT)
    )

    async def in_a_unit_of_work(work: Awaitable[None]) -> None:
        async with transaction(store) as tx:
            opened.append(tx)
            await work

    if consumer == "router":
        deliver = router.dispatch(raw)
    elif consumer == "command-delivery":
        deliver = bus.deliver_message(command)
    elif consumer == "command-dispatch":
        deliver = bus.dispatch_message(command)
    elif consumer == "saga-event":
        sagas.armed = erased
        deliver = in_a_unit_of_work(runner.on_event(raw))
    else:
        await runner.on_event(raw)
        keys.fences.clear()
        sent = (await sagas.unpublished("notify", 100))[0]
        sagas.armed = erased
        deliver = in_a_unit_of_work(runner.command_failed(edax.DeadLetter(sent, 1, edax.Rejected("refused"))))
    if erased and skips:
        assert await deliver is None
        assert await keys.status(scope) == "destroyed"
    else:
        with pytest.raises(Shredded):
            await deliver
    # The scope pending at the boundary was fenced once, inside the transaction the consumer opened (which
    # never began when that fence refused it).
    assert [scopes for _, scopes in keys.fences] == [[scope]]
    assert opened == [] or keys.fences[0][0] is opened[0]


@pytest.mark.parametrize("writer", ["projection-from-handler", "saga-fold", "protected-ledger"])
async def test_store_write_outside_a_unit_of_work_is_refused_once_the_scope_is_revoked(writer: str) -> None:
    store, keys, sealing = rig()
    raw = await event(sealing)
    writing = asyncio.Event()
    release = asyncio.Event()

    async def hold() -> None:
        # The consumer has read the plaintext and is about to write it: nothing holds back a revoke now.
        writing.set()
        await release.wait()

    unwritten: Callable[[], Awaitable[bool]]
    if writer == "projection-from-handler":
        projections = MemoryProjectionStore(store.transactor, projection="profiles")
        router = edax.EventRouter(sealing=sealing)

        async def project(msg: edax.EventMessage) -> None:
            await hold()
            await projections.save(Entity(SUBJECT, 0, json.loads(bytes(msg.data))))

        router.register(raw.name, project)
        work = router.dispatch(raw)

        async def unwritten() -> bool:
            return await projections.get(SUBJECT) is None

    elif writer == "saga-fold":

        class HeldSagas(MemorySagaStore):
            async def save(self, *args: Any, **kwargs: Any) -> Any:
                await hold()
                return await super().save(*args, **kwargs)

        sagas = HeldSagas()
        work = edax.SagaRunner(Notify(), sagas, COMMANDS, sealing=sealing).on_event(raw)

        async def unwritten() -> bool:
            return await sagas.load("notify", "workflow-7") is None

    else:

        class HeldLedger(MemoryIdempotencyStore):
            async def complete(self, *args: Any, **kwargs: Any) -> None:
                await hold()
                await super().complete(*args, **kwargs)

        ledger = HeldLedger()
        bus = edax.CommandBus(
            edax.idempotent(ledger, scope="api", tenancy=edax.Tenancy.SILO), codec=COMMANDS, sealing=sealing
        )

        async def handle(_: CreateUser) -> dict[str, str]:
            return {"email": EMAIL}

        bus.register(CreateUser, handle)
        work = bus.dispatch(CreateUser(EMAIL), idempotency_key="request-7", subject=SUBJECT)

        async def unwritten() -> bool:
            # The refused settlement stored no record: the claim is still the one the dispatch won.
            claim = edax.claim_key("api", "request-7", tenancy=edax.Tenancy.SILO)
            stored = await ledger.claim(_native.protected_claim_key(sealing.scope(None, SUBJECT), claim), 0)
            return isinstance(stored, Won)

    writer_task = asyncio.create_task(work)
    try:
        await asyncio.wait_for(writing.wait(), 1)
        assert await asyncio.wait_for(keys.revoke(sealing.scope(None, SUBJECT)), 1) == "revoking"
        release.set()
        with pytest.raises(KeyRevoking):
            await asyncio.wait_for(writer_task, 1)
    finally:
        release.set()
        await asyncio.gather(writer_task, return_exceptions=True)
    assert await unwritten()


async def test_a_task_that_leaves_every_unit_of_work_inherits_no_pending_fence() -> None:
    _, _, sealing = rig()

    async def worker(leave: bool) -> int:
        if leave:
            leave_every_unit_of_work()
        return len(pending_fences())

    async with sealing.write_guard(None, SUBJECT):
        inherited = await asyncio.create_task(worker(leave=False))
        left = await asyncio.create_task(worker(leave=True))
    assert (inherited, left) == (1, 0)


@pytest.mark.parametrize(
    "guard_in_outer", [True, False], ids=["outer-covers-inner", "inner-cannot-cover-outer"]
)
async def test_guard_reuse_respects_nested_database_lifetimes(guard_in_outer: bool) -> None:
    outer, inner = MemoryTransactor(), MemoryTransactor()
    keys = NonReentrantKeys()
    sealing = Sealing(keys, "profiles")
    owner, borrower = (outer, inner) if guard_in_outer else (inner, outer)
    async with transaction(outer):
        async with transaction(inner):
            async with transaction_guard(owner, sealing.write_guard(None, SUBJECT)):
                pass
            if guard_in_outer:
                async with transaction_guard(borrower, sealing.write_guard(None, SUBJECT)):
                    assert keys.acquisitions == 1
            else:
                with pytest.raises(BadHandle, match="does not cover this transaction"):
                    async with transaction_guard(borrower, sealing.write_guard(None, SUBJECT)):
                        pytest.fail("an inner guard cannot cover the outer transaction's later commit")
            assert keys.lock.locked()
        assert keys.lock.locked() is guard_in_outer
    assert keys.acquisitions == 1 and not keys.lock.locked()

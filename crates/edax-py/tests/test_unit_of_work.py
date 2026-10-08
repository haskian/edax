"""The guard-owned unit of work: what ``idempotent(..., unit_of_work=)`` guarantees in each mode.

Two orderings live behind one keyword, chosen by whether the ledger can join the caller's
transaction (:func:`~edax.idempotency.joins_transaction`):

* **shared** — ``begin; claim; run; complete; commit``. One atomic commit, so a rollback *is* the
  release. Effectively-once for state in that database.
* **split** — ``claim; begin; run; commit; complete``. Two independent commits, so every failure
  after the claim is compensated by hand, and ``complete`` deliberately runs *after* the commit:
  before it, a crash in between would leave a ``done`` record over work that never landed.

The table below is one case per failure window. Each asserts both halves of the outcome (how many
times the handler ran, and whether its write is visible), because either alone can look right while
the other is wrong.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar

import edax
import fakeredis
import pytest
from conftest import SCOPE, RefusingLedger, build_message, claimed, event_claimed
from edax.backend.memory import MemoryIdempotencyStore, MemoryLedger, MemoryProjectionStore, MemoryTransactor
from edax.backend.redis import AsyncRedisClient, RedisIdempotencyStore
from edax.command import CommandBus, _payload_hash, idempotent
from edax.idempotency import Claimed, Done, Pending, Record, Won, joins_transaction
from edax.projection import Entity
from edax.router import idempotent_by_id
from edax.testing.faults import BrokenCommit, CommitFault, Refusing
from hypothesis import given, settings
from hypothesis import strategies as st

DOC = "acct-1"


@dataclass
class Deposit:
    amount: int
    request_id: str
    type = "Deposit"


class Rig:
    """A guarded bus over a projection store, wired for one ledger mode and one commit behaviour.

    ``runs`` counts handler invocations; ``landed()`` reports whether the handler's write survived.
    Between them the two say what actually happened, which no single assertion does."""

    def __init__(
        self, *, shared: bool, commit: CommitFault = None, raises: Exception | None = None, result: Any = None
    ) -> None:
        self.db = MemoryTransactor()
        self.projections = MemoryProjectionStore(self.db)
        self.ledger: Any = MemoryIdempotencyStore(self.db) if shared else MemoryLedger()
        self.unit: Any = BrokenCommit(self.db, commit) if commit else self.db
        self.runs = 0
        self._raises = raises
        self._result = result

        async def handler(cmd: Deposit) -> Any:
            self.runs += 1
            entity = await self.projections.get(DOC)
            if entity is None:
                entity = Entity(DOC, 0, {"id": DOC, "balance": 0})
            entity.document["balance"] = int(entity.document["balance"]) + cmd.amount
            await self.projections.save(entity)
            if self._raises is not None:
                raise self._raises
            return int(entity.document["balance"]) if self._result is None else self._result

        self.bus = CommandBus(
            idempotent(self.ledger, scope="test", unit_of_work=self.unit, tenancy=edax.Tenancy.SILO)
        )
        self.bus.register(Deposit, handler)

    async def dispatch(self) -> Any:
        return await self.bus.dispatch(Deposit(100, "req-A"), idempotency_key="req-A")

    async def balance(self) -> int:
        entity = await self.projections.get(DOC)
        return 0 if entity is None else int(entity.document["balance"])


@dataclass
class Case:
    name: str
    shared: bool
    commit: CommitFault = None
    raises: Exception | None = None
    result: Any = None  # what the handler returns instead of the balance
    first: type[BaseException] | None = None  # what the first dispatch raises, if anything
    second: type[BaseException] | None = None  # what the redelivery raises, if anything
    cached_code: str | None = None  # the code a cached failure replays with
    runs: int = 1  # handler invocations across both dispatches
    balance: int = 100  # what the projection holds at the end


CASES = [
    # --- shared ledger: the rollback is the release -------------------------------------------
    Case("shared_success_replays_instead_of_rerunning", shared=True),
    Case(
        "shared_transient_failure_rolls_back_claim_and_work_so_the_redelivery_reruns",
        shared=True,
        raises=RuntimeError("projection store unavailable"),
        first=RuntimeError,
        second=RuntimeError,
        runs=2,
        balance=0,
    ),
    Case(
        # The cache would roll back with everything else, so it is not written at all: the key is
        # freed and the redelivery re-runs a failure that left nothing behind.
        "shared_terminal_failure_is_not_cached_because_the_rollback_would_erase_it",
        shared=True,
        raises=edax.CodecError("malformed payload"),
        first=edax.CodecError,
        second=edax.CodecError,
        runs=2,
        balance=0,
    ),
    Case(
        "shared_failed_commit_drops_the_claim_with_the_work",
        shared=True,
        commit="nothing_landed",
        first=edax.StoreError,
        second=edax.StoreError,
        runs=2,
        balance=0,
    ),
    # --- ledger of its own: every failure after the claim is compensated by hand ---------------
    Case("split_success_replays_instead_of_rerunning", shared=False),
    Case(
        "split_transient_failure_releases_the_claim_so_the_redelivery_reruns",
        shared=False,
        raises=RuntimeError("projection store unavailable"),
        first=RuntimeError,
        second=RuntimeError,
        runs=2,
        balance=0,
    ),
    Case(
        # Nothing rolls this one back, so caching it is what stops a broker redelivering forever.
        "split_terminal_failure_is_cached_and_replays",
        shared=False,
        raises=edax.CodecError("malformed payload"),
        first=edax.CodecError,
        second=edax.CachedFailure,
        cached_code="codec",
        runs=1,
        balance=0,
    ),
    Case(
        # Encoded before the commit: refused after it, the claim would be released over work that landed,
        # and the redelivery would run it again.
        "split_unencodable_result_rolls_back_and_is_cached_as_a_codec_failure",
        shared=False,
        result={"at": datetime(2024, 1, 1)},  # naive: no offset to compare by
        first=edax.CodecError,
        second=edax.CachedFailure,
        cached_code="codec",
        runs=1,
        balance=0,
    ),
    Case(
        # The window this whole change exists to close: before it, the host's commit failed
        # *after* the guard returned, leaving a `done` record over work that rolled back.
        "split_failed_commit_releases_the_claim_so_the_work_is_not_lost",
        shared=False,
        commit="nothing_landed",
        first=edax.StoreError,
        second=edax.StoreError,
        runs=2,
        balance=0,
    ),
    Case(
        # The irreducible one: the commit landed and only its response was lost, so releasing the
        # claim lets the redelivery apply the deposit a second time. At-least-once, by construction.
        "split_lost_commit_response_releases_a_claim_over_work_that_landed_so_it_runs_twice",
        shared=False,
        commit="lost_response",
        first=edax.StoreError,
        second=edax.StoreError,
        runs=2,
        balance=200,
    ),
]


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
async def test_unit_of_work_windows(case: Case) -> None:
    rig = Rig(shared=case.shared, commit=case.commit, raises=case.raises, result=case.result)

    for expected in (case.first, case.second):
        if expected is None:
            await rig.dispatch()
        else:
            with pytest.raises(expected) as raised:
                await rig.dispatch()
    if case.second is edax.CachedFailure:
        assert isinstance(raised.value, edax.CachedFailure), case.name
        assert raised.value.original_code == case.cached_code, case.name

    assert rig.runs == case.runs, f"{case.name}: handler ran {rig.runs}x"
    assert await rig.balance() == case.balance, case.name


# =========================================================================
# The broker's view: what an ack and a nack do to the claim
# =========================================================================


class Broker:
    """A guarded event handler behind the real dispatcher, so settlement is the broker's view and
    not an assertion about the guard's internals. ``deliver`` submits one message and drains, so a
    test reads ``acked`` / ``nacked`` straight after it."""

    def __init__(self, *, shared: bool, fail_first: Exception | None = None, uow: bool = True) -> None:
        self.db = MemoryTransactor()
        self.projections = MemoryProjectionStore(self.db)
        self.ledger: Any = MemoryIdempotencyStore(self.db) if shared else MemoryLedger()
        self.runs = 0
        self.acked: list[bytes] = []
        self.nacked: list[bytes] = []
        self._fail_first = fail_first

        async def handler(env: edax.EventMessage) -> None:
            self.runs += 1
            entity = await self.projections.get(DOC)
            if entity is None:
                entity = Entity(DOC, 0, {"id": DOC, "balance": 0})
            entity.document["balance"] = int(entity.document["balance"]) + 100
            await self.projections.save(entity)
            if self._fail_first is not None:
                failure, self._fail_first = self._fail_first, None  # one-shot
                raise failure

        guard = idempotent_by_id(
            self.ledger, unit_of_work=self.db if uow else None, scope=SCOPE, tenancy=edax.Tenancy.SILO
        )
        router = edax.EventRouter(guard)
        router.register("Deposited", handler)
        self.dispatcher = edax.KeyOrderedDispatcher(1, router.dispatch, max_attempts=2, on_dead=self._dead)
        self.dead: list[str] = []
        self.dispatcher.start()

    async def _dead(self, letter: edax.DeadLetter[Any]) -> None:
        # The hook sees the message; the transport handle lives in the settle closures.
        self.dead.append(letter.message.id)

    async def deliver(self, mid: bytes = b"msg-1") -> None:
        env = build_message(DOC, "Deposited", None, id="evt-1", data=b"100")
        await self.dispatcher.submit(
            edax.Delivery(env, lambda: self.acked.append(mid), lambda: self.nacked.append(mid))
        )
        # Drain this worker's queue: the delivery is settled (acked or nacked) before we look.
        await self.dispatcher._queues[self.dispatcher._worker_index(env)].join()

    async def balance(self) -> int:
        entity = await self.projections.get(DOC)
        return 0 if entity is None else int(entity.document["balance"])


@pytest.mark.parametrize("shared", [True, False], ids=["shared_unit_of_work", "ledger_of_its_own"])
async def test_a_nack_releases_the_claim_so_the_redelivery_reruns(shared: bool) -> None:
    """The guarantee both modes owe: a delivery that ends in a nack must leave its key free. A claim
    still held after a nack is worse than no guard at all: the broker redelivers, the guard reports
    ``InProgress`` or replays a success that never happened, and the work is lost."""
    broker = Broker(shared=shared, fail_first=RuntimeError("projection store unavailable"))

    await broker.deliver()
    assert broker.nacked == [b"msg-1"] and broker.acked == [], "a failed handler nacks"
    assert broker.runs == 1
    assert await broker.balance() == 0, "the handler's write did not survive its own failure"

    await broker.deliver()  # the broker redelivers what it nacked
    assert broker.acked == [b"msg-1"], "the redelivery succeeded, so it acks"
    assert broker.runs == 2, "the claim was released, so the handler ran again"
    assert await broker.balance() == 100

    await broker.deliver()  # and a redelivery of the *acked* message must not run again
    assert broker.runs == 2, "the completed record replays instead of re-running"
    assert await broker.balance() == 100
    await broker.dispatcher.stop()


@pytest.mark.parametrize("shared", [True, False], ids=["shared_unit_of_work", "ledger_of_its_own"])
async def test_an_ack_keeps_the_claim_so_the_redelivery_is_skipped(shared: bool) -> None:
    """The other half: a delivery that acks leaves a terminal record, and a redelivery inside the
    retention window replays it rather than repeating the side effect."""
    broker = Broker(shared=shared)

    await broker.deliver(b"msg-1")
    await broker.deliver(b"msg-1")
    assert broker.acked == [b"msg-1", b"msg-1"], "both deliveries are acked"
    assert broker.nacked == []
    assert broker.runs == 1, "the second delivery replayed the record"
    assert await broker.balance() == 100, "the deposit was applied once"
    await broker.dispatcher.stop()


async def test_in_progress_nacks_without_burning_the_attempt_budget() -> None:
    """A claim held by another worker is contention, not a failed attempt. Counting it would
    dead-letter a message whose holder is merely slow — ``max_attempts=2`` here, so two uncounted
    ``InProgress`` deliveries would otherwise be enough to kill it."""
    broker = Broker(shared=False)
    # Someone else takes the claim first and never settles it.
    await broker.ledger.claim(event_claimed("evt-1"), edax.payload_hash(b"100"))

    for _ in range(3):
        await broker.deliver()

    assert broker.nacked == [b"msg-1"] * 3, "every contended delivery is redelivered"
    assert broker.dead == [], "contention must never dead-letter"
    assert broker.runs == 0, "the handler never ran: the claim was held throughout"
    await broker.dispatcher.stop()


async def test_a_terminal_failure_dead_letters_and_the_key_records_it() -> None:
    """A terminal failure is not retried: the dispatcher dead-letters on the first attempt. With a
    ledger of its own the outcome is cached, so even a broker that redelivers anyway replays the
    failure instead of running the handler a second time."""
    broker = Broker(shared=False, fail_first=edax.CodecError("malformed payload"))

    await broker.deliver()
    assert broker.dead == ["evt-1"], "terminal: dead-lettered on the first attempt"
    assert broker.acked == [b"msg-1"], "a dead-lettered delivery is acked, not nacked"
    assert broker.runs == 1

    await broker.deliver()
    assert broker.runs == 1, "the cached failure replayed; the handler did not run again"
    assert broker.dead == ["evt-1", "evt-1"], "and it is terminal again, so it dies again"
    await broker.dispatcher.stop()


async def test_a_nested_unit_of_work_is_refused() -> None:
    """The Transactor contract says the SDK never nests. A guard that opened a second unit of work
    inside the host's would commit the claim independently of the writes it guards, which is the
    split-store failure wearing a shared-store face — so it fails fast instead."""
    rig = Rig(shared=True)
    async with edax.transaction(rig.db):
        with pytest.raises(edax.BadHandle, match="unit_of_work") as caught:
            await rig.dispatch()
    # Terminal and fatal: the wiring fails the same way on every redelivery, so a transient StoreError
    # here would have the dispatcher redeliver a misconfiguration forever instead of stopping on it.
    assert caught.value.terminal is True
    assert rig.runs == 0, "the handler must not run once the wiring is known to be wrong"


@dataclass
class AmbientCase:
    name: str
    ledger: Callable[[MemoryTransactor], Any]  # the ledger, given the host's database
    refused: bool  # whether the guard refuses to run inside the host's transaction


AMBIENT_CASES = [
    # The README's wiring: the claim stages in the host's transaction and rolls back with the work.
    AmbientCase("same_database_ledger_joins_so_the_rollback_frees_the_key", MemoryIdempotencyStore, False),
    # Each of these would complete before the host commits, so a rollback would leave a cached success.
    AmbientCase("ledger_of_its_own_is_refused", lambda db: MemoryLedger(), True),
    AmbientCase(
        "joining_ledger_over_another_database_is_refused",
        lambda db: MemoryIdempotencyStore(MemoryTransactor()),
        True,
    ),
    AmbientCase("redis_ledger_is_refused", lambda db: redis_ledger(), True),
]


def redis_ledger() -> RedisIdempotencyStore:
    """The Redis ledger on fakeredis, which runs the kernel's Lua: a ledger of its own that no host
    transaction can enlist."""
    return RedisIdempotencyStore(AsyncRedisClient(fakeredis.FakeAsyncRedis()), "billing:test:idem:")


@pytest.mark.parametrize("guard", ["idempotent", "idempotent_by_id"])
@pytest.mark.parametrize("case", AMBIENT_CASES, ids=lambda c: c.name)
async def test_a_guard_inside_the_hosts_transaction_runs_only_on_a_ledger_that_joins_it(
    case: AmbientCase, guard: str
) -> None:
    """The host opens the transaction around the dispatch and passes no ``unit_of_work``. Only a ledger
    that stages in that transaction may run there. Any other ledger records success before the host
    commits: a later step that aborts the transaction leaves the key replaying a result whose work never
    landed. The guard refuses it with the fatal ``BadHandle`` before the claim, so the redelivery outside
    the transaction runs the work once and it lands."""
    db = MemoryTransactor()
    projections = MemoryProjectionStore(db)
    ledger = case.ledger(db)
    runs = 0

    async def handler(message: Any) -> None:
        nonlocal runs
        runs += 1
        await projections.save(Entity(DOC, 0, {"id": DOC, "balance": 100}))

    if guard == "idempotent":
        bus = CommandBus(idempotent(ledger, scope="test", tenancy=edax.Tenancy.SILO))
        bus.register(Deposit, handler)

        async def dispatch() -> Any:
            return await bus.dispatch(Deposit(100, "req-A"), idempotency_key="req-A")
    else:
        router = edax.EventRouter(idempotent_by_id(ledger, scope=SCOPE, tenancy=edax.Tenancy.SILO))
        router.register("Deposited", handler)
        env = build_message(
            DOC, "Deposited", 1, id="evt-1", data=b'{"amount":100}', recorded_at=1_714_564_800_000_000_000
        )

        async def dispatch() -> Any:
            return await router.dispatch(env)

    with pytest.raises((edax.BadHandle, RuntimeError)) as caught:
        async with edax.transaction(db):
            await dispatch()
            raise RuntimeError("a later step aborts the host's transaction")
    if case.refused:
        assert isinstance(caught.value, edax.BadHandle) and edax.is_fatal(caught.value), case.name
        assert runs == 0, "refused before the claim, so the handler never ran"
    else:
        assert not isinstance(caught.value, edax.BadHandle), case.name
        assert runs == 1

    await dispatch()  # the redelivery, outside any transaction
    assert runs == (1 if case.refused else 2), "the key was free, so the work ran again"
    assert await projections.get(DOC) is not None, "the redelivered work landed"


@dataclass
class CommitFailureCase:
    name: str
    ledger: Callable[[MemoryTransactor], Any]  # the ledger, given the business database
    host_owns_the_transaction: bool  # the host's `transaction` around the dispatch, or `unit_of_work=`
    error: type[BaseException]  # what the first dispatch raises
    runs: int  # handler invocations in the first dispatch


COMMIT_FAILURE_CASES = [
    # --- the host's transaction around the dispatch, no `unit_of_work` --------------------------------
    CommitFailureCase(
        "host_same_database_ledger_rolls_back_the_claim_with_the_work",
        MemoryIdempotencyStore,
        host_owns_the_transaction=True,
        error=edax.StoreError,
        runs=1,
    ),
    # The F01 refusals: each ledger would have completed before the commit that then failed.
    CommitFailureCase(
        "host_ledger_of_its_own_is_refused_before_the_claim",
        lambda db: MemoryLedger(),
        host_owns_the_transaction=True,
        error=edax.BadHandle,
        runs=0,
    ),
    CommitFailureCase(
        "host_joining_ledger_over_another_database_is_refused_before_the_claim",
        lambda db: MemoryIdempotencyStore(MemoryTransactor()),
        host_owns_the_transaction=True,
        error=edax.BadHandle,
        runs=0,
    ),
    CommitFailureCase(
        "host_redis_ledger_is_refused_before_the_claim",
        lambda db: redis_ledger(),
        host_owns_the_transaction=True,
        error=edax.BadHandle,
        runs=0,
    ),
    # --- the guard owns the transaction through `unit_of_work=` ---------------------------------------
    # A ledger over another database is refused before the commit is reached; see the test below.
    CommitFailureCase(
        "guard_shared_ledger_rolls_back_the_claim_with_the_work",
        MemoryIdempotencyStore,
        host_owns_the_transaction=False,
        error=edax.StoreError,
        runs=1,
    ),
    CommitFailureCase(
        "guard_ledger_of_its_own_releases_the_claim_after_the_failed_commit",
        lambda db: MemoryLedger(),
        host_owns_the_transaction=False,
        error=edax.StoreError,
        runs=1,
    ),
]


async def record_of(ledger: Any, key: str) -> Any:
    """The key's stored state: ``None`` when the key is free, else its record's state. A free key is read
    by claiming it, so the probe releases that claim again and leaves the ledger as it found it."""
    claim = await ledger.claim(key, 0)
    if isinstance(claim, Won):
        await ledger.release(key, claim.handle)
        return None
    return claim.state


@pytest.mark.parametrize("guard", ["idempotent", "idempotent_by_id"])
@pytest.mark.parametrize("case", COMMIT_FAILURE_CASES, ids=lambda c: c.name)
async def test_a_failed_commit_after_the_handler_leaves_no_cached_success(
    case: CommitFailureCase, guard: str
) -> None:
    """The commit itself fails, after the guarded handler ran and returned. Nothing landed, so the key must
    stay free: a cached success there would replay a result whose work rolled back, and the redelivery would
    never redo it. A ledger that joins the transaction rolls its claim back with the work; a ledger of its
    own under ``unit_of_work=`` releases it after the failed commit; under the host's transaction it is
    refused before the claim, because it would complete before the commit."""
    db = MemoryTransactor()
    projections = MemoryProjectionStore(db)
    ledger = case.ledger(db)
    unit = BrokenCommit(db, "nothing_landed")
    guard_unit = None if case.host_owns_the_transaction else unit
    runs = 0

    async def handler(message: Any) -> int:
        nonlocal runs
        runs += 1
        await projections.save(Entity(DOC, 0, {"id": DOC, "balance": 100}))
        return 100

    if guard == "idempotent":
        bus = CommandBus(idempotent(ledger, scope=SCOPE, unit_of_work=guard_unit, tenancy=edax.Tenancy.SILO))
        bus.register(Deposit, handler)
        key = claimed("req-A")

        async def dispatch() -> Any:
            return await bus.dispatch(Deposit(100, "req-A"), idempotency_key="req-A")
    else:
        router = edax.EventRouter(
            idempotent_by_id(ledger, unit_of_work=guard_unit, scope=SCOPE, tenancy=edax.Tenancy.SILO)
        )

        async def on_deposited(message: edax.EventMessage) -> None:
            await handler(message)

        router.register("Deposited", on_deposited)
        env = build_message(
            DOC, "Deposited", 1, id="evt-1", data=b'{"amount":100}', recorded_at=1_714_564_800_000_000_000
        )
        key = event_claimed(env.id)

        async def dispatch() -> Any:
            return await router.dispatch(env)

    with pytest.raises(case.error) as caught:
        if case.host_owns_the_transaction:
            async with edax.transaction(unit):
                await dispatch()
        else:
            await dispatch()
    assert type(caught.value) is case.error, case.name
    assert runs == case.runs, case.name
    assert await projections.get(DOC) is None, "the failed commit left no business write"
    assert await record_of(ledger, key) is None, "no cached success over work that rolled back"

    unit.arm = None  # the redelivery commits cleanly: outside any transaction, or in the guard's own
    await dispatch()
    assert runs == case.runs + 1, "the key was free, so the redelivery ran the work"
    assert await projections.get(DOC) == Entity(DOC, 1, {"id": DOC, "balance": 100}), (
        "the redelivered work landed"
    )
    stored = await record_of(ledger, key)
    expected_result = 100 if guard == "idempotent" else None
    assert isinstance(stored, Done) and stored.result == expected_result, (
        "only now does the key record the success"
    )


async def test_losing_the_claim_never_releases_the_winners_key() -> None:
    """A loser's verdict raises from inside the guard, and an over-eager compensation there would
    delete the *winner's* live claim. Only the caller that won the claim may settle it."""
    ledger = MemoryLedger()
    db = MemoryTransactor()
    bus = CommandBus(idempotent(ledger, scope="test", unit_of_work=db, tenancy=edax.Tenancy.SILO))
    bus.register(Deposit, _never_completes)

    # A claim taken by someone else and still pending: the guard must report it, not clear it.
    await ledger.claim(claimed("req-A"), _payload_hash(Deposit(100, "req-A"), Deposit.type))
    with pytest.raises(edax.InProgress):
        await bus.dispatch(Deposit(100, "req-A"), idempotency_key="req-A")

    still_held = await ledger.claim(claimed("req-A"), _payload_hash(Deposit(100, "req-A"), Deposit.type))
    assert not isinstance(still_held, Won) and isinstance(still_held.state, Pending), (
        "the winner's claim survived the loser"
    )


class ClaimRaisesLedger:
    """A ledger whose ``claim`` reports contention as a failure instead of handing back the prior
    record — a backend that raises a serialization failure rather than blocking and re-reading.
    Records every ``release`` so a test can prove the guard made none."""

    def __init__(self) -> None:
        self._inner = MemoryLedger()
        self.raise_on_claim = False
        self.releases: list[str] = []

    async def claim(self, key: str, payload_hash: int, tx: Any | None = None) -> Claimed:
        if self.raise_on_claim:
            raise edax.StoreError("could not serialize access to the idempotency table")
        return await self._inner.claim(key, payload_hash, tx=tx)

    async def complete(self, key: str, handle: bytes, record: Record, tx: Any | None = None) -> None:
        await self._inner.complete(key, handle, record, tx=tx)

    async def release(self, key: str, handle: bytes, tx: Any | None = None) -> None:
        self.releases.append(key)
        await self._inner.release(key, handle, tx=tx)

    async def forget(self, key: str, tx: Any | None = None) -> None:
        await self._inner.forget(key, tx=tx)


async def test_a_claim_that_raises_is_not_compensated() -> None:
    """The rule stated at ``edax/idempotency.py`` above ``store.claim``: a claim that *raises* is never
    released. The guard cannot tell "my insert landed and the response was lost" from "another
    claimant holds this key and the backend reported the contention", and releasing in the second
    case deletes the winner's live claim. Retention settles the first case instead."""
    ledger = ClaimRaisesLedger()
    db = MemoryTransactor()
    bus = CommandBus(idempotent(ledger, scope="test", unit_of_work=db, tenancy=edax.Tenancy.SILO))
    bus.register(Deposit, _never_completes)

    cmd = Deposit(100, "req-A")
    await ledger.claim(claimed("req-A"), _payload_hash(cmd, cmd.type))  # the winner, still pending

    ledger.raise_on_claim = True
    with pytest.raises(edax.StoreError):
        await bus.dispatch(cmd, idempotency_key=cmd.request_id)

    assert ledger.releases == [], "a failed claim must not be compensated"
    ledger.raise_on_claim = False
    still_held = await ledger.claim(claimed("req-A"), _payload_hash(cmd, cmd.type))
    assert not isinstance(still_held, Won) and isinstance(still_held.state, Pending), (
        "the winner's claim survived the loser"
    )


class InsufficientFunds(Exception):
    """A host's domain error: deterministic, so the kernel's callers must see it as terminal."""

    terminal: ClassVar[bool] = True


def RefusingMemoryLedger() -> Any:
    """The split-store twin, where no rollback is coming and retention is what frees the key."""
    return Refusing(MemoryLedger(), "complete", "release")


@dataclass
class CompensationCase:
    handler_error: Exception
    shared: bool  # a ledger that joins the host's transaction, or one of its own
    # A terminal failure is compensated by `complete`, which falls back to `release` when refused, so
    # it meets the refusing store twice; a transient failure only releases.
    refusals: int


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            CompensationCase(InsufficientFunds("balance is 0"), shared=True, refusals=2),
            id="ambient_terminal_error_keeps_its_verdict",
        ),
        pytest.param(
            CompensationCase(RuntimeError("the downstream service timed out"), shared=True, refusals=1),
            id="ambient_transient_error_keeps_its_type",
        ),
        pytest.param(
            CompensationCase(RuntimeError("the downstream service timed out"), shared=False, refusals=1),
            id="own_ledger_refused_release_keeps_the_handlers_error",
        ),
    ],
)
async def test_a_refused_compensation_never_replaces_the_handlers_error(
    case: CompensationCase,
) -> None:
    """The handler's exception is the verdict the caller, :func:`~edax.command.retry` and the
    dispatcher branch on. A compensating write the store refuses must never take its place.

    This is the README's own wiring: the host opens the transaction around the dispatch and passes no
    ``unit_of_work``, so the guard compensates by hand — inside a transaction the handler's failed
    statement has already aborted. Letting that refusal through re-typed a terminal domain error as a
    transient ``StoreError``, so ``retry`` re-ran a deterministic failure and a dispatcher nacked to
    ``max_attempts`` instead of dead-lettering on the first attempt.
    """
    db = MemoryTransactor()
    ledger: Any = RefusingLedger(db) if case.shared else RefusingMemoryLedger()
    runs = 0

    async def handler(cmd: Deposit) -> None:
        nonlocal runs
        runs += 1
        raise case.handler_error

    bus = CommandBus(idempotent(ledger, scope="test", tenancy=edax.Tenancy.SILO))
    bus.register(Deposit, handler)
    cmd = Deposit(100, "req-A")

    with pytest.raises(type(case.handler_error)) as caught:
        if case.shared:
            async with edax.transaction(db):
                await bus.dispatch(cmd, idempotency_key=cmd.request_id)
        else:
            await bus.dispatch(cmd, idempotency_key=cmd.request_id)

    assert caught.value is case.handler_error, "the handler's own exception, not a copy of it"
    assert edax.is_terminal(caught.value) is edax.is_terminal(case.handler_error), "verdict survived"
    assert runs == 1
    assert ledger.refusals == case.refusals, "the guard compensated; the store is what refused"

    if case.shared:
        # The host's rollback is what frees the claim, so the key is clear for a redelivery.
        assert isinstance(await ledger.claim(claimed("req-A"), _payload_hash(cmd, cmd.type)), Won)
    else:
        # No rollback is coming: the claim stays pending until the store's retention frees it. That
        # is the documented residual cost of a ledger of its own, not a leak.
        with pytest.raises(edax.InProgress):
            await bus.dispatch(cmd, idempotency_key=cmd.request_id)
        assert runs == 1, "the pending claim blocked the redelivery"


async def test_joins_transaction_defaults_to_false_for_an_undeclared_store() -> None:
    """The flag is a declaration the guard branches on and cannot detect. An undeclared store gets
    the compensating path, because a needless release costs one no-op delete while a skipped one
    leaves a ``done`` record over work that rolled back."""

    # ClaimRaisesLedger declares nothing, exactly like a store written before the attribute existed.
    assert joins_transaction(ClaimRaisesLedger()) is False
    assert joins_transaction(MemoryIdempotencyStore()) is True
    assert joins_transaction(MemoryLedger()) is False, "the shipped ledger of its own declares it"


async def _never_completes(cmd: Any) -> None:
    raise AssertionError("the handler must not run when the claim is already held")


# =========================================================================
# The property both modes owe, over random histories
# =========================================================================


@dataclass
class Op:
    """One guarded dispatch: which key, how the handler ends, and how its commit ends."""

    key: int
    outcome: int  # 0 success, 1 transient failure, 2 terminal failure
    commit: int  # 0 clean, 1 commit fails with nothing landed, 2 commit lands and loses its reply


op = st.builds(Op, st.integers(0, 2), st.integers(0, 2), st.sampled_from([0, 0, 0, 1, 2]))


class History:
    """Replays a history of guarded dispatches and reports, per key, what the ledger holds and how
    many times the handler's write actually landed. The handler increments a per-key counter in the
    projection store, so "landed" is read back out of the same unit of work it was written in —
    not inferred from whether the dispatch raised."""

    def __init__(self, shared: bool) -> None:
        self.db = MemoryTransactor()
        self.projections = MemoryProjectionStore(self.db)
        self.ledger: Any = MemoryIdempotencyStore(self.db) if shared else MemoryLedger()
        self.unit = BrokenCommit(self.db, None)
        self.shared = shared

        async def handler(cmd: Deposit) -> int:
            doc = f"acct-{cmd.request_id}"
            entity = await self.projections.get(doc)
            if entity is None:
                entity = Entity(doc, 0, {"id": doc, "applied": 0})
            entity.document["applied"] = int(entity.document["applied"]) + 1
            await self.projections.save(entity)
            if cmd.amount == 1:
                raise RuntimeError("transient")
            if cmd.amount == 2:
                raise edax.CodecError("terminal")
            return int(entity.document["applied"])

        self.bus = CommandBus(
            idempotent(self.ledger, scope="test", unit_of_work=self.unit, tenancy=edax.Tenancy.SILO)
        )
        self.bus.register(Deposit, handler)

    async def run(self, ops: list[Op]) -> None:
        for o in ops:
            self.unit.arm = (None, "nothing_landed", "lost_response")[o.commit]
            # Every outcome is modelled by the assertions below, not by what the dispatch raised.
            with contextlib.suppress(Exception):
                await self.bus.dispatch(Deposit(o.outcome, str(o.key)), idempotency_key=str(o.key))

    async def applied(self, key: int) -> int:
        entity = await self.projections.get(f"acct-{key}")
        return 0 if entity is None else int(entity.document["applied"])

    async def record(self, key: int) -> Any:
        """The key's stored record, read by claiming it. Only ever called after the history is
        over, so taking the claim cannot perturb what it reports."""
        return await self.ledger.claim(claimed(str(key)), _payload_hash(Deposit(0, str(key)), Deposit.type))


@given(ops=st.lists(op, max_size=8), shared=st.booleans())
@settings(max_examples=300, deadline=None)
def test_a_done_record_never_covers_work_that_did_not_land(ops: list[Op], shared: bool) -> None:
    """The invariant both modes owe: a key that reads back as a completed success must have had its
    handler's write land at least once. Without it, work drops silently.

    The converse separates the modes, and is asserted here too: with a shared unit of work the count
    is *exactly* one, because the claim and the write are one commit. With a ledger of its own it may
    be two, because a commit that landed and lost its reply releases the claim over work that
    survived. That is at-least-once, and the reason split mode may never be called effectively-once.
    """

    async def check() -> None:
        history = History(shared)
        await history.run(ops)
        for key in range(3):
            applied = await history.applied(key)
            record = await history.record(key)
            done = not isinstance(record, Won) and isinstance(record.state, Done)
            if done:
                assert applied >= 1, f"key {key}: a done record over work that never landed"
                if shared:
                    assert applied == 1, f"key {key}: shared mode applied the work {applied}x"
            elif shared:
                # Nothing survived: whatever ran rolled back with its claim.
                assert applied == 0, f"key {key}: work landed without a record to show for it"

    asyncio.run(check())


# =========================================================================
# a ledger over another database than the guard's unit of work
# =========================================================================


@pytest.mark.parametrize("guard", ["idempotent", "idempotent_by_id"])
@pytest.mark.parametrize("handler_fails", [False, True], ids=["handler_succeeds", "handler_fails"])
async def test_a_ledger_over_another_database_than_the_unit_of_work_is_refused(
    guard: str, handler_fails: bool
) -> None:
    """A ``joins_transaction`` ledger wired over another database than ``unit_of_work``: the guard's
    transaction is not the ledger's, so the claim must not land outside it. Left to find no ambient tx of
    its own, the store would auto-commit: a success would leave a ``done`` record the guard's commit never
    covered, and a failed handler would roll back its work and leave the key pending for good. The guard
    hands the store its transaction, which the store refuses with the terminal ``BadHandle`` before the
    handler runs, so the dispatcher stops on the wiring mistake and hands the message back."""
    db, other = MemoryTransactor(), MemoryTransactor()
    ledger = MemoryIdempotencyStore(other)
    runs = 0

    async def handler(message: Any) -> None:
        nonlocal runs
        runs += 1
        if handler_fails:
            raise RuntimeError("projection store unavailable")

    env = build_message(
        DOC, "Deposited", 1, id="evt-1", data=b'{"amount":100}', recorded_at=1_714_564_800_000_000_000
    )
    if guard == "idempotent":
        bus = CommandBus(idempotent(ledger, scope="test", unit_of_work=db, tenancy=edax.Tenancy.SILO))
        bus.register(Deposit, handler)
        dispatched, key = bus.dispatch(Deposit(100, "req-A"), idempotency_key="req-A"), claimed("req-A")
    else:
        router = edax.EventRouter(
            idempotent_by_id(ledger, unit_of_work=db, scope=SCOPE, tenancy=edax.Tenancy.SILO)
        )
        router.register("Deposited", handler)
        dispatched, key = router.dispatch(env), event_claimed(env.id)
    with pytest.raises(edax.BadHandle, match="not a transaction of this MemoryTransactor"):
        await dispatched
    assert runs == 0, "refused at the claim, before the handler ran"
    assert isinstance(await ledger.claim(key, 0), Won), "no claim landed outside the guard's transaction"
    assert (db.open_count(), other.open_count()) == (0, 0), "the guard's transaction rolled back"

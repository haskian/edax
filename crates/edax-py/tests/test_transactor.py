"""Transactor seam: transaction scopes commit/rollback, publishes the ambient current_tx, and threads
one tx to every participant (events via Repository, projection, idempotency, saga).

The second half executes the transaction contract against the in-memory backend: a raise leaves
nothing behind, a rolled-back claim releases its key, a transaction reads its own writes that
nobody else sees, and a commit lands every participant's write together. The Rust SDK runs the
same four (`crates/edax/tests/transaction.rs`), because the contract is cross-language."""

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, ClassVar, Optional

import edax
import pytest
from conftest import Counter, Tick, build_envelope, counter_repo
from edax import EventEnvelope, KeyOrderedDispatcher, Snapshot, _edax, current_tx, transaction
from edax.backend.memory import (
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemoryProjectionStore,
    MemorySagaStore,
    MemoryTransactor,
)
from edax.command import CommandBus, Record, idempotent
from edax.projection import Entity
from edax.store import Tx


class CountingTransactor:
    """A Transactor handing out a fresh tx per unit of work, counting the boundary calls so a test
    can assert transaction committed once / rolled back once."""

    def __init__(self) -> None:
        self.begun = self.committed = self.rolled_back = 0

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[str]:
        self.begun += 1
        tx = f"tx-{self.begun}"
        try:
            yield tx
        except BaseException:
            self.rolled_back += 1
            raise
        else:
            self.committed += 1


async def test_transaction_commits_and_publishes_current_tx() -> None:
    tr = CountingTransactor()
    assert current_tx() is None
    async with transaction(tr) as tx:
        assert current_tx() == tx  # ambient, for participants that take no explicit tx
    assert (tr.committed, tr.rolled_back) == (1, 0)
    assert current_tx() is None  # reset on exit


async def test_transaction_rolls_back_on_error() -> None:
    tr = CountingTransactor()
    with pytest.raises(RuntimeError):
        async with transaction(tr):
            raise RuntimeError("boom")
    assert (tr.committed, tr.rolled_back) == (0, 1)
    assert current_tx() is None


async def test_one_tx_threaded_to_projection_and_idempotency() -> None:
    db = MemoryTransactor()
    proj, idem = MemoryProjectionStore(db), MemoryIdempotencyStore(db)
    async with transaction(db) as tx:
        await proj.save(Entity("u1", 0, {"id": "u1", "n": 1}), tx=tx)  # explicit
        assert await idem.claim("k1", 1, tx=current_tx()) is None  # ambient: won the claim
        await idem.complete("k1", Record(payload_hash=1, result=42), tx=current_tx())
        assert await proj.get("u1", tx=db.auto_commit()) is None, "staged, not landed"
    stored = await proj.get("u1")
    assert stored is not None, "the committed tx must have landed the projection row"
    assert stored.document["n"] == 1
    prior = await idem.claim("k1", 1)  # replay: the completed record comes back
    assert prior is not None and prior.result == 42


async def test_a_memory_store_refuses_a_foreign_tx() -> None:
    # A handle from another transactor (or a fake) names a unit of work this store's database
    # knows nothing about: enlisting "anyway" would silently drop the atomicity the caller asked
    # for, so the store fails fast, whether the handle is explicit or ambient.
    proj = MemoryProjectionStore()
    other = MemoryTransactor()
    async with transaction(other) as tx:
        with pytest.raises(edax.BadHandle, match="not a transaction of this MemoryTransactor"):
            await proj.save(Entity("u1", 0, {"n": 1}), tx=tx)
        with pytest.raises(edax.BadHandle, match="not a transaction of this MemoryTransactor"):
            await proj.get("u1")  # ambient
    async with transaction(CountingTransactor()):
        with pytest.raises(edax.BadHandle):
            await proj.get("u1")
    assert await proj.get("u1") is None, "outside any unit of work the store auto-commits"
    # Terminal, and the Rust adapter agrees: retrying cannot fix a store wired to the wrong
    # transactor, so the dispatcher must dead-letter rather than redeliver forever.
    assert edax.BadHandle("x").terminal is True
    assert edax.is_terminal(edax.BadHandle("x"))


class RecordingStore:
    """An EventStore that is its own Transactor and records the tx each call received, so a test
    can prove Repository threaded the caller's tx (not one of its own)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[str]:
        yield "store-own-tx"

    async def append(
        self, tx: Tx, aggregate_id: str, expected_version: int, events: Sequence[EventEnvelope]
    ) -> None:
        self.calls.append(("append", tx))

    async def save_snapshot(self, tx: Tx, snapshot: Snapshot) -> None:
        self.calls.append(("snapshot", tx))

    async def load_snapshot(self, tx: Tx, aggregate_id: str) -> Optional[Snapshot]:
        self.calls.append(("load_snapshot", tx))
        return None

    async def load(self, tx: Tx, aggregate_id: str, after: int = 0) -> list[EventEnvelope]:
        self.calls.append(("load", tx))
        return []


def _repo_with_pending() -> tuple[RecordingStore, edax.Repository[Counter], Counter]:
    store = RecordingStore()
    acct = Counter("a1")
    acct.apply(Tick(5))
    return store, counter_repo(store, iter(range(1, 10))), acct


async def test_repository_save_enlists_caller_tx() -> None:
    store, repo, acct = _repo_with_pending()
    tr = CountingTransactor()
    async with transaction(tr) as tx:
        await repo.save(acct, tx=tx)  # enlist in the caller's unit of work

    assert [op for op, _ in store.calls] == ["append", "snapshot"]
    assert all(seen == tx for _, seen in store.calls)  # the caller's tx, not "store-own-tx"
    assert tr.committed == 1


async def test_repository_save_opens_its_own_tx_when_none_passed() -> None:
    store, repo, acct = _repo_with_pending()
    await repo.save(acct)  # no tx, no open unit of work -> store.transaction()

    assert [op for op, _ in store.calls] == ["append", "snapshot"]
    assert all(seen == "store-own-tx" for _, seen in store.calls)


async def test_repository_save_joins_ambient_tx_without_threading() -> None:
    # The `async with transaction(store): await repo.save(agg)` form: no tx passed, yet the events
    # enlist in the caller's open unit of work rather than opening a nested one.
    store, repo, acct = _repo_with_pending()
    tr = CountingTransactor()
    async with transaction(tr) as tx:
        await repo.save(acct)  # no tx arg -> joins current_tx()

    assert all(seen == tx for _, seen in store.calls)  # the ambient tx, not "store-own-tx"
    assert tr.committed == 1


async def test_repository_load_joins_ambient_tx() -> None:
    # load mirrors save: no tx passed, yet the reads enlist in the caller's open unit of work
    # rather than opening a nested transaction.
    store, repo, _ = _repo_with_pending()
    tr = CountingTransactor()
    async with transaction(tr) as tx:
        await repo.load("a1")  # no tx arg -> joins current_tx(), opens no nested store tx

    reads = [(op, seen) for op, seen in store.calls if op in ("load_snapshot", "load")]
    assert reads and all(seen == tx for _, seen in reads)  # ambient tx, never "store-own-tx"
    assert tr.committed == 1


@dataclass
class NestCase:
    name: str
    same_transactor: bool  # the inner transaction() runs on the outer's transactor
    refused: bool
    committed: int  # units of work the *outer* transactor committed


@pytest.mark.parametrize(
    "case",
    [
        NestCase("same transactor is refused", same_transactor=True, refused=True, committed=1),
        NestCase("another database still nests", same_transactor=False, refused=False, committed=1),
    ],
    ids=lambda c: c.name,
)
async def test_nesting_a_transaction_on_one_transactor_is_refused(case: NestCase) -> None:
    # Nobody writes transaction() inside transaction() on purpose; they call one service function
    # that opens a unit of work from another that already did. The inner one commits on its own
    # clean exit and the outer rollback cannot take it back, so the guard refuses it. Two
    # transactors are two databases: nesting them is legitimate, and neither commit is atomic with
    # the other's.
    outer = CountingTransactor()
    inner = outer if case.same_transactor else CountingTransactor()
    async with transaction(outer) as tx:
        if case.refused:
            with pytest.raises(edax.BadHandle, match="already has one open"):
                async with transaction(inner):
                    pass  # pragma: no cover - the guard raises before the body runs
        else:
            async with transaction(inner):
                assert current_tx() is not None
        assert current_tx() == tx, "a refused or finished inner block leaves the outer tx ambient"
    assert (outer.committed, outer.rolled_back) == (case.committed, 0)
    if not case.same_transactor:
        assert (inner.committed, inner.rolled_back) == (1, 0)


async def test_one_transactor_opens_a_second_unit_of_work_after_the_first_closes() -> None:
    # The guard is about nesting, not about reuse: sequential units of work on one transactor are
    # the ordinary case, so the ambient owner must be reset on exit, including after a rollback.
    tr = CountingTransactor()
    with pytest.raises(RuntimeError):
        async with transaction(tr):
            raise RuntimeError("boom")
    async with transaction(tr):
        pass
    assert (tr.committed, tr.rolled_back) == (1, 1)


async def test_dispatcher_workers_do_not_inherit_the_ambient_tx() -> None:
    # A worker task copies its creator's context. Started inside transaction, it would otherwise carry
    # that tx to every handler it ever runs: after the tx committed, and from a second task, which
    # the Transactor contract forbids. Each worker is its own unit-of-work boundary.
    seen: list[object] = []
    tr = CountingTransactor()

    async def handle(env: EventEnvelope) -> None:
        seen.append(current_tx())
        # The worker inherits no owner either, so its own unit of work on that same transactor is
        # not read as nesting inside the caller's.
        async with transaction(tr):
            pass

    dispatcher = KeyOrderedDispatcher(workers=1, handler=handle)
    async with transaction(tr):
        dispatcher.start()
        await dispatcher.submit(build_envelope("acct-1", "Deposited", 1))
    await dispatcher.stop()
    assert seen == [None]
    assert (tr.committed, tr.rolled_back) == (2, 0), "the caller's unit of work and the worker's"


async def test_current_tx_is_per_task_not_process_wide() -> None:
    # A task that exists before the unit of work never sees its tx (contextvars are copied at task
    # creation), while a task spawned inside the block inherits it: a worker started from within
    # a transaction block enlists in that tx, and a sibling never does.
    tr = CountingTransactor()
    entered, leave = asyncio.Event(), asyncio.Event()

    async def sibling() -> object:
        await entered.wait()
        return current_tx()

    async def child() -> object:
        await leave.wait()
        return current_tx()

    sibling_task = asyncio.create_task(sibling())
    async with transaction(tr) as tx:
        child_task = asyncio.create_task(child())
        entered.set()
        assert await sibling_task is None
    leave.set()
    assert await child_task == tx
    assert current_tx() is None


class AmbientRecordingStore(RecordingStore):
    """Records the ambient ``current_tx()`` each seam call saw, not just the threaded one."""

    def __init__(self) -> None:
        super().__init__()
        self.ambient: list[object] = []

    async def append(
        self, tx: Tx, aggregate_id: str, expected_version: int, events: Sequence[EventEnvelope]
    ) -> None:
        self.ambient.append(current_tx())
        await super().append(tx, aggregate_id, expected_version, events)

    async def load_snapshot(self, tx: Tx, aggregate_id: str) -> Optional[Snapshot]:
        self.ambient.append(current_tx())
        return await super().load_snapshot(tx, aggregate_id)


@pytest.mark.parametrize("branch", ["save", "load"])
async def test_repository_publishes_its_own_tx_as_the_ambient_one(branch: str) -> None:
    # Repository opens its unit of work through transaction, so a participant that takes no explicit tx
    # (an IdempotencyStore, a ProjectionStore) still enlists in it — and the contextvar is reset
    # afterwards, so the tx never leaks past the call that owns it.
    store = AmbientRecordingStore()
    acct = Counter("a1")
    acct.apply(Tick(5))
    repo = counter_repo(store, iter(range(1, 10)))

    assert current_tx() is None
    if branch == "save":
        await repo.save(acct)
    else:
        await repo.load("a1")

    assert store.ambient == ["store-own-tx"]  # published for the duration of the repo's own tx
    assert current_tx() is None  # and reset on exit, not left dangling


# =========================================================================
# The transaction contract, executed against the in-memory backend
# =========================================================================


@dataclass
class Deposit:
    account_id: str
    amount: int
    request_id: str
    type: ClassVar[str] = "Deposit"


def _request_id(cmd: Any) -> str:
    return cmd.request_id


class Rig:
    """Every participant over one transactor, plus a guarded bus whose handler saves through the
    repository: the README's topology, so the contract is executed through the SDK's own
    orchestration and not through the stores alone."""

    def __init__(self) -> None:
        self.db = MemoryTransactor()
        self.store = MemoryEventStore(self.db)
        self.proj = MemoryProjectionStore(self.db)
        self.sagas = MemorySagaStore(self.db)
        self.claims = MemoryIdempotencyStore(self.db)
        self.repo = counter_repo(self.store, iter(range(1, 100)))
        self.calls = 0
        self.bus = CommandBus(idempotent(self.claims, key=_request_id))
        self.bus.register("Deposit", self._deposit)

    async def _deposit(self, cmd: Deposit) -> int:
        self.calls += 1
        acct = await self.repo.load(cmd.account_id)  # joins the ambient tx
        acct.apply(Tick(cmd.amount))
        await self.repo.save(acct)
        return acct.n

    async def write_everything(self) -> None:
        """One write on every participant, inside the ambient unit of work."""
        await self.bus.dispatch(Deposit("acct-1", 100, "req-A"))
        await self.proj.save(Entity("acct-1", 0, {"balance": 100}))
        await self.sagas.save("order-1", {"status": "open"}, 0)

    async def visible(self) -> tuple[int, int, bool, bool, bool, bool]:
        """What a reader outside any transaction sees of every participant:
        ``(events, outbox rows, snapshot, projection row, claim taken, saga instance)``."""
        none = self.db.auto_commit()  # looks past the ambient tx, whatever it is
        # A losing claim proves the key is taken; a winning probe must be undone, so it runs in a
        # transaction of its own that is rolled back.
        try:
            async with self.db.transaction() as probe:
                taken = await self.claims.claim("req-A", 0, tx=probe) is not None
                raise _Undo
        except _Undo:
            pass
        return (
            len(await self.store.load(none, "acct-1", 0)),
            len(await self.store.unpublished()),
            await self.store.load_snapshot(none, "acct-1") is not None,
            await self.proj.get("acct-1", tx=none) is not None,
            taken,
            await self.sagas.load("order-1", tx=none) is not None,
        )


class _Undo(Exception):
    """Unwinds the claim probe so its transaction rolls back."""


NOTHING = (0, 0, False, False, False, False)
EVERYTHING = (1, 1, True, True, True, True)
# Mid-transaction: no data is visible outside it, but the claim key is *held*, so a probe claimant
# loses and reads pending. That is the one footprint a transaction shows before it commits, and it
# is the point of an atomic claim: a concurrent dispatch of the same request must wait for the
# winner's outcome, never run the handler a second time.
HELD = (0, 0, False, False, True, False)


async def test_a_raise_inside_a_transaction_leaves_nothing_behind() -> None:
    rig = Rig()
    with pytest.raises(RuntimeError, match="a later write"):
        async with transaction(rig.db):
            await rig.write_everything()
            raise RuntimeError("a later write in the same unit of work failed")
    assert rig.calls == 1, "the handler ran once"
    assert await rig.visible() == NOTHING


async def test_a_rolled_back_claim_releases_the_key_so_the_next_dispatch_reruns_the_handler() -> None:
    rig = Rig()
    with pytest.raises(RuntimeError):
        async with transaction(rig.db):
            await rig.bus.dispatch(Deposit("acct-1", 100, "req-A"))
            raise RuntimeError("a later write in the same unit of work failed")
    assert rig.calls == 1

    async with transaction(rig.db):
        balance = await rig.bus.dispatch(Deposit("acct-1", 100, "req-A"))
    assert rig.calls == 2, "the handler ran again: no phantom cached result"
    assert balance == 100, "and deposited once, not twice"
    assert len(await rig.store.load(None, "acct-1", 0)) == 1

    # Now the claim is committed: a third dispatch replays without running the handler.
    async with transaction(rig.db):
        assert await rig.bus.dispatch(Deposit("acct-1", 100, "req-A")) == 100
    assert rig.calls == 2


async def test_a_transaction_reads_its_own_uncommitted_writes_which_nobody_else_sees() -> None:
    rig = Rig()
    async with transaction(rig.db) as tx:
        acct = await rig.repo.load("acct-1", tx=tx)
        acct.apply(Tick(40))
        await rig.repo.save(acct, tx=tx)
        assert (await rig.repo.load("acct-1", tx=tx)).n == 40, "read-your-writes through the repository"
        assert len(await rig.store.load(tx, "acct-1", 0)) == 1, "read-your-writes through the store"
        assert await rig.visible() == NOTHING, "nothing is visible outside before the commit"
    assert (await rig.repo.load("acct-1")).n == 40, "visible once committed"


async def test_a_commit_makes_every_participants_write_visible_together() -> None:
    rig = Rig()
    async with transaction(rig.db):
        await rig.write_everything()
        assert await rig.visible() == HELD, "nothing landed before the commit; the claim key is held"
    assert await rig.visible() == EVERYTHING
    assert rig.calls == 1


async def test_a_cancelled_unit_of_work_rolls_back() -> None:
    # Cancellation is a BaseException, not an Exception: the boundary must still roll back, or a
    # wait_for timeout would leave half a unit of work committed.
    rig = Rig()
    started = asyncio.Event()

    async def body() -> None:
        async with transaction(rig.db):
            await rig.write_everything()
            started.set()
            await asyncio.Event().wait()  # parked until cancelled

    task = asyncio.create_task(body())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await rig.visible() == NOTHING


# A real 64-bit payload hash, past 2^53.
HASH = 11_234_062_831_410_934_249
DONE = Record(payload_hash=HASH, result=42)


@pytest.mark.parametrize(
    ("branch", "write"),
    [
        ("complete", lambda claims, tx: claims.complete("req-A", DONE, tx=tx)),
        ("release", lambda claims, tx: claims.release("req-A", tx=tx)),
    ],
)
async def test_completing_or_releasing_a_held_claim_conflicts(branch: str, write: Any) -> None:
    # Exclusion covers the whole claim, not just the claim call: the holder's commit would overwrite
    # a terminal record written under another token, and a losing claimant's release would free the
    # winner's key. Redis fences both; the memory backend now refuses both.
    rig = Rig()
    async with rig.db.transaction() as holder:
        assert await rig.claims.claim("req-A", HASH, tx=holder) is None, "the holder wins the key"
        async with rig.db.transaction() as other:
            with pytest.raises(edax.ConcurrencyError):
                await write(rig.claims, other)
        with pytest.raises(edax.ConcurrencyError):
            await write(rig.claims, rig.db.auto_commit())
        # The holder is never blocked by its own staged claim.
        await write(rig.claims, holder)


def test_the_auto_commit_token_comes_from_the_native_binding() -> None:
    # Not a Python-side literal: `edax_abi::NO_TX` is the one owner of the value.
    assert MemoryTransactor().auto_commit().token == _edax.no_tx()


async def test_open_count_reports_unsettled_transactions() -> None:
    db = MemoryTransactor()
    assert db.open_count() == 0, "a fresh transactor holds nothing open"
    async with db.transaction():
        assert db.open_count() == 1, "the unit of work is open inside its block"
        async with db.transaction():
            assert db.open_count() == 2, "each begin opens one"
    assert db.open_count() == 0, "a commit closes its transaction"

    with pytest.raises(RuntimeError, match="rolled back"):
        async with db.transaction():
            raise RuntimeError("rolled back")
    assert db.open_count() == 0, "a rollback closes its transaction too"

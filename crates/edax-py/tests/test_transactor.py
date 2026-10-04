"""Transactor seam: transaction scopes commit/rollback, publishes the ambient current_tx, and threads
one tx to every participant (events via Repository, projection, idempotency, saga).

The second half executes the transaction contract against the in-memory backend: a raise leaves
nothing behind, a rolled-back claim releases its key, a transaction reads its own writes that
nobody else sees, and a commit lands every participant's write together. The Rust SDK runs the
same four (`crates/edax/tests/transaction.rs`), because the contract is cross-language."""

import asyncio
import contextlib
import threading
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, ClassVar

import edax
import pytest
from conftest import Counter, FakeKafkaClient, Tick, build_message, claimed, counter_repo
from edax import EventMessage, KeyOrderedDispatcher, Snapshot, _edax, current_tx, transaction
from edax._owned import ThreadBound
from edax.backend.kafka import KafkaConsumer
from edax.backend.memory import (
    MemoryBroker,
    MemoryEventStore,
    MemoryIdempotencyStore,
    MemoryProjectionStore,
    MemorySagaStore,
    MemoryTransactor,
)
from edax.command import CommandBus, Record, Won, idempotent
from edax.projection import Entity
from edax.store import Tx
from edax.testing import drafted
from edax.testing.faults import BrokenCommit, LostResponse


class CountingTransactor:
    """A Transactor handing out a fresh tx per unit of work, counting the boundary calls so a test
    can assert transaction committed once / rolled back once."""

    def __init__(self) -> None:
        self.begun = self.committed = self.rolled_back = 0

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[str]:
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
    assert current_tx(tr) is None
    async with transaction(tr) as tx:
        assert current_tx(tr) == tx  # ambient, for participants that take no explicit tx
        assert current_tx(CountingTransactor()) is None, "and for this database's participants only"
    assert (tr.committed, tr.rolled_back) == (1, 0)
    assert current_tx(tr) is None  # reset on exit


async def test_transaction_rolls_back_on_error() -> None:
    tr = CountingTransactor()
    with pytest.raises(RuntimeError):
        async with transaction(tr):
            raise RuntimeError("boom")
    assert (tr.committed, tr.rolled_back) == (0, 1)
    assert current_tx(tr) is None


async def test_one_tx_threaded_to_projection_and_idempotency() -> None:
    db = MemoryTransactor()
    proj, idem = MemoryProjectionStore(db), MemoryIdempotencyStore(db)
    async with transaction(db) as tx:
        await proj.save(Entity("u1", 0, {"id": "u1", "n": 1}), tx=tx)  # explicit
        won = await idem.claim("k1", 1, tx=current_tx(db))  # ambient: won the claim
        assert isinstance(won, Won)
        await idem.complete("k1", won.handle, Record(payload_hash=1, result=42), tx=current_tx(db))
        assert await proj.get("u1", tx=db.auto_commit()) is None, "staged, not landed"
    stored = await proj.get("u1")
    assert stored is not None, "the committed tx must have landed the projection row"
    assert stored.document["n"] == 1
    prior = await idem.claim("k1", 1)  # replay: the completed record comes back
    assert not isinstance(prior, Won) and prior.result == 42


async def test_a_memory_store_refuses_a_foreign_tx() -> None:
    # A handle from another transactor (or a fake) names a unit of work this store's database
    # knows nothing about: enlisting "anyway" would silently drop the atomicity the caller asked
    # for, so the store fails fast on an explicit one. Another database's ambient tx is not this
    # store's to see at all: the store runs outside any unit of work of its own database.
    proj = MemoryProjectionStore()
    other = MemoryTransactor()
    async with transaction(other) as tx:
        with pytest.raises(edax.BadHandle, match="not a transaction of this MemoryTransactor"):
            await proj.save(Entity("u1", 0, {"n": 1}), tx=tx)
        assert await proj.get("u1") is None  # the other database's ambient tx is invisible here
    async with transaction(CountingTransactor()):
        assert await proj.get("u1") is None
    with pytest.raises(edax.BadHandle):
        await proj.get("u1", tx="not-a-memory-tx")
    assert await proj.get("u1") is None, "outside any unit of work the store auto-commits"
    # Terminal, and the Rust adapter agrees: retrying cannot fix a store wired to the wrong
    # transactor, so the dispatcher must dead-letter rather than redeliver forever.
    assert edax.BadHandle("x").terminal is True
    assert edax.is_terminal(edax.BadHandle("x"))


class RecordingStore:
    """An EventStore that is its own Transactor and records the tx each call received, so a test
    can prove Repository threaded the caller's tx (not one of its own)."""

    def __init__(self, transactor: object = None) -> None:
        self.calls: list[tuple[str, object]] = []
        #: The database this store's units of work belong to, when it shares one with a transactor.
        self.transactor = transactor

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[str]:
        yield "store-own-tx"

    async def append(
        self, tx: Tx, aggregate_id: str, expected_version: int, events: Sequence[EventMessage]
    ) -> None:
        self.calls.append(("append", tx))

    async def save_snapshot(self, tx: Tx, snapshot: Snapshot) -> None:
        self.calls.append(("snapshot", tx))

    async def load_snapshot(self, tx: Tx, aggregate_id: str) -> Snapshot | None:
        self.calls.append(("load_snapshot", tx))
        return None

    async def load(self, tx: Tx, aggregate_id: str, after: int = 0) -> list[EventMessage]:
        self.calls.append(("load", tx))
        return []


def _repo_with_pending(transactor: object = None) -> tuple[RecordingStore, edax.Repository[Counter], Counter]:
    store = RecordingStore(transactor)
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
    await repo.save(acct)  # no tx, no open unit of work -> store.begin()

    assert [op for op, _ in store.calls] == ["append", "snapshot"]
    assert all(seen == "store-own-tx" for _, seen in store.calls)


async def test_repository_save_joins_ambient_tx_without_threading() -> None:
    # The `async with transaction(db): await repo.save(agg)` form: no tx passed, yet the events
    # enlist in the caller's open unit of work over the store's database rather than opening a nested one.
    tr = CountingTransactor()
    store, repo, acct = _repo_with_pending(tr)
    async with transaction(tr) as tx:
        await repo.save(acct)  # no tx arg -> joins current_tx(store), the store's database

    assert all(seen == tx for _, seen in store.calls)  # the ambient tx, not "store-own-tx"
    assert tr.committed == 1


@pytest.mark.parametrize(
    "opener",
    [
        pytest.param(MemoryTransactor, id="memory_transactor"),
        pytest.param(MemoryEventStore, id="memory_event_store"),
        pytest.param(lambda: BrokenCommit(MemoryTransactor(), arm=None), id="broken_commit"),
        pytest.param(lambda: LostResponse(MemoryEventStore()), id="lost_response"),
    ],
)
def test_no_seam_opens_a_unit_of_work_it_does_not_publish(opener: Callable[[], object]) -> None:
    """``edax.transaction`` is the one opener named ``transaction``. A method of that name once opened the
    backend's unit of work without publishing it: a ``Repository.save`` given no tx inside it committed on its
    own and survived the block's rollback, and the nesting guard never fired. The hook is ``begin``."""
    built = opener()
    assert hasattr(built, "begin"), "the backend hook edax.transaction calls"
    assert not hasattr(built, "transaction"), "an opener that would bypass current_tx"


async def test_repository_load_joins_ambient_tx() -> None:
    # load mirrors save: no tx passed, yet the reads enlist in the caller's open unit of work
    # rather than opening a nested transaction.
    tr = CountingTransactor()
    store, repo, _ = _repo_with_pending(tr)
    async with transaction(tr) as tx:
        await repo.load("a1")  # no tx arg -> joins current_tx(store), opens no nested store tx

    reads = [(op, seen) for op, seen in store.calls if op in ("load_snapshot", "load")]
    assert reads and all(seen == tx for _, seen in reads)  # ambient tx, never "store-own-tx"
    assert tr.committed == 1


@pytest.mark.parametrize(
    "same_transactor",
    [True, False],
    ids=["same transactor is refused", "another database still nests"],
)
async def test_nesting_a_transaction_on_one_transactor_is_refused(same_transactor: bool) -> None:
    # Nobody writes transaction() inside transaction() on purpose; they call one service function
    # that opens a unit of work from another that already did. The inner one commits on its own
    # clean exit and the outer rollback cannot take it back, so the guard refuses it. Two
    # transactors are two databases: nesting them is legitimate, and neither commit is atomic with
    # the other's.
    outer = CountingTransactor()
    inner = outer if same_transactor else CountingTransactor()
    async with transaction(outer) as tx:
        if same_transactor:
            with pytest.raises(edax.BadHandle, match="already has one open"):
                async with transaction(inner):
                    pass  # pragma: no cover - the guard raises before the body runs
        else:
            async with transaction(inner) as inner_tx:
                assert current_tx(inner) == inner_tx and current_tx(outer) == tx, "each database its own"
            assert (inner.committed, inner.rolled_back) == (1, 0), "the second database committed"
        assert current_tx(outer) == tx, "a refused or finished inner block leaves the outer tx ambient"
    assert (outer.committed, outer.rolled_back) == (1, 0), "one unit of work either way"


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

    async def handle(env: EventMessage) -> None:
        seen.append(current_tx(tr))
        # The worker inherits no owner either, so its own unit of work on that same transactor is
        # not read as nesting inside the caller's.
        async with transaction(tr):
            pass

    dispatcher = KeyOrderedDispatcher(workers=1, handler=handle)
    async with transaction(tr):
        dispatcher.start()
        await dispatcher.submit(build_message("acct-1", "Deposited", 1))
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
        return current_tx(tr)

    async def child() -> object:
        await leave.wait()
        return current_tx(tr)

    sibling_task = asyncio.create_task(sibling())
    async with transaction(tr) as tx:
        child_task = asyncio.create_task(child())
        entered.set()
        assert await sibling_task is None
    leave.set()
    assert await child_task == tx
    assert current_tx(tr) is None


class AmbientRecordingStore(RecordingStore):
    """Records the ambient ``current_tx()`` each seam call saw, not just the threaded one."""

    def __init__(self) -> None:
        super().__init__()
        self.ambient: list[object] = []

    async def append(
        self, tx: Tx, aggregate_id: str, expected_version: int, events: Sequence[EventMessage]
    ) -> None:
        self.ambient.append(current_tx(self))
        await super().append(tx, aggregate_id, expected_version, events)

    async def load_snapshot(self, tx: Tx, aggregate_id: str) -> Snapshot | None:
        self.ambient.append(current_tx(self))
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

    assert current_tx(store) is None
    if branch == "save":
        await repo.save(acct)
    else:
        await repo.load("a1")

    assert store.ambient == ["store-own-tx"]  # published for the duration of the repo's own tx
    assert current_tx(store) is None  # and reset on exit, not left dangling


# =========================================================================
# The transaction contract, executed against the in-memory backend
# =========================================================================


@dataclass
class Deposit:
    account_id: str
    amount: int
    request_id: str
    type: ClassVar[str] = "Deposit"


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
        self.bus = CommandBus(idempotent(self.claims, scope="test"))
        self.bus.register("Deposit", self._deposit)

    async def _deposit(self, cmd: Deposit) -> int:
        self.calls += 1
        acct = await self.repo.load(cmd.account_id)  # joins the ambient tx
        acct.apply(Tick(cmd.amount))
        await self.repo.save(acct)
        return acct.n

    async def write_everything(self) -> None:
        """One write on every participant, inside the ambient unit of work."""
        await self.bus.dispatch(Deposit("acct-1", 100, "req-A"), idempotency_key="req-A")
        await self.proj.save(Entity("acct-1", 0, {"balance": 100}))
        await self.sagas.save("order-1", {"status": "open"}, 0, "", [])

    async def visible(self) -> tuple[int, int, bool, bool, bool, bool]:
        """What a reader outside any transaction sees of every participant:
        ``(events, outbox rows, snapshot, projection row, claim taken, saga instance)``."""
        none = self.db.auto_commit()  # looks past the ambient tx, whatever it is
        # A losing claim proves the key is taken; a winning probe must be undone, so it runs in a
        # transaction of its own that is rolled back.
        try:
            async with self.db.begin() as probe:
                taken = not isinstance(await self.claims.claim(claimed("req-A"), 0, tx=probe), Won)
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
            await rig.bus.dispatch(Deposit("acct-1", 100, "req-A"), idempotency_key="req-A")
            raise RuntimeError("a later write in the same unit of work failed")
    assert rig.calls == 1

    async with transaction(rig.db):
        balance = await rig.bus.dispatch(Deposit("acct-1", 100, "req-A"), idempotency_key="req-A")
    assert rig.calls == 2, "the handler ran again: no phantom cached result"
    assert balance == 100, "and deposited once, not twice"
    assert len(await rig.store.load(None, "acct-1", 0)) == 1

    # Now the claim is committed: a third dispatch replays without running the handler.
    async with transaction(rig.db):
        assert await rig.bus.dispatch(Deposit("acct-1", 100, "req-A"), idempotency_key="req-A") == 100
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
        ("complete", lambda claims, handle, tx: claims.complete("req-A", handle, DONE, tx=tx)),
        ("release", lambda claims, handle, tx: claims.release("req-A", handle, tx=tx)),
    ],
)
async def test_completing_or_releasing_a_held_claim_conflicts(branch: str, write: Any) -> None:
    # Exclusion covers the whole claim, not just the claim call: the holder's commit would overwrite
    # a terminal record written under another token, and a losing claimant's release would free the
    # winner's key. Redis fences both; the memory backend refuses both, even with the right handle.
    rig = Rig()
    async with rig.db.begin() as holder:
        won = await rig.claims.claim("req-A", HASH, tx=holder)
        assert isinstance(won, Won), "the holder wins the key"
        async with rig.db.begin() as other:
            with pytest.raises(edax.ConcurrencyError):
                await write(rig.claims, won.handle, other)
        with pytest.raises(edax.ConcurrencyError):
            await write(rig.claims, won.handle, rig.db.auto_commit())
        # The holder is never blocked by its own staged claim.
        await write(rig.claims, won.handle, holder)


def test_the_auto_commit_token_comes_from_the_native_binding() -> None:
    # Not a Python-side literal: `edax_abi::NO_TX` is the one owner of the value.
    assert MemoryTransactor().auto_commit().token == _edax.no_tx()


async def test_open_count_reports_unsettled_transactions() -> None:
    db = MemoryTransactor()
    assert db.open_count() == 0, "a fresh transactor holds nothing open"
    async with db.begin():
        assert db.open_count() == 1, "the unit of work is open inside its block"
        async with db.begin():
            assert db.open_count() == 2, "each begin opens one"
    assert db.open_count() == 0, "a commit closes its transaction"

    with pytest.raises(RuntimeError, match="rolled back"):
        async with db.begin():
            raise RuntimeError("rolled back")
    assert db.open_count() == 0, "a rollback closes its transaction too"


@dataclass
class ForeignThreadCase:
    build: Callable[[], Any]
    call: Callable[[Any], Any]  # one native-backed call on the built object, made synchronously


def _sync(awaitable: Any) -> Any:
    """Drive a store coroutine whose native call happens before its first suspension point."""
    try:
        awaitable.send(None)
    except StopIteration as done:
        return done.value
    raise AssertionError("the store call suspended before reaching the native handle")


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(ForeignThreadCase(MemoryTransactor, lambda t: t.open_count()), id="memory_transactor"),
        pytest.param(
            ForeignThreadCase(MemoryIdempotencyStore, lambda s: _sync(s.claim("req-1", 7))),
            id="memory_store_view",
        ),
        pytest.param(ForeignThreadCase(MemoryBroker, lambda b: b.empty()), id="memory_broker"),
        pytest.param(
            ForeignThreadCase(lambda: KafkaConsumer(FakeKafkaClient()), lambda c: c.stats()),
            id="kafka_consumer",
        ),
    ],
)
def test_a_native_handle_used_from_another_thread_raises_bad_handle(case: ForeignThreadCase) -> None:
    """The native handles are bound to the thread that built them. Before the guard, a call from
    another thread panicked inside PyO3 and surfaced as ``PanicException``, a ``BaseException`` that
    every ``except Exception`` arm missed: a consumer built on the main thread and run on a worker
    thread died on its first booking. The guard raises the terminal ``BadHandle`` first."""
    built = case.build()
    case.call(built)  # the owner thread is served
    raised: list[BaseException] = []

    def foreign() -> None:
        try:
            case.call(built)
        except BaseException as e:  # noqa: BLE001 - the assertion below inspects whatever crossed
            raised.append(e)

    thread = threading.Thread(target=foreign)
    thread.start()
    thread.join()
    assert len(raised) == 1 and isinstance(raised[0], edax.BadHandle), raised
    assert raised[0].terminal is True, "a wiring mistake is not retried"


def test_a_handle_whose_owner_thread_exited_raises_bad_handle_on_a_thread_reusing_its_ident() -> None:
    """Python recycles a thread ident once its thread exits, while PyO3 checks the Rust thread id, which
    it never recycles. A guard keyed on the ident let a new thread holding the dead owner's ident through,
    and the native call panicked. The handle here is a plain object: a native one dropped after its owner
    exits cannot be dropped on its own thread, which PyO3 reports as an unraisable error."""
    bound: ThreadBound[object] = ThreadBound.lazy(object, "MemoryTransactor")
    owner_ident: list[int] = []

    def own() -> None:
        bound.get()
        owner_ident.append(threading.get_ident())

    owner = threading.Thread(target=own)
    owner.start()
    owner.join()
    seen: list[tuple[bool, str]] = []

    def use() -> None:
        try:
            bound.get()
            outcome = "served"
        except edax.BadHandle:
            outcome = "BadHandle"
        seen.append((threading.get_ident() == owner_ident[0], outcome))

    for _ in range(50):
        thread = threading.Thread(target=use)
        thread.start()
        thread.join()
        if seen[-1][0]:
            break
    if not seen[-1][0]:
        pytest.skip("this platform did not recycle the owner's thread ident")
    assert {outcome for _, outcome in seen} == {"BadHandle"}, seen


async def test_a_released_claim_with_nothing_committed_does_not_shadow_or_erase_a_winner() -> None:
    """A release with nothing committed must stage no delete. With one, the releaser's own re-claim
    reads its staged delete instead of the winner's record and wins a second time, and its commit
    erases the winner's record, so the next dispatch runs the handler again."""
    db = MemoryTransactor()
    ledger = MemoryIdempotencyStore(db)
    async with db.begin() as releaser:
        mine = await ledger.claim("req-1", 7, tx=releaser)
        assert isinstance(mine, Won)
        await ledger.release("req-1", mine.handle, tx=releaser)  # a transient failure gave the claim up
        async with db.begin() as winner:
            won = await ledger.claim("req-1", 7, tx=winner)
            assert isinstance(won, Won), "the released key is free"
            await ledger.complete("req-1", won.handle, Record(7, result=42), tx=winner)
        replay = await ledger.claim("req-1", 7, tx=releaser)
        assert not isinstance(replay, Won) and replay.result == 42, (
            "the releaser replays the winner's outcome"
        )
    stored = await ledger.claim("req-1", 7)
    assert not isinstance(stored, Won) and stored.result == 42, (
        "the releaser's commit left the winner's record"
    )


async def test_a_release_of_a_committed_claim_holds_the_key_until_it_ends() -> None:
    """A release over a committed claim stages a delete, which holds the key like a row lock: a
    second writer's record would otherwise be deleted by the releaser's commit. A release of a
    settled record deletes nothing at all: it is nobody's claim (the Redis fence's rule)."""
    db = MemoryTransactor()
    ledger = MemoryIdempotencyStore(db)
    won = await ledger.claim("req-1", 7)
    assert isinstance(won, Won)
    async with db.begin() as releaser:
        await ledger.release("req-1", won.handle, tx=releaser)
        held = await ledger.claim("req-1", 7)
        assert not isinstance(held, Won) and held.pending, "a claimant waits on the held key"
        with pytest.raises(edax.ConcurrencyError):
            await ledger.complete("req-1", won.handle, Record(7, result=42))
    assert isinstance(await ledger.claim("req-1", 7), Won), "the release landed once the releaser committed"

    other = MemoryIdempotencyStore()
    won = await other.claim("req-2", 7)
    assert isinstance(won, Won)
    await other.complete(
        "req-2", won.handle, Record(7, error="the order is malformed", error_code="rejected")
    )
    async with other.transactor.begin() as releaser:
        await other.release("req-2", won.handle, tx=releaser)
    settled = await other.claim("req-2", 7)
    assert not isinstance(settled, Won) and settled.error_code == "rejected", "the settled record stays"


# =========================================================================
# the ambient tx is per database, so two databases nest without crossing handles
# =========================================================================


@dataclass(frozen=True)
class NestedCase:
    name: str
    #: Which of the two units of work raise: the orders one (outer) and the audit one (inner).
    outer_fails: bool
    inner_fails: bool


@pytest.mark.parametrize(
    "case",
    [
        NestedCase("both_commit", outer_fails=False, inner_fails=False),
        NestedCase("inner_rolls_back_outer_commits", outer_fails=False, inner_fails=True),
        NestedCase("inner_commits_outer_rolls_back", outer_fails=True, inner_fails=False),
        NestedCase("both_roll_back", outer_fails=True, inner_fails=True),
    ],
    ids=lambda c: c.name,
)
async def test_two_databases_nest_and_each_store_joins_its_own(case: NestedCase) -> None:
    """``transaction(orders)`` around ``transaction(audit)``: a store of each database, called with no
    ``tx`` inside the inner block, joins its own database's unit of work. One slot for every database
    handed the orders stores the audit handle, which memory refused as a foreign tx and a SQL store would
    have written through the wrong connection. Each database then commits or rolls back on its own."""
    orders, audit = MemoryTransactor(), MemoryTransactor()
    projections, ledger = MemoryProjectionStore(orders), MemoryIdempotencyStore(orders)
    audit_log, orders_log = MemoryEventStore(audit), MemoryEventStore(orders)
    repo = counter_repo(orders_log, iter(range(1, 10)))
    acct = Counter("acct-1")
    acct.apply(Tick(5))

    async def inner_block() -> None:
        async with transaction(audit) as audit_tx:
            await projections.save(Entity("order-7", 0, {"status": "paid"}))
            assert isinstance(await ledger.claim("req-7", 7), Won)
            await repo.save(acct)
            await audit_log.append(None, "audit-1", 0, [drafted(build_message("audit-1", "OrderPaid", 1))])
            assert current_tx(orders) == outer_tx and current_tx(audit) == audit_tx
            if case.inner_fails:
                raise RuntimeError("the audit write failed")

    with contextlib.suppress(RuntimeError):
        async with transaction(orders) as outer_tx:
            with contextlib.suppress(RuntimeError):
                await inner_block()
            if case.outer_fails:
                raise RuntimeError("the order write failed")

    orders_landed = await projections.get("order-7") is not None
    assert orders_landed is not case.outer_fails, "the orders writes followed the orders unit of work"
    claimed = await ledger.claim("req-7", 7)
    assert isinstance(claimed, Won) is case.outer_fails, "the claim followed the orders unit of work"
    saved = bool(await orders_log.load(orders.auto_commit(), "acct-1"))
    assert saved is not case.outer_fails, "the repository joined the orders unit of work, not the audit one"
    audit_landed = bool(await audit_log.load(audit.auto_commit(), "audit-1"))
    assert audit_landed is not case.inner_fails, "the audit write followed the audit unit of work"
    assert orders.open_count() == audit.open_count() == 0


async def test_a_store_never_reads_another_databases_ambient_tx() -> None:
    """Inside a unit of work over another database, a store has no ambient tx of its own: it runs outside
    any unit of work, and an explicit handle of the other database is still refused as foreign."""
    orders, audit = MemoryTransactor(), MemoryTransactor()
    projections = MemoryProjectionStore(orders)
    async with transaction(audit) as audit_tx:
        assert current_tx(orders) is None and current_tx(projections) is None
        await projections.save(Entity("order-7", 0, {"status": "paid"}))  # auto-commit on orders
        assert await projections.get("order-7", tx=orders.auto_commit()) is not None, "landed at once"
        with pytest.raises(edax.BadHandle, match="not a transaction of this MemoryTransactor"):
            await projections.save(Entity("order-8", 0, {"status": "paid"}), tx=audit_tx)


def test_a_transactor_chain_that_loops_names_no_database() -> None:
    """``transactor`` attributes are followed to the database that owns the units of work; a chain that
    loops back is a wiring mistake, refused as terminal ``BadHandle`` rather than followed forever."""

    class Loop:
        def __init__(self) -> None:
            self.transactor: object = self

    a, b = Loop(), Loop()
    a.transactor, b.transactor = b, a
    with pytest.raises(edax.BadHandle, match="form a cycle"):
        current_tx(a)
    single = Loop()
    assert current_tx(single) is None, "an object naming itself is its own database"


async def test_put_raw_overwrites_committed_state_and_leaves_a_held_key_alone() -> None:
    """The test hook stores bytes a claim must then read as the kernel does. A committed record is
    overwritten; a key an open unit of work holds is refused as transient, and the holder keeps it."""
    db = MemoryTransactor()
    store = MemoryIdempotencyStore(db)
    won = await store.claim("k1", 1)
    assert isinstance(won, Won)
    await store.complete("k1", won.handle, Record(payload_hash=1, result=42))
    store.put_raw("k1", "{")
    prior = await store.claim("k1", 1)
    assert not isinstance(prior, Won) and prior.error_code == "codec", "the settled record was overwritten"

    async with transaction(db) as tx:
        assert isinstance(await store.claim("held", 1, tx=tx), Won)
        with pytest.raises(edax.ConcurrencyError, match="held by an open transaction"):
            store.put_raw("held", "{")
        held = await store.claim("held", 1, tx=tx)
        assert not isinstance(held, Won) and held.pending, "the holder's own claim is untouched"

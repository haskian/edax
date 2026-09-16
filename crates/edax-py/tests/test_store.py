"""Repository/Relay orchestration over the shipped in-memory EventStore (the real store, not a
fake, so these assert the store's actual concurrency/outbox rules)."""

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, fields
from typing import Any, Optional

import edax
import pytest
from conftest import Counter, CounterCodec, Tick, TickEvents, build_envelope, counter_repo
from edax import EventEnvelope, Relay, pump
from edax.backend.memory import MemoryEventStore
from edax.store import ConcurrencyError


def _repo() -> tuple[MemoryEventStore, edax.Repository[Counter]]:
    store = MemoryEventStore()  # the shipped store, shared with every SDK via edax-memory
    return store, counter_repo(store, iter(range(1, 10_000)))


async def test_reload_via_snapshot() -> None:
    _, repo = _repo()
    a = Counter("acct-2")
    a.apply(Tick(10), Tick(5))
    await repo.save(a)  # snapshots at version 2
    b = await repo.load("acct-2")
    assert b.n == 15
    assert b.sequence.initial == 2


async def test_load_pure_replay_no_snapshot() -> None:
    store, repo = _repo()
    # Seed events through the real store's append (no snapshot) so load() must pure-replay.
    async with store.transaction() as tx:
        await store.append(
            tx,
            "acct-3",
            0,
            [
                build_envelope("acct-3", "Tick", 1, id="x1", data=b"7"),
                build_envelope("acct-3", "Tick", 2, id="x2", data=b"3"),
            ],
        )
    a = await repo.load("acct-3")  # no snapshot -> replays both events
    assert a.n == 10
    assert a.sequence.initial == 2


async def test_stale_expected_version_raises() -> None:
    _, repo = _repo()
    a = Counter("acct-4")
    a.apply(Tick(1))
    await repo.save(a)

    stale = await repo.load("acct-4")  # initial 1
    other = await repo.load("acct-4")
    other.apply(Tick(2))
    await repo.save(other)  # store now at version 2

    stale.apply(Tick(3))
    with pytest.raises(ConcurrencyError):
        await repo.save(stale)  # expected 1, stored 2


async def test_outbox_relay_drains_then_idempotent() -> None:
    store, repo = _repo()
    a = Counter("acct-5")
    a.apply(Tick(1), Tick(2))
    await repo.save(a)

    published: list[str] = []
    relay = Relay(store, lambda env: published.append(env.id))
    assert await relay.drain() == 2
    assert published == ["e1", "e2"]
    assert await relay.drain() == 0  # nothing left to publish


async def test_pump_publishes_a_cdc_stream_in_order() -> None:
    async def wal() -> AsyncIterator[Any]:
        for i in (1, 2, 3):
            yield build_envelope("acct", "Tick", i, id=f"e{i}")

    published: list[str] = []
    assert await pump(wal(), lambda env: published.append(env.id)) == 3
    assert published == ["e1", "e2", "e3"]


# =========================================================================
# Branch tables
# =========================================================================


@dataclass
class SaveCase:
    loaded_version: int  # the version the aggregate was rehydrated at
    ticks: list[int]  # the pending events at save time
    sequences: list[int]  # the sequences save() must assign


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(SaveCase(0, [], []), id="empty_at_version_zero_writes_no_event"),
        pytest.param(SaveCase(2, [], []), id="empty_past_version_zero_writes_no_event"),
        pytest.param(SaveCase(0, [10], [1]), id="one_event_at_version_zero_starts_at_sequence_one"),
        pytest.param(SaveCase(2, [10, 5, 1], [3, 4, 5]), id="many_events_continue_after_the_version"),
    ],
)
async def test_repository_save_branches(case: SaveCase) -> None:
    store, repo = _repo()
    # Seed the stream so the aggregate really is at `loaded_version`, rather than asserting against
    # an aggregate that only claims to be.
    seed = Counter("acct-1")
    seed.apply(*[Tick(1) for _ in range(case.loaded_version)])
    await repo.save(seed)
    published = [e.id for e in await store.unpublished()]

    agg = await repo.load("acct-1")
    assert agg.sequence.initial == case.loaded_version
    agg.apply(*[Tick(t) for t in case.ticks])

    envs = await repo.save(agg)
    assert [e.sequence for e in envs] == case.sequences
    assert all(e.aggregate_id == "acct-1" for e in envs)
    # The aggregate is reset to exactly what was stored, so a second save is a no-op, not a replay.
    assert agg.pending == []
    assert agg.sequence.initial == case.loaded_version + len(case.ticks)
    assert agg.sequence.updates == 0
    # Every appended event is queued for publication; an empty save adds nothing to the outbox.
    assert [e.id for e in await store.unpublished()] == published + [e.id for e in envs]
    # A snapshot is written even with nothing pending, at the aggregate's current version, tagged
    # with the codec's aggregate type.
    snap = await store.load_snapshot(None, "acct-1")
    assert snap is not None and snap.version == case.loaded_version + len(case.ticks)
    assert snap.type_name == CounterCodec.type
    assert (await repo.load("acct-1")).n == case.loaded_version + sum(case.ticks)


class CommitFailsEventStore(MemoryEventStore):
    """The shipped store with one change: its own unit of work fails to commit, landing nothing.
    Stands in for a database that accepted every statement and then lost the transaction."""

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Any]:
        async with super().transaction() as tx:
            yield tx
            # Raised inside the block, so the real transactor rolls back before this propagates.
            raise edax.StoreError("commit failed; nothing landed")


async def test_a_failed_commit_leaves_the_aggregate_holding_its_pending_events() -> None:
    # `Repository.save` owns the transaction, so it waits for the commit before resetting: a commit
    # that lands nothing leaves the aggregate exactly as it was and the caller can save again. The
    # Rust SDK draws the line in the same place (`Repository::save_with`).
    store = CommitFailsEventStore()
    repo = counter_repo(store, iter(range(1, 10_000)))
    agg = Counter("acct-commit-fails")
    agg.apply(Tick(10), Tick(5))

    with pytest.raises(edax.StoreError):
        await repo.save(agg)

    assert [t.v for t in agg.pending] == [10, 5], "a failed commit must not consume the pending events"
    assert agg.sequence.initial == 0 and agg.sequence.updates == 2
    assert await store.load(None, "acct-commit-fails") == []


async def test_save_in_a_callers_transaction_resets_before_the_caller_commits() -> None:
    # The mirror case. With a caller-supplied handle the repository cannot wait for the commit —
    # the caller performs it after `save` returns — so the reset happens first and a rollback leaves
    # an aggregate that no longer holds its events. Reload it rather than saving it again.
    store = MemoryEventStore()
    repo = counter_repo(store, iter(range(1, 10_000)))
    agg = Counter("acct-caller-tx")
    agg.apply(Tick(7))

    with pytest.raises(edax.StoreError):
        async with store.transaction() as tx:
            await repo.save(agg, tx=tx)
            assert agg.pending == [], "the reset happens as soon as the store accepts the writes"
            raise edax.StoreError("a later write in the same unit of work failed")

    assert agg.pending == []  # the rollback cannot put them back
    assert await store.load(None, "acct-caller-tx") == []


async def test_snapshot_type_name_crosses_the_native_boundary() -> None:
    """One field, one name — ``type_name`` — in the dataclass, in the native record, and back. A
    half-applied rename breaks exactly one leg, so both are asserted: the save path reads
    ``snapshot.type_name``, the load path reads the native result's ``type_name``."""
    assert [f.name for f in fields(edax.Snapshot)] == ["aggregate_id", "type_name", "version", "data"]

    store = MemoryEventStore()
    stored = edax.Snapshot("acct-9", "Counter", 3, b"42")
    await store.save_snapshot(None, stored)
    assert await store.load_snapshot(None, "acct-9") == stored


class ScriptedStore:
    """An EventStore whose reads are fixed by the test: the SDK must tolerate what a backend
    returns, including an envelope with no ``sequence``."""

    def __init__(self, snapshot: Optional[edax.Snapshot], events: list[EventEnvelope]) -> None:
        self._snapshot = snapshot
        self._events = events

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        yield None

    async def load_snapshot(self, tx: Any, aggregate_id: str) -> Optional[edax.Snapshot]:
        return self._snapshot

    async def load(self, tx: Any, aggregate_id: str, after: int = 0) -> list[EventEnvelope]:
        return [e for e in self._events if e.sequence is None or e.sequence > after]


@dataclass
class LoadCase:
    snapshot: Optional[tuple[int, int]]  # (version, counter total) of the stored snapshot
    tail: list[Optional[int]]  # the sequence of each event `load` returns
    total: int  # the rehydrated Counter's n
    version: int  # the rehydrated aggregate's sequence.initial


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(LoadCase(None, [], 0, 0), id="empty_stream_with_no_snapshot_is_version_zero"),
        pytest.param(LoadCase(None, [1, 2], 2, 2), id="no_snapshot_replays_the_whole_stream"),
        pytest.param(LoadCase((2, 20), [3], 21, 3), id="snapshot_replays_only_the_tail"),
        pytest.param(LoadCase((2, 20), [], 20, 2), id="snapshot_with_nothing_after_it_is_the_version"),
        pytest.param(LoadCase((2, 20), [None], 21, 2), id="event_without_a_sequence_leaves_the_version"),
    ],
)
async def test_repository_load_branches(case: LoadCase) -> None:
    snapshot = (
        edax.Snapshot("acct-1", "Counter", case.snapshot[0], str(case.snapshot[1]).encode())
        if case.snapshot is not None
        else None
    )
    tail = [
        build_envelope("acct-1", "Tick", seq, id=f"e{i}", data=b"1")
        for i, seq in enumerate(case.tail, start=1)
    ]
    repo = counter_repo(ScriptedStore(snapshot, tail), iter(range(1, 10)))

    agg = await repo.load("acct-1")
    assert agg.n == case.total  # every returned event is folded in, sequence or not
    assert agg.sequence.initial == case.version
    assert agg.sequence.updates == 0  # replay must not queue the events as pending again
    assert agg.pending == []


async def test_repository_defaults_to_the_kernel_id_and_clock() -> None:
    # Without `new_id` / `now` the envelopes carry a kernel guid and the kernel's timestamp, so ids
    # are unique and time-ordered without the host choosing a scheme.
    store = MemoryEventStore()
    repo = edax.Repository(store, TickEvents(), CounterCodec())
    a = Counter("acct-7")
    a.apply(Tick(1), Tick(2))
    before = edax.current_timestamp()
    envs = await repo.save(a)

    assert len({e.id for e in envs}) == 2  # distinct, not a fixed stub
    assert all(len(e.id) == len(edax.guid()) for e in envs)
    assert all(before <= e.timestamp <= edax.current_timestamp() for e in envs)


@dataclass
class PumpCase:
    ids: list[str]
    fail_at: Optional[str]  # publish raises on this id
    sync_publish: bool
    published: list[str]
    raises: Optional[type[BaseException]]
    count: int


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(PumpCase([], None, False, [], None, 0), id="empty_stream_publishes_nothing"),
        pytest.param(PumpCase(["e1"], None, True, ["e1"], None, 1), id="sync_publisher_is_called"),
        pytest.param(
            PumpCase(["e1", "e2", "e3"], "e2", False, ["e1", "e2"], RuntimeError, 0),
            id="publish_raising_stops_the_pump_at_that_row",
        ),
    ],
)
async def test_pump_branches(case: PumpCase) -> None:
    published: list[str] = []

    async def wal() -> AsyncIterator[Any]:
        for i, id_ in enumerate(case.ids, start=1):
            yield build_envelope("acct", "Tick", i, id=id_, data=b"1")

    def sync_publish(env: Any) -> None:
        published.append(env.id)

    async def async_publish(env: Any) -> None:
        published.append(env.id)
        if env.id == case.fail_at:
            raise RuntimeError("broker down")

    publish = sync_publish if case.sync_publish else async_publish
    if case.raises is None:
        assert await pump(wal(), publish) == case.count
    else:
        with pytest.raises(case.raises):
            await pump(wal(), publish)
    assert published == case.published


class MarkCountingStore(MemoryEventStore):
    def __init__(self) -> None:
        super().__init__()
        self.marks = 0

    async def mark_published(self, ids: Sequence[str]) -> None:
        self.marks += 1
        await super().mark_published(ids)


@pytest.mark.parametrize(
    "branch, ids",
    [
        ("already_marked_ids", ["e1"]),
        ("unknown_ids_are_ignored", ["never-appended"]),
        ("empty_id_list", []),
    ],
)
async def test_mark_published_is_idempotent(branch: str, ids: list[str]) -> None:
    # The relay may re-mark after a lost response, and a CDC drain may mark ids the store never saw.
    # Neither may raise or touch the rows still waiting to publish.
    store, repo = _repo()
    a = Counter("acct-6")
    a.apply(Tick(1), Tick(2))
    await repo.save(a)  # outbox: e1, e2
    await store.mark_published(["e1"])
    await store.mark_published(ids)
    assert [e.id for e in await store.unpublished()] == ["e2"]


async def test_relay_empty_outbox_does_not_mark() -> None:
    store = MarkCountingStore()
    assert await Relay(store, lambda env: None).drain() == 0
    assert store.marks == 0


def test_aggregate_apply_zero_events_leaves_sequence_and_pending() -> None:
    a = Counter("acct-9", initial=4)
    a.apply()
    assert a.sequence == edax.Version(4, 0) and a.sequence.current == 4
    assert a.pending == []
    a.apply(Tick(1))
    assert a.sequence == edax.Version(4, 1) and a.sequence.current == 5
    assert a.pending == [Tick(1)] and a.n == 1


async def test_overlapping_drains_on_one_relay_do_not_double_publish() -> None:
    """A timer firing again while the last drain is still publishing would read the same rows
    twice: ``unpublished`` is a plain query and marks nothing until the batch is through. The relay
    serializes its own drains. Two *separate* relays still need the store to lease rows."""
    store, repo = _repo()
    a = Counter("acct-9")
    a.apply(Tick(1), Tick(2))
    await repo.save(a)

    published: list[str] = []

    async def publish(env: EventEnvelope) -> None:
        await asyncio.sleep(0)  # yields: an overlapping drain would interleave right here
        published.append(env.id)

    relay = Relay(store, publish)
    counts = await asyncio.gather(relay.drain(), relay.drain())

    assert sorted(counts) == [0, 2], "the second drain ran after the first marked the batch"
    assert published == ["e1", "e2"], "every row published exactly once, in order"


@dataclass(frozen=True)
class OutboxErrorCase:
    """One outbox call, the native error it hits, and the exception the SDK must raise."""

    call: str
    raised: str
    expected: type[Exception]


class _FailingOutbox:
    """Stands in for the native object: whatever the call, it raises one tagged wire error."""

    def __init__(self, raised: str) -> None:
        self._raised = raised

    async def unpublished(self, limit: int) -> Any:
        raise RuntimeError(self._raised)

    async def mark_published(self, ids: list[str]) -> None:
        raise RuntimeError(self._raised)


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            OutboxErrorCase("unpublished", "codec: bad row", edax.CodecError),
            id="unpublished_maps_a_tagged_code",
        ),
        pytest.param(
            OutboxErrorCase("unpublished", "disk gone", edax.StoreError),
            id="unpublished_maps_an_untagged_failure",
        ),
        pytest.param(
            OutboxErrorCase("mark_published", "codec: bad row", edax.CodecError),
            id="mark_published_maps_a_tagged_code",
        ),
        pytest.param(
            OutboxErrorCase("mark_published", "disk gone", edax.StoreError),
            id="mark_published_maps_an_untagged_failure",
        ),
    ],
)
async def test_outbox_calls_map_native_errors_like_every_other_call(case: OutboxErrorCase) -> None:
    """The two outbox calls go through ``errors.mapped()`` too, so a host sees one taxonomy and
    not a bare ``RuntimeError`` from these two alone."""
    store = MemoryEventStore()
    store._transactor._inner = _FailingOutbox(case.raised)  # ty: ignore[invalid-assignment]
    args: list[Any] = [] if case.call == "unpublished" else [["evt-1"]]

    with pytest.raises(case.expected):
        await getattr(store, case.call)(*args)

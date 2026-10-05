"""Repository/Relay orchestration over the shipped in-memory EventStore (the real store, not a
fake, so these assert the store's actual concurrency/outbox rules)."""

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, fields
from typing import Any
from unittest import mock

import edax
import pytest
from conftest import Counter, CounterCodec, Tick, TickEvents, build_message, counter_repo
from edax import EventMessage, Relay, pump
from edax._owned import ThreadBound
from edax.backend.memory import MemoryEventStore
from edax.store import ConcurrencyError
from edax.testing import drafted


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
    assert b.version.initial == 2


async def test_load_pure_replay_no_snapshot() -> None:
    store, repo = _repo()
    # Seed events through the real store's append (no snapshot) so load() must pure-replay.
    async with store.begin() as tx:
        await store.append(
            tx,
            "acct-3",
            0,
            [
                drafted(build_message("acct-3", "Tick", 1, id="x1", data=b"7")),
                drafted(build_message("acct-3", "Tick", 2, id="x2", data=b"3")),
            ],
        )
    a = await repo.load("acct-3")  # no snapshot -> replays both events
    assert a.n == 10
    assert a.version.initial == 2


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
    assert published == ["e2", "e3"], "e1 roots the save's flow; it names no event"
    assert await relay.drain() == 0  # nothing left to publish


async def test_pump_publishes_a_cdc_stream_in_order() -> None:
    async def wal() -> AsyncIterator[Any]:
        for i in (1, 2, 3):
            yield build_message("acct", "Tick", i, id=f"e{i}")

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
    assert agg.version.initial == case.loaded_version
    agg.apply(*[Tick(t) for t in case.ticks])

    envs = await repo.save(agg)
    assert [e.sequence for e in envs] == case.sequences
    assert all(e.aggregate_id == "acct-1" for e in envs)
    # The aggregate is reset to exactly what was stored, so a second save is a no-op, not a replay.
    assert agg.pending == []
    assert agg.version.initial == case.loaded_version + len(case.ticks)
    assert agg.version.updates == 0
    # Every appended event is queued for publication; an empty save adds nothing to the outbox.
    assert [e.id for e in await store.unpublished()] == published + [e.id for e in envs]
    # A save that crosses a version snapshots at the aggregate's current version, tagged with the codec's
    # aggregate type; one with nothing pending crosses none and leaves the snapshot where it was.
    snap = await store.load_snapshot(None, "acct-1")
    if case.loaded_version + len(case.ticks) == 0:
        assert snap is None, "nothing was ever pending, so nothing was snapshotted"
    else:
        assert snap is not None and snap.version == case.loaded_version + len(case.ticks)
        assert snap.type_name == CounterCodec.type
    assert (await repo.load("acct-1")).n == case.loaded_version + sum(case.ticks)


class CommitFailsEventStore(MemoryEventStore):
    """The shipped store with one change: its own unit of work fails to commit, landing nothing.
    Stands in for a database that accepted every statement and then lost the transaction."""

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[Any]:
        async with super().begin() as tx:
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
    assert agg.version.initial == 0 and agg.version.updates == 2
    assert await store.load(None, "acct-commit-fails") == []


async def test_save_in_a_callers_transaction_resets_before_the_caller_commits() -> None:
    # The mirror case. With a caller-supplied handle the repository cannot wait for the commit, which
    # the caller performs after `save` returns, so the reset happens first and a rollback leaves
    # an aggregate that no longer holds its events. Reload it rather than saving it again.
    store = MemoryEventStore()
    repo = counter_repo(store, iter(range(1, 10_000)))
    agg = Counter("acct-caller-tx")
    agg.apply(Tick(7))

    with pytest.raises(edax.StoreError):
        async with store.begin() as tx:
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
    await store.append(
        None, "acct-9", 0, [drafted(build_message("acct-9", "Tick", seq, data=b"14")) for seq in (1, 2, 3)]
    )
    stored = edax.Snapshot("acct-9", "Counter", 3, b"42")
    await store.save_snapshot(None, stored)
    assert await store.load_snapshot(None, "acct-9") == stored


class ScriptedStore:
    """An EventStore whose reads are fixed by the test: the SDK must tolerate what a backend
    returns, including an event with no ``sequence``."""

    def __init__(self, snapshot: edax.Snapshot | None, events: list[EventMessage]) -> None:
        self._snapshot = snapshot
        self._events = events

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[None]:
        yield None

    async def load_snapshot(self, tx: Any, aggregate_id: str) -> edax.Snapshot | None:
        return self._snapshot

    async def load(self, tx: Any, aggregate_id: str, after: int = 0) -> list[EventMessage]:
        return [e for e in self._events if e.sequence is None or e.sequence > after]


@dataclass
class LoadCase:
    snapshot: tuple[str, int, bytes] | None  # (type name, version, data) of the stored snapshot
    tail: list[int | None]  # the sequence of each event `load` returns, each a Tick of 1
    total: int | None  # the rehydrated Counter's n, or None when the load must fail
    version: int  # the rehydrated aggregate's sequence.initial
    error: str = ""  # the CodecError's message when the load fails


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(LoadCase(None, [], 0, 0), id="empty_stream_with_no_snapshot_is_version_zero"),
        pytest.param(LoadCase(None, [1, 2], 2, 2), id="no_snapshot_replays_the_whole_stream"),
        pytest.param(LoadCase(("Counter", 2, b"20"), [3], 21, 3), id="snapshot_replays_only_the_tail"),
        pytest.param(
            LoadCase(("Counter", 2, b"20"), [], 20, 2), id="snapshot_with_nothing_after_it_is_the_version"
        ),
        # A snapshot is a cache: one this codec cannot use is skipped and the stream replays from 0.
        pytest.param(
            LoadCase(("Ledger", 2, b"20"), [1, 2, 3], 3, 3), id="another_types_snapshot_replays_from_zero"
        ),
        pytest.param(
            LoadCase(("Counter", 2, b"not a number"), [1, 2, 3], 3, 3),
            id="unloadable_snapshot_replays_from_zero",
        ),
        # The kernel's dense-run rule on the loaded tail: a store that breaks the gap-free promise
        # fails the load instead of folding the wrong state.
        pytest.param(
            LoadCase(None, [1, 3], None, 0, "event 1 has sequence 3, expected 2"),
            id="a_gap_fails_the_load",
        ),
        pytest.param(
            LoadCase(None, [1, 1], None, 0, "event 1 has sequence 1, expected 2"),
            id="a_repeat_fails_the_load",
        ),
        pytest.param(
            LoadCase(("Counter", 2, b"20"), [None], None, 0, "event 0 has no sequence"),
            id="an_unsequenced_event_fails_the_load",
        ),
    ],
)
async def test_repository_load_branches(case: LoadCase) -> None:
    snapshot = (
        edax.Snapshot("acct-1", case.snapshot[0], case.snapshot[1], case.snapshot[2])
        if case.snapshot is not None
        else None
    )
    tail = [
        build_message("acct-1", "Tick", seq, id=f"e{i}", data=b"1")
        for i, seq in enumerate(case.tail, start=1)
    ]
    repo = counter_repo(ScriptedStore(snapshot, tail), iter(range(1, 10)))

    if case.total is None:
        with pytest.raises(edax.CodecError, match=case.error):
            await repo.load("acct-1")
        return
    agg = await repo.load("acct-1")
    assert agg.n == case.total  # every returned event is folded in
    assert agg.version.initial == case.version
    assert agg.version.updates == 0  # replay must not queue the events as pending again
    assert agg.pending == []


async def test_repository_defaults_to_the_kernel_id_and_the_store_to_the_kernel_clock() -> None:
    # Without `new_id` the events carry a kernel guid, and without `now` the store records them at the
    # kernel's time, so ids are unique and time-ordered without the host choosing a scheme.
    store = MemoryEventStore()
    repo = edax.Repository(store, TickEvents(), CounterCodec())
    a = Counter("acct-7")
    a.apply(Tick(1), Tick(2))
    before = edax.current_timestamp()
    envs = await repo.save(a)

    assert len({e.id for e in envs}) == 2  # distinct, not a fixed stub
    assert all(len(e.id) == len(edax.guid()) for e in envs)
    assert all(before <= e.recorded_at <= edax.current_timestamp() for e in envs)
    assert all(e.occurred_at is None for e in envs), "the repository claims no producer time"


async def test_the_store_records_every_event_at_its_own_clock() -> None:
    """The store stamps ``recorded_at``, not the repository: one reading of the injected clock per append,
    the same on what ``append`` returns, what ``load`` reads and what the outbox holds."""
    readings = iter([1_000, 2_000])
    store = MemoryEventStore(now=lambda: next(readings))
    repo = counter_repo(store, iter(range(1, 100)))
    a = Counter("acct-clock")
    a.apply(Tick(1), Tick(2))
    first = await repo.save(a)
    a.apply(Tick(3))
    second = await repo.save(a)
    assert [m.recorded_at for m in first + second] == [1_000, 1_000, 2_000]
    assert await store.load(None, "acct-clock") == first + second
    assert await store.unpublished() == first + second


@dataclass
class LineageCase:
    explicit: edax.Lineage | None  # the save's `lineage=`
    ambient: edax.Lineage | None  # the lineage `use_lineage` sets around the save
    want: edax.Lineage  # the lineage every event of the save carries


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            LineageCase(
                edax.Lineage("flow-1", "cmd-1"),
                edax.Lineage("flow-2", "evt-2"),
                edax.Lineage("flow-1", "cmd-1"),
            ),
            id="an_explicit_lineage_wins",
        ),
        pytest.param(
            LineageCase(None, edax.Lineage("flow-2", "evt-2"), edax.Lineage("flow-2", "evt-2")),
            id="the_ambient_lineage_of_the_handled_message_is_next",
        ),
        pytest.param(LineageCase(None, None, edax.Lineage("e1", "e1")), id="else_the_save_roots_a_new_flow"),
    ],
)
async def test_every_event_of_one_save_shares_one_lineage(case: LineageCase) -> None:
    _, repo = _repo()
    a = Counter("acct-flow")
    a.apply(Tick(1), Tick(2))
    if case.ambient is None:
        saved = await repo.save(a, lineage=case.explicit)
    else:
        with edax.use_lineage(case.ambient):
            saved = await repo.save(a, lineage=case.explicit)
    assert [edax.Lineage(m.correlation_id, m.causation_id) for m in saved] == [case.want, case.want]


async def test_save_stamps_the_extensions_and_metadata_on_every_event() -> None:
    _, repo = _repo()
    a = Counter("acct-ctx")
    a.apply(Tick(1), Tick(2))
    saved = await repo.save(a, extensions={"traceparent": "00-a-b-01"}, metadata={"tenant": "acme"})
    assert [(m.extensions, m.metadata) for m in saved] == [
        ({"traceparent": "00-a-b-01"}, {"tenant": "acme"})
    ] * 2


@pytest.mark.parametrize(
    ("occurred_at", "want"),
    [(None, None), (1_600_000_000_000_000_000, 1_600_000_000_000_000_000)],
    ids=["unset_reads_as_recorded", "earlier_valid_time_kept"],
)
async def test_save_carries_the_valid_time_beside_the_store_time(
    occurred_at: int | None, want: int | None
) -> None:
    store, repo = _repo()
    a = Counter(f"acct-time-{occurred_at}")
    a.apply(Tick(1), Tick(2))
    saved = await repo.save(a, occurred_at=occurred_at)
    assert [m.occurred_at for m in saved] == [want, want]
    loaded = await store.load(None, a.id)
    assert [(m.occurred_at, m.recorded_at) for m in loaded] == [(m.occurred_at, m.recorded_at) for m in saved]
    assert all(m.recorded_at > 0 for m in loaded), "the store stamps transaction time"


async def test_an_extension_key_outside_the_rule_fails_the_save_before_any_write() -> None:
    """The relay could never encode the event, so the save refuses it: terminal, and nothing written."""
    store, repo = _repo()
    a = Counter("acct-bad-ext")
    a.apply(Tick(1))
    with pytest.raises(edax.CodecError) as refused:
        await repo.save(a, extensions={"Traceparent": "00-a-b-01"})
    assert refused.value.terminal
    assert a.pending == [Tick(1)], "the aggregate still holds its event"
    assert await store.load(None, "acct-bad-ext") == []


@dataclass
class PumpCase:
    ids: list[str]
    fail_at: str | None  # publish raises on this id
    sync_publish: bool
    published: list[str]
    raises: type[BaseException] | None
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
            yield build_message("acct", "Tick", i, id=id_, data=b"1")

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
        ("already_marked_ids", ["e2"]),
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
    await repo.save(a)  # outbox: e2, e3 (e1 roots the flow)
    await store.mark_published(["e2"])
    await store.mark_published(ids)
    assert [e.id for e in await store.unpublished()] == ["e3"]


async def test_relay_empty_outbox_does_not_mark() -> None:
    store = MarkCountingStore()
    assert await Relay(store, lambda env: None).drain() == 0
    assert store.marks == 0


def test_aggregate_apply_zero_events_leaves_sequence_and_pending() -> None:
    a = Counter("acct-9", initial=4)
    a.apply()
    assert a.version == edax.Version(4, 0) and a.version.current == 4
    assert a.pending == []
    a.apply(Tick(1))
    assert a.version == edax.Version(4, 1) and a.version.current == 5
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

    async def publish(env: EventMessage) -> None:
        await asyncio.sleep(0)  # yields: an overlapping drain would interleave right here
        published.append(env.id)

    relay = Relay(store, publish)
    counts = await asyncio.gather(relay.drain(), relay.drain())

    assert sorted(counts) == [0, 2], "the second drain ran after the first marked the batch"
    assert published == ["e2", "e3"], "every row published exactly once, in order"


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

    def unpublished(self, limit: int) -> Any:
        raise RuntimeError(self._raised)

    def mark_published(self, ids: list[str]) -> None:
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
    store._transactor._native = ThreadBound(_FailingOutbox(case.raised), "MemoryTransactor")  # ty: ignore[invalid-assignment]
    args: list[Any] = [] if case.call == "unpublished" else [["evt-1"]]

    with pytest.raises(case.expected):
        await getattr(store, case.call)(*args)


@pytest.mark.parametrize(
    "every, saves, want",
    [
        (1, [1, 1, 1], [1, 2, 3]),
        (2, [1, 1, 1, 1], [None, 2, 2, 4]),
        (3, [2, 2, 2], [None, 4, 6]),
        (0, [1, 1], [None, None]),
        (1, [0], [None]),
    ],
    ids=["every_save", "every_second_version", "a_save_that_jumps_past_a_multiple", "never", "an_empty_save"],
)
async def test_snapshot_every_snapshots_when_the_version_crosses_a_multiple(
    every: int, saves: list[int], want: list[int | None]
) -> None:
    """A snapshot on every save doubles the writes. The policy snapshots when a save carries the version
    across a multiple of ``snapshot_every``, and the stream replays from it."""
    store = MemoryEventStore()
    repo = edax.Repository(store, TickEvents(), CounterCodec(), snapshot_every=every)
    got: list[int | None] = []
    for n in saves:
        agg = await repo.load("acct-1")
        agg.apply(*[Tick(1) for _ in range(n)])
        await repo.save(agg)
        snap = await store.load_snapshot(None, "acct-1")
        got.append(None if snap is None else snap.version)
    assert got == want
    assert (await repo.load("acct-1")).n == sum(saves), "a stale or absent snapshot still replays the rest"


def test_a_negative_snapshot_interval_is_refused() -> None:
    with pytest.raises(ValueError, match="at least 0, got -1"):
        edax.Repository(MemoryEventStore(), TickEvents(), CounterCodec(), snapshot_every=-1)


# =========================================================================
# Relay.run, the drain loop a host would otherwise write by hand
# =========================================================================


async def _outbox_with(n: int) -> MemoryEventStore:
    store = MemoryEventStore()
    repo = counter_repo(store, iter(range(1, 1000)))
    agg = Counter("acct-1")
    agg.apply(*[Tick(1) for _ in range(n)])
    await repo.save(agg)
    return store


async def test_run_drains_back_to_back_until_stopped() -> None:
    store = await _outbox_with(5)
    sent: list[str] = []
    stop = asyncio.Event()

    def publish(env: edax.EventMessage) -> None:
        sent.append(env.id)
        if len(sent) == 5:
            stop.set()

    relay = edax.Relay(store, publish)
    await asyncio.wait_for(relay.run(batch=2, idle_s=60, stop=stop), 5)  # never idles while rows flow
    assert len(sent) == 5 and await store.unpublished() == []


async def test_run_returns_at_once_when_already_stopped() -> None:
    store = await _outbox_with(1)
    stop = asyncio.Event()
    stop.set()
    sent: list[str] = []
    await edax.Relay(store, lambda env: sent.append(env.id)).run(stop=stop)
    assert sent == [], "a stopped relay drains nothing"


async def test_run_idles_until_stop_and_wakes_for_it() -> None:
    """Nothing to publish: the loop waits ``idle_s`` or until ``stop``, whichever comes first."""
    store = MemoryEventStore()
    stop = asyncio.Event()
    relay = edax.Relay(store, lambda env: None)
    task = asyncio.create_task(relay.run(idle_s=60, stop=stop))
    await asyncio.sleep(0)
    assert relay.last is not None and relay.last.published == 0, "it drained once and found nothing"
    stop.set()
    await asyncio.wait_for(task, 1)  # woke for the stop, not after 60 s


async def test_run_idles_again_after_the_idle_wait_times_out() -> None:
    store = MemoryEventStore()
    stop = asyncio.Event()
    drains = 0

    class Counting(edax.Relay):
        async def drain(self, batch: int = 100) -> int:
            nonlocal drains
            drains += 1
            if drains == 3:
                stop.set()
            return await super().drain(batch)

    await asyncio.wait_for(Counting(store, lambda env: None).run(idle_s=0.001, stop=stop), 5)
    assert drains == 3


async def test_run_without_stop_runs_until_cancelled() -> None:
    relay = edax.Relay(MemoryEventStore(), lambda env: None)
    task = asyncio.create_task(relay.run(idle_s=60))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_run_survives_a_failed_drain(caplog: pytest.LogCaptureFixture, slept: list[int]) -> None:
    """A row that will not publish: the drain waits its backoff step and raises, and the loop logs it and
    drains again, so one bad row never ends the relay."""
    store = await _outbox_with(1)
    stop = asyncio.Event()
    attempts = 0

    def publish(env: edax.EventMessage) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("broker unreachable")
        stop.set()

    await asyncio.wait_for(edax.Relay(store, publish).run(stop=stop, idle_s=60), 5)
    assert attempts == 3 and slept == [0, 1], "two failed drains, each waiting its own backoff step"
    assert caplog.text.count("drain failed; the next drain retries it") == 2
    assert await store.unpublished() == []


# =========================================================================
# a relay whose own store fails backs off instead of spinning
# =========================================================================


class ScriptedOutbox:
    """The real outbox with a script per store call: each ``unpublished`` and each ``mark_published`` takes
    the next answer off its list, ``False`` failing it, and succeeds once its list is empty. A failing call
    raises before it awaits anything, as a driver that refuses a closed pool does: nothing in it yields."""

    def __init__(self, inner: MemoryEventStore, reads: list[bool], marks: list[bool]) -> None:
        self.inner = inner
        self.reads = reads
        self.marks = marks
        self.calls = 0

    async def unpublished(self, limit: int = 100) -> list[EventMessage]:
        self.calls += 1
        if self.reads and not self.reads.pop(0):
            raise edax.StoreError("database down")
        return await self.inner.unpublished(limit)

    async def mark_published(self, ids: Sequence[str]) -> None:
        if self.marks and not self.marks.pop(0):
            raise edax.StoreError("connection lost")
        await self.inner.mark_published(ids)


@dataclass
class StoreDrain:
    name: str
    publish_fails: bool  # the first row's publish fails, transiently
    want: int | str  # the settled count, or the raised error's message
    want_wait: int | None  # the backoff step this drain waits, if it waits


async def test_a_store_that_fails_backs_the_drain_off_and_a_store_that_recovers_starts_over(
    slept: list[int],
) -> None:
    """The relay's own store is down: a host looping on ``drain`` waits a step that grows while it stays
    down, and starts over once a drain's store calls succeed. A row's publish failure waits the row's own
    step, kept apart from the store's."""
    drains = [
        StoreDrain("the read fails: step 0", False, "database down", 0),
        StoreDrain("it fails again: step 1", False, "database down", 1),
        StoreDrain("the mark fails: the store's step 2", False, "connection lost", 2),
        StoreDrain("the store answers, a row fails: the row's step 0", True, "broker unavailable", 0),
        StoreDrain("the read fails after a recovery: step 0 again", False, "database down", 0),
        StoreDrain("everything lands: no wait", False, 4, None),
        StoreDrain("the next store failure starts at step 0", False, "database down", 0),
    ]
    store = ScriptedOutbox(await _outbox_with(4), [False, False, True, True, False, True, False], [False])
    failed: list[str] = []

    class Failures(edax.Observer):
        def drain_failed(self, error: BaseException, published: int, dead_lettered: int) -> None:
            failed.append(str(error))

    relay = edax.Relay(store, lambda env: None, observer=Failures())
    for drain in drains:
        before = len(slept)

        def publish(env: EventMessage, fails: bool = drain.publish_fails) -> None:
            if fails and env.sequence == 1:
                raise edax.StoreError("broker unavailable")

        relay._publish = publish
        try:
            got: int | str = await relay.drain()
        except edax.StoreError as e:
            got = str(e)
        assert got == drain.want, drain.name
        assert slept[before:] == ([] if drain.want_wait is None else [drain.want_wait]), drain.name
    assert failed == [d.want for d in drains if isinstance(d.want, str)], "every failed drain is reported"
    assert await store.inner.unpublished() == [], "every row was marked in the end"


async def test_run_over_a_store_that_fails_without_awaiting_yields_and_backs_off() -> None:
    """``unpublished`` raises synchronously, so a drain never awaits anything. The
    loop must still yield to the event loop and wait a growing step, or it starves every other task on the
    loop (the dispatcher, Kafka commits, Redis heartbeats). A drain that reaches the store resets the step,
    so the next outage starts at step 0 again."""
    steps: list[int] = []
    ticks = 0
    stop = asyncio.Event()
    # Two failed reads, a drain that publishes the first row, one more failed read, then the second row.
    store = ScriptedOutbox(await _outbox_with(2), [False, False, True, False], [])

    async def record(self: edax.Pacing, attempt: int) -> None:
        steps.append(attempt)
        await asyncio.sleep(0)  # a real wait yields; this one costs no wall time

    async def other_task() -> None:
        nonlocal ticks
        while not stop.is_set():
            ticks += 1
            await asyncio.sleep(0)

    def publish(env: EventMessage) -> None:
        if env.sequence == 2:
            stop.set()

    ticker = asyncio.create_task(other_task())
    with mock.patch.object(edax.Pacing, "wait", record):
        await asyncio.wait_for(edax.Relay(store, publish).run(stop=stop, idle_s=60, batch=1), 5)
    await ticker
    assert steps == [0, 1, 0], "consecutive store failures wait a growing step; a recovery starts over"
    assert ticks >= 3, "the loop yielded on every failure, so other tasks ran"
    assert store.calls == 5 and await store.inner.unpublished() == [], "then it drained every row"

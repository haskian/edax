"""The :class:`~edax.store.EventStore` contract."""

from __future__ import annotations

from typing import Any

import pytest

from ..._values import replace
from ...errors import CodecError, ConcurrencyError, EdaxError
from ...message import EventMessage
from ...store import EventStore, Snapshot
from ...tenancy import Tenancy, tenant_key
from .._concurrency import RolledBack, race, staged_then_read, unique_id
from ..messages import draft, drafted
from ._fixtures import Transactional, requires
from .outbox import OutboxContract

#: A seam fixture: the store under test.
Store = EventStore[Any]

#: Appends against a stream whose stored version is 1, by branch: the version the caller expects, the
#: batch's sequences, and the refusal, or ``None`` for an append that lands.
_APPENDS: dict[str, tuple[int, list[int | None], type[EdaxError] | None]] = {
    "dense_at_the_stored_version": (1, [2, 3], None),
    "empty_at_the_stored_version": (1, [], None),
    "stale": (0, [1], ConcurrencyError),
    "stale_batch": (0, [1, 2], ConcurrencyError),
    "ahead": (7, [8], ConcurrencyError),
    "empty_and_ahead": (7, [], ConcurrencyError),
    "gap": (1, [2, 4], CodecError),
    "repeat": (1, [2, 2], CodecError),
    "reordered": (1, [3, 2], CodecError),
    "unsequenced": (1, [2, None], CodecError),
}

#: Snapshot saves against a stream of two events, each in a unit of work of its own, by branch: how many
#: events that unit of work appends first, the saves as ``(version, data)``, and the snapshot that must be
#: kept, or ``None`` for none. A save past the stream's version, its own appends counted, is refused.
_SNAPSHOTS: dict[str, tuple[int, list[tuple[int, bytes]], tuple[int, bytes] | None]] = {
    "none_stored": (0, [], None),
    "lags_the_stream": (0, [(1, b"lagging")], (1, b"lagging")),
    "a_later_save_supersedes": (0, [(1, b"first"), (2, b"newer")], (2, b"newer")),
    "an_older_save_never_rolls_back": (0, [(2, b"newer"), (1, b"older")], (2, b"newer")),
    "leads_the_stream": (0, [(2, b"level"), (3, b"ahead")], (2, b"level")),
    "level_with_its_own_append": (1, [(3, b"own")], (3, b"own")),
}


class EventStoreContract(Transactional):
    """The durable log: per-aggregate events under optimistic concurrency, and snapshots. The outbox rows
    ``append`` stages are read back through another port: :class:`EventOutboxContract` holds it.

    Supply an ``event_store`` fixture returning a store whose ``begin()`` is usable: the store opens its own
    units of work, so it needs no ``transactor`` fixture.
    """

    #: ``load_snapshot`` / ``save_snapshot`` are implemented.
    supports_snapshots: bool = True

    async def test_an_empty_stream_is_version_zero(self, event_store: Store) -> None:
        """ "An aggregate's version is the ``sequence`` of its last stored event, ``0`` for an empty
        stream." A store that cannot say "nothing here" cannot be appended to correctly."""
        async with event_store.begin() as tx:
            assert await event_store.load(tx, unique_id("absent")) == []

    async def test_append_then_load_returns_what_was_written(self, event_store: Store) -> None:
        """ "``load`` returns every event with ``sequence > after``, ascending, gap-free." The store
        never renumbers: the sequences that come back are the ones handed in."""
        agg = unique_id("acct")
        written = [draft(agg, 1), draft(agg, 2)]
        async with event_store.begin() as tx:
            appended = await event_store.append(tx, agg, 0, written)
            loaded = await event_store.load(tx, agg)
        assert loaded == appended, "load returns what append returned"
        assert [e.sequence for e in loaded] == [1, 2], "ascending and gap-free, as handed in"
        assert [e.id for e in loaded] == [e.id for e in written]

    async def test_load_after_skips_what_the_caller_already_has(self, event_store: Store) -> None:
        """ "every event with ``sequence > after``" — strictly after, so the row at ``after`` is not
        replayed. An off-by-one here double-applies one event per load."""
        agg = unique_id("acct")
        async with event_store.begin() as tx:
            await event_store.append(tx, agg, 0, [draft(agg, n) for n in (1, 2, 3)])
            assert [e.sequence for e in await event_store.load(tx, agg, after=2)] == [3]
            assert [e.sequence for e in await event_store.load(tx, agg, after=3)] == []

    async def test_load_sees_this_transactions_own_appends(self, event_store: Store) -> None:
        """ "including this ``tx``'s own uncommitted appends." A `Repository` loads, decides, and
        appends inside one transaction; a store that reads only committed rows makes it decide on
        stale state."""
        agg = unique_id("acct")
        async with event_store.begin() as tx:
            await event_store.append(tx, agg, 0, [draft(agg, 1)])
            assert len(await event_store.load(tx, agg)) == 1, "uncommitted, but visible to its own tx"

    @pytest.mark.parametrize("branch", list(_APPENDS))
    async def test_append_is_compare_and_set_on_a_dense_batch(self, event_store: Store, branch: str) -> None:
        """ "``append`` succeeds only if the stored version equals ``expected_version``, else it raises
        ``ConcurrencyError`` and writes nothing. A batch must carry ``expected_version + 1, + 2, ...`` in
        order, every draft sequenced."

        A read-then-write in Python passes the version check in a one-caller test and loses under
        concurrency, so both directions are checked: stale *and* ahead. An empty batch, which
        ``Repository.save`` sends for an aggregate with nothing pending, is still a compare-and-set: a store
        that shortcuts it lets a stale caller believe it holds the current version. Checking only the stored
        version passes a batch with a gap, a repeat or a reordering, and the stream then breaks the gap-free
        ascending read every reader relies on; that refusal is the kernel's terminal ``CodecError``
        (:func:`~edax.store.check_sequence_batch`), since the same batch fails the same way on every retry. A
        refused append writes nothing, the drafts before the one at fault included."""
        expected, sequences, refusal = _APPENDS[branch]
        agg = unique_id("acct")
        async with event_store.begin() as tx:
            await event_store.append(tx, agg, 0, [draft(agg, 1)])
        batch = [draft(agg, n, name=f"T{i}") for i, n in enumerate(sequences)]
        if refusal is None:
            async with event_store.begin() as tx:
                await event_store.append(tx, agg, expected, batch)
        else:
            # Caught inside the unit of work, which then commits: a store that stages the batch before it
            # refuses would leave the rows for a caller that handles the refusal and goes on.
            async with event_store.begin() as tx:
                with pytest.raises(refusal) as caught:
                    await event_store.append(tx, agg, expected, batch)
            assert caught.value.terminal is (refusal is CodecError), f"{branch}: the kernel's verdict"
        async with event_store.begin() as tx:
            loaded = [e.sequence for e in await event_store.load(tx, agg)]
        want = [1] if refusal else [1, *sequences]
        assert loaded == want, f"{branch}: the stream as the append left it"

    async def test_every_draft_field_survives_the_round_trip(self, event_store: Store) -> None:
        """ "``append`` stamps every draft with the time the store records it ... and returns the stored
        message per draft, in order. ``load`` and the outbox's ``unpublished`` return the same
        ``recorded_at``." :class:`EventOutboxContract` checks the outbox half.

        A store that persists only the fields it recognises silently drops the rest. The lineage is what
        the whole tracing story depends on, `extensions` carry the trace parent, `metadata` the
        application's context, `occurred_at` the producer's own time. A column missing for any of them
        loses data no replay can recover. Non-ASCII included, because a latin-1 column passes every test
        written in English. The batch's timestamps are not compared with each other: a store may stamp
        them with one reading or one per row."""
        agg = unique_id("acct")
        # The rich base carries every field; the second draft leaves the optional producer time out.
        written = [replace(draft(agg, 1), version=3), replace(draft(agg, 2), version=3, occurred_at=None)]
        async with event_store.begin() as tx:
            appended = await event_store.append(tx, agg, 0, written)
            loaded = await event_store.load(tx, agg)
        assert [drafted(m) for m in appended] == written, (
            "append returns one message per draft, every field as drafted"
        )
        assert all(isinstance(m.recorded_at, int) and m.recorded_at > 0 for m in appended), (
            "the store stamped recorded_at"
        )
        assert loaded == appended, "load returns the stored messages, recorded_at included"

    async def test_streams_are_independent(self, event_store: Store) -> None:
        """Versions are per aggregate. A store keying concurrency on a global sequence makes every
        aggregate contend with every other one."""
        a, b = unique_id("a"), unique_id("b")
        async with event_store.begin() as tx:
            await event_store.append(tx, a, 0, [draft(a, 1)])
            await event_store.append(tx, b, 0, [draft(b, 1)])
            assert len(await event_store.load(tx, a)) == 1
            assert len(await event_store.load(tx, b)) == 1

    async def test_the_stream_is_named_by_the_argument_not_the_draft(self, event_store: Store) -> None:
        """ "``append``, ``load`` and the snapshot calls name the stream by the key they are called with,
        never by a draft's own ``aggregate_id``." Under :attr:`~edax.tenancy.Tenancy.POOL` the repository
        calls with the aggregate id composed with its tenant, while every draft keeps the bare id. A store
        that keys its rows by the draft merges two tenants' same-named aggregates into one stream: the
        second tenant's append conflicts, and its load reads the first tenant's events."""
        agg = unique_id("acct")
        for tenant in ("acme", "globex"):
            key = tenant_key(Tenancy.POOL, tenant, agg)
            async with event_store.begin() as tx:
                await event_store.append(tx, key, 0, [draft(agg, 1, tenant=tenant)])
                loaded = await event_store.load(tx, key)
                assert [(e.aggregate_id, e.tenant) for e in loaded] == [(agg, tenant)], (
                    f"{tenant}'s event under the key {key!r} it was appended with"
                )
                if self.supports_snapshots:
                    # Version 1 is the head of the key's own stream, so a store that checks lag accepts it.
                    await event_store.save_snapshot(tx, Snapshot(key, "Account", 1, tenant.encode()))
                    snap = await event_store.load_snapshot(tx, key)
                    assert snap is not None and snap.data == tenant.encode(), f"{tenant}'s snapshot"
        async with event_store.begin() as tx:
            assert await event_store.load(tx, agg) == [], "nothing under the draft's bare aggregate id"

    async def test_a_rollback_removes_the_appended_events(self, event_store: Store) -> None:
        """ "a rollback removes all of them." This is what lets a failed handler leave no trace, and
        what the shared-ledger idempotency mode is built on."""
        requires(self.supports_transactions, "the backend declares no unit of work")
        agg = unique_id("acct")
        with pytest.raises(RuntimeError):
            async with event_store.begin() as tx:
                await event_store.append(tx, agg, 0, [draft(agg, 1)])
                raise RuntimeError("the handler failed")
        async with event_store.begin() as tx:
            assert await event_store.load(tx, agg) == [], "the rollback took the events with it"

    @pytest.mark.parametrize("branch", list(_SNAPSHOTS))
    async def test_the_latest_snapshot_is_kept_and_never_leads_the_stream(
        self, event_store: Store, branch: str
    ) -> None:
        """ "``load_snapshot`` returns the latest snapshot or ``None``. A snapshot may lag the stream but
        never lead it."

        A later save supersedes an earlier one rather than accumulating, and a slow writer's older snapshot
        arriving after a newer one never rolls the cache back, or the repository would replay from the older
        version on every load. One past the stream's version replays state that was never committed: the
        repository would load it, find no events after it, and conflict on every append from then on, so the
        store refuses it with ``CodecError`` and keeps what it had. The unit of work's own appends count: a
        snapshot of what it just wrote is not ahead."""
        requires(self.supports_snapshots, "the backend declares no snapshot support")
        own_appends, saves, kept = _SNAPSHOTS[branch]
        agg = unique_id("acct")
        async with event_store.begin() as tx:
            await event_store.append(tx, agg, 0, [draft(agg, 1), draft(agg, 2)])
        head = 2 + own_appends
        for version, data in saves:
            snapshot = Snapshot(agg, "Account", version, data)
            if version > head:
                with pytest.raises(CodecError):
                    async with event_store.begin() as tx:
                        await event_store.save_snapshot(tx, snapshot)
                continue
            async with event_store.begin() as tx:
                if own_appends:
                    await event_store.append(tx, agg, 2, [draft(agg, n) for n in range(3, head + 1)])
                await event_store.save_snapshot(tx, snapshot)
        async with event_store.begin() as tx:
            stored = await event_store.load_snapshot(tx, agg)
        want = None if kept is None else Snapshot(agg, "Account", *kept)
        assert stored == want, f"{branch}: the snapshot kept"

    async def test_an_event_id_already_stored_is_refused(self, event_store: Store) -> None:
        """ "An event id is unique store-wide." Two rows under one id, and marking the first published
        prunes the second unpublished: an event that never reaches a consumer."""
        first, second = unique_id("acct"), unique_id("acct")
        taken = draft(first, 1)
        async with event_store.begin() as tx:
            await event_store.append(tx, first, 0, [taken])
        duplicate = replace(taken, aggregate_id=second)
        with pytest.raises(CodecError):
            async with event_store.begin() as tx:
                await event_store.append(tx, second, 0, [duplicate])
        async with event_store.begin() as tx:
            assert await event_store.load(tx, second) == [], "the refused append wrote nothing"

    async def test_two_writers_at_one_version_land_exactly_one_append(self, event_store: Store) -> None:
        """ "Enforce that with a unique constraint or a row lock in the database, never a read-then-write
        in Python." Two units of work both append at version 0, each staging before either commits. A
        store that checks the version and then inserts lets both land: two events at sequence 1, a forked
        stream. Exactly one may land, and the stream holds its event alone."""
        agg = unique_id("acct")
        first, second = draft(agg, 1), draft(agg, 1)
        outcomes = await race(
            event_store.begin,
            lambda tx: event_store.append(tx, agg, 0, [first]),
            lambda tx: event_store.append(tx, agg, 0, [second]),
        )
        assert sorted(outcomes) == ["conflict", "landed"], f"exactly one writer lands, got {outcomes}"
        winner = first if outcomes[0] == "landed" else second
        async with event_store.begin() as tx:
            assert [e.id for e in await event_store.load(tx, agg)] == [winner.id], "one event, the winner's"

    async def test_an_uncommitted_append_is_invisible_to_another_transaction(
        self, event_store: Store
    ) -> None:
        """ "nothing written through the yielded ``tx`` is visible elsewhere before commit." A reader in
        another unit of work that sees a staged event folds state that may still roll back."""
        agg = unique_id("acct")
        seen = await staged_then_read(
            event_store.begin,
            lambda tx: event_store.append(tx, agg, 0, [draft(agg, 1)]),
            lambda tx: event_store.load(tx, agg),
        )
        assert seen == [], "another transaction saw a staged append"


class EventOutboxContract(OutboxContract[EventMessage]):
    """The event outbox: the rows ``append`` stages, read back through the :class:`~edax.outbox.Outbox`
    port as the stored messages themselves, ``recorded_at`` included.

    Supply an ``outbox`` fixture, the read side, and an ``event_store`` fixture over the same database, which
    writes the rows. One object may be both.
    """

    async def write_rows(self, request: pytest.FixtureRequest, n: int, *, commit: bool) -> list[EventMessage]:
        event_store: Store = request.getfixturevalue("event_store")
        agg = unique_id("acct")
        drafts = [draft(agg, sequence) for sequence in range(1, n + 1)]
        if commit:
            async with event_store.begin() as tx:
                return await event_store.append(tx, agg, 0, drafts)
        stored: list[EventMessage] = []
        with pytest.raises(RolledBack):
            async with event_store.begin() as tx:
                stored = await event_store.append(tx, agg, 0, drafts)
                raise RolledBack
        return stored

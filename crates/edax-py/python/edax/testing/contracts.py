"""Executable conformance suites for the storage and broker seams.

Every backend an adopter writes is correctness-critical, and its correctness rests on clauses no type
and no review can check — whether ``append`` really compare-and-sets in the database rather than
read-then-writing in Python, whether ``claim`` can tell "I won" from "someone else holds this". Until
this package existed the only way to test a backend was to edit ``edax``'s own test files, which
means a misread clause is encoded identically in the implementation and in its test.

Subclass the contract for each seam you implement and supply the one fixture it asks for::

    from edax.testing import EventStoreContract


    class TestPostgresEventStore(EventStoreContract):
        @pytest.fixture
        async def event_store(self, pool):
            await reset(pool)
            return PostgresEventStore(pool)

Name the subclass ``Test...`` so pytest collects it; the ``*Contract`` bases are not collected
themselves. The suites are ``async def``, so they need ``asyncio_mode = "auto"`` in your pytest
configuration (the ``test`` extra installs ``pytest-asyncio``).

**Capabilities.** Backends differ in ways the contract allows, so a clause that cannot apply is
skipped rather than failed. Declare them as class attributes on your subclass — the defaults describe
a transactional SQL backend, which is the common case and the strictest.

Those skips `raise pytest.skip.Exception(...)` rather than calling `pytest.skip(...)`, which is the
same thing at runtime and reports identically. The call form does not type-check under pytest 8,
whose `skip` is declared through a `_WithException` protocol a checker reads as taking no arguments
at all.

**Ambient enlistment.** The SDK's guards and the saga runner call a store with no ``tx`` inside
:func:`~edax.store.transaction`, so the store must join :func:`~edax.store.current_tx` of its own
transactor. Each seam with an optional ``tx`` has a clause for that path, apart from the one that passes
``tx=`` itself: a store that honours an explicit handle and auto-commits without one passes every explicit
clause.

**Concurrency and isolation.** A clause that runs one call at a time cannot tell a compare-and-set in the
database from a read-then-write in Python. The race clauses run two units of work that each stage their
write before either commits, forced in that order, not left to timing: the second one's write runs in a
task of its own, so a store that blocks it on the first one's row lock does not deadlock the clause, which
waits a bounded moment for it and then commits the first. Exactly one of the two may land. The isolation
clauses check that a staged write is invisible to every other reader until it commits. Both need an async
driver: a driver that blocks the event loop on a row lock deadlocks any concurrent asyncio code, this kit
included.

**Payloads.** Every envelope carries a time-ordered :func:`~edax.guid` id, a nanosecond
:func:`~edax.current_timestamp` and a real JSON body, as the SDK writes them, so a store with an ``id
uuid`` column or a ``timestamptz`` column takes them as they come in production.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Any, Literal, Protocol

import pytest

from .. import _edax
from ..aggregate import Event, EventEnvelope
from ..broker import Consumer
from ..command import Record, Won, joins_transaction
from ..dispatcher import Delivery
from ..errors import BadHandle, CodecError, ConcurrencyError, StoreError
from ..filter import eq
from ..projection import Entity
from ..query import encode_cursor, sort_key
from ..store import Snapshot, Tx, transaction

__all__ = [
    "BrokerContract",
    "BrokerRig",
    "EventStoreContract",
    "IdempotencyStoreContract",
    "ProjectionStoreContract",
    "SagaStoreContract",
]


def envelope(aggregate_id: str, sequence: int | None, *, name: str = "Deposited") -> EventEnvelope:
    """One event envelope as the SDK writes it: a fresh time-ordered id, the current nanosecond timestamp, a
    real JSON body and the correlation metadata a repository stamps."""
    return EventEnvelope(
        id=_edax.guid(),
        aggregate_id=aggregate_id,
        event=Event(name, 1, f'{{"account_id":"{aggregate_id}","amount":100,"currency":"EUR"}}'.encode()),
        namespace={"tenant": "acme"},
        metadata={"correlation_id": _edax.guid()},
        sequence=sequence,
        timestamp=_edax.current_timestamp(),
    )


def _id(prefix: str) -> str:
    """A fresh id per test, so a suite run against a durable store needs no truncation between tests."""
    return f"{prefix}-{_edax.guid()}"


#: How long a clause waits for a delivery the contract says must come, or for a raced unit of work to
#: finish; also how long it waits to be sure a delivery that must not come does not.
_POLL_S = 1.0
_QUIET_S = 0.05

#: How long a race clause waits for the second unit of work's write before it decides the write is blocked
#: on the first one's row lock and commits the first. A non-blocking store answers at once, so the clause
#: never waits this long for one; only a store that parks the second writer does.
_BLOCKED_S = 0.2

Outcome = Literal["landed", "conflict"]


async def _race(
    unit_of_work: Callable[[], AbstractAsyncContextManager[Tx]],
    first: Callable[[Tx], Awaitable[object]],
    second: Callable[[Tx], Awaitable[object]],
) -> tuple[Outcome, Outcome]:
    """Run two units of work that each stage their write before either commits, and report how each ended.

    The order is forced: the first stages its write, then the second stages its own (in a task of its own,
    so a store that blocks it on the first one's row lock does not deadlock this coroutine), then the first
    commits, then the second does. A second write the store parks is given :data:`_BLOCKED_S` to answer
    before the first commits anyway. :class:`~edax.errors.ConcurrencyError` from a write or a commit is a
    ``conflict``; any other error fails the clause."""
    second_staged = asyncio.Event()
    second_may_commit = asyncio.Event()

    async def run_second() -> Outcome:
        try:
            async with unit_of_work() as tx:
                await second(tx)
                second_staged.set()
                await second_may_commit.wait()
        except ConcurrencyError:
            return "conflict"
        finally:
            second_staged.set()
        return "landed"

    racer: asyncio.Task[Outcome] | None = None
    try:
        async with unit_of_work() as tx:
            await first(tx)
            racer = asyncio.create_task(run_second())
            staged = asyncio.create_task(second_staged.wait())
            await asyncio.wait({staged, racer}, timeout=_BLOCKED_S, return_when=asyncio.FIRST_COMPLETED)
            staged.cancel()
    except ConcurrencyError:
        first_outcome: Outcome = "conflict"
    else:
        first_outcome = "landed"
    second_may_commit.set()
    assert racer is not None, "the first unit of work failed before the second one started"
    return first_outcome, await asyncio.wait_for(racer, _POLL_S)


class _RolledBack(Exception):
    """The writer's way out of :func:`_staged_then_read`: a rollback, never a commit."""


async def _staged_then_read(
    unit_of_work: Callable[[], AbstractAsyncContextManager[Tx]],
    write: Callable[[Tx], Awaitable[object]],
    read: Callable[[Tx], Awaitable[object]],
) -> object:
    """Stage ``write`` in one unit of work and, while it is open, ``read`` in another; return what the
    reader saw. The reader runs in a task of its own, bounded, so a store whose reader blocks on the
    writer's lock answers once the writer rolls back. The writer always rolls back."""
    seen: list[object] = []

    async def reader() -> None:
        async with unit_of_work() as tx:
            seen.append(await read(tx))

    task: asyncio.Task[None] | None = None
    try:
        async with unit_of_work() as tx:
            await write(tx)
            task = asyncio.create_task(reader())
            await asyncio.wait({task}, timeout=_BLOCKED_S)
            raise _RolledBack
    except _RolledBack:
        pass
    assert task is not None
    await asyncio.wait_for(task, _POLL_S)
    return seen[0]


class EventStoreContract:
    """The durable log: per-aggregate events under optimistic concurrency, snapshots, and the outbox.

    Supply an ``event_store`` fixture returning a store whose ``transaction()`` is usable.
    """

    #: The transaction really rolls back. False only for a store with no unit of work at all.
    supports_rollback: bool = True
    #: ``load_snapshot`` / ``save_snapshot`` are implemented.
    supports_snapshots: bool = True
    #: ``unpublished`` / ``mark_published`` are implemented (the transactional outbox).
    supports_outbox: bool = True
    #: ``unpublished_count`` is implemented. Optional: a store that cannot count the backlog cheaply
    #: leaves it out, and the host watches :attr:`~edax.store.Relay.last` alone.
    supports_unpublished_count: bool = False

    async def test_an_empty_stream_is_version_zero(self, event_store: Any) -> None:
        """ "An aggregate's version is the ``sequence`` of its last stored event, ``0`` for an empty
        stream." A store that cannot say "nothing here" cannot be appended to correctly."""
        async with event_store.transaction() as tx:
            assert await event_store.load(tx, _id("absent")) == []

    async def test_append_then_load_returns_what_was_written(self, event_store: Any) -> None:
        """ "``load`` returns every event with ``sequence > after``, ascending, gap-free." The store
        never renumbers: the sequences that come back are the ones handed in."""
        agg = _id("acct")
        written = [envelope(agg, 1), envelope(agg, 2)]
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, written)
            loaded = await event_store.load(tx, agg)
        assert [e.sequence for e in loaded] == [1, 2], "ascending and gap-free, as handed in"
        assert [e.id for e in loaded] == [e.id for e in written]

    async def test_load_after_skips_what_the_caller_already_has(self, event_store: Any) -> None:
        """ "every event with ``sequence > after``" — strictly after, so the row at ``after`` is not
        replayed. An off-by-one here double-applies one event per load."""
        agg = _id("acct")
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, n) for n in (1, 2, 3)])
            assert [e.sequence for e in await event_store.load(tx, agg, after=2)] == [3]
            assert [e.sequence for e in await event_store.load(tx, agg, after=3)] == []

    async def test_load_sees_this_transactions_own_appends(self, event_store: Any) -> None:
        """ "including this ``tx``'s own uncommitted appends." A `Repository` loads, decides, and
        appends inside one transaction; a store that reads only committed rows makes it decide on
        stale state."""
        agg = _id("acct")
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, 1)])
            assert len(await event_store.load(tx, agg)) == 1, "uncommitted, but visible to its own tx"

    async def test_append_is_compare_and_set_on_the_stored_version(self, event_store: Any) -> None:
        """ "``append`` succeeds only if the stored version equals ``expected_version``, else it raises
        ``ConcurrencyError`` and writes nothing."

        The single most important clause in the seam, and the one a read-then-write in Python passes
        in a test and loses under concurrency. Both directions are checked: stale *and* ahead.
        """
        agg = _id("acct")
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, 1)])

        async with event_store.transaction() as tx:
            with pytest.raises(ConcurrencyError):
                await event_store.append(tx, agg, 0, [envelope(agg, 2)])  # stale: stored is 1

        async with event_store.transaction() as tx:
            with pytest.raises(ConcurrencyError):
                await event_store.append(tx, agg, 7, [envelope(agg, 8)])  # ahead: nothing wrote 2..7

        async with event_store.transaction() as tx:
            assert [e.sequence for e in await event_store.load(tx, agg)] == [1], "and wrote nothing"

    async def test_a_refused_append_writes_nothing(self, event_store: Any) -> None:
        """ "else it raises ``ConcurrencyError`` and writes nothing" — including the events before the
        one that conflicts. A store that writes the batch and then checks leaves a partial stream."""
        agg = _id("acct")
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, 1)])
        async with event_store.transaction() as tx:
            with pytest.raises(ConcurrencyError):
                await event_store.append(tx, agg, 0, [envelope(agg, 2), envelope(agg, 3)])
        async with event_store.transaction() as tx:
            assert [e.sequence for e in await event_store.load(tx, agg)] == [1]

    @pytest.mark.parametrize(
        "branch, sequences",
        [
            ("gap", [2, 4]),
            ("repeat", [2, 2]),
            ("reordered", [3, 2]),
            ("unsequenced", [2, None]),
        ],
    )
    async def test_a_batch_that_is_not_dense_is_refused_and_writes_nothing(
        self, event_store: Any, branch: str, sequences: list[int | None]
    ) -> None:
        """ "A batch must carry ``expected_version + 1, + 2, ...`` in order, every envelope sequenced."
        Checking only the stored version passes a batch with a gap, a repeat or a reordering, and the
        stream then breaks the gap-free ascending read every reader relies on. The refusal is the
        kernel's terminal ``CodecError`` (:func:`~edax.store.check_sequence_batch`): the same batch
        fails the same way on every retry, so a transient error would loop forever."""
        agg = _id("acct")
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, 1)])
        async with event_store.transaction() as tx:
            with pytest.raises(CodecError) as caught:
                batch = [envelope(agg, n, name=f"T{i}") for i, n in enumerate(sequences)]
                await event_store.append(tx, agg, 1, batch)
        assert caught.value.terminal, branch
        async with event_store.transaction() as tx:
            loaded = await event_store.load(tx, agg)
        assert [e.sequence for e in loaded] == [1], f"{branch}: and wrote nothing"

    async def test_every_envelope_field_survives_the_round_trip(self, event_store: Any) -> None:
        """A store that persists only the fields it recognises silently drops the rest.

        `namespace` carries tenancy, `metadata` carries the correlation and causation ids the whole
        tracing story depends on, and `timestamp` orders nothing but is read by humans. A column
        missing for any of them loses data no replay can recover. Non-ASCII included, because a
        latin-1 column passes every test written in English.
        """
        agg = _id("acct")
        body = f'{{"account_id":"{agg}","amount":100,"currency":"EUR","memo":"déjà-vu"}}'
        env = EventEnvelope(
            id=_edax.guid(),
            aggregate_id=agg,
            event=Event("Deposited", 3, body.encode()),
            namespace={"tenant": "acme"},
            metadata={"correlation_id": _edax.guid(), "causation_id": _edax.guid(), "note": "déjà-vu"},
            sequence=1,
            timestamp=_edax.current_timestamp(),
        )
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [env])
            (got,) = await event_store.load(tx, agg)
        assert got == env, "every field, the event's nested ones included, survived"

    async def test_an_empty_append_still_checks_the_version(self, event_store: Any) -> None:
        """`Repository.save` on an aggregate with nothing pending still calls `append`. It must remain
        a compare-and-set: a store that shortcuts an empty batch lets a stale caller believe it holds
        the current version, and the next save writes over someone else's work."""
        agg = _id("acct")
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, 1)])

        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 1, [])  # correct version, nothing to write
            assert [e.sequence for e in await event_store.load(tx, agg)] == [1], "and added nothing"

        # The part that matters: an empty batch is still compare-and-set, not a free pass.
        async with event_store.transaction() as tx:
            with pytest.raises(ConcurrencyError):
                await event_store.append(tx, agg, 7, [])

    async def test_streams_are_independent(self, event_store: Any) -> None:
        """Versions are per aggregate. A store keying concurrency on a global sequence makes every
        aggregate contend with every other one."""
        a, b = _id("a"), _id("b")
        async with event_store.transaction() as tx:
            await event_store.append(tx, a, 0, [envelope(a, 1)])
            await event_store.append(tx, b, 0, [envelope(b, 1)])
            assert len(await event_store.load(tx, a)) == 1
            assert len(await event_store.load(tx, b)) == 1

    async def test_a_rollback_removes_the_appended_events(self, event_store: Any) -> None:
        """ "a rollback removes all of them." This is what lets a failed handler leave no trace, and
        what the shared-ledger idempotency mode is built on."""
        if not self.supports_rollback:
            raise pytest.skip.Exception("backend declares no rollback")
        agg = _id("acct")
        with pytest.raises(RuntimeError):
            async with event_store.transaction() as tx:
                await event_store.append(tx, agg, 0, [envelope(agg, 1)])
                raise RuntimeError("the handler failed")
        async with event_store.transaction() as tx:
            assert await event_store.load(tx, agg) == [], "the rollback took the events with it"

    async def test_a_snapshot_round_trips_and_never_leads_the_stream(self, event_store: Any) -> None:
        """ "``load_snapshot`` returns the latest snapshot or ``None``, and may lag the stream but
        never lead it." A snapshot ahead of the events replays state that was never committed."""
        if not self.supports_snapshots:
            raise pytest.skip.Exception("backend declares no snapshot support")
        agg = _id("acct")
        async with event_store.transaction() as tx:
            assert await event_store.load_snapshot(tx, agg) is None, "none stored yet"
            await event_store.append(tx, agg, 0, [envelope(agg, 1), envelope(agg, 2)])
            await event_store.save_snapshot(tx, Snapshot(agg, "Account", 2, b'{"balance":1}'))
        async with event_store.transaction() as tx:
            snap = await event_store.load_snapshot(tx, agg)
            assert snap is not None and snap.version == 2 and snap.data == b'{"balance":1}'
            stream = await event_store.load(tx, agg)
            assert snap.version <= stream[-1].sequence, "a snapshot may lag, never lead"

    async def test_the_latest_snapshot_wins(self, event_store: Any) -> None:
        """ "the *latest* snapshot": a later save supersedes an earlier one rather than accumulating, and a
        slow writer's older snapshot arriving after a newer one never rolls the cache back, or the
        repository would replay from the older version on every load."""
        if not self.supports_snapshots:
            raise pytest.skip.Exception("backend declares no snapshot support")
        agg = _id("acct")
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, n) for n in (1, 2)])
            await event_store.save_snapshot(tx, Snapshot(agg, "Account", 1, b"first"))
            await event_store.save_snapshot(tx, Snapshot(agg, "Account", 2, b"newer"))
        async with event_store.transaction() as tx:
            await event_store.save_snapshot(tx, Snapshot(agg, "Account", 1, b"older"))
        async with event_store.transaction() as tx:
            snap = await event_store.load_snapshot(tx, agg)
            assert snap is not None and (snap.version, snap.data) == (2, b"newer")

    async def test_a_snapshot_leading_the_stream_is_refused(self, event_store: Any) -> None:
        """ "A snapshot may lag the stream but never lead it." One past the stream's version would make
        the repository load it, find no events after it, and conflict on every append from then on."""
        if not self.supports_snapshots:
            raise pytest.skip.Exception("backend declares no snapshot support")
        agg = _id("acct")
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, 1)])
        with pytest.raises(CodecError):
            async with event_store.transaction() as tx:
                await event_store.save_snapshot(tx, Snapshot(agg, "Account", 2, b"ahead"))
        async with event_store.transaction() as tx:
            # The transaction's own appends count: a snapshot of what it just wrote is not ahead.
            await event_store.append(tx, agg, 1, [envelope(agg, 2)])
            await event_store.save_snapshot(tx, Snapshot(agg, "Account", 2, b"level"))
        async with event_store.transaction() as tx:
            snap = await event_store.load_snapshot(tx, agg)
            assert snap is not None and (snap.version, snap.data) == (2, b"level")

    async def test_an_envelope_id_already_stored_is_refused(self, event_store: Any) -> None:
        """ "An envelope id is unique store-wide." Two rows under one id, and marking the first published
        prunes the second unpublished: an event that never reaches a consumer."""
        first, second = _id("acct"), _id("acct")
        taken = envelope(first, 1)
        async with event_store.transaction() as tx:
            await event_store.append(tx, first, 0, [taken])
        duplicate = EventEnvelope(
            id=taken.id,
            aggregate_id=second,
            event=taken.event,
            namespace=taken.namespace,
            metadata=taken.metadata,
            sequence=1,
            timestamp=taken.timestamp,
        )
        with pytest.raises(CodecError):
            async with event_store.transaction() as tx:
                await event_store.append(tx, second, 0, [duplicate])
        async with event_store.transaction() as tx:
            assert await event_store.load(tx, second) == [], "the refused append wrote nothing"

    async def test_the_outbox_holds_a_row_per_appended_event(self, event_store: Any) -> None:
        """ "Within one ``append`` the event rows, their outbox rows and any snapshot saved in the same
        ``tx`` commit together." The outbox is what makes publishing atomic with the state change."""
        if not self.supports_outbox:
            raise pytest.skip.Exception("backend declares no outbox")
        agg = _id("acct")
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, 1), envelope(agg, 2)])
        mine = [e for e in await event_store.unpublished(limit=1000) if e.aggregate_id == agg]
        assert [e.sequence for e in mine] == [1, 2], "one row per event, in sequence order"

    async def test_the_outbox_never_shows_an_uncommitted_row(self, event_store: Any) -> None:
        """ "``unpublished`` returns only *committed* unmarked rows." A relay that can see an
        uncommitted row publishes an event whose state change may still roll back."""
        if not (self.supports_outbox and self.supports_rollback):
            raise pytest.skip.Exception("backend declares no outbox or no rollback")
        agg = _id("acct")
        with pytest.raises(RuntimeError):
            async with event_store.transaction() as tx:
                await event_store.append(tx, agg, 0, [envelope(agg, 1)])
                raise RuntimeError("rolled back")
        mine = [e for e in await event_store.unpublished(limit=1000) if e.aggregate_id == agg]
        assert mine == [], "a rolled-back append leaves nothing to publish"

    async def test_mark_published_is_idempotent_and_ignores_unknown_ids(self, event_store: Any) -> None:
        """ "``mark_published`` is idempotent; unknown ids are ignored." A relay is at-least-once, so
        it re-marks after a lost response, and a second mark must not be an error."""
        if not self.supports_outbox:
            raise pytest.skip.Exception("backend declares no outbox")
        agg = _id("acct")
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, 1)])
        ids = [e.id for e in await event_store.unpublished(limit=1000) if e.aggregate_id == agg]
        assert ids, "the append produced an outbox row"
        await event_store.mark_published(ids)
        await event_store.mark_published(ids)  # idempotent: the relay retried after a lost response
        await event_store.mark_published([_edax.guid()])  # unknown ids are ignored
        left = [e for e in await event_store.unpublished(limit=1000) if e.aggregate_id == agg]
        assert left == [], "marked rows stop coming back"

    async def test_unpublished_count_tracks_what_is_waiting(self, event_store: Any) -> None:
        """ "``unpublished_count`` is how deep the outbox is right now." A count that ignores
        ``mark_published``, or counts uncommitted rows, reports a backlog that never drains and makes
        the health check it exists for permanently red."""
        if not (self.supports_outbox and self.supports_unpublished_count):
            raise pytest.skip.Exception("backend declares no outbox or no backlog count")
        agg = _id("acct")
        # Deltas, not absolutes: the store may hold rows from other clauses in this suite.
        before = await event_store.unpublished_count()
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, n) for n in (1, 2)])
        assert await event_store.unpublished_count() - before == 2, "one row per appended event"
        ids = [e.id for e in await event_store.unpublished(limit=1000) if e.aggregate_id == agg]
        await event_store.mark_published(ids[:1])
        assert await event_store.unpublished_count() - before == 1, "a marked row stops counting"

    async def test_unpublished_honours_its_limit(self, event_store: Any) -> None:
        """ "Up to ``limit`` committed unmarked rows." A store ignoring the limit hands a relay the
        whole backlog in one batch, and the relay's duplicate window becomes the whole backlog."""
        if not self.supports_outbox:
            raise pytest.skip.Exception("backend declares no outbox")
        agg = _id("acct")
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, n) for n in (1, 2, 3)])
        assert len(await event_store.unpublished(limit=2)) == 2, "three rows wait, so the limit decides"

    async def test_two_writers_at_one_version_land_exactly_one_append(self, event_store: Any) -> None:
        """ "Enforce that with a unique constraint or a row lock in the database, never a read-then-write
        in Python." Two units of work both append at version 0, each staging before either commits. A
        store that checks the version and then inserts lets both land: two events at sequence 1, a forked
        stream. Exactly one may land, and the stream holds its event alone."""
        agg = _id("acct")
        first, second = envelope(agg, 1), envelope(agg, 1)
        outcomes = await _race(
            event_store.transaction,
            lambda tx: event_store.append(tx, agg, 0, [first]),
            lambda tx: event_store.append(tx, agg, 0, [second]),
        )
        assert sorted(outcomes) == ["conflict", "landed"], f"exactly one writer lands, got {outcomes}"
        winner = first if outcomes[0] == "landed" else second
        async with event_store.transaction() as tx:
            assert [e.id for e in await event_store.load(tx, agg)] == [winner.id], "one event, the winner's"

    async def test_an_uncommitted_append_is_invisible_to_another_transaction(self, event_store: Any) -> None:
        """ "nothing written through the yielded ``tx`` is visible elsewhere before commit." A reader in
        another unit of work that sees a staged event folds state that may still roll back."""
        agg = _id("acct")
        seen = await _staged_then_read(
            event_store.transaction,
            lambda tx: event_store.append(tx, agg, 0, [envelope(agg, 1)]),
            lambda tx: event_store.load(tx, agg),
        )
        assert seen == [], "another transaction saw a staged append"


class ProjectionStoreContract:
    """Read-model documents under optimistic concurrency.

    Supply a ``projection_store`` fixture.
    """

    #: ``get_many`` pages identically to the kernel rather than being the backend's own dialect. Gates the
    #: three paging clauses.
    supports_kernel_queries: bool = False
    #: There is a unit of work to join. Requires a ``transactor`` fixture when true; a backend that
    #: declares ``False`` needs no such fixture.
    supports_transactions: bool = True

    async def test_an_absent_entity_reads_as_none(self, projection_store: Any) -> None:
        """ "``get`` returns the current committed row plus the ``tx``'s own writes, or ``None``." """
        assert await projection_store.get(_id("absent")) is None

    async def test_save_then_get_round_trips_and_advances_the_version(self, projection_store: Any) -> None:
        """ "A save persists at ``version + 1``, then advances ``version`` in place." A store that
        does not advance it makes the caller's next save conflict with itself."""
        entity = Entity(_id("acct"), 0, {"balance": 1})
        await projection_store.save(entity)
        assert entity.version == 1, "advanced in place, on the caller's object"
        stored = await projection_store.get(entity.id)
        assert stored is not None and stored.document == {"balance": 1} and stored.version == 1

    async def test_save_is_compare_and_set_on_the_version(self, projection_store: Any) -> None:
        """ "``save`` writes at ``version + 1`` only if the stored version still equals
        ``entity.version`` (``0`` means not yet stored), else it raises ``ConcurrencyError`` and
        writes nothing." Enforce it in the database, not by reading first."""
        entity = Entity(_id("acct"), 0, {"balance": 1})
        await projection_store.save(entity)

        stale = Entity(entity.id, 0, {"balance": 99})  # a second writer who loaded before the save
        with pytest.raises(ConcurrencyError):
            await projection_store.save(stale)

        ahead = Entity(entity.id, 7, {"balance": 99})
        with pytest.raises(ConcurrencyError):
            await projection_store.save(ahead)

        stored = await projection_store.get(entity.id)
        assert stored is not None and stored.document == {"balance": 1}, "and wrote nothing"

    async def test_a_refused_save_leaves_the_callers_version_alone(self, projection_store: Any) -> None:
        """A conflict must not advance ``entity.version``: the caller reloads and retries, and a
        version bumped by a failed save makes that retry conflict forever."""
        entity = Entity(_id("acct"), 0, {"balance": 1})
        await projection_store.save(entity)
        stale = Entity(entity.id, 0, {"balance": 99})
        with pytest.raises(ConcurrencyError):
            await projection_store.save(stale)
        assert stale.version == 0, "the failed save left the version untouched"

    async def test_get_sees_this_transactions_own_save(self, projection_store: Any, request: Any) -> None:
        """ "`get` returns the current committed row plus the `tx`'s own writes."

        A handler that saves and then re-reads inside one unit of work must see its own write, or it
        decides on stale state and the second save conflicts with the first. Needs a `transactor`
        fixture; declare `supports_transactions = False` if your store has no unit of work.
        """
        if not self.supports_transactions:
            raise pytest.skip.Exception("backend declares no unit of work")
        # Resolved lazily so a backend that skips this clause need not define the fixture at all.
        transactor = request.getfixturevalue("transactor")
        entity = Entity(_id("acct"), 0, {"balance": 1})
        async with transactor.transaction() as tx:
            await projection_store.save(entity, tx=tx)
            seen = await projection_store.get(entity.id, tx=tx)
            assert seen is not None and seen.document == {"balance": 1}, "its own uncommitted write"

    async def test_a_save_with_no_tx_joins_the_ambient_transaction(
        self, projection_store: Any, request: Any
    ) -> None:
        """ "A save under ``tx`` (or the ambient :func:`~edax.store.current_tx`) is staged until the
        transactor commits." A projector saves with no ``tx`` inside the unit of work the guard opened;
        a store that auto-commits there keeps a read-model write the rollback should have dropped."""
        if not self.supports_transactions:
            raise pytest.skip.Exception("backend declares no unit of work")
        transactor = request.getfixturevalue("transactor")
        entity = Entity(_id("acct"), 0, {"balance": 1})
        with pytest.raises(RuntimeError):
            async with transaction(transactor):
                await projection_store.save(entity)
                raise RuntimeError("the handler failed")
        assert await projection_store.get(entity.id) is None, "the rollback took the save with it"

    async def test_two_writers_at_one_version_land_exactly_one_save(
        self, projection_store: Any, request: Any
    ) -> None:
        """ "Enforce it in the database, not by reading first." Two projectors both save a new row at
        version 0, each staging before either commits. A store that reads the version and then writes
        lets the second overwrite the first: a read-model update lost. Exactly one may land."""
        if not self.supports_transactions:
            raise pytest.skip.Exception("backend declares no unit of work")
        transactor = request.getfixturevalue("transactor")
        doc_id = _id("acct")
        outcomes = await _race(
            transactor.transaction,
            lambda tx: projection_store.save(Entity(doc_id, 0, {"balance": 1}), tx=tx),
            lambda tx: projection_store.save(Entity(doc_id, 0, {"balance": 2}), tx=tx),
        )
        assert sorted(outcomes) == ["conflict", "landed"], f"exactly one writer lands, got {outcomes}"
        stored = await projection_store.get(doc_id)
        want = {"balance": 1} if outcomes[0] == "landed" else {"balance": 2}
        assert stored is not None and stored.version == 1 and stored.document == want, "the winner's row"

    async def test_an_uncommitted_save_is_invisible_to_another_transaction(
        self, projection_store: Any, request: Any
    ) -> None:
        """ "``get`` returns the current committed row plus the ``tx``'s own writes." Another unit of
        work's staged row is neither: a reader that sees it serves a document that may still roll back."""
        if not self.supports_transactions:
            raise pytest.skip.Exception("backend declares no unit of work")
        transactor = request.getfixturevalue("transactor")
        doc_id = _id("acct")
        seen = await _staged_then_read(
            transactor.transaction,
            lambda tx: projection_store.save(Entity(doc_id, 0, {"balance": 1}), tx=tx),
            lambda tx: projection_store.get(doc_id, tx=tx),
        )
        assert seen is None, "another transaction saw a staged row"

    async def test_entities_are_independent(self, projection_store: Any) -> None:
        """Concurrency is per id. A store versioning the whole table serialises every read model."""
        a, b = Entity(_id("a"), 0, {"v": 1}), Entity(_id("b"), 0, {"v": 2})
        await projection_store.save(a)
        await projection_store.save(b)
        got_a, got_b = await projection_store.get(a.id), await projection_store.get(b.id)
        assert got_a is not None and got_a.document == {"v": 1}
        assert got_b is not None and got_b.document == {"v": 2}

    async def _seed_ranked(self, projection_store: Any, run: str, ranks: list[int]) -> list[str]:
        """Save one document per rank under a filterable ``run`` tag, so a durable store shared across
        tests pages only this test's rows. Returns the ids in the kernel's order: rank, then id."""
        ids = [f"{run}-{i:02d}" for i in range(len(ranks))]
        for doc_id, rank in zip(ids, ranks, strict=True):
            await projection_store.save(Entity(doc_id, 0, {"id": doc_id, "run": run, "rank": rank}))
        return [doc_id for _, doc_id in sorted(zip(ranks, ids, strict=True))]

    async def test_get_many_pages_every_row_once_by_its_next_cursor(self, projection_store: Any) -> None:
        """ "Keyset resume strictly after the cursor tuple", and a ``next`` minted only while rows
        follow. Rows that tie on the sort key are told apart by ``id``, and a row committed between
        pages after the cursor arrives too, so a reader following ``next`` sees each row once and stops
        on the last page. A store paging by offset, or one that drops ``next``, loses or repeats rows."""
        if not self.supports_kernel_queries:
            raise pytest.skip.Exception("backend pages in its own dialect")
        run = _id("run")
        order = await self._seed_ranked(projection_store, run, [2, 1, 1, 1, 3])
        spec: dict[str, Any] = {"filter": eq("run", run), "sort": [sort_key("rank")], "limit": 2}
        page = await projection_store.get_many(**spec)
        seen = [row["id"] for row in page.rows]
        late, early = f"{run}-late", f"{run}-early"
        await projection_store.save(Entity(late, 0, {"id": late, "run": run, "rank": 9}))  # after the cursor
        await projection_store.save(Entity(early, 0, {"id": early, "run": run, "rank": 0}))  # before it
        for _ in range(len(order) + 2):  # bounded, so a store whose cursor repeats a row fails, not hangs
            if page.next is None:
                break
            page = await projection_store.get_many(**spec, after=page.next)
            seen.extend(row["id"] for row in page.rows)
        assert seen == [*order, late], "every row once, in order, the late row last, the early one never"

    async def test_next_is_the_kernels_cursor(self, projection_store: Any) -> None:
        """ "A ``next`` minted with ``encode_cursor`` from the last row." A store minting its own
        cursor pages within one service but not across them: another service, or the same one after a
        backend change, cannot read it."""
        if not self.supports_kernel_queries:
            raise pytest.skip.Exception("backend pages in its own dialect")
        run = _id("run")
        await self._seed_ranked(projection_store, run, [1, 2, 3])
        sort = [sort_key("rank")]
        page = await projection_store.get_many(filter=eq("run", run), sort=sort, limit=2)
        assert page.next == encode_cursor(page.rows[-1], sort)

    async def test_a_cursor_minted_under_another_sort_is_refused(self, projection_store: Any) -> None:
        """ "Read ``after`` with ``decode_cursor``, which refuses a cursor minted under another sort."
        Read anyway, the cursor's values bind to the wrong keys and the page is garbage, silently."""
        if not self.supports_kernel_queries:
            raise pytest.skip.Exception("backend pages in its own dialect")
        run = _id("run")
        await self._seed_ranked(projection_store, run, [1, 2, 3])
        page = await projection_store.get_many(filter=eq("run", run), sort=[sort_key("rank")], limit=1)
        with pytest.raises(CodecError):
            await projection_store.get_many(
                filter=eq("run", run), sort=[sort_key("rank", desc=True)], after=page.next
            )


def _saga_command(cid: str, version: int, index: int) -> Any:
    """The ``index``-th command a saga step committing ``version`` of ``cid`` sends: the kernel's id, a
    real ``Fulfill`` payload."""
    return _edax.SagaCommand(
        _edax.saga_command_id(cid, version, index),
        cid,
        Event("Fulfill", 1, f'{{"order_id":"{cid}"}}'.encode()),
        1_714_564_800_000_000_000,
    )


class SagaStoreContract:
    """Per-correlation saga instance state under optimistic concurrency, its tombstone, and the saga
    outbox.

    Supply a ``saga_store`` fixture.
    """

    #: There is a unit of work to join. Requires a ``transactor`` fixture when true; a backend that
    #: declares ``False`` needs no such fixture.
    supports_transactions: bool = True

    async def _unpublished(self, saga_store: Any, cid: str) -> list[str]:
        return [c.id for c in await saga_store.unpublished(limit=1000) if c.correlation_id == cid]

    async def test_an_unknown_correlation_loads_as_none(self, saga_store: Any) -> None:
        """A saga that has not started yet has no instance; the runner reads ``None`` as "new"."""
        assert await saga_store.load(_id("corr")) is None

    async def test_save_then_load_round_trips_the_state(self, saga_store: Any) -> None:
        """The runner's whole job is fold-and-persist, so the state must come back as it went in."""
        cid = _id("corr")
        version = await saga_store.save(cid, {"step": "awaiting_payment"}, 0, "", [])
        instance = await saga_store.load(cid)
        assert instance is not None and instance.state == {"step": "awaiting_payment"}
        assert instance.version == version == 1 and not instance.completed

    async def test_save_is_compare_and_set_on_the_version(self, saga_store: Any) -> None:
        """Two events for one correlation can arrive concurrently. Without a CAS here one fold
        silently overwrites the other and the saga forgets a step."""
        cid = _id("corr")
        await saga_store.save(cid, {"step": "one"}, 0, "", [])
        with pytest.raises(ConcurrencyError):
            await saga_store.save(cid, {"step": "clobbered"}, 0, "", [])
        instance = await saga_store.load(cid)
        assert instance is not None and instance.state == {"step": "one"}

    async def test_complete_writes_a_versioned_tombstone(self, saga_store: Any) -> None:
        """Completion is a write at the loaded version that keeps the row: a stale completion conflicts,
        and the instance then reads as completed one version on, not as absent."""
        cid = _id("corr")
        version = await saga_store.save(cid, {"step": "one"}, 0, "", [])
        with pytest.raises(ConcurrencyError):
            await saga_store.complete(cid, version + 5, [])
        assert await saga_store.complete(cid, version, []) == version + 1
        instance = await saga_store.load(cid)
        assert instance is not None and instance.completed and instance.version == version + 1

    async def test_a_completed_instance_takes_no_write(self, saga_store: Any) -> None:
        """The tombstone is final. A writer that loaded the instance before it completed (ABA), a late
        event that would start the saga again at version 0 (resurrection), and a second completion all
        conflict, and the tombstone stays."""
        cid = _id("corr")
        await saga_store.save(cid, {"step": "one"}, 0, "", [])
        await saga_store.complete(cid, 1, [])
        writes: list[tuple[str, Callable[[], Awaitable[object]]]] = [
            ("stale_writer", lambda: saga_store.save(cid, {"step": "two"}, 1, "", [])),
            ("fresh_start", lambda: saga_store.save(cid, {"step": "one"}, 0, "", [])),
            ("at_the_tombstone", lambda: saga_store.save(cid, {"step": "two"}, 2, "", [])),
            ("second_completion", lambda: saga_store.complete(cid, 2, [])),
        ]
        for name, write in writes:
            with pytest.raises(ConcurrencyError):
                await write()
            instance = await saga_store.load(cid)
            assert instance is not None and instance.completed and instance.version == 2, name

    async def test_the_steps_commands_commit_with_its_state(self, saga_store: Any) -> None:
        """The saga outbox: a write's commands are shown once it committed, oldest first, with every
        field intact; a refused write enqueues none, so a fold that lost the race sends nothing."""
        cid = _id("corr")
        await saga_store.save(cid, {"step": "one"}, 0, "", [_saga_command(cid, 1, 0)])
        with pytest.raises(ConcurrencyError):
            await saga_store.save(cid, {"step": "lost"}, 0, "", [_saga_command(cid, 1, 0)])
        await saga_store.complete(cid, 1, [_saga_command(cid, 2, 0), _saga_command(cid, 2, 1)])
        want = _saga_command(cid, 2, 1)
        sent = [_saga_command(cid, 1, 0).id, _saga_command(cid, 2, 0).id, want.id]
        assert await self._unpublished(saga_store, cid) == sent
        row = next(c for c in await saga_store.unpublished(limit=1000) if c.id == want.id)
        assert (
            row.correlation_id,
            row.command.name,
            row.command.version,
            bytes(row.command.data),
            row.timestamp,
        ) == (
            want.correlation_id,
            want.command.name,
            want.command.version,
            bytes(want.command.data),
            want.timestamp,
        )

    async def test_mark_published_prunes_commands_idempotently(self, saga_store: Any) -> None:
        """A relay is at-least-once, so it re-marks after a lost response: a second mark and an unknown
        id are no errors, and a marked command stops coming back while the rest still wait."""
        cid = _id("corr")
        await saga_store.save(
            cid, {"step": "one"}, 0, "", [_saga_command(cid, 1, 0), _saga_command(cid, 1, 1)]
        )
        first, second = _edax.saga_command_id(cid, 1, 0), _edax.saga_command_id(cid, 1, 1)
        await saga_store.mark_published([first])
        await saga_store.mark_published([first, _edax.saga_command_id(_id("corr"), 1, 0)])
        assert await self._unpublished(saga_store, cid) == [second]
        assert len(await saga_store.unpublished(limit=1)) == 1, "a row waits, so the limit decides"

    async def test_the_folded_marks_round_trip(self, saga_store: Any) -> None:
        """``save`` keeps ``folded`` as written and ``load`` returns it: the runner skips an event redelivered
        after its step committed by these marks. A store that drops them folds the event again, and the step's
        commands go out a second time under new ids."""
        cid = _id("corr")
        folded = f'{{"{cid}":{{"sequence":1,"id":"evt-1"}}}}'
        await saga_store.save(cid, {"step": "one"}, 0, folded, [])
        instance = await saga_store.load(cid)
        assert instance is not None and instance.folded == folded, "the marks come back as they were saved"
        await saga_store.save(cid, {"step": "two"}, 1, "", [])
        instance = await saga_store.load(cid)
        assert instance is not None and instance.folded == "", "each save replaces them"

    async def test_two_steps_at_one_version_land_exactly_one_write(
        self, saga_store: Any, request: Any
    ) -> None:
        """Two events of one correlation fold concurrently, each staging its write and its commands before
        either commits. A store that reads the version and then writes lets both land: one step's state
        overwrites the other's, and both steps' commands are sent. Exactly one may land, with its commands
        alone."""
        if not self.supports_transactions:
            raise pytest.skip.Exception("backend declares no unit of work")
        transactor = request.getfixturevalue("transactor")
        cid = _id("corr")
        command = _saga_command(cid, 1, 0)
        outcomes = await _race(
            transactor.transaction,
            lambda tx: saga_store.save(cid, {"step": "paid"}, 0, "", [command], tx=tx),
            lambda tx: saga_store.save(cid, {"step": "cancelled"}, 0, "", [command], tx=tx),
        )
        assert sorted(outcomes) == ["conflict", "landed"], f"exactly one step lands, got {outcomes}"
        instance = await saga_store.load(cid)
        want = {"step": "paid"} if outcomes[0] == "landed" else {"step": "cancelled"}
        assert instance is not None and instance.version == 1 and instance.state == want
        assert await self._unpublished(saga_store, cid) == [command.id], "the winner's command, once"

    async def test_an_uncommitted_step_is_invisible_to_another_transaction(
        self, saga_store: Any, request: Any
    ) -> None:
        """A step's staged state is not another worker's to fold: one that sees it decides on a state that
        may still roll back."""
        if not self.supports_transactions:
            raise pytest.skip.Exception("backend declares no unit of work")
        transactor = request.getfixturevalue("transactor")
        cid = _id("corr")
        seen = await _staged_then_read(
            transactor.transaction,
            lambda tx: saga_store.save(cid, {"step": "paid"}, 0, "", [], tx=tx),
            lambda tx: saga_store.load(cid, tx=tx),
        )
        assert seen is None, "another transaction saw a staged step"

    async def test_a_rolled_back_step_leaves_no_command(self, saga_store: Any, request: Any) -> None:
        """A step whose unit of work rolls back must take its commands with it: the relay would otherwise
        send a decision the store never kept."""
        if not self.supports_transactions:
            raise pytest.skip.Exception("backend declares no unit of work")
        transactor = request.getfixturevalue("transactor")
        cid = _id("corr")
        with pytest.raises(RuntimeError):
            async with transaction(transactor):
                await saga_store.save(cid, {"step": "one"}, 0, "", [_saga_command(cid, 1, 0)])
                unsent = await self._unpublished(saga_store, cid)
                assert unsent == [], "an uncommitted command is not one to send"
                raise RuntimeError("the handler failed")
        assert await self._unpublished(saga_store, cid) == []
        assert await saga_store.load(cid) is None

    async def test_a_save_with_no_tx_joins_the_ambient_transaction(
        self, saga_store: Any, request: Any
    ) -> None:
        """The runner saves with no ``tx`` inside the host's unit of work. A store that auto-commits
        there keeps the new state when the commands it decided roll back, and the saga skips a step."""
        if not self.supports_transactions:
            raise pytest.skip.Exception("backend declares no unit of work")
        transactor = request.getfixturevalue("transactor")
        cid = _id("corr")
        with pytest.raises(RuntimeError):
            async with transaction(transactor):
                await saga_store.save(cid, {"step": "awaiting_payment"}, 0, "", [])
                raise RuntimeError("the handler failed")
        assert await saga_store.load(cid) is None, "the rollback took the save with it"


class IdempotencyStoreContract:
    """The dedup ledger.

    The seam whose correctness cannot be checked by any type or review that does not know your
    isolation level, and the one where a mistake runs a handler twice. Supply an
    ``idempotency_store`` fixture, plus an ``event_store`` fixture on the same transactor when the
    store declares ``joins_transaction = True``. The suite reads that declaration from the store,
    the same place the guard reads it, so a wrong declaration fails the clauses it governs.

    A store whose claims expire (a TTL, a lease) declares ``claims_expire = True`` and overrides
    :meth:`expire_claim`; the takeover clauses then check that a lapsed claim's handle settles nothing.
    """

    #: Claims lapse on their own, so another caller can take a key over while its first claimant still
    #: runs. Such a store must fence ``complete`` and ``release`` on the handle.
    claims_expire: bool = False

    async def expire_claim(self, idempotency_store: Any, key: str) -> None:
        """Make the pending claim on ``key`` lapse now, as its TTL would. Override with ``claims_expire``."""
        raise NotImplementedError("a store that declares claims_expire overrides expire_claim")

    async def _won(self, idempotency_store: Any, key: str, payload: int) -> bytes:
        """Claim ``key``, which must be free, and return the handle."""
        claimed = await idempotency_store.claim(key, payload)
        assert isinstance(claimed, Won), f"a free key must be won, got {claimed!r}"
        assert isinstance(claimed.handle, bytes), "the handle is bytes"
        return claimed.handle

    async def test_a_free_key_is_won(self, idempotency_store: Any) -> None:
        """``claim`` returns :class:`~edax.command.Won` for the winner: nobody held this key, so run the
        handler."""
        assert isinstance(await idempotency_store.claim(_id("req"), 1234), Won)

    async def test_a_second_claim_loses_and_reads_the_winners_record(self, idempotency_store: Any) -> None:
        """The whole point of the ledger. The loser must get the winner's record back, not a win: a
        store that says "won" twice runs one command twice, which is the bug this seam exists to
        prevent."""
        key, payload = _id("req"), 1234
        await self._won(idempotency_store, key, payload)
        prior = await idempotency_store.claim(key, payload)
        assert not isinstance(prior, Won), "the second claimant must lose"
        assert prior.pending, "the winner has not completed, so the record is still pending"
        assert prior.payload_hash == payload

    async def test_concurrent_claims_have_exactly_one_winner(self, idempotency_store: Any) -> None:
        """Eight claims on one key at once, the shape of a redelivery storm. Exactly one wins. A store that
        reads, then writes in a second call, lets two of them read "free" and run the handler twice."""
        key, payload = _id("req"), 1234
        claimed = await asyncio.gather(*(idempotency_store.claim(key, payload) for _ in range(8)))
        assert sum(isinstance(c, Won) for c in claimed) == 1, f"one winner, got {claimed!r}"
        assert all(c.pending for c in claimed if not isinstance(c, Won)), (
            "every loser reads the pending claim"
        )

    async def test_a_completed_record_replays_the_result(self, idempotency_store: Any) -> None:
        """``complete`` replaces the pending claim with its outcome, and a later claim replays it
        instead of re-running the handler."""
        key, payload = _id("req"), 1234
        handle = await self._won(idempotency_store, key, payload)
        await idempotency_store.complete(key, handle, Record(payload, result={"balance": 42}))
        prior = await idempotency_store.claim(key, payload)
        assert not isinstance(prior, Won) and not prior.pending
        assert prior.result == {"balance": 42}, "the cached outcome, not a re-run"

    async def test_a_cached_failure_comes_back_as_a_failure(self, idempotency_store: Any) -> None:
        """A terminal failure is cached so a redelivery replays it rather than re-running a
        deterministic failure. The record must keep the error, not turn into a success."""
        key, payload = _id("req"), 1234
        handle = await self._won(idempotency_store, key, payload)
        failed = Record(payload, error="insufficient funds", error_code="rejected")
        await idempotency_store.complete(key, handle, failed)
        prior = await idempotency_store.claim(key, payload)
        assert not isinstance(prior, Won) and not prior.pending
        assert prior.error == "insufficient funds" and prior.result is None
        # The code rides beside the message, so a replay in any SDK sharing the ledger keeps it.
        assert prior.error_code == "rejected", "the failure's code survives the round trip"

    async def test_completing_twice_is_idempotent(self, idempotency_store: Any) -> None:
        """ "Both are idempotent." The guard can reach `complete` twice when a response is lost and
        the caller retries, so the second write must settle on the same record rather than error."""
        key, payload = _id("req"), 1234
        handle = await self._won(idempotency_store, key, payload)
        record = Record(payload, result={"balance": 42})
        await idempotency_store.complete(key, handle, record)
        await idempotency_store.complete(key, handle, record)
        prior = await idempotency_store.claim(key, payload)
        assert not isinstance(prior, Won) and prior.result == {"balance": 42}

    async def test_a_none_result_replays_as_a_success(self, idempotency_store: Any) -> None:
        """A handler returning `None` succeeded. A store that cannot tell "completed with no value"
        from "still pending" re-runs it on every redelivery, which is the whole failure this seam
        exists to prevent — and `None` is the single most common handler return."""
        key, payload = _id("req"), 1234
        handle = await self._won(idempotency_store, key, payload)
        await idempotency_store.complete(key, handle, Record(payload, result=None))
        prior = await idempotency_store.claim(key, payload)
        assert not isinstance(prior, Won)
        assert not prior.pending, "a completed record, not a pending one"
        assert prior.result is None and prior.error is None

    async def test_release_frees_the_key_for_a_retry(self, idempotency_store: Any) -> None:
        """A transient failure releases the claim so a retry can re-run. After a release the key is
        as free as it ever was."""
        key, payload = _id("req"), 1234
        handle = await self._won(idempotency_store, key, payload)
        await idempotency_store.release(key, handle)
        assert isinstance(await idempotency_store.claim(key, payload), Won), "free again"

    async def test_release_of_an_absent_key_is_not_an_error(self, idempotency_store: Any) -> None:
        """ "Idempotent: releasing an absent key is not a failure." The guard compensates on paths
        where it cannot know whether the claim ever landed, and a second release must leave the key as
        free as the first did, so the retry it exists for still wins it."""
        key = _id("req")
        handle = await self._won(idempotency_store, key, 1234)
        await idempotency_store.release(key, handle)
        await idempotency_store.release(key, handle)
        await idempotency_store.release(_id("req"), handle)  # a key that never existed
        assert isinstance(await idempotency_store.claim(key, 1234), Won), "the retry wins the freed key"

    async def test_a_lapsed_claim_settles_nothing_for_its_old_owner(self, idempotency_store: Any) -> None:
        """A claim lapses while its handler still runs and a second claimant takes the key over. The first
        claimant's ``complete`` must raise rather than overwrite the live claim, and its ``release``,
        which the guard sends next, must delete nothing. Only the live claim settles the key. A store
        that fences by key rather than by claim fails here even with two claims in one process."""
        if not self.claims_expire:
            raise pytest.skip.Exception("the store's claims do not expire")
        key, payload = _id("req"), 1234
        stale = await self._won(idempotency_store, key, payload)
        await self.expire_claim(idempotency_store, key)
        live = await self._won(idempotency_store, key, payload)
        assert live != stale, "each claim gets its own handle"

        with pytest.raises(StoreError):
            await idempotency_store.complete(key, stale, Record(payload, result="stale"))
        await idempotency_store.release(key, stale)
        held = await idempotency_store.claim(key, payload)
        assert not isinstance(held, Won) and held.pending, "the live claim survived the stale owner"

        await idempotency_store.complete(key, live, Record(payload, result="live"))
        replayed = await idempotency_store.claim(key, payload)
        assert not isinstance(replayed, Won) and replayed.result == "live"

    async def test_a_release_after_complete_keeps_the_record(self, idempotency_store: Any) -> None:
        """A ``complete`` whose write landed but whose reply was lost raises, and the guard answers with a
        ``release`` carrying the same handle. A store whose claims expire talks to its records over a
        network, so this happens; the release must leave the record of committed work, or the
        redelivery runs the handler a second time."""
        if not self.claims_expire:
            raise pytest.skip.Exception("the store's claims do not expire")
        key, payload = _id("req"), 1234
        handle = await self._won(idempotency_store, key, payload)
        await idempotency_store.complete(key, handle, Record(payload, result="done"))
        await idempotency_store.release(key, handle)
        prior = await idempotency_store.claim(key, payload)
        assert not isinstance(prior, Won) and prior.result == "done", "the release deleted a settled record"

    async def test_a_lapsed_record_frees_the_key(self, idempotency_store: Any) -> None:
        """ "A record must outlive the longest window in which the same key can be redelivered" — and
        once its retention lapses, the key is free: the next dispatch runs the handler, and the lapsed
        claim's handle settles nothing over the new one."""
        if not self.claims_expire:
            raise pytest.skip.Exception("the store's claims do not expire")
        key, payload = _id("req"), 1234
        handle = await self._won(idempotency_store, key, payload)
        await idempotency_store.complete(key, handle, Record(payload, result={"balance": 42}))
        await self.expire_claim(idempotency_store, key)
        fresh = await self._won(idempotency_store, key, payload)
        with pytest.raises(StoreError):
            await idempotency_store.complete(key, handle, Record(payload, result={"balance": 42}))
        await idempotency_store.complete(key, fresh, Record(payload, result={"balance": 43}))
        replayed = await idempotency_store.claim(key, payload)
        assert not isinstance(replayed, Won) and replayed.result == {"balance": 43}

    async def test_the_record_keeps_the_payload_hash_it_was_given(self, idempotency_store: Any) -> None:
        """The kernel compares hashes to decide ``PayloadMismatch``, so a store that truncates or
        re-signs the value turns a genuine duplicate into a mismatch.

        The hash is an unsigned 64-bit integer. Storing it in a signed column without a cast is the
        classic backend bug: everything passes until a hash above 2^63 appears.
        """
        key, payload = _id("req"), 18446744073709551615  # u64 max: the value a signed column loses
        await self._won(idempotency_store, key, payload)
        prior = await idempotency_store.claim(key, payload)
        assert not isinstance(prior, Won) and prior.payload_hash == payload, "stored losslessly"

    async def test_keys_are_independent(self, idempotency_store: Any) -> None:
        """Claiming one key must not claim another. A store with a prefix bug fails here."""
        a, b = _id("req"), _id("req")
        await self._won(idempotency_store, a, 1)
        assert isinstance(await idempotency_store.claim(b, 2), Won), "a different key is still free"

    async def test_a_rollback_frees_the_claim(self, idempotency_store: Any, request: Any) -> None:
        """ "A shared ledger runs ``begin; claim; run; complete; commit``: one atomic commit, so the
        rollback *is* the release."

        Only meaningful for a ledger that joins the transaction; a ledger of its own keeps the claim,
        which is exactly the difference between effectively-once and at-least-once. Needs an
        ``event_store`` fixture too, because the transaction has to come from somewhere.
        """
        if not joins_transaction(idempotency_store):
            raise pytest.skip.Exception("ledger declares joins_transaction = False")
        # Resolved lazily so a backend that skips this clause need not define the fixture at all.
        event_store = request.getfixturevalue("event_store")
        key = _id("req")
        with pytest.raises(RuntimeError):
            async with event_store.transaction() as tx:
                assert isinstance(await idempotency_store.claim(key, 1234, tx=tx), Won)
                raise RuntimeError("the handler failed")
        assert isinstance(await idempotency_store.claim(key, 1234), Won), "the rollback freed the key"

    async def test_a_claim_with_no_tx_joins_the_ambient_transaction(
        self, idempotency_store: Any, request: Any
    ) -> None:
        """ "Given ``tx``, or with :func:`~edax.store.current_tx` set, all three operations join that
        transaction." The guard claims with no ``tx`` inside the unit of work it opened, so this is the
        path production runs. A ledger that auto-commits there keeps the claim after the rollback,
        and the key stays pending for good."""
        if not joins_transaction(idempotency_store):
            raise pytest.skip.Exception("ledger declares joins_transaction = False")
        event_store = request.getfixturevalue("event_store")
        key = _id("req")
        with pytest.raises(RuntimeError):
            async with transaction(event_store):
                assert isinstance(await idempotency_store.claim(key, 1234), Won)
                raise RuntimeError("the handler failed")
        assert isinstance(await idempotency_store.claim(key, 1234), Won), "the rollback freed the key"

    async def test_a_claim_staged_in_an_open_transaction_is_never_won_by_another(
        self, idempotency_store: Any, request: Any
    ) -> None:
        """ "A loser inside a transaction must block on the winner's row and then read it, or raise
        ``StoreError``; it must never read a stale snapshot and report a win, which runs the handler
        twice." The first unit of work holds an uncommitted claim; a claimant in another one must not win
        while it is open. Run in a task of its own, so a store that blocks the claimant answers once the
        holder rolls back."""
        if not joins_transaction(idempotency_store):
            raise pytest.skip.Exception("ledger declares joins_transaction = False")
        event_store = request.getfixturevalue("event_store")
        key = _id("req")
        holder_open, holder_ended, contender_done = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def hold() -> None:
            try:
                async with event_store.transaction() as tx:
                    assert isinstance(await idempotency_store.claim(key, 1234, tx=tx), Won)
                    holder_open.set()
                    # Held until the contender answered, or for a bounded moment if the store parks it.
                    waiting = asyncio.ensure_future(contender_done.wait())
                    await asyncio.wait({waiting}, timeout=_BLOCKED_S)
                    waiting.cancel()
                    raise _RolledBack
            except _RolledBack:
                pass
            finally:
                holder_ended.set()

        async def contend() -> tuple[object, bool]:
            """What the contender got, and whether the holder had already rolled back by then."""
            await holder_open.wait()
            try:
                async with event_store.transaction() as tx:
                    got: object = await idempotency_store.claim(key, 1234, tx=tx)
            except StoreError:
                got = "refused"  # a lock or statement timeout: the contract's other allowed answer
            finally:
                contender_done.set()
            return got, holder_ended.is_set()

        _, (contended, after_rollback) = await asyncio.gather(hold(), contend())
        if isinstance(contended, Won):
            # A store that parked the contender on the holder's row lock answers after the rollback freed
            # the key: that win is correct. A win while the holder was open runs the handler twice.
            assert after_rollback, "won a key another open transaction held: the handler would run twice"
            return
        assert contended == "refused" or (isinstance(contended, Record) and contended.pending), contended

    async def test_a_release_with_nothing_committed_leaves_a_concurrent_winners_record(
        self, idempotency_store: Any, request: Any
    ) -> None:
        """A transient failure releases the claim inside its unit of work, and another unit of work then wins
        the key and commits its outcome before the first one commits. The releaser held nothing committed,
        so its commit must leave the winner's record, and its own re-claim must replay it. A release that
        stages a delete anyway erases the winner's record, and the next dispatch runs the handler again.

        The winner runs in a task of its own: a ledger that blocks it on the releaser's row lock does not
        deadlock the clause, which commits the releaser after a bounded wait and checks the same outcome."""
        if not joins_transaction(idempotency_store):
            raise pytest.skip.Exception("ledger declares joins_transaction = False")
        event_store = request.getfixturevalue("event_store")
        key, payload = _id("req"), 1234
        winner_done = asyncio.Event()

        async def winner() -> None:
            async with event_store.transaction() as tx:
                won = await idempotency_store.claim(key, payload, tx=tx)
                assert isinstance(won, Won), f"the released key is free, got {won!r}"
                await idempotency_store.complete(
                    key, won.handle, Record(payload, result={"balance": 42}), tx=tx
                )
            winner_done.set()

        async with event_store.transaction() as tx:
            mine = await idempotency_store.claim(key, payload, tx=tx)
            assert isinstance(mine, Won)
            await idempotency_store.release(key, mine.handle, tx=tx)
            racer = asyncio.create_task(winner())
            done = asyncio.create_task(winner_done.wait())
            await asyncio.wait({done, racer}, timeout=_BLOCKED_S, return_when=asyncio.FIRST_COMPLETED)
            done.cancel()
            if winner_done.is_set():
                replay = await idempotency_store.claim(key, payload, tx=tx)
                assert not isinstance(replay, Won) and replay.result == {"balance": 42}, (
                    f"the releaser's re-claim must replay the winner, got {replay!r}"
                )
        await asyncio.wait_for(racer, _POLL_S)
        stored = await idempotency_store.claim(key, payload)
        assert not isinstance(stored, Won) and stored.result == {"balance": 42}, (
            f"the releaser's commit erased the winner's record: {stored!r}"
        )

    async def test_a_handle_of_another_database_is_refused(self, idempotency_store: Any) -> None:
        """ "``True`` obliges you to raise terminal BadHandle for a handle from another database, never to
        auto-commit." The guard hands the store its unit of work's handle, so a store over another database
        than the guard's ``unit_of_work`` must fail at the claim. One that auto-commits instead keeps a claim
        the guard's rollback cannot take back, and the key stays pending for good."""
        if not joins_transaction(idempotency_store):
            raise pytest.skip.Exception("ledger declares joins_transaction = False")
        from ..backend.memory import MemoryTransactor  # another database than any adopter's store

        key, payload = _id("req"), 1234
        async with MemoryTransactor().transaction() as foreign:
            with pytest.raises(BadHandle):
                await idempotency_store.claim(key, payload, tx=foreign)
        assert isinstance(await idempotency_store.claim(key, payload), Won), "the refused claim wrote nothing"
        held = _id("req")
        won = await self._won(idempotency_store, held, payload)
        async with MemoryTransactor().transaction() as foreign:
            with pytest.raises(BadHandle):
                await idempotency_store.complete(held, won, Record(payload, result="done"), tx=foreign)
            with pytest.raises(BadHandle):
                await idempotency_store.release(held, won, tx=foreign)
        prior = await idempotency_store.claim(held, payload)
        assert not isinstance(prior, Won) and prior.pending, "the refused settlements wrote nothing"

    async def test_a_key_another_open_transaction_holds_is_never_settled_over_it(
        self, idempotency_store: Any, request: Any
    ) -> None:
        """ "A ``joins_transaction`` store refuses ``complete`` and ``release`` of a key another open
        transaction has written." One unit of work stages the release of a committed claim; another settles
        the same key meanwhile. Landing over it would let the holder's commit erase the record, or free its
        claim. The settler may block until the holder ends (a row lock) or be refused with a transient error;
        it may never land while the holder is open. Each settler runs in a task of its own, so a store that
        blocks it answers once the holder rolls back."""
        if not joins_transaction(idempotency_store):
            raise pytest.skip.Exception("ledger declares joins_transaction = False")
        event_store = request.getfixturevalue("event_store")
        payload = 1234
        for settle in ("complete", "release"):
            key = _id("req")
            async with event_store.transaction() as tx:
                won = await idempotency_store.claim(key, payload, tx=tx)
            assert isinstance(won, Won), f"a free key must be won, got {won!r}"
            got, after_rollback = await _settle_while_held(
                idempotency_store, event_store, key, won.handle, settle
            )
            stored = await idempotency_store.claim(key, payload)
            if got == "refused":
                assert not isinstance(stored, Won) and stored.pending, f"{settle}: the refusal wrote nothing"
                continue
            assert after_rollback, f"{settle} landed on a key another open transaction held"
            if settle == "complete":
                assert not isinstance(stored, Won) and stored.result == "done", stored
            else:
                assert isinstance(stored, Won), (
                    f"the release after the rollback freed the key, got {stored!r}"
                )


async def _settle_while_held(
    idempotency_store: Any, event_store: Any, key: str, handle: bytes, settle: str
) -> tuple[str, bool]:
    """A holder unit of work stages the release of ``key``'s committed claim and stays open; a settler in
    another one ``complete``s or ``release``s the key meanwhile. The holder rolls back once the settler
    answered, or after a bounded moment if the store parks it. Returns what the settler got, ``landed`` or
    ``refused``, and whether the holder had already rolled back by then."""
    holder_open, holder_ended, settler_done = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def hold() -> None:
        try:
            async with event_store.transaction() as tx:
                await idempotency_store.release(key, handle, tx=tx)
                holder_open.set()
                waiting = asyncio.ensure_future(settler_done.wait())
                await asyncio.wait({waiting}, timeout=_BLOCKED_S)
                waiting.cancel()
                raise _RolledBack
        except _RolledBack:
            pass
        finally:
            holder_ended.set()

    async def settler() -> tuple[str, bool]:
        await holder_open.wait()
        try:
            async with event_store.transaction() as tx:
                if settle == "complete":
                    await idempotency_store.complete(key, handle, Record(1234, result="done"), tx=tx)
                else:
                    await idempotency_store.release(key, handle, tx=tx)
            got = "landed"
        except (ConcurrencyError, StoreError):
            got = "refused"  # a transient refusal: the contract's other allowed answer
        finally:
            settler_done.set()
        return got, holder_ended.is_set()

    _, outcome = await asyncio.gather(hold(), settler())
    return outcome


class BrokerRig(Protocol):
    """What :class:`BrokerContract` drives: one broker, a producer into it, and one consumer per consumer
    group. Asking for the same group twice returns the same consumer."""

    async def publish(self, env: EventEnvelope) -> None:
        """Publish ``env`` and return once the broker holds it."""
        ...

    async def consumer(self, group: str) -> Consumer:
        """The consumer of consumer group ``group``; a group asked for the first time reads from the start."""
        ...

    async def commit(self, group: str) -> None:
        """Make what ``group`` settled durable now: a commit. A no-op for a broker that commits on ack."""
        ...

    async def crash(self, group: str) -> Consumer:
        """Kill ``group``'s consumer holding whatever it holds, with no commit, and return the consumer that
        replaces it. The replacement resumes from the group's last commit."""
        ...

    def drained(self, group: str) -> bool:
        """Whether ``group`` has committed past every message published."""
        ...


async def _poll(consumer: Consumer, n: int) -> list[Delivery]:
    """The next ``n`` deliveries. Bounded: a broker that lost one fails the clause instead of hanging it."""
    out: list[Delivery] = []
    for _ in range(n):
        delivery = await asyncio.wait_for(consumer.poll(), _POLL_S)
        assert delivery is not None, "the stream ended early"
        out.append(delivery)
    return out


async def _quiet(consumer: Consumer) -> None:
    """Assert nothing more is delivered now."""
    try:
        extra = await asyncio.wait_for(consumer.poll(), _QUIET_S)
    except asyncio.TimeoutError:
        return
    raise AssertionError(f"delivered {extra.envelope.id if extra else None} after everything was settled")


class BrokerContract:
    """The broker seam, :class:`~edax.broker.Producer` and :class:`~edax.broker.Consumer` together.

    Supply a ``broker_rig`` fixture returning a :class:`BrokerRig` over a fresh topic. The clauses are what
    the dispatcher and the handlers count on from a log-based broker such as Kafka: per-key order, a nack
    that redelivers without reordering the key, a crash that redelivers from the commit and never loses
    or skips a message, deliveries the broker retired (:attr:`~edax.dispatcher.Delivery.live`) that settle
    nothing, and consumer groups that each read every message."""

    async def test_publish_then_poll_keeps_each_keys_order(self, broker_rig: Any) -> None:
        envs = [envelope(agg, seq) for seq in (1, 2, 3) for agg in ("a", "b")]
        for env in envs:
            await broker_rig.publish(env)
        consumer = await broker_rig.consumer("g")
        seen: dict[str, list[int | None]] = {}
        for d in await _poll(consumer, len(envs)):
            seen.setdefault(d.envelope.aggregate_id, []).append(d.envelope.sequence)
            await d.ack()
        assert seen == {"a": [1, 2, 3], "b": [1, 2, 3]}
        await broker_rig.commit("g")
        assert broker_rig.drained("g")

    async def test_acks_out_of_order_settle_everything(self, broker_rig: Any) -> None:
        """The dispatcher acks in worker order, not in fetch order."""
        for env in [envelope("a", 1), envelope("b", 1), envelope("a", 2)]:
            await broker_rig.publish(env)
        consumer = await broker_rig.consumer("g")
        deliveries = await _poll(consumer, 3)
        for d in reversed(deliveries[1:]):
            await d.ack()
        await broker_rig.commit("g")
        assert not broker_rig.drained("g"), "committed past the delivery still in flight"
        await deliveries[0].ack()
        await broker_rig.commit("g")
        assert broker_rig.drained("g")

    async def test_a_nack_redelivers_the_message(self, broker_rig: Any) -> None:
        await broker_rig.publish(envelope("a", 1))
        consumer = await broker_rig.consumer("g")
        (first,) = await _poll(consumer, 1)
        await first.nack()
        (again,) = await _poll(consumer, 1)
        assert again.envelope.id == first.envelope.id
        # The broker's own redelivery count where it keeps one, else 0: never lower than before.
        assert again.attempt >= first.attempt
        assert again.live()
        await again.ack()
        await broker_rig.commit("g")
        assert broker_rig.drained("g")

    async def test_a_nack_never_reorders_its_key(self, broker_rig: Any) -> None:
        """a-2 was handed out, even acked, before a-1 failed. After the nack the next delivery of key ``a``
        is a-1, and the a-2 copy handed out before the nack is retired: running it would apply a-2 ahead
        of a-1. a-2 comes back behind a-1, so a handler sees it again: at-least-once."""
        for seq in (1, 2):
            await broker_rig.publish(envelope("a", seq))
        consumer = await broker_rig.consumer("g")
        first, second = await _poll(consumer, 2)
        await second.ack()
        await first.nack()
        assert not second.live(), "the a-2 copy handed out before the nack is still live"
        again = await _poll(consumer, 2)
        assert [d.envelope.sequence for d in again] == [1, 2], "the key came back out of order"
        for d in again:
            assert d.live()
            await d.ack()
        await broker_rig.commit("g")
        assert broker_rig.drained("g")
        await _quiet(consumer)

    async def test_a_crash_redelivers_from_the_commit(self, broker_rig: Any) -> None:
        """The consumer dies holding two unsettled deliveries. Its replacement gets both again and nothing
        the group committed past, and a late ack from the dead consumer settles nothing the group sees: a
        zombie that finishes its handler after the crash must not commit past the redelivery."""
        for env in [envelope("a", 1), envelope("b", 1), envelope("a", 2)]:
            await broker_rig.publish(env)
        consumer = await broker_rig.consumer("g")
        held = await _poll(consumer, 3)
        await held[0].ack()
        await broker_rig.commit("g")
        replacement = await broker_rig.crash("g")
        for d in held[1:]:
            await d.ack()  # late: the dead consumer finished its handler after the crash
        await broker_rig.commit("g")
        assert not broker_rig.drained("g"), "a dead consumer's late ack settled a redelivered message"
        again = await _poll(replacement, 2)
        assert sorted(d.envelope.id for d in again) == sorted(d.envelope.id for d in held[1:])
        for d in again:
            await d.ack()
        await broker_rig.commit("g")
        assert broker_rig.drained("g")
        await _quiet(replacement)

    async def test_every_consumer_group_reads_every_message(self, broker_rig: Any) -> None:
        """A projection and a saga on one topic each get every message and settle on their own."""
        for seq in (1, 2):
            await broker_rig.publish(envelope("a", seq))
        projection = await broker_rig.consumer("projection")
        for d in await _poll(projection, 2):
            await d.ack()
        await broker_rig.commit("projection")
        assert broker_rig.drained("projection")
        assert not broker_rig.drained("saga"), "one group's acks settled another's"
        saga = await broker_rig.consumer("saga")
        assert [d.envelope.sequence for d in await _poll(saga, 2)] == [1, 2]

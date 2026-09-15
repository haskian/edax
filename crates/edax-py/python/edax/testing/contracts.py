"""Executable conformance suites for the storage seams.

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
at all — and pytest 8 is what Python 3.9 resolves, which this package supports.
"""

from __future__ import annotations

import uuid
from typing import Any, Optional

import pytest

from ..aggregate import Envelope, Event, EventEnvelope
from ..command import Record
from ..errors import ConcurrencyError
from ..projection import Entity
from ..store import Snapshot

__all__ = [
    "EventStoreContract",
    "IdempotencyStoreContract",
    "ProjectionStoreContract",
    "SagaStoreContract",
]


def envelope(aggregate_id: str, sequence: Optional[int], *, name: str = "Ticked") -> EventEnvelope:
    """One event envelope, with an id unique per (aggregate, sequence) so a store keying on it can."""
    return Envelope(
        id=f"{aggregate_id}-{name}-{sequence}",
        aggregate_id=aggregate_id,
        event=Event(name, 1, b'{"n":1}'),
        namespace={},
        metadata={},
        sequence=sequence,
        timestamp=0,
    )


def _id(prefix: str) -> str:
    """A fresh id per test, so a suite run against a durable store needs no truncation between tests."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


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
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, 1), envelope(agg, 2)])
            loaded = await event_store.load(tx, agg)
        assert [e.sequence for e in loaded] == [1, 2], "ascending and gap-free, as handed in"
        assert [e.id for e in loaded] == [envelope(agg, 1).id, envelope(agg, 2).id]

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

    async def test_every_envelope_field_survives_the_round_trip(self, event_store: Any) -> None:
        """A store that persists only the fields it recognises silently drops the rest.

        `namespace` carries tenancy, `metadata` carries the correlation and causation ids the whole
        tracing story depends on, and `timestamp` orders nothing but is read by humans. A column
        missing for any of them loses data no replay can recover. Non-ASCII included, because a
        latin-1 column passes every test written in English.
        """
        agg = _id("acct")
        env = Envelope(
            id=f"{agg}-Ticked-1",
            aggregate_id=agg,
            event=Event("Ticked", 3, b'{"n":1}'),
            namespace={"tenant": "acme"},
            metadata={"correlation_id": "corr-1", "note": "déjà-vu"},
            sequence=1,
            timestamp=1234567890,
        )
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [env])
            (got,) = await event_store.load(tx, agg)
        assert got.id == env.id and got.aggregate_id == env.aggregate_id
        assert (got.event.name, got.event.version, bytes(got.event.data)) == ("Ticked", 3, b'{"n":1}')
        assert dict(got.namespace) == {"tenant": "acme"}, "namespace survived"
        assert dict(got.metadata) == {"correlation_id": "corr-1", "note": "déjà-vu"}, "metadata survived"
        assert got.timestamp == 1234567890 and got.sequence == 1

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
        """ "the *latest* snapshot": a second save supersedes the first rather than accumulating."""
        if not self.supports_snapshots:
            raise pytest.skip.Exception("backend declares no snapshot support")
        agg = _id("acct")
        async with event_store.transaction() as tx:
            await event_store.append(tx, agg, 0, [envelope(agg, n) for n in (1, 2)])
            await event_store.save_snapshot(tx, Snapshot(agg, "Account", 1, b"first"))
            await event_store.save_snapshot(tx, Snapshot(agg, "Account", 2, b"second"))
        async with event_store.transaction() as tx:
            snap = await event_store.load_snapshot(tx, agg)
            assert snap is not None and (snap.version, snap.data) == (2, b"second")

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
        await event_store.mark_published([_id("never-existed")])  # unknown ids are ignored
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
        assert len(await event_store.unpublished(limit=2)) <= 2


class ProjectionStoreContract:
    """Read-model documents under optimistic concurrency.

    Supply a ``projection_store`` fixture.
    """

    #: ``get_many`` pages identically to the kernel rather than being the backend's own dialect.
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

    async def test_entities_are_independent(self, projection_store: Any) -> None:
        """Concurrency is per id. A store versioning the whole table serialises every read model."""
        a, b = Entity(_id("a"), 0, {"v": 1}), Entity(_id("b"), 0, {"v": 2})
        await projection_store.save(a)
        await projection_store.save(b)
        got_a, got_b = await projection_store.get(a.id), await projection_store.get(b.id)
        assert got_a is not None and got_a.document == {"v": 1}
        assert got_b is not None and got_b.document == {"v": 2}


class SagaStoreContract:
    """Per-correlation saga instance state under optimistic concurrency.

    Supply a ``saga_store`` fixture.
    """

    async def test_an_unknown_correlation_loads_as_none(self, saga_store: Any) -> None:
        """A saga that has not started yet has no instance; the runner reads ``None`` as "new"."""
        assert await saga_store.load(_id("corr")) is None

    async def test_save_then_load_round_trips_the_state(self, saga_store: Any) -> None:
        """The runner's whole job is fold-and-persist, so the state must come back as it went in."""
        cid = _id("corr")
        version = await saga_store.save(cid, {"step": "awaiting_payment"}, 0)
        instance = await saga_store.load(cid)
        assert instance is not None and instance.state == {"step": "awaiting_payment"}
        assert instance.version == version

    async def test_save_is_compare_and_set_on_the_version(self, saga_store: Any) -> None:
        """Two events for one correlation can arrive concurrently. Without a CAS here one fold
        silently overwrites the other and the saga forgets a step."""
        cid = _id("corr")
        await saga_store.save(cid, {"step": "one"}, 0)
        with pytest.raises(ConcurrencyError):
            await saga_store.save(cid, {"step": "clobbered"}, 0)
        instance = await saga_store.load(cid)
        assert instance is not None and instance.state == {"step": "one"}

    async def test_delete_is_versioned_and_removes_the_instance(self, saga_store: Any) -> None:
        """Completion deletes at the version it loaded. A delete that ignores the version drops an
        instance another worker has just advanced."""
        cid = _id("corr")
        version = await saga_store.save(cid, {"step": "one"}, 0)
        with pytest.raises(ConcurrencyError):
            await saga_store.delete(cid, version + 5)
        await saga_store.delete(cid, version)
        assert await saga_store.load(cid) is None


class IdempotencyStoreContract:
    """The dedup ledger.

    The seam whose correctness cannot be checked by any type or review that does not know your
    isolation level, and the one where a mistake runs a handler twice. Supply an
    ``idempotency_store`` fixture, plus an ``event_store`` fixture on the same transactor when
    ``joins_transaction`` is true.
    """

    #: The store enlists in the caller's unit of work, so a rollback frees the claim. Requires an
    #: ``event_store`` fixture on the same transactor when true.
    #: ``False`` for a ledger of its own (Redis), where the claim survives the caller's rollback;
    #: such a ledger needs no ``event_store`` fixture.
    joins_transaction: bool = True
    #: Records expire. A suite cannot test retention without waiting, so this only gates the clause
    #: that says a *completed* record is still readable immediately after it is written.
    has_retention: bool = True

    async def test_a_free_key_is_won(self, idempotency_store: Any) -> None:
        """``claim`` returns ``None`` for the winner: nobody held this key, so run the handler."""
        assert await idempotency_store.claim(_id("req"), 1234) is None

    async def test_a_second_claim_loses_and_reads_the_winners_record(self, idempotency_store: Any) -> None:
        """The whole point of the ledger. The loser must get the winner's record back, not ``None``:
        a store that says "won" twice runs one command twice, which is the bug this seam exists to
        prevent."""
        key, payload = _id("req"), 1234
        assert await idempotency_store.claim(key, payload) is None
        prior = await idempotency_store.claim(key, payload)
        assert prior is not None, "the second claimant must lose"
        assert prior.pending, "the winner has not completed, so the record is still pending"
        assert prior.payload_hash == payload

    async def test_a_completed_record_replays_the_result(self, idempotency_store: Any) -> None:
        """``complete`` replaces the pending claim with its outcome, and a later claim replays it
        instead of re-running the handler."""
        key, payload = _id("req"), 1234
        await idempotency_store.claim(key, payload)
        await idempotency_store.complete(key, Record(payload, result={"balance": 42}))
        prior = await idempotency_store.claim(key, payload)
        assert prior is not None and not prior.pending
        assert prior.result == {"balance": 42}, "the cached outcome, not a re-run"

    async def test_a_cached_failure_comes_back_as_a_failure(self, idempotency_store: Any) -> None:
        """A terminal failure is cached so a redelivery replays it rather than re-running a
        deterministic failure. The record must keep the error, not turn into a success."""
        key, payload = _id("req"), 1234
        await idempotency_store.claim(key, payload)
        await idempotency_store.complete(key, Record(payload, error="insufficient funds"))
        prior = await idempotency_store.claim(key, payload)
        assert prior is not None and not prior.pending
        assert prior.error == "insufficient funds" and prior.result is None

    async def test_completing_twice_is_idempotent(self, idempotency_store: Any) -> None:
        """ "Both are idempotent." The guard can reach `complete` twice when a response is lost and
        the caller retries, so the second write must settle on the same record rather than error."""
        key, payload = _id("req"), 1234
        await idempotency_store.claim(key, payload)
        record = Record(payload, result={"balance": 42})
        await idempotency_store.complete(key, record)
        await idempotency_store.complete(key, record)
        prior = await idempotency_store.claim(key, payload)
        assert prior is not None and prior.result == {"balance": 42}

    async def test_a_none_result_replays_as_a_success(self, idempotency_store: Any) -> None:
        """A handler returning `None` succeeded. A store that cannot tell "completed with no value"
        from "still pending" re-runs it on every redelivery, which is the whole failure this seam
        exists to prevent — and `None` is the single most common handler return."""
        key, payload = _id("req"), 1234
        await idempotency_store.claim(key, payload)
        await idempotency_store.complete(key, Record(payload, result=None))
        prior = await idempotency_store.claim(key, payload)
        assert prior is not None
        assert not prior.pending, "a completed record, not a pending one"
        assert prior.result is None and prior.error is None

    async def test_release_frees_the_key_for_a_retry(self, idempotency_store: Any) -> None:
        """A transient failure releases the claim so a retry can re-run. After a release the key is
        as free as it ever was."""
        key, payload = _id("req"), 1234
        await idempotency_store.claim(key, payload)
        await idempotency_store.release(key)
        assert await idempotency_store.claim(key, payload) is None, "free again"

    async def test_release_of_an_absent_key_is_not_an_error(self, idempotency_store: Any) -> None:
        """ "Idempotent: releasing an absent key is not a failure." The guard compensates on paths
        where it cannot know whether the claim ever landed."""
        await idempotency_store.release(_id("never-claimed"))

    async def test_the_record_keeps_the_payload_hash_it_was_given(self, idempotency_store: Any) -> None:
        """The kernel compares hashes to decide ``PayloadMismatch``, so a store that truncates or
        re-signs the value turns a genuine duplicate into a mismatch.

        The hash is an unsigned 64-bit integer. Storing it in a signed column without a cast is the
        classic backend bug: everything passes until a hash above 2^63 appears.
        """
        key, payload = _id("req"), 18446744073709551615  # u64 max: the value a signed column loses
        assert await idempotency_store.claim(key, payload) is None
        prior = await idempotency_store.claim(key, payload)
        assert prior is not None and prior.payload_hash == payload, "stored losslessly"

    async def test_keys_are_independent(self, idempotency_store: Any) -> None:
        """Claiming one key must not claim another. A store with a prefix bug fails here."""
        a, b = _id("req"), _id("req")
        assert await idempotency_store.claim(a, 1) is None
        assert await idempotency_store.claim(b, 2) is None, "a different key is still free"

    async def test_a_rollback_frees_the_claim(self, idempotency_store: Any, request: Any) -> None:
        """ "A shared ledger runs ``begin; claim; run; complete; commit``: one atomic commit, so the
        rollback *is* the release."

        Only meaningful for a ledger that joins the transaction; a ledger of its own keeps the claim,
        which is exactly the difference between effectively-once and at-least-once. Needs an
        ``event_store`` fixture too, because the transaction has to come from somewhere.
        """
        if not self.joins_transaction:
            raise pytest.skip.Exception("ledger declares joins_transaction = False")
        # Resolved lazily so a backend that skips this clause need not define the fixture at all.
        event_store = request.getfixturevalue("event_store")
        key = _id("req")
        with pytest.raises(RuntimeError):
            async with event_store.transaction() as tx:
                assert await idempotency_store.claim(key, 1234, tx=tx) is None
                raise RuntimeError("the handler failed")
        assert await idempotency_store.claim(key, 1234) is None, "the rollback freed the key"

"""The :class:`~edax.idempotency.IdempotencyStore` contract."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from ...errors import BadHandle, ConcurrencyError, InProgress, StoreError
from ...idempotency import IdempotencyStore, Pending, Record, Won, decode_prior, joins_transaction
from ...unit_of_work import Tx, transaction
from .._concurrency import POLL_S, REFUSED, unique_id, until_answered, while_held
from ._fixtures import requires, transactor

#: A seam fixture: the store under test.
Store = IdempotencyStore[Any]

#: Records a claim must replay once ``complete`` settled the key, last one written wins, by branch.
_SETTLED: dict[str, list[Record]] = {
    "done": [Record.done(1234, {"balance": 42})],
    "failed_with_its_code": [Record.failed(1234, "insufficient funds", "rejected")],
    "done_with_none": [Record.done(1234, None)],
    "done_twice": [Record.done(1234, {"balance": 42}), Record.done(1234, {"balance": 42})],
}

#: Stored bytes no reader can decode, by branch: what a torn write, another layout or a field of the wrong
#: type leaves. The Rust kit has one more, a result its typed ``R`` cannot read; a Python result is any JSON
#: value, so every row that parses as a record replays here.
_UNREADABLE: dict[str, str] = {
    "garbage": "\x01\x7fedax",
    "torn_write": '{"payload_hash":11234062831410934249,"result":{"',
    "not_an_object": "[11234062831410934249]",
    "wrong_type": '{"payload_hash":11234062831410934249,"pending":"settling"}',
}

#: Why a clause that needs a shared rollback skips a ledger of its own.
_JOINS = "ledger declares joins_transaction = False"


class IdempotencyStoreContract:
    """The dedup ledger.

    No type and no review can check this seam without knowing your isolation level, and a mistake here
    runs a handler twice. Supply an ``idempotency_store`` fixture. The suite reads ``joins_transaction``
    from the store, the same place the guard reads it, so a wrong declaration fails the clauses it governs.

    A store that declares ``joins_transaction = True`` also needs a ``transactor`` fixture: the transactor
    whose units of work it joins.

    A store whose claims expire (a TTL, a lease) declares ``claims_expire = True`` and overrides
    :meth:`expire_claim`; the takeover clauses then check that a lapsed claim's handle settles nothing.

    A store that persists the kernel's record JSON declares ``stores_raw_records = True`` and overrides
    :meth:`put_raw_record`; the unreadable-record clause then checks that a record no reader can decode
    replays as a cached failure, as in every other SDK sharing the ledger.
    """

    #: Claims lapse on their own, so another caller can take a key over while its first claimant still
    #: runs. Such a store must fence ``complete`` and ``release`` on the handle.
    claims_expire: bool = False

    #: The store persists the kernel's record JSON, so a row can hold bytes no reader decodes.
    stores_raw_records: bool = False

    async def expire_claim(self, idempotency_store: Store, key: str) -> None:
        """Make the pending claim on ``key`` lapse now, as its TTL would. Override with ``claims_expire``."""
        raise NotImplementedError("a store that declares claims_expire overrides expire_claim")

    async def put_raw_record(self, idempotency_store: Store, key: str, raw: str) -> None:
        """Store ``raw`` as ``key``'s settled record, unchecked, the way the store persists a record (a
        Redis store frames it). Override with ``stores_raw_records``."""
        raise NotImplementedError("a store that declares stores_raw_records overrides put_raw_record")

    async def _won(self, idempotency_store: Store, key: str, payload: int) -> bytes:
        """Claim ``key``, which must be free, and return the handle."""
        claimed = await idempotency_store.claim(key, payload)
        assert isinstance(claimed, Won), f"a free key must be won, got {claimed!r}"
        assert isinstance(claimed.handle, bytes), "the handle is bytes"
        return claimed.handle

    async def test_a_free_key_is_won(self, idempotency_store: Store) -> None:
        """``claim`` returns :class:`~edax.idempotency.Won` for the winner: nobody held this key, so run the
        handler."""
        assert isinstance(await idempotency_store.claim(unique_id("req"), 1234), Won)

    async def test_a_second_claim_loses_and_reads_the_winners_record(self, idempotency_store: Store) -> None:
        """The loser must get the winner's record back, never a win: a store that says "won" twice runs
        one command twice."""
        key, payload = unique_id("req"), 1234
        await self._won(idempotency_store, key, payload)
        prior = await idempotency_store.claim(key, payload)
        assert not isinstance(prior, Won), "the second claimant must lose"
        assert prior == Record.pending(payload), (
            "the winner has not completed, so the record is still pending"
        )

    async def test_concurrent_claims_have_exactly_one_winner(self, idempotency_store: Store) -> None:
        """Eight claims on one key at once, the shape of a redelivery storm. Exactly one wins. A store that
        reads, then writes in a second call, lets two of them read "free" and run the handler twice."""
        key, payload = unique_id("req"), 1234
        claimed = await asyncio.gather(*(idempotency_store.claim(key, payload) for _ in range(8)))
        assert sum(isinstance(c, Won) for c in claimed) == 1, f"one winner, got {claimed!r}"
        assert all(isinstance(c.state, Pending) for c in claimed if not isinstance(c, Won)), (
            "every loser reads the pending claim"
        )

    @pytest.mark.parametrize("branch", list(_SETTLED))
    async def test_a_settled_record_replays_instead_of_a_rerun(
        self, idempotency_store: Store, branch: str
    ) -> None:
        """``complete`` replaces the pending claim with its outcome, and a later claim replays it instead of
        re-running the handler. Every outcome round-trips whole: a terminal failure keeps its error and its
        code, so a replay in any SDK sharing the ledger keeps them; a ``None`` result, the most common
        handler return, stays a success, not a pending claim run again on every redelivery; and a second
        ``complete``, which the guard sends after a lost response, settles on the same record."""
        key = unique_id("req")
        handle = await self._won(idempotency_store, key, 1234)
        for record in _SETTLED[branch]:
            await idempotency_store.complete(key, handle, record)
        prior = await idempotency_store.claim(key, 1234)
        assert prior == _SETTLED[branch][-1], f"{branch}: the cached outcome, not a re-run"

    @pytest.mark.parametrize("branch", list(_UNREADABLE))
    async def test_an_unreadable_record_replays_as_a_cached_codec_failure(
        self, idempotency_store: Store, branch: str
    ) -> None:
        """A row another writer left that no reader can decode still holds its key. ``claim`` must read it
        with :func:`~edax.idempotency.decode_prior`: the kernel's terminal ``codec`` failure, so the guard
        replays ``CachedFailure`` and the handler never runs again for the key, as in every other SDK
        sharing the ledger. A store that raises instead makes the two SDKs disagree; one that reads the row
        as free runs the handler a second time. The clause holds the store to the kernel's own reading,
        which keeps the stored hash where it reads, else the claimant's, and the claim leaves the row
        alone."""
        requires(self.stores_raw_records, "the store does not persist the kernel's record JSON")
        raw, claimant = _UNREADABLE[branch], 18446744073709551615
        want = decode_prior(raw, claimant)
        key = unique_id("req")
        await self.put_raw_record(idempotency_store, key, raw)
        for attempt in ("first claim", "redelivery"):
            prior = await idempotency_store.claim(key, claimant)
            assert prior == want, (
                f"{branch}: {attempt}: want the kernel's cached codec failure, got {prior!r}"
            )

    async def test_a_pending_record_cannot_settle_a_claim(self, idempotency_store: Store) -> None:
        """``complete`` takes a done or a failed record. A pending one reads exactly like the claim it
        replaces, so the key would stay claimed with no run behind it: for good in memory, for the whole
        record TTL in Redis, and ``forget`` refuses a pending claim. The store refuses it with the terminal
        :class:`~edax.errors.BadHandle` (the kernel's ``check_settled``) and leaves the claim as it was, so
        the real outcome still settles it."""
        key, payload = unique_id("req"), 1234
        handle = await self._won(idempotency_store, key, payload)
        with pytest.raises(BadHandle):
            await idempotency_store.complete(key, handle, Record.pending(payload))
        await idempotency_store.complete(key, handle, Record.done(payload, {"balance": 42}))
        prior = await idempotency_store.claim(key, payload)
        assert prior == Record.done(payload, {"balance": 42}), "the claim still settled"

    async def test_release_frees_the_key_for_a_retry(self, idempotency_store: Store) -> None:
        """A transient failure releases the claim so a retry can re-run. After a release the key is
        as free as it ever was."""
        key, payload = unique_id("req"), 1234
        handle = await self._won(idempotency_store, key, payload)
        await idempotency_store.release(key, handle)
        assert isinstance(await idempotency_store.claim(key, payload), Won), "free again"

    async def test_release_of_an_absent_key_is_not_an_error(self, idempotency_store: Store) -> None:
        """ "Both are idempotent; releasing an absent key is a no-op." The guard compensates on paths
        where it cannot know whether the claim ever landed, and a second release must leave the key as
        free as the first did, so the retry it exists for still wins it."""
        key = unique_id("req")
        handle = await self._won(idempotency_store, key, 1234)
        await idempotency_store.release(key, handle)
        await idempotency_store.release(key, handle)
        await idempotency_store.release(unique_id("req"), handle)  # a key that never existed
        assert isinstance(await idempotency_store.claim(key, 1234), Won), "the retry wins the freed key"

    async def test_a_lapsed_claim_settles_nothing_for_its_old_owner(self, idempotency_store: Store) -> None:
        """A claim lapses while its handler still runs and a second claimant takes the key over. The first
        claimant's ``complete`` must raise rather than overwrite the live claim, and its ``release``,
        which the guard sends next, must delete nothing. Only the live claim settles the key. A store
        that fences by key rather than by claim fails here even with two claims in one process."""
        requires(self.claims_expire, "the store's claims do not expire")
        key, payload = unique_id("req"), 1234
        stale = await self._won(idempotency_store, key, payload)
        await self.expire_claim(idempotency_store, key)
        live = await self._won(idempotency_store, key, payload)
        assert live != stale, "each claim gets its own handle"

        with pytest.raises(StoreError):
            await idempotency_store.complete(key, stale, Record.done(payload, "stale"))
        await idempotency_store.release(key, stale)
        held = await idempotency_store.claim(key, payload)
        assert held == Record.pending(payload), "the live claim survived the stale owner"

        await idempotency_store.complete(key, live, Record.done(payload, "live"))
        replayed = await idempotency_store.claim(key, payload)
        assert replayed == Record.done(payload, "live")

    async def test_a_release_after_complete_keeps_the_record(self, idempotency_store: Store) -> None:
        """A ``complete`` whose write landed but whose reply was lost raises, and the guard answers with a
        ``release`` carrying the same handle. A store whose claims expire talks to its records over a
        network, so this happens; the release must leave the record of committed work, or the
        redelivery runs the handler a second time."""
        requires(self.claims_expire, "the store's claims do not expire")
        key, payload = unique_id("req"), 1234
        handle = await self._won(idempotency_store, key, payload)
        await idempotency_store.complete(key, handle, Record.done(payload, "done"))
        await idempotency_store.release(key, handle)
        prior = await idempotency_store.claim(key, payload)
        assert prior == Record.done(payload, "done"), "the release deleted a settled record"

    async def test_a_lapsed_record_frees_the_key(self, idempotency_store: Store) -> None:
        """ "A record must outlive the longest window in which the same key can be redelivered" — and
        once its retention lapses, the key is free: the next dispatch runs the handler, and the lapsed
        claim's handle settles nothing over the new one."""
        requires(self.claims_expire, "the store's claims do not expire")
        key, payload = unique_id("req"), 1234
        handle = await self._won(idempotency_store, key, payload)
        await idempotency_store.complete(key, handle, Record.done(payload, {"balance": 42}))
        await self.expire_claim(idempotency_store, key)
        fresh = await self._won(idempotency_store, key, payload)
        with pytest.raises(StoreError):
            await idempotency_store.complete(key, handle, Record.done(payload, {"balance": 42}))
        await idempotency_store.complete(key, fresh, Record.done(payload, {"balance": 43}))
        replayed = await idempotency_store.claim(key, payload)
        assert replayed == Record.done(payload, {"balance": 43})

    async def test_forget_deletes_a_settled_record_and_keeps_a_pending_claim(
        self, idempotency_store: Store
    ) -> None:
        """ "``forget`` deletes a completed or failed record ... a pending claim raises InProgress and
        stays." A dead-lettered command is re-driven by forgetting its record, so the next claim must win.
        A pending claim's run may still complete, and its record would land after the delete, so it is
        refused and left alone. An absent key is a no-op."""
        payload = 1234
        settled = {
            "completed": Record.done(payload, {"balance": 42}),
            "failed": Record.failed(payload, "insufficient funds", "rejected"),
        }
        for name, record in settled.items():
            key = unique_id("req")
            handle = await self._won(idempotency_store, key, payload)
            await idempotency_store.complete(key, handle, record)
            await idempotency_store.forget(key)
            assert isinstance(await idempotency_store.claim(key, payload), Won), f"{name}: forgotten, so free"

        pending = unique_id("req")
        await self._won(idempotency_store, pending, payload)
        with pytest.raises(InProgress):
            await idempotency_store.forget(pending)
        held = await idempotency_store.claim(pending, payload)
        assert held == Record.pending(payload), "the refused forget kept the claim"

        absent = unique_id("req")
        await idempotency_store.forget(absent)
        assert isinstance(await idempotency_store.claim(absent, payload), Won), "nothing was there to forget"

    async def test_the_record_keeps_the_payload_hash_it_was_given(self, idempotency_store: Store) -> None:
        """The kernel compares hashes to decide ``PayloadMismatch``, so a store that truncates or
        re-signs the value turns a genuine duplicate into a mismatch.

        The hash is an unsigned 64-bit integer. A signed column without a cast passes every test until a
        hash above 2^63 appears.
        """
        key, payload = unique_id("req"), 18446744073709551615  # u64 max: the value a signed column loses
        await self._won(idempotency_store, key, payload)
        prior = await idempotency_store.claim(key, payload)
        assert not isinstance(prior, Won) and prior.payload_hash == payload, "stored losslessly"

    async def test_keys_are_independent(self, idempotency_store: Store) -> None:
        """Claiming one key must not claim another. A store with a prefix bug fails here."""
        a, b = unique_id("req"), unique_id("req")
        await self._won(idempotency_store, a, 1)
        assert isinstance(await idempotency_store.claim(b, 2), Won), "a different key is still free"

    async def test_a_rollback_frees_the_claim(
        self, idempotency_store: Store, request: pytest.FixtureRequest
    ) -> None:
        """ "every operation joins that transaction, so a rollback removes the claim:
        effectively-once."

        Only a ledger that joins the transaction runs this clause. A ledger of its own keeps the claim,
        which is the difference between effectively-once and at-least-once.
        """
        requires(joins_transaction(idempotency_store), _JOINS)
        unit_of_work = transactor(request)
        key = unique_id("req")
        with pytest.raises(RuntimeError):
            async with unit_of_work.begin() as tx:
                assert isinstance(await idempotency_store.claim(key, 1234, tx=tx), Won)
                raise RuntimeError("the handler failed")
        assert isinstance(await idempotency_store.claim(key, 1234), Won), "the rollback freed the key"

    async def test_a_claim_with_no_tx_joins_the_ambient_transaction(
        self, idempotency_store: Store, request: pytest.FixtureRequest
    ) -> None:
        """ "Given ``tx``, or with :func:`~edax.unit_of_work.current_tx` of its own transactor set, every
        operation joins that transaction." The guard claims with no ``tx`` inside the unit of work it
        opened, so this is the path production runs. A ledger that auto-commits there keeps the claim
        after the rollback, and the key stays pending for good."""
        requires(joins_transaction(idempotency_store), _JOINS)
        unit_of_work = transactor(request)
        key = unique_id("req")
        with pytest.raises(RuntimeError):
            async with transaction(unit_of_work):
                assert isinstance(await idempotency_store.claim(key, 1234), Won)
                raise RuntimeError("the handler failed")
        assert isinstance(await idempotency_store.claim(key, 1234), Won), "the rollback freed the key"

    async def test_a_claim_staged_in_an_open_transaction_is_never_won_by_another(
        self, idempotency_store: Store, request: pytest.FixtureRequest
    ) -> None:
        """ "A loser inside a transaction must block on the winner's row and then read it, or raise
        transient ``StoreError``; it must never read a stale snapshot and report a win, which runs the handler
        twice. Bound the wait with a lock or statement timeout, which raises ``StoreError`` or
        ``InProgress``." The first unit of work holds an uncommitted claim; a claimant in another one must
        not win while it is open. Run in a task of its own, so a store that blocks the claimant answers once
        the holder rolls back."""
        requires(joins_transaction(idempotency_store), _JOINS)
        unit_of_work = transactor(request)
        key = unique_id("req")

        async def hold(tx: Tx) -> None:
            assert isinstance(await idempotency_store.claim(key, 1234, tx=tx), Won)

        # A lock or statement timeout, `StoreError` or `InProgress`, is the contract's other allowed answer.
        contended, answered_after_rollback, _ = await while_held(
            unit_of_work.begin,
            hold,
            lambda tx: idempotency_store.claim(key, 1234, tx=tx),
            (StoreError, InProgress),
        )
        if isinstance(contended, Won):
            # A store that parked the contender on the holder's row lock answers after the rollback freed
            # the key: that win is correct. A win while the holder was open runs the handler twice, even
            # when the store's commit then waits for the holder: the guard runs the handler before it.
            assert answered_after_rollback, (
                "won a key another open transaction held: the handler would run twice"
            )
            return
        assert contended in (REFUSED, Record.pending(1234)), contended

    async def test_a_release_with_nothing_committed_leaves_a_concurrent_winners_record(
        self, idempotency_store: Store, request: pytest.FixtureRequest
    ) -> None:
        """A transient failure releases the claim inside its unit of work, and another unit of work then wins
        the key and commits its outcome before the first one commits. The releaser held nothing committed,
        so its commit must leave the winner's record, and its own re-claim must replay it. A release that
        stages a delete anyway erases the winner's record, and the next dispatch runs the handler again.

        The winner runs in a task of its own: a ledger that blocks it on the releaser's row lock does not
        deadlock the clause, which commits the releaser after a bounded wait and checks the same outcome."""
        requires(joins_transaction(idempotency_store), _JOINS)
        unit_of_work = transactor(request)
        key, payload = unique_id("req"), 1234
        winner_done = asyncio.Event()

        async def winner() -> None:
            async with unit_of_work.begin() as tx:
                won = await idempotency_store.claim(key, payload, tx=tx)
                assert isinstance(won, Won), f"the released key is free, got {won!r}"
                await idempotency_store.complete(
                    key, won.handle, Record.done(payload, {"balance": 42}), tx=tx
                )
            winner_done.set()

        async with unit_of_work.begin() as tx:
            mine = await idempotency_store.claim(key, payload, tx=tx)
            assert isinstance(mine, Won)
            await idempotency_store.release(key, mine.handle, tx=tx)
            racer = asyncio.create_task(winner())
            await until_answered(winner_done, racer)
            if winner_done.is_set():
                replay = await idempotency_store.claim(key, payload, tx=tx)
                assert replay == Record.done(payload, {"balance": 42}), (
                    f"the releaser's re-claim must replay the winner, got {replay!r}"
                )
        await asyncio.wait_for(racer, POLL_S)
        stored = await idempotency_store.claim(key, payload)
        assert stored == Record.done(payload, {"balance": 42}), (
            f"the releaser's commit erased the winner's record: {stored!r}"
        )

    async def test_a_handle_of_another_database_is_refused(self, idempotency_store: Store) -> None:
        """ "``True`` obliges you to raise terminal BadHandle for a handle from another database, never to
        auto-commit." The guard hands the store its unit of work's handle, so a store over another database
        than the guard's ``unit_of_work`` must fail at the claim. One that auto-commits instead keeps a claim
        the guard's rollback cannot take back, and the key stays pending for good."""
        requires(joins_transaction(idempotency_store), _JOINS)
        from ...backend.memory import MemoryTransactor  # another database than any adopter's store

        key, payload = unique_id("req"), 1234
        async with MemoryTransactor().begin() as foreign:
            with pytest.raises(BadHandle):
                await idempotency_store.claim(key, payload, tx=foreign)
        assert isinstance(await idempotency_store.claim(key, payload), Won), "the refused claim wrote nothing"
        held = unique_id("req")
        won = await self._won(idempotency_store, held, payload)
        async with MemoryTransactor().begin() as foreign:
            with pytest.raises(BadHandle):
                await idempotency_store.complete(held, won, Record.done(payload, "done"), tx=foreign)
            with pytest.raises(BadHandle):
                await idempotency_store.release(held, won, tx=foreign)
        prior = await idempotency_store.claim(held, payload)
        assert prior == Record.pending(payload), "the refused settlements wrote nothing"

    @pytest.mark.parametrize("settle", ["complete", "release"])
    async def test_a_key_another_open_transaction_holds_is_never_settled_over_it(
        self, idempotency_store: Store, request: pytest.FixtureRequest, settle: str
    ) -> None:
        """ "A ``joins_transaction`` store refuses ``complete`` and ``release`` of a key another open
        transaction has written." One unit of work stages the release of a committed claim; another settles
        the same key meanwhile. Landing over it would let the holder's commit erase the record, or free its
        claim. The settler may block until the holder ends (a row lock) or be refused with the transient
        :class:`~edax.errors.ConcurrencyError`; it may never land while the holder is open. The settler
        runs in a task of its own, so a store that blocks it answers once the holder rolls back."""
        requires(joins_transaction(idempotency_store), _JOINS)
        unit_of_work = transactor(request)
        key, payload = unique_id("req"), 1234
        async with unit_of_work.begin() as tx:
            won = await idempotency_store.claim(key, payload, tx=tx)
        assert isinstance(won, Won), f"a free key must be won, got {won!r}"

        async def hold(tx: Tx) -> None:
            await idempotency_store.release(key, won.handle, tx=tx)

        async def settle_meanwhile(tx: Tx) -> None:
            if settle == "complete":
                await idempotency_store.complete(key, won.handle, Record.done(payload, "done"), tx=tx)
            else:
                await idempotency_store.release(key, won.handle, tx=tx)

        # The seam's one transient refusal for a held key. A `StoreError` there fails the clause: the guard
        # would read it as a store fault, not a conflict to retry.
        got, _, after_rollback = await while_held(
            unit_of_work.begin, hold, settle_meanwhile, ConcurrencyError
        )
        stored = await idempotency_store.claim(key, payload)
        if got == REFUSED:
            assert stored == Record.pending(payload), f"{settle}: the refusal wrote nothing"
            return
        assert after_rollback, f"{settle} landed on a key another open transaction held"
        if settle == "complete":
            assert stored == Record.done(payload, "done"), stored
        else:
            assert isinstance(stored, Won), f"the release after the rollback freed the key, got {stored!r}"

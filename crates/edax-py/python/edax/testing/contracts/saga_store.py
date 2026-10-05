"""The :class:`~edax.saga.SagaStore` contract."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from ... import _native
from ...errors import ConcurrencyError
from ...message import CommandMessage, Payload
from ...saga import CompletedInstance, RunningInstance, SagaStore
from ...unit_of_work import transaction
from .._concurrency import RolledBack, race, staged_then_read, unique_id
from ._fixtures import Transactional
from .outbox import OutboxContract

#: A seam fixture: the store under test.
Store = SagaStore[Any]

#: The saga every saga store clause writes as, unless the clause names another.
_SAGA = "order"


def _saga_command(cid: str, version: int, index: int, saga: str = _SAGA) -> CommandMessage:
    """The ``index``-th command a step of ``saga`` committing ``version`` of instance ``cid`` sends: the
    kernel's row, with a real ``Fulfill`` payload, caused by the ``OrderPaid`` event ``evt-<cid>-<version>``
    of the flow ``flow-<cid>``, in that event's tenant."""
    payload = Payload("Fulfill", 1, f'{{"order_id":"{cid}"}}'.encode())
    return _native.saga_command(
        saga,
        cid,
        version,
        index,
        payload,
        f"evt-{cid}-{version}",
        f"flow-{cid}",
        1_714_564_800_000_000_001,
        "acme",
    )


class SagaStoreContract(Transactional):
    """Per-instance saga state under optimistic concurrency, its tombstone, and the commands each write
    stages in the saga outbox, each keyed by the saga's name. The outbox rows are the kernel's
    :class:`~edax.message.CommandMessage`, with the instance id as their ``aggregate_id``. Reading them back
    is the :class:`~edax.saga.SagaOutbox` port: :class:`SagaOutboxContract` holds it.

    Supply a ``saga_store`` fixture, and a ``transactor`` fixture unless the backend declares
    ``supports_transactions = False``.
    """

    async def _unpublished(self, saga_store: Store, cid: str, saga: str = _SAGA) -> list[str]:
        return [c.id for c in await saga_store.unpublished(saga, limit=1000) if c.aggregate_id == cid]

    async def test_an_unknown_instance_loads_as_none(self, saga_store: Store) -> None:
        """A saga that has not started yet has no instance; the runner reads ``None`` as "new"."""
        assert await saga_store.load(_SAGA, unique_id("corr")) is None

    async def test_save_then_load_round_trips_the_state(self, saga_store: Store) -> None:
        """The runner's whole job is fold-and-persist, so the state must come back as it went in."""
        cid = unique_id("corr")
        version = await saga_store.save(_SAGA, cid, {"step": "awaiting_payment"}, 0, "", [])
        instance = await saga_store.load(_SAGA, cid)
        assert isinstance(instance, RunningInstance) and instance.state == {"step": "awaiting_payment"}
        assert instance.version == version == 1

    async def test_save_is_compare_and_set_on_the_version(self, saga_store: Store) -> None:
        """Two events for one instance can arrive concurrently. Without a CAS here one fold
        silently overwrites the other and the saga forgets a step."""
        cid = unique_id("corr")
        await saga_store.save(_SAGA, cid, {"step": "one"}, 0, "", [])
        with pytest.raises(ConcurrencyError):
            await saga_store.save(_SAGA, cid, {"step": "clobbered"}, 0, "", [])
        instance = await saga_store.load(_SAGA, cid)
        assert isinstance(instance, RunningInstance) and instance.state == {"step": "one"}

    async def test_complete_writes_a_versioned_tombstone(self, saga_store: Store) -> None:
        """Completion is a write at the loaded version that keeps the row: a stale completion conflicts,
        and the instance then reads as completed one version on, not as absent."""
        cid = unique_id("corr")
        version = await saga_store.save(_SAGA, cid, {"step": "one"}, 0, "", [])
        with pytest.raises(ConcurrencyError):
            await saga_store.complete(_SAGA, cid, version + 5, [])
        assert await saga_store.complete(_SAGA, cid, version, []) == version + 1
        instance = await saga_store.load(_SAGA, cid)
        assert instance == CompletedInstance(version + 1)

    async def test_a_completed_instance_takes_no_write(self, saga_store: Store) -> None:
        """The tombstone is final. A writer that loaded the instance before it completed (ABA), a late
        event that would start the saga again at version 0 (resurrection), and a second completion all
        conflict, and the tombstone stays."""
        cid = unique_id("corr")
        await saga_store.save(_SAGA, cid, {"step": "one"}, 0, "", [])
        await saga_store.complete(_SAGA, cid, 1, [])
        writes: list[tuple[str, Callable[[], Awaitable[object]]]] = [
            ("stale_writer", lambda: saga_store.save(_SAGA, cid, {"step": "two"}, 1, "", [])),
            ("fresh_start", lambda: saga_store.save(_SAGA, cid, {"step": "one"}, 0, "", [])),
            ("at_the_tombstone", lambda: saga_store.save(_SAGA, cid, {"step": "two"}, 2, "", [])),
            ("second_completion", lambda: saga_store.complete(_SAGA, cid, 2, [])),
        ]
        for name, write in writes:
            with pytest.raises(ConcurrencyError):
                await write()
            instance = await saga_store.load(_SAGA, cid)
            assert instance == CompletedInstance(2), name

    async def test_the_steps_commands_commit_with_its_state(self, saga_store: Store) -> None:
        """The saga outbox: a write's commands are shown once it committed, oldest first, with every
        field intact; a refused write enqueues none, so a fold that lost the race sends nothing."""
        cid = unique_id("corr")
        await saga_store.save(_SAGA, cid, {"step": "one"}, 0, "", [_saga_command(cid, 1, 0)])
        with pytest.raises(ConcurrencyError):
            await saga_store.save(_SAGA, cid, {"step": "lost"}, 0, "", [_saga_command(cid, 1, 0)])
        await saga_store.complete(_SAGA, cid, 1, [_saga_command(cid, 2, 0), _saga_command(cid, 2, 1)])
        want = _saga_command(cid, 2, 1)
        sent = [_saga_command(cid, 1, 0).id, _saga_command(cid, 2, 0).id, want.id]
        assert await self._unpublished(saga_store, cid) == sent
        row = next(c for c in await saga_store.unpublished(_SAGA, limit=1000) if c.id == want.id)
        assert row == want, "every field of the command, its tenant, lineage and issued_at included"

    async def test_the_folded_marks_round_trip(self, saga_store: Store) -> None:
        """``save`` keeps ``folded`` as written and ``load`` returns it: the runner skips an event redelivered
        after its step committed by these marks. A store that drops them folds the event again, and the step's
        commands go out a second time under new ids."""
        cid = unique_id("corr")
        folded = f'{{"{cid}":{{"sequence":1,"id":"evt-1","unsequenced":"evt-u"}}}}'
        await saga_store.save(_SAGA, cid, {"step": "one"}, 0, folded, [])
        instance = await saga_store.load(_SAGA, cid)
        assert isinstance(instance, RunningInstance) and instance.folded == folded, (
            "the marks come back as they were saved"
        )
        await saga_store.save(_SAGA, cid, {"step": "two"}, 1, "", [])
        instance = await saga_store.load(_SAGA, cid)
        assert isinstance(instance, RunningInstance) and instance.folded == "", "each save replaces them"

    async def test_two_sagas_in_one_database_share_neither_an_instance_nor_a_command(
        self, saga_store: Store
    ) -> None:
        """Two sagas share one database so their writes join the events' unit of work, and may share an
        instance id: both key by the order. A store keyed by the instance alone hands one saga the other's
        state, or drains its command through the wrong saga's codec and sink. Each saga keeps its own row at
        its own version and its own outbox."""
        cid = unique_id("corr")
        sagas = {_SAGA: {"step": "awaiting_payment"}, "billing": {"step": "invoiced"}}
        for saga, state in sagas.items():
            assert await saga_store.save(saga, cid, state, 0, "", [_saga_command(cid, 1, 0, saga)]) == 1, saga
        for saga, state in sagas.items():
            instance = await saga_store.load(saga, cid)
            assert (
                isinstance(instance, RunningInstance) and instance.state == state and instance.version == 1
            ), saga
            assert await self._unpublished(saga_store, cid, saga) == [_saga_command(cid, 1, 0, saga).id], saga
        # The mark names both ids, so only the saga it names may prune its own row.
        both = [_saga_command(cid, 1, 0, saga).id for saga in sagas]
        await saga_store.mark_published(_SAGA, both)
        assert await self._unpublished(saga_store, cid, "billing") == [
            _saga_command(cid, 1, 0, "billing").id
        ], "a mark prunes one saga's command only"
        assert await saga_store.load(unique_id("saga"), cid) is None, (
            "a saga that never wrote has no instance"
        )

    async def test_two_steps_at_one_version_land_exactly_one_write(
        self, saga_store: Store, request: pytest.FixtureRequest
    ) -> None:
        """Two events of one correlation fold concurrently, each staging its write and its commands before
        either commits. A store that reads the version and then writes lets both land: one step's state
        overwrites the other's, and both steps' commands are sent. Exactly one may land, with its commands
        alone."""
        transactor = self._unit_of_work(request)
        cid = unique_id("corr")
        command = _saga_command(cid, 1, 0)
        outcomes = await race(
            transactor.begin,
            lambda tx: saga_store.save(_SAGA, cid, {"step": "paid"}, 0, "", [command], tx=tx),
            lambda tx: saga_store.save(_SAGA, cid, {"step": "cancelled"}, 0, "", [command], tx=tx),
        )
        assert sorted(outcomes) == ["conflict", "landed"], f"exactly one step lands, got {outcomes}"
        instance = await saga_store.load(_SAGA, cid)
        want = {"step": "paid"} if outcomes[0] == "landed" else {"step": "cancelled"}
        assert isinstance(instance, RunningInstance) and instance.version == 1 and instance.state == want
        assert await self._unpublished(saga_store, cid) == [command.id], "the winner's command, once"

    async def test_an_uncommitted_step_is_invisible_to_another_transaction(
        self, saga_store: Store, request: pytest.FixtureRequest
    ) -> None:
        """A step's staged state is not another worker's to fold: one that sees it decides on a state that
        may still roll back."""
        transactor = self._unit_of_work(request)
        cid = unique_id("corr")
        seen = await staged_then_read(
            transactor.begin,
            lambda tx: saga_store.save(_SAGA, cid, {"step": "paid"}, 0, "", [], tx=tx),
            lambda tx: saga_store.load(_SAGA, cid, tx=tx),
        )
        assert seen is None, "another transaction saw a staged step"

    async def test_a_rolled_back_step_leaves_no_command(
        self, saga_store: Store, request: pytest.FixtureRequest
    ) -> None:
        """A step whose unit of work rolls back must take its commands with it: the relay would otherwise
        send a decision the store never kept."""
        transactor = self._unit_of_work(request)
        cid = unique_id("corr")
        with pytest.raises(RuntimeError):
            async with transaction(transactor):
                await saga_store.save(_SAGA, cid, {"step": "one"}, 0, "", [_saga_command(cid, 1, 0)])
                unsent = await self._unpublished(saga_store, cid)
                assert unsent == [], "an uncommitted command is not one to send"
                raise RuntimeError("the handler failed")
        assert await self._unpublished(saga_store, cid) == []
        assert await saga_store.load(_SAGA, cid) is None

    async def test_a_save_with_no_tx_joins_the_ambient_transaction(
        self, saga_store: Store, request: pytest.FixtureRequest
    ) -> None:
        """The runner saves with no ``tx`` inside the host's unit of work. A store that auto-commits
        there keeps the new state when the commands it decided roll back, and the saga skips a step."""
        transactor = self._unit_of_work(request)
        cid = unique_id("corr")
        with pytest.raises(RuntimeError):
            async with transaction(transactor):
                await saga_store.save(_SAGA, cid, {"step": "awaiting_payment"}, 0, "", [])
                raise RuntimeError("the handler failed")
        assert await saga_store.load(_SAGA, cid) is None, "the rollback took the save with it"


class SagaOutboxContract(OutboxContract[CommandMessage]):
    """One saga's command outbox, read through :class:`~edax.saga.SagaOutbox`: the commands each step's
    write stages, every field of the kernel's row intact.

    Supply an ``outbox`` fixture, ``SagaOutbox(store, "order")``, and a ``saga_store`` fixture, the same
    store, which writes the rows as the saga ``order``. With ``supports_transactions`` on, the default, supply
    a ``transactor`` fixture too: the unit of work the rolled-back write joins.
    """

    async def write_rows(
        self, request: pytest.FixtureRequest, n: int, *, commit: bool
    ) -> list[CommandMessage]:
        saga_store: Store = request.getfixturevalue("saga_store")
        cid = unique_id("corr")
        rows = [_saga_command(cid, 1, index) for index in range(n)]
        if commit:
            await saga_store.save(_SAGA, cid, {"step": "one"}, 0, "", rows)
            return rows
        with pytest.raises(RolledBack):
            async with transaction(self._unit_of_work(request)):
                await saga_store.save(_SAGA, cid, {"step": "one"}, 0, "", rows)
                raise RolledBack
        return rows

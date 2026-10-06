"""The :class:`~edax.outbox.CommandOutbox` contract."""

from __future__ import annotations

from typing import Any

import pytest

from ..._values import replace
from ...errors import BadHandle, CodecError
from ...message import CommandMessage
from ...outbox import CommandOutbox, Outbox
from ...store import EventStore
from .._concurrency import RolledBack, unique_id
from ..messages import command, draft
from .outbox import OutboxContract

#: A seam fixture: the write side under test.
Writer = CommandOutbox[Any]


class CommandOutboxContract(OutboxContract[CommandMessage]):
    """The command outbox: the commands :meth:`~edax.repository.Repository.save` sends, written under the
    save's ``tx`` so they commit with its events, and drained by a :class:`~edax.outbox.Relay` once committed.

    Supply a ``command_outbox`` fixture, the write side, an ``outbox`` fixture, the read side (one object may
    be both), and an ``event_store`` fixture over the same database: the outbox writes under the event
    store's ``tx``.
    """

    async def write_rows(
        self, request: pytest.FixtureRequest, n: int, *, commit: bool
    ) -> list[CommandMessage]:
        event_store: EventStore[Any] = request.getfixturevalue("event_store")
        command_outbox: Writer = request.getfixturevalue("command_outbox")
        rows = [command(unique_id("order")) for _ in range(n)]
        if commit:
            async with event_store.begin() as tx:
                await command_outbox.enqueue(tx, rows)
            return rows
        with pytest.raises(RolledBack):
            async with event_store.begin() as tx:
                await command_outbox.enqueue(tx, rows)
                raise RolledBack
        return rows

    async def _waiting(self, request: pytest.FixtureRequest, aggregate_id: str) -> list[CommandMessage]:
        """The rows waiting for ``aggregate_id``, read through the ``outbox`` fixture, the read side."""
        outbox: Outbox[CommandMessage] = request.getfixturevalue("outbox")
        return [c for c in await outbox.unpublished(limit=1000) if c.aggregate_id == aggregate_id]

    async def test_enqueued_commands_commit_and_roll_back_with_the_events(
        self, command_outbox: Writer, request: pytest.FixtureRequest
    ) -> None:
        """ "``enqueue`` writes the messages under ``tx``", so a command is sent only if the events it
        follows from landed. One rolled back with its events never reaches the relay, and one committed
        comes back whole: a column the store forgot, such as the tenant a pooled bus claims inside, is a
        command its consumer cannot run."""
        event_store: EventStore[Any] = request.getfixturevalue("event_store")
        agg = unique_id("order")
        with pytest.raises(RolledBack):
            async with event_store.begin() as tx:
                await event_store.append(tx, agg, 0, [draft(agg, 1)])
                await command_outbox.enqueue(tx, [command(agg)])
                assert await self._waiting(request, agg) == [], "a staged command is not one to send"
                raise RolledBack
        assert await self._waiting(request, agg) == [], "the rollback took the command with the events"
        sent = [command(agg), command(agg)]
        async with event_store.begin() as tx:
            await event_store.append(tx, agg, 0, [draft(agg, 1)])
            await command_outbox.enqueue(tx, sent)
        assert await self._waiting(request, agg) == sent, "every field as sent, the tenant included"

    async def test_a_handle_of_another_database_is_refused(
        self, command_outbox: Writer, request: pytest.FixtureRequest
    ) -> None:
        """ "A backend over another database than the event store refuses it with the terminal
        ``BadHandle``." Written anyway, the command commits on its own, and is sent when the events it
        follows from roll back."""
        from ...backend.memory import MemoryTransactor  # another database than any adopter's outbox

        agg = unique_id("order")
        async with MemoryTransactor().begin() as foreign:
            with pytest.raises(BadHandle):
                await command_outbox.enqueue(foreign, [command(agg)])
        assert await self._waiting(request, agg) == [], "the refused enqueue wrote nothing"

    async def test_an_id_already_waiting_is_refused(
        self, command_outbox: Writer, request: pytest.FixtureRequest
    ) -> None:
        """ "A command id is unique among the rows waiting": the relay marks rows by id, so a second row
        under one id would be pruned unsent with the first. A repeat is the terminal ``CodecError``, and
        the batch that carries it writes nothing."""
        event_store: EventStore[Any] = request.getfixturevalue("event_store")
        agg = unique_id("order")
        first = command(agg)
        async with event_store.begin() as tx:
            await command_outbox.enqueue(tx, [first])
        with pytest.raises(CodecError):
            async with event_store.begin() as tx:
                await command_outbox.enqueue(tx, [command(agg), replace(first, data=b'{"order_id":"x"}')])
        assert await self._waiting(request, agg) == [first], "the refused batch wrote nothing"

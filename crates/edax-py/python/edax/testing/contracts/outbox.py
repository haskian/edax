"""The :class:`~edax.outbox.Outbox` contract: the read side every transactional outbox shares, the event
outbox, a saga's command outbox and the command outbox alike."""

from __future__ import annotations

from typing import Generic, TypeVar

import pytest

from ... import _native
from ...message import CommandMessage, EventMessage
from ...outbox import CountedOutbox, Outbox
from ._fixtures import Transactional, requires

R = TypeVar("R", EventMessage, CommandMessage)


class OutboxContract(Transactional, Generic[R]):
    """The clauses of :class:`~edax.outbox.Outbox`, stated once for every outbox. A seam's own contract
    subclasses it and says how rows are written, in :meth:`write_rows`; an adopter subclasses that one.

    Supply an ``outbox`` fixture: the read side under test. The rows are written through the write port, so
    ``write_rows`` reads that port's fixture from ``request``.
    """

    async def write_rows(self, request: pytest.FixtureRequest, n: int, *, commit: bool) -> list[R]:
        """Write ``n`` rows through the write port in one unit of work, commit it or roll it back, and return
        the rows as written: what ``unpublished`` must hand back for them once committed."""
        raise NotImplementedError

    async def _shown(self, outbox: Outbox[R], rows: list[R]) -> list[R]:
        """The rows of ``rows`` the outbox shows, in the order it shows them. Other clauses of the suite may
        leave rows of their own, so the clause looks at its own alone."""
        ids = {row.id for row in rows}
        return [row for row in await outbox.unpublished(limit=1000) if row.id in ids]

    async def test_a_committed_row_comes_back_whole(
        self, outbox: Outbox[R], request: pytest.FixtureRequest
    ) -> None:
        """ "``unpublished`` returns committed unmarked rows ... each as the write port stored it, every field
        intact." A column the store forgot, the tenant or the lineage, is a row its consumer cannot route."""
        rows = await self.write_rows(request, 2, commit=True)
        assert await self._shown(outbox, rows) == rows, "every field as written, in order"

    async def test_a_rolled_back_row_never_shows(
        self, outbox: Outbox[R], request: pytest.FixtureRequest
    ) -> None:
        """ "never a row a ``tx`` staged and may still roll back." A relay that sees one publishes a change
        that never happened."""
        requires(self.supports_transactions, "the backend declares no unit of work")
        rows = await self.write_rows(request, 1, commit=False)
        assert await self._shown(outbox, rows) == [], "a rolled-back write leaves nothing to publish"

    async def test_unpublished_is_oldest_commit_first(
        self, outbox: Outbox[R], request: pytest.FixtureRequest
    ) -> None:
        """ "Oldest commit first." A relay publishes in that order, so a store that pages newest first sends
        a later decision ahead of an earlier one."""
        first = await self.write_rows(request, 2, commit=True)
        second = await self.write_rows(request, 1, commit=True)
        assert await self._shown(outbox, first + second) == first + second

    async def test_unpublished_honours_its_limit(
        self, outbox: Outbox[R], request: pytest.FixtureRequest
    ) -> None:
        """ "``limit`` decides the page." A store ignoring it hands a relay the whole backlog in one batch,
        and the relay's duplicate window becomes the whole backlog."""
        await self.write_rows(request, 3, commit=True)
        assert len(await outbox.unpublished(limit=2)) == 2, "three rows wait, so the limit decides"

    async def test_mark_published_is_idempotent_and_ignores_unknown_ids(
        self, outbox: Outbox[R], request: pytest.FixtureRequest
    ) -> None:
        """ "``mark_published`` is idempotent and durable; an unknown id and an empty list are no-ops." A
        relay is at-least-once, so it re-marks after a lost response; a marked row stops coming back while
        the rest still wait."""
        rows = await self.write_rows(request, 2, commit=True)
        await outbox.mark_published([])
        await outbox.mark_published([rows[0].id])
        await outbox.mark_published([rows[0].id, _native.guid()])
        assert await self._shown(outbox, rows) == rows[1:], "only the marked row was pruned"

    async def test_unpublished_count_tracks_what_is_waiting(
        self, outbox: Outbox[R], request: pytest.FixtureRequest
    ) -> None:
        """ "how many committed unmarked rows wait now." A count that ignores ``mark_published``, or counts
        uncommitted rows, reports a backlog that never drains and makes the health check it exists for
        permanently red. Runs for a :class:`~edax.outbox.CountedOutbox` only."""
        if not isinstance(outbox, CountedOutbox):  # an `if`, not `requires`, so the checker narrows it
            raise pytest.skip.Exception("the outbox does not count its backlog")
        # Deltas, not absolutes: the store may hold rows from other clauses in this suite.
        before = await outbox.unpublished_count()
        if self.supports_transactions:
            await self.write_rows(request, 1, commit=False)
        rows = await self.write_rows(request, 2, commit=True)
        assert await outbox.unpublished_count() - before == 2, "one per committed row"
        await outbox.mark_published([rows[0].id])
        assert await outbox.unpublished_count() - before == 1, "a marked row stops counting"

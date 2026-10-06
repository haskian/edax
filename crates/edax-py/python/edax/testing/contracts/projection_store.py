"""The :class:`~edax.projection.ProjectionStore` contract."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from ...errors import CodecError, ConcurrencyError, EdaxError
from ...filter import eq
from ...projection import Entity, ProjectionQuery, ProjectionStore
from ...query import Query, encode_cursor, sort_key
from ...unit_of_work import transaction
from .._concurrency import race, staged_then_read, unique_id
from ._fixtures import Transactional

#: A seam fixture: the store under test.
Store = ProjectionStore[Any]

#: Saves over a row stored at version 1 as ``{"balance": 1}``, by branch: the version the writer loaded,
#: the document it saves, and the refusal. Each writes nothing.
_REFUSED_SAVES: dict[str, tuple[int, Any, type[EdaxError]]] = {
    "stale": (0, {"balance": 99}, ConcurrencyError),
    "ahead": (7, {"balance": 99}, ConcurrencyError),
    # Not documents at all, as an untyped caller can still hand them over.
    "a_list": (1, ["ada@example.com"], CodecError),
    "a_scalar": (1, 36, CodecError),
    "null": (1, None, CodecError),
}


class ProjectionStoreContract(Transactional):
    """Read-model documents under optimistic concurrency, and the kernel's paging for a store that is a
    :class:`~edax.projection.ProjectionQuery` (the suite checks with ``isinstance``).

    Supply a ``projection_store`` fixture, and a ``transactor`` fixture unless the backend declares
    ``supports_transactions = False``.
    """

    async def test_an_absent_entity_reads_as_none(self, projection_store: Store) -> None:
        """ "``get`` returns the current committed row plus the ``tx``'s own writes, or ``None``." """
        assert await projection_store.get(unique_id("absent")) is None

    async def test_save_returns_the_new_version_and_get_reads_the_keyed_row(
        self, projection_store: Store
    ) -> None:
        """ "``save`` writes at ``version + 1`` [...] and returns that new version", and "``get`` [...]
        document carries the row key as ``"id"``, overwriting any the saved document held". A store that
        returns no version makes the caller's next save conflict with itself; one that reads the document
        alone disagrees with ``get_many`` on what a row is."""
        entity = Entity(unique_id("acct"), 0, {"id": "stale", "balance": 1})
        assert await projection_store.save(entity) == 1, "the new version"
        stored = await projection_store.get(entity.id)
        assert stored == Entity(entity.id, 1, {"id": entity.id, "balance": 1}), "keyed by the row, at 1"

    @pytest.mark.parametrize("branch", list(_REFUSED_SAVES))
    async def test_a_refused_save_writes_nothing(self, projection_store: Store, branch: str) -> None:
        """ "``save`` writes at ``version + 1`` only if the stored version still equals ``entity.version``
        (``0`` means not yet stored), else it raises ``ConcurrencyError`` and writes nothing." The database
        enforces it, never a read first, and both directions are checked: stale *and* ahead. "``save``
        refuses a document :func:`~edax.projection.check_document` refuses, with ``CodecError``, and writes
        nothing": the query reads each row's key from its ``"id"`` field, so a list or a scalar could be
        saved and never found."""
        version, document, refusal = _REFUSED_SAVES[branch]
        doc_id = unique_id("acct")
        await projection_store.save(Entity(doc_id, 0, {"balance": 1}))
        with pytest.raises(refusal):
            await projection_store.save(Entity(doc_id, version, document))
        stored = await projection_store.get(doc_id)
        assert stored == Entity(doc_id, 1, {"id": doc_id, "balance": 1}), f"{branch}: and wrote nothing"

    async def test_get_sees_this_transactions_own_save(
        self, projection_store: Store, request: pytest.FixtureRequest
    ) -> None:
        """ "`get` returns the current committed row plus the `tx`'s own writes."

        A handler that saves and then re-reads inside one unit of work must see its own write, or it
        decides on stale state and the second save conflicts with the first.
        """
        transactor = self._unit_of_work(request)
        entity = Entity(unique_id("acct"), 0, {"balance": 1})
        async with transactor.begin() as tx:
            await projection_store.save(entity, tx=tx)
            seen = await projection_store.get(entity.id, tx=tx)
            assert seen is not None and seen.document["balance"] == 1, "its own uncommitted write"

    async def test_a_save_with_no_tx_joins_the_ambient_transaction(
        self, projection_store: Store, request: pytest.FixtureRequest
    ) -> None:
        """ "A save under ``tx`` (or the ambient :func:`~edax.unit_of_work.current_tx`) is staged until the
        transactor commits." A projector saves with no ``tx`` inside the unit of work the guard opened;
        a store that auto-commits there keeps a read-model write the rollback should have dropped."""
        transactor = self._unit_of_work(request)
        entity = Entity(unique_id("acct"), 0, {"balance": 1})
        with pytest.raises(RuntimeError):
            async with transaction(transactor):
                await projection_store.save(entity)
                raise RuntimeError("the handler failed")
        assert await projection_store.get(entity.id) is None, "the rollback took the save with it"

    async def test_two_writers_at_one_version_land_exactly_one_save(
        self, projection_store: Store, request: pytest.FixtureRequest
    ) -> None:
        """ "Enforce it in the database." Two projectors both save a new row at
        version 0, each staging before either commits. A store that reads the version and then writes
        lets the second overwrite the first: a read-model update lost. Exactly one may land."""
        transactor = self._unit_of_work(request)
        doc_id = unique_id("acct")
        outcomes = await race(
            transactor.begin,
            lambda tx: projection_store.save(Entity(doc_id, 0, {"balance": 1}), tx=tx),
            lambda tx: projection_store.save(Entity(doc_id, 0, {"balance": 2}), tx=tx),
        )
        assert sorted(outcomes) == ["conflict", "landed"], f"exactly one writer lands, got {outcomes}"
        stored = await projection_store.get(doc_id)
        want = {"id": doc_id, "balance": 1 if outcomes[0] == "landed" else 2}
        assert stored == Entity(doc_id, 1, want), "the winner's row"

    async def test_an_uncommitted_save_is_invisible_to_another_transaction(
        self, projection_store: Store, request: pytest.FixtureRequest
    ) -> None:
        """ "``get`` returns the current committed row plus the ``tx``'s own writes." Another unit of
        work's staged row is neither: a reader that sees it serves a document that may still roll back."""
        transactor = self._unit_of_work(request)
        doc_id = unique_id("acct")
        seen = await staged_then_read(
            transactor.begin,
            lambda tx: projection_store.save(Entity(doc_id, 0, {"balance": 1}), tx=tx),
            lambda tx: projection_store.get(doc_id, tx=tx),
        )
        assert seen is None, "another transaction saw a staged row"

    async def test_entities_are_independent(self, projection_store: Store) -> None:
        """Concurrency is per id. A store versioning the whole table serialises every read model."""
        a, b = Entity(unique_id("a"), 0, {"v": 1}), Entity(unique_id("b"), 0, {"v": 2})
        await projection_store.save(a)
        await projection_store.save(b)
        got_a, got_b = await projection_store.get(a.id), await projection_store.get(b.id)
        assert got_a is not None and got_a.document["v"] == 1
        assert got_b is not None and got_b.document["v"] == 2

    async def _seed_ranked(self, projection_store: Store, run: str, ranks: list[int]) -> list[str]:
        """Save one document per rank under a filterable ``run`` tag, so a durable store shared across
        tests pages only this test's rows. Returns the ids in the kernel's order: rank, then id."""
        ids = [f"{run}-{i:02d}" for i in range(len(ranks))]
        for doc_id, rank in zip(ids, ranks, strict=True):
            await projection_store.save(Entity(doc_id, 0, {"run": run, "rank": rank}))
        return [doc_id for _, doc_id in sorted(zip(ranks, ids, strict=True))]

    async def test_get_many_returns_the_row_key_as_the_id(self, projection_store: Store) -> None:
        """ "Each row is a document as ``get`` reads it, its row key as ``"id"``": the kernel sorts its
        keyset tiebreak on ``id`` and mints every cursor from it.

        The key is authoritative, so it overwrites an ``id`` the saved document carries. A store that
        returns the stored document alone pages correctly only while every writer remembers to copy the
        key into the document, and a filter on ``id`` — how one shared store serves several tenants —
        reads another tenant's rows the moment one writer forgets.
        """
        pages = _require_queries(projection_store)
        run = unique_id("run")
        await projection_store.save(Entity(f"{run}-a", 0, {"run": run}))
        await projection_store.save(Entity(f"{run}-b", 0, {"run": run, "id": "stale"}))

        page = await pages.get_many(Query(filter=eq("run", run), sort=[sort_key("run")]))
        keys = sorted(row.get("id", "") for row in page.rows)  # absent reads as "", so this asserts
        assert keys == [f"{run}-a", f"{run}-b"], "the id is the row key, not the document's own"

    async def test_get_many_pages_every_row_once_by_its_next_cursor(self, projection_store: Store) -> None:
        """ "Keyset resume strictly after the cursor tuple", and a ``next`` minted only while rows
        follow. Rows that tie on the sort key are told apart by ``id``, and a row committed between
        pages after the cursor arrives too, so a reader following ``next`` sees each row once and stops
        on the last page. A store paging by offset, or one that drops ``next``, loses or repeats rows."""
        pages = _require_queries(projection_store)
        run = unique_id("run")
        order = await self._seed_ranked(projection_store, run, [2, 1, 1, 1, 3])
        query = Query(filter=eq("run", run), sort=[sort_key("rank")], limit=2)
        page = await pages.get_many(query)
        seen = [row["id"] for row in page.rows]
        late, early = f"{run}-late", f"{run}-early"
        await projection_store.save(Entity(late, 0, {"id": late, "run": run, "rank": 9}))  # after the cursor
        await projection_store.save(Entity(early, 0, {"id": early, "run": run, "rank": 0}))  # before it
        for _ in range(len(order) + 2):  # bounded, so a store whose cursor repeats a row fails, not hangs
            if page.next is None:
                break
            page = await pages.get_many(replace(query, after=page.next))
            seen.extend(row["id"] for row in page.rows)
        assert seen == [*order, late], "every row once, in order, the late row last, the early one never"

    async def test_next_is_the_kernels_cursor(self, projection_store: Store) -> None:
        """ "A ``next`` minted with ``encode_cursor`` from the last row." A store minting its own
        cursor pages within one service but not across them: another service, or the same one after a
        backend change, cannot read it."""
        pages = _require_queries(projection_store)
        run = unique_id("run")
        await self._seed_ranked(projection_store, run, [1, 2, 3])
        sort = [sort_key("rank")]
        page = await pages.get_many(Query(filter=eq("run", run), sort=sort, limit=2))
        assert page.next == encode_cursor(page.rows[-1], sort)

    async def test_a_cursor_minted_under_another_sort_is_refused(self, projection_store: Store) -> None:
        """ "Read ``after`` with ``decode_cursor``, which refuses a cursor minted under another sort."
        Read anyway, the cursor's values bind to the wrong keys and the page is garbage, silently."""
        pages = _require_queries(projection_store)
        run = unique_id("run")
        await self._seed_ranked(projection_store, run, [1, 2, 3])
        page = await pages.get_many(Query(filter=eq("run", run), sort=[sort_key("rank")], limit=1))
        with pytest.raises(CodecError):
            await pages.get_many(
                Query(filter=eq("run", run), sort=[sort_key("rank", desc=True)], after=page.next)
            )


def _require_queries(projection_store: Store) -> ProjectionQuery[Any]:
    """The store as a :class:`~edax.projection.ProjectionQuery`, or a skip of the paging clause for one that
    is none."""
    if not isinstance(projection_store, ProjectionQuery):
        raise pytest.skip.Exception("the store is no ProjectionQuery")
    return projection_store

"""Step 11: query a read model: filter, sort, and page through it with a cursor.

`get_many` returns a `Page` of `rows` and a `next` cursor. Pass `next` back as `after` to get the
following page. `next` is `None` on the last page. A cursor resumes after the last row it saw, so
an insert between two reads never shifts a page.

Run:  cd examples/py && uv run python 11_projection_query.py
"""

from __future__ import annotations

import asyncio

from edax import CodecError, Entity
from edax.backend.memory import MemoryProjectionStore
from edax.filter import and_, eq, gte
from edax.query import Document, sort_key

ACCOUNTS: list[Document] = [
    {"id": "a1", "owner": "Ann", "balance": 300, "status": "active"},
    {"id": "a2", "owner": "Bob", "balance": 150, "status": "active"},
    {"id": "a3", "owner": "Cid", "balance": 150, "status": "active"},  # ties a2: id breaks it
    {"id": "a4", "owner": "Dee", "balance": 50, "status": "active"},  # filtered out: below 100
    {"id": "a5", "owner": "Eve", "balance": 500, "status": "closed"},  # filtered out: closed
    {"id": "a6", "owner": "Fay", "balance": 100, "status": "active"},
]


async def main() -> None:
    # 1. Save one document per account. Every document needs a unique "id".
    store = MemoryProjectionStore()
    for account in ACCOUNTS:
        await store.save(Entity(account["id"], 0, account))

    # 2. Active accounts with at least 100, richest first, id as the tiebreak.
    only = and_(gte("balance", 100), eq("status", "active"))
    order = [sort_key("balance", desc=True), sort_key("id")]

    # 3. Read two rows per page until `next` is None.
    pages: list[list[str]] = []
    after: str | None = None
    while True:
        page = await store.get_many(filter=only, sort=order, limit=2, after=after)
        pages.append([row["id"] for row in page.rows])
        print(f"page {len(pages)}: {[(row['owner'], row['balance']) for row in page.rows]}")
        if page.next is None:
            break
        after = page.next

    # 4. A cursor belongs to its sort. Under another sort it is refused, not misread.
    first_page = await store.get_many(filter=only, sort=order, limit=2)
    try:
        await store.get_many(filter=only, sort=[sort_key("id")], after=first_page.next)
        raise AssertionError("expected CodecError")
    except CodecError:
        print("a cursor under another sort -> CodecError")

    assert pages == [["a1", "a2"], ["a3", "a6"]], pages
    print("\nok: filtered, sorted and paged; the page boundary fell inside a tie and lost nothing")


if __name__ == "__main__":
    asyncio.run(main())

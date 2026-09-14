"""Read side: filter, sort, and keyset-paginate a projection — kernel-pinned semantics.

    save documents ─▶ ProjectionStore ─▶ get_many(filter, sort, limit) ─▶ page 1
                                              encode_cursor(last) ─▶ get_many(..., after=cursor) ─▶ page 2

Filter, row order and page boundary are all decided by the kernel, so every binding pages the same
model identically and a cursor minted here is accepted by any other. Keyset over offset: page 2 resumes
*after* the cursor, so it stays correct across ties (id tiebreak) and inserts between reads.

Run:  maturin develop -m crates/edax-py/Cargo.toml  &&  python examples/py/projection_query.py
"""

import asyncio

from edax import Entity
from edax.backend.memory import MemoryProjectionStore
from edax.filter import and_, eq, gte
from edax.query import Document, encode_cursor, sort_key

ACCOUNTS: list[Document] = [
    {"id": "a1", "balance": 300, "status": "active"},
    {"id": "a2", "balance": 150, "status": "active"},
    {"id": "a3", "balance": 150, "status": "active"},  # ties a2 on balance -> id breaks it
    {"id": "a4", "balance": 50, "status": "active"},  # filtered out: balance < 100
    {"id": "a5", "balance": 500, "status": "closed"},  # filtered out: not active
    {"id": "a6", "balance": 100, "status": "active"},
]


async def main() -> None:
    store = MemoryProjectionStore()
    for acct in ACCOUNTS:
        await store.save(Entity(acct["id"], 0, acct))

    flt = and_(gte("balance", 100), eq("status", "active"))
    sort = [sort_key("balance", desc=True), sort_key("id")]  # richest first, id as tiebreak

    page1 = await store.get_many(filter=flt, sort=sort, limit=2)
    cursor = encode_cursor(page1[-1], sort)
    page2 = await store.get_many(filter=flt, sort=sort, limit=2, after=cursor)

    ids1, ids2 = [d["id"] for d in page1], [d["id"] for d in page2]
    print("page 1:", ids1, " cursor ->", cursor)
    print("page 2:", ids2)

    assert ids1 == ["a1", "a2"], ids1
    assert ids2 == ["a3", "a6"], ids2  # resumed past the a2 tie
    print("\nok: filtered, ordered, and keyset-paginated a read model")


if __name__ == "__main__":
    asyncio.run(main())

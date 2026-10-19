"""A subject's ``private`` part at rest in the memory and Postgres sinks: one case per branch of sealing it."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, replace
from typing import Any, Literal
from uuid import uuid4

import pytest
from edax import _json, _native
from edax.backend.ingest_postgres import PostgresIngestionSink, reference_ingestion_ddl
from edax.backend.postgres import PgConnection, PostgresTransactor
from edax.errors import BadHandle, CodecError, Shredded
from edax.ingestion import BatchInput, IngestionTarget, MemoryIngestionSink
from edax.sealing import Cipher, KeyReservation, KeyScope, MemoryKeys, Sealing

URL = os.environ.get("EDAX_POSTGRES_URL")
PRIVATE = {"email": "ana@example.com", "card": {"last4": "4242"}}
PUBLIC = {"status": "paid", "total": "41.90"}

Sink = MemoryIngestionSink | PostgresIngestionSink


class CountingKeys(MemoryKeys):
    """The reference provider, recording each reservation's scope and count."""

    def __init__(self) -> None:
        super().__init__()
        self.reserved: list[tuple[str | None, int]] = []

    async def reserve(self, scope: KeyScope, cipher: Cipher, count: int = 1) -> KeyReservation:
        self.reserved.append((scope.subject, count))
        return await super().reserve(scope, cipher, count)


@pytest.fixture(params=["memory", "postgres"])
async def backend(request: pytest.FixtureRequest) -> AsyncIterator[Callable[..., Sink]]:
    if request.param == "memory":
        yield lambda target, sealing: MemoryIngestionSink(target, sealing=sealing)
        return
    if URL is None:
        pytest.skip("EDAX_POSTGRES_URL is not set: no live Postgres")
    asyncpg = pytest.importorskip("asyncpg")
    pool = await asyncpg.create_pool(URL, min_size=1, max_size=3)
    schema = f"at_rest_{uuid4().hex}"
    await pool.execute(f'CREATE SCHEMA "{schema}"')
    try:
        await pool.execute(reference_ingestion_ddl(schema))
        transactor = PostgresTransactor(pool, schema=schema)
        yield lambda target, sealing: PostgresIngestionSink(
            transactor, target, schema=schema, sealing=sealing
        )
    finally:
        await pool.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


def target(at_rest: Literal["sealed", "plaintext"] = "sealed") -> IngestionTarget:
    return IngestionTarget(
        mode="upsert",
        generation="customers-v1",
        source_incarnation="cluster-a/orders-1",
        mapper_id="customer-v1",
        at_rest=at_rest,
    )


async def stored(sink: Sink, key: str) -> Any:
    """The row exactly as the sink stored it, nothing opened."""
    if isinstance(sink, MemoryIngestionSink):
        return _json.decode(sink._database.get().get(key))

    async def read(conn: PgConnection) -> Any:
        return (await sink._read(conn, sink._role, [key])).get(key)

    return await sink._typed(read)


async def original(
    sink: Sink,
    sealing: Sealing,
    offset: int,
    subject: str | None,
    *,
    row: str = "customer-42",
    sequence: int = 1,
) -> BatchInput:
    """A source record of ``subject``'s order, as the runner hands it to the sink."""
    content = {"id": f"event-{offset}", "aggregate_id": row, "sequence": sequence}
    fingerprint = None
    if subject is not None:
        key = await sealing.fingerprint_key("shop-a", subject)
        state = _json.encode({"content": content, "private": PRIVATE, "public": PUBLIC})
        fingerprint = _native.ingest_keyed_fingerprint(key, state)
    return sink.target.input(
        "orders",
        0,
        offset,
        "shop-a",
        row,
        row,
        f"event-{offset}",
        sequence,
        content,
        PRIVATE,
        _json.encode(content),
        public=PUBLIC,
        subject=subject,
        fingerprint=fingerprint,
    )


@dataclass(frozen=True)
class Case:
    name: str
    at_rest: Literal["sealed", "plaintext"] = "sealed"
    subject: str | None = "person-7"
    deleted: bool = False
    #: Whether the stored ``private`` is a protected-state envelope rather than the plaintext.
    sealed: bool = True
    #: Reservations the commit makes: one per subject, counting every row it seals.
    reserved: tuple[tuple[str | None, int], ...] = (("person-7", 2),)


CASES = [
    Case("sealed_subject_rows_share_one_reservation"),
    Case("row_without_subject_stays_plaintext", subject=None, sealed=False, reserved=()),
    Case("plaintext_target_keeps_private_readable", at_rest="plaintext", sealed=False, reserved=()),
    Case("deletion_marker_reserves_nothing", deleted=True, sealed=False, reserved=()),
]


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
async def test_private_at_rest(backend: Callable[..., Sink], case: Case) -> None:
    keys = CountingKeys()
    sealing = Sealing(keys, "customers")
    sink = backend(target(case.at_rest), sealing)
    first = await original(sink, sealing, 1, case.subject)
    second = await original(sink, sealing, 2, case.subject, sequence=2)
    if case.deleted:
        first, second = replace(first, private=None, public=None), replace(second, private=None, public=None)
    keys.reserved.clear()  # the fingerprint key is no reservation
    result = await sink.commit([first, second])
    assert result.outcomes == ("accepted", "accepted")
    assert tuple(keys.reserved) == case.reserved
    raw = await stored(sink, first.key)
    if case.deleted:
        assert raw["private"] is None
    elif case.sealed:
        envelope = _native.decode_protected_state(_json.encode(raw["private"])).value
        assert envelope is not None and (envelope.tenant, envelope.subject) == ("shop-a", case.subject)
        assert "ana@example.com" not in _json.encode(raw)
    else:
        assert raw["private"] == PRIVATE
    assert raw["public"] == (None if case.deleted else PUBLIC)
    row = await sink.get("shop-a", "customer-42", case.subject)
    assert row is not None and row["sequence"] == 2
    assert (row["private"], row["public"]) == ((None, None) if case.deleted else (PRIVATE, PUBLIC))


async def test_sealed_target_without_sealing_writes_nothing(backend: Callable[..., Sink]) -> None:
    sealing = Sealing(MemoryKeys(), "customers")
    sink = backend(target(), None)
    with pytest.raises(BadHandle, match="sealed"):
        await sink.commit([await original(sink, sealing, 1, "person-7")])
    assert await stored(sink, sink.target.row_key("shop-a", "customer-42", "person-7")) is None


async def test_destroyed_key_refuses_the_read(backend: Callable[..., Sink]) -> None:
    keys = MemoryKeys()
    sealing = Sealing(keys, "customers")
    sink = backend(target(), sealing)
    await sink.commit([await original(sink, sealing, 1, "person-7")])
    await keys.destroy(sealing.scope("shop-a", "person-7"))
    with pytest.raises(Shredded):
        await sink.get("shop-a", "customer-42", "person-7")


async def test_ciphertext_moved_to_another_row_fails_authentication() -> None:
    sealing = Sealing(MemoryKeys(), "customers")
    sink = MemoryIngestionSink(target(), sealing=sealing)
    ours = await original(sink, sealing, 1, "person-7")
    theirs = await original(sink, sealing, 2, "person-7", row="customer-43")
    await sink.commit([ours, theirs])
    moved = await stored(sink, ours.key)
    # The authenticated context binds the row key, so a copy under another key never opens.
    with pytest.raises(CodecError):
        await sealing.open_stored("ingestion", theirs.key, 1, moved["private"])

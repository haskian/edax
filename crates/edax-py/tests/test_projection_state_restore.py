"""Consistent offline PostgreSQL restore with old connections terminated first."""

from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from typing import Any
from uuid import uuid4

import pytest
from edax.backend.postgres import PostgresTransactor
from edax.backend.projection_postgres import PostgresProjectionStateStore, reference_projection_state_ddl
from edax.errors import StoreError
from edax.outbox import Relay
from edax.projection import EffectIntent, ProjectionInput
from test_projection_state import event, initial_cutover, proof, reducer, target


async def test_offline_restore_repeats_effect_and_checks_required_history() -> None:
    url = os.environ.get("EDAX_POSTGRES_URL")
    if url is None:
        if os.environ.get("EDAX_REQUIRE_POSTGRES") == "1":
            pytest.fail("EDAX_POSTGRES_URL required")
        pytest.skip("EDAX_POSTGRES_URL not set")
    import asyncpg

    admin = await asyncpg.connect(url)
    schema = f"projection_restore_{uuid4().hex}"
    destination_schema = f"projection_destination_{uuid4().hex}"
    old_pool = await asyncpg.create_pool(url, min_size=1, max_size=1)
    fresh_pool: Any = None
    await admin.execute(f'CREATE SCHEMA "{schema}"; CREATE SCHEMA "{destination_schema}"')
    try:
        await admin.execute(reference_projection_state_ddl(schema))
        await admin.execute(f'CREATE TABLE "{destination_schema}".effects (intent_id text PRIMARY KEY)')
        old = PostgresProjectionStateStore(
            PostgresTransactor(old_pool, schema=schema), target(), schema=schema
        )
        await old.initialize()
        await old.activate(initial_cutover(old.target))
        lease = await old.acquire(0, "before-backup", proof())
        lease = await old.commit(lease, [ProjectionInput(0, event())], proof(), reducer)
        await old.release(lease)
        first_intent = (await old.unpublished(100))[0]

        # This schema contains exactly the two projection tables. Export both from one consistent
        # snapshot: manifest, cursors, state and both receipts, and the undelivered intent.
        intents_sql = f'SELECT key, generation, source_partition, source_offset, ordinal, value::text FROM "{schema}".edax_projection_intents ORDER BY key'
        async with admin.transaction(isolation="repeatable_read", readonly=True):
            backup = await admin.fetch(
                f'SELECT key, value::text FROM "{schema}".edax_projection_state ORDER BY key'
            )
            backup_intents = await admin.fetch(intents_sql)
        assert len(backup) == 7  # manifest, two checkpoints, state, two receipts, delivery cursor
        assert any(row["key"] == old.target.manifest_key + ":delivery" for row in backup)
        assert len(backup_intents) == 1
        calls: list[str] = []

        async def destination(intent: EffectIntent) -> None:
            calls.append(intent.id)
            await admin.execute(
                f'INSERT INTO "{destination_schema}".effects VALUES ($1) ON CONFLICT DO NOTHING',
                intent.id,
            )

        # The destination and marker both commit after the backup. Restore must roll the
        # marker back without rolling the independent destination's deduplication back.
        assert await Relay(old, destination).drain() == 1
        assert not await old.unpublished(100)
        later = await old.acquire(0, "writer-after-backup", proof())
        later = await old.commit(later, [ProjectionInput(1, event(2, "credit-2"))], proof(), reducer)
        assert later.next_offset == 2
        saved = await old.get("bank-eu", "account-42")
        assert saved is not None
        assert saved["public"]["balance_minor"] == 2500

        # This pool is the only writer/dispatcher capability for this isolated schema.
        # Terminate its actual server session and close the pool before touching storage.
        connection = await old_pool.acquire()
        writer_pid = await connection.fetchval("SELECT pg_backend_pid()")
        assert await admin.fetchval("SELECT pg_terminate_backend($1)", writer_pid)
        for _ in range(100):
            if not await admin.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1)", writer_pid
            ):
                break
            await asyncio.sleep(0.01)
        assert not await admin.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1)", writer_pid
        )
        old_pool.terminate()  # no pool reconnects across the restore boundary
        with pytest.raises(StoreError, match="pool is closed"):
            await old.get("bank-eu", "account-42")

        # Restore the entire isolated storage schema, not a state-only snapshot.
        async with admin.transaction():
            await admin.execute(f'DROP SCHEMA "{schema}" CASCADE; CREATE SCHEMA "{schema}"')
            await admin.execute(reference_projection_state_ddl(schema))
            await admin.executemany(
                f'INSERT INTO "{schema}".edax_projection_state (key, value) VALUES ($1, $2::jsonb)',
                [(row["key"], row["value"]) for row in backup],
            )
            await admin.executemany(
                f'INSERT INTO "{schema}".edax_projection_intents VALUES ($1, $2, $3, $4, $5, $6::jsonb)',
                [tuple(row) for row in backup_intents],
            )
        restored = await admin.fetch(
            f'SELECT key, value::text FROM "{schema}".edax_projection_state ORDER BY key'
        )
        assert restored == backup
        assert await admin.fetch(intents_sql) == backup_intents
        fresh_pool = await asyncpg.create_pool(url, min_size=1, max_size=1)
        fresh = PostgresProjectionStateStore(
            PostgresTransactor(fresh_pool, schema=schema), target(), schema=schema
        )
        await fresh.initialize()
        before = await admin.fetch(
            f'SELECT key, value::text FROM "{schema}".edax_projection_state ORDER BY key'
        )
        for invalid in (proof(beginning=2), replace(proof(), identity="recreated-topic")):
            with pytest.raises(Exception, match=r"continuity is unproven|history is missing"):
                await fresh.acquire(0, "fresh-owner", invalid)
        assert (
            await admin.fetch(f'SELECT key, value::text FROM "{schema}".edax_projection_state ORDER BY key')
            == before
        )

        recovered = await fresh.acquire(0, "fresh-owner", proof())
        assert recovered.next_offset == 1
        assert recovered.epoch == later.epoch  # restore can repeat an old fencing epoch
        saved = await fresh.get("bank-eu", "account-42")
        assert saved is not None
        assert saved["public"]["balance_minor"] == 1250
        pending = await fresh.unpublished(100)
        assert len(pending) == 1 and pending[0].id == first_intent.id
        assert await Relay(fresh, destination).drain() == 1
        assert calls == [first_intent.id, first_intent.id]
        assert await admin.fetchval(f'SELECT count(*) FROM "{destination_schema}".effects') == 1

        # A replay at a new transport offset uses the restored logical receipts. Neither
        # state nor intents are duplicated, while the authoritative cursor advances.
        recovered = await fresh.commit(recovered, [ProjectionInput(1, event())], proof(), reducer)
        assert recovered.next_offset == 2
        saved = await fresh.get("bank-eu", "account-42")
        assert saved is not None
        assert saved["public"]["balance_minor"] == 1250
        assert not await fresh.unpublished(100)
    finally:
        old_pool.terminate()
        if fresh_pool is not None:
            await fresh_pool.close()
        await admin.execute(
            f'DROP SCHEMA IF EXISTS "{schema}" CASCADE; DROP SCHEMA IF EXISTS "{destination_schema}" CASCADE'
        )
        await admin.close()

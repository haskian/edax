"""Step 22: a keyed RecordPulse command stages ingestion in the command's transaction.

No broker or edax wire envelope is needed. The gateway authenticates tenant/subject and supplies
stable reading/request IDs. Set EDAX_POSTGRES_URL to run the transaction/rollback example. Without
it, only the direct memory path runs. This demo creates and drops its own isolated schema.
MemoryKeys is a process-local demo provider; use durable keys in a deployed service. Rejection raw
records remain plaintext: give their retention/access policy the same care as the source data.

Run: cd examples/py && uv run python 22_brokerless_ingestion.py
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import asdict, dataclass
from typing import ClassVar
from uuid import uuid4

from edax import CommandBus, PayloadMismatch, Tenancy, idempotent
from edax.backend.ingest_postgres import PostgresIngestionSink, reference_ingestion_ddl
from edax.backend.postgres import PostgresIdempotencyStore, PostgresTransactor, reference_ddl
from edax.command import current_command
from edax.errors import StoreError
from edax.ingestion import BatchInput, IngestionTarget, MappedRow, MemoryIngestionSink
from edax.sealing import MemoryKeys, Sealing
from edax.unit_of_work import current_tx


@dataclass(frozen=True)
class RecordPulse:
    reading_id: str
    device_id: str
    pulse_bpm: int
    type: ClassVar[str] = "RecordPulse"


def original(target: IngestionTarget, cmd: RecordPulse, tenant: str, subject: str) -> BatchInput:
    return target.input_from_source(
        f"{cmd.device_id}/{cmd.reading_id}",
        event_id=cmd.reading_id,
        stream=cmd.device_id,
        row=MappedRow(cmd.reading_id, private={"pulse_bpm": cmd.pulse_bpm}),
        content=asdict(cmd),
        raw=json.dumps(asdict(cmd), sort_keys=True),
        tenant=tenant,
        subject=subject,
    )


async def postgres(url: str, target: IngestionTarget, sealing: Sealing) -> None:
    import asyncpg

    pool = await asyncpg.create_pool(url, min_size=1, max_size=4)
    assert pool is not None
    schema = f"pulse_example_{uuid4().hex}"
    await pool.execute(f'CREATE SCHEMA "{schema}"')
    try:
        # In a service these belong to migrations, before the application starts.
        await pool.execute(reference_ddl(schema))
        await pool.execute(reference_ingestion_ddl(schema))
        await pool.execute(f'CREATE TABLE "{schema}".receipt_count (total integer NOT NULL)')
        await pool.execute(f'INSERT INTO "{schema}".receipt_count VALUES (0)')
        transactor = PostgresTransactor(pool, schema=schema)
        sink = PostgresIngestionSink(transactor, target, schema=schema, sealing=sealing)
        fail_next = False

        async def record(cmd: RecordPulse) -> str:
            nonlocal fail_next
            message = current_command()
            assert message is not None and message.tenant is not None and message.subject is not None
            result = await sink.stage([original(target, cmd, message.tenant, message.subject)])
            tx = current_tx(transactor)
            assert tx is not None
            await transactor.call(tx.connection.execute(f'UPDATE "{schema}".receipt_count SET total=total+1'))
            if fail_next:
                fail_next = False
                raise StoreError("demo: handler failed after staging")
            # The guard commits after this return. Only bus.dispatch's return is durable success.
            return result.outcomes[0]

        bus = CommandBus(
            idempotent(
                PostgresIdempotencyStore(transactor), transactor, scope="pulse-api", tenancy=Tenancy.POOL
            ),
            sealing=sealing,
        )
        bus.register(RecordPulse, record)

        async def send(cmd: RecordPulse) -> str:
            # These scopes come from the authenticated gateway, not unchecked device fields.
            return await bus.dispatch(
                cmd, idempotency_key=cmd.reading_id, tenant="clinic-a", subject="patient-42"
            )

        first = RecordPulse("reading-1", "device-42", 72)
        assert await send(first) == await send(first) == "accepted"
        assert await pool.fetchval(f'SELECT total FROM "{schema}".receipt_count') == 1
        try:
            await send(RecordPulse("reading-1", "device-42", 99))
        except PayloadMismatch:
            pass
        else:
            raise AssertionError("a reused request ID must bind the same payload")

        second = RecordPulse("reading-2", "device-42", 75)
        fail_next = True
        try:
            await send(second)
        except StoreError:
            pass
        else:
            raise AssertionError("the injected failure must roll back")
        assert await sink.get("clinic-a", "reading-2", "patient-42") is None
        assert await pool.fetchval(f'SELECT total FROM "{schema}".receipt_count') == 1
        assert await send(second) == "accepted"
        assert await pool.fetchval(f'SELECT total FROM "{schema}".receipt_count') == 2
        print("Postgres: keyed replay, payload conflict, shared rollback and successful retry verified")
    finally:
        await pool.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


async def main() -> None:
    target = IngestionTarget("append", "pulse-v1", "wearable-bridge-v1", "pulse-reading-v1")
    sealing = Sealing(MemoryKeys(), "clinic-keys")
    sink = MemoryIngestionSink(target, sealing=sealing)
    item = original(target, RecordPulse("reading-1", "device-42", 72), "clinic-a", "patient-42")
    assert (await sink.commit([item, item])).outcomes == ("accepted", "accepted")
    url = os.environ.get("EDAX_POSTGRES_URL")
    if url:
        await postgres(url, target, sealing)
    else:
        print("Set EDAX_POSTGRES_URL to also verify keyed transactional ingestion")
    print("ok: stable brokerless identities and sealed replay")


if __name__ == "__main__":
    asyncio.run(main())

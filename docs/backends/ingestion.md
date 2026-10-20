# Append/upsert ingestion

`KafkaIngestion` collects Kafka events into database batches. Append mode stores one row per event.
Upsert mode maintains the latest complete state for each mapped key. The mapper returns a
`MappedRow(key, private, public)`; it does not read the previous row. `private` holds what belongs
to the event's erasure subject, and `public` is stored as plaintext for queries.

The Python SDK provides `PostgresIngestionSink` and `MemoryIngestionSink` behind the atomic
`BatchSink` interface. Rust kernels own admission, recovery, identity, conflict resolution,
compaction and SQL. Go has internal bindings to those decisions but no ingestion runner or
database adapter. The [Python example](../../examples/py/20_ingestion.py) calls the memory sink;
the [Go example](../../crates/edax-go/examples/20_ingestion/main.go) exercises the kernel from
inside the repository. Its internal imports are unavailable to external Go modules.

This runner supports append and complete-state upsert. [Projection processing](../sdk/projection-processing.md) uses a separate
runner and database-authoritative checkpoints. Multi-stream row merges and raw Kafka tombstones
remain outside both contracts. See the [projection guide](../sdk/projections.md) to choose between
an application handler, append/upsert ingestion and projection processing.

## Direct ingestion without a broker

Use `IngestionTarget.input_from_source()` for a normalized application record. It needs no Kafka
coordinates and no edax event envelope. The gateway authenticates the tenant/subject, validates
its device payload and maps it to one complete `MappedRow`. Producers can retain their existing
wire format; normalization and sealing can happen in the service boundary.

```python
from edax.ingestion import IngestionTarget, MappedRow


target = IngestionTarget("append", "pulse-v1", "wearable-bridge-v1", "pulse-reading-v1")
original = target.input_from_source(
    "device-42/session-9/reading-1",
    event_id="reading-1",
    stream="device-42",
    row=MappedRow("reading-1", private={"pulse_bpm": 72}),
    content={"reading_id": "reading-1", "pulse_bpm": 72},
    raw='{"reading_id":"reading-1","pulse_bpm":72}',
    tenant="clinic-a",
    subject="patient-42",
)
```

| Identity | Meaning |
|---|---|
| `source_incarnation` + `source_id` | Stable original/rejection identity; preserve both across retries |
| `event_id` | Append row identity within generation, tenant and subject; `MappedRow.key` must match |
| `stream` + `sequence` | Permanent upsert owner and monotonic version; a timestamp is not an inferred sequence |
| command `idempotency_key` | Request/outcome replay, including other application writes in that transaction |

Opaque source keys have their own kernel tag. Existing Kafka source keys retain their exact format.
A source ID does not supply persistence or replay to a websocket. A producer/gateway must durably
buffer unacknowledged readings if losing a connection must not lose data. A retry preserves the
original ID and raw representation. The sink retains rejected-source decisions; it does **not**
keep a separate permanent receipt for every accepted source ID. Accepted replay follows append
identity or upsert owner/sequence. A keyed command can additionally bind a request ID to its result.

`await sink.commit([original])` is a durable boundary. With `sealing=`, direct sinks derive keyed
fingerprints before encryption and hold the provider's scope fences through the write. A destroyed
scope refuses a new direct write; the Kafka runner separately handles already-erased deliveries.
Inputs already marked as failures still persist or replay their durable rejection after erasure.
Without `sealing=`, plaintext targets have no provider fence: the application must coordinate
writers with cleanup. Raw rejection records retain their supplied representation independently of
row encryption. Encrypt/redact them at the gateway according to the application's replay and
retention requirements; never assume sealing `private` protects `raw` or `public`.

### Join a keyed command transaction

`PostgresIngestionSink.stage(inputs, tx=None)` requires an already-open transaction of its
`PostgresTransactor`. Omitting `tx` joins that transactor's ambient unit of work. A missing, foreign,
closed, aborted or other-task handle is refused, as is an explicit handle different from the ambient
transaction. `StagedBatchResult` is provisional: acknowledge the
source or return success only after the outer transaction commits. Never publish its rejections
before that commit. `pending()` remains a committed-only outbox read.

Use the same transactor for `PostgresIdempotencyStore`, the ingestion sink and other application
writes. A keyed command handler calls `stage()` inside the idempotency middleware's transaction;
`CommandBus.dispatch()` returns only after it commits. A later handler failure rolls back accepted
rows, rejections and other writes together. A staging failure after handle admission aborts that transaction
even if the handler catches the exception. Retry the whole command with the same key and payload.

For read-own-writes, call `sink.get(..., tx=tx)` explicitly. With no `tx`, `get()` keeps its existing
committed-read boundary and refuses an ambient transaction. Guard-based key providers require
`transaction(transactor)` so their guards survive until outer commit/rollback. An explicit handle
from `transactor.begin()` is also usable when a provider fences inside that same transaction.
If acquiring a scope guard manually, enter it before `transaction(transactor)` so it covers commit.
The sink reuses guards held by enclosing dispatches and transactions; providers need not support
reentrant acquisition of the same scope.

[Example 22](../../examples/py/22_brokerless_ingestion.py) runs a keyed `RecordPulse`: sealed direct
replay, changed-payload refusal, rollback after staging, and retry with a shared application counter.
Set `EDAX_POSTGRES_URL` to run the transaction portion. Memory ingestion supports `commit()` only.

### Implement an application-owned sink

`edax.ingestion` exposes `prepare_ingestion`, `plan_ingestion`, `IngestionPlan`, `IngestionRow` and
`IngestionRejection`. Custom sinks reuse the kernel's decisions while owning their schema and I/O:

1. Validate transaction ownership before calling providers. Hold `prepare_ingestion(target, sealing,
   inputs)` through the write's commit/rollback. Staged implementations supply `transactor` and `tx`
   to retain scope fences through the outer transaction; abort the transaction on a staging failure.
2. Serialize the first manifest bind. Call `target.check_binding(previous)` and persist
   `target.manifest` atomically on first use. Keep it after payload retention and erasure cleanup.
3. Lock/read metadata for every input row in a deterministic key order. Then read durable rejection
   records for every source in a fresh statement. Protect absent keys with uniqueness or equivalent
   serialization. A race must roll back and retry the whole transaction.
4. Call `plan_ingestion(target, prepared, stored, rejected)`. Replace every `plan.writes` row in full,
   retaining stream, sequence and fingerprint even for deletion markers. Insert only **absent**
   rejection keys; replay must preserve the first raw record and its publication flag. Apply both
   sets atomically. `plan.json` preserves exact numeric tokens for database encoders; decoded maps
   use host JSON number semantics.
5. Return `BatchResult` only after confirmed commit. Expose committed pending rejections and mark
   publication atomically against each source/fingerprint. Provide scope cleanup for plaintext
   payloads and coordinate it with writer revocation. `open_private` opens stored private parts.

Subclass `edax.testing.BatchSinkContract` with a fresh `ingestion_sink` fixture for both modes.
Transactional sinks use `TransactionalBatchSinkContract` and also provide `transactor`. These suites
check replay, conflicts, concurrent writers, full-state ordering, rejection publication and staged
rollback/isolation. Add storage/provider-specific tests for commit uncertainty, cancellation and
erasure races. Passing the kit is evidence for its clauses, not proof of every database failure mode.

The reference Postgres sink still uses key/value tables and key lookup. An application can add
allowed generated columns over `public` and query them itself.
[Partitioned append ingestion](partitioned-ingestion.md) adds opt-in UUIDv7 placement and an
explicit admission floor. Other custom partition layouts need a global identity/ownership mechanism: deleting a payload must not silently
delete its replay protection. Retention is an application policy. Choose a replay horizon and keep
receipts/tombstones for it; retaining only payload partitions cannot preserve permanent deduplication.
SQL cannot filter or aggregate sealed `private` values. Range queries, indexes, retention jobs and
one-reading-to-many-measurement expansion remain application responsibilities.

## Wire the pipeline

Install `edax[kafka,postgres]`. The application owns its `asyncpg` pool and Kafka clients. Create the
schema and ingestion tables through the application's migration tool before starting the runner:

```python
from edax.backend.ingest_postgres import reference_ingestion_ddl

migration_sql = reference_ingestion_ddl(schema="billing")
```

Copy that SQL into the migration after creating `billing`. The three optional tables default to
`edax_ingest_events`, `edax_ingest_state` and `edax_ingest_rejections`. Pass a `tables` mapping with
roles `ingest_events`, `ingest_state` and `ingest_rejections` to both `reference_ingestion_ddl` and the sink to rename them. Existing Postgres
stores need no ingestion-table migration unless they use this feature. The sink checks the catalog;
it rejects incompatible key/value columns, missing usable uniqueness, required extra columns and
row-level security. It never creates tables.

The reference DDL includes a partial index containing only unpublished rejections. Keep published
rejection rows: replay needs their decisions. The partial index lets pending pages skip that
retained history.

A commit takes no advisory lock. Its plan reads lock the rows they find, in key order, so a
concurrent writer of the same key waits. It reads target metadata first, then durable rejections in
a fresh statement, so a writer's rejection remains visible even when that same writer advances the
target version. The generation's immutable mapper keeps each source's target key stable.
It inserts keys it did not find without `ON CONFLICT`. Two
batches racing to create one key therefore cannot both win: the loser rolls back and runs again,
and its next read finds the committed row. A batch's published rejections are marked in one
statement. A record whose mapped value or identity holds a NUL character becomes a durable
rejection, because Postgres cannot store it.

This sample maps subjectless order data to `public`. Subject-bearing input also requires a
configured `Sealing` provider on the runner or sink, including when `at_rest="plaintext"`.
The provider supplies revocation fences; the default `at_rest="sealed"` additionally encrypts private data.

```python
import asyncio
import json
from typing import Any

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from edax.backend.ingest_postgres import PostgresIngestionSink
from edax.backend.kafka import KafkaDeadLetterSink
from edax.backend.kafka_ingest import KafkaIngestion
from edax.backend.postgres import PostgresTransactor
from edax.batch import BatchSettings
from edax.ingestion import IngestionTarget, MappedRow
from edax.message import EventMessage


def order_state(event: EventMessage) -> MappedRow:
    # Every event carries the complete current order state, including a deletion marker.
    state: dict[str, Any] = json.loads(event.data)
    return MappedRow(event.aggregate_id, private=None, public=None if state.get("deleted") else state)


async def ingest_orders(pool: asyncpg.Pool, stop: asyncio.Event) -> None:
    transactor = PostgresTransactor(pool, schema="billing")
    target = IngestionTarget(
        mode="upsert",
        generation="orders-summary-v1",
        source_incarnation="production-cluster/orders-topic-incarnation-1",
        mapper_id="order-state-v1",
    )
    sink = PostgresIngestionSink(transactor, target, schema="billing")
    client = AIOKafkaConsumer(
        bootstrap_servers="localhost:9092",
        group_id="orders-summary-v1",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092", enable_idempotence=True)
    runner = KafkaIngestion(
        client,
        sink,
        order_state,
        on_dead=KafkaDeadLetterSink(producer, "orders.dlq"),
        settings=BatchSettings(),
    )
    client.subscribe(["orders"], listener=runner.rebalance_listener())
    await producer.start()
    try:
        await client.start()
        await runner.run(stop)
    finally:
        try:
            await client.stop()
        finally:
            try:
                await runner.wait_closed()
            finally:
                await producer.stop()
    # The caller closes the pool after every sink operation has actually finished.
```

Use one `PostgresTransactor` per pool, shared with any other Postgres stores. `commit()` refuses an
ambient unit of work: its successful return means its own transaction has committed. Direct handlers
can explicitly use `stage()` to join an open transaction, as described below. The mapper
is synchronous, must return `MappedRow`, and must be deterministic for the target's `mapper_id`.
The default decoder accepts edax JSON event envelopes. Serializer and record middleware options
follow the [Kafka backend](kafka.md).

Install the runner's listener before starting the subscription. Disable consumer auto-commit and
supply a group ID. `on_dead` is required and must return only after durable publication. A runner is
single-use. On failure or cancellation, stop its caller-owned consumer and await `wait_closed()`
before replacing it or closing resources that unfinished work still uses. Keep the producer and
pool alive until that wait completes. Cancelling `wait_closed()` leaves those operations tracked;
call it again to wait for their completion. `run(stop)` drains admitted work on a normal stop.

## Sealed events

Pass `sealing=` to `KafkaIngestion` to read sealed events. The runner opens each event before the
mapper sees it, and reads each subject's keys once per batch. Keys are dropped with the batch.

| Key state | What the runner does |
|---|---|
| Active | Maps the opened event; the row's fingerprint is an HMAC under the subject's fingerprint key |
| Revoking or unavailable | Waits without spending attempts |
| Missing or unauthorized | Stops the runner |
| Destroyed | Accepts the record without a write; the offset still commits |

A sealed record's mapper failure keeps only its error code, so a rejection or dead letter cannot
quote its plaintext. Each commit runs under the write fence of every subject it writes. A subject
erased between mapping and the commit makes the fence refuse; the runner waits, then commits those
records erased.

### The private part at rest

`MappedRow(key, private=..., public=...)` splits a row in two. `public` is stored as queryable
plaintext. Where `private` goes depends on the target's `at_rest`:

| `at_rest` | A subject's `private` part | Erasure |
|---|---|---|
| `"sealed"` (default) | Sealed under the subject's key, in the protected-state envelope | Destroying the key erases it, backups included |
| `"plaintext"` | Stored readable | The sink's cleanup participant deletes the subject's rows |

Give the sink `sealing=`; a runner built without its own uses the sink's. The sink seals what the
kernel's seal plan names before its transaction, with one key reservation per subject per batch. A
runner with sealing on neither sink nor runner refuses a subject's rows with `BadHandle`: without it no
write fence holds them against an erasure cleanup. `get` returns the row with `private` opened, and raises
`Shredded` once the subject's key is destroyed. A sealed target whose sink has no `sealing=`
refuses a subject's row with `BadHandle` and writes nothing. A row with no subject stays plaintext.
`at_rest` is part of the target contract, so changing it needs a new generation.

## State and replay contract

An `IngestionTarget` fixes `mode`, `generation`, `source_incarnation`, `mapper_id` and `at_rest`. The durable
manifest lives beside the physical target rows and in the shared rejection table.
Changing mode or target tables cannot reuse rejections from a different contract. The sink binds
both manifests atomically under a lock before any read, write or publication operation, and caches
that binding only after a confirmed commit.
Change the generation and rebuild when source
identity, mapping or keying changes. The application supplies and verifies the source incarnation;
a reused topic name alone is not an identity check.

Unkeyed replay checks use the shared 64-bit FNV-1a `payload_hash`. Equality is hash equality,
not a collision-resistant proof of identical content. Use the sealed/keyed fingerprint path for
untrusted producers. Its HMAC-SHA-256 fingerprint protects replay identity. Changing the unkeyed
algorithm requires an explicit persisted-state migration; it is not changed implicitly.

| Mode | Identity and accepted write | Replay/conflict behavior |
| --- | --- | --- |
| `append` | Target generation, tenant, erasure subject and stable event ID. The mapper's row key is ignored. | Exact replay is accepted without rewriting. Different content under that identity becomes a durable rejection. |
| `upsert` | Target generation, tenant, erasure subject and mapped row key; one aggregate stream permanently owns it. Each original needs a sequence in `0..=i64::MAX`. | A higher sequence replaces complete state. A lower sequence is ignored. An equal sequence preserves the first committed value: identical content is accepted; different content is rejected. |

The accepted final upsert state is independent of batch cuts, but rejection outcomes for conflicting
intermediate sequences can depend on where a batch ends. The store retains only the current sequence's
fingerprint; older replays are accepted as stale. Producers must keep one aggregate's erasure subject
stable: ingestion includes it in the row identity and does not maintain a cross-subject owner index.
Replays of a sealed event must reuse its original ciphertext. Re-sealing the same event ID is a content
change and requires a deliberate migration, not ordinary redelivery.

The erasure subject comes from the event envelope, never from the mapper. It sits in the row key
before the row identity, so one key range holds every row of a subject. An event with no subject
uses `null` there.

A row whose `private` and `public` are both `None` is a retained deletion marker. It retains the stream owner and sequence, so an old
update cannot resurrect the row and another stream cannot take its key. Keep those markers; removing
them weakens replay protection. There is no offset or timestamp fallback for a missing sequence.

The kernel checks every original before compaction, excludes persisted rejections before ownership
selection, and emits only the greatest accepted sequence per upsert key. Within a batch the first
eligible original proposes an absent key's owner, and the first value seen at a newer sequence wins
against conflicting duplicates. Lower sequences are compared with committed current state. Historical
fingerprints are not retained: once a higher version commits, older conflicting versions are stale
and ignored. Therefore permanent historical conflict reporting is not part of this contract.

Accepted rows and new rejection records commit together. A committed rejection wins on replay even
if target state later changes. Every input carries `raw_fingerprint`, the SHA-256 of its original
source record, so a full record and its bounded stub replay as one source. Reusing a source
coordinate with another fingerprint fails closed. Checks from a transaction that rolled back are not
durable rejection records.

Use `target.input(...)` to build inputs for a custom source or direct sink calls. It builds and
validates kernel-owned scoped identities. Pass `raw_fingerprint` when `raw` may be a stub; otherwise
the kernel fingerprints `raw` itself. Source offsets are decimal strings inside canonical key
JSON, preserving the full nonnegative signed 64-bit range. The Kafka runner retains the native full
event envelope as JSON **text** in `content`; it does not parse that envelope into an arbitrary JSON
value for fingerprinting. This preserves integer nanosecond timestamps and 64-bit sequences without
crossing the canonical JSON safe-integer limit. Mapped host values still follow the ordinary
[host-value encoding contract](../kernels/filter.md#host-values-every-sdk-writes-alike).
The kernel checks original numeric tokens before parsing payloads, so an oversized integer
cannot round into another record's identity. Unsafe integer payloads become durable rejections;
represent exact large application numbers as strings.

## Durable rejection delivery

A rejected original is acknowledged only after its transaction commits and publication is confirmed.
A failed publication leaves its outbox record pending. A crash between publication and marking can
publish it again; destinations must deduplicate by immutable source identity. Publication is at least
once, even when a Kafka producer uses idempotence. Published rejection records remain stored so replay
continues to observe the original decision.

The runner appends `edax-ingest-source`, `edax-ingest-sha256` and `edax-ingest-oversized` headers
before calling `on_dead`. `KafkaDeadLetterSink` preserves them, including when it sends a stub.
Deduplicate on the last `edax-ingest-source` header: it contains the complete target generation,
source incarnation and source coordinate. Topic, partition and offset alone can repeat after a
source is replaced. The digest identifies the complete original bytes; `edax-ingest-oversized`
is `true` when ingestion omitted those bytes. Read the last occurrence of each header so a
same-named original header cannot override the runner's provenance.

`RejectionOutbox(sink)` presents the sink's durable rejections as an outbox, so the shared `Relay`
drains them. `runner.publish_rejection` sends one to the runner's `on_dead` callback, exactly as the
runner does before it acknowledges a batch. The relay can drain independently of source polling,
including after the source consumer has stopped:

```python
from edax.ingestion import RejectionOutbox
from edax.outbox import Relay

# Keep the sink's pool and the dead-letter producer open. No source fetch is needed.
relay = Relay(RejectionOutbox(sink), runner.publish_rejection)
while await relay.drain(100):
    pass
```

The adapter's acknowledgement contract is stricter than the general `Outbox` contract. Malformed
ids are ignored, but a decoded source/fingerprint pair raises `BadHandle` if its source is missing
or its fingerprint differs from the stored rejection. Only mark ids returned by this adapter.
This preserves the sink's publication checks; it does not promise unknown-id no-ops.

A separate recovery process can reopen the same sink and target and pass its own `publish` callback.
It receives a `Rejection` with the durable source identity, the kernel error and the raw record. It
must preserve the destination's chosen wire format and wait for durable delivery. A callback failure
leaves the row pending. `Relay` adds its usual retry pacing, `on_dead` and observer hooks.
Normal-sized records are retained in the outbox, so recovery does not need the source record to remain
in Kafka.

The raw limit includes key, value and headers. Oversized values or headers produce a bounded provenance
stub instead of retaining their bytes. The durable stub records topic, partition, offset, timestamps
and a SHA-256 digest over framed original bytes and source metadata. Full records carry the same
digest, so changing the size limit does not change replay identity. It does not preserve
the oversized payload. On a replay verified by that digest, inline publication uses the current
record representation, which may now be a bounded stub. The durable outbox retains its first raw
representation; an independent publisher still reads that original. The rejection's error and
publication status remain unchanged. Recovering bytes absent from the original durable rejection
requires the source or another archive; the
outbox remains sufficient to publish the diagnostic rejection. `KafkaDeadLetterSink` can also fall
back to its coordinate-bearing stub when the destination refuses a large dead letter.

## Limits, scheduling and failure boundaries

| Setting | Default |
| --- | ---: |
| `max_records` | 500 originals per batch |
| `max_bytes` | 1 MiB prepared-input flush threshold |
| `flush_ms` | 50 ms |
| `concurrency` | 4 sink operations |
| `retained_records` | 2,000 originals |
| `retained_bytes` | 16 MiB |
| `max_record_bytes` | 1 MiB |
| `max_mapped_bytes` | 1 MiB, including native envelope and mapped row |
| `max_attempts` | 5 |

Batch age, record count and measured bytes trigger a flush. Limits must be positive;
`max_record_bytes <= max_bytes` and `retained_records > max_records`. Before fetching and invoking a
mapper the runner reserves `6 * (max_record_bytes + max_mapped_bytes)` bytes for source, mapping and
serialization expansion. Thus the default raw/mapped limits require at least **12 MiB** of retained
capacity; 16 MiB is the default. Admitted originals are charged for three serialized prepared-input
copies, including space for the kernel's bounded rejection diagnostic. Driver error text is not copied
into a rejection because it can contain an arbitrarily large input. A record that exceeds its reserved
expansion becomes a bounded durable rejection without exceeding admission credits.

These credits bound retained prepared-input representations, not process resident memory. Broker
prefetch, Python object overhead, sink buffers and allocations inside arbitrary mapper/middleware
callbacks are excluded. The Postgres planner reads prior state metadata without its payload and uses
the current fingerprint-verified raw representation on replay. Stored error and ownership metadata,
and the independent outbox publisher's pages, are not byte-limited by runner admission. Keep
callbacks bounded and cooperative. `retained_records`, `retained_bytes`, `peak_records` and
`peak_bytes` expose the runner's accounting. Credits and operation slots remain held until unfinished
operations actually release their data, even if those operations suppress cancellation.

The coordinator fetches **one original** with `getmany(..., max_records=1)` from each ready partition
in round-robin order, then accumulates database batches. This improves database batching; it makes no
claim of fewer Kafka fetch calls. Each partition has one ordered recovery chain. Sink slots are
released during retry backoff. Reserved admission headroom lets healthy partitions make progress;
when retained retries block admission, the runner can evict a backing-off chain. It rewinds from the
first still-live original, retires its generation, and retains attempt counts and the retry-ready
deadline. Eviction cannot turn backoff into an immediate retry or replay an already-settled prefix
as the new completion frontier.

Transient middleware and mapper failures rewind the open batch and release its data.
Their attempt counts and backoff deadlines survive replay. Other partitions remain eligible while
that partition waits; a transient preparation failure never acknowledges or rejects an original.
A terminal data failure becomes a bounded rejection keyed by source coordinates, even when its
producer-supplied identity cannot form a valid target key.
Serializers must be pure decoders: their exceptions classify malformed input as poison, as in the
ordinary Kafka consumer. Put transient external lookups in record middleware.

Only a deterministic database error with a known rollback permits bisection. Children preserve original
order and run left first; an isolated invalid original becomes a durable rejection. Authentication
or schema failures stop. Infrastructure failures and uncertain commits retry unchanged originals,
then stop at the attempt ceiling without advancing or turning healthy records into dead letters.
A lost COMMIT response is uncertain even if a subsequent rollback command succeeds.

Fetches and offset-commit completions carry assignment generations. Revocation retires tokens before
waiting for commit serialization. Late work cannot settle a new assignment. An unresolved operation
at a deadline stops admission; cancellation does not prove rollback or release. The defaults are
30 seconds per runner operation, 10 seconds per offset commit and 30 seconds per drain. The Postgres
transactor also applies its database and connection deadlines. An uncooperative operation can make
`wait_closed()` wait beyond those deadlines; terminate the underlying client operation before
replacing resources, rather than treating a deadline as evidence that the old work ended.

# Time-partitioned append ingestion

The Python sink and native memory reference implement this contract.

```python
target = IngestionTarget(
    "append",
    generation="pulse-v1",
    source_incarnation="sensor-v1",
    mapper_id="pulse-v1",
    at_rest="plaintext",
    partition="event_id_uuid7",
)
# Apply reference_ingestion_ddl(schema="pulse", partitioned=True) in a migration.
# Create RANGE partitions before starting ingestion.
sink = PostgresIngestionSink(transactor, target, schema="pulse")
# Retention job: await sink.advance_horizon(cutoff_unix_microseconds),
# then DROP whole partitions ending at or before the confirmed floor.
```

Existing unpartitioned tables need an application migration and a new target generation.
There is no in-place layout conversion. Keep the old sink and tables through cutover.
For rollback, stop partitioned writers and retain the control row and its floor; never
reset the floor or route old deliveries into a new empty authority.

Opt in with `IngestionTarget(partition="event_id_uuid7", mode="append", ...)`.
Ordinary ingestion keeps its existing layout and behavior. Upsert and projection state
are outside this contract. The host creates and drops partitions through migrations.

## Identity and admission

The kernel derives `partition_at` from the UUIDv7 event identity, exactly to the millisecond.
It never trusts a mapper-selected timestamp. The primary key is `(key, partition_at)`.
Every occurrence of a key must have the same exact timestamp, even within one partition.
After target/key validation, malformed UUIDs become durable per-record rejections
before sealing-provider work. Input construction retains its existing identity checks.

Each physical events table has one monotonic admission floor, in Unix microseconds.
All generations inherit it. A valid, non-erased input below the floor returns `expired`
and writes nothing. Source acknowledgement follows the transaction commit. An existing durable
rejection still wins and requires publication. Expiry is an explicit discard policy;
it is not proof that the source delivered all older events. It does not reveal whether
a previous attempt committed before its response was lost. Sources must retain IDs
across retries. Sealing and erasure prerequisites still apply to valid expired inputs.

## Retention protocol

A control table in the events schema owns the floor and the events/rejections pairing.
It defaults to `edax_ingest_partition_control`; the `ingest_partition_control` role renames it.
Every writer of one events table must name the same control table. Migrations provision the
control row at zero. A missing row fails closed. Renaming the control table is a plain
`ALTER TABLE ... RENAME`. The control row's key is the schema-qualified events table
(`"pulse"."readings"`), and each manifest's key starts with it (`"pulse"."readings":manifest:`).
The row's `rejections` value holds the qualified rejections table. Renaming the events table
or the schema rewrites those keys, and renaming the rejections table rewrites that value, in
the same migration.
Changing the rejections table cannot establish another authority for the same parent.
Manifests remain immutable per generation. Unpartitioned manifests do not move.

Every write transaction first takes one shared transaction advisory gate for the events
table. It then reads the control row `FOR SHARE` in a separate statement. Both locks
survive until the outer transaction ends, including staged writes. Cached binding never
replaces this read. Locks scale with tables and transactions, not individual records.

`advance_horizon(to)` runs in its own transaction. It refuses an ambient transaction,
takes the exclusive gate, locks the control row, and stores `max(current, to)`.
It returns only after confirmed commit. Resolve an uncertain commit by retrying before
performing retention DDL. A long-running writer can delay advancement.

After advancement commits, the host may **drop** complete partitions whose exclusive
upper bound is at or below the returned floor. Disable independent automatic drops.
Detached or archived tables escape parent-table erasure cleanup and are unsupported
unless the application supplies a separate tracked cleanup participant. Rejection rows
are not pruned in this version. Their raw representation needs the existing source
redaction or encryption policy; sealing payload rows does not seal rejection raw data.

## SQL and failure behavior

The catalog check requires a RANGE parent on `partition_at`, a valid unique btree on
`(key COLLATE "C", partition_at)`, and the ordinary column and row-security rules.
Point reads and batch writes include exact partition coordinates. Dense batch reads
also include explicit time bounds. Timestamp conversion uses integer day and sub-day
components, including the full UUIDv7 timestamp range. No host datetime or float epoch
conversion participates in identity.

A missing destination partition is a retryable infrastructure failure. Ordinary named
CHECK failures retain their current classification. The host must create future
partitions before use. A DEFAULT partition can conceal maintenance failures.

Statement count is bounded per batch. Work is not constant: sparse batches can span
many partitions, and generic prepared plans can lock partitions they do not scan.
Retention should bound attached partition count. Measure actual driver execution plans.

## Evidence and limits

The policy follows *Streaming Systems*, “When: Allowed Lateness (i.e., Garbage
Collection),” pp. 72–76: bounded admission permits state reclamation. Its watermark
discussion, pp. 66–69, distinguishes completeness evidence from time assumptions.
*Building Event-Driven Microservices*, second edition, “Handling Late Events,”
pp. 240–241, makes discard a business choice. Its “Effectively Once Processing Without
Client-Broker Transactions,” pp. 217–222, explains identity retention requirements.
These sources justify the policy boundaries, not this particular schema.

*PostgreSQL 14 Internals*, “Row-Level Locks: Wait Queue,” pp. 255–257, describes
compatible lockers bypassing an exclusive row-lock waiter. Two independent PostgreSQL
17.11 runs reproduced it. A transaction advisory gate queued new readers behind the
waiting advance; cancellation released the queue. Existing holders may reacquire, so
this is not a universal fairness guarantee. A separate locked floor read either sees
the new floor or fails a stale repeatable-read snapshot.

The [PostgreSQL advisory lock documentation](https://www.postgresql.org/docs/17/explicit-locking.html#ADVISORY-LOCKS)
specifies transaction lifetime and shared lock-memory limits. Local experiments also
found JSON tuple membership alone scans all attached partitions. The shipped query's bounds
reduce a dense batch to one scanned partition. Testing through asyncpg with 52 and 260
attached partitions still locked every partition under both custom and generic plans.
The bounds come from initialization subplans, so pruning happens after relation locking.
Scalar bound parameters can improve custom planning; they do not remove generic-plan
locking. These are plan observations, not a production throughput claim.

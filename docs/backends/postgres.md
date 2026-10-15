# The Postgres backend

Every durable seam on one Postgres database, over an `asyncpg` pool you own. That covers the event
store with its snapshots and outbox, and the command outbox. It covers read-model projections, with the
kernel's query pushed down to SQL. It also covers the idempotency ledger and the saga store.

Install with `pip install edax[postgres]` (asyncpg 0.30 or later). edax creates no table: your
migration tool owns the schema, and edax checks it against a table contract. The `edax-postgres`
kernel owns every other decision: the SQL, the row layout, the query compiler, the error verdict and
the ledger fence. [The Postgres kernel](../kernels/postgres.md) describes each one. This page covers the adapter
and how to run it.

## Wiring

One `PostgresTransactor` per pool, and every store built over it, so their writes under one `tx`
commit or roll back together.

```python
import asyncpg
from edax import transaction
from edax.backend.postgres import (
    PostgresCommandOutbox,
    PostgresEventStore,
    PostgresIdempotencyStore,
    PostgresProjectionStore,
    PostgresSagaStore,
    PostgresTransactor,
    default_settings,
)

pool = await asyncpg.create_pool(DSN, min_size=2, max_size=20)
db = PostgresTransactor(
    pool,
    schema="billing",
    tables={"events": "billing_events"},  # any role you leave out is named edax_<role>
    settings=default_settings(lock_timeout_ms=2_000),
)

events = PostgresEventStore(db)
commands = PostgresCommandOutbox(db)
accounts = PostgresProjectionStore(db, "accounts")
ledger = PostgresIdempotencyStore(db)
sagas = PostgresSagaStore(db)
await db.check()  # fail fast at startup: every table these stores use meets the contract

async with transaction(db) as tx:
    ...  # every store above joins `tx`, or the ambient unit of work when given none
```

A second transactor over the same pool raises `BadHandle`. Two would be two units of work over one
pool. A store of one would then auto-commit on a second connection inside the other's unit of work,
and could wait on its own row lock. A store built over the pool itself raises `TypeError`.

A store given no `tx` inside `transaction(db)` joins it. Outside any unit of work it runs its call in a
short transaction of its own. So a call that reads a row and then writes it, such as a ledger
`complete`, always holds the row lock for both.

## Schema: yours, held to a contract

edax provisions nothing. Create the tables with your own migration tool (Flyway, Alembic, sqitch,
Liquibase), in the schema and under the names you choose. edax needs no function, no extension and no
version table, so a managed Postgres (RDS, Cloud SQL, Supabase, Neon) works. The server must be
Postgres 14 or later, with a `UTF8` database encoding.

**Start from the reference schema.** [`postgres-schema.sql`](./postgres-schema.sql) is one schema that
meets the contract, for the default names. `reference_ddl(schema, tables)` writes it for yours. Copy
it into a migration and adapt it: add indexes, partitioning, foreign keys and storage settings as you
need. edax never runs it.

**The contract.** [The table contract](../kernels/postgres.md#the-table-contract) lists, per table,
what correctness rests on.

- **Columns:** every column edax reads or writes, with an accepted type. A column edax writes `NULL`
  into is nullable. `position` has a default, an identity or a sequence.
- **Unique keys:** a unique index over exactly each key's columns. It is not partial, not over an
  expression, not deferrable.
- **Collation:** every text key column, and its unique index, collates as `"C"`.
- **Yours:** extra columns that are nullable or defaulted, extra indexes, constraint names,
  partitioning, storage settings.

**The check.** Before its first statement on a table, a store has the transactor hold that table to
the contract. Every mismatch comes back in one `BadHandle`, a wiring mistake no retry fixes, for
example "`billing.edax_ledger.record` is json, needs text or character varying". Call `await
db.check()` at startup to fail fast. The check reads the catalog only, so the service's role needs no
DDL rights.

**Upgrades.** The contract has a version, `CONTRACT_VERSION`, and changes only by an addition. A release
that needs a new column or table says so in its notes. Migrate first, then roll out the release. Old
replicas ignore the addition.

**Suggestions, not requirements.** Tune these to your workload.

| Suggestion | Why |
|---|---|
| `fillfactor` 80–90 on `projections`, `saga_instances`, `ledger` and `relay_leases` | their updates change no indexed column, so a page with room keeps them heap-only (HOT): no new index entries, less vacuum |
| aggressive autovacuum on the outbox tables, for example `autovacuum_vacuum_scale_factor = 0.01` | each mark deletes rows, and dead rows slow the next poll |
| an index on `event_outbox (event_id)`, and a foreign key to `events (id)` | `mark_published` deletes by event id; the key catches a stray row |
| expression or GIN indexes on the projection paths you filter and sort on | see [projections and queries](#projections-and-queries) |
| list partitioning of `projections` by `projection` | one table per read model, dropped or rebuilt on its own |
| retention jobs on `ledger.settled_at` and `saga_instances.completed_at` | edax never purges; keep records past your longest redelivery window |

## Isolation and timeouts

Every transaction runs at `READ COMMITTED`, whatever the server's default. Each compare-and-set
here is written for it: under that level Postgres re-checks an `UPDATE`'s `WHERE` on the newest row
after a lock wait.

| Operation | Why `READ COMMITTED` |
|---|---|
| `append` | the unique `(stream, sequence)` index catches every race; `SERIALIZABLE` would turn the conflict into `40001` |
| `load`, `load_snapshot` | events never change once written |
| `save_snapshot` | sees its own transaction's appends; the upsert is re-checked after a lock wait |
| projection `get`, `save` | `get` must return the committed row; `save` re-checks `version` after a wait |
| `get_many` | each page reads committed rows, as the memory store does |
| ledger `claim` | `ON CONFLICT DO NOTHING` waits, then the next statement sees the holder's record |
| ledger `complete`, `release`, `forget` | the row lock covers the fence and the write |
| saga `load`, `save`, `complete` | `load` locks the instance inside a unit of work; the write re-checks `version` |
| outbox reads | committed rows only, without the predicate locks `SERIALIZABLE` takes |

`default_settings()` holds the kernel's timeouts. `begin()` sets the lock, statement and idle
timeouts with `SET LOCAL`, so they hold for that transaction only and survive a transaction-mode
pooler. The client gives up on a call only after the server would have, so a timeout reports what
ran out.

The kernel renders the `BEGIN` and the three `SET LOCAL` into one text, which the transactor sends
as one simple query. A store call outside a unit of work therefore costs three round trips: that
text, its statement, and `COMMIT`. A transaction that fails to open is rolled back before its
connection returns to the pool.

| Setting | Default | What it bounds |
|---|---|---|
| `lock_timeout_ms` | 2 s | a wait on another unit of work's row |
| `statement_timeout_ms` | 5 s | one statement |
| `op_timeout_ms` | 10 s | one call, client side |
| `idle_in_transaction_ms` | 60 s | a unit of work idle between statements, so a frozen worker cannot hold rows forever |
| `acquire_timeout_ms` | 5 s | the wait for a pooled connection |
| `claim_ttl_ms` | 5 min | a committed pending ledger claim, before another claimant takes it over |

`lock_timeout_ms` must exceed the server's `deadlock_timeout`. The startup check refuses an equal
or shorter value with `BadHandle` and names both values. Otherwise a deadlock could become a claim
timeout before Postgres detects it. The dispatcher retries a claim timeout without counting an
attempt, so that mistake could hide a recurring deadlock.

## Errors and the unit of work

The kernel types every failure. A version conflict is `ConcurrencyError`. A reused id, or a value
the server cannot store, is `CodecError`. A missing table or a refused privilege is `BadHandle`, a
wiring mistake. Anything else is `StoreError`.

A timeout is typed by the operation that ran. A lock wait that runs out on a claim is `InProgress`,
and so is a statement timeout there. A lock wait that runs out is `ConcurrencyError` on an `append`,
a projection `save`, a saga write and a ledger `complete`, `release` or `forget`. There another unit
of work holds the row this write compares or fences. Everywhere else, including a `get_many` or a `load`
that runs past the statement timeout, a timeout is `StoreError`. A read compares no version, so it
never raises `ConcurrencyError`.

A statement the server refuses aborts the transaction. The handle remembers it: a block that catches
the error and exits cleanly is rolled back, and raises `StoreError`, rather than report a commit
that never happened. A refusal the kernel makes before any statement runs, such as a document that is
not an object, leaves the unit of work usable. So does a version conflict read from a returned row,
such as a projection `save` at a stale version.

`append` keeps its unique violation on purpose. A batch that lost the race must not land half of
itself on top of the winner's events.

`append` sends no event back. Its statement returns the stream's version before the write and its
clock reading. The kernel stamps the drafts with that reading, which gives the events `load` reads
back later.

A unit of work belongs to the task that opened it. A store call with its handle from another task
raises `BadHandle`, because one connection cannot run two statements at once.

A connection dropped by an idle timeout or failover raises `StoreError`. Cleanup preserves that
error even if the pool has already released the connection. Cancellation during rollback terminates
the connection and preserves the cancellation.

## Projections and queries

`PostgresProjectionStore(db, "accounts")` holds one read model. Every read model shares the
`edax_projections` table, keyed by `(projection, id)`. A document is stored as the kernel writes it,
with the row key as its `"id"`.

`get_many` compiles the query to one statement that pages exactly as the kernel's reference applier
does: the same filter verdicts, the same total order, the same cursors. Two services paging one read
model agree on every row and every cursor, whichever store served them.

**Numbers.** No value is refused for its size. A double of magnitude 2^53 or more is stored as its
exact digits. The compiler binds comparands and cursors the same way, so it compares as the kernel
compares it. A document written to the table by other means is outside that guarantee.
`jsonb` spells numbers its own way on read: `1e-7` comes back as `0.0000001`, a large double as its
exact digits, and `-0.0` as `0`. Each reads back as the same value. `jsonb` also returns keys in its
own order.

**NUL.** A string or a key holding NUL is refused with `CodecError`: Postgres text cannot hold it.
The memory store keeps it.

**Indexes.** A filter or a sort on a document path reads the document, so a large read model pays a
scan. The `id` path compiles to the row key column, which the primary key orders. An `id` prefix
filter compiles to `id >= $lo AND id < $hi`, so the primary key serves it even in a generic
prepared plan. Add your own expression indexes for the paths you filter on most.

**Row locks.** With `lock_reads=True`, a `get` inside a unit of work holds the row until it ends. Two
writers of one row then wait for each other instead of failing their compare-and-set.

## The idempotency ledger

`PostgresIdempotencyStore` declares `joins_transaction = True`. Pass `unit_of_work=db` to the guard,
and it runs `begin; claim; run; complete; commit`: one commit, effectively-once.

A claim is an `INSERT … ON CONFLICT DO NOTHING`. A claimant of a key another open unit of work holds
waits for it on the row lock. When the holder commits, the claimant reads its record and replays it.
When the holder rolls back, the claimant wins. When the lock wait runs out first, the claim raises
`InProgress`, which a dispatcher retries without counting an attempt. That covers a consumer that
lost its partition while still running: the new owner waits, then replays the old owner's outcome.

A claim made outside any unit of work commits pending. It lapses after `claim_ttl_ms`, so a crashed
claimant does not hold its key forever. That mode is at-least-once: a handler slower than the TTL
can run twice. Records do not expire; purge settled rows past your longest redelivery window with
your own job, by `settled_at`.

`complete`, `release` and `forget` wait for a held ledger row, up to the lock timeout. They never
skip it. A takeover by another claimant can still roll back, and the original claimant must then be
able to complete. Inside a joined unit of work the claimant already holds its row, so it never waits.

## The saga store

`PostgresSagaStore` keys instances by `(saga, id)`. A step's commands are written in the same
statement as its state, keyed by the kernel's `saga_command_id`. Inside a unit of work, `load` locks
the instance, so two steps of one instance wait for each other instead of failing. A completed
instance stays as a tombstone, stamped `completed_at`; purge old tombstones with your own job.

## Outboxes and relays

`PostgresEventStore`, `PostgresCommandOutbox` and `PostgresSagaStore` are each an `Outbox` read side,
and `CountedOutbox`. The reads run on a connection of their own, outside any unit of work, and see
committed rows only. `mark_published` deletes the rows it acknowledges.

Run one relay per outbox, or give each relay a `RelayLease`:

```python
events = PostgresEventStore(db, lease=RelayLease(owner=f"{hostname}:{pid}", ttl_ms=30_000))
```

Only the relay holding an outbox's lease reads rows. Each outbox table has its own lease, named after
the table, so stores over different tables never block each other. The holder renews when less than
half the lease remains.
Earlier polls read without updating the lease row, as do polls by a relay that does not hold it.
When the holder stops, another relay takes the lease over once it lapses. Rows are never leased one by one: two
relays could then publish one aggregate's events out of order.

Choose `ttl_ms` at least twice the slowest batch publish time and twice the relay's `idle_s` interval
(converted to milliseconds). Allow extra time for database and network delays. A read keeps at
least half the lease from the statement's clock reading, not from when publishing starts.
The lease is not a fencing token. A relay paused past expiry can still publish its batch after
another relay takes over. That repeats events, and it also breaks the per-key order:

1. Relay A reads events 1 and 2 of one aggregate, then pauses past its lease.
2. Relay B takes the lease, reads the same unmarked rows plus event 3, and publishes 1, 2, 3.
3. A wakes and publishes 1 and 2 again. The broker now holds 1, 2, 3, 1, 2.

The first copy of each event still arrives in order. The late copies are harmless only to a consumer
that drops them: `idempotent_by_id` within its retention, or a staleness check on the sequence. No
check of the lease's time before the publish closes this gap, because the pause can land between the
check and the send. Only a token the broker enforces can, such as a transactional producer, which
edax does not use. Keep consumers idempotent.

Delete-heavy outbox tables need vacuum to keep up: set aggressive autovacuum on them, as suggested above.
A transaction left open for long holds back cleanup everywhere, so set
`idle_in_transaction_session_timeout` and watch the age of `backend_xmin`.

## Operations

- **Pool size.** One connection per open unit of work, plus one per auto-commit call and relay read.
  Size the pool for your dispatcher workers plus your relays, plus one.
- **PgBouncer.** In transaction mode, build the pool with `statement_cache_size=0`, or use PgBouncer
  1.21 or later with `max_prepared_statements`. The timeouts use `SET LOCAL`, which transaction mode
  keeps. The text that opens a transaction is one simple query with no parameters, which transaction
  mode passes through whole.
- **Failover.** Connect with `target_session_attrs="read-write"`. A write that reaches a read-only
  server is a transient `StoreError`.
- **Lock order.** A handler that writes several aggregates or projection rows writes them in sorted
  key order, so two handlers cannot deadlock. A deadlock is a transient `StoreError`; the `retry`
  middleware runs the unit of work again from the start.
- **A lost commit reply.** The work committed but the caller saw a failure. The joined ledger
  replays the recorded outcome on redelivery. An unguarded projection handler applies the event
  again, so guard it with `idempotent_by_id`.
- **Observability.** Every statement opens with an `/* edax:<op> */` tag, so `pg_stat_statements`
  groups it by operation.

## Multi-tenancy

- **Pool.** The repository composes the tenant into the stream key, and each event and command keeps
  its tenant column. A projection id is not tenant-scoped: compose the tenant into it, and filter by
  `id` prefix, which the primary key orders.
- **Bridge.** A transactor reads one schema, and a pool has one transactor. Give each tenant schema
  its own pool and transactor, built with `schema=`.
- **Row-level security.** Use it as extra protection on events and outboxes only. A relay reads every
  tenant's rows, so its role needs `BYPASSRLS`.

## See also

- [The Postgres kernel](../kernels/postgres.md): every decision this adapter carries out.
- [Backends](./README.md): the seam contracts and the conformance suite.
- [Transactions](../sdk/transactions.md): `Transactor`, `current_tx`, `transaction`.
- [Idempotency](../sdk/idempotency.md): both guards and both modes.

## Encrypted records and erasure

Table contract 2 adds nullable `subject` and `encryption` columns to message/outbox tables, and
nullable `tenant`, `subject`, `encryption` columns to snapshots. Existing null rows remain historical
plaintext and require an explicit compatible reader policy. Use the
[additive migration](postgres-crypto-migration.sql) with your own schema names and migration tool.
Edax never provisions these tables.

Durable erasure uses the separate [reference control schema](postgres-erasure-schema.sql) and
`edax.backend.erasure_postgres.PostgresErasureStore`. Its operation records and control outbox use
no subject payload key. `guard_in_transaction(connection, scope)` must run on the same connection
as participant writes and hold through commit; cleanup uses `revoke_in_transaction` before deletion.
Acquire scope fences before application row locks. These helpers require READ COMMITTED because
revocation must be read in a fresh statement snapshot after a lock wait.

A provider guard and a Postgres destination guard protect different resources. A production
participant must wire both where required. The reference memory provider is process-local and
cannot fence another service. See [provider and participant conformance](../guides/crypto-shredding.md).

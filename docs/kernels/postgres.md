# The Postgres kernel — `edax-postgres`

The Postgres store kernel. It owns the table contract, the SQL each operation runs and how a value is
laid out in a row. It owns how a read-model query becomes SQL, what a server error means, and the ledger's fence.

Software development kits (SDKs) that point at one Postgres database must agree on all of it. One
SDK that wrote a row in another layout, or compared a value another way, would answer differently
from the other on the same data. `edax-postgres` holds those decisions. It links no Postgres client:
the SQL is text, every parameter is text, and every message, record or instance a statement returns
is one JSON text column. Each SDK ships only the client calls around it.

| Module | Owns | Golden vectors |
|---|---|---|
| `config` | the settings a store refuses, their defaults and overrides, the relay lease, the claim read bound, the `bigint` bound | (unit tests) |
| `error` | the verdict on a server error, per operation, and on a client-side failure | `postgres_error.json` |
| `schema` | the table contract, the table names a host chooses, the catalog check, the reference schema and the server check | `postgres_contract.json` |
| `statements` | every statement, byte for byte, rendered for the host's table names, and the text that opens a transaction with the settings' timeouts | `postgres_statements.json` |
| `rows` | the parameters each statement binds, the statement a call chooses, and the values each read decodes | `postgres_rows.json` |
| `documents` | the stored text of a projection document, and exact number text | `postgres_rows.json`, `postgres_query.json` |
| `query` | the read-model query compiled to one statement, and the page its rows make | `postgres_query.json` |
| `erasure` | durable operations, atomic control outbox, permanent revocations, catalog checks and transaction advisory fences | erasure unit and live tests |
| `ingest` | optional ingestion table contracts, row locks and atomic state/rejection writes | `postgres_ingest.json` |
| `projection` | optional projection state and intent tables, shared and partition locks, one read and one write per kind per batch, indexed pending delivery, delete on delivery and pruning | `postgres_projection_state.json` |
| `ledger` | the claim owner, what a claim does with the row it read, the ledger's parameters, and the fence on each settling write | `postgres_ledger.json` |

`crates/edax-postgres/live-check` runs the SQL against a real server. It replays `filter.json`,
`query.json` and `postgres_query.json`, walks every page of every sorted query, and runs a seeded
differential over documents built to hit each rule. It checks the inline decimal reading against
the kernel's, and the contract check against a real catalog. It commits one sequence of projection
transitions to the `projection` tables and to `edax-memory`'s `MemoryProjectionState`. Both must then
hold the same records and page, mark and prune the same intents. A second SDK replays the
same files.

## The table contract

edax provisions nothing. The host creates the tables with its own migration tool, under the names it
chooses. edax states what each table must hold, and checks the catalog against it before the first
statement on that table runs. No database object but tables and indexes is needed: no function, no
extension, no version table.

**Table names.** `Tables` holds the schema and a name per role. A role the host does not name takes
`edax_<role>`. Every name is 1 to 63 of `a-z`, `0-9` and `_`, not starting with a digit, so it is quoted
without escaping. Every statement qualifies every table with its schema, so no statement depends on
`search_path`, and a connection pooler that drops it cannot redirect a write.

| Role | Holds | Unique keys |
|---|---|---|
| `events` | one row per event, every field its own column | `id`; `(stream, sequence)` |
| `snapshots` | the latest snapshot per stream | `stream` |
| `event_outbox` | one row per unpublished event, `position` defaulted | none |
| `command_outbox` | the commands a save sends, until a relay marks them | `id` |
| `projections` | read-model documents, as `jsonb` | `(projection, id)` |
| `ledger` | the record, the owner of a pending claim, who settled it | `key` |
| `saga_instances` | saga state, a completed instance kept as a tombstone | `(saga, id)` |
| `saga_outbox` | each step's commands | `(saga, id)` |
| `relay_leases` | the optional relay lease per outbox | `outbox` |

**What the contract requires.** `contract()` lists it table by table, and `postgres_contract.json`
carries it.

- Each column edax reads or writes exists, with an accepted type: `text` or `character varying` for
  text, `bigint`, `bytea`, `jsonb`, `boolean`, `timestamp with time zone`.
- A column edax writes `NULL` into is nullable. A column edax never writes (`position`) has a default:
  an identity or a sequence.
- Each unique key has a unique index over exactly its columns. It is not partial, not over an
  expression, not deferrable and valid. Each compare-and-set and each insert-if-absent rests on one.
- Every text key column, and its unique index, collates as `"C"` or `"POSIX"`. That is byte order,
  which for UTF-8 is code point order, the order the kernel sorts strings in. A deterministic
  collation also makes two keys equal only when their bytes are.

**What the host owns.** Extra columns are allowed when nullable or defaulted, so an `INSERT` that
does not name them still lands. Extra indexes, foreign keys, partitioning, storage settings and any
constraint names are the host's.

**The check.** The `catalog` statement reads each table's columns and unique indexes from the system
catalog. `check_catalog` holds them to the contract and lists every mismatch in one `bad_handle`
error. A compatible database answers which unique index guards which key, by the index's name. The
error verdict reads a unique violation by that key, so the host's constraint names never matter.
`check_server` refuses a server older than Postgres 14, and a database whose encoding is not `UTF8`.
It also refuses a `lock_timeout_ms` at or below the server's `deadlock_timeout`. With a shorter lock
timeout, both sides of a deadlock time out before Postgres looks for one. A claim reports that timeout
as `in_progress`, which a dispatcher retries without counting, so the deadlock would repeat forever.

**Versions.** `CONTRACT_VERSION` names the contract. It changes only by an addition, a new column or
table a release needs. The release notes say which contract a release requires.

**The reference schema.** `reference_ddl(tables)` writes one schema that meets the contract: a
starting point to copy into a migration, never run by edax. `docs/backends/postgres-schema.sql` is
that schema for the default names, regenerated with the golden vectors.

## The statements

Each parameter is declared `text` and cast in the statement. Each statement opens with an
`/* edax:<op> */` tag, so `pg_stat_statements` groups it. A kernel function gives every statement its
parameters, and NUL in any text parameter is refused as `codec` before any I/O. Where the statement
turns on the input, the function names it too. It picks an insert or a compare-and-set by the
expected version, a locked or a plain read, and a leased or an unleased outbox read. The adapter only
runs what it is handed.

`save_snapshot` binds seven parameters: `$1` stream, `$2` type name, `$3` version, `$4` data as
hexadecimal bytes, `$5` tenant, `$6` subject, and `$7` serialized encryption information. The last
three are nullable for an ordinary snapshot. The statement returns the stream version for the
kernel's snapshot check.

Saga writes bind the kernel's completion verdict separately from state JSON: `$6` on insert and
`$7` on update. A protected completed saga retains its encrypted state envelope while setting
`completed` and `completed_at`, so retention can still find it. The statement does not parse
application state as `jsonb` to infer completion.

Five shapes carry a decision.

- **A transaction opens in one round trip.** `begin` renders `BEGIN ISOLATION LEVEL READ COMMITTED`
  and the lock, statement and idle timeouts as `SET LOCAL`, from settings `check_config` accepts. The
  text binds no parameter, so a client sends it as one simple query. A store call alone costs three
  round trips: `begin`, its statement, `commit`.
- **`append` is a plain `INSERT`.** It checks the stream's last sequence in the same statement. The
  unique index on `(stream, sequence)`, the `events_stream_sequence` key, catches a writer that raced
  past the check. That
  violation aborts the transaction on purpose. An `ON CONFLICT DO NOTHING` would land the rest of a
  batch on top of the winner's event, and a caller that caught the conflict could commit it. It
  returns the stream's version before the write and its clock reading, not the events: every other
  stored field is the draft's own. `append_result` stamps the drafts the caller bound with that
  reading, as the memory store does.
- **Outbox rows follow the batch.** Each write inserts its outbox rows in sequence order, in the same
  statement, so a relay reads them in that order.
- **The ledger fence is one step.** `ledger_hold` locks the key's row with `FOR UPDATE`. The settling
  write follows in the same transaction. A store outside a unit of work opens a transaction for the
  pair. The lock waits rather than skipping a held row: a takeover that rolls back must let the
  original claimant settle its record.
- **A relay lease is per outbox, not per row.** The `*_leased` reads take or renew the outbox's lease
  in the same statement. A relay that does not hold it reads no rows. Leasing single rows would let two
  relays send one aggregate's events out of order. The holder renews the lease only when less than
  half of it remains. A poll by a fresh holder or by a non-holder writes nothing to the lease row.
  The statement names the lease after its schema-qualified outbox table, plus the saga name for a
  saga outbox. So two outboxes never share a lease, and no SDK chooses the name.

## Numbers

`jsonb` keeps a number as the decimal value of its text. The kernel reads a number that is not an
integer as the nearest `f64`. Two rules make the two agree.

- **The stored text is the kernel's.** `stored_document` re-serializes the keyed document, so a host
  writing `0.30000000000000000001` stores `0.3`, the double the kernel compares.
- **A double of magnitude 2^53 or more is written exactly.** Below 2^53 the shortest text of a double
  sits closer to it than any integer. So ordering by the text orders by the double. Past it, the
  shortest text can sit on the far side of an integer: `5.808820077685227e16` reads as
  `58088200776852270`, below the integer `58088200776852271`, which the double exceeds. So
  `exact_json` writes such a double as its exact digits and a `.0`: `58088200776852272.0`. `jsonb`
  keeps that value, and every reader reads it back as the same double.

This read-model document/query contract does not reject a number for its magnitude. Ingestion and
fold application values have the stricter [canonical JSON numeric contract](cqrs.md#numbers).
The compiler binds comparands and cursor values through the same
exact text. A document written to the table by other means is outside the guarantee. `jsonb` also
spells numbers its own way on the way out: `1e-7` comes back as `0.0000001`, and `-0.0` as `0`. Each
reads back as the same value.

A string or a key holding NUL is refused as `codec`, because Postgres text cannot hold it. This is
the one value Postgres refuses that the memory store keeps. A NUL inside a metadata value survives,
because the map is stored as JSON text.

## The query

`compile(tables, projection, spec)` turns the kernel's query into one statement. It reproduces the
reference applier, not `jsonb`'s own semantics.

| Kernel rule | SQL |
|---|---|
| a path segment is an object key, never an array index | `->` with a text operand, which yields `NULL` on an array or a scalar |
| a missing or null path fails a leaf, and `Not` flips it | nullable document leaves use `coalesce(…, false)` |
| equality is numeric at every depth | `jsonb` `=` |
| an ordering holds within one type | typed by the comparand: `::numeric`, `COLLATE "C"` text, `::boolean` |
| decimal text compares by value | `decimal_sql`, inline SQL, gated on a string value |
| `Contains` tests each element | `EXISTS (… jsonb_array_elements …)`, never `@>` |
| the total type order | each sort key expands to its type rank, then each type's value |
| `"7"` and `"07"` tie under the decimal collation | only text that is not decimal has a text column |
| the `id` tiebreak | the row key column, which every stored document carries as `"id"` |
| an `id` prefix | `id >= $lo AND id < $hi` on the row key column, which the primary key serves even in a generic plan |
| keyset resume strictly after the cursor | an `OR` over the keys, each with every earlier key tied |
| an unsorted query keeps input order | `ORDER BY id`, the memory store's key order |

A comparand holding NUL cannot be bound. No stored document holds one, so the compiler folds it to
the answer the kernel gives every stored document. Equality is false. A string ordering compares
against the text before the NUL.

`page(spec, rows)` builds the page from the rows the statement returned. The statement fetches one
row past the limit. `next` is minted from the page's last row only when that extra row came back,
and never for an unsorted query.

`decimal_sql(text)` reads decimal text in plain SQL, inline in the statement, so the contract needs
no function. It reads the kernel's decimal grammar. That is an optional sign, ASCII digits with at
most one point, and an optional exponent within 32 bits. The text is at most 2000 bytes, with at most
1000 digits. It reads the text with plain string functions, as the kernel does: one sign, a split on
the one `e` or `E`, then on the one point. It normalizes the mantissa and exponent before the cast, so
`"0E+999999999"` reads as 0, as the kernel reads it. It answers `NULL` for any other text, and text
outside the grammar reaches each cast as `NULL`, even when the server folds a bound value while it plans.

## The verdict

`verdict(op, sqlstate, unique, message)` reads a server error against the operation that ran.
`unique` is the contract key the violated unique index guards: the adapter maps the index name the
server reports through the `Compatibility` the table check answered. It is `None` when no key of the
contract was violated.

| SQLSTATE | Verdict |
|---|---|
| `23505` on the `events_stream_sequence` key | `concurrency`: another writer appended first |
| `23505` on an `ingest` or `projection_state` | `concurrency`: another transaction inserted the key first, so a retry reads it |
| `55P03`, `57014` on a `claim` | `in_progress`: another unit of work holds the key, and a dispatcher retries it uncounted |
| `55P03` on an `append`, `projection_save`, `saga_write` or `settle` | `concurrency`: another unit of work holds the row this write compares or fences |
| `55P03` on `ingest` or `projection_state`, `55P03` or `57014` on `erasure_fence` | `in_progress`: lock contention waits without spending an attempt |
| `55P03` on remaining operations, `57014` on any but `claim` or `erasure_fence` | `store`, transient: a read or an unversioned write ran out of time, which is no version conflict |
| `40001`, `40P01`, `25006`, `25P02` | `store`, transient |
| the `22`, `23` and `54` classes, `23505` on any other key or on none | `codec`: data or a constraint the server refuses, which fails the same way again |
| the `28`, `3D`, `3F` and `42` classes | `bad_handle`: a wiring mistake no retry fixes |
| anything else, the `08` and `53` classes among them | `store`, transient |

Exact states match first, then classes.

`client_verdict(op, kind, message)` reads a failure the client reports without a SQLSTATE: its kind
is `encode` or `other`. A parameter the client cannot encode is `codec`. Anything else, a timeout or a
broken connection among them, is `store`.

## The ledger

A ledger row holds the kernel's record text, the `owner` while it is a pending claim, and the
`settled_by` owner once a claim settled it. A claimant's handle is its owner token. Each settling
write reads the row as the kernel's `Holder` class, and the kernel's `complete_fence` and
`release_fence` decide.
`postgres_ledger.json` replays every class through every write, and `idempotency_fence.json` is
replayed through the same code.

Inside a unit of work a pending claim never commits: the claim and its record commit together, or
roll back together. A claim made outside one commits pending. `claim_read` lets another claimant
take it over once it is older than the claim time to live (TTL), so a crashed claimant does not hold
its key forever. That mode is at-least-once: a handler slower than the TTL can run twice.

A claim that lost its insert reads the row, and `claim_read` names what it does next: `prior`,
`take_over` or `vanished`. A lost take-over counts as a vanished row. The kernel bounds the loop: a
row still gone on the third read gives the claim up as the transient `store` error.

## Optional ingestion and projection state storage

`ingest` and `projection` each provide optional table contracts and reference DDL. They do not extend the
base event-store schema or provision tables at runtime. Hosts apply the DDL through migrations.

`ingest::statements` supplies target-manifest checks, row-locking plan reads, inserts and updates,
and pending rejection reads and marks. Ordinary ingestion takes no advisory lock: a plan read locks the
rows it finds, and an absent key is inserted without `ON CONFLICT`, so a concurrent insert fails
and retries. A key range bounds each target, so the key index serves it on every supported
version. The partial index selects unpublished rejections so delivery does not scan lifetime
publication history. The adapter commits accepted writes and durable rejections in one owned
transaction. A failure carries the `ingest` verdict and the rollback evidence, and
`edax_event::batch::recover` decides from both. A unique violation is `concurrency`, so it retries.
`delete_scope_events` and `delete_scope_state` take the bounds `ingest::scope_range` returns and
report how many rows they deleted: an erasure cleanup of a plaintext target. They never touch
rejections.

`projection::statements` supplies two tables. The state table holds the manifest, checkpoints,
states, receipts and a delivery cursor as key/value rows with no secondary index and fillfactor 70,
so updates stay heap-only (HOT).
The intents table holds undelivered intents with an index on their source position. A transaction
takes the generation lock (shared for partition work, exclusive for initialization, freeze and
activation) and its partition's lock before it reads. It reads with one statement, then inserts,
updates and inserts intents with one statement each. An insert has no `ON CONFLICT`, so a concurrent
insert of one key fails the transaction instead of overwriting. Backup and restore must preserve
both tables together.

The projection adapter executes the plan from `edax-cqrs::projection` under those locks. `pending`
reads the manifest first, so an inactive generation reads no intent. Then it reads each
partition's eligible range through the index, in source order. It rotates partition order after the
last partition of the previous page and atomically saves the new turn in a separate row at
`manifest_key + ":delivery"`. The suffix cannot collide with a canonical projection record key.
This scheduling row uses the existing table and needs no DDL migration. Empty pages leave it unchanged;
failed delivery still advances the turn so healthy partitions remain reachable. Delivery deletes the intent;
`prune` deletes intents a cutover made permanently ineligible. Activation rewrites no intent.
`delete_scope` takes the bounds of `state_scope_range` and deletes that scope's state records, never
receipts or checkpoints. The erasure statements add `revoked_operations`, a page of every operation
past `persisted`, which a cleanup participant's sweep reads after a restore.
`postgres_ingest.json` and `postgres_projection_state.json` pin the SQL. Live tests cover rollback, ownership races,
query plans and restore behavior beyond what pure vectors establish.

See [ingestion](../backends/ingestion.md) for migrations and batch recovery, and
[projection processing](../sdk/projection-processing.md) for cutover and restore procedures.

## See also

- [The Postgres backend](../backends/postgres.md): the adapter, the unit of work and the operations
  guide.
- [cqrs.md](./cqrs.md#query--ordering-and-pagination): the reference applier the compiler
  reproduces.
- [filter.md](./filter.md): the null and missing-path rule.
- [golden-vectors.md](./golden-vectors.md): how the vectors are generated and asserted.

Opt-in `ingest::partition` SQL places append rows by exact UUIDv7 time. A transaction advisory
gate per physical table fences its durable admission floor. Rejection decisions remain permanent.
See [partitioned ingestion](../backends/partitioned-ingestion.md).

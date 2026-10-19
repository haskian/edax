# Projection processing

`KafkaProjection` builds state by applying each event to the previous state of its aggregate. For
example, a balance reducer adds a deposit and subtracts a withdrawal. The store loads the state;
your reducer receives that state and one event, then returns the new state.

`PostgresProjectionStateStore` saves the new state, event and sequence receipts, outgoing intents, and source
checkpoint in one transaction per batch. `MemoryProjectionStateStore` provides the same contract for tests, without
durable storage. Use [upsert ingestion](../backends/ingestion.md) when each event already contains the complete
replacement state. The [projection guide](projections.md) compares the available write paths.

Python provides the runner and both stores. Go has internal kernel bindings but no projection runner or
database adapter. The runnable [Python example](../../examples/py/21_projection_processing.py) uses the memory store;
the [Go example](../../crates/edax-go/examples/21_projection_processing/main.go) demonstrates the kernel transitions
inside this repository. Its internal imports are not available to external Go modules.

## Write a reducer

```python
from edax.errors import CodecError
from edax.projection import ProjectionEvent, ProjectionResult


def reduce_balance(state: dict[str, int] | None, event: ProjectionEvent) -> ProjectionResult:
    balance = state["balance_minor"] if state is not None else 0
    amount = event.payload["amount_minor"]  # The decoder validates the event schema.
    if event.event_type == "MoneyDeposited":
        balance += amount
    elif event.event_type == "MoneyWithdrawn":
        balance -= amount
    else:
        raise CodecError(f"Unsupported balance event: {event.event_type}")
    return ProjectionResult({"balance_minor": balance})
```

A command handler decides whether a requested withdrawal is allowed. This reducer applies a
withdrawal that already happened. A command handler can reuse a reducer when updating its own
aggregate, but does not need the Kafka projection runner for that transaction. Event sourcing stores the
authoritative event history; this runner consumes events to maintain derived state.

The first event receives `state=None`. Later events receive the previously committed state, or
the state produced by an earlier event in the same batch. A stored `None` is also a valid state;
it does not remove the aggregate's sequence history. The runner calls the reducer only for a
new event. A rolled-back attempt can call it again, so it must be deterministic and have no
external side effects. Return `ProjectionIntent` values for effects that should be delivered after commit.

## Batches and reducer calls

The runner collects a batch from one Kafka partition, bounded by record count, bytes and age.
A batch may contain several events for the same aggregate and events for other aggregates.
The store processes them in source order, calling the reducer once per new event:

```text
account A starts at 100
batch: A +20, B +5, A -30
calls: reduce(A=100, +20) → A=120
       reduce(B, +5)     → updated B
       reduce(A=120, -30) → A=90
commit: both states, receipts, intents and the partition checkpoint
```

The two calls for A share the transaction's staged state. The reducer receives one event at a
time, never an event list. Replays skip the reducer but advance source progress. Every new delta
is applied; there is no latest-event compaction. A reducer or validation failure rolls back the
entire batch. If the database commits but its reply is lost, the runner recovers from the stored
checkpoint before retrying.

A batch is one transaction in two kernel steps. `PrepareBatch` decides which inputs are novel.
The store then calls the reducer once per novel input. `ApplyBatch` checks every result and
returns all writes. The store reads every record the batch needs in one statement. It writes each
kind of change in one more, so the round trips per batch do not grow with its size.

## Input and reducer contract

A `ProjectionEvent` names tenant, aggregate, stable event ID, aggregate sequence, event type,
schema version, payload and metadata. These are the only reducer-visible fields.
The kernel canonicalizes the envelope before invoking the reducer and stores canonical
state. Envelope and reducer output must remain within the kernel's canonical numeric domain
after normalization. The kernel checks original integer tokens before JSON parsing can round
them; represent exact large numbers as strings. Transport offset and delivery attempt describe
delivery and do not participate in event identity. Every store refuses a NUL character in the
tenant, the replacement state or an intent payload, and a storage key over 2048 bytes (a long
aggregate or event ID). Both are `codec` errors, because Postgres cannot store either.

Every new event must have the next aggregate sequence, beginning at one. A no-op still
advances that sequence. The aggregate stays on its original source partition. Both event
identity and aggregate/sequence identity are retained indefinitely. Reusing either with
different content stops that partition. An identical event at a new offset advances only
its checkpoint. Deltas are never compacted.

Reducers and decoders are synchronous, pure, deterministic and bounded by the host.
They must not perform I/O. A reducer returns `ProjectionResult(state, intents)`; `None` is a real
replacement state. Each `ProjectionIntent` has a stable key within its event, a destination and
payload. A poison event or sequence gap never advances progress, even if an operator
publishes a diagnostic copy elsewhere.

## Starting state and snapshots

Normal processing loads the last projection state from the store. Recovery reloads that state and the
checkpoint committed with it, then fetches later records. The reducer does not query an external
balance or snapshot service; a changing lookup could produce different results on replay.

Importing an aggregate snapshot as a new generation's starting state is not supported. A snapshot saying
"account A is at sequence 42" does not prove which records from other accounts have been processed.
Even if A's snapshot includes Kafka offset 500, account B may still need its event at offset 400.
Starting the entire partition at 501 would skip B's update. A future snapshot-import contract would
need to establish complete state and matching source progress together.

Each state key belongs to one tenant and aggregate, on one source partition. The runner can process
many such aggregates, but cannot combine their events into one shared balance or impose a global
order across partitions. A [custom projection handler](projections.md) owns those rules.
Use the [offline restore procedure](#offline-restore-procedure) to restore an existing generation's complete
state, receipts, outbox and checkpoints from a consistent backup.

## Provision and initialize

Hosts run the returned DDL through their migration tooling. No runtime API creates tables.
The two projection tables are optional and do not change ordinary PostgreSQL or ingestion tables.
`edax_projection_state` holds manifests, checkpoints, states, receipts and delivery cursors as key/value
rows with no secondary index, so state updates stay heap-only (HOT). `edax_projection_intents` holds undelivered
effect intents with an index on their source position.

```python
from edax.backend.projection_postgres import PostgresProjectionStateStore, reference_projection_state_ddl
from edax.projection import ProjectionTarget, ProjectionCutover

migration_sql = reference_projection_state_ddl(schema="billing")
target = ProjectionTarget(
    generation="balances-v1",
    source_identity="verified-cluster-uuid/verified-topic-uuid",
    source_history="enforced-no-rollback-policy-v1",
    topic="account-events",
    reducer_id="balance-reducer-v1",
    envelope_id="canonical-event-v1",
    keying_id="account-partition-v1",
    effect_namespace="account-receipts-v1",
    starts={0: 0, 1: 0},
)
store = PostgresProjectionStateStore(transactor, target, schema="billing")
await store.initialize()
```

The strings above identify contracts; they are not evidence by themselves. The source
capability below must verify them. Every generation binds its full contract permanently.
Changing reducer, envelope, keying, partition set or source requires a new generation.
Starts must cover complete required history. An arbitrary aggregate snapshot cannot
establish a partition checkpoint. Initialization is an explicit new-generation operation;
never initialize storage assembled from an incomplete restore.

A fresh generation starts with dispatch disabled. For an initial deployment, explicitly
activate its starting vector after the host confirms it is the sole effect producer:

```python
await store.activate(
    ProjectionCutover(
        target.source_identity,
        target.source_history,
        target.topic,
        target.starts,
    )
)
```

## Source verification and runner

Supply a `ProjectionSource.verify(topic_partition)` capability returning `SourceProof`.
It must obtain supported evidence for cluster/topic incarnation and retained beginning
and readable end. A topic name, nickname, aggregate sequence or consumer position alone
is insufficient. Same-topic truncation must be detected by history evidence, or prevented
by an enforceable no-rollback/no-unsafe-election deployment contract. The SDK has no
permissive default verifier. The current aiokafka consumer API alone does not establish
these identity/history guarantees.

The runner checks evidence when it takes or recovers a partition, before every batch commit, and
when an idle partition extends its lease after half of it has passed. An idle partition otherwise
touches neither the source nor the database. A checkpoint below the retained beginning or beyond
the readable end stops the partition.
Topic recreation, history change or unprovable continuity also stops it. In read-committed
mode, the verifier uses the last stable offset as the readable end.

The application supplies `verified_source` and `decode_event`. The runner decodes each record to an
`EventMessage` with its serializers, as `KafkaConsumer` does; `decode_event` validates that message
and returns a `ProjectionEvent`. Pass the `reduce_balance` function above as the reducer:

```python
from edax.backend.kafka_projection import KafkaProjection

runner = KafkaProjection(client, store, verified_source, decode_event, reduce_balance)
client.subscribe([target.topic], listener=runner.rebalance_listener())
await client.start()
try:
    await runner.run()
finally:
    try:
        await runner.stop()
    finally:
        try:
            await client.stop()
        finally:
            await runner.wait_closed()
```

Configure `enable_auto_commit=False` and `auto_offset_reset="none"`. The client and pool
remain caller-owned. Custom clients must deliver assignment callbacks before new-assignment
fetch results. On each acquisition/recovery the runner seeks from the database checkpoint.
Broker commits are unnecessary hints and are never used to reconstruct projection progress.

`partition_failures` exposes stopped partitions. Healthy partitions continue. Repair the
source/application problem before reassignment or restart. Transient database failures use
the batch retry ceiling. Waiting for another valid owner does not consume that failure budget.
An unresolved operation remains tracked after its deadline; that partition cannot retry until
the operation really ends. `stop()` has a drain deadline; `wait_closed()` waits for actual
resource release and can therefore take longer. Cancelling `wait_closed()` cancels only that wait;
it leaves owned operations running and tracked. Stop the Kafka consumer before waiting so
unfinished fetches can end. Keep the database pool and other resources used by outstanding
operations alive until `wait_closed()` finishes. Custom source verifiers must also provide a
way to end their outstanding reads.

## Sealed events

Pass `sealing=` to read sealed events. The runner opens each event before `decode_event` sees it, and
reads each subject's keys once per batch. `decode_event` must keep the envelope's tenant, aggregate,
id and sequence. The runner sets the event's `subject` from the envelope, and the state sits under it.

| Key state | What the runner does |
|---|---|
| Active | Reduces the opened event; its receipts bind an HMAC under the subject's fingerprint key |
| Revoking or unavailable | Waits without spending attempts |
| Missing or unauthorized | Stops the partition |
| Destroyed | Records the event's receipts and moves past it, without a reducer call or a state change |

Each commit runs under the write fence of every subject it writes. A subject erased between staging
and the commit makes the fence refuse; the runner waits, stages the batch again and finds it erased.

## Ownership and transaction boundary

A lease includes the exact generation/source/topic/partition checkpoint key, owner, epoch,
next offset and expiry. Use a fresh unique owner for each logical acquisition. Retrying that
same acquisition after a lost response is idempotent. Takeover increments the epoch.
A still-current assignment can refresh an expired lease with a fresh epoch. Every write
compares its owner, fence and expected database cursor under the transaction's locks.
Each batch commit extends the lease in the same transaction, so a batch that staged for a long
time cannot lose its lease before the write. A commit whose stored cursor differs from its lease's,
as after a failover that lost a commit, fails as transient: the runner reloads the cursor and
fetches again.

PostgreSQL takes two advisory transaction locks. Partition work holds the generation lock shared
and its partition's lock exclusively, so partitions commit in parallel. Partition work is acquire,
refresh, release, load and commit. Initialization, freeze and activation hold the generation lock
alone. A transaction inserts every record it read as absent. Two partitions racing to record one
event identity therefore cannot both succeed: the loser fails and retries, and the kernel then
reports the conflict. A transaction already holding a lock finishes before a takeover. Different generations
use different locks. A transaction that waits past the lock
timeout is contention, `in_progress`: the runner waits again without spending a retry attempt.
The kernel reads every other server error by its SQLSTATE. A missing table or refused login stops
the partition at once; a lost connection or reply is transient.

The adapter uses `PostgresTransactor.run_owned` and refuses ambient transactions. A result
is returned only after commit confirmation. After a lost response, the runner reloads the
database cursor under its valid ownership before refetching. Numeric Kafka offset gaps
are accepted; aggregate sequence gaps are not.

## Outbox and rebuild activation

The store is an [outbox](outbox.md) of `EffectIntent` rows, so the shared `Relay` delivers them:

```python
from edax.outbox import Relay

relay = Relay(store, deliver, on_dead=park_refused_effect, max_attempts=5)
await relay.run(stop=stopping)
```

`unpublished` returns eligible intents oldest first within each partition, taking each partition's
next in turn across pages, even when a page fits fewer intents than there are busy partitions.
The turn advances when a page is selected, including when delivery fails, and PostgreSQL persists
it across relay restarts in the existing state table. Empty pages do not change it. Offsets of two
partitions say nothing about which is older. Each aggregate stays on one partition, so its intents
keep their order. The
relay holds back only the aggregate whose delivery failed. `mark_published` deletes the intent. A refused intent goes
to `on_dead` after its attempts, so it cannot block the others. The destination must atomically
deduplicate `EffectIntent.id` with its business effect. Delivery is at least once: a lost
publication marker or restored backup can repeat it. The ID derives from effect namespace, tenant,
aggregate, logical event ID and intent key. It excludes generation, lease, source offset and
restore incarnation.

Rebuild into a new generation with dispatch disabled. Then:

1. Coordinate a cutover and call `old_store.freeze()`. It drains transactions under the
   generation lock, retires owners and returns the complete source-bound checkpoint vector.
   Only an active generation can provide this handover vector.
2. Let the new generation reach every returned cutoff. It may process a batch across a cutoff.
3. Call `new_store.activate(cutover)`. Activation validates source identity/history and every
   partition, then atomically persists immutable cutoffs. It rewrites no intent: delivery reads
   each intent against the cutoffs, so historical intents and future replays below a cutoff are
   never delivered. `store.prune(limit)` later deletes those intents a page at a time.
4. The old generation can finish delivery below its frozen upper cutoffs. Drain its relay
   before step 3 if one aggregate's effects must stay in order across the cutover. The new generation
   can deliver only at or above its lower cutoffs. Keep the same effect namespace for the same
   logical effects; a new effect contract needs an explicitly different namespace.

Do not pass a newly invented vector as a replacement for the former generation's committed
checkpoint vector. Re-emitting historical effects is outside automatic activation. Changing
source incarnation invalidates an offset-based handover.

## Offline restore procedure

Restore both projection tables from one consistent backup. They contain manifests, checkpoints,
state, both deduplication indexes and undelivered intents. Never mix records from different backups
or pair aggregate snapshots with separately reconstructed cursors.

1. Stop admission and dispatch. Block reconnects and revoke access for every old writer and
   dispatcher, including application processes with buffered outbox pages.
2. Terminate those processes and database sessions. Verify that no old connections or
   transactions remain. Keep reconnects disabled throughout maintenance.
3. Restore the complete snapshot while clients remain offline. Check the restored generation
   contract and independently verify source identity/history and retained bounds.
4. Start fresh clients with fresh acquisition owners. Seek from restored database checkpoints.
   Enable only dispatch allowed by the restored activation vector. Destination deduplication
   must remain durable independently of the restored publication markers.

A fencing epoch stored inside the snapshot can move backwards on restore. Incrementing it
before restoring does not protect against an old process. This release requires verified
termination and access isolation; it does not automate deployment credentials or process
termination. Online restore needs an external, storage-enforced incarnation authority and
is not supported. `test_projection_state_restore.py` exercises the offline boundary with terminated
PostgreSQL connections and restored state/checkpoints/deduplication/outbox records.

## Limits and verification

Batch record, serialized-input and age limits use the shared batch kernel. The runner fetches
one record at a time to reserve capacity before decoding. One partition batch executes at a
time; `BatchSettings.concurrency` does not enable concurrent database commits. Pending I/O from
an earlier deadline can coexist but retains its data credits until it actually finishes.

Retention gauges cover prepared serialized inputs and their transaction copies. They do not
bound process RSS, broker prefetch, Python objects, persisted history, loaded aggregate state,
reducer-produced state/intents or external delivery payloads. The host must bound callback
allocation and execution time. Async deadlines cannot preempt synchronous callback code.
Outbox paging bounds record count, not payload bytes. The memory reference retains all history.

A batch commit costs a fixed number of statements: the locks, one read, one clock read, and one
insert, one update and one intent insert. The state table keeps 30 % of each page free
(fillfactor 70) so state and checkpoint updates stay heap-only. A high-latency database needs
suitable operation deadlines.

Rust transition/boundary vectors are replayed through Python and Go. Python tests cover both
memory and live PostgreSQL, actual lock contention, cancellation rollback, lost COMMIT replies,
and offline restore. The Kafka tests cover late fetches, retention races and assignment fencing.
Go exposes the shared kernels; a Go projection runner and database adapter are not included.

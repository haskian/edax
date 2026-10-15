# Ordered Kafka folds

`KafkaFold` builds state by applying each event to the previous state of its aggregate. For
example, a balance reducer adds a deposit and subtracts a withdrawal. The store loads the state;
your reducer receives that state and one event, then returns the new state.

`PostgresFoldStore` saves the new state, event and sequence receipts, outgoing intents, and source
checkpoint in one transaction. `MemoryFoldStore` provides the same contract for tests, without
durable storage. Use [upsert ingestion](ingestion.md) when each event already contains the complete
replacement state. The [projection guide](../sdk/projections.md) compares the available write paths.

Python provides the runner and both stores. Go has internal kernel bindings but no fold runner or
database adapter. The runnable [Python example](../../examples/py/21_folds.py) uses the memory store;
the [Go example](../../crates/edax-go/examples/21_folds/main.go) demonstrates the kernel transitions
inside this repository. Its internal imports are not available to external Go modules.

## Write a reducer

```python
from edax.errors import CodecError
from edax.fold import FoldEvent, FoldResult


def reduce_balance(state: dict[str, int] | None, event: FoldEvent) -> FoldResult:
    balance = state["balance_minor"] if state is not None else 0
    amount = event.payload["amount_minor"]  # The decoder validates the event schema.
    if event.event_type == "MoneyDeposited":
        balance += amount
    elif event.event_type == "MoneyWithdrawn":
        balance -= amount
    else:
        raise CodecError(f"Unsupported balance event: {event.event_type}")
    return FoldResult({"balance_minor": balance})
```

A command handler decides whether a requested withdrawal is allowed. This reducer applies a
withdrawal that already happened. A command handler can reuse a reducer when updating its own
aggregate, but does not need a Kafka fold runner for that transaction. Event sourcing stores the
authoritative event history; this runner consumes events to maintain derived state.

The first event receives `state=None`. Later events receive the previously committed state, or
the state produced by an earlier event in the same batch. A stored `None` is also a valid state;
it does not remove the aggregate's sequence history. The runner calls the reducer only for a
new event. A rolled-back attempt can call it again, so it must be deterministic and have no
external side effects. Return `FoldIntent` values for effects that should be delivered after commit.

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

Batching groups commits. The current PostgreSQL adapter still issues per-event statements;
it does not combine a batch into one bulk SQL operation.

## Input and reducer contract

A `FoldEvent` names tenant, aggregate, stable event ID, aggregate sequence, event type,
schema version, payload and metadata. These are the only reducer-visible fields.
The kernel canonicalizes the envelope before invoking the reducer and stores canonical
state. Envelope and reducer output must remain within the kernel's canonical numeric domain
after normalization. The kernel checks original integer tokens before JSON parsing can round
them; represent exact large numbers as strings. Transport offset and delivery attempt describe
delivery and do not participate in event identity.

Every new event must have the next aggregate sequence, beginning at one. A no-op still
advances that sequence. The aggregate stays on its original source partition. Both event
identity and aggregate/sequence identity are retained indefinitely. Reusing either with
different content stops that partition. An identical event at a new offset advances only
its checkpoint. Deltas are never compacted.

Reducers and decoders are synchronous, pure, deterministic and bounded by the host.
They must not perform I/O. A reducer returns `FoldResult(state, intents)`; `None` is a real
replacement state. Each `FoldIntent` has a stable key within its event, a destination and
payload. A poison event or sequence gap never advances progress, even if an operator
publishes a diagnostic copy elsewhere.

## Starting state and snapshots

Normal processing loads the last fold state from the store. Recovery reloads that state and the
checkpoint committed with it, then fetches later records. The reducer does not query an external
balance or snapshot service; a changing lookup could produce different results on replay.

Importing an aggregate snapshot as a new fold's starting state is not supported. A snapshot saying
"account A is at sequence 42" does not prove which records from other accounts have been processed.
Even if A's snapshot includes Kafka offset 500, account B may still need its event at offset 400.
Starting the entire partition at 501 would skip B's update. A future snapshot-import contract would
need to establish complete state and matching source progress together.

Each state key belongs to one tenant and aggregate, on one source partition. The runner can process
many such aggregates, but cannot combine their events into one shared balance or impose a global
order across partitions. A [custom projection handler](../sdk/projections.md) owns those rules.
Use the [offline restore procedure](#offline-restore-procedure) to restore an existing fold's complete
state, receipts, outbox and checkpoints from a consistent backup.

## Provision and initialize

Hosts run the returned DDL through their migration tooling. No runtime API creates tables.
The fold table is optional and does not change ordinary PostgreSQL or ingestion tables.

```python
from edax.backend.fold_postgres import PostgresFoldStore, reference_fold_ddl
from edax.fold import FoldTarget, FoldCutover

migration_sql = reference_fold_ddl(schema="billing")
target = FoldTarget(
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
store = PostgresFoldStore(transactor, target, schema="billing")
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
    FoldCutover(
        target.source_identity,
        target.source_history,
        target.topic,
        target.starts,
    )
)
```

## Source verification and runner

Supply a `FoldSource.verify(topic_partition)` capability returning `SourceProof`.
It must obtain supported evidence for cluster/topic incarnation and retained beginning
and readable end. A topic name, nickname, aggregate sequence or consumer position alone
is insufficient. Same-topic truncation must be detected by history evidence, or prevented
by an enforceable no-rollback/no-unsafe-election deployment contract. The SDK has no
permissive default verifier. The current aiokafka consumer API alone does not establish
these identity/history guarantees.

The runner checks evidence before fetching and again after fetching, including empty
fetches. A checkpoint below retained beginning or beyond readable end stops the partition.
Topic recreation, history change or unprovable continuity also stops it. In read-committed
mode, the verifier uses the last stable offset as the readable end.

The application supplies `verified_source` and `decode_event`; the decoder must validate and return
a `FoldEvent`. Pass the `reduce_balance` function above as the reducer:

```python
from edax.backend.kafka_fold import KafkaFold

runner = KafkaFold(client, store, verified_source, decode_event, reduce_balance)
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
Broker commits are unnecessary hints and are never used to reconstruct fold progress.

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

## Ownership and transaction boundary

A lease includes the exact generation/source/topic/partition checkpoint key, owner, epoch,
next offset and expiry. Use a fresh unique owner for each logical acquisition. Retrying that
same acquisition after a lost response is idempotent. Takeover increments the epoch.
A still-current assignment can refresh an expired lease with a fresh epoch. Every write
compares its owner, fence and expected database cursor under the transaction lock.
The runner refreshes after collecting a batch, so healthy fetches cannot spend the lease before
the write starts. Refresh and commit share one operation deadline.

PostgreSQL serializes all transactions in one generation with an advisory transaction lock.
This includes absent-key insertion, ownership changes, freeze, activation and publication
markers. A transaction already holding the lock finishes before takeover. The initial
implementation deliberately has no concurrent per-partition database writes within a
generation. Different generations use different locks.

The adapter uses `PostgresTransactor.run_owned` and refuses ambient transactions. A result
is returned only after commit confirmation. After a lost response, the runner reloads the
database cursor under its valid ownership before refetching. Numeric Kafka offset gaps
are accepted; aggregate sequence gaps are not.

## Outbox and rebuild activation

`FoldOutbox(store, deliver).drain(limit)` publishes one bounded-count page and then records
publication. The destination must atomically deduplicate `OutboxIntent.id` with its business
effect. Delivery is at least once: a lost publication marker or restored backup can repeat it.
The ID derives from effect namespace, tenant, aggregate, logical event ID and intent key.
It excludes generation, lease, source offset and restore incarnation.

Rebuild into a new generation with dispatch disabled. Then:

1. Coordinate a cutover and call `old_store.freeze()`. It drains transactions under the
   generation lock, retires owners and returns the complete source-bound checkpoint vector.
   Only an active generation can provide this handover vector.
2. Let the new generation reach every returned cutoff. It may process a batch across a cutoff.
3. Call `new_store.activate(cutover)`. Activation validates source identity/history and every
   partition, then atomically persists immutable cutoffs. Historical intents are permanently
   suppressed and removed from the pending index. Future replay below a cutoff stays suppressed.
4. The old generation can finish delivery below its frozen upper cutoffs. The new generation
   can deliver only at or above its lower cutoffs. Keep the same effect namespace for the same
   logical effects; a new effect contract needs an explicitly different namespace.

Do not pass a newly invented vector as a replacement for the former generation's committed
checkpoint vector. Re-emitting historical effects is outside automatic activation. Changing
source incarnation invalidates an offset-based handover.

## Offline restore procedure

Restore the entire fold table from one consistent backup. It contains manifests, checkpoints,
state, both deduplication indexes and outbox records. Never mix records from different backups
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
is not supported. `test_fold_restore.py` exercises the offline boundary with terminated
PostgreSQL connections and restored state/checkpoints/deduplication/outbox records.

## Limits and verification

Batch record, serialized-input and age limits use the shared batch kernel. The runner fetches
one record at a time to reserve capacity before decoding. One partition batch executes at a
time; `BatchSettings.concurrency` does not enable concurrent database folds. Pending I/O from
an earlier deadline can coexist but retains its data credits until it actually finishes.

Retention gauges cover prepared serialized inputs and their transaction copies. They do not
bound process RSS, broker prefetch, Python objects, persisted history, loaded aggregate state,
reducer-produced state/intents or external delivery payloads. The host must bound callback
allocation and execution time. Async deadlines cannot preempt synchronous callback code.
Outbox paging bounds record count, not payload bytes. The memory reference retains all history.

Each novel event currently needs roughly five SQL calls inside the generation lock. High-latency
databases may need smaller batches and suitable operation deadlines. No throughput speedup is
claimed. SQL batching or COPY staging requires a separate benchmark and must preserve these
transaction semantics.

Rust transition/boundary vectors are replayed through Python and Go. Python tests cover both
memory and live PostgreSQL, actual lock contention, cancellation rollback, lost COMMIT replies,
and offline restore. The Kafka tests cover late fetches, retention races and assignment fencing.
Go exposes the shared kernels; a Go fold runner and database adapter are not included.

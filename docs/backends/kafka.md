# The Kafka backend

`KafkaProducer` and `KafkaConsumer`: the broker seam over a caller-owned `aiokafka` client. The
per-partition commit watermark makes an offset broker safe to ack out of order, and the rewind keeps
a key in order through a nack.

Kafka backs the two broker seams. The adapters bridge a client you construct and start to
`edax.broker.Producer` and `edax.broker.Consumer` — nothing more, so the base package imports no
broker library. Install with `pip install edax[kafka]`. The broker seam is the Python software
development kit (SDK)'s; the Rust SDK ships no `Producer` / `Consumer` trait.

The adapters decide nothing about the wire. The record's key, its `content-type` header, the media
type naming the value, the partition it lands on, and the offset watermark all live in the
`edax-kafka` kernel. Golden vectors pin
them. This page is the client half; [kernels/kafka.md](../kernels/kafka.md) is the decisions
themselves.

```mermaid
classDiagram
    class Producer {
        <<Protocol>>
        +publish()
    }
    class Consumer {
        <<Protocol>>
        +poll()
        +run()
    }
    class edax_kafka {
        <<kernel>>
        +kafka_record()
        +kafka_partition_for()
        +kafka_route()
        +kafka_dead_letter_record()
        +offset_tracker()
    }
    Producer <|.. KafkaProducer
    Consumer <|.. KafkaConsumer
    KafkaProducer ..> AIOKafkaProducer : partitions_for, send_and_wait
    KafkaConsumer ..> AIOKafkaConsumer : getone, commit, seek
    KafkaProducer ..> Serializer : encode
    KafkaConsumer ..> Serializer : decode
    KafkaProducer ..> edax_kafka : builds the record, names the partition
    KafkaConsumer ..> edax_kafka : routes by the header, books the offset, asks what is due
```

## Constructing them

```
KafkaProducer(client, topic, *, serializer=None, key=None, middleware=())
KafkaConsumer(client, *, serializer=None, serializers=(), middleware=(),
              commit_interval_s=1.0, commit_timeout_s=10.0)
KafkaDeadLetterSink(client, topic)
consumer.rebalance_listener(drain_timeout_s=30.0)
```

`client` is an already-started `AIOKafkaProducer` or an already-started, already-subscribed
`AIOKafkaConsumer`, or any client with the same calls: the `ProducerClient` and `ConsumerClient`
protocols in `edax.backend.kafka` list them. `serializer` defaults to `edax.KernelSerializer("json")` on both
sides. On the consumer it is also the default for a record that carries no `content-type` header.
Everything after the `*` is keyword-only, so adding an argument later cannot shift a positional one.

Building an adapter checks the client's settings that lose data. A consumer that auto-commits, or
has no `group_id`, raises `ValueError`. A producer that acks on the leader alone warns. The probes
read private `aiokafka` attributes, so a client of another library is not judged.

```python
import edax
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from edax.backend.kafka import KafkaConsumer, KafkaDeadLetterSink, KafkaProducer

producer = AIOKafkaProducer(bootstrap_servers="localhost:9092", enable_idempotence=True)
await producer.start()
publisher = KafkaProducer(producer, "orders")

client = AIOKafkaConsumer(
    bootstrap_servers="localhost:9092",
    group_id="billing",
    enable_auto_commit=False,  # the adapter commits the watermark; a timer would commit ahead of it
    auto_offset_reset="earliest",
)
consumer = KafkaConsumer(client)
client.subscribe(["orders"], listener=consumer.rebalance_listener())
await client.start()

dispatcher = edax.KeyOrderedDispatcher(
    workers=8, handler=handle, on_dead=KafkaDeadLetterSink(producer, "orders.dlq")
)
dispatcher.start()
await consumer.run(dispatcher)
```

## The `Producer` contract

Four promises, and each has a failure mode behind it.

| Promise | Why | In the adapter |
|---|---|---|
| `publish` returns only after **every in-sync replica** has the message (`acks="all"`) | the relay marks the outbox row published once `publish` returns; a leader that fails before a follower copied the record takes it along, and the row says it was sent | `await client.send_and_wait(...)`, never fire-and-forget; a client that acks on the leader alone warns |
| On failure `publish` **raises** | the relay then leaves the whole batch unmarked and retries it next drain | the client's exception propagates unchanged; nothing is swallowed |
| The message key is the **aggregate id** | a broker that orders per key delivers one aggregate's events in `sequence` order | the kernel's key rule, applied inside `_edax.kafka_record`; the adapter passes `None` unless you gave it `key=` |
| The partition is **Java's murmur2 of the key** | a second producer, in another language or on another client library, must put one key on the same partition | `_edax.kafka_partition_for(key, len(await client.partitions_for(topic)))`, passed as `partition=` |
| Delivery is **at-least-once** | a crash between publish and `mark_published` republishes the same envelope; consumers dedup by `env.id` | inherent to the outbox; see [outbox](../sdk/outbox.md) |

`publish` is called serially by one relay, so it need not be thread-safe or reentrant.

The key is what preserves order. Kafka orders within a partition, and the key chooses the
partition, so one aggregate's events stay in one partition and arrive in the order they were
published. Override it with `key=` if your partitioning differs, a tenant id for instance.
Per-aggregate order then holds only if that key is still a function of the aggregate id. With no
`key=` the adapter passes no override at all, so the default is the kernel's own rule and
`testdata/golden/kafka_record.json` is what pins it.

Which partition a key chooses is not left to the client library. Client libraries disagree:
aiokafka and every Java client use murmur2, while librdkafka (`confluent-kafka`, `rdkafka`) defaults
to CRC32. Two producers on one topic would then put one aggregate on two partitions, and its order
would break silently. So the adapter asks the kernel, `partition_for` (Java's rule over the record's
final key, after the middleware ran), and passes the answer as `partition=`. Every edax shell does
the same, and `testdata/golden/kafka_partition.json` pins it, Kafka's own Java test vectors
included. The partition count comes from `client.partitions_for(topic)`, so adding partitions to a
topic moves keys, exactly as it does for a Java producer.

The durable-ack rule is asserted by a fake client whose `send` raises `"publish must await the
broker ack (send_and_wait), not fire and forget"`. How strong that ack is remains the client's
setting. Build the producer with `enable_idempotence=True`. It implies `acks="all"`, and a retried
send after a lost ack then writes no duplicate. `aiokafka` defaults to `acks=1` with no idempotence,
so the adapter warns on any `acks` but `"all"`. On the broker, keep `min.insync.replicas` at 2 or more
and unclean leader election off, or `acks="all"` waits for one replica only.

## Headers

Every record the adapter publishes carries one header, whatever serializer wrote the value. The
kernel writes it; the adapter only hands it to the client.

| Header | Value | A reader uses it to |
|---|---|---|
| `content-type` | the media type of the value bytes, for example `application/vnd.edax.envelope+json` | pick the serializer that decodes the value |

The set is closed: a header lands only with a reader for it. The aggregate id is the key already,
the id, event name and version are inside the value, and trace context is the host's own
`traceparent`. Add a header of your own with record middleware, below. The name is exported as
`edax.backend.kafka.CONTENT_TYPE`, read from the kernel. `testdata/golden/kafka_record.json` pins
the header, so a consumer in another language reads the same name and the same bytes.

## The `Consumer` contract

| Promise | What it means |
|---|---|
| `poll` returns the next `Delivery`, or `None` at end of stream | `Delivery.attempt` is the broker's own redelivery count if it keeps one, else `0` |
| **Per-key order** | messages for one `aggregate_id` arrive in publish order. Across keys, any order. |
| **`ack` means exactly this message** | the dispatcher acks out of order, so an offset broker must track a per-partition watermark |
| **`nack` rewinds the partition** | the nacked message and every later one of its partition come back, in order, acked or not. The deliveries already handed out are retired (`Delivery.live` is `False`), so the dispatcher skips them and a key never runs ahead of itself |
| every delivery is settled **at most once**, by ack *or* nack | `ack` and `nack` may be called from any worker task on the same loop, concurrently across keys; a retired delivery is not settled at all |
| nothing comes back on its own | a delivery left unsettled is never redelivered while the consumer lives, so the loop settles every one |
| overall semantics are **at-least-once** | a crash after the handler runs but before the commit redelivers. Handlers must be idempotent. |

Read the `Consumer` seam in `edax.broker`. `run` submits deliveries to the dispatcher until
cancelled or `poll` returns `None`; the `_ConsumerLoop` mixin provides it, so an adapter only writes
`poll`. A poll that raises a transient error does not end the loop: it waits one backoff step and
polls again. A terminal one ends it. `KafkaConsumer.run` also refuses a dispatcher without `on_dead`.
On Kafka a nack rewinds, so a message that fails for good with nowhere to go would stall its
partition forever. It ends when the client is stopped, and commits what settled on the way out.

Kafka keeps no per-message redelivery count, so `KafkaConsumer` leaves `Delivery.attempt` at its
default of `0`. The dispatcher's dead-letter ceiling then rests on an in-process counter, which
resets on restart.

## Custom serializers

A `Serializer` names the media type it writes, as `media_type`. The producer puts that media type
in the `content-type` header; the consumer reads the header back and routes the record to the
serializer registered under it.

```python
import edax
from edax.backend.kafka import KafkaConsumer

consumer = KafkaConsumer(client, serializer=edax.KernelSerializer("json"), serializers=[ProtoSerializer()])
```

`serializer` is the default and claims its own media type like any other. `serializers` are the
extra ones. Three rules follow, and each one has a failure behind it.

- **Register the same media type in every SDK that reads the topic.** A custom format is a
  cross-language agreement, and the media type is its name. Only the name is shared; the code that
  encodes it stays per SDK.
- **A media type no serializer claims is poison.** The consumer raises `CodecError` and the delivery
  dead-letters. It never falls back to the default, because decoding msgpack bytes as the host's own
  format is how a wrong envelope reaches a handler instead of the dead-letter sink. An empty
  `content-type` is a media type nobody claims, and a null one crosses as empty, so both are poison.
- **A record with no `content-type` header uses the default.** Anything published before this
  kernel, or by a client that drops headers, still decodes.

The rule is the kernel's `route`, so every SDK sends one record to the same serializer.
`testdata/golden/kafka_route.json` pins it.

## Record middleware

A middleware rewrites a record on its way to or from the broker. It is synchronous, and it takes
bytes to bytes.

<!-- doclint: check -->
```python
from edax.backend.kafka import KafkaHeader, KafkaRecord


class Traceparent:
    """Stamps one trace context on every published record."""

    def __init__(self, traceparent: str) -> None:
        self._value: bytes = traceparent.encode()

    def on_publish(self, record: KafkaRecord) -> KafkaRecord:
        headers = [*record.headers, KafkaHeader("traceparent", self._value)]
        return KafkaRecord(record.key, record.value, headers)

    def on_poll(self, record: KafkaRecord) -> KafkaRecord:
        return record
```

Pass instances as `middleware=` to either adapter. `on_publish` runs in order, after the kernel has
built the record and before the send. `on_poll` runs in reverse, after the fetch and before the
content-type lookup. A pair of middlewares therefore wraps the record like an onion, and one that
compresses or encrypts the value on the way out unwraps it on the way in. Rewrite `content-type`
(`CONTENT_TYPE`) when you do, and restore it on the way in, so the consumer still routes to the
serializer that wrote the bytes. A middleware may also rewrite the key; the producer places the
record by the key it ends with.

Failures split the way the rest of the SDK splits them. A middleware that raises `CodecError` on
poll marks the message poison, so it dead-letters. Any other exception is transient: the poll seeks
back to the record and raises, and the loop waits one backoff step and polls again. An async hook, a key-management call for instance, is a later addition if a
host asks for one.

## The offset-broker subtlety

**On an offset broker an ack is a per-partition watermark. It is never "commit the current
position."**

The bug that rule prevents is concrete. `KeyOrderedDispatcher` routes by `partition(aggregate_id,
workers)`, so several workers handle one partition's messages concurrently. Worker A may finish
offset 7 before worker B finishes 5. A bare `client.commit()` commits the *current fetch position*,
which is past 7 — and therefore past 5, which is still running. The broker now believes 5 was
handled. Restart the consumer and 5 is never redelivered. The message is lost, silently, and only
under concurrency.

The consumer commits the lowest unsettled offset instead, or one past the highest delivered when none
remain. **That state machine is the kernel's**, not the adapter's: `edax-kafka` keeps the
bookkeeping and answers with a decision, and the adapter performs it.

```python
token = tracker.book(msg.topic, msg.partition, msg.offset)  # before decoding
...
tracker.ack(token)  # settles; never waits on the broker
...
due = tracker.due().items  # the committer, every commit_interval_s
await asyncio.wait_for(client.commit({tp(d): d.offset for d in due}), commit_timeout_s)
for d in due:
    tracker.committed(d.topic, d.partition, d.offset)
```

Two properties follow from that shape.

**Settlement is by token.** `book` returns a token naming one delivery, and `ack` and `nack` take
that token, not a `(partition, offset)` pair. A token retires when its delivery settles, when a
redelivery re-books the same offset, when a nack rewinds its partition, and when `reset` drops its
partition. That closes the
stale-settle hole coordinates leave open. Revoke a partition, call `reset(tp)` from your rebalance
listener, get the partition assigned back, and re-book offset 5. The pre-revoke ack for 5 then
arrives and settles nothing, so it cannot discard the fresh booking.

**Commits are batched, off the ack path.** An ack only settles. One committer task, started by `run`,
commits every partition `due` names in one call every `commit_interval_s`, and once more when `run`
ends. Each commit runs under `commit_timeout_s`: `aiokafka` retries a commit while the coordinator is
away, without end, and the deadline is what stops that from hanging the consumer. `flush()` commits
on demand; call it after `dispatcher.stop()` to commit the drained work before closing the client.

**A commit is recorded only after the client confirms it.** A commit that raised or timed out is
never recorded as done, so the same watermark is due again. The monotonic guard lives in the kernel
too: two commits that returned out of order leave the higher one recorded. `stats()` exposes the
whole record per `TopicPartition`: unsettled count, how many are still live, highest offset
delivered, and last committed watermark.

```mermaid
sequenceDiagram
    autonumber
    participant W as dispatcher workers
    participant C as KafkaConsumer
    participant K as Kafka

    C->>K: getone three times, offsets 0, 1 and 2
    Note over C: unsettled: 0, 1, 2
    W->>C: ack offset 2
    W->>C: ack offset 1
    Note over C: due: nothing, offset 0 is still unsettled
    W->>C: ack offset 0
    Note over C: due: watermark 3, one past the highest delivered
    C->>K: the committer commits 3 in one call
    Note over K: a restart resumes at 3, having lost nothing
```

`test_kafka.py::test_out_of_order_ack_commits_watermark_only` asserts this.
`test_watermark_is_tracked_per_partition` asserts it per partition. One partition's progress must
never settle another's.

`nack` is the mirror image. It seeks the partition back, and it seeks to the **lowest** unsettled
offset, not to this one. `seek` sets the partition's single fetch position, so nacking 5 and then 7
would move it to 7 and 5 would never come back. Every offset from there on is fetched again, so the
nack retires every token of the partition. The copies already queued in the dispatcher are skipped
(`Delivery.live`), and a later event of a key never runs before the redelivered earlier one.
Re-reading acked offsets is at-least-once, not loss. The nacked offset also stays unsettled until its
redelivered copy acks, so no commit passes it.

## End to end

```mermaid
sequenceDiagram
    autonumber
    participant H as command handler
    participant S as EventStore
    participant R as Relay
    participant P as KafkaProducer
    participant K as Kafka
    participant C as KafkaConsumer
    participant D as KeyOrderedDispatcher
    participant E as event handler

    H->>S: append event rows plus outbox rows in one tx
    Note over S: committed together, the transactional Outbox
    R->>S: unpublished, up to the batch limit
    S-->>R: committed rows, oldest first
    R->>P: publish each envelope
    P->>K: send_and_wait, key is the aggregate id
    K-->>P: durable ack
    R->>S: mark_published, after the whole batch
    Note over R,S: a crash before this republishes, at-least-once
    C->>K: getone
    K-->>C: message on a partition, at an offset
    C->>D: submit a Delivery
    D->>E: worker is partition of the aggregate id
    alt the handler returns
        D->>C: ack
        C->>K: the committer commits the per-partition watermark
    else the handler raises
        D->>C: nack, after one backoff step
        C->>K: seek to the lowest unsettled offset
        Note over C,D: the partition's queued copies are retired and skipped
    end
```

The relay marks the batch published only after every row in it was published. That ordering is what
makes the gap at-least-once rather than lossy.

## What the adapter needs, and what it leaves to you

It needs six things from the client library:

| Call | Used for |
|---|---|
| `await client.partitions_for(topic)` | the topic's partition count, for the kernel's placement; aiokafka answers from cached metadata |
| `await client.send_and_wait(topic, value=, key=, partition=, headers=)` | `publish`, with the broker's ack awaited; `headers` is a list of `(name, bytes)` pairs |
| `await client.getone()` | `poll`; the message must expose `topic`, `partition`, `offset`, `value`, `key` and `headers` |
| `await client.commit({tp: offset, ...})` | committing every due watermark in one call, under the commit deadline |
| `client.seek(tp, offset)` | redelivery on `nack`; synchronous |
| `aiokafka.TopicPartition` | the bookkeeping key, imported lazily so the base package pulls no broker library |
| `aiokafka.ConsumerRebalanceListener` | the base of `rebalance_listener()`, imported when you ask for one |

It deliberately leaves the rest to the host:

- **The client itself.** Construct, configure, start, subscribe and stop an `AIOKafkaProducer` or
  `AIOKafkaConsumer` yourself. The adapters take an already-started, already-subscribed client.
- **Durability.** The `acks` setting decides how strong the ack that `send_and_wait` waits for is.
  The `Producer` contract asks for every in-sync replica: build the producer with
  `enable_idempotence=True`, as in the example above.
- **Committing.** The adapter issues its own commits, and the consumer it is handed must not issue
  any of its own. `aiokafka` commits on a timer by default. `enable_auto_commit` defaults to `True`,
  and it commits the *fetch* position, which runs ahead of what has been handled. That is the exact
  bug the watermark exists to avoid: an offset still on a worker is reported as done, and a restart
  never redelivers it. So `KafkaConsumer` refuses a client that auto-commits, and one with no
  `group_id`, whose every commit would fail.
- **Timeouts.** `getone` blocks until a message arrives, by design. A send is bounded by the client's
  `request_timeout_ms` and batch expiry. A commit is not, so the adapter bounds it with
  `commit_timeout_s`.
- **Rebalance.** The watermark bookkeeping holds only while the partition is assigned. Pass
  `consumer.rebalance_listener()` to `subscribe`. On revoke it commits what is due, resets the
  revoked partitions so the queued deliveries are skipped, and waits for the handlers still running,
  up to `drain_timeout_s`. No key is then handled by two members at once. The new owner starts from
  the commit, so the skipped work runs there: at-least-once. Keep `drain_timeout_s` above the slowest
  handler and below the client's `max_poll_interval_ms`. Without the listener, a revoked partition
  keeps its stale bookkeeping, and the queued work of it runs alongside the new owner.
- **Threads.** `KafkaConsumer` keeps its offset bookkeeping in native state bound to the thread that
  built it. Build the consumer on the thread that runs its event loop. A consumer built on the main
  thread and run in `threading.Thread(target=asyncio.run, ...)` raises the terminal `BadHandle` on
  its first booking, where it used to panic past every `except Exception`.
- **Topics and partitions.** Naming, partition count and retention are yours. Partition count bounds
  real parallelism; `workers` on the dispatcher is local parallelism only.
- **The wire format.** Pass any `edax.aggregate.Serializer`. `edax.KernelSerializer()` is the kernel's envelope
  format, `"json"` or `"msgpack"`. The value bytes are the serializer's. The key, the partition, the
  header and the media type in `content-type` stay the kernel's, so a custom format keeps the same
  record shape. Two serializers claiming one media type are refused with `AlreadyRegistered`: one
  topic would otherwise decode two ways.
- **Dead-lettering.** `KeyOrderedDispatcher`'s `max_attempts` and `on_dead` own that, and
  `KafkaConsumer.run` requires `on_dead`. `KafkaDeadLetterSink(producer, topic)` is a ready sink. It
  publishes the record as it was fetched, before any middleware: key, value and every header
  verbatim, plus `edax-dlq-topic`, `edax-dlq-partition`, `edax-dlq-offset` and `edax-dlq-error`. A
  republish to the source topic after the fix then routes and decodes as the original did. The record
  rides on `Delivery.origin` as a `KafkaFetched`, so a sink of your own calls
  `delivery.origin.dead_letter(error)` for it. The kernel's `dead_letter_record` builds it, and its
  `dead_letter_source` reads the coordinates back. A payload that will not decode reaches the
  sink with the stand-in id `topic/partition/offset`. A sink that raises nacks the message after one
  backoff step, so the redelivery gives it another try. If the sink then sees one offset twice, the
  coordinates are the dedup key.

## Testing it

The seam contract, `edax.testing.BrokerContract`, runs over every in-tree adapter, in-memory and
Kafka alike, in `crates/edax-py/tests/test_broker_contract.py`. Its clauses: per-key order, a nack
that keeps the key's order, redelivery from the commit after a crash, and consumer groups. `test_broker_model.py` drives
both under random publishes, failures, commits and crashes. Kafka-only semantics live in
`crates/edax-py/tests/test_kafka.py`: the watermark and the committer, the producer's key, topic and
durability probe, the registry, the middleware, the dead-letter record, and the loop under faults.

Those run on a Kafka-shaped fake in `tests/conftest.py`. It holds partitioned logs and a group's
commits, and it redelivers only on a seek, a rebalance or a restart. It revokes eagerly through the
listener, refuses a seek or a commit on a partition the member does not hold, and can freeze its
commits.
`crates/edax-py/tests/test_kafka_live.py` runs the same faults against a real broker when
`EDAX_KAFKA_BOOTSTRAP` is set, and `EDAX_KAFKA_CONTAINER` adds a frozen broker and a restart.

The kernel's own contract is replayed in `crates/edax-py/tests/test_edax.py`. Five tests read
`testdata/golden/` through `_edax`: `test_golden_kafka_record`, `test_golden_kafka_decode` (through
a real `KafkaConsumer`), `test_golden_kafka_route`, `test_golden_kafka_dead_letter` and
`test_golden_kafka_offsets`. `test_kafka.py` replays every `kafka_partition.json` row through a real
`KafkaProducer`. Those are what prove this SDK and a second one agree on the record,
the partition and the watermark.

## See also

- [outbox](../sdk/outbox.md) — outbox rows, `Relay`, `pump`, and where `publish` is called from.
- [consuming](../sdk/consuming.md) — `KeyOrderedDispatcher`, `EventRouter`, and who calls `ack`.
- [backends index](./README.md) — the seam contracts and the checklist for writing one.
- [kernels/kafka.md](../kernels/kafka.md) — the record, the header, placement, the media types and
  the offset state machine, decision by decision.
- [memory](./memory.md) — `MemoryBroker`, the same seams with no infrastructure.
- `examples/py/17_kafka.py` — the outbox relay into `KafkaProducer`, a `KafkaConsumer` with a
  `KafkaDeadLetterSink`, against a real broker when `EDAX_KAFKA_BOOTSTRAP` is set.
- [delivery guarantees](../guides/delivery-guarantees.md) — the end-to-end guarantee matrix.

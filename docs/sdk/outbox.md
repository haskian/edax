# The transactional Outbox

How `EventStore.append` writes to-publish rows beside the event rows, and how a `Relay` or a `pump`
gets them to a broker.

A service that changes state and publishes an event has two systems to write to. It cannot write to
both atomically. The transactional Outbox removes the second write from the critical path. The event
and its to-publish row commit together in one database transaction. A separate process moves the row
to the broker afterwards.

```mermaid
flowchart LR
    H["handler"] -->|"Repository.save"| A["EventStore.append"]
    A -->|"event rows"| DB[("one transaction")]
    A -->|"outbox rows, same commit"| DB
    DB -->|"unpublished"| RL["Relay.drain — polling"]
    DB -->|"write-ahead log"| CDC["change data capture"]
    CDC -->|"decoded envelopes"| PM["pump"]
    RL -->|"publish"| B["Producer, a broker"]
    PM -->|"publish"| B
    RL -->|"mark_published"| DB
```

## Why a dual write cannot be made atomic

Write the row, then publish. The process dies in between: the state changed and nobody was told.
Publish, then write the row. The process dies in between: consumers acted on an event whose state
never committed. Reordering does not help, because there is no ordering of two independent commits
that survives a crash between them. Making them one commit needs a distributed transaction across a
database and a broker, which edax does not use and most brokers do not offer.

The outbox avoids it. There is only one commit, because the to-publish row **is** a database row
written by the same store, in the same transaction, as the state it announces.

`Outbox` (a Python `Protocol`, a Rust trait) is only the *read* side a `Relay` drains: `unpublished`
and `mark_published`. There is no outbox you write to on its own. An `EventStore` is an `Outbox` of
envelopes, written by `append`; a saga store is an `Outbox` of `SagaCommand`s, written by the step's
`save` or `complete`. So no backend can split the state write and the to-publish write across two
systems by accident.

The remaining gap is the publish itself, which is still a separate step and therefore still
at-least-once. That gap is bounded and recoverable: the row is durable, so a redelivery is a
duplicate, never a loss.

## Stored shapes

```mermaid
erDiagram
    AGGREGATE_STREAM ||--o{ EVENT_ROW : "ordered by sequence"
    AGGREGATE_STREAM ||--o| SNAPSHOT_ROW : "at most one, may lag never lead"
    EVENT_ROW ||--|| OUTBOX_ROW : "same envelope, same commit"

    EVENT_ROW {
        string id PK "envelope id"
        string aggregate_id FK
        int sequence "unique per aggregate_id"
        string event_name
        int event_version
        bytes event_data "host-serialized payload"
        map namespace
        map metadata
        int timestamp "unix nanoseconds"
    }
    OUTBOX_ROW {
        string id PK "the same envelope id"
        string aggregate_id "the broker message key"
        bytes envelope "the wire form"
        int published_at "null until mark_published"
    }
    SNAPSHOT_ROW {
        string aggregate_id PK
        string type_name
        int version
        bytes data
    }
```

`EVENT_ROW` and `SNAPSHOT_ROW` mirror `EventEnvelope` and `Snapshot`. The outbox row's primary key
is the **same envelope id** as the event row's; that shared id is what a consumer deduplicates on
after a redelivery. The id is unique store-wide: an `append` carrying an id the store already holds
is refused with `CodecError` and writes nothing. Without that, two rows share an id, and marking the
first published prunes the second before it was ever sent. The primary key gives it on SQL; the
in-memory store checks it (`EventStoreContract.test_an_envelope_id_already_stored_is_refused`).

A snapshot may lag the stream but never lead it. `save_snapshot` refuses one past the stream's
version as the transaction sees it, as `CodecError`, and keeps the higher version when an older one
arrives late (`test_a_snapshot_leading_the_stream_is_refused`,
`test_an_older_snapshot_never_replaces_a_newer_one`). `Repository.load` treats a snapshot as a
cache: one whose `type_name` is not this codec's, or that does not load, is skipped, and the stream
replays from version 0. The replayed run is checked with the kernel's dense-batch rule, so a store
that returns a gap or a repeat fails the load instead of folding the wrong state.

`published_at IS NULL` is the shape the `EventStore` docstring recommends for a polling relay on a
SQL database. It is a suggestion, not a requirement. The in-memory adapter instead prunes a row from
the outbox when it is marked. The outbox then holds only unpublished rows. `unpublished` becomes a
bounded prefix scan rather than a filter over a growing log.

## The row lifecycle

```mermaid
stateDiagram-v2
    state "staged in tx" as Staged
    state "unpublished" as Unpub
    state "published" as Pub

    [*] --> Staged: append writes the event row and the outbox row under one tx
    Staged --> [*]: rollback drops both, nothing was ever visible
    Staged --> Unpub: commit lands both together, durably
    Unpub --> Unpub: publish raised, or the mark_published response was lost — drained again
    Unpub --> Pub: mark_published with this envelope id
    Pub --> Pub: mark_published again changes nothing
    Pub --> [*]: pruned, or retained by the backend
```

`unpublished` returns only **committed** rows, oldest commit first, and at minimum one aggregate's
rows in `sequence` order. A row staged inside an open transaction is not a row to publish; the
in-memory store enforces that by reading the outbox outside any transaction token at all.

### mark_published is idempotent

> `mark_published` is idempotent and durable. Both run outside any `tx`.
> — the `EventStore` contract (`edax.store`); Rust's `Outbox::mark_published` says the same

Idempotent means three things concretely, and all three are tested:

| Call | Must do |
|---|---|
| ids already marked | nothing, no raise |
| ids the store never saw — a change-data-capture drain marking rows by id | nothing, no raise |
| an empty id list | nothing, no raise |

None of the three may touch the rows still waiting to publish.

This matters because the relay's own acknowledgement can be lost. The relay publishes a batch, calls
`mark_published`, and the response never arrives. It has no way to tell "the mark landed" from "the
mark did not land". So it re-drains, publishes the same rows again, and marks them again. Both
repeats must be harmless: the second publish is caught downstream by the dedup on `env.id`, the
second mark is caught by idempotence here.

## Relay.drain — polling

```mermaid
sequenceDiagram
    participant L as relay loop
    participant R as Relay
    participant K as the kernel's retry policy
    participant S as Outbox seam
    participant P as Producer, a broker

    L->>R: drain batch
    activate R
    R->>S: unpublished limit equals batch
    S-->>R: committed rows not yet marked, oldest first
    loop each row, in order
        R->>P: publish row
        alt the broker durably accepted it
            P-->>R: ok
        else publish raised
            P-->>R: error
            R->>K: failed row id, terminal or transient
            alt dead: terminal, or max_attempts reached, and on_dead is set
                R->>R: on_dead row, then go on with the next row
            else handed back
                Note over R: stop the batch: the rows behind it may be its aggregate's later events
            end
        end
    end
    R->>S: mark_published every row settled, published or dead-lettered
    Note over R,S: the window between a publish and mark_published<br/>is where a crash causes a re-publish
    alt a row was handed back
        R->>R: wait the row's backoff step, then raise its error
    else
        R-->>L: how many rows were settled
    end
    deactivate R
```

```python
published = await edax.Relay(store, events.publish).drain()
```

`examples/py/outbox.py:81`. `drain` publishes one batch and returns how many rows it settled. To keep
draining, run the relay's own loop rather than writing one:

```python
stop = asyncio.Event()
task = asyncio.create_task(relay.run(idle_s=1.0, stop=stop))  # set `stop` (or cancel) to end it
```

`run` drains back to back while rows flow. After a drain that settled nothing, it waits `idle_s` or until
`stop`. After a failed drain it goes again: that drain already waited its own backoff step and reported to
the observer. So one bad row never ends the relay. Rust's `Relay::drain(batch, publish)` takes the publish closure per call
instead of at construction; `publish` there is synchronous, so wrap it in your async runtime for a
network transport. Both relays drain any `Outbox`: the event store's, and a saga store's command
outbox (see [sagas.md](./sagas.md)).

**What `drain` needs from the store**

| Requirement | Why |
|---|---|
| `unpublished` returns committed rows only | a staged row may still roll back |
| oldest commit first, one aggregate's rows in `sequence` order | per-key ordering downstream |
| rows stay until `mark_published` | a crash mid-batch re-delivers rather than drops |
| `mark_published` is idempotent and durable | the acknowledgement itself can be lost |
| row ids are unique | `mark_published` by id must not prune a row that was never sent |

**What `drain` needs from the `Producer`**

| Requirement | Why |
|---|---|
| `publish` returns only after a **durable** broker ack | returning early lets the row be marked before the message exists — a lost event |
| `publish` **raises** on failure, never swallows | the relay hands the row back and retries it |
| the message key is `aggregate_id` | a broker that orders per key delivers one aggregate's events in `sequence` order |

Read the `Producer` seam in `edax.broker`. The relay calls `publish` serially, so an adapter need
not be thread-safe or reentrant.

### A row that fails

A drain marks every row it settled, also when a later row fails, so the published prefix is not
published again. The failing row stops the batch. Whether it is handed back or dead-lettered is the
kernel's retry policy (`edax_event::Retries`, pinned by `testdata/golden/retries.json`), the same one
the `KeyOrderedDispatcher` runs, keyed by row id:

| The publish failure | `on_dead` set | Result |
|---|---|---|
| terminal (an over-size message, a codec error) | yes | dead-lettered and marked; the rows behind it go out in the same drain |
| transient, at the `max_attempts`-th failure of that row | yes | dead-lettered and marked |
| transient, below the ceiling (or `max_attempts=0`) | either | handed back; the drain waits the row's backoff step, then raises |
| terminal | no | handed back with a growing wait, on every drain |
| `InProgress` (a guarded command another dispatcher holds) | either | handed back without spending an attempt |
| `on_dead` itself raises | — | handed back; the next drain gives the sink another try |

So one poison row no longer stalls the outbox: wire `on_dead` and it moves aside. Without a sink the
relay still cannot skip it, because publishing the rows behind it would reorder its aggregate. The
growing wait keeps a host's drain loop from spinning on it. `relay.last.dead_lettered` counts the
rows a drain moved aside.

**Run one relay per store.** The `Outbox` contract does not lease rows. Two relays would read
the same rows and double-publish. For N relays, make `unpublished` claim rows. Use a row lock that
skips rows already locked and is held until `mark_published`, or a `leased_until` column.

Overlapping drains of **one** relay take turns: the Python `Relay` holds an `asyncio.Lock`, the Rust
`Relay` a `Mutex`, so a timer and a "drain now" cannot double-publish. That lock is per relay, not per
store. It does not help across processes.

**A dispatcher `submit` is not a valid `publish`.** `submit` only enqueues the envelope. A crash
between the enqueue and the handler loses the event, which turns the outbox into at-most-once.
`publish` must return only after the transport durably accepted the event.

## pump — fed from change data capture

Change data capture (CDC) reads the database's own write-ahead log. Because `append` already wrote
the outbox row inside the transaction, a log consumer sees that row exactly when the transaction
commits — no polling, no second query.

```mermaid
sequenceDiagram
    participant W as write-ahead log
    participant C as your CDC client
    participant PU as pump
    participant P as Producer, a broker

    C->>W: stream changes from the last committed position
    loop each decoded envelope
        W-->>C: an outbox row, at its commit point
        C->>PU: yield the envelope through the async source
        PU->>P: publish envelope
        alt published
            P-->>PU: ok
            PU-->>C: the pump advances
            C->>C: commit the stream position — this is the ack
        else publish raised
            P-->>PU: error
            Note over PU,C: the pump stops at that row and the position is not advanced,<br/>so resuming re-publishes from the last committed one
            PU-->>C: the error propagates
        end
    end
```

```python
async def wal() -> AsyncIterator[Any]:
    for i in (1, 2, 3):
        yield build_envelope("acct", "Tick", i, id=f"e{i}")


published: list[str] = []
assert await pump(wal(), lambda env: published.append(env.id)) == 3
assert published == ["e1", "e2", "e3"]
```

`test_store.py` pins that. `pump` publishes every envelope from the source until the source ends,
and returns how many.

**What `pump` needs from the store**: almost nothing. `unpublished` and `mark_published` may be
no-ops, and row retention is yours to manage. What it needs instead is a `source`: an async iterable
of `EventEnvelope`, decoded from the log with your `Serializer`.

**What `pump` needs from the `Producer`**: the same three guarantees `Relay` needs. `publish` may be
sync or async in both cases.

There is no `mark_published` in this mode. The **stream position is the acknowledgement**, so commit
it in your CDC client only after `pump` has passed the row. A raising `publish` stops the pump;
resume from the last committed position for at-least-once. The branch table pins that behaviour:
publishing `e1, e2, e3` with a failure on `e2` leaves `["e1", "e2"]` attempted, raises, and returns
no count.

### Choosing between them

| | `Relay.drain` | `pump` |
|---|---|---|
| trigger | your polling loop | the database's write-ahead log |
| latency | one poll interval | the commit itself |
| needs from the store | `unpublished` plus `mark_published` | neither |
| acknowledgement | `mark_published` by envelope id | the CDC stream position |
| concurrency | one relay per store, unless `unpublished` leases rows | whatever your CDC client supports |
| row retention | handled by `mark_published` | yours |

Satisfy one of the two; the `EventStore` contract requires exactly that.

## How deep is the backlog

`Relay.last` is the lag of the drain that finished. It stands still when the relay does, so a dead
relay and an idle system read the same from outside. The number that separates them is the depth of
the outbox, and a store may offer it:

```python
async def unpublished_count(self) -> int:
    """Committed unmarked rows waiting, outside any transaction."""
```

This is an optional seam method, not part of the `EventStore` Protocol: requiring it would break
every store written before it existed. Read it with `getattr(store, "unpublished_count", None)`, and
set `supports_unpublished_count = True` on your `EventStoreContract` subclass to run the clause that
checks it. `MemoryEventStore` implements it.

In SQL it is `SELECT count(*) FROM outbox WHERE published_at IS NULL`. On a large table prefer an
estimate or a partial index over an exact scan on every scrape: the alert reads a trend, not a
number. The depth is the correctness lag of every downstream projection and saga, so it belongs in
the same dashboard as consumer lag.

## At-least-once publication

The same envelope can reach the broker more than once. Three windows produce it:

| Window | What happens |
|---|---|
| a crash between `publish` and `mark_published` | the row is still unpublished; the next drain publishes it again |
| a lost `mark_published` response | the relay cannot tell; it re-drains and re-publishes |
| a CDC client resuming from an uncommitted position | the log replays from there |

Publishing twice is correct behaviour, not a bug to remove. The alternative — mark first, publish
after — turns a duplicate into a **lost event**, which no downstream can recover. edax picks the
recoverable failure, in both modes.

What catches the duplicate is downstream, never the relay:

- consumers deduplicate by `env.id`, or by `dedup` on `sequence`.
- the idempotency guard claims the event id before the handler runs — see
  [idempotency](./idempotency.md).
- the end-to-end statement of what that adds up to is in [delivery
  guarantees](../guides/delivery-guarantees.md).

## Worked example

```python
async def handle(env: edax.EventEnvelope) -> None:
    # same aggregate -> same worker -> serialized: no lock around this read-modify-write
    agg = env.aggregate_id
    balances[agg] += json.loads(env.event.data)["amount"]
    v = version[agg]
    data = json.dumps({"balance": balances[agg]}).encode()
    async with edax.transaction(store) as tx:
        await store.append(tx, agg, v, [_env(f"{agg}-ev-{v + 1}", agg, "Deposited", v + 1, data)])
    version[agg] = v + 1


dispatcher = edax.KeyOrderedDispatcher(workers=4, handler=handle)
dispatcher.start()
for _ in REQUESTS:
    await dispatcher.submit(await _poll(commands))  # worker acks after the append
await dispatcher.stop()

published = await edax.Relay(store, events.publish).drain()
```

`examples/py/outbox.py:63`. Two things to notice. The input delivery is acked only **after** the
append commits, so a crash before the commit redelivers the input rather than dropping the work.
`MemoryBroker` is both `Producer` and `Consumer`. The same wiring runs against a real broker once
you swap in an adapter over your own client. The shape does not change.

`examples/py/command_handling.py:114` shows the other half: the outbox holds exactly the events that
committed. A dispatch that replayed from the idempotency ledger, one rejected for a payload mismatch,
and an attempt that failed after its save and rolled back wrote nothing at all.

## See also

- [Aggregates](./aggregates.md) — `Repository.save`, which is what calls `append`.
- [Transactions](./transactions.md) — the unit of work the event rows and outbox rows share.
- [Idempotency](./idempotency.md) — the guard that catches the redelivered envelope.
- [Delivery guarantees](../guides/delivery-guarantees.md) — the end-to-end matrix.
- [Consuming](./consuming.md) — `KeyOrderedDispatcher`, the other end of a published event.
- [Kafka backend](../backends/kafka.md) — a real `Producer` behind `Relay`.

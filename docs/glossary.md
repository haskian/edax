# Glossary

Every term this documentation uses, defined once. When a doc uses one of these words, it means
exactly this.

## Layers

**Kernel** — a pure Rust crate that decides something every service must agree on, bit for bit.
`edax-core`, `edax-event`, `edax-cqrs`, plus the per-technology `edax-kafka` and `edax-redis`. No
I/O, no threads, no callbacks, synchronous. See [kernels](kernels/README.md).

**Protocol module** — a kernel crate named `edax-<tech>` that owns one technology's shared
decisions: its bytes, its names and its state machines. `edax-kafka` and `edax-redis` are the two
that ship. It links no client library.

**I/O shell** — the per-SDK adapter over a protocol module. It calls the client and the kernel, and
decides nothing in between. `edax.backend.kafka` and `edax.backend.redis` are the two Python ones.

**SDK** — software development kit, the per-language layer that wires the kernels to a host's
storage and transports. `crates/edax` (Rust) and `crates/edax-py/python/edax` (Python). The SDK
wires; the kernel decides.

**ABI** — application binary interface. `crates/edax-abi`, a flat Rust surface over the kernels. The
Python native module is generated from it. See [architecture](architecture.md).

**Seam** — a `Protocol` (Python) or trait (Rust) a host implements against its own database or
broker: `EventStore`, `ProjectionStore`, `IdempotencyStore`, `SagaStore`, `Transactor`, `Producer`,
`Consumer`. Its docstring is the specification.

**Backend** — a concrete implementation of one or more seams. Three ship: `memory`, `redis`,
`kafka`. See [backends](backends/README.md).

**Golden vector** — a JSON file under `testdata/golden/` generated from a kernel and asserted by
every SDK's test suite. A golden vector always wins over a disagreeing test. See [golden
vectors](kernels/golden-vectors.md).

## Write side

**Aggregate** — a consistency boundary with an id, a version, and a fold from events to state.

**Aggregate id** — the routing and ordering key. It decides the partition, and it is the broker
message key.

**Envelope** — the wire form of a stored event: the event itself plus id, aggregate id, sequence,
namespace, metadata and timestamp. `EventEnvelope` in Rust, `edax.EventEnvelope` in Python.

**Optimistic concurrency** — `EventStore.append` and `ProjectionStore.save` are database-enforced
compare-and-set operations on a version. A stale writer gets `ConcurrencyError` and nothing is
written.

**Snapshot** — a serialized aggregate state stored so a load replays fewer events.

**Transactional Outbox** — writing the events and the rows to publish in one transaction, so
committed state and published events cannot diverge. `Relay` and `pump` drain it. See
[outbox](sdk/outbox.md).

## Transactions

**Unit of work** — one transaction boundary that several stores share. Opened with
`edax.transaction(transactor)` in Python.

**Ambient transaction** — the open handle a Python participant finds through
`current_tx(transactor)`, keyed by its database and carried in a `contextvars` variable. Rust has
none. A Rust host passes the handle explicitly, or publishes it to the guard through a
`Fn(&C) -> Tx` provider keyed by the command.

**`joins_transaction`** — an optional attribute an `IdempotencyStore` declares to say whether it can
enlist in the caller's transaction. It defaults to `False`, which is the safe side. See
[transactions](sdk/transactions.md).

## Idempotency

**Guard** — the idempotency middleware: `idempotent` on the command path, `idempotent_by_id` on
the consume path. The Rust SDK adds `idempotent_in` and `idempotent_by_id_in`, which take the
host's transaction handle.

**Ledger** — the `IdempotencyStore` the guard writes to.

**Idempotency key** — the string one intent is claimed under. There is no command id. On the command
path the key is a command *field* the host's `key` function reads, which makes it the request id a
client retries with. On the consume path it is the envelope's `id`.

**Claim** — the atomic insert-if-absent that reserves an idempotency key. It must be linearizable
across processes, never a read then a write.

**Record** — what is stored under a key: `pending`, `done` or `failed`, plus the payload hash and
the outcome.

**Payload hash** — the kernel's hash of the whole command value, which binds a key to one payload.
Reusing a key with different bytes is a `PayloadMismatch`.

**Replay** — returning a cached outcome instead of running the handler again.

**Dedup window** — how long a record is retained, and therefore how long a redelivery is caught.

**Shared ledger** — the guard mode where the ledger commits inside the work's own transaction. The
rollback is the release. See [idempotency](sdk/idempotency.md).

**Own ledger** — the guard mode where the ledger commits separately from the work, for example a
Redis ledger beside your aggregate database. Two independent commits cannot be made atomic.

**Claim TTL** — how long a `pending` claim survives without a `complete`. It bounds how long a
crashed worker blocks a key.

**Claim handle** — what a won claim returns (`Won(handle)`, `Claim::Won(ClaimHandle)`) and the
claimant presents on `complete`, `release` and `extend`. In the Redis ledger it is the exact value the
claim wrote.

**Fence** — the check a store applies to a claim handle. The call writes only while the key still
holds that exact claim. A claimant whose claim lapsed then cannot settle the new holder's claim, and a
late release cannot delete a settled record.

**Idempotent Consumer** — the pattern edax implements: a dedup table beside the business change.

**Transactional Inbox** — a pattern edax does not implement. An inbox stores the message body
and acks the broker before processing. edax stores a key, a payload hash and an outcome, never the
message.

## Delivery

**At-least-once** — a message may arrive more than once, never zero times.

**At-least-once with a dedup window** — at-least-once plus a ledger that catches repeats for as long
as the record is retained. What you get when the ledger commits separately from the work.

**Effectively-once** — at-least-once delivery plus deduplication that makes the effect land once.
What you get when the ledger shares the work's transaction.

**Exactly-once** — edax does not promise it, and neither does any system built on two independent
commits.

**Partition** — the kernel's local worker bucket for an aggregate id, `fnv1a32(id) % W`. It
selects a dispatcher worker, so one aggregate stays ordered while other aggregates run in parallel.
Broker placement is a separate decision: `edax-kafka`'s `partition_for` uses Kafka's murmur2 rule.

**Settle** — to finish a delivery with `ack` or `nack`. Every delivery is settled exactly once.
`ack` settles only that message; `nack` redelivers.

**Dead-letter** — where a terminal failure, or one that exhausted its attempt budget, is sent.

**Watermark** — the per-partition offset a consumer has committed. An `ack` advances it. It never
means "commit the current position".

**Poison message** — a message that fails deterministically on every delivery, for example one that
does not decode. It must reach the dead-letter path, never a redelivery loop.

**Media type** — the name a wire format goes by between services (RFC 6838, once "MIME type"), for
example `application/vnd.edax.envelope+json`. A Kafka record carries it in the `content-type` header,
and a consumer routes the record to the serializer registered under it. A media type no serializer
claims makes the message poison, never a fallback. Not a synonym for the header: `content-type` is
the field name, the media type is its value.

**Vendor tree (`vnd.`)** — the RFC 6838 prefix for a product-specific media type, as opposed to an
IANA standard such as `application/json`. `application/vnd.edax.envelope+json` reads: vendor `edax`,
format `envelope`, parse as JSON (the `+json` suffix, RFC 6839). Registration is optional in this
tree and has not been done. A custom serializer names its format the same way,
`application/vnd.<vendor>.<format>+<syntax>`.

## Failure

**Transient** — worth retrying: `StoreError`, `ConcurrencyError`, `InProgress`.

**Terminal** — never worth retrying: `CodecError`, `PayloadMismatch`, `CachedFailure`,
`AlreadyRegistered`, `HandlerNotFound`, `BadHandle`, `Rejected`.

The kernel's `ErrorCode` carries the verdict, so no SDK restates it. See [errors](sdk/errors.md).

## Read side

**Projection** — a read model built by folding events into an `Entity`.

**Entity** — id, version, document. The version makes `save` a compare-and-set.

**Filter** — the backend-neutral query abstract syntax tree (AST) in `edax-cqrs`, plus the reference
evaluator that pins the match verdict. A missing or null path makes a leaf false, which is why
`ne(p, x)` and `not_(eq(p, x))` differ.

**Keyset cursor** — a base64url position token from the kernel's `query`, portable between services. A
unique `id` tiebreak is appended automatically so paging never drops or repeats tied rows.

**Upcast** — raising a stored event from its written version to the current one. The kernel plans
the ordered steps and rejects a gap; your code rewrites the payload.

**Causation** — `correlation_id` and `causation_id` propagation, carried in metadata so the wire
format is untouched.

**Correlation id (metadata)** — `metadata["correlation_id"]`, constant across one logical flow. Not
the same thing as a saga's instance key, below.

## Patterns

**Process manager** — the same thing this documentation calls a saga: a workflow that correlates
events, keeps its own state, and emits commands.

**Saga instance key** — what `Saga.correlation_id(env)` returns: which running instance this event
belongs to, and the id the `SagaStore` is keyed by. It shares a word with the metadata
`correlation_id` and is a different value — that one names a flow, this one names stored state.
Nothing derives either from the other. It is often the aggregate id; see
[sagas](sdk/sagas.md#the-correlation-rule).

## Abbreviations

Every short form this documentation uses. Each page also expands the form on its own first use.

| Short form | Expansion |
|---|---|
| ABI | application binary interface |
| API | application programming interface |
| AST | abstract syntax tree |
| CDC | change data capture |
| CI | continuous integration |
| CQRS | Command Query Responsibility Segregation |
| FIFO | first in, first out |
| FNV-1a | Fowler-Noll-Vo hash, variant 1a |
| I/O | input and output |
| LRU | least recently used |
| NTP | Network Time Protocol |
| SDK | software development kit |
| SQS | Amazon Simple Queue Service |
| TTL | time to live |
| UUID | universally unique identifier |
| XA | eXtended Architecture, the two-phase commit standard |

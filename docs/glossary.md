# Glossary

Every term this documentation uses, defined once. When a doc uses one of these words, it means
exactly this.

## Layers

**Kernel** — a pure Rust crate that decides something every service must agree on, bit for bit.
`edax-core`, `edax-event`, `edax-cqrs`. No I/O, no threads, no callbacks, synchronous. See
[kernels](kernels/README.md).

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
namespace, metadata and timestamp. `EventEnvelope` in Rust, `edax.Envelope` in Python.

**Optimistic concurrency** — `EventStore.append` and `ProjectionStore.save` are database-enforced
compare-and-set operations on a version. A stale writer gets `ConcurrencyError` and nothing is
written.

**Snapshot** — a serialized aggregate state stored so a load replays fewer events.

**Transactional Outbox** — writing the events and the rows to publish in one transaction, so
committed state and published events cannot diverge. `Relay` and `pump` drain it. See
[outbox](sdk/outbox.md).

## Transactions

**Unit of work** — one transaction boundary that several stores share. Opened with
`edax.transaction(store)` in Python.

**Ambient transaction** — the open handle a Python participant finds through `current_tx()`, carried
in a `contextvars` variable. Rust has none; a Rust host passes the handle explicitly or publishes it
through a `Fn() -> Tx` provider.

**`joins_transaction`** — an optional attribute an `IdempotencyStore` declares to say whether it can
enlist in the caller's transaction. It defaults to `False`, which is the safe side. See
[transactions](sdk/transactions.md).

## Idempotency

**Guard** — the idempotency middleware: `idempotent` and `idempotent_in` on the command path,
`idempotent_by_id` and `idempotent_by_id_in` on the consume path.

**Ledger** — the `IdempotencyStore` the guard writes to.

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

**Fence** — a token a claimant holds and presents on `complete` or `release`. The store applies the
call only when the token matches, so a losing claimant cannot settle the winner's claim.

**Idempotent Consumer** — the pattern edax implements: a dedup table beside the business change.

**Transactional Inbox** — the pattern edax does **not** implement. An inbox stores the message body
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

**Partition** — the kernel's consistent-hash bucket for an aggregate id. It selects a dispatcher
worker, so one aggregate stays ordered while other aggregates run in parallel.

**Settle** — to finish a delivery with `ack` or `nack`. Every delivery is settled exactly once.
`ack` settles only that message; `nack` redelivers.

**Dead-letter** — where a terminal failure, or one that exhausted its attempt budget, is sent.

**Watermark** — the per-partition offset a consumer has committed. An `ack` advances it. It never
means "commit the current position".

**Poison message** — a message that fails deterministically on every delivery, for example one that
does not decode. It must reach the dead-letter path, never a redelivery loop.

## Failure

**Transient** — worth retrying: `StoreError`, `ConcurrencyError`, `InProgress`.

**Terminal** — never worth retrying: `CodecError`, `PayloadMismatch`, `CachedFailure`,
`AlreadyRegistered`, `HandlerNotFound`, `BadHandle`.

The kernel's `ErrorCode` carries the verdict, so no SDK restates it. See [errors](sdk/errors.md).

## Read side

**Projection** — a read model built by folding events into an `Entity`.

**Entity** — id, version, document. The version makes `save` a compare-and-set.

**Filter** — the backend-neutral query abstract syntax tree (AST) in `edax-core`, plus the reference
evaluator that pins the match verdict. A missing or null path makes a leaf false, which is why
`ne(p, x)` and `not_(eq(p, x))` differ.

**Keyset cursor** — a base64 position token from the kernel's `query`, portable between services. A
unique `id` tiebreak is appended automatically so paging never drops or repeats tied rows.

**Upcast** — raising a stored event from its written version to the current one. The kernel plans
the ordered steps and rejects a gap; your code rewrites the payload.

**Causation** — `correlation_id` and `causation_id` propagation, carried in metadata so the wire
format is untouched.

## Patterns

**Process manager** — the same thing this documentation calls a saga: a workflow that correlates
events, keeps its own state, and emits commands.

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

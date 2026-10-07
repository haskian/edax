# Glossary

Every term this documentation uses, defined once. When a doc uses one of these words, it means
exactly this.

## Layers

**Kernel** — a synchronous Rust crate that defines shared decisions across languages and processes.
The kernels are `edax-core`, `edax-event`, `edax-cqrs`, `edax-kafka` and `edax-redis`. They call no
database or broker client. Clock and identifier generation have explicit nondeterministic
exceptions. See [kernels](kernels/README.md).

**Protocol module** — a kernel crate named `edax-<tech>` that owns one technology's shared
decisions: its bytes, its names and its state machines. `edax-kafka` and `edax-redis` are the two
that ship. It links no client library.

**I/O shell** — the per-SDK adapter over a protocol module. It calls the client and the kernel, and
decides nothing in between. `edax.backend.kafka` and `edax.backend.redis` are the two Python ones.

**SDK** — software development kit, the per-language layer that wires the kernels to a host's
storage and transports: `crates/edax` (Rust reference model), `crates/edax-py/python/edax` (Python)
and `crates/edax-go` (Go).

**ABI** — application binary interface. `crates/edax-abi`, a flat Rust surface over the kernels. The
Python native module and Go C binding are generated from it. See [architecture](architecture.md).

**Seam** — a `Protocol` (Python), interface (Go) or trait (Rust) a host implements against its own database or
broker: `EventStore`, `ProjectionStore`, `IdempotencyStore`, `SagaStore`, `Transactor`, `Producer`,
`Consumer`. Its docstring is the specification.

**Backend** — a concrete implementation of one or more seams. Three ship: `memory`, `redis`,
`kafka`. See [backends](backends/README.md).

**Golden vector** — a JSON file under `testdata/golden/` generated from a kernel and replayed by
the kernel and SDK tests. A golden vector always wins over a disagreeing test. See [golden
vectors](kernels/golden-vectors.md).

## Write side

**Aggregate** — a consistency boundary with an id, a version, and a fold from events to state.

**Aggregate id** — the aggregate a message belongs to or targets. An event always has one. A
command may name one. When present, it is the message's ordering key.

**Optimistic concurrency** — `EventStore.append` and `ProjectionStore.save` are database-enforced
compare-and-set operations on a version. A stale writer gets `ConcurrencyError` and nothing is
written.

**Snapshot** — a serialized aggregate state stored so a load replays fewer events.

**Transactional Outbox** — writing the events and the rows to publish in one transaction, so
committed state and published events cannot diverge. `Relay` and `pump` drain it. See
[outbox](sdk/outbox.md).

**Command outbox** — the optional outbox for commands a unit of work decides. `Repository.save(...,
commands=[...])` stages them in the same transaction as the events, and a `Relay` sends them after
the commit. A saga keeps its own command outbox in the `SagaStore`.

## Messages

**Message** — what crosses a process boundary. Two kinds exist, an event message and a command
message. Both are flat: identity, payload, lineage, time, then two string maps. See
[edax-event](kernels/event.md#message--the-messages).

**Event message** — a stored fact, as the store returns it and every reader downstream sees it.
`EventMessage` in Rust and Python. Its fields are listed in
[edax-event](kernels/event.md#message--the-messages).

**Event draft** — an event as `Repository` hands it to `EventStore.append`. `EventDraft` has every
field of an event message but `recorded_at`, which the store stamps.

**Command message** — a request for something to happen, as a bus dispatches it, an outbox holds it
and a broker carries it. `CommandMessage`. Its fields are listed in
[edax-event](kernels/event.md#message--the-messages).

**Payload** — a codec's output, an event's or a command's: `name`, `version` and `data`. An aggregate
holds its pending events in this shape until a save turns them into drafts, and a saga hands the
commands it sends over in it. Not a wire type of its own.

**Kind** — `event` or `command`. The bytes do not say which kind they hold; the media type does.

**Typed field** — a message field an edax kernel or SDK decision reads, such as `aggregate_id` or
`correlation_id`.

**Extensions** — a string map for edax extensions, such as a W3C `traceparent`. A key outside the
kernel's [extension key rule](kernels/event.md#three-tiers-of-fields) raises `CodecError`.

**Metadata** — a string map that belongs to the application. edax never reads it. Tenancy is a
separate typed message field.

**Broker facts** — the offset, partition and broker time of a delivery. They never enter a message.
A transport reports them beside the message, as `KafkaFetched` does.

**Ordering key** — the key that orders a message against others: its aggregate id, or a command's
own id when it names no aggregate. See [edax-event](kernels/event.md#the-ordering-key).

## Time

[edax-event](kernels/event.md#time) owns these terms: who sets each one and what it means.

**Valid time** — when a fact happened in the world: an event's `occurred_at`.

**Transaction time** — when the store recorded a fact: an event's `recorded_at`.

**Issued at** — a command's `issued_at`: the issuer's clock when it built the command.

**Broker time** — the time a broker attaches to a record, carried on the delivery.

**Processing time** — when a handler takes a message. edax never stores it.

## Lineage

**Lineage** — the two typed ids that place a message in its flow: `correlation_id` and
`causation_id`. `Lineage` in Rust and Python. See [edax-event](kernels/event.md#causation--lineage).

**Correlation id** — a message field, constant across one logical flow, such as one request or one
saga run. Not the same thing as a saga's instance id, below.

**Causation id** — a message field: the id of the single message that directly caused this one. A
root message's causation id is its own correlation id.

**Propagate** — the rule for a child: its causation id is the parent's id, and it inherits the
parent's correlation id.

**Originate** — the rule for a root: fill only the ids the caller lacks. A missing correlation id is
a fresh one. A missing causation id is the correlation id.

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

**Idempotency key** — the string one client intent is claimed under. On the command path it is the
command message's `idempotency_key`, such as an HTTP `Idempotency-Key` a client retries with. `None`
makes no claim. On the consume path it is the event's `id`.

**Command id** — a command message's `id`: this one message instance. It is the causation id of what
the command causes, and the key a retry policy counts attempts by. It is not the idempotency key.

**Scope** — the name of one guard, such as a service, a stage within one, or a read model.
`idempotent` and `idempotent_by_id` require it, so two guards on one ledger never replay or skip each
other's outcome.

**Claim key** — the ledger key a command guard claims, `claim_key(scope, key)`, composed with the
tenant under a pool. An event guard claims the event id under its scope the same way (`edax:evt:`),
`event_claim_key(scope, event_id)`.
See [edax-cqrs](kernels/cqrs.md#claim_key--which-ledger-key-a-command-guard-claims).

**Claim** — the atomic insert-if-absent that reserves an idempotency key. It must be linearizable
across processes, never a read then a write.

**Record** — what is stored under a key: `pending`, `done` or `failed`, plus the payload hash and
the outcome.

**Payload hash** — the hash stored with a claim, which binds a key to one payload. For a command it is
the command hash. Reusing a key with a different command is a `PayloadMismatch`.

**Command hash** — the kernel's hash of a command's name and its whole payload value, after any
upcast. One key used for two command names is a mismatch, even when the payloads are equal.

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

**Forget** — deleting a settled record, so a re-driven message runs its handler again. Not a
release: a release is the claimant settling its own claim. See
[edax-cqrs](kernels/cqrs.md#forget--deleting-a-settled-record).

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

**Partition** — the kernel's local worker bucket for an ordering key, `fnv1a32(key) % W`. It
selects a dispatcher worker, so one aggregate stays ordered while other aggregates run in parallel.
Broker placement is a separate decision: `edax-kafka`'s `partition_for` uses Kafka's murmur2 rule.

**Settle** — to finish a delivery with `ack` or `nack`. A delivery is settled at most once.
Retired deliveries are skipped. A failed settlement may leave a delivery unsettled until the
broker recovers it. `ack` settles that message; `nack` requests redelivery.

**Dead-letter** — where a terminal failure, or one that exhausted its attempt budget, is sent.

**Watermark** — the per-partition offset a consumer has committed. An `ack` advances it. It never
means "commit the current position".

**Poison message** — a message that fails deterministically on every delivery, for example one that
does not decode. It must reach the dead-letter path, never a redelivery loop.

**Media type** — the name a wire format goes by between services (RFC 6838, once "MIME type"), for
example `application/vnd.edax.event+json`. edax mints one per format and kind. A Kafka record carries it in the `content-type` header,
and a consumer routes the record to the serializer registered under it. A media type no serializer
claims makes the message poison, never a fallback. Not a synonym for the header: `content-type` is
the field name, the media type is its value.

**Vendor tree (`vnd.`)** — the RFC 6838 prefix for a product-specific media type, as opposed to an
IANA standard such as `application/json`. `application/vnd.edax.command+json` reads: vendor `edax`,
kind `command`, parse as JSON (the `+json` suffix, RFC 6839). Registration is optional in this
tree and has not been done. A custom serializer names its format the same way,
`application/vnd.<vendor>.<format>+<syntax>`.

## Failure

**Transient** — worth retrying: `StoreError`, `ConcurrencyError`, `InProgress`.

**Terminal** — never worth retrying: `CodecError`, `PayloadMismatch`, `CachedFailure`,
`AlreadyRegistered`, `HandlerNotFound`, `BadHandle`, `Rejected`.

The kernel's `ErrorCode` carries the verdict, so no SDK restates it. See [errors](sdk/errors.md).

## Read side

**Projection** — a read model built by folding events into an `Entity`.

**Entity** — id, version, document: a value. The version makes `save` a compare-and-set, and `save`
returns the new one. A read carries the id in the document as `"id"`.

**Filter** — the backend-neutral query abstract syntax tree (AST) in `edax-cqrs`, plus the reference
evaluator that pins the match verdict. A missing or null path makes a leaf false, which is why
`ne(p, x)` and `not_(eq(p, x))` differ.

**Keyset cursor** — a base64url position token from the kernel's `query`, portable between services. A
unique `id` tiebreak is appended automatically so paging never drops or repeats tied rows.

**Upcast** — raising a stored event from its written version to the current version of its name.
That version is one past the highest step of the name's upcaster, or 1 with none. The kernel decides
that version, plans
the ordered steps and rejects a gap; your code rewrites the payload.

## Patterns

**Process manager** — the same thing this documentation calls a saga: a workflow that correlates
events, keeps its own state, and emits commands.

**Saga instance id** — what `Saga.instance_id(msg)` returns: which running instance this event
belongs to, and the id the `SagaStore` is keyed by. It is not the event's `correlation_id`: that one
names a flow, this one names stored state. Every command the instance sends carries it as
`aggregate_id`. It is often the aggregate id; see [sagas](sdk/sagas.md#the-instance-rule).

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

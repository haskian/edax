# Multi-tenancy

A tenant is a first-class field of every message, and the key rule that follows from it is the
kernel's. You choose a **tenancy mode** where you build a service, and every seam of that service
keys the same way from then on.

What edax does **not** choose is the physical layout. One database or one per tenant, one schema or
one per tenant, one table or one per tenant, one topic or one per tenant. That is yours. The three
models below are the shapes those choices take.

| Mode | Model | A tenant is | Keys are | Separation comes from |
|---|---|---|---|---|
| `Tenancy.SILO` | single-tenant, shared-nothing | optional | bare | the infrastructure: one stack per tenant |
| `Tenancy.POOL` | fully shared, logical isolation | required | composed with the tenant | the key, at every seam |
| `Tenancy.BRIDGE` | hybrid, shared app and isolated data | required | bare | the store instance you resolve per tenant |

## The field and the rule

Every `EventMessage` and `CommandMessage` carries `tenant`, beside `aggregate_id` and the lineage. It
is a typed field and not a metadata key, because the kernel reads it. `tenant_key(mode, tenant, key)`
composes the key a seam is called with. `testdata/golden/tenancy.json` pins the result, so every
binding spells one tenant's keys identically.

<!-- doclint: check -->
```python
from edax import Tenancy, tenant_key

tenant_key(Tenancy.POOL, "acme", "order-1")  # "4:acme:order-1"
tenant_key(Tenancy.BRIDGE, "acme", "order-1")  # "order-1": its store is already the tenant's
tenant_key(Tenancy.POOL, None, "order-1")  # CodecError: a pool cannot key without a tenant
```

The composed form is `"{len(tenant)}:{tenant}:{key}"`. The byte-length prefix makes it injective: a
tenant named `a:b` with key `c` cannot spell the key of a tenant named `a` with key `b:c`. So the
charset stays open. A tenant id only has to be non-empty and at most `TENANT_MAX` bytes, which
`check_tenant` enforces on every message the codec reads or writes.

**A mode that requires a tenant refuses a call without one.** That is the property worth having. In a
pool, forgetting the tenant raises `CodecError` before any read, rather than quietly returning every
tenant's rows.

## What each seam does with it

Pass `tenancy=` once, where you build the thing:

```python
repo = Repository(store, events, aggregates, tenancy=Tenancy.POOL)
bus = CommandBus(idempotent(ledger, scope="api", tenancy=Tenancy.POOL))
router = EventRouter(idempotent_by_id(ledger, scope="balances", tenancy=Tenancy.POOL))
producer = KafkaProducer(client, "events", tenancy=Tenancy.POOL)
```

| Seam | What the tenant keys | How it travels |
|---|---|---|
| `Repository.load(id, tenant=...)` | the event stream and the snapshot | the aggregate carries it, so the save that follows cannot name another |
| `Repository.save` | every event's `tenant` field, and every saved command's | stamped by the repository, from the aggregate; a command naming another tenant is refused |
| `idempotent(..., tenancy=...)` | the ledger claim, inside its `scope` | read off the command message |
| `forget(..., tenancy=..., tenant=...)`, `forget_event(...)` | the ledger record a re-drive deletes | passed by the operator, from the dead-lettered message |
| `idempotent_by_id(..., tenancy=...)` | the event claim, inside its `scope` | read off the event message |
| `CommandBus.dispatch(..., tenant=...)` | the message it builds | a typed field of the message |
| `KafkaProducer` | the record key, and so the partition: the kernel's `edax_kafka::record_key` | read off the message |
| `SagaRunner` | the instance, through the saga's own `instance_id` | inherited by every command a step emits |
| `ProjectionStore` | the entity id, and a query predicate | yours to compose: a read model's id is yours |

Two seams are deliberately left to you.

**Projections.** A read model's id is your own. Compose it with `tenant_key` and filter with a prefix
on `id`, which a store always returns, or keep a `tenant` field in the document and filter on that.
The conformance suite proves a backend returns the row key as `id`, which is what makes the prefix
filter safe.

**Sagas.** A saga's correlation is derived by your `instance_id`, so compose it there:

```python
class Shipping:
    name = "shipping"

    def instance_id(self, msg: EventMessage) -> str | None:
        return tenant_key(Tenancy.POOL, msg.tenant, msg.aggregate_id)
```

The runner then keys the instance and its outbox rows by that one string. Every command a step emits
inherits the folded event's tenant, so the bus that runs it claims inside the same tenant.

## 1. Single-tenant, the silo model

Each tenant gets its own instance of the application and its own infrastructure. Nothing is shared,
so isolation is maximal and so is the cost per tenant.

Variations, from strictest down:

- **Physical isolation.** Separate hardware per tenant. Reserved for defence and similar compliance.
- **Virtual isolation.** One cloud, one virtual private cloud or subnet per tenant. The usual form.
- **Control plane and data plane split.** A shared control plane does onboarding and billing and
  starts a dedicated data plane per tenant. The data plane is a silo; the control plane is a pool, so
  it takes the pool rules.

**In edax.** `Tenancy.SILO`, the default of a repository and a producer. The idempotency guards,
`claim_key` and `forget` have no default, so pass the mode to each of them. A tenant is optional
and no key carries one. Stamp one anyway if the data may ever move into a pool: the field travels, costs almost nothing, and gives you
an audit trail.

## 2. Fully shared, the pool model

Every tenant uses the same application instance, the same database and the same topics at once.
Isolation is logical: each request carries a tenant, and every key is composed from it.

The cell-based variation puts a bounded set of tenants in one shared deployment and runs many such
cells. A cell is a pool inside, so these rules hold within it unchanged.

**In edax.** `Tenancy.POOL`. The physical layout underneath is still yours:

- **One table, the tenant in the key.** A tenant's rows are then a prefix scan. This is what the
  in-memory backend does, and what the conformance suite describes.
- **One schema per tenant, resolved per connection.** Compose the keys anyway and you get both: the
  schema separates, and the key stays unambiguous if the schemas are ever merged.
- **Row-level security.** Keep composing, and have the database enforce it too. The two agree,
  because the tenant is in the key the query already uses.

```python
repo = Repository(store, events, aggregates, tenancy=Tenancy.POOL)
account = await repo.load("order-1", tenant=tenant)  # 4:acme:order-1 in the store
account.apply(Deposited(100))
await repo.save(account)  # every event stamped with the tenant it was loaded for
```

A new aggregate starts the same way: `repo.load(id, tenant=...)` returns it empty at version 0 with the
tenant set. One built with its constructor carries no tenant, and its save raises `CodecError`.

## 3. Hybrid, the bridge model

The stack is split by layer. The stateless layers run pooled, for efficiency. The stateful layers run
as silos, for data sovereignty, simpler backups and isolation of heavy queries.

Variations:

- **Database per tenant.** One shared application, a separate database server per tenant.
- **Schema per tenant.** One shared server, a separate schema per tenant. Cheaper, and it still
  allows per-tenant backups and per-tenant structure.
- **Component-level hybrid.** Small services stay pooled; a heavy service gets dedicated workers per
  tenant, so one tenant's load cannot degrade everyone.

**In edax.** `Tenancy.BRIDGE`. A tenant is required, so every message is attributable and routable,
and keys stay bare because the store you resolve is already that tenant's. Build one set of stores
per tenant and share everything stateless:

- Per tenant: `EventStore`, `ProjectionStore`, `SagaStore`, `RedisIdempotencyStore(prefix=...)`, a
  `Relay` per store, and the producer's topic.
- Shared: `CommandBus`, `EventRouter`, the dispatcher, the middleware chain, the serializers.

edax ships no tenant registry, so the lookup from tenant to store set is yours, usually a dictionary
built at startup in your composition root.

A `BRIDGE` service pointed at one shared store shares a stream between tenants, by design: the model
separates by store, not by key. If the stores are shared, the mode is `POOL`.

## What is still yours to get right

The mode closes what a library can close. These are the cases it cannot:

| Case | What happens | What to do |
|---|---|---|
| a read model id or query with no tenant | another tenant's rows, with no error | compose with `tenant_key`, filter on `id` or on a `tenant` field |
| a saga `instance_id` with no tenant | two tenants' workflows fold into one instance | compose inside `instance_id` |
| an explicit `key=` on a producer | it replaces the composed key outright | compose it yourself, or drop the override |
| a tenant id read from a request | `check_tenant` bounds it, and nothing more | decide who exists against your own list |
| no tenant on a pool `dispatch` | `CodecError`, before the claim | pass it from the request context |

**Assert on the way out.** Compare `msg.tenant`, or a document's own tenant, against the tenant the
request is for. A mismatch is a composition defect, and the assertion turns it into an error instead
of a disclosure.

## Brokers

A broker with tenancy of its own raises a question the store side does not: which destination does a
message go to? A producer holds one topic, fixed when you build it, and `publish` takes only the
message, so the destination is yours to choose per message.

That makes every Pulsar layout the same host code. Vary the tenant segment of
`persistent://<tenant>/<namespace>/<topic>`, the namespace segment, or the topic name; edax never
sees the string:

```python
producers = {name: PulsarProducer(client, topic) for name, topic in destinations.items()}


async def publish(msg: EventMessage) -> None:
    producer = producers.get(msg.tenant or "", parking)
    await producer.publish(msg)


relay = Relay(store, publish)
```

The same shape covers a Pub/Sub topic path, an SNS topic ARN, or one Kafka topic per tenant. Route a
tenant you cannot place to a parking destination rather than raising, because a row whose tenant is
unknown fails the same way on every attempt.

On a shared topic, `tenancy=Tenancy.POOL` makes the record key the composed key. The kernel decides
it (`edax_kafka::record_key`, pinned by `kafka_record.json`), so every SDK producing to the topic
places one aggregate on one partition. Two tenants' same-named aggregates then take different
partitions, and a consumer reads the tenant off the key without decoding the value. A subscription
that filters on attributes instead, such as SNS to SQS, needs the tenant as an attribute, and the
adapter you write is what adds it.

### One shared outbox, one shared queue

A relay publishes a batch in order and marks every row it settled, including when a later row fails.
The rows that went out are not re-sent. A failing row holds back only its own key, the tenant and the
aggregate, so one tenant's poison row does not hold up another tenant's rows.

- **An outage is shared.** A destination that is down fails every tenant's rows. Two transient
  failures in a row stop the drain, and the relay backs off per its retry policy and drains again
  once that destination recovers.
- **The stall is bounded, if you wire a sink.** A row the retry policy calls dead goes to `on_dead`
  and is marked settled. One unroutable tenant therefore cannot block the outbox forever.

Per-aggregate order survives either way. Publishing is sequential, and a row is marked only once it
is settled, so no later sequence goes out before an earlier one.

Do not catch a per-tenant publish failure and return normally. That marks an unpublished row as
published, which is the one thing the outbox exists to prevent. Use `on_dead` instead.

The arrangement that isolates tenants is one relay per tenant. The contract asks that two relays over
one store lease the rows each one returns. A per-tenant view meets that by construction: give each
relay an `unpublished` that selects one tenant, and the sets never overlap.

### Consuming

One consumer can serve many topics, and many consumers can feed one dispatcher.

- **Ordering stays correct** with several sources on one dispatcher. The worker is
  `partition(ordering_key(msg), workers)`, and each worker is first-in-first-out.
- **The topic does not reach the handler.** A delivery carries the message, its settle handles and
  its origin record. A handler reads `msg.tenant`, never the topic it arrived on.
- **A shared dispatcher shares its blast radius.** A handler retrying with backoff holds its worker,
  and every key that hashes to it waits. The worker hashes the bare ordering key, not the composed
  one, so two tenants' aggregates with one id always share a worker. One consumer and one dispatcher
  per tenant is the isolating arrangement.
- **A poison message is confined** to the topic or partition it arrives on. Wire `on_dead` so it is
  dead-lettered and acked, or it redelivers and pins the position behind it.

## Moving an existing deployment into a pool

Turning `SILO` into `POOL` changes every key: the stream, the snapshot, the claim and the record key
all gain their tenant. The Kafka partition follows the record key, so it moves with it. The
dispatcher worker hashes the bare ordering key, so it stays where it was. Per-aggregate order holds on
each side of the change and not across it.

Do it before there is production data, or copy each tenant's streams into their composed keys while
nothing writes.

## See also

- [edax-core](../kernels/core.md) — `scoped`, `Tenancy` and `tenant_key`, and the golden that pins them.
- [edax-event](../kernels/event.md) — the message fields, and which of them a kernel reads.
- [Aggregates](../sdk/aggregates.md) — `Repository`, and where the tenant is stamped.
- [Commands](../sdk/commands.md) — the claim key, and the guard that composes it.
- [Sagas](../sdk/sagas.md) — the instance id, which is yours to compose.
- [Projections](../sdk/projections.md) — the filter, the sort order, and keyset cursors.
- [Outbox](../sdk/outbox.md) — what `Relay` promises, and why a dual write cannot be atomic.
- [kafka](../backends/kafka.md) — the record key, the partition rule, and the headers.

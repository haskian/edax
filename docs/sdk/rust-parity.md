# Rust and Python parity

Both software development kits (SDKs) call the same kernels, so every shared decision matches. The
orchestration differs where the language does. This page is the one home for those differences.

## The name map

| Concept | Rust `crates/edax` | Python `crates/edax-py/python/edax` |
|---|---|---|
| aggregate | `Aggregate`, `Root` | `Aggregate`, `Version` |
| event codec | `SerdeCodec<T, Format>`, aliased `JsonCodec` and `MsgpackCodec` | `json_codec()`, `msgpack_codec()` |
| repository | `Repository` | `Repository` |
| outbox drain | `Relay::drain` | `Relay.drain`, plus `pump` for change data capture |
| command bus | `CommandBus`, `Middleware`, `retry`, `idempotent`, `idempotent_in` | `CommandBus`, `retry`, `idempotent` |
| consume path | `KeyOrderedDispatcher`, `EventRouter`, `idempotent_by_id`, `idempotent_by_id_in` | `KeyOrderedDispatcher`, `EventRouter`, `idempotent_by_id` |
| saga | `Saga`, `SagaInstance`, `SagaRunner`, `SagaStore` | the same, plus `Completed` |
| read side | `Entity`, `ProjectionStore`, `Query` | `Entity`, `ProjectionStore`, the `edax.query` and `edax.filter` modules |
| transaction | `Transactor`, `with_tx` | `Transactor`, `transaction`, `with_tx`, `current_tx` |
| host-called kernel decisions | `should_apply`, `originate`, `propagate`, `guid` | the same, plus `partition`, `rendezvous`, `payload_hash`, `upcast_plan`, `backoff_millis` |
| guard owning the transaction | not implemented; the host publishes a handle provider to the `_in` form | `unit_of_work=` on both guards |
| backends | in-memory, plus `InMemoryBroker` | in-memory, Redis, Kafka |
| concurrency | synchronous; dispatcher workers are operating-system threads | `asyncio`; workers are tasks |

## Codecs

Rust ships one codec type. `SerdeCodec<T, Format>` carries the format as a zero-sized type
parameter. `JsonCodec` and `MsgpackCodec` are aliases of it. `msgpack` is a cargo feature and it is
on by default. Turn it off to drop the MessagePack dependency.

Python ships two constructors, `json_codec()` and `msgpack_codec()`, over one implementation.

## Aggregates

Rust `Root` holds `id`, `version: u64` and a private `pending`. `current()` is `version +
pending.len()`. A host reaches `pending` only through `apply` and `take_pending`, so the version has
one source of truth.

Python `Version(initial, updates)` splits the same number. `current` is their sum.

## No ambient transaction in Rust

Rust has no counterpart to Python's `current_tx`. The middleware chain is synchronous. A `Handler<C,
O>` wraps a plain function, so there is no task whose task-locals could carry an ambient handle. A
process-wide static would be wrong the moment two dispatches overlap. So the handle is passed, not
discovered.

| Form | Where the handle comes from | Use |
|---|---|---|
| explicit `&mut Tx` | the caller threads it | `Repository::load_in` / `save_in`, `EventStore::append`, `ProjectionStore::save` |
| a provider closure | the host publishes its open handle | `idempotent_in(store, key, current)`, `idempotent_by_id_in(store, current)` |

The provider takes the work as its input, so each call can return a handle of its own:

- `idempotent_in(store, key, current)` calls `current` as `Fn(&C) -> Tx`, with the command.
- `idempotent_by_id_in(store, current)` calls `current` as `Fn(&EventEnvelope) -> Tx`, with the
  envelope.

That input is what lets a host hand each dispatcher worker its own live handle. A shared slot would
be a race condition between two workers.

`current` runs once per guarded dispatch. It returns the handle that the claim, the terminal record
and any release run under. Outside any unit of work it returns the auto-commit handle. `idempotent`
is the same thing with `T::default` as the provider.

| | Python | Rust |
|---|---|---|
| how a participant finds the handle | explicit `tx=`, else `current_tx()` | explicit `&mut Tx`, or a provider closure |
| where the ambient value lives | `contextvars.ContextVar` per asyncio task | nowhere; sync middleware has no task-local |
| repository entry points | one `load` / `save` with `tx=None` | two: `load` / `load_in`, `save` / `save_in` |
| scoped boundary | `async with with_tx(transactor)` | `transactor.transaction(body)` or `with_tx(&transactor, body)` |
| what a rollback costs the caller | the aggregate was already reset: reload it | the same |

Same contract, same four executed clauses, different wiring.

`with_tx` rolls back if the body panics. A drop guard owns the rollback, so a panicking handler
never leaves a token open and never holds its keys forever.

## The Rust command bus is Send and Sync

`CommandBus` is held behind an `Arc` and is `Send + Sync`. `HandlerError` is `Box<dyn Error + Send +
Sync>`. A saga running inside a dispatcher worker can therefore dispatch through a `CommandBus`
shared across worker threads. The saga-to-command topology compiles.

## No Rust guard that owns the transaction

There is no Rust equivalent of Python's `unit_of_work=`. There is no `joins_transaction` and no
one-commit guard. Two reasons:

- No durable Rust backend ships. `crates/edax-adapters` holds in-memory stores only. Redis and Kafka
  backends live in the Python SDK. The two-commit split that `joins_transaction` describes has no
  Rust backend to describe.
- The dispatcher spawns worker threads that share one handler. A guard that opened a transaction
  inside that shared handler would have to decide per thread which transaction it owns. The provider
  closure already asks the host to do that, with the host's own knowledge of its connection pool.

## What is symmetric

The seam is symmetric on failure. `IdempotencyStore::claim` returns `Result<Claim<O>, Error>` in
Rust. That matches a Python `claim` that may raise. A Rust store reports a backend failure instead
of guessing a verdict.

Everything else is parity: the claim, run and settle orchestration, the terminal and transient
split, the kernel verdict, the record wire shape, and the `complete_or_release` rule. Twin test
suites assert all of it, one per language, over the same case table.

## See also

- [SDK map](./README.md) — the seams and the orchestration around them.
- [Idempotency](./idempotency.md) — the two guard modes and every failure window.
- [Transactions](./transactions.md) — `Transactor`, `current_tx` and `with_tx`.
- [Aggregates](./aggregates.md) — `Root`, `Version` and the save path.

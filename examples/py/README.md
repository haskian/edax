# Python examples

Nineteen small programs, one idea each, in learning order. Each one prints what happens, step by
step. Each one ends in asserts and a final `ok:` line, so a clean exit means it worked.

Every example runs on the in-memory backends. Two of them can switch to a real Redis or Kafka (see
[Real backends](#real-backends)).

## Run them

```sh
cd examples/py
uv sync                          # builds the local edax SDK once; needs a Rust toolchain
uv run python 01_command_bus.py
```

Working in a checkout with the development environment instead? Build the native module, then run
any file with that interpreter:

```sh
cd crates/edax-py && mise exec -- maturin develop && cd ../..
crates/edax-py/.venv/bin/python examples/py/01_command_bus.py
```

`maturin develop` is not optional there. Read [the stale native module
trap](../../docs/guides/getting-started.md#the-stale-native-module-trap).

## Learning order

| File | What you learn | Key API | Read next |
|---|---|---|---|
| [`01_command_bus.py`](01_command_bus.py) | send a command to its handler | `CommandBus`, `register`, `dispatch` | [commands](../../docs/sdk/commands.md) |
| [`02_aggregate.py`](02_aggregate.py) | keep state as events; save and load it | `Aggregate`, `Repository`, `json_codec` | [aggregates](../../docs/sdk/aggregates.md) |
| [`03_snapshots.py`](03_snapshots.py) | load from a snapshot plus the tail | `Repository(snapshot_every=)` | [aggregates](../../docs/sdk/aggregates.md#snapshots) |
| [`04_upcasting.py`](04_upcasting.py) | read events an older version stored | `Upcaster`, `json_codec(version=, upcasters=)` | [kernels/event](../../docs/kernels/event.md) |
| [`05_idempotent_commands.py`](05_idempotent_commands.py) | apply a command once, even when sent twice | `idempotent(..., unit_of_work=)`, `retry`, `PayloadMismatch` | [commands](../../docs/sdk/commands.md#the-recommended-wiring) |
| [`06_errors_and_retry.py`](06_errors_and_retry.py) | retry transient errors, not terminal ones | `retry`, `Rejected`, `Pacing`, `is_terminal` | [errors](../../docs/sdk/errors.md) |
| [`07_outbox_relay.py`](07_outbox_relay.py) | publish saved events through the outbox | `Relay.drain`, `unpublished_count` | [outbox](../../docs/sdk/outbox.md) |
| [`08_consume_events.py`](08_consume_events.py) | consume in order per key, in parallel across keys | `KeyOrderedDispatcher`, `EventRouter`, `MemoryBroker.run` | [consuming](../../docs/sdk/consuming.md) |
| [`09_dead_letters.py`](09_dead_letters.py) | dead-letter what keeps failing | `on_dead`, `max_attempts`, `Rejected` | [consuming](../../docs/sdk/consuming.md#retry-and-dead-letter) |
| [`10_idempotent_events.py`](10_idempotent_events.py) | update a read model once per event | `idempotent_by_id(..., unit_of_work=)`, `ProjectionStore` | [idempotency](../../docs/sdk/idempotency.md) |
| [`11_projection_query.py`](11_projection_query.py) | filter, sort and page a read model | `get_many`, `edax.filter`, `sort_key`, `Page.next` | [projections](../../docs/sdk/projections.md) |
| [`12_causation.py`](12_causation.py) | link events into one traceable flow | `ensure_ids`, `propagate`, `save(metadata=)` | [kernels/event](../../docs/kernels/event.md) |
| [`13_saga.py`](13_saga.py) | turn events into commands, through a saga outbox | `SagaRunner`, `Completed`, `runner.relay(on_dead=)` | [sagas](../../docs/sdk/sagas.md) |
| [`14_observability.py`](14_observability.py) | see claims, transactions, drains and latency | `Observer`, `timing`, `Relay.last` | [observability](../../docs/sdk/observability.md) |
| [`15_lease_takeover.py`](15_lease_takeover.py) | rehearse a Redis claim expiring mid-handler | `MemoryLedger`, `expire`, `InProgress` | [backends/memory](../../docs/backends/memory.md#test-on-the-ordering-you-deploy-memoryledger) |
| [`16_redis_ledger.py`](16_redis_ledger.py) | keep the idempotency ledger in Redis | `RedisIdempotencyStore`, `CachedFailure` | [backends/redis](../../docs/backends/redis.md) |
| [`17_kafka.py`](17_kafka.py) | publish and consume through Kafka | `KafkaProducer`, `KafkaConsumer`, `KafkaDeadLetterSink` | [backends/kafka](../../docs/backends/kafka.md) |
| [`18_contract_kit.py`](18_contract_kit.py) | test a store you wrote with the contract kit | `edax.testing.ProjectionStoreContract` | [guides/testing](../../docs/guides/testing.md) |
| [`19_fault_injection.py`](19_fault_injection.py) | prove a handler survives a lost commit reply | `edax.testing.faults.BrokenCommit` | [delivery guarantees](../../docs/guides/delivery-guarantees.md) |

Steps 1 to 7 are the write side. Steps 8 to 11 are the read side. Steps 12 to 14 connect and watch
the two. Steps 15 to 19 take it to production backends and to your own tests.

## Real backends

`16_redis_ledger.py` and `17_kafka.py` run on the in-memory stand-ins unless an environment variable
names a real server:

```sh
docker run -d --rm --name edax-redis -p 6379:6379 redis:8-alpine
EDAX_REDIS_URL=redis://localhost:6379/0 uv run python 16_redis_ledger.py

EDAX_KAFKA_BOOTSTRAP=localhost:9092 uv run python 17_kafka.py
```

The Kafka example creates two fresh topics and deletes them when it ends. Continuous integration
(CI) sets both variables, so there these two run against real servers.

## How they are checked

CI runs every file and fails on a missing `ok:` line. `crates/edax-py/tests/test_examples.py` runs
every `main()` inside the test suite as well. A new example needs no registration: name it with the
next number, end it in asserts and an `ok:` line, and both checks pick it up.

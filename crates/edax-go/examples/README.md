# Go examples

Nineteen examples cover the same topics and learning order as the
[Python examples](../../../examples/py/README.md). Each program checks its results and prints a
final `ok:` line. The contract-kit example is a Go test package.

## Run them

Build the native library from the repository root, then run an example:

```sh
mise run go:build
cd crates/edax-go
mise exec -- go run -race ./examples/01_command_bus
```

To smoke-test every Go example from the repository root:

```sh
mise run examples:go
```

The task runs each program with the race detector. It fails on an error or a missing `ok:` marker.
It runs the contract-kit package with `go test -race -count=1 -v`. Pass directory names to select examples:

```sh
mise run examples:go 05_idempotent_commands 13_saga
```

Each numbered directory stands alone and shows its setup, error handling and use of the public SDK.

## Learning order

| Directory | What it demonstrates |
|---|---|
| [01_command_bus](01_command_bus/) | Dispatch a command and reject an unregistered handler |
| [02_aggregate](02_aggregate/) | Apply events, save an aggregate and replay its stream |
| [03_snapshots](03_snapshots/) | Load a snapshot and replay only the remaining events |
| [04_upcasting](04_upcasting/) | Read an older payload without changing stored bytes |
| [05_idempotent_commands](05_idempotent_commands/) | Replay a command result and reject a changed payload |
| [06_errors_and_retry](06_errors_and_retry/) | Roll back a failed attempt and distinguish transient from terminal errors |
| [07_outbox_relay](07_outbox_relay/) | Keep an outbox row until publication succeeds |
| [08_consume_events](08_consume_events/) | Consume concurrently while preserving order per account |
| [09_dead_letters](09_dead_letters/) | Bound retries and settle terminal failures through a dead-letter sink |
| [10_idempotent_events](10_idempotent_events/) | Commit the event claim and projection update together |
| [11_projection_query](11_projection_query/) | Filter, sort and paginate a read model |
| [12_causation](12_causation/) | Carry correlation and causation through handlers |
| [13_saga](13_saga/) | Turn events into commands through the saga outbox |
| [14_observability](14_observability/) | Observe claims, transactions and relay results |
| [15_lease_takeover](15_lease_takeover/) | Refuse a late result after another worker takes over a claim |
| [16_redis_ledger](16_redis_ledger/) | Use a separate ledger and replay success or terminal failure |
| [17_kafka](17_kafka/) | Publish an outbox and consume through a dispatcher |
| [18_contract_kit](18_contract_kit/) | Run the public backend contracts with store factories |
| [19_fault_injection](19_fault_injection/) | Survive a lost commit reply without applying a deposit twice |

The [Go SDK guide](../../../docs/sdk/go.md) describes the interfaces. The
[examples guide](../../../docs/guides/examples.md) maps these topics onto the message flow.

## Real backends

Every example runs without external services by default. Examples 16 and 17 switch to Redis and
Kafka when these environment variables are set:

```sh
EDAX_REDIS_URL=redis://localhost:6379/0 mise run examples:go 16_redis_ledger
EDAX_KAFKA_BOOTSTRAP=localhost:9092 mise run examples:go 17_kafka
```

The live examples use separate names for each run. Redis records expire after one minute;
the Kafka example deletes its topics when it finishes. Run them against disposable development
services. Continuous integration runs every example in memory on Linux
and macOS. A separate Linux job runs the Redis and Kafka examples against real services.

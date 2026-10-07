# Go backends

Go adapters live under `github.com/haskian/edax/crates/edax-go/backend`.
The Redis adapter leaves its caller-owned client open.
The managed Kafka clients in `backend/kafka/franz` own their connections and close them with `Close`.
Run the [Go setup](../sdk/go.md) before compiling an adapter.

## Redis

`redis.Store` implements `edax.IdempotencyStore` with the same Lua scripts and value format as Python.
Use the same prefix in both SDKs to share a ledger.
The prefix should identify the service and environment, such as `billing:prod:idem:`.

`redis.NewClient` adapts a caller-owned `go-redis/v9` client or cluster client.
It uses `EVALSHA` and retries with the script source on `NOSCRIPT`.
Configure `ContextTimeoutEnabled: true` on that client so socket operations honor the store's deadline.
[go-redis's script implementation](https://github.com/redis/go-redis/blob/master/script.go) supplies the script cache behavior.

```go
client := goredis.NewClient(&goredis.Options{
    Addr: "localhost:6379",
    ContextTimeoutEnabled: true,
})
defer client.Close()

store, err := redis.New(redis.NewClient(client), redis.Config{
    Prefix: "billing:prod:idem:",
    TTL: 24 * time.Hour,
    ClaimTTL: 30 * time.Second,
    OpTimeout: 5 * time.Second,
    Heartbeat: true,
})
if err != nil {
    return err
}
defer store.Close()
```

Zero durations select the kernel defaults. The kernel validates all configured durations.
With heartbeats, the operation timeout must stay below one third of the claim lifetime.
The store renews a claim until completion, release, lease loss or `Close`.
`LeaseLost` reports a refused renewal or terminal renewal failure. Transient failures retry on the next scheduled beat.
Set `Config.Logger` to route renewal warnings to your service logger. Nil captures `slog.Default()` when the store is built.
The store logs the ledger key and failure, but never the claim handle.

Completion and release compare the current value with the claim handle.
An expired worker cannot settle a replacement claim.
`Forget` removes settled records and refuses an active claim.
Redis does not join another database's transaction. Handlers must tolerate redelivery after a partial failure.
The ledger fence does not fence writes in another database.

The [runnable Redis example](../../crates/edax-go/examples/16_redis_ledger/main.go) uses Redis when `EDAX_REDIS_URL` is set and checks that a second call replays the result.
Run it from `crates/edax-go`:

```sh
EDAX_REDIS_URL=redis://localhost:6379/0 mise exec -- go run ./examples/16_redis_ledger
```

The backend tests run the kernel scripts in `miniredis`. They cover claim expiry, stale settlement, corrupt records, heartbeat failures and cancellation.
They do not establish behavior during a real Redis failover.

## Kafka

`kafka.Producer[M]` and `kafka.Consumer[M]` accept typed, caller-owned client interfaces.
`M` is `edax.EventMessage` or `edax.CommandMessage`.
Each consumer handles one message kind. Use separate typed consumers when your application processes both kinds.
The default serializer uses the kernel's JSON format. `NewKernelSerializer[M]("msgpack")` selects MessagePack.

### Managed franz-go clients

`franz.NewProducer` creates the network client used by `kafka.NewProducer` and `kafka.NewDeadLetterSink`.
It fixes manual partitioning and acknowledgements from all in-sync replicas.
It rejects disabled idempotence and transactions.
`franz.NewConsumer[M]` subscribes to topics and configures the consumer group, manual commits and rebalance callbacks.
Pass ordinary `kgo.Opt` values for brokers, authentication, TLS and client tuning.

```go
client, err := franz.NewProducer(kgo.SeedBrokers("localhost:9092"))
if err != nil {
    return err
}
defer client.Close()

producer, err := kafka.NewProducer[edax.EventMessage](
    client, "invoices", kafka.ProducerOptions[edax.EventMessage]{},
)
if err != nil {
    return err
}

consumer, err := franz.NewConsumer[edax.EventMessage](
    "billing", []string{"invoices"}, kafka.ConsumerOptions[edax.EventMessage]{},
    kgo.SeedBrokers("localhost:9092"),
)
if err != nil {
    return err
}
defer consumer.Close()
```

Set `ConsumerOptions.Logger` to route managed client rebalance warnings to your service logger.
Nil captures `slog.Default()` when the managed consumer is built.

The managed consumer owns the three partition callbacks. It replaces callback options supplied by the caller.
It rejects `BlockRebalanceOnPoll` and direct partition assignment.
Callbacks must run independently so the adapter can cancel a blocked poll before retiring deliveries.
A revoke pauses those partitions, commits safe offsets and drains active handlers.
A lost assignment retires and drains without committing. Reassignment resets tokens and resumes fetching.
Choose a franz-go `RebalanceTimeout` longer than the adapter's `CommitTimeout` plus `DrainTimeout`.
These steps follow [franz-go's callback lifecycle](https://github.com/twmb/franz-go/blob/v1.22.1/pkg/kgo/config.go).

The producer enables `AllowIdempotentProduceCancellation` so a publish deadline also stops an in-flight send.
Kafka may have accepted that record before the timeout. An application retry can produce a duplicate.
Use an idempotent handler when side effects must survive this ambiguity.
Franz-go still deduplicates its own internal retries. See its [cancellation contract](https://github.com/twmb/franz-go/blob/v1.22.1/pkg/kgo/config.go).

The [runnable Kafka example](../../crates/edax-go/examples/17_kafka/main.go) publishes an outbox through Kafka and consumes it with a dispatcher.
It creates fresh topics for the run and deletes them when it finishes.
Unset `EDAX_KAFKA_BOOTSTRAP` to run in memory. Set it to comma-separated broker addresses for Kafka.
From `crates/edax-go`, run:

```sh
EDAX_KAFKA_BOOTSTRAP=localhost:9092 mise exec -- go run ./examples/17_kafka
```

The bridge tests run without a broker. They verify exact offsets, protocol error mapping, blocked-poll cancellation and revoke/lost behavior.
They do not establish live cluster rebalance, broker restart or failover behavior.

### Other Kafka clients

Use the interfaces below when integrating another client. The caller owns that client.
A producer client implements two methods:

```go
type ProducerClient interface {
    Partitions(context.Context, string) (uint32, error)
    Send(context.Context, string, int32, kafka.Record) error
}
```

`Partitions` returns the topic's partition count.
`Send` must use the supplied partition and wait for the broker acknowledgement.
Configure idempotent production and acknowledgements from every in-sync replica.
The kernel selects the partition from the final record key, after middleware.
Metadata lookup and send share the producer's timeout.
A send that times out may still reach the broker. Consumers must tolerate duplicates.

A consumer client implements polling, explicit offset commits and seeks:

```go
type ConsumerClient interface {
    Poll(context.Context) (*kafka.Fetched, error)
    Commit(context.Context, map[kafka.TopicPartition]int64) error
    Seek(context.Context, kafka.TopicPartition, int64) error
}
```

Disable automatic commits and configure a consumer group.
Report those settings through `ConsumerOptions.AutoCommit` and `HasGroup`.
All client calls must honor their contexts. Poll returns `io.EOF` when the client stops.
Convert Kafka protocol errors with `kafka.BrokerError(code, err)` so the kernel supplies their retry classification.
Leave network and context errors unchanged.


The consumer books an offset before decoding its value.
A nack asks the kernel for the rewind position and retires later deliveries from that partition.
The next poll retries a failed seek before fetching another record.
A concurrent nack cancels an outstanding poll before seeking.
Only the kernel's completed watermark reaches `Commit`.
A failed commit leaves that watermark due for retry.

Connect the client's rebalance callbacks to `Consumer.Revoke` and `Consumer.Assign`.
Stop delivering records from revoked partitions before calling `Revoke`.
It retires queued deliveries, attempts the last safe commit and waits for active handlers.
`DrainTimeout` bounds that wait. An overrun can overlap the next owner's handlers.
`Assign` resets old tokens before new records arrive.
Use `Lost` instead of `Revoke` when group membership has already been lost; it does not commit.
Do not invoke these callbacks synchronously from inside the client's `Poll` method.

`Run(ctx, dispatcher)` starts the dispatcher, polls and commits periodically.
It requires both `DispatcherOptions.OnDead` and `OnPoison`.
Use `kafka.DeadLetters[M](sink)` and `sink.Poison` to connect a `kafka.DeadLetterSink`.
The sink preserves the original fetched bytes and adds the kernel's failure headers.
The dispatcher acknowledges the source only after the dead-letter send succeeds.

On shutdown, `Run` drains handlers and attempts a final commit under separate deadlines.
Stop `Run` before closing the consumer or its client.
For manual polling, `delivery, poison, err := consumer.Poll(ctx)` returns exactly one result:
`delivery` for a decoded message, `poison` for an undecodable record, or `err` for a polling failure.
Send `poison` to a durable sink before acknowledging it. Its `Failure` field preserves the decoding
error; undecodable records do not use the poll error result.
This error means a fetched record could not become `M`. It preserves the raw record, the cause and settlement callbacks.
The typed memory broker cannot encounter undecodable wire data, so it never returns this error.
Both poll methods use the usual `(delivery, error)` shape; Kafka exposes the decode failure through that error value.
Backend failures contain fresh `*edax.Error` values. Use `errors.Is` for the taxonomy sentinel and `errors.As` for its code and message.
Wrapped client errors remain available through `errors.Is` and `errors.As`.
The [backend tests](../../crates/edax-go/backend/kafka/kafka_test.go) show client implementations and settlement flows.
They cover kernel behavior with fake clients. They do not run a live Kafka cluster.

## Test a backend you write

`edaxtest` runs the store contracts against a factory supplied by your tests.
Each factory receives `testing.TB`. It must return an empty, isolated backend and register its cleanup with `t.Cleanup`.

```go
func TestLedger(t *testing.T) {
    edaxtest.IdempotencyStore(t, func(t testing.TB) edax.IdempotencyStore {
        return newTestLedger(t)
    })
}
```

The package checks these contracts:

| Function | Required behavior |
| --- | --- |
| `EventStore` | Version checks, atomic batches, envelope fields, snapshots and event outbox |
| `IdempotencyStore` | Exclusive claims, cached results, stale handle fences and forget |
| `ProjectionStore` | Missing rows, version checks and preserved documents |
| `CommandOutbox` | Transactional enqueue and idempotent publication marks |
| `SagaStore` | Version checks, commands and completed tombstones |
| `Transactions` | Commit or rollback events, ledger, projections, commands and saga state together |
| `EventBroker` | Per-key order, nack replay and retired later deliveries |

`Transactions` takes a factory for `TransactionFixture` whose stores share one database and unit of work.
Do not pass Redis as that fixture's ledger. Redis cannot join the database transaction.
`EventBroker` takes publish and poll callbacks. Configure its fixture so the test's aggregate uses one ordered partition.

The [memory contract tests](../../crates/edax-go/edaxtest/contracts_test.go) provide a complete fixture example.
These checks cover the shared SDK promises. Add backend-specific failure and isolation tests for your database or broker.

# Go SDK

The Go module is `github.com/haskian/edax/crates/edax-go`. It provides command dispatch, aggregates,
repositories, idempotency, event routing, ordered consumption, outbox relays, sagas and projection
queries. These call the same Rust kernels as the Python SDK. Go interfaces and `context.Context`
connect the kernels to storage and transport.

The module includes an in-memory backend. Redis supplies a separate idempotency ledger. Kafka
adapters support managed franz-go clients and custom client interfaces. See [Go backends](../backends/go.md) for their setup
and limits. You still supply durable event, projection and saga stores.

Append/upsert ingestion and ordered folds currently have internal kernel bindings only. Go examples
[20](../../crates/edax-go/examples/20_ingestion/main.go) and
[21](../../crates/edax-go/examples/21_folds/main.go) run inside this module to show their decisions.
External modules cannot import `internal/kernel`. Go has no public ingestion/fold runner or
PostgreSQL adapter; see the [Python ingestion](../backends/ingestion.md) and
[fold](../backends/folds.md) APIs for the available orchestration.

## Build from source

The binding uses cgo and a native Rust static library. Install a C compiler and the repository's
[mise toolchain](../guides/getting-started.md). From the repository root:

```sh
mise install
mise run go:build
cd crates/edax-go
mise exec -- go test ./...
mise exec -- go test -race ./...
```

`mise run go:build` builds `edax-abi-ffi` and places its static library under the generated package's
`.lib` directory for the current platform. It supports Linux and macOS on amd64 and arm64.
`CGO_ENABLED=0` is unsupported. Rebuild after any kernel or ABI change; Go tests cannot rebuild
the Rust library themselves.

For an application using this checkout, add a local module replacement:

```sh
go mod edit -replace github.com/haskian/edax/crates/edax-go=/absolute/path/to/edax/crates/edax-go
go get github.com/haskian/edax/crates/edax-go
```

The Go native library must be built in that checkout before compiling the application. This is a
source installation workflow; the Python wheel does not install the Go library.

## Examples

The [twenty-one Go examples](../../crates/edax-go/examples/README.md) follow the Python learning order.
Each program checks its results. Run them after building the native library:

```sh
mise run examples:go
```

The task uses the race detector and runs the contract-kit example as a Go test. Examples 16 and 17
use memory by default. Set `EDAX_REDIS_URL` or `EDAX_KAFKA_BOOTSTRAP` to exercise a real backend.

## API map

| Concern | Go API | Shared contract |
|---|---|---|
| Commands | `CommandBus`, `CommandHandler`, `Middleware`, `Retry`, `Idempotent` | [commands](commands.md) |
| Aggregates | embed `Aggregate`, implement `Transition`, record changes with `Apply` | [aggregates](aggregates.md) |
| Repository | `Repository[A]`, `PayloadCodec`, `AggregateCodec[A]`, `SaveOptions` | [aggregates](aggregates.md) |
| Transactions | `UnitOfWork`, `Tx`, `WithinTransaction`, `CurrentTransaction` | [transactions](transactions.md) |
| Event handlers | `EventRouter`, `EventMiddleware`, `IdempotentByID` | [idempotency](idempotency.md) |
| Consumption | `Delivery[M]`, `KeyOrderedDispatcher[M]`, `Undecodable`, `DeadLetter[M]` | [consuming](consuming.md) |
| Publishing | `Outbox[M]`, `Relay[M]`, `Pump` | [outbox](outbox.md) |
| Sagas | `Saga`, `SagaStep`, `SagaStore`, `SagaRunner` | [sagas](sagas.md) |
| Read models | `ProjectionStore`, `Entity`, `QuerySpec`, filter constructors | [projections](projections.md) |
| Payloads | `NewJSONCodec`, `NewMessagePackCodec`, `Upcaster`, `Payload`; kernel envelope codecs | [event kernel](../kernels/event.md) |
| Observability | `Observer`, `Observation`, `WithObserver` and component observer options | [observability](observability.md) |

The linked pages explain delivery and storage contracts. Their examples use Python. Go method
signatures live in `crates/edax-go`; `go doc` shows the public interfaces and their comments.

## Typed event handler

The registry maps a stable wire name to your application type. `TypedEventHandler` decodes the
payload and returns codec errors through the normal handler path:

```go
type Credited struct {
    AccountID string `json:"account_id"`
    Amount    string `json:"amount"`
    Currency  string `json:"currency"`
}

codec, err := edax.NewJSONCodec(map[string]func() any{
    "AccountCredited": func() any { return new(Credited) },
})
if err != nil {
    return err
}
router := edax.NewEventRouter()
handler, err := edax.TypedEventHandler(codec, func(ctx context.Context, event *Credited) error {
    return updateBalance(ctx, event.AccountID, event.Amount, event.Currency)
})
if err != nil {
    return err
}
if err := router.Register("AccountCredited", handler); err != nil {
    return err
}
```

`updateBalance` is your application operation. A complete runnable example is
[`ExampleTypedEventHandler`](../../crates/edax-go/router_example_test.go); `go test` checks its output.
For broker delivery, put the router behind a dispatcher and add an idempotency guard around any
handler that changes stored state.

## Commands and transactions

A `CommandHandler` receives a context and a `CommandMessage`. It returns a result and an error.
Middleware wraps handlers in registration order, with the first middleware outermost.
`Dispatch` handles a local command. `Execute` handles a received envelope and uses the kernel's
fallback idempotency key when the envelope has none.

Pass `IdempotentOptions{Scope: "payments", Tenancy: "silo", UnitOfWork: db}` to an idempotency
guard. A ledger and business stores over the same `db` commit together. A separate Redis ledger
cannot join that transaction; its guard commits business work before completing the ledger record.
The resulting crash window is the same as [Python's separate-ledger mode](idempotency.md).

Use `TypedCommandHandler[C, R]` to decode an envelope before calling a typed domain handler.
Both typed handler constructors return an error immediately if the codec or function is nil.
Use `ExecuteAs[R]` for a typed result on both execution and cached replay:

```go
handler, err := edax.TypedCommandHandler(codec, payInvoice)
if err != nil {
    return err
}
if err := bus.Register("PayInvoice", handler); err != nil {
    return err
}
receipt, err := edax.ExecuteAs[Receipt](ctx, bus, message)
```

Here `payInvoice` is your domain handler and `Receipt` is its result type. The guard stores results
as JSON. Raw `bus.Execute` and `Guard` results therefore use `map[string]any` for objects, `[]any`
for arrays and `json.Number` for numbers, on both the first call and replay. An unguarded handler
keeps its original result type. `Dispatch` without an idempotency key also bypasses the guard.

Pass the handler's payload codec as `IdempotentOptions.Codec` whenever the handler decodes with
MessagePack or upcasts. The guard then hashes the decoded, upcast value, which is what the handler
runs on. Without the codec, the guard hashes the raw bytes. A renamed field then reads as a
different request, and the same bytes under a changed upcast replay the old result. Only a JSON
command that no upcaster touches can omit the option.

Declare exact decimal and timestamp fields as `edax.Decimal` and `edax.Instant`.
`edax.InstantNow()` reads the kernel clock and `edax.InstantFromUnixNanos()` reads a stored event's
`RecordedAt` or `OccurredAt`, both floored to the microsecond as every SDK floors them. These types use
the kernel's canonical text in JSON and MessagePack. A plain `float64` or `time.Time` field can
encode different bytes from a Python decimal or timestamp. Upcast transforms receive raw payload
bytes in the codec's chosen encoding.

The SDK writes every host value it hands to the kernel as JSON in one form. That covers the JSON
codec's payloads, filters, query and projection documents, the hashed command value and the cached
command result. It writes what `encoding/json` writes, except a `float64` or `float32`. Those keep
a fraction or an exponent, as in the Python SDK: `2.0` and `9007199254740992.0`, never `2` and
`9007199254740992`. So the kernel reads a whole float as a float. It neither refuses one past 2^53
nor sorts it as an integer. NaN and the infinities are written as `null`, as in every SDK. A float
`,string` field keeps Go's spelling. `ExecuteAs` converts through the same form, so a float result
never decodes into an integer type, guarded or not.

Place `Retry` outside the guard that owns the transaction. Each retry then starts a fresh unit of
work. Calling retry inside an existing transaction returns `ErrBadHandle`.

A guard with no `UnitOfWork` may run inside the host's `WithinTransaction` only when its ledger
joins that transaction: a `TransactionalStore` whose database has the context's open transaction.
Any other ledger would record success before the host commits, so `Guard` returns the fatal
`ErrBadHandle` before the claim. For a ledger of its own, set `IdempotentOptions.UnitOfWork`.

`WithinTransaction` passes a transaction through the context. Store methods use
`CurrentTransaction(ctx, owner)` to join the right database. Opening another transaction on that
same owner is refused. A repository or saga runner joins a transaction already present in the
context. Otherwise it opens one. `Repository.Load` returns the zero aggregate value on any error,
including a failed event transition or transaction commit. Dispatcher workers remove transaction values inherited from
the context passed to `Start`; each handler must establish its own transaction boundary.

## Consume events

Register event handlers by name with `EventRouter.Register`. An unknown name is ignored unless
`OnUnhandled` is set. Both registered handlers and the fallback run through the router's
middleware. The context carries the event's lineage while the handler runs.

Wrap a router in `KeyOrderedDispatcher[EventMessage]`. Use `KeyOrderedDispatcher[CommandMessage]`
with a handler that calls `CommandBus.Execute` to consume commands. The kernel chooses each
worker from the message's ordering key. `MaxQueue` limits each worker's waiting deliveries;
zero means unbounded. `Submit` waits when that queue is full and respects its context deadline.

A `Delivery` contains the message and transport-owned `OnAck`, `OnNack` and `Live` callbacks.
The dispatcher acknowledges successful handling. The kernel chooses retry, dead-letter or stop
after a handler error. `ErrInProgress` is contention and does not spend the failed-attempt budget.
A dead-letter sink must complete its durable write before returning nil. The dispatcher then
acknowledges the original delivery.

A broker that rewinds on nack must retire the copies it already handed out. `Live` tells the
worker to skip those copies. A queue that re-enqueues behind later messages cannot preserve the
same ordering guarantee. See [delivery guarantees](../guides/delivery-guarantees.md).

Kafka `Poll(ctx)` returns `(delivery, poison, err)`. Exactly one is non-nil: a decoded delivery,
an undecodable record, or a polling failure. Use `SettlePoison` for an `Undecodable` record. `OnPoison` receives the raw bytes and broker origin;
it cannot pass through an ordinary event handler. Kafka's `Run` requires both the decoded-message
dead-letter sink and the poison sink.

`Stop(ctx)` refuses new deliveries and drains accepted work. At the deadline it cancels workers
and returns the context error. Handlers must honor cancellation: Go cannot forcibly stop a
handler that keeps running. `Start` refuses to overlap such a worker with a replacement.
`WaitIdle` waits for accepted work; `WaitHandlers` waits for deliveries already taken by workers,
including their settlement. Use `Close` to release the native retry policies after shutdown.

## Relays and sagas

`Relay[M]` reads committed outbox rows, publishes them and marks settled rows. Its publisher must
return only after a durable broker acknowledgement. A crash between publish and mark republishes
the row. Consumers must deduplicate it. Concurrent drains on one relay take turns; multiple
relays need a backend that holds one lease over the whole outbox. Per-row leases break per-key
order.

The kernel's drain policy blocks later rows of a failed key while allowing other keys to advance.
It also decides retry counts and when a wider failure stops the batch. `Last` reads the last
completed batch without waiting for publication. `Close` rejects new drains immediately; an
active drain finishes before its native policy is released. `Run` retries transient
failures after their backoff and ends on fatal errors. `Pump` provides the alternative for a
change-data-capture iterator: the source commits its position only after publishing succeeds.

A `Saga` names itself, maps an event to an instance and returns a `SagaStep` from `Handle`.
The step contains JSON state, encoded command payloads and a completion flag. `SagaRunner`
asks the kernel whether to fold the event and whether to save, complete or do nothing.
The store atomically writes the state, folded marks and commands at the expected version.
Repeated events and completed instances do not fold again.

Use `runner.Outbox()` with a command relay. `runner.PublishTo(bus)` executes rows through a
command bus. `runner.CommandFailed` is a dead-letter sink that folds the kernel's failure event
back into the originating saga. Commands carry deterministic kernel IDs, their parent event's
lineage and its tenant. In a pooled store, `Saga.InstanceID` must include the tenant.

## Errors, lifetime and instrumentation

Use `errors.Is(err, edax.ErrConcurrency)` to compare an error code. Use `errors.As` with
`*edax.Error` to read its code and message. `IsTerminal` and `IsFatal` ask the kernel for the
verdict. An ordinary Go error has no kernel code and follows the kernel's unclassified-failure
rule. For joined errors, a fatal branch takes precedence, followed by a terminal branch.
The original error tree remains available through `errors.Is` and `errors.As`.
The `Err*` category sentinels are immutable constants. They support `errors.Is` but do not expose mutable `*Error` fields.
Do not classify errors by matching their message text.

The generated kernel facade is internal. The root package exposes message envelopes, store
records and selected helpers; native handles and decision records are implementation details.
`ErrorCode` and `Tenancy` names are generated from the Rust kernel. Use `CodeStore`, `CodeCodec`
and the other error constants when creating failures. Use `TenancySilo`, `TenancyPool` or
`TenancyBridge` in options. Unknown string values remain subject to kernel validation.

Native text arguments and record fields must be valid UTF-8 without embedded NUL characters.
The facade returns `ErrCodec` for unsupported text before it crosses the C interface.
Binary payloads use `[]byte` and have no such restriction.

SDK components that own native resources have `Close() error`. Close each owner when it finishes.
The internal facade serializes native calls with `Close` so a goroutine cannot call a freed handle.
A runtime cleanup releases unreachable handles as a fallback; its timing is not deterministic.
Aggregate instances are mutable domain objects and belong to one caller. Configure payload registries before concurrent
use.

Observers receive completion records without payload bytes. They must support concurrent calls.
A panic in an observer is logged with its value and stack and does not change message settlement.
Use `WithLogger(ctx, logger)` or a component’s `Logger` option to select its `slog.Logger`. The
default is `slog.Default()`. Use stable operation and message names for metric labels; IDs belong
in logs and traces.

Go's context and shutdown rules are described in the [standard library](https://pkg.go.dev/context)
and [memory model](https://go.dev/ref/mem). Cancellation signals work to stop; synchronization
establishes when its effects are visible to another goroutine.

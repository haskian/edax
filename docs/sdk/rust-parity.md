# Rust and Python parity

The Rust `edax` crate (`crates/edax`) is a reference model, not a deployable software development kit
(SDK). It is synchronous, it ships in-memory backends only, and it is `publish = false`. It exists to hold
the Python SDK's orchestration to the kernels from a second language. Twin test suites run the same case
tables and the same golden vectors in both. A shared decision lives in a kernel, so both SDKs call the same
code. This page lists what the model covers and where it differs on purpose. The model gains a feature
only when a shared decision needs a second implementation to pin it, never to match Python name for name.

## What the model holds to the kernels

| Concept | Rust `crates/edax` | Python `crates/edax-py/python/edax` |
|---|---|---|
| aggregate | `Aggregate`, `Root` | `Aggregate`, `Version` |
| event codec | `RegistryCodec<T, PayloadFormat>`, `SerdeCodec<T, PayloadFormat>`, `Upcaster` | `json_codec()`, `msgpack_codec()`, `Upcaster` |
| repository | `Repository` (`load` / `save`, `load_in` / `save_in`, `save_with` for a lineage, `load_for` / `load_for_in` for a tenant) | `Repository` (`tx=None` joins the ambient unit of work; `save(commands=...)` stages a command outbox; `load(tenant=...)` for a tenant) |
| tenancy | `Repository::tenancy(Tenancy)`, and `edax_core::tenant_key` where a host keys | `tenancy=` on `Repository`, `idempotent` and `KafkaProducer`; `edax.tenant_key` where a host keys |
| outbox drain | `Relay::drain` over any `Outbox<R>` | `Relay.drain` and `Relay.run`, plus `pump` |
| command bus | `CommandBus` (`register::<C, O>`, `wrap`), `retry`, `idempotent`, `idempotent_in`, `Terminal` | `CommandBus` (`dispatch`, `dispatch_message`), `command_message`, `current_command`, `retry`, `idempotent`, `forget`, `Rejected` |
| consume path | `KeyOrderedDispatcher`, `Delivery::with_live`, `EventRouter`, `idempotent_by_id` | `KeyOrderedDispatcher`, `Delivery.live`, `EventRouter`, `idempotent_by_id` |
| saga | `Saga`, `SagaRunner` with `command_failed`, `SagaStore` with its outbox | the same, plus `SagaRunner.relay` and the `saga_skipped` observation |
| transaction | `Transactor` (with `identity`), `transaction`, `retry_transaction` | `Transactor`, `transaction`, `current_tx` |
| messages | `EventMessage`, `EventDraft`, `CommandMessage`, `Lineage` from `edax-event` | the same native classes |
| in-memory backends | `MemoryTransactor` and its stores, `MemoryLedger`, `MemoryBroker` (`with_partitions`, `group`, `crash`) | the same, plus `MemoryCommandOutbox` and the Redis and Kafka adapters |

The decisions behind every row are the kernels': `edax-core`, `edax-event`, `edax-cqrs`, `edax-memory` and
`edax-kafka`. So the two columns cannot disagree on a hash, a verdict, a fence, a commit watermark or a wire
byte. The in-memory stores are one implementation (`edax-memory`) behind both.

## Where the model differs

- **No ambient transaction.** Rust has no `current_tx`. A handler is a synchronous closure on its caller's
  thread. So a participant takes the handle explicitly (`&mut Tx`). A guard gets it from a provider closure
  the host passes (`idempotent_in(store, scope, key, current)`). The closure takes the command or the event
  message, so each worker can return its own handle.
- **No ambient message.** Rust has no `current_command` and no ambient lineage. A command is a plain value:
  the guard reads its key through a closure, `idempotent(store, scope, key)`, and a save takes its
  `Lineage` explicitly through `save_with`. The scope, the claim key and the hash are the kernel's, so
  both SDKs claim the same key with the same hash.
- **A nested unit of work is refused, not joined.** `transaction` keeps the databases open on its thread,
  keyed by `Transactor::identity`. A second unit of work on one of them is refused with the terminal
  `bad_handle` before it begins. `Repository::save` inside a host's body is such a unit of work. It can
  therefore never commit apart from the host; `save_in` joins the open one. Python joins the ambient unit
  of work instead. The limit is the identity. Two transactors over one database are caught only when the
  backend reports one identity for both, as the memory backend does.
- **No guard that owns the transaction.** There is no `unit_of_work=` and no `joins_transaction`: no
  durable Rust backend ships, so the two-commit ordering has nothing to describe. `MemoryLedger` (`Tx = ()`)
  is the ledger of its own, so the split ordering and the claim fence can still be rehearsed.
- **No I/O shells.** The `edax-kafka` and `edax-redis` kernels have Python shells only.
- **No telemetry seam.** Rust has no `Observer` and no `timing` middleware.
- **Threads, not tasks.** Dispatcher workers are operating-system threads; `retry`, the relay and the
  dispatcher sleep their backoff on the calling thread through `Pacing`.

## See also

- [Stability](../stability.md) — what the version promises, and that the Rust crates are outside it.
- [Idempotency](./idempotency.md) — the two guard orderings and every failure window.
- [Transactions](./transactions.md) — `Transactor`, `current_tx` and `transaction`.

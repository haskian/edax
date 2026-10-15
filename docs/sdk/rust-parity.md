# Rust and Python parity

The Rust `edax` crate (`crates/edax`) is a reference model, not a deployable software development kit
(SDK). It is synchronous, it ships in-memory backends only, and it is `publish = false`. It exists to hold
the Python SDK's orchestration to the kernels from a second language. Twin test suites run the same case
tables and the same golden vectors in both. A shared decision lives in a kernel, so the Rust and Python SDKs call the same
code. This page lists what the model covers and where it differs on purpose. The model gains a feature
only when a shared decision needs a second implementation to pin it, never to match Python name for name.

## What the model holds to the kernels

| Concept | Rust `crates/edax` | Python `crates/edax-py/python/edax` |
|---|---|---|
| aggregate | `Aggregate`, `Root` | `Aggregate`, `Version` |
| event codec | `RegistryCodec<T, F>`, `SerdeCodec<T, F>` with `F` the marker `Json` or `Msgpack`, `Upcaster` | `json_codec()`, `msgpack_codec()`, `Upcaster` |
| host values | a field declared as `Decimal` or `Instant` (from `edax_cqrs`) | a field declared as `edax.Decimal` or `edax.Instant` (`edax.host_value`); the encoder also writes a `decimal.Decimal` and an aware `datetime` outside a codec |
| repository | `Repository` (`load(tenant, id)` and `save(aggregate, SaveOptions)` in a unit of work of their own, `load_in` / `save_in` inside the caller's; `SaveOptions` carries the lineage, extensions, metadata and `occurred_at`) | `Repository` (`tx=None` joins the ambient unit of work; `save(commands=...)` stages a command outbox; `load(tenant=...)` for a tenant) |
| tenancy | `Repository::tenancy(Tenancy)`, a `Tenancy` argument on `idempotent`, `idempotent_by_id`, `forget`, `forget_event` and `MemoryBroker::with_partitions`, and `edax_core::tenant_key` where a host keys | `tenancy=` on `Repository`, `idempotent`, `idempotent_by_id`, `forget`, `forget_event`, `KafkaProducer` and `MemoryBroker`; `edax.tenant_key` where a host keys |
| outbox drain | `Relay::drain` over any `Outbox<R>` of messages, the kernel's `Drain` deciding; the host's loop stops on an `is_fatal` error; `CountedOutbox` for the backlog depth | `Relay.drain` over `_edax.Drain`, and `Relay.run`, which ends on an `is_fatal` error, plus `pump`; `CountedOutbox` |
| command bus | `CommandBus` (`register::<C>`, `register_with_codec::<C, _>` with the type's codec, `dispatch`, `dispatch_message::<C>`), `command_message` with a codec, `DispatchOptions`, `retry`, `idempotent`, `idempotent_in`, `forget`, `Terminal`, `Rejected` with a `Reason` (the write side only: its replay error carries no reason) | `CommandBus(codec=...)` (`register`, `dispatch`, `dispatch_message`), its codec a `CommandCodec` that names the class it reads each command as, in place of Rust's `Domain = C` bound; `command_message`, `current_command`, `retry`, `idempotent`, `forget`, `Rejected` (a subclass names its `reason=`), `reason_of` |
| consume path | `KeyOrderedDispatcher<M>` over events or commands (`submit` takes a `Delivery<M>` only and returns the fatal error that stopped it), `Delivery::with_live`, `EventRouter`, `idempotent_by_id`, `forget_event` | `KeyOrderedDispatcher[M]` (`submit` takes a `Delivery[M]` only and raises it), `Delivery.live`, `EventRouter`, `idempotent_by_id`, `forget_event` |
| saga | `Saga`, `SagaRunner` with `command_failed` and `outbox`, `SagaStore` keyed by the saga's name, `SagaOutbox` (a `CountedOutbox`) | the same, plus `SagaRunner.publish_to` and the `saga_skipped` observation |
| transaction | `Transactor` (with a required `identity`), `transaction`, `retry_transaction` | `Transactor`, `transaction`, `current_tx` |
| messages | `EventMessage`, `EventDraft`, `CommandMessage`, `Lineage` and the codec's `Payload` from `edax-event` | the same native classes |
| in-memory backends | `MemoryTransactor` and its stores, `MemoryLedger`, `MemoryBroker` (`with_partitions(partitions, tenancy)`, a fallible `publish`, `group`, `crash`) | the same, plus `MemoryCommandOutbox` and the Redis and Kafka adapters |
| store contract kit | `edax::testkit::contract` behind the `testkit` feature: one function per seam (`saga_store`, `outbox`, `counted_outbox`, `event_store`, `idempotency_held_key`, `idempotency_unreadable_record`, `idempotency_forget`); no projection or paging contract yet, so a Rust projection backend runs the kernel's rules itself (`edax::document`, `edax::keyed_document`, `Query::validate`) | `edax.testing`: one `*Contract` class per seam, plus the broker and projection suites; `edax.projection.check_document`, `keyed_document` and `edax.query.check_query` |

The decisions behind every row are the kernels': `edax-core`, `edax-event`, `edax-cqrs`, `edax-memory` and
`edax-kafka`. So the two columns cannot disagree on a hash, a verdict, a fence, a commit watermark or a wire
byte. The in-memory stores are one implementation (`edax-memory`) behind both.

The orchestration steps each SDK runs as its own input and output loop decide through the kernel too, so the
two loops only differ in how they wait and unwind:

| Step | Kernel decision | Golden vector |
|---|---|---|
| `retry` and `retry_transaction` | `edax_event::retry_step` | `retry_step.json` |
| the relay's drain | `edax_event::Drain` | `relay_drain.json` |
| a save's drafts, version and snapshot | `edax_event::draft_batch`; a load's version from `check_batch` | `draft_batch.json`, `sequence_batch.json` |
| the guard's failure settle and a loser's answer | `edax_cqrs::settle_failure`, `edax_cqrs::replay` | `idempotency_settle.json`, `idempotency_guard.json`, `idempotency_record.json` |
| an event payload's bytes | `edax_event::Format::encode_payload` / `decode_payload` (a struct from a map only, at every depth) | `event_codec.json` |

A Rust host must declare a decimal field as `edax::Decimal` and an instant field as `edax::Instant`.
A Python host declares `edax.Decimal` and `edax.Instant`, the same two types. Python's encoder writes
them, and every `decimal.Decimal` and `datetime`, in the kernel's canonical text. So a command hash,
a document and an event payload carry one text per value. A Rust field of another type does not:
an `f64`, a `String` that holds `9.50`, or a timestamp type that keeps its offset writes other bytes.
Its command then hashes apart from the same command sent from Python, which the shared ledger reads
as a payload mismatch. In both SDKs `Decimal` and `Instant`
canonicalize when they are built and refuse what the kernel refuses (`NaN`, an instant without an
offset). Both read a payload field from its text, so a fraction finer than a microsecond is refused,
not rounded. The `Withdraw` rows of `command_hash.json` and
the `Priced` row of `event_codec.json` pin them.

## Where the model differs

- **No command outbox on save.** Python's `save(commands=...)` stages commands with the events and
  stamps each with the aggregate's tenant through the kernel's `own_command` (`command_owner.json`).
  The Rust `Repository` has no command outbox, so it has no such path; a Rust host enqueues its own.
- **No ambient transaction.** Rust has no `current_tx`. A handler is a synchronous closure on its caller's
  thread. So a participant takes the handle explicitly (`&mut Tx`). A guard gets it from a provider
  closure. The host passes that closure to `idempotent_in(store, scope, tenancy, current)`. The
  closure takes the command or the event message, so each worker can return its own handle.
- **No ambient message.** Rust has no `current_command` and no ambient lineage. The message is the unit
  of dispatch, as in Python: middleware receives it as an argument, and the guard reads its key and
  tenant off it. A handler receives the command alone and has no way to read the message. `dispatch`
  starts a new flow unless `DispatchOptions::lineage` names one, and a save starts one unless
  `SaveOptions::lineage` names one. The claim key, tenant included, and the hash are the
  kernel's (`claim_key`, `event_idempotency`), so the Rust and Python SDKs claim the same key with the same hash under
  every tenancy. `forget` and `forget_event` delete under that key in both.
- **A nested unit of work is refused, not joined.** `transaction` keeps the databases open on its thread,
  keyed by `Transactor::identity`. A second unit of work on one of them is refused with the terminal
  `bad_handle` before it begins. `Repository::save` inside a host's body is such a unit of work. It can
  therefore never commit apart from the host; `save_in` joins the open one. Python joins the ambient unit
  of work instead. The limit is the identity. Two transactors over one database are caught only when the
  backend reports one identity for both, as the memory backend does.
- **No guard that owns the transaction.** There is no `unit_of_work=` and no `joins_transaction`: no
  durable Rust backend ships, so the two-commit ordering has nothing to describe. `MemoryLedger` (`Tx = ()`)
  is the ledger of its own, so the split ordering and the claim fence can still be rehearsed. One
  consequence differs from Python. `idempotent_in` writes a terminal failure's cached record through the
  host's handle, so it lands if the host commits after the error. Python's one-commit guard never writes
  that record ([idempotency](./idempotency.md)). The host owns the commit, so it decides.
- **No I/O shells.** Python and Go provide adapters over `edax-kafka` and `edax-redis`; the Rust reference model does not.
- **No telemetry seam.** Rust has no `Observer` and no `timing` middleware, and the model takes no logging
  dependency. So three failures pass silently in Rust, on purpose. Python logs each and carries on:
  - A snapshot that does not load. `Repository::load` replays the stream from version 0.
  - A compensation the idempotency store refuses after a failed or panicking handler. The handler's
    error, or the panic, is what the caller sees.
  - A relay's dead-letter sink that fails. `drain` returns the publish error and hands the row back.

  Each SDK takes the same decision in each case. Only Python's log line is missing. The dispatcher has
  no `stats()` either. Each Rust worker keeps its own retry policy, and nothing reports the size of its
  attempt map. Nor is there a `settle_poison`: a payload that will not decode belongs to an I/O shell.
- **Threads, not tasks.** Dispatcher workers are operating-system threads; `retry`, the relay and the
  dispatcher sleep their backoff on the calling thread through `Pacing`.
- **Names Python cannot spell.** A keyword or a builtin takes a trailing underscore: the filter
  constructors `and_`, `or_`, `not_` and `in_`, and `Upcaster.to(data, from_=..., to=...)`. Every
  other public name matches its Rust twin: `DeadLetter`, `RegistryCodec`, `UpcastStep`,
  `tenant_key(mode, tenant, key)`.

## See also

- [Stability](../stability.md) — what the version promises, and that the Rust crates are outside it.
- [Idempotency](./idempotency.md) — the two guard orderings and every failure window.
- [Transactions](./transactions.md) — `Transactor`, `current_tx` and `transaction`.

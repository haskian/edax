# What a version promises

edax follows [Semantic Versioning](https://semver.org). This page says which parts of the project
that promise covers. A toolkit with a Rust core, two software development kits (SDKs) and a set of
on-disk formats has more surfaces than a library usually does.

Until 0.1.0 nothing here applies: 0.0.x is early alpha and every surface may change without notice.

## Before 1.0

A minor release (`0.x` to `0.(x+1)`) may break any surface below. A patch release (`0.x.y` to
`0.x.(y+1)`) breaks none of them.

That is what `0.` means. The seams are young, and some of them will move once real backends are
written against them. Pin a minor in your dependency declaration
(`edax ~= 0.1.0`, `edax = "0.1"`) and read the changelog before you lift it.

## What is covered

| Surface | Promise |
|---|---|
| Names exported from `edax` (Python) | The stable API. Breaks only in a minor. |
| Kernel decisions: local partition and Kafka placement, the ordering key, lineage, the extension key rule, the canonical form (RFC 8785) and the payload and command hashes, the claim key, the idempotency record, verdict, settle and forget, the replay of an unreadable record, filter order and keyset cursors, the upcast plan, the retry and dead-letter policy, the saga step | Pinned by golden vectors. A change here is a breaking change even if no signature moves. |
| The message wire format, events and commands, and its media types | Pinned by golden vectors. Within a minor, old bytes stay readable. A minor may refuse an old shape; the upgrade notes below name each one. |
| The Redis key, value layout and scripts | A cross-SDK contract: two services share a ledger only while it holds. The value carries a layout version, so a later layout is named, and replays as a cached `codec` failure instead of being split at the wrong place. |
| The storage seam protocols (`EventStore`, `ProjectionStore`, `IdempotencyStore`, `SagaStore`, `Outbox`, `CountedOutbox`, `CommandOutbox`, `Transactor`), each generic in its transaction handle | Adding a required method is a breaking change. Adding an optional one, as its own port or declared by a capability flag, is not. |
| The conformance suites in `edax.testing` | A new clause may fail a backend that passed before, so clauses are added in a minor. |

## What is not covered

- Anything named with a leading underscore, and the generated `_edax` extension module.
- The exact text of an exception message. The type and its taxonomy code are stable; the prose is not.
- Log and observer output beyond the documented `Observer` methods.
- The examples, the documentation and the test suite.
- The Rust crates. Nothing publishes them. The release ships the `edax` Python wheel, which reaches them
  as path dependencies, and their surface follows the Python SDK's needs. That includes the Rust `edax`
  crate (`crates/edax`), which is a reference model, not a deployable SDK. It is synchronous and ships
  in-memory backends only. It implements the Python orchestration a second time, against the same
  kernels. Twin test suites run both over the same case tables and golden vectors. It is `publish = false`. It gains
  a feature only when a shared decision needs a second implementation to pin it. See
  [Rust and Python parity](sdk/rust-parity.md).

## Upgrade notes

### 0.7 to 0.8

- **Payload host values are `edax.Decimal` and `edax.Instant`.** `json_codec` and `msgpack_codec`
  refuse a `datetime` or `decimal.Decimal` field at any depth. They raise `CodecError` when they are
  built. msgspec converted such a field before the kernel saw its text. So a fraction finer than a
  microsecond was rounded, and `"1_000"`, `" 9.5 "` or a JSON number read as a decimal.
  The Rust and Go SDKs refuse all of them. Declare the field as `edax.Decimal` or `edax.Instant`
  instead. Each is built from text through the kernel. Convert with `to_decimal()`, `to_datetime()`,
  `Decimal.from_decimal()` and `Instant.from_datetime()`. Stored payloads keep their bytes: a value
  an earlier release wrote reads as before, a MessagePack timestamp included. A stored value the
  kernel refuses now raises `CodecError` on read, so a consumer dead-letters it.
- **Plain values outside a codec.** A filter, a document, a saga state and a command hash still
  accept a `decimal.Decimal` and an aware `datetime`. They are written as the host types' text.

### 0.5 to 0.6

- **Saga store seam keyed by the saga's name.** Every `SagaStore` method takes the saga's `name`
  first: `load(saga, id)`, `save(saga, id, ...)`, `complete(saga, id, ...)`, `unpublished(saga, limit)`,
  `mark_published(saga, ids)`. Key the instance table and the outbox by `(saga, id)`, so two sagas
  over one database share neither an instance nor a command. A relay drains one saga's outbox:
  `Relay(runner.outbox(), publish)`, not `Relay(store, publish)`. Existing rows need the saga's name
  added to their key.
- **Drain legacy saga commands before migrating.** Version 0.6 scopes command IDs by saga name;
  version 0.5 used `instance:version:index`. New runners do not retain every failure of an old-format
  command, so replay after another failure can repeat compensation.
  Quiesce new saga inputs and drain all saga command outboxes under 0.5, including commands emitted
  by compensation. Settle or explicitly resolve old dead letters and pending failure notifications
  before stopping the old runners. Verify no old commands or failure deliveries remain in brokers,
  outboxes, or scheduled redrives; then migrate instance keys and deploy 0.6.
  Preserve saved folded marks and idempotency records. Do not rewrite command IDs to the new format:
  a changed identity can execute an already-completed command again. If old deliveries remain,
  postpone the upgrade; adding saga names to storage keys alone is unsafe.
- **Store refusals.** `IdempotencyStore.complete` refuses a pending record with `BadHandle`, and
  `ProjectionStore.save` refuses a document that is not a JSON object with `CodecError`
  (`edax.projection.check_document`). The contract kit holds a store of your own to both.
- **`Event` is now `Payload`.** The codec's output, `name`, `version` and `data`, is `edax.Payload`
  (Rust `edax::Payload`, `edax_event::Payload`), since a command carries one too. `EventCodec.marshal`
  returns it. The structural type `unmarshal` reads is `edax.message.Marshaled`.
- **Projection ports.** `ProjectionStore.save` returns the new version and leaves the `Entity` as it
  was; `Entity` is frozen (Rust: `save(tx, &entity) -> Result<u64, Error>`). `get_many` moved to its
  own port, `ProjectionQuery`, and the contract kit runs the paging clauses for a store that is one
  (`supports_kernel_queries` is gone). `get` returns the document with its row key as `"id"`, as
  `get_many` does (`edax.projection.keyed_document`).
- **Outbox reads take a limit.** `Outbox.unpublished(limit)` and `SagaStore.unpublished(saga, limit)`
  have no default; `Relay.drain` and `Relay.run` default to `edax.outbox.DEFAULT_BATCH`. `Relay` is
  generic in its row type. `SagaRunner.relay(bus, ...)` is gone: write
  `Relay(runner.outbox(), runner.publish_to(bus), on_dead=runner.command_failed)`.
- **Command bus.** `dispatch_message` runs the message as received: middleware and
  `current_command()` see the key the sender set, and the guard claims a keyless one under its id. A
  duplicate handler raises `AlreadyRegistered` naming it, in the bus and the router alike. A `scope`
  that is not a `str` raises `BadHandle`. The codec protocol is `PayloadCodec` (`EventCodec` stays its
  name in the event role), and `RegistryEventCodec` is `RegistryCodec`.
- **Stored naive datetimes stop reading.** `json_codec` and `msgpack_codec` now check every `datetime`
  and `Decimal` they read, as they check what they write. An event stored with a naive `datetime` (no
  UTC offset) raises `CodecError` on read. Its aggregate stops loading, and a consumer dead-letters it.
  So does a stored `Decimal` that is `NaN` or infinite. Before the upgrade, rewrite such payloads with
  an offset, or give the name an upcaster step that adds one.
- **Observer label.** The delivery outcome `stale` is `retired`.
- **Memory backends.** `MemoryLedger` wraps a `MemoryIdempotencyStore` instead of subclassing it, and
  exposes no `transactor`. `Snapshot` is frozen.
- **Kernel paths (Rust).** `edax_event::domain` is `edax_event::message`, `edax_event::dedup` is
  `edax_event::idempotency`, and `received_key` moved to `edax_cqrs`. `payload_hash` lives in
  `edax_core` (`edax_cqrs` re-exports it), and `edax_core::verdict` is `edax_core::is_terminal`.
  `saga_command` takes a `Cause` in place of the folded event's id, correlation id and tenant.
  `edax_cqrs::query` and `edax_cqrs::record` are private: name their items from the crate root.
- **One dead-letter value.** Every sink takes one `DeadLetter`: `message`, `attempt`, `failure` and
  `origin`, from a dispatcher and a relay alike. A dispatcher sink was `on_dead(delivery, error)` and a
  relay sink `on_dead(row, error)`; read `letter.message`, `letter.failure` and `letter.origin` instead.
  Python and Rust name the field `failure`. Go exports it as `Failure`.
  `SagaRunner.command_failed` takes the `DeadLetter` too. In Rust, `Relay::on_dead` takes a
  `DeadLetter<&R, &HandlerError>`, and `SagaRunner::command_failed(tx, dead)` takes it.
- **Poison is `Undecodable`.** A record that will not decode is an `edax.Undecodable` (`id`, `payload`,
  `error`), not an `UndecodableDelivery` with a stand-in event. `Consumer.poll` returns it, and a
  dispatcher sink gets it as `DeadLetter.message`. `settle_poison(poison)` takes it alone.
- **Consumer loop.** The `Consumer` protocol is `poll` alone. `edax.broker.consume(consumer,
  dispatcher)` replaces the `ConsumerLoop` mixin; `KafkaConsumer.run` and `MemoryBroker.run` stay.
- **Async event handlers.** `EventRouter.register`, `on` and `on_unhandled` refuse a handler that is
  not a coroutine function with `TypeError`, as `CommandBus.register` does.
- **Saga instances.** `SagaStore.load` returns a `RunningInstance(state, version, folded)` or a
  `CompletedInstance(version)`, not a `SagaInstance` with a `completed` flag.
- **Renames.** `tenant_key(mode, tenant, key)` takes the tenant before the key, as Rust does.
  `edax.upcast.Upcast` is `UpcastStep`. `edax.projection` and `edax.store` no longer re-export
  `ConcurrencyError`: import it from `edax` or `edax.errors`. The dispatcher's `handler` attribute is
  private. `edax.upcast.schema_version` is gone; `Upcaster.step` checks the version itself.
- **A command bus owns its codec.** Build it as `CommandBus(*middleware, codec=commands)` and call
  `dispatch_message(msg)` without a codec. `register` refuses a command the codec does not name with
  `BadHandle`, and so does `dispatch_message` on a bus built without a codec. `dispatch` builds its
  message with the codec too. `SagaRunner.publish_to(bus)` refuses a bus with no codec. The bus takes a
  `CommandCodec`: a `PayloadCodec` with `type_of(name)`, which `RegistryCodec` has. A hand-written
  command codec adds it. `register` also refuses a class the codec reads the name as another class.
- **Wiring refusals.** `KeyOrderedDispatcher.submit` refuses a `Delivery.attempt` outside an unsigned
  32-bit integer with `BadHandle`, and `KafkaConsumer` refuses a serializer whose `kind` is no message
  kind. Both used to fail later, with the delivery unsettled.
- **Typing.** A dispatcher's `Handler[M]` is async, as a router's handlers are. A sync one still runs,
  but a type checker refuses it. `Consumer`, `Polled` and `consume` are generic in the message kind, so
  `consume` checks that the consumer and the dispatcher carry the same one.

### 0.4 to 0.5

- **Decimal and datetime encoding.** The SDK's one encoder writes a `Decimal` as the kernel's canonical
  decimal text (`"9.5"` for `Decimal("9.50")`). It writes an aware `datetime` as fixed-width UTC text
  (`"2024-01-01T00:30:00.000000Z"`). It refuses a naive `datetime`, and a `Decimal` that is `NaN`,
  infinite or past 1000 digits, as `CodecError`.
- **Command hashes move.** A command that carries a `Decimal` or a `datetime` hashes differently from
  0.4. A client retry that crosses the upgrade with such a command runs its handler again. Let the
  idempotency ledgers expire first if that matters.
- **Stored documents and saga states.** Documents written by 0.4 keep the old text. Rewrite them, or a
  filter or a sort compares old and new spellings apart. A cached command result that held a
  `Decimal` replays its canonical text, so `Decimal("50.00")` comes back as `Decimal("50")`.
- **Decimal order.** A range filter or a sort orders decimal text by value only under the decimal
  collation. A `Decimal` comparand sets it, and `sort_key(path, decimal=True)` sets it for a sort.
- **Codec versions are per name.** `json_codec` and `msgpack_codec` lost `version=`. Each event name
  now writes one past the highest step of its own upcaster, and 1 with none. A row stored at the old
  registry-wide `version` reads as written by a newer writer, so its aggregate stops loading with
  `CodecError`. Before the upgrade, give each name identity upcaster steps up to the old `version`:
  `Upcaster().step(1, lambda data: data)` for a 0.4 codec built with `version=2`.
- **Event guard keys moved.** `idempotent_by_id` now claims `edax:evt:<scope length>:<scope>:<id>`, no
  longer the raw event id. No 0.4 record is found again, so a redelivery that crosses the upgrade runs
  its handler again. Let the event idempotency ledgers expire first, or drain every consumer past its
  topic before the upgrade.
- **Integers at ±2^53.** The canonical form and the keyset cursor now refuse an integer of magnitude
  2^53, as they refused larger ones. A command or a sort value that carries one raises `CodecError`.
  Carry such a value as a string or a `Decimal`.
- **Tenancy is required.** `idempotent`, `idempotent_by_id`, `claim_key` and `forget` take a `tenancy`
  with no default. Pass `tenancy=Tenancy.SILO` where you relied on the default, or the mode the
  deployment runs.

### 0.1 to 0.2

0.2 is a minor release, so it may break the surfaces above, and it does.

- **The message wire format.** 0.2 replaces the 0.1 event envelope with two flat messages,
  `EventMessage` and `CommandMessage`. The payload no longer nests, `namespace` is gone, and
  `timestamp` became the store's `recorded_at` beside an optional `occurred_at`. The lineage moved out
  of `metadata` into the typed `correlation_id` and `causation_id` fields. Trace context moved to
  `extensions`. MessagePack is a map keyed by field name, never 0.1's positional array.
- **Media types.** `application/vnd.edax.envelope+json` and `+msgpack` are gone. Events are
  `application/vnd.edax.event+json` and `+msgpack`. Commands are `application/vnd.edax.command+json`
  and `+msgpack`. A 0.2 consumer treats a 0.1 media type as poison and dead-letters it. A 0.1
  record without the header fails the new shape the same way.
- **Command hash and claim key.** The command hash now covers the command name with the payload.
  A command guard claims `claim_key(scope, key)`, not the raw key. So every hash a 0.1 ledger stored
  differs from the one 0.2 computes, under a key 0.2 never claims. A client retry that crosses the
  upgrade runs its handler again. Event guards keep the raw event id until 0.5.
- **Drain before you upgrade.** 0.2 has no reader for any 0.1 message, by design. Stop the 0.1
  producers. Let every relay publish its outbox rows, saga command outboxes included, and every
  consumer commit past them. Let every idempotency ledger expire, past the longest window a client
  retries in. Then start 0.2 everywhere.
- **Event store seam.** `EventStore.append` takes `EventDraft`s and returns the stored
  `EventMessage`s, each stamped with the store's `recorded_at`. A durable store needs columns for
  `occurred_at`, `recorded_at`, `correlation_id`, `causation_id` and `extensions`.
- **Command identity.** `idempotent` takes a required `scope` and no `key` function. The key comes
  from the dispatch, `dispatch(cmd, idempotency_key=...)`, or from the message's `idempotency_key`.
  A saga's `correlation_id(env)` became `instance_id(msg)`, and `SagaCommand` became `CommandMessage`.
  `SagaRunner.relay` takes the `CommandBus`, not a dispatch callable. `Delivery.envelope` became
  `Delivery.message`.
- **New required seam methods.** `IdempotencyStore` gained `forget(key, tx=None)`, and `RedisConn`
  gained `get(key)`. A store or a connection of your own must implement them.
- **Saga store seam.** `SagaStore.save` takes the instance's `folded` marks, and `load` returns them
  as `SagaInstance.folded`. A durable saga store needs a text column for them. An instance saved
  before the column existed has empty marks, so its first redelivery after the upgrade may fold once
  more.
- **Serializer seam.** A `Serializer` declares `kind`, `"event"` or `"command"`, beside its
  `media_type`. `decode` returns an `EventMessage` for the first and a `CommandMessage` for the
  second. A serializer of your own must add it.
- **Kafka route.** The kernel's route takes the default serializer's kind. Under a command default,
  a record with no `content-type` header is the `headerless` route, and the consumer dead-letters it.
- **Lineage helpers.** `ensure_ids` is gone; `originate()` replaces it. `propagate(msg)` and
  `originate(correlation_id=, causation_id=)` return a `Lineage`, not a metadata dictionary.
- **Store clock.** `Repository(now=)` is gone. The store stamps `recorded_at` with its own clock:
  `MemoryEventStore(now=)`, or a durable store's own.
- **Saga name.** A `Saga` declares `name`. Each command it sends carries the idempotency key
  `"{len(name)}:{name}:{id}"`, not the bare command id. So two sagas that share an instance id and a
  bus never claim one key.
- **Retry policy at the boundary.** The native `retries` takes the sink flag at construction, and
  `Retries.failed` takes the failure's error code instead of a failure kind.
- **Unreadable idempotency records.** A stored record no reader can decode replays as
  `CachedFailure` of code `codec`, in the Rust and Python SDKs, instead of raising `CodecError` on every claim. So
  does a cached result that does not convert to the handler's `result_type`. A store reads the record
  that holds a key with `edax.idempotency.decode_prior`; a store of your own that calls `decode_record`
  there must switch.

## Supported runtimes

Python 3.10 and later. The wheel is `abi3-py310`, so one wheel serves every later interpreter.

Rust: the channel pinned in `rust-toolchain.toml`, for building the kernels the wheel links. A minimum
supported Rust version is not declared, because no Rust crate is published.

## Deprecation

Before 1.0 there is no deprecation cycle: a minor removes or renames a name outright, and the
changelog names the replacement. edax keeps no compatibility aliases while the seams are young.

From 1.0, a name that is going away is marked deprecated in one minor and removed no earlier than the
next one. The deprecation names its replacement. Python marks it with `DeprecationWarning`.

## How a release is cut

[version.yml](../.github/workflows/version.yml) chooses the increment from Conventional Commit subjects.
`versionbump!` requests a major release. `feat` requests a minor release. `fix` requests a patch release.
A breaking `feat!` still requests a minor release.

Cocogitto updates the changelog and runs `mise run version:set` before creating the tag.
The script updates the workspace and ABI crate versions. Maturin reads the Python package version from Cargo.
The workflow pushes the tag and dispatches [release.yml](../.github/workflows/release.yml) on it.
The release rejects a tag whose version differs from the tree.

After CI and install checks pass, the release attaches the wheels and source distribution to a GitHub Release.
PyPI upload requires `PYPI_PUBLISH_ENABLED=true` in the repository's Actions variables.
The `pypi` environment provides the configured approval rules.
[Releasing the Python SDK](releasing.md) lists the publisher settings and activation steps.

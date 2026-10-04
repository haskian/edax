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
| The storage seam protocols (`EventStore`, `ProjectionStore`, `IdempotencyStore`, `SagaStore`, `Outbox`, `CommandOutbox`, `Transactor`) | Adding a required method is a breaking change. Adding an optional one, declared by a capability flag, is not. |
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
  upgrade runs its handler again. Event guards keep the raw event id.
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
  `CachedFailure` of code `codec`, in both SDKs, instead of raising `CodecError` on every claim. So
  does a cached result that does not convert to the handler's `result_type`. A store reads the record
  that holds a key with `edax.command.decode_prior`; a store of your own that calls `decode_record`
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

`cog bump` reads the conventional commits since the last tag and writes `CHANGELOG.md` and the tag.
Which increment it is handed is [AGENTS.md's rule](../AGENTS.md): `versionbump!` for a major, `feat`
for a minor, `fix` for a patch. A breaking `feat!` is still only a minor.
`scripts/set_version.py` rewrites the two version literals in the tree, so the tag and the artefacts
cannot disagree. The release refuses to build a tag whose tree declares a different version.
Pushing the tag builds the wheels and publishes them. See [release.yml](../.github/workflows/release.yml).

Publishing runs under a `pypi` GitHub environment with a required reviewer. The workflow cannot
publish until that environment exists, so a tag pushed before it is created builds wheels and stops.

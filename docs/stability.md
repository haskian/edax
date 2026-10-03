# What a version promises

edax follows [Semantic Versioning](https://semver.org). This page says which parts of the project
that promise covers. A toolkit with a Rust core, two software development kits (SDKs) and a set of
on-disk formats has more surfaces than a library usually does.

Until 0.1.0 nothing here applies: 0.0.x is early alpha and every surface may change without notice.

## Before 1.0

A minor release (`0.x` to `0.(x+1)`) may break any surface below. A patch release (`0.x.y` to
`0.x.(y+1)`) breaks none of them.

That is what `0.` means, and it is not a formality: the seams are young, and some of them will move
once real backends have been written against them. Pin a minor in your dependency declaration
(`edax ~= 0.1.0`, `edax = "0.1"`) and read the changelog before you lift it.

## What is covered

| Surface | Promise |
|---|---|
| Names exported from `edax` (Python) | The stable API. Breaks only in a minor. |
| Kernel decisions: local partition and Kafka placement, the canonical form (RFC 8785) and the payload and command hashes, the idempotency record, verdict and settle, filter order and keyset cursors, the upcast plan, the retry and dead-letter policy, the saga step | Pinned by golden vectors. A change here is a breaking change even if no signature moves. |
| The envelope wire format | Pinned by golden vectors. Within a minor, old bytes stay readable. A minor may refuse an old shape; the upgrade notes below name each one. |
| The Redis key, value layout and scripts | A cross-SDK contract: two services share a ledger only while it holds. The value carries a layout version, so a later layout is refused by name. |
| The storage seam protocols (`EventStore`, `ProjectionStore`, `IdempotencyStore`, `SagaStore`, `Outbox`, `Transactor`) | Adding a required method is a breaking change. Adding an optional one, declared by a capability flag, is not. |
| The conformance suites in `edax.testing` | A new clause may fail a backend that passed before, so clauses are added in a minor. |

## What is not covered

- Anything named with a leading underscore, and the generated `_edax` extension module.
- The exact text of an exception message. The type and its taxonomy code are stable; the prose is not.
- Log and observer output beyond the documented `Observer` methods.
- The examples, the documentation and the test suite.
- The Rust crates. Nothing publishes them. The release ships the `edax` Python wheel, which reaches them
  as path dependencies, and their surface follows the Python SDK's needs. That includes the Rust `edax`
  crate (`crates/edax`), which is a **reference model**, not a deployable SDK. It is synchronous and ships
  in-memory backends only. It exists so a second language holds the Python orchestration to the kernels,
  through twin test suites over the same case tables and golden vectors. It is `publish = false`. It gains
  a feature only when a shared decision needs a second implementation to pin it. See
  [Rust and Python parity](sdk/rust-parity.md).

## Upgrade notes

### 0.1 to 0.2

0.2 is a minor release, so it may break the surfaces above, and it does.

- **MessagePack envelopes.** 0.1 wrote a MessagePack envelope as a positional array. 0.2 writes and
  reads only the map keyed by field name, and refuses an array with the terminal `codec` error. A
  consumer dead-letters every 0.1 MessagePack record it meets. JSON envelopes are unchanged.
  Before you upgrade, drain the outbox: let every 0.1 relay publish its rows and every consumer
  commit past them. Or republish the retained records as JSON. 0.2 has no reader for the array
  form, by design.
- **Saga store seam.** `SagaStore.save` takes the instance's `folded` marks, and `load` returns them
  as `SagaInstance.folded`. A durable saga store needs a text column for them. An instance saved
  before the column existed has empty marks, so its first redelivery after the upgrade may fold once
  more.
- **Retry policy at the boundary.** The native `retries` takes the sink flag at construction, and
  `Retries.failed` takes the failure's error code instead of a failure kind.

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

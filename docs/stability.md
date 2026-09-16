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
| Names exported from `edax` (Python) and `edax::` (Rust) | The stable API. Breaks only in a minor. |
| Kernel decisions: partition, payload hash, idempotency verdict, filter order, upcast plan | Pinned by golden vectors. A change here is a breaking change even if no signature moves. |
| The envelope wire format | Pinned by golden vectors. Old bytes stay readable. |
| The Redis key layout | A cross-SDK contract: two services share a ledger only while it holds. |
| The storage seam protocols (`EventStore`, `ProjectionStore`, `IdempotencyStore`, `SagaStore`, `Transactor`) | Adding a required method is a breaking change. Adding an optional one, declared by a capability flag, is not. |
| The conformance suites in `edax.testing` | A new clause may fail a backend that passed before, so clauses are added in a minor. |

## What is not covered

- Anything named with a leading underscore, and the generated `_edax` extension module.
- The exact text of an exception message. The type and its taxonomy code are stable; the prose is not.
- Log and observer output beyond the documented `Observer` methods.
- The examples, the documentation and the test suite.
- The Rust crates below the SDK: `edax-core`, `edax-event`, `edax-cqrs`, `edax-abi`. They are
  published so the SDK can depend on them, and their surface follows the SDK's needs. Depend on
  `edax` itself.

## Supported runtimes

Python 3.9 and later. The wheel is `abi3-py39`, so one wheel serves every later interpreter.

Rust: the channel pinned in `rust-toolchain.toml`. A minimum supported Rust version is not yet
declared; raising the pin is a minor release.

## Deprecation

A name that is going away is marked deprecated in one minor and removed no earlier than the next
one. The deprecation names its replacement. Python marks it with `DeprecationWarning`, Rust with
`#[deprecated]`.

## How a release is cut

`cog bump` reads the conventional commits since the last tag and writes `CHANGELOG.md` and the tag.
Which increment it is handed is [AGENTS.md's rule](../AGENTS.md): `versionbump!` for a major, `feat`
for a minor, `fix` for a patch. A breaking `feat!` is still only a minor.
`scripts/set_version.py` rewrites the two version literals in the tree, so the tag and the artefacts
cannot disagree. The release refuses to build a tag whose tree declares a different version.
Pushing the tag builds the wheels and publishes them. See [release.yml](../.github/workflows/release.yml).

Publishing runs under a `pypi` GitHub environment with a required reviewer. The workflow cannot
publish until that environment exists, so a tag pushed before it is created builds wheels and stops.

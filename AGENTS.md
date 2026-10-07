# Agent rules

Operational gotchas only — design lives in `README.md`.

## Scope: Python and Go

Bindings support **Python** (`crates/edax-py`) and **Go** (`crates/edax-go`).
The kernels and reference Rust SDK (`crates/edax`) stay. Use type hints throughout Python.
Alef generates the C interface (`crates/edax-abi-ffi`) and Go native package
(`crates/edax-go/internal/native`) from `edax-abi`. Never hand-edit generated bindings or headers.
Keep Go wrappers idiomatic. All consistency decisions still belong in the kernels.

## Toolchain via `mise`

`cargo` / `maturin` / `uv` / `alef` / `go` aren't on `PATH` — prefix with `mise exec --` (run `mise install`
once on a fresh checkout; `mise trust` if it complains). `mise.toml` pins alef to the `alef_version` in
`alef.toml` and installs its prebuilt GitHub release binary. Never switch it back to `cargo:alef`:
two parallel `cargo:` builds race on the same rustup download and break `mise install`.

## Build & test

```
# Rust kernels, memory backend + SDK (golden vectors run here); CI runs the same
mise exec -- cargo test --workspace

# Python SDK (venv needs msgspec)
cd crates/edax-py && mise exec -- uv venv && source .venv/bin/activate
mise exec -- uv pip install msgspec pytest pytest-asyncio hypothesis "redis>=8" "fakeredis[lua]" "aiokafka>=0.8"
mise exec -- maturin develop && python -m pytest -q
```

Use `crates/edax-py/.venv/bin/python` (has `edax` + `msgspec`); the repo-root `.venv` lacks `msgspec`.

The Redis suites run on fakeredis, which runs the kernel's Lua. Set `EDAX_REDIS_URL=redis://localhost:56380/0`
(a throwaway `redis:8-alpine`) to run them live too, and `EDAX_REDIS_CONTAINER=<name>` for the pause tests.
The Kafka suites run on the Kafka-shaped fake in `tests/conftest.py`. Set `EDAX_KAFKA_BOOTSTRAP=localhost:59192`
(a throwaway `apache/kafka:3.8.0`; 59092 is often another project's) to run `test_kafka_live.py`, and
`EDAX_KAFKA_CONTAINER=<name>` for the pause and restart tests.

## Generated code

`crates/edax-py/src/lib.rs` and `crates/edax-py/python/edax/_edax.pyi` are **alef-generated** from
`edax-abi` — never hand-edit them. Change `crates/edax-abi/src/` and run
`mise run bindings:generate`: it runs `alef generate --lang python,ffi,go --clean` and refreshes the
shipped stub. Plain `alef generate` is destructive here — see `alef.toml`. CI regenerates and fails on
any diff in those two files, and runs `mypy.stubtest` against the built module
(`crates/edax-py/stubtest-allowlist.txt` lists alef's own conventions only). alef also rewrites
`crates/edax-abi-py/Cargo.toml` at a path it hardcodes; that manifest is not a workspace member and
is never built. The Python package under `python/edax/` **is** yours to edit; `src/lib.rs`, the stub
and the `.alef/` scaffold are not.

`edax-abi` has one module per kernel crate (`primitives.rs` for `edax-core`, then `event.rs`, `retry.rs`,
`saga.rs`, `cqrs.rs`, `memory.rs`, `kafka.rs`, `redis.rs`), each declared with `mod` and re-exported with
`pub use` from `lib.rs`, which keeps only the shared helpers. alef follows those declarations, but it
fingerprints only the files `sources` in `alef.toml` lists: list a new module there too.

Go's generated C interface is in `crates/edax-abi-ffi`; generated Go code is in
`crates/edax-go/internal/native`. Edit `.mise/cmd/go-facade/` to change the internal kernel facade,
then regenerate `crates/edax-go/internal/kernel/native.gen.go`. CI checks all these generated outputs, including the generated error and tenancy names.
The root `kernel.go` exposes only the records and helpers used by the public SDK.
The facade tool parses Go declarations with `go/parser`. The `edax-core` example `binding_names`
exports error and tenancy names to it; do not parse Rust source or copy those name lists.
Keep the ABI's explicit presence helpers for optional byte slices and upcast vectors: alef otherwise
conflates absent and empty input. The result envelopes for optional method records let alef free
their native handles; calling the original optional-return methods from Go leaks those handles.
The facade discovers these cases from alef's IR. An optional slice requires a `<Function>Present`
companion with `present bool` (or `<parameter>Present bool`) immediately before that slice.
An optional record method requires a `<Method>Result` companion returning an envelope whose only
field is `Value` with the original result type. Keep the other parameters and error result unchanged.
The generator validates these signatures and rejects missing companions. Do not add method-name
overrides to it. Multiple optional slices or combined presence/envelope cases need a convention
before the generator can support them.
Remove these workarounds only after upgrading alef and proving those cases still pass.
`poly.toml` is repository-owned because alef hardcodes the wrong directory for its Go hook.

## Kernels are the source of truth

Consistency logic (partition, codec, query/filter, sequencing, idempotency, dedup, causation, upcast
planning) lives in the kernels — call it via `_edax`, don't fork it. `testdata/golden/*.json` is the
contract: **golden wins** over a disagreeing test unless you're deliberately changing it. Regenerate:

```
EDAX_REGEN=1 mise exec -- cargo test -p edax-event --test golden
EDAX_REGEN=1 mise exec -- cargo test -p edax-cqrs  --test golden
EDAX_REGEN=1 mise exec -- cargo test -p edax-core  --test golden
EDAX_REGEN=1 mise exec -- cargo test -p edax-kafka --test golden
EDAX_REGEN=1 mise exec -- cargo test -p edax-redis --test golden
EDAX_REGEN=1 mise exec -- cargo test -p edax       --test golden
```

**One kernel crate per concern.** Each kernel's full scope, module by module, is the table at the top
of its `docs/kernels/<crate>.md`; every other description lists the same module names.
`edax-core` is primitives only: `hash` (with `payload_hash`, the one hash both idempotency guards bind),
`key` (tenancy, the tenant key, the guard key), `backoff`,
`clock`, `guid`, `version` (`next_version`), `names` (`closed_names!`, the one shape of a closed set
of names) and `error` (the taxonomy). It also holds two Rust-only
runtime helpers, `lock` and `TokenMint`, which no binding crosses and no golden vector pins.
`edax-event` is event-driven: `message` (the messages, their `Payload` and the ordering key), `codec`,
`causation`, `partition` (local fan-out), `staleness`, `idempotency` (the event guard's key and hash),
`sequence` (and a save's `draft_batch`), `upcast`,
and the orchestration decisions the SDKs make: `retry` (`Retries`, which every dispatcher performs, and
`retry_step`), `drain` (the relay's `Drain`) and `saga` (`saga_admit`, `saga_write`, `saga_command_id`).
`edax-cqrs` is both halves of its name: the command side's `canonical_json`, `command_hash`,
`received_key`, `claim_key` and the idempotency `record` (the claimant's `outcome` and `replay`, the
winner's `settle_failure`), the ledger fence every store applies (`complete_fence`, `release_fence`; Redis's
Lua replays it from `idempotency_fence.json`), the host values `Decimal` and `Instant`, *and* the read side's `filter` and
`query`. The `record` serves the event guard too: `edax-event` names the key and hash, `edax-cqrs` keeps
the state machine. Filter and query live together because both read documents through one value model
and one collation (the crate-private `value` and `collation` modules) — never split them across crates.

**One kernel crate per technology.** A Kafka or Redis decision all SDKs must match belongs in
`edax-kafka` or `edax-redis` with a golden vector, never in the Python adapter. `edax-kafka` owns
`record` (the record key, header, media-type route and dead-letter record), `partition`
(`partition_for`), `offsets` (the offset watermark and the revoke) and `client` (`check_config`,
`check_consumer`, `check_dead_sink`, `acks_warning` and `broker_error`).
`edax-redis` owns `ledger` (the key, settings and their defaults, value layout, claim handle and reply outcomes) and
`scripts` (the fence scripts and the reply verdict). The adapter is the input and output (I/O) shell: it
calls the client and the kernel, and decides nothing on its own.

`#[non_exhaustive]` goes on a kernel enum a host only *reads* — one whose new variant a host may
refuse at runtime (`Format`, `Operator`). It stays **off** an enum whose every variant a host must
decide about (`ErrorCode`), so Rust consumers must handle each variant explicitly.
Go cannot enforce exhaustive switches. Generate its error and tenancy constants from the compiled Rust
kernel name lists, check them against the native names, and reject unknown orchestration decisions.

**One error type.** Every kernel function fails with `edax_core::Error { code, message }` — never a
bare `String`, never a crate-local error enum — so the code (and with it the terminal / transient
verdict) is the kernel's, not the caller's. Its `Display` is the wire form `"<code>: <message>"`. The
Rust SDK re-exports it as `edax::Error`; the default verdict for a failure with no code is
`edax_core::is_terminal(None)`, pinned by `error_wire.json`.

## Binding responsibilities

Binding crates = `edax-abi` + generated `_edax` and `edax-abi-ffi`. SDKs = `python/edax/` and `crates/edax-go`, which own orchestration (`CommandBus` +
`retry`/`idempotent` middleware, `EventRouter` + `idempotent_by_id`, dispatcher, `Saga`, store/transactor seams).

Binding crate does **ONLY**: type conversion (Rust ↔ Python), error → exception, Python-idiomatic
config/builders. Every `edax-abi` method is synchronous. Python links no tokio or `pyo3-async-runtimes`.
Alef's generated C crate declares tokio as scaffold infrastructure; no edax ABI call starts async work. Never put `async fn` on the ABI: the generated wrapper
spawns it on a runtime thread, where it starts at once and a cancelled `await` cannot stop it. The
SDK's async store views await nothing of the native call.

Neither layer may **re-derive kernel logic** — no duplicated validation (filter null/missing, upcast
gaps, concurrency checks), no re-implemented idempotency/dedup/`payload_hash` decisions, retry/dead-letter counts or saga
writes (the SDK *wires* middleware, dispatchers, relays and runners; the kernel *decides*), no routing/partition placement (`partition` and
`partition_for` are the kernel's), no business rules (host app's). If it must match across versions/languages, it's a
kernel + golden vector.

## Binding implementation (PyO3)

Shape these at the `edax-abi` surface, never by hand-editing the generated `src/lib.rs`.

- **No panics** — no `.unwrap()`/`.expect()`/`panic!`/`unreachable!`. Return `Result<T, String>` (alef maps it to a raising `PyResult<T>`); the `String` is always a kernel `Error` rendered with `.map_err(wire)`, never a code chosen at the call site. `[workspace.lints.clippy]` denies `unwrap`/`expect`/indexing in every crate the binding links (each opts in with `[lints] workspace = true`); `clippy.toml` lifts it for tests only. For a `#[non_exhaustive]` match, return `Err` on the catch-all, don't `unreachable!`.
- **Cross-language decisions stay in the kernel** — hash *canonical* bytes via `payload_hash_json` (the kernel canonicalizes to RFC 8785 and refuses an integer literal outside ±(2^53−1)); never canonicalize host-side: Python's `json.dumps(ensure_ascii=True)` diverges from serde on non-ASCII, breaking idempotency keys across SDKs. `crates/edax-cqrs/feature-check` (its own workspace) replays the canonical vectors with the `serde_json` features that would move them: `mise exec -- cargo test --manifest-path crates/edax-cqrs/feature-check/Cargo.toml`.
- **The boundary is what alef supports, not a free choice.** alef crosses data as either native `#[pyclass]` structs (envelopes — structured, no JSON) or JSON text (`String`/`Vec<u8>`). It has **no** object-passing for arbitrary values — a `serde_json::Value` param is silently coerced to a `String` (parsed with `unwrap_or_default`), so `filter`/`query` stay JSON-text. The generated free functions *do* release the GIL — alef wraps each in `py.detach` (pyo3 0.28's rename of `allow_threads`) — but that is alef's choice, not a config knob. Methods on the native handles do not, and every handle is `unsendable`: the SDK binds each to the thread that built it (`edax._owned.ThreadBound`), which raises `BadHandle` instead of PyO3's panic. Killing the double-parse needs alef support (or modeling the type as a pyclass struct) — it is **not** a local fix; don't burn time faking it.

## Conventional commits

Every PR **title** must be a Conventional Commit — `feat(edax-py): ...`, `fix: ...`. The allowed
types are cog's, plus whatever `cog.toml` registers; `mise exec -- cog verify "<title>"` is the
authority, and `.github/workflows/pr-title.yml` runs it on every PR. Merges are squash-only, so the
PR title *is* the commit that lands on main, and `version.yml` reads those titles to pick the release.

| Title since the last tag | Release |
| --- | --- |
| `versionbump!:` | **major** — the only thing that cuts one |
| `feat:` | minor |
| `fix:` | patch |
| any other type | none |

**A major is deliberate and manual.** `!` and a `BREAKING CHANGE:` footer mark a break in the
changelog but never bump the major — a breaking `feat!:` still releases a minor. To go to the next
major, merge a PR titled `versionbump!: <why>` (scope allowed: `versionbump(kafka)!:`). The type is
spelled without a hyphen because cog's grammar rejects one in a commit type: `version-bump!:` fails
to parse, so `versionbump` is registered in `cog.toml` instead.

There is **no version literal in `crates/edax-py/pyproject.toml`** — it declares `dynamic =
["version"]` and maturin reads the Cargo version. Nothing reads the git tag at build time; the tag
and the wheel agree only because `cog` runs `mise run version:set` *before* it tags. So never hand-
edit a version: run the bump. A hand-pushed tag builds a wheel carrying the old version, which the
release's `version` job refuses before it builds anything.

## Before you finish

Run the kernel, Python and Go tests. `cargo test` alone misses binding regressions.
Build Go with `mise run go:build`, then run `mise exec -- go test -race ./...` from `crates/edax-go`.
The static native library must be rebuilt after an ABI or kernel change.

`pytest` does **not** rebuild the native module. After any change under `edax-core`, `edax-event`,
`edax-cqrs`, `edax-memory`, `edax-kafka`, `edax-redis` or `edax-abi`, run `maturin develop` first or
the Python suite passes green against the previously compiled kernel and proves nothing. CI does
this in the right order (`.github/workflows/python.yml`).

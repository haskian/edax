# Agent rules

Operational gotchas only — design lives in `README.md`.

## Scope: Python-only

Supported binding is **Python** (`crates/edax-py`). Kernels + reference Rust SDK (`crates/edax`) stay; Typehints are a must, use them everywhere.
**no Go / C ABI / FFI** — don't reintroduce `edax-ffi`, `bindings/go`, `cbindgen`, `cgo`, or `*.h`.

## Toolchain via `mise`

`cargo` / `maturin` / `uv` / `alef` aren't on `PATH` — prefix with `mise exec --` (run `mise install`
once on a fresh checkout; `mise trust` if it complains). `mise.toml` pins alef to the `alef_version` in
`alef.toml`; the first `mise install` compiles it.

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
`edax-abi` — never hand-edit them. Change `crates/edax-abi/src/lib.rs` and run
`scripts/regenerate_binding.sh`: it runs `alef generate --lang python --clean` and refreshes the
shipped stub. Plain `alef generate` is destructive here — see `alef.toml`. CI regenerates and fails on
any diff in those two files, and runs `mypy.stubtest` against the built module
(`crates/edax-py/stubtest-allowlist.txt` lists alef's own conventions only). alef also rewrites
`crates/edax-abi-py/Cargo.toml` at a path it hardcodes; that manifest is not a workspace member and
is never built. The Python package under `python/edax/` **is** yours to edit; `src/lib.rs`, the stub
and the `.alef/` scaffold are not.

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

**One kernel crate per concern.** `edax-core` is primitives only — hash, backoff, clock, guid, the
error taxonomy. `edax-event` is event-driven: local fan-out `partition`, codec, causation, staleness,
dedup, sequence, upcast, and the orchestration decisions both SDKs make on the consume side: the
retry and dead-letter policy (`Retries`, which every dispatcher and relay performs) and the saga
step (`saga_admit`, `saga_write`, `saga_command_id`).
`edax-cqrs` is both halves of its name: the command side's idempotency decision (the claimant's
`outcome` and the winner's `settle`) *and* the read
side's `filter` and `query`. Filter and query live together because the query evaluates the filter
(`filter::resolve` and `value_cmp` are public for it) — never split them across crates.

**One kernel crate per technology.** A Kafka or Redis decision two SDKs must match (record key,
header, media type, broker placement `partition_for`, offset watermark, ledger value layout, fence script) belongs in `edax-kafka` or
`edax-redis` with a golden vector, never in the Python adapter. The adapter is the input and output
(I/O) shell: it calls the client and the kernel, and decides nothing on its own.

`#[non_exhaustive]` goes on a kernel enum a host only *reads* — one whose new variant a host may
refuse at runtime (`Format`, `Operator`). It stays **off** an enum whose every variant a host must
decide about, so a new one is a compile error in every SDK instead of a wildcard's silent default
(`ErrorCode`).

**One error type.** Every kernel function fails with `edax_core::Error { code, message }` — never a
bare `String`, never a crate-local error enum — so the code (and with it the terminal / transient
verdict) is the kernel's, not the caller's. Its `Display` is the wire form `"<code>: <message>"`. The
Rust SDK re-exports it as `edax::Error`; the default verdict for a failure with no code is
`edax_core::verdict(None)`, pinned by `error_wire.json`.

## Binding responsibilities

Binding crate = `edax-abi` + generated `_edax`. SDK = `python/edax/`, which owns orchestration (`CommandBus` +
`retry`/`idempotent` middleware, `EventRouter` + `idempotent_by_id`, dispatcher, `Saga`, store/transactor seams).

Binding crate does **ONLY**: type conversion (Rust ↔ Python), error → exception, Python-idiomatic
config/builders. It has no async-runtime integration: every `edax-abi` method is synchronous, so alef
links no tokio and no `pyo3-async-runtimes`. Never put `async fn` on the ABI: the generated wrapper
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
- **Cross-language decisions stay in the kernel** — hash *canonical* bytes via `payload_hash_json` (the kernel canonicalizes to RFC 8785 and refuses an integer literal past 2^53); never canonicalize host-side: Python's `json.dumps(ensure_ascii=True)` diverges from serde on non-ASCII, breaking idempotency keys across SDKs. `crates/edax-cqrs/feature-check` (its own workspace) replays the canonical vectors with the `serde_json` features that would move them: `mise exec -- cargo test --manifest-path crates/edax-cqrs/feature-check/Cargo.toml`.
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
and the wheel agree only because `cog` runs `scripts/set_version.py` *before* it tags. So never hand-
edit a version: run the bump. A hand-pushed tag builds a wheel carrying the old version, which the
release's `version` job refuses before it builds anything.

## Before you finish

Run **both** the kernel tests and the Python tests — `cargo test` alone misses binding regressions.

`pytest` does **not** rebuild the native module. After any change under `edax-core`, `edax-event`,
`edax-cqrs`, `edax-memory`, `edax-kafka`, `edax-redis` or `edax-abi`, run `maturin develop` first or
the Python suite passes green against the previously compiled kernel and proves nothing. CI does
this in the right order (`.github/workflows/python.yml`).

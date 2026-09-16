# Agent rules

Operational gotchas only — design lives in `README.md`.

## Scope: Python-only

Supported binding is **Python** (`crates/edax-py`). Kernels + reference Rust SDK (`crates/edax`) stay; Typehints are a must, use them everywhere.
**no Go / C ABI / FFI** — don't reintroduce `edax-ffi`, `bindings/go`, `cbindgen`, `cgo`, or `*.h`.

## Toolchain via `mise`

`cargo` / `maturin` / `uv` aren't on `PATH` — prefix with `mise exec --` (run `mise install` once on a
fresh checkout; `mise trust` if it complains).

## Build & test

```
# Rust kernels + SDK (golden vectors run here)
mise exec -- cargo test -p edax-core -p edax-event -p edax-cqrs -p edax-kafka -p edax-redis -p edax

# Python SDK (venv needs msgspec)
cd crates/edax-py && mise exec -- uv venv && source .venv/bin/activate
mise exec -- uv pip install msgspec pytest pytest-asyncio
mise exec -- maturin develop && python -m pytest -q
```

Use `crates/edax-py/.venv/bin/python` (has `edax` + `msgspec`); the repo-root `.venv` lacks `msgspec`.

## Generated code

`crates/edax-py/src/lib.rs` is **alef-generated** from `edax-abi` — never hand-edit it. Change
`crates/edax-abi/src/lib.rs` and regenerate with `--clean` only (`alef generate --lang python
--clean`); plain `alef generate` is destructive here — see `alef.toml`. The Python package under
`python/edax/` **is** yours to edit; `src/lib.rs` and the `.alef/` scaffold are not.

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
```

**One kernel crate per technology.** A Kafka or Redis decision two SDKs must match (record key,
headers, media type, offset watermark, ledger value layout, fence script) belongs in `edax-kafka` or
`edax-redis` with a golden vector, never in the Python adapter. The adapter is the input and output
(I/O) shell: it calls the client and the kernel, and decides nothing on its own.

`#[non_exhaustive]` goes on a kernel enum a host only *reads* — one whose new variant a host may
refuse at runtime (`Format`, `Operator`, `Decision`, `CodecError`). It stays **off** an enum whose
every variant a host must decide about, so a new one is a compile error in every SDK instead of a
wildcard's silent default (`ErrorCode`, and `AdapterError` which mirrors it).

## Binding responsibilities

Binding crate = `edax-abi` + generated `_edax`. SDK = `python/edax/`, which owns orchestration (`CommandBus` +
`retry`/`idempotent` middleware, `EventRouter` + `idempotent_by_id`, dispatcher, `Saga`, store/transactor seams).

Binding crate does **ONLY**: type conversion (Rust ↔ Python), error → exception, Python-idiomatic
config/builders, async-runtime integration (`pyo3-async-runtimes`).

Neither layer may **re-derive kernel logic** — no duplicated validation (filter null/missing, upcast
gaps, concurrency checks), no re-implemented idempotency/dedup/`payload_hash` decisions (the SDK
*wires* middleware; the kernel *decides*), no routing/partition placement (`partition`/`rendezvous` is
the kernel's), no business rules (host app's). If it must match across versions/languages, it's a
kernel + golden vector.

## Binding implementation (PyO3)

Shape these at the `edax-abi` surface, never by hand-editing the generated `src/lib.rs`.

- **No panics** — no `.unwrap()`/`.expect()`/`panic!`/`unreachable!`. Return `Result<T, String>` (alef maps it to a raising `PyResult<T>`). For a `#[non_exhaustive]` match, return `Err` on the catch-all, don't `unreachable!`.
- **Cross-language decisions stay in the kernel** — hash *canonical* bytes via `payload_hash_json` (the kernel canonicalizes); never canonicalize host-side: Python's `json.dumps(ensure_ascii=True)` diverges from serde on non-ASCII, breaking idempotency keys across SDKs.
- **The boundary is what alef supports, not a free choice.** alef crosses data as either native `#[pyclass]` structs (envelopes — structured, no JSON) or JSON text (`String`/`Vec<u8>`). It has **no** object-passing for arbitrary values — a `serde_json::Value` param is silently coerced to a `String` (parsed with `unwrap_or_default`), so `filter`/`query` stay JSON-text. The generated wrappers *do* release the GIL — alef wraps each call in `py.detach` (pyo3 0.28's rename of `allow_threads`), 24 of them in `crates/edax-py/src/lib.rs` — but that is alef's choice, not a config knob. Killing the double-parse needs alef support (or modeling the type as a pyclass struct) — it is **not** a local fix; don't burn time faking it.

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

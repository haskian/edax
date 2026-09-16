# edax-core

The primitives both other kernels build on: the query filter, Fowler-Noll-Vo 1a (FNV-1a) hashing,
the retry backoff schedule, the clock, id generation, and the error taxonomy. The filter has a page
of its own, [filter.md](./filter.md).

Everything here is a pure function of its arguments except `guid` and `current_timestamp`, which are
the two deliberate exceptions. The crate's whole dependency set is `serde`, `serde_json` and `uuid`
(`crates/edax-core/Cargo.toml`).

| Module | Owns the decision | Deterministic | Golden vector | Reached from |
|---|---|---|---|---|
| `hash` | FNV-1a 32 and 64 over bytes | yes | indirectly, via `partition.json` / `payload_hash.json` | not exported; only its callers are |
| `backoff` | How long before retry `n`? | yes | `testdata/golden/backoff.json` | `backoff_millis` on the ABI |
| `clock` | Unix nanos, strictly increasing per process | **no** | none — the guarantee is monotonicity | `current_timestamp` on the ABI |
| `guid` | A fresh universally unique identifier (UUID) v7 | **no** | none — the guarantees are structural | `guid` on the ABI |
| `error` | Is this failure terminal or transient? | yes | `testdata/golden/error_taxonomy.json` | `error_codes` / `error_code_is_terminal` |

## `filter`

A sealed abstract syntax tree (AST) plus one reference evaluator, in [filter.md](./filter.md).

## `backoff`

`min(BASE_MILLIS * 2^attempt, CAP_MILLIS)` milliseconds, with `attempt` 0-based. `BASE_MILLIS` is
100 and `CAP_MILLIS` is 30_000.

| attempt | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10+ |
|---|---|---|---|---|---|---|---|---|---|---|---|
| millis | 100 | 200 | 400 | 800 | 1600 | 3200 | 6400 | 12800 | 25600 | 30000 | 30000 |

**There is no jitter, and that is deliberate.** Randomness would differ per language and could not
be pinned by a golden vector. If many workers retry at the same instant and overload a shared
broker, add jitter host-side; it does not belong in the frozen kernel. The arithmetic saturates
rather than overflowing — the shift falls back to `u64::MAX`, the multiply saturates, the cap clamps
— so `backoff_millis(1000)` is 30_000, not a panic. `backoff` returns the same schedule as a
`Duration`.

Four callers, two per SDK:

| Caller | What it does with the delay |
|---|---|
| Rust `retry` middleware | blocks the thread between attempts; the index is the number of failures so far, so the first retry sleeps `backoff_millis(0)` |
| Python `retry` middleware | `await asyncio.sleep(_edax.backoff_millis(i) / 1000)` between attempts |
| Rust `KeyOrderedDispatcher` | on `InProgress`, waits one step then redelivers |
| Python `KeyOrderedDispatcher` | the same rule, with a per-envelope step counter in `_contended` |

Both dispatchers wait one step before nacking. See
[consuming](../sdk/consuming.md#inprogress-is-contention-not-a-failed-attempt).

`testdata/golden/backoff.json` holds 17 cases: attempts 0 through 11, plus 20, 63, 64, 100 and 1000
to pin the cap and the absence of overflow. The `test_edax.py::test_golden_backoff` asserts them.
`backoff_cases` in the `edax-core` golden test derives the vector.

## `hash`

FNV-1a, fully specified and dependency-free, so every binding computes the same hash. Two widths,
two jobs:

| Function | Offset basis | Prime | Used by |
|---|---|---|---|
| `fnv1a32` | `0x811c9dc5` | `0x01000193` | `partition_bytes` — `fnv1a32(aggregate_id) % n` |
| `fnv1a64` | `0xcbf29ce484222325` | `0x00000100000001b3` | `rendezvous_bytes` scoring and `payload_hash` |

Neither function is on the ABI surface. Only its users are: `partition`, `rendezvous`,
`payload_hash` and `payload_hash_json`. So a host cannot hash the wrong bytes by reaching past the
decision that chose them.

Known vectors, asserted in the kernel:

| Input | `fnv1a32` | `fnv1a64` |
|---|---|---|
| `""` | `0x811c9dc5` | `0xcbf29ce484222325` |
| `"a"` | `0xe40c292c` | `0xaf63dc4c8601ec8c` |
| `"foobar"` | `0xbf9cf968` | `0x85944171f73967e8` |

FNV-1a is a *consistency* hash, not a security one: use it to make two services bucket and bind the
same bytes identically, never where collision resistance matters.

## `clock` and `guid`

The two functions in the toolkit that are not deterministic — the crate docstring names them as the
exceptions.

```rust
/// A fresh time-ordered globally-unique id (UUID v7), lowercase hyphenated.
pub fn guid() -> String {
    Uuid::now_v7().to_string()
}
```
<sup>`edax_core::guid`</sup>

`guid` returns a UUID version 7: a 48-bit Unix-millis prefix followed by random bits, so ids are
unique, roughly time-ordered and index-friendly. It carries no golden vector, only structural
guarantees a test asserts: the `8-4-4-4-12` hyphenated form, version 7, the RFC4122 variant, and
that two calls differ.

`current_timestamp` returns Unix time in nanoseconds. Each call yields a value strictly greater than
every prior call in this process. That holds even if the wall clock stalls inside a nanosecond. It
also holds if the clock steps backwards under Network Time Protocol (NTP) slew, a leap second, or
suspend. It does that with a process-local `AtomicU64` high-water mark and a compare-exchange loop,
taking `wall.max(last + 1)`. A pre-1970 clock has no Unix duration, so it falls back to `0` and
defers to the mark, which keeps the one guarantee the function actually makes.

**Why they still live in the kernel.** The value is not the contract; the *shape* is. Every service
must generate an id that sorts the same way and a timestamp that never repeats, or event ordering
and the outbox break at the language boundary. One implementation of "what a fresh event id looks
like" beats one per SDK. Both SDKs take them as overridable defaults, so nothing is locked in:

```python
self._new_id = new_id or _edax.guid  # time-ordered id; override for a deterministic clock
self._now = now or _edax.current_timestamp
```
<sup>`Repository.__init__`</sup>

Rust does the same through `Repository::with_clock`, which the docstring introduces as "useful for
deterministic tests".

Two limits. The high-water mark is per process — it spans neither a restart nor a second machine, so
it is not a cross-service ordering primitive. And `u64` nanoseconds overflow around the year 2554;
the Rust SDK narrows to `i64`, which fits until 2262.

## `error`

`ErrorCode` is one enum with one row per failure kind, and each row carries a **verdict**: terminal
or transient. That verdict is the cross-language decision, because terminality decides what an
idempotency guard persists. A terminal failure is cached under the key and replayed to every later
claimant. A transient one releases the claim, so the next attempt re-runs.

| Code | Verdict | Means |
|---|---|---|
| `concurrency` | transient | an expected version did not match the stored one |
| `codec` | **terminal** | a value could not be marshaled or unmarshaled |
| `store` | transient | the backing store failed for a reason other than a version conflict |
| `payload_mismatch` | **terminal** | an idempotency key was reused with a different payload |
| `in_progress` | transient | the key is claimed by another run that has not completed |
| `cached_failure` | **terminal** | replay of a terminal failure cached under the key |
| `handler_not_found` | **terminal** | no handler is registered for the command type |
| `already_registered` | **terminal** | a handler is already registered — a wiring mistake at startup |
| `bad_handle` | **terminal** | a transaction handle names no open transaction — never begun, already settled, or another store's |

<sup>`edax_core::ErrorCode`; the same nine rows, with wire forms, in `testdata/golden/error_taxonomy.json`</sup>

A macro generates the enum over that row list, so the variant list, `ALL`, `as_str` and
`is_terminal` cannot drift. A new code is one new row. A row cannot be added without a verdict.
`ErrorCode` is deliberately **not** `#[non_exhaustive]`, so a new code is a compile error in every
SDK rather than a wildcard's silent default.

Failures cross a native binding as `"<code>: <message>"`, splitting on the first `": "`.
`ErrorCode::tag` is the one place that builds that string and `ErrorCode::split` the one place that
takes it apart. `parse` returns `None` for a name the taxonomy does not know — the caller keeps its
own default; the kernel does not guess.

Nine codes exist. Only three cross the binding today. The store adapters emit `concurrency`, `codec`
and `store`; an SDK raises the other six. One enum covers both, so a kind that starts crossing later
needs no new parser, and terminality is pinned for all of them.

What *branches* on the verdict — the exception hierarchy, `is_terminal`, the retry middleware, the
idempotency guards, the dead-letter path — belongs to the SDK and is documented in
[../sdk/errors.md](../sdk/errors.md). The short version: nothing above the kernel states its own
verdict. Python reads it at class-definition time; Rust maps a variant to a code and reads the
verdict back off it.

## See also

- [README.md](./README.md) — what the three kernels own and why the boundary sits there.
- [filter.md](./filter.md) — the filter AST, its null rule and the two build surfaces.
- [event.md](./event.md) — `edax-event`, which reuses `hash` for routing and owns the read
  side.
- [cqrs.md](./cqrs.md) — `edax-cqrs`, which reuses `fnv1a64` as `payload_hash`.
- [golden-vectors.md](./golden-vectors.md) — how `filter.json`, `backoff.json` and
  `error_taxonomy.json` are generated and asserted.
- [../sdk/errors.md](../sdk/errors.md) — the exception hierarchy and every code path that branches
  on terminality.
- [../sdk/commands.md](../sdk/commands.md) — the `retry` middleware that consumes the backoff
  schedule.
- [../sdk/projections.md](../sdk/projections.md) — `ProjectionStore` and how a backend translates a
  filter.

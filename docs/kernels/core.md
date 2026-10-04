# edax-core

The primitives the other kernels build on: Fowler-Noll-Vo 1a (FNV-1a) hashing, the retry backoff
schedule, the clock, id generation and the error taxonomy. Two runtime primitives the adapters share
sit here too: a poison-tolerant lock and a settlement-token mint. The query filter is not here: it lives in
`edax-cqrs`, beside the query that evaluates it ([filter.md](./filter.md)).

Everything here is a pure function of its arguments except `guid` and `current_timestamp`. The
crate's whole dependency set is `uuid`
(`crates/edax-core/Cargo.toml`). Its modules are private and re-exported flat: one path per item,
`edax_core::guid`, never `edax_core::guid::guid`.

| Module | Owns the decision | Deterministic | Golden vector | Reached from |
|---|---|---|---|---|
| `hash` | FNV-1a 32 and 64 over bytes | yes | indirectly, via `partition.json` / `payload_hash.json` | not on the ABI; only its callers are |
| `key` | `scoped(scope, key)`: one key under a scope, `"{len(scope)}:{scope}:{key}"`, injective because the length (in bytes) comes first. `Tenancy` and `tenant_key(mode, tenant, key)`: which modes require a tenant, and which compose it into a key. `guard_key(mode, tenant, kind, scope, key)`: the ledger key an idempotency guard claims, `"edax:{kind}:"` and the scoped key, with the tenant outermost | yes | `testdata/golden/tenancy.json`, and indirectly `claim_key.json` / `event_idempotency.json` / `saga.json` | `tenant_key`, `tenancy_rules`, `check_tenant` on the ABI |
| `backoff` | How long before retry `n`, given a random draw? | yes | `testdata/golden/backoff.json` | `backoff_millis` / `default_backoff` on the ABI |
| `clock` | Unix nanos, strictly increasing per process | **no** | none — the guarantee is monotonicity | `current_timestamp` on the ABI |
| `guid` | A fresh universally unique identifier (UUID) v7 | **no** | none — the guarantees are structural | `guid` on the ABI |
| `error` | Is this failure terminal or transient? What is the default for a failure with no code? | yes | `testdata/golden/error_taxonomy.json`, `testdata/golden/error_wire.json` | `error_codes`, `error_code_is_terminal`, `error_verdict`, `split_error` |
| `sync` | `lock`: a poisoned mutex hands back its state instead of panicking again | yes | none | the native handles in `edax-abi`, `edax-memory`, `edax-kafka` |
| `token` | `TokenMint` and `NO_TOKEN`: one live token per delivery or transaction, `0` never issued | yes | none — the guarantee is uniqueness | the Kafka `OffsetTracker`, the memory idempotency claims and transactions |

## `backoff`

Capped exponential backoff with **full jitter**. A `Backoff { base_millis, cap_millis }` policy gives
each retry `attempt` (0-based) a ceiling, `min(base_millis * 2^attempt, cap_millis)`, and the wait is a
random point in `0 ..= ceiling`. `Backoff::DEFAULT` is a base of 100 and a cap of 30_000.

| attempt | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10+ |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ceiling (millis) | 100 | 200 | 400 | 800 | 1600 | 3200 | 6400 | 12800 | 25600 | 30000 | 30000 |

The randomness is an argument, not a side effect. The caller passes a random `u64`, and
`backoff_millis(policy, attempt, entropy)` reduces it into the range as `entropy % (ceiling + 1)`.
The kernel stays pure. A golden vector pins `(policy, attempt, entropy) -> millis`, and every SDK
computes the same wait for the same draw. Each SDK draws its own entropy per wait; a test passes a
fixed one. Without jitter, every consumer that hit one downed dependency would retry in lockstep, at
the same delays, for as long as the outage lasts. The arithmetic saturates rather than overflowing: the shift falls back to
`u64::MAX`, the multiply saturates, the cap clamps. So every `u64` attempt is defined, and a port that
doubles in a loop cannot spin at the top of the range.

Every caller waits through its SDK's **pacing**: the policy, the sleep and the source of the draw, in
one value. A host tunes the schedule there, and a test waits in no wall time.

| Caller | Pacing |
|---|---|
| Rust `retry(attempts, pacing)` and `retry_transaction(.., &pacing, ..)` | `edax::Pacing { backoff, sleep, entropy }`; the index is the number of failures so far, so the first retry waits step 0 |
| Python `retry(attempts, pacing=...)` | `edax.Pacing(backoff=..., sleep=..., entropy=...)`, default `asyncio.sleep` and `random.getrandbits(64)` |
| Rust `KeyOrderedDispatcher` | `DispatcherBuilder::pacing`; each nack waits the step the kernel's `Retries` answers |
| Python `KeyOrderedDispatcher` | the `pacing=` argument, with the same `Retries` answer |

Both dispatchers wait one step before nacking, and `Retries` (in `edax-event`) counts `InProgress` as
contention, apart from failed attempts. See
[consuming](../sdk/consuming.md#inprogress-is-contention-not-a-failed-attempt).

`testdata/golden/backoff.json` holds the default policy over attempts 0 through 11 plus 20, 63, 64,
100, 1000, `u32::MAX` and `u64::MAX`. Each attempt has three draws: the ceiling, one past it, and a
large constant. Five more rows pin the edges of a policy: a zero base, a cap below the base, and an
unbounded one. `test_edax.py::test_golden_backoff` asserts them through the raw call and through
`edax.Backoff`. A proptest in the kernel and a Hypothesis test in Python hold the bound: a wait never
passes its ceiling, and a ceiling never passes the cap.

## `hash`

FNV-1a, fully specified and dependency-free, so every binding computes the same hash. Two widths,
two jobs:

| Function | Offset basis | Prime | Used by |
|---|---|---|---|
| `fnv1a32` | `0x811c9dc5` | `0x01000193` | `partition` — `fnv1a32(aggregate_id) % n` |
| `fnv1a64` | `0xcbf29ce484222325` | `0x00000100000001b3` | `payload_hash` |

Neither function is on the ABI surface. Only its users are: `partition`, `payload_hash` and
`payload_hash_json`. So a host cannot hash the wrong bytes by reaching past the decision that chose
them.

Known vectors, asserted in the kernel:

| Input | `fnv1a32` | `fnv1a64` |
|---|---|---|
| `""` | `0x811c9dc5` | `0xcbf29ce484222325` |
| `"a"` | `0xe40c292c` | `0xaf63dc4c8601ec8c` |
| `"foobar"` | `0xbf9cf968` | `0x85944171f73967e8` |

FNV-1a is a *consistency* hash, not a security one: use it to make two services bucket and bind the
same bytes identically, never where collision resistance matters.

## `clock` and `guid`

The two functions in this crate that are not deterministic. The crate docstring names them as the
exceptions.

```rust
/// A fresh time-ordered globally-unique id (UUID v7), lowercase hyphenated.
pub fn guid() -> String {
    Uuid::now_v7().to_string()
}
```
<sup>`edax_core::guid`</sup>

`guid` returns a UUID version 7: a 48-bit Unix-millis timestamp, a 42-bit counter and 32 random bits.
The counter is shared within a process, so ids are strictly increasing in mint order there. That
makes them a safe index key. Across processes they are ordered only by their millisecond. It carries
no golden vector, only structural guarantees a test asserts: the `8-4-4-4-12` hyphenated form,
version 7, the RFC4122 variant, and in-process ordering over 50_000 mints.

`current_timestamp` returns Unix time in nanoseconds as an `i64`, the type of every time a message
carries, so no caller narrows. Each call yields a value strictly greater than every prior call in this process,
across threads too. That holds even if the wall clock stalls inside a nanosecond. It also holds if
the clock steps backwards under Network Time Protocol (NTP) slew, a leap second, or suspend. It does
that with a process-local `AtomicI64` high-water mark and a compare-exchange loop, taking
`wall.max(last + 1)`. A pre-1970 clock has no Unix duration, so it falls back to `0` and defers to the
mark, which keeps the one guarantee the function makes.

The price of strict monotonicity shows when the wall clock jumps forward and is then corrected (a VM
resume, a bad NTP source). Every later call then returns the mark plus one nanosecond until the wall
clock catches up. Timestamps stay ordered but sit at the bad future value for that long, and a lag
computed from them (the relay's `oldest_timestamp`) reads short. Nothing in-process can tell such a jump from real
time.

**Why they still live in the kernel.** The contract is the shape of the value. Every service
must generate an id that sorts the same way and a timestamp that never repeats, or event ordering
and the outbox break at the language boundary. One implementation of "what a fresh event id looks
like" beats one per SDK. Both SDKs take them as overridable defaults, so nothing is locked in:

```python
self._new_id = new_id or _edax.guid  # Repository: a time-ordered event id
self._now = now or _edax.current_timestamp  # MemoryEventStore: the clock it records with
```

Rust does the same through `Repository::with_clock`, for deterministic tests. There the repository
reads the clock and hands the reading to `EventStore::append` as `recorded_at`.

Two limits. The high-water mark is per process — it spans neither a restart nor a second machine, so
it is not a cross-service ordering primitive. And `i64` nanoseconds run out in the year 2262, where
the value saturates.

## `sync` and `token`

`lock(&Mutex<T>)` recovers a poisoned lock instead of panicking. A panic inside a native call
surfaces in Python as `PanicException`, a `BaseException` the SDK's `except Exception` arms never
see. So a poisoned lock must not turn into a second panic on every later call. The state behind it
stays consistent because every guarded section is panic-free: the workspace lints deny `unwrap`,
`expect` and indexing in every crate that takes the lock.

`TokenMint` hands out settlement tokens, one per delivery or transaction, starting at 1 and stepping
by one. It is atomic, so one type serves a per-instance state machine (each `OffsetTracker` has its
own) and a process-wide `static` (memory transactions) alike. `NO_TOKEN` (`0`) is never issued, so a
host's "no delivery" or "no transaction" placeholder cannot alias a live token. A settlement names a
delivery by its token, never by its coordinates, so a late settlement for a copy a rewind or a
rebalance replaced settles nothing.

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
| `rejected` | **terminal** | a host's own business rejection, marked terminal by the host |

<sup>`edax_core::ErrorCode`; the same ten rows, with wire forms, in `testdata/golden/error_taxonomy.json`</sup>

A macro generates the enum over that row list, so the variant list, `ALL`, `as_str` and
`is_terminal` cannot drift. A new code is one new row. A row cannot be added without a verdict.
`ErrorCode` is deliberately **not** `#[non_exhaustive]`, so a new code is a compile error in every
SDK rather than a wildcard's silent default.

A `rejected` row covers a host's own business rejection ("insufficient funds"), marked terminal by
the host. Rust wraps it in `edax::Terminal`. Python raises `edax.Rejected`, or any exception that
declares `terminal = True`.

**One error type.** Every kernel function fails with `edax_core::Error { code, message }`, never a
bare `String`. The code comes from the kernel, so no caller picks one or forgets to. Its
`Display` is the wire form `"<code>: <message>"`, built by `ErrorCode::tag`; `Error::from_wire` is the
one place that takes it apart, on the first `": "`. The binding crosses `to_string()` and adds
nothing. The Rust SDK re-exports the same type as `edax::Error`, so a failure renders one way and a
cached failure stores the same bytes in every SDK. A name the taxonomy does not know is itself a
`codec` error (`"timeout".parse::<ErrorCode>()`), because the same name fails the same way every
time.

**The unclassified default is the kernel's too.** `verdict(None)` and `verdict_of_wire(wire)` give the
verdict for a failure with no code the kernel knows. Such a failure is a host's own exception, or a
wire string with an unknown code, no separator, an uppercase code, or nothing at all. It is a `store`
failure (`Error::from_wire` reads such a string as one), so it is transient. A wrongly transient
failure is redelivered loudly and the work survives, while a wrongly terminal one is cached under its
key and poisons it. `testdata/golden/error_wire.json` pins every case, negative ones included, so no
SDK states this default for itself.

What *branches* on the verdict — the exception hierarchy, `is_terminal`, the retry middleware, the
idempotency guards, the dead-letter path — belongs to the SDK and is documented in
[../sdk/errors.md](../sdk/errors.md). The short version: nothing above the kernel states its own
verdict. Python reads it at class-definition time and reads the unclassified default from
`error_verdict`; Rust reads the code off the error and asks `edax_core::verdict`.

## See also

- [README.md](./README.md) — what each kernel owns and why the boundary sits there.
- [event.md](./event.md) — `edax-event`, which reuses `fnv1a32` for local fan-out and owns
  `Retries`.
- [cqrs.md](./cqrs.md) — `edax-cqrs`, which reuses `fnv1a64` as `payload_hash` and owns the
  read side (filter and query).
- [kafka.md](./kafka.md) — `edax-kafka`, whose `OffsetTracker` mints its tokens with `TokenMint`.
- [golden-vectors.md](./golden-vectors.md) — how `backoff.json`, `error_taxonomy.json` and
  `error_wire.json` are generated and asserted.
- [../sdk/errors.md](../sdk/errors.md) — the exception hierarchy and every code path that branches
  on terminality.
- [../sdk/commands.md](../sdk/commands.md) — the `retry` middleware that consumes the backoff
  schedule.

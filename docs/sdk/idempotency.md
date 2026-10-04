# Idempotency

The four guards, the two orderings they can run, and what the ledger holds after every failure.

A guard wraps a handler, claims a key in a ledger before the handler runs, and writes the outcome
after. A redelivery of the same key replays that outcome instead of running the handler again. What
the replay is worth depends on whether the claim and the handler's write commit together. This page
covers both cases.

| Guard (Rust) | Path | Keys on | Hashes | Python equivalent |
|---|---|---|---|---|
| `idempotent(store, scope, tenancy, key)` | command | `claim_key(tenancy, tenant, scope, key)` of what `key(&cmd)` returns; `None` bypasses | the name and the whole command value, canonicalized | `edax.command.idempotent(store, scope=..., tenancy=...)`, keyed by the message's `idempotency_key` and `tenant` |
| `idempotent_in(store, scope, tenancy, key, current)` | command | same, plus a `Fn(&C) -> Tx` handle provider | same | `edax.command.idempotent(store, scope=..., unit_of_work=)` |
| `idempotent_by_id(store, scope, tenancy)` | consume | `msg.id` under `scope`, and the message's `tenant` under a pool | the raw `msg.data` bytes | `edax.router.idempotent_by_id(store, scope=..., tenancy=...)` |
| `idempotent_by_id_in(store, scope, tenancy, current)` | consume | same, plus a `Fn(&EventMessage) -> Tx` handle provider | same | `edax.router.idempotent_by_id(store, unit_of_work=, scope=...)` |

The Rust guards live in `edax::command` and `edax::router`. The Python guards live in `edax.command`
and `edax.router`.

## The four guards

Both paths share one orchestration. Python routes both through `_guarded`; Rust routes both through
`guarded`. The guards differ only in how the key and the hash are derived and in what the caller
does with the result.

**Command path.** The key is the client's intent. In Python it is the `idempotency_key` of the
command message: given to `dispatch`, carried by a message `dispatch_message` runs, or the message's
own `id` when it carries none. A Rust host still reads it, and the tenant, with a key closure that
returns an `IdempotencyKey` (a plain `String` is a key with no tenant). `None` makes no claim, and the
handler runs. The guard claims `claim_key(scope, key)`, so two guards with different scopes on one
ledger never replay each other's outcome. Under a pool the key carries the tenant, and under a pool or
a bridge a command with no tenant is refused as `codec` before the claim. The hash covers the command name and the whole
command value, every instance field. The kernel serializes, canonicalizes and hashes it in
`edax_cqrs::command_hash`. A Python command declares its name as `type: ClassVar[str]`. A Rust
command implements `command_type()`. Reusing a key with another command, or another name, is a
`PayloadMismatch`.

**Consume path.** The key is `msg.id` under the guard's `scope`, which names the consumer:
`edax:evt:{len(scope)}:{scope}:{id}`, composed with the message's `tenant` under a pool as a command
claim is. Each consumer keeps its own record, so two consumers guarding over one ledger (two read
models in one database, say) each run their handler once. A ledger keyed by the bare id would let the
second find the first's record and skip its own work without an error. The hash is the kernel's
`payload_hash`, Fowler-Noll-Vo 1a (FNV-1a) 64, over the raw `msg.data` bytes. Extensions, metadata
and time take no part. Those bytes are host-serialized, so they are not
canonicalized as JSON. Two events sharing an `id` with different bodies raise `PayloadMismatch`: an
integrity error, not a duplicate.

There is one behavioural difference. No caller awaits an event handler's result. A redelivery of a
completed event therefore returns nothing and skips silently. A command redelivery returns the
winner's cached value.

The seam is one Protocol in Python (`edax.command.IdempotencyStore`) and one trait in Rust
(`edax::IdempotencyStore<O>`, which carries the transaction handle type as `type Tx`). `joins_transaction`
is Python-only; see [Rust parity](#rust-parity).

A won claim comes back with a claim handle: `Won(handle)` in Python, `Claim::Won(ClaimHandle)` in
Rust. The guard passes it to `complete` and `release`, so a store whose claims can lapse settles a key
only while it still holds that exact claim. The Redis ledger uses the claim's own stored value as the
handle (see [backends/redis.md](../backends/redis.md#the-claim-handle)). The memory stores mint a
handle per claim and fence on it the same way; their `expire(key)` hook lapses a claim for a test.

## The two modes

The Python guard picks the ordering from two facts: whether the host passed `unit_of_work`, and
whether the ledger declares `joins_transaction`. The flag is a declaration the guard branches on and
cannot detect; absent, the answer is `False`.

| `unit_of_work` | `joins_transaction` | Function | Ordering |
|---|---|---|---|
| not given | either | `_guarded_around_the_work` | `claim; run; complete`; all three enlist in the host's ambient `current_tx` if one is open |
| given | `True` | `_guarded_in_one_commit` | `begin; claim; run; complete; commit` |
| given | `False` | `_guarded_around_the_work` | `claim; begin; run; commit; complete` |

`MemoryIdempotencyStore` declares `True`; `RedisIdempotencyStore` declares `False`, because Redis
cannot enlist in another database's transaction.

A store that declares `True` must refuse a handle belonging to another database: it raises
`BadHandle` instead of auto-committing. The guard skips its compensation on that store's word,
so a silent auto-commit would leave a `done` record for work that rolled back.

### Shared ledger: `begin; claim; run; complete; commit`

```mermaid
sequenceDiagram
    participant B as broker or caller
    participant G as guard
    participant L as ledger
    participant S as business store
    participant T as transaction

    B->>G: dispatch
    G->>T: begin
    activate T
    G->>L: "claim(key, payload_hash)"
    L-->>G: won
    G->>S: run the handler, which writes
    G->>L: "complete(key, handle, done record)"
    G->>T: commit
    deactivate T
    Note over T: failure window - a rollback here drops claim and write together, so there is nothing to compensate
    T-->>B: result
```

One atomic commit, so the rollback *is* the release. Every failure path in `_guarded_in_one_commit`
is a rollback that drops the claim with the work; the function writes no compensation at all.

Two consequences follow:

- A terminal failure is not cached. The record would roll back with everything else, so the key
  is freed. A later reuse of that key with a different payload is no longer caught as
  `PayloadMismatch`. The redelivery re-runs a deterministic failure, which left nothing behind.
- Losing the claim commits an empty transaction, which costs one round trip.

This mode is effectively-once for state in that database.

### Own ledger: `claim; begin; run; commit; complete`

```mermaid
sequenceDiagram
    participant B as broker or caller
    participant G as guard
    participant L as ledger
    participant S as business store
    participant T as transaction

    B->>G: dispatch
    G->>L: "claim(key, payload_hash)"
    L-->>G: won
    G->>T: begin
    activate T
    G->>S: run the handler, which writes
    G->>T: commit
    deactivate T
    Note over G,T: failure window - a commit that lands and loses only its reply releases a claim over work that survived
    G->>L: "complete(key, handle, done record)"
    G-->>B: result
```

Two independent commits, which no ordering can make atomic. This is at-least-once with a dedup
window the width of the ledger's retention. Every failure up to and including the commit releases
the claim by hand.

**Why `complete` runs after `commit`.** Take the other ordering, `claim; begin; run; complete;
commit`, with a ledger that cannot join the transaction. It would write the `done` record before the
business commit. A crash in between then leaves a `done` record over work that never landed. The
redelivery replays a success that did not happen, and the work is lost without an error. Running
`complete` after the commit inverts the loss: the worst case is a claim released over work that did
land, so the redelivery repeats it. A repeat is recoverable by an idempotent handler; silent loss is
not.

`examples/py/10_idempotent_events.py` runs the shared mode. `examples/py/19_fault_injection.py`
runs the own-ledger mode through the commit-lands-reply-lost window, with `BrokenCommit` from
`edax.testing.faults`.

```python
# examples/py/10_idempotent_events.py: one database for the ledger and the read model
db = MemoryTransactor()
balances = MemoryProjectionStore(db)
router = EventRouter(idempotent_by_id(MemoryIdempotencyStore(db), unit_of_work=db, scope="balances"))
```

## The record lifecycle

```mermaid
stateDiagram-v2
    [*] --> absent
    absent --> pending: claim wins - insert-if-absent
    pending --> done: complete with a result
    pending --> failed: complete with a terminal error
    pending --> absent: release - transient failure, cancellation, or a failed complete
    done --> absent: dedup window ends, or forget
    failed --> absent: dedup window ends, or forget
    note right of pending
        a loser reads pending as InProgress: transient
    end note
    note right of done
        a loser replays the stored result
    end note
    note right of failed
        a loser raises CachedFailure: terminal
        unreachable in shared mode
    end note
```

The wire shape is the kernel's: `{"payload_hash", "result", "error", "error_code", "pending"}`, read
in the order `pending` wins, then `error`, then `result`. A missing `pending` reads as `false`, so a
record written before the field existed reads as completed. A `null` result on a completed,
non-failed record is a success whose value is null, not "no result".

**What a rollback erases in shared mode.** The claim and the terminal record are staged in the same
transaction as the handler's writes. A rollback erases `absent -> pending`. It also erases whichever
of `pending -> done` or `pending -> failed` was staged. The key returns to `absent` with no trace.
That is why `failed` is unreachable there: `_guarded_in_one_commit` never writes it, and a record
written anyway would roll back with the failure that produced it.

**Forget.** `IdempotencyStore.forget(key)` deletes a settled record, so a re-driven message runs
its handler again. `edax.command.forget(store, scope, key, tenancy=..., tenant=...)` forgets what a
command guard of that scope stored, under the same composed key the guard claimed. Use it when an operator re-drives a dead-lettered message after fixing its cause. The
kernel's [forget decision](../kernels/cqrs.md#forget--deleting-a-settled-record) says which records
it deletes. See [commands](./commands.md#forget-re-driving-a-dead-lettered-command) for the call.

**Expiry.** A record must outlive the longest window in which the same key can be redelivered:
broker retry plus client retry. Expiry inside that window turns a replay into a re-run.
`RedisIdempotencyStore` splits the window in two. `claim_ttl_ms` bounds how long a pending claim
blocks its key when the worker holding it dies. `ttl_ms` is the dedup window for a terminal record.
Pinned by `test_claim_branches` and `test_complete_branches` in
`crates/edax-py/tests/test_redis.py`, which read each key's TTL back.

## The loser paths

A claimant that does not win reads the winner's record. The kernel classifies it. It checks the hash
first, then the state. The software development kit (SDK) only maps the verdict onto its own error
type.

| Verdict | Raised as | Class | What the caller does |
|---|---|---|---|
| `payload_mismatch` | `PayloadMismatch(key)` | terminal | dead-letter; the key was reused with a different payload |
| `in_progress` | `InProgress(key)` | transient | nack and let the broker redeliver; the winner has not settled |
| `failed` | `CachedFailure(error)` | terminal | dead-letter; the winner already failed deterministically |
| `succeeded` | returns the stored result, `None` included | none | replay |

**A replay is the handler's type, not its JSON.** A cached success crosses the ledger as JSON. Python's
`idempotent(..., result_type=...)` converts it back with msgspec. A retried request then gets the
`Invoice` the first dispatch returned, not a `dict`. Without `result_type`, the guard reads the return
annotation of the handler it wraps directly, when that annotation resolves at module scope. With neither,
with `result_type=typing.Any`, or with an `object` annotation, the replay is the plain JSON value. A type
msgspec cannot convert to raises `TypeError` at `register`. A stored result that does not convert to the
declared type is an unreadable record, described next. `tests/test_replay_type.py` runs every branch,
plus a crash-and-redeliver over each ledger ordering. Rust's guard decodes the record as the bus's `O`.

The hash is checked before the state, so a mismatched payload is rejected even while the winner is
still in progress. The kernel's `outcome` decides that, pinned by `outcome_branches`.

**An unreadable record is a cached failure.** A record no reader can decode still holds its key. It
may be a torn write, another layout, a field of the wrong type, or a result that does not convert to
the handler's type. Every store reads it with the kernel's `decode_prior`, which turns it into a terminal
`codec` failure. The guard replays it as `CachedFailure`, with `original_code` `codec` and a message
that names the decode error (`codec: unreadable idempotency record: EOF while parsing ...`). The
handler never runs again for the key, and the dispatcher dead-letters the message instead of
redelivering it until the record expires. Both SDKs reach the same failure from the same bytes,
pinned by the `decode_error` rows of `idempotency_record.json`.

The failure keeps the stored `payload_hash` when that one field still reads. Another command that
reuses the key is then still told `PayloadMismatch`, not that its own command failed. When the hash
itself does not read, the failure carries the claimant's hash, so the claimant gets the failure.
`forget` the key to run the handler again.

**A loser must never release the winner's key.** Python's `_replay` raises and returns; it writes
nothing. Rust's `guarded` does the same on the `Claim::Prior` arm. An over-eager compensation there
would delete a live claim another worker is still working under. Pinned by
`test_losing_the_claim_never_releases_the_winners_key`.

**The one pre-claim exception.** A `claim` that fails is not compensated, even though the insert
may have landed and only its response been lost. Both SDKs' seams are fallible here (Python's
`claim` raises, Rust's returns `Result<Claim<O>, Error>`), and both guards propagate the store's own
verdict without releasing. A failed claim returned no handle, so there is nothing to release with.
An orphan pending record costs `InProgress` until the claim's retention expires.

A store can narrow that window. The Redis ledger's claim script returns the value it found, and a
client that re-sends the script after a lost reply finds its own claim. The kernel reads that as won
(`claim_outcome`), so a reply lost and re-sent costs nothing.

`InProgress` is not a failed attempt. See
[consuming](./consuming.md#inprogress-is-contention-not-a-failed-attempt).

### `CachedFailure` is reachable in one mode only

The same handler code has two replay semantics, decided by the ledger rather than by the code.

One exception holds in both modes: a record no reader can decode replays as `CachedFailure`
([above](#the-loser-paths)).

Under a shared ledger a terminal failure is never cached. The `failed` record is written inside
the transaction the handler's failure rolls back, so it disappears with the work. The key is free,
and a client retrying a permanently failing request re-executes the handler every time. Under an
own ledger the same failure survives its own commit, so the next dispatch of that key raises
`CachedFailure` and the handler never runs again.

A shared ledger buys effectively-once for state in that database and pays for it by not remembering
failures. An own ledger remembers failures and pays at-least-once for the work. The difference is
invisible at the call site, so a service that swaps its ledger changes its replay
behaviour without changing a line of handler code. Test on the ordering you deploy. `MemoryLedger` is the
own-ledger shape in memory, with the same claim fence. Its `expire(key)` hook forces the takeover a Redis
claim TTL produces ([memory backend](../backends/memory.md#test-on-the-ordering-you-deploy-memoryledger)).

`CachedFailure` also carries only `str(e)` and the taxonomy code, not the exception type. Anything a caller derives
from the error class, an HTTP status in particular, is lost on replay. Map the status from the
message, or keep the mapping outside the guard.

## Every failure window

Rows 1 to 9 are the case table of `test_unit_of_work.py`, one case per window. "Runs" and "landed"
are counted across two dispatches of one key: the first, then its redelivery.

| # | Mode | What fails, when | Ledger holds after | Redelivery does | Runs | Work | Test |
|---|---|---|---|---|---|---|---|
| 1 | shared | nothing | `done`, committed with the write | replays the cached result | 1 | landed once | `shared_success_replays_instead_of_rerunning` |
| 2 | shared | handler, transiently | nothing: the rollback dropped the claim | re-runs the handler | 2 | rolled back both times | `shared_transient_failure_rolls_back_claim_and_work_so_the_redelivery_reruns` |
| 3 | shared | handler, terminally | nothing: the record would roll back, so it is never written | re-runs, fails the same way | 2 | rolled back both times | `shared_terminal_failure_is_not_cached_because_the_rollback_would_erase_it` |
| 4 | shared | commit, nothing landed | nothing | re-runs the handler | 2 | never landed | `shared_failed_commit_drops_the_claim_with_the_work` |
| 5 | own ledger | nothing | `done`, its own commit after the work's | replays the cached result | 1 | landed once | `split_success_replays_instead_of_rerunning` |
| 6 | own ledger | handler, transiently | nothing: the guard released the claim | re-runs the handler | 2 | rolled back both times | `split_transient_failure_releases_the_claim_so_the_redelivery_reruns` |
| 7 | own ledger | handler, terminally | `failed`, carrying the message | raises `CachedFailure`; the handler does not run | 1 | rolled back | `split_terminal_failure_is_cached_and_replays` |
| 8 | own ledger | commit, nothing landed | nothing: the guard released the claim | re-runs the handler | 2 | never landed | `split_failed_commit_releases_the_claim_so_the_work_is_not_lost` |
| 9 | own ledger | commit lands, its reply is lost | nothing; see below | re-runs and applies the work a second time | 2 | landed twice | `split_lost_commit_response_releases_a_claim_over_work_that_landed_so_it_runs_twice` |

In row 9 the guard released a claim over work that survived. It cannot tell a lost reply from a
commit that never happened.

No ordering removes row 9, so an own ledger is never effectively-once. A
handler behind one must keep its own write idempotent. `EventStore.append` and
`ProjectionStore.save` already are, through their compare-and-set on version.

Windows outside that table, each pinned elsewhere:

| What fails, when | Ledger holds after | Redelivery does | Test |
|---|---|---|---|
| the command will not serialize | nothing: `CodecError` is raised before any claim | fails the same way, still without a claim | `test_idempotent_branches[unencodable_payload_is_terminal_before_any_claim]` |
| `claim` fails | unknown; see below | re-claims. If the insert had landed, `InProgress` until retention frees the key | `test_a_claim_that_raises_is_not_compensated` |
| `complete` fails after a success | nothing: `_complete_or_release` releases instead | re-runs the handler | `test_idempotent_branches[success_with_failing_complete_releases_the_claim]` |
| `complete` lands, its reply is lost (every in-tree ledger fences: Redis and both memory stores) | `done`: the release carries the claim's handle and finds the settled record, so it deletes nothing | replays the cached result | `test_redis.py::test_a_complete_whose_reply_was_lost_keeps_the_record` |
| `complete` fails after a terminal failure | nothing: released, and the caller sees the handler's error, not the store's | re-runs the handler | `test_idempotent_branches[terminal_failure_with_failing_complete_shows_the_handlers_verdict]` |
| the handler's result will not serialize | `failed` of code `codec`: the guard encodes the result before the work commits, so the work rolls back | raises `CachedFailure`; the handler does not run | `test_unserializable_result_is_terminal_and_cached`, `split_unencodable_result_rolls_back_and_is_cached_as_a_codec_failure` |
| the dispatch is cancelled mid-handler | nothing: the `BaseException` branch releases | re-runs the handler | `test_cancellation_never_strands_a_pending_claim[cancelled_handler_releases_the_claim]` |
| the dispatch is cancelled mid-`complete` | nothing: released | re-runs the handler | `test_cancellation_never_strands_a_pending_claim[cancelled_complete_releases_the_claim]` |
| a Rust handler panics | nothing: released, and the panic keeps unwinding | re-runs the handler | `a_panicking_handler_releases_the_claim` |
| a stored record will not decode (a torn write, another writer's layout) | the unreadable bytes, untouched | raises `CachedFailure` of code `codec`; the handler does not run | `test_an_unreadable_record_replays_as_a_cached_codec_failure` (contract), `an_unreadable_record_replays_as_a_cached_codec_failure_and_never_runs_the_handler` |

When `claim` fails the ledger holds either nothing, or a pending record whose response was lost. The
guard does not compensate that, and the handler does not run. The Rust twin is
`a_failed_claim_is_reported_and_never_compensated`.

Python has no panic branch; its equivalent is the `BaseException` arm that catches cancellation.
Rust catches the unwind explicitly, because an unwinding handler would otherwise leave the key
pending with nobody left to settle it.

Two broker-level guarantees sit over all of this, asserted in both modes:

- A delivery that ends in a nack must leave its key free
  (`test_a_nack_releases_the_claim_so_the_redelivery_reruns`).
- A delivery that acks leaves a terminal record, and a redelivery inside the retention window
  replays it (`test_an_ack_keeps_the_claim_so_the_redelivery_is_skipped`).

One property runs over random histories in both SDKs. A key that reads back as a completed success
must have had its handler's write land at least once. Shared mode strengthens it to exactly one.
Pinned by `test_a_done_record_never_covers_work_that_did_not_land` and its Rust twin,
`a_done_record_never_covers_work_that_did_not_land`.

## Rules and constraints

**Nesting is refused.** Passing `unit_of_work` while a transaction is already open raises
`BadHandle` before the handler runs. A guard that opened a second unit of work inside the host's
would commit the claim independently of the writes it guards. That is the split-store failure inside
what looks like a shared store. `test_a_nested_unit_of_work_is_refused` pins it. Pass
`unit_of_work`, or open the transaction around the dispatch, never both.

**Many SQL databases abort the transaction on the first failed statement.** Every statement after
that failure is refused. An attempt made inside such an aborted transaction cannot reach the data,
and the abort masks the real error.

It is also why a retry must re-run a whole unit of work, never a handler inside one. A second attempt
inside the same transaction applies the first attempt's staged writes again, and one commit lands
both. So `retry` refuses to run while any `transaction` is open, and goes outside a guard given
`unit_of_work`. [Commands](./commands.md#where-retry-sits-relative-to-the-guard) states the rule.

```python
# examples/py/05_idempotent_commands.py: retry outermost, the guard owns the unit of work.
bus = CommandBus(retry(3), idempotent(MemoryIdempotencyStore(db), scope="accounts", unit_of_work=db))
```

**Store contract clauses for a claim taken inside a transaction**:

- `claim` stays an atomic insert-if-absent inside a transaction. Implement it as `INSERT ... ON
  CONFLICT DO NOTHING`, `SET NX`, or a unique-key insert, never a read followed by a write.
- A loser must reach one of two outcomes. The guard is correct under either.
  - It blocks on the winner's uncommitted row and then reads it, reporting the holder's record.
  - It fails, which the backend must raise as `StoreError`. That is transient, so the guard nacks and
    the redelivery re-claims.
- A third outcome is a bug. Swallowing the failure and then reading under a stale snapshot finds no
  row. It reports a win and runs the handler twice.
- Which of the two outcomes a database gives depends on its isolation level. Establishing that is
  the backend author's job, and no durable backend ships here to establish it for you.
- The blocking wait must be bounded by a lock or statement timeout, and the expiry must also surface
  as `StoreError`. A claim is a remote call like any other.
- A backend that cannot block may instead return the holder's record as pending, which the guard
  reports as `InProgress`. The in-memory store does that: it re-masks a holder's staged
  record as pending, because the holder may still roll back. Both outcomes are correct, so a caller
  must handle either.

**Record contents.** `payload_hash` is the kernel's unsigned 64-bit hash: persist it losslessly
as an unsigned integer, a `NUMERIC` or a string. A signed `BIGINT` reads every value at or above
`2**63` back as a negative integer. The SDK rejects that as a terminal `CodecError`. A transient
error there would redeliver forever. A `result` integer is exact within the `i64`/`u64` range and
comes back as a double outside it, so a wider integer travels as a string.

## What this is and is not

edax implements the Idempotent Consumer pattern: a dedup table beside the business change,
holding the key, the payload hash and the outcome.

It is not the transactional Inbox. The Inbox persists the message *body* and acks the broker
before processing, which decouples the handler from the broker. edax stores no message body, and the
guard runs synchronously in the delivery path, so the handler cannot be decoupled that way.

**The ceiling.** In shared mode, database state in that one database can be effectively-once: the
claim and the write are one commit. Nothing outside that database is covered. An email, a text
message or a payment application programming interface (API) call is a second system. It has a
commit of its own. It needs its own idempotency key in that system, for example Stripe's
`Idempotency-Key`. The guard collapses the redeliveries it sees. It cannot withdraw a request that
already left.

In an own ledger the ceiling is lower still: at-least-once with a dedup window. [delivery
guarantees](../guides/delivery-guarantees.md) states what edax does and does not promise.

## Rust parity

`joins_transaction` and `unit_of_work=` are Python-only. Rust hosts publish a handle provider to
`idempotent_in` or `idempotent_by_id_in` instead. [Rust parity](./rust-parity.md) states the whole
difference.

## See also

- [transactions.md](./transactions.md) — the `Transactor` seam, `current_tx`, `transaction` and
  `joins_transaction` from the transaction's side.
- [errors.md](./errors.md) — the terminal/transient taxonomy every branch above reads.
- [commands.md](./commands.md) — the `CommandBus`, the middleware chain and where `retry` sits.
- [consuming.md](./consuming.md) — how the dispatcher settles `InProgress`, a nack and a
  dead-letter.
- [../backends/redis.md](../backends/redis.md) — the cross-process ledger, its two time-to-live
  values (TTLs) and its fence.
- [../kernels/cqrs.md](../kernels/cqrs.md) — `command_hash`, `payload_hash`, `outcome` and the
  record.
- [../guides/delivery-guarantees.md](../guides/delivery-guarantees.md) — where these guarantees sit
  end to end.

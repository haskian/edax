# The Redis kernel — `edax-redis`

The Redis idempotency-ledger kernel: the key, the bytes stored under it, the Lua each write runs,
and what a reply means.

A Redis ledger is a cross-language contract. Software development kits (SDKs) that share one key
space must agree on four things. They are the key, the stored value, the script each write runs, and
what a reply means. Otherwise one takes over a claim another still holds. `edax-redis` holds exactly
those decisions. It links no Redis client library, so every SDK ships only the client calls around
it and the time to live (TTL) values it was configured with.

| Module | Owns | Golden vectors |
|---|---|---|
| `ledger` | the key, the refused settings, the heartbeat schedule, the stored value's layout, the claim handle, the claim, complete and forget outcomes | `redis_value.json`, `redis_ledger.json` |
| `scripts` | the five Lua bodies, and the verdict on a Redis error reply | `redis_scripts.json`, `redis_fence.json`, `redis_ledger.json` |

## The key

`ledger_key(prefix, key)` is `{prefix}{key}` as UTF-8 bytes. Two SDKs that composed it differently
would never meet on one ledger, and nothing would fail: both would run every command. So the
composition is the kernel's, not each SDK's. An empty prefix is refused, because it would put every
service sharing a Redis into one key space.

No separator is added, so concatenation is not injective: prefix `billing:` with key `prod:x` is the
key prefix `billing:prod:` gives `x`. Distinct services take prefixes neither of which starts with
the other.

`check_config(prefix, ttl_ms, claim_ttl_ms, op_timeout_ms, heartbeat)` refuses the settings a store
is built with. It refuses an empty prefix, and a TTL outside 1 to 2^53 - 1 milliseconds: the largest
integer every SDK language's number type holds exactly. It refuses a deadline that is not positive.
It refuses a heartbeat whose `op_timeout_ms` is not under `claim_ttl_ms / HEARTBEATS_PER_TTL` (3).
The heartbeat sends a renewal every third of the TTL, timed from the claim's send on a monotonic
clock, so a slow reply never delays the next beat. A renewal sent at `S` that lands holds the lease
to at least `S + claim_ttl_ms`. If the next one stalls, the one after is sent at
`S + 2 * claim_ttl_ms / 3` and lands within `op_timeout_ms`. It must land before the lease lapses,
or a second worker wins a key the first still runs. The bound holds only on that schedule, so the
schedule is the kernel's too. `heartbeat_due_ms(sent_at_ms, claim_ttl_ms, beat)` is when beat `beat`
is due: `sent_at_ms + beat * claim_ttl_ms / 3`, rounded down so a beat is never late. Redis
refuses a zero or negative TTL on every write, so a store checks its settings when it is built. Every refusal is a terminal
`bad_handle` error.

The defaults a store takes when its host sets none are the kernel's too: `DEFAULT_TTL_MS` (a day),
`DEFAULT_CLAIM_TTL_MS` (five minutes) and `DEFAULT_OP_TIMEOUT_MS` (five seconds). Two SDKs on one
ledger left at their defaults then agree on its windows.

## The value

One key holds the layout version, the owner token, then the idempotency record:

```
idem:order-7  ->  "1" + "<32 hex characters of owner token>" + "<kernel record JSON>"
```

**Why one key and not two.** A sibling key holding the token would expire on a clock of its own. The
two clocks drift apart. A claimant could then win the record, lose the owner, and hold a claim it
can never settle. One key means one TTL governs both halves.

**Why a version byte.** A later layout must be refused by name, not split at the wrong place. The
unversioned layout of edax 0.1 starts with the token's first hexadecimal digit, so this kernel
refuses it as layout version `'0'`.

| Function | Builds |
|---|---|
| `new_claim(payload_hash)` | a claim value under a fresh owner token |
| `claim_value(owner, payload_hash)` | the same, with the owner given: version, owner, the kernel's **pending** record |
| `settled_value(claim, record_json)` | the claim's own owner, then a settled record the caller already encoded |
| `split_value(raw)` | a stored value read back apart into `(owner, record)` |

The record half has exactly one author. `claim_value` asks `edax-cqrs` for the pending record's
bytes, and `settled_value` passes the caller's bytes through untouched, once
`edax_cqrs::check_settled` has refused a pending record as `bad_handle`. A pending record would hold
the key as a claim nobody runs, for the whole record TTL. A host that already holds a
record's bytes must not re-encode them here, or the result would round-trip through a second
serializer on its way to the ledger. `testdata/golden/idempotency_record.json` keeps pinning that
record's wire shape.

## The owner token and the claim handle

`new_claim` mints the owner token as a universally unique identifier (UUID) version 7 in its simple form: 32
lowercase hexadecimal characters, no hyphens. Version 7 rather than random bytes, so tokens sort by
mint time and a ledger dump reads in order. A 42-bit counter shared within a process and 32 random
bits are what make two claimants distinguishable (see [core.md](./core.md)).

**The claim value is the claim's handle.** The claimant writes it, the store returns it as
`Won(handle)`, and every later write compares the stored value with it exactly. A claim that
lapsed and was taken over holds another value, so its old owner settles nothing. A settled record
holds another value too, so a late release never deletes the outcome of committed work. No width is
chosen by the caller, so no prefix of a token can match another's.

`check_claim(claim)` refuses a handle this kernel never minted. It refuses a handle with no layout
version, and one too short. It refuses an owner token that is not 32 lowercase hexadecimal characters,
and a record half that is not a pending record. `settled_value` applies the same check. The scripts
would only fail to match such a handle, so refusing it names the wiring mistake instead. Upper case
is refused rather than folded, because the scripts compare bytes. The token never appears in a message: inside a claim value it authorizes
settling that claim.

`split_value` is strict about the frame and liberal about the token. It refuses a value that is not
UTF-8, carries another layout version, or is too short to hold a token, because none of those can be
settled by anyone. It accepts a token this kernel would not mint, so the record behind it is never
stranded.

| `split_value` input | Result |
|---|---|
| a value this kernel wrote | `(owner, record)` |
| the version and owner alone | that owner, and an empty record |
| a 32-character owner this kernel would not mint | accepted, because reading is liberal |
| no layout version, or another one | codec error naming the version |
| fewer than 32 characters after the version | codec error naming the width |
| empty | codec error |
| bytes that are not UTF-8 | codec error naming the offending index |
| a multibyte character straddling the token's width | codec error, since the token is ASCII and this value is not one |

## The claim outcome

`claim_outcome(reply, claim)` reads the claim script's reply:

| Reply | Outcome |
|---|---|
| nil, the key was free | `Won` |
| the caller's own claim value | `Won`: the first send landed, its reply was lost, and the client sent the script again |
| any other value | `Prior(record_json)`, the record that holds the key, for `edax_cqrs::decode_prior` |
| a value `split_value` refuses | `Prior` of the kernel's unreadable record: a `codec` failure under the claimant's hash, naming the refusal |
| a value `split_value` refuses, when `claim` is not a claim this kernel minted | codec error |

A value `split_value` refuses still holds the key, and nobody can settle it. So the claimant reads
it the way it reads an unreadable record half: a cached `codec` failure every SDK replays the same
way.
`redis_ledger.json` pins the row.

The second row is why the outcome is a kernel decision. redis-py re-sends a command whose reply was
lost by default. A store that read its own claim as someone else's would report `InProgress` with no
handler running, until the claim lapsed.

## The complete outcome

`complete_outcome(key, written)` reads the complete script's reply. `1` is `Ok`. `0` means the key
held neither this claim nor this settled value: the claim lapsed and was taken over, or the key settled
with another record. That is `edax_cqrs::lapsed_claim(key)`, a `store` error, the same text the memory
store's fence refuses with. The outcome was not recorded, so the caller must not report success.

## The forget outcome

A forget deletes a settled record, so a re-driven message runs its handler again. The store reads the
key, then asks `forget_outcome(key, held)` what to do with the value it read:

| Value read | Outcome |
|---|---|
| none, the key is free | `Nothing`: send no script |
| a pending claim | `in_progress` error naming `key`: its run may still complete, and its record would land after the delete |
| a completed or failed record | `Delete` |
| a frame `split_value` refuses, or a record half no reader decodes | `Delete`: `claim_outcome` replays it as a cached failure, so forgetting it is the way out |

On `Delete` the store runs `FORGET_SCRIPT` with the value it read. The script deletes the key only
while it still holds that exact value. A record that lapsed between the read and the script, and a
claim that took the key over, both survive. Then the store reads the key again, at most `FORGET_READS`
times, before it gives up with `forget_exhausted(key)`, a transient `store` error. The record half's verdict is `edax_cqrs::forget`, so the
memory store and the Redis ledger refuse the same keys with the same text.

## The scripts

Every write is one script, so the compare and the write are one atomic step. A `GET` followed by a
`SET` from the client would leave exactly the gap the fence exists to close.

| Constant | `ARGV` | Does |
|---|---|---|
| `CLAIM_SCRIPT` | claim value, claim TTL | writes the claim if the key is free; replies nil, or the value the key holds |
| `COMPLETE_SCRIPT` | handle, settled value, record TTL | writes the settled value if the key holds the handle or already holds the settled value; replies `1` or `0` |
| `RELEASE_SCRIPT` | handle | deletes the key if it holds the handle; replies `1` or `0` |
| `FORGET_SCRIPT` | the value read | deletes the key if it still holds that value; replies `1` or `0` |
| `EXTEND_SCRIPT` | handle, claim TTL | resets the TTL if the key holds the handle; replies `1` or `0` |

`FORGET_SCRIPT` has `RELEASE_SCRIPT`'s body, so both share one `EVALSHA` digest. Each deletes a key
only while it holds the exact value the caller names. A release names its claim handle, and a forget
names the settled value it read.

Every script is idempotent, so a client that re-sends one after a lost reply gets the answer the
first send would have had. Each touches one key, `KEYS[1]`, so each is safe on Redis Cluster.

**The bodies are constants, byte for byte.** A client registers the source once and sends `EVALSHA`
after, and that digest is taken over these exact bytes, leading newline included. An SDK that
reformatted a script would cache a second digest for one fence, so two SDKs would run two scripts on
the server. `testdata/golden/redis_scripts.json` is the source of truth for the bodies.

**What each script does is a vector too.** `testdata/golden/redis_fence.json` has one row per case.
A row holds the key's value and TTL before, the script and its arguments, and the reply, value and
TTL after. The generator writes the expectations out by hand rather than computing them, because no
Rust code re-implements the scripts. Each SDK replays the rows against a server that runs Lua. Python runs them against fakeredis
and, given `EDAX_REDIS_URL`, against a live Redis.

**Which write the fence lets through is backend-neutral.** `edax_cqrs::complete_fence` and
`release_fence` decide from what the key holds: the caller's claim, another claim, the same settled
record, another record, or nothing. The memory store calls them. Lua cannot call Rust, so
`testdata/golden/idempotency_fence.json` pins the same table, and the Python suite replays it against
these scripts: one fence in every ledger. `redis_fence.json` adds what is Redis's own: the TTLs, the
claim and extend scripts, and handles compared byte for byte.

## The reply verdict

`reply_error(reply)` turns a Redis error reply, prefix included, into the kernel's error:

| Prefix | Code | Why |
|---|---|---|
| `WRONGTYPE` | `codec`, terminal | the key holds something the ledger cannot read |
| `ERR`, `NOPERM`, `NOAUTH`, `WRONGPASS` | `bad_handle`, terminal | a refused command, script or credential fails the same way every time |
| anything else | `store`, transient | a passing server state (`READONLY` during a failover, `LOADING`, `BUSY`, `OOM`, `TRYAGAIN`, `NOSCRIPT` and more) or a prefix the kernel does not know |

The unknown-prefix default is transient, the core default: a wrongly transient failure is redelivered
loudly, a wrongly terminal one is dead-lettered.

## What stays in the SDK

The kernel owns every decision two SDKs must make the same way. Each SDK keeps only the input and
output (I/O):

- the two TTL values, for a pending claim and for a settled record, and their defaults.
- the per-call deadline, and the heartbeat task that sends `EXTEND_SCRIPT` when `heartbeat_due_ms` says.
- the connection seam, `RedisConn` in Python, and the client it adapts.

## The golden vectors

| File | Case shape | What it pins |
|---|---|---|
| `redis_value.json` | `name`, `op`, the operands, `value` or `want` | `claim`, `settled` and `split`: a pending claim, a hash at both `u64` ends, a cached success and failure, a non-ASCII result, and every `split_value` branch |
| `redis_ledger.json` | `name`, `op`, the operands, `want` or `error` | `key`, `config`, `handle`, `outcome`, `forget`, `settle`, `complete_outcome`, `forget_exhausted`, `heartbeat_due` and `reply`: every refusal and every outcome row above |
| `redis_scripts.json` | `name`, `body` | the five Lua bodies verbatim, so every SDK's `EVALSHA` digest matches |
| `redis_fence.json` | `name`, `initial`, `script`, `args`, `want_reply`, `want_value`, `want_ttl_ms` | what each script does to a key, including a lost reply re-sent, a takeover, malformed handles, and a forget after a takeover |

Regenerate with `EDAX_REGEN=1 mise exec -- cargo test -p edax-redis --test golden`, and read the
diff before committing it. A change to any file is a compatibility event: live ledgers already hold
values in the old layout, and clients already cache the old digests.

## How each SDK reaches it

| | Rust | Python |
|---|---|---|
| the key | `ledger_key` | `_edax.redis_ledger_key` |
| the settings | `check_config`, `DEFAULT_TTL_MS`, `DEFAULT_CLAIM_TTL_MS`, `DEFAULT_OP_TIMEOUT_MS` | `_edax.redis_check_config`, `_edax.redis_defaults()` |
| the heartbeat | `heartbeat_due_ms` | `_edax.redis_heartbeat_due_ms` |
| a claim value | `new_claim`, `claim_value` (fixed owner, a test hook) | `_edax.redis_new_claim` |
| a settled value | `settled_value`, `check_claim` | `_edax.redis_settled_value`, `_edax.redis_check_claim` |
| read one back | `split_value`, `claim_outcome` (an `edax_cqrs::Claimed`) | `_edax.redis_claim_outcome`, one `ClaimResult` shape with the memory store's claim |
| a complete's reply | `complete_outcome` | `_edax.redis_complete_outcome` |
| a forget | `forget_outcome`, `FORGET_READS`, `forget_exhausted` | `_edax.redis_forget_outcome`, `_edax.redis_forget_reads`, `_edax.redis_forget_exhausted` |
| the scripts | `CLAIM_SCRIPT`, `COMPLETE_SCRIPT`, `RELEASE_SCRIPT`, `FORGET_SCRIPT`, `EXTEND_SCRIPT` | `_edax.redis_scripts()`, one field each |
| a reply error | `reply_error` | `_edax.redis_reply_error` |

The Rust SDK ships the kernel and no client shell. The Redis ledger lives in the Python SDK:
`RedisIdempotencyStore` in `edax.backend.redis` calls the client and these functions. Read
[backends/redis.md](../backends/redis.md) for the store's side.

## See also

- [backends/redis.md](../backends/redis.md) — the store: the two TTLs, lease renewal, the handle in
  use, durability, and the failure windows it leaves.
- [kernels/cqrs.md](./cqrs.md) — the idempotency record whose bytes fill the value's last part.
- [kernels/golden-vectors.md](./golden-vectors.md) — how the vectors here are generated and asserted.
- [sdk/idempotency.md](../sdk/idempotency.md) — the guard that claims, completes and releases, and
  `forget` for a re-drive.

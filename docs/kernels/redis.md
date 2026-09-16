# The Redis kernel — `edax-redis`

The Redis idempotency-ledger kernel: the bytes stored under a key, and the Lua a fenced write runs.

A Redis ledger's bytes are a cross-language contract. Two software development kits (SDKs) pointed
at one key space must agree on what a stored value looks like, and on the script a fenced write
runs. Otherwise one takes over a claim the other still holds. `edax-redis` holds exactly those
bytes. It links no Redis client library, so every SDK ships only the client calls around it.

| Module | Owns | Golden vector |
|---|---|---|
| `ledger` | the stored value's layout, the owner token, the two fence scripts | `redis_value.json`, `redis_scripts.json` |

## The value

One key, `{prefix}{key}`, holds the owner token followed by the idempotency record:

```
idem:order-7  ->  "<32 hex characters of owner token><kernel record JSON>"
```

**Why one key and not two.** A sibling key holding the token would expire on a clock of its own. The
two clocks drift apart. A claimant could then win the record, lose the owner, and hold a claim it
can never settle. One key means one time to live (TTL) governs both halves.

| Function | Builds |
|---|---|
| `claim_value(owner, payload_hash)` | the owner plus the kernel's **pending** record for that hash |
| `record_value(owner, record_json)` | the owner plus a settled record the caller already encoded |
| `split_value(raw)` | the same value read back apart into `(owner, record)` |

The record half has exactly one author. `claim_value` asks `edax-cqrs` for the pending record's
bytes, and `record_value` passes the caller's bytes through untouched. A host that already holds a
record's bytes must not re-encode them here, or the result would round-trip through a second
serializer on its way to the ledger. `testdata/golden/idempotency_record.json` keeps pinning that
record's wire shape.

## The owner token

`new_owner()` mints a universally unique identifier (UUID) version 7 in its simple form: 32
lowercase hexadecimal characters, no hyphens. Version 7 rather than random bytes, so tokens sort by
mint time and a ledger dump reads in order. The 74 random bits are what make two claimants
distinguishable.

The two writers validate the token before they build a value. A token of another width would match a
prefix of someone else's, because the fence compares a fixed width. A token with a non-hexadecimal
byte is one this kernel did not mint. Both are a codec error naming the width, never the token
itself: the token is the capability that authorizes overwriting a claim.

`split_value` is strict about the frame and liberal about the token. It refuses a value that is not
UTF-8, and one too short to hold a token, because neither can be settled by anyone. It accepts a
token this kernel would not mint, so a ledger written by an older store stays readable. What the
record half says is the record decoder's business, not this function's.

| `split_value` input | Result |
|---|---|
| a value this kernel wrote | `(owner, record)` |
| the owner alone | that owner, and an empty record |
| a 32-character owner this kernel would not mint | accepted, because reading is liberal |
| fewer than 32 characters, or empty | codec error naming the width |
| bytes that are not UTF-8 | codec error naming the offending index |
| a multibyte character straddling character 32 | codec error, since the token is ASCII and this value is not one |

## The fence

A claim can lapse and be taken over. The lapsed worker's `complete` must not overwrite the new
holder's claim, and its `release` must not delete it. Both settle through a compare-and-write.

```lua
if redis.call('GETRANGE', KEYS[1], 0, #ARGV[1] - 1) == ARGV[1] then
  return redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3]) and 1 or 0
end
return 0
```

```lua
if redis.call('GETRANGE', KEYS[1], 0, #ARGV[1] - 1) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
```

`GETRANGE` reads only the token's width, compares it to the caller's token, and writes only on a
match. A `GET` followed by a `SET` would leave exactly the gap the fence exists to close. Both
scripts return `1` when the write landed and `0` when the key is absent or another claimant holds
it, which is how a caller learns it lost its claim.

**Both bodies are constants, byte for byte.** A client registers the source once and sends `EVALSHA`
after, and that digest is taken over these exact bytes, leading newline included. An SDK that
reformatted the script would cache a second digest for one fence, so the two SDKs would run two
scripts on the server. `testdata/golden/redis_scripts.json` is the source of truth for the bodies,
and a Rust unit test asserts that both scripts are the same guard over a different write.

## What stays in the SDK

The kernel owns bytes. Everything around them is orchestration, and each SDK keeps its own:

- key composition, `{prefix}{key}`, and the prefix itself.
- the two TTL defaults, for a pending claim and for a settled record.
- the bound on the claim retry loop.
- the `complete` and `release` decision trees, including which failure keeps the token.
- the connection seam, `RedisConn` in Python, and the client it adapts.

That split is the same rule the other backend kernels follow: a protocol module in Rust, an input
and output (I/O) shell per SDK.

## The golden vectors

| File | Case shape | What it pins |
|---|---|---|
| `redis_value.json` | `name`, `op`, plus the operands and `value` or `want` | seventeen cases across `claim`, `record` and `split`: a pending claim, a hash at both `u64` ends, a cached success and failure, a non-ASCII result, and every `split_value` branch |
| `redis_scripts.json` | `name`, `body` | both Lua bodies verbatim, so a second SDK's `EVALSHA` digest matches |

Regenerate with `EDAX_REGEN=1 mise exec -- cargo test -p edax-redis --test golden`, and read the
diff before committing it. A change to either file is a compatibility event: live ledgers already
hold values in the old layout, and clients already cache the old digest.

## How each SDK reaches it

| | Rust | Python |
|---|---|---|
| mint a token | `edax_redis::new_owner` | `_edax.redis_owner()` |
| build a value | `claim_value`, `record_value` | `_edax.redis_claim_value`, `_edax.redis_record_value` |
| read one back | `split_value` | `_edax.redis_split_value` |
| the fence bodies | `FENCED_SET_SCRIPT`, `FENCED_DEL_SCRIPT` | `_edax.redis_fenced_set_script()`, `_edax.redis_fenced_del_script()` |

The Rust SDK ships the kernel and no client shell. The Redis ledger lives in the Python SDK:
`RedisIdempotencyStore` in `edax.backend.redis` calls the client and these functions. Read
[backends/redis.md](../backends/redis.md) for the store's side.

## See also

- [backends/redis.md](../backends/redis.md) — the store: the two TTLs, the fence in use, and the
  failure windows it leaves.
- [kernels/cqrs.md](./cqrs.md) — the idempotency record whose bytes fill the value's second half.
- [kernels/golden-vectors.md](./golden-vectors.md) — how the two vectors here are generated and
  asserted.
- [sdk/idempotency.md](../sdk/idempotency.md) — the guard that claims, completes and releases.

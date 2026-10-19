# The sealing kernel — `edax-seal`

`edax-seal` owns authenticated-encryption primitives and the reference key-provider state machine.
It performs no provider or storage I/O. Record context belongs to the kernel that owns that record;
SDKs supply I/O and hold write fences through transaction completion.

| Module or API | Owns | Evidence |
|---|---|---|
| `edax-seal` public primitives | Cipher names, key/nonce/tag lengths, encryption framing, authenticated encryption, OS randomness, HMAC-SHA256, injective `Aad` and `KeyScope` encoding | `crates/edax-seal/tests/seal.rs` |
| `edax-seal::keys` (re-exported) | Atomic in-memory reservations, generations, separate fingerprint purpose, permanent scope/tenant revocation, key destruction | `crates/edax-seal/tests/keys.rs` |
| `edax-event::sealing` | Message/state AAD, immutable ownership, strict plaintext policy, provider-status interpretation, protected-state encoding, prepared-batch reconciliation | `testdata/golden/sealing.json`, event sealing tests |
| `edax-event::erasure` | Frozen erasure operations, legal transitions, acknowledgements, control-notification content | Event erasure tests |
| `edax-cqrs::protected` | Canonical keyed fingerprints, protected result/failure records, replay decisions, preservation of ordinary claim keys | `protected_ledger.json`, CQRS protected tests |
| `edax-postgres::erasure` | Durable workflow SQL, notifications, destination tombstones and transaction fences | Postgres kernel and live adapter tests |

This is an optional protocol with typed fields, not an extension-key convention. Cryptography does
not belong in `edax-core`; core supplies primitive identifier checks and the shared error taxonomy.
The synchronous ABI converts types; Python and Go orchestrate providers and stores. See the
[crypto-shredding guide](../guides/crypto-shredding.md) for setup, migration, and operational limits.

## Framing and algorithms

`EncryptionInfo` carries `format_version: u32`, `key_namespace: String`, `key_id: String`,
`cipher: Cipher`, and `nonce: Vec<u8>`. Only format version 1 is accepted. Namespace and key ID
must be nonempty. A key ID is an immutable opaque generation reference, never a URL or secret.
The crypto format version is independent of a message's payload schema `version`.

`SealedValue` pairs that information with `data`: ciphertext followed by the full 16-byte tag.
Both supported ciphers use 32-byte keys. `aes-256-gcm` uses a 12-byte nonce;
`xchacha20-poly1305` uses a 24-byte nonce and is the default. Unknown algorithms and malformed
framing fail; readers never infer a cipher or downgrade to plaintext.
Byte fields are Base64 in human-readable serialization and native bytes in MessagePack.

`seal` and `open` accept explicit AAD. `seal` accepts an explicit nonce in its encryption
information for interoperability; normal SDK writes use `EncryptionInfo::fresh`, which stamps the
format version and obtains a nonce from OS randomness. `default_cipher()` owns the SDK default.
`open` exposes no plaintext on authentication failure. The provider reserves usage before sealing,
including invocations whose later storage write fails.

AES random-nonce generations have a hard ceiling of `AES_GCM_MAX_INVOCATIONS = 2^32` across all
writers for the same key. Rotation is a provider responsibility, not a counter per SDK instance.
This implements the invocation constraint in
[NIST SP 800-38D §8.3](https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf);
it does not license nonce reuse or remove other algorithm limits.

## Exact associated-data encoding

Every context starts with `u32(1)`, then the domain as a string. This version prefix is part of
the binary encoding. The following notation describes actual kernel bytes:

| Notation | Encoding |
|---|---|
| `str(x)` | UTF-8 bytes, prefixed by their byte length as big-endian `u64` |
| `bytes(x)` | Raw bytes prefixed by big-endian `u64` length |
| `u32`, `u64`, `i64` | Fixed-width big-endian integer |
| `opt_str`, `opt_u64`, `opt_i64` | One byte: 0 for absent, 1 followed by the encoded present value |
| `map(x)` | Big-endian `u64` entry count, then sorted string key/value pairs |
| `encryption` | `u32(format_version), str(key_namespace), str(key_id), str(cipher), bytes(nonce)` |

Maps use the kernel's `BTreeMap<String, String>` order. Absent and empty strings differ. No host
JSON serialization participates in message AAD. A field list below is ordered, not a set.

### Events

Domain: `edax.message.v1`.

```text
str("event"), encryption, str(id),
opt_str(tenant), opt_str(subject),
str(aggregate_id), str(name), u32(version), opt_u64(sequence),
str(correlation_id), str(causation_id), opt_i64(occurred_at),
map(extensions), map(metadata)
```

`recorded_at` is deliberately excluded: the store assigns it after a draft is sealed.
All other producer context above is fixed before sealing. Prepared-write reconciliation ignores
only this store-assigned time and requires the stored batch to match the retained drafts.

### Commands

Domain: `edax.message.v1`.

```text
str("command"), encryption, str(id),
opt_str(tenant), opt_str(subject), opt_str(aggregate_id),
str(name), u32(version), opt_str(idempotency_key),
str(correlation_id), str(causation_id), i64(issued_at),
map(extensions), map(metadata)
```

A payload cannot be moved from an event to a command, between identities, or between owners without
authentication failing. AEAD itself does not prevent replay of the unchanged valid envelope;
the sequencing and idempotency kernels supply that behavior.
See [RFC 5116](https://www.rfc-editor.org/rfc/rfc5116.html#section-1.2).

### Stored state

Domain: `edax.state.v1`.

```text
str(kind), encryption, str(identity), str(type_name), u64(version),
opt_str(tenant), opt_str(subject), str(marks), u32(completed)
```

`kind` is one of `snapshot`, `saga`, `ingestion`, `projection` and `intent`. `completed` is 0 or 1.
Snapshot identity is its storage key. Saga identity is its instance ID, with saga name as
`type_name`. Marks are the stored serialized processing marks. The other three seal a subject's
private data in the CQRS stores. `ingestion` binds the row key and its sequence, 0 in append mode.
`projection` binds the state key and the event sequence. `intent` binds the intent's storage key
with version 0. Each leaves `type_name` and marks empty and `completed` 0. The storage key carries the
generation and the scope fields carry the tenant and subject. So a sealed value copied to another row,
generation or subject fails authentication. The CQRS kernel's seal and open plans name these values;
a store never builds them.

The protected-state JSON envelope carries `edax_protected_state: 1`, `tenant`, `subject`,
`completed`, `encryption`, and `data`. `completed` must be present and boolean.
Completion stores encrypted JSON `null` with
`completed=true`, retaining the authenticated immutable owner. Runners authenticate it before
applying completed-instance admission; a generic store's running-state carrier does not mean the
saga is still active. Flipping the marker fails authentication.

### Scopes, command fingerprints, and results

Scope domain: `edax.key-scope`.

```text
str(key_namespace), opt_str(tenant), opt_str(subject)
```

The complete tuple is encoded even when the chosen storage tenancy mode does not compose tenant
keys. It cannot alias scopes through delimiter characters or absent values.

Command-fingerprint domain: `edax.command-fingerprint.v1`.

```text
str(name), u32(version), bytes(canonical_json(data))
```

The CQRS canonicalizer supplies RFC 8785 bytes. The fingerprint is all 32 bytes of HMAC-SHA256
under a dedicated per-scope key; its record includes `algorithm="hmac-sha256"`, that fingerprint
key's immutable `key_id`, and `value`. Data-key generation, message ID, and nonce do not affect
command equality. Fingerprint keys remain stable through data-key rotation and are destroyed on
erasure. They must never be used as data-encryption keys.

Result domain: `edax.ledger-result.v1`.

```text
u32(protected_record.format_version), bytes(scope.encode()),
str(claim_id), str(owner), str("done"),
str(fingerprint.algorithm), str(fingerprint.key_id), bytes(fingerprint.value),
encryption
```

Here `owner` is the store's claim fencing token, not the erasure subject.
The protected claim key is the existing ledger identity unchanged. Scope is authenticated inside
the record. A new scope cannot bypass an old claim or replay a result from another scope.
A protected record is pending, done with an encrypted result, or failed with a terminal nonfatal
taxonomy code. It contains no plaintext payload hash or arbitrary exception text. Legacy record
framing remains available for unprotected callers; protected readers reject malformed or legacy
completed records.

## Provider state machine

The reference `MemoryKeyProvider` is mutable synchronous state. Sharing hosts serialize its calls
with a mutex. Its `reserve(scope, cipher)` atomically chooses a generation and charges one
invocation; exhausted generations rotate while remaining readable. `reserve_many(scope, cipher,
count)` charges `count` invocations to one generation, so a batch seals many values under one
reservation, each with its own nonce. A generation with fewer than `count` left rotates; unused uses
stay charged. A count of zero or above the cipher's limit is `bad_handle`. Encryption generations are
separate per cipher. `fingerprint(scope)` selects a separate stable-purpose key.

`resolve(scope, cipher, key_id)` and `resolve_fingerprint(scope, key_id)` never create keys.
They require exact scope and purpose, including after destruction. A permanent tombstone cannot
turn an unrelated missing or unauthorized ID into evidence of erasure.

| State or condition | Kernel result |
|---|---|
| `unknown` | Reads fail with `KeyMissing`; a new write may reserve |
| `active` | Reads and reservations allowed |
| `revoking` | `KeyRevoking`; reads and reservations fenced |
| `destroyed` | `Shredded` for an exact destroyed reference; creation remains fenced |
| Provider unavailable | `KeyUnavailable` |
| Reference missing / scope or purpose wrong | `KeyMissing` / `KeyUnauthorized` |

`revoke` installs a permanent fence. A scope without a subject covers every existing and future
subject under that exact namespace and tenant. `destroy` erases all matching historical data keys
and fingerprint keys while retaining generation metadata and tombstones. It does not revive a
scope. `KeyMaterial` redacts debug output and zeroizes owned bytes on drop.

The provider is a process-local reference, not durable key storage. SDKs do not reuse a key cache.
Providers must preserve reservations, histories, and permanent revocations across restarts and
restore; wrapped key copies and backup handling are their responsibility. Already returned secret
or plaintext copies cannot be recalled. Host-language copying, especially Python immutable bytes,
prevents a claim that every process-memory copy is deterministically wiped.
Go clears operation-owned slices, but binding transfers and native allocations can leave copies
outside that slice. Buffer cleanup is not proof of complete host-process memory erasure.

## Workflow and I/O boundaries

The event erasure kernel accepts only ordered confirmed transitions:
`persisted -> revoked -> notified -> destruction_confirmed -> complete`.
An operation fixes scope, request and operation IDs, and its required participant set.
Acknowledgements belong to known required participants. Destruction confirmation requires all of
them; removing an offline participant is not a recovery transition.
Closed `ErasureStage` and `ErasureAction` names are exported for SDK agreement checks.
`validate_participants` rejects registered callbacks omitted from the required set, while permitting
required participants with no local callback. Workflow misuse returns `BadHandle`; decoding malformed
persisted operations returns `Codec` through `decode_operation`.

The store persists the initial operation before effects, compares revisions on updates, and
atomically enqueues the stable operation-ID notification with the notified transition.
Publication and acknowledgement are host I/O. The separate authorized control route must survive
tenant payload-key revocation. Acknowledgement asserts destination fencing, drained writers, and
completed cleanup; the kernel cannot independently verify that external evidence.

Coordinators attempt every pending cleanup callback even if an earlier one fails. They persist
successful acknowledgements before returning the first cleanup error. Recovery pages unfinished
operations by ID with a keyset cursor. Reset the cursor after an empty page so blocked operations
and newly inserted earlier IDs receive another pass.

Provider write guards must survive through destination commit. The memory guard coordinates one
process. The Postgres implementation uses transaction-scoped destination locks and a fresh
revocation check after acquisition under READ COMMITTED; the same transaction performs the
application write or cleanup. The guide describes the SDK transaction restrictions and the limits
of completion. [NIST SP 800-88 Rev. 2](https://csrc.nist.gov/pubs/sp/800/88/r2/final) informs the
sanitization scope; this state machine supplies no universal deletion proof.

## Conformance tests

Run from the repository root:

```sh
mise exec -- cargo test -p edax-seal
mise exec -- cargo test -p edax-event --test sealing --test erasure
mise exec -- cargo test -p edax-event --lib sealing
mise exec -- cargo test -p edax-cqrs --test protected
```

The [cipher tests](../../crates/edax-seal/tests/seal.rs) include independently published AES-GCM and
XChaCha20-Poly1305 vectors, malformed framing, and authentication failures.
The [provider tests](../../crates/edax-seal/tests/keys.rs) cover exhausted-budget rotation,
historical reads, wrong scope/cipher/purpose, creation racing revocation under a host lock,
tenant tombstones, and destruction of historical generations.
The [message tests](../../crates/edax-event/tests/sealing.rs) and
[golden bytes](../../testdata/golden/sealing.json) pin shared message behavior.
Inline state tests reject completion tampering for both ciphers.
The [workflow tests](../../crates/edax-event/tests/erasure.rs) and
[protected ledger tests](../../crates/edax-cqrs/tests/protected.rs) exercise the state machines.
[`protected_ledger.json`](../../testdata/golden/protected_ledger.json) pins scope encoding, command
fingerprints, result associated data, persisted records, and legacy/cross-scope refusals for both ciphers.

Rebuild the native module after kernel or ABI changes, then test Python:

```sh
(cd crates/edax-py && VIRTUAL_ENV="$PWD/.venv" mise exec -- maturin develop)
crates/edax-py/.venv/bin/python -m pytest -q \
  crates/edax-py/tests/test_crypto_workflows.py \
  crates/edax-py/tests/test_protected_ledger.py \
  crates/edax-py/tests/test_erasure.py
```

Those suites cover ciphertext at storage boundaries, owner changes, strict legacy policy, prepared
write retries, raw/opened views, result replay, write-draining fences, offline participants, delayed
destruction, and workflow recovery. Rebuild Go's static library before its race tests:

```sh
mise run go:build
(cd crates/edax-go && mise exec -- go test -race ./...)
```

For a disposable live Postgres database with `EDAX_POSTGRES_URL` configured:

```sh
EDAX_REQUIRE_POSTGRES=1 crates/edax-py/.venv/bin/python -m pytest -q \
  crates/edax-py/tests/test_erasure_postgres.py
```

The live suite verifies durable-operation recreation, notification CAS, paused writers, tenant
revocation against new children, transaction rollback and isolation checks, and protected state
storage. Memory tests cannot establish these durable properties.

These are runnable reference conformance scenarios, not a parameterized certification harness
for any provider or participant. Adapt and run the same races against each production port,
including restart and backup restoration. A passing in-memory suite does not demonstrate remote
KMS destruction, cleanup of unregistered systems, or distribution-wide write fencing.

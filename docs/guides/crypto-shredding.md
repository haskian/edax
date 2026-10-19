# Crypto-shredding

Edax can encrypt whole event and command payloads, aggregate snapshots, saga state, cached command
results, ingestion rows and projection state under keys belonging to an erasure subject. Revoking that
subject blocks new work; destroying its keys makes retained ciphertext unreadable through the provider.
Plaintext copies, such as a target kept `at_rest="plaintext"`, caches and exports, require their own
cleanup participants.

Encryption is an optional edax capability with typed `subject` and `encryption` envelope fields.
The [seal kernel](../kernels/seal.md) owns cryptography;
the event and CQRS kernels own authenticated context and workflow decisions. The SDKs wire
those decisions to storage and key providers. Configuring no sealing preserves ordinary operation.

This version assumes trusted producers, stores, and transports. After key destruction, a consumer
can confirm destruction through the provider but cannot authenticate the ciphertext or its associated
fields. The skip decision relies on that trust boundary.

The [verification record](crypto-shredding-verification.md) maps the implementation and review fixes
to tests. It also records the completed checks and the deployment checks still required.

## What stays visible

Only `data` is encrypted in an event or command. Its ciphertext includes the authentication tag.
Message and aggregate IDs, tenant, subject, payload name and version stay public.
So do correlation and causation IDs, times, event sequence, command idempotency key,
`extensions`, `metadata`, and the encryption information. Broker keys and headers stay public too.
State identity, version, processing marks, owner, and completion marker are public.
In ingestion rows and projection state, only the `private` part is sealed. The row or state key
(generation, tenant, subject, row or aggregate ID), the stream owner, the sequence, the `public` part
and the keyed fingerprints stay readable. Rejections keep the raw source record, which a sealed event
keeps sealed, and only the error code of a sealed record.
The [message field table](../kernels/event.md#message--the-messages) lists each envelope field and its type.

Use opaque identifiers and keep personal data out of those fields. Encryption does not conceal
payload length, traffic timing, or linkage between records. Whole-payload destruction also loses
nonpersonal business values inside that payload; separate facts with different retention needs
when designing events.

Authenticated encryption protects the payload and binds selected public fields as associated data.
It does not provide authorization or replay prevention on its own; tenancy, sequencing, and
idempotency still apply. [RFC 5116](https://www.rfc-editor.org/rfc/rfc5116.html#section-1.2)
makes that distinction explicit.

## Configure sealing

The reference provider is useful for tests and development. Restarting it loses both keys and
revocation tombstones: it cannot recover old ciphertext or enforce permanent erasure after restart.
No durable production key provider is supplied.

```python
from edax.sealing import MemoryKeys, Sealing

keys = MemoryKeys()
sealing = Sealing(keys, "development", cipher="xchacha20-poly1305")
subject_scope = sealing.scope("acme", "person-42")
tenant_scope = sealing.scope("acme", None)
```

Pass the same configuration to the application seams that write or read protected records:

```python
repo = Repository(store, events, aggregates, sealing=sealing)
bus = CommandBus(codec=commands, sealing=sealing)
router = EventRouter(sealing=sealing)
runner = SagaRunner(saga, saga_store, commands, sealing=sealing)

aggregate = await repo.load("account-1", tenant="acme", subject="person-42")
await bus.dispatch(command, tenant="acme", subject="person-42")
```

The second example assumes the application's codecs, stores, and domain types already exist.
Add the normal tenancy and middleware configuration for your deployment.

In Go, construct `NewMemoryKeys()` and `NewSealing(keys, namespace, cipher)`; an empty cipher selects
XChaCha20-Poly1305. Configure `RepositoryOptions.Sealing`, `NewSealedCommandBus`,
`NewSealedEventRouter`, and `SagaOptions.Sealing`. Set an aggregate's subject with `SetSubject`
before its first committed save. Incoming protected records restore their owner.

A key scope is the exact tuple `(key_namespace, tenant, subject)`. It retains the tenant even in
silo and bridge tenancy modes, where storage keys may be bare. An absent subject selects a tenant
lifecycle scope; encrypted data writes require a subject. Tenant revocation covers both existing
and future subjects in that namespace and tenant.

An aggregate or saga's first committed protected state fixes its tenant and subject. Later events,
snapshots, state, and emitted commands must preserve that ownership. There is no per-event subject
override within a protected aggregate. A saga cannot switch subjects by receiving a later event.

## Envelope and key generations

The field formerly proposed as `kid` is named `key_id`. The protocol uses descriptive names:

| Name | Meaning |
|---|---|
| `subject` | Entity whose protected data must be erasable together |
| `key_namespace` | Configured security domain for deployments sharing a provider |
| `key_id` | Opaque identifier for one immutable key generation |
| `cipher` | Authenticated-encryption algorithm |
| `encryption` | Typed information needed to open the payload |
| `seal` / `open` | Encrypt and authenticate / authenticate and decrypt |
| `erase` | Revoke access, coordinate cleanup, destroy keys, and track completion |

`EncryptionInfo` contains `format_version`, `key_namespace`, `key_id`, `cipher`, and `nonce`.
The current encryption format version is 1. A message's separate `version` describes its
payload schema; upcasting that schema does not change the encryption format.

Messages keep their existing envelope. There is no extra outer wrapper and no `edax.kid` or
`edax.cipher` extension. This JSON shows the structure; the byte placeholders are not test vectors:

```json
{
  "id": "evt_01",
  "aggregate_id": "customer_42",
  "tenant": "org_7",
  "subject": "person_42",
  "sequence": 3,
  "name": "CustomerAddressChanged",
  "version": 1,
  "correlation_id": "flow_01",
  "causation_id": "cmd_01",
  "occurred_at": null,
  "recorded_at": 1792022400000000000,
  "extensions": {},
  "metadata": {},
  "encryption": {
    "format_version": 1,
    "key_namespace": "production-eu",
    "key_id": "key_opaque_generation",
    "cipher": "xchacha20-poly1305",
    "nonce": "<base64 nonce>"
  },
  "data": "<base64 ciphertext followed by the authentication tag>"
}
```

The codec or media type determines whether a message is an event or command. Authentication also
binds that kind. A missing historical `encryption` field decodes as `null`; a present record must
contain every required field. Sealing-enabled readers reject plaintext unless explicitly configured
for migration.

A `key_id` identifies one immutable key generation. It is public, opaque, and never interpreted
as a URL or provider address. It is neither key material nor the erasure subject. The configured
namespace is checked before a provider lookup. The use of a key identifier follows the distinction
between a key reference and key material in [RFC 7516 §4.1.6](https://www.rfc-editor.org/rfc/rfc7516.html#section-4.1.6);
the edax envelope is not JWE.

Rewrapping a provider's stored key preserves `key_id` and its usage count. Generating a different
data key creates a new `key_id`. Raw keys, wrapped keys, credentials, and wrapping-key references
never travel in message envelopes. Production providers keep wrapped material and enforce access
to it separately.

The kernel constructs associated authenticated data (AAD) from the finalized envelope. It binds
every encryption field, message kind, identity, ownership, payload type/version, lineage, producer
time, and both context maps. Events also bind sequence; commands bind their idempotency key.
It excludes store-assigned `recorded_at`, broker offsets, and transport headers.
The [binary contract](../kernels/seal.md#exact-associated-data-encoding) specifies lengths, integer
widths, optional tags, and map ordering. Authenticated fields cannot change after sealing.

| Cipher | Key | Nonce | Tag |
|---|---|---|---|
| `xchacha20-poly1305` (default) | 32 bytes | 24 bytes | 16 bytes |
| `aes-256-gcm` | 32 bytes | 12 bytes | 16 bytes |

Readers use the cipher recorded in each envelope, independently of the current writer setting.
Unknown ciphers, versions, malformed lengths, and authentication failures are errors; there is no
fallback to plaintext. JSON encodes data and nonce bytes as Base64; MessagePack preserves bytes.

The SDK requests a usage reservation before sealing and obtains a fresh OS-random nonce.
Reservations count even if persistence later fails. AES-GCM generations rotate before exceeding
**2^32 reservations across all writers using that key**, following the random-IV invocation bound
in [NIST SP 800-38D §8.3](https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf).
A provider must enforce the count atomically across its clients. Rotation retains historical keys
for reads; erasure destroys every generation, including the separate fingerprint key.
Never reuse a nonce with its key. The low-level explicit-nonce primitive exists for interoperability
and vectors, not as a normal application nonce policy.

## Reads, delivery, and retries

Authentication happens before payload decoding or upcasting. Middleware, event idempotency guards,
and transport paths retain the raw ciphertext envelope. Handlers receive an opened view; that view
does not replace the raw message in the outbox, broker, or retry path.

Direct reads and direct command execution raise `Shredded` after confirmed destruction. Only the
trusted delivery opener converts provider-confirmed destruction into a consumed skip. Python's
`CommandBus.deliver_message` and Go's `Deliver` use that path; Python `dispatch_message` and Go
`Execute` remain direct execution. A `Shredded` raised by application handler code is not mistaken
for a successful opening skip. Python's `write_fence="transaction"` is the one exception, described
under "Fence writes through commit".

In Rust, use `CommandBus::deliver` for consumer delivery and `dispatch_message` for direct calls.
`SagaRunner::command_delivery` likewise returns `None` only for a trusted destroyed-scope decision.

| Provider condition | Read behavior |
|---|---|
| Active exact scope, purpose, cipher, and key ID | Authenticate and open |
| Unknown scope or missing generation | Fatal `KeyMissing`; never skip |
| Wrong owner or key purpose | Fatal `KeyUnauthorized`; never skip |
| Revocation awaiting destruction | `KeyRevoking`; uncounted contention retries |
| Provider outage | `KeyUnavailable`; uncounted contention retries |
| Exact referenced generation confirmed destroyed | `Shredded`; delivery opener may skip |

The retry kernel treats unavailable and revoking keys as contention. These retries have no attempt
limit. An offline cleanup participant can keep destruction pending and hold a Kafka partition
indefinitely. Revocation alone never permits a delivery skip. Providers must resolve exact ownership
and generation before reporting destruction; a scope-level status alone cannot justify dropping
an arbitrary message.

A repository retains the prepared event IDs, nonces, ciphertext, snapshot, and commands across
save retries. After a commit with a lost response, it reconciles the exact prepared batch against
stored events. It does not reseal that batch or accept mutation while it is prepared. This is
in-memory retry state, not a durable prepared-transaction log across process restarts.

## Snapshots, saga completion, and command results

Stored records reuse `EncryptionInfo` and the ciphertext-plus-tag representation:

| Record | Public fields | Protected content |
|---|---|---|
| Aggregate snapshot | Storage identity, tenant, subject, aggregate type, snapshot version, encryption information | Serialized aggregate state |
| Saga instance | Saga name, instance ID, tenant, subject, version, processing marks, completion marker, encryption information | Serialized application state |
| Command-ledger success | Claim identity, scope, claim owner, settlement state, keyed fingerprint, encryption information | Serialized handler result |
| Command-ledger failure | Claim identity, scope, claim owner, keyed fingerprint, stable error code | No arbitrary exception text |
| Erasure notification | `type="edax.erasure.requested"`, operation ID, scope containing namespace/tenant/subject | No application payload; separate authorized control route |
| Provider record | Scope, purpose, cipher, immutable key ID, lifecycle state, usage count | Provider-held secret material; production wrapping is provider-owned |

Saga processing marks identify consumed events and watermarks. They are separate from application
state. Stored-state authentication binds record kind, identity, ownership, type, version, and saga
marks/completion. Ledger-result authentication also binds the claim owner and full keyed fingerprint.

Snapshots carry encryption fields alongside their state bytes. Saga application state uses the
kernel's protected-state envelope. A completed protected
saga retains its owner and an authenticated `completed=true` marker with encrypted JSON `null`.
It is saved through the existing state-storage port. A backend may therefore carry it as a stored
running-state value, but the runner authenticates the marker before applying completed-instance
admission. Custom backends must preserve that envelope rather than replace it with an ownerless
plaintext tombstone. The protected format requires an explicit boolean `completed` field.
The Postgres kernel also sets its completion and retention columns from this marker.

Protected command idempotency uses a full HMAC-SHA256 fingerprint of the canonical command payload,
name, and schema version, under a dedicated key for the scope. It does not persist an unkeyed
plaintext hash. Data-key rotation does not change this fingerprint.

The adapter preserves the ordinary ledger claim key and uses its existing fence with a constant
outer hash of zero. Enabling protection therefore still encounters pre-migration results and
in-flight claims. It never creates a second claim by prefixing the key with the encryption scope.
The protected record authenticates that scope; reusing the same claim under another scope fails
closed. While a claim is pending, contenders remain `InProgress`; full payload comparison happens against the settled
protected record. Success stores an encrypted result, including JSON `null`. Replay verifies
the full fingerprint and scope, resolves the exact result key, authenticates the result, and checks
that the scope remains active before returning decoded data. Legacy or malformed completed
records fail closed.

Failures persist only their shared error code. Ordinary idempotency settlement also no longer
persists arbitrary exception messages. Ordinary ledgers retain validated rejection reason names
from the host's fixed vocabulary; protected ledgers omit those reasons too. Keep personal data out
of reason names. Kafka dead letters use code-only error information and
remove all previous dead-letter error fields; sealed saga-command failures omit exception text.
Ordinary saga failures retain exception text. Application
logs, observers, custom metadata, and external error sinks still need their own privacy policy.

Secret destruction does not guarantee that every process-memory copy is zeroized. Rust's
`KeyMaterial` zeroizes its owned buffer. Go clears operation-owned key slices, but host and native
transfers can leave other copies. Python immutable bytes cannot be deterministically cleared.
Already returned keys or plaintext cannot be recalled. Keep keys out of diagnostics and avoid
retaining them beyond the operation. A Kafka batch runner treats one batch as one operation: its
`Sealing.for_batch` view reads each subject's keys once, under the operation deadline, and drops
them with the batch. Its commit's write fence still reads the status afresh.

## Fence writes through commit

Revoking access to keys does not recall plaintext or keys already returned to a writer.
Every destination must prevent a writer that started earlier from recreating erased data after
cleanup.

The provider's Python `guard(scope)` or Go `Hold(ctx, scope)` contract fences writes through
transaction commit or rollback. SDK-managed transaction resources retain the guard until that
boundary. The memory implementation serializes only participating writers in one process.
It is not a distributed transaction fence. A production implementation must coordinate with the
actual destination transaction, not merely lock a remote key-service request.

Python's `Sealing(..., write_fence="transaction")` moves the fence inside the transaction that
writes. It needs a provider with `transactor` and `fence(tx, scopes)`, the `TransactionFence`
protocol. A write guard then admits nothing on its own. Each edax transaction that opens while it is
entered fences the scope. On the provider's database the lock goes inside the transaction. A
transaction on another database, or one another task opened, holds `guard` through its commit. An edax store writing outside any unit
of work fences its own short transaction the same way. A Redis or memory store, which has no
transaction to fence, holds `guard` through the write. Deletes take no fence: removing a row never
brings erased data back. The lock shares the write's connection, so a dropped connection takes the
write with it. A handler that writes nothing pays no fence. Writes through connections edax does
not manage, and side effects outside storage, are not fenced in this mode. When a fence refuses
inside a handler, a consumer skips the message only if the provider confirms the scope destroyed. A Postgres `fence`
takes the kernel's locks for all its scopes in one call and reads the status in a separate statement.
The locks are hash buckets, at most 32 tenant and 32 subject buckets per transaction, so a batch that
writes for many subjects cannot fill the server's shared lock table. A cleanup waits for every writer
in its bucket, so a collision delays unrelated writes; the status read stays exact.
`PostgresErasureStore.guard_in_transaction` does both against the edax erasure tables. It refuses a
tombstoned scope with `Rejected`, so a provider maps that refusal to its own key status.

Python's reference guard permits nesting within an admitted operation. Structured child tasks in
one managed transaction share its guard lifetime; independent operations wait. Revoking or destroying
from the admitted operation raises `BadHandle`. Go callers must propagate
the guard's returned context through nested SDK calls and must not share one guard across
concurrent goroutines.

The Python `PostgresErasureStore` provides durable workflow rows, control notifications, and local
destination tombstones; it stores no keys. Its `guard_in_transaction(connection, scope)` must use
the **same connection and READ COMMITTED transaction** as the protected application write.
`revoke_in_transaction` installs the destination fence; delete the destination's plaintext in that
transaction before acknowledging cleanup. `writer_guard` opens a guarded transaction for callers
that need one. Applying its DDL and connecting provider guards to application storage are explicit
integration work. An unrelated database lock cannot fence another service's writes.

The reference Rust SDK rejects sealed `Repository::save_in`: a borrowed transaction would commit
after the provider guard was released. Use `Repository::save`. Protected Rust sagas use
`SagaRunner::new_sealed` with a `SagaStore<State = serde_json::Value>`; the application state
remains typed and serializable. Use `run_committed`, which owns the commit boundary; sealed
`run` with a borrowed transaction and ambient transactions in `run_committed` are rejected.

## Erasure workflow and recovery

`ErasureCoordinator` persists an operation before external effects. Its scope, request ID,
operation ID, and required participant set are immutable. Repeating a request resumes that
operation; changing its participant set is an error.

| Durable stage | Confirmation required |
|---|---|
| `persisted` | Initial operation stored |
| `revoked` | Provider confirms permanent revocation |
| `notified` | Stable control notification atomically enqueued with this transition |
| `destruction_confirmed` | Every required participant acknowledged; provider confirms all keys destroyed |
| `complete` | Final workflow transition persisted |

A participant acknowledgement means its destination fence is installed, earlier writers have
drained, plaintext cleanup is finished, and its backup/restore obligations are satisfied. The
coordinator trusts that acknowledgement; it cannot inspect every external copy. Cleanup callbacks
must be idempotent because a crash can repeat an effect before its confirmation was saved.
Every registered participant must be in the required set. Required participants without local
callbacks are valid; remote workers can acknowledge them after completing their obligations.

This example deliberately leaves an offline participant pending:

```python
from edax.erasure import ErasureCoordinator, MemoryErasureStore

operations = MemoryErasureStore()  # Process-local test implementation.
eraser = ErasureCoordinator(keys, operations, {"search-index": None})
operation = await eraser.erase(subject_scope, "erase-request-1")
assert operation.stage == "notified"
# A worker later resumes with an idempotent cleanup callback, or an authorized
# controller acknowledges only after verifying the participant's cleanup.
```

Use `status`, `resume`, and `acknowledge` to observe and advance existing work. A cleanup failure
does not prevent later participants from running. The coordinator persists each successful
acknowledgement, then propagates the first cleanup error unchanged. Provider and storage failures
also propagate. Missing callbacks and delayed destruction leave the operation pending. No failure
text is stored in the operation.

Recover unfinished operations in pages: Python uses `pending(limit, after=operation_id)`, Rust
`pending_after(limit, Some(operation_id))`, and Go `PendingAfter(ctx, limit, &operationID)`.
Advance the cursor to the last returned operation ID even if that operation cannot finish.
After an empty page, reset it to `None` or `nil` and start another pass. This revisits blocked work
and finds new IDs inserted before the cursor. Repeatedly reading only the first page can starve
later operations behind an offline participant.

`notified` means durably enqueued, not delivered. Relay `unpublished` notifications unchanged on
a separately authorized control path, then call `mark_published` after broker acceptance.
Deduplicate by operation ID. The control path must work after subject or tenant payload-key
revocation; do not encrypt it under the keys being erased.

### Ingestion and projection stores

Ingestion sinks and projection state stores keep a subject's `private` part sealed by default, so
destroying the key erases it, backups included. A target built with `at_rest="plaintext"` keeps it
readable instead, and needs a cleanup participant. The sink's or store's `cleanup()` returns one, a
`ScopeCleanup`:

```python
cleanup = sink.cleanup(erasure_store)  # Postgres: revoke and delete in one transaction
eraser = ErasureCoordinator(keys, operations, {"orders-ingestion": cleanup})
```

A subject scope deletes that subject's rows; a tenant scope deletes every row of the tenant,
unsealed rows included. Rejections and projection receipts stay: they hold no plaintext of the
subject. Given the `PostgresErasureStore` of a `write_fence="transaction"` provider, the Postgres
cleanup revokes the scope in its own transaction before the delete. A fenced writer therefore either
committed before it or is refused after it. Memory cleanup only deletes. The provider's revoke
already drained the guarded writers.

Edax does not check that a plaintext target's cleanup is registered. `cleanup.resweep(operations)`
deletes again for every scope the erasure store has revoked, and returns how many rows it found. Run
it on a schedule or as a deploy check, and alert when it reports more than zero. That count means a
missing participant, or a restore that brought rows back. It covers a restore only when the erasure
store outlives the restored database. The coordinator can report `complete` before the next sweep.

Completion describes the frozen participant set and the provider's confirmations. It is not proof
that an unregistered export, old backup, compromised key copy, or unfenced service has disappeared.
Production providers must preserve tombstones through backup restoration and include all wrapped
keys and recovery copies in their destruction policy. Retaining public metadata is deliberate.
[NIST SP 800-88 Rev. 2](https://csrc.nist.gov/pubs/sp/800/88/r2/final) places cryptographic erase
within a sanitization program; using this workflow alone is not a certification or universal
deletion guarantee.

## Migrate readers before writers

1. Inventory plaintext payloads, state, result ledgers, failure records, exports, and backups.
   Choose immutable owners and a durable provider before writing production ciphertext.
2. Deploy readers that understand the typed encryption fields, both supported ciphers, and protected
   state and ledger formats. If historical plaintext must remain readable, explicitly set
   `allow_plaintext=True` during migration; the default is strict rejection.
   Older readers ignore the new `encryption` field and pass ciphertext to their plaintext codec,
   so deploy compatible readers before enabling any sealed writer.
3. Install destination fences, durable workflow storage, control routing, and cleanup participants.
   Exercise recovery before enabling encrypted writers.
4. Enable sealing on writers and every relevant reader. Writers always seal even when legacy
   plaintext reads are allowed. Keep old cipher generations available until their data is erased.
5. Migrate or retire legacy state and completed ledger records deliberately. Enabling a reader does
   not encrypt old bytes or scrub historical failure text. Protected replay will not silently accept
   an ordinary completed record.
6. Disable plaintext reads after the historical-data policy has been satisfied. Preserve that policy
   and permanent revocations during rollback and restore.

Legacy plaintext compatibility still checks permanent scope and tenant revocations. Restoring old
plaintext must not restore access to an erased scope. Changing a writer cipher does not require
rewriting historical ciphertext.

## Conformance evidence

The [kernel reference](../kernels/seal.md#conformance-tests) lists runnable cipher, provider, workflow,
and SDK tests. The memory suites demonstrate contracts, not durable provider behavior.
For a production provider, repeat exact-scope lookup, concurrent reservation/rotation, revocation,
historical-key destruction, and restart/restore scenarios against that provider.

For each cleanup participant, exercise a paused writer through commit, erasure racing a future
writer, an offline participant, repeated callbacks, and recovery after every persisted stage.
The [live Postgres tests](../../crates/edax-py/tests/test_erasure_postgres.py) cover those destination
fences and durable notifications for the supplied Postgres adapter. Other destinations need their
own evidence. SDKs keep no key cache beyond one batch, but that does not erase plaintext already
retained by application code or guarantee deterministic wiping of Python's immutable bytes.

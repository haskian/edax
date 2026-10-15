# Crypto-shredding verification

This page records the implementation review and verification completed on 2026-10-15.
Use the [crypto-shredding guide](crypto-shredding.md) for configuration and migration, and the
[seal kernel reference](../kernels/seal.md) for exact formats and authenticated bytes.

The implementation covers Rust kernels, the synchronous ABI, generated Python/Go bindings, three
SDKs, memory providers and operation stores, and Python's Postgres erasure adapter.
Production key providers and application cleanup participants remain deployment responsibilities.

## Coverage and executable evidence

| Contract | Existing tests and fixtures |
|---|---|
| Both ciphers match independent vectors; wrong key, nonce, tag, ciphertext, and context fail authentication | [Cipher tests](../../crates/edax-seal/tests/seal.rs) |
| Exact JSON/MessagePack bytes; absent/null encryption; malformed partial records; nonce and tag lengths | [Envelope goldens](../../testdata/golden/envelopes.json), [decode cases](../../testdata/golden/envelope_decode.json), [event golden runner](../../crates/edax-event/tests/golden.rs) |
| Message kind, tenant, namespace, subject, identity, version, and context bind ciphertext to its record | [Message/state tests](../../crates/edax-event/tests/sealing.rs), [sealing goldens](../../testdata/golden/sealing.json) |
| State swaps and completion tampering fail; completed sagas retain authenticated ownership | [State kernel tests](../../crates/edax-event/src/sealing.rs), [Python workflows](../../crates/edax-py/tests/test_crypto_workflows.py), [Go workflows](../../crates/edax-go/crypto_workflows_test.go) |
| Atomic usage reservation, budget rotation, historical reads, purpose separation, tenant revocation, and all-generation destruction | [Provider tests](../../crates/edax-seal/tests/keys.rs) |
| Command replay survives fresh encryption and data-key rotation; legacy claims cannot cause duplicate execution | [CQRS tests](../../crates/edax-cqrs/tests/protected.rs), [protected goldens](../../testdata/golden/protected_ledger.json), [Python ledger tests](../../crates/edax-py/tests/test_protected_ledger.py) |
| Strict plaintext policy, restored ownership, prepared-write reconciliation, sealed compensation, and ciphertext in persisted workflow records | [Rust SDK tests](../../crates/edax/src/sealing.rs), [Python workflows](../../crates/edax-py/tests/test_crypto_workflows.py), [Go workflows](../../crates/edax-go/crypto_workflows_test.go) |
| Middleware cannot substitute authenticated envelopes; raw ciphertext stays separate from handler views | [Python commands](../../crates/edax-py/tests/test_command.py), [Python workflows](../../crates/edax-py/tests/test_crypto_workflows.py), [Go sealing tests](../../crates/edax-go/sealing_test.go) |
| Failure records omit sensitive exception text; Kafka values and dead-letter headers preserve protected payloads | [Settlement goldens](../../testdata/golden/idempotency_settle.json), [Kafka goldens](../../testdata/golden/kafka_dead_letter.json), [Kafka adapter tests](../../crates/edax-py/tests/test_kafka.py), [live Kafka tests](../../crates/edax-py/tests/test_kafka_live.py) |
| Paused writers drain before cleanup; cancellation releases guards; expired contexts cannot admit work; nested guards avoid deadlocks | [Python workflows](../../crates/edax-py/tests/test_crypto_workflows.py), [Go sealing tests](../../crates/edax-go/sealing_test.go), [live Postgres erasure tests](../../crates/edax-py/tests/test_erasure_postgres.py) |
| Every workflow stage recovers after a lost response; cleanup failures do not starve healthy participants; offline participants and delayed destruction remain pending | [Kernel workflow tests](../../crates/edax-event/tests/erasure.rs), [Rust coordinator](../../crates/edax/src/erasure.rs), [Python coordinator](../../crates/edax-py/tests/test_erasure.py), [Go coordinator](../../crates/edax-go/erasure_test.go) |
| Postgres stores encrypted rows and protected saga completion; schema, SQL parameters, outbox updates, and destination fences agree | [Row goldens](../../testdata/golden/postgres_rows.json), [live reference checks](../../crates/edax-postgres/live-check/tests/coordination.rs), [Python Postgres tests](../../crates/edax-py/tests/test_postgres.py), [erasure tests](../../crates/edax-py/tests/test_erasure_postgres.py) |

These tests exercise specific failure paths. Fixture scans check for known plaintext in persisted
records; they do not establish that arbitrary application metadata, logs, or external systems are
free of personal data.

## Review decisions

The implementation review contained 49 inline findings and five additional findings.
The numbers below retain their order from that review. Related findings are grouped so each
row describes the resulting behavior rather than the editing history.

| Findings | Resolution |
|---|---|
| 1, 4, 9 | Preserve ordinary command claim keys during migration. Encode scopes in the kernel and pin fingerprints, records, and replay across SDKs with golden vectors. |
| 2, 3, 18, 21 | Attempt all pending cleanup callbacks, persist successful acknowledgements, paginate recovery, and refuse changed or incomplete required-participant sets. |
| 5 | Retain unbounded contention for `KeyRevoking`. Skipping before confirmed destruction would violate delivery semantics. Ordered Kafka partitions can remain blocked. |
| 6, 44, 48 | Provide explicit consumer delivery entry points. Direct calls still raise `Shredded`; unconfigured routers reject encrypted input before handlers. |
| 7, 36, 50 | Require saga ownership before effects, propagate subjects to commands, and test protected compensation and replay in all three SDKs. |
| 8, 12, 13, 14, 42 | Permit explicit ownerless legacy command migration where supported. Validate persisted tenant/owner, restore subjects from events, preserve unsealed Go tenancy behavior, and ignore wrong-type snapshots before opening. |
| 10, 20, 22, 23, 29, 30, 31, 32 | Keep cipher defaults, lengths, scopes, lifecycle/action names, and record versions in their owning kernels. Export those decisions through generated bindings. |
| 11, 17, 39, 53 | Derive Postgres saga completion from the protected marker. Remove the JSONB state parse, test real sealed row layouts, and check reference DDL against the kernel. |
| 15, 16 | Retain one authenticated opening through command/router dispatch. Python and Go can reuse it for nested sagas with the same `Sealing` instance. Rust nested saga processing may open again. |
| 19, 26, 40 | Distinguish workflow misuse from malformed records. Require the protected completion field and expand malformed-envelope fixtures. |
| 24, 27, 28, 33, 34, 35 | Remove unused ABI functions and ciphertext copies. Index current key purposes, disable unused dependency randomness features, reduce Go secret copies, and redact key reservations. |
| 25, 37, 51 | Persist stable failure codes. Retain existing message arguments for source compatibility but ignore them in settlement/dead-letter construction. Preserve the shared `<code>: <message>` wire format. |
| 38, 41, 52, 54 | Correct field/module tables, message value semantics, deployment order, and CQRS responsibility descriptions. Keep `EncryptionInfo` value semantics in `message.py` to avoid a circular import. |
| 43, 49 | Serialize Rust aggregate state only when a snapshot is due. Avoid unused command-signature encoding on unsealed Python saves. |
| 45, 46, 47 | Reject guarded revoke/destroy reentry, preserve decorated memory-provider fence identity, coordinate Python child-task admission, and reject unsafe borrowed transactions before writes. |

Additional review found authenticated-envelope substitution in Python middleware and mutable Go
handler views. Both now compare immutable encoded originals before trusting an opened view.
Go guard admission checks transaction ancestry before and after provider I/O, including saved
contexts from settled inner transactions.

Python guard ownership now distinguishes managed child tasks from independent operations.
Contended siblings share pending admission; cancellation wakes waiters. The review also corrected
live Postgres callers using the old saga parameter layout and Python fixture reconstruction of
typed encryption records.

The later simplification pass extracted Python's cleanup loop, replaced Go's conflict flag with a
direct retry, and reused the validated Go key scope. Existing tests passed without changes.

## Recorded verification

These are dated run results, not a fixed test-count requirement. Both runs rebuilt the native
libraries before the full Python and Go suites. Skipped tests are listed separately from passes.

| Run | Results | Limits |
|---|---|---|
| Hardening review, 2026-10-15 | Rust: 525 passed. Python: 2,469 passed. Go: all packages passed `go test -race ./...`. | Rust ignored one existing illustrative doctest. Python skipped the live Redis module and 14 transaction-join contracts for stores declaring `joins_transaction=False`. |
| Live integrations during hardening | Postgres reference: 6 coordination and 8 differential/schema tests passed. Python ran live Postgres and isolated Kafka, including broker pause/restart. | These cover the supplied adapters and test deployments. |
| Static and generated checks during hardening | Clippy with warnings denied, Ruff, formatting, ty, strict mypy, stubtest, facade generator tests, documentation lint, and binding regeneration passed. CQRS alternate-feature tests: 5 passed. | Generated bindings were rebuilt through the prescribed tasks. |
| Simplification pass, 2026-10-15 | Rust: 525 passed, one ignored doctest. Python: 2,460 passed, 16 skipped. Go: all packages passed uncached race tests. Type and formatting checks passed. | Live Postgres ran. Live Kafka and Redis were skipped, along with the 14 nonjoining-ledger contracts. |

## Reproduce the checks

From the repository root, with the Python test environment installed:

```sh
mise exec -- cargo test --workspace
mise exec -- cargo clippy --workspace --all-targets -- -D warnings
mise exec -- cargo test --manifest-path crates/edax-cqrs/feature-check/Cargo.toml

mise run bindings:generate
(cd crates/edax-py && VIRTUAL_ENV="$PWD/.venv" mise exec -- maturin develop)
(cd crates/edax-py && .venv/bin/python -m pytest -q -ra)

ALEF_EXPORT_GENERATED_HEADERS=1 mise run go:build
(cd crates/edax-go && mise exec -- go test -race -count=1 ./...)
mise run docs:lint
```

Inspect regenerated output for unintended changes. Kernel tests alone do not exercise the bindings;
pytest alone does not rebuild its native module.

Set `EDAX_POSTGRES_URL` to a disposable database and `EDAX_REQUIRE_POSTGRES=1` before the Python run.
Run the SQL reference checks with the same URL:

```sh
mise exec -- cargo test --manifest-path crates/edax-postgres/live-check/Cargo.toml -- --test-threads=1
```

For Kafka, set `EDAX_KAFKA_BOOTSTRAP` and `EDAX_KAFKA_CONTAINER` to an isolated test broker.
The container variable enables pause/restart fault tests. Use a disposable broker: those tests
interrupt it. Configure `EDAX_REDIS_URL` and `EDAX_REDIS_CONTAINER` to include live Redis and its
pause tests. See [testing](testing.md) and the repository [build rules](../../AGENTS.md) for setup.

## Deployment evidence still required

Run the same contracts against the production provider and each cleanup destination. Verify atomic
reservation across writers, historical-key destruction, tenant revocation racing new subjects, and
permanent tombstones across restart and restore. Confirm destruction of wrapped keys and recovery
copies; a successful deletion request is insufficient.

Each participant must prove that cleanup fences new writes, drains admitted work, deletes plaintext,
and satisfies its backup obligations before acknowledgement. Test an offline participant and a
crash after each durable workflow transition. The reference tests are executable examples; they
are not a provider-independent certification harness.

Completion covers the declared participant set and provider confirmations. It cannot account for
unregistered exports or retained application plaintext. The memory provider loses keys and tombstones
on restart. SDKs do not cache reusable keys, but runtime and FFI copies can survive buffer clearing.
The [operational limits](crypto-shredding.md) remain part of the deployment contract.

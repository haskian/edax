# Brokerless ingestion

Implement an additive path over the existing ingestion kernel. Keep Kafka's persisted source keys and `commit()` contract unchanged. No websocket client, timeseries query engine, or fan-out.

1. Add kernel-owned opaque source IDs and Python input construction; replay new vectors through Rust/Python/Go. Source identity locates an original/rejection; append event identity and upsert owner/sequence retain their existing replay semantics. Command idempotency can additionally bind request identity to its result.
2. Add explicit Postgres `stage()` inside an existing owned transaction. Return a distinct provisional result. Reuse SQL application and preserve manifest rollback, task/transaction identity checks, and scope fences until commit/rollback.
3. Publish a kernel-backed planning interface and reusable sink conformance suites. Run the suites against memory, Postgres, and an application-owned example where feasible; include replay, conflict, rejection, ordering, concurrent writes, and staged rollback/isolation.
4. Provide a brokerless keyed RecordPulse example and document gateway normalization, stable IDs, plaintext query/erasure boundaries, retention and custom storage obligations.
5. Regenerate bindings/headers, rebuild native libraries, and run Rust, Python 3.10/3.13, Go race and static checks. Commit and push the completed feature.

## Completed implementation

- Opaque source IDs and key-derived write scopes live in `edax-cqrs`, with Rust/Python/Go golden replay. The ABI transports the identity as JSON so a NUL-containing source ID reaches the same kernel check across C and Python boundaries. Existing Kafka source keys are unchanged.
- Postgres staging shares the owned commit's SQL application. Results are provisional; caught staging failures after handle admission abort the transaction. Explicit transactional reads check the manifest. A rolled-back bind is never cached.
- Direct preparation snapshots inputs, derives scopes from kernel keys, fingerprints plaintext before encryption, and retains configured fences through the appropriate transaction boundary. Public planning types and sink conformance suites cover application-owned storage integration. The suites run against memory and Postgres; public planning is also tested independently without private ABI calls.
- Example 22 uses a keyed `RecordPulse`, a shared application counter and a Postgres ledger. It verifies duplicate replay, payload mismatch, rollback after staging, and successful retry. Documentation covers gateway normalization, raw rejection retention, public/private SQL limits and custom partitioned-storage obligations.

## Verification

- Rust workspace: 576 passed. Clippy and rustfmt passed.
- Postgres kernel live checks: 16 passed.
- Fresh generated bindings, exported C headers and rebuilt Python/Go native libraries. Go race suite passed, including the new golden cases.
- Python 3.13 against live Postgres, Redis and Kafka, including container pause/restart tests: 3,274 passed, 21 skipped. The initial run had two Kafka CLI setup timeouts from the temporary container's mismatched internal/external ports; the corrected run passed.
- Python 3.10 against live Postgres, Redis and Kafka: 3,266 passed, 29 skipped. This full run reported one non-failing PyO3 warning: an unsendable `Retries` handle was finalized on another thread. The brokerless example and the final 19 direct-ingestion tests passed separately with warnings treated as errors. The warning is recorded rather than suppressed; generated handle finalization is outside this feature.
- The final direct-ingestion tests also passed on 3.13 (19 tests, including a final explicit-transaction ownership guard added after full-suite collection).
- Sixteen concurrent staged/owned writers in each mode produce exactly one accepted value and 15 durable rejections. Tests also verify cancellation rollback, manifest rebinding after rollback, forged host scope metadata, and guard-based/database-based fences through outer transaction completion.
- Ruff, format checks, ty, strict mypy, native stubtest, documentation lint and examples 20–22 passed.

No websocket runner, time-series query layer, retention scheduler, fan-out, or separate accepted-source receipt ledger was added. Those responsibilities remain as described in the design above.

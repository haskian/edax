# PR 24 remediation plan

Baseline: `e900577790f1b1c6b628de07817e4ecb15ede374`, against `origin/main` at `4401f2297840f804e67614022ec0579df412125f`.

Sources: [this session's review](../.context/PR-review.md) and [linked session's review](../.context/review/pr24/SUMMARY.md), session `36fdf466-4d7d-42a1-96d7-31b357a69e7c`. Detailed dispositions and reproduced evidence live in [merged-review](../.context/merged-review/). The user authorized implementation, commit and push after this plan is made. No branch rename or history rewrite.

The final dispositions are preserved in [review-findings.md](review-findings.md).

## Required guarantees

- A malformed original becomes one durable rejection without stopping healthy work. Uncertain commits retry the same input; only proven rollback permits isolation. Offset settlement never invents rollback evidence.
- Projection exhaustion stops the affected partition; ingestion retains its documented whole-runner stop on exhausted or fatal work. Rebalances, empty fetches, key contention and host cancellation cannot reset attempt ceilings, lose work, acknowledge uncommitted offsets, or stall unrelated partitions.
- Subject and tenant revocation exclude late writes. Cleanup is bound to the configured key namespace and the explicitly registered generation. Completed erasure blocks delivery of every intent, including a JSON null payload.
- Per-aggregate delivery preserves order while unrelated aggregates progress with bounded pages and memory. Skipped work remains revisitable.
- Original numeric tokens are checked before lossy parsing. Memory and Postgres preserve equal values. Python and Go use kernel decisions and replay the same contract.

## Decisions and limits

Keep the existing single `payload_hash` contract. Document the non-cryptographic unkeyed replay limitation; do not silently migrate persisted fingerprints. Keyed sealed fingerprints remain HMAC-SHA-256. A speculative hash collision does not justify an incompatible global algorithm migration in this repair.

Keep bounded Postgres fence buckets. The old exact-scope protocol and the new bucket protocol cannot safely overlap. Require a stop/drain upgrade for all writers and erasers, including open transactions, before resuming the new version. Do not add unbounded legacy locks to every batch and recreate lock-table exhaustion.

Retained generations require separate named cleanup participants, registered before an erasure freezes its participant set. Do not scan or erase arbitrary namespaces/generations. Subjectless public state is supported; tenant erasure must fence and delete it, but this repair does not invent tenant-only encryption.

No heuristic stops a stream just because several records have the same SQL error: that cannot distinguish bad data from a host trigger. Preserve the explicit error/rollback contract. Document host schema responsibilities.

## Ordered tasks

Each task includes focused regression tests. Generated outputs do not count as separately maintained files. Independent tasks may run together, but each source file has one owner.

| ID | Task / likely files | Acceptance and verification | Dependencies | Scope |
|---|---|---|---|---|
| T01 | Ingestion identity and rejection boundary: `edax-cqrs/src/{ingest,namespace}.rs`, tests | Empty/oversize/NUL producer identity cannot wedge runner; optional tenants obey the same nonempty/64-byte rule as erasure scopes; custom keys cannot bypass subject/tenant rules; diagnostics cannot carry NUL or unbounded host text. Rust unit/golden tests plus adapter poison tests. | None | M |
| T02 | Prepared-memory overflow: `edax-event/src/batch.rs`, kernel tests, `kafka_ingest.py` | An allowed input exceeding its serialized reserve becomes a bounded rejection without exceeding credits; true configuration/size arithmetic failures remain fatal. Compact-wire/escaped-content regression. | T01 | M |
| T03 | Ingestion lifecycle: `_kafka.py`, `kafka_ingest.py`, recovery tests | In-flight commits survive routine revoke/latency within deadline; idle leave triggers polling/rejoin; assignment failure is visible; offset-flush failure cannot recover already-settled inputs. Real broker + deterministic barrier tests. | T01–T02 for poison tests | M |
| T04 | Projection runner ownership: `kafka_projection.py`, lifecycle tests | Stop owns/cancels only its loop; external cancellation propagates; attempts survive empty fetch/seek; abandoned batches rewind; stale assignment/lease cannot mutate current state. Fake seek-empty and cancellation tests; broker rejoin check. | None | M |
| T05 | Bounded provider and fenced operations: `sealing.py`, Kafka runners, deadline tests | Entire fenced write is owned by a tracked task; fence remains held until actual completion; hung provider cannot stall healthy partitions; key unavailability is uncounted; no second concurrent commit for one partition. Cooperative and cancellation-suppressing provider/commit tests; repeated deadlines must not create unbounded unresolved child tasks. | T03–T04 | M |
| T06 | Namespace-bound cleanup: `erasure.py`, ingestion/projection cleanup factories | A cleanup refuses or ignores a foreign key namespace; resweep only touches its namespace; zero page limit is rejected consistently. Memory/PG namespace sentinel regression. | None | M |
| T07 | Tenant fence support: `sealing.py`, both runners, fence tests | Subjectless inputs acquire tenant revocation protection; mixed subjects sort safely; paused writer cannot recreate data after tenant erasure. Memory and transaction-fence barrier tests. | T05–T06 | M |
| T08 | Postgres cleanup progress and contention: `edax-postgres/src/{erasure,error,ingest}.rs`, erasure adapter | Cleanup uses bounded committed pages after revocation; retry advances after interruption; fence/row-lock contention is uncounted; status cannot complete before cleanup drains. Live Postgres timeout and concurrent-writer tests. | T06–T07 | M |
| T09 | Exact Postgres ingestion and memory parity: `ingest_postgres.py`, `ingestion.py`, tests | Preserve binary64 numeric value on insert/update; empty fingerprint is refused; pending limit and publication atomicity match PG. Exact-number, invalid-page and multi-key rollback tests. | T01 | M |
| T10 | Projection input contracts: `edax-cqrs/src/projection.rs`, ABI `cqrs.rs`, runner | Validate original payload/metadata numeric tokens before Value conversion; always compare decoded identity to envelope; invalid contract/subject/cursor is refused by kernel. Wide-integer, omitted-subject and stale-cursor regressions. | T04 | M |
| T11 | Intent ordering and lifecycle: `projection.py`, projection kernel, tests | Null payload cannot bypass revoked/destroyed scope; failed key lookup holds later effects for that aggregate only; no overtaking after retry. Memory and PG real-relay regressions. | T05,T10 | M |
| T12 | Bounded fair intent scan: memory/PG projection stores and kernel, `projection.py` | Blocked page cannot starve healthy aggregates; continuation is bounded, includes stable ordering and eventually revisits skipped rows; restart/concurrent marking/pruning remain safe. Require the reference aggregate-order index and bounded per-partition candidates; no full-backlog ranking. Page-size 1/100 multi-aggregate tests on both stores. | T11 | M |
| T13 | Shared sealed-state context: event sealing kernel/ABI, `sealing.py`, vectors | Kernel owns fixed context fields for ingestion/projection/intent; authenticated bytes and null payload plans have golden coverage; no ciphertext compatibility break. Existing ciphertext round-trip + tamper tests. | T10–T11 | M |
| T14 | ABI and configuration consistency: ABI helpers/event/kafka/postgres, tests | Width conversions cannot truncate admission counts; no duplicated wire code; projection config errors are fatal wiring errors; common JSON helpers preserve original-token checks. Rust ABI tests, regenerate bindings and stubtest. | T01,T10,T13 | M |
| T15 | Contract replay coverage: Rust vector tests, Python/Go readers | Both SDKs assert projection read keys and lock; Go replays every Kafka offset operation including generation commits; missing changed-branch vectors cover contention/retry and stale-owner/cutover/sequence/clock boundaries. Assert-mode all readers; regenerate only deliberate changes. | T01–T14 relevant kernel surfaces | L: execute as independent small test slices |
| T16 | Truthful docs/examples and migration gate: `AGENTS.md`, kernel/backend/SDK docs, examples | One canonical complete regen list; correct SQL verdict and lifecycle errors; explicit retained-generation, hash and stop/drain upgrade limits; examples respect public/private and atomic prepare/apply contract. Run examples and diff audit. | T01–T15 finalized behavior | L: docs then examples |
| T17 | Integration, adversarial review and release | Fresh native builds; Rust workspace/feature/live PG, Python full suite/live adapters, Go race and generated parity pass. Re-run original repros for accepted findings; audit diff for unintended changes; commit Conventional Commit and push current branch. | T01–T16 | M |

## Checkpoints and scheduling

1. Completed before implementation: three independent aspect reviewers reconciled every report ID, replayed the strongest new claims, and cross-reviewed another lane. Critiques and resolutions are in `.context/merged-review/plan-stress-test.md`.
2. After boundary/store slices (T01–T02, T06–T10): run focused Rust and Python tests with a freshly built native module. Verify no unscoped cleanup or numeric round trip.
3. After runner/delivery slices (T03–T05, T11–T13): run cancellation, rebalance, retry and concurrent erasure tests. Check pending task/fence lifetime after timeout, not only returned exceptions.
4. Before commit (T14–T17): run the complete validation below and review the final diff. Failures must be fixed or explicitly identified as environment blockers; no green claims from stale native artifacts.

Runtime adapters, storage/erasure, and projection/sealing are parallel ownership lanes. Parent owns ingestion kernel boundaries, ABI helper cleanup, contract replays and documentation. Shared ABI generation runs once after all source edits settle. Never run two generators against the same outputs.

## Final verification commands

```sh
mise exec -- cargo test --workspace
mise exec -- cargo test --manifest-path crates/edax-cqrs/feature-check/Cargo.toml
# EDAX_POSTGRES_URL points at an isolated throwaway Postgres server.
mise exec -- cargo test --manifest-path crates/edax-postgres/live-check/Cargo.toml -- --test-threads=1
mise run bindings:generate
# From crates/edax-py with its .venv active:
mise exec -- maturin develop
.venv/bin/python -m pytest -q
.venv/bin/python -m mypy.stubtest edax._edax --allowlist stubtest-allowlist.txt
# From repository root:
ALEF_EXPORT_GENERATED_HEADERS=1 mise run go:build
# From crates/edax-go:
mise exec -- go test -race ./...
```

Use real isolated Kafka/Postgres/Redis services for adapter and pause/rebalance tests. Confirm all generated changes derive from source and regeneration is idempotent. Record exact test counts, commands and environmental limits in `.context/merged-review/verification.md`. Commit and push only after the required checks and final review.

## Completed verification

All T01–T17 implementation and review gates passed on 2026-10-19. Independent runtime, storage and projection reviewers cross-checked the changes. The final adversarial pass found two more identity/scope boundary gaps; both were fixed and included in the fresh native builds below.

| Check | Result |
|---|---|
| Rust workspace | 575 passed; one intentionally ignored contract documentation example |
| Rust JSON feature combinations | 8 passed |
| Rust live Postgres differential checks | 16 passed |
| Python full suite with live Kafka, Redis and Postgres | 3,221 passed; 21 inapplicable transaction-join contracts skipped because those ledgers declare `joins_transaction = False` |
| Go `go test -race ./...` | All eight tested packages passed |
| Clippy, Ruff, formatting, ty, strict mypy and stubtest | Passed |
| Documentation lint | Zero problems in 45 files |
| Python and Go ingestion/projection examples | Passed |
| Binding generation | Repeated generation changed no tracked generated output; exported C headers and native libraries rebuilt |
| Delivery query performance | 100,000 intents across 1,000 aggregates; indexed pages of 1 and 100 completed under 1 ms on the local fixture |

The Python suite used isolated Kafka 3.8.0, Redis 8 and Postgres 17 containers, including pause/restart and concurrency tests. Native Python and Go libraries were rebuilt after the final kernel changes. The final Python run also covers shutdown while a broker commit outlives its cancelled waiter. Python 3.10 grammar and typing compatibility were checked; a separate Python 3.10 runtime was not exercised. Performance measurements describe this fixture, not a production latency guarantee.

Before deployment, provision the [aggregate delivery index](../docs/sdk/projection-processing.md#provision-and-initialize), then follow the [stop/drain fence-protocol upgrade](../docs/guides/crypto-shredding.md). Mixed old/new writers and erasers are unsupported. Register every retained generation before freezing erasure participants. The hash and subjectless-state limits in this plan remain deliberate contracts, with matching documentation.

Detailed local command logs, query plans and independent review reports remain in `.context/merged-review/`; the test results and deployment requirements above are tracked with the implementation.

## Python 3.10 CI follow-up

Run `39260275997` stopped reporting progress at 44% and remained in its test step for over 80 minutes. The stalled run was cancelled to recover its logs. Two deterministic regressions reproduced Python 3.10 lifecycle failures:

- An idle event completing concurrently with caller cancellation lets `asyncio.wait_for` absorb that cancellation. Both Kafka runners now share a bounded event wait based on `asyncio.wait`.
- Clearing traceback frame locals can close a suspended host coroutine still handling the same exception. Cleanup now detaches saved traceback references. Exception types, messages and cause relationships survive; saved traceback locations are deliberately discarded. Existing weak-reference tests still prove released payloads are collectible.

Both new regressions failed before the fix. Afterward, 66 focused tests passed under Python 3.10. Its full suite with live Kafka, Redis and Postgres passed: **3,214 passed, 29 skipped in 82.43 seconds**. This supersedes the earlier lack of Python 3.10 runtime verification. The local Python 3.13 suite also passed: **2,793 passed, 128 skipped** without live-service environment variables. Rust workspace tests (575), Go race tests (eight packages), Ruff, formatting, ty and strict mypy passed again.

CI now prints individual test names, emits a stack dump after a 60-second test stall and limits the test step to ten minutes. The stack dump diagnoses a stall; the step limit bounds it. No tests were removed or disabled.

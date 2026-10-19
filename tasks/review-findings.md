# Merged PR review findings

Baseline e900577. Source session: `36fdf466-4d7d-42a1-96d7-31b357a69e7c` in workspace `634d131b-d55c-4880-bd99-eb0a5c59b64f`. This ledger records disposition; [plan.md](plan.md) is the single execution plan. Local `.context` paths identify evidence retained in the review workspace.


The accepted changes are implemented. Final validation and deployment conditions are recorded in [plan.md](plan.md). The assessment sections below preserve the original evidence and decisions; their baseline references do not describe the repaired tree.

## Contract and cross-review disposition

Baseline e900577. The linked review's contract report was checked against the generators, ABI readers, docs and tests. Existing vectors pass; absent assertions and contradictory docs are separate from proven runtime failures.

| Linked contract ID | Disposition | Action |
|---|---|---|
| M1, L3 | Confirmed divergent regeneration lists | T16: one complete canonical AGENTS list, links elsewhere; correct EDAX_REGEN=1 behavior. |
| M2 | Confirmed stale SQL verdict docs | T16: update after new contention operations settle. |
| M3 | Confirmed missing SDK reads assertions | T15: assert keys+lock for every valid projection request, refusal for invalid requests. |
| M4 | Confirmed missing Go Kafka offset vector replay | T15: every operation including capture_due/committed_generation; unknown operation fails. |
| M5 | Real unkeyed noncryptographic limitation; adversarial collision not reproduced; mandated shared payload_hash is existing contract | T16: explicitly qualify exact-content claims and require keyed path for hostile producers; no incompatible hash migration. |
| M6 | Confirmed changed-branch coverage gaps | T15: stale refresh, lease/clock/epoch boundaries, sequence zero and activation variants in Rust vectors replayed by SDKs. |
| L1 | Confirmed incomplete vector inventory | T16: add missing rows and remove duplicative appended prose. |
| L2 | Confirmed false Go postgres-error coverage claim | T15/T16: add error replay if straightforward, otherwise truthfully identify Python coverage. |
| L4 | Confirmed memory architecture omission | T16: list projection_state and describe kernel staging accurately. |
| L5 | Confirmed projection config codec vs BadHandle inconsistency | T14: kernel configuration parse uses BadHandle, golden pins correction. |
| L6 | Confirmed dependency-owned base64 message pinned | T15: compare codec code for parser text, retain precise messages for kernel-owned failures. |
| L7 | Confirmed unpinned overflow/corrupt-store branches | T15: focused Rust cases, host-width-aware overflow test. |
| L8 | Confirmed missing new AAD kind vectors | T13: kernel context builder and golden byte checks. |
| L9 | Confirmed misleading subject-change message | T10: explicit subject-change error; update deliberate golden result. |
| L10 | Coverage/documentation overclaim, no observed feature divergence | T15/T16: extend feature-check for new canonical paths where compatible; describe exact covered suites. |
| L11 | Confirmed misleading example private/public use | T16: use public for subjectless order state, explicit at_rest/sealing guidance. |
| L12 | Limited vectors do not carry complete ABI fixtures; no demonstrated divergence | T15: add complete boundary fixtures rather than redesign all existing vectors. Existing minimal vectors remain valid kernel tests. |
| L13 | Confirmed incomplete transaction statement list | T16: distinguish fixed data statements from fence/catalog initialization calls. |
| L14 | Confirmed stale kernel record description | T16: list ingestion source/provenance responsibilities. |

## Both review provenance

All eleven confirmed correctness findings in `.context/PR-review.md` remain included: namespace=T06; old/new locks=T16 upgrade gate; tenant late-write=T07; wide integer=T10; numeric storage=T09; null intent=T11; sealed commit isolation=T05; delivery starvation=T12; provider deadline=T05; poison identity=T01–T03; NUL rejection=T01. Its P3 stale shutdown documentation joins T16.

The linked summary H1–H4 and M1–M14 are covered by the runtime/storage/projection assessment ledgers plus this ledger. Summary bullets are bundles and are not counted as independent bugs. Low findings map by their original report IDs, not an inflated duplicate count. Fixed-at-HEAD items are controls, not remediation tasks.

## Rejected unsafe remedies

- Resetting retries on any poll permits infinite poison retries. Reset only durable progress.
- Retrying after an unknown commit as if rollback occurred can duplicate/lose effects. Preserve uncertainty and ownership until completion.
- A raw source cursor for delivery can let a later sameaggregate intent overtake a blocked head. Rotate aggregate groups, always selecting each group's oldest retained prefix.
- Releasing a fence when a coroutine exceeds its deadline lets its late commit bypass erasure. The actual operation owns its fence through completion.
- Adding exact legacy locks to every new batched writer recreates unbounded advisory-lock pressure. The deployment must drain the old protocol before enabling the new one.
- Skipping the revocation step during resweep is unsafe after restoring a destination whose revocation tombstones were also restored. Reinstall/drain once per resweep call, then page deletes.
- Blind cross-generation cleanup can erase another target. Register every retained generation before participant freeze.
- A consecutive-SQL-error heuristic cannot distinguish legitimate bad records from invalid host schema. Keep rollback-aware isolation and document the schema contract.


## Runtime reconciliation

Reviewed current HEAD `e900577790f1b1c6b628de07817e4ecb15ede374`. Sources: linked review `pr24/kafka-ingest.md` (KI), `kafka-projection.md` (KP), `batch.md` (B), and `.context/PR-review.md`. No implementation edits in this assessment.

## Disposition ledger

| Source | Status | Evidence and disposition |
|---|---|---|
| KI1 | Confirmed | Re-ran `probe_revoke_during_flush.py` and `probe_commit_timeout_ends_run.py`: run exits with transient StoreError while broker commit subsequently succeeds. Suspend admission while interrupted commit owns serialization; bounded drain, then continue. Revoke must wait before sending its retired watermark. |
| KI2 | Confirmed | Saved live probe reports empty parts forever after idle leave. Installed aiokafka `subscription_state.fetch_context` and coordinator idle/rejoin code support exact mechanism. Add bounded paused keepalive fetches when no normal fetch can run, including empty assignment. Never consume/drop unbooked records. Apply to both runners. |
| KI3 | Confirmed coverage gap | Existing live suite uses manual assignment. Add real subscribed two-member join/revoke/rejoin regression plus fake seek-buffer invalidation and delayed commit. Covered by KI1/KI2, not independent product defect. |
| KI4 | Confirmed | Re-ran assignment failure probe: failure None, no partitions, run alive after listener error swallowed. Record classified callback failure in runner before re-raising. |
| KI5 | Confirmed | `getmany().result` and position bypass shared classified context. Wrap driver boundaries; do not classify host mapper/reducer exceptions as driver errors. |
| KI6 | Confirmed duplicate | Same documented BadHandle/actual transient StoreError finding as primary review. Fix docs with KI1. |
| KP H1 / B1 | Confirmed duplicate | Re-ran cold-after-seek client: 65 seeks without failure vs 3 seeks and stopped partition for warm fake. Reset retries only after durable progress; keep contention backoff across empty polls. |
| KP M1 | Confirmed | Re-ran slow-reserve probe: StoreError failure, 2 counted attempts, 0 contention. Store `_seal` uses raw Sealing rather than bounded batch keys. Store owner fixes shared key budget, including reserve/open, below operation deadline. Avoid solely rejecting equal defaults. |
| KP M2 | Confirmed duplicate | Re-ran sealed isolation probe: healthy partition progresses only without sealing. Move complete fence entry/commit/exit into one tracked operation task. No nested operation slot. Cancellation-suppressing commit retains fence until actual completion. |
| KP M3 | Confirmed | Re-ran stop probe: CancelledError reaches host on requested graceful stop. Own internal loop task; requested stop returns normally; external caller cancellation still propagates. wait_closed waits runner operations, not lifetime of caller's task. |
| KP M4 | Confirmed duplicate | Linked intent-order probe and store code skip first blocked same-aggregate intent then deliver later one. Projection store owner adds per-aggregate holdback with fair bounded scan. |
| KP L1 | Confirmed | Re-ran cancellation probe: cancelled run keeps committing. Whole fenced operation ownership removes inline cancellation absorber. Ingestion settle helper must preserve cancellation after actual operation release. |
| KP L2 | Confirmed defensive contract gap | Requires custom client violating aiokafka listener ordering, but current Protocol does not forbid it. Discarded fetched batch must set seek_needed before releasing data. Tiny same-boundary fix. |
| KP L3 | Confirmed defensive contract gap | Duplicate assignment replaces live owner. Match ingestion idempotent assignment behavior by preserving an existing live entry. |
| KP L4 | Confirmed | Postgres transaction classifies reducer ValueError as driver encoding CodecError while memory preserves host error. Projection store owner wraps/unwraps reducer exceptions across driver transaction boundary. |
| KP L5 | Confirmed limited impact | renewal scheduled after response, but durable expiry based on earlier DB clock. Schedule renewal from start of acquire/refresh/commit; no change lease safety/fencing. |
| KP L6 | Partly confirmed | Whole-batch replay/stop on poison is intentional atomic behavior, not bug; do not commit valid prefix opportunistically. Add topic/partition/offset context to preparation errors so repair is possible. |
| KP L7 | Confirmed documentation/example gap | Go example uses separate reads/commits despite transaction contract. Parent owns example correction; no observed corruption because apply revalidates. |
| B2 | Confirmed duplicate | Primary review invalid-key reproduction. Terminal failed input retries same invalid identity outside catch. Kernel must classify empty data key Codec, not wiring BadHandle; rejection builder resets all identity to kernel source fallback. Mapper returning wrong object remains fatal. |
| B3 | Confirmed duplicate | Primary live Postgres NUL rejection probe. Preparation diagnostics must use kernel bounded code-only rejection, not host exception text. Kernel validates direct failure originals too. |
| B4 | Confirmed | Re-ran compact-wire/escaped aggregate probe: canonical 1,027,844 B within configured 1,048,576; serialized 3,083,740 exceeds reserved cap 2,228,224; fatal BadHandle stops. Keep reserve bounded: classify overcharge as per-record Codec, rebuild source fallback bounded rejection, and charge that. Do not inflate every default allocation. |
| B5 | Confirmed | `_flush` inside sink recovery try sees already acknowledged originals. Separate broker flush from sink recovery once acknowledgements complete; never rewrite those attempts or re-run sink. |
| B6 | Confirmed coverage gap | Add terminal/transient uncertain nonisolable retry golden branches, contention waits=0, charge=0, overcharge as Codec. Parent kernel ownership. |
| B7 | Confirmed redundancy | Preparation has no effects, so explicit rolled_back=True/isolable=True kernel recovery supplies terminal reject diagnostic. Keep one kernel authority; no new unnecessary ABI helper if current batch_recover expresses it. |
| B8 | Not established as defect | Host trigger/check constraints may intentionally reject every invalid record. Identical SQLSTATE parent/child is expected for legitimate bad batches, so proposed stop heuristic would break bisection contract. Retain documented host schema contract; add no speculative heuristic. |
| B9 | Confirmed doc contradiction | Qualify no-rejection statement to transient preparation failure; terminal preparation failures become durable rejections. |

## Coherent implementation slices

1. **Preparation admission** (`kafka_ingest.py`, kernel/root coordination): shared source-fallback rejection builder; bounded kernel diagnostics; valid BadHandle wiring stays fatal; overcharge degrades to rejection. Tests empty/oversized key and tenant, NUL diagnostic, compact encoded oversized representation, healthy following original.
2. **Commit ownership** (`_kafka.py`, `kafka_ingest.py`): interrupted broker calls keep serialization until actual finish; admission waits within lifecycle deadline rather than marking transient state fatal. Retired commits wait on earlier call and cannot start after revoke deadline. Move flush outside sink recovery. Tests short in-flight revoke, slow commit eventual success, never-ending commit deadline, no stale retry entries/no duplicate sink call.
3. **Consumer activity and failure surfacing** (both runners): keepalive fetch uses normal client API under paused assignment and bounded lifetime; any returned record is rewound before another normal fetch. Preserve busy partitions' pauses. Empty-assignment outstanding fetch must allow rejoin and must not overlap normal fetch. Record listener failure and classify broker I/O. Tests idle leave/rejoin, all workers blocked, no admission room, late assignment/result, pause preservation, real group churn.
4. **Projection task ownership** (`kafka_projection.py`): own run loop task; one tracked whole fenced commit; track input opening too. Only reset retries on committed batch, renewal scheduled from operation start, idempotent assign, seek on discarded batch, source context in preparation errors. Tests equal healthy progress with/without sealing, guard-entry and resolve cancellation suppressors, external cancellation, stop/wait_closed under host awaits, cold-after-seek attempt ceiling, lease delay.
5. **Store dependencies** (other owner): bounded key operations retain unresolved lifetimes and release eventual exceptions; direct storage calls have bounded provider behavior; subject/tenant fences, ordering/fairness, decoder-envelope normalization. Runtime task ownership must not release guards/data while child I/O lives.

## Stress constraints for implementation review

- A timeout means unknown completion, never proof of rollback. No overlapping broker commit and no offset rewind while another fetch owns the position.
- Keepalive must not fetch into an unreserved application buffer. Pause existing assignments first, cap returned record count, immediately rewind unexpected newly assigned records, and preserve existing pause state.
- Cancellation must remain observable to caller while wait_closed remains able to observe retained operations. A stop request cancels only the internal loop.
- Kernel decisions and bounded diagnostics stay kernels; Python owns task/driver orchestration only.
- Live regression and full native-rebuilt Rust/Python/Go checks remain required after integration. Saved probes establish old failure; final tests must assert new successful behavior and negative lifetime bounds.

## Ownership

Runtime agent: `backend/kafka_ingest.py`, `backend/kafka_projection.py`, `backend/_kafka.py`, their focused tests and Kafka live runner tests. Parent: `edax-event::batch`, `edax-cqrs::ingest`, ABI helpers, golden vectors, docs/Go example unless delegated. Projection agent: `projection.py`, `sealing.py`, projection store tests. No commits from runtime agent.


## Storage reconciliation at e900577

Read current kernels/adapters and both reviews. Re-ran namespace, publication atomicity, batch cut, numeric, NUL rejection and advisory lock contention probes. Live probes used the existing isolated-schema Postgres at localhost:55433 and dropped their schemas. No implementation changed.

## Disposition ledger

| Source finding | Disposition | Evidence and remediation |
|---|---|---|
| postgres F1 | Confirmed; duplicate primary NUL rejection finding | Re-run: memory rejects; Postgres 22P05; recovery stops. Kernel must escape NUL in durable diagnostics. Raw host records with literal NUL need separate validation, not silent alteration of source identity. |
| postgres F2 | Confirmed; duplicate tenant fence finding | Both runners exclude subject=None, but cleanup covers tenant rows. Include tenant scopes in commit fences and destroyed checks. Widen Scope and deterministic sorting. |
| postgres F3 | Confirmed new | Re-run three subjects sharing subject/22: unrelated writer raises StoreError on 55P03. Add erasure_fence operation; 55P03 and 57014 during lock acquisition become InProgress. Preserve StoreError for unrelated transaction errors. |
| postgres F4 | Confirmed new | delete_range is unbounded; all owned transactions use finite statement_timeout. Existing 6M-row live reproduction documented. Bound kernel SQL pages; commit revocation/drain plus first page, then commit subsequent pages separately until empty. Retrying after a partial failure remains safe and makes progress. |
| postgres F5 | Confirmed; primary finding stronger | Re-run float 1e21 becomes Python int. Primary probe also shows actual value corruption at 5.808820077685227e16. Use existing documents::exact_json before SQL, preserve its text across Python, and normalize stored row reads through kernel if needed. |
| postgres F6 | Confirmed policy inconsistency | Current golden explicitly counts ingestion lock timeout. Lock contention is not a failed original; treat 55P03 as InProgress for ingest. Keep statement timeout counted outside dedicated fence lock acquisition. |
| postgres F7 | Confirmed docs | Update verdict table with actual operations after F3/F6 fix. |
| postgres F8 | Confirmed redundancy; small refactor fits numeric fix | Kernel should return insert/update/new-rejection application sets rather than Python recomputing them. Projection vanished row code alignment belongs projection owner. |
| erasure 1 | Confirmed; duplicate tenant fence + adds tenant intent delivery | Fence subjectless tenant writes; independently check tenant status for subjectless intents before delivery. Cleanup state alone cannot retract pending intents. Projection owner must cover. |
| erasure 2 | Documentation gap; no automatic cross-generation deletion | Scope ranges intentionally bind immutable generations. Completion explicitly covers frozen participants, not unregistered stores. Document/register every retained or building generation; sweep each. A wildcard scan would violate ownership and is not justified. |
| erasure 3 | Confirmed; duplicate rolling protocol incompatibility | Old exact locks and new buckets never meet. Adding old locks only to new revocations does NOT protect new writers from old revocations. Either enforce drain/stop rollout or put legacy exact locks on every new writer too (unbounded lock-table pressure returns). Recommend explicit stop/drain upgrade contract, retaining bounded buckets. |
| erasure 4 | Confirmed; duplicate namespace isolation | Re-run: staging resweep deletes production row. Bind ScopeCleanup to required key_namespace, inferred from sealing or explicit for plaintext/unsealed stores. Wrong namespace must not delete; resweep skips unrelated namespaces. |
| erasure 5 | Documentation gap | Subjectless private parts intentionally remain plaintext. Explain that sealed targets retaining them still need registered cleanup for tenant erasure. Do not introduce tenant-only encryption without changing protected-scope contracts. |
| erasure 6 | Confirmed new | resweep(0) silently scans nothing; add check_page at resweep and revoked entry points. |
| erasure 7 | Expected cost with optimization opportunity, unsafe proposed shortcut | A complete anti-restore sweep must revisit all durable revocations. Skipping destination revoke merely because workflow says revoked is unsafe after restoration of the destination tombstones. Keep reinstalling/draining destination fences. Document full-history cost and pagination. Optional caller-owned cursor needs a distinct partial-sweep API, not this fix. |
| erasure 8 | Tests accepted; parity expansion rejected | Add paused-write/cleanup race and scope_revoked/lock golden coverage alongside fixes. Go has no state stores; adding full erasure SDK solely for parity is outside current documented scope. |
| ingest H1 | Confirmed; duplicate poison identity | Parent/runtime/kernel owners: reconstruct rejection identity from source coordinates and classify record-derived invalid identity as CodecError, not BadHandle wiring. |
| ingest M1 | Confirmed; duplicate tenant fence | As above. |
| ingest L1 | Requires project-wide contract decision, not proven PR-specific defect | Shared payload_hash is explicitly authoritative FNV across existing idempotency guards; hypothetical malicious collision without concrete fixture. Do not silently change ingestion fingerprint format in isolation. If accepted hardening, use versioned domain-separated digest with goldens; parent decides consistently across projection. |
| ingest L2 | Confirmed documented behavior, docs clarification only | Re-run yields accepted/accepted/rejected versus all accepted after cut. Final state invariant holds; docs allow first value at newer-than-committed sequence. State explicitly that dead-letter outcomes may depend on batch cuts. |
| ingest L3 | Confirmed new | Re-run invalid publication map marks one entry before error. Add atomic kernel map publication; validate every source/fingerprint under same mutex before changing any entry. Preserve single-record ABI if needed, add batch method. |
| ingest L4 | Confirmed redundancy | Same remediation as postgres F8. Avoid broad protocol redesign: explicit application sets returned by kernel, adapters apply. |
| ingest L5 | Confirmed new | Memory pending omits check_page. Add shared check_page before native conversion. |
| ingest L6 | Confirmed new | validate_inputs only checks subject nonempty; row_key/scope_range enforce <=256 UTF-8 bytes/no NUL. Apply namespace::check_subject in kernel input validation. |

## Fix ordering and tests

1. Fix kernel diagnostics/input identity/subject validation before runner fallback tests. Kernel owner handles these.
2. Atomic publication and plan application sets. Reuse sets for exact JSON SQL serialization. Regress insert/update, append/upsert, nested negative/positive large doubles, null fields, rejected replay, and invalid publication map. Keep SQL text opaque in host.
3. Namespace-bound cleanup API and positive pages. Default namespace from sealing. Unsealed stores require explicit namespace; update tests/examples. Wrong namespace direct cleanup refuses or no-ops consistently; resweep skips it.
4. Fence classification + tenant scope runner changes, then cleanup pagination. First page must commit durable destination revocation before releasing exclusive fences. Subsequent pages can omit revocation only within that successful invocation. Retry invocation reinstalls it. Missing erasure argument retains provider guarantee that revoke drains writers first.
5. Document non-rolling upgrade, all retained generation registration, subjectless cleanup, resweep full-history cost, actual SQLSTATE table.

Regression proof: current saved probes fail before fix; turn them into tests that fail current HEAD. Run Rust cqrs/memory/postgres tests, regenerate changed golden vectors, rebuild bindings/native Python, run targeted Python memory + live Postgres suites, then parent runs all Rust/Python/Go race checks. Do not run huge-row performance fixtures in CI: use modest rows and assert multiple bounded statements/committed page progress, plus injected failure after one committed page and idempotent retry.


## Projection, sealing and ABI reconciliation

Current HEAD e900577. Source claims checked against current implementation; saved review probes remain the reproduction cases. No implementation changed during assessment.

| Source | Disposition / evidence | Remediation and proof |
|---|---|---|
| projection F1 | Confirmed. `_delivered` skips an unavailable row then opens later rows of the same aggregate; Outbox ordering contract applies before Relay sees rows. | Hold back the aggregate after one deferred opening, with memory/Postgres flaky-key and null-payload regressions. |
| projection F2 | Partially holds, duplicate erasure tenant-fence finding. Tenant-only encryption claim is false: protected_scope requires subject; subjectless plaintext is intentional. | Runtime tenant/None fences + cleanup guidance for legacy subjectless data, NOT a new tenant-only crypto format. Null-subject delivery must also observe tenant tombstones when sealing is configured. |
| projection F3 | Confirmed, original review P1. `projection_value` rounds beyond-u64 tokens before validation. | Kernel event_json validates original payload/metadata integer tokens; both ABI event-consuming functions use it. Keep i64 protocol counters. |
| projection F4 | Confirmed consistency authority gap; Python currently protects its commit. | Add optional expected cursor to Refresh, checked in kernel before extension. Commit supplies it; public recovery Refresh keeps its reload semantics. Golden moved-cursor regression. |
| projection F5 | Genuine contract coverage gap. | Parent expands branch goldens; coordinate optional Refresh field and event numeric vectors. |
| sealing M1 | Confirmed, original review starvation finding. Existing cursor rotates partitions only, so one aggregate still blocks peers in its partition. | Durable bounded within-partition aggregate rotation, preserving each aggregate's prefix. Regression blocked/healthy same partition, page size 1/100, memory/Postgres. |
| sealing M2 | Confirmed. Unsealed runner returns decoder identity unchanged, permits omission of envelope subject. | Runtime agent always invokes original-token-safe projection_sealed_input, before fence and commit. |
| sealing M3 | Confirmed cross-SDK contract authority gap. Python chooses fixed AAD fields. | Kernel Part.context + ABI stored_context, all stored kinds use it. Pin identical existing AAD bytes in Rust/Python/Go vectors. |
| sealing L1 | Confirmed tenant late-write duplicate. | Runtime tenant fences; storage cleanup bindings/docs; Scope type and deterministic None-safe ordering fixed here. |
| sealing L2 | Documented-design limitation, not safe local bug fix. Subject is intentionally part of ingestion identity; changing needs global owner index+storage migration. | Parent clarify immutable producer ownership and pin current fork semantics if needed. Do not introduce global uniqueness ad hoc. |
| sealing L3 | Confirmed configuration safety gap. Runner and store can fence different namespaces/providers. | Runtime constructor rejects inconsistent simultaneous Sealing providers/namespaces. Compatible separate Sealing objects remain allowed. |
| sealing L4 | Confirmed wrong-namespace deletion duplicate. | Storage agent binds cleanup namespace. |
| sealing L5 | Confirmed policy precedence issue: destroyed plaintext is rejected/deadlettered before status. | Consumer legacy reads check revocation before plaintext policy. Retain active-scope policy refusal and revoking wait. Tests no plaintext rejection after destruction. |
| sealing L6 | Retention improvement with concrete lifecycle opportunity; no zeroization guarantee. | Runtime clears ingestion admission key view when launching work; unresolved bounded calls still owned until settle. |
| sealing L7 | Explicit producer contract forbids re-sealing same ID; no new demonstrated defect. | Preserve fingerprint semantics, document migration/reseal restriction. Changing would invalidate persisted receipts. |
| sealing L8 | Confirmed subject validation inconsistency. | Storage agent centralizes no-NUL subject in core, removes redundant namespace rule; codec and key tests. |
| sealing L9 | Confirmed direct store provider hangs and misclassified runner retries. | Bound reserve/get via shared provider coordinator, hard timeout with no repeated unresolved call; test cancellation-suppressing calls and eventual cleanup. |
| sealing L10 | Confirmed doc mismatch. | Parent update append version prose to row sequence or zero if absent. |
| abi 1 | Confirmed doubled codec prefix. | event_json migration removes double wrapping; malformed event test exact single prefix. |
| abi 2 | Confirmed parsed contracts bypass validate. | projection_contract invokes kernel validate; invalid empty identity/topic/starts/at_rest refused consistently. |
| abi 3 | Latent unsupported 32-bit correctness and ABI-local policy duplication; valid narrow cleanup. | Parent/shared ABI owner centralizes existing saturating count conversion; test boundaries without widening supported-target claims. |
| abi 4 | Confirmed small redundancy; covered in touched code. | projection_transition uses write_json; parent postgres ABI analogous. Preserve intentional Value canonicalization for persisted numeric values. |
| abi 5 | Confirmed NUL tenant interop inconsistency. | Storage/core owner establishes kernel tenant rule at identity boundary; no host-only Go workaround. |
| abi 6 | Confirmed memory/PG page limit mismatch. | Storage agent MemoryIngestionSink.pending calls check_page. |
| abi 7 | Confirmed empty-vs-absent fingerprint mismatch. | Storage agent explicit None check, empty rejected by kernel. |

Additional independently confirmed originals within this scope: null-payload subject intents bypass key status (seal JSON null too; lifecycle-check old null records), and wait_for cancellation suppression exceeds key deadline (bounded coordinator owns outstanding provider calls).

## Implementation dependencies and boundaries

1. Rust projection event reader, validated ABI contract and kernel expected-cursor check; null-intent sealing/opening rules; kernel stored-part AAD builder. No generated outputs hand-edited.
2. Python provider coordinator deadlines and tracked unresolved calls, null legacy opening admission, sameaggregate holdback. Runtime whole-operation tracking complements rather than replaces provider deadline.
3. Memory/Postgres pending selection gets durable fairness within a partition without ever returning a later effect ahead of its retained prefix; this must be reviewed before implementation because naive keyset scanning permits cross-page overtaking.
4. Regression tests in new projection review regression files, plus existing at-rest/sealing/transaction/race suites. Parent owns golden writers/replays and docs; communicate new vector operations before regeneration.

Owned files: edax-cqrs/src/projection.rs and namespace.rs; edax-abi/src/cqrs.rs (+ minimal ABI stored-context export); edax-memory/src/projection_state.rs; edax-postgres/src/projection.rs; Python projection.py, sealing.py, backend/projection_postgres.py. Storage agent must coordinate namespace.rs shared subject change and projection cleanup factory.



## Final adversarial additions

- Empty producer aggregate/event IDs also required Codec classification in validate_inputs, independently of mapper row keys. Add a runner regression with a nonempty mapper key and a healthy following original.
- Ingestion/projection tenant identities must obey the erasure scope's nonempty and 64-byte limit; rejecting only NUL left custom-written rows outside the cleanup workflow's representable scopes.
- The first fair delivery proposal ranked the whole backlog. Replace it with indexed bounded per-partition group candidates; provision and verify the new expression index. A 100,000-intent fixture used index scans and returned pages1/100 in under1ms. Obsolete cutover history still needs pruning.
- Preserve caller cancellation on Python3.10 without Task.cancelling (introduced later). Owned run-loop stop catches its own requested cancellation; cancellation of the public run await always propagates.

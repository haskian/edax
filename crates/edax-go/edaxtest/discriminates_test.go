package edaxtest

// The kit passing a correct backend proves little: a clause that asserts nothing passes too. Each case
// here breaks one clause the way a real backend breaks it, then asserts that the clause fails it for
// the stated reason, and passes the same backend without the mistake.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// verdict records a clause's failures instead of failing the test that runs it. A fatal failure ends
// the clause's goroutine, as testing.T does.
type verdict struct {
	testing.TB
	mu      sync.Mutex
	reports []string
}

func (v *verdict) record(report string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.reports = append(v.reports, report)
}
func (v *verdict) Error(args ...any)                 { v.record(fmt.Sprint(args...)) }
func (v *verdict) Errorf(format string, args ...any) { v.record(fmt.Sprintf(format, args...)) }
func (v *verdict) Fail()                             { v.record("Fail") }
func (v *verdict) FailNow()                          { v.record("FailNow"); runtime.Goexit() }
func (v *verdict) Fatal(args ...any)                 { v.record(fmt.Sprint(args...)); runtime.Goexit() }
func (v *verdict) Fatalf(format string, args ...any) {
	v.record(fmt.Sprintf(format, args...))
	runtime.Goexit()
}

// A skipped clause is a rig error, never a catch.
func (v *verdict) Skip(args ...any)                 { v.TB.Fatalf("clause skipped: %s", fmt.Sprint(args...)) }
func (v *verdict) Skipf(format string, args ...any) { v.TB.Fatalf("clause skipped: "+format, args...) }
func (v *verdict) SkipNow()                         { v.TB.Fatal("clause skipped") }
func (v *verdict) Failed() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.reports) > 0
}
func (v *verdict) report() string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return strings.Join(v.reports, "; ")
}

// judge runs one clause against factory in a goroutine of its own and returns its verdict.
func judge[T any](t *testing.T, run func(testing.TB, Factory[T]), factory Factory[T]) *verdict {
	t.Helper()
	recorded := &verdict{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		run(recorded, factory)
	}()
	<-done
	return recorded
}

// against judges the named clause on the backend wrap breaks, and on the sound backend it wraps.
func against[T any](clauses []clause[T], name string, sound Factory[T], wrap func(T) T) func(*testing.T) (broken, control *verdict) {
	return func(t *testing.T) (*verdict, *verdict) {
		t.Helper()
		index := slices.IndexFunc(clauses, func(candidate clause[T]) bool { return candidate.name == name })
		if index < 0 {
			t.Fatalf("no clause %q", name)
		}
		run := clauses[index].run
		return judge(t, run, func(tb testing.TB) T { return wrap(sound(tb)) }), judge(t, run, sound)
	}
}

func soundEvents(t testing.TB) EventBackend { return MemoryFixture(t).Events }
func soundLedger(t testing.TB) edax.IdempotencyStore {
	return MemoryFixture(t).Ledger
}
func soundProjections(t testing.TB) edax.ProjectionStore { return MemoryFixture(t).Projections }
func soundCommands(t testing.TB) edax.CommandOutbox      { return MemoryFixture(t).Commands }
func soundSagas(t testing.TB) edax.SagaStore             { return MemoryFixture(t).Sagas }

// acceptsAConflictingAppend runs `INSERT ... ON CONFLICT DO NOTHING` and never checks the affected
// row count: the losing writer believes its event landed.
type acceptsAConflictingAppend struct{ EventBackend }

func (s acceptsAConflictingAppend) Append(ctx context.Context, key string, expected uint64, drafts []edax.EventDraft) ([]edax.EventMessage, error) {
	stored, err := s.EventBackend.Append(ctx, key, expected, drafts)
	if errors.Is(err, edax.ErrConcurrency) {
		return nil, nil
	}
	return stored, err
}

// eventsNewestFirst pages `ORDER BY recorded_at DESC`: the relay sends a later event ahead of an earlier one.
type eventsNewestFirst struct{ EventBackend }

func (s eventsNewestFirst) Unpublished(ctx context.Context, limit uint64) ([]edax.EventMessage, error) {
	rows, err := s.EventBackend.Unpublished(ctx, 1000)
	slices.Reverse(rows)
	return rows[:min(uint64(len(rows)), limit)], err
}

// readsThenWrites claims with a SELECT, then an INSERT in a second round trip, with no unique
// constraint between them. Concurrent claimants all read "free", and each runs the handler.
type readsThenWrites struct {
	edax.IdempotencyStore
	mu      sync.Mutex
	records map[string]string
	reads   int
	ready   chan struct{}
}

func (l *readsThenWrites) Claim(ctx context.Context, key string, hash uint64) (*edax.ClaimResult, error) {
	l.mu.Lock()
	prior, held := l.records[key]
	l.reads++
	if l.reads == 2 {
		close(l.ready)
	}
	l.mu.Unlock()
	// Force two reads before either write, independent of the scheduler's speed.
	select {
	case <-l.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if held {
		return &edax.ClaimResult{Prior: &prior}, nil
	}
	record, err := edax.EncodeRecord(edax.IdempotencyRecord{PayloadHash: hash, State: "pending"})
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.records[key] = record
	l.mu.Unlock()
	return &edax.ClaimResult{Won: true, Handle: []byte(key)}, nil
}

// getDropsTheRowKey returns the stored document alone, so a document saved with a stale "id" reads
// back with it, and Get disagrees with GetMany on what a row is.
type getDropsTheRowKey struct{ edax.ProjectionStore }

func (s getDropsTheRowKey) Get(ctx context.Context, id string) (*edax.ProjectionRow, error) {
	row, err := s.ProjectionStore.Get(ctx, id)
	if err != nil || row == nil {
		return row, err
	}
	var document map[string]any
	if err = json.Unmarshal([]byte(row.DocumentJSON), &document); err != nil {
		return nil, err
	}
	document["id"] = "stale"
	wire, err := json.Marshal(document)
	return &edax.ProjectionRow{Version: row.Version, DocumentJSON: string(wire)}, err
}

// wrapsAnyDocument stores a list, a scalar or null under a wrapper key instead of refusing it: the
// query can key the row now, but the caller's document is not what comes back.
type wrapsAnyDocument struct{ edax.ProjectionStore }

func (s wrapsAnyDocument) Save(ctx context.Context, id string, version uint64, document string) (uint64, error) {
	if !strings.HasPrefix(document, "{") {
		document = `{"value":` + document + `}`
	}
	return s.ProjectionStore.Save(ctx, id, version, document)
}

// acceptsAConflictingSave upserts and ignores the affected row count: the losing projector believes
// its read-model update landed.
type acceptsAConflictingSave struct{ edax.ProjectionStore }

func (s acceptsAConflictingSave) Save(ctx context.Context, id string, version uint64, document string) (uint64, error) {
	saved, err := s.ProjectionStore.Save(ctx, id, version, document)
	if errors.Is(err, edax.ErrConcurrency) {
		return version + 1, nil
	}
	return saved, err
}

// rewritePage decodes a GetMany page, lets change edit it, and encodes it again.
func rewritePage(wire string, change func(*edax.Page)) (string, error) {
	var page edax.Page
	if err := json.Unmarshal([]byte(wire), &page); err != nil {
		return "", err
	}
	change(&page)
	encoded, err := json.Marshal(page)
	return string(encoded), err
}

// getManyDropsTheRowKey returns the stored documents alone, `SELECT doc` where the key is its own
// column, so rows carry whatever "id" a writer happened to save.
type getManyDropsTheRowKey struct{ edax.ProjectionStore }

func (s getManyDropsTheRowKey) GetMany(ctx context.Context, query string) (string, error) {
	wire, err := s.ProjectionStore.GetMany(ctx, query)
	if err != nil {
		return wire, err
	}
	return rewritePage(wire, func(page *edax.Page) {
		for _, row := range page.Rows {
			delete(row, "id")
		}
	})
}

// getManyDropsNext returns the rows but never a next cursor, as a store written against a
// list-returning seam would: a reader stops after the first page and silently loses the rest.
type getManyDropsNext struct{ edax.ProjectionStore }

func (s getManyDropsNext) GetMany(ctx context.Context, query string) (string, error) {
	wire, err := s.ProjectionStore.GetMany(ctx, query)
	if err != nil {
		return wire, err
	}
	return rewritePage(wire, func(page *edax.Page) { page.Next = nil })
}

// commandsNewestFirst pages `ORDER BY issued_at DESC`: the relay sends a later decision first.
type commandsNewestFirst struct{ edax.CommandOutbox }

func (s commandsNewestFirst) Unpublished(ctx context.Context, limit uint64) ([]edax.CommandMessage, error) {
	rows, err := s.CommandOutbox.Unpublished(ctx, 1000)
	slices.Reverse(rows)
	return rows[:min(uint64(len(rows)), limit)], err
}

// acceptsAConflictingStep upserts the instance and ignores the affected row count: the losing fold
// believes its state and commands landed.
type acceptsAConflictingStep struct{ edax.SagaStore }

func (s acceptsAConflictingStep) Save(ctx context.Context, saga, id string, state json.RawMessage, expected uint64, folded string, commands []edax.CommandMessage) (uint64, error) {
	saved, err := s.SagaStore.Save(ctx, saga, id, state, expected, folded, commands)
	if errors.Is(err, edax.ErrConcurrency) {
		return expected + 1, nil
	}
	return saved, err
}

// dropsFolded has no column for the folded marks: every redelivery of an event already folded is
// folded again.
type dropsFolded struct{ edax.SagaStore }

func (s dropsFolded) Load(ctx context.Context, saga, id string) (*edax.SagaInstance, error) {
	instance, err := s.SagaStore.Load(ctx, saga, id)
	if instance != nil {
		instance.Folded = "{}"
	}
	return instance, err
}

// dropsSnapshots acknowledges a snapshot it never writes, as a cache table nobody migrated would: the
// repository then replays every stream from its first event.
type dropsSnapshots struct{ EventBackend }

func (dropsSnapshots) SaveSnapshot(context.Context, edax.Snapshot) error { return nil }

// corruptsReplay preserves the claim but loses or changes its completed result.
type corruptsReplay struct {
	edax.IdempotencyStore
	missing bool
}

func (s corruptsReplay) Claim(ctx context.Context, key string, hash uint64) (*edax.ClaimResult, error) {
	claim, err := s.IdempotencyStore.Claim(ctx, key, hash)
	if err != nil || claim.Prior == nil {
		return claim, err
	}
	record, err := kernel.DecodeIdempotencyRecord(*claim.Prior)
	if err != nil || record.State != "done" {
		return claim, err
	}
	if s.missing {
		claim.Prior = nil
		return claim, nil
	}
	record.ResultJSON = edax.Ptr(`{"invoice_id":"1042","paid":false}`)
	wire, err := edax.EncodeRecord(*record)
	claim.Prior = &wire
	return claim, err
}

// A database may wait for the holder to settle instead of returning its uncommitted claim.
type waitsForHeldClaim struct{ edax.IdempotencyStore }

func (s waitsForHeldClaim) Claim(ctx context.Context, key string, hash uint64) (*edax.ClaimResult, error) {
	claim, err := s.IdempotencyStore.Claim(ctx, key, hash)
	if err != nil || claim.Prior == nil {
		return claim, err
	}
	record, err := kernel.DecodeIdempotencyRecord(*claim.Prior)
	if err != nil {
		return nil, err
	}
	if record.State == "pending" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return claim, nil
}

func TestTransactionContractDoesNotWaitOnItsOwnHeldClaim(t *testing.T) {
	for _, clause := range transactionClauses() {
		t.Run(clause.name, func(t *testing.T) {
			result := judge(t, clause.run, func(tb testing.TB) TransactionFixture {
				fixture := MemoryFixture(tb)
				fixture.Ledger = waitsForHeldClaim{fixture.Ledger}
				return fixture
			})
			if result.Failed() {
				t.Fatal(result.report())
			}
		})
	}
}

func TestTheKitFailsABackendThatBreaksItsClause(t *testing.T) {
	for _, test := range []struct {
		name string
		// expect is a fragment of the report the clause must make, so a failure for another reason
		// does not count as a catch.
		expect string
		judge  func(*testing.T) (broken, control *verdict)
	}{
		{"events_accept_a_conflicting_append", "exactly one writer lands", against(eventStoreClauses(), "two_writers_at_one_version_land_exactly_one_append", soundEvents, func(store EventBackend) EventBackend { return acceptsAConflictingAppend{store} })},
		{"event_outbox_pages_newest_first", "oldest commit first", against(eventStoreClauses(), "unpublished_is_oldest_commit_first", soundEvents, func(store EventBackend) EventBackend { return eventsNewestFirst{store} })},
		{"ledger_reads_then_writes", "want exactly one winner", against(idempotencyStoreClauses(), "concurrent_claims_have_exactly_one_winner", soundLedger, func(edax.IdempotencyStore) edax.IdempotencyStore {
			return &readsThenWrites{records: map[string]string{}, ready: make(chan struct{})}
		})},
		{"projection_get_drops_the_row_key", "reads map[id:stale", against(projectionStoreClauses(), "keyed_row_and_version", soundProjections, func(store edax.ProjectionStore) edax.ProjectionStore { return getDropsTheRowKey{store} })},
		{"projection_wraps_a_list", "a_list save was not refused", against(projectionStoreClauses(), "refused_save/a_list", soundProjections, func(store edax.ProjectionStore) edax.ProjectionStore { return wrapsAnyDocument{store} })},
		{"projection_wraps_a_scalar", "a_scalar save was not refused", against(projectionStoreClauses(), "refused_save/a_scalar", soundProjections, func(store edax.ProjectionStore) edax.ProjectionStore { return wrapsAnyDocument{store} })},
		{"projection_wraps_null", "null save was not refused", against(projectionStoreClauses(), "refused_save/null", soundProjections, func(store edax.ProjectionStore) edax.ProjectionStore { return wrapsAnyDocument{store} })},
		{"projection_accepts_a_conflicting_save", "exactly one writer lands", against(projectionStoreClauses(), "two_writers_at_one_version_land_exactly_one_save", soundProjections, func(store edax.ProjectionStore) edax.ProjectionStore { return acceptsAConflictingSave{store} })},
		{"projection_get_many_drops_the_row_key", "want each row key", against(projectionStoreClauses(), "get_many_returns_the_row_key_as_the_id", soundProjections, func(store edax.ProjectionStore) edax.ProjectionStore { return getManyDropsTheRowKey{store} })},
		{"projection_get_many_drops_next", "every row once", against(projectionStoreClauses(), "get_many_pages_every_row_once_by_its_next_cursor", soundProjections, func(store edax.ProjectionStore) edax.ProjectionStore { return getManyDropsNext{store} })},
		{"command_outbox_pages_newest_first", "oldest commit first", against(commandOutboxClauses(), "unpublished_is_oldest_commit_first", soundCommands, func(store edax.CommandOutbox) edax.CommandOutbox { return commandsNewestFirst{store} })},
		{"saga_accepts_a_conflicting_step", "exactly one writer lands", against(sagaStoreClauses(), "two_steps_at_one_version_land_exactly_one_write", soundSagas, func(store edax.SagaStore) edax.SagaStore { return acceptsAConflictingStep{store} })},
		{"saga_drops_the_folded_marks", "want what the step saved", against(sagaStoreClauses(), "save_load_and_complete", soundSagas, func(store edax.SagaStore) edax.SagaStore { return dropsFolded{store} })},
		{"snapshot_never_written", "not atomic", against(transactionClauses(), "commit", MemoryFixture, func(fixture TransactionFixture) TransactionFixture {
			fixture.Events = dropsSnapshots{fixture.Events}
			return fixture
		})},
		{"committed_replay_missing", "committed ledger record", against(transactionClauses(), "commit", MemoryFixture, func(fixture TransactionFixture) TransactionFixture {
			fixture.Ledger = corruptsReplay{IdempotencyStore: fixture.Ledger, missing: true}
			return fixture
		})},
		{"cached_result_changed", "cached result", against(idempotencyStoreClauses(), "claim_and_replay", soundLedger, func(store edax.IdempotencyStore) edax.IdempotencyStore {
			return corruptsReplay{IdempotencyStore: store}
		})},
	} {
		t.Run(test.name, func(t *testing.T) {
			broken, control := test.judge(t)
			if !broken.Failed() {
				t.Fatal("the clause passed a backend that breaks it")
			}
			if report := broken.report(); !strings.Contains(report, test.expect) {
				t.Fatalf("the clause failed for another reason: %s", report)
			}
			if control.Failed() {
				t.Fatalf("the clause fails the sound backend: %s", control.report())
			}
		})
	}
}

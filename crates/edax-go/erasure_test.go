package edax_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

func TestErasureCleanupFailureDoesNotStarveHealthyParticipants(t *testing.T) {
	ctx := context.Background()
	keys := edax.NewMemoryKeys()
	defer keys.Close()
	store := edax.NewMemoryErasureStore()
	fail := true
	calls := map[string]int{}
	coordinator := erasureCoordinator(t, keys, store, map[string]edax.ErasureCleanup{
		"a-offline": func(context.Context, edax.KeyScope) error {
			calls["a-offline"]++
			if fail {
				return edax.ErrStore
			}
			return nil
		},
		"z-healthy": func(context.Context, edax.KeyScope) error { calls["z-healthy"]++; return nil },
	})
	scope := erasureScope()
	if _, err := coordinator.Erase(ctx, scope, "fair-cleanup"); !errors.Is(err, edax.ErrStore) {
		t.Fatalf("missing failed participant error: %v", err)
	}
	stored, err := store.Get(ctx, scope, "fair-cleanup")
	if err != nil || stored == nil {
		t.Fatalf("missing operation: %v", err)
	}
	if stored.Operation.Stage() != "notified" || !reflect.DeepEqual(stored.Operation.Acknowledgements(), []string{"z-healthy"}) || calls["z-healthy"] != 1 {
		t.Fatalf("healthy participant starved: operation=%s calls=%v", stored.Operation.Record(), calls)
	}
	status, err := keys.Status(ctx, scope)
	if err != nil || status != "revoking" {
		t.Fatalf("destroyed before all cleanup: %s %v", status, err)
	}
	fail = false
	completed, err := coordinator.Resume(ctx, scope, "fair-cleanup")
	if err != nil || completed.Stage() != "complete" || calls["a-offline"] != 2 || calls["z-healthy"] != 1 {
		t.Fatalf("cleanup recovery: operation=%s calls=%v err=%v", completed.Record(), calls, err)
	}
}

func TestErasureRefusesExcludedRegisteredParticipant(t *testing.T) {
	for _, required := range [][]string{{}, {"database"}} {
		name := "empty-required"
		if len(required) != 0 {
			name = "omitted-cache"
		}
		t.Run(name, func(t *testing.T) {
			keys := edax.NewMemoryKeys()
			defer keys.Close()
			cleanup := func(context.Context, edax.KeyScope) error { t.Fatal("constructor ran cleanup"); return nil }
			_, err := edax.NewErasureCoordinator(keys, edax.NewMemoryErasureStore(), edax.ErasureCoordinatorOptions{
				Participants: map[string]edax.ErasureCleanup{"database": cleanup, "cache": cleanup}, RequiredParticipants: required,
			})
			if !errors.Is(err, edax.ErrBadHandle) {
				t.Fatalf("registered cleanup silently excluded: %v", err)
			}
		})
	}
}

func TestErasureRecoveryCursorPassesBlockedOperationAndWraps(t *testing.T) {
	ctx := context.Background()
	keys := edax.NewMemoryKeys()
	defer keys.Close()
	store := edax.NewMemoryErasureStore()
	scope := erasureScope()
	readyScope := scope
	readyScope.Subject = edax.Ptr("ready-subject")
	for _, row := range []struct {
		id, request string
		required    []string
	}{
		{"a-blocked", "blocked", []string{"offline"}},
		{"b-ready", "ready", nil},
	} {
		owner := scope
		if row.request == "ready" {
			owner = readyScope
		}
		if _, err := store.Create(ctx, erasureCandidate(t, owner, row.id, row.request, row.required...)); err != nil {
			t.Fatal(err)
		}
	}
	coordinator := erasureCoordinator(t, keys, store, nil)
	blocked, err := coordinator.Resume(ctx, scope, "blocked")
	if err != nil || blocked.Stage() != "notified" {
		t.Fatalf("blocked fixture: %s %v", blocked.Record(), err)
	}
	first, err := store.PendingAfter(ctx, 1, nil)
	if err != nil || len(first) != 1 || first[0].Operation.OperationID() != "a-blocked" {
		t.Fatalf("first page: %v %v", first, err)
	}
	cursor := first[0].Operation.OperationID()
	second, err := store.PendingAfter(ctx, 1, &cursor)
	if err != nil || len(second) != 1 || second[0].Operation.OperationID() != "b-ready" {
		t.Fatalf("blocked row hid next page: %v %v", second, err)
	}
	ready, err := coordinator.Resume(ctx, readyScope, "ready")
	if err != nil || ready.Stage() != "complete" {
		t.Fatalf("newer operation never completed: %s %v", ready.Record(), err)
	}
	cursor = second[0].Operation.OperationID()
	last, err := store.PendingAfter(ctx, 1, &cursor)
	if err != nil || len(last) != 0 {
		t.Fatalf("end page: %v %v", last, err)
	}
	if _, err := store.Create(ctx, erasureCandidate(t, scope, "0-inserted", "inserted", "offline")); err != nil {
		t.Fatal(err)
	}
	wrapped, err := store.PendingAfter(ctx, 3, nil)
	if err != nil || len(wrapped) != 2 || wrapped[0].Operation.OperationID() != "0-inserted" || wrapped[1].Operation.OperationID() != "a-blocked" {
		t.Fatalf("cursor wrap lost earlier insertion or returned completed work: %v %v", wrapped, err)
	}
	zero, err := store.PendingAfter(ctx, 0, nil)
	if err != nil || len(zero) != 0 {
		t.Fatalf("zero limit: %v %v", zero, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.PendingAfter(cancelled, 1, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled cursor query: %v", err)
	}
}

func erasureScope() edax.KeyScope {
	tenant, subject := "tenant", "subject"
	return edax.KeyScope{KeyNamespace: "production", Tenant: &tenant, Subject: &subject}
}
func erasureCandidate(t *testing.T, scope edax.KeyScope, id, request string, participants ...string) edax.ErasureOperation {
	t.Helper()
	record, err := kernel.ErasureCreate(scope, id, request, participants)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := edax.ParseErasureOperation(record)
	if err != nil {
		t.Fatal(err)
	}
	return operation
}
func erasureCoordinator(t *testing.T, keys edax.KeyProvider, store edax.ErasureStore, participants map[string]edax.ErasureCleanup) *edax.ErasureCoordinator {
	t.Helper()
	coordinator, err := edax.NewErasureCoordinator(keys, store, edax.ErasureCoordinatorOptions{Participants: participants})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func TestErasureOfflineParticipantAndStableNotification(t *testing.T) {
	ctx := context.Background()
	scope := erasureScope()
	keys := edax.NewMemoryKeys()
	defer keys.Close()
	store := edax.NewMemoryErasureStore()
	cleanupCalls := 0
	coordinator := erasureCoordinator(t, keys, store, map[string]edax.ErasureCleanup{
		"database": func(ctx context.Context, _ edax.KeyScope) error {
			notifications, err := store.Unpublished(ctx, 10)
			if err != nil {
				return err
			}
			if len(notifications) != 1 {
				t.Fatal("cleanup ran before durable notification")
			}
			cleanupCalls++
			return nil
		}, "offline": nil,
	})
	operation, err := coordinator.Erase(ctx, scope, "request")
	if err != nil {
		t.Fatal(err)
	}
	if operation.Stage() != "notified" || cleanupCalls != 1 {
		t.Fatalf("unexpected pending operation: %s, %d", operation.Record(), cleanupCalls)
	}
	status, err := keys.Status(ctx, scope)
	if err != nil || status != "revoking" {
		t.Fatalf("keys destroyed before offline acknowledgement: %s %v", status, err)
	}
	if _, err := coordinator.Acknowledge(ctx, scope, "request", "unknown"); err == nil {
		t.Fatal("accepted unknown participant")
	}
	if _, err := coordinator.Acknowledge(ctx, scope, "request", "offline"); err != nil {
		t.Fatal(err)
	}
	completed, err := coordinator.Resume(ctx, scope, "request")
	if err != nil {
		t.Fatal(err)
	}
	if completed.Stage() != "complete" {
		t.Fatal(completed.Record())
	}
	if _, err := coordinator.Acknowledge(ctx, scope, "request", "offline"); err != nil {
		t.Fatal("duplicate acknowledgement", err)
	}
	again, err := coordinator.Erase(ctx, scope, "request")
	if err != nil {
		t.Fatal(err)
	}
	if again.OperationID() != operation.OperationID() || cleanupCalls != 1 {
		t.Fatal("retry changed operation or repeated acknowledged cleanup")
	}
	notifications, err := store.Unpublished(ctx, 10)
	if err != nil || len(notifications) != 1 {
		t.Fatalf("notifications: %v %v", notifications, err)
	}
	if strings.Contains(notifications[0].Record, "ciphertext") || notifications[0].OperationID != operation.OperationID() {
		t.Fatal("unexpected control notification")
	}
	if err := store.MarkPublished(ctx, operation.OperationID()); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Resume(ctx, scope, "request"); err != nil {
		t.Fatal(err)
	}
	notifications, _ = store.Unpublished(ctx, 10)
	if len(notifications) != 0 {
		t.Fatal("retry resurrected published notification")
	}
	pending, _ := store.Pending(ctx, 10)
	if len(pending) != 0 {
		t.Fatal("completed operation remained pending")
	}
	// Mutating a decoded scope cannot change the stored immutable workflow.
	copy := operation.Scope()
	*copy.Subject = "changed"
	loaded, _ := store.GetByID(ctx, operation.OperationID())
	if *loaded.Operation.Scope().Subject != "subject" {
		t.Fatal("scope alias mutated durable record")
	}
}

func TestErasureMemoryCASAndFrozenParticipants(t *testing.T) {
	ctx := context.Background()
	scope := erasureScope()
	store := edax.NewMemoryErasureStore()
	initial, err := store.Create(ctx, erasureCandidate(t, scope, "operation", "request", "database"))
	if err != nil {
		t.Fatal(err)
	}
	retry, err := store.Create(ctx, erasureCandidate(t, scope, "new-id", "request", "database"))
	if err != nil || retry != initial {
		t.Fatalf("idempotent create: %v %v", retry, err)
	}
	if _, err := store.Create(ctx, erasureCandidate(t, scope, "new-id", "request", "new-participant")); err == nil {
		t.Fatal("participant set changed")
	}
	if _, err := store.Advance(ctx, initial, "notify", nil); err == nil {
		t.Fatal("notified before revoke")
	}
	revoked, err := store.Advance(ctx, initial, "revoke", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance(ctx, initial, "revoke", nil); !errors.Is(err, edax.ErrConcurrency) {
		t.Fatalf("stale CAS: %v", err)
	}
	if notifications, _ := store.Unpublished(ctx, 10); len(notifications) != 0 {
		t.Fatal("failed transition enqueued notification")
	}
	notified, err := store.Advance(ctx, revoked, "notify", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance(ctx, notified, "destroy", nil); err == nil {
		t.Fatal("destruction without cleanup")
	}
	if notifications, _ := store.Unpublished(ctx, 10); len(notifications) != 1 {
		t.Fatal("notification missing")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Get(cancelled, scope, "request"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

type erasureLostResponse struct {
	edax.ErasureStore
	action string
	fired  bool
}

func (s *erasureLostResponse) Advance(ctx context.Context, previous edax.StoredErasure, action string, participant *string) (edax.StoredErasure, error) {
	next, err := s.ErasureStore.Advance(ctx, previous, action, participant)
	if err == nil && action == s.action && !s.fired {
		s.fired = true
		return edax.StoredErasure{}, edax.ErrStore
	}
	return next, err
}
func TestErasureRecoversAfterEveryCommittedTransition(t *testing.T) {
	for _, action := range []string{"revoke", "notify", "acknowledge", "destroy", "complete"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			scope := erasureScope()
			keys := edax.NewMemoryKeys()
			defer keys.Close()
			store := &erasureLostResponse{ErasureStore: edax.NewMemoryErasureStore(), action: action}
			coordinator := erasureCoordinator(t, keys, store, map[string]edax.ErasureCleanup{"database": func(context.Context, edax.KeyScope) error { return nil }})
			if _, err := coordinator.Erase(ctx, scope, "request"); !errors.Is(err, edax.ErrStore) {
				t.Fatalf("expected lost response at %s: %v", action, err)
			}
			operation, err := coordinator.Resume(ctx, scope, "request")
			if err != nil || operation.Stage() != "complete" {
				t.Fatalf("recovery: %s %v", operation.Record(), err)
			}
			notifications, _ := store.Unpublished(ctx, 10)
			if len(notifications) != 1 {
				t.Fatalf("notification after recovery: %v", notifications)
			}
		})
	}
}

type delayedErasureKeys struct {
	edax.KeyProvider
	failRevoke   bool
	delayDestroy bool
}

func (k *delayedErasureKeys) Revoke(ctx context.Context, scope edax.KeyScope) (string, error) {
	if k.failRevoke {
		return "", edax.ErrStore
	}
	return k.KeyProvider.Revoke(ctx, scope)
}
func (k *delayedErasureKeys) Destroy(ctx context.Context, scope edax.KeyScope) (string, error) {
	if k.delayDestroy {
		return "revoking", nil
	}
	return k.KeyProvider.Destroy(ctx, scope)
}
func TestErasureProviderAndCleanupFailuresDoNotAdvance(t *testing.T) {
	ctx := context.Background()
	scope := erasureScope()
	memory := edax.NewMemoryKeys()
	defer memory.Close()
	keys := &delayedErasureKeys{KeyProvider: memory, failRevoke: true, delayDestroy: true}
	store := edax.NewMemoryErasureStore()
	cleanupErr := error(edax.ErrConcurrency)
	coordinator := erasureCoordinator(t, keys, store, map[string]edax.ErasureCleanup{"database": func(context.Context, edax.KeyScope) error { return cleanupErr }})
	if _, err := coordinator.Erase(ctx, scope, "request"); !errors.Is(err, edax.ErrStore) {
		t.Fatal(err)
	}
	status, _ := coordinator.Status(ctx, scope, "request")
	if status == nil || status.Stage() != "persisted" {
		t.Fatal("provider outage advanced operation")
	}
	keys.failRevoke = false
	if _, err := coordinator.Resume(ctx, scope, "request"); !errors.Is(err, edax.ErrConcurrency) {
		t.Fatal("participant concurrency failure was swallowed", err)
	}
	status, _ = coordinator.Status(ctx, scope, "request")
	if status.Stage() != "notified" || len(status.Acknowledgements()) != 0 {
		t.Fatal("failed participant was acknowledged")
	}
	cleanupErr = nil
	operation, err := coordinator.Resume(ctx, scope, "request")
	if err != nil || operation.Stage() != "notified" {
		t.Fatalf("delayed destruction: %s %v", operation.Record(), err)
	}
	keys.delayDestroy = false
	operation, err = coordinator.Resume(ctx, scope, "request")
	if err != nil || operation.Stage() != "complete" {
		t.Fatalf("destruction recovery: %s %v", operation.Record(), err)
	}
}

func TestErasureConcurrentWorkersAndTenantFence(t *testing.T) {
	ctx := context.Background()
	scope := erasureScope()
	scope.Subject = nil
	keys := edax.NewMemoryKeys()
	defer keys.Close()
	store := edax.NewMemoryErasureStore()
	var calls atomic.Int32
	coordinator := erasureCoordinator(t, keys, store, map[string]edax.ErasureCleanup{"database": func(context.Context, edax.KeyScope) error { calls.Add(1); return nil }})
	var workers sync.WaitGroup
	results := make(chan edax.ErasureOperation, 12)
	failures := make(chan error, 12)
	for range 12 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			operation, err := coordinator.Erase(ctx, scope, "request")
			if err != nil {
				failures <- err
			} else {
				results <- operation
			}
		}()
	}
	workers.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	id := ""
	for operation := range results {
		if operation.Stage() != "complete" {
			t.Fatal(operation.Record())
		}
		if id != "" && operation.OperationID() != id {
			t.Fatal("multiple operation identities")
		}
		id = operation.OperationID()
	}
	if calls.Load() == 0 {
		t.Fatal("cleanup never ran")
	}
	child := erasureScope()
	subject := "created-after-erasure"
	child.Subject = &subject
	if _, err := keys.Reserve(ctx, child, "xchacha20-poly1305"); err == nil {
		t.Fatal("tenant erasure allowed new subject key")
	}
}

type erasureLostCreate struct {
	edax.ErasureStore
	fired bool
}

func (s *erasureLostCreate) Create(ctx context.Context, candidate edax.ErasureOperation) (edax.StoredErasure, error) {
	stored, err := s.ErasureStore.Create(ctx, candidate)
	if err == nil && !s.fired {
		s.fired = true
		return edax.StoredErasure{}, edax.ErrStore
	}
	return stored, err
}
func TestErasureCreationCommitsBeforeAnyDestructiveEffect(t *testing.T) {
	ctx := context.Background()
	scope := erasureScope()
	keys := edax.NewMemoryKeys()
	defer keys.Close()
	// Allocate a live generation to distinguish a persisted request from revocation.
	if _, err := keys.Reserve(ctx, scope, "xchacha20-poly1305"); err != nil {
		t.Fatal(err)
	}
	store := &erasureLostCreate{ErasureStore: edax.NewMemoryErasureStore()}
	coordinator := erasureCoordinator(t, keys, store, nil)
	if _, err := coordinator.Erase(ctx, scope, "request"); !errors.Is(err, edax.ErrStore) {
		t.Fatal(err)
	}
	stored, err := store.Get(ctx, scope, "request")
	if err != nil || stored == nil || stored.Operation.Stage() != "persisted" {
		t.Fatalf("persisted request: %v %v", stored, err)
	}
	// The failed creation response prevents every destructive provider call.
	if _, err := keys.Reserve(ctx, scope, "xchacha20-poly1305"); err != nil {
		t.Fatal("provider revoked before create confirmation", err)
	}
	operation, err := coordinator.Erase(ctx, scope, "request")
	if err != nil || operation.Stage() != "complete" {
		t.Fatalf("retry: %s %v", operation.Record(), err)
	}
	if operation.OperationID() != stored.Operation.OperationID() {
		t.Fatal("retry replaced persisted operation identity")
	}
}

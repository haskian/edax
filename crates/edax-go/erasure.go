package edax

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// ErasureOperation is an immutable, validated kernel workflow record.
// Its required participants are frozen when the operation is first persisted.
type ErasureOperation struct{ record string }
type erasureWire struct {
	OperationID  string   `json:"operation_id"`
	RequestID    string   `json:"request_id"`
	Scope        KeyScope `json:"scope"`
	Required     []string `json:"required_participants"`
	Acknowledged []string `json:"acknowledgements"`
	Stage        string   `json:"stage"`
}

// ParseErasureOperation validates a persisted workflow through the kernel.
func ParseErasureOperation(record string) (ErasureOperation, error) {
	canonical, err := kernel.ErasureValidate(record)
	return ErasureOperation{record: canonical}, err
}
func (o ErasureOperation) wire() erasureWire {
	var wire erasureWire
	// Only the validated constructor populates record. A zero value exposes empty identity.
	_ = json.Unmarshal([]byte(o.record), &wire)
	return wire
}
func (o ErasureOperation) Record() string                 { return o.record }
func (o ErasureOperation) OperationID() string            { return o.wire().OperationID }
func (o ErasureOperation) RequestID() string              { return o.wire().RequestID }
func (o ErasureOperation) Scope() KeyScope                { return o.wire().Scope }
func (o ErasureOperation) Stage() string                  { return o.wire().Stage }
func (o ErasureOperation) RequiredParticipants() []string { return o.wire().Required }
func (o ErasureOperation) Acknowledgements() []string     { return o.wire().Acknowledged }

// StoredErasure pairs an operation with its compare-and-set revision.
type StoredErasure struct {
	Version   uint64
	Operation ErasureOperation
}

// ErasureNotification is a stable control message. Publish Record unchanged, then mark its ID published.
type ErasureNotification struct {
	OperationID string
	Record      string
}

// ErasureStore persists workflow transitions and atomically enqueues notification on entering notified.
// Recovery advances PendingAfter's cursor to the last returned operation ID, then resets it to nil
// after an empty page to revisit blocked work and discover newly inserted earlier IDs.
// Publication is at least once; consumers deduplicate by operation ID.
type ErasureStore interface {
	Create(context.Context, ErasureOperation) (StoredErasure, error)
	Get(context.Context, KeyScope, string) (*StoredErasure, error)
	GetByID(context.Context, string) (*StoredErasure, error)
	Advance(context.Context, StoredErasure, string, *string) (StoredErasure, error)
	Pending(context.Context, uint64) ([]StoredErasure, error)
	PendingAfter(context.Context, uint64, *string) ([]StoredErasure, error)
	Unpublished(context.Context, uint64) ([]ErasureNotification, error)
	MarkPublished(context.Context, string) error
}

type erasureRequest struct{ scope, request string }

// MemoryErasureStore is a synchronized process-local reference. Restart loses its records and outbox.
// Production stores must persist operations, revisions, and notifications atomically.
type MemoryErasureStore struct {
	mu            sync.Mutex
	operations    map[string]StoredErasure
	requests      map[erasureRequest]string
	notifications map[string]ErasureNotification
}

func NewMemoryErasureStore() *MemoryErasureStore {
	return &MemoryErasureStore{operations: map[string]StoredErasure{}, requests: map[erasureRequest]string{}, notifications: map[string]ErasureNotification{}}
}
func erasureRequestKey(scope KeyScope, request string) (erasureRequest, error) {
	encoded, err := kernel.EncodeKeyScope(scope)
	return erasureRequest{string(encoded), request}, err
}
func (s *MemoryErasureStore) Create(ctx context.Context, candidate ErasureOperation) (StoredErasure, error) {
	if err := ctx.Err(); err != nil {
		return StoredErasure{}, err
	}
	expected, err := kernel.ErasureCreate(candidate.Scope(), candidate.OperationID(), candidate.RequestID(), candidate.RequiredParticipants())
	if err != nil {
		return StoredErasure{}, err
	}
	if expected != candidate.record {
		return StoredErasure{}, &Error{Code: CodeBadHandle, Message: "an erasure store creates only initial operations"}
	}
	key, err := erasureRequestKey(candidate.Scope(), candidate.RequestID())
	if err != nil {
		return StoredErasure{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.requests[key]; ok {
		existing := s.operations[id]
		return existing, kernel.ErasureCheckRequest(existing.Operation.record, candidate.record)
	}
	if _, ok := s.operations[candidate.OperationID()]; ok {
		return StoredErasure{}, &Error{Code: CodeBadHandle, Message: "erasure operation ID is already in use"}
	}
	stored := StoredErasure{Version: 1, Operation: candidate}
	s.requests[key] = candidate.OperationID()
	s.operations[candidate.OperationID()] = stored
	return stored, nil
}
func (s *MemoryErasureStore) Get(ctx context.Context, scope KeyScope, request string) (*StoredErasure, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := erasureRequestKey(scope, request)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.requests[key]
	if !ok {
		return nil, nil
	}
	row := s.operations[id]
	return &row, nil
}
func (s *MemoryErasureStore) GetByID(ctx context.Context, id string) (*StoredErasure, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.operations[id]
	if !ok {
		return nil, nil
	}
	return &row, nil
}
func (s *MemoryErasureStore) Advance(ctx context.Context, previous StoredErasure, action string, participant *string) (StoredErasure, error) {
	if err := ctx.Err(); err != nil {
		return StoredErasure{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.operations[previous.Operation.OperationID()]
	if !ok || current != previous {
		return StoredErasure{}, &Error{Code: CodeConcurrency, Message: "erasure operation revision changed"}
	}
	if current.Version == math.MaxUint64 {
		return StoredErasure{}, &Error{Code: CodeBadHandle, Message: "erasure operation revision exhausted"}
	}
	record, err := kernel.ErasureApply(current.Operation.record, action, participant)
	if err != nil {
		return StoredErasure{}, err
	}
	next, err := ParseErasureOperation(record)
	if err != nil {
		return StoredErasure{}, err
	}
	notification, err := kernel.ErasureNotification(current.Operation.record, next.record)
	if err != nil {
		return StoredErasure{}, err
	}
	stored := StoredErasure{Version: current.Version + 1, Operation: next}
	s.operations[next.OperationID()] = stored
	if notification != nil {
		s.notifications[next.OperationID()] = ErasureNotification{OperationID: next.OperationID(), Record: *notification}
	}
	return stored, nil
}
func (s *MemoryErasureStore) Pending(ctx context.Context, limit uint64) ([]StoredErasure, error) {
	return s.PendingAfter(ctx, limit, nil)
}

// PendingAfter returns unfinished operations strictly after the cursor in ascending ID order.
// Reset the cursor to nil after an empty page to start another recovery pass.
func (s *MemoryErasureStore) PendingAfter(ctx context.Context, limit uint64, after *string) ([]StoredErasure, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.operations))
	for id := range s.operations {
		if after == nil || id > *after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	rows := []StoredErasure{}
	for _, id := range ids {
		if uint64(len(rows)) >= limit {
			break
		}
		row := s.operations[id]
		if row.Operation.Stage() != "complete" {
			rows = append(rows, row)
		}
	}
	return rows, nil
}
func (s *MemoryErasureStore) Unpublished(ctx context.Context, limit uint64) ([]ErasureNotification, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.notifications))
	for id := range s.notifications {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	rows := []ErasureNotification{}
	for _, id := range ids {
		if uint64(len(rows)) >= limit {
			break
		}
		rows = append(rows, s.notifications[id])
	}
	return rows, nil
}
func (s *MemoryErasureStore) MarkPublished(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.notifications, id)
	return nil
}

// ErasureCleanup must idempotently fence writes, drain work, and remove plaintext, including restore copies.
// Return nil only when all participant obligations are confirmed. Errors never acknowledge cleanup.
type ErasureCleanup func(context.Context, KeyScope) error

// ErasureCoordinatorOptions fixes required participants; nil RequiredParticipants uses the registry keys.
// A nil or missing callback stays pending until explicitly acknowledged or a later worker provides it.
type ErasureCoordinatorOptions struct {
	Participants         map[string]ErasureCleanup
	RequiredParticipants []string
}

// ErasureCoordinator resumes persisted effects. The key provider and participants must support retries.
type ErasureCoordinator struct {
	keys         KeyProvider
	store        ErasureStore
	participants map[string]ErasureCleanup
	required     []string
}

func NewErasureCoordinator(keys KeyProvider, store ErasureStore, options ErasureCoordinatorOptions) (*ErasureCoordinator, error) {
	if keys == nil || store == nil {
		return nil, &Error{Code: CodeBadHandle, Message: "erasure coordinator requires keys and a store"}
	}
	participants := make(map[string]ErasureCleanup, len(options.Participants))
	for name, cleanup := range options.Participants {
		participants[name] = cleanup
	}
	required := append([]string(nil), options.RequiredParticipants...)
	if options.RequiredParticipants == nil {
		for name := range participants {
			required = append(required, name)
		}
	}
	sort.Strings(required)
	registered := make([]string, 0, len(participants))
	for name := range participants { registered = append(registered, name) }
	if err := kernel.ErasureValidateParticipants(required, registered); err != nil { return nil, err }
	return &ErasureCoordinator{keys: keys, store: store, participants: participants, required: required}, nil
}
func (c *ErasureCoordinator) Erase(ctx context.Context, scope KeyScope, request string) (ErasureOperation, error) {
	record, err := kernel.ErasureCreate(scope, kernel.Guid(), request, c.required)
	if err != nil {
		return ErasureOperation{}, err
	}
	candidate, err := ParseErasureOperation(record)
	if err != nil {
		return ErasureOperation{}, err
	}
	stored, err := c.store.Create(ctx, candidate)
	if err != nil {
		return ErasureOperation{}, err
	}
	return c.Resume(ctx, stored.Operation.Scope(), stored.Operation.RequestID())
}
func (c *ErasureCoordinator) Status(ctx context.Context, scope KeyScope, request string) (*ErasureOperation, error) {
	stored, err := c.store.Get(ctx, scope, request)
	if err != nil || stored == nil {
		return nil, err
	}
	operation := stored.Operation
	return &operation, nil
}
func (c *ErasureCoordinator) require(ctx context.Context, scope KeyScope, request string) (StoredErasure, error) {
	stored, err := c.store.Get(ctx, scope, request)
	if err != nil {
		return StoredErasure{}, err
	}
	if stored == nil {
		return StoredErasure{}, &Error{Code: CodeBadHandle, Message: "erasure operation does not exist"}
	}
	return *stored, nil
}
func (c *ErasureCoordinator) Acknowledge(ctx context.Context, scope KeyScope, request, participant string) (ErasureOperation, error) {
	for {
		stored, err := c.require(ctx, scope, request)
		if err != nil {
			return ErasureOperation{}, err
		}
		next, err := c.store.Advance(ctx, stored, "acknowledge", &participant)
		if errors.Is(err, ErrConcurrency) {
			continue
		}
		return next.Operation, err
	}
}

// Resume advances only confirmed effects, preserving the last durable stage on provider or cleanup failure.
// Every pending cleanup is attempted and its success persisted before the first cleanup error is returned.
func (c *ErasureCoordinator) Resume(ctx context.Context, scope KeyScope, request string) (ErasureOperation, error) {
	var cleanupError error
	for {
		stored, err := c.require(ctx, scope, request)
		if err != nil {
			return ErasureOperation{}, err
		}
		operation := stored.Operation
		action := ""
		switch operation.Stage() {
		case "persisted":
			status, err := c.keys.Revoke(ctx, operation.Scope())
			if err != nil {
				return ErasureOperation{}, err
			}
			confirmed, err := kernel.ErasureProviderConfirmed("revoke", status)
			if err != nil {
				return ErasureOperation{}, err
			}
			if !confirmed {
				return operation, nil
			}
			action = "revoke"
		case "revoked":
			action = "notify"
		case "notified":
			acknowledged := map[string]bool{}
			for _, name := range operation.Acknowledgements() {
				acknowledged[name] = true
			}
			conflict := false
			for _, name := range operation.RequiredParticipants() {
				cleanup := c.participants[name]
				if acknowledged[name] || cleanup == nil {
					continue
				}
				if err := cleanup(ctx, operation.Scope()); err != nil {
					if cleanupError == nil {
						cleanupError = err
					}
					continue
				}
				next, err := c.store.Advance(ctx, stored, "acknowledge", &name)
				if errors.Is(err, ErrConcurrency) {
					conflict = true
					break
				}
				if err != nil {
					return ErasureOperation{}, err
				}
				stored = next
			}
			if conflict {
				continue
			}
			if cleanupError != nil {
				return ErasureOperation{}, cleanupError
			}
			operation = stored.Operation
			canDestroy, err := kernel.ErasureCanDestroy(operation.record)
			if err != nil {
				return ErasureOperation{}, err
			}
			if !canDestroy {
				return operation, nil
			}
			status, err := c.keys.Destroy(ctx, operation.Scope())
			if err != nil {
				return ErasureOperation{}, err
			}
			confirmed, err := kernel.ErasureProviderConfirmed("destroy", status)
			if err != nil {
				return ErasureOperation{}, err
			}
			if !confirmed {
				return operation, nil
			}
			action = "destroy"
		case "destruction_confirmed":
			action = "complete"
		case "complete":
			if cleanupError != nil {
				return ErasureOperation{}, cleanupError
			}
			return operation, nil
		default:
			return ErasureOperation{}, invalidDecision("erasure stage " + operation.Stage())
		}
		_, err = c.store.Advance(ctx, stored, action, nil)
		if errors.Is(err, ErrConcurrency) {
			continue
		}
		if err != nil {
			return ErasureOperation{}, err
		}
	}
}

var _ ErasureStore = (*MemoryErasureStore)(nil)

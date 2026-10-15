// Package memory adapts the Rust memory stores to the Go SDK's context transactions.
package memory

import (
	"context"
	"encoding/json"
	"sync"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// Database owns the shared journal. Its stores commit together in WithinTransaction.
type Database struct{ native *kernel.MemoryTransactor }

// New creates an empty journal. Close the database and each store view when finished.
func New() *Database { return &Database{native: kernel.GetMemoryTransactor()} }

// Close releases this native handle. Other retained handles keep their own ownership.
func (d *Database) Close() error { return d.native.Close() }

// UnitOfWork returns this database as the transaction owner.
func (d *Database) UnitOfWork() edax.UnitOfWork { return d }

// Begin allocates a transaction. The caller must commit or roll it back.
func (d *Database) Begin(ctx context.Context) (edax.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	token, err := d.native.Begin()
	if err != nil {
		return nil, err
	}
	return &transaction{database: d, token: token}, nil
}

type transaction struct {
	mu       sync.Mutex
	database *Database
	token    int64
	closed   bool
}

// Commit atomically publishes staged writes and refuses an already settled transaction.
func (t *transaction) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.closed {
		return &edax.Error{Code: edax.CodeBadHandle, Message: "transaction is closed"}
	}
	if err := t.database.native.Commit(t.token); err != nil {
		return err
	}
	t.closed = true
	return nil
}

// Rollback discards staged writes. Repeated rollback after settlement is harmless.
func (t *transaction) Rollback(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	_, err := t.database.native.Rollback(t.token)
	if err == nil {
		t.closed = true
	}
	return err
}
func (d *Database) token(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	active, ok := edax.CurrentTransaction(ctx, d)
	if !ok {
		return kernel.NoTx(), nil
	}
	tx, ok := active.(*transaction)
	if !ok || tx.database != d {
		return 0, &edax.Error{Code: edax.CodeBadHandle, Message: "transaction belongs to another database"}
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.closed {
		return 0, &edax.Error{Code: edax.CodeBadHandle, Message: "transaction is closed"}
	}
	return tx.token, nil
}

// EventStore is a concurrency-safe view of the shared memory journal. Close it when finished.
type EventStore struct {
	database *Database
	native   *kernel.MemoryEventStore
}

// EventStore opens a separately owned event-log view of this database.
func (d *Database) EventStore() (*EventStore, error) {
	h, e := d.native.EventStore()
	if e != nil {
		return nil, e
	}
	return &EventStore{d, h}, nil
}

// Close releases this native handle. Other retained handles keep their own ownership.
func (s *EventStore) Close() error { return s.native.Close() }

// UnitOfWork identifies the database whose transaction this view joins.
func (s *EventStore) UnitOfWork() edax.UnitOfWork { return s.database }

// Load reads events after the supplied stream version, including staged events in ctx.
func (s *EventStore) Load(ctx context.Context, key string, after uint64) ([]edax.EventMessage, error) {
	t, e := s.database.token(ctx)
	if e != nil {
		return nil, e
	}
	return s.native.Load(t, key, after)
}

// Append checks the expected stream version and atomically stages events and outbox rows.
func (s *EventStore) Append(ctx context.Context, key string, expected uint64, drafts []edax.EventDraft) ([]edax.EventMessage, error) {
	t, e := s.database.token(ctx)
	if e != nil {
		return nil, e
	}
	return s.native.Append(t, key, expected, drafts, edax.CurrentTimestamp())
}

// LoadSnapshot returns the cached aggregate snapshot, or nil when absent.
func (s *EventStore) LoadSnapshot(ctx context.Context, key string) (*edax.Snapshot, error) {
	t, e := s.database.token(ctx)
	if e != nil {
		return nil, e
	}
	row, e := s.native.LoadSnapshot(t, key)
	if e != nil || row == nil {
		return nil, e
	}
	return &edax.Snapshot{AggregateID: key, TypeName: row.TypeName, Version: row.Version, Data: row.Data}, nil
}

// SaveSnapshot replaces the aggregate cache in the context transaction, when present.
func (s *EventStore) SaveSnapshot(ctx context.Context, row edax.Snapshot) error {
	t, e := s.database.token(ctx)
	if e != nil {
		return e
	}
	return s.native.SaveSnapshot(t, row.AggregateID, row.TypeName, row.Version, row.Data)
}

// Unpublished reads at most limit committed outbox rows; staged writes are not published.
func (s *EventStore) Unpublished(ctx context.Context, limit uint64) ([]edax.EventMessage, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	return s.native.Unpublished(limit)
}

// MarkPublished acknowledges committed outbox rows. Repeated marks are harmless.
func (s *EventStore) MarkPublished(ctx context.Context, ids []string) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	return s.native.MarkPublished(ids)
}

// UnpublishedCount returns the number of committed rows awaiting publication.
func (s *EventStore) UnpublishedCount(ctx context.Context) (uint64, error) {
	if e := ctx.Err(); e != nil {
		return 0, e
	}
	return s.native.UnpublishedCount()
}

// IdempotencyStore is a concurrency-safe view of the shared memory journal. Close it when finished.
type IdempotencyStore struct {
	database *Database
	native   *kernel.MemoryIdempotencyStore
}

// IdempotencyStore opens a separately owned ledger view of this database.
func (d *Database) IdempotencyStore() (*IdempotencyStore, error) {
	h, e := d.native.IdempotencyStore()
	if e != nil {
		return nil, e
	}
	return &IdempotencyStore{d, h}, nil
}

// Close releases this native handle. Other retained handles keep their own ownership.
func (s *IdempotencyStore) Close() error { return s.native.Close() }

// UnitOfWork identifies the database whose transaction this view joins.
func (s *IdempotencyStore) UnitOfWork() edax.UnitOfWork { return s.database }

// Claim atomically acquires a new key or returns its prior idempotency record.
func (s *IdempotencyStore) Claim(ctx context.Context, key string, hash uint64) (*edax.ClaimResult, error) {
	t, e := s.database.token(ctx)
	if e != nil {
		return nil, e
	}
	return s.native.Claim(t, key, hash)
}

// Complete stores the result only if handle still owns the pending claim.
func (s *IdempotencyStore) Complete(ctx context.Context, key string, handle []byte, record edax.IdempotencyRecord) error {
	t, e := s.database.token(ctx)
	if e != nil {
		return e
	}
	wire, e := edax.EncodeRecord(record)
	if e != nil {
		return e
	}
	return s.native.Complete(t, key, handle, wire)
}

// Release removes a pending claim only if handle still owns it.
func (s *IdempotencyStore) Release(ctx context.Context, key string, handle []byte) error {
	t, e := s.database.token(ctx)
	if e != nil {
		return e
	}
	return s.native.Release(t, key, handle)
}

// Forget removes a settled record so the operation may run again.
func (s *IdempotencyStore) Forget(ctx context.Context, key string) error {
	t, e := s.database.token(ctx)
	if e != nil {
		return e
	}
	return s.native.Forget(t, key)
}

// Expire simulates lease or retention expiry by removing committed state; staged writes remain.
func (s *IdempotencyStore) Expire(key string) (bool, error) { return s.native.Expire(key) }

// ProjectionStore is a concurrency-safe view of one read model in the shared memory journal. Close it
// when finished.
type ProjectionStore struct {
	database   *Database
	native     *kernel.MemoryProjectionStore
	projection string
}

// ProjectionStore opens a separately owned view of the read model named projection. Read models of one
// database share no row, as the Postgres projections table keys each by its name, so two keyed by one id
// never overwrite each other.
func (d *Database) ProjectionStore(projection string) (*ProjectionStore, error) {
	h, e := d.native.ProjectionStore()
	if e != nil {
		return nil, e
	}
	return &ProjectionStore{d, h, projection}, nil
}

// Close releases this native handle. Other retained handles keep their own ownership.
func (s *ProjectionStore) Close() error { return s.native.Close() }

// UnitOfWork identifies the database whose transaction this view joins.
func (s *ProjectionStore) UnitOfWork() edax.UnitOfWork { return s.database }

// Get reads a projection and its version, returning nil when absent.
func (s *ProjectionStore) Get(ctx context.Context, id string) (*edax.ProjectionRow, error) {
	t, e := s.database.token(ctx)
	if e != nil {
		return nil, e
	}
	return s.native.Get(t, s.projection, id)
}

// Save checks the expected version and stages the replacement state atomically.
func (s *ProjectionStore) Save(ctx context.Context, id string, version uint64, document string) (uint64, error) {
	t, e := s.database.token(ctx)
	if e != nil {
		return 0, e
	}
	return s.native.Save(t, s.projection, id, version, document)
}

// GetMany applies the kernel query to the context transaction’s projection view.
func (s *ProjectionStore) GetMany(ctx context.Context, query string) (string, error) {
	t, e := s.database.token(ctx)
	if e != nil {
		return "", e
	}
	return s.native.GetMany(t, s.projection, query)
}

// SagaStore is a concurrency-safe view of the shared memory journal. Close it when finished.
type SagaStore struct {
	database *Database
	native   *kernel.MemorySagaStore
}

// SagaStore opens a separately owned saga view of this database.
func (d *Database) SagaStore() (*SagaStore, error) {
	h, e := d.native.SagaStore()
	if e != nil {
		return nil, e
	}
	return &SagaStore{d, h}, nil
}

// Close releases this native handle. Other retained handles keep their own ownership.
func (s *SagaStore) Close() error { return s.native.Close() }

// UnitOfWork identifies the database whose transaction this view joins.
func (s *SagaStore) UnitOfWork() edax.UnitOfWork { return s.database }

// Load reads committed state or this context transaction’s staged state.
func (s *SagaStore) Load(ctx context.Context, saga, id string) (*edax.SagaInstance, error) {
	t, e := s.database.token(ctx)
	if e != nil {
		return nil, e
	}
	row, e := s.native.Load(t, saga, id)
	if e != nil || row == nil {
		return nil, e
	}
	return &edax.SagaInstance{Version: row.Version, Completed: row.Completed, State: json.RawMessage(row.StateJSON), Folded: row.Folded}, nil
}

// Save checks the expected version and stages the replacement state atomically.
func (s *SagaStore) Save(ctx context.Context, saga, id string, state json.RawMessage, expected uint64, folded string, commands []edax.CommandMessage) (uint64, error) {
	t, e := s.database.token(ctx)
	if e != nil {
		return 0, e
	}
	return s.native.Save(t, saga, id, expected, string(state), folded, commands)
}

// Complete atomically writes a completion tombstone and its commands at the expected version.
func (s *SagaStore) Complete(ctx context.Context, saga, id string, expected uint64, commands []edax.CommandMessage) (uint64, error) {
	t, e := s.database.token(ctx)
	if e != nil {
		return 0, e
	}
	return s.native.Complete(t, saga, id, expected, commands)
}

// Unpublished reads at most limit committed outbox rows; staged writes are not published.
func (s *SagaStore) Unpublished(ctx context.Context, saga string, limit uint64) ([]edax.CommandMessage, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	return s.native.Unpublished(saga, limit)
}

// MarkPublished acknowledges committed outbox rows. Repeated marks are harmless.
func (s *SagaStore) MarkPublished(ctx context.Context, saga string, ids []string) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	return s.native.MarkPublished(saga, ids)
}

// CommandOutbox is a concurrency-safe view of the shared memory journal. Close it when finished.
type CommandOutbox struct {
	database *Database
	native   *kernel.MemoryCommandOutbox
}

// CommandOutbox opens a separately owned command-outbox view of this database.
func (d *Database) CommandOutbox() (*CommandOutbox, error) {
	h, e := d.native.CommandOutbox()
	if e != nil {
		return nil, e
	}
	return &CommandOutbox{d, h}, nil
}

// Close releases this native handle. Other retained handles keep their own ownership.
func (s *CommandOutbox) Close() error { return s.native.Close() }

// UnitOfWork identifies the database whose transaction this view joins.
func (s *CommandOutbox) UnitOfWork() edax.UnitOfWork { return s.database }

// Enqueue stages commands atomically in the context transaction, when present.
func (s *CommandOutbox) Enqueue(ctx context.Context, messages []edax.CommandMessage) error {
	t, e := s.database.token(ctx)
	if e != nil {
		return e
	}
	return s.native.Enqueue(t, messages)
}

// Unpublished reads at most limit committed outbox rows; staged writes are not published.
func (s *CommandOutbox) Unpublished(ctx context.Context, limit uint64) ([]edax.CommandMessage, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	return s.native.Unpublished(limit)
}

// MarkPublished acknowledges committed outbox rows. Repeated marks are harmless.
func (s *CommandOutbox) MarkPublished(ctx context.Context, ids []string) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	return s.native.MarkPublished(ids)
}

var _ edax.EventStore = (*EventStore)(nil)
var _ edax.IdempotencyStore = (*IdempotencyStore)(nil)
var _ edax.ProjectionStore = (*ProjectionStore)(nil)
var _ edax.SagaStore = (*SagaStore)(nil)
var _ edax.CommandOutbox = (*CommandOutbox)(nil)

// UnpublishedCount returns the number of committed rows awaiting publication.
func (s *CommandOutbox) UnpublishedCount(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return s.native.UnpublishedCount()
}

// UnpublishedCount returns the number of committed rows awaiting publication.
func (s *SagaStore) UnpublishedCount(ctx context.Context, saga string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return s.native.UnpublishedCount(saga)
}

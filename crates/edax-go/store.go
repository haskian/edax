package edax

import (
	"context"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
	// Snapshot stores an aggregate at a committed stream version.
)

type Snapshot struct {
    Tenant *string
    Subject *string
    Encryption *EncryptionInfo
	AggregateID string
	TypeName    string
	Version     uint64
	Data        []byte
}

// TransactionalStore identifies the database whose context transaction it joins.
type TransactionalStore interface{ UnitOfWork() UnitOfWork }

// EventStore atomically appends dense event batches and their outbox rows.
type EventStore interface {
	TransactionalStore
	Load(ctx context.Context, aggregateID string, afterVersion uint64) ([]EventMessage, error)
	Append(ctx context.Context, aggregateID string, expectedVersion uint64, drafts []EventDraft) ([]EventMessage, error)
	LoadSnapshot(ctx context.Context, aggregateID string) (*Snapshot, error)
	SaveSnapshot(ctx context.Context, snapshot Snapshot) error
}

// IdempotencyStore claims keys atomically. Only the current claim handle may settle a key.
type IdempotencyStore interface {
	Claim(ctx context.Context, key string, payloadHash uint64) (*ClaimResult, error)
	Complete(ctx context.Context, key string, handle []byte, record IdempotencyRecord) error
	Release(ctx context.Context, key string, handle []byte) error
	Forget(ctx context.Context, key string) error
}

// ProjectionStore stores versioned JSON documents and applies kernel query semantics.
type ProjectionStore interface {
	TransactionalStore
	Get(ctx context.Context, id string) (*ProjectionRow, error)
	Save(ctx context.Context, id string, expectedVersion uint64, documentJSON string) (uint64, error)
	GetMany(ctx context.Context, queryJSON string) (string, error)
}

// CommandOutbox stages commands in the context's transaction.
type CommandOutbox interface {
	TransactionalStore
	Enqueue(ctx context.Context, commands []CommandMessage) error
	Unpublished(ctx context.Context, limit uint64) ([]CommandMessage, error)
	MarkPublished(ctx context.Context, ids []string) error
}

// EncodeRecord preserves the kernel's ledger format, including full-width hashes.
func EncodeRecord(record IdempotencyRecord) (string, error) {
	result := "null"
	if record.ResultJSON != nil {
		result = *record.ResultJSON
	}
	return kernel.EncodeIdempotencyRecord(record.PayloadHash, record.State == "pending", result, record.Error, record.ErrorCode)
}

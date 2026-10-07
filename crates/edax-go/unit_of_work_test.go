package edax_test

import (
	"context"
	"errors"
	"testing"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
)

type cleanupOwner struct{ tx *cleanupTx }

func (o *cleanupOwner) Begin(context.Context) (edax.Tx, error) { return o.tx, nil }

type cleanupTx struct {
	deadline bool
	canceled bool
	commits  int
}

func (t *cleanupTx) Commit(context.Context) error { t.commits++; return nil }
func (t *cleanupTx) Rollback(ctx context.Context) error {
	limit, ok := ctx.Deadline()
	t.deadline = ok && time.Until(limit) > 0 && time.Until(limit) <= 5*time.Second
	t.canceled = ctx.Err() != nil
	return nil
}
func TestRollbackHasIndependentDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx := new(cleanupTx)
	err := edax.WithinTransaction(ctx, &cleanupOwner{tx}, func(context.Context) error { cancel(); return context.Canceled })
	if !errors.Is(err, context.Canceled) || !tx.deadline || tx.canceled || tx.commits != 0 {
		t.Fatalf("error %v cleanup %+v", err, tx)
	}
}

type malformedProjection struct{ edax.ProjectionStore }

func (malformedProjection) Get(context.Context, string) (*edax.ProjectionRow, error) {
	return &edax.ProjectionRow{Version: 1, DocumentJSON: `{"id":"invoice-1042","amount":"12.00"} {"id":"unexpected"}`}, nil
}
func TestProjectionRejectsTrailingJSON(t *testing.T) {
	_, err := edax.LoadEntity(context.Background(), malformedProjection{}, "invoice-1042")
	if !errors.Is(err, edax.ErrCodec) {
		t.Fatalf("malformed document accepted: %v", err)
	}
}

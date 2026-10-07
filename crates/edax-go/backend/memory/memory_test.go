package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

func draft(id string, sequence uint64) edax.EventDraft {
	return edax.EventDraft{ID: id, AggregateID: "account-42", Name: "Deposited", Version: 1, Sequence: &sequence, Data: []byte(`{"amount":125,"currency":"EUR"}`), CorrelationID: "deposit-request-7", CausationID: "deposit-request-7"}
}

func TestTransactionAtomicity(t *testing.T) {
	for _, test := range []struct {
		name string
		fail bool
	}{{"commit", false}, {"rollback", true}} {
		t.Run(test.name, func(t *testing.T) {
			db := memory.New()
			defer db.Close()
			events, err := db.EventStore()
			if err != nil {
				t.Fatal(err)
			}
			defer events.Close()
			ledger, err := db.IdempotencyStore()
			if err != nil {
				t.Fatal(err)
			}
			defer ledger.Close()
			ctx := context.Background()
			failure := errors.New("handler refused deposit")
			err = edax.WithinTransaction(ctx, db, func(ctx context.Context) error {
				claim, err := ledger.Claim(ctx, "accounts/deposit-request-7", 42)
				if err != nil {
					return err
				}
				if !claim.Won {
					t.Fatal("new claim lost")
				}
				if _, err = events.Append(ctx, "account-42", 0, []edax.EventDraft{draft("deposit-7", 1)}); err != nil {
					return err
				}
				rows, err := events.Load(ctx, "account-42", 0)
				if err != nil || len(rows) != 1 {
					t.Fatalf("own writes: %v %v", rows, err)
				}
				hidden, err := events.Load(context.Background(), "account-42", 0)
				if err != nil || len(hidden) != 0 {
					t.Fatalf("uncommitted visible: %v %v", hidden, err)
				}
				result := "125"
				if err = ledger.Complete(ctx, "accounts/deposit-request-7", claim.Handle, edax.IdempotencyRecord{PayloadHash: 42, State: "done", ResultJSON: &result}); err != nil {
					return err
				}
				if test.fail {
					return failure
				}
				return nil
			})
			if test.fail && !errors.Is(err, failure) {
				t.Fatal(err)
			}
			if !test.fail && err != nil {
				t.Fatal(err)
			}
			rows, err := events.Load(ctx, "account-42", 0)
			if err != nil {
				t.Fatal(err)
			}
			wanted := 1
			if test.fail {
				wanted = 0
			}
			if len(rows) != wanted {
				t.Fatalf("events %d want %d", len(rows), wanted)
			}
			pending, err := events.Unpublished(ctx, 10)
			if err != nil || len(pending) != wanted {
				t.Fatalf("outbox %d: %v", len(pending), err)
			}
			claim, err := ledger.Claim(ctx, "accounts/deposit-request-7", 42)
			if err != nil {
				t.Fatal(err)
			}
			if claim.Won != test.fail {
				t.Fatalf("claim won %v after %s", claim.Won, test.name)
			}
		})
	}
}

func TestContextAndVersionFences(t *testing.T) {
	db := memory.New()
	defer db.Close()
	store, err := db.EventStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var stale context.Context
	if err = edax.WithinTransaction(context.Background(), db, func(ctx context.Context) error {
		stale = ctx
		nested := edax.WithinTransaction(ctx, db, func(context.Context) error { return nil })
		if !errors.Is(nested, edax.ErrBadHandle) {
			t.Fatalf("nested: %v", nested)
		}
		_, err := store.Append(ctx, "account-42", 0, []edax.EventDraft{draft("deposit-7", 1)})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Load(stale, "account-42", 0); !errors.Is(err, edax.ErrBadHandle) {
		t.Fatalf("stale tx: %v", err)
	}
	if _, err = store.Append(context.Background(), "account-42", 0, []edax.EventDraft{draft("deposit-8", 1)}); !errors.Is(err, edax.ErrConcurrency) {
		t.Fatalf("expected concurrency: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = store.Load(ctx, "account-42", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}

func TestBrokerNackRetiresLaterDelivery(t *testing.T) {
	broker, err := memory.NewBroker(1, "silo")
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, item := range []edax.EventDraft{draft("deposit-7", 1), draft("deposit-8", 2)} {
		event, err := kernel.RecordEvent(item, 1)
		if err != nil {
			t.Fatal(err)
		}
		if err = broker.PublishEvent(ctx, *event); err != nil {
			t.Fatal(err)
		}
	}
	first, _, err := broker.Poll(ctx, "projection")
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := broker.Poll(ctx, "projection")
	if err != nil {
		t.Fatal(err)
	}
	if err = first.Nack(ctx); err != nil {
		t.Fatal(err)
	}
	if second.IsLive() {
		t.Fatal("nack left later token live")
	}
	replay, _, err := broker.Poll(ctx, "projection")
	if err != nil {
		t.Fatal(err)
	}
	if replay.Message.ID != first.Message.ID {
		t.Fatalf("wrong replay: %s", replay.Message.ID)
	}
}

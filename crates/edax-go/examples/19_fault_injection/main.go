// Lose a reply after commit, then prove a transactional event guard absorbs redelivery.
// Run from crates/edax-go: mise exec -- go run ./examples/19_fault_injection
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
)

type deposited struct {
	Amount int `json:"amount"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: the lost reply caused redelivery; the inner guard kept the balance at 100")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	codec, err := edax.NewJSONCodec(map[string]func() any{"Deposited": func() any { return new(deposited) }})
	if err != nil {
		return err
	}
	db, ledgerDB := memory.New(), memory.New()
	defer func() { err = errors.Join(err, db.Close()) }()
	defer func() { err = errors.Join(err, ledgerDB.Close()) }()
	balances, err := db.ProjectionStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, balances.Close()) }()
	innerLedger, err := db.IdempotencyStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, innerLedger.Close()) }()
	outerLedger, err := ledgerDB.IdempotencyStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, outerLedger.Close()) }()
	outer, err := edax.IdempotentByID(struct{ edax.IdempotencyStore }{outerLedger}, edax.IdempotentOptions{Scope: "balances"})
	if err != nil {
		return err
	}
	inner, err := edax.IdempotentByID(innerLedger, edax.IdempotentOptions{Scope: "balances-fold"})
	if err != nil {
		return err
	}
	deliveries, applied := 0, 0
	loseReply := true
	// This test seam fails after the database commits. The caller sees the same
	// error as a failed write, though both the balance and inner claim are durable.
	commitWithLostReply := func(next edax.EventHandler) edax.EventHandler {
		return func(ctx context.Context, event edax.EventMessage) error {
			deliveries++
			if err := edax.WithinTransaction(ctx, db, func(ctx context.Context) error { return next(ctx, event) }); err != nil {
				return err
			}
			if loseReply {
				loseReply = false
				return &edax.Error{Code: edax.CodeStore, Message: "commit landed; reply was lost"}
			}
			return nil
		}
	}
	router := edax.NewEventRouter(outer, commitWithLostReply, inner)
	if err := router.Register("Deposited", func(ctx context.Context, event edax.EventMessage) error {
		applied++
		row, err := balances.Get(ctx, event.AggregateID)
		if err != nil {
			return err
		}
		var document struct {
			Balance int `json:"balance"`
		}
		var version uint64
		if row != nil {
			version = row.Version
			if err := json.Unmarshal([]byte(row.DocumentJSON), &document); err != nil {
				return err
			}
		}
		decoded, err := codec.Unmarshal(edax.Payload{Name: event.Name, Version: event.Version, Data: event.Data})
		if err != nil {
			return err
		}
		deposit, ok := decoded.(*deposited)
		if !ok {
			return fmt.Errorf("unexpected deposit type %T", decoded)
		}
		document.Balance += deposit.Amount
		encoded, err := json.Marshal(document)
		if err != nil {
			return err
		}
		_, err = balances.Save(ctx, event.AggregateID, version, string(encoded))
		return err
	}); err != nil {
		return err
	}
	payload, err := codec.Marshal(deposited{Amount: 100})
	if err != nil {
		return err
	}
	event := edax.EventMessage{ID: "e1", AggregateID: "acct-1", Name: payload.Name, Version: payload.Version, Data: payload.Data, CorrelationID: "e1", CausationID: "e1", RecordedAt: edax.CurrentTimestamp()}
	if err := router.Dispatch(ctx, event); !errors.Is(err, edax.ErrStore) {
		return fmt.Errorf("lost reply: %v", err)
	}
	if err := router.Dispatch(ctx, event); err != nil {
		return err
	}
	row, err := balances.Get(ctx, "acct-1")
	if err != nil {
		return err
	}
	if row == nil {
		return fmt.Errorf("balance was not saved")
	}
	var document struct {
		Balance int `json:"balance"`
	}
	if err := json.Unmarshal([]byte(row.DocumentJSON), &document); err != nil {
		return err
	}
	if deliveries != 2 || applied != 1 || document.Balance != 100 || row.Version != 1 {
		return fmt.Errorf("deliveries=%d applied=%d balance=%d version=%d", deliveries, applied, document.Balance, row.Version)
	}
	return nil
}

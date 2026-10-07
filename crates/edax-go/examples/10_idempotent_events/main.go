// Commit an event's idempotency claim and read-model change in one transaction.
// Run from crates/edax-go: mise exec -- go run ./examples/10_idempotent_events
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
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
	fmt.Println("ok: balance 150; duplicate skipped; failed write and claim rolled back together")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db := memory.New()
	defer func() { err = errors.Join(err, db.Close()) }()
	balances, err := db.ProjectionStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, balances.Close()) }()
	ledger, err := db.IdempotencyStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, ledger.Close()) }()
	codec, err := edax.NewJSONCodec(map[string]func() any{"Deposited": func() any { return new(deposited) }})
	if err != nil {
		return err
	}
	guard, err := edax.IdempotentByID(ledger, edax.IdempotentOptions{Scope: "balances", Tenancy: edax.TenancySilo, UnitOfWork: db})
	if err != nil {
		return err
	}
	router := edax.NewEventRouter(guard)
	var runs []string
	auditDown := true
	if err := router.Register("Deposited", func(ctx context.Context, event edax.EventMessage) error {
		runs = append(runs, event.ID)
		row, err := edax.LoadEntity(ctx, balances, event.AggregateID)
		if err != nil {
			return err
		}
		balance := int64(0)
		if row == nil {
			row = &edax.Entity{ID: event.AggregateID}
		} else {
			amount, ok := row.Document["balance"].(json.Number)
			if !ok {
				return errors.New("balance is not a JSON number")
			}
			balance, err = amount.Int64()
			if err != nil {
				return err
			}
		}
		value, err := codec.Unmarshal(edax.Payload{Name: event.Name, Version: event.Version, Data: event.Data})
		if err != nil {
			return err
		}
		deposit, ok := value.(*deposited)
		if !ok {
			return fmt.Errorf("unexpected deposit type %T", value)
		}
		row.Document = edax.Document{"balance": balance + int64(deposit.Amount)}
		if _, err := edax.SaveEntity(ctx, balances, *row); err != nil {
			return err
		}
		if event.ID == "e2" && auditDown {
			auditDown = false
			return &edax.Error{Code: edax.CodeStore, Message: "audit log unreachable"}
		}
		return nil
	}); err != nil {
		return err
	}
	deposit := func(id string, amount int) (edax.EventMessage, error) {
		payload, err := codec.Marshal(&deposited{Amount: amount})
		return edax.EventMessage{ID: id, AggregateID: "acct-1", Name: payload.Name, Version: payload.Version, Data: payload.Data, CorrelationID: "deposits", CausationID: "deposit-command", RecordedAt: edax.CurrentTimestamp()}, err
	}
	first, err := deposit("e1", 100)
	if err != nil {
		return err
	}
	second, err := deposit("e2", 50)
	if err != nil {
		return err
	}
	for range 2 {
		if err := router.Dispatch(ctx, first); err != nil {
			return err
		}
	}
	if err := router.Dispatch(ctx, second); !errors.Is(err, edax.ErrStore) {
		return fmt.Errorf("expected audit failure, got %v", err)
	}
	row, err := edax.LoadEntity(ctx, balances, "acct-1")
	if err != nil {
		return err
	}
	if row == nil || row.Document["balance"] != json.Number("100") {
		return fmt.Errorf("failed write did not roll back: %+v", row)
	}
	fmt.Println("e1 delivered twice; e2 rolled back: balance 100")
	if err := router.Dispatch(ctx, second); err != nil {
		return err
	}
	row, err = edax.LoadEntity(ctx, balances, "acct-1")
	if err != nil {
		return err
	}
	if row == nil || row.Document["balance"] != json.Number("150") || !slices.Equal(runs, []string{"e1", "e2", "e2"}) {
		return fmt.Errorf("balance=%+v handler runs=%v", row, runs)
	}
	return nil
}

// Old events load with the new schema without changing stored bytes.
// Run from crates/edax-go: mise exec -- go run ./examples/04_upcasting
package main

import (
	"bytes"
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
	Amount   int    `json:"amount"`
	Currency string `json:"currency"`
}
type account struct {
	edax.Aggregate
	Balances map[string]int
}

func (a *account) Transition(event any) error {
	deposit, ok := event.(*deposited)
	if !ok {
		return fmt.Errorf("unexpected account event %T", event)
	}
	a.Balances[deposit.Currency] += deposit.Amount
	return nil
}

type accountCodec struct{}

func (accountCodec) TypeName() string { return "Account" }
func (accountCodec) Load(id string, _ uint64, data []byte) (*account, error) {
	a := &account{Aggregate: edax.NewAggregate(id), Balances: make(map[string]int)}
	if data != nil {
		if err := json.Unmarshal(data, &a.Balances); err != nil {
			return nil, err
		}
	}
	return a, nil
}
func (accountCodec) Dump(a *account) ([]byte, error) { return json.Marshal(a.Balances) }

// Every deposit written by version 1 was in euros.
func addCurrency(data []byte) ([]byte, error) {
	var deposit deposited
	if err := json.Unmarshal(data, &deposit); err != nil {
		return nil, err
	}
	deposit.Currency = "EUR"
	return json.Marshal(deposit)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: Old events load with the new schema without changing stored bytes.")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := memory.New()
	defer func() { err = errors.Join(err, db.Close()) }()
	store, err := db.EventStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	oldData := []byte(`{"amount":100}`)
	draft := edax.EventDraft{ID: "e1", AggregateID: "acct-1", Sequence: edax.Ptr(uint64(1)), Name: "Deposited", Version: 1, Data: oldData, CorrelationID: "e1", CausationID: "e1"}
	if err := edax.WithinTransaction(ctx, db, func(ctx context.Context) error {
		_, err := store.Append(ctx, "acct-1", 0, []edax.EventDraft{draft})
		return err
	}); err != nil {
		return err
	}
	events, err := edax.NewJSONCodec(map[string]func() any{"Deposited": func() any { return new(deposited) }})
	if err != nil {
		return err
	}
	if err := events.WithUpcaster("Deposited", &edax.Upcaster{Steps: map[uint32]func([]byte) ([]byte, error){1: addCurrency}}); err != nil {
		return err
	}
	repo, err := edax.NewRepository(store, events, accountCodec{}, edax.RepositoryOptions{SnapshotEvery: edax.Ptr(uint64(0))})
	if err != nil {
		return err
	}
	a, err := repo.Load(ctx, "acct-1", nil)
	if err != nil {
		return err
	}
	if a.Balances["EUR"] != 100 {
		return fmt.Errorf("old deposit was not upcast: %v", a.Balances)
	}
	if err := edax.Apply(a, &deposited{Amount: 50, Currency: "USD"}); err != nil {
		return err
	}
	saved, err := repo.Save(ctx, a, edax.SaveOptions{})
	if err != nil {
		return err
	}
	if len(saved) != 1 || saved[0].Version != 2 {
		return fmt.Errorf("new deposit did not use schema version 2")
	}
	stored, err := store.Load(ctx, "acct-1", 0)
	if err != nil {
		return err
	}
	if len(stored) != 2 || stored[0].Version != 1 || !bytes.Equal(stored[0].Data, oldData) {
		return fmt.Errorf("upcasting changed the original stored event")
	}
	loaded, err := repo.Load(ctx, "acct-1", nil)
	if err != nil {
		return err
	}
	if len(loaded.Balances) != 2 || loaded.Balances["EUR"] != 100 || loaded.Balances["USD"] != 50 || loaded.Version().Current() != 2 {
		return fmt.Errorf("replayed balances=%v version=%d", loaded.Balances, loaded.Version().Current())
	}
	fmt.Printf("stored versions 1 and 2; replayed balances %v\n", loaded.Balances)

	return nil
}

// Two saved events rebuild the account.
// Run from crates/edax-go: mise exec -- go run ./examples/02_aggregate
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
)

type deposited struct {
	Amount int `json:"amount"`
}
type withdrawn struct {
	Amount int `json:"amount"`
}
type account struct {
	edax.Aggregate
	Balance int
}

func (a *account) Transition(event any) error {
	switch event := event.(type) {
	case *deposited:
		a.Balance += event.Amount
	case *withdrawn:
		a.Balance -= event.Amount
	default:
		return fmt.Errorf("unexpected account event %T", event)
	}
	return nil
}

type accountCodec struct{}

func (accountCodec) TypeName() string { return "Account" }
func (accountCodec) Load(id string, _ uint64, data []byte) (*account, error) {
	a := &account{Aggregate: edax.NewAggregate(id)}
	if data != nil {
		value, err := strconv.Atoi(string(data))
		if err != nil {
			return nil, err
		}
		a.Balance = value
	}
	return a, nil
}
func (accountCodec) Dump(a *account) ([]byte, error) { return []byte(strconv.Itoa(a.Balance)), nil }

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: Two saved events rebuild the account.")
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
	events, err := edax.NewJSONCodec(map[string]func() any{"Deposited": func() any { return new(deposited) }, "Withdrawn": func() any { return new(withdrawn) }})
	if err != nil {
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
	if a.Balance != 0 || a.Version().Current() != 0 {
		return fmt.Errorf("new account is not empty")
	}
	if err := edax.Apply(a, &deposited{100}, &withdrawn{30}); err != nil {
		return err
	}
	if a.Balance != 70 || len(a.Pending()) != 2 {
		return fmt.Errorf("pending events did not change state")
	}
	saved, err := repo.Save(ctx, a, edax.SaveOptions{})
	if err != nil {
		return err
	}
	if len(saved) != 2 || saved[0].Sequence == nil || *saved[0].Sequence != 1 || saved[1].Sequence == nil || *saved[1].Sequence != 2 || len(a.Pending()) != 0 {
		return fmt.Errorf("save did not persist and clear two ordered events")
	}
	loaded, err := repo.Load(ctx, "acct-1", nil)
	if err != nil {
		return err
	}
	if loaded.Balance != 70 || loaded.Version().Current() != 2 {
		return fmt.Errorf("replayed balance=%d version=%d", loaded.Balance, loaded.Version().Current())
	}
	fmt.Printf("replayed balance %d at version %d\n", loaded.Balance, loaded.Version().Current())

	return nil
}

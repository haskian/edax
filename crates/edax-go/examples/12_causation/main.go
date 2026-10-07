// Continue a request's correlation and causation IDs through an event handler.
// Run from crates/edax-go: mise exec -- go run ./examples/12_causation
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

type accountOpened struct {
	Owner string `json:"owner"`
}
type bonusPaid struct {
	Amount int `json:"amount"`
}
type account struct {
	edax.Aggregate
	Balance int
}

func (a *account) Transition(event any) error {
	switch event := event.(type) {
	case *accountOpened:
	case *bonusPaid:
		a.Balance += event.Amount
	default:
		return fmt.Errorf("unknown account event %T", event)
	}
	return nil
}

type accountCodec struct{}

func (accountCodec) TypeName() string { return "Account" }
func (accountCodec) Load(id string, _ uint64, data []byte) (*account, error) {
	a := &account{Aggregate: edax.NewAggregate(id)}
	if data != nil {
		if err := json.Unmarshal(data, &a.Balance); err != nil {
			return nil, err
		}
	}
	return a, nil
}
func (accountCodec) Dump(a *account) ([]byte, error) { return json.Marshal(a.Balance) }

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: one correlation ID; the bonus names its opening event as its cause")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db := memory.New()
	defer func() { err = errors.Join(err, db.Close()) }()
	store, err := db.EventStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	codec, err := edax.NewJSONCodec(map[string]func() any{
		"AccountOpened": func() any { return new(accountOpened) },
		"BonusPaid":     func() any { return new(bonusPaid) },
	})
	if err != nil {
		return err
	}
	repo, err := edax.NewRepository(store, codec, accountCodec{}, edax.RepositoryOptions{})
	if err != nil {
		return err
	}
	a, err := repo.Load(ctx, "acct-1", nil)
	if err != nil {
		return err
	}
	if err := edax.Apply(a, &accountOpened{Owner: "Ann"}); err != nil {
		return err
	}
	// The inbound request starts a flow; its root names itself as its cause.
	root := edax.Lineage{CorrelationID: "req-42", CausationID: "req-42"}
	saved, err := repo.Save(ctx, a, edax.SaveOptions{Lineage: &root})
	if err != nil {
		return err
	}
	if len(saved) != 1 {
		return fmt.Errorf("expected one opening event, got %d", len(saved))
	}
	opened := saved[0]
	var bonuses []edax.EventMessage
	router := edax.NewEventRouter()
	if err := router.Register("AccountOpened", func(ctx context.Context, event edax.EventMessage) error {
		a, err := repo.Load(ctx, event.AggregateID, nil)
		if err != nil {
			return err
		}
		if err := edax.Apply(a, &bonusPaid{Amount: 10}); err != nil {
			return err
		}
		// The router supplies the parent's lineage through ctx.
		bonuses, err = repo.Save(ctx, a, edax.SaveOptions{})
		return err
	}); err != nil {
		return err
	}
	if err := router.Dispatch(ctx, opened); err != nil {
		return err
	}
	if len(bonuses) != 1 {
		return fmt.Errorf("expected one bonus, got %d", len(bonuses))
	}
	bonus := bonuses[0]
	if opened.CorrelationID != "req-42" || opened.CausationID != "req-42" || bonus.CorrelationID != "req-42" || bonus.CausationID != opened.ID {
		return fmt.Errorf("incorrect lineage: opened=%+v bonus=%+v", opened, bonus)
	}
	for _, event := range []edax.EventMessage{opened, bonus} {
		fmt.Printf("%s: correlation=%s causation=%s\n", event.Name, event.CorrelationID, event.CausationID)
	}
	return nil
}

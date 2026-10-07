// Outbox rows survive an outage and publish in order.
// Run from crates/edax-go: mise exec -- go run ./examples/07_outbox_relay
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
type account struct {
	edax.Aggregate
	Balance int
}

func (a *account) Transition(event any) error {
	switch event := event.(type) {
	case *deposited:
		a.Balance += event.Amount

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
	fmt.Println("ok: Outbox rows survive an outage and publish in order.")
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
	events, err := edax.NewJSONCodec(map[string]func() any{"Deposited": func() any { return new(deposited) }})
	if err != nil {
		return err
	}
	repo, err := edax.NewRepository(store, events, accountCodec{}, edax.RepositoryOptions{SnapshotEvery: edax.Ptr(uint64(1))})
	if err != nil {
		return err
	}

	a, err := repo.Load(ctx, "acct-1", nil)
	if err != nil {
		return err
	}
	if err := edax.Apply(a, &deposited{100}, &deposited{50}); err != nil {
		return err
	}
	if _, err := repo.Save(ctx, a, edax.SaveOptions{}); err != nil {
		return err
	}
	count, err := store.UnpublishedCount(ctx)
	if err != nil {
		return err
	}
	if count != 2 {
		return fmt.Errorf("save left %d outbox rows, want 2", count)
	}
	brokerDown := errors.New("broker unreachable")
	broken, err := edax.NewRelay(store, func(context.Context, edax.EventMessage) error { return brokerDown }, edax.RelayOptions[edax.EventMessage]{Pacing: edax.Pacing{Policy: &edax.BackoffPolicy{BaseMillis: 1, CapMillis: 10}}})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, broken.Close()) }()
	if _, err := broken.Drain(ctx, 10); !errors.Is(err, brokerDown) {
		return fmt.Errorf("expected broker outage, got %v", err)
	}
	count, err = store.UnpublishedCount(ctx)
	if err != nil {
		return err
	}
	if count != 2 {
		return fmt.Errorf("outage lost rows: remaining=%d", count)
	}
	var published []uint64
	relay, err := edax.NewRelay(store, func(_ context.Context, event edax.EventMessage) error {
		if event.Name != "Deposited" || event.Sequence == nil {
			return fmt.Errorf("unexpected event %+v", event)
		}
		published = append(published, *event.Sequence)
		return nil
	}, edax.RelayOptions[edax.EventMessage]{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, relay.Close()) }()
	sent, err := relay.Drain(ctx, 10)
	if err != nil {
		return err
	}
	if sent != 2 || len(published) != 2 || published[0] != 1 || published[1] != 2 {
		return fmt.Errorf("published=%v count=%d", published, sent)
	}
	empty, err := relay.Drain(ctx, 10)
	if err != nil {
		return err
	}
	count, err = store.UnpublishedCount(ctx)
	if err != nil {
		return err
	}
	if empty != 0 || count != 0 {
		return fmt.Errorf("drained outbox still has work: sent=%d rows=%d", empty, count)
	}
	fmt.Printf("after outage: published sequences %v; next drain sent %d\n", published, empty)

	return nil
}

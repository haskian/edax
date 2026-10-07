// Observe command latency, idempotency, transactions and relay publication.
// Run from crates/edax-go: mise exec -- go run ./examples/14_observability
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: both dispatches were timed; claims, transactions and publication were observed")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var mu sync.Mutex
	counts := make(map[string]int)
	observer := edax.ObserverFunc(func(event edax.Observation) {
		mu.Lock()
		defer mu.Unlock()
		counts[event.Operation]++
		fmt.Printf("%s: %s (%s)\n", event.Operation, event.Outcome, event.Duration)
	})
	ctx = edax.WithObserver(ctx, observer)
	db := memory.New()
	defer func() { err = errors.Join(err, db.Close()) }()
	ledger, err := db.IdempotencyStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, ledger.Close()) }()
	store, err := db.EventStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	guard, err := edax.Idempotent(ledger, edax.IdempotentOptions{Scope: "accounts-api", UnitOfWork: db})
	if err != nil {
		return err
	}
	bus := edax.NewCommandBus(guard)
	runs := 0
	if err := bus.Register("Deposit", func(context.Context, edax.CommandMessage) (any, error) { runs++; return "deposited 100", nil }); err != nil {
		return err
	}
	key := "req-1"
	message, err := edax.NewCommandMessage(ctx, edax.Payload{Name: "Deposit", Version: 1, Data: []byte(`{"account_id":"acct-1","amount":100}`)}, edax.CommandOptions{IdempotencyKey: &key})
	if err != nil {
		return err
	}
	for range 2 {
		if _, err := bus.Dispatch(ctx, message); err != nil {
			return err
		}
	}
	sequence := uint64(1)
	err = edax.WithinTransaction(ctx, db, func(ctx context.Context) error {
		_, err := store.Append(ctx, "acct-1", 0, []edax.EventDraft{{ID: "e1", AggregateID: "acct-1", Sequence: &sequence, Name: "Deposited", Version: 1, Data: []byte(`{"amount":100}`), CorrelationID: "e1", CausationID: "e1"}})
		return err
	})
	if err != nil {
		return err
	}
	relay, err := edax.NewRelay(store, func(context.Context, edax.EventMessage) error { return nil }, edax.RelayOptions[edax.EventMessage]{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, relay.Close()) }()
	if _, err := relay.Drain(ctx, 10); err != nil {
		return err
	}
	stats := relay.Last()
	mu.Lock()
	defer mu.Unlock()
	if runs != 1 || counts["command"] != 2 || counts["idempotency.claim"] != 2 || counts["idempotency.settle"] != 1 || counts["transaction"] != 3 || counts["drain"] != 1 || stats == nil || stats.Published != 1 || stats.OldestTimestamp == nil {
		return fmt.Errorf("unexpected observations: runs=%d counts=%v stats=%+v", runs, counts, stats)
	}
	return nil
}

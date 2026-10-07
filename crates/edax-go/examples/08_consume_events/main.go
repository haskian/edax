// Consume events in order per account with several dispatcher workers.
// Run from crates/edax-go: mise exec -- go run ./examples/08_consume_events
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: all six events acknowledged; each account stayed in order")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	broker, err := memory.NewBroker(3, edax.TenancySilo)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, broker.Close()) }()
	accounts := []string{"alice", "dave", "eve"}
	for sequence := uint64(1); sequence <= 2; sequence++ {
		for _, account := range accounts {
			message := edax.EventMessage{ID: fmt.Sprintf("%s-%d", account, sequence), AggregateID: account, Name: "Deposited", Version: 1, Sequence: edax.Ptr(sequence), Data: fmt.Appendf(nil, `{"amount":%d}`, 10*sequence), CorrelationID: "deposits", CausationID: "deposit-command", RecordedAt: edax.CurrentTimestamp()}
			if err := broker.PublishEvent(ctx, message); err != nil {
				return err
			}
		}
	}

	var mu sync.Mutex // Different accounts may run on different workers.
	seen := make(map[string][]uint64)
	router := edax.NewEventRouter()
	if err := router.Register("Deposited", func(_ context.Context, event edax.EventMessage) error {
		if event.Sequence == nil {
			return fmt.Errorf("event %s has no sequence", event.ID)
		}
		mu.Lock()
		defer mu.Unlock()
		seen[event.AggregateID] = append(seen[event.AggregateID], *event.Sequence)
		fmt.Printf("%s #%d\n", event.AggregateID, *event.Sequence)
		return nil
	}); err != nil {
		return err
	}
	dispatcher, err := edax.NewKeyOrderedDispatcher(router.Dispatch, edax.DispatcherOptions[edax.EventMessage]{Workers: 3})
	if err != nil {
		return err
	}
	defer func() {
		stopCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		err = errors.Join(err, dispatcher.Stop(stopCtx), dispatcher.Close())
	}()
	if err := dispatcher.Start(ctx); err != nil {
		return err
	}
	for range 6 {
		delivery, _, err := broker.Poll(ctx, "balances")
		if err != nil {
			return err
		}
		if delivery == nil {
			return errors.New("expected an event delivery")
		}
		if err := dispatcher.Submit(ctx, delivery); err != nil {
			return errors.Join(err, delivery.Nack(ctx))
		}
	}
	if err := dispatcher.WaitIdle(ctx); err != nil {
		return err
	}
	for _, account := range accounts {
		if !slices.Equal(seen[account], []uint64{1, 2}) {
			return fmt.Errorf("%s sequences: %v", account, seen[account])
		}
	}
	empty, err := broker.IsEmpty("balances")
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("broker still has unacknowledged events")
	}
	return nil
}

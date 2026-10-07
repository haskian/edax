// Dead-letter terminal failures immediately and transient failures after three attempts.
// Run from crates/edax-go: mise exec -- go run ./examples/09_dead_letters
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: rejection dead-lettered immediately; flaky event recovered; persistent failure stopped at attempt 3")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	broker, err := memory.NewBroker(1, edax.TenancySilo)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, broker.Close()) }()
	for _, account := range []string{"alice", "bob", "carol", "dave"} {
		event := edax.EventMessage{ID: account + "-1", AggregateID: account, Name: "Deposited", Version: 1, Sequence: edax.Ptr(uint64(1)), Data: []byte(`{"amount":10}`), CorrelationID: "deposits", CausationID: "deposit-command", RecordedAt: edax.CurrentTimestamp()}
		if err := broker.PublishEvent(ctx, event); err != nil {
			return err
		}
	}
	attempts := make(map[string]int)
	var dead []string
	handle := func(_ context.Context, event edax.EventMessage) error {
		account := event.AggregateID
		attempts[account]++
		fmt.Printf("%s attempt %d\n", account, attempts[account])
		switch {
		case account == "bob":
			return &edax.Error{Code: edax.CodeRejected, Message: "bob closed his account"}
		case account == "carol" && attempts[account] < 3, account == "dave":
			return &edax.Error{Code: edax.CodeStore, Message: "ledger timeout"}
		default:
			return nil
		}
	}
	dispatcher, err := edax.NewKeyOrderedDispatcher(handle, edax.DispatcherOptions[edax.EventMessage]{
		Workers: 1, MaxAttempts: 3,
		Pacing: edax.Pacing{Policy: &edax.BackoffPolicy{BaseMillis: 1, CapMillis: 10}},
		OnDead: func(_ context.Context, letter edax.DeadLetter[edax.EventMessage]) error {
			dead = append(dead, fmt.Sprintf("%s at attempt %d", letter.Message.AggregateID, letter.Attempt))
			return nil // A production sink returns only after its durable write succeeds.
		},
	})
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
	for {
		empty, err := broker.IsEmpty("deposits")
		if err != nil {
			return err
		}
		if empty {
			break
		}
		delivery, _, err := broker.Poll(ctx, "deposits")
		if err != nil {
			return err
		}
		if delivery == nil {
			return errors.New("expected an event delivery")
		}
		if err := dispatcher.Submit(ctx, delivery); err != nil {
			return errors.Join(err, delivery.Nack(ctx))
		}
		// Settlement finishes before polling again, including any nack for redelivery.
		if err := dispatcher.WaitIdle(ctx); err != nil {
			return err
		}
	}
	if !maps.Equal(attempts, map[string]int{"alice": 1, "bob": 1, "carol": 3, "dave": 3}) {
		return fmt.Errorf("attempts: %v", attempts)
	}
	if !slices.Equal(dead, []string{"bob at attempt 1", "dave at attempt 3"}) {
		return fmt.Errorf("dead letters: %v", dead)
	}
	return nil
}

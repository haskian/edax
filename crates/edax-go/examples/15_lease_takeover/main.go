// Expire a live claim and prove that its old owner cannot overwrite the winner.
// Run from crates/edax-go: mise exec -- go run ./examples/15_lease_takeover
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: worker B's outcome survived; both handlers ran, so the charge still needs downstream deduplication")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db := memory.New()
	defer func() { err = errors.Join(err, db.Close()) }()
	ledger, err := db.IdempotencyStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, ledger.Close()) }()
	hash, err := edax.PayloadHashJSON([]byte(`{"payment_id":"pay-1","amount":40}`))
	if err != nil {
		return err
	}
	const key = "payments:pay-1"
	started, finish := make(chan struct{}), make(chan struct{})
	var runs atomic.Int32
	charge := func(ctx context.Context) (any, error) {
		run := runs.Add(1)
		if run == 1 {
			close(started)
			select {
			case <-finish:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return fmt.Sprintf("charged by run %d", run), nil
	}
	worker := make(chan error, 1)
	go func() { _, err := edax.Guard(ctx, ledger, key, hash, charge); worker <- err }()
	// Join the worker before releasing its native store, including error paths.
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-worker
		}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		return ctx.Err()
	}
	if _, err := edax.Guard(ctx, ledger, key, hash, charge); !errors.Is(err, edax.ErrInProgress) {
		return fmt.Errorf("held claim: %w", err)
	}
	expired, err := ledger.Expire(key)
	if err != nil {
		return err
	}
	if !expired {
		return fmt.Errorf("claim did not expire")
	}
	winner, err := edax.Guard(ctx, ledger, key, hash, charge)
	if err != nil {
		return err
	}
	close(finish)
	err = <-worker
	joined = true
	if !errors.Is(err, edax.ErrStore) {
		return fmt.Errorf("late completion: %w", err)
	}
	replay, err := edax.Guard(ctx, ledger, key, hash, charge)
	if err != nil {
		return err
	}
	if winner != "charged by run 2" || replay != winner || runs.Load() != 2 {
		return fmt.Errorf("winner=%v replay=%v runs=%d", winner, replay, runs.Load())
	}
	return nil
}

// Replay successful and terminal outcomes through Redis, or a separate memory ledger.
// Run from crates/edax-go: mise exec -- go run ./examples/16_redis_ledger
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
	"github.com/haskian/edax/crates/edax-go/backend/redis"
	goredis "github.com/redis/go-redis/v9"
)

type chargeCard struct {
	PaymentID string `json:"payment_id"`
	Card      string `json:"card"`
	Amount    int    `json:"amount"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: success and terminal failure replayed without rerunning either handler")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if url := os.Getenv("EDAX_REDIS_URL"); url != "" {
		return onRedis(ctx, url)
	}
	fmt.Println("EDAX_REDIS_URL is unset: using a separate memory ledger")
	db := memory.New()
	defer func() { err = errors.Join(err, db.Close()) }()
	ledger, err := db.IdempotencyStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, ledger.Close()) }()
	// Expose only the ledger seam: this store does not join the application's transaction.
	return chargeTwice(ctx, struct{ edax.IdempotencyStore }{ledger})
}

func onRedis(ctx context.Context, url string) (err error) {
	options, err := goredis.ParseURL(url)
	if err != nil {
		return err
	}
	options.ContextTimeoutEnabled = true
	client := goredis.NewClient(options)
	defer func() { err = errors.Join(err, client.Close()) }()
	ledger, err := redis.New(redis.NewClient(client), redis.Config{Prefix: "examples:" + edax.Guid() + ":", TTL: time.Minute})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, ledger.Close()) }()
	fmt.Println("using Redis; example records expire after one minute")
	return chargeTwice(ctx, ledger)
}

func chargeTwice(ctx context.Context, ledger edax.IdempotencyStore) error {
	codec, err := edax.NewJSONCodec(map[string]func() any{"ChargeCard": func() any { return new(chargeCard) }})
	if err != nil {
		return err
	}
	guard, err := edax.Idempotent(ledger, edax.IdempotentOptions{Scope: "payments", Tenancy: edax.TenancySilo})
	if err != nil {
		return err
	}
	bus := edax.NewCommandBus(guard)
	runs := 0
	handler, err := edax.TypedCommandHandler(codec, func(_ context.Context, command *chargeCard) (string, error) {
		runs++
		if command.Card == "blocked" {
			return "", &edax.Error{Code: edax.CodeRejected, Message: "card declined"}
		}
		return fmt.Sprintf("charged %d", command.Amount), nil
	})
	if err != nil {
		return err
	}
	if err := bus.Register("ChargeCard", handler); err != nil {
		return err
	}
	for _, command := range []chargeCard{{PaymentID: "pay-1", Card: "visa", Amount: 40}, {PaymentID: "pay-2", Card: "blocked", Amount: 99}} {
		id := command.PaymentID
		payload, err := codec.Marshal(command)
		if err != nil {
			return err
		}
		message, err := edax.NewCommandMessage(ctx, payload, edax.CommandOptions{ID: id})
		if err != nil {
			return err
		}
		for attempt := range 2 {
			result, err := bus.Execute(ctx, message)
			if id == "pay-1" {
				if err != nil {
					return err
				}
				if result != "charged 40" {
					return fmt.Errorf("unexpected result: %v", result)
				}
			} else {
				want := error(edax.ErrRejected)
				if attempt == 1 {
					want = edax.ErrCachedFailure
				}
				if !errors.Is(err, want) {
					return fmt.Errorf("attempt %d: got %v, want %v", attempt+1, err, want)
				}
			}
			fmt.Printf("%s attempt %d: result=%v error=%v\n", id, attempt+1, result, err)
		}
	}
	if runs != 2 {
		return fmt.Errorf("handler ran %d times, want 2", runs)
	}
	return nil
}

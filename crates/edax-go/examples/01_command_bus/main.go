// Commands reach their registered handlers.
// Run from crates/edax-go: mise exec -- go run ./examples/01_command_bus
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/haskian/edax/crates/edax-go"
)

type deposit struct {
	AccountID string `json:"account_id"`
	Amount    int    `json:"amount"`
}
type withdraw struct {
	AccountID string `json:"account_id"`
	Amount    int    `json:"amount"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: Commands reach their registered handlers.")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	codec, err := edax.NewJSONCodec(map[string]func() any{
		"Deposit":  func() any { return new(deposit) },
		"Withdraw": func() any { return new(withdraw) },
	})
	if err != nil {
		return err
	}
	balances := make(map[string]int)
	handler, err := edax.TypedCommandHandler(codec, func(_ context.Context, cmd *deposit) (int, error) {
		balances[cmd.AccountID] += cmd.Amount
		return balances[cmd.AccountID], nil
	})
	if err != nil {
		return err
	}
	bus := edax.NewCommandBus()
	if err := bus.Register("Deposit", handler); err != nil {
		return err
	}
	for _, step := range []struct{ amount, balance int }{{100, 100}, {50, 150}} {
		payload, err := codec.Marshal(deposit{AccountID: "acct-1", Amount: step.amount})
		if err != nil {
			return err
		}
		message, err := edax.NewCommandMessage(ctx, payload, edax.CommandOptions{})
		if err != nil {
			return err
		}
		balance, err := edax.ExecuteAs[int](ctx, bus, message)
		if err != nil {
			return err
		}
		if balance != step.balance {
			return fmt.Errorf("deposit %d: balance %d, want %d", step.amount, balance, step.balance)
		}
		fmt.Printf("deposit %d -> balance %d\n", step.amount, balance)
	}
	payload, err := codec.Marshal(withdraw{AccountID: "acct-1", Amount: 30})
	if err != nil {
		return err
	}
	message, err := edax.NewCommandMessage(ctx, payload, edax.CommandOptions{})
	if err != nil {
		return err
	}
	if _, err := bus.Execute(ctx, message); !errors.Is(err, edax.ErrHandlerNotFound) {
		return fmt.Errorf("unregistered withdrawal: expected HandlerNotFound, got %v", err)
	}
	fmt.Println("withdraw -> HandlerNotFound")

	return nil
}

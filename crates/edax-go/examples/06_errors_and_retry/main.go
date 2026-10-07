// Transient writes roll back before retry; terminal errors stop.
// Run from crates/edax-go: mise exec -- go run ./examples/06_errors_and_retry
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

type withdrawn struct {
	Amount int `json:"amount"`
}
type withdraw struct {
	AccountID string `json:"account_id"`
	Amount    int    `json:"amount"`
}

// The client supplies the same request key when retrying a request.
func send(ctx context.Context, bus *edax.CommandBus, codec *edax.JSONCodec, amount int, key string) (int, error) {
	payload, err := codec.Marshal(withdraw{AccountID: "acct-1", Amount: amount})
	if err != nil {
		return 0, err
	}
	message, err := edax.NewCommandMessage(ctx, payload, edax.CommandOptions{IdempotencyKey: edax.Ptr(key)})
	if err != nil {
		return 0, err
	}
	return edax.ExecuteAs[int](ctx, bus, message)
}

func (a *account) Withdraw(amount int) error {
	if a.Balance < amount {
		return &edax.Error{Code: edax.CodeRejected, Message: fmt.Sprintf("insufficient funds: balance %d < %d", a.Balance, amount)}
	}
	return edax.Apply(a, &withdrawn{amount})
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: Transient writes roll back before retry; terminal errors stop.")
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

	opened, err := repo.Load(ctx, "acct-1", nil)
	if err != nil {
		return err
	}
	if err := edax.Apply(opened, &deposited{100}); err != nil {
		return err
	}
	if _, err := repo.Save(ctx, opened, edax.SaveOptions{}); err != nil {
		return err
	}
	ledger, err := db.IdempotencyStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, ledger.Close()) }()
	// The guard owns the transaction. Retry starts each attempt from committed state.
	idempotent, err := edax.Idempotent(ledger, edax.IdempotentOptions{Scope: "accounts-api", Tenancy: edax.TenancySilo, UnitOfWork: db})
	if err != nil {
		return err
	}
	retry, err := edax.Retry(3, edax.Pacing{Policy: &edax.BackoffPolicy{BaseMillis: 1, CapMillis: 10}})
	if err != nil {
		return err
	}
	bus := edax.NewCommandBus(retry, idempotent)
	commands, err := edax.NewJSONCodec(map[string]func() any{"Withdraw": func() any { return new(withdraw) }})
	if err != nil {
		return err
	}

	attempts := make(map[int]int)
	receiptDown := errors.New("receipt printer unreachable")
	handler, err := edax.TypedCommandHandler(commands, func(ctx context.Context, cmd *withdraw) (int, error) {
		attempts[cmd.Amount]++
		a, err := repo.Load(ctx, cmd.AccountID, nil)
		if err != nil {
			return 0, err
		}
		if err := a.Withdraw(cmd.Amount); err != nil {
			return 0, err
		}
		if _, err := repo.Save(ctx, a, edax.SaveOptions{}); err != nil {
			return 0, err
		}
		// Fail after the write. The next attempt must reload after rollback.
		if cmd.Amount == 30 && attempts[cmd.Amount] == 1 {
			return 0, receiptDown
		}
		return a.Balance, nil
	})
	if err != nil {
		return err
	}
	if err := bus.Register("Withdraw", handler); err != nil {
		return err
	}
	balance, err := send(ctx, bus, commands, 30, "req-1")
	if err != nil {
		return err
	}
	_, rejected := send(ctx, bus, commands, 500, "req-2")
	if !errors.Is(rejected, edax.ErrRejected) {
		return fmt.Errorf("expected Rejected, got %v", rejected)
	}
	if !edax.IsTerminal(rejected) || edax.IsTerminal(receiptDown) {
		return fmt.Errorf("unexpected terminal/transient classification")
	}
	a, err := repo.Load(ctx, "acct-1", nil)
	if err != nil {
		return err
	}
	if balance != 70 || a.Balance != 70 || a.Version().Current() != 2 || attempts[30] != 2 || attempts[500] != 1 {
		return fmt.Errorf("balance=%d stored=%d version=%d attempts=%v", balance, a.Balance, a.Version().Current(), attempts)
	}
	fmt.Printf("withdraw 30 -> balance %d after %d attempts; withdraw 500 rejected after %d attempt\n", balance, attempts[30], attempts[500])

	return nil
}

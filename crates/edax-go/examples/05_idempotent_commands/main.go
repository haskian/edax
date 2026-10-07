// Repeated requests deposit once; changed payloads are refused.
// Run from crates/edax-go: mise exec -- go run ./examples/05_idempotent_commands
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

type deposit struct {
	AccountID string `json:"account_id"`
	Amount    int    `json:"amount"`
}

// The client supplies the same request key when retrying a request.
func send(ctx context.Context, bus *edax.CommandBus, codec *edax.JSONCodec, amount int, key string) (int, error) {
	payload, err := codec.Marshal(deposit{AccountID: "acct-1", Amount: amount})
	if err != nil {
		return 0, err
	}
	message, err := edax.NewCommandMessage(ctx, payload, edax.CommandOptions{IdempotencyKey: edax.Ptr(key)})
	if err != nil {
		return 0, err
	}
	return edax.ExecuteAs[int](ctx, bus, message)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: Repeated requests deposit once; changed payloads are refused.")
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
	commands, err := edax.NewJSONCodec(map[string]func() any{"Deposit": func() any { return new(deposit) }})
	if err != nil {
		return err
	}

	handled := 0
	handler, err := edax.TypedCommandHandler(commands, func(ctx context.Context, cmd *deposit) (int, error) {
		handled++
		a, err := repo.Load(ctx, cmd.AccountID, nil)
		if err != nil {
			return 0, err
		}
		if err := edax.Apply(a, &deposited{cmd.Amount}); err != nil {
			return 0, err
		}
		// The context carries the guard's transaction into the repository.
		if _, err := repo.Save(ctx, a, edax.SaveOptions{}); err != nil {
			return 0, err
		}
		return a.Balance, nil
	})
	if err != nil {
		return err
	}
	if err := bus.Register("Deposit", handler); err != nil {
		return err
	}
	first, err := send(ctx, bus, commands, 100, "req-A")
	if err != nil {
		return err
	}
	again, err := send(ctx, bus, commands, 100, "req-A")
	if err != nil {
		return err
	}
	if _, err := send(ctx, bus, commands, 999, "req-A"); !errors.Is(err, edax.ErrPayloadMismatch) {
		return fmt.Errorf("expected PayloadMismatch, got %v", err)
	}
	a, err := repo.Load(ctx, "acct-1", nil)
	if err != nil {
		return err
	}
	if first != 100 || again != 100 || a.Balance != 100 || a.Version().Current() != 1 || handled != 1 {
		return fmt.Errorf("first=%d replay=%d balance=%d version=%d handler runs=%d", first, again, a.Balance, a.Version().Current(), handled)
	}
	fmt.Printf("request=%d replay=%d; changed payload refused; handler ran %d time\n", first, again, handled)

	return nil
}

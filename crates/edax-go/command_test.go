package edax_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
)

func TestCommandGuardBranches(t *testing.T) {
	cases := []struct {
		name    string
		failure error
		cached  bool
	}{
		{"success", nil, true},
		{"transient", &edax.Error{Code: edax.CodeStore, Message: "invoice database unavailable"}, false},
		{"terminal", &edax.Error{Code: edax.CodeRejected, Message: "invoice is already paid"}, true},
		{"cancelled", context.Canceled, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			database := memory.New()
			defer database.Close()
			ledger, err := database.IdempotencyStore()
			if err != nil {
				t.Fatal(err)
			}
			defer ledger.Close()
			middleware, err := edax.Idempotent(ledger, edax.IdempotentOptions{Scope: "invoice-api"})
			if err != nil {
				t.Fatal(err)
			}
			bus := edax.NewCommandBus(middleware)
			calls := 0
			if err = bus.Register("PayInvoice", func(ctx context.Context, message edax.CommandMessage) (any, error) {
				calls++
				current, ok := edax.CurrentCommand(ctx)
				if !ok || current.ID != message.ID {
					t.Error("missing current command")
				}
				lineage, ok := edax.CurrentLineage(ctx)
				if !ok || lineage.CausationID != message.ID {
					t.Error("missing child lineage")
				}
				return map[string]any{"invoice_id": "invoice-42", "paid": true}, test.failure
			}); err != nil {
				t.Fatal(err)
			}
			message := edax.CommandMessage{ID: "cmd-pay-invoice-42", Name: "PayInvoice", Version: 1, Data: []byte(`{"invoice_id":"invoice-42","amount":1250}`), CorrelationID: "flow-invoice-42", CausationID: "flow-invoice-42"}
			first, firstErr := bus.Execute(context.Background(), message)
			second, secondErr := bus.Execute(context.Background(), message)
			expectedCalls := 2
			if test.cached {
				expectedCalls = 1
			}
			if calls != expectedCalls {
				t.Fatalf("calls=%d want %d errors=%v/%v", calls, expectedCalls, firstErr, secondErr)
			}
			if test.failure == nil {
				left, _ := json.Marshal(first)
				right, _ := json.Marshal(second)
				if string(left) != string(right) || firstErr != nil || secondErr != nil {
					t.Fatalf("replay %s %s %v %v", left, right, firstErr, secondErr)
				}
				changed := message
				changed.Data = []byte(`{"invoice_id":"invoice-42","amount":1500}`)
				if _, err = bus.Execute(context.Background(), changed); !errors.Is(err, edax.ErrPayloadMismatch) {
					t.Fatalf("mismatch: %v", err)
				}
			} else {
				if !errors.Is(firstErr, test.failure) {
					t.Fatalf("first error: %v", firstErr)
				}
				if test.cached && !errors.Is(secondErr, edax.ErrCachedFailure) {
					t.Fatalf("cached failure: %v", secondErr)
				}
			}
		})
	}
}

func TestCommandRetryAndLocalKey(t *testing.T) {
	for _, test := range []struct {
		name     string
		terminal bool
		want     int
	}{{"transient", false, 3}, {"terminal", true, 1}} {
		t.Run(test.name, func(t *testing.T) {
			waits := 0
			retry, err := edax.Retry(3, edax.Pacing{Sleep: func(context.Context, time.Duration) error { waits++; return nil }})
			if err != nil {
				t.Fatal(err)
			}
			bus := edax.NewCommandBus(retry)
			calls := 0
			bus.Register("IssueInvoice", func(context.Context, edax.CommandMessage) (any, error) {
				calls++
				if test.terminal {
					return nil, edax.ErrRejected
				}
				if calls < 3 {
					return nil, edax.ErrStore
				}
				return "invoice-42", nil
			})
			result, err := bus.Execute(context.Background(), edax.CommandMessage{ID: "cmd-42", Name: "IssueInvoice"})
			if calls != test.want || waits != test.want-1 {
				t.Fatalf("calls=%d waits=%d", calls, waits)
			}
			if !test.terminal && (err != nil || result != "invoice-42") {
				t.Fatalf("result=%v err=%v", result, err)
			}
		})
	}
	if _, err := edax.Retry(0, edax.Pacing{}); !errors.Is(err, edax.ErrBadHandle) {
		t.Fatalf("zero attempts: %v", err)
	}
	database := memory.New()
	defer database.Close()
	ledger, _ := database.IdempotencyStore()
	defer ledger.Close()
	idempotent, _ := edax.Idempotent(ledger, edax.IdempotentOptions{Scope: "invoice-api"})
	bus := edax.NewCommandBus(idempotent)
	calls := 0
	bus.Register("IssueInvoice", func(context.Context, edax.CommandMessage) (any, error) { calls++; return nil, nil })
	message := edax.CommandMessage{ID: "cmd-local", Name: "IssueInvoice", Data: []byte(`{"invoice_id":"invoice-42"}`)}
	for range 2 {
		if _, err := bus.Dispatch(context.Background(), message); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("local no-key command was guarded: %d", calls)
	}
	if err := bus.Register("IssueInvoice", func(context.Context, edax.CommandMessage) (any, error) { return nil, nil }); !errors.Is(err, edax.ErrAlreadyRegistered) {
		t.Fatalf("duplicate: %v", err)
	}
	if _, err := bus.Execute(context.Background(), edax.CommandMessage{Name: "Missing"}); !errors.Is(err, edax.ErrHandlerNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func TestGuardTransactionRollbackAndPanic(t *testing.T) {
	database := memory.New()
	defer database.Close()
	ledger, _ := database.IdempotencyStore()
	defer ledger.Close()
	projections, _ := database.ProjectionStore()
	defer projections.Close()
	ctx := context.Background()
	calls := 0
	run := func(ctx context.Context) (any, error) {
		calls++
		_, err := projections.Save(ctx, "invoice-42", 0, `{"id":"invoice-42","paid":true}`)
		if err != nil {
			return nil, err
		}
		return nil, edax.ErrRejected
	}
	for range 2 {
		if _, err := edax.GuardWithUnitOfWork(ctx, ledger, database, "pay-invoice-42", 42, run); !errors.Is(err, edax.ErrRejected) {
			t.Fatal(err)
		}
	}
	row, err := projections.Get(ctx, "invoice-42")
	if err != nil || row != nil || calls != 2 {
		t.Fatalf("rollback row=%v calls=%d err=%v", row, calls, err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic disappeared")
			}
		}()
		edax.Guard(ctx, ledger, "panic-invoice", 43, func(context.Context) (any, error) { panic("handler panic") })
	}()
	claim, err := ledger.Claim(ctx, "panic-invoice", 43)
	if err != nil || !claim.Won {
		t.Fatalf("panic claim leaked: %v %v", claim, err)
	}
	retry, _ := edax.Retry(2, edax.Pacing{})
	wrapped := retry(func(context.Context, edax.CommandMessage) (any, error) {
		t.Error("retry entered transaction")
		return nil, nil
	})
	err = edax.WithinTransaction(ctx, database, func(txctx context.Context) error {
		_, err := wrapped(txctx, edax.CommandMessage{Name: "PayInvoice"})
		return err
	})
	if !errors.Is(err, edax.ErrBadHandle) {
		t.Fatalf("transaction retry: %v", err)
	}
}

type refusingLedger struct{ edax.IdempotencyStore }

func (store refusingLedger) Complete(context.Context, string, []byte, edax.IdempotencyRecord) error {
	return fmt.Errorf("store completion lost")
}

func TestGuardRefusedCompletionReleases(t *testing.T) {
	database := memory.New()
	defer database.Close()
	ledger, _ := database.IdempotencyStore()
	defer ledger.Close()
	_, err := edax.Guard(context.Background(), refusingLedger{ledger}, "invoice-42", 42, func(context.Context) (any, error) { return "paid", nil })
	if err == nil {
		t.Fatal("completion refusal lost")
	}
	claimed, err := ledger.Claim(context.Background(), "invoice-42", 42)
	if err != nil || !claimed.Won {
		t.Fatalf("claim not released: %v %v", claimed, err)
	}
}

func TestMessagePackCommandAndWrongLedgerOwner(t *testing.T) {
	database := memory.New()
	defer database.Close()
	ledger, _ := database.IdempotencyStore()
	defer ledger.Close()
	other := memory.New()
	defer other.Close()
	_, err := edax.GuardWithUnitOfWork(context.Background(), ledger, other, "invoice-42", 42, func(context.Context) (any, error) { t.Error("wrong database ran handler"); return nil, nil })
	if !errors.Is(err, edax.ErrBadHandle) {
		t.Fatalf("wrong database: %v", err)
	}
	codec, err := edax.NewMessagePackCodec(map[string]func() any{"AccountOpened": func() any { return &accountOpened{} }})
	if err != nil {
		t.Fatal(err)
	}
	middleware, err := edax.Idempotent(ledger, edax.IdempotentOptions{Scope: "accounts", Codec: codec})
	if err != nil {
		t.Fatal(err)
	}
	bus := edax.NewCommandBus(middleware)
	calls := 0
	type receipt struct {
		ID string `json:"id"`
	}
	handler, err := edax.TypedCommandHandler(codec, func(ctx context.Context, command *accountOpened) (receipt, error) {
		calls++
		return receipt{ID: "account-42"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = bus.Register("AccountOpened", handler); err != nil {
		t.Fatal(err)
	}
	payload, err := codec.Marshal(&accountOpened{Owner: "Ada", Currency: "EUR"})
	if err != nil {
		t.Fatal(err)
	}
	message, err := edax.NewCommandMessage(context.Background(), payload, edax.CommandOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, err := edax.ExecuteAs[receipt](context.Background(), bus, message)
		if err != nil || result.ID != "account-42" {
			t.Fatalf("receipt %+v %v", result, err)
		}
	}
	if calls != 1 {
		t.Fatalf("replayed handler %d times", calls)
	}
}

func TestCommandInvalidTextKeepsObserverContext(t *testing.T) {
	for _, id := range []string{"cmd-pay\x00invoice-42", string([]byte{'c', 255})} {
		t.Run(fmt.Sprintf("%x", id), func(t *testing.T) {
			bus := edax.NewCommandBus()
			calls := 0
			if err := bus.Register("PayInvoice", func(context.Context, edax.CommandMessage) (any, error) {
				calls++
				return nil, nil
			}); err != nil {
				t.Fatal(err)
			}
			observations := 0
			ctx := edax.WithObserver(context.Background(), edax.ObserverFunc(func(observation edax.Observation) {
				observations++
				if !errors.Is(observation.Err, edax.ErrCodec) {
					t.Errorf("observer failure: %v", observation.Err)
				}
			}))
			if _, err := bus.Execute(ctx, edax.CommandMessage{ID: id, Name: "PayInvoice", CorrelationID: "invoice-flow-42"}); !errors.Is(err, edax.ErrCodec) {
				t.Fatalf("invalid command id: %v", err)
			}
			if calls != 0 || observations != 1 {
				t.Fatalf("handler calls=%d observations=%d", calls, observations)
			}
		})
	}
}

func TestGuardedCommandResultsHaveStableTypes(t *testing.T) {
	type line struct {
		ProductID string `json:"product_id"`
		Quantity  uint64 `json:"quantity"`
	}
	type receipt struct {
		InvoiceID string `json:"invoice_id"`
		Lines     []line `json:"lines"`
	}
	for _, test := range []struct {
		name               string
		result, normalized any
	}{
		{"struct", receipt{"invoice-42", []line{{"book-17", 9007199254740993}}}, map[string]any{"invoice_id": "invoice-42", "lines": []any{map[string]any{"product_id": "book-17", "quantity": json.Number("9007199254740993")}}}},
		{"map", map[string]int{"invoice_total": 1250}, map[string]any{"invoice_total": json.Number("1250")}},
		{"slice", []string{"invoice-42"}, []any{"invoice-42"}},
		{"number", uint64(9007199254740993), json.Number("9007199254740993")},
		{"string", "invoice-42", "invoice-42"},
		{"nil", nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := memory.New()
			defer db.Close()
			ledger, err := db.IdempotencyStore()
			if err != nil {
				t.Fatal(err)
			}
			defer ledger.Close()
			middleware, err := edax.Idempotent(ledger, edax.IdempotentOptions{Scope: "invoice-api"})
			if err != nil {
				t.Fatal(err)
			}
			bus := edax.NewCommandBus(middleware)
			calls := 0
			handler := func(context.Context, edax.CommandMessage) (any, error) { calls++; return test.result, nil }
			if err := bus.Register("PayInvoice", handler); err != nil {
				t.Fatal(err)
			}
			message := edax.CommandMessage{ID: "pay-invoice-42", Name: "PayInvoice", Version: 1, Data: []byte(`{"invoice_id":"invoice-42"}`)}
			for range 2 {
				got, err := bus.Execute(context.Background(), message)
				if err != nil || !reflect.DeepEqual(got, test.normalized) {
					t.Fatalf("result %T(%#v), want %T(%#v): %v", got, got, test.normalized, test.normalized, err)
				}
			}
			if calls != 1 {
				t.Fatalf("handler ran %d times", calls)
			}
			plain := edax.NewCommandBus()
			if err := plain.Register("PayInvoice", handler); err != nil {
				t.Fatal(err)
			}
			got, err := plain.Execute(context.Background(), message)
			if err != nil || !reflect.DeepEqual(got, test.result) {
				t.Fatalf("unguarded result %T(%v): %v", got, got, err)
			}
		})
	}
}

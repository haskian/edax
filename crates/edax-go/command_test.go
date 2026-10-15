package edax_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
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
				// The ledger caches the code beside the bare message, never the wire form.
				if want := "cached_failure: rejected: "; test.cached && secondErr.Error() != want {
					t.Fatalf("cached failure %q want %q", secondErr, want)
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
	projections, _ := database.ProjectionStore("accounts")
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

// ownLedger hides the store's database, as a ledger of its own (Redis) has none to join.
type ownLedger struct{ edax.IdempotencyStore }

func TestGuardInsideHostTransactionRunsOnlyOnLedgerThatJoinsIt(t *testing.T) {
	aborted := errors.New("a later step aborts the host transaction")
	// The memory database cannot fail a commit on its own. Settling the transaction under the host
	// stands in for a database that aborts it, as a serialization failure or a dropped connection
	// would: the host's commit then fails and nothing it staged lands.
	commitFails := func(txctx context.Context, db *memory.Database) error {
		tx, ok := edax.CurrentTransaction(txctx, db)
		if !ok {
			return errors.New("no host transaction to abort")
		}
		return tx.Rollback(txctx)
	}
	laterStepAborts := func(context.Context, *memory.Database) error { return aborted }
	cases := []struct {
		name   string
		ledger func(same, foreign *memory.IdempotencyStore) edax.IdempotencyStore
		// ownUnit gives the guard the host's database as its own unit of work.
		ownUnit bool
		end     func(context.Context, *memory.Database) error
		// want is the host's error. bad_handle with no run is the guard's refusal before the claim.
		want    error
		refused bool
	}{
		{name: "same database ledger joins so the rollback frees the key", ledger: func(same, _ *memory.IdempotencyStore) edax.IdempotencyStore { return same }, end: laterStepAborts, want: aborted},
		{name: "same database ledger joins so a failed commit frees the key", ledger: func(same, _ *memory.IdempotencyStore) edax.IdempotencyStore { return same }, end: commitFails, want: edax.ErrBadHandle},
		{name: "ledger of its own is refused", ledger: func(_, foreign *memory.IdempotencyStore) edax.IdempotencyStore { return ownLedger{foreign} }, end: commitFails, want: edax.ErrBadHandle, refused: true},
		{name: "joining ledger over another database is refused", ledger: func(_, foreign *memory.IdempotencyStore) edax.IdempotencyStore { return foreign }, end: commitFails, want: edax.ErrBadHandle, refused: true},
		{name: "guard given its own transactor is refused as a nested unit of work", ledger: func(_, foreign *memory.IdempotencyStore) edax.IdempotencyStore { return ownLedger{foreign} }, ownUnit: true, end: commitFails, want: edax.ErrBadHandle, refused: true},
	}
	for _, test := range cases {
		for _, path := range []string{"command", "event"} {
			t.Run(test.name+"/"+path, func(t *testing.T) {
				ctx := context.Background()
				db, other := memory.New(), memory.New()
				defer db.Close()
				defer other.Close()
				same, _ := db.IdempotencyStore()
				defer same.Close()
				foreign, _ := other.IdempotencyStore()
				defer foreign.Close()
				rows, _ := db.ProjectionStore("accounts")
				defer rows.Close()
				ledger := test.ledger(same, foreign)
				options := edax.IdempotentOptions{Scope: "accounts"}
				if test.ownUnit {
					options.UnitOfWork = db
				}
				runs := 0
				deposit := func(ctx context.Context) error {
					runs++
					_, err := rows.Save(ctx, "account-42", 0, `{"id":"account-42","balance":1250}`)
					return err
				}
				var dispatch func(context.Context) error
				var ledgerKey, wantResult string
				var payloadHash uint64
				if path == "command" {
					middleware, err := edax.Idempotent(ledger, options)
					if err != nil {
						t.Fatal(err)
					}
					bus := edax.NewCommandBus(middleware)
					if err = bus.Register("Deposit", func(ctx context.Context, _ edax.CommandMessage) (any, error) { return 1250, deposit(ctx) }); err != nil {
						t.Fatal(err)
					}
					key := "deposit-42"
					message := edax.CommandMessage{ID: "cmd-deposit-42", Name: "Deposit", Version: 1, Data: []byte(`{"account":"account-42","amount":1250}`), IdempotencyKey: &key, CorrelationID: "flow-42", CausationID: "flow-42"}
					dispatch = func(ctx context.Context) error { _, err := bus.Execute(ctx, message); return err }
					if ledgerKey, err = kernel.ClaimKey(string(edax.TenancySilo), options.Scope, key, nil); err != nil {
						t.Fatal(err)
					}
					if payloadHash, err = kernel.CommandHashJSON(message.Name, message.Data); err != nil {
						t.Fatal(err)
					}
					wantResult = "1250"
				} else {
					guard, err := edax.IdempotentByID(ledger, options)
					if err != nil {
						t.Fatal(err)
					}
					router := edax.NewEventRouter(guard)
					if err = router.Register("PaymentReceived", func(ctx context.Context, _ edax.EventMessage) error { return deposit(ctx) }); err != nil {
						t.Fatal(err)
					}
					event := sagaEvent("paid-42", "PaymentReceived", 1)
					dispatch = func(ctx context.Context) error { return router.Dispatch(ctx, event) }
					guarded, err := kernel.GetEventIdempotency(event, string(edax.TenancySilo), options.Scope)
					if err != nil {
						t.Fatal(err)
					}
					ledgerKey, payloadHash, wantResult = guarded.Key, guarded.PayloadHash, "null"
				}
				err := edax.WithinTransaction(ctx, db, func(txctx context.Context) error {
					if err := dispatch(txctx); err != nil {
						return err
					}
					return test.end(txctx, db)
				})
				wantRuns := 1
				if test.refused {
					wantRuns = 0
				}
				if !errors.Is(err, test.want) || edax.IsFatal(err) != errors.Is(test.want, edax.ErrBadHandle) || runs != wantRuns {
					t.Fatalf("host transaction: err=%v runs=%d, want %v after %d runs", err, runs, test.want, wantRuns)
				}
				if row, err := rows.Get(ctx, "account-42"); err != nil || row != nil {
					t.Fatalf("work landed though the host's transaction did not: row=%v err=%v", row, err)
				}
				// The key holds nothing, so no success is cached over work that never landed.
				free, err := ledger.Claim(ctx, ledgerKey, payloadHash)
				if err != nil || !free.Won {
					t.Fatalf("ledger kept a record after the host's transaction failed: %+v %v", free, err)
				}
				if err = ledger.Release(ctx, ledgerKey, free.Handle); err != nil {
					t.Fatal(err)
				}
				if err = dispatch(ctx); err != nil {
					t.Fatalf("redelivery outside the transaction: %v", err)
				}
				row, err := rows.Get(ctx, "account-42")
				if err != nil || row == nil || runs != wantRuns+1 {
					t.Fatalf("redelivered work did not land: row=%v err=%v runs=%d want %d", row, err, runs, wantRuns+1)
				}
				settled, err := ledger.Claim(ctx, ledgerKey, payloadHash)
				if err != nil || settled.Won || settled.Prior == nil {
					t.Fatalf("redelivery left no record: %+v %v", settled, err)
				}
				record, err := kernel.DecodePriorIdempotencyRecord(*settled.Prior, payloadHash)
				if err != nil || record.State != "done" || record.ResultJSON == nil || *record.ResultJSON != wantResult {
					t.Fatalf("redelivery record %+v, want done with %s: %v", record, wantResult, err)
				}
			})
		}
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
		{"whole float past 2^53", 9007199254740992.0, json.Number("9007199254740992.0")},
		{"whole float in a struct", struct {
			Total float64 `json:"total"`
		}{2}, map[string]any{"total": json.Number("2.0")}},
		{"infinite float", math.Inf(1), nil},
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

func TestCommandMiddlewareFactoryReentersBus(t *testing.T) {
	handled := func(result string) edax.CommandHandler {
		return func(context.Context, edax.CommandMessage) (any, error) { return result, nil }
	}
	for _, test := range []struct {
		name    string
		reenter func(*edax.CommandBus) error
		wantErr error
		want    map[string]any
	}{
		{name: "registers another name", reenter: func(bus *edax.CommandBus) error { return bus.Register("AuditInvoice", handled("audit")) },
			want: map[string]any{"IssueInvoice": "issue", "AuditInvoice": "audit"}},
		{name: "dispatches", reenter: func(bus *edax.CommandBus) error {
			if _, err := bus.Dispatch(context.Background(), edax.CommandMessage{ID: "cmd-audit", Name: "AuditInvoice"}); !errors.Is(err, edax.ErrHandlerNotFound) {
				return fmt.Errorf("dispatch during registration: %w", err)
			}
			return nil
		}, want: map[string]any{"IssueInvoice": "issue"}},
		// The factory's registration lands first, so the outer one is the duplicate.
		{name: "registers the same name", reenter: func(bus *edax.CommandBus) error { return bus.Register("IssueInvoice", handled("inner")) },
			wantErr: edax.ErrAlreadyRegistered, want: map[string]any{"IssueInvoice": "inner"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var bus *edax.CommandBus
			var reentered error
			entered := false
			bus = edax.NewCommandBus(func(next edax.CommandHandler) edax.CommandHandler {
				if !entered {
					entered = true
					reentered = test.reenter(bus)
				}
				return next
			})
			done := make(chan error, 1)
			go func() { done <- bus.Register("IssueInvoice", handled("issue")) }()
			select {
			case err := <-done:
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("register: %v want %v", err, test.wantErr)
				}
			case <-time.After(time.Second):
				t.Fatal("middleware factory deadlocked on the bus")
			}
			if reentered != nil {
				t.Fatal(reentered)
			}
			for name, want := range test.want {
				if got, err := bus.Dispatch(context.Background(), edax.CommandMessage{ID: "cmd-" + name, Name: name}); err != nil || got != want {
					t.Fatalf("%s: got %v err %v want %v", name, got, err, want)
				}
			}
		})
	}
}

type transfer struct {
	Amount float64 `json:"amount"`
}

func TestGuardHashesWholeFloats(t *testing.T) {
	codec, err := edax.NewJSONCodec(map[string]func() any{"Transfer": func() any { return new(transfer) }})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		codec edax.PayloadCodec
	}{
		{"raw JSON bytes", nil},
		{"value decoded through the codec", codec},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := memory.New()
			defer db.Close()
			ledger, err := db.IdempotencyStore()
			if err != nil {
				t.Fatal(err)
			}
			defer ledger.Close()
			middleware, err := edax.Idempotent(ledger, edax.IdempotentOptions{Scope: "payments", Codec: test.codec})
			if err != nil {
				t.Fatal(err)
			}
			bus := edax.NewCommandBus(middleware)
			calls := 0
			if err = bus.Register("Transfer", func(context.Context, edax.CommandMessage) (any, error) { calls++; return nil, nil }); err != nil {
				t.Fatal(err)
			}
			// The Python SDK's bytes for transfer{Amount: 2^53}.
			message := edax.CommandMessage{ID: "transfer-42", Name: "Transfer", Version: 1, Data: []byte(`{"amount":9007199254740992.0}`)}
			for range 2 {
				if _, err = bus.Execute(context.Background(), message); err != nil {
					t.Fatal(err)
				}
			}
			message.Data = []byte(`{"amount":9007199254740994.0}`)
			if _, err = bus.Execute(context.Background(), message); calls != 1 || !errors.Is(err, edax.ErrPayloadMismatch) {
				t.Fatalf("calls=%d changed amount: %v", calls, err)
			}
		})
	}
}

type payInvoice struct {
	InvoiceID string `json:"invoice_id"`
	Amount    int64  `json:"amount"`
}

// hashRecorder keeps the payload hash of every claim, which is the hash the guard bound to the key.
type hashRecorder struct {
	edax.IdempotencyStore
	hashes []uint64
}

func (r *hashRecorder) Claim(ctx context.Context, key string, hash uint64) (*edax.ClaimResult, error) {
	r.hashes = append(r.hashes, hash)
	return r.IdempotencyStore.Claim(ctx, key, hash)
}

// TestGuardHashesTheUpcastValueOnlyWithTheHandlersCodec pins docs/sdk/go.md's guidance for a typed
// JSON command whose codec upcasts: only the handler's codec makes the guard hash the value the
// handler runs on. Each pair sends one request key at v1, then at v2.
func TestGuardHashesTheUpcastValueOnlyWithTheHandlersCodec(t *testing.T) {
	// The Python SDK's hashes of the latest-schema values, produced once by
	// crates/edax-py/.venv/bin/python over `class PayInvoice(msgspec.Struct): invoice_id: str; amount: int`:
	// edax.command._payload_hash(PayInvoice(invoice_id="inv-42", amount=N), "PayInvoice").
	const pythonAmount1250, pythonAmount125000 uint64 = 14763933841558427194, 2178581780193684354
	// The same interpreter's edax._edax.command_hash_json("PayInvoice", b'{"invoice_id":"inv-42","cents":1250}'):
	// the v1 bytes hashed as they arrive.
	const rawCents1250 uint64 = 1715745920176444827
	renameCents := func(fields map[string]any) {
		fields["amount"] = fields["cents"]
		delete(fields, "cents")
	}
	dollarsToCents := func(fields map[string]any) { fields["amount"] = fields["amount"].(float64) * 100 }
	for _, test := range []struct {
		name          string
		upcast        func(map[string]any)
		v1, v2        string
		codec         bool
		first, second any
		wantErr       error
		hashes        []uint64
	}{
		// v1 {"cents"} and v2 {"amount"} decode to one handler value, so the second send is a replay.
		{name: "renamed field with the codec replays", upcast: renameCents, v1: `{"invoice_id":"inv-42","cents":1250}`, v2: `{"invoice_id":"inv-42","amount":1250}`, codec: true,
			first: json.Number("1250"), second: json.Number("1250"), hashes: []uint64{pythonAmount1250, pythonAmount1250}},
		{name: "renamed field without the codec is a payload mismatch", upcast: renameCents, v1: `{"invoice_id":"inv-42","cents":1250}`, v2: `{"invoice_id":"inv-42","amount":1250}`,
			first: json.Number("1250"), wantErr: edax.ErrPayloadMismatch, hashes: []uint64{rawCents1250, pythonAmount1250}},
		// One set of bytes means 1250 dollars at v1 and 1250 cents at v2: a different request.
		{name: "changed meaning with the codec is a payload mismatch", upcast: dollarsToCents, v1: `{"invoice_id":"inv-42","amount":1250}`, v2: `{"invoice_id":"inv-42","amount":1250}`, codec: true,
			first: json.Number("125000"), wantErr: edax.ErrPayloadMismatch, hashes: []uint64{pythonAmount125000, pythonAmount1250}},
		{name: "changed meaning without the codec replays the wrong result", upcast: dollarsToCents, v1: `{"invoice_id":"inv-42","amount":1250}`, v2: `{"invoice_id":"inv-42","amount":1250}`,
			first: json.Number("125000"), second: json.Number("125000"), hashes: []uint64{pythonAmount1250, pythonAmount1250}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			codec, err := edax.NewJSONCodec(map[string]func() any{"PayInvoice": func() any { return new(payInvoice) }})
			if err != nil {
				t.Fatal(err)
			}
			if err = codec.WithUpcaster("PayInvoice", &edax.Upcaster{Steps: map[uint32]func([]byte) ([]byte, error){
				1: func(raw []byte) ([]byte, error) {
					var fields map[string]any
					if err := json.Unmarshal(raw, &fields); err != nil {
						return nil, err
					}
					test.upcast(fields)
					return json.Marshal(fields)
				},
			}}); err != nil {
				t.Fatal(err)
			}
			db := memory.New()
			defer db.Close()
			store, err := db.IdempotencyStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			ledger := &hashRecorder{IdempotencyStore: store}
			options := edax.IdempotentOptions{Scope: "billing"}
			if test.codec {
				options.Codec = codec
			}
			middleware, err := edax.Idempotent(ledger, options)
			if err != nil {
				t.Fatal(err)
			}
			bus := edax.NewCommandBus(middleware)
			runs := 0
			handler, err := edax.TypedCommandHandler(codec, func(_ context.Context, command *payInvoice) (int64, error) {
				runs++
				return command.Amount, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = bus.Register("PayInvoice", handler); err != nil {
				t.Fatal(err)
			}
			key := "pay-inv-42"
			send := func(id string, version uint32, data string) (any, error) {
				return bus.Execute(ctx, edax.CommandMessage{ID: id, Name: "PayInvoice", Version: version, Data: []byte(data), IdempotencyKey: &key, CorrelationID: "billing-run-7", CausationID: "billing-run-7"})
			}
			first, err := send("cmd-pay-inv-42-v1", 1, test.v1)
			if err != nil || first != test.first {
				t.Fatalf("first send %v (%T): %v", first, first, err)
			}
			second, err := send("cmd-pay-inv-42-v2", 2, test.v2)
			if !errors.Is(err, test.wantErr) || second != test.second {
				t.Fatalf("second send %v (%T): %v, want %v: %v", second, second, err, test.second, test.wantErr)
			}
			if runs != 1 || !reflect.DeepEqual(ledger.hashes, test.hashes) {
				t.Fatalf("runs=%d hashes=%v, want 1 run and %v", runs, ledger.hashes, test.hashes)
			}
		})
	}
}

func TestExecuteAsReadsAFloatResultAsTheGuardStoresIt(t *testing.T) {
	db := memory.New()
	defer db.Close()
	ledger, err := db.IdempotencyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	middleware, err := edax.Idempotent(ledger, edax.IdempotentOptions{Scope: "payments"})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		bus  *edax.CommandBus
	}{
		{"guarded", edax.NewCommandBus(middleware)},
		{"unguarded", edax.NewCommandBus()},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.bus.Register("Total", func(context.Context, edax.CommandMessage) (any, error) { return 2.0, nil }); err != nil {
				t.Fatal(err)
			}
			message := edax.CommandMessage{ID: "total-" + test.name, Name: "Total", Version: 1, Data: []byte(`{}`)}
			total, err := edax.ExecuteAs[float64](context.Background(), test.bus, message)
			if err != nil || total != 2 {
				t.Fatalf("float total %v: %v", total, err)
			}
			// A float result is a float on both paths, so neither reads it as an integer.
			if count, err := edax.ExecuteAs[int](context.Background(), test.bus, message); !errors.Is(err, edax.ErrCodec) {
				t.Fatalf("integer total %v: %v", count, err)
			}
		})
	}
}

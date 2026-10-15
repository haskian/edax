package edax_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

type purchaseSaga struct {
	calls    int
	fail     bool
	failures []json.RawMessage
	after    func()
}

func (s *purchaseSaga) Name() string { return "Purchase" }
func (s *purchaseSaga) InstanceID(event edax.EventMessage) (string, bool) {
	return event.AggregateID, event.AggregateID != "unrelated"
}
func (s *purchaseSaga) Handle(_ context.Context, _ json.RawMessage, event edax.EventMessage) (edax.SagaStep, error) {
	s.calls++
	if s.after != nil {
		s.after()
	}
	if s.fail {
		return edax.SagaStep{}, edax.ErrStore
	}
	if event.Name == "ShipmentConfirmed" {
		return edax.SagaStep{Complete: true}, nil
	}
	if event.Name == "SagaCommandFailed" {
		s.failures = append(s.failures, json.RawMessage(event.Data))
		return edax.SagaStep{State: json.RawMessage(`{"order_id":"order-1","status":"compensating"}`)}, nil
	}
	return edax.SagaStep{State: json.RawMessage(`{"order_id":"order-1","status":"paid"}`), Commands: []edax.Payload{{Name: "ShipOrder", Version: 1, Data: []byte(`{"order_id":"order-1","address":"Main Street 7"}`)}}}, nil
}

type compensatingPurchase struct{ purchaseSaga }

func (s *compensatingPurchase) Handle(_ context.Context, state json.RawMessage, event edax.EventMessage) (edax.SagaStep, error) {
	s.calls++
	if event.Name != "SagaCommandFailed" {
		return edax.SagaStep{State: json.RawMessage(`{"failed":[]}`), Commands: []edax.Payload{
			{Name: "ShipOrder", Version: 1, Data: []byte(`{"order_id":"order-1","address":"Main Street 7"}`)},
			{Name: "ReserveStock", Version: 1, Data: []byte(`{"order_id":"order-1","sku":"coffee-250g","quantity":2}`)},
		}}, nil
	}
	var failed struct {
		CommandID string `json:"command_id"`
		Command   string `json:"command"`
		Code      string `json:"code"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(event.Data, &failed); err != nil {
		return edax.SagaStep{}, err
	}
	var current struct {
		Failed []string `json:"failed"`
	}
	if err := json.Unmarshal(state, &current); err != nil {
		return edax.SagaStep{}, err
	}
	current.Failed = append(current.Failed, failed.CommandID)
	s.failures = append(s.failures, append(json.RawMessage(nil), event.Data...))
	next, err := json.Marshal(current)
	if err != nil {
		return edax.SagaStep{}, err
	}
	payload, err := json.Marshal(map[string]string{"order_id": "order-1", "failed_command": failed.CommandID})
	if err != nil {
		return edax.SagaStep{}, err
	}
	return edax.SagaStep{State: next, Commands: []edax.Payload{{Name: "RefundOrder", Version: 1, Data: payload}}}, nil
}

func TestSagaCompensatesInterleavedCommandFailuresOnce(t *testing.T) {
	ctx := context.Background()
	db := memory.New()
	defer db.Close()
	store, err := db.SagaStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	saga := &compensatingPurchase{}
	runner, err := edax.NewSagaRunner(saga, store, edax.SagaOptions{Now: func() int64 { return 1720000000000000000 }})
	if err != nil {
		t.Fatal(err)
	}
	paid := sagaEvent("paid-1", "PaymentReceived", 1)
	paid.Tenant = edax.Ptr("tenant-acme")
	if err := runner.OnEvent(ctx, paid); err != nil {
		t.Fatal(err)
	}
	commands, err := runner.Outbox().Unpublished(ctx, 10)
	if err != nil || len(commands) != 2 {
		t.Fatalf("initial commands: %v %v", commands, err)
	}
	var firstState *edax.SagaInstance
	failedIDs := []string{commands[0].ID, commands[1].ID}
	for index, commandIndex := range []int{0, 1, 0, 1} {
		command := commands[commandIndex]
		if err := runner.CommandFailed(ctx, edax.DeadLetter[edax.CommandMessage]{Message: command, Attempt: 3, Failure: &edax.Error{Code: edax.CodeRejected, Message: "carrier refused shipment"}}); err != nil {
			t.Fatal(err)
		}
		instance, err := store.Load(ctx, "Purchase", "order-1")
		if err != nil || instance == nil {
			t.Fatalf("instance: %+v %v", instance, err)
		}
		wantFailures := min(index+1, 2)
		wantState, err := json.Marshal(map[string][]string{"failed": failedIDs[:wantFailures]})
		if err != nil {
			t.Fatal(err)
		}
		if instance.Version != uint64(1+wantFailures) || instance.Completed || string(instance.State) != string(wantState) || saga.calls != 1+wantFailures {
			t.Fatalf("fold %d: %+v state=%s calls=%d", index, instance, instance.State, saga.calls)
		}
		if index == 1 {
			firstState = instance
		}
		if index > 1 && !reflect.DeepEqual(instance, firstState) {
			t.Fatalf("redelivery changed stored state: %+v", instance)
		}
	}
	rows, err := runner.Outbox().Unpublished(ctx, 10)
	if err != nil || len(rows) != 4 || len(saga.failures) != 2 {
		t.Fatalf("compensation rows=%v failures=%s err=%v", rows, saga.failures, err)
	}
	for index, row := range rows[2:] {
		wantID, err := kernel.SagaCommandID("Purchase", "order-1", uint64(index+2), 0)
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]string
		if err := json.Unmarshal(row.Data, &payload); err != nil {
			t.Fatal(err)
		}
		var failure map[string]any
		if err := json.Unmarshal(saga.failures[index], &failure); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(failure, map[string]any{"command_id": commands[index].ID, "command": commands[index].Name, "code": "rejected", "error": "carrier refused shipment"}) {
			t.Fatalf("failure payload: %v", failure)
		}
		if row.ID != wantID || row.IdempotencyKey == nil || *row.IdempotencyKey != wantID || row.Name != "RefundOrder" || row.Version != 1 || row.CausationID != commands[index].ID+":failed" || row.CorrelationID != paid.CorrelationID || row.Tenant == nil || *row.Tenant != *paid.Tenant || !reflect.DeepEqual(payload, map[string]string{"order_id": "order-1", "failed_command": commands[index].ID}) {
			t.Fatalf("compensation: %+v payload=%v", row, payload)
		}
	}
}

type interruptedSagaStore struct {
	edax.SagaStore
	lostReply bool
	stale     bool
}

func (s *interruptedSagaStore) Load(ctx context.Context, name, id string) (*edax.SagaInstance, error) {
	if s.stale {
		return nil, nil
	}
	return s.SagaStore.Load(ctx, name, id)
}

func (s *interruptedSagaStore) Save(ctx context.Context, name, id string, state json.RawMessage, expected uint64, folded string, commands []edax.CommandMessage) (uint64, error) {
	version, err := s.SagaStore.Save(ctx, name, id, state, expected, folded, commands)
	if err == nil && s.lostReply {
		// The server commits the transaction, but its reply never reaches the runner.
		tx, ok := edax.CurrentTransaction(ctx, s.UnitOfWork())
		if !ok {
			return 0, edax.ErrBadHandle
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, edax.ErrStore
	}
	return version, err
}

func TestSagaRecoversFromInterruptedFolds(t *testing.T) {
	for _, test := range []struct {
		name  string
		want  error
		calls int
	}{
		{"lost_write_reply", edax.ErrStore, 1},
		{"conflicting_fold", edax.ErrConcurrency, 2},
		{"cancel_before_fold", context.Canceled, 0},
		{"cancel_after_handler", context.Canceled, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db := memory.New()
			defer db.Close()
			store, err := db.SagaStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			wrapped := &interruptedSagaStore{SagaStore: store}
			saga := &purchaseSaga{}
			runner, err := edax.NewSagaRunner(saga, wrapped, edax.SagaOptions{})
			if err != nil {
				t.Fatal(err)
			}
			event := sagaEvent("paid-1", "PaymentReceived", 1)
			var winner *edax.SagaInstance
			if test.name == "conflicting_fold" {
				if err := runner.OnEvent(ctx, event); err != nil {
					t.Fatal(err)
				}
				winner, err = store.Load(ctx, "Purchase", "order-1")
				if err != nil {
					t.Fatal(err)
				}
				wrapped.stale = true
			}
			attempt, cancel := context.WithCancel(ctx)
			defer cancel()
			switch test.name {
			case "lost_write_reply":
				wrapped.lostReply = true
			case "cancel_before_fold":
				cancel()
			case "cancel_after_handler":
				saga.after = cancel
			}
			if err := runner.OnEvent(attempt, event); !errors.Is(err, test.want) {
				t.Fatalf("fold: %v want %v", err, test.want)
			}
			instance, err := store.Load(ctx, "Purchase", "order-1")
			if err != nil {
				t.Fatal(err)
			}
			rows, err := runner.Outbox().Unpublished(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			landed := test.name == "lost_write_reply" || test.name == "conflicting_fold"
			if saga.calls != test.calls {
				t.Fatalf("calls=%d want %d", saga.calls, test.calls)
			}
			if landed {
				if instance == nil || instance.Version != 1 || instance.Completed || string(instance.State) != `{"order_id":"order-1","status":"paid"}` || len(rows) != 1 {
					t.Fatalf("committed instance=%+v commands=%v", instance, rows)
				}
				if winner != nil && !reflect.DeepEqual(instance, winner) {
					t.Fatalf("loser changed winner: %+v", instance)
				}
			} else if instance != nil || len(rows) != 0 {
				t.Fatalf("cancelled fold persisted %+v %v", instance, rows)
			}
			wrapped.lostReply, wrapped.stale, saga.after = false, false, nil
			if err := runner.OnEvent(ctx, event); err != nil {
				t.Fatal(err)
			}
			retried, err := store.Load(ctx, "Purchase", "order-1")
			if err != nil {
				t.Fatal(err)
			}
			commands, err := runner.Outbox().Unpublished(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			wantCalls := test.calls
			if !landed {
				wantCalls++
			}
			if retried == nil || retried.Version != 1 || retried.Completed || string(retried.State) != `{"order_id":"order-1","status":"paid"}` || len(commands) != 1 || saga.calls != wantCalls {
				t.Fatalf("retry state=%+v rows=%v calls=%d", retried, commands, saga.calls)
			}
			if landed && (!reflect.DeepEqual(instance, retried) || !reflect.DeepEqual(rows, commands)) {
				t.Fatal("redelivery changed a committed fold")
			}
		})
	}
}
func sagaEvent(id, name string, sequence uint64) edax.EventMessage {
	return edax.EventMessage{ID: id, AggregateID: "order-1", Name: name, Version: 1, Sequence: &sequence, Data: []byte(`{"order_id":"order-1","amount":"45.00","currency":"EUR"}`), CorrelationID: "checkout-1", CausationID: "payment-1", RecordedAt: 1720000000000000000, Extensions: map[string]string{"traceparent": "trace-1"}, Metadata: map[string]string{"source": "checkout"}}
}
func TestSagaCommitsStateAndCommandsAndSkipsRedelivery(t *testing.T) {
	ctx := context.Background()
	db := memory.New()
	defer db.Close()
	store, err := db.SagaStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	saga := &purchaseSaga{}
	var skips []string
	runner, err := edax.NewSagaRunner(saga, store, edax.SagaOptions{Now: func() int64 { return 1720000000000000000 }, OnSkipped: func(_, action, _ string) { skips = append(skips, action) }})
	if err != nil {
		t.Fatal(err)
	}
	paid := sagaEvent("paid-1", "PaymentReceived", 2)
	if err := runner.OnEvent(ctx, paid); err != nil {
		t.Fatal(err)
	}
	instance, err := store.Load(ctx, "Purchase", "order-1")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := runner.Outbox().Unpublished(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if instance == nil || instance.Version != 1 || instance.Completed || len(rows) != 1 {
		t.Fatalf("instance=%+v rows=%v", instance, rows)
	}
	expected, err := kernel.SagaCommandID("Purchase", "order-1", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].ID != expected || rows[0].IdempotencyKey == nil || *rows[0].IdempotencyKey != expected || rows[0].CausationID != paid.ID || rows[0].CorrelationID != paid.CorrelationID {
		t.Fatalf("command=%+v", rows[0])
	}
	for _, event := range []edax.EventMessage{paid, sagaEvent("old-1", "PaymentReceived", 1)} {
		if err := runner.OnEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	if err := runner.OnEvent(ctx, sagaEvent("shipped-1", "ShipmentConfirmed", 3)); err != nil {
		t.Fatal(err)
	}
	if err := runner.OnEvent(ctx, sagaEvent("late-1", "PaymentReceived", 4)); err != nil {
		t.Fatal(err)
	}
	instance, err = store.Load(ctx, "Purchase", "order-1")
	if err != nil {
		t.Fatal(err)
	}
	if saga.calls != 2 || instance == nil || !instance.Completed || instance.Version != 2 || len(skips) != 3 {
		t.Fatalf("calls=%d instance=%+v skips=%v", saga.calls, instance, skips)
	}
}
func TestSagaCommandFailedCarriesBareMessage(t *testing.T) {
	for _, test := range []struct {
		name          string
		failure       error
		code, message any
	}{
		{name: "kernel error", failure: &edax.Error{Code: edax.CodeRejected, Message: "address is not deliverable"}, code: "rejected", message: "address is not deliverable"},
		{name: "sentinel", failure: edax.ErrCodec, code: "codec", message: ""},
		{name: "host error", failure: fmt.Errorf("carrier API: %w", errors.New("connection reset")), code: nil, message: "carrier API: connection reset"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db := memory.New()
			defer db.Close()
			store, err := db.SagaStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			saga := &purchaseSaga{}
			runner, err := edax.NewSagaRunner(saga, store, edax.SagaOptions{Now: func() int64 { return 1720000000000000000 }})
			if err != nil {
				t.Fatal(err)
			}
			if err = runner.OnEvent(ctx, sagaEvent("paid-1", "PaymentReceived", 1)); err != nil {
				t.Fatal(err)
			}
			rows, err := runner.Outbox().Unpublished(ctx, 10)
			if err != nil || len(rows) != 1 {
				t.Fatalf("rows=%v err=%v", rows, err)
			}
			if err = runner.CommandFailed(ctx, edax.DeadLetter[edax.CommandMessage]{Message: rows[0], Attempt: 3, Failure: test.failure}); err != nil {
				t.Fatal(err)
			}
			if len(saga.failures) != 1 {
				t.Fatalf("failures=%s", saga.failures)
			}
			var payload map[string]any
			if err = json.Unmarshal(saga.failures[0], &payload); err != nil {
				t.Fatal(err)
			}
			// The code travels beside the message, so the message never repeats it.
			if payload["code"] != test.code || payload["error"] != test.message {
				t.Fatalf("payload=%s", saga.failures[0])
			}
		})
	}
}
func TestSagaFailureRollsBackStateAndCommands(t *testing.T) {
	ctx := context.Background()
	db := memory.New()
	defer db.Close()
	store, err := db.SagaStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	saga := &purchaseSaga{fail: true}
	runner, err := edax.NewSagaRunner(saga, store, edax.SagaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.OnEvent(ctx, sagaEvent("paid-1", "PaymentReceived", 1)); !errors.Is(err, edax.ErrStore) {
		t.Fatal(err)
	}
	instance, err := store.Load(ctx, "Purchase", "order-1")
	if err != nil || instance != nil {
		t.Fatalf("instance=%v err=%v", instance, err)
	}
	rows, err := runner.Outbox().Unpublished(ctx, 10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	saga.fail = false
	if err := runner.OnEvent(ctx, sagaEvent("paid-1", "PaymentReceived", 1)); err != nil {
		t.Fatal(err)
	}
	rows, err = runner.Outbox().Unpublished(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("retry rows=%v err=%v", rows, err)
	}
}
func TestEventGuardSharesProjectionTransaction(t *testing.T) {
	ctx := context.Background()
	db := memory.New()
	defer db.Close()
	ledger, err := db.IdempotencyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	projections, err := db.ProjectionStore("orders")
	if err != nil {
		t.Fatal(err)
	}
	defer projections.Close()
	guard, err := edax.IdempotentByID(ledger, edax.IdempotentOptions{Scope: "order-summary", UnitOfWork: db})
	if err != nil {
		t.Fatal(err)
	}
	router := edax.NewEventRouter(guard)
	calls := 0
	fail := true
	if err := router.Register("PaymentReceived", func(ctx context.Context, event edax.EventMessage) error {
		calls++
		_, err := projections.Save(ctx, event.AggregateID, 0, `{"status":"paid","total":"45.00"}`)
		if err != nil {
			return err
		}
		if fail {
			return edax.ErrStore
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	event := sagaEvent("paid-1", "PaymentReceived", 1)
	if err := router.Dispatch(ctx, event); !errors.Is(err, edax.ErrStore) {
		t.Fatal(err)
	}
	row, err := projections.Get(ctx, event.AggregateID)
	if err != nil || row != nil {
		t.Fatalf("rolled-back row=%v err=%v", row, err)
	}
	fail = false
	for range 2 {
		if err := router.Dispatch(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	row, err = projections.Get(ctx, event.AggregateID)
	if err != nil || row == nil || row.Version != 1 || calls != 2 {
		t.Fatalf("row=%v calls=%d err=%v", row, calls, err)
	}
}

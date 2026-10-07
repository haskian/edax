package edax_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

type purchaseSaga struct {
	calls int
	fail  bool
}

func (s *purchaseSaga) Name() string { return "Purchase" }
func (s *purchaseSaga) InstanceID(event edax.EventMessage) (string, bool) {
	return event.AggregateID, event.AggregateID != "unrelated"
}
func (s *purchaseSaga) Handle(_ context.Context, _ json.RawMessage, event edax.EventMessage) (edax.SagaStep, error) {
	s.calls++
	if s.fail {
		return edax.SagaStep{}, edax.ErrStore
	}
	if event.Name == "ShipmentConfirmed" {
		return edax.SagaStep{Complete: true}, nil
	}
	return edax.SagaStep{State: json.RawMessage(`{"order_id":"order-1","status":"paid"}`), Commands: []edax.Payload{{Name: "ShipOrder", Version: 1, Data: []byte(`{"order_id":"order-1","address":"Main Street 7"}`)}}}, nil
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
	projections, err := db.ProjectionStore()
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

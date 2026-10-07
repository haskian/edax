// Save saga state and commands atomically, then relay each command through an idempotent bus.
// Run from crates/edax-go: mise exec -- go run ./examples/13_saga
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
)

type shipOrder struct {
	OrderID string `json:"order_id"`
}
type orderSaga struct{ commands *edax.JSONCodec }

func (orderSaga) Name() string                                      { return "order" }
func (orderSaga) InstanceID(event edax.EventMessage) (string, bool) { return event.AggregateID, true }
func (s orderSaga) Handle(_ context.Context, state json.RawMessage, event edax.EventMessage) (edax.SagaStep, error) {
	switch event.Name {
	case "OrderPlaced":
		return edax.SagaStep{State: json.RawMessage(`"awaiting payment"`)}, nil
	case "PaymentReceived":
		payload, err := s.commands.Marshal(&shipOrder{OrderID: event.AggregateID})
		if err != nil {
			return edax.SagaStep{}, err
		}
		return edax.SagaStep{Complete: true, Commands: []edax.Payload{payload}}, nil
	default:
		return edax.SagaStep{State: state}, nil
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: completed saga; one ShipOrder despite duplicate payment and command delivery")
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
	sagas, err := db.SagaStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, sagas.Close()) }()
	codec, err := edax.NewJSONCodec(map[string]func() any{"ShipOrder": func() any { return new(shipOrder) }})
	if err != nil {
		return err
	}
	guard, err := edax.Idempotent(ledger, edax.IdempotentOptions{Scope: "shipping", Tenancy: edax.TenancySilo})
	if err != nil {
		return err
	}
	bus := edax.NewCommandBus(guard)
	var shipped []string
	var lineages []edax.Lineage
	handler, err := edax.TypedCommandHandler(codec, func(ctx context.Context, command *shipOrder) (any, error) {
		lineage, ok := edax.CurrentLineage(ctx)
		if !ok {
			return nil, errors.New("shipping handler has no lineage")
		}
		shipped = append(shipped, command.OrderID)
		lineages = append(lineages, lineage)
		return nil, nil
	})
	if err != nil {
		return err
	}
	if err := bus.Register("ShipOrder", handler); err != nil {
		return err
	}
	runner, err := edax.NewSagaRunner(orderSaga{commands: codec}, sagas, edax.SagaOptions{})
	if err != nil {
		return err
	}
	event := func(name string, sequence uint64) edax.EventMessage {
		return edax.EventMessage{ID: "order-1-" + name, AggregateID: "order-1", Name: name, Version: 1, Sequence: edax.Ptr(sequence), Data: []byte(`{"order_id":"order-1"}`), CorrelationID: "checkout-1", CausationID: "checkout-1", RecordedAt: edax.CurrentTimestamp()}
	}
	placed, paid := event("OrderPlaced", 1), event("PaymentReceived", 2)
	if err := runner.OnEvent(ctx, placed); err != nil {
		return err
	}
	instance, err := sagas.Load(ctx, "order", "order-1")
	if err != nil {
		return err
	}
	if instance == nil || instance.Completed || string(instance.State) != `"awaiting payment"` {
		return fmt.Errorf("unexpected running saga: %+v", instance)
	}
	fmt.Println("OrderPlaced: awaiting payment")
	for range 2 {
		if err := runner.OnEvent(ctx, paid); err != nil {
			return err
		}
	}
	rows, err := runner.Outbox().Unpublished(ctx, 100)
	if err != nil {
		return err
	}
	if len(rows) != 1 || len(shipped) != 0 {
		return fmt.Errorf("before relay: commands=%d shipped=%v", len(rows), shipped)
	}
	row := rows[0]
	if row.IdempotencyKey == nil || *row.IdempotencyKey != row.ID || row.AggregateID == nil || *row.AggregateID != "order-1" || row.CorrelationID != paid.CorrelationID || row.CausationID != paid.ID {
		return fmt.Errorf("unexpected saga command: %+v", row)
	}
	var dead []string
	relay, err := edax.NewRelay(runner.Outbox(), runner.PublishTo(bus), edax.RelayOptions[edax.CommandMessage]{
		OnDead: func(_ context.Context, letter edax.DeadLetter[edax.CommandMessage]) error {
			dead = append(dead, letter.Message.ID)
			return nil
		},
	})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, relay.Close()) }()
	sent, err := relay.Drain(ctx, 100)
	if err != nil {
		return err
	}
	// A crash after publish but before marking the row can resend the same command.
	if _, err := bus.Dispatch(ctx, row); err != nil {
		return err
	}
	remaining, err := runner.Outbox().Unpublished(ctx, 100)
	if err != nil {
		return err
	}
	instance, err = sagas.Load(ctx, "order", "order-1")
	if err != nil {
		return err
	}
	if sent != 1 || len(remaining) != 0 || instance == nil || !instance.Completed || !slices.Equal(shipped, []string{"order-1"}) || len(dead) != 0 || !slices.Equal(lineages, []edax.Lineage{{CorrelationID: paid.CorrelationID, CausationID: row.ID}}) {
		return fmt.Errorf("sent=%d remaining=%d state=%+v shipped=%v dead=%v lineage=%v", sent, len(remaining), instance, shipped, dead, lineages)
	}
	return nil
}

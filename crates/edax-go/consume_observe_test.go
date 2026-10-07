package edax

import (
	"context"
	"testing"
	"time"
)

func TestConsumeObservationMatchesSettlement(t *testing.T) {
	var observed Observation
	observer := ObserverFunc(func(event Observation) {
		if event.Operation == "delivery" {
			observed = event
		}
	})
	dispatcher, err := NewKeyOrderedDispatcher(func(context.Context, EventMessage) error { return ErrCodec }, DispatcherOptions[EventMessage]{Workers: 1, Observer: observer, OnDead: func(context.Context, DeadLetter[EventMessage]) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := dispatcher.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Submit(ctx, &Delivery[EventMessage]{Message: consumeEvent("credit-1", "account-1", 1)}); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if observed.Outcome != "dead" || observed.ID != "credit-1" || observed.Name != "AccountCredited" || observed.Err != ErrCodec {
		t.Fatalf("observation=%+v", observed)
	}
	stats, err := dispatcher.Stats()
	if err != nil || stats.Pending != 0 || stats.Handling != 0 || !stats.Running {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}
func TestRelayObservationPreservesPartialProgress(t *testing.T) {
	store := &relayTestStore{rows: []EventMessage{consumeEvent("published", "account-1", 1), consumeEvent("failed", "account-2", 1)}}
	var observed Observation
	relay, err := NewRelay(store, func(_ context.Context, event EventMessage) error {
		if event.ID == "failed" {
			return ErrStore
		}
		return nil
	}, RelayOptions[EventMessage]{Pacing: instantPacing(), Observer: ObserverFunc(func(event Observation) { observed = event })})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	if _, err := relay.Drain(context.Background(), 10); err == nil {
		t.Fatal("expected failed publish")
	}
	if observed.Operation != "drain" || observed.Outcome != "failed" || observed.Published != 1 || observed.OldestTimestamp == nil || *observed.OldestTimestamp != store.rows[0].RecordedAt || observed.Err == nil {
		t.Fatalf("observation=%+v", observed)
	}
}

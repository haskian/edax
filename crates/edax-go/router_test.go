package edax

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestEventRouterMiddlewareAndUnhandled(t *testing.T) {
	var order []string
	middleware := func(name string) EventMiddleware {
		return func(next EventHandler) EventHandler {
			return func(ctx context.Context, message EventMessage) error {
				order = append(order, name)
				return next(ctx, message)
			}
		}
	}
	router := NewEventRouter(middleware("outer"), middleware("inner"))
	handler := func(ctx context.Context, message EventMessage) error {
		lineage, ok := CurrentLineage(ctx)
		if !ok || lineage.CausationID != message.ID || lineage.CorrelationID != message.CorrelationID {
			t.Errorf("lineage=%+v", lineage)
		}
		order = append(order, "handler")
		return nil
	}
	if err := router.Register("AccountCredited", handler); err != nil {
		t.Fatal(err)
	}
	if err := router.Register("AccountCredited", handler); !errors.Is(err, ErrAlreadyRegistered) {
		t.Fatalf("duplicate=%v", err)
	}
	event := consumeEvent("credit-1", "account-1", 1)
	if err := router.Dispatch(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"outer", "inner", "handler"}) {
		t.Fatal(order)
	}
	order = nil
	event.Name = "UnrelatedEvent"
	if err := router.Dispatch(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(order) != 0 {
		t.Fatal(order)
	}
	if err := router.OnUnhandled(handler); err != nil {
		t.Fatal(err)
	}
	if err := router.Dispatch(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"outer", "inner", "handler"}) {
		t.Fatal(order)
	}
}
func TestEventRouterRefusesBrokenWiring(t *testing.T) {
	for _, test := range []struct {
		name    string
		router  *EventRouter
		handler EventHandler
	}{
		{"nil handler", NewEventRouter(), nil},
		{"nil middleware", NewEventRouter(nil), func(context.Context, EventMessage) error { return nil }},
		{"nil wrapped handler", NewEventRouter(func(EventHandler) EventHandler { return nil }), func(context.Context, EventMessage) error { return nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.router.Register("AccountCredited", test.handler); !errors.Is(err, ErrBadHandle) {
				t.Fatal(err)
			}
		})
	}
}

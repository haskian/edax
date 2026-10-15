package edax

import (
	"context"
	"errors"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
	"reflect"
	"testing"
	"time"
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
func TestEventRouterMiddlewareFactoryReentersRouter(t *testing.T) {
	register := func(router *EventRouter, handler EventHandler) error {
		return router.Register("AccountCredited", handler)
	}
	dispatch := func(router *EventRouter, _ EventHandler) error {
		return router.Dispatch(context.Background(), consumeEvent("credit-0", "account-1", 1))
	}
	for _, test := range []struct {
		name    string
		install func(*EventRouter, EventHandler) error
		reenter func(*EventRouter, EventHandler) error
		wantErr error
		want    map[string]string
	}{
		{name: "registers another name", install: register, reenter: func(router *EventRouter, handler EventHandler) error {
			return router.Register("AccountAudited", handler)
		},
			want: map[string]string{"AccountCredited": "outer", "AccountAudited": "inner"}},
		{name: "dispatches", install: register, reenter: dispatch, want: map[string]string{"AccountCredited": "outer"}},
		// The factory's registration lands first, so the outer one is the duplicate.
		{name: "registers the same name", install: register, reenter: register, wantErr: ErrAlreadyRegistered, want: map[string]string{"AccountCredited": "inner"}},
		{name: "unhandled dispatches", install: (*EventRouter).OnUnhandled, reenter: dispatch, want: map[string]string{"AccountAudited": "outer"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var seen []string
			handler := func(label string) EventHandler {
				return func(context.Context, EventMessage) error { seen = append(seen, label); return nil }
			}
			var router *EventRouter
			var reentered error
			entered := false
			router = NewEventRouter(func(next EventHandler) EventHandler {
				if !entered {
					entered = true
					reentered = test.reenter(router, handler("inner"))
				}
				return next
			})
			done := make(chan error, 1)
			go func() { done <- test.install(router, handler("outer")) }()
			select {
			case err := <-done:
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("install: %v want %v", err, test.wantErr)
				}
			case <-time.After(time.Second):
				t.Fatal("middleware factory deadlocked on the router")
			}
			if reentered != nil {
				t.Fatal(reentered)
			}
			for name, want := range test.want {
				seen = nil
				event := consumeEvent("credit-1", "account-1", 1)
				event.Name = name
				if err := router.Dispatch(context.Background(), event); err != nil || !reflect.DeepEqual(seen, []string{want}) {
					t.Fatalf("%s: seen %v err %v want %s", name, seen, err, want)
				}
			}
		})
	}
}

func TestOpenedEventProvenanceRejectsMutableAliases(t *testing.T) {
	for _, field := range []string{"data", "metadata", "subject"} {
		t.Run(field, func(t *testing.T) {
			keys := NewMemoryKeys()
			defer keys.Close()
			sealing, err := NewSealing(keys, "test", "")
			if err != nil {
				t.Fatal(err)
			}
			draft := EventDraft{ID: "event", AggregateID: "aggregate", Subject: Ptr("owner"), Name: "Changed", Version: 1, Data: []byte(`{"value":1}`), CorrelationID: "flow", CausationID: "cause", Metadata: map[string]string{"source": "original"}}
			sealed, err := sealing.SealEvent(context.Background(), draft)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := kernel.RecordEvent(*sealed, 1)
			if err != nil {
				t.Fatal(err)
			}
			plain := NewEventRouter()
			if err := plain.Register("Changed", func(context.Context, EventMessage) error {
				t.Fatal("unconfigured router passed ciphertext to handler")
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := plain.Dispatch(context.Background(), *raw); !errors.Is(err, ErrBadHandle) {
				t.Fatalf("unconfigured router accepted encryption: %v", err)
			}
			router := NewSealedEventRouter(sealing)
			err = router.Register("Changed", func(ctx context.Context, view EventMessage) error {
				if originalEvent(ctx, view).Encryption == nil {
					t.Fatal("unchanged view lost provenance")
				}
				switch field {
				case "data":
					view.Data[0] = '['
				case "metadata":
					view.Metadata["source"] = "forged"
				case "subject":
					*view.Subject = "forged"
				}
				cached := ctx.Value(authenticatedEventKey{}).(authenticatedEvent)
				_, err := cached.openedPayload(originalEvent(ctx, view))
				if !errors.Is(err, ErrCodec) {
					t.Fatalf("mutated %s retained authenticated provenance: %v", field, err)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := router.Dispatch(context.Background(), *raw); err != nil {
				t.Fatal(err)
			}
		})
	}
}

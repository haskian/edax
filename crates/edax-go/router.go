package edax

import (
	"bytes"
	"context"
	"fmt"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
	"sync"
)

// EventHandler processes one event and returns failures for the caller to settle or retry.
type EventHandler func(context.Context, EventMessage) error

// EventMiddleware wraps a handler; the first registered middleware runs outermost.
type EventMiddleware func(EventHandler) EventHandler

// EventRouter routes by event name. Unhandled events run no middleware unless OnUnhandled is set.
type EventRouter struct {
	mu         sync.RWMutex
	handlers   map[string]EventHandler
	middleware []EventMiddleware
	unhandled  EventHandler
	sealing    *Sealing
}

// NewEventRouter creates an empty registry with the supplied middleware.
func NewEventRouter(middleware ...EventMiddleware) *EventRouter {
	return &EventRouter{handlers: make(map[string]EventHandler), middleware: append([]EventMiddleware(nil), middleware...)}
}

// NewSealedEventRouter opens authenticated payloads at the handler boundary.
func NewSealedEventRouter(sealing *Sealing, middleware ...EventMiddleware) *EventRouter {
	r := NewEventRouter(middleware...)
	r.sealing = sealing
	return r
}

type openedEventKey struct{}
type openedEventPair struct {
	raw      EventMessage
	viewWire []byte
}

type authenticatedEventKey struct{}
type authenticatedEvent struct {
	sealing      *Sealing
	wire, opened []byte
}

func (event authenticatedEvent) openedPayload(message EventMessage) ([]byte, error) {
	wire, err := EncodeEvent("json", message)
	if err != nil {
		return nil, err
	}
	if event.sealing == nil || !bytes.Equal(wire, event.wire) {
		return nil, &Error{Code: CodeCodec, Message: "event changed after authentication"}
	}
	return append([]byte(nil), event.opened...), nil
}

func originalEvent(ctx context.Context, message EventMessage) EventMessage {
	if pair, ok := ctx.Value(openedEventKey{}).(openedEventPair); ok {
		wire, err := EncodeEvent("json", message)
		if err == nil && bytes.Equal(wire, pair.viewWire) {
			return pair.raw
		}
	}
	return message
}

func openedEventView(raw EventMessage, data []byte) (EventMessage, error) {
	encoded, err := EncodeEvent("json", raw)
	if err != nil {
		return EventMessage{}, err
	}
	view, err := DecodeEvent("json", encoded)
	if err != nil {
		return EventMessage{}, err
	}
	view.Data = append([]byte(nil), data...)
	view.Encryption = nil
	return *view, nil
}

// wrap runs the factories outside the lock, so one may register or dispatch on
// this router. The middleware list never changes after construction.
func (r *EventRouter) wrap(handler EventHandler) (EventHandler, error) {
	if r.sealing != nil {
		next := handler
		handler = func(ctx context.Context, raw EventMessage) error {
			authenticated, _ := ctx.Value(authenticatedEventKey{}).(authenticatedEvent)
			if authenticated.sealing != r.sealing {
				return &Error{Code: CodeBadHandle, Message: "event handler requires authenticated dispatch"}
			}
			data, err := authenticated.openedPayload(raw)
			if err != nil {
				return err
			}
			view, err := openedEventView(raw, data)
			if err != nil {
				return err
			}
			viewWire, err := EncodeEvent("json", view)
			if err != nil {
				return err
			}
			ctx = context.WithValue(ctx, openedEventKey{}, openedEventPair{raw: raw, viewWire: viewWire})
			return next(ctx, view)
		}
	}

	for i := len(r.middleware) - 1; i >= 0; i-- {
		if r.middleware[i] == nil {
			return nil, &Error{Code: CodeBadHandle, Message: "nil event middleware"}
		}
		handler = r.middleware[i](handler)
		if handler == nil {
			return nil, &Error{Code: CodeBadHandle, Message: "middleware returned a nil event handler"}
		}
	}
	return handler, nil
}

// Register installs a named handler and refuses duplicates. It is safe during dispatch.
func (r *EventRouter) Register(name string, handler EventHandler) error {
	if name == "" || handler == nil {
		return &Error{Code: CodeBadHandle, Message: "event registration needs a name and handler"}
	}
	r.mu.RLock()
	_, exists := r.handlers[name]
	r.mu.RUnlock()
	if exists {
		return &Error{Code: CodeAlreadyRegistered, Message: name}
	}
	wrapped, err := r.wrap(handler)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[name]; exists {
		return &Error{Code: CodeAlreadyRegistered, Message: name}
	}
	if r.handlers == nil {
		r.handlers = make(map[string]EventHandler)
	}
	r.handlers[name] = wrapped
	return nil
}

// OnUnhandled sets a middleware-wrapped fallback; nil restores silent skipping.
func (r *EventRouter) OnUnhandled(handler EventHandler) error {
	var wrapped EventHandler
	if handler != nil {
		var err error
		if wrapped, err = r.wrap(handler); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unhandled = wrapped
	return nil
}

// Dispatch invokes the named or fallback handler with child-message lineage in ctx.
// Different dispatches may invoke handlers concurrently; the router does not settle broker deliveries.
func (r *EventRouter) Dispatch(ctx context.Context, message EventMessage) error {
	if r.sealing == nil && message.Encryption != nil {
		return &Error{Code: CodeBadHandle, Message: "encrypted events require router sealing"}
	}
	if r.sealing != nil {
		wire, err := EncodeEvent("json", message)
		if err != nil {
			return err
		}
		owned, err := DecodeEvent("json", wire)
		if err != nil {
			return err
		}
		message = *owned
		data, skip, err := r.sealing.OpenEventDelivery(ctx, message)
		if err != nil || skip {
			return err
		}
		defer clear(data)
		ctx = context.WithValue(ctx, authenticatedEventKey{}, authenticatedEvent{sealing: r.sealing, wire: wire, opened: data})
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.RLock()
	handler := r.handlers[message.Name]
	if handler == nil {
		handler = r.unhandled
	}
	r.mu.RUnlock()
	if handler == nil {
		return nil
	}
	if r.sealing != nil {
		scope, err := r.sealing.Scope(message.Tenant, message.Subject)
		if err != nil {
			return err
		}
		guarded, release, err := r.sealing.Guard(ctx, scope)
		if err != nil {
			if destroyedKey(err) {
				return nil
			}
			return err
		}
		ctx = guarded
		if !HoldCurrentTransactionResource(ctx, release) {
			defer release()
		}
	}
	handlerContext, err := CausedBy(ctx, message.ID, message.CorrelationID)
	if err != nil {
		return err
	}
	return handler(handlerContext, message)
}

// IdempotentByID shares the command guard's settlement rules. The scope separates consumers.
func IdempotentByID(store IdempotencyStore, options IdempotentOptions) (EventMiddleware, error) {
	if store == nil {
		return nil, &Error{Code: CodeBadHandle, Message: "event idempotency requires a store"}
	}
	if options.Tenancy == "" {
		options.Tenancy = TenancySilo
	}
	if _, err := kernel.GetTenancyRules(string(options.Tenancy)); err != nil {
		return nil, err
	}
	return func(next EventHandler) EventHandler {
		return func(ctx context.Context, message EventMessage) error {
			if authenticated, ok := ctx.Value(authenticatedEventKey{}).(authenticatedEvent); ok {
				if _, err := authenticated.openedPayload(message); err != nil {
					return err
				}
			}
			input, err := kernel.GetEventIdempotency(message, string(options.Tenancy), options.Scope)
			if err != nil {
				return err
			}
			_, err = GuardWithUnitOfWork(ctx, store, options.UnitOfWork, input.Key, input.PayloadHash, func(ctx context.Context) (any, error) { return nil, next(ctx, message) })
			return err
		}
	}, nil
}

// ForgetEvent removes the event guard record for this scope and tenancy, allowing another attempt.
func ForgetEvent(ctx context.Context, store IdempotencyStore, scope string, tenancy Tenancy, message EventMessage) error {
	if tenancy == "" {
		tenancy = TenancySilo
	}
	if store == nil {
		return &Error{Code: CodeBadHandle, Message: "forget event requires a store"}
	}
	input, err := kernel.GetEventIdempotency(message, string(tenancy), scope)
	if err != nil {
		return err
	}
	return store.Forget(ctx, input.Key)
}

// TypedEventHandler decodes a registered payload into the application's event type.
func TypedEventHandler[E any](codec PayloadCodec, handler func(context.Context, E) error) (EventHandler, error) {
	if codec == nil || handler == nil {
		return nil, &Error{Code: CodeBadHandle, Message: "typed event handler requires a codec and function"}
	}
	return func(ctx context.Context, message EventMessage) error {
		value, err := codec.Unmarshal(Payload{Name: message.Name, Version: message.Version, Data: message.Data})
		if err != nil {
			return err
		}
		event, ok := value.(E)
		if !ok {
			return &Error{Code: CodeCodec, Message: fmt.Sprintf("event codec returned %T for %s", value, message.Name)}
		}
		return handler(ctx, event)
	}, nil
}

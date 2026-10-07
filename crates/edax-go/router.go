package edax

import (
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
}

// NewEventRouter creates an empty registry with the supplied middleware.
func NewEventRouter(middleware ...EventMiddleware) *EventRouter {
	return &EventRouter{handlers: make(map[string]EventHandler), middleware: append([]EventMiddleware(nil), middleware...)}
}
func (r *EventRouter) wrap(handler EventHandler) (EventHandler, error) {
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
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.handlers[name]; ok {
		return &Error{Code: CodeAlreadyRegistered, Message: name}
	}
	wrapped, err := r.wrap(handler)
	if err != nil {
		return err
	}
	if r.handlers == nil {
		r.handlers = make(map[string]EventHandler)
	}
	r.handlers[name] = wrapped
	return nil
}

// OnUnhandled sets a middleware-wrapped fallback; nil restores silent skipping.
func (r *EventRouter) OnUnhandled(handler EventHandler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if handler == nil {
		r.unhandled = nil
		return nil
	}
	wrapped, err := r.wrap(handler)
	if err != nil {
		return err
	}
	r.unhandled = wrapped
	return nil
}

// Dispatch invokes the named or fallback handler with child-message lineage in ctx.
// Different dispatches may invoke handlers concurrently; the router does not settle broker deliveries.
func (r *EventRouter) Dispatch(ctx context.Context, message EventMessage) error {
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

package edax

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// CommandHandler executes a command envelope. TypedCommandHandler decodes its payload.
type CommandHandler func(context.Context, CommandMessage) (any, error)

// Middleware wraps a command handler. The first registered middleware runs outermost.
type Middleware func(CommandHandler) CommandHandler

type commandContextKey struct{}
type commandDispatch struct {
	message  CommandMessage
	received bool
	sealing  *Sealing
	wire     []byte
	opened   []byte
}

func (dispatch commandDispatch) openedPayload(message CommandMessage) ([]byte, error) {
	wire, err := EncodeCommand("json", message)
	if err != nil {
		return nil, err
	}
	if dispatch.sealing == nil || !bytes.Equal(wire, dispatch.wire) {
		return nil, &Error{Code: CodeCodec, Message: "command changed after authentication"}
	}
	return append([]byte(nil), dispatch.opened...), nil
}

// CurrentCommand returns the envelope for the current handler invocation.
func CurrentCommand(ctx context.Context) (CommandMessage, bool) {
	dispatch, ok := ctx.Value(commandContextKey{}).(commandDispatch)
	if ok && dispatch.sealing != nil {
		message, err := DecodeCommand("json", dispatch.wire)
		if err != nil {
			return CommandMessage{}, false
		}
		return *message, true
	}
	return dispatch.message, ok
}

// CommandBus routes command names through middleware in registration order.
// The first middleware is the outermost. Registration and execution are concurrent-safe.
type CommandBus struct {
	mu         sync.RWMutex
	handlers   map[string]CommandHandler
	middleware []Middleware
	sealing    *Sealing
}

// NewCommandBus creates an empty registry. Registration and dispatch may run concurrently.
func NewCommandBus(middleware ...Middleware) *CommandBus {
	return &CommandBus{handlers: make(map[string]CommandHandler), middleware: append([]Middleware(nil), middleware...)}
}

// NewSealedCommandBus encrypts local commands and authenticates received commands.
// Middleware and CurrentCommand see ciphertext; registered handlers receive an opened view.
func NewSealedCommandBus(sealing *Sealing, middleware ...Middleware) *CommandBus {
	bus := NewCommandBus(middleware...)
	bus.sealing = sealing
	return bus
}

// Register binds a name to a handler and its middleware. Duplicate names are refused.
func (b *CommandBus) Register(name string, handler CommandHandler) error {
	if name == "" || handler == nil {
		return &Error{Code: CodeBadHandle, Message: "command registration needs a name and handler"}
	}
	b.mu.RLock()
	_, exists := b.handlers[name]
	b.mu.RUnlock()
	if exists {
		return &Error{Code: CodeAlreadyRegistered, Message: name}
	}
	// Factories run outside the lock, so one may register or dispatch on this bus.
	if b.sealing != nil {
		registered := handler
		handler = func(ctx context.Context, raw CommandMessage) (any, error) {
			dispatch, _ := ctx.Value(commandContextKey{}).(commandDispatch)
			data, err := dispatch.openedPayload(raw)
			if err != nil {
				return nil, err
			}
			wire, err := EncodeCommand("json", raw)
			if err != nil {
				return nil, err
			}
			opened, err := DecodeCommand("json", wire)
			if err != nil {
				return nil, err
			}
			opened.Data, opened.Encryption = append([]byte(nil), data...), nil
			return registered(ctx, *opened)
		}
	}
	for i := len(b.middleware) - 1; i >= 0; i-- {
		if b.middleware[i] == nil {
			return &Error{Code: CodeBadHandle, Message: "nil command middleware"}
		}
		handler = b.middleware[i](handler)
		if handler == nil {
			return &Error{Code: CodeBadHandle, Message: "middleware returned a nil command handler"}
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.handlers[name]; exists {
		return &Error{Code: CodeAlreadyRegistered, Message: name}
	}
	if b.handlers == nil {
		b.handlers = make(map[string]CommandHandler)
	}
	b.handlers[name] = handler
	return nil
}

// Execute dispatches a received message. Its id is the fallback idempotency key.
// With Idempotent middleware, successful results are decoded JSON values on both
// execution and replay. Use ExecuteAs for a typed result. Unguarded handlers retain
// their original result type.
func (b *CommandBus) Execute(ctx context.Context, message CommandMessage) (any, error) {
	value, _, err := b.dispatch(ctx, message, true, false)
	return value, err
}

// Deliver skips only when the trusted opening decision confirms destroyed keys.
// Errors returned by handlers, including Shredded, are never converted into skips.
func (b *CommandBus) Deliver(ctx context.Context, message CommandMessage) (any, bool, error) {
	return b.dispatch(ctx, message, true, true)
}

// Dispatch runs a local command. A missing idempotency key leaves it unguarded.
func (b *CommandBus) Dispatch(ctx context.Context, message CommandMessage) (any, error) {
	value, _, err := b.dispatch(ctx, message, false, false)
	return value, err
}

func (b *CommandBus) dispatch(ctx context.Context, message CommandMessage, received, delivery bool) (value any, skipped bool, err error) {
	started := time.Now()
	defer func() {
		ObserveContext(ctx, CurrentObserver(ctx), Observation{Operation: "command", Name: message.Name, ID: message.ID, CorrelationID: message.CorrelationID, Duration: time.Since(started), Err: err})
	}()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	b.mu.RLock()
	handler := b.handlers[message.Name]
	b.mu.RUnlock()
	dispatch := commandDispatch{received: received, sealing: b.sealing}
	if b.sealing != nil {
		if !received && message.Encryption == nil {
			sealed, sealErr := b.sealing.SealCommand(ctx, message)
			if sealErr != nil {
				return nil, false, sealErr
			}
			message = *sealed
		}
		dispatch.wire, err = EncodeCommand("json", message)
		if err != nil {
			return nil, false, err
		}
		owned, decodeErr := DecodeCommand("json", dispatch.wire)
		if decodeErr != nil {
			return nil, false, decodeErr
		}
		message = *owned
		if delivery {
			dispatch.opened, skipped, err = b.sealing.OpenCommandDelivery(ctx, message)
		} else {
			dispatch.opened, err = b.sealing.OpenCommand(ctx, message)
		}
		if err != nil || skipped {
			return nil, skipped, err
		}
		defer clear(dispatch.opened)
		scope, scopeErr := b.sealing.Scope(message.Tenant, message.Subject)
		if scopeErr != nil {
			return nil, false, scopeErr
		}
		var release func()
		ctx, release, err = b.sealing.Guard(ctx, scope)
		if err != nil {
			if delivery && destroyedKey(err) {
				return nil, true, nil
			}
			return nil, false, err
		}
		if !HoldCurrentTransactionResource(ctx, release) {
			defer release()
		}
	} else if message.Encryption != nil {
		return nil, false, &Error{Code: CodeBadHandle, Message: "encrypted commands require command bus sealing"}
	}
	if handler == nil {
		return nil, false, &Error{Code: CodeHandlerNotFound, Message: message.Name}
	}
	dispatch.message = message
	ctx = context.WithValue(ctx, commandContextKey{}, dispatch)
	ctx, err = CausedBy(ctx, message.ID, message.CorrelationID)
	if err != nil {
		return nil, false, err
	}
	value, err = handler(ctx, message)
	return value, false, err
}

// CommandOptions contains envelope metadata independent of its payload.
type CommandOptions struct {
	ID             string
	IdempotencyKey *string
	AggregateID    *string
	Tenant         *string
	Subject        *string
	Lineage        *Lineage
	Extensions     map[string]string
	Metadata       map[string]string
}

// NewCommandMessage stamps a serialized payload with the kernel clock and lineage.
func NewCommandMessage(ctx context.Context, payload Payload, options CommandOptions) (CommandMessage, error) {
	flow, err := messageLineage(ctx, options.Lineage)
	if err != nil {
		return CommandMessage{}, err
	}
	id := options.ID
	if id == "" {
		id = Guid()
	}
	message := CommandMessage{ID: id, Name: payload.Name, Version: payload.Version, Data: payload.Data,
		IdempotencyKey: options.IdempotencyKey, AggregateID: options.AggregateID, Tenant: options.Tenant, Subject: options.Subject,
		CorrelationID: flow.CorrelationID, CausationID: flow.CausationID, IssuedAt: CurrentTimestamp(),
		Extensions: options.Extensions, Metadata: options.Metadata}
	// The codec applies the complete kernel message contract before dispatch or publish.
	if _, err := EncodeCommand("json", message); err != nil {
		return CommandMessage{}, err
	}
	return message, nil
}

// Retry retries transient failures with the shared kernel policy.
func Retry(attempts uint32, pacing Pacing) (Middleware, error) {
	if err := kernel.RetryAttempts(attempts); err != nil {
		return nil, err
	}
	return func(next CommandHandler) CommandHandler {
		return func(ctx context.Context, message CommandMessage) (any, error) {
			if InTransaction(ctx) {
				return nil, &Error{Code: CodeBadHandle, Message: "retry cannot run inside a transaction"}
			}
			for failed := uint32(0); ; failed++ {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				value, err := next(ctx, message)
				if err == nil {
					return value, nil
				}
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				step, decisionErr := kernel.RetryStep(attempts, failed, errorCode(err))
				if decisionErr != nil {
					return nil, decisionErr
				}
				if step == nil {
					return nil, err
				}
				if waitErr := pacing.Wait(ctx, *step); waitErr != nil {
					return nil, waitErr
				}
			}
		}
	}, nil
}

package edax

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"
)

// Observation reports one completed operation without exposing payload bytes.
type Observation struct {
	Operation, Name, ID, CorrelationID, Outcome string
	Attempt                                     uint32
	Duration                                    time.Duration
	Err                                         error
	Published, DeadLettered                     uint64
	OldestTimestamp                             *int64
}

// Observer receives synchronous completion hooks. Implementations must be safe for concurrent calls.
type Observer interface{ Observe(event Observation) }

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(Observation)

// Observe invokes the wrapped function.
func (f ObserverFunc) Observe(event Observation) { f(event) }

// Observe isolates instrumentation failures from message settlement.
func Observe(observer Observer, event Observation) {
	ObserveContext(context.Background(), observer, event)
}

// ObserveContext isolates observer panics and logs their value and stack through the context logger.
func ObserveContext(ctx context.Context, observer Observer, event Observation) {
	if observer == nil {
		return
	}
	defer func() {
		if failure := recover(); failure != nil {
			CurrentLogger(ctx).ErrorContext(ctx, "edax observer panicked", "panic", failure, "stack", string(debug.Stack()), "operation", event.Operation, "name", event.Name)
		}
	}()
	observer.Observe(event)
}

type observerKey struct{}
type observerValue struct{ observer Observer }

// WithObserver selects the observer inherited by SDK operations using ctx.
func WithObserver(ctx context.Context, observer Observer) context.Context {
	return context.WithValue(ctx, observerKey{}, observerValue{observer})
}

// CurrentObserver returns the observer in ctx, or nil when none was supplied.
func CurrentObserver(ctx context.Context) Observer {
	value, _ := ctx.Value(observerKey{}).(observerValue)
	return value.observer
}

// WithLogger selects the logger for SDK diagnostics in this context.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

type loggerKey struct{}

// CurrentLogger returns the context logger or slog.Default when none was supplied.
func CurrentLogger(ctx context.Context) *slog.Logger {
	logger, _ := ctx.Value(loggerKey{}).(*slog.Logger)
	if logger == nil {
		return slog.Default()
	}
	return logger
}

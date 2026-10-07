package edax

import (
	"context"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
	// WithLineage carries the lineage of newly produced messages in this context.
)

type lineageContextKey struct{}

func WithLineage(ctx context.Context, lineage Lineage) context.Context {
	return context.WithValue(ctx, lineageContextKey{}, lineage)
}

// CurrentLineage returns the lineage carried by ctx, if one has been assigned.
func CurrentLineage(ctx context.Context) (Lineage, bool) {
	lineage, ok := ctx.Value(lineageContextKey{}).(Lineage)
	return lineage, ok
}

// CausedBy makes subsequent messages children of this message.
func CausedBy(ctx context.Context, id, correlationID string) (context.Context, error) {
	lineage, err := kernel.Propagate(id, correlationID)
	if err != nil {
		return ctx, err
	}
	return WithLineage(ctx, *lineage), nil
}

func messageLineage(ctx context.Context, specified *Lineage) (Lineage, error) {
	if specified != nil {
		return *specified, nil
	}
	if lineage, ok := CurrentLineage(ctx); ok {
		return lineage, nil
	}
	lineage, err := kernel.Originate(Guid(), nil, nil)
	if err != nil {
		return Lineage{}, err
	}
	return *lineage, nil
}

// Ptr constructs an optional value in a message or kernel call.
func Ptr[T any](value T) *T { return &value }

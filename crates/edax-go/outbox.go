package edax

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"sync"
	"time"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// Outbox returns committed rows. Multiple relays require the backend to lease rows.
type Outbox[M Message] interface {
	Unpublished(ctx context.Context, limit uint64) ([]M, error)
	MarkPublished(ctx context.Context, ids []string) error
}

// CountedOutbox exposes committed backlog depth without loading every row.
type CountedOutbox[M Message] interface {
	Outbox[M]
	UnpublishedCount(ctx context.Context) (uint64, error)
}

// Publish returns nil only after the destination durably acknowledges a message.
type Publish[M Message] func(context.Context, M) error

// RelayOptions fixes retry limits, pacing, sinks and diagnostics for a relay.
// Configure these dependencies before use; callbacks run synchronously during a drain.
type RelayOptions[M Message] struct {
	Observer    Observer
	Logger      *slog.Logger
	MaxAttempts uint32
	OnDead      DeadLetterSink[M]
	Pacing      Pacing
	OnError     func(error)
}

// DrainStats describes one completed drain, including successfully settled dead letters.
type DrainStats struct {
	OldestTimestamp *int64
	Published       uint64
	DeadLettered    uint64
	Duration        time.Duration
}

// Relay publishes committed rows and marks successful rows only after durable acknowledgement.
// The kernel decides per-key blocking, retry budgets and whether a batch must stop.
type Relay[M Message] struct {
	mu           sync.Mutex
	drainSlot    chan struct{}
	closedSignal chan struct{}
	active       bool
	store        Outbox[M]
	publish      Publish[M]
	options      RelayOptions[M]
	policy       *kernel.Drain
	last         *DrainStats
	closed       bool
}

// NewRelay creates an idle relay with native retry state. Call Close when finished.
func NewRelay[M Message](store Outbox[M], publish Publish[M], options RelayOptions[M]) (*Relay[M], error) {
	if store == nil || publish == nil {
		return nil, &Error{Code: CodeBadHandle, Message: "relay requires an outbox and publisher"}
	}
	return &Relay[M]{drainSlot: make(chan struct{}, 1), closedSignal: make(chan struct{}), store: store, publish: publish, options: options, policy: kernel.GetDrain(options.MaxAttempts, options.OnDead != nil)}, nil
}

// Last returns a copy of the last completed drain statistics, or nil before the first drain.
// It does not wait for a currently active drain.
func (r *Relay[M]) Last() *DrainStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.last == nil {
		return nil
	}
	result := *r.last
	if result.OldestTimestamp != nil {
		stamp := *result.OldestTimestamp
		result.OldestTimestamp = &stamp
	}
	return &result
}

// Close refuses new drains immediately. An active drain finishes before its native handle is released.
// Close can be called by a publisher or sink; it does not wait for their I/O.
func (r *Relay[M]) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		close(r.closedSignal)
	}
	if r.active {
		return nil
	}
	return r.policy.Close()
}

// Drain publishes at most batch rows and returns the number published successfully.
// Drains are serialized; waiting for another drain honors ctx cancellation.
func (r *Relay[M]) Drain(ctx context.Context, batch uint64) (count uint64, result error) {
	if r.options.Logger != nil {
		ctx = WithLogger(ctx, r.options.Logger)
	}
	observation := Observation{Operation: "drain"}
	started := time.Now()
	defer func() {
		observation.Duration = time.Since(started)
		observation.Err = result
		observation.Outcome = "completed"
		if result != nil {
			observation.Outcome = "failed"
		}
		observer := r.options.Observer
		if observer == nil {
			observer = CurrentObserver(ctx)
		}
		ObserveContext(ctx, observer, observation)
	}()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-r.closedSignal:
		return 0, closedHandle("relay")
	case r.drainSlot <- struct{}{}:
	}
	defer func() { <-r.drainSlot }()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return 0, closedHandle("relay")
	}
	r.active = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.active = false
		if r.closed {
			result = errors.Join(result, r.policy.Close())
		}
	}()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	started = time.Now()
	rows, err := r.store.Unpublished(ctx, batch)
	if err != nil {
		return 0, r.storeFailed(ctx, err)
	}
	for _, row := range rows {
		var written int64
		switch message := any(row).(type) {
		case EventMessage:
			written = message.RecordedAt
		case CommandMessage:
			written = message.IssuedAt
		}
		if observation.OldestTimestamp == nil || written < *observation.OldestTimestamp {
			stamp := written
			observation.OldestTimestamp = &stamp
		}
	}
	keys := make([]kernel.DrainRow, len(rows))
	for i, row := range rows {
		id, _, aggregate, tenant, _ := messageFields(row)
		keys[i] = kernel.DrainRow{ID: id, AggregateID: aggregate, Tenant: tenant}
	}
	if err := r.policy.Begin(keys); err != nil {
		return 0, err
	}
	failures := make(map[string]error)
	for _, row := range rows {
		id, _, _, _, _ := messageFields(row)
		admitted, err := r.policy.Admits(id)
		if err != nil {
			return 0, err
		}
		if !admitted {
			continue
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := r.publish(ctx, row); err != nil {
			failures[id] = err
			verdict, failure := r.policy.Failed(id, errorCode(err))
			if failure != nil {
				return 0, failure
			}
			switch verdict.Action {
			case "hand_back", "stop":
			case "dead_letter":
				if verdict.Attempt == nil || r.options.OnDead == nil {
					return 0, invalidDecision("drain dead-letter attempt")
				}
				sinkErr := r.options.OnDead(ctx, DeadLetter[M]{Message: row, Attempt: *verdict.Attempt, Failure: err})
				sunk, failure := r.policy.Sunk(id, *verdict.Attempt, sinkErr == nil, errorCode(sinkErr))
				if failure != nil {
					return 0, failure
				}
				if sinkErr != nil && sunk.Action == "stop" {
					failures[id] = sinkErr
				}
			default:
				return 0, invalidDecision("drain action " + verdict.Action)
			}
		} else if err := r.policy.Published(id); err != nil {
			return 0, err
		}
	}
	drained, err := r.policy.Finish()
	if err != nil {
		return 0, err
	}
	observation.Published = drained.Published
	observation.DeadLettered = drained.DeadLettered
	settled := drained.Published + drained.DeadLettered
	if len(drained.Settled) > 0 {
		if err := r.store.MarkPublished(ctx, drained.Settled); err != nil {
			return 0, r.storeFailed(ctx, err)
		}
	}
	if err := r.policy.StoreAnswered(); err != nil {
		return settled, err
	}
	if drained.RaiseID != nil {
		failure, ok := failures[*drained.RaiseID]
		if !ok {
			return settled, invalidDecision("drain failure id")
		}
		if drained.WaitStep != nil {
			failure = errors.Join(failure, r.options.Pacing.Wait(ctx, *drained.WaitStep))
		}
		return settled, failure
	}
	r.mu.Lock()
	r.last = &DrainStats{Published: drained.Published, DeadLettered: drained.DeadLettered, Duration: time.Since(started), OldestTimestamp: observation.OldestTimestamp}
	r.mu.Unlock()
	return settled, nil
}
func (r *Relay[M]) storeFailed(ctx context.Context, failure error) error {
	step, err := r.policy.StoreFailed()
	if err != nil {
		return errors.Join(failure, err)
	}
	return errors.Join(failure, r.options.Pacing.Wait(ctx, step))
}

// Run stops on cancellation or a fatal configuration failure. Transient failures have
// already waited their backoff in Drain and are reported through OnError.
func (r *Relay[M]) Run(ctx context.Context, batch uint64, idle time.Duration) error {
	if idle <= 0 {
		return &Error{Code: CodeBadHandle, Message: "relay idle interval must be positive"}
	}
	if r.options.Logger != nil {
		ctx = WithLogger(ctx, r.options.Logger)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		count, err := r.Drain(ctx, batch)
		if err != nil {
			if IsFatal(err) || ctx.Err() != nil {
				return err
			}
			if r.options.OnError != nil {
				r.options.OnError(err)
			} else {
				CurrentLogger(ctx).ErrorContext(ctx, "edax relay drain failed", "error", err)
			}
			continue
		}
		if count == 0 {
			timer := time.NewTimer(idle)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}

// Pump publishes a change-data-capture stream. Commit the source position only after
// publish returns successfully; a publish failure ends the pump.
func Pump[M Message](ctx context.Context, source iter.Seq2[M, error], publish Publish[M]) (uint64, error) {
	var count uint64
	for row, err := range source {
		if err != nil {
			return count, err
		}
		if err := ctx.Err(); err != nil {
			return count, err
		}
		if err := publish(ctx, row); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

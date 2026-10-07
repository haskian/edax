package edax

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// ErrDispatcherStopped means submission was refused because workers are not accepting work.
const ErrDispatcherStopped sdkError = "edax dispatcher is stopped"

type sdkError string

func (e sdkError) Error() string { return string(e) }

// DispatcherOptions fixes worker capacity, retry policy and settlement callbacks.
// Callbacks may run concurrently on different workers and must honor their contexts.
type DispatcherOptions[M Message] struct {
	Observer Observer
	Logger   *slog.Logger
	Workers  uint32
	// MaxQueue bounds each worker's queue. Zero allows unbounded queues.
	MaxQueue    int
	MaxAttempts uint32
	Pacing      Pacing
	OnDead      DeadLetterSink[M]
	OnPoison    func(context.Context, *Undecodable) error
	// OnError reports settlement failures. They are not handler failures and never dead-letter.
	OnError func(error)
}

// KeyOrderedDispatcher keeps each ordering key on one worker. A stopped delivery remains
// the caller's responsibility when Submit returns an error.
type KeyOrderedDispatcher[M Message] struct {
	mu            sync.Mutex
	changed       chan struct{}
	queues        [][]*Delivery[M]
	pending       int
	handling      int
	running       bool
	closed        bool
	stopping      bool
	fatal         error
	done          chan struct{}
	cancel        context.CancelFunc
	workerContext context.Context
	handler       func(context.Context, M) error
	options       DispatcherOptions[M]
	retries       *kernel.Retries
	poisonRetries *kernel.Retries
}

// NewKeyOrderedDispatcher allocates a stopped dispatcher. Workers must be positive.
// Call Start before submission and Close when the dispatcher is no longer needed.
func NewKeyOrderedDispatcher[M Message](handler func(context.Context, M) error, options DispatcherOptions[M]) (*KeyOrderedDispatcher[M], error) {
	if handler == nil || options.Workers == 0 || options.MaxQueue < 0 {
		return nil, &Error{Code: CodeBadHandle, Message: "dispatcher needs a handler, positive workers and a nonnegative queue bound"}
	}
	return &KeyOrderedDispatcher[M]{handler: handler, options: options, changed: make(chan struct{}), queues: make([][]*Delivery[M], options.Workers), retries: kernel.GetRetries(options.MaxAttempts, options.OnDead != nil), poisonRetries: kernel.GetRetries(options.MaxAttempts, options.OnPoison != nil)}, nil
}
func (d *KeyOrderedDispatcher[M]) signal() { close(d.changed); d.changed = make(chan struct{}) }

// Start launches workers with ctx cancellation and values, excluding ambient transactions.
// Starting while workers are still running is refused; a stopped dispatcher can be restarted.
func (d *KeyOrderedDispatcher[M]) Start(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return closedHandle("dispatcher")
	}
	if d.running {
		return &Error{Code: CodeBadHandle, Message: "dispatcher already started"}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.options.Logger != nil {
		ctx = WithLogger(ctx, d.options.Logger)
	}
	workerCtx, cancel := context.WithCancel(WithoutTransaction(ctx))
	d.cancel = cancel
	d.workerContext = workerCtx
	d.running = true
	d.stopping = false
	d.fatal = nil
	d.done = make(chan struct{})
	var workers sync.WaitGroup
	for i := range d.queues {
		workers.Add(1)
		go func(index int) { defer workers.Done(); d.worker(workerCtx, index) }(i)
	}
	go func() {
		workers.Wait()
		d.mu.Lock()
		d.running = false
		d.signal()
		close(d.done)
		d.mu.Unlock()
	}()
	return nil
}

// Submit transfers settlement responsibility to the dispatcher only when it returns nil.
// It waits for queue space until ctx is canceled or the dispatcher stops.
func (d *KeyOrderedDispatcher[M]) Submit(ctx context.Context, delivery *Delivery[M]) error {
	if delivery == nil {
		return &Error{Code: CodeBadHandle, Message: "delivery is nil"}
	}
	id, _, aggregate, _, _ := messageFields(delivery.Message)
	key, err := kernel.OrderingKey(id, aggregate)
	if err != nil {
		return err
	}
	partition, err := kernel.Partition(key, d.options.Workers)
	if err != nil {
		return err
	}
	index := int(partition)
	for {
		d.mu.Lock()
		if d.fatal != nil {
			err := d.fatal
			d.mu.Unlock()
			return err
		}
		if !d.running || d.stopping || d.workerContext.Err() != nil {
			d.mu.Unlock()
			return ErrDispatcherStopped
		}
		if err := ctx.Err(); err != nil {
			d.mu.Unlock()
			return err
		}
		if d.options.MaxQueue == 0 || len(d.queues[index]) < d.options.MaxQueue {
			d.queues[index] = append(d.queues[index], delivery)
			d.pending++
			d.signal()
			d.mu.Unlock()
			return nil
		}
		changed := d.changed
		d.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
func (d *KeyOrderedDispatcher[M]) worker(ctx context.Context, index int) {
	for {
		d.mu.Lock()
		if len(d.queues[index]) == 0 {
			if d.stopping || ctx.Err() != nil {
				d.mu.Unlock()
				return
			}
			changed := d.changed
			d.mu.Unlock()
			select {
			case <-ctx.Done():
			case <-changed:
			}
			continue
		}
		delivery := d.queues[index][0]
		d.queues[index][0] = nil
		d.queues[index] = d.queues[index][1:]
		fatal := d.fatal
		d.handling++
		d.signal()
		d.mu.Unlock()
		var err error
		if delivery.IsLive() {
			if ctx.Err() != nil || fatal != nil {
				err = d.handBack(ctx, delivery)
				d.observeDelivery(ctx, delivery, "stopped", 0, 0, err)
			} else {
				err = d.settle(ctx, delivery)
			}
		} else {
			d.observeDelivery(ctx, delivery, "retired", 0, 0, nil)
		}
		if err != nil {
			d.report(ctx, err)
		}
		d.mu.Lock()
		d.pending--
		d.handling--
		d.signal()
		d.mu.Unlock()
	}
}
func (d *KeyOrderedDispatcher[M]) settle(ctx context.Context, delivery *Delivery[M]) (result error) {
	id, _, _, _, correlation := messageFields(delivery.Message)
	attempt, err := d.retries.Attempt(id, delivery.Attempt)
	if err != nil {
		return errors.Join(err, d.handBack(ctx, delivery))
	}
	outcome := "unsettled"
	started := time.Now()
	handlerContext, lineageErr := CausedBy(ctx, id, correlation)
	if lineageErr != nil {
		err = lineageErr
	} else {
		err = d.handler(handlerContext, delivery.Message)
	}
	duration := time.Since(started)
	defer func() {
		failure := err
		if result != nil {
			failure = result
			outcome = "unsettled"
		}
		d.observeDelivery(ctx, delivery, outcome, attempt, duration, failure)
	}()
	if !delivery.IsLive() {
		outcome = "retired"
		return nil
	}
	if ctx.Err() != nil {
		outcome = "stopped"
		return d.handBack(ctx, delivery)
	}
	if err == nil {
		outcome = "ack"
		ackErr := delivery.Ack(ctx)
		return errors.Join(ackErr, d.retries.Succeeded(id))
	}
	verdict, failure := d.retries.Failed(id, delivery.Attempt, errorCode(err))
	if failure != nil {
		return errors.Join(failure, d.handBack(ctx, delivery))
	}
	switch verdict.Action {
	case "stop":
		outcome = "stopped"
		d.stopOn(err)
		return delivery.Nack(ctx)
	case "nack":
		outcome = "nack"
		if verdict.Failure == "contended" {
			outcome = "in_progress"
		}
		if verdict.Step == nil {
			return errors.Join(invalidDecision("retry step"), d.handBack(ctx, delivery))
		}
		if waitErr := d.options.Pacing.Wait(ctx, *verdict.Step); waitErr != nil {
			if ctx.Err() != nil {
				outcome = "stopped"
				return d.handBack(ctx, delivery)
			}
			return errors.Join(waitErr, d.handBack(ctx, delivery))
		}
		return delivery.Nack(ctx)
	case "dead":
		if verdict.Attempt == nil || d.options.OnDead == nil {
			return errors.Join(invalidDecision("dead-letter attempt"), d.handBack(ctx, delivery))
		}
		sinkErr := d.options.OnDead(ctx, DeadLetter[M]{Message: delivery.Message, Attempt: *verdict.Attempt, Failure: err, Origin: delivery.Origin})
		step, decisionErr := d.retries.Sunk(id, *verdict.Attempt, sinkErr == nil, errorCode(sinkErr))
		if decisionErr != nil {
			return errors.Join(decisionErr, d.handBack(ctx, delivery))
		}
		if sinkErr == nil {
			outcome = "dead"
			return delivery.Ack(ctx)
		}
		outcome = "sink_failed"
		if step == nil {
			outcome = "stopped"
			d.stopOn(sinkErr)
			return delivery.Nack(ctx)
		}
		if waitErr := d.options.Pacing.Wait(ctx, *step); waitErr != nil {
			if ctx.Err() != nil {
				outcome = "stopped"
				return d.handBack(ctx, delivery)
			}
			return errors.Join(waitErr, d.handBack(ctx, delivery))
		}
		return delivery.Nack(ctx)
	default:
		return errors.Join(invalidDecision("retry action "+verdict.Action), d.handBack(ctx, delivery))
	}
}
func invalidDecision(name string) error {
	return &Error{Code: CodeBadHandle, Message: "missing or unknown kernel " + name}
}
func (d *KeyOrderedDispatcher[M]) handBack(parent context.Context, delivery *Delivery[M]) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(WithoutTransaction(parent)), 5*time.Second)
	defer cancel()
	return delivery.Nack(ctx)
}
func (d *KeyOrderedDispatcher[M]) stopOn(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fatal == nil {
		d.fatal = err
	}
	d.signal()
}
func (d *KeyOrderedDispatcher[M]) report(ctx context.Context, err error) {
	if d.options.OnError != nil {
		d.options.OnError(err)
	} else {
		CurrentLogger(ctx).ErrorContext(ctx, "edax dispatcher settlement failed", "error", err)
	}
}

// Err returns the fatal failure from the current or most recent worker run.
func (d *KeyOrderedDispatcher[M]) Err() error { d.mu.Lock(); defer d.mu.Unlock(); return d.fatal }

// HasDeadLetterSink reports whether failed decoded messages have a configured sink.
func (d *KeyOrderedDispatcher[M]) HasDeadLetterSink() bool { return d.options.OnDead != nil }

// HasPoisonSink reports whether undecodable records have a configured sink.
func (d *KeyOrderedDispatcher[M]) HasPoisonSink() bool { return d.options.OnPoison != nil }

// Stop drains accepted deliveries. At its deadline it cancels workers and returns the
// deadline error. A handler that ignores cancellation can delay worker exit; Start refuses
// to overlap that worker with another one.
func (d *KeyOrderedDispatcher[M]) Stop(ctx context.Context) error {
	d.mu.Lock()
	if !d.running {
		d.mu.Unlock()
		return nil
	}
	d.stopping = true
	d.signal()
	done, cancel := d.done, d.cancel
	d.mu.Unlock()
	select {
	case <-done:
		return d.Err()
	case <-ctx.Done():
		cancel()
		return ctx.Err()
	}
}

// WaitIdle waits until accepted work, including settlement, is finished or ctx is canceled.
func (d *KeyOrderedDispatcher[M]) WaitIdle(ctx context.Context) error { return d.wait(ctx, false) }

// WaitHandlers waits for active delivery processing, including settlement; queued work may remain.
func (d *KeyOrderedDispatcher[M]) WaitHandlers(ctx context.Context) error { return d.wait(ctx, true) }
func (d *KeyOrderedDispatcher[M]) wait(ctx context.Context, handlers bool) error {
	for {
		d.mu.Lock()
		count := d.pending
		if handlers {
			count = d.handling
		}
		if count == 0 {
			d.mu.Unlock()
			return nil
		}
		changed := d.changed
		d.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Close permanently stops admission, waits for workers, then releases native retry state.
// A handler that ignores cancellation can block Close; use Stop with a deadline first.
func (d *KeyOrderedDispatcher[M]) Close() error {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
	err := d.Stop(context.Background())
	return errors.Join(err, d.retries.Close(), d.poisonRetries.Close())
}

// SettlePoison uses the same retry decisions as failed decoded deliveries. Only the
// poison sink sees the raw record; a successful sink is followed by acknowledgement.
func (d *KeyOrderedDispatcher[M]) SettlePoison(ctx context.Context, poison *Undecodable) error {
	if poison == nil {
		return &Error{Code: CodeBadHandle, Message: "poison delivery is nil"}
	}
	d.mu.Lock()
	fatal, stopping, running := d.fatal, d.stopping, d.running
	d.mu.Unlock()
	if fatal != nil || stopping || !running {
		if fatal == nil {
			fatal = ErrDispatcherStopped
		}
		return errors.Join(fatal, poison.Nack(ctx))
	}
	if !poison.IsLive() {
		return nil
	}
	code := "codec"
	verdict, err := d.poisonRetries.Failed(poison.ID, poison.Attempt, &code)
	if err != nil {
		return err
	}
	switch verdict.Action {
	case "nack":
		if verdict.Step == nil {
			return invalidDecision("poison retry step")
		}
		if err := d.options.Pacing.Wait(ctx, *verdict.Step); err != nil {
			return errors.Join(err, d.handBackPoison(ctx, poison))
		}
		return poison.Nack(ctx)
	case "dead":
		if verdict.Attempt == nil || d.options.OnPoison == nil {
			return invalidDecision("poison sink")
		}
		sinkErr := d.options.OnPoison(ctx, poison)
		step, err := d.poisonRetries.Sunk(poison.ID, *verdict.Attempt, sinkErr == nil, errorCode(sinkErr))
		if err != nil {
			return err
		}
		if sinkErr == nil {
			return poison.Ack(ctx)
		}
		if step == nil {
			d.stopOn(sinkErr)
			return errors.Join(sinkErr, poison.Nack(ctx))
		}
		if err := d.options.Pacing.Wait(ctx, *step); err != nil {
			return errors.Join(err, d.handBackPoison(ctx, poison))
		}
		return poison.Nack(ctx)
	case "stop":
		d.stopOn(poison.Failure)
		return errors.Join(poison.Failure, poison.Nack(ctx))
	default:
		return invalidDecision("poison retry action " + verdict.Action)
	}
}
func (d *KeyOrderedDispatcher[M]) handBackPoison(parent context.Context, poison *Undecodable) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(WithoutTransaction(parent)), 5*time.Second)
	defer cancel()
	return poison.Nack(ctx)
}

func (d *KeyOrderedDispatcher[M]) observeDelivery(ctx context.Context, delivery *Delivery[M], outcome string, attempt uint32, duration time.Duration, failure error) {
	id, name, _, _, correlation := messageFields(delivery.Message)
	observer := d.options.Observer
	if observer == nil {
		observer = CurrentObserver(ctx)
	}
	ObserveContext(ctx, observer, Observation{Operation: "delivery", Name: name, ID: id, CorrelationID: correlation, Outcome: outcome, Attempt: attempt, Duration: duration, Err: failure})
}

// DispatcherStats is a snapshot of local work. Retry counters can change immediately
// after the snapshot; they are diagnostics, not coordination primitives.
type DispatcherStats struct {
	Running   bool
	Stopping  bool
	Queued    []int
	Handling  int
	Pending   int
	Attempts  uint64
	Contended uint64
}

// Stats returns queue activity and native retry counters for decoded deliveries.
func (d *KeyOrderedDispatcher[M]) Stats() (DispatcherStats, error) {
	d.mu.Lock()
	stats := DispatcherStats{Running: d.running && d.fatal == nil && d.workerContext.Err() == nil, Stopping: d.stopping, Handling: d.handling, Pending: d.pending, Queued: make([]int, len(d.queues))}
	for i, queue := range d.queues {
		stats.Queued[i] = len(queue)
	}
	d.mu.Unlock()
	tracked, err := d.retries.Tracked()
	if err != nil {
		return stats, err
	}
	stats.Attempts = tracked.Attempts
	stats.Contended = tracked.Contended
	return stats, nil
}

package edax

import (
	"context"
	"sync"
)

// Message is either envelope the kernels can route and serialize.
type Message interface{ EventMessage | CommandMessage }

// Delivery carries settlement callbacks for one broker delivery. Do not copy it after use.
// Configure fields before sharing it; settlement methods are safe for concurrent calls.
type Delivery[M Message] struct {
	Message M
	OnAck   func(context.Context) error
	OnNack  func(context.Context) error
	Attempt uint32
	Live    func() bool
	Origin  any
	mu      sync.Mutex
	settled bool
}

// IsLive reports whether the broker still owns this delivery; a nil callback means live.
func (d *Delivery[M]) IsLive() bool { return d.Live == nil || d.Live() }

// Settled reports successful acknowledgement or negative acknowledgement. It is safe during settlement.
func (d *Delivery[M]) Settled() bool { d.mu.Lock(); defer d.mu.Unlock(); return d.settled }

// Ack acknowledges a live delivery once. A callback failure leaves it unsettled for retry.
// Ack and Nack serialize their callbacks; callbacks must not call settlement methods on this delivery.
func (d *Delivery[M]) Ack(ctx context.Context) error { return d.settle(ctx, d.OnAck) }

// Nack returns a live delivery to the broker once. Failed callbacks can be retried.
func (d *Delivery[M]) Nack(ctx context.Context) error { return d.settle(ctx, d.OnNack) }
func (d *Delivery[M]) settle(ctx context.Context, callback func(context.Context) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled || !d.IsLive() {
		return nil
	}
	if callback != nil {
		if err := callback(ctx); err != nil {
			return err
		}
	}
	d.settled = true
	return nil
}

// DeadLetter carries the failure and broker origin to a durable sink.
type DeadLetter[M Message] struct {
	Message M
	Attempt uint32
	Failure error
	Origin  any
}

// DeadLetterSink must return nil only after durably accepting the failed message.
type DeadLetterSink[M Message] func(context.Context, DeadLetter[M]) error

// Undecodable is a polled record whose envelope cannot reach a handler.
// Its settlement callbacks refer to the original broker record.
// Configure fields before sharing it, and do not copy it after settlement starts.
type Undecodable struct {
	ID      string
	Name    string
	Raw     []byte
	Failure error
	Origin  any
	Attempt uint32
	OnAck   func(context.Context) error
	OnNack  func(context.Context) error
	Live    func() bool
	mu      sync.Mutex
	settled bool
}

// Error returns the decoding failure text, or a fallback when no failure was supplied.
func (d *Undecodable) Error() string {
	if d.Failure != nil {
		return d.Failure.Error()
	}
	return "undecodable message"
}

// Unwrap exposes the original decoding failure to errors.Is and errors.As.
func (d *Undecodable) Unwrap() error { return d.Failure }

// IsLive reports whether the broker still owns this delivery; a nil callback means live.
func (d *Undecodable) IsLive() bool { return d.Live == nil || d.Live() }

// Settled reports successful acknowledgement or negative acknowledgement. It is safe during settlement.
func (d *Undecodable) Settled() bool { d.mu.Lock(); defer d.mu.Unlock(); return d.settled }

// Ack acknowledges a live delivery once. A callback failure leaves it unsettled for retry.
// Ack and Nack serialize their callbacks; callbacks must not call settlement methods on this delivery.
func (d *Undecodable) Ack(ctx context.Context) error { return d.settle(ctx, d.OnAck) }

// Nack returns a live delivery to the broker once. Failed callbacks can be retried.
func (d *Undecodable) Nack(ctx context.Context) error { return d.settle(ctx, d.OnNack) }
func (d *Undecodable) settle(ctx context.Context, callback func(context.Context) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled || !d.IsLive() {
		return nil
	}
	if callback != nil {
		if err := callback(ctx); err != nil {
			return err
		}
	}
	d.settled = true
	return nil
}

func messageFields[M Message](message M) (id string, name string, aggregate *string, tenant *string, correlation string) {
	switch m := any(message).(type) {
	case EventMessage:
		return m.ID, m.Name, &m.AggregateID, m.Tenant, m.CorrelationID
	case CommandMessage:
		return m.ID, m.Name, m.AggregateID, m.Tenant, m.CorrelationID
	}
	panic("unreachable message type")
}

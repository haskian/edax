package memory

import (
	"context"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// Broker is a partitioned memory log with independent consumer-group watermarks.
type Broker struct {
	native       *kernel.MemoryBroker
	PollInterval time.Duration
}

// NewBroker creates an empty partitioned log. Configure PollInterval before concurrent use.
func NewBroker(partitions uint32, tenancy edax.Tenancy) (*Broker, error) {
	h, e := kernel.GetMemoryBroker(partitions, string(tenancy))
	if e != nil {
		return nil, e
	}
	return &Broker{native: h, PollInterval: time.Millisecond}, nil
}

// Close releases the broker handle. Subsequent operations return ErrBadHandle.
func (b *Broker) Close() error { return b.native.Close() }

// PublishEvent appends an event using kernel partition placement.
func (b *Broker) PublishEvent(ctx context.Context, message edax.EventMessage) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	return b.native.PublishEvent(message)
}

// PublishCommand appends a command using kernel partition placement.
func (b *Broker) PublishCommand(ctx context.Context, message edax.CommandMessage) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	return b.native.PublishCommand(message)
}

// Crash simulates group failure by invalidating outstanding deliveries and exposing unacknowledged work.
func (b *Broker) Crash(group string) error { return b.native.Crash(group) }

// IsEmpty reports whether the group has any unacknowledged messages remaining.
func (b *Broker) IsEmpty(group string) (bool, error) { return b.native.IsEmpty(group) }

// Poll returns one event or command. Cancellation stops waiting without settling unseen work.
func (b *Broker) Poll(ctx context.Context, group string) (*edax.Delivery[edax.EventMessage], *edax.Delivery[edax.CommandMessage], error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		row, err := b.native.TryPoll(group)
		if err != nil {
			return nil, nil, err
		}
		if row != nil {
			ack := func(ctx context.Context) error {
				if e := ctx.Err(); e != nil {
					return e
				}
				_, e := b.native.Ack(group, row.Token)
				return e
			}
			nack := func(ctx context.Context) error {
				if e := ctx.Err(); e != nil {
					return e
				}
				_, e := b.native.Nack(group, row.Token)
				return e
			}
			live := func() bool { live, e := b.native.IsLive(group, row.Token); return e == nil && live }
			if row.Event != nil {
				return &edax.Delivery[edax.EventMessage]{Message: *row.Event, OnAck: ack, OnNack: nack, Live: live}, nil, nil
			}
			if row.Command != nil {
				return nil, &edax.Delivery[edax.CommandMessage]{Message: *row.Command, OnAck: ack, OnNack: nack, Live: live}, nil
			}
			return nil, nil, &edax.Error{Code: edax.CodeCodec, Message: "memory delivery has no message"}
		}
		interval := b.PollInterval
		if interval <= 0 {
			interval = time.Millisecond
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
	}
}

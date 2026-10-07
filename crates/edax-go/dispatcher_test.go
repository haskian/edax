package edax

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func instantPacing() Pacing {
	return Pacing{Sleep: func(context.Context, time.Duration) error { return nil }, Entropy: func() uint64 { return 0 }}
}
func TestDispatcherSettlesByKernelVerdict(t *testing.T) {
	for _, test := range []struct {
		name            string
		failure         error
		sink            bool
		sinkFailure     error
		ack, nack, dead int
		fatal           bool
	}{
		{name: "success", ack: 1},
		{name: "unclassified retry", failure: errors.New("network interrupted"), nack: 1},
		{name: "terminal without sink", failure: ErrCodec, nack: 1},
		{name: "terminal dead letter", failure: ErrCodec, sink: true, ack: 1, dead: 1},
		{name: "budget exhausted", failure: ErrStore, sink: true, ack: 1, dead: 1},
		{name: "contention does not exhaust budget", failure: ErrInProgress, sink: true, nack: 1},
		{name: "fatal stops", failure: ErrBadHandle, sink: true, nack: 1, fatal: true},
		{name: "sink retry", failure: ErrCodec, sink: true, sinkFailure: ErrStore, nack: 1, dead: 1},
		{name: "sink fatal", failure: ErrCodec, sink: true, sinkFailure: ErrBadHandle, nack: 1, dead: 1, fatal: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var acks, nacks, dead atomic.Int32
			opts := DispatcherOptions[EventMessage]{Workers: 1, MaxAttempts: 1, Pacing: instantPacing()}
			if test.sink {
				opts.OnDead = func(context.Context, DeadLetter[EventMessage]) error { dead.Add(1); return test.sinkFailure }
			}
			dispatcher, err := NewKeyOrderedDispatcher(func(context.Context, EventMessage) error { return test.failure }, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer dispatcher.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := dispatcher.Start(ctx); err != nil {
				t.Fatal(err)
			}
			delivery := &Delivery[EventMessage]{Message: consumeEvent("credit-1", "account-1", 1), OnAck: func(context.Context) error { acks.Add(1); return nil }, OnNack: func(context.Context) error { nacks.Add(1); return nil }}
			if err := dispatcher.Submit(ctx, delivery); err != nil {
				t.Fatal(err)
			}
			if err := dispatcher.WaitIdle(ctx); err != nil {
				t.Fatal(err)
			}
			if int(acks.Load()) != test.ack || int(nacks.Load()) != test.nack || int(dead.Load()) != test.dead {
				t.Fatalf("ack=%d nack=%d dead=%d", acks.Load(), nacks.Load(), dead.Load())
			}
			if (dispatcher.Err() != nil) != test.fatal {
				t.Fatalf("fatal=%v", dispatcher.Err())
			}
		})
	}
}
func TestDispatcherOrderingAndRetirement(t *testing.T) {
	var mu sync.Mutex
	seen := make(map[string][]uint64)
	dispatcher, err := NewKeyOrderedDispatcher(func(_ context.Context, message EventMessage) error {
		mu.Lock()
		defer mu.Unlock()
		seen[message.AggregateID] = append(seen[message.AggregateID], *message.Sequence)
		return nil
	}, DispatcherOptions[EventMessage]{Workers: 4, MaxQueue: 2, Pacing: instantPacing()})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := dispatcher.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(1); sequence <= 20; sequence++ {
		for _, aggregate := range []string{"account-1", "account-2"} {
			delivery := &Delivery[EventMessage]{Message: consumeEvent(fmt.Sprintf("%s-%d", aggregate, sequence), aggregate, sequence)}
			if err := dispatcher.Submit(ctx, delivery); err != nil {
				t.Fatal(err)
			}
		}
	}
	retired := &Delivery[EventMessage]{Message: consumeEvent("retired", "account-1", 21), Live: func() bool { return false }, OnAck: func(context.Context) error { t.Error("retired delivery acked"); return nil }}
	if err := dispatcher.Submit(ctx, retired); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	for aggregate, values := range seen {
		if len(values) != 20 {
			t.Fatalf("%s: %v", aggregate, values)
		}
		for i, value := range values {
			if value != uint64(i+1) {
				t.Fatalf("%s: %v", aggregate, values)
			}
		}
	}
}
func TestDispatcherCancellationHandsBackAcceptedWork(t *testing.T) {
	entered := make(chan struct{})
	var nacks atomic.Int32
	dispatcher, err := NewKeyOrderedDispatcher(func(ctx context.Context, _ EventMessage) error { close(entered); <-ctx.Done(); return ctx.Err() }, DispatcherOptions[EventMessage]{Workers: 1, MaxQueue: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	if err := dispatcher.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	delivery := func(id string) *Delivery[EventMessage] {
		return &Delivery[EventMessage]{Message: consumeEvent(id, "account-1", 1), OnNack: func(context.Context) error { nacks.Add(1); return nil }}
	}
	if err := dispatcher.Submit(context.Background(), delivery("first")); err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := dispatcher.Submit(context.Background(), delivery("second")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := dispatcher.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop: %v", err)
	}
	drained, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if err := dispatcher.WaitIdle(drained); err != nil {
		t.Fatal(err)
	}
	if nacks.Load() != 2 {
		t.Fatalf("nacks=%d", nacks.Load())
	}
	if err := dispatcher.Submit(drained, delivery("third")); !errors.Is(err, ErrDispatcherStopped) {
		t.Fatalf("submit: %v", err)
	}
}
func TestPoisonSettlement(t *testing.T) {
	for _, take := range []bool{false, true} {
		t.Run(fmt.Sprint(take), func(t *testing.T) {
			var ack, nack atomic.Int32
			dispatcher, err := NewKeyOrderedDispatcher(func(context.Context, EventMessage) error { t.Error("poison reached handler"); return nil }, DispatcherOptions[EventMessage]{Workers: 1, Pacing: instantPacing(), OnPoison: func(context.Context, *Undecodable) error {
				if take {
					return nil
				}
				return ErrStore
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer dispatcher.Close()
			if err := dispatcher.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			poison := &Undecodable{ID: "poison-1", Failure: ErrCodec, Raw: []byte("malformed event"), OnAck: func(context.Context) error { ack.Add(1); return nil }, OnNack: func(context.Context) error { nack.Add(1); return nil }}
			if err := dispatcher.SettlePoison(context.Background(), poison); err != nil {
				t.Fatal(err)
			}
			if take && (ack.Load() != 1 || nack.Load() != 0) || !take && (ack.Load() != 0 || nack.Load() != 1) {
				t.Fatalf("ack=%d nack=%d", ack.Load(), nack.Load())
			}
		})
	}
}

func TestDispatcherRefusesAdmissionAfterWorkerContextCancellation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	dispatcher, err := NewKeyOrderedDispatcher(func(context.Context, EventMessage) error { close(entered); <-release; return nil }, DispatcherOptions[EventMessage]{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	defer close(release)
	workerContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := dispatcher.Start(workerContext); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Submit(context.Background(), &Delivery[EventMessage]{Message: consumeEvent("first", "account-1", 1)}); err != nil {
		t.Fatal(err)
	}
	<-entered
	cancel()
	// An idle worker may already have exited while the first handler still owns its
	// delivery. A separate live context must not admit work during that shutdown.
	if err := dispatcher.Submit(context.Background(), &Delivery[EventMessage]{Message: consumeEvent("second", "account-2", 1)}); !errors.Is(err, ErrDispatcherStopped) {
		t.Fatalf("submit after worker cancellation: %v", err)
	}
}

package edax

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDispatcherCleanupPreservesValuesAndRenewsDeadline(t *testing.T) {
	for _, poison := range []bool{false, true} {
		name := "delivery"
		if poison {
			name = "poison"
		}
		t.Run(name, func(t *testing.T) {
			type traceKey struct{}
			ctx := context.WithValue(context.Background(), traceKey{}, "trace-account-credit")
			ctx = context.WithValue(ctx, transactionMarker{}, true)
			observer := ObserverFunc(func(Observation) {})
			ctx = WithObserver(ctx, observer)
			ctx, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
			defer cancel()
			var cleanup context.Context
			nack := func(ctx context.Context) error {
				cleanup = ctx
				if ctx.Err() != nil || ctx.Value(traceKey{}) != "trace-account-credit" || CurrentObserver(ctx) == nil || InTransaction(ctx) {
					t.Errorf("cleanup lost values, retained transaction, or inherited cancellation: %v", ctx.Err())
				}
				deadline, ok := ctx.Deadline()
				if remaining := time.Until(deadline); !ok || remaining <= 0 || remaining > 5*time.Second {
					t.Errorf("cleanup deadline=%v exists=%v", deadline, ok)
				}
				return nil
			}
			dispatcher, err := NewKeyOrderedDispatcher(func(context.Context, EventMessage) error { return nil }, DispatcherOptions[EventMessage]{Workers: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer dispatcher.Close()
			if poison {
				err = dispatcher.handBackPoison(ctx, &Undecodable{OnNack: nack})
			} else {
				err = dispatcher.handBack(ctx, &Delivery[EventMessage]{OnNack: nack})
			}
			if err != nil {
				t.Fatal(err)
			}
			if cleanup == nil || cleanup.Err() != context.Canceled {
				t.Fatal("cleanup timer was not canceled after settlement")
			}
		})
	}
}

func TestObserveContextLogsPanicDiagnostics(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	ObserveContext(WithLogger(context.Background(), logger), ObserverFunc(func(Observation) { panic("metrics exporter unavailable") }), Observation{Operation: "delivery", Name: "AccountCredited"})
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["panic"] != "metrics exporter unavailable" || record["operation"] != "delivery" || record["name"] != "AccountCredited" || record["level"] != "ERROR" {
		t.Fatalf("record=%v", record)
	}
	stack, _ := record["stack"].(string)
	if !strings.Contains(stack, "TestObserveContextLogsPanicDiagnostics") {
		t.Fatalf("missing panic stack: %q", stack)
	}
	if CurrentLogger(context.Background()) != slog.Default() {
		t.Fatal("default logger changed")
	}
}

type callbackOutbox struct {
	read func(context.Context, uint64) ([]EventMessage, error)
	mark func(context.Context, []string) error
}

func (s callbackOutbox) Unpublished(ctx context.Context, limit uint64) ([]EventMessage, error) {
	return s.read(ctx, limit)
}
func (s callbackOutbox) MarkPublished(ctx context.Context, ids []string) error {
	return s.mark(ctx, ids)
}

func TestRelayCallbacksCanReadStatsAndClose(t *testing.T) {
	for _, phase := range []string{"read", "publish", "dead letter", "mark", "backoff"} {
		t.Run(phase, func(t *testing.T) {
			var relay *Relay[EventMessage]
			callback := func(at string) {
				if phase == at {
					_ = relay.Last()
					if err := relay.Close(); err != nil {
						t.Error(err)
					}
				}
			}
			store := callbackOutbox{
				read: func(context.Context, uint64) ([]EventMessage, error) {
					callback("read")
					return []EventMessage{consumeEvent("credit-1", "account-1", 1)}, nil
				},
				mark: func(context.Context, []string) error { callback("mark"); return nil },
			}
			var err error
			relay, err = NewRelay(store, func(context.Context, EventMessage) error {
				callback("publish")
				if phase == "dead letter" {
					return ErrCodec
				}
				if phase == "backoff" {
					return ErrStore
				}
				return nil
			}, RelayOptions[EventMessage]{OnDead: func(context.Context, DeadLetter[EventMessage]) error { callback("dead letter"); return nil }, Pacing: Pacing{Sleep: func(context.Context, time.Duration) error { callback("backoff"); return nil }}})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, err := relay.Drain(context.Background(), 10); done <- err }()
			select {
			case err := <-done:
				if phase == "backoff" {
					if !errors.Is(err, ErrStore) {
						t.Fatal(err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("callback blocked reading stats or closing relay")
			}
			if _, err := relay.Drain(context.Background(), 10); !errors.Is(err, ErrBadHandle) {
				t.Fatalf("closed relay admitted drain: %v", err)
			}
			if err := relay.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRelaySerializesDrainsWithoutBlockingStateOrCancellation(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	store := &relayTestStore{rows: []EventMessage{consumeEvent("credit-1", "account-1", 1)}}
	relay, err := NewRelay(store, func(context.Context, EventMessage) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	}, RelayOptions[EventMessage]{})
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { _, err := relay.Drain(context.Background(), 10); finished <- err }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := relay.Drain(ctx, 10); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting drain: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("drains overlapped")
	}
	stateDone := make(chan error, 1)
	go func() { _ = relay.Last(); stateDone <- relay.Close() }()
	select {
	case err := <-stateDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("state access blocked behind publish")
	}
	if _, err := relay.Drain(context.Background(), 10); !errors.Is(err, ErrBadHandle) {
		t.Fatalf("close did not refuse drain: %v", err)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if last := relay.Last(); last == nil || last.Published != 1 {
		t.Fatalf("last=%v", last)
	}
	if _, err := relay.policy.Finish(); !errors.Is(err, ErrBadHandle) {
		t.Fatalf("native handle was not closed after drain: %v", err)
	}
}

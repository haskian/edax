package edax

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

type relayTestStore struct {
	rows    []EventMessage
	marked  []string
	loadErr error
	markErr error
}

func (s *relayTestStore) Unpublished(context.Context, uint64) ([]EventMessage, error) {
	return s.rows, s.loadErr
}
func (s *relayTestStore) MarkPublished(_ context.Context, ids []string) error {
	if s.markErr != nil {
		return s.markErr
	}
	s.marked = append(s.marked, ids...)
	return nil
}
func TestRelayPreservesKernelDrainDecisions(t *testing.T) {
	for _, test := range []struct {
		name              string
		failure           error
		sink              bool
		sinkFailure       error
		published, marked []string
		wantErr           error
	}{
		{name: "success", published: []string{"first", "later", "other"}, marked: []string{"first", "later", "other"}},
		{name: "failed key blocks later row", failure: ErrStore, published: []string{"first", "other"}, marked: []string{"other"}, wantErr: ErrStore},
		{name: "dead letter advances key", failure: ErrCodec, sink: true, published: []string{"first", "later", "other"}, marked: []string{"first", "later", "other"}},
		{name: "sink failure blocks key", failure: ErrCodec, sink: true, sinkFailure: ErrStore, published: []string{"first", "other"}, marked: []string{"other"}, wantErr: ErrCodec},
		{name: "fatal stops batch", failure: ErrBadHandle, published: []string{"first"}, wantErr: ErrBadHandle},
		{name: "sink fatal stops batch", failure: ErrCodec, sink: true, sinkFailure: ErrBadHandle, published: []string{"first"}, wantErr: ErrBadHandle},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &relayTestStore{rows: []EventMessage{consumeEvent("first", "account-1", 1), consumeEvent("later", "account-1", 2), consumeEvent("other", "account-2", 1)}}
			var published []string
			options := RelayOptions[EventMessage]{Pacing: instantPacing()}
			if test.sink {
				options.OnDead = func(context.Context, DeadLetter[EventMessage]) error { return test.sinkFailure }
			}
			relay, err := NewRelay(store, func(_ context.Context, event EventMessage) error {
				published = append(published, event.ID)
				if event.ID == "first" {
					return test.failure
				}
				return nil
			}, options)
			if err != nil {
				t.Fatal(err)
			}
			defer relay.Close()
			count, err := relay.Drain(context.Background(), 10)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("drain error %v want %v", err, test.wantErr)
			}
			if !reflect.DeepEqual(published, test.published) || !reflect.DeepEqual(store.marked, test.marked) {
				t.Fatalf("published=%v marked=%v", published, store.marked)
			}
			if count != uint64(len(test.marked)) {
				t.Fatalf("count=%d", count)
			}
			if (relay.Last() == nil) != (test.wantErr != nil) {
				t.Fatalf("last=%v", relay.Last())
			}
		})
	}
}
func TestRelayStoreFailuresRemainRetryable(t *testing.T) {
	for _, op := range []string{"read", "mark"} {
		t.Run(op, func(t *testing.T) {
			store := &relayTestStore{rows: []EventMessage{consumeEvent("first", "account-1", 1)}}
			if op == "read" {
				store.loadErr = ErrStore
			} else {
				store.markErr = ErrStore
			}
			relay, err := NewRelay(store, func(context.Context, EventMessage) error { return nil }, RelayOptions[EventMessage]{Pacing: instantPacing()})
			if err != nil {
				t.Fatal(err)
			}
			defer relay.Close()
			if _, err := relay.Drain(context.Background(), 10); !errors.Is(err, ErrStore) {
				t.Fatal(err)
			}
			store.loadErr = nil
			store.markErr = nil
			if count, err := relay.Drain(context.Background(), 10); err != nil || count != 1 {
				t.Fatalf("retry count=%d err=%v", count, err)
			}
		})
	}
}
func TestPumpStopsBeforeAcknowledgingFailedPublish(t *testing.T) {
	yielded := 0
	source := func(yield func(EventMessage, error) bool) {
		for i := 1; i <= 3; i++ {
			yielded++
			if !yield(consumeEvent(fmt.Sprint(i), "account-1", uint64(i)), nil) {
				return
			}
		}
	}
	count, err := Pump(context.Background(), source, func(_ context.Context, event EventMessage) error {
		if event.ID == "2" {
			return ErrStore
		}
		return nil
	})
	if count != 1 || !errors.Is(err, ErrStore) || yielded != 2 {
		t.Fatalf("count=%d yielded=%d err=%v", count, yielded, err)
	}
}

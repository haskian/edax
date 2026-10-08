package edax

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
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
func TestRelayParentCancellationKeepsRetryBudget(t *testing.T) {
	for _, test := range []struct {
		name    string
		publish func(ctx context.Context, cancel context.CancelFunc) error
		wantErr error
		sunk    []uint32
	}{
		{name: "parent cancelled", publish: func(ctx context.Context, cancel context.CancelFunc) error {
			cancel()
			return ctx.Err()
		}, wantErr: context.Canceled},
		{name: "publisher timeout", publish: func(ctx context.Context, _ context.CancelFunc) error {
			child, stop := context.WithTimeout(ctx, 0)
			defer stop()
			<-child.Done()
			return child.Err()
		}, sunk: []uint32{1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &relayTestStore{rows: []EventMessage{consumeEvent("first", "account-1", 1)}}
			var sunk []uint32
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			failing := true
			relay, err := NewRelay(store, func(publishing context.Context, _ EventMessage) error {
				if failing {
					return test.publish(publishing, cancel)
				}
				return nil
			}, RelayOptions[EventMessage]{MaxAttempts: 1, Pacing: instantPacing(), OnDead: func(_ context.Context, letter DeadLetter[EventMessage]) error {
				sunk = append(sunk, letter.Attempt)
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer relay.Close()
			if _, err = relay.Drain(ctx, 10); !errors.Is(err, test.wantErr) {
				t.Fatalf("drain error %v want %v", err, test.wantErr)
			}
			if !reflect.DeepEqual(sunk, test.sunk) {
				t.Fatalf("sunk=%v want %v", sunk, test.sunk)
			}
			if test.wantErr == nil {
				return
			}
			// The cancelled attempt left the budget whole: a live retry publishes.
			failing = false
			if count, err := relay.Drain(context.Background(), 10); err != nil || count != 1 || !reflect.DeepEqual(store.marked, []string{"first"}) || len(sunk) != 0 {
				t.Fatalf("retry count=%d err=%v marked=%v sunk=%v", count, err, store.marked, sunk)
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

// lostMarkOutbox holds one batch of rows, drops each row once it is marked, and refuses the marks
// while refuseMarks is set. Every mark it is asked for joins the shared transcript.
type lostMarkOutbox struct {
	rows        []EventMessage
	marked      map[string]bool
	refuseMarks bool
	transcript  *[]string
}

func (s *lostMarkOutbox) Unpublished(context.Context, uint64) ([]EventMessage, error) {
	var rows []EventMessage
	for _, row := range s.rows {
		if !s.marked[row.ID] {
			rows = append(rows, row)
		}
	}
	return rows, nil
}
func (s *lostMarkOutbox) MarkPublished(_ context.Context, ids []string) error {
	if s.refuseMarks {
		*s.transcript = append(*s.transcript, fmt.Sprint("mark refused ", ids))
		return ErrStore
	}
	*s.transcript = append(*s.transcript, fmt.Sprint("mark ", ids))
	for _, id := range ids {
		s.marked[id] = true
	}
	return nil
}

// TestRelayRepublishesARowWhoseDeadLetterMarkWasLost pins docs/sdk/outbox.md's lost-mark rule. A row
// fails transiently up to the ceiling, the sink takes it, and the mark of that drain fails. The relay
// forgot the row's attempts when the sink took it, so the next drain publishes it again with a fresh
// budget instead of handing it straight to the sink.
func TestRelayRepublishesARowWhoseDeadLetterMarkWasLost(t *testing.T) {
	const ceiling = 2
	// The first two drains are the same for every case: hand back, then dead-letter at the ceiling.
	// One transient failure per drain never reads as an outage, which needs two in one drain, so the
	// second drain raises only the refused mark.
	reachSinkAndLoseMark := [][]string{
		{"publish invoice-issued-42: store"},
		{"publish invoice-issued-42: store", "sink invoice-issued-42 at attempt 2", "mark refused [invoice-issued-42]"},
	}
	for _, test := range []struct {
		name string
		// republished is what each publish after the lost mark returns.
		republished []error
		after       [][]string
		errs        []error
	}{
		{name: "republish succeeds so the row reaches the transport and the sink", republished: []error{nil},
			after: [][]string{{"publish invoice-issued-42", "mark [invoice-issued-42]"}}, errs: []error{nil}},
		{name: "transient republish spends a fresh budget before the sink takes it again", republished: []error{ErrStore, ErrStore},
			after: [][]string{
				{"publish invoice-issued-42: store"},
				{"publish invoice-issued-42: store", "sink invoice-issued-42 at attempt 2", "mark [invoice-issued-42]"},
			}, errs: []error{ErrStore, nil}},
		{name: "terminal republish goes back to the sink at once", republished: []error{ErrCodec},
			after: [][]string{{"publish invoice-issued-42: codec", "sink invoice-issued-42 at attempt 1", "mark [invoice-issued-42]"}}, errs: []error{nil}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var transcript []string
			store := &lostMarkOutbox{rows: []EventMessage{consumeEvent("invoice-issued-42", "invoice-42", 1)}, marked: map[string]bool{}, transcript: &transcript}
			outcomes := append([]error{ErrStore, ErrStore}, test.republished...)
			publishes := 0
			relay, err := NewRelay(store, func(_ context.Context, event EventMessage) error {
				if publishes >= len(outcomes) {
					t.Fatalf("publish %d of %s has no scripted outcome", publishes+1, event.ID)
				}
				failure := outcomes[publishes]
				publishes++
				if failure != nil {
					transcript = append(transcript, fmt.Sprintf("publish %s: %s", event.ID, *errorCode(failure)))
				} else {
					transcript = append(transcript, "publish "+event.ID)
				}
				return failure
			}, RelayOptions[EventMessage]{MaxAttempts: ceiling, Pacing: instantPacing(), OnDead: func(_ context.Context, letter DeadLetter[EventMessage]) error {
				transcript = append(transcript, fmt.Sprintf("sink %s at attempt %d", letter.Message.ID, letter.Attempt))
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer relay.Close()
			drains := append(slices.Clone(reachSinkAndLoseMark), test.after...)
			wantErrs := append([]error{ErrStore, ErrStore}, test.errs...)
			for index, want := range drains {
				store.refuseMarks = index == 1
				transcript = nil
				_, err := relay.Drain(context.Background(), 10)
				if !errors.Is(err, wantErrs[index]) || (wantErrs[index] == nil) != (err == nil) {
					t.Fatalf("drain %d error %v, want %v", index+1, err, wantErrs[index])
				}
				if !reflect.DeepEqual(transcript, want) {
					t.Fatalf("drain %d did %q, want %q", index+1, transcript, want)
				}
			}
			if rows, _ := store.Unpublished(context.Background(), 10); len(rows) != 0 {
				t.Fatalf("rows left unmarked: %v", rows)
			}
		})
	}
}

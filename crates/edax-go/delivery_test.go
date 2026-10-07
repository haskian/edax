package edax

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func consumeEvent(id, aggregate string, sequence uint64) EventMessage {
	return EventMessage{ID: id, AggregateID: aggregate, Name: "AccountCredited", Version: 1, Sequence: &sequence, Data: []byte(`{"account_id":"account-1","amount":"12.5","currency":"EUR"}`), CorrelationID: "flow-1", CausationID: "deposit-1", RecordedAt: 1720000000000000000, Extensions: map[string]string{"traceparent": "trace-1"}, Metadata: map[string]string{"source": "payments"}}
}
func TestDeliverySettlement(t *testing.T) {
	failed := errors.New("commit reply lost")
	for _, test := range []struct {
		name      string
		live      bool
		failFirst bool
	}{{"ack", true, false}, {"retry settlement failure", true, true}, {"retired", false, false}} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			delivery := &Delivery[EventMessage]{Message: consumeEvent("credit-1", "account-1", 1), Live: func() bool { return test.live }, OnAck: func(context.Context) error {
				if calls.Add(1) == 1 && test.failFirst {
					return failed
				}
				return nil
			}, OnNack: func(context.Context) error { calls.Add(100); return nil }}
			err := delivery.Ack(context.Background())
			if test.failFirst && !errors.Is(err, failed) {
				t.Fatalf("first ack: %v", err)
			}
			if err := delivery.Ack(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := delivery.Nack(context.Background()); err != nil {
				t.Fatal(err)
			}
			want := int32(1)
			if test.failFirst {
				want = 2
			}
			if !test.live {
				want = 0
			}
			if calls.Load() != want || delivery.Settled() != test.live {
				t.Fatalf("calls=%d settled=%v", calls.Load(), delivery.Settled())
			}
		})
	}
}

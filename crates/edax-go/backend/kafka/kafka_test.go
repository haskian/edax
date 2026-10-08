package kafka

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

type broker struct {
	mu          sync.Mutex
	records     []*Fetched
	cursor      int
	sent        []Fetched
	commits     []map[TopicPartition]int64
	seeks       []int64
	commitError error
	sendError   error
	seekError   error
}

func (b *broker) Partitions(context.Context, string) (uint32, error) { return 4, nil }
func (b *broker) Send(_ context.Context, topic string, partition int32, record Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sent = append(b.sent, Fetched{Topic: topic, Partition: partition, Record: record})
	return b.sendError
}
func (b *broker) Poll(ctx context.Context) (*Fetched, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.cursor == len(b.records) {
		return nil, io.EOF
	}
	record := b.records[b.cursor]
	b.cursor++
	return record, nil
}
func (b *broker) Commit(_ context.Context, offsets map[TopicPartition]int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commits = append(b.commits, offsets)
	return b.commitError
}
func (b *broker) Seek(_ context.Context, _ TopicPartition, offset int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seeks = append(b.seeks, offset)
	if b.seekError != nil {
		return b.seekError
	}
	for i, record := range b.records {
		if record.Offset == offset {
			b.cursor = i
			break
		}
	}
	return nil
}
func message(id string) edax.EventMessage {
	return edax.EventMessage{ID: id, AggregateID: "invoice:1042", Name: "InvoiceIssued", Version: 1, Data: []byte(`{"invoice_id":"1042","total":"49.90","currency":"EUR"}`), CorrelationID: "checkout:1042", CausationID: "issue:1042", RecordedAt: 1760000000000}
}
func fetched(t *testing.T, offset int64) *Fetched {
	t.Helper()
	serializer, err := NewKernelSerializer[edax.EventMessage]("json")
	if err != nil {
		t.Fatal(err)
	}
	event := message("event:1042")
	wire, err := serializer.Encode(event)
	if err != nil {
		t.Fatal(err)
	}
	record, err := kernel.KafkaEventRecord("silo", event, wire, serializer.MediaType(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return &Fetched{Topic: "invoices", Partition: 1, Offset: offset, Record: *record}
}
func consumer(t *testing.T, b *broker) *Consumer[edax.EventMessage] {
	t.Helper()
	c, err := NewConsumer[edax.EventMessage](b, ConsumerOptions[edax.EventMessage]{HasGroup: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func poll(t *testing.T, c *Consumer[edax.EventMessage]) *edax.Delivery[edax.EventMessage] {
	t.Helper()
	d, poison, err := c.Poll(context.Background())
	if err != nil || poison != nil || d == nil {
		t.Fatalf("delivery=%v poison=%v err=%v", d, poison, err)
	}
	return d
}
func TestOffsetSettlement(t *testing.T) {
	for _, name := range []string{"ordered_commit", "nack_retires", "revoke", "assign", "failed_commit"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			b := &broker{records: []*Fetched{fetched(t, 10), fetched(t, 11), fetched(t, 12)}}
			c := consumer(t, b)
			first, second, third := poll(t, c), poll(t, c), poll(t, c)
			switch name {
			case "ordered_commit":
				if err := second.Ack(ctx); err != nil {
					t.Fatal(err)
				}
				if err := c.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				if len(b.commits) != 0 {
					t.Fatalf("committed past first: %+v", b.commits)
				}
				if err := first.Ack(ctx); err != nil {
					t.Fatal(err)
				}
				if err := c.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				if len(b.commits) != 1 || b.commits[0][TopicPartition{"invoices", 1}] != 12 {
					t.Fatalf("watermark: %+v", b.commits)
				}
			case "nack_retires":
				if err := first.Ack(ctx); err != nil {
					t.Fatal(err)
				}
				if err := second.Nack(ctx); err != nil {
					t.Fatal(err)
				}
				if third.IsLive() || len(b.seeks) != 1 || b.seeks[0] != 11 {
					t.Fatalf("rewind: live=%v seeks=%v", third.IsLive(), b.seeks)
				}
				if err := third.Ack(ctx); err != nil {
					t.Fatal(err)
				}
				if err := c.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				if b.commits[0][TopicPartition{"invoices", 1}] != 11 {
					t.Fatalf("stale ack advanced watermark: %+v", b.commits)
				}
				replay := poll(t, c)
				if !replay.IsLive() {
					t.Fatal("replacement token retired")
				}
				if err := replay.Ack(ctx); err != nil {
					t.Fatal(err)
				}
			case "revoke":
				if err := first.Ack(ctx); err != nil {
					t.Fatal(err)
				}
				if err := c.Revoke(ctx, []TopicPartition{{"invoices", 1}}); err != nil {
					t.Fatal(err)
				}
				if second.IsLive() || third.IsLive() {
					t.Fatal("revoke kept delivery live")
				}
				if len(b.commits) != 1 || b.commits[0][TopicPartition{"invoices", 1}] != 11 {
					t.Fatalf("revoke commit: %+v", b.commits)
				}
				stats, err := c.Stats()
				if err != nil || len(stats) != 0 {
					t.Fatalf("revoked stats: %+v %v", stats, err)
				}
			case "assign":
				if err := c.Assign([]TopicPartition{{"invoices", 1}}); err != nil {
					t.Fatal(err)
				}
				if first.IsLive() || second.IsLive() {
					t.Fatal("assignment kept old tokens")
				}
				if err := c.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				if len(b.commits) != 0 {
					t.Fatal("assignment committed unprocessed work")
				}
			case "failed_commit":
				if err := first.Ack(ctx); err != nil {
					t.Fatal(err)
				}
				b.commitError = errors.New("coordinator unavailable")
				if err := c.Flush(ctx); err == nil {
					t.Fatal("commit failure swallowed")
				}
				b.commitError = nil
				if err := c.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				if len(b.commits) != 2 || b.commits[1][TopicPartition{"invoices", 1}] != 11 {
					t.Fatalf("commit retry: %+v", b.commits)
				}
			}
		})
	}
}
func TestPoisonAndDeadLetter(t *testing.T) {
	for _, name := range []string{"unknown_media", "bad_payload", "sink_failure"} {
		t.Run(name, func(t *testing.T) {
			record := fetched(t, 10)
			if name == "unknown_media" {
				record.Record.Headers = []Header{{Key: kernel.KafkaContentTypeHeader(), Value: []byte("application/vnd.other.event+json")}}
			} else {
				record.Record.Value = []byte("corrupt message")
			}
			b := &broker{records: []*Fetched{record}}
			c := consumer(t, b)
			delivery, poison, err := c.Poll(context.Background())
			if err != nil || delivery != nil || poison == nil || !errors.Is(poison.Failure, edax.ErrCodec) {
				t.Fatalf("delivery=%v poison=%v err=%v", delivery, poison, err)
			}
			expectedID, failure := kernel.KafkaPoisonID("invoices", 1, 10)
			if failure != nil {
				t.Fatal(failure)
			}
			if poison.ID != expectedID {
				t.Fatalf("poison identity %q", poison.ID)
			}
			sink, err := NewDeadLetterSink(b, "invoices.dead", 0)
			if err != nil {
				t.Fatal(err)
			}
			if name == "sink_failure" {
				b.sendError = errors.New("broker down")
				if err = sink.Poison(context.Background(), poison); err == nil {
					t.Fatal("sink failure swallowed")
				}
				if err = c.Flush(context.Background()); err != nil {
					t.Fatal(err)
				}
				if len(b.commits) != 0 {
					t.Fatal("failed sink committed poison")
				}
				return
			}
			if err = sink.Poison(context.Background(), poison); err != nil {
				t.Fatal(err)
			}
			if len(b.sent) != 1 || b.sent[0].Topic != "invoices.dead" || string(b.sent[0].Record.Value) != string(record.Record.Value) {
				t.Fatalf("lost raw dead letter: %+v", b.sent)
			}
			// The kernel renders "<code>: <message>", so the sink passes the bare message.
			var wrapped *edax.Error
			if !errors.As(poison.Failure, &wrapped) {
				t.Fatalf("poison failure %T", poison.Failure)
			}
			rendered := ""
			for _, header := range b.sent[0].Record.Headers {
				if header.Key == "edax-dlq-error" {
					rendered = string(header.Value)
				}
			}
			if want := "codec: " + wrapped.Message; rendered != want {
				t.Fatalf("dead-letter error header %q want %q", rendered, want)
			}
			if err = poison.Ack(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err = c.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			if b.commits[0][TopicPartition{"invoices", 1}] != 11 {
				t.Fatal("poison ack not committed")
			}
		})
	}
}
func TestProducerPartitionAndCommandCodec(t *testing.T) {
	b := &broker{}
	producer, err := NewProducer[edax.EventMessage](b, "invoices", ProducerOptions[edax.EventMessage]{})
	if err != nil {
		t.Fatal(err)
	}
	if err = producer.Publish(context.Background(), message("invoice-issued:1042")); err != nil {
		t.Fatal(err)
	}
	wanted, err := kernel.KafkaPartitionFor(b.sent[0].Record.Key, 4)
	if err != nil {
		t.Fatal(err)
	}
	if b.sent[0].Partition != int32(wanted) {
		t.Fatal("client partition replaced kernel choice")
	}
	serializer, err := NewKernelSerializer[edax.CommandMessage]("msgpack")
	if err != nil {
		t.Fatal(err)
	}
	command := edax.CommandMessage{ID: "issue:1042", Name: "IssueInvoice", Version: 1, Data: []byte(`{"invoice_id":"1042"}`), CorrelationID: "checkout:1042", CausationID: "checkout:1042", IssuedAt: 1760000000000}
	wire, err := serializer.Encode(command)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := serializer.Decode(wire)
	if err != nil || decoded.ID != command.ID {
		t.Fatalf("command roundtrip: %+v %v", decoded, err)
	}
}
func TestRunDrainsBeforeFinalCommit(t *testing.T) {
	corrupt := fetched(t, 10)
	corrupt.Record.Value = []byte("corrupt invoice envelope")
	b := &broker{records: []*Fetched{corrupt, fetched(t, 11)}}
	c := consumer(t, b)
	var mu sync.Mutex
	handled, poisoned := 0, 0
	dispatcher, err := edax.NewKeyOrderedDispatcher(func(_ context.Context, _ edax.EventMessage) error { mu.Lock(); handled++; mu.Unlock(); return nil }, edax.DispatcherOptions[edax.EventMessage]{Workers: 1, OnDead: func(context.Context, edax.DeadLetter[edax.EventMessage]) error { return nil }, OnPoison: func(context.Context, *edax.Undecodable) error { poisoned++; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = c.Run(ctx, dispatcher); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if handled != 1 || poisoned != 1 || len(b.commits) != 1 || b.commits[0][TopicPartition{"invoices", 1}] != 12 {
		t.Fatalf("handled=%d poisoned=%d commits=%+v", handled, poisoned, b.commits)
	}
}

type idleBroker struct {
	*broker
	waiting chan struct{}
}

func (b *idleBroker) Poll(ctx context.Context) (*Fetched, error) {
	b.mu.Lock()
	ready := b.cursor < len(b.records)
	b.mu.Unlock()
	if ready {
		return b.broker.Poll(ctx)
	}
	select {
	case b.waiting <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestNackInterruptsOutstandingPoll(t *testing.T) {
	b := &idleBroker{broker: &broker{records: []*Fetched{fetched(t, 10)}}, waiting: make(chan struct{}, 1)}
	c, err := NewConsumer[edax.EventMessage](b, ConsumerOptions[edax.EventMessage]{HasGroup: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	first := poll(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := c.Poll(ctx); done <- err }()
	select {
	case <-b.waiting:
	case <-ctx.Done():
		t.Fatal("poll did not wait")
	}
	if err = first.Nack(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("poll interruption: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("nack blocked behind idle poll")
	}
	replay := poll(t, c)
	if replay.Message.ID != first.Message.ID {
		t.Fatal("nack failed to replay")
	}
}
func TestFailedSeekRetriesBeforePoll(t *testing.T) {
	b := &broker{records: []*Fetched{fetched(t, 10), fetched(t, 11)}}
	c := consumer(t, b)
	first := poll(t, c)
	b.seekError = errors.New("seek temporarily unavailable")
	if err := first.Nack(context.Background()); err == nil {
		t.Fatal("seek error swallowed")
	}
	if _, _, err := c.Poll(context.Background()); err == nil {
		t.Fatal("poll passed failed rewind")
	}
	b.seekError = nil
	replay := poll(t, c)
	origin := replay.Origin.(*Fetched)
	if origin.Offset != 10 {
		t.Fatalf("failed seek skipped offset 10: %d", origin.Offset)
	}
}

func TestBoundaryErrorIdentityAndCause(t *testing.T) {
	cause := errors.New("schema registry unavailable")
	for _, test := range []struct {
		name      string
		makeError func() error
		sentinel  error
	}{
		{"poison", func() error { return poisonFailure("invoices:2:41", cause) }, edax.ErrCodec},
		{"broker", func() error { return BrokerError(29, cause) }, edax.ErrBadHandle},
	} {
		t.Run(test.name, func(t *testing.T) {
			first, second := test.makeError(), test.makeError()
			var one, two *edax.Error
			if !errors.As(first, &one) || !errors.As(second, &two) {
				t.Fatalf("missing typed error: %v", first)
			}
			if one == two || one == test.sentinel || one.Message == "" {
				t.Fatalf("shared or empty error: %+v", one)
			}
			if !errors.Is(first, cause) {
				t.Fatalf("lost cause: %v", first)
			}
			if !errors.Is(first, test.sentinel) {
				t.Fatalf("lost error identity: %v", first)
			}
			if test.name == "broker" && !edax.IsFatal(first) {
				t.Fatalf("lost broker classification: %v", first)
			}
			original := two.Message
			one.Message = "changed by caller"
			if two.Message != original {
				t.Fatal("mutation crossed error instances")
			}
		})
	}
}

func TestBrokerErrorNil(t *testing.T) {
	if err := BrokerError(29, nil); err != nil {
		t.Fatalf("nil failure became %v", err)
	}
}

type pollFailureMiddleware struct{ failure error }

func (m pollFailureMiddleware) OnPublish(_ context.Context, record Record) (Record, error) {
	return record, nil
}
func (m pollFailureMiddleware) OnPoll(_ context.Context, record Record) (Record, error) {
	return record, m.failure
}

func TestPollOutcomesAreExclusive(t *testing.T) {
	for _, test := range []struct {
		name       string
		failure    error
		wantError  error
		wantPoison bool
	}{
		{name: "delivery"},
		{name: "terminal_middleware", failure: edax.ErrCodec, wantPoison: true},
		{name: "transient_middleware", failure: edax.ErrStore, wantError: edax.ErrStore},
		{name: "fatal_middleware", failure: edax.ErrBadHandle, wantError: edax.ErrBadHandle},
		{name: "end_of_stream", wantError: io.EOF},
		{name: "canceled", wantError: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := &broker{records: []*Fetched{fetched(t, 10)}}
			if test.name == "end_of_stream" {
				b.records = nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if test.name == "canceled" {
				cancel()
			}
			c, err := NewConsumer[edax.EventMessage](b, ConsumerOptions[edax.EventMessage]{HasGroup: true, Middleware: []Middleware{pollFailureMiddleware{test.failure}}})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			delivery, poison, err := c.Poll(ctx)
			if !errors.Is(err, test.wantError) || (poison != nil) != test.wantPoison || (delivery != nil) != (test.wantError == nil && !test.wantPoison) {
				t.Fatalf("delivery=%v poison=%v error=%v", delivery, poison, err)
			}
			if poison != nil {
				if !errors.Is(poison.Failure, test.failure) || poison.Settled() || !poison.IsLive() {
					t.Fatalf("poison metadata %+v", poison)
				}
				if err := poison.Ack(ctx); err != nil {
					t.Fatal(err)
				}
				if err := c.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				if len(b.commits) != 1 || b.commits[0][TopicPartition{"invoices", 1}] != 11 {
					t.Fatalf("poison settlement=%v", b.commits)
				}
			}
			if test.failure != nil && !test.wantPoison && (len(b.seeks) != 1 || b.seeks[0] != 10) {
				t.Fatalf("failed middleware did not return offset: %v", b.seeks)
			}
		})
	}
}

package franz

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type fakeClient struct {
	mu           sync.Mutex
	fetches      kgo.Fetches
	pollStarted  chan struct{}
	commits      []map[string]map[int32]kgo.EpochOffset
	seeks        []map[string]map[int32]kgo.EpochOffset
	transitions  []string
	commitCode   int16
	request      kmsg.Request
	response     kmsg.Response
	requestError error
	sent         *kgo.Record
	sendError    error
}

func (f *fakeClient) Request(_ context.Context, r kmsg.Request) (kmsg.Response, error) {
	f.request = r
	return f.response, f.requestError
}
func (f *fakeClient) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	f.sent = records[0]
	return kgo.ProduceResults{{Record: records[0], Err: f.sendError}}
}
func (f *fakeClient) Close() {}
func (f *fakeClient) PollRecords(ctx context.Context, max int) kgo.Fetches {
	if max != 1 {
		panic("bridge must request one record")
	}
	f.mu.Lock()
	fetches := f.fetches
	f.fetches = nil
	started := f.pollStarted
	f.mu.Unlock()
	if fetches != nil {
		return fetches
	}
	if started != nil {
		close(started)
	}
	<-ctx.Done()
	return kgo.NewErrFetch(ctx.Err())
}
func (f *fakeClient) CommitOffsetsSync(_ context.Context, offsets map[string]map[int32]kgo.EpochOffset, callback func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error)) {
	f.mu.Lock()
	f.commits = append(f.commits, offsets)
	f.transitions = append(f.transitions, "commit")
	f.mu.Unlock()
	callback(nil, nil, &kmsg.OffsetCommitResponse{Topics: []kmsg.OffsetCommitResponseTopic{{Topic: "invoices", Partitions: []kmsg.OffsetCommitResponseTopicPartition{{Partition: 2, ErrorCode: f.commitCode}}}}}, nil)
}
func (f *fakeClient) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seeks = append(f.seeks, offsets)
}
func (f *fakeClient) PauseFetchPartitions(p map[string][]int32) map[string][]int32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.transitions = append(f.transitions, "pause")
	return p
}
func (f *fakeClient) ResumeFetchPartitions(map[string][]int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.transitions = append(f.transitions, "resume")
}
func record(t *testing.T) *kgo.Record {
	t.Helper()
	event := edax.EventMessage{ID: "invoice:1042:issued", AggregateID: "invoice:1042", Name: "InvoiceIssued", Version: 1, Data: []byte(`{"invoice_id":"1042","total":"49.90","currency":"EUR"}`), CorrelationID: "checkout:1042", CausationID: "issue:1042", RecordedAt: 1760000000000}
	serializer, err := kafka.NewKernelSerializer[edax.EventMessage]("json")
	if err != nil {
		t.Fatal(err)
	}
	wire, err := serializer.Encode(event)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: "invoices", Partition: 2, Offset: 41, Key: []byte("invoice:1042"), Value: wire, Headers: []kgo.RecordHeader{{Key: "content-type", Value: []byte(serializer.MediaType())}}, Timestamp: time.UnixMilli(event.RecordedAt)}
}
func fetch(record *kgo.Record) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: record.Topic, Partitions: []kgo.FetchPartition{{Partition: record.Partition, Records: []*kgo.Record{record}}}}}}}
}

func TestProducerAndMetadata(t *testing.T) {
	topic := "invoices"
	for _, tc := range []struct {
		name      string
		code      int16
		missing   bool
		wantError bool
	}{{name: "success"}, {name: "authorization", code: 29, wantError: true}, {name: "missing", missing: true, wantError: true}} {
		t.Run(tc.name, func(t *testing.T) {
			response := &kmsg.MetadataResponse{}
			if !tc.missing {
				response.Topics = []kmsg.MetadataResponseTopic{{Topic: &topic, ErrorCode: tc.code, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0}, {Partition: 1}, {Partition: 2}}}}
			}
			client := &fakeClient{response: response}
			producer := &Producer{client: client}
			count, err := producer.Partitions(context.Background(), topic)
			if (err != nil) != tc.wantError {
				t.Fatalf("count=%d err=%v", count, err)
			}
			if !tc.wantError && count != 3 {
				t.Fatalf("count=%d", count)
			}
			if tc.code == 29 && !edax.IsFatal(err) {
				t.Fatalf("authorization must be fatal: %v", err)
			}
			if got := client.request.(*kmsg.MetadataRequest); len(got.Topics) != 1 || *got.Topics[0].Topic != topic {
				t.Fatalf("request=%+v", got)
			}
		})
	}
	client := &fakeClient{}
	producer := &Producer{client: client}
	source := record(t)
	wire := kafka.Record{Key: source.Key, Value: source.Value, Headers: []kafka.Header{{Key: "content-type", Value: source.Headers[0].Value}}}
	if err := producer.Send(context.Background(), topic, 2, wire); err != nil {
		t.Fatal(err)
	}
	if client.sent.Partition != 2 || client.sent.Topic != topic || !reflect.DeepEqual(client.sent.Value, source.Value) || !reflect.DeepEqual(client.sent.Headers, source.Headers) {
		t.Fatalf("sent=%+v", client.sent)
	}
	client.sendError = kerr.TopicAuthorizationFailed
	if err := producer.Send(context.Background(), topic, 2, wire); !edax.IsFatal(err) {
		t.Fatalf("err=%v", err)
	}
}
func TestPollPreservesRecordBeforePartitionError(t *testing.T) {
	source := record(t)
	fetches := fetch(source)
	fetches[0].Topics[0].Partitions = append(fetches[0].Topics[0].Partitions, kgo.FetchPartition{Partition: 3, Err: kerr.TopicAuthorizationFailed})
	client := &fakeClient{fetches: fetches}
	transport := &transport{client: client}
	got, err := transport.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Offset != 41 || got.Partition != 2 || *got.Timestamp != source.Timestamp.UnixMilli() || !reflect.DeepEqual(got.Record.Value, source.Value) {
		t.Fatalf("record=%+v", got)
	}
	if _, err = transport.Poll(context.Background()); !edax.IsFatal(err) {
		t.Fatalf("pending err=%v", err)
	}
	client.fetches = kgo.NewErrFetch(kgo.ErrClientClosed)
	if _, err = transport.Poll(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("closed=%v", err)
	}
}
func TestCommitAndSeekExactOffsets(t *testing.T) {
	client := &fakeClient{}
	transport := &transport{client: client}
	partition := kafka.TopicPartition{Topic: "invoices", Partition: 2}
	if err := transport.Commit(context.Background(), map[kafka.TopicPartition]int64{partition: 42}); err != nil {
		t.Fatal(err)
	}
	if got := client.commits[0]["invoices"][2]; got.Offset != 42 || got.Epoch != -1 {
		t.Fatalf("commit=%+v", got)
	}
	client.commitCode = 29
	if err := transport.Commit(context.Background(), map[kafka.TopicPartition]int64{partition: 42}); !edax.IsFatal(err) {
		t.Fatalf("per-partition error=%v", err)
	}
	if err := transport.Seek(context.Background(), partition, 41); err != nil {
		t.Fatal(err)
	}
	if got := client.seeks[0]["invoices"][2]; got.Offset != 41 || got.Epoch != -1 {
		t.Fatalf("seek=%+v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := transport.Seek(ctx, partition, 50); !errors.Is(err, context.Canceled) || len(client.seeks) != 1 {
		t.Fatalf("canceled seek=%v", err)
	}
}
func TestRebalanceInterruptsPollAndRetiresDelivery(t *testing.T) {
	for _, kind := range []string{"revoked", "lost"} {
		t.Run(kind, func(t *testing.T) {
			client := &fakeClient{fetches: fetch(record(t))}
			transport := &transport{client: client}
			consumer, err := kafka.NewConsumer[edax.EventMessage](transport, kafka.ConsumerOptions[edax.EventMessage]{HasGroup: true})
			if err != nil {
				t.Fatal(err)
			}
			defer consumer.Close()
			partitions := map[string][]int32{"invoices": {2}}
			transport.rebalance("assigned", context.Background(), consumer, partitions)
			delivery, poison, err := consumer.Poll(context.Background())
			if err != nil || poison != nil || delivery == nil {
				t.Fatalf("delivery=%v poison=%v err=%v", delivery, poison, err)
			}
			if err = delivery.Ack(context.Background()); err != nil {
				t.Fatal(err)
			}
			client.pollStarted = make(chan struct{})
			done := make(chan error, 1)
			go func() { _, _, err := consumer.Poll(context.Background()); done <- err }()
			<-client.pollStarted
			transitionDone := make(chan struct{})
			go func() { transport.rebalance(kind, context.Background(), consumer, partitions); close(transitionDone) }()
			select {
			case <-transitionDone:
			case <-time.After(time.Second):
				t.Fatal("rebalance blocked on PollRecords")
			}
			if err = <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("poll=%v", err)
			}
			if delivery.IsLive() {
				t.Fatal("retired delivery still live")
			}
			want := []string{"resume", "pause"}
			if kind == "revoked" {
				want = append(want, "commit")
			}
			if !reflect.DeepEqual(client.transitions, want) {
				t.Fatalf("transitions=%v", client.transitions)
			}
			if kind == "revoked" && client.commits[0]["invoices"][2].Offset != 42 {
				t.Fatal("wrong revoke watermark")
			}
			if kind == "lost" && len(client.commits) != 0 {
				t.Fatal("committed after group loss")
			}
		})
	}
}
func TestManagedOptions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		option kgo.Opt
	}{{"blocking_rebalance", kgo.BlockRebalanceOnPoll()}, {"direct_partitions", kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"invoices": {0: kgo.NewOffset().AtStart()}})}} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewConsumer[edax.EventMessage]("billing", []string{"invoices"}, kafka.ConsumerOptions[edax.EventMessage]{}, tc.option)
			if err == nil {
				c.Close()
				t.Fatal("unsafe option accepted")
			}
		})
	}
	producer, err := NewProducer(kgo.RequiredAcks(kgo.NoAck()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	client := producer.client.(*kgo.Client)
	if client.OptValue(kgo.RequiredAcks) != kgo.AllISRAcks() || !client.OptValue(kgo.AllowIdempotentProduceCancellation).(bool) {
		t.Fatal("producer safety options missing")
	}
	for _, opt := range []kgo.Opt{kgo.DisableIdempotentWrite(), kgo.TransactionalID("billing")} {
		p, err := NewProducer(opt)
		if err == nil {
			p.Close()
			t.Fatal("unsafe producer option accepted")
		}
	}
	consumer, err := NewConsumer[edax.EventMessage]("billing", []string{"invoices"}, kafka.ConsumerOptions[edax.EventMessage]{})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if !consumer.client.OptValue(kgo.DisableAutoCommit).(bool) || consumer.client.OptValue(kgo.BlockRebalanceOnPoll).(bool) {
		t.Fatal("consumer safety options missing")
	}
	// Exercise the actual registered hooks with an empty assignment; no broker is needed.
	for _, option := range []any{kgo.OnPartitionsAssigned, kgo.OnPartitionsRevoked, kgo.OnPartitionsLost} {
		callback := consumer.client.OptValue(option).(func(context.Context, *kgo.Client, map[string][]int32))
		callback(context.Background(), consumer.client, map[string][]int32{})
	}
}

type failedLifecycle struct{ failure error }

func (f failedLifecycle) Assign([]kafka.TopicPartition) error                  { return f.failure }
func (f failedLifecycle) Revoke(context.Context, []kafka.TopicPartition) error { return f.failure }
func (f failedLifecycle) Lost(context.Context, []kafka.TopicPartition) error   { return f.failure }
func TestRebalanceUsesInjectedLogger(t *testing.T) {
	for _, kind := range []string{"assigned", "revoked", "lost"} {
		t.Run(kind, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil)).With("service", "billing")
			cause := errors.New("ledger unavailable")
			transport := &transport{client: &fakeClient{}, logger: logger}
			transport.rebalance(kind, context.Background(), failedLifecycle{cause}, map[string][]int32{"invoices": {2}})
			if !errors.Is(transport.takeError(), cause) {
				t.Fatal("rebalance cause lost")
			}
			if output := logs.String(); !strings.Contains(output, "service=billing") || !strings.Contains(output, "transition="+kind) || !strings.Contains(output, cause.Error()) {
				t.Fatalf("injected log=%s", output)
			}
		})
	}
}

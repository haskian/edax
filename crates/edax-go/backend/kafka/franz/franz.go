// Package franz supplies managed franz-go clients for the edax Kafka adapter.
package franz

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func brokerError(err error) error {
	var protocol *kerr.Error
	if errors.As(err, &protocol) {
		return kafka.BrokerError(int64(protocol.Code), err)
	}
	return err
}
func configuration(message string) error {
	return &edax.Error{Code: edax.CodeBadHandle, Message: message}
}

type producerClient interface {
	Request(context.Context, kmsg.Request) (kmsg.Response, error)
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Close()
}

// Producer owns a franz-go client. Close it after publishers and dead-letter
// sinks stop. NewProducer fixes manual partitioning and all-replica acks.
type Producer struct{ client producerClient }

// NewProducer accepts connection, authentication and tuning options. Transactions
// and disabled idempotence are rejected. Cancellation can leave an accepted
// record without an acknowledgement; retrying it can produce a duplicate.
func NewProducer(options ...kgo.Opt) (*Producer, error) {
	options = append(append([]kgo.Opt(nil), options...), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.AllowIdempotentProduceCancellation())
	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, err
	}
	if client.OptValue(kgo.DisableIdempotentWrite).(bool) || client.OptValue(kgo.TransactionalID).(string) != "" {
		client.Close()
		return nil, configuration("edax Kafka requires nontransactional idempotent production")
	}
	return &Producer{client: client}, nil
}

// Close stops the owned producer client after publishers have finished.
func (p *Producer) Close() { p.client.Close() }

// Partitions reads the topic partition count and classifies broker protocol errors.
func (p *Producer) Partitions(ctx context.Context, topic string) (uint32, error) {
	request := kmsg.NewPtrMetadataRequest()
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &topic}}
	response, err := p.client.Request(ctx, request)
	if err != nil {
		return 0, brokerError(err)
	}
	metadata, ok := response.(*kmsg.MetadataResponse)
	if !ok {
		return 0, configuration("unexpected Kafka metadata response")
	}
	for _, row := range metadata.Topics {
		if row.Topic == nil || *row.Topic != topic {
			continue
		}
		if err = kerr.ErrorForCode(row.ErrorCode); err != nil {
			return 0, brokerError(err)
		}
		for _, partition := range row.Partitions {
			if err = kerr.ErrorForCode(partition.ErrorCode); err != nil {
				return 0, brokerError(err)
			}
		}
		return uint32(len(row.Partitions)), nil
	}
	return 0, brokerError(kerr.UnknownTopicOrPartition)
}

// Send uses the explicit partition and waits for the acknowledgement or cancellation.
func (p *Producer) Send(ctx context.Context, topic string, partition int32, record kafka.Record) error {
	outgoing := &kgo.Record{Topic: topic, Partition: partition, Key: record.Key, Value: record.Value, Headers: make([]kgo.RecordHeader, len(record.Headers))}
	for i, h := range record.Headers {
		outgoing.Headers[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
	}
	return brokerError(p.client.ProduceSync(ctx, outgoing).FirstErr())
}

type consumerClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitOffsetsSync(context.Context, map[string]map[int32]kgo.EpochOffset, func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error))
	SetOffsets(map[string]map[int32]kgo.EpochOffset)
	PauseFetchPartitions(map[string][]int32) map[string][]int32
	ResumeFetchPartitions(map[string][]int32)
}
type transport struct {
	logger   *slog.Logger
	client   consumerClient
	offsets  sync.Mutex
	failures sync.Mutex
	pending  error
}

func (t *transport) report(err error) {
	if err != nil {
		t.failures.Lock()
		t.pending = errors.Join(t.pending, err)
		t.failures.Unlock()
	}
}
func (t *transport) takeError() error {
	t.failures.Lock()
	defer t.failures.Unlock()
	err := t.pending
	t.pending = nil
	return err
}
func (t *transport) Poll(ctx context.Context) (*kafka.Fetched, error) {
	var records []*kgo.Record
	var failure error
	for {
		if err := t.takeError(); err != nil {
			return nil, err
		}
		fetches := t.client.PollRecords(ctx, 1)
		failure = nil
		fetches.EachError(func(_ string, _ int32, err error) {
			if errors.Is(err, kgo.ErrClientClosed) {
				err = io.EOF
			}
			failure = errors.Join(failure, brokerError(err))
		})
		records = fetches.Records()
		if len(records) > 0 {
			break
		}
		if failure != nil {
			return nil, failure
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// An idle partition yields an empty, error-free fetch. Only client
		// closure (io.EOF above) or cancellation ends the stream.
	}
	// Fetches may carry a record and unrelated partition errors together. Deliver
	// the record first so its offset is booked before reporting those errors.
	t.report(failure)
	record := records[0]
	fetched := &kafka.Fetched{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Record: kafka.Record{Key: record.Key, Value: record.Value, Headers: make([]kafka.Header, len(record.Headers))}}
	for i, h := range record.Headers {
		fetched.Record.Headers[i] = kafka.Header{Key: h.Key, Value: h.Value}
	}
	if !record.Timestamp.IsZero() {
		timestamp := record.Timestamp.UnixMilli()
		kind := int(record.Attrs.TimestampType())
		fetched.Timestamp = &timestamp
		fetched.TimestampType = &kind
	}
	return fetched, nil
}
func (t *transport) Commit(ctx context.Context, offsets map[kafka.TopicPartition]int64) error {
	t.offsets.Lock()
	defer t.offsets.Unlock()
	request := make(map[string]map[int32]kgo.EpochOffset)
	for p, offset := range offsets {
		if request[p.Topic] == nil {
			request[p.Topic] = make(map[int32]kgo.EpochOffset)
		}
		request[p.Topic][p.Partition] = kgo.EpochOffset{Epoch: -1, Offset: offset}
	}
	var result error
	t.client.CommitOffsetsSync(ctx, request, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, response *kmsg.OffsetCommitResponse, err error) {
		result = brokerError(err)
		if err != nil {
			return
		}
		if response == nil {
			result = configuration("missing Kafka commit response")
			return
		}
		for _, topic := range response.Topics {
			for _, partition := range topic.Partitions {
				result = errors.Join(result, brokerError(kerr.ErrorForCode(partition.ErrorCode)))
			}
		}
	})
	return result
}
func (t *transport) Seek(ctx context.Context, p kafka.TopicPartition, offset int64) error {
	t.offsets.Lock()
	defer t.offsets.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	t.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{p.Topic: {p.Partition: {Epoch: -1, Offset: offset}}})
	return nil
}

// Consumer owns its broker client and the edax offset tracker. Stop Run and all
// direct polling before Close. The embedded Consumer exposes Poll, Run and Flush.
type Consumer[M edax.Message] struct {
	*kafka.Consumer[M]
	client *kgo.Client
}

// NewConsumer subscribes to topics with manual offset commits. It owns rebalance
// callbacks. BlockRebalanceOnPoll and direct partition assignments are rejected.
// Configure brokers, TLS, SASL and fetch tuning through clientOptions.
func NewConsumer[M edax.Message](group string, topics []string, options kafka.ConsumerOptions[M], clientOptions ...kgo.Opt) (*Consumer[M], error) {
	if len(topics) == 0 {
		return nil, configuration("Kafka consumer needs at least one topic")
	}
	options.AutoCommit = false
	options.HasGroup = group != ""
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	transport := &transport{logger: options.Logger}
	consumer, err := kafka.NewConsumer[M](transport, options)
	if err != nil {
		return nil, err
	}
	ready := make(chan struct{})
	transition := func(kind string, ctx context.Context, _ *kgo.Client, partitions map[string][]int32) {
		<-ready
		transport.rebalance(kind, ctx, consumer, partitions)
	}
	clientOptions = append(append([]kgo.Opt(nil), clientOptions...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topics...), kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(func(ctx context.Context, c *kgo.Client, p map[string][]int32) { transition("assigned", ctx, c, p) }),
		kgo.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, p map[string][]int32) { transition("revoked", ctx, c, p) }),
		kgo.OnPartitionsLost(func(ctx context.Context, c *kgo.Client, p map[string][]int32) { transition("lost", ctx, c, p) }))
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		close(ready)
		consumer.Close()
		return nil, err
	}
	transport.client = client
	close(ready)
	if client.OptValue(kgo.BlockRebalanceOnPoll).(bool) || len(client.OptValue(kgo.ConsumePartitions).(map[string]map[int32]kgo.Offset)) > 0 {
		client.Close()
		consumer.Close()
		return nil, configuration("edax Kafka requires concurrent rebalance callbacks and group subscriptions")
	}
	return &Consumer[M]{Consumer: consumer, client: client}, nil
}

// Close stops the owned broker client before releasing native offset state.
// Stop Run and direct polling before calling it.
func (c *Consumer[M]) Close() error { c.client.Close(); return c.Consumer.Close() }
func flatten(partitions map[string][]int32) []kafka.TopicPartition {
	var result []kafka.TopicPartition
	for topic, ids := range partitions {
		for _, id := range ids {
			result = append(result, kafka.TopicPartition{Topic: topic, Partition: id})
		}
	}
	return result
}

var _ kafka.ProducerClient = (*Producer)(nil)
var _ kafka.ConsumerClient = (*transport)(nil)

// Callbacks run independently of PollRecords unless BlockRebalanceOnPoll is set.
// Pause first: Revoke releases the adapter's poll gate before this callback returns.
type lifecycle interface {
	Assign([]kafka.TopicPartition) error
	Revoke(context.Context, []kafka.TopicPartition) error
	Lost(context.Context, []kafka.TopicPartition) error
}

func (t *transport) rebalance(kind string, ctx context.Context, consumer lifecycle, partitions map[string][]int32) {
	var failure error
	switch kind {
	case "assigned":
		failure = consumer.Assign(flatten(partitions))
		if failure == nil {
			t.client.ResumeFetchPartitions(partitions)
		}
	case "revoked":
		t.client.PauseFetchPartitions(partitions)
		failure = consumer.Revoke(ctx, flatten(partitions))
	case "lost":
		t.client.PauseFetchPartitions(partitions)
		failure = consumer.Lost(ctx, flatten(partitions))
	}
	if failure != nil {
		t.logger.WarnContext(ctx, "Kafka rebalance failed", "transition", kind, "error", failure)
		t.report(failure)
	}
}

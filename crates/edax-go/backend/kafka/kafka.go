// Package kafka connects typed edax messages to caller-owned Kafka clients.
// Kernels own record routing, partition placement and offset settlement.
package kafka

import (
	"context"
	"fmt"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// Record carries the Kafka key, payload and ordered headers.
type Record = kernel.KafkaRecord

// Header is one Kafka header; repeated names retain their order.
type Header = kernel.KafkaHeader

// TopicPartition identifies one partition within a topic.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// Fetched preserves the original record before middleware, for dead letters.
type Fetched struct {
	Topic         string
	Partition     int32
	Offset        int64
	Record        Record
	Timestamp     *int64
	TimestampType *int
}

// ProducerClient must send on the explicit partition and await the broker ack.
// Use acknowledgements from all in-sync replicas and idempotent production.
type ProducerClient interface {
	Partitions(context.Context, string) (uint32, error)
	Send(context.Context, string, int32, Record) error
}

// ConsumerClient owns subscription and rebalance notifications. Disable auto
// commit and set a consumer group. Calls must honor their context. Poll returns
// io.EOF when the client stops, and keeps waiting through empty fetches. Notify Consumer.Revoke before releasing partitions
// and Consumer.Assign before delivering records from a new assignment.
type ConsumerClient interface {
	Poll(context.Context) (*Fetched, error)
	Commit(context.Context, map[TopicPartition]int64) error
	Seek(context.Context, TopicPartition, int64) error
}

// BrokerError classifies a Kafka protocol error code through the kernel.
func BrokerError(code int64, failure error) error {
	if failure == nil {
		return nil
	}
	wire, err := kernel.KafkaBrokerError(code, failure.Error())
	if err != nil {
		return err
	}
	typed := edax.ErrorFromWire(wire)
	return edax.WrapError(typed.Code, typed.Message, failure)
}

// Serializer converts one message kind. Kind is event or command.
type Serializer[M edax.Message] interface {
	MediaType() string
	Kind() string
	Encode(M) ([]byte, error)
	Decode([]byte) (M, error)
}

type kernelSerializer[M edax.Message] struct{ format, kind, mediaType string }

// NewKernelSerializer selects the shared JSON or MessagePack wire format for M.
func NewKernelSerializer[M edax.Message](format string) (Serializer[M], error) {
	var zero M
	kind := "event"
	if _, ok := any(zero).(edax.CommandMessage); ok {
		kind = "command"
	}
	media, err := kernel.MediaType(format, kind)
	if err != nil {
		return nil, err
	}
	return &kernelSerializer[M]{format: format, kind: kind, mediaType: media}, nil
}
func (s *kernelSerializer[M]) MediaType() string { return s.mediaType }
func (s *kernelSerializer[M]) Kind() string      { return s.kind }
func (s *kernelSerializer[M]) Encode(message M) ([]byte, error) {
	switch value := any(message).(type) {
	case edax.EventMessage:
		return edax.EncodeEvent(s.format, value)
	case edax.CommandMessage:
		return edax.EncodeCommand(s.format, value)
	}
	return nil, &edax.Error{Code: edax.CodeCodec, Message: "unsupported message kind"}
}
func (s *kernelSerializer[M]) Decode(wire []byte) (M, error) {
	var zero M
	if s.kind == "event" {
		value, err := edax.DecodeEvent(s.format, wire)
		if err != nil {
			return zero, err
		}
		return any(*value).(M), nil
	}
	value, err := edax.DecodeCommand(s.format, wire)
	if err != nil {
		return zero, err
	}
	return any(*value).(M), nil
}

// Middleware runs in registration order on publish and reverse order on poll.
type Middleware interface {
	OnPublish(context.Context, Record) (Record, error)
	OnPoll(context.Context, Record) (Record, error)
}

// ProducerOptions selects serialization, tenancy, middleware and the send deadline.
// Zero timeout selects 30 seconds; an empty tenancy selects silo.
type ProducerOptions[M edax.Message] struct {
	Serializer     Serializer[M]
	Tenancy        edax.Tenancy
	Key            func(M) (string, error)
	Middleware     []Middleware
	PublishTimeout time.Duration
}

// Producer serializes messages and publishes them to kernel-selected partitions.
type Producer[M edax.Message] struct {
	client  ProducerClient
	topic   string
	options ProducerOptions[M]
}

// NewProducer binds a topic to a caller-owned client. It defaults to JSON encoding.
func NewProducer[M edax.Message](client ProducerClient, topic string, options ProducerOptions[M]) (*Producer[M], error) {
	if client == nil {
		return nil, &edax.Error{Code: edax.CodeBadHandle, Message: "Kafka producer client is nil"}
	}
	if options.PublishTimeout == 0 {
		options.PublishTimeout = 30 * time.Second
	}
	millis := options.PublishTimeout.Milliseconds()
	if err := kernel.KafkaCheckConfig(&millis, nil, nil, nil); err != nil {
		return nil, err
	}
	if options.Tenancy == "" {
		options.Tenancy = edax.TenancySilo
	}
	if options.Serializer == nil {
		serializer, err := NewKernelSerializer[M]("json")
		if err != nil {
			return nil, err
		}
		options.Serializer = serializer
	}
	options.Middleware = append([]Middleware(nil), options.Middleware...)
	return &Producer[M]{client: client, topic: topic, options: options}, nil
}

// Publish applies middleware and waits for the broker acknowledgement.
// Metadata lookup and send share one deadline. A timeout can still mean acceptance.
func (p *Producer[M]) Publish(ctx context.Context, message M) error {
	wire, err := p.options.Serializer.Encode(message)
	if err != nil {
		return err
	}
	var key *string
	if p.options.Key != nil {
		value, failure := p.options.Key(message)
		if failure != nil {
			return failure
		}
		key = &value
	}
	var record *Record
	switch value := any(message).(type) {
	case edax.EventMessage:
		record, err = kernel.KafkaEventRecord(string(p.options.Tenancy), value, wire, p.options.Serializer.MediaType(), key)
	case edax.CommandMessage:
		record, err = kernel.KafkaCommandRecord(string(p.options.Tenancy), value, wire, p.options.Serializer.MediaType(), key)
	}
	if err != nil {
		return err
	}
	rewritten := *record
	for _, middleware := range p.options.Middleware {
		rewritten, err = middleware.OnPublish(ctx, rewritten)
		if err != nil {
			return err
		}
	}
	return send(ctx, p.client, p.topic, rewritten, p.options.PublishTimeout)
}
func send(ctx context.Context, client ProducerClient, topic string, record Record, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	count, err := client.Partitions(ctx, topic)
	if err != nil {
		return err
	}
	partition, err := kernel.KafkaPartitionFor(record.Key, count)
	if err != nil {
		return err
	}
	return client.Send(ctx, topic, int32(partition), record)
}

// DeadLetterSink preserves the fetched bytes and lets the kernel add failure
// headers. A send must complete before the dispatcher acknowledges the source.
type DeadLetterSink struct {
	client  ProducerClient
	topic   string
	timeout time.Duration
}

// NewDeadLetterSink binds a dead-letter topic to a caller-owned producer client.
// A zero timeout selects 30 seconds.
func NewDeadLetterSink(client ProducerClient, topic string, timeout time.Duration) (*DeadLetterSink, error) {
	if client == nil {
		return nil, &edax.Error{Code: edax.CodeBadHandle, Message: "Kafka dead-letter client is nil"}
	}
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	millis := timeout.Milliseconds()
	if err := kernel.KafkaCheckConfig(&millis, nil, nil, nil); err != nil {
		return nil, err
	}
	return &DeadLetterSink{client: client, topic: topic, timeout: timeout}, nil
}

// Publish sends the original Fetched record with kernel-defined failure headers.
// A record rejected as a codec error gets one attempt as a dead-letter stub.
func (s *DeadLetterSink) Publish(ctx context.Context, origin any, failure error) error {
	fetched, ok := origin.(*Fetched)
	if !ok || fetched == nil {
		return &edax.Error{Code: edax.CodeBadHandle, Message: "Kafka dead letter requires its fetched origin"}
	}
	code := kernel.CodeOf(failure)
	record, err := kernel.KafkaDeadLetterRecord(fetched.Record, fetched.Topic, fetched.Partition, fetched.Offset, kernel.MessageOf(failure), code)
	if err != nil {
		return err
	}
	err = send(ctx, s.client, s.topic, *record, s.timeout)
	if err == nil {
		return nil
	}
	if code := kernel.CodeOf(err); code == nil || *code != string(edax.CodeCodec) {
		return err
	}
	stub, stubErr := kernel.KafkaDeadLetterStub(*record)
	if stubErr != nil {
		return stubErr
	}
	return send(ctx, s.client, s.topic, *stub, s.timeout)
}

// Poison sends an undecodable record without acknowledging its source delivery.
func (s *DeadLetterSink) Poison(ctx context.Context, poison *edax.Undecodable) error {
	return s.Publish(ctx, poison.Origin, poison.Failure)
}

// DeadLetters adapts the sink to a typed dispatcher dead-letter callback.
func DeadLetters[M edax.Message](sink *DeadLetterSink) edax.DeadLetterSink[M] {
	return func(ctx context.Context, dead edax.DeadLetter[M]) error {
		return sink.Publish(ctx, dead.Origin, dead.Failure)
	}
}

func poisonFailure(id string, cause error) error {
	return edax.WrapError(edax.CodeCodec, fmt.Sprintf("kafka %s did not decode: %v", id, cause), cause)
}

// PartitionStats reports a partition's current offset watermark and in-flight deliveries.
type PartitionStats = kernel.PartitionStats

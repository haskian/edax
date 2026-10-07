// Relay orders to Kafka (or memory), then dispatch valid orders and dead-letter invalid ones.
// Run from crates/edax-go: mise exec -- go run ./examples/17_kafka
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/kafka"
	"github.com/haskian/edax/crates/edax-go/backend/kafka/franz"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: the relay published three orders; valid orders packed and invalid order dead-lettered")
}

type pollEvent func(context.Context) (*edax.Delivery[edax.EventMessage], *edax.Undecodable, error)

type orderPlaced struct {
	Quantity int `json:"quantity"`
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if bootstrap := os.Getenv("EDAX_KAFKA_BOOTSTRAP"); bootstrap != "" {
		return onKafka(ctx, bootstrap)
	}
	fmt.Println("EDAX_KAFKA_BOOTSTRAP is unset: using MemoryBroker")
	broker, err := memory.NewBroker(1, edax.TenancySilo)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, broker.Close()) }()
	poll := func(ctx context.Context) (*edax.Delivery[edax.EventMessage], *edax.Undecodable, error) {
		event, command, err := broker.Poll(ctx, "packing")
		if command != nil {
			return nil, nil, errors.New("unexpected command")
		}
		return event, nil, err
	}
	return packOrders(ctx, broker.PublishEvent, poll, nil)
}

func packOrders(ctx context.Context, publish edax.Publish[edax.EventMessage], poll pollEvent, sink *kafka.DeadLetterSink) (err error) {
	codec, err := edax.NewJSONCodec(map[string]func() any{"OrderPlaced": func() any { return new(orderPlaced) }})
	if err != nil {
		return err
	}
	db := memory.New()
	defer func() { err = errors.Join(err, db.Close()) }()
	store, err := db.EventStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	err = edax.WithinTransaction(ctx, db, func(ctx context.Context) error {
		for index, quantity := range []int{2, 0, 1} {
			id := fmt.Sprintf("order-%d", index+1)
			sequence := uint64(1)
			payload, err := codec.Marshal(orderPlaced{Quantity: quantity})
			if err != nil {
				return err
			}
			_, err = store.Append(ctx, id, 0, []edax.EventDraft{{ID: id + "-placed", AggregateID: id, Sequence: &sequence, Name: payload.Name, Version: payload.Version, Data: payload.Data, CorrelationID: id, CausationID: id}})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	relay, err := edax.NewRelay(store, publish, edax.RelayOptions[edax.EventMessage]{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, relay.Close()) }()
	count, err := relay.Drain(ctx, 10)
	if err != nil {
		return err
	}
	if count != 3 {
		return fmt.Errorf("relay published %d orders, want 3", count)
	}
	var mu sync.Mutex
	var packed, dead []string
	var settlementError error
	writeDead := func(context.Context, edax.DeadLetter[edax.EventMessage]) error { return nil }
	if sink != nil {
		writeDead = kafka.DeadLetters[edax.EventMessage](sink)
	}
	options := edax.DispatcherOptions[edax.EventMessage]{Workers: 2, MaxQueue: 8,
		OnError: func(err error) { mu.Lock(); defer mu.Unlock(); settlementError = errors.Join(settlementError, err) },
		OnDead: func(ctx context.Context, letter edax.DeadLetter[edax.EventMessage]) error {
			if err := writeDead(ctx, letter); err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			dead = append(dead, letter.Message.AggregateID)
			return nil
		},
	}
	if sink != nil {
		options.OnPoison = sink.Poison
	}
	dispatcher, err := edax.NewKeyOrderedDispatcher(func(_ context.Context, event edax.EventMessage) error {
		decoded, err := codec.Unmarshal(edax.Payload{Name: event.Name, Version: event.Version, Data: event.Data})
		if err != nil {
			return err
		}
		order, ok := decoded.(*orderPlaced)
		if !ok {
			return fmt.Errorf("unexpected order type %T", decoded)
		}
		if order.Quantity < 1 {
			return &edax.Error{Code: edax.CodeRejected, Message: event.AggregateID + " has no items"}
		}
		mu.Lock()
		defer mu.Unlock()
		packed = append(packed, event.AggregateID)
		return nil
	}, options)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dispatcher.Close()) }()
	if err := dispatcher.Start(ctx); err != nil {
		return err
	}
	// Poll exactly the three published records, then drain accepted work before closing clients.
	for received := 0; received < 3; {
		delivery, poison, err := poll(ctx)
		if err != nil {
			// A nack or a rebalance cancels the poll in flight. Poll again unless the run timed out.
			if errors.Is(err, context.Canceled) && ctx.Err() == nil {
				continue
			}
			return err
		}
		if poison != nil {
			return fmt.Errorf("unexpected undecodable order: %w", poison.Failure)
		}
		if delivery == nil || !delivery.IsLive() {
			continue
		}
		if err := dispatcher.Submit(ctx, delivery); err != nil {
			return errors.Join(err, delivery.Nack(ctx))
		}
		received++
	}
	if err := dispatcher.Stop(ctx); err != nil {
		return err
	}
	mu.Lock()
	defer mu.Unlock()
	if settlementError != nil {
		return settlementError
	}
	slices.Sort(packed)
	if !slices.Equal(packed, []string{"order-1", "order-3"}) || !slices.Equal(dead, []string{"order-2"}) {
		return fmt.Errorf("packed=%v dead=%v", packed, dead)
	}
	fmt.Printf("packed=%v dead-lettered=%v\n", packed, dead)
	return nil
}

func onKafka(ctx context.Context, bootstrap string) (err error) {
	brokers := kgo.SeedBrokers(strings.Split(bootstrap, ",")...)
	admin, err := kgo.NewClient(brokers)
	if err != nil {
		return err
	}
	defer admin.Close()
	topic := "orders-" + edax.Guid()
	topics := []string{topic, topic + ".dlq"}
	// The demo creates isolated topics. Production deployments provision these up front.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err = errors.Join(err, deleteTopics(cleanup, admin, topics))
	}()
	if err := createTopics(ctx, admin, topics); err != nil {
		return err
	}
	client, err := franz.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer client.Close()
	producer, err := kafka.NewProducer[edax.EventMessage](client, topic, kafka.ProducerOptions[edax.EventMessage]{})
	if err != nil {
		return err
	}
	sink, err := kafka.NewDeadLetterSink(client, topic+".dlq", 5*time.Second)
	if err != nil {
		return err
	}
	consumer, err := franz.NewConsumer[edax.EventMessage](topic+"-packing", []string{topic}, kafka.ConsumerOptions[edax.EventMessage]{}, brokers, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, consumer.Close()) }()
	fmt.Printf("using Kafka topic %s\n", topic)
	if err := packOrders(ctx, producer.Publish, consumer.Poll, sink); err != nil {
		return err
	}
	return consumer.Flush(ctx)
}

func createTopics(ctx context.Context, client *kgo.Client, topics []string) error {
	request := kmsg.NewPtrCreateTopicsRequest()
	request.TimeoutMillis = 10_000
	for _, topic := range topics {
		entry := kmsg.NewCreateTopicsRequestTopic()
		entry.Topic, entry.NumPartitions, entry.ReplicationFactor = topic, 1, 1
		request.Topics = append(request.Topics, entry)
	}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return err
	}
	for _, topic := range response.Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			return fmt.Errorf("create %s: %w", topic.Topic, err)
		}
	}
	return nil
}

func deleteTopics(ctx context.Context, client *kgo.Client, topics []string) error {
	request := kmsg.NewPtrDeleteTopicsRequest()
	request.TimeoutMillis = 5_000
	request.TopicNames = topics
	for _, topic := range topics {
		request.Topics = append(request.Topics, kmsg.DeleteTopicsRequestTopic{Topic: &topic})
	}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return err
	}
	for _, topic := range response.Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
			return err
		}
	}
	return nil
}

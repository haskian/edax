// Publish and consume one invoice using local Kafka topics invoices and invoices.dead.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/kafka"
	"github.com/haskian/edax/crates/edax-go/backend/kafka/franz"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bootstrap := os.Getenv("EDAX_KAFKA_BOOTSTRAP")
	if bootstrap == "" {
		bootstrap = "localhost:9092"
	}
	brokers := kgo.SeedBrokers(strings.Split(bootstrap, ",")...)
	client, err := franz.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer client.Close()
	producer, err := kafka.NewProducer[edax.EventMessage](client, "invoices", kafka.ProducerOptions[edax.EventMessage]{})
	if err != nil {
		return err
	}
	sink, err := kafka.NewDeadLetterSink(client, "invoices.dead", 5*time.Second)
	if err != nil {
		return err
	}
	consumer, err := franz.NewConsumer[edax.EventMessage]("edax-invoice-example", []string{"invoices"}, kafka.ConsumerOptions[edax.EventMessage]{}, brokers, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return err
	}
	defer consumer.Close()
	event := edax.EventMessage{ID: fmt.Sprintf("invoice:1042:issued:%d", time.Now().UnixNano()), AggregateID: "invoice:1042", Name: "InvoiceIssued", Version: 1, Data: []byte(`{"invoice_id":"1042","total":"49.90","currency":"EUR"}`), CorrelationID: "checkout:1042", CausationID: "issue:1042", RecordedAt: time.Now().UnixMilli()}
	if err = producer.Publish(ctx, event); err != nil {
		return err
	}
	for {
		delivery, poison, failure := consumer.Poll(ctx)
		if poison != nil {
			if err = sink.Poison(ctx, poison); err != nil {
				return err
			}
			if err = poison.Ack(ctx); err != nil {
				return err
			}
			continue
		}
		if failure != nil {
			// A rebalance can cancel one poll while the caller's deadline remains live.
			if errors.Is(failure, context.Canceled) && ctx.Err() == nil {
				continue
			}
			return failure
		}
		if !delivery.IsLive() {
			continue
		}
		fmt.Printf("%s: %s\n", delivery.Message.Name, delivery.Message.Data)
		if err = delivery.Ack(ctx); err != nil {
			return err
		}
		if err = consumer.Flush(ctx); err != nil {
			return err
		}
		if delivery.Message.ID == event.ID {
			return nil
		}
	}
}

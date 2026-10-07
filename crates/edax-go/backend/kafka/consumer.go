package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// ConsumerOptions controls decoding, commit cadence and bounded shutdown.
// Zero durations select a one-second commit interval, ten-second commit timeout
// and thirty-second handler drain.
type ConsumerOptions[M edax.Message] struct {
	// Logger receives managed client diagnostics. Nil captures slog.Default at construction.
	Logger         *slog.Logger
	Serializer     Serializer[M]
	Serializers    []Serializer[M]
	Middleware     []Middleware
	CommitInterval time.Duration
	CommitTimeout  time.Duration
	DrainTimeout   time.Duration
	Pacing         edax.Pacing
	// AutoCommit and HasGroup describe the caller-owned client's actual settings.
	AutoCommit    bool
	HasGroup      bool
	CommitFailed  func(error)
	RevokeOverrun func(error)
}

// Consumer books fetched offsets and settles them through the kernel tracker.
// Its client must deliver rebalance notifications and honor cancellation.
type Consumer[M edax.Message] struct {
	client      ConsumerClient
	options     ConsumerOptions[M]
	serializers []Serializer[M]
	mediaTypes  []string
	tracker     *kernel.OffsetTracker
	mu          sync.Mutex
	pollMu      sync.Mutex
	pollCancel  context.CancelFunc
	transitions int
	rewinds     map[TopicPartition]int64
	commitMu    sync.Mutex
	runMu       sync.Mutex
	dispatcher  *edax.KeyOrderedDispatcher[M]
}

// NewConsumer wraps a caller-owned client with manual commits and a consumer group.
// It defaults to the kernel JSON serializer.
func NewConsumer[M edax.Message](client ConsumerClient, options ConsumerOptions[M]) (*Consumer[M], error) {
	if client == nil {
		return nil, &edax.Error{Code: edax.CodeBadHandle, Message: "Kafka consumer client is nil"}
	}
	if err := kernel.KafkaCheckConsumer(options.AutoCommit, options.HasGroup); err != nil {
		return nil, err
	}
	if options.CommitInterval == 0 {
		options.CommitInterval = time.Second
	}
	if options.CommitTimeout == 0 {
		options.CommitTimeout = 10 * time.Second
	}
	if options.DrainTimeout == 0 {
		options.DrainTimeout = 30 * time.Second
	}
	interval, timeout, drain := options.CommitInterval.Milliseconds(), options.CommitTimeout.Milliseconds(), options.DrainTimeout.Milliseconds()
	if err := kernel.KafkaCheckConfig(nil, &interval, &timeout, &drain); err != nil {
		return nil, err
	}
	if options.Serializer == nil {
		serializer, err := NewKernelSerializer[M]("json")
		if err != nil {
			return nil, err
		}
		options.Serializer = serializer
	}
	serializers := append([]Serializer[M]{options.Serializer}, options.Serializers...)
	media := make([]string, len(serializers))
	for i, serializer := range serializers {
		if serializer == nil {
			return nil, &edax.Error{Code: edax.CodeBadHandle, Message: "nil Kafka serializer"}
		}
		if _, err := kernel.MediaType("json", serializer.Kind()); err != nil {
			return nil, err
		}
		media[i] = serializer.MediaType()
	}
	if err := kernel.KafkaCheckMediaTypes(media); err != nil {
		return nil, err
	}
	options.Middleware = append([]Middleware(nil), options.Middleware...)
	return &Consumer[M]{client: client, options: options, serializers: serializers, mediaTypes: media, tracker: kernel.GetOffsetTracker(), rewinds: make(map[TopicPartition]int64)}, nil
}

// Close frees native bookkeeping. Stop Run and external polling first.
// The caller retains ownership of the broker client.
func (c *Consumer[M]) Close() error { return c.tracker.Close() }

// Stats reports the kernel tracker state for every known partition.
func (c *Consumer[M]) Stats() ([]PartitionStats, error) { return c.tracker.Stats() }

// Poll books each offset before decoding. A successful poll returns either a delivery
// or an Undecodable record with settlement callbacks, never both. An error returns
// neither; it reports a polling or bookkeeping failure rather than undecodable data.
func (c *Consumer[M]) Poll(ctx context.Context) (*edax.Delivery[M], *edax.Undecodable, error) {
	fetched, token, err := c.fetch(ctx)
	if err != nil {
		return nil, nil, err
	}
	original := *fetched
	original.Record = cloneRecord(fetched.Record)
	fetched = &original
	live := func() bool { value, err := c.tracker.IsLive(token); return err == nil && value }
	ack := func(context.Context) error { _, err := c.tracker.Ack(token); return err }
	nack := func(ctx context.Context) error {
		resume := c.pauseFetch()
		defer resume()
		c.mu.Lock()
		defer c.mu.Unlock()
		decision, err := c.tracker.Nack(token)
		if err != nil {
			return err
		}
		if decision.Seek == nil {
			return nil
		}
		partition := TopicPartition{fetched.Topic, fetched.Partition}
		c.rewinds[partition] = *decision.Seek
		if err = c.seek(ctx, partition, *decision.Seek); err != nil {
			return err
		}
		delete(c.rewinds, partition)
		return nil
	}
	record := cloneRecord(fetched.Record)
	for i := len(c.options.Middleware) - 1; i >= 0; i-- {
		record, err = c.options.Middleware[i].OnPoll(ctx, record)
		if err != nil {
			if edax.IsTerminal(err) && !edax.IsFatal(err) {
				poison, failure := c.poison(fetched, err, ack, nack, live)
				return nil, poison, failure
			}
			return nil, nil, errors.Join(err, nack(ctx))
		}
	}
	route, err := kernel.GetKafkaRoute(record.Headers, c.mediaTypes, c.options.Serializer.Kind())
	if err != nil {
		return nil, nil, errors.Join(err, nack(ctx))
	}
	serializer := c.options.Serializer
	if route.Registered != nil {
		serializer = c.serializers[*route.Registered]
	} else if route.Unknown != nil {
		poison, failure := c.poison(fetched, fmt.Errorf("no serializer for content type %q", *route.Unknown), ack, nack, live)
		return nil, poison, failure
	} else if route.Headerless {
		poison, failure := c.poison(fetched, errors.New("command record has no readable content-type header"), ack, nack, live)
		return nil, poison, failure
	}
	message, err := serializer.Decode(record.Value)
	if err != nil {
		poison, failure := c.poison(fetched, err, ack, nack, live)
		return nil, poison, failure
	}
	return &edax.Delivery[M]{Message: message, OnAck: ack, OnNack: nack, Live: live, Origin: fetched}, nil, nil
}

// Fetch and booking share the same lock as seek and rebalance. A nack cancels
// an outstanding poll before waiting for that lock, so idle topics cannot stall it.
func (c *Consumer[M]) fetch(ctx context.Context) (*Fetched, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for partition, offset := range c.rewinds {
		if err := c.seek(ctx, partition, offset); err != nil {
			return nil, 0, err
		}
		delete(c.rewinds, partition)
	}
	polling, cancel := context.WithCancel(ctx)
	c.pollMu.Lock()
	if c.transitions > 0 {
		c.pollMu.Unlock()
		cancel()
		return nil, 0, context.Canceled
	}
	c.pollCancel = cancel
	c.pollMu.Unlock()
	defer func() { cancel(); c.pollMu.Lock(); c.pollCancel = nil; c.pollMu.Unlock() }()
	fetched, err := c.client.Poll(polling)
	if err != nil {
		return nil, 0, err
	}
	if fetched == nil {
		return nil, 0, io.EOF
	}
	token, err := c.tracker.Book(fetched.Topic, fetched.Partition, fetched.Offset)
	return fetched, token, err
}
func (c *Consumer[M]) pauseFetch() func() {
	c.pollMu.Lock()
	c.transitions++
	if c.pollCancel != nil {
		c.pollCancel()
	}
	c.pollMu.Unlock()
	return func() { c.pollMu.Lock(); c.transitions--; c.pollMu.Unlock() }
}

func (c *Consumer[M]) poison(fetched *Fetched, cause error, ack, nack func(context.Context) error, live func() bool) (*edax.Undecodable, error) {
	id, err := kernel.KafkaPoisonID(fetched.Topic, fetched.Partition, fetched.Offset)
	if err != nil {
		return nil, err
	}
	return &edax.Undecodable{ID: id, Name: "kafka", Raw: append([]byte(nil), fetched.Record.Value...), Failure: poisonFailure(id, cause), Origin: fetched, OnAck: ack, OnNack: nack, Live: live}, nil
}
func cloneRecord(record Record) Record {
	result := Record{Key: append([]byte{}, record.Key...), Value: append([]byte{}, record.Value...), Headers: make([]Header, len(record.Headers))}
	for i, h := range record.Headers {
		result.Headers[i] = Header{Key: h.Key, Value: append([]byte{}, h.Value...)}
	}
	return result
}
func (c *Consumer[M]) seek(ctx context.Context, partition TopicPartition, offset int64) error {
	ctx, cancel := context.WithTimeout(ctx, c.options.CommitTimeout)
	defer cancel()
	return c.client.Seek(ctx, partition, offset)
}

// Flush commits completed watermarks. Failed commits remain due for retry.
func (c *Consumer[M]) Flush(ctx context.Context) error {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()
	due, err := c.tracker.Due()
	if err != nil {
		return err
	}
	return c.commit(ctx, due)
}
func (c *Consumer[M]) commit(ctx context.Context, due []kernel.KafkaDue) error {
	if len(due) == 0 {
		return nil
	}
	offsets := make(map[TopicPartition]int64, len(due))
	for _, d := range due {
		offsets[TopicPartition{d.Topic, d.Partition}] = d.Offset
	}
	ctx, cancel := context.WithTimeout(ctx, c.options.CommitTimeout)
	defer cancel()
	if err := c.client.Commit(ctx, offsets); err != nil {
		return err
	}
	for _, d := range due {
		if err := c.tracker.Committed(d.Topic, d.Partition, d.Offset); err != nil {
			return err
		}
	}
	return nil
}

// Assign resets tokens before the client delivers records for a new assignment.
func (c *Consumer[M]) Assign(partitions []TopicPartition) error {
	resume := c.pauseFetch()
	defer resume()
	c.commitMu.Lock()
	defer c.commitMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range partitions {
		delete(c.rewinds, p)
		if err := c.tracker.Reset(p.Topic, p.Partition); err != nil {
			return err
		}
	}
	return nil
}

// Revoke retires queued deliveries, commits the last safe watermarks and waits
// for active handlers. The client must stop delivering these partitions first.
// A failed commit causes redelivery. A drain timeout can overlap the new owner.
func (c *Consumer[M]) Revoke(ctx context.Context, partitions []TopicPartition) error {
	return c.retire(ctx, partitions, true)
}

// Lost retires deliveries and drains active handlers without committing offsets.
// Use it after the broker has already removed this member from the group.
func (c *Consumer[M]) Lost(ctx context.Context, partitions []TopicPartition) error {
	return c.retire(ctx, partitions, false)
}

func (c *Consumer[M]) retire(ctx context.Context, partitions []TopicPartition, commit bool) error {
	resume := c.pauseFetch()
	defer resume()
	c.commitMu.Lock()
	c.mu.Lock()
	due := make([]kernel.KafkaDue, 0, len(partitions))
	for _, p := range partitions {
		delete(c.rewinds, p)
		row, err := c.tracker.Revoke(p.Topic, p.Partition)
		if err != nil {
			c.mu.Unlock()
			c.commitMu.Unlock()
			return err
		}
		if row != nil {
			due = append(due, *row)
		}
	}
	c.mu.Unlock()
	var commitErr error
	if commit {
		commitErr = c.commit(ctx, due)
	}
	c.commitMu.Unlock()
	if commitErr != nil && c.options.CommitFailed != nil {
		c.options.CommitFailed(commitErr)
	}
	c.runMu.Lock()
	dispatcher := c.dispatcher
	c.runMu.Unlock()
	if dispatcher == nil {
		return commitErr
	}
	ctx, cancel := context.WithTimeout(ctx, c.options.DrainTimeout)
	defer cancel()
	drainErr := dispatcher.WaitHandlers(ctx)
	if drainErr != nil && c.options.RevokeOverrun != nil {
		c.options.RevokeOverrun(drainErr)
	}
	return errors.Join(commitErr, drainErr)
}

// Run starts the dispatcher, polls and commits until cancellation or EOF. It
// drains the dispatcher and attempts a final bounded commit before returning.
func (c *Consumer[M]) Run(ctx context.Context, dispatcher *edax.KeyOrderedDispatcher[M]) (runErr error) {
	if dispatcher == nil {
		return &edax.Error{Code: edax.CodeBadHandle, Message: "Kafka dispatcher is nil"}
	}
	if err := kernel.KafkaCheckDeadSink(dispatcher.HasDeadLetterSink() && dispatcher.HasPoisonSink()); err != nil {
		return err
	}
	c.runMu.Lock()
	if c.dispatcher != nil {
		c.runMu.Unlock()
		return &edax.Error{Code: edax.CodeBadHandle, Message: "Kafka consumer already running"}
	}
	c.dispatcher = dispatcher
	c.runMu.Unlock()
	defer func() { c.runMu.Lock(); c.dispatcher = nil; c.runMu.Unlock() }()
	if err := dispatcher.Start(context.WithoutCancel(ctx)); err != nil {
		return err
	}
	polling, cancel := context.WithCancel(ctx)
	commitDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(c.options.CommitInterval)
		defer ticker.Stop()
		for {
			select {
			case <-polling.Done():
				commitDone <- nil
				return
			case <-ticker.C:
				failure := c.Flush(polling)
				if failure != nil && c.options.CommitFailed != nil {
					c.options.CommitFailed(failure)
				}
				if edax.IsFatal(failure) {
					commitDone <- failure
					cancel()
					return
				}
				if failure = dispatcher.Err(); failure != nil {
					commitDone <- failure
					cancel()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		runErr = errors.Join(runErr, <-commitDone)
		drain, cancelDrain := context.WithTimeout(context.WithoutCancel(ctx), c.options.DrainTimeout)
		runErr = errors.Join(runErr, dispatcher.Stop(drain))
		cancelDrain()
		final, cancelFinal := context.WithTimeout(context.WithoutCancel(ctx), c.options.CommitTimeout)
		finalErr := c.Flush(final)
		cancelFinal()
		if finalErr != nil && c.options.CommitFailed != nil {
			c.options.CommitFailed(finalErr)
		}
		runErr = errors.Join(runErr, finalErr)
	}()
	streak := kernel.GetStreak()
	defer streak.Close()
	for {
		delivery, poison, err := c.Poll(polling)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if poison != nil {
			if err = dispatcher.SettlePoison(polling, poison); err != nil {
				return err
			}
			if err = streak.Answered(); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			if polling.Err() != nil {
				return polling.Err()
			}
			if edax.IsFatal(err) || edax.IsTerminal(err) {
				return err
			}
			step, failure := streak.Failed()
			if failure != nil {
				return failure
			}
			if err = c.options.Pacing.Wait(polling, step); err != nil {
				return err
			}
			continue
		}
		if err = streak.Answered(); err != nil {
			return err
		}
		if err = dispatcher.Submit(polling, delivery); err != nil {
			return errors.Join(err, delivery.Nack(context.WithoutCancel(ctx)))
		}
	}
}

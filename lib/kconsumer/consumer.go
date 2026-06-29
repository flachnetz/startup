package kconsumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/flachnetz/startup/v2/startup_tracing"
	"go.opentelemetry.io/otel/trace"
)

// The default time that the kafka consumer polls for a new message before
// it checks the context for cancellation.
var DefaultPollTimeout = 100 * time.Millisecond

// HandleMessage is the user-provided function called for each consumed message.
// It receives the raw *kafka.Message so the caller has access to key, headers,
// and partition/offset metadata.
type HandleMessage func(ctx context.Context, msg *kafka.Message) error

// PartitionConsumer subscribes to a topic and runs a dedicated goroutine per
// assigned partition. Offset storage is explicit: only messages that were
// successfully handled are stored for auto-commit.
//
// If you return an error from your handler, you get two retries before the
// consumer will quit and shutdown with an error.
type PartitionConsumer struct {
	Topics   []string
	Consumer *kafka.Consumer
}

type partitionWorker struct {
	topic     string
	partition int32
	msgs      chan *kafka.Message
	handled   atomic.Int64 // last successfully handled offset; -1 = none
	done      chan struct{}
	errCh     chan error
}

func (c *PartitionConsumer) Consume(ctx context.Context, handle HandleMessage) error {
	if err := c.Consumer.SubscribeTopics(c.Topics, nil); err != nil {
		return fmt.Errorf("subscribe to %v: %w", c.Topics, err)
	}

	workers := partitionsWorkers{
		Consumer: c.Consumer,
		Workers:  map[int32]*partitionWorker{},
	}

	slog.Info("Partition consumer started", slog.Any("topics", c.Topics))

	lastStored := time.Now()

	for {
		if err := ctx.Err(); err != nil {
			slog.Info("Context closed, shutting consumer down", sl.Error(err))
			workers.Shutdown()

			return fmt.Errorf("context: %w", err)
		}

		// check all workers for done or errors
		for _, w := range workers.Workers {
			select {
			case err := <-w.errCh:
				return fmt.Errorf("worker for partition %d died with error: %w", w.partition, err)

			case <-w.done:
				return fmt.Errorf("worker for partition %d died", w.partition)

			default:
				// do not block
			}
		}

		msg, err := c.Consumer.ReadMessage(DefaultPollTimeout)
		if err != nil {
			if ke, ok := errors.AsType[kafka.Error](err); ok {
				if ke.IsTimeout() {
					continue
				}
				if ke.IsFatal() {
					return fmt.Errorf("fatal kafka error: %w", ke)
				}
			}
			slog.Warn("Error reading message", slog.Any("error", err))
			continue
		}

		if time.Since(lastStored) >= 5*time.Second {
			workers.StoreOffsets()
			lastStored = time.Now()
		}

		w := workers.Get(ctx, *msg.TopicPartition.Topic, msg.TopicPartition.Partition, handle)

		select {
		case w.msgs <- msg:

		case err := <-w.errCh:
			return fmt.Errorf("worker for partition %d died with error: %w", w.partition, err)

		case <-w.done:
			return fmt.Errorf("worker for partition %d died", w.partition)
		}
	}
}

func runWorker(ctx context.Context, w *partitionWorker, handle HandleMessage) {
	defer close(w.done)

	log := slog.With(
		slog.String("topic", w.topic),
		slog.Int("partition", int(w.partition)),
	)

	for msg := range w.msgs {
		offset := msg.TopicPartition.Offset

		err := startup_tracing.Trace(ctx, "kafka:consume", func(ctx context.Context, span trace.Span) (err error) {
			for attempt := 1; attempt <= 3; attempt++ {
				if err = handle(ctx, msg); err != nil {
					log.Error("Handle failed",
						slog.Int64("offset", int64(offset)),
						slog.Int("attempt", attempt),
						slog.Any("error", err),
					)

					// wait a short moment before retrying
					time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
					continue
				}

				return nil
			}

			return
		})
		if err != nil {
			log.Error("Giving up on message", slog.Int64("offset", int64(offset)))
			w.errCh <- err
			break
		}

		w.handled.Store(int64(offset))
	}

	log.Info("Worker exiting", slog.Int64("lastHandled", w.handled.Load()))
}

type partitionsWorkers struct {
	Consumer *kafka.Consumer
	Workers  map[int32]*partitionWorker
}

func (p *partitionsWorkers) StoreOffsets() {
	for _, w := range p.Workers {
		if h := w.handled.Load(); h >= 0 {
			_, _ = p.Consumer.StoreOffsets([]kafka.TopicPartition{{
				Topic: &w.topic, Partition: w.partition, Offset: kafka.Offset(h + 1),
			}})
		}
	}
}

func (p *partitionsWorkers) Get(ctx context.Context, topic string, partition int32, handle HandleMessage) *partitionWorker {
	if w, ok := p.Workers[partition]; ok {
		return w
	}

	w := &partitionWorker{
		topic:     topic,
		partition: partition,
		msgs:      make(chan *kafka.Message, 64),
		done:      make(chan struct{}),
		errCh:     make(chan error),
	}

	w.handled.Store(-1)
	p.Workers[partition] = w

	slog.Info("Spawning worker",
		slog.String("topic", topic),
		slog.Int("partition", int(partition)),
	)

	go runWorker(ctx, w, handle)

	return w
}

func (p *partitionsWorkers) Shutdown() {
	// close each worker and wait for it to finish
	for _, w := range p.Workers {
		close(w.msgs)
		<-w.done
	}

	// store the latest offsets
	p.StoreOffsets()

	// and commit them to kafka
	if _, err := p.Consumer.Commit(); err != nil {
		slog.Error("Final commit failed", slog.Any("error", err))
	}
}

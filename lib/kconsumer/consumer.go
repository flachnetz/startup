package kconsumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/flachnetz/startup/v2/startup_tracing"
	"go.opentelemetry.io/otel/trace"
)

// DefaultPollTimeout is default time that the kafka consumer polls for a new message before
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
// If your handler returns an error, it is retried twice (three attempts in
// total). If it still fails, or if it panics, the affected worker stops and
// Consume shuts the whole consumer down, returning that error. A panic is
// recovered and reported like any other handler failure, so it never crashes
// the process or deadlocks the remaining partition workers.
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

// Consume subscribes to the configured topics and dispatches every consumed
// message to handle. Each assigned partition is processed by its own worker
// goroutine, so messages within a partition are handled in order while
// different partitions run concurrently.
//
// Offsets are stored only after a message was handled successfully and are
// flushed to the broker roughly every five seconds (and once more on
// shutdown). Consume blocks until ctx is canceled or a worker fails (because
// its handler exhausted its retries or panicked), in which case it shuts the
// workers down and returns an error.
func (c *PartitionConsumer) Consume(ctx context.Context, handle HandleMessage) error {
	if err := c.Consumer.SubscribeTopics(c.Topics, nil); err != nil {
		return fmt.Errorf("subscribe to %v: %w", c.Topics, err)
	}

	workers := partitionsWorkers{
		Consumer: c.Consumer,
		Workers:  map[int32]*partitionWorker{},
	}

	defer workers.Shutdown()

	slog.InfoContext(ctx, "Partition consumer started", slog.Any("topics", c.Topics))

	lastStored := time.Now()

	for {
		if err := ctx.Err(); err != nil {
			slog.InfoContext(ctx, "Context closed, shutting consumer down", sl.Error(err))
			return fmt.Errorf("context: %w", err)
		}

		// check all workers for done or errors
		for _, w := range workers.Workers {
			if err := w.stopped(); err != nil {
				return err
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
			slog.WarnContext(ctx, "Error reading message", slog.Any("error", err))
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
			// The worker always writes to errCh before closing done, so prefer
			// the concrete error if one is available.
			return w.stopped()
		}
	}
}

// stopped reports a non-nil error if the worker has stopped, preferring the
// concrete failure over the generic "died". It never blocks. Because a worker
// always writes to errCh before closing done, checking errCh first guarantees
// that a reported error is never lost.
func (w *partitionWorker) stopped() error {
	select {
	case err := <-w.errCh:
		return fmt.Errorf("worker for partition %d died with error: %w", w.partition, err)
	default:
	}

	select {
	case <-w.done:
		return fmt.Errorf("worker for partition %d died", w.partition)
	default:
		return nil
	}
}

func runWorker(ctx context.Context, w *partitionWorker, handle HandleMessage) {
	defer close(w.done)

	log := slog.With(
		slog.String("topic", w.topic),
		slog.Int("partition", int(w.partition)),
	)

	// A panic in the handler would otherwise crash the whole process. Recover it
	// and report it as an error so Consume can shut the consumer down cleanly
	// instead of leaving the other workers deadlocked.
	defer func() {
		if r := recover(); r != nil {
			log.ErrorContext(ctx,
				"Worker panicked",
				slog.Any("panic", r),
				slog.String("stack", string(debug.Stack())),
			)
			// Report the panic. errCh is buffered, so this never blocks even if
			// Consume already returned because another worker failed first.
			w.errCh <- fmt.Errorf("worker for partition %d panicked: %v", w.partition, r)
		}
	}()

	for msg := range w.msgs {
		offset := msg.TopicPartition.Offset

		err := startup_tracing.Trace(ctx, "kafka:consume", func(ctx context.Context, span trace.Span) (err error) {
			for attempt := 1; attempt <= 3; attempt++ {
				if err = continueTrace(ctx, msg, handle); err != nil {
					log.ErrorContext(ctx,
						"Handle failed",
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
			log.ErrorContext(ctx, "Giving up on message", slog.Int64("offset", int64(offset)))
			// errCh is buffered, so this never blocks even if Consume already
			// returned because another worker failed first.
			w.errCh <- err
			break
		}

		w.handled.Store(int64(offset))
	}

	log.InfoContext(ctx, "Worker exiting", slog.Int64("lastHandled", w.handled.Load()))
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
		// buffered so a failing worker can report its error without blocking,
		// even if Consume has already returned for another worker.
		errCh: make(chan error, 1),
	}

	w.handled.Store(-1)
	p.Workers[partition] = w

	slog.InfoContext(ctx,
		"Spawning worker",
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

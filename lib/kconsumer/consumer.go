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
// shutdown). On a rebalance all workers are drained and their offsets are
// committed synchronously before partition ownership changes, so worker state
// never survives an assignment change. Consume blocks until ctx is canceled
// or a worker fails (because its handler exhausted its retries or panicked),
// in which case it shuts the workers down and returns an error.
func (c *PartitionConsumer) Consume(ctx context.Context, handle HandleMessage) error {
	workers := partitionsWorkers{
		Consumer: c.Consumer,
		Workers:  map[topicPartition]*partitionWorker{},
	}

	// The rebalance callback is invoked from within ReadMessage on this
	// goroutine, so it is safe to touch the workers map here. Worker state
	// must never survive an assignment change: on revoke we drain all workers
	// and store their offsets. We do NOT call Commit() here — the broker
	// rejects explicit commits during a rebalance. Instead, StoreOffsets marks
	// them in librdkafka's internal state, and the library automatically
	// commits stored offsets as part of the rebalance protocol.
	rebalanceCb := func(consumer *kafka.Consumer, event kafka.Event) error {
		slog.InfoContext(ctx, "Rebalance event", slog.String("event", event.String()))

		switch event.(type) {
		case kafka.RevokedPartitions:
			workers.DrainAll()

		case kafka.AssignedPartitions:
			// Should already be empty after the preceding revoke, but drain
			// defensively in case an assignment arrives without one.
			workers.DrainAll()
		}

		return nil
	}

	if err := c.Consumer.SubscribeTopics(c.Topics, rebalanceCb); err != nil {
		return fmt.Errorf("subscribe to %v: %w", c.Topics, err)
	}

	// try to cleanup a little by unsubscribing in the end
	defer c.Consumer.Unsubscribe()

	defer workers.Shutdown()

	slog.InfoContext(ctx, "Partition consumer started", slog.Any("topics", c.Topics))

	lastStored := time.Now()

	for {
		if err := ctx.Err(); err != nil {
			slog.InfoContext(ctx, "Context closed, shutting consumer down", sl.Error(err))
			return fmt.Errorf("context: %w", err)
		}

		// check all workers for done or errors, including workers that
		// failed while being drained during a rebalance
		if err := workers.Failure(); err != nil {
			return err
		}

		for _, w := range workers.Workers {
			if err := w.stopped(); err != nil {
				return err
			}
		}

		// store offsets periodically. This runs before ReadMessage so offsets
		// are also stored while the topic is idle.
		if time.Since(lastStored) >= 5*time.Second {
			workers.StoreOffsets()
			lastStored = time.Now()
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
			log.ErrorContext(
				ctx,
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
					log.ErrorContext(
						ctx,
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

type topicPartition struct {
	topic     string
	partition int32
}

type partitionsWorkers struct {
	Consumer *kafka.Consumer
	Workers  map[topicPartition]*partitionWorker

	// first error observed while draining workers
	err error
}

func (p *partitionsWorkers) StoreOffsets() {
	for _, w := range p.Workers {
		if h := w.handled.Load(); h >= 0 {
			_, _ = p.Consumer.StoreOffsets([]kafka.TopicPartition{{
				Topic:     &w.topic,
				Partition: w.partition,
				Offset:    kafka.Offset(h + 1),
			}})
		}
	}
}

func (p *partitionsWorkers) Get(ctx context.Context, topic string, partition int32, handle HandleMessage) *partitionWorker {
	key := topicPartition{topic, partition}

	if w, ok := p.Workers[key]; ok {
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
	p.Workers[key] = w

	slog.InfoContext(
		ctx,
		"Spawning worker",
		slog.String("topic", topic),
		slog.Int("partition", int(partition)),
	)

	go runWorker(ctx, w, handle)

	return w
}

// DrainAll stops all workers, waits for them to finish, stores their offsets
// and removes them from the map. All worker errors are joined and reported
// via Failure.
func (p *partitionsWorkers) DrainAll() {
	for key, w := range p.Workers {
		close(w.msgs)
		<-w.done

		// done is always closed after a drain, so only a concrete error on
		// errCh indicates a failure here.
		select {
		case err := <-w.errCh:
			p.err = errors.Join(p.err, fmt.Errorf("worker for partition %d died with error: %w", w.partition, err))
		default:
		}

		if h := w.handled.Load(); h >= 0 {
			_, _ = p.Consumer.StoreOffsets([]kafka.TopicPartition{{
				Topic:     &w.topic,
				Partition: w.partition,
				Offset:    kafka.Offset(h + 1),
			}})
		}

		delete(p.Workers, key)
	}
}

// Failure returns the first error observed while draining workers, if any.
func (p *partitionsWorkers) Failure() error {
	return p.err
}

// Commit synchronously commits all stored offsets. A commit without any
// stored offsets is not an error.
func (p *partitionsWorkers) Commit() {
	slog.Debug("Comitting offsets now")
	if _, err := p.Consumer.Commit(); err != nil {
		if ke, ok := errors.AsType[kafka.Error](err); ok && ke.Code() == kafka.ErrNoOffset {
			return
		}

		slog.Error("Commit failed", sl.Error(err))
	}
}

func (p *partitionsWorkers) Shutdown() {
	p.DrainAll()
	p.Commit()
}

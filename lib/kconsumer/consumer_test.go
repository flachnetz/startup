package kconsumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/flachnetz/startup/v2/lib/testx"
	"github.com/stretchr/testify/require"
)

func TestPartitionConsumer(t *testing.T) {
	const (
		topic    = "test-topic"
		msgCount = 5
	)

	ctx, cancelTimeout := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancelTimeout()

	cluster := testx.KafkaCluster(t)

	cluster.CreateTopic(topic, 3)

	cluster.Send(messageOf(topic, 0, "message-0"))
	cluster.Send(messageOf(topic, 1, "message-1"))
	cluster.Send(messageOf(topic, 2, "message-2"))
	cluster.Send(messageOf(topic, 0, "message-3"))
	cluster.Send(messageOf(topic, 1, "message-4"))

	// t.Context() is cancelled when the test finishes; we wrap it so we can
	// cancel the consumer ourselves once all expected messages arrived.
	ctx, cancelContext := context.WithCancel(t.Context())
	defer cancelContext()

	var (
		mu       sync.Mutex
		received []*kafka.Message
	)

	done := make(chan struct{})
	handle := func(ctx context.Context, msg *kafka.Message) error {
		mu.Lock()
		received = append(received, msg)
		n := len(received)
		mu.Unlock()

		if n == msgCount {
			slog.InfoContext(ctx, "Got all messages")
			close(done)
		}

		return nil
	}

	consumer := &PartitionConsumer{
		Consumer: cluster.Consumer(),
		Topics:   []string{topic},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Consume(ctx, handle)
	}()

	select {
	case <-done:
		// got all messages, stop the consumer
		cancelContext()
	case <-time.After(30 * time.Second):
		cancelContext()
		require.Fail(t, "timed out waiting for messages")
	}

	// Consume should return after the context is canceled.
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled, "unexpected consume error")

	case <-time.After(10 * time.Second):
		require.Fail(t, "consumer did not shut down after cancel")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, msgCount)
}

// TestPartitionConsumer_WorkerReturnsErrors makes every handler invocation fail.
// After the configured retries the affected worker gives up, and Consume must
// return that error instead of hanging. With two failing partitions this also
// exercises the shutdown path where more than one worker fails at once.
func TestPartitionConsumer_WorkerReturnsErrors(t *testing.T) {
	const topic = "error-topic"

	cluster := testx.KafkaCluster(t)
	cluster.CreateTopic(topic, 2)

	cluster.Send(messageOf(topic, 0, "fail-0"))
	cluster.Send(messageOf(topic, 1, "fail-1"))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var calls atomic.Int64
	handle := func(ctx context.Context, msg *kafka.Message) error {
		calls.Add(1)
		return fmt.Errorf("permanent failure")
	}

	consumer := &PartitionConsumer{
		Consumer: cluster.Consumer(),
		Topics:   []string{topic},
	}

	errCh := make(chan error, 1)
	go func() { errCh <- consumer.Consume(ctx, handle) }()

	select {
	case err := <-errCh:
		require.Error(t, err)
		require.NotErrorIs(t, err, context.Canceled)
		require.Contains(t, err.Error(), "permanent failure")

	case <-time.After(30 * time.Second):
		cancel()
		require.Fail(t, "consumer did not shut down after repeated handler errors")
	}

	// the failing message must have been retried three times before giving up.
	require.GreaterOrEqual(t, calls.Load(), int64(3))
}

// TestPartitionConsumer_WorkerPanics ensures a panic inside the handler does not
// crash the process or deadlock the other workers. The panicking worker must be
// recovered, reported as an error, and Consume must shut everything down.
func TestPartitionConsumer_WorkerPanics(t *testing.T) {
	const topic = "panic-topic"

	cluster := testx.KafkaCluster(t)
	cluster.CreateTopic(topic, 2)

	// partition 0 is handled fine, partition 1 panics.
	cluster.Send(messageOf(topic, 0, "ok-0"))
	cluster.Send(messageOf(topic, 1, "boom-1"))
	cluster.Send(messageOf(topic, 0, "ok-2"))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	handle := func(ctx context.Context, msg *kafka.Message) error {
		if msg.TopicPartition.Partition == 1 {
			panic("handler blew up")
		}
		return nil
	}

	consumer := &PartitionConsumer{
		Consumer: cluster.Consumer(),
		Topics:   []string{topic},
	}

	errCh := make(chan error, 1)
	go func() { errCh <- consumer.Consume(ctx, handle) }()

	select {
	case err := <-errCh:
		require.Error(t, err)
		require.NotErrorIs(t, err, context.Canceled)
		require.Contains(t, err.Error(), "panicked")

	case <-time.After(20 * time.Second):
		cancel()
		require.Fail(t, "consumer did not shut down after worker panic")
	}
}

func messageOf(topic string, partition int, payload string) *kafka.Message {
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     new(topic),
			Partition: int32(partition),
		},
		Value: []byte(payload),
	}
}

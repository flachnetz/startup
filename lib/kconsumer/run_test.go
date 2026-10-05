package kconsumer

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/flachnetz/startup/v2/lib/testx"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/stretchr/testify/require"
)

// syncBuffer is a bytes.Buffer safe for the concurrent writes of a slog handler.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// runConsumerLogged starts RunConsumer with a context logger that writes to the
// returned buffer and returns a channel closed when RunConsumer returns.
func runConsumerLogged(ctx context.Context, consumer *PartitionConsumer, handle HandleMessage) (*syncBuffer, <-chan struct{}) {
	logs := &syncBuffer{}
	ctx = sl.WithLogger(ctx, slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		RunConsumer(ctx, consumer, handle)
	}()

	return logs, stopped
}

// TestRunConsumer_ShutdownIsNotAnError stops a healthy consumer by cancelling its
// context. The shutdown must not be reported as "Consumer stopped with error".
func TestRunConsumer_ShutdownIsNotAnError(t *testing.T) {
	const topic = "shutdown-topic"

	cluster := testx.KafkaCluster(t)
	cluster.CreateTopic(topic, 1)
	cluster.Send(messageOf(topic, 0, "message-0"))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	handled := make(chan struct{})
	var once sync.Once
	handle := func(ctx context.Context, msg *kafka.Message) error {
		once.Do(func() { close(handled) })
		return nil
	}

	logs, stopped := runConsumerLogged(ctx, &PartitionConsumer{Consumer: cluster.Consumer(), Topics: []string{topic}}, handle)

	select {
	case <-handled:
	case <-time.After(30 * time.Second):
		require.Fail(t, "consumer did not handle the message")
	}

	cancel()

	select {
	case <-stopped:
	case <-time.After(15 * time.Second):
		require.Fail(t, "RunConsumer did not return after cancel")
	}

	require.Contains(t, logs.String(), "Stopping consumer, Context is close")
	require.NotContains(t, logs.String(), "Consumer stopped with error")
}

// TestRunConsumer_LogsConsumeErrors makes the handler fail permanently. Consume
// returns that error while the context is still live, so it must be logged at
// ERROR before the consumer restarts.
func TestRunConsumer_LogsConsumeErrors(t *testing.T) {
	const topic = "failing-topic"

	cluster := testx.KafkaCluster(t)
	cluster.CreateTopic(topic, 1)
	cluster.Send(messageOf(topic, 0, "fail-0"))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	handle := func(ctx context.Context, msg *kafka.Message) error {
		return fmt.Errorf("permanent failure")
	}

	logs, stopped := runConsumerLogged(ctx, &PartitionConsumer{Consumer: cluster.Consumer(), Topics: []string{topic}}, handle)

	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(logs.String(), "Consumer stopped with error") {
		if time.Now().After(deadline) {
			require.Fail(t, "consume error was not logged", logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	require.Contains(t, logs.String(), "level=ERROR msg=\"Consumer stopped with error, restarting\"")
	require.Contains(t, logs.String(), "permanent failure")

	cancel()

	select {
	case <-stopped:
	case <-time.After(15 * time.Second):
		require.Fail(t, "RunConsumer did not return after cancel")
	}
}

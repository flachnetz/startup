package kconsumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/flachnetz/startup/v2/lib/testx"
	"github.com/stretchr/testify/require"
)

func TestPartitionConsumer(t *testing.T) {
	const (
		topic    = "test-topic"
		groupID  = "test-group"
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
			slog.Info("Got all messages")
			close(done)
		}

		return nil
	}

	consumer := &PartitionConsumer{
		Topics:  []string{topic},
		Brokers: []string{cluster.BootstrapServers},
		GroupID: groupID,
		// MockCluster speaks plaintext, so override the default ssl protocol.
		Properties: []string{"security.protocol=plaintext"},
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

func produceMessagesAsync(t *testing.T, brokers string, messages ...kafka.Message) {
	errorCh := make(chan error)

	go func() {
		for {
			select {
			case <-t.Context().Done():
				return

			case err := <-errorCh:
				t.Logf("Failure sending mocked messages: %s", err)
			}
		}
	}()

	go func() {
		producer, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": brokers,
		})
		if err != nil {
			errorCh <- fmt.Errorf("create producer: %v", err)
			return
		}

		defer producer.Close()

		for idx, msg := range messages {
			if err := producer.Produce(new(msg), nil); err != nil {
				errorCh <- fmt.Errorf("produce message %d: %v", idx, err)
				return
			}
		}

		if remaining := producer.Flush(10_000); remaining > 0 {
			// TODO Flush actually sends those messages but still reports that they
			//  are remaining and unsent. Probably a MockCluster issue.
			//  We just ignore it here and assume that we'll
			errorCh <- fmt.Errorf("failed to flush %d messages", remaining)
			return
		}
	}()
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

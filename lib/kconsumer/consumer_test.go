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
	"github.com/flachnetz/startup/v2/lib/goid"
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

// TestPartitionConsumer_MultiTopic verifies that messages from different topics
// but the same partition number are dispatched to separate workers (goroutines)
// and don't collide. This was a bug when the worker map was keyed by partition alone.
func TestPartitionConsumer_MultiTopic(t *testing.T) {
	const (
		topicA   = "multi-topic-a"
		topicB   = "multi-topic-b"
		msgCount = 4
	)

	cluster := testx.KafkaCluster(t)
	cluster.CreateTopic(topicA, 2)
	cluster.CreateTopic(topicB, 2)

	// Send messages to the same partition number on both topics.
	cluster.Send(messageOf(topicA, 0, "a-p0-1"))
	cluster.Send(messageOf(topicA, 0, "a-p0-2"))
	cluster.Send(messageOf(topicB, 0, "b-p0-1"))
	cluster.Send(messageOf(topicB, 0, "b-p0-2"))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Track which goroutine handled each message.
	type handled struct {
		topic string
		gid   goid.Id
	}

	var (
		mu       sync.Mutex
		received []handled
	)

	done := make(chan struct{})
	handle := func(ctx context.Context, msg *kafka.Message) error {
		mu.Lock()
		received = append(received, handled{
			topic: *msg.TopicPartition.Topic,
			gid:   goid.Get(),
		})
		n := len(received)
		mu.Unlock()

		if n == msgCount {
			close(done)
		}
		return nil
	}

	consumer := &PartitionConsumer{
		Consumer: cluster.Consumer(),
		Topics:   []string{topicA, topicB},
	}

	errCh := make(chan error, 1)
	go func() { errCh <- consumer.Consume(ctx, handle) }()

	select {
	case <-done:
		cancel()
	case <-time.After(15 * time.Second):
		cancel()
		require.Fail(t, "timed out waiting for messages")
	}

	<-errCh

	require.Len(t, received, msgCount)

	// Group goroutine IDs by topic. Messages from the same topic+partition
	// must share a goroutine, but different topics on the same partition
	// number must use different goroutines.
	gidByTopic := map[string]goid.Id{}
	for _, h := range received {
		if prev, ok := gidByTopic[h.topic]; ok {
			require.Equal(t, prev, h.gid,
				"messages from same topic+partition should be on same goroutine")
		} else {
			gidByTopic[h.topic] = h.gid
		}
	}

	require.NotEqual(t, gidByTopic[topicA], gidByTopic[topicB],
		"different topics on same partition must use different worker goroutines")
}

// TestPartitionConsumer_OffsetCommittedOnShutdown verifies that after a clean
// shutdown the committed offset reflects all handled messages. A second consumer
// in the same group should not re-read them.
func TestPartitionConsumer_OffsetCommittedOnShutdown(t *testing.T) {
	const topic = "offset-commit-topic"

	cluster := testx.KafkaCluster(t)
	cluster.CreateTopic(topic, 1)

	cluster.Send(messageOf(topic, 0, "msg-0"))
	cluster.Send(messageOf(topic, 0, "msg-1"))
	cluster.Send(messageOf(topic, 0, "msg-2"))

	groupID := fmt.Sprintf("offset-test-%d", time.Now().UnixNano())

	newConsumer := func() *kafka.Consumer {
		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"group.id":              groupID,
			"bootstrap.servers":     cluster.BootstrapServers,
			"auto.offset.reset":     "earliest",
			"session.timeout.ms":    3000,
			"heartbeat.interval.ms": 1000,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })
		return c
	}

	// First consumer: handle all 3 messages then shut down.
	ctx1, cancel1 := context.WithCancel(t.Context())
	defer cancel1()

	var count atomic.Int64
	done := make(chan struct{})

	pc1 := &PartitionConsumer{
		Consumer: newConsumer(),
		Topics:   []string{topic},
	}

	errCh := make(chan error, 1)
	go func() {
		_ = pc1.Consume(ctx1, func(ctx context.Context, msg *kafka.Message) error {
			if count.Add(1) == 3 {
				close(done)
			}
			return nil
		})

		// close the actual consumer
		errCh <- pc1.Consumer.Close()
	}()

	select {
	case <-done:
		cancel1()
	case <-time.After(15 * time.Second):
		cancel1()
		require.Fail(t, "first consumer timed out")
	}

	// wait for the consumer to be closed
	<-errCh

	// Second consumer in the same group should not get any old messages.
	// Send a new message so we can confirm the consumer is working.
	cluster.Send(messageOf(topic, 0, "msg-3"))

	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()

	var received2 atomic.Int64
	var values2 sync.Map
	done2 := make(chan struct{})

	pc2 := &PartitionConsumer{
		Consumer: newConsumer(),
		Topics:   []string{topic},
	}

	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- pc2.Consume(ctx2, func(ctx context.Context, msg *kafka.Message) error {
			values2.Store(string(msg.Value), true)
			if received2.Add(1) == 1 {
				close(done2)
			}
			return nil
		})
	}()

	select {
	case <-done2:
		// Give a brief moment for any extra (unwanted) messages to arrive.
		time.Sleep(500 * time.Millisecond)
		cancel2()
	case <-time.After(15 * time.Second):
		cancel2()
		require.Fail(t, "second consumer timed out")
	}

	<-errCh2

	// The second consumer should only have received msg-3.
	_, gotOld := values2.Load("msg-0")
	require.False(t, gotOld, "second consumer re-read msg-0; offset was not committed")
	_, gotNew := values2.Load("msg-3")
	require.True(t, gotNew, "second consumer did not receive msg-3")
}

// TestPartitionConsumer_Rebalance verifies that when a second consumer joins the
// group (triggering a rebalance), offsets for handled messages are committed so
// the new consumer does not re-process them.
func TestPartitionConsumer_Rebalance(t *testing.T) {
	const topic = "rebalance-topic"

	cluster := testx.KafkaCluster(t)
	cluster.CreateTopic(topic, 2)

	groupID := fmt.Sprintf("rebalance-test-%d", time.Now().UnixNano())

	newConsumer := func() *kafka.Consumer {
		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"group.id":              groupID,
			"bootstrap.servers":     cluster.BootstrapServers,
			"auto.offset.reset":     "earliest",
			"session.timeout.ms":    3000,
			"heartbeat.interval.ms": 1000,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })
		return c
	}

	// Pre-produce messages to both partitions.
	for i := range 10 {
		cluster.Send(messageOf(topic, i%2, fmt.Sprintf("msg-%d", i)))
	}

	// Allow producer to flush.
	time.Sleep(200 * time.Millisecond)

	// First consumer handles messages.
	ctx1, cancel1 := context.WithCancel(t.Context())
	defer cancel1()

	var handled1 atomic.Int64
	pc1 := &PartitionConsumer{
		Consumer: newConsumer(),
		Topics:   []string{topic},
	}

	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- pc1.Consume(ctx1, func(ctx context.Context, msg *kafka.Message) error {
			handled1.Add(1)
			return nil
		})
	}()

	// Wait until first consumer has handled some messages.
	require.Eventually(t, func() bool {
		return handled1.Load() >= 5
	}, 10*time.Second, 50*time.Millisecond)

	time.Sleep(10 * time.Second)

	// Now start a second consumer in the same group to trigger rebalance.
	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()

	var handled2Values sync.Map
	pc2 := &PartitionConsumer{
		Consumer: newConsumer(),
		Topics:   []string{topic},
	}

	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- pc2.Consume(ctx2, func(ctx context.Context, msg *kafka.Message) error {
			handled2Values.Store(string(msg.Value), true)
			return nil
		})
	}()

	// Give time for rebalance and second consumer to process.
	time.Sleep(5 * time.Second)

	cancel1()
	cancel2()
	<-errCh1
	<-errCh2

	// The key assertion: total handled across both consumers should equal the
	// total message count. If offsets weren't committed on revoke, the second
	// consumer would re-read messages already handled by the first, leading to
	// duplicates. We can't perfectly assert zero duplicates with at-least-once
	// semantics, but we can confirm the second consumer didn't start from zero.
	totalHandled := handled1.Load()
	var handled2Count int64
	handled2Values.Range(func(_, _ any) bool {
		handled2Count++
		return true
	})

	// With 10 messages total and proper offset commits, the total unique
	// processing across both consumers should be <= 10 + small rebalance overlap.
	t.Logf("consumer1 handled %d, consumer2 handled %d unique", totalHandled, handled2Count)
	require.LessOrEqual(t, totalHandled+handled2Count, int64(15),
		"too many messages processed; offsets likely not committed on rebalance")
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

package testx

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/stretchr/testify/require"
)

// Kafka is a handle to a running in-memory mock Kafka cluster together with a
// producer for sending messages to it.
type Kafka struct {
	BootstrapServers string

	testing *testing.T
	cluster *kafka.MockCluster
	sendCh  chan *kafka.Message
}

// KafkaCluster starts a single-broker in-memory mock Kafka cluster and a background
// producer. The cluster and producer are closed automatically on test cleanup.
//
// Warning: The mock cluster does not support topic creation with AdminClient.CreateTopic,
// you need to create topics via the CreateTopic method on the mock cluster
func KafkaCluster(t *testing.T) *Kafka {
	t.Helper()

	// create a new mock cluster
	cluster, err := kafka.NewMockCluster(1)
	require.NoError(t, err, "create mock cluster")

	t.Cleanup(cluster.Close)

	// create a kafka producer
	servers := cluster.BootstrapServers()
	require.NotEmpty(t, servers)

	config := &kafka.ConfigMap{"bootstrap.servers": servers}
	producer, err := kafka.NewProducer(config)
	require.NoError(t, err)

	// start send loop
	sendCh := make(chan *kafka.Message, 1024)
	go kafkaWorker(t.Context(), producer, sendCh)

	return &Kafka{
		BootstrapServers: cluster.BootstrapServers(),

		testing: t,
		cluster: cluster,
		sendCh:  sendCh,
	}
}

func kafkaWorker(ctx context.Context, producer *kafka.Producer, messagesCh chan *kafka.Message) {
	defer producer.Close()

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-messagesCh:
			err := producer.Produce(msg, nil)
			if err != nil {
				slog.Error("Failed to produce message", sl.Error(err))
			}

		default:
			// flush for a moment
			producer.Flush(50)

			// flush might return directly, so wait a little here
			time.Sleep(25 * time.Millisecond)
		}
	}
}

// Send queues msg for asynchronous delivery to the cluster.
func (k *Kafka) Send(msg *kafka.Message) {
	k.sendCh <- msg
}

// CreateTopic creates a topic with the given number of partitions and a replication
// factor of 1. The test fails if the topic cannot be created.
func (k *Kafka) CreateTopic(name string, partitions int) {
	err := k.cluster.CreateTopic(name, partitions, 1)
	require.NoErrorf(k.testing, err, "Create topic %q", name)
}

// TestConsumer creates a consumer subscribed to the given topic(s). The consumer is
// closed automatically on test cleanup.
func (k *Kafka) TestConsumer(topic string, moreTopics ...string) *KafkaConsumer {
	slog.Info("Create test consumer")
	consumer := k.Consumer()

	topics := append([]string{topic}, moreTopics...)

	slog.Info("Subscribing to test topic", slog.Any("topics", topics))
	err := consumer.SubscribeTopics(topics, nil)
	require.NoErrorf(k.testing, err, "Subscribe to topic %q", topic)

	return &KafkaConsumer{
		Consumer: consumer,
		testing:  k.testing,
		topic:    topic,
	}
}

func (k *Kafka) Consumer() *kafka.Consumer {
	config := &kafka.ConfigMap{
		"group.id":          time.Now().String(),
		"bootstrap.servers": k.BootstrapServers,
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	require.NoError(k.testing, err, "Create consumer")

	k.testing.Cleanup(func() { _ = consumer.Close() })

	return consumer
}

func (k *Kafka) Producer() *kafka.Producer {
	config := &kafka.ConfigMap{"bootstrap.servers": k.cluster.BootstrapServers()}

	producer, err := kafka.NewProducer(config)
	require.NoError(k.testing, err, "Create producer")

	k.testing.Cleanup(producer.Close)

	return producer
}

// KafkaConsumer is a handle to a Kafka consumer subscribed to one or more topics.
type KafkaConsumer struct {
	Consumer *kafka.Consumer
	testing  *testing.T
	topic    string
}

// MessageTimeout reads the next message, failing the test if none arrives within
// timeout.
func (c *KafkaConsumer) MessageTimeout(timeout time.Duration) *kafka.Message {
	msg, err := c.Consumer.ReadMessage(timeout)
	require.NoErrorf(c.testing, err, "Read message from topic %q", c.topic)

	return msg
}

// Message reads the next message, blocking essentially indefinitely until one
// arrives.
func (c *KafkaConsumer) Message() *kafka.Message {
	return c.MessageTimeout(1000 * time.Second)
}

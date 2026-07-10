package startup_kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	confluent "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/flachnetz/startup/v2/startup_base"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/flachnetz/startup/v2/startup_tracing"
)

// KafkaOptions provides simple flags for to create a kafka consumer or producer, and to create
// topics on startup
type KafkaOptions struct {
	// Inputs holds values that are not parsed from the command line but injected
	// by the caller before Initialize runs.
	Inputs struct {
		// Set topics to automatically create topics on startup
		Topics TopicsFunc

		// Additional configuration options to apply to all clients
		DefaultConfig kafka.ConfigMap
	}

	KafkaAddresses        []string `long:"kafka-address" env:"KAFKA_ADDRESS" validate:"dive,hostport" description:"Address of kafka server to use. Can be specified multiple times to connect to multiple brokers."`
	KafkaOffsetReset      string   `long:"kafka-offset-reset" env:"KAFKA_OFFSET_RESET" default:"smallest" description:"Offset reset for kafka topic" choice:"smallest" choice:"largest"`
	KafkaReplication      int16    `long:"kafka-replication" env:"KAFKA_REPLICATION" default:"3" description:"Default kafka replication for new topics." validate:"gt=0"`
	KafkaSecurityProtocol string   `long:"kafka-security-protocol" env:"KAFKA_SECURITY_PROTOCOL" default:"ssl" description:"Security protocol" choice:"ssl" choice:"plaintext"`
	KafkaProperties       []string `long:"kafka-property" env:"KAFKA_PROPERTY" description:"Rdkafka properties in key=value format"`

	// schema registry is not really kafka, but it is only used with kafka,
	// so I guess it is okay to put it here.
	ConfluentURL string `long:"confluent-url" default:"http://confluent-registry.shared.svc.cluster.local" env:"EVENT_SENDER_CONFLUENT_URL" description:"Confluent schema registry url."`
}

// Initialize creates the configured topics on startup. It is a no-op unless
// Inputs.Topics was set, so applications that only consume or produce do not
// need an admin connection.
func (opts *KafkaOptions) Initialize(ctx context.Context) {
	if opts.Inputs.Topics == nil {
		return
	}

	config := opts.DefaultConfig(nil)
	client, err := kafka.NewAdminClient(new(config))
	startup_base.FatalOnError(err, "Create kafka admin client")

	defer client.Close()

	err = CreateTopics(ctx, client, opts.Inputs.Topics(opts.KafkaReplication))
	startup_base.FatalOnError(err, "Initialize kafka topics")
}

// NewConsumer creates a kafka consumer using DefaultConfig, with the given
// overrides applied on top of the defaults.
func (opts *KafkaOptions) NewConsumer(consumerGroup string, overrideConfig kafka.ConfigMap) *kafka.Consumer {
	configMap := opts.DefaultConfig(overrideConfig)

	// Resolve the special RANDOM group into a unique group id once, so repeated
	// calls keep using the same generated group.
	if consumerGroup == "RANDOM" {
		consumerGroup = fmt.Sprintf("golang-%d", time.Now().UnixNano())
	}

	// set consumer group
	configMap["group.id"] = consumerGroup

	consumer, err := kafka.NewConsumer(new(configMap))
	startup_base.FatalOnError(err, "create kafka consumer failed")

	go logClient(slog.With(slog.String("kafka", "consumer")), consumer)

	return consumer
}

// NewProducer creates a kafka producer using DefaultConfig, with the given
// overrides applied on top of the defaults.
func (opts *KafkaOptions) NewProducer(overrideConfig kafka.ConfigMap) *kafka.Producer {
	configMap := opts.DefaultConfig(overrideConfig)

	producer, err := kafka.NewProducer(new(configMap))
	startup_base.FatalOnError(err, "create kafka producer failed")

	go logClient(slog.With(slog.String("kafka", "producer")), producer)

	return producer
}

// DefaultConfig builds the rdkafka config map from the parsed options. The
// overrideConfig is merged on top of the defaults, and any --kafka-property
// flags are applied last so they always win.
func (opts *KafkaOptions) DefaultConfig(overrideConfig kafka.ConfigMap) kafka.ConfigMap {
	config := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(opts.KafkaAddresses, ","),
		"auto.offset.reset": opts.KafkaOffsetReset,
		"security.protocol": opts.KafkaSecurityProtocol,

		// compress outgoing messages
		"compression.codec": "zstd",
		"compression.level": 6,

		// buffer messages locally before sending them in one batch
		"queue.buffering.max.ms":       100,
		"queue.buffering.max.messages": 100000,
		"queue.buffering.max.kbytes":   4 * 1024,

		// do not keep too many received messages in the local cache
		"queued.max.messages.kbytes": 4 * 1024,

		// this is the same that the java client uses. This way the client maps
		// the same key to the same partition as the java client does.
		"partitioner": "murmur2_random",

		// enable auto commit without auto offset tracking by default.
		// this way we can decide what offsets to commit by just calling consumer.StoreOffset
		"enable.auto.commit":       true,
		"enable.auto.offset.store": false,

		// send logs to logs channel
		"go.logs.channel.enable": true,
	}

	// extend with custom config from inputs
	maps.Copy(config, opts.Inputs.DefaultConfig)

	// extend with custom config
	maps.Copy(config, overrideConfig)

	// set values from cli
	for _, prop := range opts.KafkaProperties {
		err := config.Set(prop)
		startup_base.FatalOnError(err, "Set kafka property %q", prop)
	}

	return config
}

// ConfluentClient creates a schema registry client pointing at ConfluentURL.
func (opts *KafkaOptions) ConfluentClient() confluent.Client {
	if opts.ConfluentURL == "" {
		startup_base.FatalOnError(errors.New("url not set"), "Create confluent client")
	}

	config := confluent.NewConfig(opts.ConfluentURL)
	config.HTTPClient = startup_tracing.WithSpanPropagation(
		&http.Client{
			Timeout: 3 * time.Second,
		},
	)

	client, err := confluent.NewClient(config)
	startup_base.FatalOnError(err, "Create confluent client")

	return client
}

// CreateTopics creates the given topics, deduplicating by name and treating an
// already existing topic as success.
func CreateTopics(ctx context.Context, adminClient *kafka.AdminClient, topics Topics) error {
	var topicSpecifications []kafka.TopicSpecification

	metadata, err := adminClient.GetMetadata(nil, true, 5000)
	if err != nil {
		return fmt.Errorf("fetch metadata: %w", err)
	}

	// skip duplicate topic names so CreateTopics does not reject the request
	topicSeen := map[string]bool{}

	for _, topic := range topics {
		if topicSeen[topic.Name] {
			continue
		}

		topicSeen[topic.Name] = true

		metaTopic, exists := metadata.Topics[topic.Name]
		if exists {
			partitionCount := len(metaTopic.Partitions)

			slog.Info(
				"Kafka topic already exists",
				slog.String("topic", metaTopic.Topic),
				slog.Int("partitionCount", partitionCount),
			)

			continue
		}

		topicSpecifications = append(topicSpecifications, kafka.TopicSpecification{
			Topic:             topic.Name,
			NumPartitions:     int(topic.NumPartitions),
			ReplicationFactor: int(topic.ReplicationFactor),
			Config:            topic.Config,
		})

	}

	if len(topicSpecifications) == 0 {
		slog.Info("All kafka topics already exist", slog.Int("count", len(topics)))
		return nil
	}

	slog.Info("Creating kafka topics", slog.Int("count", len(topics)))
	results, err := adminClient.CreateTopics(ctx, topicSpecifications)

	// check results first
	for _, result := range results {
		switch result.Error.Code() {
		case kafka.ErrNoError:
			slog.Info("Kafka topic created", slog.String("topic", result.Topic))

		case kafka.ErrTopicAlreadyExists:
			slog.Info("Kafka topic already exists", slog.String("topic", result.Topic))

		default:
			slog.Warn("Failed to create topic", slog.String("topic", result.Topic), sl.Error(result.Error))
			err = errors.Join(err, fmt.Errorf("create topic %q: %w", result.Topic, result.Error))
		}
	}

	// and then fail if we have any kind of error
	if err != nil {
		return err
	}

	return nil
}

type kafkaClient interface {
	Logs() chan kafka.LogEvent
}

func logClient(log *slog.Logger, client kafkaClient) {
	for l := range client.Logs() {
		if l.Level <= 3 {
			log.Error(l.Message, slog.String("name", l.Name), slog.String("tag", l.Tag))
		} else {
			log.Info(l.Message, slog.String("name", l.Name), slog.String("tag", l.Tag))
		}
	}
}

package startup_kafka

import (
	"github.com/Shopify/sarama"
	"github.com/flachnetz/startup/lib/kafka"
	"github.com/flachnetz/startup/lib/schema"
	"github.com/flachnetz/startup/startup_base"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

var log = logrus.WithField("prefix", "kafka")

type KafkaOptions struct {
	Addresses          []string `long:"kafka-address" validate:"dive,hostport" description:"Address of kafka server to use. Can be specified multiple times to connect to multiple brokers."`
	DefaultReplication int16    `long:"kafka-replication" default:"1" validate:"min=1" description:"Default replication factor for topic creation."`

	Inputs struct {
		// You can provide an extra kafka config to override the
		// default config.
		KafkaConfig *sarama.Config

		// You could specify a topics function to automatically create topics
		// with this kafka instance
		Topics kafka.TopicsFunc
	}

	kafkaClientOnce sync.Once
	kafkaClient     sarama.Client

	schemaRegistryOnce sync.Once
	schemaRegistry     schema.Registry
}

func (opts *KafkaOptions) KafkaClient() sarama.Client {
	opts.kafkaClientOnce.Do(func() {
		config := opts.Inputs.KafkaConfig
		if config == nil {
			log.Debugf("No config supplied, using default config")
			config = defaultConfig()
		}

		kafkaClient, err := sarama.NewClient(opts.Addresses, config)
		startup_base.PanicOnError(err, "Cannot create kafka client")

		if opts.Inputs.Topics != nil {
			topics := opts.Inputs.Topics(opts.DefaultReplication)
			log.Infof("Ensure that %d topics exist", len(topics))

			err := kafka.EnsureTopics(kafkaClient, topics)
			startup_base.PanicOnError(err, "Cannot create topics on kafka broker")
		}

		opts.kafkaClient = kafkaClient
	})

	return opts.kafkaClient
}

func (opts *KafkaOptions) SchemaRegistry(topic string, replicationFactor int) schema.Registry {
	opts.schemaRegistryOnce.Do(func() {
		registry, err := kafka.NewSchemaRegistry(opts.KafkaClient(), topic, int16(replicationFactor))
		startup_base.PanicOnError(err, "Cannot create kafka based schema registry")

		opts.schemaRegistry = registry
	})

	return opts.schemaRegistry
}

func defaultConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Net.MaxOpenRequests = 16
	config.Net.DialTimeout = 10 * time.Second
	config.Consumer.MaxWaitTime = 8 * time.Second
	config.Consumer.Fetch.Min = 1024
	config.Consumer.Return.Errors = true
	config.ChannelBufferSize = 4

	config.Version = sarama.V1_1_0_0
	return config
}

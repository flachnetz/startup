package startup_schema

import (
	"github.com/flachnetz/startup"
	"github.com/flachnetz/startup/lib/schema"
	"github.com/flachnetz/startup/startup_consul"
	"github.com/flachnetz/startup/startup_kafka"
	"github.com/sirupsen/logrus"
	"sync"
)

var log = logrus.WithField("prefix", "schema-registry")

type SchemaRegistryOptions struct {
	SchemaBackend string `long:"schema-backend" default:"consul" description:"Avro schema registry to use. Can be 'consul', 'kafka' or 'noop'."`

	Kafka struct {
		Topic      string `long:"schema-kafka-topic" default:"avro_schema" description:"Topic to write schema descriptions to"`
		ReplFactor int    `long:"schema-kafka-replication" default:"1" validate:"min=1" description:"Replication factor for kafka topic when kafka is used as backend."`
	}

	registryOnce sync.Once
	registry     schema.Registry
}

func (opts *SchemaRegistryOptions) SchemaRegistry() schema.Registry {
	return opts.registry
}

func (opts *SchemaRegistryOptions) Initialize(kafka *startup_kafka.KafkaOptions, consul *startup_consul.ConsulOptions) {
	opts.registryOnce.Do(func() {
		log.Infof("Using schema registry backend: %s", opts.SchemaBackend)

		switch opts.SchemaBackend {
		case "noop":
			opts.registry = schema.NewNoopRegistry()

		case "consul":
			opts.registry = consul.SchemaRegistry()

		case "kafka":
			opts.registry = kafka.SchemaRegistry(opts.Kafka.Topic, opts.Kafka.ReplFactor)

		default:
			startup.Panicf("Invalid option given for schema backend type: %s", opts.SchemaBackend)
		}
	})
}

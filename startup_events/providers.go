package startup_events

import (
	"github.com/Landoop/schema-registry"
	"github.com/Shopify/sarama"
	"github.com/flachnetz/startup/v2/lib/schema"
)

type KafkaClientProvider interface {
	KafkaClient() sarama.Client
}

type SchemaRegistryProvider interface {
	SchemaRegistry() schema.Registry
}

type ConfluentClientProvider interface {
	ConfluentClient() *schemaregistry.Client
}

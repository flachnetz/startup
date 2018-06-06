package startup_events

import (
	"github.com/Shopify/sarama"
	"github.com/flachnetz/startup/lib/schema"
)

type KafkaClientProvider interface {
	KafkaClient() sarama.Client
}

type SchemaRegistryProvider interface {
	SchemaRegistry() schema.Registry
}

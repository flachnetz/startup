package startup_events

import (
	"github.com/Landoop/schema-registry"

	"github.com/flachnetz/startup/v2/lib/schema"
)

type SchemaRegistryProvider interface {
	SchemaRegistry() schema.Registry
}

type ConfluentClientProvider interface {
	ConfluentClient() *schemaregistry.Client
}

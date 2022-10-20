package startup_schema

import (
	"sync"

	confluent "github.com/Landoop/schema-registry"
	"github.com/flachnetz/startup/v2"
	"github.com/flachnetz/startup/v2/lib/schema"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_consul"
	"github.com/sirupsen/logrus"
)

var log = logrus.WithField("prefix", "schema-registry")

// Deprecated:
// Stop using this and use the more recent startup_event thingy.
type SchemaRegistryOptions struct {
	SchemaBackend string `long:"schema-backend" default:"consul" description:"Avro schema registry to use. Can be 'consul' or 'noop'."`

	registryOnce sync.Once
	registry     schema.Registry
}

func (opts *SchemaRegistryOptions) SchemaRegistry() schema.Registry {
	return opts.registry
}

func (opts *SchemaRegistryOptions) Initialize(consul *startup_consul.ConsulOptions) {
	opts.registryOnce.Do(func() {
		log.Infof("Using schema registry backend: %s", opts.SchemaBackend)

		switch opts.SchemaBackend {
		case "noop":
			opts.registry = schema.NewNoopRegistry()

		case "consul":
			opts.registry = consul.SchemaRegistry()

		default:
			startup_base.Panicf("Invalid option given for schema backend type: %s", opts.SchemaBackend)
		}
	})
}

type ConfluentClientOptions struct {
	ConfluentUrl startup.URL `long:"confluent-url" default:"http://confluent-registry.shared.svc.cluster.local" description:"URL to the confluent schema registry."`

	clientOnce sync.Once
	client     *confluent.Client
}

func (opts *ConfluentClientOptions) ConfluentClient(options ...confluent.Option) *confluent.Client {
	opts.clientOnce.Do(func() {
		client, err := confluent.NewClient(opts.ConfluentUrl.String(), options...)
		startup_base.PanicOnError(err, "Cannot initialize confluent client.")

		opts.client = client
	})

	return opts.client
}

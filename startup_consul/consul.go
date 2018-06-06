package startup_consul

import (
	"github.com/flachnetz/startup"
	"github.com/flachnetz/startup/lib/schema"
	consul "github.com/hashicorp/consul/api"
	"github.com/sirupsen/logrus"
	"sync"
)

type ConsulOptions struct {
	Consul string `long:"consul" default:"localhost:8500" validate:"hostport" description:"Consul address. Defaults to localhost:8500."`
	SSL    bool   `long:"consul-ssl" description:"Use https to connect to the consul api."`
	DC     string `long:"consul-datacenter" description:"Override the default consul datacenter to query."`

	clientOnce sync.Once
	client     *consul.Client

	registryOnce sync.Once
	registry     schema.Registry
}

func (opts *ConsulOptions) ConsulClient() *consul.Client {
	opts.clientOnce.Do(func() {
		consulConfig := consul.DefaultConfig()

		consulConfig.Address = opts.Consul

		if opts.SSL {
			consulConfig.Scheme = "https"
		}

		if opts.DC != "" {
			consulConfig.Datacenter = opts.DC
		}

		consulClient, err := consul.NewClient(consulConfig)
		startup.PanicOnError(err, "Could not create consul client")

		opts.client = consulClient
	})

	return opts.client
}

func (opts *ConsulOptions) SchemaRegistry() schema.Registry {
	opts.registryOnce.Do(func() {
		opts.registry = schema.NewCachedRegistry(&consulSchemaRegistry{
			consul: opts.ConsulClient(),
			log:    logrus.WithField("prefix", "schema-registry"),
		})
	})

	return opts.registry
}

package startup_events

import (
	"github.com/Shopify/sarama"
	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/lib/kafka"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

var log = logrus.WithField("prefix", "events")

type EventOptions struct {
	EventSenderConfig string `long:"event-sender" default:"" description:"Event sender to use. Event sender type followed by arguments, e.g: confluent,address=http://confluent-registry.shared.svc.cluster.local,kafka=kafka.kafka.svc.cluster.local:9092,replication=1,blocking=true"`
	DisableTls        bool   `long:"event-sender-disable-tls" description:"Do not enable tls."`

	Inputs struct {
		// A function to create the event topics. This option must be specified.
		Topics events.TopicsFunc `validate:"required"`

		// optional kafka config to use with the kafka events producer
		KafkaConfig *sarama.Config
	}

	eventSenderOnce sync.Once
	eventSender     events.EventSender
}

func (opts *EventOptions) EventSender() events.EventSender {
	opts.eventSenderOnce.Do(func() {
		config := opts.Inputs.KafkaConfig
		if config == nil {
			log.Debugf("No kafka config supplied, using default config")
			config = defaultConfig()
		}

		config.Net.TLS.Enable = !opts.DisableTls
		providers := events.Providers{
			Kafka:  kafkaClientProvider{config},
			Topics: opts.Inputs.Topics,
		}

		eventSender, err := events.ParseEventSenders(providers, opts.EventSenderConfig)
		startup_base.PanicOnError(err, "initialize event sender")

		// register as global event sender
		events.Events = eventSender

		opts.eventSender = eventSender
	})

	return opts.eventSender
}

type kafkaClientProvider struct {
	config *sarama.Config
}

func (p kafkaClientProvider) KafkaClient(addresses []string) (sarama.Client, error) {
	config := p.config
	if config == nil {
		log.Debugf("No kafka config supplied, using default config")
		config = kafka.DefaultConfig()
	}

	return sarama.NewClient(addresses, config)
}

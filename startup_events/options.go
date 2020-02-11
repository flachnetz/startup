package startup_events

import (
	"github.com/Shopify/sarama"
	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

var log = logrus.WithField("prefix", "events")

type EventOptions struct {
	EventSenderConfig string `long:"event-sender" default:"" description:"Event sender to use. Event sender type followed by arguments, e.g: consul,address=<consul address>,kafka=<kafka address>"`

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
		providers := events.Providers{
			Kafka:  kafkaClientProvider{opts.Inputs.KafkaConfig},
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
		config = defaultConfig()
	}

	return sarama.NewClient(addresses, config)
}

func defaultConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Net.MaxOpenRequests = 16
	config.Net.DialTimeout = 10 * time.Second
	config.Producer.Timeout = 3 * time.Second
	config.Producer.Retry.Max = 16
	config.Producer.Retry.Backoff = 250 * time.Millisecond
	config.ChannelBufferSize = 4

	config.Version = sarama.V1_1_1_0

	return config
}

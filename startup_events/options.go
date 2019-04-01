package startup_events

import (
	"github.com/flachnetz/startup/lib/events"
	"github.com/flachnetz/startup/startup_base"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"sync"
)

var log = logrus.WithField("prefix", "events")

type EventOptions struct {
	Sender string `long:"event-sender" default:"none" description:"Event sender to use. Can be 'none', 'stdout', 'file:name.gz' or 'kafka'."`

	KafkaEncoder           string `long:"event-kafka-encoder" default:"avro" description:"Event encoder to use with kafka. Valid options are 'json', 'avro' and 'confluent'."`
	KafkaReplicationFactor int16  `long:"event-kafka-replication-factor" validate:"min=1" default:"1" description:"Replication factor for kafka topics."`

	Inputs struct {
		// Enable blocking. This should normally not be used for production and only
		// be used in batch cli tools
		KafkaBlocking bool

		// A function to create the event topics. This option must be specified.
		Topics events.TopicsFunc `validate:"required"`
	}

	eventSenderOnce sync.Once
	eventSender     events.EventSender
}

func (opts *EventOptions) EventSender(kafkaClient KafkaClientProvider, registry SchemaRegistryProvider, confluent ConfluentClientProvider) events.EventSender {
	opts.eventSenderOnce.Do(func() {
		eventSender := opts.newEventSender(kafkaClient, registry, confluent)

		// register as global event sender
		events.Events = eventSender

		opts.eventSender = eventSender
	})

	return opts.eventSender
}

func (opts *EventOptions) newEventSender(kafkaClient KafkaClientProvider, registry SchemaRegistryProvider, confluentClient ConfluentClientProvider) events.EventSender {
	topics := opts.Inputs.Topics(opts.KafkaReplicationFactor)
	if topics.Fallback == "" {
		startup_base.Panicf("Cannot create kafka event sender: no fallback topic was specified.")
	}

	log.Infof("Using event sender %s", opts.Sender)

	switch opts.Sender {
	case "none":
		return &events.NoopEventSender{}

	case "logrus", "logging":
		return events.LogrusEventSender{FieldLogger: logrus.WithField("prefix", "events")}

	case "stdout":
		return nopCloser{events.WriterEventSender{WriteCloser: os.Stdout}}

	case "kafka":

		var encoder events.Encoder

		switch opts.KafkaEncoder {
		case "json":
			encoder = events.NewJSONEncoder()

		case "avro":
			encoder = events.NewAvroEncoder(registry.SchemaRegistry())

		case "confluent":
			encoder = events.NewAvroConfluentEncoder(confluentClient.ConfluentClient())

		default:
			startup_base.Errorf("Invalid event encoder specified: %s", opts.KafkaEncoder)
		}

		kafkaConfig := events.KafkaSenderConfig{
			Encoder:       encoder,
			TopicsConfig:  topics,
			AllowBlocking: opts.Inputs.KafkaBlocking,
		}

		kafkaSender, err := events.NewKafkaSender(kafkaClient.KafkaClient(), kafkaConfig)
		startup_base.PanicOnError(err, "Cannot create kafka event sender")

		log.Info("Event sender for kafka initialized")
		return kafkaSender

	default:
		if strings.HasPrefix(opts.Sender, "file:") {
			filename := strings.TrimPrefix(opts.Sender, "file:")
			sender, err := events.GZIPEventSender(filename)
			startup_base.PanicOnError(err, "Could not open events file")

			return sender
		}

		panic(startup_base.Errorf("Invalid option given for event sender type: %s", opts.Sender))
	}
}

type nopCloser struct {
	events.EventSender
}

func (nopCloser) Close() error {
	return nil
}

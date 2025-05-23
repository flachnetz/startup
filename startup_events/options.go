package startup_events

import (
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"

	confluent "github.com/Landoop/schema-registry"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/flachnetz/startup/v2/startup_tracing"
	"github.com/pkg/errors"

	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/startup_base"
)

type EventOptions struct {
	ConfluentURL string `long:"event-sender-confluent-url" default:"http://confluent-registry.shared.svc.cluster.local" description:"Confluent schema registry url."`

	Async struct {
		Kafka struct {
			Addr        string   `long:"event-sender-kafka-addr" default:"kafka.shared.svc.cluster.local:9093" description:"Kafka bootstrap hosts"`
			DisableTLS  bool     `long:"event-sender-kafka-disable-tls" description:"Disable TLS, might simplify local testing"`
			Replication int16    `long:"event-sender-kafka-replication-factor" default:"3" description:"Replication factor to use when creating kafka topics"`
			Properties  []string `long:"event-sender-kafka-properties" description:"Pairs of key=value containing standard librdkafka configuration properties as documented in: https://github.com/edenhill/librdkafka/tree/master/CONFIGURATION.md"`
		}

		BufferSize uint `long:"event-sender-async-buffer-size" default:"1024" description:"Number of elements to buffer in async event sender. If the buffer is full, new events will be discarded."`
	}

	WriteToFile string `long:"event-sender-file" description:"File to write all events to. Sender will be encoded as json"`

	Inputs struct {
		// A function to create the event topics. This option must be specified.
		Topics events.TopicsFunc `validate:"required"`
	}

	eventSenderOnce sync.Once
	eventSender     events.EventSender
}

func (opts *EventOptions) EventSender(clientId string) events.EventSender {
	opts.eventSenderOnce.Do(func() {
		eventSender, err := initializeEventSender(opts, clientId)
		startup_base.FatalOnError(err, "initialize event sender")

		// register as global event sender
		events.Sender = eventSender

		opts.eventSender = eventSender
	})

	return opts.eventSender
}

func initializeEventSender(opts *EventOptions, clientId string) (events.EventSender, error) {
	confluentClient, err := confluentClient(opts.ConfluentURL)
	if err != nil {
		return nil, errors.WithMessage(err, "confluent registry client")
	}

	kafkaSender, err := kafkaSender(opts, clientId)
	if err != nil {
		return nil, errors.WithMessage(err, "kafka client")
	}

	fileSender, err := fileSender(opts.WriteToFile)
	if err != nil {
		if kafkaSender != nil {
			kafkaSender.Close()
		}

		return nil, errors.WithMessage(err, "file sender")
	}

	// build list of topics parameterized with the replication factor that we
	// would like to have now.
	eventTopics := opts.Inputs.Topics(opts.Async.Kafka.Replication)

	// buffer size for async event queue
	bufferSize := opts.Async.BufferSize

	eventSenderInitializer, err := events.NewInitializer(
		confluentClient,
		kafkaSender,
		fileSender,
		eventTopics,
		bufferSize,
	)
	if err != nil {
		if kafkaSender != nil {
			kafkaSender.Close()
		}

		if fileSender != nil {
			_ = fileSender.Close()
		}

		return nil, errors.WithMessage(err, "initialize event sender")
	}

	defer startup_base.Close(
		eventSenderInitializer, "cleanup event sender initializer",
	)

	return eventSenderInitializer.Initialize()
}

func fileSender(file string) (io.WriteCloser, error) {
	if file == "" {
		return nil, nil
	}

	return os.OpenFile(file, os.O_CREATE|os.O_WRONLY, 0o644)
}

func kafkaSender(opts *EventOptions, clientId string) (*kafka.Producer, error) {
	if opts.Async.Kafka.Addr == "" {
		return nil, nil
	}

	isCommaOrSpace := func(ch rune) bool {
		return unicode.IsSpace(ch) || ch == ','
	}

	// split by spaces or commas
	bootstrapServers := strings.FieldsFunc(opts.Async.Kafka.Addr, isCommaOrSpace)
	kafkaConfig := kafka.ConfigMap{
		"client.id":         clientId,
		"bootstrap.servers": strings.Join(bootstrapServers, ","),
		"compression.codec": "gzip",
		"compression.level": "6",

		// buffer messages locally before sending them in one batch
		"queue.buffering.max.ms": "200",

		// this is the same that the java client uses. This way the client maps
		// the same key to the same partition as the java client does.
		"partitioner": "murmur2_random",
	}

	// enable or disable ssl
	if opts.Async.Kafka.DisableTLS {
		kafkaConfig["security.protocol"] = "plaintext"
	} else {
		kafkaConfig["security.protocol"] = "ssl"
	}

	for _, value := range opts.Async.Kafka.Properties {
		if err := kafkaConfig.Set(value); err != nil {
			return nil, errors.WithMessagef(err, "parse kafka config %q", value)
		}
	}

	kafkaClient, err := kafka.NewProducer(&kafkaConfig)
	if err != nil {
		return nil, errors.WithMessage(err, "kafka producer")
	}

	return kafkaClient, nil
}

func confluentClient(baseUrl string) (*confluent.Client, error) {
	if baseUrl == "" {
		return nil, nil
	}

	httpClient := startup_tracing.WithSpanPropagation(
		&http.Client{
			Timeout: 3 * time.Second,
		},
	)

	return confluent.NewClient(baseUrl, confluent.UsingClient(httpClient))
}

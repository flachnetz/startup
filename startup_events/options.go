package startup_events

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	confluent "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/flachnetz/startup/v2/startup_kafka"

	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/startup_base"
)

type EventOptions struct {
	AsyncBufferSize uint   `long:"event-sender-async-buffer-size" env:"EVENT_SENDER_ASYNC_BUFFER_SIZE" default:"1024" description:"Maximum number of elements to buffer in async event sender. If the buffer is full, new events will be discarded."`
	WriteToFile     string `long:"event-sender-file" env:"EVENT_SENDER_FILE" description:"File to write all events to. Sender will be encoded as json"`

	Inputs struct {
		// A function to define event mapping & existing topics
		Topics events.TopicsFunc `validate:"required"`

		// OutboxTable is the name of the outbox table
		OutboxTable string `json:"outboxTable"`
	}

	kafkaOptions startup_kafka.KafkaOptions

	eventSenderOnce sync.Once
	eventSender     events.EventSender
}

func (opts *EventOptions) Initialize(kafkaOptions startup_kafka.KafkaOptions) {
	opts.kafkaOptions = kafkaOptions
}

func (opts *EventOptions) EventSender() events.EventSender {
	opts.eventSenderOnce.Do(func() {
		eventSender, err := initializeEventSender(opts)
		startup_base.FatalOnError(err, "initialize event sender")

		// register as global event sender
		events.Sender = eventSender

		opts.eventSender = eventSender
	})

	return opts.eventSender
}

func initializeEventSender(opts *EventOptions) (events.EventSender, error) {
	var confluentClient confluent.Client
	if opts.kafkaOptions.ConfluentURL != "" {
		confluentClient = opts.kafkaOptions.ConfluentClient()
	}

	var kafkaSender *kafka.Producer
	if len(opts.kafkaOptions.KafkaAddresses) > 0 {
		kafkaSender = opts.kafkaOptions.NewProducer(nil)
	}

	fileSender, err := fileSender(opts.WriteToFile)
	if err != nil {
		if kafkaSender != nil {
			kafkaSender.Close()
		}

		return nil, fmt.Errorf("file sender: %w", err)
	}

	outboxTable := opts.Inputs.OutboxTable
	if outboxTable == "" {
		return nil, errors.New("no outbox table specified in Inputs")
	}

	// build list of topics parameterized with the replication factor that we
	// would like to have now.
	eventTopics := opts.Inputs.Topics(opts.kafkaOptions.KafkaReplication)

	// buffer size for async event queue
	bufferSize := opts.AsyncBufferSize

	eventSenderInitializer, err := events.NewInitializer(
		confluentClient,
		kafkaSender,
		fileSender,
		eventTopics,
		outboxTable,
		bufferSize,
	)
	if err != nil {
		if kafkaSender != nil {
			kafkaSender.Close()
		}

		if fileSender != nil {
			_ = fileSender.Close()
		}

		return nil, fmt.Errorf("initialize event sender: %w", err)
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

package events

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/flachnetz/startup/v2/lib/kafka"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"reflect"
	"sync"
)

var errorTopic = "event_sender_errors"

type Encoder interface {
	// Encodes an event into some kind of binary representation.
	Encode(event Event) ([]byte, error)

	// Close the encoder.
	Close() error
}

type KafkaSenderConfig struct {
	// Set to true to block Send() if the buffers are full.
	AllowBlocking bool

	// Topics configuration
	TopicsConfig EventTopics

	// The event encoder to use
	Encoder Encoder

	EventBufferSize int
}

type KafkaSender struct {
	log             logrus.FieldLogger
	events          chan Event
	eventsWg        sync.WaitGroup
	encoder         Encoder
	fallbackEncoder Encoder
	producer        sarama.AsyncProducer

	allowBlocking bool
	topicForEvent func(event Event) string
}

func NewKafkaSender(kafkaClient sarama.Client, senderConfig KafkaSenderConfig) (*KafkaSender, error) {

	topics := getTopicsWithErrorTopic(senderConfig.TopicsConfig.Topics())
	// ensure that all topics that might be used later exist
	if err := kafka.EnsureTopics(kafkaClient, topics); err != nil {
		return nil, errors.WithMessage(err, "ensure topics")
	}

	// create the producer based on the client
	producer, err := sarama.NewAsyncProducerFromClient(kafkaClient)
	if err != nil {
		return nil, errors.WithMessage(err, "create producer from client")
	}

	// just set to default
	if senderConfig.EventBufferSize <= 0 {
		senderConfig.EventBufferSize = 1024
	}

	sender := &KafkaSender{
		log:             logrus.WithField("prefix", "kafka"),
		events:          make(chan Event, senderConfig.EventBufferSize),
		encoder:         senderConfig.Encoder,
		fallbackEncoder: jsonEncoder{},
		producer:        producer,
		allowBlocking:   senderConfig.AllowBlocking,
		topicForEvent:   topicForEventFunc(senderConfig.TopicsConfig.TopicForType),
	}

	sender.eventsWg.Add(2)
	go sender.handleEvents()
	go sender.consumeErrorChannel()

	return sender, nil
}

func (kafka *KafkaSender) Init(events []Event) error {
	kafka.log.Infof("registering schemas")
	for _, ev := range events {
		if _, err := kafka.encoder.Encode(ev); err != nil {
			return errors.WithMessage(err, "init event schema")
		}
		kafka.log.Infof("registration succeeded for schema %s", ev.Schema())
	}
	return nil
}

func (kafka *KafkaSender) Send(event Event) {
	if kafka.allowBlocking {
		kafka.events <- event

	} else {
		select {
		case kafka.events <- event:
			// everything is fine

		default:
			// the channel is full
			kafka.log.Errorf("Could not enqueue event, channel is full: %v", kafka.events)
		}
	}
}

func (kafka *KafkaSender) Close() error {
	// Do not accept new events and wait for all events to be processed.
	// This stops and waits for the handleEvents() goroutine.
	close(kafka.events)
	kafka.eventsWg.Wait()

	// close the producer and wait for all kafka events to be sent
	err := kafka.producer.Close()
	return errors.WithMessage(err, "closing producer")
}

func (kafka *KafkaSender) handleEvents() {
	defer kafka.eventsWg.Done()

	for event := range kafka.events {
		// encode events to binary data
		encoded, err := kafka.encoder.Encode(event)
		if err != nil {
			kafka.handleError(err, event)
			continue
		}

		// and enqueue it for sending
		kafka.producer.Input() <- &sarama.ProducerMessage{
			Topic: kafka.topicForEvent(event),
			Value: sarama.ByteEncoder(encoded),
		}
	}
}

func (kafka *KafkaSender) handleError(err error, event Event) {
	if event != nil {
		payload, _ := json.Marshal(event)
		kafka.log.Errorf("Failed to send event %s: %s -> routing to %s topic as json", string(payload), err, errorTopic)
		kafka.producer.Input() <- &sarama.ProducerMessage{
			Topic: errorTopic,
			Value: sarama.ByteEncoder(payload),
		}
	} else {
		kafka.log.Errorf("Failed to send event: %s", err)
	}
}

func (kafka *KafkaSender) consumeErrorChannel() {
	kafka.eventsWg.Done()

	for err := range kafka.producer.Errors() {
		kafka.handleError(err, nil)
	}
}

func topicForEventFunc(topicForType func(t reflect.Type) string) func(event Event) string {
	return func(event Event) string {
		t := reflect.TypeOf(event)
		return topicForType(t)
	}
}

type TopicsFunc func(replicationFactor int16) EventTopics

type EventTopics struct {
	EventTypes       map[reflect.Type]kafka.Topic
	SchemaInitEvents []Event
	FailOnSchemaInit bool

	// This is the fallback topic if a type can not be matched to one of the event types.
	// It will be created automatically.
	Fallback string
}

func (topics EventTopics) TopicForType(t reflect.Type) string {
	if topic, ok := topics.EventTypes[t]; ok {
		return topic.Name
	}

	log.Warnf("Got event with unknown type %s, using fallback topic %s.",
		t.String(), topics.Fallback)

	return topics.Fallback
}

func (topics EventTopics) Topics() kafka.Topics {
	var result kafka.Topics

	for _, topic := range topics.EventTypes {
		result = append(result, topic)
	}

	return result
}

func getTopicsWithErrorTopic(topics kafka.Topics) kafka.Topics {
	var replication int16 = 1
	// get highest replication for error topic
	for _, v := range topics {
		if v.ReplicationFactor > replication {
			replication = v.ReplicationFactor
		}
	}
	return append(topics, kafka.Topic{
		Name:              errorTopic,
		NumPartitions:     9,
		ReplicationFactor: replication,
	})
}

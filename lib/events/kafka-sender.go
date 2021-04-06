package events

import (
	"context"
	kafka2 "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sync"
	"time"

	"github.com/flachnetz/startup/v2/lib/kafka"
	"github.com/flachnetz/startup/v2/startup_base"
)


type RecordHeader struct {
	Key   []byte
	Value []byte
}

type KafkaMessage struct {
	Event
	Key     string
	Headers []RecordHeader
}

func ToKafkaEvent(key string, ev Event) *KafkaMessage {
	return &KafkaMessage{
		Event: ev,
		Key:   key,
	}
}


type KafkaConfluentSender struct {
	log             logrus.FieldLogger
	events          chan Event
	eventsWg        sync.WaitGroup
	encoder         Encoder
	fallbackEncoder Encoder
	kafkaProducer   *kafka2.Producer

	allowBlocking bool
	topicForEvent func(event Event) string
}

func NewKafkaConfluentSender(producer *kafka2.Producer, senderConfig KafkaSenderConfig) (*KafkaConfluentSender, error) {

	// just set to default
	if senderConfig.EventBufferSize <= 0 {
		senderConfig.EventBufferSize = 1024
	}

	sender := &KafkaConfluentSender{
		log:             logrus.WithField("prefix", "kafka"),
		events:          make(chan Event, senderConfig.EventBufferSize),
		encoder:         senderConfig.Encoder,
		fallbackEncoder: jsonEncoder{},
		kafkaProducer:   producer,
		allowBlocking:   senderConfig.AllowBlocking,
		topicForEvent:   topicForEventFunc(senderConfig.TopicsConfig.TopicForType),
	}
	topics := getTopicsWithErrorTopic(senderConfig.TopicsConfig.Topics())
	// ensure that all topics that might be used later exist
	if err := sender.CreateTopics(topics); err != nil {
		return nil, errors.WithMessage(err, "ensure topics")
	}

	sender.eventsWg.Add(1)
	go sender.handleEvents()

	return sender, nil
}

func (s *KafkaConfluentSender) CreateTopics(topics kafka.Topics) error {
	if len(topics) == 0 {
		return nil
	}

	adminClient, err := kafka2.NewAdminClientFromProducer(s.kafkaProducer)
	startup_base.FatalOnError(err, "admin client")
	defer adminClient.Close()

	for _, topic := range topics {
		res, err := adminClient.CreateTopics(context.Background(), []kafka2.TopicSpecification{{
			Topic:             topic.Name,
			NumPartitions:     int(topic.NumPartitions),
			ReplicationFactor: int(topic.ReplicationFactor),
		}})
		if err != nil {
			return errors.Wrap(err, "topic creation")
		}
		if len(res) != 1 || res[0].Error.Code() != kafka2.ErrNoError && res[0].Error.Code() != kafka2.ErrTopicAlreadyExists {
			return errors.Errorf("topic creation failed: %+v", res)
		}
	}

	return nil
}

func (s *KafkaConfluentSender) Init(events []Event) error {
	s.log.Infof("registering schemas")
	for _, ev := range events {
		if _, err := s.encoder.Encode(ev); err != nil {
			return errors.WithMessage(err, "init event schema")
		}
		s.log.Infof("registration succeeded for schema %s", ev.Schema())
	}
	return nil
}

func (s *KafkaConfluentSender) Send(event Event) {
	if s.allowBlocking {
		s.events <- event

	} else {
		select {
		case s.events <- event:
			// everything is fine

		default:
			// the channel is full
			s.log.Errorf("Could not enqueue event, channel is full: %v", s.events)
		}
	}
}

func (s *KafkaConfluentSender) Close() error {
	// Do not accept new events and wait for all events to be processed.
	// This stops and waits for the handleEvents() goroutine.
	close(s.events)
	s.eventsWg.Wait()

	s.kafkaProducer.Flush(15 * 1000)
	s.kafkaProducer.Close()
	return nil
}

func (s *KafkaConfluentSender) handleEvents() {
	defer s.eventsWg.Done()

	for event := range s.events {
		// encode events to binary data
		encoded, err := s.encoder.Encode(event)
		if err != nil {
			s.log.Errorf("encoding of message %+v failed: %s", event, err.Error())
			continue
		}

		topicForEvent := ""

		var headers []kafka2.Header
		var key []byte
		if msg, ok := event.(*KafkaMessage); ok {
			if msg.Key != "" {
				key = []byte(msg.Key)
			}
			for _, h := range msg.Headers {
				headers = append(headers, kafka2.Header{Key: string(h.Key), Value: h.Value})
			}
			topicForEvent = s.topicForEvent(msg.Event)
		} else {
			topicForEvent = s.topicForEvent(event)
		}
		select {
		case <-time.After(5 * time.Second):
			s.log.Errorf("sending of message %+v timed out", event)
			break
		default:
			syncChan := make(chan kafka2.Event, 1)
			err := s.kafkaProducer.Produce(&kafka2.Message{
				TopicPartition: kafka2.TopicPartition{
					Topic:     &topicForEvent,
					Partition: kafka2.PartitionAny,
				},
				Value:   encoded,
				Key:     key,
				Headers: headers,
			}, syncChan)
			if err != nil {
				s.log.Errorf("cannot send message %+v: %s", event, err.Error())
			}
			<-syncChan
		}
	}
}

package events

import (
	"context"
	"fmt"
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
	eventBufferSize int
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
		eventBufferSize: senderConfig.EventBufferSize,
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
	s.log.Infof("trying to create topics %+v", topics)
	if len(topics) == 0 {
		return nil
	}

	adminClient, err := kafka2.NewAdminClientFromProducer(s.kafkaProducer)
	startup_base.FatalOnError(err, "admin client")
	defer adminClient.Close()

	for _, topic := range topics {
		config := map[string]string{}
		for k, v := range topic.Config {
			if v != nil {
				config[k] = *v
			}
		}
		res, err := adminClient.CreateTopics(context.Background(), []kafka2.TopicSpecification{{
			Topic:             topic.Name,
			NumPartitions:     int(topic.NumPartitions),
			ReplicationFactor: int(topic.ReplicationFactor),
			Config:            config,
		}})
		if err != nil {
			return errors.Wrap(err, "topic creation")
		}
		if len(res) != 1 || res[0].Error.Code() != kafka2.ErrNoError && res[0].Error.Code() != kafka2.ErrTopicAlreadyExists {
			return errors.Errorf("topic creation failed: %+v", res)
		} else {
			s.log.Infof("Topics created command returned with %+v", res)
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
			s.log.Errorf("Could not enqueue event, channel size of %d reached", s.eventBufferSize)
		}
	}
}

func (s *KafkaConfluentSender) SendBlocking(event Event) error {
	msg, err := s.buildKafkaMsg(event)
	if err != nil {
		return err
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case <-timer.C:
		errorMsg := fmt.Sprintf("sending of message %+v timed out", event)
		return errors.New(errorMsg)
	default:
		syncChan := make(chan kafka2.Event, 1)
		err := s.kafkaProducer.Produce(msg, syncChan)
		if err != nil {
			errorMsg := fmt.Sprintf("cannot send message %+v: %s", event, err.Error())
			return errors.New(errorMsg)
		}
		<-syncChan
		return msg.TopicPartition.Error
	}
}

func (s *KafkaConfluentSender) buildKafkaMsg(event Event) (*kafka2.Message, error) {
	encoded, err := s.encoder.Encode(event)
	if err != nil {
		errorMsg := fmt.Sprintf("encoding of message %+v failed: %s", event, err.Error())
		return nil, errors.New(errorMsg)
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
	msg := &kafka2.Message{
		TopicPartition: kafka2.TopicPartition{
			Topic:     &topicForEvent,
			Partition: kafka2.PartitionAny,
		},
		Value:   encoded,
		Key:     key,
		Headers: headers,
	}
	return msg, nil
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
		if err := s.SendBlocking(event); err != nil {
			s.log.Errorf("Failed to sent event %+w to kafka: %s", event, err)
		}
	}
}

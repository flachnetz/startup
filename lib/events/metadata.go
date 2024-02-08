package events

import (
	"reflect"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pkg/errors"
)

type EventHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type EventMetadata struct {
	Type    reflect.Type
	Topic   string
	Key     *string
	Headers EventHeaders
}

func (topics *NormalizedEventTypes) MetadataOf(event Event) (*EventMetadata, error) {
	var key *string
	var headers EventHeaders

	if msg, ok := event.(*KafkaEvent); ok {
		key = &msg.Key
		headers = msg.Headers

		// unwrap the actual event
		event = msg.Event
	}

	// now we can get the actual event type
	eventType := derefEventType(reflect.TypeOf(event))

	// get the topic of the actual event
	topic, err := topics.TopicForType(eventType)
	if err != nil {
		return nil, errors.WithMessage(err, "lookup event type")
	}

	// build metadata object for event
	meta := &EventMetadata{
		Type:    eventType,
		Topic:   topic,
		Key:     key,
		Headers: headers,
	}

	return meta, nil
}

type EventHeaders []EventHeader

func (headers EventHeaders) ToKafka() []kafka.Header {
	var result []kafka.Header

	for _, header := range headers {
		result = append(result, kafka.Header{
			Key:   header.Key,
			Value: []byte(header.Value),
		})
	}

	return result
}

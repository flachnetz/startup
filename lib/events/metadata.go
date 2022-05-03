package events

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"reflect"
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

		for _, h := range msg.Headers {
			headers = append(headers, EventHeader{
				Key:   string(h.Key),
				Value: string(h.Value),
			})
		}

		// unwrap the actual event
		event = msg
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

func (headers EventHeaders) ToJSON() []byte {
	if len(headers) == 0 {
		return nil
	}

	bytes, _ := json.Marshal(headers)
	return bytes
}

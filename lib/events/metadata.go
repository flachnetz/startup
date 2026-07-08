package events

import (
	"fmt"
	"reflect"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
	var topic string

	if ev, ok := asEventType[*KafkaEvent](event); ok {
		key = &ev.Key
		headers = ev.Headers
		topic = ev.Topic

		// unwrap the actual event
		event = ev.Event
	}

	if ev, ok := asEventType[*eventWithTraceContext](event); ok {
		for key, value := range ev.TraceContext {
			headers = append(headers, EventHeader{
				Key:   key,
				Value: value,
			})
		}
	}

	// now we can get the actual event type
	eventType := derefEventType(reflect.TypeOf(unwrap(event)))

	if topic == "" {
		var err error

		// get the topic of the actual event
		topic, err = topics.TopicForType(eventType)
		if err != nil {
			return nil, fmt.Errorf("lookup event type: %w", err)
		}
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

func unwrap(event Event) Event {
	type unwrapper interface {
		Unwrap() Event
	}

	for {
		if unwrapper, ok := event.(unwrapper); ok {
			event = unwrapper.Unwrap()
			continue
		}

		return event
	}
}

func asEventType[T Event](event Event) (T, bool) {
	type unwrapper interface {
		Unwrap() Event
	}

	for {
		eventT, ok := event.(T)
		if ok {
			return eventT, true
		}

		if unwrapper, ok := event.(unwrapper); ok {
			event = unwrapper.Unwrap()
			continue
		}

		var tZero T
		return tZero, false
	}
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

package events

import (
	"github.com/flachnetz/startup/v2/lib/kafka"
	"github.com/pkg/errors"
	"reflect"
)

// TopicsFunc builds an EventTopics instance for the given kafka replication factor.
type TopicsFunc func(replicationFactor int16) EventTopics

// EventTopics contains a mapping from event struct type to a kafka topic.
// This map must contain all event types that are going to be send. If this misses an event,
// sending that event will fail.
type EventTopics struct {
	EventTypes map[reflect.Type]kafka.Topic
}

func (topics *EventTopics) Topics() kafka.Topics {
	var result kafka.Topics

	for _, topic := range topics.EventTypes {
		result = append(result, topic)
	}

	return result
}

type NormalizedEventTypes struct {
	EventTopics
}

// Normalized returns a Normalized EventTopics instance where the event types point
// directly to the events struct type and not to any kind of pointer.
// This ways we prevent issues with pointers to event types not getting matched to a topic.
func (topics *EventTopics) Normalized() (*NormalizedEventTypes, error) {
	normalizedTypes := map[reflect.Type]kafka.Topic{}

	for eventType, kafkaTopic := range topics.EventTypes {
		eventType = derefEventType(eventType)

		if eventType.Kind() != reflect.Struct {
			return nil, errors.Errorf("invalid event type: '%s'", eventType)
		}

		normalizedTypes[eventType] = kafkaTopic
	}

	normalized := EventTopics{EventTypes: normalizedTypes}
	return &NormalizedEventTypes{normalized}, nil
}

func (topics *NormalizedEventTypes) TopicForType(eventType reflect.Type) (string, error) {
	eventType = derefEventType(eventType)

	if topic, ok := topics.EventTypes[eventType]; ok {
		return topic.Name, nil
	}

	return "", errors.Errorf("no topic found for event type %s", eventType)
}

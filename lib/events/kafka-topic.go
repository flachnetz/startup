package events

import (
	"reflect"

	"github.com/flachnetz/startup/v2/lib/kafka"
)

var errorTopic = "event_sender_errors"

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

func (topics *EventTopics) Topics() kafka.Topics {
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

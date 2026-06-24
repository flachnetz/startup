package events

type KafkaEvent struct {
	Event

	// If defined, this will override the normal topic selection. You need to ensure that
	// the topic exists or can be auto created.
	Topic string

	Key     string
	Headers []EventHeader
}

func (e *KafkaEvent) Unwrap() Event {
	return e.Event
}

func WithKey(ev Event, key string, headers ...EventHeader) *KafkaEvent {
	return WithKeyAndTopic(ev, key, "", headers...)
}

func WithKeyAndTopic(ev Event, key, topic string, headers ...EventHeader) *KafkaEvent {
	if msg, ok := ev.(*KafkaEvent); ok {
		msg.Key = key
		msg.Topic = topic
		msg.Headers = append(msg.Headers, headers...)
		return msg
	}

	return &KafkaEvent{
		Event:   ev,
		Key:     key,
		Topic:   topic,
		Headers: append(EventHeaders(nil), headers...),
	}
}

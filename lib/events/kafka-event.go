package events

type KafkaEvent struct {
	Event
	Key     string
	Headers []EventHeader
}

func ToKafkaEvent(key string, ev Event) *KafkaEvent {
	return WithKey(ev, key)
}

func WithKey(ev Event, key string, headers ...EventHeader) *KafkaEvent {
	if msg, ok := ev.(*KafkaEvent); ok {
		msg.Key = key
		msg.Headers = append(msg.Headers, headers...)
		return msg
	}

	return &KafkaEvent{
		Event:   ev,
		Key:     key,
		Headers: append(EventHeaders(nil), headers...),
	}
}

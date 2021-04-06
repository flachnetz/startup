package startup_events

import (
	"sync"

	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/startup_base"
)

type EventOptions struct {
	EventSenderConfig string `long:"event-sender" default:"" description:"Event sender to use. Event sender type followed by arguments, e.g: confluent,address=http://confluent-registry.shared.svc.cluster.local,kafka=kafka.kafka.svc.cluster.local:9092,replication=1,blocking=true"`
	DisableTls        bool   `long:"event-sender-disable-tls" description:"Do not enable tls."`

	Inputs struct {
		// A function to create the event topics. This option must be specified.
		Topics events.TopicsFunc `validate:"required"`
	}

	eventSenderOnce sync.Once
	eventSender     events.EventSender
}

func (opts *EventOptions) EventSender(clientId string) events.EventSender {
	opts.eventSenderOnce.Do(func() {
		eventSender, err := events.ParseEventSenders(clientId, opts.Inputs.Topics, opts.EventSenderConfig, opts.DisableTls)
		startup_base.PanicOnError(err, "initialize event sender")

		// register as global event sender
		events.Events = eventSender

		opts.eventSender = eventSender
	})

	return opts.eventSender
}

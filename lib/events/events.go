package events

import (
	"github.com/sirupsen/logrus"
	"io"
)

var log = logrus.WithField("prefix", "events")

type Event interface {
	// Returns the avro schema of this event
	Schema() string

	// Writes the event (in avro format) to the given writer.
	Serialize(io.Writer) error
}

type EventSender interface {
	// Send the given event. This method should be non blocking and
	// must never fail. You might want to use a channel for buffering
	// events internally. Errors will be logged to the terminal
	// but otherwise ignored.
	Send(event Event)

	// Close the event sender and flush all pending events.
	// Waits for all events to be send out.
	Close() error
}

// Global instance to send events. Defaults to a simple sender that prints
// events using a logger instance.
var Events EventSender = LogrusEventSender{logrus.WithField("prefix", "events")}

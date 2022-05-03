package events

import (
	"context"
	"database/sql"
	"github.com/sirupsen/logrus"
	"io"
	"time"
)

func TimeToEventTimestamp(ts time.Time) int64 {
	return ts.UnixNano() / int64(time.Millisecond)
}

var log = logrus.WithField("prefix", "events")

type Event interface {
	// Schema returns the avro schema of this event
	Schema() string

	// Serialize writes the event (in avro format) to the given writer.
	Serialize(io.Writer) error
}

type EventSender interface {
	// SendAsync sends the given event. This method should be non blocking and
	// must never fail. You might want to use a channel for buffering
	// events internally. Errors will be logged to the terminal
	// but otherwise ignored.
	SendAsync(event Event)

	// SendInTx sends the message in the transaction.
	// Returns an error, if sending fails.
	SendInTx(ctx context.Context, tx *sql.Tx, event Event) error

	// Close the event sender and flush all pending events.
	// Waits for all events to be send out.
	Close() error
}

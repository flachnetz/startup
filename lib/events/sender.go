package events

import (
	"context"
	"io"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
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
	// must never fail. If sending would block, you are allowed to discard the event.
	// Errors will be logged to the terminal but otherwise ignored.
	SendAsync(ctx context.Context, event Event)

	// SendAsyncCh returns a channel you can use to enqueue events to the sender. Sending events on this
	// channel will give you no delivery guarantees for those events.
	SendAsyncCh() chan<- Event

	// SendInTx sends the message in the transaction.
	// Returns an error, if sending fails.
	SendInTx(ctx context.Context, tx sqlx.ExecerContext, event Event) error

	// Close the event sender and flush all pending events.
	// Waits for all events to be send out.
	Close() error
}

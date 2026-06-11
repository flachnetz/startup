package events

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
)

func TimeToEventTimestamp(ts time.Time) int64 {
	return ts.UnixNano() / int64(time.Millisecond)
}

func FromEventTimestamp(timestamp int64) time.Time {
	return time.Unix(0, timestamp*int64(time.Millisecond))
}

var log = slog.With(slog.String("prefix", "events"))

type Event interface {
	// Schema returns the avro schema of this event
	Schema() string

	// Serialize writes the event (in avro format) to the given writer.
	Serialize(io.Writer) error
}

type EventSender interface {
	// SendAsync sends the given event. This method should be non-blocking and
	// must never fail. If sending would block, you are allowed to discard the event.
	// Errors will be logged to the terminal but otherwise ignored.
	SendAsync(ctx context.Context, event Event)

	// SendInTx sends the message in the transaction.
	// Returns an error, if sending fails.
	SendInTx(ctx context.Context, tx sqlx.ExecerContext, event Event) error

	// Close the event sender and flush all pending events.
	// Waits for all events to be sent out.
	Close() error
}

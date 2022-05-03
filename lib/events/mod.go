package events

import (
	"context"
	"database/sql"
)

var Sender EventSender = &NoopEventSender{}

// SendAsync sends the event without blocking and with no error reporting.
// If an error happens or the event queue is full, the event might be dropped.
//
func SendAsync(event Event) {
	Sender.SendAsync(event)
}

// SendTx ensures that the event is sent if the transaction is committed successfully.
// If the transaction rollbacks, the event will be discarded.
//
func SendTx(ctx context.Context, tx *sql.Tx, event Event) error {
	return Sender.SendInTx(ctx, tx, event)
}

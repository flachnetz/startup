package events

import (
	"context"
	"database/sql"
)

// SendAsync sends the event without blocking and with no error reporting.
// If an error happens or the event queue is full, the event might be dropped.
//
// If no async event sender is configured, the method will panic.
//
func SendAsync(event Event) {
	if sender, ok := Events.(AsyncEventSender); ok {
		sender.Send(event)
	} else {
		panic(ErrNoAsyncSupport)
	}
}

// SendTx ensures that the event is sent if the transaction is committed successfully.
// If the transaction rollbacks, the event will be discarded.
//
func SendTx(ctx context.Context, tx *sql.Tx, event Event) error {
	if sender, ok := Events.(TransactionalEventSender); ok {
		return sender.SendInTx(ctx, tx, event)
	} else {
		return ErrNoTxSupport
	}
}

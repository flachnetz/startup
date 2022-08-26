package events

import (
	"context"
	"github.com/jmoiron/sqlx"
)

var Sender EventSender = &NoopEventSender{}

// SendAsync sends the event without blocking and with no error reporting.
// If an error happens or the event queue is full, the event might be dropped.
func SendAsync(ctx context.Context, event Event) {
	Sender.SendAsync(ctx, event)
}

// SendTx ensures that the event is sent if the transaction is committed successfully.
// If the transaction rollbacks, the event will be discarded.
func SendTx(ctx context.Context, tx sqlx.ExecerContext, event Event) error {
	return Sender.SendInTx(ctx, tx, event)
}

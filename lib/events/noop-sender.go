package events

import (
	"context"
	"database/sql"
)

type NoopEventSender struct{}

func (n NoopEventSender) SendAsync(event Event) {
}

func (n NoopEventSender) SendInTx(ctx context.Context, tx *sql.Tx, event Event) error {
	return nil
}

func (n NoopEventSender) Close() error {
	return nil
}

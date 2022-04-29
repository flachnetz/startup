package events

import (
	"context"
	"database/sql"
)

type NoopEventSender struct{}

func (NoopEventSender) Send(Event) {
}

func (s NoopEventSender) SendInTx(ctx context.Context, tx *sql.Tx, event Event) error {
	return nil
}

func (NoopEventSender) Close() error {
	return nil
}

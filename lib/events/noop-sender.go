package events

import (
	"context"
	"github.com/jmoiron/sqlx"
)

type NoopEventSender struct{}

func (n NoopEventSender) SendAsync(ctx context.Context, event Event) {
}

func (n NoopEventSender) SendInTx(ctx context.Context, tx sqlx.ExecerContext, event Event) error {
	return nil
}

func (n NoopEventSender) Close() error {
	return nil
}

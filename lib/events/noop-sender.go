package events

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/flachnetz/startup/v2/lib/events/avro"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/jmoiron/sqlx"
)

type NoopEventSender struct{}

func (n *NoopEventSender) SendAsync(ctx context.Context, event Event) {
	err := event.Serialize(io.Discard)
	if err != nil {
		eventType := avro.EventTypeOf(event)
		slog.WarnContext(ctx, "Failed to serialize event", sl.Error(err), slog.String("error", eventType))
	}
}

func (n *NoopEventSender) SendInTx(_ context.Context, _ sqlx.ExecerContext, event Event) error {
	err := event.Serialize(io.Discard)
	if err != nil {
		eventType := avro.EventTypeOf(event)
		return fmt.Errorf("serialize event of type %T: %w", eventType, err)
	}

	return nil
}

func (n *NoopEventSender) Close() error {
	return nil
}

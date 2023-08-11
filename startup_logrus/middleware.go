package startup_logrus

import (
	"context"
	"fmt"
	"log/slog"
)

type Middleware func(context.Context, slog.Record) (slog.Record, bool, error)

type wrapped struct {
	slog.Handler
	middleware Middleware
}

func Wrap(handler slog.Handler, middleware Middleware) slog.Handler {
	return wrapped{Handler: handler, middleware: middleware}
}

func (w wrapped) Handle(ctx context.Context, record slog.Record) error {
	record, ok, err := w.middleware(ctx, record)
	if err != nil {
		return fmt.Errorf("apply middleware: %w", err)
	}

	if !ok {
		return nil
	}

	return w.Handler.Handle(ctx, record)
}

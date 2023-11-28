package startup_logrus

import (
	"context"
	"fmt"
	"log/slog"
)

type Middleware func(context.Context, slog.Record) (slog.Record, bool, error)

type wrapped struct {
	delegate   slog.Handler
	middleware Middleware
}

func Wrap(handler slog.Handler, middleware Middleware) slog.Handler {
	return wrapped{delegate: handler, middleware: middleware}
}

func (w wrapped) Enabled(ctx context.Context, level slog.Level) bool {
	return w.delegate.Enabled(ctx, level)
}

func (w wrapped) WithAttrs(attrs []slog.Attr) slog.Handler {
	w.delegate = w.delegate.WithAttrs(attrs)
	return w
}

func (w wrapped) WithGroup(name string) slog.Handler {
	w.delegate = w.delegate.WithGroup(name)
	return w
}

func (w wrapped) Handle(ctx context.Context, record slog.Record) error {
	record, ok, err := w.middleware(ctx, record)
	if err != nil {
		return fmt.Errorf("apply middleware: %w", err)
	}

	if !ok {
		return nil
	}

	return w.delegate.Handle(ctx, record)
}

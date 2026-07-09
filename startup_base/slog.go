package startup_base

import (
	"context"
	"log/slog"
)

// nilHandler discards everything and does as little work as possible
type nilHandler struct{}

func (n nilHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return false
}

func (n nilHandler) Handle(ctx context.Context, record slog.Record) error {
	return nil
}

func (n nilHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return n
}

func (n nilHandler) WithGroup(name string) slog.Handler {
	return n
}

// lazyHandler will defer calls to WithAttrs and WithGroup to the given Delegate and
// only evaluate those calls once an event is actually handled.
type lazyHandler struct {
	Delegate func() slog.Handler
}

func (v *lazyHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (v *lazyHandler) Handle(ctx context.Context, record slog.Record) error {
	return v.Delegate().Handle(ctx, record)
}

func (v *lazyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &lazyHandler{
		Delegate: func() slog.Handler {
			return v.Delegate().WithAttrs(attrs)
		},
	}
}

func (v *lazyHandler) WithGroup(name string) slog.Handler {
	return &lazyHandler{
		Delegate: func() slog.Handler {
			return v.Delegate().WithGroup(name)
		},
	}
}
